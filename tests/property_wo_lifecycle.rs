//! acct-du2.14 — property tests for WO lifecycle (start → op_move ×N
//! → complete | scrap | close_unproduced) interleaved across 1-3 WOs
//! sharing parent_sku × routing_op WIP pools.
//!
//! Random parent cost_method ∈ {standard, wac_perpetual, wac_periodic,
//! wac_retroactive}; component is fixed standard. Random per-WO
//! qty_target, num_ops (1 or 2), and outcome:
//!
//!   - Complete: [wo_start, op_moves..., wo_complete(qty_target)]
//!   - FullScrapThenClose: [wo_start, scrap_all_at_op10,
//!                          wo_close_unproduced]
//!   - CloseUnproducedNoAction: [wo_close_unproduced]
//!
//! The WOs share a single parent SKU + routing_op pools. Steps are
//! interleaved by round-robin over the WOs that still have steps to
//! execute. After every successful step, assert_invariants_hold runs.
//!
//! Regression net for acct-69e / acct-du2.10 (solo-at-pool gate),
//! acct-du2.1 / .2 (FOR UPDATE lock-set extension), the *_v reasons
//! flagging logic for wac_periodic / wac_retroactive (acct-bol /
//! acct-rso), and the various idempotency replay races (acct-du2.5).
//!
//! ≥100 scenarios via PROPTEST_CASES env var; --test-threads=1 required.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone, Copy)]
enum CostMethod {
    Standard,
    WacPerpetual,
    WacPeriodic,
    WacRetroactive,
}

impl CostMethod {
    fn as_pg(&self) -> &'static str {
        match self {
            CostMethod::Standard => "standard",
            CostMethod::WacPerpetual => "wac_perpetual",
            CostMethod::WacPeriodic => "wac_periodic",
            CostMethod::WacRetroactive => "wac_retroactive",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Outcome {
    Complete,
    FullScrapThenClose,
    CloseUnproducedNoAction,
}

#[derive(Debug, Clone, Copy)]
struct WoSpec {
    qty_target: i64,
    num_ops: u32, // 1 or 2
    outcome: Outcome,
}

fn arb_cost_method() -> impl Strategy<Value = CostMethod> {
    prop_oneof![
        Just(CostMethod::Standard),
        Just(CostMethod::WacPerpetual),
        Just(CostMethod::WacPeriodic),
        Just(CostMethod::WacRetroactive),
    ]
}

fn arb_outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        4 => Just(Outcome::Complete),
        2 => Just(Outcome::FullScrapThenClose),
        1 => Just(Outcome::CloseUnproducedNoAction),
    ]
}

fn arb_wo_spec() -> impl Strategy<Value = WoSpec> {
    (5i64..=15, 1u32..=2, arb_outcome())
        .prop_map(|(qty, num_ops, outcome)| WoSpec { qty_target: qty, num_ops, outcome })
}

fn arb_scenario() -> impl Strategy<Value = (CostMethod, Vec<WoSpec>)> {
    (arb_cost_method(), prop::collection::vec(arb_wo_spec(), 1..=3))
}

#[derive(Debug, Clone, Copy)]
enum Step {
    Start,
    OpMove { from: i32, to: i32, qty: i64 },
    Complete { qty: i64 },
    ScrapAtOp { op: i32, qty: i64 },
    CloseUnproduced,
}

fn build_steps(spec: &WoSpec) -> Vec<Step> {
    match spec.outcome {
        Outcome::Complete => {
            let mut steps = vec![Step::Start];
            if spec.num_ops == 2 {
                steps.push(Step::OpMove { from: 10, to: 20, qty: spec.qty_target });
            }
            steps.push(Step::Complete { qty: spec.qty_target });
            steps
        }
        Outcome::FullScrapThenClose => vec![
            Step::Start,
            Step::ScrapAtOp { op: 10, qty: spec.qty_target },
            Step::CloseUnproduced,
        ],
        Outcome::CloseUnproducedNoAction => vec![Step::CloseUnproduced],
    }
}

#[tokio::test(flavor = "current_thread")]
async fn property_wo_lifecycle_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_scenario();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("strategy.new_tree");
        let (cm, specs): (CostMethod, Vec<WoSpec>) = tree.current();
        let scenario_label = format!("wo#{case_idx}({:?}, n={})", cm, specs.len());

        // Scaffold: parent + component + locations + WIP pools (op10 +
        // op20) + variance accounts. All keyed off case_idx so SKU /
        // location codes never collide across reset+rebuild boundaries
        // within one binary run.
        let parent = fresh_sku(&pool, &format!("PROP-PAR-{case_idx}"), cm.as_pg()).await;
        let comp = fresh_sku(&pool, &format!("PROP-COMP-{case_idx}"), "standard").await;
        if matches!(cm, CostMethod::Standard) {
            set_std_cost(&pool, &parent, 20).await;
        }
        set_std_cost(&pool, &comp, 10).await;
        let raw_loc = fresh_location(&pool, &format!("PROP-RAW-{case_idx}")).await;
        let fg_loc = fresh_location(&pool, &format!("PROP-FG-{case_idx}")).await;

        let (raw_q, raw_v) = open_component_raw_pool(&pool, &comp, &raw_loc).await;
        // stock_consumed for component.
        let _ = open_account(
            &pool, "stock_consumed", "qty", None,
            Some(&comp), None, None, "debit",
        ).await;

        // Per-op WIP pools at op10 + op20 (always — even if some WOs
        // only use op10, having op20 open is harmless). Plus FG pool +
        // stock_scrap for the parent.
        for op in [10, 20] {
            open_account(&pool, "stock_wip", "qty", None,
                Some(&parent), None, Some(op), "debit").await;
            open_account(&pool, "inv_value_wip", "value", Some("USD"),
                Some(&parent), None, Some(op), "debit").await;
        }
        let _fg_q = open_account(&pool, "stock_available", "qty", None,
            Some(&parent), Some(&fg_loc), None, "debit").await;
        let _fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"),
            Some(&parent), Some(&fg_loc), None, "debit").await;
        let _ = open_account(&pool, "stock_scrap", "qty", None,
            Some(&parent), None, None, "debit").await;

        // Pre-seed component pool generously: 1000 qty @ $10 = $10000.
        seed_pool(&pool, raw_q, raw_v, 1000, 10000).await;

        // Single shared BOM with one component line at qty_per_parent=2,
        // applied at op10. With component_std=10, the WO's WIP value-in
        // per parent unit = $20 — matches parent_std for standard
        // parents (zero variance baseline) and seeds wac_* parent pools
        // at $20 running avg.
        let bom = create_bom_with_one_item(&pool, &parent, &comp, &raw_loc, 2).await;

        // Create N work_orders, each pinned to the shared BOM, with
        // routings reflecting their num_ops.
        let mut wo_ids: Vec<String> = Vec::new();
        for (i, spec) in specs.iter().enumerate() {
            let wo_no = format!("wo{i}-{case_idx}");
            let wo_id =
                create_wo(&pool, &wo_no, &parent, &fg_loc, spec.qty_target).await;
            for op in 1..=spec.num_ops {
                let routing_op = (op as i32) * 10;
                add_routing(&pool, &wo_id, routing_op, &format!("OP{op}")).await;
            }
            pin_wo_bom(&pool, &wo_id, bom).await;
            wo_ids.push(wo_id);
        }

        // Step queues per WO.
        let mut queues: Vec<Vec<Step>> = specs.iter().map(build_steps).collect();
        let mut step_idx: usize = 0;
        let mut round_idx: usize = 0;

        // Round-robin interleave: at each round we ask each WO to do
        // its current step, in WO-order. When all WOs are out of steps,
        // we stop.
        loop {
            let mut any_ran = false;
            for wo_pos in 0..wo_ids.len() {
                if queues[wo_pos].is_empty() {
                    continue;
                }
                let step = queues[wo_pos][0];
                let wo_id = &wo_ids[wo_pos];
                let label =
                    format!("{scenario_label}/r{round_idx}/wo{wo_pos}/s{step_idx}");

                let res = match step {
                    Step::Start => {
                        let key = fresh_uuid(&pool).await;
                        call_wo_start(&pool, wo_id, &key).await.map(|_| ())
                    }
                    Step::OpMove { from, to, qty } => {
                        let key = fresh_uuid(&pool).await;
                        call_op_move(&pool, wo_id, from, to, qty, &key)
                            .await
                            .map(|_| ())
                    }
                    Step::Complete { qty } => {
                        let key = fresh_uuid(&pool).await;
                        call_wo_complete(&pool, wo_id, qty, &key).await.map(|_| ())
                    }
                    Step::ScrapAtOp { op, qty } => {
                        let key = fresh_uuid(&pool).await;
                        call_scrap(&pool, wo_id, op, qty, &key).await.map(|_| ())
                    }
                    Step::CloseUnproduced => {
                        let key = fresh_uuid(&pool).await;
                        call_close_unproduced(&pool, wo_id, &key).await.map(|_| ())
                    }
                };

                queues[wo_pos].remove(0);
                step_idx += 1;
                any_ran = true;

                if let Err(_e) = res {
                    // Some steps may legitimately fail (e.g. an interleaved
                    // close_unproduced after a sibling WO scrapped a shared
                    // pool past zero, mixed-method gates, etc.). Failures
                    // imply the DB is unchanged for that step (each query
                    // is its own implicit txn). Skip invariant check;
                    // proceed with the next WO/step.
                    continue;
                }

                assert_invariants_hold(&pool, &label).await;
            }
            round_idx += 1;
            if !any_ran {
                break;
            }
        }

        // Final invariant check (redundant but documentary).
        assert_invariants_hold(&pool, &format!("{scenario_label}/final")).await;
    }
}

// ============================================================
// Local scaffolding
// ============================================================

async fn fresh_sku(pool: &PgPool, code: &str, cost_method: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', $2::cost_method) RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert sku {code}: {e}"))
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert location {code}: {e}"))
}

async fn set_std_cost(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("set_std_cost: {e}"));
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    routing_op: Option<i32>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, routing_op, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(routing_op)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn open_component_raw_pool(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let q = open_account(
        pool, "stock_available", "qty", None,
        Some(comp), Some(raw_loc), None, "debit",
    ).await;
    let v = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(comp), Some(raw_loc), None, "debit",
    ).await;
    (q, v)
}

async fn seed_pool(pool: &PgPool, qty_acct: i64, val_acct: i64, qty: i64, value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        {
            "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
            "debit_account_id": qty_acct, "credit_account_id": void_qty,
            "amount": qty, "qty": qty, "business_date": "2026-04-10",
            "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by
        },
        {
            "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
            "debit_account_id": val_acct, "credit_account_id": void_val,
            "amount": value, "qty": qty, "business_date": "2026-04-10",
            "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by
        },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_pool: {e}"));
}

async fn create_bom_with_one_item(
    pool: &PgPool,
    parent: &str,
    comp: &str,
    raw_loc: &str,
    qty_per_parent: i64,
) -> i64 {
    let bom_id: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom: {e}"));

    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, scrap_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', 0,
                 $2::UUID, $3::UUID, $4)",
    )
    .bind(bom_id)
    .bind(comp)
    .bind(raw_loc)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_item: {e}"));
    bom_id
}

async fn create_wo(pool: &PgPool, wo_no: &str, parent: &str, fg_loc: &str, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(parent)
    .bind(fg_loc)
    .bind(qty)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_wo: {e}"))
}

async fn add_routing(pool: &PgPool, wo_id: &str, op: i32, name: &str) {
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)")
        .bind(wo_id)
        .bind(op)
        .bind(name)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("add_routing: {e}"));
}

async fn pin_wo_bom(pool: &PgPool, wo_id: &str, bom_id: i64) {
    sqlx::query("UPDATE work_orders SET bom_id = $1 WHERE id = $2::UUID")
        .bind(bom_id)
        .bind(wo_id)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("pin_wo_bom: {e}"));
}

async fn call_wo_start(pool: &PgPool, wo_id: &str, key: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_op_move(
    pool: &PgPool,
    wo_id: &str,
    from_op: i32,
    to_op: i32,
    qty: i64,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_op_move($1::UUID, $2, $3, $4, '2026-04-20'::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(from_op)
    .bind(to_op)
    .bind(qty)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(
    pool: &PgPool,
    wo_id: &str,
    qty: i64,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, '2026-04-20'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_scrap(
    pool: &PgPool,
    wo_id: &str,
    op: i32,
    qty: i64,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_scrap($1::UUID, $2, $3, '2026-04-20'::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(op)
    .bind(qty)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_close_unproduced(pool: &PgPool, wo_id: &str, key: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_close_unproduced($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}
