//! acct-d5t8 — property test for interleaved cost methods within a single
//! period close. Generates a fixture with N SKUs each randomly assigned
//! one of {standard, wac_perpetual, wac_periodic, wac_retroactive} per a
//! 50/30/15/5 mix (with a hard guarantee that all four methods appear at
//! least once per scenario), runs a random sequence of inventory
//! adjustments and retroactive cost-adjust queue entries against them,
//! then close_period with force_provisional=TRUE and asserts:
//!
//!   - close_period succeeded
//!   - 0 unfinalized posting_lines_provisional rows for the closed period
//!   - 0 unflushed inventory_cost_adjustments_retroactive rows
//!   - I1-I7 hold (assert_invariants_hold)
//!
//! The "hybridization" angle vs property_period_close.rs: that test only
//! mixes wac_periodic + wac_retroactive. This one adds standard +
//! wac_perpetual into the same period close run, plus mixes raw + fg
//! classes per op. Catches:
//!
//!   - Hook-ordering interactions when standard / wac_perpetual SKUs are
//!     present alongside SKUs that need close hooks
//!   - Cross-class drift in _post_posting_lines_compute_amount under
//!     interleaved cost methods (R2 credit-side resolution)
//!   - cost_adjust_retroactive_hook flushing across SKUs of all 4 methods
//!     without leaving orphans
//!
//! Acts as the regression baseline behavior for the upcoming acct-w0lo
//! dispatcher strategy-registration refactor.
//!
//! ≥100 scenarios via PROPTEST_CASES env var; --test-threads=1 required.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
enum InvClass {
    Raw,
    Fg,
}

impl InvClass {
    fn as_pg(&self) -> &'static str {
        match self {
            InvClass::Raw => "raw",
            InvClass::Fg => "fg",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    /// Adjust IN: positive qty_delta with asserted unit_cost. Posts a
    /// receipt-style adjustment that grows the (sku, class) pool.
    AdjustIn { sku_idx: usize, class: InvClass, qty: i64, unit_cost: i64 },
    /// Adjust OUT: negative qty_delta. wac_* depletes at running avg and
    /// flags posting_lines_provisional; standard depletes at std cost; all
    /// methods reduce qty.
    AdjustOut { sku_idx: usize, class: InvClass, qty: i64 },
    /// Queue retroactive cost-adjust entry. Method-agnostic — the close
    /// hook will flush against in-period depletions.
    QueueRetro { sku_idx: usize, class: InvClass, target_avg: i64 },
}

fn arb_class() -> impl Strategy<Value = InvClass> {
    prop_oneof![Just(InvClass::Raw), Just(InvClass::Fg)]
}

fn arb_op(num_skus: usize) -> impl Strategy<Value = Op> {
    let n = num_skus;
    prop_oneof![
        // 60% adjust IN
        3 => (0usize..n, arb_class(), 5i64..=50, 5i64..=20)
            .prop_map(|(s, c, q, uc)| Op::AdjustIn { sku_idx: s, class: c, qty: q, unit_cost: uc }),
        // 30% adjust OUT
        2 => (0usize..n, arb_class(), 3i64..=30)
            .prop_map(|(s, c, q)| Op::AdjustOut { sku_idx: s, class: c, qty: q }),
        // 10% queue retroactive
        1 => (0usize..n, arb_class(), 1i64..=25)
            .prop_map(|(s, c, t)| Op::QueueRetro { sku_idx: s, class: c, target_avg: t }),
    ]
}

fn arb_op_seq(num_skus: usize) -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(num_skus), 4..=12)
}

/// SKU population: 4 fixed slots, one per cost method, plus 0..=3 random
/// extras drawn from the 50/30/15/5 mix. Ensures every scenario exercises
/// all four methods.
fn arb_extras() -> impl Strategy<Value = Vec<CostMethod>> {
    let extra_dist = prop_oneof![
        50 => Just(CostMethod::Standard),
        30 => Just(CostMethod::WacPerpetual),
        15 => Just(CostMethod::WacPeriodic),
        5 => Just(CostMethod::WacRetroactive),
    ];
    prop::collection::vec(extra_dist, 0..=3)
}

#[derive(Default, Debug, Clone, Copy)]
struct Pool {
    qty: i64,
    value: i64,
}

#[tokio::test(flavor = "current_thread")]
async fn property_costmethod_hybrid_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let pid = period_id(&pool, "2026-04").await;

        // Population: one of each cost method, plus extras from the mix.
        let mut methods: Vec<CostMethod> = vec![
            CostMethod::Standard,
            CostMethod::WacPerpetual,
            CostMethod::WacPeriodic,
            CostMethod::WacRetroactive,
        ];
        let extras_tree = arb_extras().new_tree(&mut runner).expect("extras tree");
        let extras: Vec<CostMethod> = extras_tree.current();
        methods.extend(extras);

        let scenario_label =
            format!("hybrid#{case_idx}(n={}, ext={})", methods.len(), methods.len() - 4);

        // Build SKUs + per-class pools at one shared location. Pre-seed
        // each (sku, class) pool with 200 qty @ $10 = $2000 so depletions
        // never empty mid-scenario, AND so wac_periodic_close_hook always
        // has at least one in-period receipt.
        let loc = fresh_location(&pool, &format!("D5T8-LOC-{case_idx}")).await;
        let mut sku_ids: Vec<String> = Vec::with_capacity(methods.len());
        // pools[sku_idx][0]=raw, pools[sku_idx][1]=fg
        let mut pools: Vec<[Pool; 2]> = Vec::with_capacity(methods.len());

        for (i, cm) in methods.iter().enumerate() {
            let code = format!("D5T8-S{i}-{case_idx}");
            let sku = fresh_sku(&pool, &code, cm.as_pg()).await;
            // Standard SKUs need a standard_costs row before any
            // adjust-OUT can dispatch (_resolve_standard_cost_at otherwise
            // raises P0018).
            if matches!(cm, CostMethod::Standard) {
                set_std_cost(&pool, &sku, 10).await;
            }

            // stock_available is keyed on (sku, location) — one account
            // covers both raw + fg classes (per-class qty divisor lives
            // on the value pool's transfers.qty per mig 0030).
            let _ = open_account(
                &pool, "stock_available", "qty", None,
                Some(&sku), Some(&loc), None, "debit",
            ).await;
            for class in ["raw", "fg"] {
                let value_kind = format!("inv_value_{class}");
                let _ = open_account(
                    &pool, &value_kind, "value", Some("USD"),
                    Some(&sku), Some(&loc), None, "debit",
                ).await;
            }

            // Pre-seed both raw + fg pools. Each gets 200 qty.
            // For standard SKUs, post_inventory_adjustment rejects an
            // asserted unit_cost — they use std cost (= $10 from
            // set_std_cost above). For wac_* SKUs, we assert $10/unit.
            // Either way, pool ends up at 200 qty / $2000 value.
            let seed_uc = if matches!(cm, CostMethod::Standard) { None } else { Some(10) };
            seed_pool(&pool, &sku, &loc, "raw", 200, seed_uc).await;
            seed_pool(&pool, &sku, &loc, "fg", 200, seed_uc).await;

            sku_ids.push(sku);
            pools.push([Pool { qty: 200, value: 2000 }, Pool { qty: 200, value: 2000 }]);
        }

        // Generate op sequence and apply.
        let strategy = arb_op_seq(methods.len());
        let tree = strategy.new_tree(&mut runner).expect("op_seq tree");
        let ops: Vec<Op> = tree.current();

        let mut bd_offset: u32 = 2;
        for (i, op) in ops.iter().enumerate() {
            let bd = bd_in_april(bd_offset);
            bd_offset = (bd_offset + 1).min(28);
            let op_label = format!("{scenario_label}/op{i}");

            let res = match *op {
                Op::AdjustIn { sku_idx, class, qty, unit_cost } => {
                    // Standard SKUs reject p_unit_cost (use std cost); pass
                    // None and value-track at the seeded std cost ($10).
                    let is_std = matches!(methods[sku_idx], CostMethod::Standard);
                    let effective_uc = if is_std { 10 } else { unit_cost };
                    let r = adjust(
                        &pool,
                        &sku_ids[sku_idx],
                        &loc,
                        qty,
                        if is_std { None } else { Some(unit_cost) },
                        class.as_pg(),
                        &bd,
                    )
                    .await;
                    if r.is_ok() {
                        let p = &mut pools[sku_idx][class_slot(class)];
                        p.qty += qty;
                        p.value += qty * effective_uc;
                    }
                    r
                }
                Op::AdjustOut { sku_idx, class, qty } => {
                    let p = &pools[sku_idx][class_slot(class)];
                    // Skip if depletion would leave qty <= 0. Pool would
                    // empty mid-scenario and downstream ops on it would
                    // P0010 — fine, but skipping keeps op success rate up.
                    if qty >= p.qty {
                        continue;
                    }
                    let r = adjust(
                        &pool,
                        &sku_ids[sku_idx],
                        &loc,
                        -qty,
                        None,
                        class.as_pg(),
                        &bd,
                    )
                    .await;
                    if r.is_ok() {
                        let avg = if p.qty > 0 { p.value / p.qty } else { 0 };
                        let pm = &mut pools[sku_idx][class_slot(class)];
                        pm.qty -= qty;
                        pm.value -= qty * avg;
                    }
                    r
                }
                Op::QueueRetro { sku_idx, class, target_avg } => {
                    queue_retro(
                        &pool,
                        &sku_ids[sku_idx],
                        &loc,
                        class.as_pg(),
                        target_avg,
                        pid,
                        &bd,
                    )
                    .await
                }
            };

            if res.is_err() {
                continue;
            }

            assert_invariants_hold(&pool, &op_label).await;
        }

        // Close the period. force_provisional=TRUE bypasses P0020 if
        // some wac_periodic SKU happened to receive no in-period
        // receipts (rare but possible if all ops on that SKU were
        // adjust-OUT, which can't happen because pre-seed counts as a
        // receipt — defensive anyway).
        let actor = fresh_uuid(&pool).await;
        let close_res: sqlx::Result<serde_json::Value> = sqlx::query_scalar(
            "SELECT close_period($1, $2::UUID, TRUE, FALSE)",
        )
        .bind(pid)
        .bind(&actor)
        .fetch_one(&pool)
        .await;
        let summary = close_res.unwrap_or_else(|e| {
            panic!("[{scenario_label}] close_period failed: {e}")
        });

        // (a) all posting_lines_provisional rows for this period are finalized.
        let unfinalized: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM posting_lines_provisional
              WHERE period_id = $1 AND finalized_at IS NULL",
        )
        .bind(pid)
        .fetch_one(&pool)
        .await
        .expect("unfinalized count");
        assert_eq!(
            unfinalized, 0,
            "[{scenario_label}] {unfinalized} posting_lines_provisional rows un-finalized after close (summary={summary})",
        );

        // (b) all retroactive cost-adjust queue rows for this period are flushed.
        let unfinalized_retro: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inventory_cost_adjustments_retroactive
              WHERE target_period_id = $1 AND finalized_at IS NULL",
        )
        .bind(pid)
        .fetch_one(&pool)
        .await
        .expect("unfinalized retro count");
        assert_eq!(
            unfinalized_retro, 0,
            "[{scenario_label}] {unfinalized_retro} retro adjustments un-finalized after close",
        );

        // (c) standard 7 invariants hold post-close.
        assert_invariants_hold(&pool, &format!("{scenario_label}/post-close")).await;
    }
}

fn class_slot(c: InvClass) -> usize {
    match c {
        InvClass::Raw => 0,
        InvClass::Fg => 1,
    }
}

fn bd_in_april(offset: u32) -> String {
    let day = offset.clamp(2, 28);
    format!("2026-04-{day:02}")
}

// ============================================================
// Local scaffolding (mirrors property_period_close.rs)
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
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
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

async fn period_id(pool: &PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

/// Pre-seed a (sku, location, class) pool. Pass unit_cost=Some(N) for
/// wac_* SKUs to assert N$/unit; pass None for standard SKUs (their
/// post_inventory_adjustment uses the standard_costs row).
async fn seed_pool(pool: &PgPool, sku: &str, loc: &str, class: &str, qty: i64, unit_cost: Option<i64>) {
    adjust(pool, sku, loc, qty, unit_cost, class, "2026-04-02")
        .await
        .unwrap_or_else(|e| panic!("seed_pool {class} {qty}@{unit_cost:?}: {e}"));
}

async fn adjust(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    class: &str,
    business_date: &str,
) -> sqlx::Result<serde_json::Value> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
             $1::UUID, $2::UUID, $3, $4, 'USD', $5, $6::DATE,
             $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .map(serde_json::Value::String)
}

async fn queue_retro(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    class: &str,
    target_avg: i64,
    target_period_id: i64,
    business_date: &str,
) -> sqlx::Result<serde_json::Value> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_cost_adjustment_retroactive(
             $1, $2::UUID, $3::UUID, 'USD', $4, $5, $6::DATE,
             $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(target_period_id)
    .bind(sku)
    .bind(loc)
    .bind(class)
    .bind(target_avg)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .map(serde_json::Value::String)
}
