//! Tier 2 of acct-8in (acct-smn). Tests wac_periodic parent SKUs on
//! multi-op routings end-to-end through op_move + close hook's
//! topological per-pool recompute.
//!
//! Coverage:
//!   * 2-op clean: single WO, no drift → all variances 0.
//!   * 2-op drift: two WOs with different BOMs, op10 final_avg drifts;
//!     op_move_v rows recorded variance, do not post; chain correction
//!     captured at wo_complete_v leaf.
//!   * 3-op chain: extends drift through three pools; cache propagation.
//!   * Rework cycle: op_move(20→10) raises P0036.
//!   * Mixed standard + wac_periodic: standard WO doesn't flag, doesn't
//!     touch wac analysis.

mod common;

use common::*;
use sqlx::PgPool;
use serde_json::json;

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
    .unwrap_or_else(|e| panic!("set_std_cost {sku_id}={cost}: {e}"));
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

async fn create_wo(pool: &PgPool, wo_no: &str, parent_id: &str, fg_loc: &str, qty_target: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(parent_id)
    .bind(fg_loc)
    .bind(qty_target)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_wo: {e}"))
}

async fn pin_wo_bom(pool: &PgPool, wo_id: &str, bom_id: i64) {
    sqlx::query("UPDATE work_orders SET bom_id = $1 WHERE id = $2::UUID")
        .bind(bom_id)
        .bind(wo_id)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("pin_wo_bom: {e}"));
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

async fn create_bom_header(pool: &PgPool, parent: &str, alternate_no: i32, is_primary: bool) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, $2, 'A', $3, 'active') RETURNING id",
    )
    .bind(parent)
    .bind(alternate_no)
    .bind(is_primary)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom_header: {e}"))
}

async fn add_bom_item(pool: &PgPool, bom_id: i64, line_no: i32, applies_at_op: i32, comp: &str, comp_loc: &str, qty_per_parent: i64) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 100,
                 $4::UUID, $5::UUID, $6)",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(comp)
    .bind(comp_loc)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_item: {e}"));
}

async fn pre_load_raw(pool: &PgPool, raw_qty: i64, raw_val: i64, qty: i64, value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": qty, "qty": qty, "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": value, "qty": qty, "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("pre_load_raw: {e}"));
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn period_id(pool: &PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
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

async fn call_op_move(pool: &PgPool, wo_id: &str, from_op: i32, to_op: i32, qty: i64, key: &str) -> sqlx::Result<String> {
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

async fn call_wo_complete(pool: &PgPool, wo_id: &str, qty: i64, key: &str) -> sqlx::Result<String> {
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

async fn close_period(pool: &PgPool, pid: i64) -> serde_json::Value {
    try_close_period(pool, pid).await.expect("close_period")
}

async fn try_close_period(pool: &PgPool, pid: i64) -> sqlx::Result<serde_json::Value> {
    let actor = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(pool)
    .await
}

/// Multi-op WIP scaffold: a wac_periodic parent SKU with stock_wip / inv_value_wip
/// accounts at every routing op in `ops`, plus FG accounts. Returns shared infra
/// IDs for tests to use across multiple WOs.
struct MultiOpInfra {
    parent_id: String,
    fg_loc: String,
    fg_val_acct: i64,
    raw_loc: String,
    wip_qty_by_op: Vec<(i32, i64)>,
    wip_val_by_op: Vec<(i32, i64)>,
}

impl MultiOpInfra {
    fn val_op(&self, op: i32) -> i64 {
        self.wip_val_by_op.iter().find(|(o, _)| *o == op).expect("op val").1
    }
    fn qty_op(&self, op: i32) -> i64 {
        self.wip_qty_by_op.iter().find(|(o, _)| *o == op).expect("op qty").1
    }
}

async fn build_multi_op_infra(pool: &PgPool, parent_code: &str, ops: &[i32]) -> MultiOpInfra {
    let parent = fresh_sku(pool, parent_code, "wac_periodic").await;
    let fg_loc = fresh_location(pool, &format!("{parent_code}-FG")).await;
    let raw_loc = fresh_location(pool, &format!("{parent_code}-RAW")).await;

    let mut wip_qty_by_op = Vec::new();
    let mut wip_val_by_op = Vec::new();
    for &op in ops {
        let q = open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(op), "debit").await;
        let v = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(op), "debit").await;
        wip_qty_by_op.push((op, q));
        wip_val_by_op.push((op, v));
    }
    let _fg_qty = open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let fg_val_acct = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;

    MultiOpInfra { parent_id: parent, fg_loc, fg_val_acct, raw_loc, wip_qty_by_op, wip_val_by_op }
}

// ============================================================
// Tests
// ============================================================

/// 2-op chain, single WO: no drift.
///   Routing op10 → op20. BOM: comp (std=10, qty/p=2) at op10.
///   wo_start: rm_issue 20×$10 = $200 → pool@op10 = $200, qty 10.
///   op_move(10→20, 10): provisional unit 200/10=$20 → pool@op20 += $200, qty 10. pool@op10=$0.
///   wo_complete(10): provisional unit 200/10=$20 → drain $200 to FG. pool@op20=$0.
///   Close: pool@op10 final_avg = $200/10 = $20 (no drift). pool@op20 final_avg = $200/10 = $20.
///   All variances = 0; op_move_v variance recorded as 0; wo_complete_v variance 0.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_two_op_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_multi_op_infra(&pool, "WAC2-CLEAN", &[10, 20]).await;
    let comp = fresh_sku(&pool, "WAC2-CLEAN-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_qty = open_account(&pool, "stock_available", "qty", None, Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    pre_load_raw(&pool, raw_qty, raw_val, 200, 2000).await;
    let _consumed = open_account(&pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit").await;

    let wo_id = create_wo(&pool, "WAC2-CLEAN-WO", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    add_routing(&pool, &wo_id, 20, "FINISH").await;

    let bom_id = create_bom_header(&pool, &infra.parent_id, 1, true).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &infra.raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, infra.val_op(10)).await, 200);
    assert_eq!(balance(&pool, infra.qty_op(10)).await, 10);

    call_op_move(&pool, &wo_id, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, infra.val_op(10)).await, 0, "op10 drained");
    assert_eq!(balance(&pool, infra.val_op(20)).await, 200, "op20 received");
    assert_eq!(balance(&pool, infra.qty_op(20)).await, 10);

    call_wo_complete(&pool, &wo_id, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 200, "FG drained");
    assert_eq!(balance(&pool, infra.val_op(20)).await, 0, "op20 drained");

    // Both op_move_v and wo_complete_v flagged.
    let flagged_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
           AND t.reason IN ('op_move_v', 'wo_complete_v')",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(flagged_count, 2, "both op_move_v and wo_complete_v flagged");

    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(2));

    // No drift → all variances 0.
    let variances: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT t.reason::text, tp.variance_amount
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
          ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        variances,
        vec![
            ("op_move_v".to_string(), Some(0)),
            ("wo_complete_v".to_string(), Some(0)),
        ],
        "all variances zero on no-drift chain"
    );

    // Pool balances stay zero (no variance was posted to inv_value_wip).
    assert_eq!(balance(&pool, infra.val_op(10)).await, 0);
    assert_eq!(balance(&pool, infra.val_op(20)).await, 0);
}

/// 2-op chain, two WOs with different BOM costs: drift through op10 →
/// captured at wo_complete_v leaf via final_avg cache.
///
///   WO1: BOM alt=1 with comp std=10, qty/p=2 → rm_issue $200 for qty 10.
///        op_move(10→20, 10) at running avg $20 → pool@op20 += $200.
///        wo_complete(10) at running avg $20 → drain $200 to FG.
///   WO2: BOM alt=2 with comp std=20, qty/p=2 → rm_issue $400 for qty 10.
///        op_move(10→20, 10) at running avg $40 → pool@op20 += $400.
///        wo_complete(10) at running avg $40 → drain $400 to FG.
///
///   In-period flow:
///     pool@op10 receipts: WO1 $200 + WO2 $400 = $600. qty in 20.
///     final_avg(op10) = $600 / 20 = $30.
///     WO1 op_move_v variance = ($30 − $20) × 10 = +$100.
///     WO2 op_move_v variance = ($30 − $40) × 10 = -$100.
///
///     pool@op20 corrected receipts (cache lookup):
///       WO1 op_move corrected = $200 + $100 = $300.
///       WO2 op_move corrected = $400 − $100 = $300.
///     corrected_value_in(op20) = $600. qty_in = 20. final_avg(op20) = $30.
///     WO1 wo_complete_v variance = ($30 − $20) × 10 = +$100.
///     WO2 wo_complete_v variance = ($30 − $40) × 10 = -$100.
///
///   Posted:
///     op_move_v rows: variance_amount recorded, NO variance transfer
///       (variance_transfer_id IS NULL). pool@op10/op20 untouched.
///     WO1 wo_complete_v: dr fg / cr variance_wac_period $100 (single-leg).
///     WO2 wo_complete_v: dr variance_wac_period / cr fg $100.
///
///   Final balances: FG = $200 + $400 + $100 − $100 = $600. variance_wac_period = 0.
///   Per-WO economics: WO1's 10 units cost $300 ($30/unit); WO2's 10 cost
///   $300 ($30/unit). Both at the period average.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_two_op_drift_propagates_to_leaf() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_multi_op_infra(&pool, "WAC2-DRIFT", &[10, 20]).await;
    let comp1 = fresh_sku(&pool, "WAC2-DRIFT-C1", "standard").await;
    let comp2 = fresh_sku(&pool, "WAC2-DRIFT-C2", "standard").await;
    set_std_cost(&pool, &comp1, 10).await;
    set_std_cost(&pool, &comp2, 20).await;

    let raw_qty1 = open_account(&pool, "stock_available", "qty", None, Some(&comp1), Some(&infra.raw_loc), None, "debit").await;
    let raw_val1 = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp1), Some(&infra.raw_loc), None, "debit").await;
    let raw_qty2 = open_account(&pool, "stock_available", "qty", None, Some(&comp2), Some(&infra.raw_loc), None, "debit").await;
    let raw_val2 = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp2), Some(&infra.raw_loc), None, "debit").await;
    pre_load_raw(&pool, raw_qty1, raw_val1, 100, 1000).await;
    pre_load_raw(&pool, raw_qty2, raw_val2, 100, 2000).await;
    let _consumed1 = open_account(&pool, "stock_consumed", "qty", None, Some(&comp1), None, None, "debit").await;
    let _consumed2 = open_account(&pool, "stock_consumed", "qty", None, Some(&comp2), None, None, "debit").await;

    // Two BOM headers under same parent (alternates 1 and 2).
    let bom1 = create_bom_header(&pool, &infra.parent_id, 1, true).await;
    add_bom_item(&pool, bom1, 1, 10, &comp1, &infra.raw_loc, 2).await;
    let bom2 = create_bom_header(&pool, &infra.parent_id, 2, false).await;
    add_bom_item(&pool, bom2, 1, 10, &comp2, &infra.raw_loc, 2).await;

    let wo1 = create_wo(&pool, "WAC2-DRIFT-WO1", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    add_routing(&pool, &wo1, 20, "FINISH").await;
    pin_wo_bom(&pool, &wo1, bom1).await;

    let wo2 = create_wo(&pool, "WAC2-DRIFT-WO2", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    add_routing(&pool, &wo2, 20, "FINISH").await;
    pin_wo_bom(&pool, &wo2, bom2).await;

    let pid = period_id(&pool, "2026-04").await;

    // WO1 sequential: start, op_move, complete.
    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo1, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();

    // WO2 sequential: drives drift on op10 final_avg.
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo2, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();

    // FG mid-period (before close): $200 (WO1) + $400 (WO2) = $600 at provisional unit costs.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 600, "FG pre-close");
    assert_eq!(balance(&pool, infra.val_op(10)).await, 0, "op10 drained");
    assert_eq!(balance(&pool, infra.val_op(20)).await, 0, "op20 drained");

    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(4));

    // Inspect variances per row.
    let rows: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT t.reason::text, tp.variance_amount, tp.variance_transfer_id
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
          ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);
    // WO1 op_move_v (+100) — internal, no transfer.
    assert_eq!(rows[0].0, "op_move_v");
    assert_eq!(rows[0].1, Some(100));
    assert_eq!(rows[0].2, None, "op_move_v variance not posted");
    // WO1 wo_complete_v (+100) — leaf, single-leg posted.
    assert_eq!(rows[1].0, "wo_complete_v");
    assert_eq!(rows[1].1, Some(100));
    assert!(rows[1].2.is_some(), "wo_complete_v variance posted");
    // WO2 op_move_v (-100) — internal.
    assert_eq!(rows[2].0, "op_move_v");
    assert_eq!(rows[2].1, Some(-100));
    assert_eq!(rows[2].2, None);
    // WO2 wo_complete_v (-100) — leaf.
    assert_eq!(rows[3].0, "wo_complete_v");
    assert_eq!(rows[3].1, Some(-100));
    assert!(rows[3].2.is_some());

    // Pool@op10 / op20 stayed at zero (no internal variance posted).
    assert_eq!(balance(&pool, infra.val_op(10)).await, 0, "op10 untouched by close");
    assert_eq!(balance(&pool, infra.val_op(20)).await, 0, "op20 untouched by close");

    // FG: started $600, +$100 (WO1 wo_complete_v variance), -$100 (WO2) = $600.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 600, "FG balance preserved");

    // variance_wac_period: net 0 for clean drift (no truncation residue).
    let var_acct = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0, "variance_wac_period net 0");
}

/// 3-op chain: cost shift propagates through three pools.
///   Routing 10 → 20 → 30. BOM: comp std=10 at op10 (qty/p=2).
///   Single WO qty=5. rm_issue $100 → pool@op10. op_move(10→20) at $20 →
///   pool@op20=$100. op_move(20→30) at $20 → pool@op30=$100.
///   wo_complete(5) at $20 → drain $100 to FG.
///   No drift since single WO; final_avg(op10/20/30) = $20. All variances 0.
///   Verifies the cache propagates correctly through 3 pools.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_three_op_chain_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_multi_op_infra(&pool, "WAC3", &[10, 20, 30]).await;
    let comp = fresh_sku(&pool, "WAC3-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_qty = open_account(&pool, "stock_available", "qty", None, Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 1000).await;
    let _consumed = open_account(&pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit").await;

    let wo = create_wo(&pool, "WAC3-WO", &infra.parent_id, &infra.fg_loc, 5).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    add_routing(&pool, &wo, 20, "GRIND").await;
    add_routing(&pool, &wo, 30, "FINISH").await;
    let bom = create_bom_header(&pool, &infra.parent_id, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo, 20, 30, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo, 5, &fresh_uuid(&pool).await).await.unwrap();

    assert_eq!(balance(&pool, infra.fg_val_acct).await, 100);

    let summary = close_period(&pool, pid).await;
    // 2 op_move_v + 1 wo_complete_v.
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(3));

    let variances: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT t.reason::text, tp.variance_amount
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
          ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        variances,
        vec![
            ("op_move_v".to_string(), Some(0)),
            ("op_move_v".to_string(), Some(0)),
            ("wo_complete_v".to_string(), Some(0)),
        ],
        "no drift on single WO; topological walk through 3 pools is clean"
    );
}

/// 3-op chain with drift: two WOs at different costs through three pools.
/// Cache propagates op10's correction through op20 to op30, ending up at
/// FG (the leaf) for the entire chain.
///
///   WO1: comp std=10 (qty/p=2) → rm_issue=$200/qty10. Pool@op10: $20/u.
///        op_move 10→20 at $20×10=$200. Pool@op20: $20/u.
///        op_move 20→30 at $20×10=$200. Pool@op30: $20/u.
///        wo_complete(10) at $20×10=$200. Drains to FG.
///   WO2: comp std=20 (qty/p=2) → rm_issue=$400/qty10. Pool@op10: $40/u.
///        op_move 10→20 at $40×10=$400. Pool@op20: $40/u.
///        op_move 20→30 at $40×10=$400. Pool@op30: $40/u.
///        wo_complete(10) at $40×10=$400. Drains to FG.
///
///   Pool@op10: in $600/qty 20. final_avg = $30. Provisional unit costs
///     for each op_move_v: WO1=$20, WO2=$40. Variances: +$100 / -$100.
///   Pool@op20: corrected receipts = ($200+$100) + ($400-$100) = $600/qty 20.
///     final_avg = $30. op_move_v variances at this pool: WO1 (orig $200 cor
///     $300, prov unit $20, var = ($30-$20)*10 = +$100), WO2 (orig $400 cor
///     $300, var = ($30-$40)*10 = -$100).
///   Pool@op30: same propagation; same result.
///   Leaf wo_complete_v: WO1 var +$100, WO2 var -$100. Posted to FG /
///     variance_wac_period.
///
///   FG: $200 + $400 + $100 - $100 = $600. variance_wac_period: 0.
///   Pools at op10/op20/op30 unchanged by close (no internal posting).
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_three_op_drift_propagates() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_multi_op_infra(&pool, "WAC3D", &[10, 20, 30]).await;
    let comp1 = fresh_sku(&pool, "WAC3D-C1", "standard").await;
    let comp2 = fresh_sku(&pool, "WAC3D-C2", "standard").await;
    set_std_cost(&pool, &comp1, 10).await;
    set_std_cost(&pool, &comp2, 20).await;

    let raw_qty1 = open_account(&pool, "stock_available", "qty", None, Some(&comp1), Some(&infra.raw_loc), None, "debit").await;
    let raw_val1 = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp1), Some(&infra.raw_loc), None, "debit").await;
    let raw_qty2 = open_account(&pool, "stock_available", "qty", None, Some(&comp2), Some(&infra.raw_loc), None, "debit").await;
    let raw_val2 = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp2), Some(&infra.raw_loc), None, "debit").await;
    pre_load_raw(&pool, raw_qty1, raw_val1, 100, 1000).await;
    pre_load_raw(&pool, raw_qty2, raw_val2, 100, 2000).await;
    let _consumed1 = open_account(&pool, "stock_consumed", "qty", None, Some(&comp1), None, None, "debit").await;
    let _consumed2 = open_account(&pool, "stock_consumed", "qty", None, Some(&comp2), None, None, "debit").await;

    let bom1 = create_bom_header(&pool, &infra.parent_id, 1, true).await;
    add_bom_item(&pool, bom1, 1, 10, &comp1, &infra.raw_loc, 2).await;
    let bom2 = create_bom_header(&pool, &infra.parent_id, 2, false).await;
    add_bom_item(&pool, bom2, 1, 10, &comp2, &infra.raw_loc, 2).await;

    let wo1 = create_wo(&pool, "WAC3D-WO1", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    add_routing(&pool, &wo1, 20, "GRIND").await;
    add_routing(&pool, &wo1, 30, "FINISH").await;
    pin_wo_bom(&pool, &wo1, bom1).await;

    let wo2 = create_wo(&pool, "WAC3D-WO2", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    add_routing(&pool, &wo2, 20, "GRIND").await;
    add_routing(&pool, &wo2, 30, "FINISH").await;
    pin_wo_bom(&pool, &wo2, bom2).await;

    let pid = period_id(&pool, "2026-04").await;

    for wo in [&wo1, &wo2] {
        call_wo_start(&pool, wo, &fresh_uuid(&pool).await).await.unwrap();
        call_op_move(&pool, wo, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
        call_op_move(&pool, wo, 20, 30, 10, &fresh_uuid(&pool).await).await.unwrap();
        call_wo_complete(&pool, wo, 10, &fresh_uuid(&pool).await).await.unwrap();
    }

    // Mid-period FG: $200 + $400 = $600.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 600);

    let summary = close_period(&pool, pid).await;
    // 2 op_move_v × 2 ops × 2 WOs = 4 op_move_v + 2 wo_complete_v = 6.
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(6));

    let rows: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT t.reason::text, tp.variance_amount, tp.variance_transfer_id
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
          ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 6);

    // Per-WO expected variances: WO1 +100 across all 3 chain points,
    // WO2 -100 across all 3 chain points. Order of transfer_ids:
    //   WO1: op_move_v (10→20), op_move_v (20→30), wo_complete_v.
    //   WO2: op_move_v (10→20), op_move_v (20→30), wo_complete_v.
    let expected: Vec<(&str, i64, bool)> = vec![
        ("op_move_v",     100,  false),
        ("op_move_v",     100,  false),
        ("wo_complete_v", 100,  true),
        ("op_move_v",    -100,  false),
        ("op_move_v",    -100,  false),
        ("wo_complete_v",-100,  true),
    ];
    for (i, (reason, var, has_xfer)) in expected.iter().enumerate() {
        assert_eq!(&rows[i].0, reason, "row {} reason", i);
        assert_eq!(rows[i].1, Some(*var), "row {} variance", i);
        assert_eq!(rows[i].2.is_some(), *has_xfer,
                   "row {} variance_transfer_id (op_move_v internal vs wo_complete_v leaf)", i);
    }

    // Pool balances stay zero (no internal variance posted).
    assert_eq!(balance(&pool, infra.val_op(10)).await, 0);
    assert_eq!(balance(&pool, infra.val_op(20)).await, 0);
    assert_eq!(balance(&pool, infra.val_op(30)).await, 0);

    // FG: 600 + 100 - 100 = 600.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 600);

    // variance_wac_period: net 0.
    let var_acct = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0);
}

// NOTE: an interleaved multi-WO same-parent same-pool test was prototyped
// here but exposes a pre-existing tier-1 bug in post_wo_complete:
// the pre-balance step (mig 0061) compares this WO's `total_drain`
// against the WHOLE pool@last_op balance, which includes other still-
// active WOs' WIP. When WO1 closes while WO2 is mid-flight in the same
// pool, WO1's pre-balance absorbs WO2's portion into variance_wo_close
// — an over-eager sweep, present for standard / wac_perpetual /
// wac_periodic alike. Tracked as a separate follow-up; out of tier 2's
// scope (which is the close-hook recompute itself, not the residual
// sweep boundary). Sequential multi-WO same-pool same-period is covered
// by `wac_periodic_two_op_drift_propagates_to_leaf` and
// `wac_periodic_three_op_drift_propagates`.

/// Rework cycle: a wac_periodic WO does op_move(20 → 10) creating a cycle
/// in the pool DAG. close hook detects via Kahn's algorithm and raises P0036.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_rework_cycle_raises_p0036() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_multi_op_infra(&pool, "WACRWK", &[10, 20]).await;
    let comp = fresh_sku(&pool, "WACRWK-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_qty = open_account(&pool, "stock_available", "qty", None, Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 1000).await;
    let _consumed = open_account(&pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit").await;

    let wo = create_wo(&pool, "WACRWK-WO", &infra.parent_id, &infra.fg_loc, 5).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    add_routing(&pool, &wo, 20, "FINISH").await;
    let bom = create_bom_header(&pool, &infra.parent_id, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    // Rework: send back to op10. Edge op20 → op10 combined with op10 → op20
    // creates a cycle in the DAG.
    call_op_move(&pool, &wo, 20, 10, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo, 5, &fresh_uuid(&pool).await).await.unwrap();

    expect_sqlstate(
        "P0036",
        || async { try_close_period(&pool, pid).await.map(|_| ()) },
    )
    .await;
}

/// Mixed standard + wac_periodic WOs in same period: standard WO doesn't
/// flag anything; wac_periodic walk only touches wac-flagged pools.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_mixed_with_standard() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_multi_op_infra(&pool, "WACMIX", &[10, 20]).await;
    let comp = fresh_sku(&pool, "WACMIX-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_qty = open_account(&pool, "stock_available", "qty", None, Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&infra.raw_loc), None, "debit").await;
    pre_load_raw(&pool, raw_qty, raw_val, 200, 2000).await;
    let _consumed = open_account(&pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit").await;

    // Standard parent in parallel.
    let std_parent = fresh_sku(&pool, "WACMIX-STD", "standard").await;
    set_std_cost(&pool, &std_parent, 30).await;
    let std_fg_loc = fresh_location(&pool, "WACMIX-STD-FG").await;
    let _std_w1 = open_account(&pool, "stock_wip", "qty", None, Some(&std_parent), None, Some(10), "debit").await;
    let _std_v1 = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&std_parent), None, Some(10), "debit").await;
    let _std_w2 = open_account(&pool, "stock_wip", "qty", None, Some(&std_parent), None, Some(20), "debit").await;
    let std_v2 = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&std_parent), None, Some(20), "debit").await;
    let _std_fg_qty = open_account(&pool, "stock_available", "qty", None, Some(&std_parent), Some(&std_fg_loc), None, "debit").await;
    let std_fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&std_parent), Some(&std_fg_loc), None, "debit").await;

    let std_wo = create_wo(&pool, "WACMIX-STD-WO", &std_parent, &std_fg_loc, 5).await;
    add_routing(&pool, &std_wo, 10, "MILL").await;
    add_routing(&pool, &std_wo, 20, "FINISH").await;
    let std_bom = create_bom_header(&pool, &std_parent, 1, true).await;
    add_bom_item(&pool, std_bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    // wac_periodic WO.
    let wac_wo = create_wo(&pool, "WACMIX-WAC-WO", &infra.parent_id, &infra.fg_loc, 5).await;
    add_routing(&pool, &wac_wo, 10, "MILL").await;
    add_routing(&pool, &wac_wo, 20, "FINISH").await;
    let wac_bom = create_bom_header(&pool, &infra.parent_id, 1, true).await;
    add_bom_item(&pool, wac_bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &std_wo, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &std_wo, 10, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &std_wo, 5, &fresh_uuid(&pool).await).await.unwrap();

    call_wo_start(&pool, &wac_wo, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wac_wo, 10, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wac_wo, 5, &fresh_uuid(&pool).await).await.unwrap();

    // Only the wac_periodic WO's depletions flagged.
    let flagged_wac: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers_provisional
          WHERE cost_method = 'wac_periodic' AND period_id = $1",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(flagged_wac, 2, "1 op_move_v + 1 wo_complete_v on wac WO");

    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(2));

    // Standard WO results untouched: FG = 5 × $30 = $150 (standard parent_std).
    assert_eq!(balance(&pool, std_fg_val).await, 150);
    // Standard WO's wip pools left at 0 (drained at close); no variance posted to them.
    assert_eq!(balance(&pool, std_v2).await, 0);
}
