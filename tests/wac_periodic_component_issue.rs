//! Tier 2 of acct-rgb (acct-7py). Tests rm_issue_to_wo from a wac_periodic
//! component with wac_periodic parent — the homogeneous in-scope case for
//! tier 2. The close hook's topological pool walk extends to include
//! rm_issue_to_wo edges (raw → WIP); rm_issue is treated as internal-chain
//! (record variance, propagate via cache, no leaf post).
//!
//! Coverage:
//!   * Single WO with raw cost drift in-period: rm_issue issued at early-
//!     period running avg; close hook re-amounts via raw final_avg; variance
//!     propagates to wo_complete_v leaf.
//!   * Two WOs sharing the same wac_periodic component: drift accumulates
//!     across both WOs.
//!   * Mixed components in same BOM (one std + one wac_periodic): only the
//!     wac side flags into transfers_provisional and gets recompute.
//!   * 3-tier chain: raw (wac_periodic) → WIP@op10 → WIP@op20 → FG.
//!     Topological walk handles all 4 pools.
//!   * Mixed parent/component cost methods raise P0026 (deferred to acct-7eo).
//!   * Force-provisional close with empty raw pool.

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

/// Seed a wac_periodic component pool via a 2-leg cycle_count_adj batch
/// (qty + value). Both legs share business_date so the pool's in-period
/// receipts (debits) sum cleanly for the close-hook qty divisor.
async fn seed_pool(
    pool: &PgPool,
    raw_qty: i64,
    raw_val: i64,
    qty: i64,
    value: i64,
    business_date: &str,
) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": qty, "qty": qty, "business_date": business_date,
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": value, "qty": qty, "business_date": business_date,
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_pool: {e}"));
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
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, scrap_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 0,
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

async fn close_period_force(pool: &PgPool, pid: i64, force_prov: bool) -> sqlx::Result<serde_json::Value> {
    let actor = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, $3, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .bind(force_prov)
    .fetch_one(pool)
    .await
}

/// Open the parent's WIP (per-op) and FG accounts.
async fn open_parent_wip_fg(pool: &PgPool, parent: &str, fg_loc: &str, ops: &[i32]) -> (Vec<i64>, i64) {
    let mut wip_vals = Vec::new();
    for &op in ops {
        let _q = open_account(pool, "stock_wip", "qty", None, Some(parent), None, Some(op), "debit").await;
        let v = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(parent), None, Some(op), "debit").await;
        wip_vals.push(v);
    }
    let _fg_q = open_account(pool, "stock_available", "qty", None, Some(parent), Some(fg_loc), None, "debit").await;
    let fg_v = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(parent), Some(fg_loc), None, "debit").await;
    (wip_vals, fg_v)
}

async fn open_component_raw(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let q = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let v = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _consumed = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
    (q, v)
}

// ============================================================
// Tests
// ============================================================

/// Single WO, single op, wac_periodic parent + wac_periodic component.
/// Pool seeded at $10/unit, then BEFORE rm_issue (i.e., setting the
/// running avg for issue-time math), but at close the pool's final_avg
/// changes (additional in-period receipts at higher cost).
///
/// Setup: parent=wac_periodic, comp=wac_periodic.
///   * Seed comp pool: 100 units @ $10 = $1000.
///   * Create WO qty_target=10 with comp×2/unit ⇒ adj_qty=20 at op10.
///   * post_wo_start: rm_issue at running avg $10 → value=$200. WIP@op10
///     fills with $200. rm_issue's value-leg flagged into transfers_provisional.
///   * Mid-period: seed 100 more units @ $14 = $1400. Pool now 180 units / $2200.
///   * Complete WO. wo_complete_v drains WIP@op10 at running avg $20 (=$200/10)
///     to FG. Both rm_issue and wo_complete_v are flagged.
///   * Close period.
///     - Component pool's final_avg = $2400/200 = $12 (in-period RECEIPTS:
///       $1000 + $1400; qty 100 + 100 = 200). variance for rm_issue =
///       (12 - 10) × 20 = $40. RECORDED, no post.
///     - WIP@op10 corrected_value_in via cache = original $200 + variance $40
///       = $240. qty_in = 10. final_avg = $24.
///     - For wo_complete_v with credit=WIP@op10: provisional_unit = $200/10 = $20.
///       variance = (24 - 20) × 10 = $40. WIP source → single-leg pattern.
///       variance > 0 → DR FG (orig_debit), CR variance_wac_period, amount=$40.
///     - Net: FG +$40, variance_wac_period +$40 (ledger-balanced).
#[tokio::test(flavor = "multi_thread")]
async fn rm_issue_close_drift_propagates_to_leaf_via_cache() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P1", "wac_periodic").await;
    let comp = fresh_sku(&pool, "P1-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P1-RAW").await;
    let fg_loc = fresh_location(&pool, "P1-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_vals, fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10]).await;
    let wip10 = wip_vals[0];

    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_wac_pre = balance(&pool, var_wac).await;

    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;
    assert_eq!(balance(&pool, raw_v).await, 1000);

    let wo_id = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 200, "WIP filled at running avg $10");
    assert_eq!(balance(&pool, raw_v).await, 800, "raw drained $200");

    // Drift: another receipt mid-period at higher cost.
    seed_pool(&pool, raw_q, raw_v, 100, 1400, "2026-04-15").await;

    call_wo_complete(&pool, &wo_id, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 200, "FG @ WIP running avg $20");
    assert_eq!(balance(&pool, wip10).await, 0, "WIP drained");

    close_period_force(&pool, pid, false).await.expect("close");

    // After close: variance > 0 routes DR FG / CR variance_wac_period.
    // FG: original $200 + $40 = $240; variance_wac_period balance -$40.
    assert_eq!(balance(&pool, var_wac).await - var_wac_pre, -40,
               "variance_wac_period credited (decreases) on variance>0");
    assert_eq!(balance(&pool, fg_v).await, 240, "FG corrected to $240 = qty × final_chain_avg");

    // rm_issue's TP row: variance_amount = 40, transfer_id NULL (internal).
    let (rm_var, rm_xfer_id): (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT p.variance_amount, p.variance_transfer_id
           FROM transfers_provisional p
           JOIN transfers t ON t.id = p.transfer_id
          WHERE t.reason = 'rm_issue_to_wo' AND t.amount = 200",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rm_var, Some(40), "rm_issue variance recorded");
    assert!(rm_xfer_id.is_none(), "rm_issue is internal-chain (no posted transfer)");
}

/// Two WOs in the same period sharing a wac_periodic component pool.
/// WO1 issues at $10 (early). Drift seeded mid-period. WO2 issues at the
/// post-drift running avg. Close hook recomputes both rm_issues at raw
/// final_avg; downstream wo_complete_v variances aggregate on FG.
#[tokio::test(flavor = "multi_thread")]
async fn two_wo_share_wac_periodic_component_drift_aggregates() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P2", "wac_periodic").await;
    let comp = fresh_sku(&pool, "P2-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P2-RAW").await;
    let fg_loc = fresh_location(&pool, "P2-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_vals, fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10]).await;
    let wip10 = wip_vals[0];

    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_wac_pre = balance(&pool, var_wac).await;

    // Initial: 100 @ $10 = $1000. WO1 at $10.
    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;

    let wo1 = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;
    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    // WO1: rm_issue 20 × $10 = $200. WIP→FG @ $20/unit ⇒ FG +$200.

    // Drift: 100 @ $14 = $1400. New running avg = ($800 + $1400)/(80 + 100) = $2200/180 ≈ $12.
    seed_pool(&pool, raw_q, raw_v, 100, 1400, "2026-04-15").await;

    let wo2 = create_wo(&pool, "WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    pin_wo_bom(&pool, &wo2, bom).await;

    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    // WO2 rm_issue: pool_value=2200, pool_qty=180 → unit=12 (truncated). value=20×12=$240.
    let pool_after_wo2_issue: i64 = balance(&pool, raw_v).await;
    assert_eq!(pool_after_wo2_issue, 1960, "raw after WO2 issue: $2200 - $240");

    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();
    // WIP@op10 received $240 from WO2. WIP avg before WO2 wo_complete = $240/10=$24.
    // wo_complete_v drains $240 to FG. FG +$240. WIP=0.
    assert_eq!(balance(&pool, wip10).await, 0);
    let fg_pre_close = balance(&pool, fg_v).await;
    assert_eq!(fg_pre_close, 200 + 240, "FG = WO1 $200 + WO2 $240");

    close_period_force(&pool, pid, false).await.expect("close");

    // Component pool final_avg = (1000 + 1400) / (100 + 100) = $2400/200 = $12.
    // For each rm_issue (credit=raw):
    //   WO1 rm: amount=$200, qty=20 → provisional_unit=$10, variance=(12-10)×20=$40.
    //   WO2 rm: amount=$240, qty=20 → provisional_unit=$12, variance=(12-12)×20=$0.
    // WIP@op10 corrected_value_in = $200 + 40 (cache) + $240 + 0 = $480. qty_in=20.
    // WIP final_avg = $24.
    // For each wo_complete_v (credit=WIP@op10):
    //   WO1 wo_complete_v: amount=$200, qty=10 → prov=$20, variance=(24-20)×10=$40. Leaf, post: FG +$40.
    //   WO2 wo_complete_v: amount=$240, qty=10 → prov=$24, variance=(24-24)×10=$0. No post.
    // Total FG correction: +$40. variance_wac_period: -$40 (credited).
    assert_eq!(balance(&pool, fg_v).await, fg_pre_close + 40);
    assert_eq!(balance(&pool, var_wac).await - var_wac_pre, -40);

    // Verify both rm_issues recorded variance, neither posted.
    let (n_internal, n_with_xfer): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE p.variance_transfer_id IS NULL),
                COUNT(*) FILTER (WHERE p.variance_transfer_id IS NOT NULL)
           FROM transfers_provisional p
           JOIN transfers t ON t.id = p.transfer_id
          WHERE t.reason = 'rm_issue_to_wo' AND p.finalized_at IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_internal, 2, "both rm_issues internal");
    assert_eq!(n_with_xfer, 0);
}

/// Mixed components in same BOM: one standard + one wac_periodic. The
/// standard rm_issue is unflagged; the wac_periodic rm_issue flags. Close
/// hook walks only the wac side.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_std_and_wac_periodic_components_only_wac_flagged() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P3", "wac_periodic").await;
    let comp_std = fresh_sku(&pool, "P3-CSTD", "standard").await;
    set_std_cost(&pool, &comp_std, 5).await;
    let comp_wac = fresh_sku(&pool, "P3-CWAC", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P3-RAW").await;
    let fg_loc = fresh_location(&pool, "P3-FG").await;

    let (std_q, std_v) = open_component_raw(&pool, &comp_std, &raw_loc).await;
    let (wac_q, wac_v) = open_component_raw(&pool, &comp_wac, &raw_loc).await;
    let (wip_vals, fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10]).await;
    let wip10 = wip_vals[0];

    seed_pool(&pool, std_q, std_v, 100, 500, "2026-04-10").await; // std @ $5/unit
    seed_pool(&pool, wac_q, wac_v, 100, 1000, "2026-04-10").await; // wac @ $10/unit

    let wo = create_wo(&pool, "WO3", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp_std, &raw_loc, 1).await; // 10 × $5 = $50
    add_bom_item(&pool, bom, 2, 10, &comp_wac, &raw_loc, 2).await; // 20 × $10 = $200

    let pid = period_id(&pool, "2026-04").await;
    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 250, "WIP = std $50 + wac $200");

    // Drift wac side only.
    seed_pool(&pool, wac_q, wac_v, 100, 1400, "2026-04-15").await;
    // wac final_avg = (1000+1400)/(100+100) = $12. variance for rm_issue = (12-10)×20 = $40.

    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await).await.unwrap();
    // FG @ WIP avg = 250/10 = $25 → FG +$250. WIP=0.

    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_wac_pre = balance(&pool, var_wac).await;
    let fg_pre = balance(&pool, fg_v).await;

    close_period_force(&pool, pid, false).await.expect("close");

    // Std side: NO transfers_provisional rows for std component (component is standard).
    // WAC side rm_issue: variance=$40 internal-chain. WIP@op10 corrected_value_in = $50 + $200 + $40 = $290.
    // qty_in (parent's stock_wip per _wac_close_pool_qty_in) = 10.
    // WIP final_avg = $29. wo_complete_v provisional_unit = $250/10 = $25.
    // variance = (29-25)×10 = $40. Leaf single-leg: FG +$40 / var_wac -$40.
    assert_eq!(balance(&pool, var_wac).await - var_wac_pre, -40);
    assert_eq!(balance(&pool, fg_v).await, fg_pre + 40);

    // Verify std rm_issue NOT in transfers_provisional.
    let std_flagged: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional p
           JOIN transfers t ON t.id = p.transfer_id
          WHERE t.reason = 'rm_issue_to_wo' AND t.credit_account_id = $1",
    )
    .bind(std_v)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(std_flagged, 0, "std component rm_issue unflagged");

    let wac_flagged: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional p
           JOIN transfers t ON t.id = p.transfer_id
          WHERE t.reason = 'rm_issue_to_wo' AND t.credit_account_id = $1",
    )
    .bind(wac_v)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(wac_flagged, 1);
}

/// 3-tier chain: raw (wac_periodic) → WIP@op10 → WIP@op20 → FG.
/// Topological walk: raw → WIP10 → WIP20. Drift on raw cascades through
/// op_move_v cache and lands at the leaf wo_complete_v on WIP@op20.
#[tokio::test(flavor = "multi_thread")]
async fn three_tier_chain_raw_to_wip10_to_wip20_to_fg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P4", "wac_periodic").await;
    let comp = fresh_sku(&pool, "P4-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P4-RAW").await;
    let fg_loc = fresh_location(&pool, "P4-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_vals, fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10, 20]).await;
    let (wip10, wip20) = (wip_vals[0], wip_vals[1]);

    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_wac_pre = balance(&pool, var_wac).await;

    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;

    let wo = create_wo(&pool, "WO4", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    add_routing(&pool, &wo, 20, "FINISH").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;
    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 200);

    call_op_move(&pool, &wo, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    // op_move_v: WIP10 unit=$20 → drain $200 to WIP20.
    assert_eq!(balance(&pool, wip10).await, 0);
    assert_eq!(balance(&pool, wip20).await, 200);

    // Drift: pool gets new receipts (qty 100 @ $14 = $1400). Final_avg = $12.
    seed_pool(&pool, raw_q, raw_v, 100, 1400, "2026-04-15").await;

    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await).await.unwrap();
    // WIP20 unit=$20 → FG +$200. WIP20=0.
    let fg_pre_close = balance(&pool, fg_v).await;
    assert_eq!(fg_pre_close, 200);

    close_period_force(&pool, pid, false).await.expect("close");

    // Topological walk: raw → WIP10 → WIP20.
    // raw final_avg = $12. rm_issue variance = (12-10)×20 = $40. INTERNAL.
    // WIP10 corrected_value_in = $200 + $40 = $240. qty_in=10 (parent WIP qty in-period).
    //   final_avg = $24. op_move_v provisional_unit = $200/10 = $20.
    //   variance = (24-20)×10 = $40. INTERNAL (op_move_v).
    // WIP20 corrected_value_in = $200 + $40 = $240. qty_in=10.
    //   final_avg = $24. wo_complete_v provisional_unit = $200/10 = $20.
    //   variance = (24-20)×10 = $40. LEAF (single-leg, FG side debit-normal).
    //   FG +$40, variance_wac_period -$40 (credited on variance>0).
    assert_eq!(balance(&pool, fg_v).await, fg_pre_close + 40);
    assert_eq!(balance(&pool, var_wac).await - var_wac_pre, -40);

    // Verify rm_issue + op_move_v are internal-chain; only wo_complete_v posts.
    let (rm_post, op_post, wc_post): (i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE t.reason = 'rm_issue_to_wo' AND p.variance_transfer_id IS NOT NULL),
                COUNT(*) FILTER (WHERE t.reason = 'op_move_v' AND p.variance_transfer_id IS NOT NULL),
                COUNT(*) FILTER (WHERE t.reason = 'wo_complete_v' AND p.variance_transfer_id IS NOT NULL)
           FROM transfers_provisional p
           JOIN transfers t ON t.id = p.transfer_id
          WHERE p.finalized_at IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rm_post, 0, "rm_issue is internal-chain");
    assert_eq!(op_post, 0, "op_move_v is internal-chain");
    assert_eq!(wc_post, 1, "wo_complete_v posted leaf variance");
}

/// Mixed parent/component cost methods raise P0026 (deferred to acct-7eo).
/// Standard parent + wac_periodic component: rm_issue cannot land variance
/// cleanly on a non-wac-periodic destination WIP. Verify gate fires.
#[tokio::test(flavor = "multi_thread")]
async fn standard_parent_with_wac_periodic_component_raises_p0026() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P5", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "P5-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P5-RAW").await;
    let fg_loc = fresh_location(&pool, "P5-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (_wip_vals, _fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10]).await;
    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;

    let wo = create_wo(&pool, "WO5", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let err = call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await
        .expect_err("expected P0026 mixed cost methods");
    let sqlstate = err.as_database_error().and_then(|e| e.code().map(|c| c.to_string()));
    assert_eq!(sqlstate, Some("P0026".to_string()),
               "mixed parent/component cost methods raise P0026, got {err:?}");
    assert!(format!("{err}").contains("rm_issue_mixed_cost_method"),
            "error message mentions mixed cost methods: {err}");
}

/// wac_perpetual parent + wac_periodic component also raises P0026 (same
/// reason: variance has no clean landing on a non-wac-periodic destination
/// WIP for retroactive correction).
#[tokio::test(flavor = "multi_thread")]
async fn wac_perpetual_parent_with_wac_periodic_component_raises_p0026() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P6", "wac_perpetual").await;
    let comp = fresh_sku(&pool, "P6-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P6-RAW").await;
    let fg_loc = fresh_location(&pool, "P6-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (_wip_vals, _fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10]).await;
    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;

    let wo = create_wo(&pool, "WO6", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let err = call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await
        .expect_err("expected P0026 mixed cost methods");
    let sqlstate = err.as_database_error().and_then(|e| e.code().map(|c| c.to_string()));
    assert_eq!(sqlstate, Some("P0026".to_string()));
}

/// Single WO with NO drift: pool seeded once, rm_issue at running avg,
/// no further receipts. final_avg = provisional. All variances = 0.
/// Verifies the no-op path through the close hook.
#[tokio::test(flavor = "multi_thread")]
async fn single_wo_no_drift_all_variances_zero() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P7", "wac_periodic").await;
    let comp = fresh_sku(&pool, "P7-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P7-RAW").await;
    let fg_loc = fresh_location(&pool, "P7-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_vals, fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10]).await;
    let wip10 = wip_vals[0];

    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await; // $10/unit

    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_wac_pre = balance(&pool, var_wac).await;

    let wo = create_wo(&pool, "WO7", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;
    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await).await.unwrap();

    let fg_pre = balance(&pool, fg_v).await;
    assert_eq!(fg_pre, 200);
    assert_eq!(balance(&pool, wip10).await, 0);

    close_period_force(&pool, pid, false).await.expect("close");

    // No drift: all variances = 0.
    assert_eq!(balance(&pool, var_wac).await - var_wac_pre, 0);
    assert_eq!(balance(&pool, fg_v).await, fg_pre);

    // All TPs finalized with variance=0.
    let nonzero: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional p
          WHERE p.finalized_at IS NOT NULL AND p.variance_amount <> 0",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(nonzero, 0);
}

/// Rework cycle (op_move 20→10) on wac_periodic parent: cycle in pool
/// DAG raises P0036. (Mirrors acct-smn rework test but with rm_issue
/// edges added to confirm cycle detection still works.)
#[tokio::test(flavor = "multi_thread")]
async fn rework_cycle_with_rm_issue_present_still_raises_p0036() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P8", "wac_periodic").await;
    let comp = fresh_sku(&pool, "P8-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P8-RAW").await;
    let fg_loc = fresh_location(&pool, "P8-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (_wip_vals, _fg_v) = open_parent_wip_fg(&pool, &parent, &fg_loc, &[10, 20]).await;
    seed_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;

    let wo = create_wo(&pool, "WO8", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    add_routing(&pool, &wo, 20, "FINISH").await;
    let bom = create_bom_header(&pool, &parent, 1, true).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let pid = period_id(&pool, "2026-04").await;
    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    // Rework: 20 → 10 creates op_move_v cycle WIP20 → WIP10.
    call_op_move(&pool, &wo, 20, 10, 10, &fresh_uuid(&pool).await).await.unwrap();

    let err = close_period_force(&pool, pid, false).await
        .expect_err("expected P0036 cycle");
    let sqlstate = err.as_database_error().and_then(|e| e.code().map(|c| c.to_string()));
    assert_eq!(sqlstate, Some("P0036".to_string()), "rework cycle raises P0036, got {err:?}");
}
