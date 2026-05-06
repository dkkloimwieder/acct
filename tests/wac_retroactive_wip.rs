//! Tier 3 of acct-8in / acct-rgb (acct-rso). Tests wac_retroactive
//! parent SKUs and components on WIP — single-op, multi-op, with drift,
//! plus rework + mixed cost-method error paths.
//!
//! Coverage:
//!   * single-op clean: wac_retroactive parent, no drift → all variances 0.
//!   * two-op clean: chronological replay through op_move_v on a single
//!     pool chain.
//!   * three-op clean: topological walk visits 3 pools in order.
//!   * mixed standard parent + wac_retroactive component raises P0026.
//!   * mixed wac_perpetual parent + wac_retroactive component raises P0026.
//!   * rework cycle (op_move 20→10) raises P0036.
//!   * wac_retroactive component on wac_retroactive parent: rm_issue_to_wo
//!     flagged wac_retroactive; close-hook chronological replay re-amounts
//!     rm_issue value-leg via component pool's running avg; drift propagates
//!     to wo_complete_v leaf via cache.
//!   * out-of-order receipt vs depletion (business_date < posted_at) reveals
//!     wac_retroactive's chronological-by-BD distinction from posted-order.

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

async fn add_routing(pool: &PgPool, wo_id: &str, op: i32, name: &str) {
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)")
        .bind(wo_id)
        .bind(op)
        .bind(name)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("add_routing: {e}"));
}

async fn create_bom_header(pool: &PgPool, parent: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
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

async fn call_wo_start(pool: &PgPool, wo_id: &str, key: &str, business_date: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_op_move(pool: &PgPool, wo_id: &str, from_op: i32, to_op: i32, qty: i64, key: &str, business_date: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_op_move($1::UUID, $2, $3, $4, $5::DATE, $6::UUID, $7::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(from_op)
    .bind(to_op)
    .bind(qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(pool: &PgPool, wo_id: &str, qty: i64, key: &str, business_date: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(business_date)
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

struct WipInfra {
    parent_id: String,
    fg_loc: String,
    fg_val_acct: i64,
    raw_loc: String,
    wip_val_by_op: Vec<(i32, i64)>,
}

impl WipInfra {
    fn val_op(&self, op: i32) -> i64 {
        self.wip_val_by_op.iter().find(|(o, _)| *o == op).expect("op val").1
    }
}

async fn build_wip_infra(pool: &PgPool, parent_code: &str, ops: &[i32]) -> WipInfra {
    let parent = fresh_sku(pool, parent_code, "wac_retroactive").await;
    let fg_loc = fresh_location(pool, &format!("{parent_code}-FG")).await;
    let raw_loc = fresh_location(pool, &format!("{parent_code}-RAW")).await;

    let mut wip_val_by_op = Vec::new();
    for &op in ops {
        let _q = open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(op), "debit").await;
        let v = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(op), "debit").await;
        wip_val_by_op.push((op, v));
    }
    let _fg_qty = open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let fg_val_acct = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;

    WipInfra { parent_id: parent, fg_loc, fg_val_acct, raw_loc, wip_val_by_op }
}

async fn build_component(pool: &PgPool, code: &str, cost_method: &str, raw_loc: &str, std_cost: i64) -> (String, i64, i64) {
    let comp = fresh_sku(pool, code, cost_method).await;
    if cost_method == "standard" {
        set_std_cost(pool, &comp, std_cost).await;
    }
    let _consumed = open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit").await;
    let raw_qty = open_account(pool, "stock_available", "qty", None, Some(&comp), Some(raw_loc), None, "debit").await;
    let raw_val = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(raw_loc), None, "debit").await;
    (comp, raw_qty, raw_val)
}

// ============================================================
// Tests
// ============================================================

/// Single-op, no drift. wac_retroactive parent, standard component.
/// One WO. After close, all variances 0; provisional rows finalized.
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_single_op_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR1-CLN", &[10]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR1-CLN-C", "standard", &infra.raw_loc, 20).await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 2000).await;

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR1-CLN-WO", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;

    let key1 = fresh_uuid(&pool).await;
    call_wo_start(&pool, &wo, &key1, "2026-04-20").await.unwrap();
    let key2 = fresh_uuid(&pool).await;
    call_wo_complete(&pool, &wo, 10, &key2, "2026-04-20").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let _result = close_period(&pool, pid).await;

    // FG should hold 10 × $20 × 2 qty/p = $400.
    let fg_val = balance(&pool, infra.fg_val_acct).await;
    assert_eq!(fg_val, 400, "FG value = 10 units × $40 unit cost = $400");

    // No variance posted on variance_wac_retroactive.
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    let var_balance = balance(&pool, var_wacr).await;
    assert_eq!(var_balance, 0, "no drift → variance_wac_retroactive nets to 0");

    // All flagged provisionals finalized with variance_amount=0.
    let unfinalized: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional p
         JOIN transfers t ON t.id = p.transfer_id
         WHERE p.cost_method = 'wac_retroactive' AND p.finalized_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unfinalized, 0, "all wac_retroactive provisionals finalized");
}

/// Two-op chain, no drift. wac_retroactive parent.
/// Confirms topological walk visits both pools and chronological replay
/// produces zero variance when nothing drifts.
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_two_op_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR2-CLN", &[10, 20]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR2-CLN-C", "standard", &infra.raw_loc, 10).await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 1000).await;

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR2-CLN-WO", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    add_routing(&pool, &wo, 20, "ASSEMBLE").await;

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 10, &fresh_uuid(&pool).await, "2026-04-21").await.unwrap();
    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await, "2026-04-22").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let _r = close_period(&pool, pid).await;

    // FG = 10 × ($10 × 2 qty/p) = $200.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 200);
    // WIP@op10 and op20 drained to 0.
    assert_eq!(balance(&pool, infra.val_op(10)).await, 0);
    assert_eq!(balance(&pool, infra.val_op(20)).await, 0);
    // No variance.
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    assert_eq!(balance(&pool, var_wacr).await, 0);
}

/// Three-op chain, no drift. Confirms topological walk handles a 3-pool
/// chain (op10 → op20 → op30 → FG).
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_three_op_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR3-CLN", &[10, 20, 30]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR3-CLN-C", "standard", &infra.raw_loc, 5).await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 500).await;

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR3-CLN-WO", &infra.parent_id, &infra.fg_loc, 10).await;
    for op in [10, 20, 30] { add_routing(&pool, &wo, op, &format!("OP{op}")).await; }

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 10, &fresh_uuid(&pool).await, "2026-04-21").await.unwrap();
    call_op_move(&pool, &wo, 20, 30, 10, &fresh_uuid(&pool).await, "2026-04-22").await.unwrap();
    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await, "2026-04-23").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let _r = close_period(&pool, pid).await;

    // FG = 10 × ($5 × 2) = $100.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 100);
    for op in [10, 20, 30] {
        assert_eq!(balance(&pool, infra.val_op(op)).await, 0, "op{op} drained");
    }
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    assert_eq!(balance(&pool, var_wacr).await, 0);
}

/// Standard parent + wac_retroactive component (acct-7eo, mig 0077).
/// rm_issue posts at running avg; close hook detects mixed shape and
/// posts single-leg variance through variance_material_mixed against
/// the component pool. Drift created by backdating a later receipt.
#[tokio::test(flavor = "multi_thread")]
async fn standard_parent_wac_retroactive_component_routes_mixed_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "WACR-MIX-S", "standard").await;
    set_std_cost(&pool, &parent, 10).await;
    let fg_loc = fresh_location(&pool, "WACR-MIX-S-FG").await;
    let raw_loc = fresh_location(&pool, "WACR-MIX-S-RAW").await;
    let _q = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let wip10 = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let _fq = open_account(&pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;

    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR-MIX-S-C", "wac_retroactive", &raw_loc, 0).await;
    // First receipt 100 @ $5 (bd 2026-04-15 — the pre_load_raw default).
    pre_load_raw(&pool, raw_qty, raw_val, 100, 500).await;

    let bom = create_bom_header(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR-MIX-S-WO", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;

    // rm_issue at issue-time avg $5. WIP +$100, raw -$100 (qty 20).
    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 100);

    // Backdated receipt @ bd 2026-04-10 (BEFORE issue bd). Posts now,
    // but chronological replay puts it before the rm_issue. 100 @ $7.
    let posted_by = fresh_uuid(&pool).await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(&pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": 100, "qty": 100, "business_date": "2026-04-10",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": 700, "qty": 100, "business_date": "2026-04-10",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(&pool).await.unwrap();

    // wo_complete drains WIP at parent_std=$10 → FG +$100. WIP=0.
    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 0);
    assert_eq!(balance(&pool, fg_v).await, 100);

    // Close period. Chronological replay: receipt1 ($5, bd 04-15) +
    // receipt2 ($7, bd 04-10) → at rm_issue's bd 04-20, running avg
    // = (500+700)/200 = $6. Recomputed = $6 × 20 = $120. Variance = $20.
    // Mixed-case routing: dr variance_material_mixed $20, cr raw_v $20.
    let pid = period_id(&pool, "2026-04").await;
    let var_mix = account_id_by_kind_currency(&pool, "variance_material_mixed", Some("USD")).await;
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    let pre_var_mix = balance(&pool, var_mix).await;
    let pre_var_wacr = balance(&pool, var_wacr).await;
    let pre_wip = balance(&pool, wip10).await;

    let _ = close_period(&pool, pid).await;

    assert_eq!(balance(&pool, var_mix).await - pre_var_mix, 20,
               "mixed-case wac_retroactive variance posted to variance_material_mixed");
    assert_eq!(balance(&pool, var_wacr).await - pre_var_wacr, 0,
               "variance_wac_retroactive untouched for mixed case");
    assert_eq!(balance(&pool, wip10).await, pre_wip,
               "destination WIP untouched");

    common::assert_invariants_hold(&pool, "mixed_retroactive_std_parent").await;
}

/// wac_perpetual parent + wac_retroactive component (acct-7eo). Same
/// routing: variance lands on variance_material_mixed at close.
#[tokio::test(flavor = "multi_thread")]
async fn wac_perpetual_parent_wac_retroactive_component_routes_mixed_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "WACR-MIX-P", "wac_perpetual").await;
    let fg_loc = fresh_location(&pool, "WACR-MIX-P-FG").await;
    let raw_loc = fresh_location(&pool, "WACR-MIX-P-RAW").await;
    let _q = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let wip10 = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let _fq = open_account(&pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;

    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR-MIX-P-C", "wac_retroactive", &raw_loc, 0).await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 500).await;

    let bom = create_bom_header(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR-MIX-P-WO", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 100);

    // Backdated drift receipt.
    let posted_by = fresh_uuid(&pool).await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(&pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": 100, "qty": 100, "business_date": "2026-04-10",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": 700, "qty": 100, "business_date": "2026-04-10",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(&pool).await.unwrap();

    // wac_perpetual parent: wo_complete drains WIP at running avg = $100/10 = $10.
    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    assert_eq!(balance(&pool, wip10).await, 0);
    assert_eq!(balance(&pool, fg_v).await, 100);

    let pid = period_id(&pool, "2026-04").await;
    let var_mix = account_id_by_kind_currency(&pool, "variance_material_mixed", Some("USD")).await;
    let pre_var_mix = balance(&pool, var_mix).await;

    let _ = close_period(&pool, pid).await;

    assert_eq!(balance(&pool, var_mix).await - pre_var_mix, 20);
    assert_eq!(balance(&pool, wip10).await, 0);

    common::assert_invariants_hold(&pool, "mixed_retroactive_wac_perpetual_parent").await;
}

/// Rework cycle on wac_retroactive parent: op_move(20→10) creates a cycle
/// in the pool DAG. close_period raises P0036.
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_rework_cycle_raises_p0036() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR-REWORK", &[10, 20]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR-REWORK-C", "standard", &infra.raw_loc, 10).await;
    pre_load_raw(&pool, raw_qty, raw_val, 200, 2000).await;

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR-REWORK-WO", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;
    add_routing(&pool, &wo, 20, "ASSEMBLE").await;

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    call_op_move(&pool, &wo, 10, 20, 10, &fresh_uuid(&pool).await, "2026-04-21").await.unwrap();
    // Rework: move back from op20 to op10. Creates op20 → op10 edge plus
    // existing op10 → op20 edge → cycle in pool DAG.
    call_op_move(&pool, &wo, 20, 10, 10, &fresh_uuid(&pool).await, "2026-04-22").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let result = try_close_period(&pool, pid).await;
    let err = result.expect_err("close should raise P0036");
    let s = format!("{err:?}");
    assert!(s.contains("P0036") || s.contains("wac_retroactive_pool_cycle"),
            "expected P0036 / wac_retroactive_pool_cycle, got: {s}");
}

/// wac_retroactive component (with no drift) on wac_retroactive parent:
/// rm_issue_to_wo gate lifted (no P0026); flagged wac_retroactive; close
/// hook chronological replay leaves variance 0.
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_component_on_wac_retroactive_parent_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR-CC-CLN", &[10]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR-CC-CLN-C", "wac_retroactive", &infra.raw_loc, 0).await;
    pre_load_raw(&pool, raw_qty, raw_val, 100, 1500).await; // unit avg = $15

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let wo = create_wo(&pool, "WACR-CC-CLN-WO", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "MILL").await;

    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    call_wo_complete(&pool, &wo, 10, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let _r = close_period(&pool, pid).await;

    // 10 units × 2 qty/p × $15 = $300 should land in FG.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 300);
    // raw pool: 100 - 20 = 80 units, $1500 - $300 = $1200.
    assert_eq!(balance(&pool, raw_val).await, 1200);
    // No variance (single chain, no drift).
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    assert_eq!(balance(&pool, var_wacr).await, 0);

    // All wac_retroactive provisionals finalized.
    let unfinalized: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional
         WHERE cost_method = 'wac_retroactive' AND finalized_at IS NULL",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(unfinalized, 0);
}

/// Out-of-order test: receipt with earlier business_date but LATER posted_at
/// than a depletion. wac_retroactive's chronological-by-BD replay places
/// the receipt before the depletion → recomputed cost differs from
/// mid-period (perpetual-style) running avg → variance posted.
///
/// Setup: wac_retroactive component pool with pre-period 10×$10=$100 (unit avg $10).
/// 1. WO1 wo_start at BD=2026-04-10 (rm_issue 4×$10=$40). Pool: 6 units, $60.
/// 2. WO1 wo_complete at BD=2026-04-10. Drain WIP @ unit cost. (WIP is rm_issue $40
///    on 4 parent units = $10/unit → drain $40.)
/// 3. Late receipt on component: BD=2026-04-05 (BEFORE WO1's BD), posted_at=now (AFTER).
///    20 units at $20 = $400 added.
/// 4. Close 2026-04 period.
///
/// Replay on component pool:
///   pre-period: 10 units, $100.
///   in-period chronological by BD:
///     BD 2026-04-05: receipt +20×$20=$400. pool: 30 units, $500. unit=$16.67.
///     BD 2026-04-10: rm_issue 4 units. recompute = 4 × ($500/30) = 4 × $16 = $64
///       (truncated). variance = $64 - $40 = $24. Internal-chain (rm_issue_to_wo):
///       record variance_amount=$24, no transfer.
///     BD 2026-04-10: wo_complete on wac_retroactive parent. value-leg drains
///       WIP@op10 with the original mid-period $40. The cache LEFT JOIN brings
///       in the rm_issue's variance_amount=$24 → WIP corrected_value_in = $40+$24=$64.
///       For wo_complete_v on wac_retroactive parent: chronologically replayed
///       on WIP@op10 pool. WIP@op10 receives $40 (orig) + $24 (rm_issue cache) = $64.
///       4 parent units. unit avg = $64/4 = $16. wo_complete drain = 4×$16=$64.
///       variance = $64 - $40 = $24 (single-leg: variance_wac_retroactive ↔ FG).
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_chronological_replay_late_receipt() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR-LATE", &[10]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR-LATE-C", "wac_retroactive", &infra.raw_loc, 0).await;
    // Pre-period seed: 10 units at $100 → unit $10.
    let posted_by = fresh_uuid(&pool).await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(&pool).await;
    let pre_seed = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": 10, "qty": 10, "business_date": "2026-03-15",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": 100, "qty": 10, "business_date": "2026-03-15",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
    ]);
    // Pre-period seed bypasses period-closed check (2026-03 is closed in the fixture).
    sqlx::query("SELECT post_transfers($1, TRUE)").bind(pre_seed).execute(&pool).await.unwrap();

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await; // qty/p=2

    let wo = create_wo(&pool, "WACR-LATE-WO", &infra.parent_id, &infra.fg_loc, 2).await; // 2 parent units → 4 component units
    add_routing(&pool, &wo, 10, "MILL").await;

    // 1. WO1 wo_start at BD=2026-04-10 (rm_issue uses pool avg $10).
    call_wo_start(&pool, &wo, &fresh_uuid(&pool).await, "2026-04-10").await.unwrap();
    // 2. WO1 wo_complete at BD=2026-04-10.
    call_wo_complete(&pool, &wo, 2, &fresh_uuid(&pool).await, "2026-04-10").await.unwrap();

    // FG at this point: 2 × $20 (WIP unit cost) = $40.
    let fg_pre = balance(&pool, infra.fg_val_acct).await;
    assert_eq!(fg_pre, 40, "WO1 drains WIP @ mid-period $20/unit");

    // 3. Late receipt: BD=2026-04-05 (BEFORE wo_start), posted_at NOW.
    let late_doc = fresh_uuid(&pool).await;
    let late = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": late_doc,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": 20, "qty": 20, "business_date": "2026-04-05",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": late_doc,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": 400, "qty": 20, "business_date": "2026-04-05",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)").bind(late).execute(&pool).await.unwrap();

    // 4. Close 2026-04.
    let pid = period_id(&pool, "2026-04").await;
    let _r = close_period(&pool, pid).await;

    // After replay:
    //   Component pool pre-period: 10 units, $100.
    //   In-period chronological by BD:
    //     04-05: receipt +20×$20=$400. pool: 30 units, $500.
    //     04-10: rm_issue 4 units. unit = $500/30 = $16. recompute = 4×$16 = $64.
    //       rm_issue_to_wo internal-chain: variance_amount = $64 - $40 = $24, no transfer.
    //     pool after rm_issue: $500-$64=$436, 26 units (running, not posted).
    //   WIP@op10 pool replay (topologically after raw):
    //     Pre-period: 0.
    //     In-period:
    //       BD 04-10 wo_start qty-leg: stock_wip(op10) +2 (qty event, sub-pri 0).
    //       BD 04-10 rm_issue value-leg DEBIT on inv_value_wip(op10): pool_value += $40 + cache.var($24) = $64. (priority 1)
    //       BD 04-10 wo_complete_v value-leg CREDIT on inv_value_wip(op10): outflow priority 1.
    //         At this moment pool_value=$64, pool_qty=2 (from wo_start qty-leg). avg = $32.
    //         recompute = 2 × $32 = $64. variance = $64 - $40 = $24 (single-leg posting on inv_value_wip src).
    //       BD 04-10 wo_complete qty-leg credit on stock_wip: priority 2. pool_qty -= 2.
    //
    // So:
    //   - FG receives $40 (orig wo_complete drain) + $24 (variance posted single-leg) = $64.
    //   - variance_wac_retroactive: $24 (rm_issue) + $24 (wo_complete) … wait, single-leg.
    //
    // Single-leg variance for inv_value_wip source: variance routes between v_orig_debit
    // (= FG account) and variance_wac_retroactive. v_variance > 0 → DR FG, CR var. So
    // FG +$24, var_wac_retroactive -$24.
    //
    // For rm_issue_to_wo internal-chain (v_orig_reason='rm_issue_to_wo'): no transfer
    // posted; variance_amount only recorded. So variance_wac_retroactive is only
    // touched by the wo_complete leaf: var balance = -$24.

    // Expected:
    //   FG = $40 + $24 = $64.
    //   variance_wac_retroactive = -$24 (credited).
    let fg_after = balance(&pool, infra.fg_val_acct).await;
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    let var_balance = balance(&pool, var_wacr).await;

    assert_eq!(fg_after, 64,
               "FG = $40 (orig wo_complete) + $24 (chronological replay variance via cache + single-leg posting on WIP source)");
    assert_eq!(var_balance, -24,
               "variance_wac_retroactive credited $24 (single-leg leaf on inv_value_wip source)");
}

/// Two-WO interleaved on wac_retroactive parent: confirms multi-WO sharing
/// stock_wip(parent, op) pools is handled chronologically without spurious
/// variance.
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_two_wo_shared_pool_no_drift() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let infra = build_wip_infra(&pool, "WACR-2WO", &[10]).await;
    let (comp, raw_qty, raw_val) = build_component(&pool, "WACR-2WO-C", "standard", &infra.raw_loc, 10).await;
    pre_load_raw(&pool, raw_qty, raw_val, 200, 2000).await;

    let bom = create_bom_header(&pool, &infra.parent_id).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &infra.raw_loc, 2).await;

    let wo1 = create_wo(&pool, "WACR-2WO-A", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    let wo2 = create_wo(&pool, "WACR-2WO-B", &infra.parent_id, &infra.fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;

    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await, "2026-04-20").await.unwrap();
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await, "2026-04-21").await.unwrap();
    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await, "2026-04-21").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let _r = close_period(&pool, pid).await;

    // Each WO drains $200 → FG total = $400.
    assert_eq!(balance(&pool, infra.fg_val_acct).await, 400);
    let var_wacr = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    assert_eq!(balance(&pool, var_wacr).await, 0,
               "no late-arrival, no drift → variance 0");
}
