//! acct-69e — interleaved multi-WO sharing the same parent_sku × routing_op
//! WIP pool. Pre-tier-1 (mig 0064) post_wo_complete's pre-balance step and
//! per-op residual sweep absorbed OTHER WOs' WIP balance into THIS WO's
//! variance_wo_close. Migration 0068 gates both on "this WO is sole
//! occupant of the pool" (stock_wip qty == p_qty before drain, == 0 after).
//!
//! Coverage:
//!   * Standard parent: 2 WOs interleaved through 2 ops; both close
//!     drains correct, no premature absorption.
//!   * wac_perpetual parent: same.
//!   * wac_periodic parent: same; close-hook still runs cleanly.
//!   * 3 WOs sharing pool: middle WO closes, others continue, last
//!     WO absorbs cumulative truncation residue.
//!   * Sequential same-pool baseline (1 WO at a time): pre-balance still
//!     fires when sole occupant — proves the gate is non-restrictive on
//!     the existing path.

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

async fn create_wo(pool: &PgPool, wo_no: &str, parent: &str, fg_loc: &str, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
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

async fn create_bom(pool: &PgPool, parent: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom: {e}"))
}

async fn add_bom_item(pool: &PgPool, bom_id: i64, line_no: i32, op: i32, comp: &str, comp_loc: &str, qty_per_parent: i64) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, scrap_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 0,
                 $4::UUID, $5::UUID, $6)",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(op)
    .bind(comp)
    .bind(comp_loc)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_item: {e}"));
}

async fn open_component_raw(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let q = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let v = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _consumed = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
    (q, v)
}

async fn open_parent_pool_per_op(pool: &PgPool, parent: &str, fg_loc: &str, ops: &[i32]) -> (Vec<(i64, i64)>, i64, i64) {
    let mut wip_by_op = Vec::new();
    for &op in ops {
        let q = open_account(pool, "stock_wip", "qty", None, Some(parent), None, Some(op), "debit").await;
        let v = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(parent), None, Some(op), "debit").await;
        wip_by_op.push((q, v));
    }
    let _fg_q = open_account(pool, "stock_available", "qty", None, Some(parent), Some(fg_loc), None, "debit").await;
    let fg_v = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(parent), Some(fg_loc), None, "debit").await;
    let var_close = account_id_by_kind_currency(pool, "variance_wo_close", Some("USD")).await;
    (wip_by_op, fg_v, var_close)
}

async fn seed_raw_pool(pool: &PgPool, raw_q: i64, raw_v: i64, qty: i64, value: i64, business_date: &str) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_q, "credit_account_id": void_qty,
          "amount": qty, "qty": qty, "business_date": business_date,
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_v, "credit_account_id": void_val,
          "amount": value, "qty": qty, "business_date": business_date,
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_raw_pool: {e}"));
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

// ============================================================
// Tests
// ============================================================

/// Standard parent, 2 WOs interleaved through 2-op routing.
///   Parent: standard, parent_std=$20.
///   Component: standard, comp_std=$10. qty/p=2 ⇒ rm_issue at op10
///   adds $20/parent-unit to WIP; balanced cost (no over/under-absorb).
///
/// Sequence:
///   * Both WOs wo_start(qty=10): pool@op10 = $400 (= $200 + $200), qty=20.
///   * Both WOs op_move(10→20, 10): pool@op10=$0, pool@op20=$400 qty=20.
///   * WO1 wo_complete(10): drain $20 × 10 = $200 to FG. Pool@op20 should
///     end at $200 (WO2's portion). variance_wo_close should NOT absorb
///     WO2's portion. Pre-fix this test would fail: pre-balance saw
///     $400 in pool, drained $200 prematurely to variance_wo_close.
///   * WO2 wo_complete(10): pool@op20=$200, qty=10 (sole), pre-balance
///     fires; drain $200 to FG. variance_wo_close absorbs any final
///     truncation residue (= 0 here, exact-cost case).
///
/// Final balances: each WO's FG = $200 (clean). variance_wo_close = 0.
#[tokio::test(flavor = "multi_thread")]
async fn standard_parent_two_wos_interleaved_no_premature_absorption() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P-STD", "standard").await;
    set_std_cost(&pool, &parent, 20).await;
    let comp = fresh_sku(&pool, "P-STD-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "STD-RAW").await;
    let fg_loc = fresh_location(&pool, "STD-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip, fg_v, var_close) = open_parent_pool_per_op(&pool, &parent, &fg_loc, &[10, 20]).await;
    let (_qty10, val10) = wip[0];
    let (_qty20, val20) = wip[1];

    seed_raw_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;

    let var_pre = balance(&pool, var_close).await;

    let wo1 = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    add_routing(&pool, &wo1, 20, "FINISH").await;
    let bom = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let wo2 = create_wo(&pool, "WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    add_routing(&pool, &wo2, 20, "FINISH").await;
    pin_wo_bom(&pool, &wo2, bom).await;

    // Interleave: both wo_start, both op_move, then wo_complete in order.
    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 400, "pool@op10 after both wo_start");

    call_op_move(&pool, &wo1, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo2, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 0);
    assert_eq!(balance(&pool, val20).await, 400);

    // WO1 wo_complete first: SHARED pool ($400, qty=20). p_qty=10. Pre-fix:
    // pre-balance would post $200 to variance_wo_close; FG would still get
    // $200 (the per-output drain). With gate: pre-balance skipped; per-
    // output drain $200 to FG; pool ends at $200 (WO2's portion).
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 200, "WO1 FG drain");
    assert_eq!(balance(&pool, val20).await, 200, "WO2 portion preserved in pool");
    assert_eq!(balance(&pool, var_close).await - var_pre, 0,
               "no premature absorption from interleaved WO1 close");

    // WO2 wo_complete: pool=$200 qty=10 (sole). pre-balance valid; truncation=0.
    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 400, "WO2 FG drain");
    assert_eq!(balance(&pool, val20).await, 0, "pool fully drained");
    assert_eq!(balance(&pool, var_close).await - var_pre, 0,
               "no variance for exact-cost interleaved standard WOs");
}

/// wac_perpetual parent, 2 WOs interleaved. Same setup; WIP drains at
/// running avg. The shared pool's avg = (sum value)/(sum qty) at time
/// of each completion.
///   * Pool@op20 after both arrived: $400, qty=20, avg=$20.
///   * WO1 wo_complete(10): drain at avg $20 → FG +$200. Pool $200, qty=10.
///   * WO2 wo_complete(10): drain at avg $20 → FG +$200. Pool $0.
///   * variance_wo_close = 0 (no truncation).
#[tokio::test(flavor = "multi_thread")]
async fn wac_perpetual_parent_two_wos_interleaved() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P-WACP", "wac_perpetual").await;
    let comp = fresh_sku(&pool, "P-WACP-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "WACP-RAW").await;
    let fg_loc = fresh_location(&pool, "WACP-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip, fg_v, var_close) = open_parent_pool_per_op(&pool, &parent, &fg_loc, &[10, 20]).await;
    let (_qty20, val20) = wip[1];

    seed_raw_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;
    let var_pre = balance(&pool, var_close).await;

    let wo1 = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    add_routing(&pool, &wo1, 20, "FINISH").await;
    let bom = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let wo2 = create_wo(&pool, "WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    add_routing(&pool, &wo2, 20, "FINISH").await;
    pin_wo_bom(&pool, &wo2, bom).await;

    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo1, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo2, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val20).await, 400);

    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 200);
    assert_eq!(balance(&pool, val20).await, 200, "WO2 portion preserved");
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);

    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 400);
    assert_eq!(balance(&pool, val20).await, 0);
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);
}

/// wac_periodic parent, 2 WOs interleaved. wo_complete_v transfers are
/// flagged, close hook recomputes. With the gate, neither WO's pre-balance
/// poaches the other's WIP — each gets $200 drain.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_parent_two_wos_interleaved() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P-WACR", "wac_periodic").await;
    let comp = fresh_sku(&pool, "P-WACR-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "WACR-RAW").await;
    let fg_loc = fresh_location(&pool, "WACR-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip, fg_v, var_close) = open_parent_pool_per_op(&pool, &parent, &fg_loc, &[10, 20]).await;
    let (_qty20, val20) = wip[1];

    seed_raw_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;
    let var_pre = balance(&pool, var_close).await;

    let wo1 = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    add_routing(&pool, &wo1, 20, "FINISH").await;
    let bom = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let wo2 = create_wo(&pool, "WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    add_routing(&pool, &wo2, 20, "FINISH").await;
    pin_wo_bom(&pool, &wo2, bom).await;

    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo1, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo2, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 200);
    assert_eq!(balance(&pool, val20).await, 200);
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);
    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 400);
    assert_eq!(balance(&pool, val20).await, 0);

    // Close hook: no drift in component pool; final_avg = $10 (raw) and
    // $20 (WIP). All variances 0. variance_wac_period unchanged.
    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_wac_pre = balance(&pool, var_wac).await;
    let actor = fresh_uuid(&pool).await;
    sqlx::query("SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid)
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("close_period");
    assert_eq!(balance(&pool, var_wac).await - var_wac_pre, 0);
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);
    assert_eq!(balance(&pool, fg_v).await, 400);
}

/// 3 WOs interleaved through 2-op routing. Middle WO closes between the
/// others. Verify the gate handles >2 sharing scenario.
///   * 3 wo_starts: pool@op10=$600 qty=30.
///   * 3 op_moves: pool@op20=$600 qty=30.
///   * WO1 wo_complete(10): pool=$600 qty=30, NOT sole; gate skips pre-balance.
///     Drain: $200 to FG. Pool ends $400, qty=20.
///   * WO2 wo_complete(10): pool=$400 qty=20, NOT sole; gate skips.
///     Drain: $200 to FG. Pool ends $200, qty=10.
///   * WO3 wo_complete(10): pool=$200 qty=10, SOLE; gate fires.
///     Pre-balance: $200 - $200 = 0 (no truncation). Drain: $200 to FG.
///     Pool ends $0. variance_wo_close = 0.
#[tokio::test(flavor = "multi_thread")]
async fn three_wos_interleaved_pool_drains_correctly() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P-3WO", "standard").await;
    set_std_cost(&pool, &parent, 20).await;
    let comp = fresh_sku(&pool, "P-3WO-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "3WO-RAW").await;
    let fg_loc = fresh_location(&pool, "3WO-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip, fg_v, var_close) = open_parent_pool_per_op(&pool, &parent, &fg_loc, &[10, 20]).await;
    let (_qty20, val20) = wip[1];

    seed_raw_pool(&pool, raw_q, raw_v, 200, 2000, "2026-04-10").await;
    let var_pre = balance(&pool, var_close).await;

    let bom = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let mut wos = Vec::new();
    for i in 1..=3 {
        let wo = create_wo(&pool, &format!("WO{i}"), &parent, &fg_loc, 10).await;
        add_routing(&pool, &wo, 10, "MILL").await;
        add_routing(&pool, &wo, 20, "FINISH").await;
        pin_wo_bom(&pool, &wo, bom).await;
        wos.push(wo);
    }

    for wo in &wos {
        call_wo_start(&pool, wo, &fresh_uuid(&pool).await).await.unwrap();
    }
    for wo in &wos {
        call_op_move(&pool, wo, 10, 20, 10, &fresh_uuid(&pool).await).await.unwrap();
    }
    assert_eq!(balance(&pool, val20).await, 600);

    call_wo_complete(&pool, &wos[0], 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val20).await, 400);
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);

    call_wo_complete(&pool, &wos[1], 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val20).await, 200);
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);

    call_wo_complete(&pool, &wos[2], 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val20).await, 0);
    assert_eq!(balance(&pool, fg_v).await, 600, "all 3 WOs FG drains");
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);
}

/// Sequential same-pool baseline: 2 WOs but second starts AFTER first
/// fully closes. Each WO is solo at every step. Pre-balance fires
/// normally for each. Confirms the gate is non-restrictive.
#[tokio::test(flavor = "multi_thread")]
async fn sequential_same_pool_baseline_pre_balance_still_fires() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P-SEQ", "standard").await;
    set_std_cost(&pool, &parent, 20).await;
    let comp = fresh_sku(&pool, "P-SEQ-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "SEQ-RAW").await;
    let fg_loc = fresh_location(&pool, "SEQ-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip, fg_v, var_close) = open_parent_pool_per_op(&pool, &parent, &fg_loc, &[10]).await;
    let (_qty10, val10) = wip[0];

    seed_raw_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;
    let var_pre = balance(&pool, var_close).await;

    let bom = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    // WO1 cycle: start → complete.
    let wo1 = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    pin_wo_bom(&pool, &wo1, bom).await;
    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 0, "WO1 fully drained");
    assert_eq!(balance(&pool, fg_v).await, 200);

    // WO2 cycle: start → complete (pool empty before WO2 start).
    let wo2 = create_wo(&pool, "WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    pin_wo_bom(&pool, &wo2, bom).await;
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 0);
    assert_eq!(balance(&pool, fg_v).await, 400);

    // No spurious variance for clean sequential same-pool runs.
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);
}

/// Pre-fix demonstration: verify the gate logic by simulating the buggy
/// scenario explicitly. With the fix, variance_wo_close should NOT
/// receive WO2's $200 prematurely. Without the fix (mig 0064 body),
/// it would. This test serves as a regression marker.
#[tokio::test(flavor = "multi_thread")]
async fn gate_prevents_premature_absorption_of_other_wo_wip() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P-GATE", "standard").await;
    set_std_cost(&pool, &parent, 20).await;
    let comp = fresh_sku(&pool, "P-GATE-C", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "GATE-RAW").await;
    let fg_loc = fresh_location(&pool, "GATE-FG").await;

    let (raw_q, raw_v) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip, _fg_v, var_close) = open_parent_pool_per_op(&pool, &parent, &fg_loc, &[10]).await;
    let (_qty10, val10) = wip[0];

    seed_raw_pool(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;
    let var_pre = balance(&pool, var_close).await;

    let bom = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom, 1, 10, &comp, &raw_loc, 2).await;

    let wo1 = create_wo(&pool, "WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    pin_wo_bom(&pool, &wo1, bom).await;
    let wo2 = create_wo(&pool, "WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    pin_wo_bom(&pool, &wo2, bom).await;

    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 400);

    // WO1 wo_complete: shared pool. With gate: no pre-balance. Pre-fix:
    // pre-balance would have credited val10 by $200, draining WO2's portion.
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 200,
               "WO2's $200 portion preserved (would be 0 pre-fix)");
    assert_eq!(balance(&pool, var_close).await - var_pre, 0,
               "variance_wo_close NOT polluted by WO2's portion");

    // WO2 wo_complete: now sole. Pre-balance fires; pool drains cleanly.
    call_wo_complete(&pool, &wo2, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, val10).await, 0);
    assert_eq!(balance(&pool, var_close).await - var_pre, 0);
}
