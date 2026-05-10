//! T1 probes for post_so_ship lot_fifo branch consulting
//! reservation pins (mig 0053, acct-5vh9, E2.5-followup W2).
//!
//! When the SKU is lot_fifo and the so_line has pinned active /
//! allocated reservations, post_so_ship MUST resolve the lot from
//! the pin set rather than blindly trusting the caller's lot_id
//! key. Resolution table:
//!
//!   pins | caller_lot | action
//!   -----+------------+-----------------------------------
//!    0   | NULL       | FIFO walk (existing behavior)
//!    0   | given      | use caller's (existing behavior)
//!    1   | NULL       | use pin's lot_id
//!    1   | matches    | use it
//!    1   | mismatches | P0054 ship_lot_pin_conflict
//!    >1  | NULL       | P0055 ambiguous_pinned_reservation
//!    >1  | matches    | use caller's (resolves ambiguity)
//!    >1  | mismatches | P0054 (caller bypasses pins)
//!
//! Coverage:
//!   E2.5f.S1 single pin + NULL caller → walks pin's lot
//!   E2.5f.S2 single pin + caller match → walks it (consistent)
//!   E2.5f.S3 single pin + caller mismatch → P0054
//!   E2.5f.S4 0 pins + NULL caller → FIFO walk (back-compat)
//!   E2.5f.S5 0 pins + caller given → use caller's (back-compat)
//!   E2.5f.S6 multi-pin + NULL caller → P0055
//!   E2.5f.S7 multi-pin + caller matches one → use caller's
//!   E2.5f.S8 multi-pin + caller outside pins → P0054

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Scaffolding (mirrors lot_fg_lifecycle_t1)
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
    .unwrap()
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
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
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, routing_op,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6,
                 $7::UUID, $8::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(routing_op)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
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
    .unwrap();
}

async fn seed_standard_component(pool: &PgPool, sku: &str, loc: &str, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, NULL, 'USD', 'raw',
            '2026-04-10'::DATE, $4::UUID, $5::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap();
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
    .unwrap()
}

async fn add_routing(pool: &PgPool, wo_id: &str, op: i32, name: &str) {
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)")
        .bind(wo_id)
        .bind(op)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

async fn create_bom(pool: &PgPool, parent: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn add_bom_item(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    op: i32,
    comp: &str,
    comp_loc: &str,
    qty_per_parent: i64,
) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 100,
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
    .unwrap();
}

async fn call_wo_start(pool: &PgPool, wo_id: &str, business_date: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, NULL)::text",
    )
    .bind(wo_id)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(
    pool: &PgPool,
    wo_id: &str,
    qty: i64,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn create_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn add_so_line(
    pool: &PgPool,
    so_id: &str,
    line_no: i32,
    sku_id: &str,
    ship_loc_id: &str,
    qty_ordered: i64,
    unit_price: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD', 0)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn call_so_ship(
    pool: &PgPool,
    so_id: &str,
    lines: serde_json::Value,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(so_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn reserve_pinned_for_so_line(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    so_id: &str,
    so_line_id: &str,
    lot_id: i64,
) -> String {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT reserve_inventory(
            $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
            '2099-01-01'::TIMESTAMPTZ, NULL, $6::BIGINT, TRUE
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty)
    .bind(so_id)
    .bind(so_line_id)
    .bind(lot_id)
    .fetch_one(pool)
    .await
    .unwrap()
    .expect("reserve returned NULL")
}

#[allow(dead_code)]
struct ShipScaffold {
    parent: String,
    fg_loc: String,
    customer_id: String,
    parent_fg_qty: i64,
    parent_fg_val: i64,
    cogs_acct: i64,
}

async fn scaffold_lot_fg(pool: &PgPool, suffix: &str) -> ShipScaffold {
    let parent = fresh_sku(pool, &format!("FG-LOT-S-{suffix}"), "lot_fifo").await;
    sqlx::query("UPDATE skus SET tracked_by = 'lot' WHERE id = $1::UUID")
        .bind(&parent)
        .execute(pool)
        .await
        .unwrap();

    let comp = fresh_sku(pool, &format!("FG-LOT-S-{suffix}-C"), "standard").await;
    set_std_cost(pool, &comp, 50).await;
    let raw_loc = fresh_location(pool, &format!("FG-LOT-S-{suffix}-RAW")).await;
    let fg_loc = fresh_location(pool, &format!("FG-LOT-S-{suffix}-FG")).await;
    let customer_id = fresh_customer(pool, &format!("FG-LOT-S-{suffix}-CUST")).await;

    open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, None, "debit").await;

    open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    let parent_fg_qty = open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    open_account(pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    open_account(pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;

    seed_standard_component(pool, &comp, &raw_loc, 200).await;

    let bom_id = create_bom(pool, &parent).await;
    add_bom_item(pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    ShipScaffold { parent, fg_loc, customer_id, parent_fg_qty, parent_fg_val, cogs_acct }
}

async fn run_wo_lot(
    pool: &PgPool,
    wo_no: &str,
    sf: &ShipScaffold,
    qty: i64,
    business_date: &str,
) -> i64 {
    let wo_id = create_wo(pool, wo_no, &sf.parent, &sf.fg_loc, qty).await;
    add_routing(pool, &wo_id, 10, "ASSEMBLE").await;
    call_wo_start(pool, &wo_id, business_date).await.expect("wo_start");
    call_wo_complete(pool, &wo_id, qty, business_date).await.expect("wo_complete");

    sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY lot_id DESC LIMIT 1",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn lot_residual(pool: &PgPool, lot_id: i64) -> i64 {
    let txt: String = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty(
            il.lot_id, il.receipt_date
         )::TEXT
           FROM inventory_lots il WHERE il.lot_id = $1",
    )
    .bind(lot_id)
    .fetch_one(pool)
    .await
    .unwrap();
    txt.parse::<f64>().unwrap() as i64
}

// ============================================================
// E2.5f.S1 — single pin + NULL caller → walks pin's lot
// ============================================================

#[tokio::test]
async fn ship_single_pin_no_caller_uses_pin() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S1").await;

    let lot_a = run_wo_lot(&pool, "WO-S1-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S1-B", &sf, 10, "2026-04-12").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 5, 100).await;

    // Pin to lot B (the LATER lot), so FIFO default would pick A.
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 5, &so_id, &line, lot_b).await;

    call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 5 }]),
        "2026-04-15",
    )
    .await
    .expect("so_ship");

    // lot_b drained 5; lot_a untouched. Validates W2 used the pin
    // even though FIFO default would have picked A.
    assert_eq!(lot_residual(&pool, lot_a).await, 10);
    assert_eq!(lot_residual(&pool, lot_b).await, 5);
}

// ============================================================
// E2.5f.S2 — single pin + caller match → walks the pin's lot
// ============================================================

#[tokio::test]
async fn ship_single_pin_caller_match_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S2").await;

    let lot_a = run_wo_lot(&pool, "WO-S2-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S2-B", &sf, 10, "2026-04-12").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 4, 100).await;

    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 4, &so_id, &line, lot_b).await;

    call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 4, "lot_id": lot_b }]),
        "2026-04-15",
    )
    .await
    .expect("so_ship");

    assert_eq!(lot_residual(&pool, lot_a).await, 10);
    assert_eq!(lot_residual(&pool, lot_b).await, 6);
}

// ============================================================
// E2.5f.S3 — single pin + caller mismatch → P0054
// ============================================================

#[tokio::test]
async fn ship_single_pin_caller_mismatch_raises_p0054() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S3").await;

    let lot_a = run_wo_lot(&pool, "WO-S3-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S3-B", &sf, 10, "2026-04-12").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 5, 100).await;

    // Pin to A but caller asks for B.
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 5, &so_id, &line, lot_a).await;

    let err = call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 5, "lot_id": lot_b }]),
        "2026-04-15",
    )
    .await
    .unwrap_err();
    let code = err.as_database_error().and_then(|e| e.code()).map(|s| s.to_string());
    assert_eq!(code.as_deref(), Some("P0054"), "got {err:?}");
}

// ============================================================
// E2.5f.S4 — 0 pins + NULL caller → FIFO walk (back-compat)
// ============================================================

#[tokio::test]
async fn ship_no_pins_caller_null_fifo_walk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S4").await;

    let lot_a = run_wo_lot(&pool, "WO-S4-A", &sf, 5, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S4-B", &sf, 10, "2026-04-12").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 7, 100).await;

    // No reservations.
    call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 7 }]),
        "2026-04-15",
    )
    .await
    .expect("so_ship");

    // FIFO: drain lot_a (5) fully, then 2 from lot_b.
    assert_eq!(lot_residual(&pool, lot_a).await, 0);
    assert_eq!(lot_residual(&pool, lot_b).await, 8);
}

// ============================================================
// E2.5f.S5 — 0 pins + caller given → use caller's (back-compat)
// ============================================================

#[tokio::test]
async fn ship_no_pins_caller_supplied_uses_caller() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S5").await;

    let lot_a = run_wo_lot(&pool, "WO-S5-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S5-B", &sf, 10, "2026-04-12").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 6, 100).await;

    call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 6, "lot_id": lot_b }]),
        "2026-04-15",
    )
    .await
    .expect("so_ship");

    assert_eq!(lot_residual(&pool, lot_a).await, 10);
    assert_eq!(lot_residual(&pool, lot_b).await, 4);
}

// ============================================================
// E2.5f.S6 — multi-pin + NULL caller → P0055 ambiguous
// ============================================================

#[tokio::test]
async fn ship_multi_pin_no_caller_raises_p0055() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S6").await;

    let _lot_a = run_wo_lot(&pool, "WO-S6-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S6-B", &sf, 10, "2026-04-12").await;
    let lot_c = run_wo_lot(&pool, "WO-S6-C", &sf, 10, "2026-04-14").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 5, 100).await;

    // Two pins to different lots on the SAME so_line.
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 2, &so_id, &line, lot_b).await;
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 3, &so_id, &line, lot_c).await;

    let err = call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 5 }]),
        "2026-04-15",
    )
    .await
    .unwrap_err();
    let code = err.as_database_error().and_then(|e| e.code()).map(|s| s.to_string());
    assert_eq!(code.as_deref(), Some("P0055"), "got {err:?}");
}

// ============================================================
// E2.5f.S7 — multi-pin + caller matches one → use caller's
// ============================================================

#[tokio::test]
async fn ship_multi_pin_caller_matches_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S7").await;

    let _lot_a = run_wo_lot(&pool, "WO-S7-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S7-B", &sf, 10, "2026-04-12").await;
    let lot_c = run_wo_lot(&pool, "WO-S7-C", &sf, 10, "2026-04-14").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 4, 100).await;

    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 2, &so_id, &line, lot_b).await;
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 2, &so_id, &line, lot_c).await;

    // Caller picks lot_c (one of the pins) — disambiguates.
    call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 4, "lot_id": lot_c }]),
        "2026-04-15",
    )
    .await
    .expect("so_ship");

    assert_eq!(lot_residual(&pool, lot_b).await, 10);
    assert_eq!(lot_residual(&pool, lot_c).await, 6);
}

// ============================================================
// E2.5f.S8 — multi-pin + caller outside pins → P0054
// ============================================================

#[tokio::test]
async fn ship_multi_pin_caller_outside_pins_raises_p0054() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_lot_fg(&pool, "S8").await;

    let lot_a = run_wo_lot(&pool, "WO-S8-A", &sf, 10, "2026-04-10").await;
    let lot_b = run_wo_lot(&pool, "WO-S8-B", &sf, 10, "2026-04-12").await;
    let lot_c = run_wo_lot(&pool, "WO-S8-C", &sf, 10, "2026-04-14").await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let line = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 5, 100).await;

    // Pins to B + C; caller picks A → not in pin set.
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 2, &so_id, &line, lot_b).await;
    reserve_pinned_for_so_line(&pool, &sf.parent, &sf.fg_loc, 3, &so_id, &line, lot_c).await;

    let err = call_so_ship(
        &pool,
        &so_id,
        json!([{ "so_line_id": line, "qty_shipped": 5, "lot_id": lot_a }]),
        "2026-04-15",
    )
    .await
    .unwrap_err();
    let code = err.as_database_error().and_then(|e| e.code()).map(|s| s.to_string());
    assert_eq!(code.as_deref(), Some("P0054"), "got {err:?}");
}
