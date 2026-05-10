//! T1 probes for lot_fifo parent + by-products at post_wo_complete
//! (mig 0055, acct-fjxp).
//!
//! Coverage:
//!   B1 nrv_credit          → FG-lot created at post-NRV unit_cost; pool balanced
//!   B2 negligible          → no value-leg; lot at parent_unit_cost
//!   B3 disposal_cost period → disposal_expense + accrued_disposal_liability;
//!                              parent FG pool unaffected
//!   B4 disposal_cost inventoriable → P0006 (rejected)
//!   B5 nrv_credit + full FIFO depletion → pool drains to zero, no residue
//!   B6 nrv_credit + yield variance      → variance_yield_byproduct posts
//!   B7 lot_fifo parent + so_ship after NRV credit → COGS reflects NRV-reduced unit_cost

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Local scaffolding (mirrors tests/lot_fg_lifecycle_t1.rs)
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

async fn fresh_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vend {code}"))
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

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
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

#[allow(clippy::too_many_arguments)]
async fn add_byproduct(
    pool: &PgPool,
    bom_id: i64,
    by_product_no: i32,
    bp_sku: &str,
    bp_loc: &str,
    qty_per_parent: i64,
    unit_value: i64,
    treatment: &str,
    disposal_basis: Option<&str>,
    disposal_vendor: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment, disposal_basis,
             disposal_vendor_id, disposal_expense_account_kind)
         VALUES ($1, $2, $3::UUID, $4::UUID, $5, $6, $7, $8, $9::UUID, NULL)",
    )
    .bind(bom_id)
    .bind(by_product_no)
    .bind(bp_sku)
    .bind(bp_loc)
    .bind(qty_per_parent)
    .bind(unit_value)
    .bind(treatment)
    .bind(disposal_basis)
    .bind(disposal_vendor)
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

struct Scaffold {
    parent: String,
    fg_loc: String,
    customer_id: String,
    bom_id: i64,
    parent_wip_val: i64,
    parent_fg_val: i64,
    parent_fg_qty: i64,
    cogs_acct: i64,
}

async fn scaffold(pool: &PgPool, suffix: &str) -> Scaffold {
    let parent = fresh_sku(pool, &format!("FG-LOT-FJ-{suffix}"), "lot_fifo").await;
    sqlx::query("UPDATE skus SET tracked_by = 'lot' WHERE id = $1::UUID")
        .bind(&parent)
        .execute(pool)
        .await
        .unwrap();

    let comp = fresh_sku(pool, &format!("CMP-FJ-{suffix}"), "standard").await;
    set_std_cost(pool, &comp, 50).await;
    let raw_loc = fresh_location(pool, &format!("FJ-RAW-{suffix}")).await;
    let fg_loc = fresh_location(pool, &format!("FJ-FG-{suffix}")).await;
    let customer_id = fresh_customer(pool, &format!("FJ-CUST-{suffix}")).await;

    open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, None, "debit").await;

    open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    let parent_wip_val =
        open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    let parent_fg_qty =
        open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val =
        open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    open_account(pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    open_account(pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;

    seed_standard_component(pool, &comp, &raw_loc, 200).await;

    let bom_id = create_bom(pool, &parent).await;
    add_bom_item(pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    Scaffold {
        parent,
        fg_loc,
        customer_id,
        bom_id,
        parent_wip_val,
        parent_fg_val,
        parent_fg_qty,
        cogs_acct,
    }
}

/// Open accounts for a by-product SKU at its FG location.
async fn open_byproduct_accounts(pool: &PgPool, bp_sku: &str, bp_loc: &str) -> (i64, i64) {
    let q = open_account(
        pool, "stock_available", "qty", None,
        Some(bp_sku), Some(bp_loc), None, None, "debit",
    ).await;
    let v = open_account(
        pool, "inv_value_fg", "value", Some("USD"),
        Some(bp_sku), Some(bp_loc), None, None, "debit",
    ).await;
    (q, v)
}

// ============================================================
// B1 — nrv_credit on lot_fifo parent: FG-lot created at post-NRV unit_cost
// ============================================================

#[tokio::test]
async fn lot_fifo_parent_with_nrv_credit_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B1").await;

    // By-product setup: 1 unit per parent @ $5 NRV.
    let bp_sku = fresh_sku(&pool, "BP-FJ-B1", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B1-LOC").await;
    set_std_cost(&pool, &bp_sku, 5).await;
    let (bp_qty_acct, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    add_byproduct(&pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, 5, "nrv_credit", None, None).await;

    let wo = create_wo(&pool, "WO-FJ-B1", &sf.parent, &sf.fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;

    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");

    // Set actual_qty = planned (10) for this case. wo_start auto-populated wo_by_products.
    sqlx::query("UPDATE wo_by_products SET actual_qty = 10 WHERE wo_id = $1::UUID")
        .bind(&wo)
        .execute(&pool)
        .await
        .unwrap();

    call_wo_complete(&pool, &wo, 10, "2026-04-16").await.expect("wo_complete");

    // Parent FG: drain = 10 components @ $50 = $500. NRV credit = 10 * $5 = $50.
    // Net FG drain = $500 - $50 = $450. FG-lot at $450 / 10 = $45/unit.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 10, "parent FG qty=10");
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 450, "parent FG val=$450 post-NRV");
    assert_eq!(balance(&pool, sf.parent_wip_val).await, 0, "WIP drained to 0");

    // By-product: 10 units @ $5 = $50.
    assert_eq!(balance(&pool, bp_qty_acct).await, 10, "by-product qty=10");
    assert_eq!(balance(&pool, bp_val_acct).await, 50, "by-product value=$50");

    // FG-lot row created at post-NRV unit_cost.
    let lot: (String, String, String) = sqlx::query_as(
        "SELECT lot_code, original_quantity::TEXT, unit_cost::TEXT
           FROM inventory_lots
          WHERE product_id = $1::UUID
          ORDER BY lot_id DESC LIMIT 1",
    )
    .bind(&sf.parent)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(lot.0.starts_with("WO-"), "auto-gen lot_code");
    assert_eq!(lot.1, "10.000000");
    assert_eq!(lot.2, "45.0000", "FG-lot unit_cost reflects NRV credit");

    assert_invariants_hold(&pool, "B1 nrv_credit").await;
}

// ============================================================
// B2 — negligible: no value-leg, parent FG drained at full unit_cost
// ============================================================

#[tokio::test]
async fn lot_fifo_parent_with_negligible_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B2").await;

    let bp_sku = fresh_sku(&pool, "BP-FJ-B2", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B2-LOC").await;
    set_std_cost(&pool, &bp_sku, 0).await;
    let (bp_qty_acct, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    // negligible REQUIRES unit_value = 0.
    add_byproduct(&pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, 0, "negligible", None, None).await;

    let wo = create_wo(&pool, "WO-FJ-B2", &sf.parent, &sf.fg_loc, 5).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    sqlx::query("UPDATE wo_by_products SET actual_qty = 5 WHERE wo_id = $1::UUID")
        .bind(&wo).execute(&pool).await.unwrap();
    call_wo_complete(&pool, &wo, 5, "2026-04-16").await.expect("wo_complete");

    // No NRV credit; parent FG = 5 * $50 = $250. FG-lot at $50/unit.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 5);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 250);

    // By-product qty leg fired (5 units), value leg did NOT fire (negligible).
    assert_eq!(balance(&pool, bp_qty_acct).await, 5, "bp qty=5");
    assert_eq!(balance(&pool, bp_val_acct).await, 0, "bp value=0 (negligible)");

    let unit_cost: String = sqlx::query_scalar(
        "SELECT unit_cost::TEXT FROM inventory_lots
          WHERE product_id = $1::UUID ORDER BY lot_id DESC LIMIT 1",
    )
    .bind(&sf.parent)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(unit_cost, "50.0000", "FG-lot unit_cost = full parent unit (no NRV)");

    assert_invariants_hold(&pool, "B2 negligible").await;
}

// ============================================================
// B3 — disposal_cost period: doesn't touch parent FG pool
// ============================================================

#[tokio::test]
async fn lot_fifo_parent_with_disposal_cost_period_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B3").await;

    let bp_sku = fresh_sku(&pool, "BP-FJ-B3", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B3-LOC").await;
    set_std_cost(&pool, &bp_sku, 0).await;
    let (bp_qty_acct, _) = open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    let vendor_id = fresh_vendor(&pool, "VEND-FJ-B3").await;
    // accrued_disposal_liability is partitioned by counterparty + currency.
    open_account(
        &pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, None, Some(&vendor_id), "credit",
    ).await;

    // disposal_expense default open account for USD.
    let disp_exp_acct = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT id FROM accounts WHERE kind='disposal_expense'
                                    AND currency='USD' AND NOT is_closed LIMIT 1",
    )
    .fetch_one(&pool).await.unwrap();
    let disp_exp_acct = match disp_exp_acct {
        Some(id) => id,
        None => open_account(
            &pool, "disposal_expense", "value", Some("USD"),
            None, None, None, None, "debit",
        ).await,
    };

    // disposal_cost requires unit_value < 0 by CHECK constraint.
    add_byproduct(
        &pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, -5, "disposal_cost",
        Some("period"), Some(&vendor_id),
    ).await;

    let wo = create_wo(&pool, "WO-FJ-B3", &sf.parent, &sf.fg_loc, 8).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    sqlx::query("UPDATE wo_by_products SET actual_qty = 8 WHERE wo_id = $1::UUID")
        .bind(&wo).execute(&pool).await.unwrap();
    call_wo_complete(&pool, &wo, 8, "2026-04-16").await.expect("wo_complete");

    // Parent FG = 8 * $50 = $400. NOT reduced by disposal (period basis).
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 400);

    // disposal_expense debited 8 * $5 = $40.
    assert_eq!(balance(&pool, disp_exp_acct).await, 40);

    // By-product qty leg posted; bp value = 0 (disposal_cost doesn't credit bp_val_acct).
    assert_eq!(balance(&pool, bp_qty_acct).await, 8);

    // FG-lot at full $50/unit (unaffected by period disposal).
    let unit_cost: String = sqlx::query_scalar(
        "SELECT unit_cost::TEXT FROM inventory_lots
          WHERE product_id = $1::UUID ORDER BY lot_id DESC LIMIT 1",
    )
    .bind(&sf.parent)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(unit_cost, "50.0000");

    assert_invariants_hold(&pool, "B3 disposal_cost period").await;
}

// ============================================================
// B4 — disposal_cost inventoriable: P0006 (rejected)
// ============================================================

#[tokio::test]
async fn lot_fifo_parent_with_disposal_cost_inventoriable_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B4").await;

    let bp_sku = fresh_sku(&pool, "BP-FJ-B4", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B4-LOC").await;
    set_std_cost(&pool, &bp_sku, 0).await;
    let _ = open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    let vendor_id = fresh_vendor(&pool, "VEND-FJ-B4").await;
    open_account(
        &pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, None, Some(&vendor_id), "credit",
    ).await;

    add_byproduct(
        &pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, -5, "disposal_cost",
        Some("inventoriable"), Some(&vendor_id),
    ).await;

    let wo = create_wo(&pool, "WO-FJ-B4", &sf.parent, &sf.fg_loc, 5).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    sqlx::query("UPDATE wo_by_products SET actual_qty = 5 WHERE wo_id = $1::UUID")
        .bind(&wo).execute(&pool).await.unwrap();

    let res = call_wo_complete(&pool, &wo, 5, "2026-04-16").await;
    let err = res.expect_err("disposal_cost inventoriable on lot_fifo must raise");
    let dberr = err.as_database_error().unwrap();
    assert_eq!(dberr.code().unwrap().to_string(), "P0006");
    let msg = dberr.message();
    assert!(
        msg.contains("disposal_cost(inventoriable)"),
        "expected descriptive error, got: {msg}",
    );
}

// ============================================================
// B5 — nrv_credit + full FIFO depletion: pool drains exactly to zero
// ============================================================

#[tokio::test]
async fn lot_fifo_parent_with_nrv_credit_full_depletion_balances_pool() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B5").await;

    let bp_sku = fresh_sku(&pool, "BP-FJ-B5", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B5-LOC").await;
    set_std_cost(&pool, &bp_sku, 5).await;
    open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    add_byproduct(&pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, 5, "nrv_credit", None, None).await;

    let wo = create_wo(&pool, "WO-FJ-B5", &sf.parent, &sf.fg_loc, 4).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    sqlx::query("UPDATE wo_by_products SET actual_qty = 4 WHERE wo_id = $1::UUID")
        .bind(&wo).execute(&pool).await.unwrap();
    call_wo_complete(&pool, &wo, 4, "2026-04-16").await.expect("wo_complete");

    // FG: 4 units, $200 - $20 NRV = $180 → unit_cost = $45/unit.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 4);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 180);

    // Now ship all 4 units. FIFO walk uses lot's $45/unit.
    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 4, 100).await;
    call_so_ship(
        &pool, &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 4 }]),
        "2026-04-17",
    ).await.expect("so_ship");

    // Pool fully drained.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 0, "FG qty drained");
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 0, "FG value drained — no residue");

    // Lot residual = 0.
    let resid: String = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty(il.lot_id, il.receipt_date)::TEXT
           FROM inventory_lots il WHERE il.product_id = $1::UUID
          ORDER BY il.lot_id DESC LIMIT 1",
    )
    .bind(&sf.parent)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(resid.parse::<f64>().unwrap() as i64, 0);

    // COGS got 4 * $45 = $180.
    assert_eq!(balance(&pool, sf.cogs_acct).await, 180);

    assert_invariants_hold(&pool, "B5 full depletion").await;
}

// ============================================================
// B6 — nrv_credit + yield variance (actual_qty > planned_qty)
// ============================================================

#[tokio::test]
async fn lot_fifo_parent_with_nrv_credit_yield_variance_posts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B6").await;

    let bp_sku = fresh_sku(&pool, "BP-FJ-B6", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B6-LOC").await;
    set_std_cost(&pool, &bp_sku, 5).await;
    let (_bp_qty, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    // bom_by_products.qty_per_parent = 1; planned_qty = parent_qty.
    // Test: drive actual_qty > planned_qty so yield variance fires positive.
    add_byproduct(&pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, 5, "nrv_credit", None, None).await;

    let wo = create_wo(&pool, "WO-FJ-B6", &sf.parent, &sf.fg_loc, 5).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    // planned = 5 (per qty_per_parent=1 * parent_qty=5); actual = 7 → +2 yield.
    sqlx::query("UPDATE wo_by_products SET actual_qty = 7 WHERE wo_id = $1::UUID")
        .bind(&wo).execute(&pool).await.unwrap();
    call_wo_complete(&pool, &wo, 5, "2026-04-16").await.expect("wo_complete");

    // bp_val_acct: planned = 5 * $5 = $25 (NRV credit) + yield = 2 * $5 = $10 (yield variance).
    // Total bp_val_acct debit = $35.
    assert_eq!(balance(&pool, bp_val_acct).await, 35);

    // variance_yield_byproduct credit = $10.
    let yield_var: i64 = sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts
          WHERE kind = 'variance_yield_byproduct' AND currency = 'USD' AND NOT is_closed",
    )
    .fetch_one(&pool).await.unwrap();
    assert_eq!(yield_var, -10, "variance_yield_byproduct -10 (credit-side received yield)");

    assert_invariants_hold(&pool, "B6 yield variance").await;
}

// ============================================================
// B7 — Full lifecycle: lot_fifo + NRV credit + so_ship → COGS at NRV unit_cost
// ============================================================

#[tokio::test]
async fn lot_fifo_so_ship_after_nrv_uses_post_nrv_unit_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B7").await;

    let bp_sku = fresh_sku(&pool, "BP-FJ-B7", "standard").await;
    let bp_loc = fresh_location(&pool, "BP-FJ-B7-LOC").await;
    set_std_cost(&pool, &bp_sku, 5).await;
    open_byproduct_accounts(&pool, &bp_sku, &bp_loc).await;

    add_byproduct(&pool, sf.bom_id, 1, &bp_sku, &bp_loc, 1, 5, "nrv_credit", None, None).await;

    let wo = create_wo(&pool, "WO-FJ-B7", &sf.parent, &sf.fg_loc, 10).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    sqlx::query("UPDATE wo_by_products SET actual_qty = 10 WHERE wo_id = $1::UUID")
        .bind(&wo).execute(&pool).await.unwrap();
    call_wo_complete(&pool, &wo, 10, "2026-04-16").await.expect("wo_complete");

    // Ship 3 units. FIFO walks the single FG-lot at $45/unit.
    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 3, 100).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let _shp: String = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&so_id)
    .bind(serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 3 }]))
    .bind("2026-04-17")
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("so_ship");

    // COGS: 3 units * $45 = $135.
    let cogs_delta = balance(&pool, sf.cogs_acct).await;
    assert_eq!(cogs_delta, 135);

    // Snapshot on so_shipment_lines: cost_method_at_ship='lot_fifo' + unit_cost=45.
    let (cm, uc): (String, i64) = sqlx::query_as(
        "SELECT cost_method_at_ship::TEXT, unit_cost::BIGINT
           FROM so_shipment_lines WHERE so_line_id = $1::UUID
          ORDER BY id DESC LIMIT 1",
    )
    .bind(&so_line_id)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(cm, "lot_fifo");
    assert_eq!(uc, 45, "audit field reflects post-NRV unit_cost");

    assert_invariants_hold(&pool, "B7 lot_fifo + NRV + so_ship").await;
}
