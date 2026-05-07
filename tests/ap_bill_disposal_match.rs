//! `acct-3yno` / `acct-7t4.5` — post_ap_bill `kind='disposal_match'`
//!
//! Closes the AP-side reconciliation loop on the by-products epic.
//! Disposal vendor sends a bill for actual waste pickup; we match it
//! against the WO-time accrual posted by mig 0099.
//!
//! Per-test assertions:
//!   1. Exact-price match drains accrued_disposal_liability(vendor) to
//!      zero, ap(vendor) credited, no variance
//!   2. Within-tolerance Δ absorbs to variance_match_tolerance
//!   3. Out-of-tolerance bill rejects with P0024
//!   4. Partial bill leaves remainder accrued
//!   5. Multi-WO bill: one bill covers two separate wo_events
//!   6. Mixed bill: po_match + service + disposal_match in one call
//!   7. Vendor mismatch raises P0025
//!   8. Currency mismatch raises P0025
//!   9. Over-bill rejection (cumulative qty > accrued)
//!   10. assert_invariants_hold (I1–I7)

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Helpers (mostly copied from wo_complete_disposal_cost.rs)
// ============================================================

async fn one_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("sku {code}: {e}"))
}

async fn one_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("loc {code}: {e}"))
}

async fn one_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("vendor {code}: {e}"))
}

async fn set_vendor_tolerance(pool: &PgPool, vendor_id: &str, pct: f64) {
    sqlx::query("UPDATE vendors SET unit_cost_tolerance_pct = $1::NUMERIC WHERE id = $2::UUID")
        .bind(pct)
        .bind(vendor_id)
        .execute(pool)
        .await
        .expect("set tolerance");
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    routing_op: Option<i32>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id,
             counterparty_id, routing_op, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6::UUID, $7,
                 $8::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(routing_op)
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
    .unwrap_or_else(|e| panic!("std_cost {sku_id}: {e}"));
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

#[allow(dead_code)]
struct Wo {
    wo_id: String,
    parent_id: String,
    qty_target: i64,
    fg_loc_id: String,
    bom_id: i64,
}

async fn scaffold_wo_with_components(
    pool: &PgPool,
    suffix: &str,
    qty_target: i64,
    parent_std: i64,
    component_std: i64,
) -> Wo {
    let parent_code = format!("DM-P-{suffix}");
    let comp_code = format!("DM-C-{suffix}");
    let raw_loc_code = format!("DM-R-{suffix}");
    let fg_loc_code = format!("DM-FG-{suffix}");

    let parent_id = one_sku(pool, &parent_code).await;
    let comp_id = one_sku(pool, &comp_code).await;
    let raw_loc = one_location(pool, &raw_loc_code).await;
    let fg_loc = one_location(pool, &fg_loc_code).await;
    set_std_cost(pool, &parent_id, parent_std).await;
    set_std_cost(pool, &comp_id, component_std).await;

    open_account(pool, "stock_wip", "qty", None,
        Some(&parent_id), None, None, Some(10), "debit").await;
    open_account(pool, "inv_value_wip", "value", Some("USD"),
        Some(&parent_id), None, None, Some(10), "debit").await;
    open_account(pool, "stock_available", "qty", None,
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;
    open_account(pool, "inv_value_fg", "value", Some("USD"),
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;

    open_account(pool, "stock_consumed", "qty", None,
        Some(&comp_id), None, None, None, "debit").await;
    let raw_qty = open_account(pool, "stock_available", "qty", None,
        Some(&comp_id), Some(&raw_loc), None, None, "debit").await;
    let raw_val = open_account(pool, "inv_value_raw", "value", Some("USD"),
        Some(&comp_id), Some(&raw_loc), None, None, "debit").await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    let posted_by = fresh_uuid(pool).await;
    let did = fresh_uuid(pool).await;
    let mint = serde_json::json!([
        {"reason":"cycle_count_adj","document_kind":"dm_seed","document_id":did,
         "debit_account_id":raw_qty,"credit_account_id":void_qty,
         "amount":qty_target * 10,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"dm_seed","document_id":did,
         "debit_account_id":raw_val,"credit_account_id":void_val,
         "amount":qty_target * 10 * component_std,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)")
        .bind(mint).execute(pool).await.expect("seed raw");

    let bom_id = create_bom_header(pool, &parent_code).await;
    add_bom_item(pool, bom_id, 1, 10, &comp_code, &raw_loc_code, 1, 100.0).await;

    let posted_by = fresh_uuid(pool).await;
    let wo_id: String = sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID)
         RETURNING id::text",
    )
    .bind(format!("DM-WO-{suffix}"))
    .bind(&parent_id)
    .bind(&fg_loc)
    .bind(qty_target)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .expect("create wo");
    sqlx::query(
        "INSERT INTO wo_routings (wo_id, routing_op, op_name)
         VALUES ($1::UUID, 10, 'MILL')",
    )
    .bind(&wo_id)
    .execute(pool)
    .await
    .expect("routing");

    Wo {
        wo_id,
        parent_id,
        qty_target,
        fg_loc_id: fg_loc,
        bom_id,
    }
}

#[allow(clippy::too_many_arguments)]
async fn add_bom_disposal(
    pool: &PgPool,
    bom_id: i64,
    by_product_no: i32,
    output_sku_id: &str,
    fg_loc_id: &str,
    qty_per_parent: f64,
    unit_value: i64,
    disposal_basis: &str,
    vendor_id: &str,
) {
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment,
             disposal_basis, disposal_vendor_id)
         VALUES ($1, $2, $3::UUID, $4::UUID, $5, $6, 'disposal_cost', $7, $8::UUID)",
    )
    .bind(bom_id)
    .bind(by_product_no)
    .bind(output_sku_id)
    .bind(fg_loc_id)
    .bind(qty_per_parent)
    .bind(unit_value)
    .bind(disposal_basis)
    .bind(vendor_id)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_disposal: {e}"));
}

async fn call_wo_start(pool: &PgPool, wo_id: &str) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query("SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)")
        .bind(wo_id)
        .bind(&posted_by)
        .bind(&key)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("post_wo_start: {e}"));
}

async fn call_wo_complete(pool: &PgPool, wo_id: &str, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, '2026-04-15'::DATE,
                                   $3::UUID, $4::UUID, NULL)::TEXT",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("post_wo_complete qty={qty}: {e}"))
}

async fn most_recent_wo_complete_event_id(pool: &PgPool, wo_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT id::text FROM wo_events
         WHERE wo_id = $1::UUID AND event_kind = 'wo_complete'
         ORDER BY posted_at DESC LIMIT 1",
    )
    .bind(wo_id)
    .fetch_one(pool)
    .await
    .expect("wo_event")
}

async fn open_byproduct_qty_acct(pool: &PgPool, sku_id: &str, loc_id: &str) -> i64 {
    open_account(pool, "stock_available", "qty", None,
        Some(sku_id), Some(loc_id), None, None, "debit").await
}

/// Open the per-vendor accrued_disposal_liability(USD) + ap(USD)
/// + ap_unsettled(USD) accounts for a vendor.
async fn open_vendor_accounts(pool: &PgPool, vendor_id: &str) -> (i64, i64, i64) {
    let liability = open_account(
        pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, Some(vendor_id), None, "credit",
    )
    .await;
    let ap = open_account(
        pool, "ap", "value", Some("USD"),
        None, None, Some(vendor_id), None, "credit",
    )
    .await;
    let ap_unsettled = open_account(
        pool, "ap_unsettled", "value", Some("USD"),
        None, None, Some(vendor_id), None, "credit",
    )
    .await;
    (liability, ap, ap_unsettled)
}

/// Run a full WO from start to complete with one disposal_cost by-product
/// declaration. Returns the wo_event_id of the closing wo_complete.
async fn run_wo_with_disposal(
    pool: &PgPool,
    suffix: &str,
    qty_target: i64,
    bp_qty_per_parent: f64,
    bp_unit_value: i64,
    disposal_basis: &str,
    vendor_id: &str,
) -> (String, String, i32, i64) {
    let wo = scaffold_wo_with_components(pool, suffix, qty_target, 600, 60).await;
    let bp_sku = one_sku(pool, &format!("DM-BP-{suffix}")).await;
    open_byproduct_qty_acct(pool, &bp_sku, &wo.fg_loc_id).await;
    if disposal_basis == "inventoriable" {
        // co-product fg gets the value leg too
        open_account(pool, "inv_value_fg", "value", Some("USD"),
            Some(&bp_sku), Some(&wo.fg_loc_id), None, None, "debit").await;
    }
    add_bom_disposal(
        pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        bp_qty_per_parent, bp_unit_value, disposal_basis, vendor_id,
    ).await;

    call_wo_start(pool, &wo.wo_id).await;
    call_wo_complete(pool, &wo.wo_id, qty_target).await;
    let event_id = most_recent_wo_complete_event_id(pool, &wo.wo_id).await;

    let actual_qty: i64 = sqlx::query_scalar(
        "SELECT actual_qty FROM wo_by_products WHERE wo_id = $1::UUID AND by_product_no = 1",
    )
    .bind(&wo.wo_id)
    .fetch_one(pool)
    .await
    .expect("actual qty");
    (wo.wo_id, event_id, 1, actual_qty)
}

async fn call_post_ap_bill(
    pool: &PgPool,
    vendor_id: &str,
    lines: serde_json::Value,
) -> Result<(), sqlx::Error> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2, '2026-04-16'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(vendor_id)
    .bind(&lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .map(|_| ())
}

fn extract_sqlstate(e: &sqlx::Error) -> String {
    e.as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.into_owned())
        .unwrap_or_default()
}

// ============================================================
// 1. Exact-price match drains accrual to zero
// ============================================================

#[tokio::test]
async fn disposal_match_exact_price_drains_accrual() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "EXACT").await;
    let (liability, ap, _ap_unsettled) = open_vendor_accounts(&pool, &vendor_id).await;

    // Period basis (simpler: liability only, no fg adder).
    // unit_value = -25, qty_per_parent = 1, qty_target = 10 → accrual = 250
    let (_wo_id, event_id, by_no, actual_qty) =
        run_wo_with_disposal(&pool, "EXACT", 10, 1.0, -25, "period", &vendor_id).await;
    assert_eq!(actual_qty, 10);
    assert_eq!(balance(&pool, liability).await, -250); // credit balance: 250

    // Post bill at the exact accrued price.
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 10, "unit_cost": 25, "amount": 250}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines).await.expect("bill");

    // Liability fully drained.
    assert_eq!(balance(&pool, liability).await, 0);
    // ap credited 250 (raw signed: −250, credit-normal).
    assert_eq!(balance(&pool, ap).await, -250);
    // No tolerance variance.
    let var_tol = account_id_by_kind_currency(&pool, "variance_match_tolerance", Some("USD")).await;
    assert_eq!(balance(&pool, var_tol).await, 0);

    assert_invariants_hold(&pool, "disposal_match_exact_price_drains_accrual").await;
}

// ============================================================
// 2. Within-tolerance Δ absorbs to variance_match_tolerance
// ============================================================

#[tokio::test]
async fn disposal_match_within_tolerance_absorbs() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "TOL").await;
    set_vendor_tolerance(&pool, &vendor_id, 5.0).await;
    let (liability, ap, _) = open_vendor_accounts(&pool, &vendor_id).await;

    // unit_value = -100, qty 10 → accrual 1000
    let (_wo_id, event_id, by_no, _) =
        run_wo_with_disposal(&pool, "TOL", 10, 1.0, -100, "period", &vendor_id).await;
    assert_eq!(balance(&pool, liability).await, -1000);

    // Bill at 102 per unit (2% over, within 5% tolerance).
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 10, "unit_cost": 102, "amount": 1020}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines).await.expect("bill");

    // Liability drained at accrual price (10 × 100 = 1000).
    assert_eq!(balance(&pool, liability).await, 0);
    // ap credited at bill price (10 × 102 = 1020).
    assert_eq!(balance(&pool, ap).await, -1020);
    // variance_match_tolerance gets the 20 unfavorable (debit).
    let var_tol = account_id_by_kind_currency(&pool, "variance_match_tolerance", Some("USD")).await;
    assert_eq!(balance(&pool, var_tol).await, 20);

    assert_invariants_hold(&pool, "disposal_match_within_tolerance_absorbs").await;
}

// ============================================================
// 3. Out-of-tolerance bill rejects with P0024
// ============================================================

#[tokio::test]
async fn disposal_match_out_of_tolerance_rejects() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "OOT").await;
    set_vendor_tolerance(&pool, &vendor_id, 5.0).await;
    open_vendor_accounts(&pool, &vendor_id).await;

    let (_wo_id, event_id, by_no, _) =
        run_wo_with_disposal(&pool, "OOT", 10, 1.0, -100, "period", &vendor_id).await;

    // Bill at 110 per unit (10% over, exceeds 5% tolerance).
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 10, "unit_cost": 110, "amount": 1100}
    ]);
    let err = call_post_ap_bill(&pool, &vendor_id, lines).await.expect_err("expected P0024");
    assert_eq!(extract_sqlstate(&err), "P0024", "{err}");
}

// ============================================================
// 4. Partial bill leaves remainder accrued
// ============================================================

#[tokio::test]
async fn partial_disposal_match() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "PRT").await;
    let (liability, ap, _) = open_vendor_accounts(&pool, &vendor_id).await;

    // accrual: qty 10 × 50 = 500
    let (_wo_id, event_id, by_no, _) =
        run_wo_with_disposal(&pool, "PRT", 10, 1.0, -50, "period", &vendor_id).await;
    assert_eq!(balance(&pool, liability).await, -500);

    // First bill: qty 4 × 50 = 200
    let lines1 = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 4, "unit_cost": 50, "amount": 200}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines1).await.expect("bill1");

    // Liability remainder: 500 − 200 = 300 (credit balance).
    assert_eq!(balance(&pool, liability).await, -300);
    assert_eq!(balance(&pool, ap).await, -200);

    // Second bill: qty 6 × 50 = 300 (drains the rest).
    let lines2 = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 6, "unit_cost": 50, "amount": 300}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines2).await.expect("bill2");

    assert_eq!(balance(&pool, liability).await, 0);
    assert_eq!(balance(&pool, ap).await, -500);

    assert_invariants_hold(&pool, "partial_disposal_match").await;
}

// ============================================================
// 5. Multi-WO bill: one bill covers two separate wo_events
// ============================================================

#[tokio::test]
async fn disposal_match_multi_wo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "MWO").await;
    let (liability, ap, _) = open_vendor_accounts(&pool, &vendor_id).await;

    let (_wo1, ev1, _, _) =
        run_wo_with_disposal(&pool, "MWO1", 10, 1.0, -25, "period", &vendor_id).await;
    let (_wo2, ev2, _, _) =
        run_wo_with_disposal(&pool, "MWO2", 6, 1.0, -25, "period", &vendor_id).await;
    // Total accrual: 250 + 150 = 400
    assert_eq!(balance(&pool, liability).await, -400);

    // Single bill drains both.
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": ev1,
         "by_product_no": 1,
         "qty": 10, "unit_cost": 25, "amount": 250},
        {"kind":"disposal_match",
         "disposal_wo_event_id": ev2,
         "by_product_no": 1,
         "qty": 6, "unit_cost": 25, "amount": 150}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines).await.expect("multi bill");

    assert_eq!(balance(&pool, liability).await, 0);
    assert_eq!(balance(&pool, ap).await, -400);

    assert_invariants_hold(&pool, "disposal_match_multi_wo").await;
}

// ============================================================
// 6. Mixed bill: po_match + service + disposal_match
// ============================================================

#[tokio::test]
async fn mixed_bill_po_service_disposal() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "MIX").await;
    let (liability, ap, ap_unsettled) = open_vendor_accounts(&pool, &vendor_id).await;
    let vendor_pool_q = open_account(
        &pool, "vendor_pool", "qty", None, None, None, Some(&vendor_id), None, "credit",
    )
    .await;

    // Disposal accrual leg.
    let (_wo, event_id, _, _) =
        run_wo_with_disposal(&pool, "MIX", 5, 1.0, -40, "period", &vendor_id).await;
    // accrual: 5 × 40 = 200
    assert_eq!(balance(&pool, liability).await, -200);

    // Set up a po_match path: SKU + raw loc + receipt.
    let raw_sku = one_sku(&pool, "DM-RAW-MIX").await;
    let raw_loc_id = one_location(&pool, "DM-RW-MIX").await;
    set_std_cost(&pool, &raw_sku, 100).await;
    open_account(&pool, "stock_available", "qty", None,
        Some(&raw_sku), Some(&raw_loc_id), None, None, "debit").await;
    open_account(&pool, "inv_value_raw", "value", Some("USD"),
        Some(&raw_sku), Some(&raw_loc_id), None, None, "debit").await;

    let posted_by = fresh_uuid(&pool).await;
    let po_id: String = sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&vendor_id)
    .fetch_one(&pool)
    .await
    .expect("po");
    let po_line_id: String = sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 5, 100, 'USD')
         RETURNING id::text",
    )
    .bind(&po_id)
    .bind(&raw_sku)
    .bind(&raw_loc_id)
    .fetch_one(&pool)
    .await
    .expect("po line");

    // Receive 5 → accrues ap_unsettled 500 to vendor.
    let recv_key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&po_id)
    .bind(serde_json::json!([
        {"po_line_id": po_line_id, "qty_received": 5}
    ]))
    .bind(&posted_by)
    .bind(&recv_key)
    .execute(&pool)
    .await
    .expect("po receipt");
    assert_eq!(balance(&pool, ap_unsettled).await, -500);
    assert_eq!(balance(&pool, vendor_pool_q).await, -5);

    // Service expense account.
    let svc_acct = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    // Mixed bill: po_match line clears 500 ap_unsettled, service line adds 50,
    // disposal_match drains 200 liability.
    let lines = serde_json::json!([
        {"kind":"po_match",
         "po_line_id": po_line_id,
         "qty": 5, "unit_cost": 100, "amount": 500},
        {"kind":"service",
         "expense_account_id": svc_acct,
         "amount": 50},
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": 1,
         "qty": 5, "unit_cost": 40, "amount": 200}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines).await.expect("mixed bill");

    // ap credited: 500 (po) + 50 (service) + 200 (disposal) = 750.
    assert_eq!(balance(&pool, ap).await, -750);
    // ap_unsettled drained.
    assert_eq!(balance(&pool, ap_unsettled).await, 0);
    // accrued_disposal_liability drained.
    assert_eq!(balance(&pool, liability).await, 0);

    assert_invariants_hold(&pool, "mixed_bill_po_service_disposal").await;
}

// ============================================================
// 7. Vendor mismatch raises P0025
// ============================================================

#[tokio::test]
async fn vendor_mismatch_raises_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_a = one_vendor(&pool, "VA").await;
    let vendor_b = one_vendor(&pool, "VB").await;
    open_vendor_accounts(&pool, &vendor_a).await;
    open_vendor_accounts(&pool, &vendor_b).await;

    let (_wo, event_id, by_no, _) =
        run_wo_with_disposal(&pool, "VEND", 5, 1.0, -25, "period", &vendor_a).await;

    // Bill against vendor_b for vendor_a's wo_by_products row.
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 5, "unit_cost": 25, "amount": 125}
    ]);
    let err = call_post_ap_bill(&pool, &vendor_b, lines).await.expect_err("P0025");
    assert_eq!(extract_sqlstate(&err), "P0025", "{err}");
}

// ============================================================
// 8. Over-bill rejects with P0024
// ============================================================

#[tokio::test]
async fn over_bill_rejects() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "OB").await;
    open_vendor_accounts(&pool, &vendor_id).await;

    let (_wo, event_id, by_no, actual) =
        run_wo_with_disposal(&pool, "OB", 5, 1.0, -25, "period", &vendor_id).await;
    assert_eq!(actual, 5);

    // Bill qty 6 against accrued 5 → should reject.
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 6, "unit_cost": 25, "amount": 150}
    ]);
    let err = call_post_ap_bill(&pool, &vendor_id, lines).await.expect_err("over-bill P0024");
    assert_eq!(extract_sqlstate(&err), "P0024", "{err}");
}

// ============================================================
// 9. Inventoriable disposal accrual drained correctly
// ============================================================
//
// Confirms the bill-side machinery is treatment-agnostic: inventoriable
// produces the same accrual shape as period (just with a different
// debit on the WO side), and a disposal_match bill drains it identically.

#[tokio::test]
async fn disposal_match_drains_inventoriable_accrual() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vendor_id = one_vendor(&pool, "INV").await;
    let (liability, ap, _) = open_vendor_accounts(&pool, &vendor_id).await;

    // Inventoriable basis: parent fg gets the disposal adder; liability
    // accrues identically.
    let (_wo, event_id, by_no, _) =
        run_wo_with_disposal(&pool, "INV", 10, 1.0, -25, "inventoriable", &vendor_id).await;
    assert_eq!(balance(&pool, liability).await, -250);

    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": by_no,
         "qty": 10, "unit_cost": 25, "amount": 250}
    ]);
    call_post_ap_bill(&pool, &vendor_id, lines).await.expect("bill");

    assert_eq!(balance(&pool, liability).await, 0);
    assert_eq!(balance(&pool, ap).await, -250);

    assert_invariants_hold(&pool, "disposal_match_drains_inventoriable_accrual").await;
}
