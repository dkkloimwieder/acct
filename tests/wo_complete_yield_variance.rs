//! `acct-a41h` / `acct-7t4.6` — yield variance for by-products at
//! `post_wo_complete`.
//!
//! Option A (discrete variance line, per industry-standard ERPs):
//!
//!   * Value-leg base posts at `planned_qty × unit_value` (treatment-
//!     specific routing).
//!   * Yield variance leg posts `(actual_qty − planned_qty) ×
//!     unit_value` — sign-aware between `variance_yield_byproduct`
//!     and the treatment-specific counterparty:
//!       - nrv_credit: against `inv_value_fg(by_product)`
//!       - disposal_cost: against `accrued_disposal_liability(vendor)`
//!     so the AP-side bill match (mig 0100) drains the actual amount.
//!
//! Per-test assertions:
//!   1. planned == actual: no variance fires
//!   2. actual > planned, nrv_credit: favorable (variance credit), bp fg gains delta
//!   3. actual < planned, nrv_credit: unfavorable (variance debit), bp fg loses delta
//!   4. actual > planned, disposal_cost (period): unfavorable (more disposal)
//!   5. actual < planned, disposal_cost (period): favorable (less disposal)
//!   6. inventoriable disposal with yield variance: liability adjusts to actual
//!   7. assert_invariants_hold (I1–I7)

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Helpers
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
    parent_fg_qty: i64,
    parent_fg_val: i64,
    parent_wip_val: i64,
}

async fn scaffold_wo(
    pool: &PgPool,
    suffix: &str,
    qty_target: i64,
    parent_std: i64,
    component_std: i64,
) -> Wo {
    let parent_code = format!("YV-P-{suffix}");
    let comp_code = format!("YV-C-{suffix}");
    let raw_loc_code = format!("YV-R-{suffix}");
    let fg_loc_code = format!("YV-FG-{suffix}");

    let parent_id = one_sku(pool, &parent_code).await;
    let comp_id = one_sku(pool, &comp_code).await;
    let raw_loc = one_location(pool, &raw_loc_code).await;
    let fg_loc = one_location(pool, &fg_loc_code).await;
    set_std_cost(pool, &parent_id, parent_std).await;
    set_std_cost(pool, &comp_id, component_std).await;

    open_account(pool, "stock_wip", "qty", None,
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_wip_val = open_account(pool, "inv_value_wip", "value", Some("USD"),
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_fg_qty = open_account(pool, "stock_available", "qty", None,
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"),
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
        {"reason":"cycle_count_adj","document_kind":"yv_seed","document_id":did,
         "debit_account_id":raw_qty,"credit_account_id":void_qty,
         "amount":qty_target * 10,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"yv_seed","document_id":did,
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
    .bind(format!("YV-WO-{suffix}"))
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
        parent_fg_qty,
        parent_fg_val,
        parent_wip_val,
    }
}

async fn add_bom_nrv(
    pool: &PgPool,
    bom_id: i64,
    by_product_no: i32,
    output_sku_id: &str,
    fg_loc_id: &str,
    qty_per_parent: f64,
    unit_value: i64,
) {
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, $2, $3::UUID, $4::UUID, $5, $6, 'nrv_credit')",
    )
    .bind(bom_id)
    .bind(by_product_no)
    .bind(output_sku_id)
    .bind(fg_loc_id)
    .bind(qty_per_parent)
    .bind(unit_value)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_nrv: {e}"));
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

async fn call_wo_complete(pool: &PgPool, wo_id: &str, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, $2, '2026-04-15'::DATE,
                                   $3::UUID, $4::UUID, NULL)",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_wo_complete qty={qty}: {e}"));
}

async fn open_byproduct_accounts(
    pool: &PgPool,
    output_sku_id: &str,
    fg_loc_id: &str,
) -> (i64, i64) {
    let bp_qty = open_account(pool, "stock_available", "qty", None,
        Some(output_sku_id), Some(fg_loc_id), None, None, "debit").await;
    let bp_val = open_account(pool, "inv_value_fg", "value", Some("USD"),
        Some(output_sku_id), Some(fg_loc_id), None, None, "debit").await;
    (bp_qty, bp_val)
}

async fn set_actual_qty(pool: &PgPool, wo_id: &str, by_product_no: i32, actual: i64) {
    sqlx::query(
        "UPDATE wo_by_products SET actual_qty = $1
         WHERE wo_id = $2::UUID AND by_product_no = $3",
    )
    .bind(actual)
    .bind(wo_id)
    .bind(by_product_no)
    .execute(pool)
    .await
    .expect("set actual qty");
}

async fn yield_var_acct(pool: &PgPool) -> i64 {
    account_id_by_kind_currency(pool, "variance_yield_byproduct", Some("USD")).await
}

// ============================================================
// 1. planned == actual: no variance fires
// ============================================================

#[tokio::test]
async fn planned_eq_actual_no_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "EQ", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-EQ").await;
    let (bp_qty, bp_val) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    add_bom_nrv(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 1.0, 50).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // planned defaults to actual at wo_start; both = 10.
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // bp_qty 10, bp_val 500 (planned base only).
    assert_eq!(balance(&pool, bp_qty).await, 10);
    assert_eq!(balance(&pool, bp_val).await, 500);
    // Parent fg = 6000 − 500 = 5500.
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 5500);
    // Yield variance untouched.
    assert_eq!(balance(&pool, yield_var_acct(&pool).await).await, 0);

    assert_invariants_hold(&pool, "planned_eq_actual_no_variance").await;
}

// ============================================================
// 2. nrv_credit favorable: actual > planned (more by-product yielded)
// ============================================================

#[tokio::test]
async fn nrv_favorable_actual_gt_planned() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "FAV", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-FAV").await;
    let (bp_qty, bp_val) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    add_bom_nrv(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 1.0, 50).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // Caller asserts higher actual yield: 14 vs planned 10 (Δ = +4).
    set_actual_qty(&pool, &wo.wo_id, 1, 14).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // bp_qty = actual = 14
    assert_eq!(balance(&pool, bp_qty).await, 14);
    // bp_val = base (planned × u = 500) + variance (Δ × u = 200) = 700
    assert_eq!(balance(&pool, bp_val).await, 700);
    // Parent fg = 6000 − planned_drain (500) = 5500
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 5500);
    // Yield variance: favorable (credit balance) = -200 raw signed
    assert_eq!(balance(&pool, yield_var_acct(&pool).await).await, -200);

    assert_invariants_hold(&pool, "nrv_favorable_actual_gt_planned").await;
}

// ============================================================
// 3. nrv_credit unfavorable: actual < planned
// ============================================================

#[tokio::test]
async fn nrv_unfavorable_actual_lt_planned() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "UNF", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-UNF").await;
    let (bp_qty, bp_val) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    add_bom_nrv(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 1.0, 50).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // Caller asserts lower actual yield: 6 vs planned 10 (Δ = -4).
    set_actual_qty(&pool, &wo.wo_id, 1, 6).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // bp_qty = actual = 6
    assert_eq!(balance(&pool, bp_qty).await, 6);
    // bp_val = base (500) - variance (200) = 300
    assert_eq!(balance(&pool, bp_val).await, 300);
    // Parent fg = 6000 − 500 = 5500 (unchanged from planned drain)
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 5500);
    // Yield variance: unfavorable (debit) = +200
    assert_eq!(balance(&pool, yield_var_acct(&pool).await).await, 200);

    assert_invariants_hold(&pool, "nrv_unfavorable_actual_lt_planned").await;
}

// ============================================================
// 4. disposal_cost period unfavorable (actual > planned: more disposal)
// ============================================================

#[tokio::test]
async fn disposal_period_unfavorable_more_disposal() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "DUF", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-DUF").await;
    open_account(&pool, "stock_available", "qty", None,
        Some(&bp_sku), Some(&wo.fg_loc_id), None, None, "debit").await;
    let vendor_id = one_vendor(&pool, "YV-V-DUF").await;
    let liability = open_account(
        &pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, Some(&vendor_id), None, "credit",
    )
    .await;
    let disposal_exp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -25, "period", &vendor_id,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // Actual disposal exceeds plan: 12 vs planned 10 (Δ = +2)
    set_actual_qty(&pool, &wo.wo_id, 1, 12).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // disposal_expense at planned: 10 × 25 = 250
    assert_eq!(balance(&pool, disposal_exp).await, 250);
    // Liability accrued at actual: 12 × 25 = 300 (planned 250 + variance 50)
    assert_eq!(balance(&pool, liability).await, -300);
    // variance_yield_byproduct: Δ × |unit_value| = 2 × 25 = 50 unfavorable (debit)
    assert_eq!(balance(&pool, yield_var_acct(&pool).await).await, 50);
    // Parent fg unchanged from no-disposal baseline (period basis doesn't inflate)
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 6000);

    assert_invariants_hold(&pool, "disposal_period_unfavorable_more_disposal").await;
}

// ============================================================
// 5. disposal_cost period favorable (actual < planned: less disposal)
// ============================================================

#[tokio::test]
async fn disposal_period_favorable_less_disposal() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "DFV", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-DFV").await;
    open_account(&pool, "stock_available", "qty", None,
        Some(&bp_sku), Some(&wo.fg_loc_id), None, None, "debit").await;
    let vendor_id = one_vendor(&pool, "YV-V-DFV").await;
    let liability = open_account(
        &pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, Some(&vendor_id), None, "credit",
    )
    .await;
    let disposal_exp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -25, "period", &vendor_id,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // Less disposal: 7 vs planned 10 (Δ = -3)
    set_actual_qty(&pool, &wo.wo_id, 1, 7).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // disposal_expense at planned: 250
    assert_eq!(balance(&pool, disposal_exp).await, 250);
    // Liability at actual: 7 × 25 = 175 (planned 250 - favorable 75)
    assert_eq!(balance(&pool, liability).await, -175);
    // variance_yield_byproduct: -3 × 25 = -75 favorable (credit)
    assert_eq!(balance(&pool, yield_var_acct(&pool).await).await, -75);

    assert_invariants_hold(&pool, "disposal_period_favorable_less_disposal").await;
}

// ============================================================
// 6. disposal_cost inventoriable + yield variance
// ============================================================
//
// Confirms the inventoriable variance routes between
// variance_yield_byproduct and accrued_disposal_liability — NOT
// against co-product fg. Co-products are charged at the planned-
// yield basis (constant); the actual-vs-planned delta is yield
// variance, not a co-product COGS variance.

#[tokio::test]
async fn disposal_inventoriable_variance_isolates_to_pl() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "DIN", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-DIN").await;
    open_account(&pool, "stock_available", "qty", None,
        Some(&bp_sku), Some(&wo.fg_loc_id), None, None, "debit").await;
    let vendor_id = one_vendor(&pool, "YV-V-DIN").await;
    let liability = open_account(
        &pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, Some(&vendor_id), None, "credit",
    )
    .await;

    // unit_value = -25, planned = 10 → planned disposal 250 → all 250 goes
    // to parent fg (single-output WO; allocation_pct = 100%).
    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -25, "inventoriable", &vendor_id,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // Actual exceeds plan: 13 vs 10 (Δ = +3)
    set_actual_qty(&pool, &wo.wo_id, 1, 13).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Parent fg = std × qty (6000) + planned_disposal_inflation (250) = 6250.
    // Variance does NOT touch co-product fg.
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 6250);
    // Liability at actual: 13 × 25 = 325 (planned 250 + variance 75)
    assert_eq!(balance(&pool, liability).await, -325);
    // variance_yield_byproduct: 3 × 25 = 75 unfavorable (debit)
    assert_eq!(balance(&pool, yield_var_acct(&pool).await).await, 75);

    assert_invariants_hold(&pool, "disposal_inventoriable_variance_isolates_to_pl").await;
}

// ============================================================
// 7. AP-side bill drains liability after yield variance applied
// ============================================================
//
// End-to-end: WO with yield variance accrues liability at actual
// qty; vendor bill matches against actual; liability drains cleanly.

#[tokio::test]
async fn ap_bill_drains_liability_after_yield_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, "BIL", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "YV-O-BIL").await;
    open_account(&pool, "stock_available", "qty", None,
        Some(&bp_sku), Some(&wo.fg_loc_id), None, None, "debit").await;
    let vendor_id = one_vendor(&pool, "YV-V-BIL").await;
    let liability = open_account(
        &pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, Some(&vendor_id), None, "credit",
    )
    .await;
    let ap = open_account(
        &pool, "ap", "value", Some("USD"),
        None, None, Some(&vendor_id), None, "credit",
    )
    .await;

    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -50, "period", &vendor_id,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    // Actual = 12 vs planned 10 → liability ends at 12 × 50 = 600
    set_actual_qty(&pool, &wo.wo_id, 1, 12).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;
    assert_eq!(balance(&pool, liability).await, -600);

    // Find the wo_complete event id for the bill match.
    let event_id: String = sqlx::query_scalar(
        "SELECT id::text FROM wo_events
         WHERE wo_id = $1::UUID AND event_kind = 'wo_complete'",
    )
    .bind(&wo.wo_id)
    .fetch_one(&pool)
    .await
    .expect("event id");

    // Bill at exact accrued unit price for the full actual qty.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([
        {"kind":"disposal_match",
         "disposal_wo_event_id": event_id,
         "by_product_no": 1,
         "qty": 12, "unit_cost": 50, "amount": 600}
    ]);
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2, '2026-04-16'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&vendor_id)
    .bind(&lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("bill");

    // Liability fully drained, ap credited 600.
    assert_eq!(balance(&pool, liability).await, 0);
    assert_eq!(balance(&pool, ap).await, -600);

    assert_invariants_hold(&pool, "ap_bill_drains_liability_after_yield_variance").await;
}
