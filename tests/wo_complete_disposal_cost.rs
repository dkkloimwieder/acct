//! `acct-6g47` / `acct-7t4.4` — post_wo_complete disposal_cost treatment.
//!
//! Two disposal_basis paths plus mixed-treatments coverage:
//!
//!   * inventoriable: disposal cost INFLATES co-product fg basis
//!     (per-output split by allocation_pct, last-output residual).
//!     Co-products absorb full v_total_drain (parent_std × p_qty) AND
//!     get a SECOND debit from the disposal adder.
//!     Liability accrues on `accrued_disposal_liability(vendor, ccy)`.
//!
//!   * period: disposal posts directly to `disposal_expense` (or a
//!     caller-supplied expense kind). Co-product COGS basis unchanged.
//!     Liability accrues on `accrued_disposal_liability(vendor, ccy)`.
//!
//! Gates inherited from mig 0098: v_will_close + v_cost_method='standard'.
//! Mixed-treatment WO covers nrv_credit + negligible + disposal_cost
//! coexistence in one WO with per-(ledger_kind, currency) double-entry
//! invariants.

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
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID, $7,
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
    parent_std: i64,
    qty_target: i64,
    fg_loc_id: String,
    bom_id: i64,
    parent_wip_qty: i64,
    parent_wip_val: i64,
    parent_fg_qty: i64,
    parent_fg_val: i64,
}

async fn scaffold_wo_with_components(
    pool: &PgPool,
    suffix: &str,
    qty_target: i64,
    parent_std: i64,
    component_std: i64,
) -> Wo {
    let parent_code = format!("DC-P-{suffix}");
    let comp_code = format!("DC-C-{suffix}");
    let raw_loc_code = format!("DC-R-{suffix}");
    let fg_loc_code = format!("DC-FG-{suffix}");

    let parent_id = one_sku(pool, &parent_code).await;
    let comp_id = one_sku(pool, &comp_code).await;
    let raw_loc = one_location(pool, &raw_loc_code).await;
    let fg_loc = one_location(pool, &fg_loc_code).await;
    set_std_cost(pool, &parent_id, parent_std).await;
    set_std_cost(pool, &comp_id, component_std).await;

    let parent_wip_qty = open_account(pool, "stock_wip", "qty", None,
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_wip_val = open_account(pool, "inv_value_wip", "value", Some("USD"),
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_fg_qty = open_account(pool, "stock_available", "qty", None,
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"),
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;

    let _consumed = open_account(pool, "stock_consumed", "qty", None,
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
        {"reason":"cycle_count_adj","document_kind":"dc_seed","document_id":did,
         "debit_account_id":raw_qty,"credit_account_id":void_qty,
         "amount":qty_target * 10,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"dc_seed","document_id":did,
         "debit_account_id":raw_val,"credit_account_id":void_val,
         "amount":qty_target * 10 * component_std,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
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
    .bind(format!("DC-WO-{suffix}"))
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
        parent_std,
        qty_target,
        fg_loc_id: fg_loc,
        bom_id,
        parent_wip_qty,
        parent_wip_val,
        parent_fg_qty,
        parent_fg_val,
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
    expense_kind: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment,
             disposal_basis, disposal_vendor_id, disposal_expense_account_kind)
         VALUES ($1, $2, $3::UUID, $4::UUID, $5, $6, 'disposal_cost',
                 $7, $8::UUID, $9::account_kind)",
    )
    .bind(bom_id)
    .bind(by_product_no)
    .bind(output_sku_id)
    .bind(fg_loc_id)
    .bind(qty_per_parent)
    .bind(unit_value)
    .bind(disposal_basis)
    .bind(vendor_id)
    .bind(expense_kind)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_disposal no={by_product_no}: {e}"));
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
    .unwrap_or_else(|e| panic!("add_bom_nrv no={by_product_no}: {e}"));
}

async fn add_bom_negligible(
    pool: &PgPool,
    bom_id: i64,
    by_product_no: i32,
    output_sku_id: &str,
    fg_loc_id: &str,
    qty_per_parent: f64,
) {
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, $2, $3::UUID, $4::UUID, $5, 0, 'negligible')",
    )
    .bind(bom_id)
    .bind(by_product_no)
    .bind(output_sku_id)
    .bind(fg_loc_id)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_negligible no={by_product_no}: {e}"));
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

/// Open a by-product output's qty + value accounts at the given location.
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

/// Open the per-vendor accrued_disposal_liability(USD) account.
async fn open_vendor_accrued(pool: &PgPool, vendor_id: &str) -> i64 {
    open_account(
        pool, "accrued_disposal_liability", "value", Some("USD"),
        None, None, Some(vendor_id), None, "credit",
    )
    .await
}

// ============================================================
// 1. inventoriable: disposal cost inflates co-product fg basis
// ============================================================

#[tokio::test]
async fn inventoriable_inflates_co_product_basis() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "INV1", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "DC-O-INV1").await;
    let (bp_qty_acct, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    let vendor_id = one_vendor(&pool, "DC-V-INV1").await;
    let liability_acct = open_vendor_accrued(&pool, &vendor_id).await;
    let disposal_exp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    // unit_value = -25, qty_per_parent = 1, qty_target = 10:
    //   total disposal = |−25| × 10 = 250
    //   single co-product (parent SKU itself, default primary @ 100%) absorbs all 250
    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -25, "inventoriable", &vendor_id, None,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Parent FG value: full v_total_drain (= 600 × 10 = 6000) + disposal adder 250 = 6250.
    let parent_fg_val = balance(&pool, wo.parent_fg_val).await;
    assert_eq!(
        parent_fg_val, 6250,
        "parent fg = std×qty + disposal = 6000 + 250 = 6250 (got {parent_fg_val})"
    );
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 10);

    // By-product fg: qty 10, value 0 (inventoriable does NOT credit by-product fg).
    assert_eq!(balance(&pool, bp_qty_acct).await, 10);
    assert_eq!(balance(&pool, bp_val_acct).await, 0);

    // accrued_disposal_liability: credit balance = 250 (raw signed: −250).
    assert_eq!(balance(&pool, liability_acct).await, -250);

    // disposal_expense untouched (inventoriable routes through fg, not P&L).
    assert_eq!(balance(&pool, disposal_exp).await, 0);

    // Parent WIP fully drained (inventoriable does NOT touch parent WIP).
    assert_eq!(balance(&pool, wo.parent_wip_qty).await, 0);
    assert_eq!(balance(&pool, wo.parent_wip_val).await, 0);

    assert_invariants_hold(&pool, "inventoriable_inflates_co_product_basis").await;
}

// ============================================================
// 2. period: disposal posts directly to disposal_expense
// ============================================================

#[tokio::test]
async fn period_recognizes_separately() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "PER1", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "DC-O-PER1").await;
    let (bp_qty_acct, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    let vendor_id = one_vendor(&pool, "DC-V-PER1").await;
    let liability_acct = open_vendor_accrued(&pool, &vendor_id).await;
    let disposal_exp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    // unit_value = -30, qty 10 → 300 total disposal expense
    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -30, "period", &vendor_id, None,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Parent FG: full v_total_drain (period basis does NOT inflate co-product).
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 6000);
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 10);

    // By-product fg: qty 10, value 0.
    assert_eq!(balance(&pool, bp_qty_acct).await, 10);
    assert_eq!(balance(&pool, bp_val_acct).await, 0);

    // disposal_expense debited 300; accrued_disposal_liability credited 300.
    assert_eq!(balance(&pool, disposal_exp).await, 300);
    assert_eq!(balance(&pool, liability_acct).await, -300);

    // Parent WIP fully drained.
    assert_eq!(balance(&pool, wo.parent_wip_qty).await, 0);
    assert_eq!(balance(&pool, wo.parent_wip_val).await, 0);

    assert_invariants_hold(&pool, "period_recognizes_separately").await;
}

// ============================================================
// 3. inventoriable + multiple co-products: split by allocation_pct
// ============================================================

#[tokio::test]
async fn inventoriable_with_two_coproducts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "INV2", 10, 600, 60).await;

    // Need a second co-product SKU + its fg accounts.
    let cop2_sku = one_sku(&pool, "DC-COP2-INV2").await;
    let cop2_fg_qty = open_account(&pool, "stock_available", "qty", None,
        Some(&cop2_sku), Some(&wo.fg_loc_id), None, None, "debit").await;
    let cop2_fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"),
        Some(&cop2_sku), Some(&wo.fg_loc_id), None, None, "debit").await;

    // Replace default wo_outputs (auto-init at wo_start) with explicit two-output split.
    // Pre-populate before wo_start so auto-init skips.
    sqlx::query(
        "INSERT INTO wo_outputs (wo_id, output_no, output_sku_id, fg_location_id, qty,
                                 allocation_method, allocation_pct)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 6, 'fixed_ratio', 60),
                ($1::UUID, 2, $4::UUID, $3::UUID, 4, 'fixed_ratio', 40)",
    )
    .bind(&wo.wo_id)
    .bind(&wo.parent_id)
    .bind(&wo.fg_loc_id)
    .bind(&cop2_sku)
    .execute(&pool)
    .await
    .expect("preset outputs");

    let bp_sku = one_sku(&pool, "DC-O-INV2").await;
    let (bp_qty_acct, _bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    let vendor_id = one_vendor(&pool, "DC-V-INV2").await;
    let liability_acct = open_vendor_accrued(&pool, &vendor_id).await;

    // unit_value = -50, qty 10 → 500 total disposal.
    // 60% / 40% split → 300 to parent fg, 200 to cop2 fg.
    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -50, "inventoriable", &vendor_id, None,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Co-product 1 (parent SKU): qty 6, value share = 6000 × 60 / 100 = 3600,
    // PLUS disposal share = 500 × 60 / 100 = 300 → fg val 3900.
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 6);
    assert_eq!(
        balance(&pool, wo.parent_fg_val).await,
        3900,
        "co-product 1 fg = 3600 + 300 = 3900"
    );

    // Co-product 2: qty 4, base share = 6000 − 3600 = 2400 (residual);
    // disposal share = 500 − 300 = 200 (residual) → fg val 2600.
    assert_eq!(balance(&pool, cop2_fg_qty).await, 4);
    assert_eq!(
        balance(&pool, cop2_fg_val).await,
        2600,
        "co-product 2 fg = 2400 + 200 = 2600"
    );

    // By-product qty 10.
    assert_eq!(balance(&pool, bp_qty_acct).await, 10);

    // Liability: 500 total credit.
    assert_eq!(balance(&pool, liability_acct).await, -500);

    assert_invariants_hold(&pool, "inventoriable_with_two_coproducts").await;
}

// ============================================================
// 4. period with custom expense kind
// ============================================================

#[tokio::test]
async fn period_with_custom_expense_kind() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "CUS1", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "DC-O-CUS1").await;
    let (bp_qty_acct, _) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    let vendor_id = one_vendor(&pool, "DC-V-CUS1").await;
    let liability_acct = open_vendor_accrued(&pool, &vendor_id).await;
    let disposal_exp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;
    // labor_expense already seeded USD-only; reuse as a "custom" P&L kind.
    let labor_exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    // unit_value = -20, qty 10 → 200 total
    // disposal_expense_account_kind = 'labor_expense' (caller override)
    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -20, "period", &vendor_id, Some("labor_expense"),
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Default disposal_expense untouched; labor_expense routed instead.
    assert_eq!(balance(&pool, disposal_exp).await, 0);
    assert_eq!(balance(&pool, labor_exp).await, 200);
    assert_eq!(balance(&pool, liability_acct).await, -200);
    assert_eq!(balance(&pool, bp_qty_acct).await, 10);

    assert_invariants_hold(&pool, "period_with_custom_expense_kind").await;
}

// ============================================================
// 5. Mixed treatments in one WO: nrv + negligible + disposal_inv + disposal_per
// ============================================================

#[tokio::test]
async fn mixed_treatments_in_one_wo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "MIX", 10, 600, 60).await;

    // 4 by-product SKUs.
    let nrv_sku = one_sku(&pool, "DC-O-MIX-NRV").await;
    let neg_sku = one_sku(&pool, "DC-O-MIX-NEG").await;
    let inv_sku = one_sku(&pool, "DC-O-MIX-INV").await;
    let per_sku = one_sku(&pool, "DC-O-MIX-PER").await;

    let (nrv_qty, nrv_val) = open_byproduct_accounts(&pool, &nrv_sku, &wo.fg_loc_id).await;
    let (neg_qty, neg_val) = open_byproduct_accounts(&pool, &neg_sku, &wo.fg_loc_id).await;
    let (inv_qty, inv_val) = open_byproduct_accounts(&pool, &inv_sku, &wo.fg_loc_id).await;
    let (per_qty, per_val) = open_byproduct_accounts(&pool, &per_sku, &wo.fg_loc_id).await;

    let vendor_id = one_vendor(&pool, "DC-V-MIX").await;
    let liability_acct = open_vendor_accrued(&pool, &vendor_id).await;
    let disposal_exp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    // nrv_credit: 1.0 × qty=10, unit_value=40 → drain 400 from parent WIP, by-product val 400
    add_bom_nrv(&pool, wo.bom_id, 1, &nrv_sku, &wo.fg_loc_id, 1.0, 40).await;
    // negligible: 0.5 × qty=10 = 5, unit_value=0
    add_bom_negligible(&pool, wo.bom_id, 2, &neg_sku, &wo.fg_loc_id, 0.5).await;
    // disposal inventoriable: 1.0 × qty=10, unit_value=-15 → 150 to co-product fg
    add_bom_disposal(
        &pool, wo.bom_id, 3, &inv_sku, &wo.fg_loc_id,
        1.0, -15, "inventoriable", &vendor_id, None,
    ).await;
    // disposal period: 1.0 × qty=10, unit_value=-25 → 250 to disposal_expense
    add_bom_disposal(
        &pool, wo.bom_id, 4, &per_sku, &wo.fg_loc_id,
        1.0, -25, "period", &vendor_id, None,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Parent fg: drain = 6000 − 400 (nrv) = 5600, plus disposal adder 150 → 5750.
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 5750);
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 10);

    // nrv: qty 10, val 400.
    assert_eq!(balance(&pool, nrv_qty).await, 10);
    assert_eq!(balance(&pool, nrv_val).await, 400);

    // negligible: qty 5, val 0.
    assert_eq!(balance(&pool, neg_qty).await, 5);
    assert_eq!(balance(&pool, neg_val).await, 0);

    // disposal_inv: qty 10, val 0 (inventoriable does not credit by-product val).
    assert_eq!(balance(&pool, inv_qty).await, 10);
    assert_eq!(balance(&pool, inv_val).await, 0);

    // disposal_per: qty 10, val 0.
    assert_eq!(balance(&pool, per_qty).await, 10);
    assert_eq!(balance(&pool, per_val).await, 0);

    // Liability: 150 (inventoriable) + 250 (period) = 400 credit.
    assert_eq!(balance(&pool, liability_acct).await, -400);

    // disposal_expense: 250 (only the period basis hits P&L).
    assert_eq!(balance(&pool, disposal_exp).await, 250);

    // Parent WIP fully drained.
    assert_eq!(balance(&pool, wo.parent_wip_qty).await, 0);
    assert_eq!(balance(&pool, wo.parent_wip_val).await, 0);

    assert_invariants_hold(&pool, "mixed_treatments_in_one_wo").await;
}

// ============================================================
// 6. Vendor account missing → P0010
// ============================================================

#[tokio::test]
async fn missing_vendor_liability_account_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "MIS", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "DC-O-MIS").await;
    let (_bp_qty, _bp_val) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    let vendor_id = one_vendor(&pool, "DC-V-MIS").await;
    // Deliberately do NOT open accrued_disposal_liability for this vendor.

    add_bom_disposal(
        &pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id,
        1.0, -10, "period", &vendor_id, None,
    ).await;

    call_wo_start(&pool, &wo.wo_id).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let err = sqlx::query("SELECT post_wo_complete($1::UUID, 10, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)")
        .bind(&wo.wo_id)
        .bind(&posted_by)
        .bind(&key)
        .execute(&pool)
        .await
        .expect_err("expected P0010");
    let sqlstate = err.as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.into_owned())
        .unwrap_or_default();
    assert_eq!(sqlstate, "P0010", "expected P0010 (account_missing); got {sqlstate}: {err}");
}
