//! `acct-u1n9` / `acct-7t4.3` — post_wo_complete by-products pre-pass.
//!
//! Covers nrv_credit + negligible treatments (disposal_cost is deferred
//! to acct-7t4.4 and is silently no-op'd here).
//!
//! Gates:
//!   * v_will_close (close-call only)
//!   * v_cost_method = 'standard' (WAC parent + by-products tracked
//!     as acct-nnyl follow-up)
//!
//! Per-test assertions:
//!   1. nrv_credit reduces co-product fg gain by unit_value × actual_qty
//!   2. by-product fg gains qty + value
//!   3. parent WIP drains by full v_total_drain (= parent_std × p_qty)
//!   4. negligible adds qty only, no value movement
//!   5. multiple by-products accumulate correctly
//!   6. partial completes do NOT fire by-products until close call
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

/// Scaffold: parent SKU at standard cost 600, one component (cost 60),
/// one routing op (10), one fg location, parent stock_wip+inv_value_wip
/// at op 10, parent stock_available+inv_value_fg at fg, plus pre-stocked
/// raw inv. Returns the WO id and account ids.
async fn scaffold_wo_with_components(
    pool: &PgPool,
    suffix: &str,
    qty_target: i64,
    parent_std: i64,
    component_std: i64,
) -> Wo {
    let parent_code = format!("BPC-P-{suffix}");
    let comp_code = format!("BPC-C-{suffix}");
    let raw_loc_code = format!("BPC-R-{suffix}");
    let fg_loc_code = format!("BPC-FG-{suffix}");

    let parent_id = one_sku(pool, &parent_code).await;
    let comp_id = one_sku(pool, &comp_code).await;
    let raw_loc = one_location(pool, &raw_loc_code).await;
    let fg_loc = one_location(pool, &fg_loc_code).await;
    set_std_cost(pool, &parent_id, parent_std).await;
    set_std_cost(pool, &comp_id, component_std).await;

    // Parent op 10 accounts.
    let parent_wip_qty = open_account(pool, "stock_wip", "qty", None,
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_wip_val = open_account(pool, "inv_value_wip", "value", Some("USD"),
        Some(&parent_id), None, None, Some(10), "debit").await;

    // Parent FG accounts (default output destination).
    let parent_fg_qty = open_account(pool, "stock_available", "qty", None,
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"),
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;

    // Component accounts + pre-stock so rm_issue at wo_start has stock to drain.
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
        {"reason":"cycle_count_adj","document_kind":"bp_seed","document_id":did,
         "debit_account_id":raw_qty,"credit_account_id":void_qty,
         "amount":qty_target * 10,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"bp_seed","document_id":did,
         "debit_account_id":raw_val,"credit_account_id":void_val,
         "amount":qty_target * 10 * component_std,"qty":qty_target * 10,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint).execute(pool).await.expect("seed raw");

    // BOM with one component item line.
    let bom_id = create_bom_header(pool, &parent_code).await;
    add_bom_item(pool, bom_id, 1, 10, &comp_code, &raw_loc_code, 1, 100.0).await;

    // WO + routing.
    let posted_by = fresh_uuid(pool).await;
    let wo_id: String = sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID)
         RETURNING id::text",
    )
    .bind(format!("BPC-WO-{suffix}"))
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

async fn add_bom_by_product(
    pool: &PgPool,
    bom_id: i64,
    by_product_no: i32,
    output_sku_id: &str,
    fg_loc_id: &str,
    qty_per_parent: f64,
    unit_value: i64,
    treatment: &str,
) {
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, $2, $3::UUID, $4::UUID, $5, $6, $7)",
    )
    .bind(bom_id)
    .bind(by_product_no)
    .bind(output_sku_id)
    .bind(fg_loc_id)
    .bind(qty_per_parent)
    .bind(unit_value)
    .bind(treatment)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_by_product no={by_product_no}: {e}"));
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

#[allow(dead_code)]
async fn call_op_move(pool: &PgPool, wo_id: &str, from: i32, to: i32, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_op_move($1::UUID, $2, $3, $4, '2026-04-15'::DATE,
                              $5::UUID, $6::UUID, NULL)",
    )
    .bind(wo_id)
    .bind(from)
    .bind(to)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_op_move: {e}"));
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

// ============================================================
// 1. nrv_credit reduces co-product drain
// ============================================================

#[tokio::test]
async fn nrv_credit_reduces_co_product_drain() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "NRV2", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "BPC-O-NRV2").await;
    let (bp_qty_acct, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    add_bom_by_product(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 1.0, 50, "nrv_credit").await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Parent FG: should gain (parent_std − unit_value) × qty = 5500.
    // ... but actually the co-product loop distributes v_total_drain=6000-500=5500
    // to the (single) co-product, which IS the parent SKU. So parent fg val
    // gains 5500. That's:
    //   v_total_drain (raw) = parent_std × qty_target = 6000
    //   v_byproduct_drain = unit_value × actual_qty = 50 × 10 = 500
    //   v_total_drain (after pre-pass) = 5500
    //   co-product takes 5500 (only output, allocation_pct=100)
    let parent_fg_val = balance(&pool, wo.parent_fg_val).await;
    assert_eq!(
        parent_fg_val, 5500,
        "parent fg value = (600 − 50) × 10 = 5500 (got {parent_fg_val})"
    );

    // Parent FG qty: 10 (full qty regardless of by-product).
    let parent_fg_qty = balance(&pool, wo.parent_fg_qty).await;
    assert_eq!(parent_fg_qty, 10);

    // By-product FG: qty 10, value 500.
    assert_eq!(balance(&pool, bp_qty_acct).await, 10);
    assert_eq!(balance(&pool, bp_val_acct).await, 500);

    // Parent WIP drained: qty 0, value 0.
    assert_eq!(balance(&pool, wo.parent_wip_qty).await, 0);
    assert_eq!(balance(&pool, wo.parent_wip_val).await, 0);

    assert_invariants_hold(&pool, "nrv_credit_reduces_co_product_drain").await;
}

// ============================================================
// 2. negligible adds qty only
// ============================================================

#[tokio::test]
async fn negligible_only_qty_leg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "NEG", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "BPC-O-NEG").await;
    let (bp_qty_acct, bp_val_acct) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    add_bom_by_product(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 0.5, 0, "negligible").await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Parent FG gets full v_total_drain = 6000 (negligible has no value leg).
    let parent_fg_val = balance(&pool, wo.parent_fg_val).await;
    assert_eq!(
        parent_fg_val, 6000,
        "parent fg value unchanged at 6000 since negligible has no value leg"
    );
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 10);

    // By-product: qty = ROUND(0.5 × 10) = 5; value = 0 (no value leg).
    assert_eq!(balance(&pool, bp_qty_acct).await, 5);
    assert_eq!(balance(&pool, bp_val_acct).await, 0);

    assert_invariants_hold(&pool, "negligible_only_qty_leg").await;
}

// ============================================================
// 3. multiple by-products: 2 nrv + 1 negligible
// ============================================================

#[tokio::test]
async fn multiple_by_products_mixed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "MIX", 10, 600, 60).await;

    let bp1_sku = one_sku(&pool, "BPC-O-MIX1").await;
    let bp2_sku = one_sku(&pool, "BPC-O-MIX2").await;
    let bp3_sku = one_sku(&pool, "BPC-O-MIX3").await;
    let (bp1_qty, bp1_val) = open_byproduct_accounts(&pool, &bp1_sku, &wo.fg_loc_id).await;
    let (bp2_qty, bp2_val) = open_byproduct_accounts(&pool, &bp2_sku, &wo.fg_loc_id).await;
    let (bp3_qty, bp3_val) = open_byproduct_accounts(&pool, &bp3_sku, &wo.fg_loc_id).await;

    add_bom_by_product(&pool, wo.bom_id, 1, &bp1_sku, &wo.fg_loc_id, 1.0, 50, "nrv_credit").await;
    add_bom_by_product(&pool, wo.bom_id, 2, &bp2_sku, &wo.fg_loc_id, 0.5, 30, "nrv_credit").await;
    add_bom_by_product(&pool, wo.bom_id, 3, &bp3_sku, &wo.fg_loc_id, 0.2, 0, "negligible").await;

    call_wo_start(&pool, &wo.wo_id).await;
    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // BP1: qty=10, val=500
    assert_eq!(balance(&pool, bp1_qty).await, 10);
    assert_eq!(balance(&pool, bp1_val).await, 500);
    // BP2: qty=ROUND(0.5×10)=5, val=30×5=150
    assert_eq!(balance(&pool, bp2_qty).await, 5);
    assert_eq!(balance(&pool, bp2_val).await, 150);
    // BP3 (negligible): qty=ROUND(0.2×10)=2, val=0
    assert_eq!(balance(&pool, bp3_qty).await, 2);
    assert_eq!(balance(&pool, bp3_val).await, 0);

    // Parent FG: drain = 6000 − (500 + 150) = 5350
    let parent_fg_val = balance(&pool, wo.parent_fg_val).await;
    assert_eq!(parent_fg_val, 5350);
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 10);

    // Parent WIP fully drained.
    assert_eq!(balance(&pool, wo.parent_wip_val).await, 0);
    assert_eq!(balance(&pool, wo.parent_wip_qty).await, 0);

    assert_invariants_hold(&pool, "multiple_by_products_mixed").await;
}

// ============================================================
// 4. partial completes do not fire by-products
// ============================================================

#[tokio::test]
async fn partial_complete_no_by_product_until_close() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Use parent_std = component_std so per-call drain (parent_std × p_qty)
    // matches the WIP pool fill (component_std × qty_target) exactly. With
    // mismatched costs, partial drains under-/over-shoot the pool, and the
    // pre-balance correction only fires on the closing call — but partial
    // calls before that would underflow inv_value_wip and trip accounts_check.
    let wo = scaffold_wo_with_components(&pool, "PRT", 10, 60, 60).await;

    let bp_sku = one_sku(&pool, "BPC-O-PRT").await;
    let (bp_qty, bp_val) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    // unit_value=5: by-product credit (5×10=50) leaves co-product 550.
    add_bom_by_product(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 1.0, 5, "nrv_credit").await;

    call_wo_start(&pool, &wo.wo_id).await;

    // Partial complete 1: qty=4 (not closing).
    call_wo_complete(&pool, &wo.wo_id, 4).await;
    assert_eq!(
        balance(&pool, bp_qty).await,
        0,
        "by-product qty must not fire on partial complete"
    );
    assert_eq!(balance(&pool, bp_val).await, 0);

    // Partial complete 2: qty=3 (not closing).
    call_wo_complete(&pool, &wo.wo_id, 3).await;
    assert_eq!(balance(&pool, bp_qty).await, 0);
    assert_eq!(balance(&pool, bp_val).await, 0);

    // Closing complete: qty=3 (4+3+3 = 10 = qty_target).
    call_wo_complete(&pool, &wo.wo_id, 3).await;
    assert_eq!(
        balance(&pool, bp_qty).await,
        10,
        "by-product qty fires fully on close"
    );
    assert_eq!(balance(&pool, bp_val).await, 50);

    assert_invariants_hold(&pool, "partial_complete_no_by_product_until_close").await;
}

// ============================================================
// 5. WAC parent gate: nrv_credit on WAC parent silently skips
// ============================================================
//
// Confirms the acct-nnyl deferral. The wo_by_products row exists but
// no by-product transfers fire. The WO completes correctly using the
// WAC running-avg pricing without any by-product interaction.

#[tokio::test]
async fn wac_parent_nrv_credit_silently_skipped() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Build a WAC perpetual parent. Mirror scaffold_wo_with_components
    // but flip cost_method on parent SKU + pre-seed the WIP pool with
    // a known cost for predictable running-avg.
    let parent_code = "BPC-WAC-P";
    let comp_code = "BPC-WAC-C";
    let raw_loc_code = "BPC-WAC-R";
    let fg_loc_code = "BPC-WAC-FG";

    let parent_id: String = sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'wac_perpetual') RETURNING id::text",
    )
    .bind(parent_code)
    .fetch_one(&pool)
    .await
    .expect("wac sku");
    let comp_id = one_sku(&pool, comp_code).await;
    let raw_loc = one_location(&pool, raw_loc_code).await;
    let fg_loc = one_location(&pool, fg_loc_code).await;
    set_std_cost(&pool, &comp_id, 60).await;

    let parent_wip_qty = open_account(&pool, "stock_wip", "qty", None,
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_wip_val = open_account(&pool, "inv_value_wip", "value", Some("USD"),
        Some(&parent_id), None, None, Some(10), "debit").await;
    let parent_fg_qty = open_account(&pool, "stock_available", "qty", None,
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"),
        Some(&parent_id), Some(&fg_loc), None, None, "debit").await;
    let _consumed = open_account(&pool, "stock_consumed", "qty", None,
        Some(&comp_id), None, None, None, "debit").await;
    let raw_qty = open_account(&pool, "stock_available", "qty", None,
        Some(&comp_id), Some(&raw_loc), None, None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"),
        Some(&comp_id), Some(&raw_loc), None, None, "debit").await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;

    let posted_by = fresh_uuid(&pool).await;
    let did = fresh_uuid(&pool).await;
    let mint = serde_json::json!([
        {"reason":"cycle_count_adj","document_kind":"bp_seed","document_id":did,
         "debit_account_id":raw_qty,"credit_account_id":void_qty,
         "amount":100,"qty":100,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"bp_seed","document_id":did,
         "debit_account_id":raw_val,"credit_account_id":void_val,
         "amount":6000,"qty":100,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint).execute(&pool).await.expect("seed raw");

    let bom_id = create_bom_header(&pool, parent_code).await;
    add_bom_item(&pool, bom_id, 1, 10, comp_code, raw_loc_code, 1, 100.0).await;

    let wo_id: String = sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ('BPC-WAC-WO', $1::UUID, $2::UUID, 10, 'USD', $3::UUID)
         RETURNING id::text",
    )
    .bind(&parent_id)
    .bind(&fg_loc)
    .bind(&posted_by)
    .fetch_one(&pool)
    .await
    .expect("create wac wo");
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'MILL')")
        .bind(&wo_id).execute(&pool).await.expect("routing");

    let bp_sku = one_sku(&pool, "BPC-WAC-O").await;
    let (bp_qty, bp_val) = open_byproduct_accounts(&pool, &bp_sku, &fg_loc).await;
    add_bom_by_product(&pool, bom_id, 1, &bp_sku, &fg_loc, 1.0, 50, "nrv_credit").await;

    call_wo_start(&pool, &wo_id).await;
    call_wo_complete(&pool, &wo_id, 10).await;

    // WAC parent: by-product silently skipped. wo_by_products row was
    // snapshotted at start (acct-7t4.2) but pre-pass skipped the value
    // leg per acct-nnyl gate. By-product fg accounts UNTOUCHED.
    assert_eq!(
        balance(&pool, bp_qty).await,
        0,
        "WAC parent: by-product qty leg silently skipped (acct-nnyl)"
    );
    assert_eq!(balance(&pool, bp_val).await, 0);

    // Parent FG receives full WAC drain. Pool was 100×60=6000 / 100 = 60 per
    // unit at wo_start. After wo_start the WIP pool gained component drain
    // of 10×60=600 (one rm_issue at op 10). Pool value 600, qty 10.
    // wo_complete drains 10×60 = 600 to parent fg.
    let parent_fg_val_bal = balance(&pool, parent_fg_val).await;
    assert_eq!(parent_fg_val_bal, 600);
    assert_eq!(balance(&pool, parent_fg_qty).await, 10);
    assert_eq!(balance(&pool, parent_wip_qty).await, 0);
    assert_eq!(balance(&pool, parent_wip_val).await, 0);

    assert_invariants_hold(&pool, "wac_parent_nrv_credit_silently_skipped").await;
}

// ============================================================
// 6. zero actual_qty: full unfavorable yield variance (acct-7t4.6)
// ============================================================
//
// Under Option A (mig 0101), value-leg base posts at planned_qty ×
// unit_value regardless of actual. The variance leg captures the
// (actual − planned) × unit_value delta. With actual=0, the variance
// fully reverses the base on by-product fg, leaving net bp_val=0 and
// variance_yield_byproduct accumulating the full unfavorable amount
// as debit. Parent WIP still drains by planned × unit_value (the
// committed standard-cost drain), so co-products absorb less than the
// full parent_std × qty_target.

#[tokio::test]
async fn zero_actual_qty_records_full_yield_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_with_components(&pool, "ZRO", 10, 600, 60).await;

    let bp_sku = one_sku(&pool, "BPC-O-ZRO").await;
    let (bp_qty, bp_val) = open_byproduct_accounts(&pool, &bp_sku, &wo.fg_loc_id).await;
    add_bom_by_product(&pool, wo.bom_id, 1, &bp_sku, &wo.fg_loc_id, 1.0, 50, "nrv_credit").await;

    call_wo_start(&pool, &wo.wo_id).await;

    sqlx::query(
        "UPDATE wo_by_products SET actual_qty = 0 WHERE wo_id = $1::UUID AND by_product_no = 1",
    )
    .bind(&wo.wo_id)
    .execute(&pool)
    .await
    .expect("zero actual");

    call_wo_complete(&pool, &wo.wo_id, 10).await;

    // Qty leg skipped (actual_qty=0; post_transfers requires amount>0).
    assert_eq!(balance(&pool, bp_qty).await, 0);
    // Value base posted +500 to bp_val; yield variance posted −500
    // → net bp_val = 0.
    assert_eq!(balance(&pool, bp_val).await, 0);

    // Parent fg = parent_std × qty_target − planned_drain
    //           = 6000 − (planned=10 × unit_value=50) = 5500.
    assert_eq!(balance(&pool, wo.parent_fg_val).await, 5500);
    assert_eq!(balance(&pool, wo.parent_fg_qty).await, 10);

    // variance_yield_byproduct: 500 unfavorable (debit).
    let yield_var = account_id_by_kind_currency(&pool, "variance_yield_byproduct", Some("USD")).await;
    assert_eq!(balance(&pool, yield_var).await, 500);

    assert_invariants_hold(&pool, "zero_actual_qty_records_full_yield_variance").await;
}
