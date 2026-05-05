//! `acct-ari` — post_customer_return / credit memo matrix.
//!
//! Coverage:
//!   * Happy path (restock): qty back to stock_available, value back to
//!     inv_value_fg, revenue + tax reverse against ar.
//!   * Disposition: scrap → stock_scrap (qty) + variance_scrap (value).
//!   * Disposition: repair → stock_quarantine (qty) + inv_value_fg (value).
//!   * Partial return: ship 10, return 4, then return 4 more — third
//!     return-of-3 raises P0045 (cumulative > shipped).
//!   * Tax pro-rated correctly on partial returns.
//!   * Multi-line return.
//!   * Idempotency: replay returns existing id.
//!   * Validation:
//!     - Unknown customer → P0044
//!     - Empty p_lines → P0044
//!     - Unknown ship_line → P0044
//!     - Wrong customer ownership → P0044
//!     - qty_returned <= 0 → P0044
//!     - Over-return (cumulative > shipped) → P0045

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffold (mirror of so_ship's scaffold + return-side accounts)
// ============================================================

#[allow(dead_code)]
struct ReturnScaffold {
    customer_id: String,
    so_id: String,
    sku_id: String,
    ship_loc_id: String,
    so_line_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
    cust_ar: i64,
    revenue_acct: i64,
    cogs_acct: i64,
    tax_acct: i64,
    var_scrap_acct: i64,
    stock_scrap_acct: i64,
    stock_quarantine_acct: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
}

async fn fresh_customer(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert customer {code}: {e}"))
}

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
    .unwrap_or_else(|e| panic!("insert loc {code}: {e}"))
}

async fn create_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_so: {e}"))
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
    tax_amount: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD', $7)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .bind(tax_amount)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("add_so_line: {e}"))
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
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open_account {kind}: {e}"))
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
    .unwrap_or_else(|e| panic!("set_std_cost {sku_id}: {e}"));
}

/// Standard-SKU scaffold with all return-side accounts opened
/// (stock_scrap, stock_quarantine, variance_scrap, ar). qty_ordered
/// goods, std_cost = 60, unit_price = 100, tax_amount = 15.
async fn scaffold(pool: &PgPool, suffix: &str, qty_ordered: i64) -> ReturnScaffold {
    let customer_id = fresh_customer(pool, &format!("CUST-RET-{suffix}"), "USD").await;
    let sku_id = fresh_sku(pool, &format!("SKU-RET-{suffix}"), "standard").await;
    let ship_loc_id = fresh_location(pool, &format!("RET-{suffix}")).await;

    set_std_cost(pool, &sku_id, 60).await;

    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(pool, &so_id, 1, &sku_id, &ship_loc_id, qty_ordered, 100, 15).await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None, Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;
    let cust_qty = open_account(
        pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let stock_scrap_acct = open_account(
        pool, "stock_scrap", "qty", None, Some(&sku_id), None, None, "debit",
    )
    .await;
    let stock_quarantine_acct = open_account(
        pool, "stock_quarantine", "qty", None, Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;

    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let tax_acct = account_id_by_kind_currency(pool, "sales_tax_payable", Some("USD")).await;
    let var_scrap_acct = account_id_by_kind_currency(pool, "variance_scrap", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    ReturnScaffold {
        customer_id,
        so_id,
        sku_id,
        ship_loc_id,
        so_line_id,
        qty_acct,
        val_acct,
        cust_qty,
        cust_unsettled,
        cust_ar,
        revenue_acct,
        cogs_acct,
        tax_acct,
        var_scrap_acct,
        stock_scrap_acct,
        stock_quarantine_acct,
        creation_void_qty,
        creation_void_val,
    }
}

async fn seed_fg(pool: &PgPool, s: &ReturnScaffold, qty: i64, total_value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"return_test_seed", "document_id":doc_id,
         "debit_account_id":s.qty_acct, "credit_account_id":s.creation_void_qty,
         "amount":qty, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"return_test_seed", "document_id":doc_id,
         "debit_account_id":s.val_acct, "credit_account_id":s.creation_void_val,
         "amount":total_value, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_fg: {e}"));
}

/// Ship qty units against the SO line, then invoice them so ar carries
/// a positive balance for the credit memo to credit against. Returns
/// the so_shipment_lines.id of the ship line.
async fn ship_and_invoice(pool: &PgPool, s: &ReturnScaffold, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": qty}]);
    let _ship_doc_id: String = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("post_so_ship: {e}"));

    let ship_line_id: String = sqlx::query_scalar(
        "SELECT ssl.id::text FROM so_shipment_lines ssl
          WHERE ssl.so_line_id = $1::UUID
          ORDER BY ssl.id DESC LIMIT 1",
    )
    .bind(&s.so_line_id)
    .fetch_one(pool)
    .await
    .expect("ship_line lookup");

    // Invoice the shipment so ar_unsettled clears to ar.
    let inv_lines = json!([{
        "kind": "so_match",
        "so_line_id": s.so_line_id,
        "qty": qty,
        "unit_price": 100,
        "amount": qty * 100
    }]);
    let posted_by2 = fresh_uuid(pool).await;
    let key2 = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB,
                                       '2026-04-21'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.customer_id)
    .bind(inv_lines)
    .bind(&posted_by2)
    .bind(&key2)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_customer_invoice: {e}"));

    ship_line_id
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn call_return(
    pool: &PgPool,
    customer_id: &str,
    lines: serde_json::Value,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_customer_return($1::UUID, $2::JSONB,
                                      '2026-04-25'::DATE,
                                      $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(customer_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Happy paths — three dispositions
// ============================================================

#[tokio::test]
async fn restock_disposition_full_return() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "RESTOCK", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    let qty_after_ship = balance(&pool, s.qty_acct).await;
    let val_after_ship = balance(&pool, s.val_acct).await;
    let ar_after_invoice = balance(&pool, s.cust_ar).await;
    let unsettled_after_invoice = balance(&pool, s.cust_unsettled).await;

    // Ship + invoice: qty drained, value drained, ar=1000 (revenue), and
    // ar_unsettled=15 (tax — invoice does NOT clear tax on so_match
    // lines per mig 0081 design note).
    assert_eq!(qty_after_ship, 0);
    assert_eq!(val_after_ship, 0);
    assert_eq!(ar_after_invoice, 1000);
    assert_eq!(unsettled_after_invoice, 15);

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 10,
        "disposition": "restock"
    }]);
    let _id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // Qty back to stock_available; value back to inv_value_fg.
    assert_eq!(balance(&pool, s.qty_acct).await, 10);
    assert_eq!(balance(&pool, s.val_acct).await, 600);
    // ar drained by 1000 (revenue reversal). ar_unsettled drained by 15
    // (tax reversal). Both at 0 now.
    assert_eq!(balance(&pool, s.cust_ar).await, 0);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);

    assert_invariants_hold(&pool, "restock_disposition_full_return").await;
}

#[tokio::test]
async fn scrap_disposition_full_return() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "SCRAP", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    let var_scrap_before = balance(&pool, s.var_scrap_acct).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 10,
        "disposition": "scrap"
    }]);
    call_return(&pool, &s.customer_id, lines).await.expect("return");

    // Qty went to stock_scrap (not stock_available).
    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.stock_scrap_acct).await, 10);
    // Value went to variance_scrap (write-off), not inv_value_fg.
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.var_scrap_acct).await - var_scrap_before, 600);
    // ar drained (revenue reversal); ar_unsettled drained (tax reversal).
    assert_eq!(balance(&pool, s.cust_ar).await, 0);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
}

#[tokio::test]
async fn repair_disposition_full_return() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "REPAIR", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 10,
        "disposition": "repair"
    }]);
    call_return(&pool, &s.customer_id, lines).await.expect("return");

    // Qty went to stock_quarantine (held for repair workflow).
    assert_eq!(balance(&pool, s.stock_quarantine_acct).await, 10);
    // Value went back to inv_value_fg (still ours, just held).
    assert_eq!(balance(&pool, s.val_acct).await, 600);
}

// ============================================================
// Partial returns
// ============================================================

#[tokio::test]
async fn partial_then_remainder_drains_to_zero() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "PARTIAL", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    // Return 4 of 10.
    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 4,
        "disposition": "restock"
    }]);
    call_return(&pool, &s.customer_id, lines).await.expect("return-1");

    // 4 units back; tax pro-rated 15*4/10 = 6.
    assert_eq!(balance(&pool, s.qty_acct).await, 4);
    assert_eq!(balance(&pool, s.val_acct).await, 240);
    // ar reduced by 4*100 = 400 (revenue only). Was 1000, now 600.
    assert_eq!(balance(&pool, s.cust_ar).await, 600);
    // ar_unsettled reduced by 6 (tax pro-rated). Was 15, now 9.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 9);

    // Return the remaining 6.
    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 6,
        "disposition": "restock"
    }]);
    call_return(&pool, &s.customer_id, lines).await.expect("return-2");

    assert_eq!(balance(&pool, s.qty_acct).await, 10);
    assert_eq!(balance(&pool, s.val_acct).await, 600);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);
    // tax-pro on second return = 15*6/10 = 9. 6+9 = 15 (fully reversed).
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
}

#[tokio::test]
async fn over_return_cumulative_raises_p0045() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "OVER", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    // Return 7, then try to return 4 more (cumulative 11 > shipped 10).
    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 7,
        "disposition": "restock"
    }]);
    call_return(&pool, &s.customer_id, lines).await.expect("return-1");

    let over_lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 4,
        "disposition": "restock"
    }]);
    expect_sqlstate("P0045", || async {
        call_return(&pool, &s.customer_id, over_lines.clone()).await
    })
    .await;
}

// ============================================================
// Idempotency + multi-line
// ============================================================

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "IDEMP", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 5,
        "disposition": "restock"
    }]);
    let id1: String = sqlx::query_scalar(
        "SELECT post_customer_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                      $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id)
    .bind(lines.clone())
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("return-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_customer_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                      $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("return-2 replay");

    assert_eq!(id1, id2);
    // Only one return drained (qty=5 not qty=10).
    assert_eq!(balance(&pool, s.qty_acct).await, 5);
}

// ============================================================
// Validation
// ============================================================

#[tokio::test]
async fn unknown_customer_raises_p0044() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    let bogus_ship = fresh_uuid(&pool).await;
    let lines = json!([{"ship_line_id": bogus_ship, "qty_returned": 1, "disposition": "restock"}]);
    expect_sqlstate("P0044", || async {
        call_return(&pool, &bogus, lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn empty_lines_raises_p0044() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "EMPTY", 10).await;
    expect_sqlstate("P0044", || async {
        call_return(&pool, &s.customer_id, json!([])).await
    })
    .await;
}

#[tokio::test]
async fn unknown_ship_line_raises_p0044() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "BAD-SHIP", 10).await;
    let bogus_ship = fresh_uuid(&pool).await;
    let lines = json!([{"ship_line_id": bogus_ship, "qty_returned": 1, "disposition": "restock"}]);
    expect_sqlstate("P0044", || async {
        call_return(&pool, &s.customer_id, lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn wrong_customer_ownership_raises_p0044() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "WRONG-OWN", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    // A second customer that did not place this SO.
    let other_cust = fresh_customer(&pool, "CUST-OTHER", "USD").await;
    let lines = json!([{"ship_line_id": ship_line, "qty_returned": 1, "disposition": "restock"}]);
    expect_sqlstate("P0044", || async {
        call_return(&pool, &other_cust, lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn qty_returned_zero_raises_p0044() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "ZERO", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    let lines = json!([{"ship_line_id": ship_line, "qty_returned": 0, "disposition": "restock"}]);
    expect_sqlstate("P0044", || async {
        call_return(&pool, &s.customer_id, lines.clone()).await
    })
    .await;
}

// ============================================================
// State-aware routing (acct-tk7)
// ============================================================

/// Ship `qty` units against the SO line WITHOUT invoicing them.
async fn ship_no_invoice(pool: &PgPool, s: &ReturnScaffold, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": qty}]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("post_so_ship: {e}"));

    sqlx::query_scalar(
        "SELECT ssl.id::text FROM so_shipment_lines ssl
          WHERE ssl.so_line_id = $1::UUID
          ORDER BY ssl.id DESC LIMIT 1",
    )
    .bind(&s.so_line_id)
    .fetch_one(pool)
    .await
    .expect("ship_line lookup")
}

async fn invoice_qty(pool: &PgPool, s: &ReturnScaffold, qty: i64) {
    let inv_lines = json!([{
        "kind": "so_match",
        "so_line_id": s.so_line_id,
        "qty": qty,
        "unit_price": 100,
        "amount": qty * 100
    }]);
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB,
                                       '2026-04-21'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.customer_id)
    .bind(inv_lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_customer_invoice: {e}"));
}

async fn ar_split_columns(pool: &PgPool, return_id: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT
           COALESCE(SUM(qty_to_ar_unsettled), 0)::BIGINT,
           COALESCE(SUM(qty_to_ar), 0)::BIGINT
         FROM customer_return_lines WHERE return_id = $1::UUID",
    )
    .bind(return_id)
    .fetch_one(pool)
    .await
    .expect("ar_split_columns")
}

#[tokio::test]
async fn pre_invoice_return_routes_to_ar_unsettled() {
    // Ship 10 (no invoice). Return 10 → revenue reverses against ar_unsettled
    // (where it currently sits), not ar (which is 0).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "PRE-INV", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;

    // After ship: ar_unsettled = 1000 (revenue) + 15 (tax) = 1015. ar = 0.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 1015);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 10,
        "disposition": "restock"
    }]);
    let return_id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // ar_unsettled drained to 0 (1000 revenue + 15 tax both reverse against it).
    // ar untouched.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);
    // Inventory back.
    assert_eq!(balance(&pool, s.qty_acct).await, 10);
    assert_eq!(balance(&pool, s.val_acct).await, 600);

    let (to_us, to_ar) = ar_split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 10);
    assert_eq!(to_ar, 0);

    assert_invariants_hold(&pool, "pre_invoice_return_routes_to_ar_unsettled").await;
}

#[tokio::test]
async fn partial_invoice_then_return_splits_routing() {
    // Ship 10, invoice 6. Return 7.
    // Routing: 4 to ar_unsettled (un-invoiced), 3 to ar (invoiced).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "AR-SPLIT", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;
    invoice_qty(&pool, &s, 6).await;

    // After ship+partial-invoice: ar = 600 (6*100); ar_unsettled = 415
    // (4*100 unrevenued + 15 tax remaining).
    assert_eq!(balance(&pool, s.cust_ar).await, 600);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 415);

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 7,
        "disposition": "restock"
    }]);
    let return_id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // Routing: 4 units route to ar_unsettled (revenue 400), 3 units route
    // to ar (revenue 300). Tax pro-rated 7/10 of 15 = 10 (integer trunc),
    // routes to ar_unsettled.
    //
    // ar_unsettled: 415 - 400 (revenue reversal) - 10 (tax reversal) = 5.
    // ar: 600 - 300 = 300.
    assert_eq!(balance(&pool, s.cust_ar).await, 300);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 5);

    let (to_us, to_ar) = ar_split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 4);
    assert_eq!(to_ar, 3);
}

#[tokio::test]
async fn cumulative_ar_across_multiple_returns() {
    // Ship 10 (no invoice). Return 4 (all to unsettled). Invoice 6. Return 4.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "AR-CUM", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;

    let lines1 = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 4,
        "disposition": "restock"
    }]);
    let r1 = call_return(&pool, &s.customer_id, lines1).await.expect("return-1");
    let (a1, b1) = ar_split_columns(&pool, &r1).await;
    assert_eq!(a1, 4);
    assert_eq!(b1, 0);

    // After return 1: ar_unsettled drained by 4*100 + tax_pro = 400 + 6 = 406.
    // So ar_unsettled = 1015 - 406 = 609.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 609);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);

    // Invoice 6: avail = 10 - 0 - 4 = 6. Allowed.
    invoice_qty(&pool, &s, 6).await;
    // ar_unsettled drained by 600 (revenue clear). ar gets 600.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 9);  // 609 - 600 = 9 (residual tax)
    assert_eq!(balance(&pool, s.cust_ar).await, 600);

    // Return 4 more (all on ar side; unsettled remainder = 10-6-4=0).
    let lines2 = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 4,
        "disposition": "restock"
    }]);
    let r2 = call_return(&pool, &s.customer_id, lines2).await.expect("return-2");
    let (a2, b2) = ar_split_columns(&pool, &r2).await;
    assert_eq!(a2, 0);
    assert_eq!(b2, 4);

    // ar drains by 400 → 200. Tax pro-rated 4/10 of 15 = 6, drains ar_unsettled.
    assert_eq!(balance(&pool, s.cust_ar).await, 200);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 3);
}

#[tokio::test]
async fn over_invoice_after_return_to_unsettled_rejected() {
    // Ship 10, return 3 to ar_unsettled (no invoice yet). Invoice 10 should fail.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "OVER-INV", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 3,
        "disposition": "restock"
    }]);
    call_return(&pool, &s.customer_id, lines).await.expect("return");

    // Try to invoice 10. avail = 10 - 0 - 3 = 7. Should fail with P0040.
    let inv_lines = json!([{
        "kind": "so_match",
        "so_line_id": s.so_line_id,
        "qty": 10,
        "unit_price": 100,
        "amount": 1000
    }]);
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0040", || async {
        sqlx::query(
            "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB,
                                           '2026-04-21'::DATE, $3::UUID, $4::UUID, NULL)",
        )
        .bind(&s.customer_id)
        .bind(inv_lines.clone())
        .bind(&posted_by)
        .bind(&key)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn override_closed_period_allows_back_post() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "AR-OVERRIDE", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    sqlx::query(
        "UPDATE periods SET closed_at = clock_timestamp()
          WHERE opens_at <= '2026-04-25'::DATE AND closes_at >= '2026-04-25'::DATE",
    )
    .execute(&pool)
    .await
    .expect("close period");

    let posted_by = fresh_uuid(&pool).await;
    let key1 = fresh_uuid(&pool).await;
    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 5,
        "disposition": "restock"
    }]);

    // Without override → P0005.
    expect_sqlstate("P0005", || async {
        sqlx::query_scalar::<_, String>(
            "SELECT post_customer_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                          $3::UUID, $4::UUID, NULL, FALSE)::text",
        )
        .bind(&s.customer_id)
        .bind(lines.clone())
        .bind(&posted_by)
        .bind(&key1)
        .fetch_one(&pool)
        .await
    })
    .await;

    // With override → succeeds.
    let key2 = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_customer_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                      $3::UUID, $4::UUID, NULL, TRUE)::text",
    )
    .bind(&s.customer_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key2)
    .fetch_one(&pool)
    .await
    .expect("override return");

    assert_eq!(balance(&pool, s.qty_acct).await, 5);
}

// ============================================================
// Disposition × pre-invoice / partial-invoice combinations (acct-hik)
// ============================================================

#[tokio::test]
async fn pre_invoice_return_disposition_scrap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "PRE-INV-SCRAP", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;

    assert_eq!(balance(&pool, s.cust_unsettled).await, 1015);
    let var_scrap_before = balance(&pool, s.var_scrap_acct).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 10,
        "disposition": "scrap"
    }]);
    let return_id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // qty: stock_scrap; value: variance_scrap; revenue: ar_unsettled (pre-invoice).
    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.stock_scrap_acct).await, 10);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.var_scrap_acct).await - var_scrap_before, 600);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);

    let (to_us, to_ar) = ar_split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 10);
    assert_eq!(to_ar, 0);

    assert_invariants_hold(&pool, "pre_invoice_return_disposition_scrap").await;
}

#[tokio::test]
async fn pre_invoice_return_disposition_repair() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "PRE-INV-REPAIR", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 10,
        "disposition": "repair"
    }]);
    let return_id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // qty: stock_quarantine; value: inv_value_fg; revenue: ar_unsettled.
    assert_eq!(balance(&pool, s.stock_quarantine_acct).await, 10);
    assert_eq!(balance(&pool, s.val_acct).await, 600);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);

    let (to_us, to_ar) = ar_split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 10);
    assert_eq!(to_ar, 0);
}

#[tokio::test]
async fn partial_invoice_split_disposition_scrap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "SPLIT-SCRAP", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;
    invoice_qty(&pool, &s, 6).await;

    assert_eq!(balance(&pool, s.cust_ar).await, 600);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 415);
    let var_scrap_before = balance(&pool, s.var_scrap_acct).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 7,
        "disposition": "scrap"
    }]);
    let return_id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // 4 → ar_unsettled (un-invoiced); 3 → ar (invoiced). Tax 7/10 of 15 = 10
    // pro-rated, all to ar_unsettled. Cogs reversal goes to variance_scrap.
    // Inventory qty: stock_scrap +7, stock_available untouched.
    // Inv value untouched (still drained from ship); variance_scrap += 7*60=420.
    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.stock_scrap_acct).await, 7);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.var_scrap_acct).await - var_scrap_before, 7 * 60);
    assert_eq!(balance(&pool, s.cust_ar).await, 300);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 5);

    let (to_us, to_ar) = ar_split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 4);
    assert_eq!(to_ar, 3);
}

#[tokio::test]
async fn partial_invoice_split_disposition_repair() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "SPLIT-REPAIR", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_no_invoice(&pool, &s, 10).await;
    invoice_qty(&pool, &s, 6).await;

    let lines = json!([{
        "ship_line_id": ship_line,
        "qty_returned": 7,
        "disposition": "repair"
    }]);
    let return_id = call_return(&pool, &s.customer_id, lines).await.expect("return");

    // qty → stock_quarantine; value → inv_value_fg; revenue split as before.
    assert_eq!(balance(&pool, s.stock_quarantine_acct).await, 7);
    assert_eq!(balance(&pool, s.val_acct).await, 7 * 60);
    assert_eq!(balance(&pool, s.cust_ar).await, 300);
    assert_eq!(balance(&pool, s.cust_unsettled).await, 5);

    let (to_us, to_ar) = ar_split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 4);
    assert_eq!(to_ar, 3);
}

// ============================================================
// Multi-line return doc spanning split states (acct-7nv, AR side)
// ============================================================

#[tokio::test]
async fn ar_multi_line_return_routes_each_line_independently() {
    // Customer has two so_lines (different SKUs / ship_locs). Ship both,
    // invoice only line A. Return both in one doc → line A drains ar;
    // line B drains ar_unsettled.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let customer_id = fresh_customer(&pool, "CUST-AR-MULTI", "USD").await;
    let sku_a = fresh_sku(&pool, "SKU-AR-A", "standard").await;
    let sku_b = fresh_sku(&pool, "SKU-AR-B", "standard").await;
    let loc_a = fresh_location(&pool, "AR-A").await;
    let loc_b = fresh_location(&pool, "AR-B").await;

    set_std_cost(&pool, &sku_a, 60).await;
    set_std_cost(&pool, &sku_b, 80).await;

    let so_id = create_so(&pool, &customer_id).await;
    let line_a = add_so_line(&pool, &so_id, 1, &sku_a, &loc_a, 10, 100, 0).await;
    let line_b = add_so_line(&pool, &so_id, 2, &sku_b, &loc_b, 10, 120, 0).await;

    // Open accounts for both lines.
    let qty_a = open_account(
        &pool, "stock_available", "qty", None, Some(&sku_a), Some(&loc_a), None, "debit",
    )
    .await;
    let val_a = open_account(
        &pool, "inv_value_fg", "value", Some("USD"), Some(&sku_a), Some(&loc_a), None, "debit",
    )
    .await;
    let qty_b = open_account(
        &pool, "stock_available", "qty", None, Some(&sku_b), Some(&loc_b), None, "debit",
    )
    .await;
    let val_b = open_account(
        &pool, "inv_value_fg", "value", Some("USD"), Some(&sku_b), Some(&loc_b), None, "debit",
    )
    .await;
    let _cust_qty = open_account(
        &pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        &pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_ar = open_account(
        &pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;

    // Seed FG inventory for both SKUs.
    let creation_void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;
    let posted_by = fresh_uuid(&pool).await;
    let doc_id = fresh_uuid(&pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":qty_a,"credit_account_id":creation_void_qty,
         "amount":10,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":val_a,"credit_account_id":creation_void_val,
         "amount":600,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":qty_b,"credit_account_id":creation_void_qty,
         "amount":10,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":val_b,"credit_account_id":creation_void_val,
         "amount":800,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by}
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(&pool)
        .await
        .expect("seed");

    // Ship both lines.
    let posted_by2 = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let ship_lines = json!([
        {"so_line_id": line_a, "qty_shipped": 10},
        {"so_line_id": line_b, "qty_shipped": 10}
    ]);
    sqlx::query(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&so_id)
    .bind(ship_lines)
    .bind(&posted_by2)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("ship both");

    let ship_a: String = sqlx::query_scalar(
        "SELECT id::text FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&line_a)
    .fetch_one(&pool)
    .await
    .expect("ship_a");
    let ship_b: String = sqlx::query_scalar(
        "SELECT id::text FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&line_b)
    .fetch_one(&pool)
    .await
    .expect("ship_b");

    // Invoice ONLY line A.
    let inv_lines = json!([{
        "kind": "so_match", "so_line_id": line_a,
        "qty": 10, "unit_price": 100, "amount": 1000
    }]);
    let posted_by3 = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB,
                                       '2026-04-21'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&customer_id)
    .bind(inv_lines)
    .bind(&posted_by3)
    .bind(&key2)
    .execute(&pool)
    .await
    .expect("invoice A");

    // Pre-state: ar=1000 (line A invoiced); ar_unsettled = 0 (A) + 1200 (B) = 1200.
    assert_eq!(balance(&pool, cust_ar).await, 1000);
    assert_eq!(balance(&pool, cust_unsettled).await, 1200);

    // Return both lines in ONE doc.
    let return_lines = json!([
        {"ship_line_id": ship_a, "qty_returned": 10, "disposition": "restock"},
        {"ship_line_id": ship_b, "qty_returned": 10, "disposition": "restock"}
    ]);
    let return_id = call_return(&pool, &customer_id, return_lines).await.expect("multi-line ar return");

    // ar drained (line A invoiced); ar_unsettled drained (line B not invoiced).
    assert_eq!(balance(&pool, cust_ar).await, 0);
    assert_eq!(balance(&pool, cust_unsettled).await, 0);

    // Per-line splits.
    let split_a: (i64, i64) = sqlx::query_as(
        "SELECT qty_to_ar_unsettled::BIGINT, qty_to_ar::BIGINT
         FROM customer_return_lines WHERE return_id = $1::UUID AND ship_line_id = $2::UUID",
    )
    .bind(&return_id)
    .bind(&ship_a)
    .fetch_one(&pool)
    .await
    .expect("split_a");
    assert_eq!(split_a, (0, 10));

    let split_b: (i64, i64) = sqlx::query_as(
        "SELECT qty_to_ar_unsettled::BIGINT, qty_to_ar::BIGINT
         FROM customer_return_lines WHERE return_id = $1::UUID AND ship_line_id = $2::UUID",
    )
    .bind(&return_id)
    .bind(&ship_b)
    .fetch_one(&pool)
    .await
    .expect("split_b");
    assert_eq!(split_b, (10, 0));

    assert_invariants_hold(&pool, "ar_multi_line_return_routes_each_line_independently").await;
}

// ============================================================
// Multi-currency state-aware routing on AR side (acct-bh0)
// ============================================================

#[tokio::test]
async fn ar_multi_currency_split_routes_per_currency_partition() {
    // Customer with two so_lines: USD and EUR. Ship both. Invoice USD
    // only. Return both — USD line drains ar (USD); EUR drains
    // ar_unsettled (EUR).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let customer_id = fresh_customer(&pool, "CUST-AR-FX", "USD").await;
    let sku_usd = fresh_sku(&pool, "SKU-AR-USD", "standard").await;
    let sku_eur = fresh_sku(&pool, "SKU-AR-EUR", "standard").await;
    let loc_usd = fresh_location(&pool, "AR-FX-USD").await;
    let loc_eur = fresh_location(&pool, "AR-FX-EUR").await;

    set_std_cost(&pool, &sku_usd, 60).await;
    set_std_cost(&pool, &sku_eur, 80).await;

    let so_id = create_so(&pool, &customer_id).await;
    // sales_order_lines.currency is per-line.
    let line_usd: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD', 0)
         RETURNING id::text",
    )
    .bind(&so_id)
    .bind(&sku_usd)
    .bind(&loc_usd)
    .fetch_one(&pool)
    .await
    .expect("line_usd");
    let line_eur: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, 2, $2::UUID, $3::UUID, 10, 120, 'EUR', 0)
         RETURNING id::text",
    )
    .bind(&so_id)
    .bind(&sku_eur)
    .bind(&loc_eur)
    .fetch_one(&pool)
    .await
    .expect("line_eur");

    let qty_usd = open_account(
        &pool, "stock_available", "qty", None, Some(&sku_usd), Some(&loc_usd), None, "debit",
    )
    .await;
    let val_usd = open_account(
        &pool, "inv_value_fg", "value", Some("USD"), Some(&sku_usd), Some(&loc_usd), None, "debit",
    )
    .await;
    let qty_eur = open_account(
        &pool, "stock_available", "qty", None, Some(&sku_eur), Some(&loc_eur), None, "debit",
    )
    .await;
    let val_eur = open_account(
        &pool, "inv_value_fg", "value", Some("EUR"), Some(&sku_eur), Some(&loc_eur), None, "debit",
    )
    .await;
    let _cust_qty = open_account(
        &pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let unsettled_usd = open_account(
        &pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let unsettled_eur = open_account(
        &pool, "ar_unsettled", "value", Some("EUR"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let ar_usd = open_account(
        &pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let ar_eur = open_account(
        &pool, "ar", "value", Some("EUR"), None, None, Some(&customer_id), "debit",
    )
    .await;
    // EUR cogs (revenue EUR is seeded; don't duplicate it).
    let _cogs_eur = open_account(
        &pool, "cogs", "value", Some("EUR"), None, None, None, "debit",
    )
    .await;

    // Seed FG for both SKUs.
    let creation_void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let creation_void_usd = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;
    let creation_void_eur = open_account(
        &pool, "creation_void", "value", Some("EUR"), None, None, None, "unrestricted",
    )
    .await;
    let posted_by = fresh_uuid(&pool).await;
    let doc_id = fresh_uuid(&pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":qty_usd,"credit_account_id":creation_void_qty,
         "amount":10,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":val_usd,"credit_account_id":creation_void_usd,
         "amount":600,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":qty_eur,"credit_account_id":creation_void_qty,
         "amount":10,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":val_eur,"credit_account_id":creation_void_eur,
         "amount":800,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by}
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(&pool)
        .await
        .expect("seed multi-ccy");

    // Ship both.
    let posted_by2 = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let ship_lines = json!([
        {"so_line_id": line_usd, "qty_shipped": 10},
        {"so_line_id": line_eur, "qty_shipped": 10}
    ]);
    sqlx::query(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&so_id)
    .bind(ship_lines)
    .bind(&posted_by2)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("ship both ccy");

    let ship_usd: String = sqlx::query_scalar(
        "SELECT id::text FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&line_usd)
    .fetch_one(&pool)
    .await
    .expect("ship_usd");
    let ship_eur: String = sqlx::query_scalar(
        "SELECT id::text FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&line_eur)
    .fetch_one(&pool)
    .await
    .expect("ship_eur");

    // Invoice USD only.
    let inv_lines = json!([{
        "kind": "so_match", "so_line_id": line_usd,
        "qty": 10, "unit_price": 100, "amount": 1000
    }]);
    let posted_by3 = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB,
                                       '2026-04-21'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&customer_id)
    .bind(inv_lines)
    .bind(&posted_by3)
    .bind(&key2)
    .execute(&pool)
    .await
    .expect("invoice USD");

    // Pre-state: USD ar=1000, USD ar_unsettled=0; EUR ar=0, EUR ar_unsettled=1200.
    assert_eq!(balance(&pool, ar_usd).await, 1000);
    assert_eq!(balance(&pool, unsettled_usd).await, 0);
    assert_eq!(balance(&pool, ar_eur).await, 0);
    assert_eq!(balance(&pool, unsettled_eur).await, 1200);

    // Return both — single document.
    let return_lines = json!([
        {"ship_line_id": ship_usd, "qty_returned": 10, "disposition": "restock"},
        {"ship_line_id": ship_eur, "qty_returned": 10, "disposition": "restock"}
    ]);
    call_return(&pool, &customer_id, return_lines).await.expect("multi-ccy return");

    assert_eq!(balance(&pool, ar_usd).await, 0);
    assert_eq!(balance(&pool, unsettled_usd).await, 0);
    assert_eq!(balance(&pool, ar_eur).await, 0);
    assert_eq!(balance(&pool, unsettled_eur).await, 0);

    assert_invariants_hold(&pool, "ar_multi_currency_split_routes_per_currency_partition").await;
}

// ============================================================
// cost_method snapshot at ship time (acct-6d8, AR symmetry)
// ============================================================

#[tokio::test]
async fn so_shipment_lines_persists_cost_method_at_ship() {
    // Sanity: post_so_ship populates cost_method_at_ship from the SKU's
    // cost_method at ship time. AR-side symmetry with mig 0087.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "PERSIST-SHIP", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let ship_line = ship_and_invoice(&pool, &s, 10).await;

    let snapshot: String = sqlx::query_scalar(
        "SELECT cost_method_at_ship::text FROM so_shipment_lines WHERE id = $1::UUID",
    )
    .bind(&ship_line)
    .fetch_one(&pool)
    .await
    .expect("snapshot read");
    assert_eq!(snapshot, "standard");
}
