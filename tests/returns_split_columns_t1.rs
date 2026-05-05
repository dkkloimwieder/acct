//! `acct-3lv` — T1 probe for the qty-split CHECK constraints added in
//! mig 0086 to po_return_lines and customer_return_lines.
//!
//! Verifies that direct INSERTs (bypassing post_po_return /
//! post_customer_return) violating
//!   `qty_to_*_unsettled >= 0 AND qty_to_* >= 0 AND
//!    qty_to_*_unsettled + qty_to_* = qty_returned`
//! raise SQLSTATE 23514.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ------------------------------------------------------------
// AP side (po_return_lines_split_check)
// ------------------------------------------------------------

async fn make_recv_line(pool: &PgPool) -> (String, String) {
    let sku = sqlx::query_scalar::<_, String>(
        "SELECT id::text FROM skus WHERE code = 'SKU-A'",
    )
    .fetch_one(pool)
    .await
    .expect("SKU-A");
    let loc = sqlx::query_scalar::<_, String>(
        "SELECT id::text FROM locations WHERE code = 'MAIN'",
    )
    .fetch_one(pool)
    .await
    .expect("MAIN");

    let vendor: String = sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ('VEN-T1-AP', 'V-T1', 'USD') RETURNING id::text",
    )
    .fetch_one(pool)
    .await
    .expect("vendor");
    let po: String = sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .expect("po");
    let po_line: String = sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD') RETURNING id::text",
    )
    .bind(&po)
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .expect("po_line");

    // Open vendor-side AP staging account so post_po_receipt can post.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('vendor_pool', 'qty', NULL, $1::UUID, 'credit')",
    )
    .bind(&vendor)
    .execute(pool)
    .await
    .expect("vendor_pool");
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit')",
    )
    .bind(&vendor)
    .execute(pool)
    .await
    .expect("ap_unsettled");

    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"po_line_id": po_line, "qty_received": 10}]);
    sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)",
    )
    .bind(&po)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .expect("receipt");

    let recv_line: String = sqlx::query_scalar(
        "SELECT id::text FROM po_receipt_lines WHERE po_line_id = $1::UUID",
    )
    .bind(&po_line)
    .fetch_one(pool)
    .await
    .expect("recv_line");

    // Insert a po_returns header directly (raw, not via post_po_return).
    let return_id: String = sqlx::query_scalar(
        "INSERT INTO po_returns (vendor_id, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, '2026-04-25', $2::UUID, $3::UUID) RETURNING id::text",
    )
    .bind(&vendor)
    .bind(fresh_uuid(pool).await)
    .bind(fresh_uuid(pool).await)
    .fetch_one(pool)
    .await
    .expect("po_returns");

    (return_id, recv_line)
}

#[tokio::test]
async fn po_return_split_sum_too_high_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, recv_line) = make_recv_line(&pool).await;

    // qty_returned=5, but split sums to 6 → CHECK fails.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO po_return_lines
                (return_id, line_no, recv_line_id, qty_returned, unit_cost,
                 qty_to_ap_unsettled, qty_to_ap)
             VALUES ($1::UUID, 1, $2::UUID, 5, 100, 4, 2)",
        )
        .bind(&return_id)
        .bind(&recv_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn po_return_split_sum_too_low_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, recv_line) = make_recv_line(&pool).await;

    // qty_returned=5, split sums to 4 → CHECK fails.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO po_return_lines
                (return_id, line_no, recv_line_id, qty_returned, unit_cost,
                 qty_to_ap_unsettled, qty_to_ap)
             VALUES ($1::UUID, 1, $2::UUID, 5, 100, 2, 2)",
        )
        .bind(&return_id)
        .bind(&recv_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn po_return_split_negative_unsettled_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, recv_line) = make_recv_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO po_return_lines
                (return_id, line_no, recv_line_id, qty_returned, unit_cost,
                 qty_to_ap_unsettled, qty_to_ap)
             VALUES ($1::UUID, 1, $2::UUID, 5, 100, -1, 6)",
        )
        .bind(&return_id)
        .bind(&recv_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn po_return_split_negative_ap_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, recv_line) = make_recv_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO po_return_lines
                (return_id, line_no, recv_line_id, qty_returned, unit_cost,
                 qty_to_ap_unsettled, qty_to_ap)
             VALUES ($1::UUID, 1, $2::UUID, 5, 100, 6, -1)",
        )
        .bind(&return_id)
        .bind(&recv_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn po_return_split_valid_zero_zero_violates_qty_returned() {
    // qty_returned=5 with both splits 0 → 0 != 5 → CHECK fails.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, recv_line) = make_recv_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO po_return_lines
                (return_id, line_no, recv_line_id, qty_returned, unit_cost,
                 qty_to_ap_unsettled, qty_to_ap)
             VALUES ($1::UUID, 1, $2::UUID, 5, 100, 0, 0)",
        )
        .bind(&return_id)
        .bind(&recv_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn po_return_split_valid_full_unsettled_succeeds() {
    // Sanity: split that does sum to qty_returned succeeds.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, recv_line) = make_recv_line(&pool).await;

    sqlx::query(
        "INSERT INTO po_return_lines
            (return_id, line_no, recv_line_id, qty_returned, unit_cost,
             qty_to_ap_unsettled, qty_to_ap)
         VALUES ($1::UUID, 1, $2::UUID, 5, 100, 5, 0)",
    )
    .bind(&return_id)
    .bind(&recv_line)
    .execute(&pool)
    .await
    .expect("valid insert succeeds");
}

// ------------------------------------------------------------
// AR side (customer_return_lines_split_check)
// ------------------------------------------------------------

async fn make_ship_line(pool: &PgPool) -> (String, String) {
    let sku = sqlx::query_scalar::<_, String>(
        "SELECT id::text FROM skus WHERE code = 'SKU-A'",
    )
    .fetch_one(pool)
    .await
    .expect("SKU-A");
    let loc = sqlx::query_scalar::<_, String>(
        "SELECT id::text FROM locations WHERE code = 'MAIN'",
    )
    .fetch_one(pool)
    .await
    .expect("MAIN");

    let customer: String = sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ('CUST-T1-AR', 'C-T1', 'USD') RETURNING id::text",
    )
    .fetch_one(pool)
    .await
    .expect("customer");

    let so: String = sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&customer)
    .fetch_one(pool)
    .await
    .expect("so");

    let so_line: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD', 0) RETURNING id::text",
    )
    .bind(&so)
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .expect("so_line");

    // Per-customer accounts for ship.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('customer_pool', 'qty', NULL, $1::UUID, 'debit')",
    )
    .bind(&customer)
    .execute(pool)
    .await
    .expect("customer_pool");
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ar_unsettled', 'value', 'USD', $1::UUID, 'debit')",
    )
    .bind(&customer)
    .execute(pool)
    .await
    .expect("ar_unsettled");

    // Seed inv_value_fg for SKU-A so ship can drain it (need stock + value
    // since SKU-A is standard — uses standard cost = 100).
    let qty_acct = account_id_stock_available(pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":qty_acct,"credit_account_id":creation_void_qty,
         "amount":10,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":val_acct,"credit_account_id":creation_void_val,
         "amount":1000,"qty":10,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by}
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .expect("seed FG");

    // Ship via post_so_ship.
    let posted_by2 = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"so_line_id": so_line, "qty_shipped": 10}]);
    sqlx::query(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&so)
    .bind(lines)
    .bind(&posted_by2)
    .bind(&key)
    .execute(pool)
    .await
    .expect("ship");

    let ship_line: String = sqlx::query_scalar(
        "SELECT id::text FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&so_line)
    .fetch_one(pool)
    .await
    .expect("ship_line");

    let return_id: String = sqlx::query_scalar(
        "INSERT INTO customer_returns (customer_id, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, '2026-04-25', $2::UUID, $3::UUID) RETURNING id::text",
    )
    .bind(&customer)
    .bind(fresh_uuid(pool).await)
    .bind(fresh_uuid(pool).await)
    .fetch_one(pool)
    .await
    .expect("customer_returns");

    (return_id, ship_line)
}

#[tokio::test]
async fn customer_return_split_sum_too_high_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, ship_line) = make_ship_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO customer_return_lines
                (return_id, line_no, ship_line_id, qty_returned, disposition,
                 unit_cost, unit_price, tax_amount,
                 qty_to_ar_unsettled, qty_to_ar)
             VALUES ($1::UUID, 1, $2::UUID, 5, 'restock', 60, 100, 0, 4, 2)",
        )
        .bind(&return_id)
        .bind(&ship_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn customer_return_split_sum_too_low_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, ship_line) = make_ship_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO customer_return_lines
                (return_id, line_no, ship_line_id, qty_returned, disposition,
                 unit_cost, unit_price, tax_amount,
                 qty_to_ar_unsettled, qty_to_ar)
             VALUES ($1::UUID, 1, $2::UUID, 5, 'restock', 60, 100, 0, 2, 2)",
        )
        .bind(&return_id)
        .bind(&ship_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn customer_return_split_negative_unsettled_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, ship_line) = make_ship_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO customer_return_lines
                (return_id, line_no, ship_line_id, qty_returned, disposition,
                 unit_cost, unit_price, tax_amount,
                 qty_to_ar_unsettled, qty_to_ar)
             VALUES ($1::UUID, 1, $2::UUID, 5, 'restock', 60, 100, 0, -1, 6)",
        )
        .bind(&return_id)
        .bind(&ship_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn customer_return_split_negative_ar_raises_23514() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (return_id, ship_line) = make_ship_line(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO customer_return_lines
                (return_id, line_no, ship_line_id, qty_returned, disposition,
                 unit_cost, unit_price, tax_amount,
                 qty_to_ar_unsettled, qty_to_ar)
             VALUES ($1::UUID, 1, $2::UUID, 5, 'restock', 60, 100, 0, 6, -1)",
        )
        .bind(&return_id)
        .bind(&ship_line)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}
