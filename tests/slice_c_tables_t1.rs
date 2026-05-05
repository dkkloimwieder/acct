//! `acct-th7` / Slice C T1 — schema invariant probes for the outflow-
//! cycle tables (customers, sales_order_lines, so_shipments,
//! so_shipment_lines, customer_invoices, customer_invoice_lines,
//! ar_payments).
//!
//! Pins table-level CHECK / FK / UNIQUE constraints so a schema
//! regression surfaces here as a focused failure rather than a
//! cascading matrix-test panic.

mod common;

use common::*;
use sqlx::PgPool;

async fn one_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .expect("insert customer")
}

async fn one_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .expect("insert sku")
}

async fn one_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .expect("insert loc")
}

async fn one_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .expect("insert so")
}

// ============================================================
// customers
// ============================================================

#[tokio::test]
async fn customers_code_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_customer(&pool, "T1-CUST").await;
    let dup: Result<String, _> = sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ('T1-CUST', 'dup', 'USD') RETURNING id::text",
    )
    .fetch_one(&pool)
    .await;
    assert!(dup.is_err(), "duplicate customer code must fail");
}

#[tokio::test]
async fn customers_default_currency_required() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO customers (code, name) VALUES ('NO-CCY', 'x')",
    )
    .fetch_one(&pool)
    .await;
    assert!(r.is_err(), "default_currency NOT NULL must reject");
}

// ============================================================
// sales_orders.customer_id FK
// ============================================================

#[tokio::test]
async fn so_customer_fk_enforced() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    let r: Result<String, _> = sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status) VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&bogus)
    .fetch_one(&pool)
    .await;
    assert!(r.is_err(), "FK to nonexistent customer must reject");
}

#[tokio::test]
async fn so_customer_id_nullable_for_stub() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Stub backwards-compat: NULL customer_id allowed.
    let id: String = sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status) VALUES (NULL, 'open') RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .expect("NULL customer_id should be allowed");
    assert!(!id.is_empty());
}

// ============================================================
// sales_order_lines
// ============================================================

#[tokio::test]
async fn so_lines_unique_per_so_line_no() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-SOL").await;
    let sku = one_sku(&pool, "T1-SOL-SKU").await;
    let loc = one_location(&pool, "T1-SOL-LOC").await;
    let so = one_so(&pool, &cust).await;

    sqlx::query(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD')",
    )
    .bind(&so)
    .bind(&sku)
    .bind(&loc)
    .execute(&pool)
    .await
    .expect("insert line 1");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 5, 100, 'USD')",
    )
    .bind(&so)
    .bind(&sku)
    .bind(&loc)
    .fetch_one(&pool)
    .await;
    assert!(r.is_err(), "duplicate (so_id, line_no) must reject");
}

#[tokio::test]
async fn so_lines_qty_ordered_positive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-QTY").await;
    let sku = one_sku(&pool, "T1-QTY-SKU").await;
    let loc = one_location(&pool, "T1-QTY-LOC").await;
    let so = one_so(&pool, &cust).await;

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 0, 100, 'USD')",
    )
    .bind(&so).bind(&sku).bind(&loc)
    .fetch_one(&pool).await;
    assert!(r.is_err(), "qty_ordered must be > 0");
}

#[tokio::test]
async fn so_lines_unit_price_nonneg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-UP").await;
    let sku = one_sku(&pool, "T1-UP-SKU").await;
    let loc = one_location(&pool, "T1-UP-LOC").await;
    let so = one_so(&pool, &cust).await;

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 1, -1, 'USD')",
    )
    .bind(&so).bind(&sku).bind(&loc)
    .fetch_one(&pool).await;
    assert!(r.is_err(), "unit_price must be >= 0");
}

// ============================================================
// so_shipments + so_shipment_lines
// ============================================================

#[tokio::test]
async fn shipments_idempotency_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-SHP-IDEMP").await;
    let so = one_so(&pool, &cust).await;
    let key = fresh_uuid(&pool).await;
    let posted_by = fresh_uuid(&pool).await;

    sqlx::query(
        "INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, '2026-04-20', $2::UUID, $3::UUID)",
    )
    .bind(&so)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("insert 1");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, '2026-04-20', $2::UUID, $3::UUID)",
    )
    .bind(&so)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await;
    assert!(r.is_err(), "duplicate idempotency_key must reject");
}

#[tokio::test]
async fn shipment_lines_unique_per_shipment_so_line() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-SHL").await;
    let sku = one_sku(&pool, "T1-SHL-SKU").await;
    let loc = one_location(&pool, "T1-SHL-LOC").await;
    let so = one_so(&pool, &cust).await;
    let so_line: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD')
         RETURNING id::text",
    )
    .bind(&so).bind(&sku).bind(&loc).fetch_one(&pool).await.expect("line");

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let ship: String = sqlx::query_scalar(
        "INSERT INTO so_shipments (so_id, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, '2026-04-20', $2::UUID, $3::UUID) RETURNING id::text",
    )
    .bind(&so).bind(&posted_by).bind(&key).fetch_one(&pool).await.expect("ship");

    sqlx::query(
        "INSERT INTO so_shipment_lines (shipment_id, so_line_id, qty_shipped,
            unit_cost, unit_price)
         VALUES ($1::UUID, $2::UUID, 5, 60, 100)",
    )
    .bind(&ship).bind(&so_line).execute(&pool).await.expect("line 1");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO so_shipment_lines (shipment_id, so_line_id, qty_shipped,
            unit_cost, unit_price)
         VALUES ($1::UUID, $2::UUID, 3, 60, 100)",
    )
    .bind(&ship).bind(&so_line).fetch_one(&pool).await;
    assert!(r.is_err(), "duplicate (shipment_id, so_line_id) must reject");
}

// ============================================================
// customer_invoice_lines kind CHECK
// ============================================================

#[tokio::test]
async fn invoice_line_so_match_requires_qty_unit_price() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-INV-1").await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let inv: String = sqlx::query_scalar(
        "INSERT INTO customer_invoices (customer_id, currency, business_date,
            posted_by, idempotency_key)
         VALUES ($1::UUID, 'USD', '2026-04-20', $2::UUID, $3::UUID)
         RETURNING id::text",
    )
    .bind(&cust).bind(&posted_by).bind(&key).fetch_one(&pool).await.expect("inv");

    // so_match without so_line_id, qty, unit_price → CHECK fails.
    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO customer_invoice_lines (invoice_id, line_no, kind, amount)
         VALUES ($1::UUID, 1, 'so_match', 100)",
    )
    .bind(&inv).fetch_one(&pool).await;
    assert!(r.is_err(), "so_match without required fields must reject");
}

#[tokio::test]
async fn invoice_line_service_rejects_so_line_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-INV-2").await;
    let sku = one_sku(&pool, "T1-INV-2-SKU").await;
    let loc = one_location(&pool, "T1-INV-2-LOC").await;
    let so = one_so(&pool, &cust).await;
    let so_line: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD')
         RETURNING id::text",
    )
    .bind(&so).bind(&sku).bind(&loc).fetch_one(&pool).await.expect("line");

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let inv: String = sqlx::query_scalar(
        "INSERT INTO customer_invoices (customer_id, currency, business_date,
            posted_by, idempotency_key)
         VALUES ($1::UUID, 'USD', '2026-04-20', $2::UUID, $3::UUID)
         RETURNING id::text",
    )
    .bind(&cust).bind(&posted_by).bind(&key).fetch_one(&pool).await.expect("inv");

    let revenue_id = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    // service WITH so_line_id → CHECK fails.
    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO customer_invoice_lines (invoice_id, line_no, kind,
            so_line_id, revenue_account_id, amount)
         VALUES ($1::UUID, 1, 'service', $2::UUID, $3, 100)",
    )
    .bind(&inv).bind(&so_line).bind(revenue_id).fetch_one(&pool).await;
    assert!(r.is_err(), "service with so_line_id must reject");
}

#[tokio::test]
async fn invoice_line_unknown_kind_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-INV-K").await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let inv: String = sqlx::query_scalar(
        "INSERT INTO customer_invoices (customer_id, currency, business_date,
            posted_by, idempotency_key)
         VALUES ($1::UUID, 'USD', '2026-04-20', $2::UUID, $3::UUID)
         RETURNING id::text",
    )
    .bind(&cust).bind(&posted_by).bind(&key).fetch_one(&pool).await.expect("inv");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO customer_invoice_lines (invoice_id, line_no, kind, amount)
         VALUES ($1::UUID, 1, 'wat', 100)",
    )
    .bind(&inv).fetch_one(&pool).await;
    assert!(r.is_err(), "unknown kind must reject");
}

// ============================================================
// ar_payments
// ============================================================

#[tokio::test]
async fn ar_payments_amount_positive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-AP-NEG").await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO ar_payments (customer_id, currency, amount, business_date,
            posted_by, idempotency_key)
         VALUES ($1::UUID, 'USD', 0, '2026-04-25', $2::UUID, $3::UUID)",
    )
    .bind(&cust).bind(&posted_by).bind(&key).fetch_one(&pool).await;
    assert!(r.is_err(), "amount must be > 0");
}

#[tokio::test]
async fn ar_payments_idempotency_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-AP-IDEMP").await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO ar_payments (customer_id, currency, amount, business_date,
            posted_by, idempotency_key)
         VALUES ($1::UUID, 'USD', 100, '2026-04-25', $2::UUID, $3::UUID)",
    )
    .bind(&cust).bind(&posted_by).bind(&key).execute(&pool).await.expect("ins 1");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO ar_payments (customer_id, currency, amount, business_date,
            posted_by, idempotency_key)
         VALUES ($1::UUID, 'USD', 100, '2026-04-25', $2::UUID, $3::UUID)",
    )
    .bind(&cust).bind(&posted_by).bind(&key).fetch_one(&pool).await;
    assert!(r.is_err(), "duplicate idempotency_key must reject");
}

// ============================================================
// Account partitioning indexes
// ============================================================

#[tokio::test]
async fn ar_partitioned_by_customer_currency() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-PART").await;

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ar', 'value', 'USD', $1::UUID, 'debit')",
    )
    .bind(&cust).execute(&pool).await.expect("ar 1");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ar', 'value', 'USD', $1::UUID, 'debit')",
    )
    .bind(&cust).fetch_one(&pool).await;
    assert!(r.is_err(), "duplicate ar(customer, USD) must reject");
}

#[tokio::test]
async fn customer_pool_partitioned_by_customer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-CP").await;

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('customer_pool', 'qty', $1::UUID, 'debit')",
    )
    .bind(&cust).execute(&pool).await.expect("cp 1");

    let r: Result<sqlx::postgres::PgRow, _> = sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('customer_pool', 'qty', $1::UUID, 'debit')",
    )
    .bind(&cust).fetch_one(&pool).await;
    assert!(r.is_err(), "duplicate customer_pool(customer) must reject");
}

// ============================================================
// reservation_status enum
// ============================================================

#[tokio::test]
async fn reservation_status_shipped_accepted() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = one_customer(&pool, "T1-RES").await;
    let sku = one_sku(&pool, "T1-RES-SKU").await;
    let loc = one_location(&pool, "T1-RES-LOC").await;
    let so = one_so(&pool, &cust).await;
    let so_line: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines (so_id, line_no, sku_id, ship_location_id,
            qty_ordered, unit_price, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, 10, 100, 'USD')
         RETURNING id::text",
    )
    .bind(&so).bind(&sku).bind(&loc).fetch_one(&pool).await.expect("line");

    let r = sqlx::query(
        "INSERT INTO inventory_reservations
            (sku_id, location_id, qty, so_id, so_line_id, status, expires_at)
         VALUES ($1::UUID, $2::UUID, 1, $3::UUID, $4::UUID, 'shipped',
                 clock_timestamp() + INTERVAL '1 hour')",
    )
    .bind(&sku).bind(&loc).bind(&so).bind(&so_line).execute(&pool).await;
    assert!(r.is_ok(), "shipped status must be accepted: {r:?}");
}
