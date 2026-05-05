//! `acct-mv8` — post_so_allocate matrix.
//!
//! Coverage:
//!   * Happy path: reservations transition active → allocated → shipped.
//!   * Idempotency: replay returns existing allocation id.
//!   * Validation: P0043 (so not found).
//!   * Allocate with no active reservations is a graceful no-op.
//!   * Cancellation from allocated state: allocated → cancelled is
//!     permitted by the data model; post_so_ship skips cancelled
//!     reservations (only flips active OR allocated).
//!   * Pre-existing 'active'-only ship path still works (regression
//!     lock for the post_so_ship WHERE-clause widening).

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffold (mirror of tests/so_ship.rs minus tax helpers)
// ============================================================

#[allow(dead_code)]
struct AllocScaffold {
    customer_id: String,
    so_id: String,
    sku_id: String,
    ship_loc_id: String,
    so_line_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
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

async fn scaffold(pool: &PgPool, suffix: &str, qty_ordered: i64) -> AllocScaffold {
    let customer_id = fresh_customer(pool, &format!("CUST-A-{suffix}"), "USD").await;
    let sku_id = fresh_sku(pool, &format!("SKU-A-{suffix}"), "standard").await;
    let ship_loc_id = fresh_location(pool, &format!("ALC-{suffix}")).await;

    set_std_cost(pool, &sku_id, 60).await;

    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(pool, &so_id, 1, &sku_id, &ship_loc_id, qty_ordered, 100).await;

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

    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    AllocScaffold {
        customer_id,
        so_id,
        sku_id,
        ship_loc_id,
        so_line_id,
        qty_acct,
        val_acct,
        cust_qty,
        cust_unsettled,
        creation_void_qty,
        creation_void_val,
    }
}

async fn seed_fg(pool: &PgPool, s: &AllocScaffold, qty: i64, total_value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"alloc_test_seed", "document_id":doc_id,
         "debit_account_id":s.qty_acct, "credit_account_id":s.creation_void_qty,
         "amount":qty, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"alloc_test_seed", "document_id":doc_id,
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

async fn create_reservation(pool: &PgPool, s: &AllocScaffold, qty: i64) -> String {
    sqlx::query_scalar(
        "SELECT reserve_inventory(
            $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
            '2099-01-01'::TIMESTAMPTZ, NULL
         )::text",
    )
    .bind(&s.sku_id)
    .bind(&s.ship_loc_id)
    .bind(qty)
    .bind(&s.so_id)
    .bind(&s.so_line_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("reserve_inventory: {e}"))
}

async fn reservation_status(pool: &PgPool, rsv_id: &str) -> String {
    sqlx::query_scalar(
        "SELECT status::text FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(rsv_id)
    .fetch_one(pool)
    .await
    .expect("reservation_status")
}

async fn call_allocate(pool: &PgPool, so_id: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                  $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(so_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_ship(
    pool: &PgPool,
    s: &AllocScaffold,
    qty_shipped: i64,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": qty_shipped}]);
    sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Happy path
// ============================================================

#[tokio::test]
async fn active_to_allocated_to_shipped() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "HAPPY", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let rsv = create_reservation(&pool, &s, 5).await;

    assert_eq!(reservation_status(&pool, &rsv).await, "active");

    call_allocate(&pool, &s.so_id).await.expect("allocate");
    assert_eq!(reservation_status(&pool, &rsv).await, "allocated");

    call_ship(&pool, &s, 5).await.expect("ship");
    assert_eq!(reservation_status(&pool, &rsv).await, "shipped");

    assert_invariants_hold(&pool, "active_to_allocated_to_shipped").await;
}

#[tokio::test]
async fn allocate_creates_so_allocations_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "ROW", 5).await;
    seed_fg(&pool, &s, 5, 300).await;
    let _rsv = create_reservation(&pool, &s, 3).await;

    let alloc_id = call_allocate(&pool, &s.so_id).await.expect("allocate");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM so_allocations WHERE id = $1::UUID AND so_id = $2::UUID",
    )
    .bind(&alloc_id)
    .bind(&s.so_id)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(count, 1);
}

// ============================================================
// Idempotency
// ============================================================

#[tokio::test]
async fn allocate_idempotency_returns_existing_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "IDEMP", 5).await;
    seed_fg(&pool, &s, 5, 300).await;
    let rsv = create_reservation(&pool, &s, 3).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let id1: String = sqlx::query_scalar(
        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                  $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("alloc-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                  $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("alloc-2 replay");

    assert_eq!(id1, id2);
    assert_eq!(reservation_status(&pool, &rsv).await, "allocated");
}

// ============================================================
// Validation
// ============================================================

#[tokio::test]
async fn unknown_so_raises_p0043() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate("P0043", || async { call_allocate(&pool, &bogus).await }).await;
}

#[tokio::test]
async fn allocate_with_no_active_reservations_is_noop() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "NOOP", 5).await;
    // No reservations created. Allocate should succeed (it's idempotent
    // by design — allocates 0 reservations) and return an alloc id.
    let alloc_id = call_allocate(&pool, &s.so_id).await.expect("allocate");
    assert!(!alloc_id.is_empty());

    let allocated_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_reservations
          WHERE so_id = $1::UUID AND status = 'allocated'",
    )
    .bind(&s.so_id)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(allocated_count, 0);
}

// ============================================================
// Cancellation from allocated state
// ============================================================

#[tokio::test]
async fn cancellation_from_allocated_ship_skips_cancelled() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "CANCEL", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let rsv = create_reservation(&pool, &s, 5).await;

    call_allocate(&pool, &s.so_id).await.expect("allocate");
    assert_eq!(reservation_status(&pool, &rsv).await, "allocated");

    // Cancel the allocated reservation directly. (An explicit
    // post_so_cancel_reservation workflow is left as a future
    // follow-up; this test verifies the data model permits the
    // transition and that post_so_ship correctly skips cancelled
    // reservations.)
    sqlx::query(
        "UPDATE inventory_reservations
            SET status = 'cancelled', resolved_at = clock_timestamp()
          WHERE id = $1::UUID",
    )
    .bind(&rsv)
    .execute(&pool)
    .await
    .expect("cancel reservation");
    assert_eq!(reservation_status(&pool, &rsv).await, "cancelled");

    // post_so_ship still runs (qty / cogs / revenue post normally),
    // but the cancelled reservation is NOT flipped to 'shipped'.
    call_ship(&pool, &s, 5).await.expect("ship");
    assert_eq!(reservation_status(&pool, &rsv).await, "cancelled");
}

// ============================================================
// Regression: 'active' direct-to-shipped path still works
// ============================================================

#[tokio::test]
async fn active_to_shipped_direct_path_still_works() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "DIRECT", 10).await;
    seed_fg(&pool, &s, 10, 600).await;
    let rsv = create_reservation(&pool, &s, 5).await;

    assert_eq!(reservation_status(&pool, &rsv).await, "active");

    // Skip allocate; ship directly. This is the Slice C MVP path —
    // the post_so_ship WHERE-clause widening to status IN ('active',
    // 'allocated') must keep this working.
    call_ship(&pool, &s, 5).await.expect("ship");
    assert_eq!(reservation_status(&pool, &rsv).await, "shipped");
}
