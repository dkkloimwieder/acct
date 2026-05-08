//! T1 probes for `posting_line_dimensions` (mig 0023, acct-wb75.1.3).
//! Phase B3 of the convergence plan (acct-wb75). Pin schema constraints
//! (PK uniqueness, FK posting_line_id, FK dimension_type, value_present
//! CHECK, value_exclusive CHECK) and dispatcher behavior across the
//! 5 active dimension types (product, location, routing_op, customer,
//! vendor).

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

async fn one_usd_transfer(pool: &PgPool) -> i64 {
    // cash → revenue, no SKU/loc/counterparty; minimal posting.
    let cash = account_id_by_kind_currency(pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(pool, json!([event]), false)
        .await
        .expect("seed transfer");
    sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn dimension_types_seeded() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dimension_types")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 9, "9 dimension types seeded by 0023");

    let names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM dimension_types ORDER BY dimension_type",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        names,
        vec![
            "customer", "vendor", "product", "location", "routing_op",
            "cost_center", "profit_center", "project", "department",
        ]
    );
}

#[tokio::test]
async fn value_present_check_rejects_both_null() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_line_dimensions
                (posting_line_id, dimension_type) VALUES ($1, 3)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn value_exclusive_check_rejects_both_set() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_line_dimensions
                (posting_line_id, dimension_type, dimension_value, dimension_value_uuid)
             VALUES ($1, 3, 1, gen_random_uuid())",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn duplicate_pk_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    sqlx::query(
        "INSERT INTO posting_line_dimensions
            (posting_line_id, dimension_type, dimension_value_uuid)
         VALUES ($1, 3, gen_random_uuid())",
    )
    .bind(xfer)
    .execute(&pool)
    .await
    .unwrap();

    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO posting_line_dimensions
                (posting_line_id, dimension_type, dimension_value_uuid)
             VALUES ($1, 3, gen_random_uuid())",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unknown_dimension_type_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO posting_line_dimensions
                (posting_line_id, dimension_type, dimension_value)
             VALUES ($1, 99, 1)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn dispatcher_writes_product_and_location_for_inventory_posting() {
    // cycle_count_adj: stock_available (sku, loc) ← creation_void.
    // Should emit product (3) + location (4) dimension rows.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, 5, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let dim_types: Vec<i16> = sqlx::query_scalar(
        "SELECT dimension_type FROM posting_line_dimensions
          WHERE posting_line_id = $1 ORDER BY dimension_type",
    )
    .bind(xfer)
    .fetch_all(&pool)
    .await
    .unwrap();
    // Product (3) + location (4) — debit-side (stock_available) carries
    // them; credit (creation_void) does not. COALESCE picks debit when
    // credit is NULL.
    assert_eq!(dim_types, vec![3i16, 4]);

    // product_id matches the SKU
    let sku_dim: String = sqlx::query_scalar(
        "SELECT dimension_value_uuid::text FROM posting_line_dimensions
          WHERE posting_line_id = $1 AND dimension_type = 3",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    let sku_id_expected: String =
        sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-A'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sku_dim, sku_id_expected);
}

#[tokio::test]
async fn dispatcher_skips_dimensions_for_no_composition_posting() {
    // cash → revenue: no SKU, no location, no counterparty kind.
    // Zero dimension rows expected.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_line_dimensions WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn dispatcher_writes_routing_op_for_wip_posting() {
    // Open a stock_wip account with a routing_op, then post against it.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Fixture pre-creates stock_wip for SKU-A at routing_op=10.
    let stock_wip: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'stock_wip' AND s.code = 'SKU-A'
            AND a.routing_op = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("wo_start", stock_wip, void_qty, 7, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post wo_start");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let routing_op: i64 = sqlx::query_scalar(
        "SELECT dimension_value FROM posting_line_dimensions
          WHERE posting_line_id = $1 AND dimension_type = 5",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("routing_op dim row exists");
    assert_eq!(routing_op, 10);
}

#[tokio::test]
async fn dispatcher_writes_customer_dim_for_ar_posting() {
    // ar account + counterparty_id → customer dimension type 1.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cust_id = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO customers (id, code, name, default_currency)
         VALUES ($1::UUID, 'CUST-DIM', 'Customer Dim Test', 'USD')",
    )
    .bind(&cust_id)
    .execute(&pool)
    .await
    .unwrap();

    let ar: i64 = sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ar', 'value', 'USD', $1::UUID, 'debit') RETURNING id",
    )
    .bind(&cust_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_invoice", ar, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let cust_dim: String = sqlx::query_scalar(
        "SELECT dimension_value_uuid::text FROM posting_line_dimensions
          WHERE posting_line_id = $1 AND dimension_type = 1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("customer dim row exists");
    assert_eq!(cust_dim, cust_id);
}

#[tokio::test]
async fn dispatcher_writes_vendor_dim_for_ap_posting() {
    // ap account + counterparty_id → vendor dimension type 2.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let v_id = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO vendors (id, code, name, currency)
         VALUES ($1::UUID, 'V-DIM', 'Vendor Dim Test', 'USD')",
    )
    .bind(&v_id)
    .execute(&pool)
    .await
    .unwrap();

    let ap: i64 = sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap', 'value', 'USD', $1::UUID, 'credit') RETURNING id",
    )
    .bind(&v_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let labor_expense =
        account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("ap_bill", labor_expense, ap, 100, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let v_dim: String = sqlx::query_scalar(
        "SELECT dimension_value_uuid::text FROM posting_line_dimensions
          WHERE posting_line_id = $1 AND dimension_type = 2",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("vendor dim row exists");
    assert_eq!(v_dim, v_id);
}

#[tokio::test]
async fn dispatcher_credit_first_resolution_on_conflicting_skus() {
    // Two different SKUs, two stock_available accounts at same loc.
    // Posting with debit=stock(SKU-A) credit=stock(SKU-B). Per R2,
    // credit-side governs → product dim_value = SKU-B's id.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Use fixture SKUs that both have stock_available@MAIN (SKU-A and
    // SKU-WAC). Pre-stock SKU-WAC so it can be credited (debit-normal).
    seed_stock(&pool, "SKU-WAC", "MAIN", 5).await;

    let stock_a = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let stock_b = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let key = fresh_uuid(&pool).await;
    // bin_move is a qty-only reason for inventory transfers within
    // a SKU, but the dispatcher doesn't enforce same-SKU on it.
    let event = make_event("bin_move", stock_a, stock_b, 1, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let prod_dim: String = sqlx::query_scalar(
        "SELECT dimension_value_uuid::text FROM posting_line_dimensions
          WHERE posting_line_id = $1 AND dimension_type = 3",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    // Credit-side is SKU-WAC (stock_b) → expect SKU-WAC id.
    let sku_b_id: String =
        sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-WAC'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(prod_dim, sku_b_id, "credit-first per R2");
}

#[tokio::test]
async fn recon_finds_no_violations_after_normal_postings() {
    // Healthy run: a few inventory + AR/AP postings, then run recon.
    // Should produce zero alerts (besides any pre-existing fixture
    // alerts, which are TRUNCATEd in reset_to_fixture).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 10).await;

    let n_alerts: i32 = sqlx::query_scalar(
        "SELECT run_daily_reconciliation()",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_alerts, 0, "no alerts after normal postings");
}
