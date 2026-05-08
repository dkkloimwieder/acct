//! T1 probes for `posting_line_inventory` (mig 0024, acct-wb75.2.2).
//! Phase C of the convergence plan (acct-wb75). Pin schema constraints
//! (PK uniqueness, FK posting_line_id, FK product_id, NOT NULL
//! product_id, CHECK quantity > 0, CHECK unit_cost >= 0) and dispatcher
//! behavior across qty-leg postings, value-leg postings with explicit
//! qty, and the recon check #6.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

async fn one_usd_no_qty(pool: &PgPool) -> i64 {
    // cash → revenue, no qty, no SKU. Should NOT produce a
    // posting_line_inventory row.
    let cash = account_id_by_kind_currency(pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(pool, json!([event]), false)
        .await
        .expect("seed");
    sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn fk_posting_line_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO posting_line_inventory
                (posting_line_id, product_id, quantity, cost_method_at_event)
             VALUES (99999999,
                     (SELECT id FROM skus WHERE code = 'SKU-A'),
                     1, 'standard')",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn product_id_not_null_required() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_no_qty(&pool).await;

    expect_sqlstate("23502", || async {
        sqlx::query(
            "INSERT INTO posting_line_inventory
                (posting_line_id, product_id, quantity, cost_method_at_event)
             VALUES ($1, NULL, 1, 'standard')",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn quantity_positive_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_no_qty(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_line_inventory
                (posting_line_id, product_id, quantity, cost_method_at_event)
             VALUES ($1,
                     (SELECT id FROM skus WHERE code = 'SKU-A'),
                     0, 'standard')",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unit_cost_non_negative_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_no_qty(&pool).await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_line_inventory
                (posting_line_id, product_id, quantity, unit_cost,
                 cost_method_at_event)
             VALUES ($1,
                     (SELECT id FROM skus WHERE code = 'SKU-A'),
                     1, -1, 'standard')",
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

    let xfer = one_usd_no_qty(&pool).await;

    sqlx::query(
        "INSERT INTO posting_line_inventory
            (posting_line_id, product_id, quantity, cost_method_at_event)
         VALUES ($1, (SELECT id FROM skus WHERE code = 'SKU-A'),
                 1, 'standard')",
    )
    .bind(xfer)
    .execute(&pool)
    .await
    .unwrap();

    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO posting_line_inventory
                (posting_line_id, product_id, quantity, cost_method_at_event)
             VALUES ($1, (SELECT id FROM skus WHERE code = 'SKU-A'),
                     1, 'standard')",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn dispatcher_skips_extension_when_qty_null() {
    // cash → revenue: no qty, no SKU. No extension row.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_no_qty(&pool).await;

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_line_inventory WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn dispatcher_writes_extension_for_qty_leg() {
    // cycle_count_adj qty-leg: stock_available ← creation_void.
    // Both ledger_kind='qty'. Should produce extension row with
    // unit_cost=NULL (qty-leg, not value-leg).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, 7, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let row: (String, f64, Option<f64>, String) = sqlx::query_as(
        "SELECT product_id::text, quantity::float8,
                unit_cost::float8, cost_method_at_event::text
           FROM posting_line_inventory WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("extension row exists");

    let sku_id_expected: String =
        sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-A'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, sku_id_expected, "product_id matches credit-side SKU");
    assert_eq!(row.1, 7.0, "quantity = ABS(qty)");
    assert_eq!(row.2, None, "unit_cost NULL on qty-leg");
    assert_eq!(row.3, "standard", "cost_method snapshot from skus");
}

#[tokio::test]
async fn dispatcher_writes_unit_cost_for_value_leg() {
    // Value-leg with explicit qty. Seed the wip10_value pool first so
    // the credit doesn't push debit-normal account negative.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let wip10_value: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'inv_value_wip' AND s.code = 'SKU-A' AND a.routing_op = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let wip20_value: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'inv_value_wip' AND s.code = 'SKU-A' AND a.routing_op = 20",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let void_val =
        account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;

    let seed_key = fresh_uuid(&pool).await;
    let seed = make_event_with_qty(
        "cost_adjustment", wip10_value, void_val, 1000, 10,
        "2026-04-15", &seed_key,
    );
    call_post_posting_lines(&pool, json!([seed]), false)
        .await
        .expect("seed wip10");

    let key = fresh_uuid(&pool).await;
    // amount=500, qty=5 → unit_cost = 100.0
    let event = make_event_with_qty(
        "op_move_v", wip20_value, wip10_value, 500, 5,
        "2026-04-15", &key,
    );
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post op_move_v");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let (qty, unit_cost): (f64, Option<f64>) = sqlx::query_as(
        "SELECT quantity::float8, unit_cost::float8
           FROM posting_line_inventory WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("extension row exists");

    assert_eq!(qty, 5.0);
    assert_eq!(unit_cost, Some(100.0), "unit_cost = amount/qty for value-leg");
}

#[tokio::test]
async fn dispatcher_credit_first_sku_resolution() {
    // bin_move SKU-A (OUT) → SKU-A (MAIN). Both sides have sku_id
    // (same SKU). Credit-first per R2.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 50).await;
    let main_stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let out_stock = account_id_stock_available(&pool, "SKU-A", "OUT").await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("bin_move", out_stock, main_stock, 10, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post bin_move");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let product_id: String = sqlx::query_scalar(
        "SELECT product_id::text FROM posting_line_inventory
          WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    let sku_a: String =
        sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-A'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(product_id, sku_a);
}

#[tokio::test]
async fn cost_method_snapshot_reflects_sku_method() {
    // SKU-WAC has cost_method='wac_perpetual'. The extension's
    // cost_method_at_event must snapshot the SKU's method at posting
    // time.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let stock = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, 3, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let cm: String = sqlx::query_scalar(
        "SELECT cost_method_at_event::text FROM posting_line_inventory
          WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cm, "wac_perpetual");
}

#[tokio::test]
async fn recon_finds_no_violations_after_normal_postings() {
    // After a representative variety of qty-emitting events, recon
    // returns 0 alerts (empty alert table).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 50).await;
    let main_stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let out_stock = account_id_stock_available(&pool, "SKU-A", "OUT").await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("bin_move", out_stock, main_stock, 10, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post bin_move");

    let alerts: i32 =
        sqlx::query_scalar("SELECT run_daily_reconciliation()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(alerts, 0, "no recon alerts after normal qty postings");
}

#[tokio::test]
async fn recon_check_6_flags_orphan_qty_posting_line() {
    // Manually delete a posting_line_inventory row to simulate a
    // dispatcher regression. Recon check #6 should surface it.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 50).await;
    let xfer: i64 = sqlx::query_scalar(
        "SELECT id FROM posting_lines WHERE qty IS NOT NULL ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query("DELETE FROM posting_line_inventory WHERE posting_line_id = $1")
        .bind(xfer)
        .execute(&pool)
        .await
        .unwrap();

    let alerts: i32 =
        sqlx::query_scalar("SELECT run_daily_reconciliation()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(alerts >= 1, "recon must flag the orphan qty posting_line");

    let kind: String = sqlx::query_scalar(
        "SELECT alert_kind FROM reconciliation_alerts
          WHERE alert_kind = 'inventory_extension_missing'
          ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kind, "inventory_extension_missing");
}
