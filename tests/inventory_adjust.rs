//! `acct-dj8` — first Phase 1 feature test.
//!
//! Pure inventory adjustment workflow. Wraps the cycle_count_adj
//! ledger primitive in a thin document-layer function with an audit
//! row, idempotent replay, and direction-aware sign handling.

mod common;

use common::*;

/// Resolve a sku id by code as text.
async fn sku_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("sku {code}: {e}"))
}

/// Resolve a location id by code as text.
async fn loc_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("location {code}: {e}"))
}

/// Read `(debits_total - credits_total)` for an account by id.
async fn balance(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("balance")
}

/// Call post_inventory_adjustment with an auto-generated posted_by.
#[allow(clippy::too_many_arguments)]
async fn adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    loc: &str,
    qty_delta: i64,
    unit_cost: i64,
    currency: &str,
    inv_class: &str,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, $5,
            $6, $7::DATE, $8::UUID, $9::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(currency)
    .bind(inv_class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn adjust_in_creates_balances_and_audit_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let doc_id = adjust(&pool, &sku, &loc, 50, 10, "USD", "fg", "2026-04-15", &key)
        .await
        .expect("adjust in");

    let row_count: i64 = sqlx::query_scalar(
        "SELECT count(*)::BIGINT FROM inventory_adjustments
          WHERE id = $1::UUID AND qty_delta = 50 AND unit_cost = 10",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(row_count, 1);

    assert_eq!(balance(&pool, qty_acct).await, 50, "stock_available");
    assert_eq!(balance(&pool, val_acct).await, 500, "inv_value_fg = 50 * 10");
    assert_eq!(balance(&pool, void_qty).await, -50, "creation_void(qty)");
    assert_eq!(balance(&pool, void_val).await, -500, "creation_void(USD)");
}

#[tokio::test]
async fn adjust_out_reduces_balances_and_audit_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed an "in" first so we have something to take out.
    let k1 = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, 100, 5, "USD", "fg", "2026-04-15", &k1)
        .await
        .expect("seed in");
    assert_eq!(balance(&pool, qty_acct).await, 100);
    assert_eq!(balance(&pool, val_acct).await, 500);

    let k2 = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, -30, 5, "USD", "fg", "2026-04-15", &k2)
        .await
        .expect("adjust out");

    assert_eq!(balance(&pool, qty_acct).await, 70, "stock_available after out");
    assert_eq!(balance(&pool, val_acct).await, 350, "inv_value_fg after out");
}

#[tokio::test]
async fn adjust_idempotent_replay_returns_same_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;

    let key = fresh_uuid(&pool).await;
    let id1 = adjust(&pool, &sku, &loc, 25, 4, "USD", "fg", "2026-04-15", &key)
        .await
        .expect("first call");
    let id2 = adjust(&pool, &sku, &loc, 25, 4, "USD", "fg", "2026-04-15", &key)
        .await
        .expect("replay");

    assert_eq!(id1, id2, "replay returns same doc id");
    assert_eq!(balance(&pool, qty_acct).await, 25, "balance moved once");

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*)::BIGINT FROM inventory_adjustments WHERE idempotency_key = $1::UUID",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn adjust_zero_qty_rejected_by_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;

    let key = fresh_uuid(&pool).await;
    let res = adjust(&pool, &sku, &loc, 0, 10, "USD", "fg", "2026-04-15", &key).await;
    let err = res.expect_err("zero qty must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("inventory_adjustments")
            && (msg.contains("check") || msg.contains("23514")),
        "expected CHECK violation on inventory_adjustments, got: {msg}"
    );
}

#[tokio::test]
async fn adjust_negative_unit_cost_rejected_by_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;

    let key = fresh_uuid(&pool).await;
    let res = adjust(&pool, &sku, &loc, 10, -1, "USD", "fg", "2026-04-15", &key).await;
    let err = res.expect_err("negative unit_cost must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("inventory_adjustments")
            && (msg.contains("check") || msg.contains("23514")),
        "expected CHECK violation on inventory_adjustments, got: {msg}"
    );
}

#[tokio::test]
async fn adjust_in_against_wac_sku_re_averages_pool() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    let k1 = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, 100, 5, "USD", "fg", "2026-04-15", &k1)
        .await
        .expect("seed");
    assert_eq!(balance(&pool, qty_acct).await, 100);
    assert_eq!(balance(&pool, val_acct).await, 500);

    let k2 = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, 100, 7, "USD", "fg", "2026-04-15", &k2)
        .await
        .expect("add at 7");
    assert_eq!(balance(&pool, qty_acct).await, 200);
    assert_eq!(balance(&pool, val_acct).await, 1200);
    let unit = balance(&pool, val_acct).await / balance(&pool, qty_acct).await;
    assert_eq!(unit, 6, "WAC pool re-averaged to 6");
}

#[tokio::test]
async fn adjust_with_currency_eur_missing_account_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;

    // Fixture has no EUR inv_value_fg for SKU-A → P0010.
    let key = fresh_uuid(&pool).await;
    let res = adjust(&pool, &sku, &loc, 10, 5, "EUR", "fg", "2026-04-15", &key).await;
    let err = res.expect_err("EUR inv_value_fg missing must fail");
    let msg = format!("{err}");
    assert!(msg.contains("P0010") || msg.contains("inv_value_fg"),
            "expected P0010, got: {msg}");
}

#[tokio::test]
async fn adjust_with_class_raw_uses_inv_value_raw() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let raw_acct = account_id_for_selector(
        &pool, "inv_value_raw", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let fg_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    let key = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, 40, 3, "USD", "raw", "2026-04-15", &key)
        .await
        .expect("raw class");

    assert_eq!(balance(&pool, raw_acct).await, 120, "inv_value_raw debited");
    assert_eq!(balance(&pool, fg_acct).await, 0, "inv_value_fg untouched");
}

#[tokio::test]
async fn adjust_qty_only_unit_cost_zero_skips_value_leg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    let key = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, 15, 0, "USD", "fg", "2026-04-15", &key)
        .await
        .expect("qty-only");

    assert_eq!(balance(&pool, qty_acct).await, 15);
    assert_eq!(balance(&pool, val_acct).await, 0, "no value leg posted");
}
