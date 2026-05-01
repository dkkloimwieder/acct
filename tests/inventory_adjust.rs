//! `acct-dj8` — Phase 1 inventory adjustment workflow.
//!
//! Tests the cost-method-aware dispatch:
//!   - standard SKU: NULL p_unit_cost uses skus.standard_cost; explicit
//!     p_unit_cost is rejected (P0011).
//!   - WAC SKU IN:   NULL uses pool average (or P0011 if pool empty);
//!     explicit re-averages.
//!   - WAC SKU OUT:  NULL uses pool average (classic WAC); explicit
//!     posts at asserted cost; pool average drifts to reflect true
//!     remaining cost.
//!   - fifo / lot:   P0006 (not implemented).

mod common;

use common::*;

async fn sku_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("sku {code}: {e}"))
}

async fn loc_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("location {code}: {e}"))
}

async fn balance(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("balance")
}

/// Call `post_inventory_adjustment` with an Option<i64> for unit_cost
/// (None → SQL NULL → use system cost).
#[allow(clippy::too_many_arguments)]
async fn adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    loc: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
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

// ---- Standard SKU (SKU-A, standard_cost = 100) ----

#[tokio::test]
async fn standard_sku_null_uses_standard_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let adj = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    // SKU-A standard_cost = 100. Pass NULL → effective cost = 100.
    // 50 units × $100 = $5,000 value side.
    let key = fresh_uuid(&pool).await;
    let doc_id = adjust(&pool, &sku, &loc, 50, None, "USD", "fg", "2026-04-15", &key)
        .await
        .expect("adjust in at standard");

    assert_eq!(balance(&pool, qty_acct).await, 50);
    assert_eq!(balance(&pool, val_acct).await, 5000, "50 * standard_cost(100)");
    assert_eq!(balance(&pool, adj).await, -5000, "adjustment income");

    // Audit row records the effective cost, not the caller's input.
    let recorded_cost: i64 = sqlx::query_scalar(
        "SELECT unit_cost FROM inventory_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .expect("audit unit_cost");
    assert_eq!(recorded_cost, 100, "audit row records effective cost");
}

#[tokio::test]
async fn standard_sku_with_explicit_cost_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    // Standard SKU rejects ANY explicit p_unit_cost — even one matching
    // standard_cost. Caller should pass NULL.
    expect_sqlstate("P0011", || async {
        adjust(&pool, &sku, &loc, 10, Some(100), "USD", "fg", "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn standard_sku_out_uses_standard_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed in 100 at standard ($100); then take 30 out at standard.
    adjust(&pool, &sku, &loc, 100, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed in");
    assert_eq!(balance(&pool, val_acct).await, 10_000);

    adjust(&pool, &sku, &loc, -30, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("take out");
    assert_eq!(balance(&pool, qty_acct).await, 70);
    assert_eq!(balance(&pool, val_acct).await, 7000, "10000 - 3000");
}

// ---- WAC SKU (SKU-WAC) ----

#[tokio::test]
async fn wac_in_with_null_against_empty_pool_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    // Pool is empty. NULL p_unit_cost can't compute pool avg → P0011.
    expect_sqlstate("P0011", || async {
        adjust(&pool, &sku, &loc, 100, None, "USD", "fg", "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn wac_in_explicit_seeds_pool() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed an empty pool with explicit cost: 100 units at $5.
    adjust(&pool, &sku, &loc, 100, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed");
    assert_eq!(balance(&pool, qty_acct).await, 100);
    assert_eq!(balance(&pool, val_acct).await, 500);
}

#[tokio::test]
async fn wac_in_with_null_against_populated_pool_uses_pool_avg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed pool: 100 units at $5 → avg $5.
    adjust(&pool, &sku, &loc, 100, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed");

    // Add 50 more with NULL → uses pool avg ($5). Pool unchanged ratio.
    adjust(&pool, &sku, &loc, 50, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("add at avg");

    assert_eq!(balance(&pool, qty_acct).await, 150);
    assert_eq!(balance(&pool, val_acct).await, 750);
    let avg = balance(&pool, val_acct).await / balance(&pool, qty_acct).await;
    assert_eq!(avg, 5, "pool avg unchanged");
}

#[tokio::test]
async fn wac_in_explicit_re_averages_pool() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // 100 at $5 then 100 at $7 → 200 at avg $6.
    adjust(&pool, &sku, &loc, 100, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("first lot");
    adjust(&pool, &sku, &loc, 100, Some(7), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("second lot");

    assert_eq!(balance(&pool, qty_acct).await, 200);
    assert_eq!(balance(&pool, val_acct).await, 1200);
    assert_eq!(balance(&pool, val_acct).await / balance(&pool, qty_acct).await, 6);
}

#[tokio::test]
async fn wac_out_with_null_uses_pool_avg_classic_wac() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed: 100 at $5, 100 at $7. Pool: 200 units, $1200, avg $6.
    adjust(&pool, &sku, &loc, 100, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("a");
    adjust(&pool, &sku, &loc, 100, Some(7), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("b");

    // Take 30 out with NULL → uses pool avg ($6). 30 × $6 = $180.
    adjust(&pool, &sku, &loc, -30, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("classic out");

    assert_eq!(balance(&pool, qty_acct).await, 170);
    assert_eq!(balance(&pool, val_acct).await, 1020, "1200 - 180");
    let avg = balance(&pool, val_acct).await / balance(&pool, qty_acct).await;
    assert_eq!(avg, 6, "classic WAC: pool avg preserved on OUT");
}

#[tokio::test]
async fn wac_out_explicit_cheaper_drifts_pool_avg_up() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed: 100 at $5, 100 at $7. Pool: 200 units, $1200, avg $6.
    adjust(&pool, &sku, &loc, 100, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("a");
    adjust(&pool, &sku, &loc, 100, Some(7), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("b");

    // Caller asserts: 30 leaving at $5 (the cheaper lot they know is gone).
    // Value-side credit: 30 × $5 = $150 (not $180 at pool avg).
    adjust(&pool, &sku, &loc, -30, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("out at asserted $5");

    // Pool: 170 units, $1200 - $150 = $1050. New avg = 1050 / 170 ≈ 6.176
    // (BIGINT truncates to 6 — so the assertion below uses exact values).
    assert_eq!(balance(&pool, qty_acct).await, 170);
    assert_eq!(balance(&pool, val_acct).await, 1050,
               "1200 - 150 (asserted cost), not 1200 - 180 (pool avg)");

    // The remaining 170 units' true average is $6.176 — drifted UP from
    // $6 because the cheaper material left. (BIGINT division truncates
    // to 6 for the cheap eyeball check, but the dollar-precise balance
    // confirms the drift is recorded.)
    let drift_check_value: i64 = balance(&pool, val_acct).await;
    let drift_check_qty: i64 = balance(&pool, qty_acct).await;
    assert!(drift_check_value * 100 / drift_check_qty > 600,
            "pool avg (×100) drifted above 600 (was 600 before): got {}",
            drift_check_value * 100 / drift_check_qty);
}

#[tokio::test]
async fn wac_out_explicit_more_expensive_drifts_pool_avg_down() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed: 100 at $5, 100 at $7 → pool avg $6.
    adjust(&pool, &sku, &loc, 100, Some(5), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("a");
    adjust(&pool, &sku, &loc, 100, Some(7), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("b");

    // Caller asserts: 30 leaving at $7 (the expensive lot left).
    adjust(&pool, &sku, &loc, -30, Some(7), "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("out at asserted $7");

    // Pool: 170 units, $1200 - $210 = $990. Avg = 990/170 ≈ 5.82.
    assert_eq!(balance(&pool, qty_acct).await, 170);
    assert_eq!(balance(&pool, val_acct).await, 990, "1200 - 210");

    let v: i64 = balance(&pool, val_acct).await;
    let q: i64 = balance(&pool, qty_acct).await;
    assert!(v * 100 / q < 600,
            "pool avg (×100) drifted below 600: got {}", v * 100 / q);
}

// ---- Sign + P&L direction ----

#[tokio::test]
async fn adjust_in_posts_as_adjustment_income_credit_balance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let adj = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    adjust(&pool, &sku, &loc, 50, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("in");
    // 50 × 100 = 5000; inv_adj_expense credited.
    assert_eq!(balance(&pool, adj).await, -5000, "adjustment income");
}

#[tokio::test]
async fn adjust_out_posts_as_adjustment_expense_debit_balance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let adj = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    adjust(&pool, &sku, &loc, 100, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed");
    assert_eq!(balance(&pool, adj).await, -10_000, "all income so far");

    adjust(&pool, &sku, &loc, -50, None, "USD", "fg", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("out");
    // Out posts a debit: net = -10000 + 5000 = -5000 (still net income).
    assert_eq!(balance(&pool, adj).await, -5000, "net adjustment after one in + one out");
}

// ---- Idempotency, table CHECKs, missing accounts ----

#[tokio::test]
async fn adjust_idempotent_replay_returns_same_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;

    let key = fresh_uuid(&pool).await;
    let id1 = adjust(&pool, &sku, &loc, 25, None, "USD", "fg", "2026-04-15", &key)
        .await.expect("first");
    let id2 = adjust(&pool, &sku, &loc, 25, None, "USD", "fg", "2026-04-15", &key)
        .await.expect("replay");
    assert_eq!(id1, id2);
    assert_eq!(balance(&pool, qty_acct).await, 25, "balance moves once");
}

#[tokio::test]
async fn adjust_zero_qty_rejected_by_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;

    let res = adjust(&pool, &sku, &loc, 0, None, "USD", "fg", "2026-04-15",
                     &fresh_uuid(&pool).await).await;
    let err = res.expect_err("zero qty must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("inventory_adjustments")
            && (msg.contains("check") || msg.contains("23514")),
        "expected CHECK violation, got: {msg}"
    );
}

#[tokio::test]
async fn adjust_with_currency_eur_missing_account_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0010", || async {
        adjust(&pool, &sku, &loc, 10, None, "EUR", "fg", "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
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

    adjust(&pool, &sku, &loc, 40, None, "USD", "raw", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("raw");
    // 40 × 100 = 4000.
    assert_eq!(balance(&pool, raw_acct).await, 4000, "inv_value_raw debited");
    assert_eq!(balance(&pool, fg_acct).await, 0, "inv_value_fg untouched");
}

// ---- Future cost methods ----

#[tokio::test]
async fn adjust_against_fifo_sku_errors() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    // SKU-FIF has no inv_value_fg account in the fixture, so the
    // missing-account guard fires first (P0010) before the cost-method
    // gate (P0006). When fifo accounts exist, P0006 will fire from
    // the cost_method dispatcher. Either error is acceptable as proof
    // the call did not silently succeed.
    let res = adjust(&pool, &sku, &loc, 10, Some(5), "USD", "fg",
                     "2026-04-15", &key).await;
    let err = res.expect_err("fifo path must fail");
    let db_err = err.as_database_error().expect("database error");
    let code = db_err.code().expect("sqlstate");
    assert!(
        code.as_ref() == "P0006" || code.as_ref() == "P0010",
        "expected P0006 or P0010, got {code}: {}",
        db_err.message(),
    );
}
