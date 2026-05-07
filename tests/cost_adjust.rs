//! `acct-14m` — Phase 1 Epic D: cost_adjustment workflow.
//!
//! Tests the value-only revaluation workflow:
//!   - wac_perpetual: write-up posts revaluation gain (variance_cost_adjustment
//!     credited); write-down posts revaluation loss (variance debited);
//!     no-op (delta=0) records audit row only.
//!   - standard: P0011.
//!   - wac_periodic / wac_retroactive / lot / fifo: P0006.
//!   - empty pool: P0010.
//!   - idempotency: replay returns same id, no double-post.

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

/// Seed a WAC pool via post_inventory_adjustment (sets up qty + value).
async fn seed_wac_pool(
    pool: &sqlx::PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    unit_cost: i64,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD',
            'fg', '2026-04-15'::DATE, $5::UUID, $6::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(unit_cost)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("seed");
}

/// Call post_cost_adjustment.
#[allow(clippy::too_many_arguments)]
async fn cost_adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    loc: &str,
    currency: &str,
    inv_class: &str,
    target_unit_cost: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment(
            $1::UUID, $2::UUID, $3, $4, $5::BIGINT,
            $6::DATE, $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(currency)
    .bind(inv_class)
    .bind(target_unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

// ---- Happy paths ----

#[tokio::test]
async fn write_up_posts_revaluation_gain() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    // Seed: 200 units at $6 → pool value $1200, avg $6.
    seed_wac_pool(&pool, &sku, &loc, 100, 5).await;
    seed_wac_pool(&pool, &sku, &loc, 100, 7).await;
    assert_eq!(balance(&pool, val_acct).await, 1200);

    // Adjust avg to $7. delta = 200 * 7 - 1200 = +200 (write-up gain).
    let key = fresh_uuid(&pool).await;
    let doc_id = cost_adjust(&pool, &sku, &loc, "USD", "fg", 7, "2026-04-15", &key)
        .await
        .expect("write-up");

    assert_eq!(balance(&pool, val_acct).await, 1400, "pool revalued to 200*7");
    assert_eq!(balance(&pool, var_acct).await, -200,
               "variance_cost_adjustment credited (revaluation gain)");

    // Audit row recorded prior, target, delta, pool_qty.
    let (prior, target, delta, qty): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT prior_unit_cost, target_unit_cost, delta_value, pool_qty
           FROM inventory_cost_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .expect("audit row");
    assert_eq!(prior, 6);
    assert_eq!(target, 7);
    assert_eq!(delta, 200);
    assert_eq!(qty, 200);
}

#[tokio::test]
async fn write_down_posts_revaluation_loss() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    // Seed: 200 units at $6 avg.
    seed_wac_pool(&pool, &sku, &loc, 100, 5).await;
    seed_wac_pool(&pool, &sku, &loc, 100, 7).await;

    // Adjust avg to $5. delta = 200 * 5 - 1200 = -200 (write-down loss).
    let key = fresh_uuid(&pool).await;
    cost_adjust(&pool, &sku, &loc, "USD", "fg", 5, "2026-04-15", &key)
        .await
        .expect("write-down");

    assert_eq!(balance(&pool, val_acct).await, 1000, "pool revalued to 200*5");
    assert_eq!(balance(&pool, var_acct).await, 200,
               "variance_cost_adjustment debited (revaluation loss)");
}

#[tokio::test]
async fn no_op_records_audit_row_but_no_transfer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    // Seed: 100 units at $5, pool avg $5.
    seed_wac_pool(&pool, &sku, &loc, 100, 5).await;
    assert_eq!(balance(&pool, val_acct).await, 500);

    // Adjust avg to $5 (same). delta = 0.
    let key = fresh_uuid(&pool).await;
    let doc_id = cost_adjust(&pool, &sku, &loc, "USD", "fg", 5, "2026-04-15", &key)
        .await
        .expect("no-op");

    assert_eq!(balance(&pool, val_acct).await, 500, "pool unchanged");
    assert_eq!(balance(&pool, var_acct).await, 0, "variance untouched");

    // Audit row exists with delta=0.
    let delta: i64 = sqlx::query_scalar(
        "SELECT delta_value FROM inventory_cost_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .expect("audit");
    assert_eq!(delta, 0);

    // No cost_adjustment transfer was posted.
    let transfer_count: i64 = sqlx::query_scalar(
        "SELECT count(*)::BIGINT FROM posting_lines
          WHERE document_id = $1::UUID AND reason = 'cost_adjustment'",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(transfer_count, 0, "no transfer posted on delta=0");
}

#[tokio::test]
async fn write_up_then_write_down_nets_correctly() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    seed_wac_pool(&pool, &sku, &loc, 100, 5).await;  // pool: 100 @ $5

    // Up to $7: delta = +200.
    cost_adjust(&pool, &sku, &loc, "USD", "fg", 7, "2026-04-15",
                &fresh_uuid(&pool).await).await.expect("up");
    assert_eq!(balance(&pool, val_acct).await, 700);
    assert_eq!(balance(&pool, var_acct).await, -200, "gain so far");

    // Down to $4: delta = 100*4 - 700 = -300.
    cost_adjust(&pool, &sku, &loc, "USD", "fg", 4, "2026-04-15",
                &fresh_uuid(&pool).await).await.expect("down");
    assert_eq!(balance(&pool, val_acct).await, 400);
    // Variance: -200 + 300 = +100 (net loss).
    assert_eq!(balance(&pool, var_acct).await, 100, "net loss");
}

// ---- Cost-method gating ----

#[tokio::test]
async fn standard_sku_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-A").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0011", || async {
        cost_adjust(&pool, &sku, &loc, "USD", "fg", 50, "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn fifo_sku_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0006", || async {
        cost_adjust(&pool, &sku, &loc, "USD", "fg", 5, "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}

// ---- Pool state errors ----

#[tokio::test]
async fn empty_pool_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let key = fresh_uuid(&pool).await;

    // Pool is empty (no seed).
    expect_sqlstate("P0010", || async {
        cost_adjust(&pool, &sku, &loc, "USD", "fg", 5, "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn negative_target_unit_cost_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    seed_wac_pool(&pool, &sku, &loc, 100, 5).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        cost_adjust(&pool, &sku, &loc, "USD", "fg", -1, "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}

// ---- Idempotency ----

#[tokio::test]
async fn replay_returns_same_id_no_double_post() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    seed_wac_pool(&pool, &sku, &loc, 100, 5).await;

    let key = fresh_uuid(&pool).await;
    let id1 = cost_adjust(&pool, &sku, &loc, "USD", "fg", 7, "2026-04-15", &key)
        .await
        .expect("first");
    let id2 = cost_adjust(&pool, &sku, &loc, "USD", "fg", 7, "2026-04-15", &key)
        .await
        .expect("replay");

    assert_eq!(id1, id2, "same idempotency key returns same id");
    // Pool moved exactly once: 100*7 = 700 (not 900 if double-posted).
    assert_eq!(balance(&pool, val_acct).await, 700);
}

// ---- Class routing ----

#[tokio::test]
async fn class_raw_revalues_inv_value_raw() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-WAC").await;
    let loc = loc_id(&pool, "MAIN").await;
    let raw_acct = account_id_for_selector(
        &pool, "inv_value_raw", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let fg_acct = account_id_for_selector(
        &pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    // Seed a raw pool directly via post_inventory_adjustment with class=raw.
    let posted_by = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 100::BIGINT, 5::BIGINT, 'USD',
            'raw', '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    )
    .bind(&sku)
    .bind(&loc)
    .bind(&posted_by)
    .bind(fresh_uuid(&pool).await)
    .fetch_one(&pool)
    .await
    .expect("seed raw");

    // Cost-adjust the raw pool.
    cost_adjust(&pool, &sku, &loc, "USD", "raw", 7, "2026-04-15",
                &fresh_uuid(&pool).await).await.expect("revalue raw");

    assert_eq!(balance(&pool, raw_acct).await, 700, "raw revalued");
    assert_eq!(balance(&pool, fg_acct).await, 0, "fg untouched");
}
