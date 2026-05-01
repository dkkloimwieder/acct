//! `acct-8qi` / `acct-og1.1` — smoke tests for cost_adjust_retroactive.
//!
//! Two smoke cases per og1.1 acceptance:
//!   1. queue → close → variance posted, queue finalized.
//!   2. target_period closed at queue time → P0021 (ref acct-7h4).
//!
//! Full matrix (idempotency, method-agnostic, variance routing,
//! double-correction with wac_periodic, etc.) is og1.2.

mod common;

use common::*;

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
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

#[allow(clippy::too_many_arguments)]
async fn queue_retro(
    pool: &sqlx::PgPool,
    target_period: i64,
    sku: &str,
    loc: &str,
    target_avg: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment_retroactive(
            $1::BIGINT, $2::UUID, $3::UUID, 'USD', 'fg',
            $4::BIGINT, $5::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(target_period)
    .bind(sku)
    .bind(loc)
    .bind(target_avg)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

/// Seed wac_perpetual pool 100u @ $5, then deplete 30u (provisional cost
/// 30 × 5 = 150). Returns (sku_id, loc_id, val_acct_id).
async fn seed_pool_with_depletion(pool: &sqlx::PgPool) -> (String, String, i64) {
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-WAC'")
        .fetch_one(pool).await.expect("sku");
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool).await.expect("loc");

    // Receive 100 @ $5.
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 100::BIGINT, 5::BIGINT, 'USD', 'fg',
            '2026-04-05'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    )
    .bind(&sku).bind(&loc).bind(&posted_by).bind(&key)
    .fetch_one(pool).await.expect("receive");

    // Deplete 30 (running avg=$5).
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, -30::BIGINT, NULL::BIGINT, 'USD', 'fg',
            '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    )
    .bind(&sku).bind(&loc).bind(&posted_by).bind(&key)
    .fetch_one(pool).await.expect("deplete");

    let val_acct = account_id_for_selector(
        pool, "inv_value_fg", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    ).await;

    (sku, loc, val_acct)
}

#[tokio::test]
async fn queue_then_close_posts_variance_and_finalizes() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (sku, loc, val_acct) = seed_pool_with_depletion(&pool).await;

    // Pool state: 70u value 350 (received 500, depleted 150).
    assert_eq!(balance(&pool, val_acct).await, 350);

    // Queue retroactive adjustment for the depletion: target_avg = $7.
    // Variance for the in-period depletion = 30 × (7 - 5) = 60 (write-up).
    let pid = period_id(&pool, "2026-04").await;
    let key = fresh_uuid(&pool).await;
    let queue_id = queue_retro(&pool, pid, &sku, &loc, 7, "2026-04-15", &key)
        .await
        .expect("queue");

    // Pre-close: queue exists, no variance posted yet.
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0, "no variance posted at queue time");
    assert_eq!(balance(&pool, val_acct).await, 350, "pool unchanged at queue time");

    // Close the period.
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close");

    // Hook reports 1 queue row finalized.
    assert_eq!(
        summary["hook_results"]["cost_adjust_retroactive"].as_i64(),
        Some(1),
        "one queue row finalized"
    );

    // Pool got an additional -60 (write-up: pool credited via variance).
    // Wait — write-up means original_debit += variance, pool -= variance.
    // For inventory_adjustment depletion the original_debit was inv_adj_expense
    // (creation_void on value-side via post_inventory_adjustment), so the
    // pool side gets a CREDIT of 60 → pool balance goes 350 - 60 = 290.
    assert_eq!(balance(&pool, val_acct).await, 290,
               "pool: 350 - 60 (extra credit from variance routing)");

    // variance_cost_adjust_retro nets to zero per close (2-transfer routing).
    assert_eq!(balance(&pool, var_acct).await, 0,
               "variance_cost_adjust_retro nets to zero");

    // Queue row finalized with totals.
    let (finalized_at_set, dep_count, total_var): (bool, i64, i64) = sqlx::query_as(
        "SELECT (finalized_at IS NOT NULL), finalized_count, total_variance
           FROM inventory_cost_adjustments_retroactive WHERE id = $1::UUID",
    )
    .bind(&queue_id)
    .fetch_one(&pool)
    .await
    .expect("queue row");
    assert!(finalized_at_set, "finalized_at stamped");
    assert_eq!(dep_count, 1, "one depletion processed");
    assert_eq!(total_var, 60, "total variance = 30 × (7 - 5)");
}

#[tokio::test]
async fn queueing_against_closed_period_raises_p0021() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Close the period first (no provisional rows, no recon alerts in
    // baseline fixture).
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close");

    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-WAC'")
        .fetch_one(&pool).await.expect("sku");
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool).await.expect("loc");
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("P0021", || async {
        queue_retro(&pool, pid, &sku, &loc, 7, "2026-04-15", &key)
            .await
            .map(|_| ())
    })
    .await;
}
