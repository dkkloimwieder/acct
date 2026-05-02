//! T1 probes for `inventory_cost_adjustments_retroactive` (migration 0032).

mod common;

use common::*;
use sqlx::PgPool;

async fn ctx(pool: &PgPool) -> (String, String, i64) {
    let sku: String =
        sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-A'")
            .fetch_one(pool)
            .await
            .unwrap();
    let loc: String =
        sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
            .fetch_one(pool)
            .await
            .unwrap();
    let period: i64 = sqlx::query_scalar("SELECT id FROM periods WHERE code = '2026-04'")
        .fetch_one(pool)
        .await
        .unwrap();
    (sku, loc, period)
}

async fn insert_retro(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    period_id: i64,
    inventory_class: &str,
    target_avg: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
    sqlx::query(
        "INSERT INTO inventory_cost_adjustments_retroactive
            (sku_id, location_id, currency, inventory_class, target_period_id,
             target_avg, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, $2::UUID, 'USD', $3, $4, $5, $6::DATE,
                 '00000000-0000-0000-0000-0000000000bb'::UUID, $7::UUID)",
    )
    .bind(sku)
    .bind(loc)
    .bind(inventory_class)
    .bind(period_id)
    .bind(target_avg)
    .bind(business_date)
    .bind(idempotency_key)
    .execute(pool)
    .await
}

#[tokio::test]
async fn negative_target_avg_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc, period) = ctx(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23514", || async {
        insert_retro(&pool, &sku, &loc, period, "raw", -1, "2026-04-15", &key).await
    })
    .await;
}

#[tokio::test]
async fn invalid_inventory_class_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc, period) = ctx(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23514", || async {
        insert_retro(&pool, &sku, &loc, period, "wip", 100, "2026-04-15", &key).await
    })
    .await;
}

#[tokio::test]
async fn idempotency_key_unique_collision() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc, period) = ctx(&pool).await;
    let key = fresh_uuid(&pool).await;

    insert_retro(&pool, &sku, &loc, period, "raw", 100, "2026-04-15", &key)
        .await
        .expect("first insert");

    expect_sqlstate("23505", || async {
        insert_retro(&pool, &sku, &loc, period, "fg", 50, "2026-04-16", &key).await
    })
    .await;
}

#[tokio::test]
async fn unknown_target_period_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc, _) = ctx(&pool).await;
    let key = fresh_uuid(&pool).await;
    // 999999 is unlikely to be a real period id.
    expect_sqlstate("23503", || async {
        insert_retro(&pool, &sku, &loc, 999999, "raw", 100, "2026-04-15", &key).await
    })
    .await;
}

#[tokio::test]
async fn unknown_sku_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (_, loc, period) = ctx(&pool).await;
    let bogus_sku = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23503", || async {
        insert_retro(&pool, &bogus_sku, &loc, period, "raw", 100, "2026-04-15", &key).await
    })
    .await;
}

#[tokio::test]
async fn unknown_location_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _, period) = ctx(&pool).await;
    let bogus_loc = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23503", || async {
        insert_retro(&pool, &sku, &bogus_loc, period, "raw", 100, "2026-04-15", &key).await
    })
    .await;
}
