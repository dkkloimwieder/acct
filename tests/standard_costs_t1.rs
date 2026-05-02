//! T1 probes for `standard_costs` (migration 0027).

mod common;

use common::*;
use sqlx::PgPool;

async fn sku_id(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-A'")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn insert_std_cost(
    pool: &PgPool,
    sku: &str,
    cost: i64,
    effective_at: &str,
    idempotency_key: &str,
) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, $3::DATE, '00000000-0000-0000-0000-0000000000bb'::UUID, $4::UUID)",
    )
    .bind(sku)
    .bind(cost)
    .bind(effective_at)
    .bind(idempotency_key)
    .execute(pool)
    .await
}

#[tokio::test]
async fn negative_cost_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = sku_id(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23514", || async {
        insert_std_cost(&pool, &sku, -1, "2026-04-15", &key).await
    })
    .await;
}

#[tokio::test]
async fn zero_cost_allowed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = sku_id(&pool).await;
    let key = fresh_uuid(&pool).await;

    // cost CHECK is >= 0, so 0 is allowed (zero-cost SKUs e.g. samples).
    insert_std_cost(&pool, &sku, 0, "2026-04-15", &key)
        .await
        .expect("zero cost should be valid");
}

#[tokio::test]
async fn idempotency_key_unique_collision() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = sku_id(&pool).await;
    let key = fresh_uuid(&pool).await;

    insert_std_cost(&pool, &sku, 100, "2026-04-15", &key)
        .await
        .expect("first insert");

    expect_sqlstate("23505", || async {
        insert_std_cost(&pool, &sku, 200, "2026-04-16", &key).await
    })
    .await;
}

#[tokio::test]
async fn unknown_sku_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus_sku = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23503", || async {
        insert_std_cost(&pool, &bogus_sku, 100, "2026-04-15", &key).await
    })
    .await;
}

#[tokio::test]
async fn null_required_field_violates_not_null() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = sku_id(&pool).await;

    // Missing idempotency_key (NOT NULL).
    expect_sqlstate("23502", || async {
        sqlx::query(
            "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by)
             VALUES ($1::UUID, 100, '2026-04-15', '00000000-0000-0000-0000-0000000000bb'::UUID)",
        )
        .bind(&sku)
        .execute(&pool)
        .await
    })
    .await;
}
