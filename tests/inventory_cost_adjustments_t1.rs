//! T1 probes for `inventory_cost_adjustments` (migration 0024).

mod common;

use common::*;
use sqlx::PgPool;

async fn ids(pool: &PgPool) -> (String, String) {
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
    (sku, loc)
}

async fn insert_inv_cost_adj(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    inventory_class: &str,
    target_unit_cost: i64,
    pool_qty: i64,
    idempotency_key: &str,
) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
    sqlx::query(
        "INSERT INTO inventory_cost_adjustments
            (sku_id, location_id, currency, inventory_class,
             prior_unit_cost, target_unit_cost, delta_value, pool_qty,
             business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, $2::UUID, 'USD', $3, 100, $4, 0, $5,
                 '2026-04-15', '00000000-0000-0000-0000-0000000000bb'::UUID,
                 $6::UUID)",
    )
    .bind(sku)
    .bind(loc)
    .bind(inventory_class)
    .bind(target_unit_cost)
    .bind(pool_qty)
    .bind(idempotency_key)
    .execute(pool)
    .await
}

#[tokio::test]
async fn negative_target_unit_cost_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc) = ids(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23514", || async {
        insert_inv_cost_adj(&pool, &sku, &loc, "raw", -1, 10, &key).await
    })
    .await;
}

#[tokio::test]
async fn zero_pool_qty_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc) = ids(&pool).await;
    let key = fresh_uuid(&pool).await;

    // pool_qty CHECK is > 0; zero pool means there's nothing to revalue.
    expect_sqlstate("23514", || async {
        insert_inv_cost_adj(&pool, &sku, &loc, "raw", 100, 0, &key).await
    })
    .await;
}

#[tokio::test]
async fn invalid_inventory_class_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc) = ids(&pool).await;
    let key = fresh_uuid(&pool).await;

    // 'wip' is rejected at the table layer (the function also rejects
    // it for wac_periodic / wac_retroactive paths, but the CHECK fires
    // first for direct INSERTs).
    expect_sqlstate("23514", || async {
        insert_inv_cost_adj(&pool, &sku, &loc, "wip", 100, 10, &key).await
    })
    .await;
}

#[tokio::test]
async fn idempotency_key_unique_collision() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, loc) = ids(&pool).await;
    let key = fresh_uuid(&pool).await;

    insert_inv_cost_adj(&pool, &sku, &loc, "raw", 100, 10, &key)
        .await
        .expect("first insert");

    expect_sqlstate("23505", || async {
        insert_inv_cost_adj(&pool, &sku, &loc, "fg", 200, 5, &key).await
    })
    .await;
}

#[tokio::test]
async fn unknown_sku_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (_, loc) = ids(&pool).await;
    let bogus_sku = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23503", || async {
        insert_inv_cost_adj(&pool, &bogus_sku, &loc, "raw", 100, 10, &key).await
    })
    .await;
}

#[tokio::test]
async fn unknown_location_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _) = ids(&pool).await;
    let bogus_loc = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    expect_sqlstate("23503", || async {
        insert_inv_cost_adj(&pool, &sku, &bogus_loc, "raw", 100, 10, &key).await
    })
    .await;
}
