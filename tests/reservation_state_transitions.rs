//! State-transition guard: every reservation transition (allocate,
//! cancel, expire) carries `WHERE status = 'active'`. Any attempt to
//! transition from a non-active state is a silent no-op (UPDATE
//! returns 0 affected rows). Doc Part IV §3.3.

mod common;

use common::*;
use sqlx::PgPool;

async fn update_status(pool: &PgPool, id: &str, new_status: &str) -> u64 {
    sqlx::query(
        "UPDATE inventory_reservations
            SET status = $2::reservation_status,
                resolved_at = clock_timestamp()
          WHERE id = $1::UUID
            AND status = 'active'",
    )
    .bind(id)
    .bind(new_status)
    .execute(pool)
    .await
    .expect("status update")
    .rows_affected()
}

#[tokio::test]
async fn transitions_only_apply_from_active() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 100).await;
    let so_id = fresh_sales_order(&pool).await;

    // active → allocated (1 row); subsequent attempts no-op.
    let r_alloc = try_reserve(
        &pool,
        "SKU-A",
        "MAIN",
        1,
        &so_id,
        &fresh_uuid(&pool).await,
    )
    .await
    .expect("reserve r_alloc");
    assert_eq!(update_status(&pool, &r_alloc, "allocated").await, 1);
    assert_eq!(update_status(&pool, &r_alloc, "allocated").await, 0);
    assert_eq!(update_status(&pool, &r_alloc, "cancelled").await, 0);
    assert_eq!(update_status(&pool, &r_alloc, "expired").await, 0);

    // active → cancelled.
    let r_cancel = try_reserve(
        &pool,
        "SKU-A",
        "MAIN",
        1,
        &so_id,
        &fresh_uuid(&pool).await,
    )
    .await
    .expect("reserve r_cancel");
    assert_eq!(update_status(&pool, &r_cancel, "cancelled").await, 1);
    assert_eq!(update_status(&pool, &r_cancel, "allocated").await, 0);
    assert_eq!(update_status(&pool, &r_cancel, "expired").await, 0);

    // active → expired.
    let r_exp = try_reserve(
        &pool,
        "SKU-A",
        "MAIN",
        1,
        &so_id,
        &fresh_uuid(&pool).await,
    )
    .await
    .expect("reserve r_exp");
    assert_eq!(update_status(&pool, &r_exp, "expired").await, 1);
    assert_eq!(update_status(&pool, &r_exp, "allocated").await, 0);
    assert_eq!(update_status(&pool, &r_exp, "cancelled").await, 0);

    // Final shape: one of each terminal status.
    let counts: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status::text, COUNT(*)::BIGINT
           FROM inventory_reservations
          GROUP BY status
          ORDER BY status",
    )
    .fetch_all(&pool)
    .await
    .expect("count by status");
    assert_eq!(
        counts,
        vec![
            ("allocated".to_string(), 1),
            ("cancelled".to_string(), 1),
            ("expired".to_string(), 1),
        ]
    );
}
