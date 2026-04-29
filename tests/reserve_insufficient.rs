//! Reserve insert is gated by `qty_promisable >= qty`. When stock is
//! short, the inner SELECT yields zero rows so the INSERT inserts
//! nothing — RETURNING is empty, the caller infers "out of stock"
//! from a 0-row count (doc Part IV §3.3).

mod common;

use common::*;

#[tokio::test]
async fn insufficient_stock_inserts_no_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 5).await;

    let so_id = fresh_sales_order(&pool).await;
    let line = fresh_uuid(&pool).await;

    let result = try_reserve(&pool, "SKU-A", "MAIN", 10, &so_id, &line).await;
    assert!(result.is_none(), "expected no row inserted, got {result:?}");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_reservations")
        .fetch_one(&pool)
        .await
        .expect("count reservations");
    assert_eq!(count, 0, "no reservation row should exist");
}
