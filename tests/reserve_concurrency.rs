//! Concurrent reserves against the same (sku, location) MUST NOT
//! over-promise. The `FOR UPDATE` row lock on the stock_available
//! account row in the §3.3 CTE serializes concurrent callers — each
//! sees the previous winner's INSERT in its `qty_promisable` sum.
//!
//! Setup: on-hand=10, five tasks each request 3 units. Deterministic
//! outcome under correct serialization is exactly 3 successes (sum=9)
//! and 2 failures. Anything else means the lock leaked.

mod common;

use common::*;

#[tokio::test]
async fn concurrent_reserves_never_over_promise() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 10).await;
    let so_id = fresh_sales_order(&pool).await;

    let mut handles = Vec::with_capacity(5);
    for _ in 0..5 {
        let pool_c = pool.clone();
        let so_id_c = so_id.clone();
        let line = fresh_uuid(&pool).await;
        handles.push(tokio::spawn(async move {
            try_reserve(&pool_c, "SKU-A", "MAIN", 3, &so_id_c, &line).await
        }));
    }

    let mut successes = 0_usize;
    for h in handles {
        if h.await.expect("task panic").is_some() {
            successes += 1;
        }
    }

    let total_reserved: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty), 0)::BIGINT
           FROM inventory_reservations
          WHERE status = 'active'",
    )
    .fetch_one(&pool)
    .await
    .expect("sum active reservations");

    assert!(
        total_reserved <= 10,
        "over-promise: total_reserved={total_reserved} > on-hand=10"
    );
    assert_eq!(
        total_reserved,
        (successes as i64) * 3,
        "DB sum and success count must agree (no torn writes); successes={successes}, total={total_reserved}"
    );
    assert_eq!(
        successes, 3,
        "expected exactly 3 successes under correct FOR UPDATE serialization (5×3=15 vs on-hand=10), got {successes}"
    );
}
