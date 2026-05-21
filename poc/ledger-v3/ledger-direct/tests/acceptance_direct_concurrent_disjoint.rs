//! acct-sp11 acceptance: two concurrent submissions on disjoint pools
//! run in parallel.
//!
//! Inverse of acceptance_direct_concurrent_overlap. Each session
//! touches its own pool; pool_lock rows are distinct, so the two
//! transactions execute in parallel. Verifies that elapsed time is
//! roughly one hold-time rather than two — the contention floor is
//! NOT triggered.

mod common;

use std::time::Instant;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn disjoint_pools_run_in_parallel() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_a_id, inv, ap) = seed_fifo_fixture(&pool).await;

    // Seed a second sku + location + pool for the disjoint workload.
    let sku2: i64 = sqlx::query_scalar(
        "INSERT INTO sku (code, name) VALUES ('SKU-2', 'Test SKU 2') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let loc2: i64 = sqlx::query_scalar(
        "INSERT INTO location (code, name) VALUES ('LOC-2', 'Test Loc 2') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let pool_b: i64 = sqlx::query_scalar(
        "INSERT INTO pool (sku_id, location_id, method) VALUES ($1, $2, 'fifo') RETURNING id",
    )
    .bind(sku2)
    .bind(loc2)
    .fetch_one(&pool)
    .await
    .unwrap();

    let session = |source_id: i64, pid: i64| {
        let pool = pool.clone();
        let lines = po_receipt_line(pid, 1, 100, inv, ap);
        async move {
            let mut tx = pool.begin().await.unwrap();
            let json: serde_json::Value = serde_json::from_str(&lines).unwrap();
            sqlx::query("SELECT ledger_submit_trx('po_receipt', $1, '2026-05-21T12:00:00+00:00', $2::jsonb)")
                .bind(source_id)
                .bind(json)
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("SELECT pg_sleep(0.2)").execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
        }
    };

    let start = Instant::now();
    let a = session(1, pool_a_id);
    let b = session(2, pool_b);
    tokio::join!(a, b);
    let elapsed = start.elapsed();

    // Two 200ms holds in parallel ~200ms wall-time; serialized ~400ms.
    // 350ms ceiling is the parallelism floor.
    assert!(
        elapsed.as_millis() < 350,
        "elapsed {elapsed:?} suggests serial execution; disjoint pools should not block"
    );

    let trx_count: i64 = sqlx::query_scalar("SELECT count(*) FROM trx")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(trx_count, 2);
}
