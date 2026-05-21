//! acct-sp11 acceptance: two concurrent submissions on a shared pool
//! serialize via pool_lock FOR UPDATE.
//!
//! Both sessions submit a receipt into the SAME pool; the pool_lock
//! row gates them sequentially. Verifies that:
//!   - both submissions succeed
//!   - trx_seq assignments are monotonic without collision (UNIQUE
//!     (pool_id, trx_seq) is the guardrail)
//!   - elapsed time exceeds 2× the artificial server-side delay,
//!     confirming serialization rather than parallelism

mod common;

use std::time::Instant;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn overlapping_pool_serializes() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fifo_fixture(&pool).await;

    // Use SELECT pg_sleep(0.2) inside a tx that ALSO calls
    // ledger_submit_trx to artificially extend each session's hold on
    // pool_lock. We wrap the whole thing in a function call:
    //   BEGIN;
    //   SELECT ledger_submit_trx(...);
    //   SELECT pg_sleep(0.2);
    //   COMMIT;

    let session = |source_id: i64| {
        let pool = pool.clone();
        let lines = po_receipt_line(pool_id, 1, 100, inv, ap);
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
    let a = session(1);
    let b = session(2);
    tokio::join!(a, b);
    let elapsed = start.elapsed();

    // Two 200ms holds; if they were parallel the wall-time would be
    // ~200ms, serialized ~400ms. Allow generous noise margin (300ms
    // floor) so this isn't flaky on busy CI.
    assert!(
        elapsed.as_millis() >= 300,
        "elapsed {elapsed:?} suggests parallel; pool_lock not serializing"
    );

    // Both submissions landed; trx_seq is monotonic 1 → 2.
    let trx_count: i64 = sqlx::query_scalar("SELECT count(*) FROM trx")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(trx_count, 2);
    let seqs: Vec<i64> =
        sqlx::query_scalar("SELECT trx_seq FROM trx_line WHERE pool_id = $1 ORDER BY trx_seq")
            .bind(pool_id)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(seqs, vec![1, 2]);
}
