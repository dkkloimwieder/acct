//! acct-p0d8 acceptance: affinity-grouping + trx_seq monotonicity.
//!
//! design-v3 §8.3 second bullet: "Concurrent submissions to
//! overlapping pools: verify affinity grouping (one commit_group
//! handles all overlap, single committer per group)." Plus plan §G X7
//! invariant on trx_seq monotonicity per pool.
//!
//! Verifies:
//!   - N submissions to the same pool pack into a SMALLER count of
//!     commit_groups than naive 1-per-group (router_envelope_histogram
//!     bucket >= 1 has nonzero count)
//!   - All N submissions land as trx rows
//!   - trx_seq values on the touched pool are strictly monotonic in
//!     1..=N (PK on (pool_id, trx_seq) plus the per-pool MAX-scan
//!     allocator from §13.1)
//!   - Disjoint-pool submissions don't false-share a commit_group (the
//!     converse property — submissions to different pools may pack
//!     into one group too via union-find, but the trx_seq counters
//!     remain independent per pool)

mod common;

use common::*;
use std::time::Duration;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn overlapping_submissions_pack_and_seq_monotonic() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    // Send 8 submissions to the SAME pool, back-to-back. The router
    // window scan should pack them by union-find — affinity puts every
    // submission touching pool_id into the same group. Sequential
    // enqueue is fast (single staging SPI) and typically all 8 land
    // within one router tick (50ms).
    const N: i64 = 8;
    for i in 0..N {
        enqueue(
            &pool,
            "po_receipt",
            700 + i,
            &format!("2026-05-22T12:00:{:02}+00:00", i),
            &po_receipt_line(pool_id, 1, 10, inv, ap),
        )
        .await
        .expect("enqueue");
    }

    // Wait for processing to drain.
    for i in 0..N {
        assert!(
            poll_for_trx(&pool, "po_receipt", 700 + i, Duration::from_secs(15))
                .await
                .is_some(),
            "trx (po_receipt, {}) must materialize",
            700 + i
        );
    }

    // trx_seq strictly monotonic 1..=N for this pool.
    let seqs: Vec<i64> = sqlx::query_scalar(
        "SELECT trx_seq FROM trx_line WHERE pool_id = $1 ORDER BY trx_seq",
    )
    .bind(pool_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(seqs.len(), N as usize, "must be N trx_lines on pool");
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(
            *s,
            (i + 1) as i64,
            "trx_seq must be contiguous 1..=N; got {seqs:?}"
        );
    }

    // Affinity-pack evidence: router_envelope_histogram has buckets
    // >=1 (size >= 2) populated. If every commit_group were size 1,
    // only bucket 0 would have a nonzero count.
    let rows: Vec<(i32, i32, i32, i64)> = sqlx::query_as(
        "SELECT bucket, lower, upper, count \
           FROM ledger_routed_router_envelope_histogram() \
          ORDER BY bucket",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let multi_bucket_count: i64 = rows.iter().skip(1).map(|r| r.3).sum();
    assert!(
        multi_bucket_count > 0,
        "router must produce at least one multi-envelope commit_group; \
         histogram: {rows:?}"
    );

    // Sanity on overall pool state — N receipts of qty=1 unit_cost=10
    // → WAC pool ends at qty=N unit_cost=10.
    let (ps_qty, ps_uc): (i64, i64) =
        sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(ps_qty, N);
    // WAC stores value_sum (N receipts × qty=1 × unit_cost=10 = N×10 = 80).
    assert_eq!(ps_uc, N * 10);
}

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn disjoint_pools_keep_independent_trx_seq() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );

    // Seed two completely disjoint pools (different SKUs and locations).
    let (_, _, pool_a, inv_a, ap_a) = seed_fixture(&pool, "wac").await;

    // Second pool via additional sku/location/account rows.
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
    let inv_b: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('1001', 'Inventory B', 'asset') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let ap_b: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('2001', 'AP B', 'liability') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let pool_b: i64 = sqlx::query_scalar(
        "INSERT INTO pool (sku_id, location_id, method) \
         VALUES ($1, $2, 'wac'::pool_method) RETURNING id",
    )
    .bind(sku2)
    .bind(loc2)
    .fetch_one(&pool)
    .await
    .unwrap();

    // 3 submissions to pool_a, 2 to pool_b, interleaved.
    let _ = enqueue(&pool, "po_receipt", 801, "2026-05-22T12:00:00+00:00", &po_receipt_line(pool_a, 1, 5, inv_a, ap_a)).await.unwrap();
    let _ = enqueue(&pool, "po_receipt", 901, "2026-05-22T12:00:00+00:00", &po_receipt_line(pool_b, 1, 7, inv_b, ap_b)).await.unwrap();
    let _ = enqueue(&pool, "po_receipt", 802, "2026-05-22T12:00:01+00:00", &po_receipt_line(pool_a, 1, 5, inv_a, ap_a)).await.unwrap();
    let _ = enqueue(&pool, "po_receipt", 902, "2026-05-22T12:00:01+00:00", &po_receipt_line(pool_b, 1, 7, inv_b, ap_b)).await.unwrap();
    let _ = enqueue(&pool, "po_receipt", 803, "2026-05-22T12:00:02+00:00", &po_receipt_line(pool_a, 1, 5, inv_a, ap_a)).await.unwrap();

    for sid in [801, 802, 803, 901, 902] {
        assert!(
            poll_for_trx(&pool, "po_receipt", sid, Duration::from_secs(15))
                .await
                .is_some(),
            "trx {sid} must materialize"
        );
    }

    let seqs_a: Vec<i64> = sqlx::query_scalar(
        "SELECT trx_seq FROM trx_line WHERE pool_id = $1 ORDER BY trx_seq",
    )
    .bind(pool_a)
    .fetch_all(&pool)
    .await
    .unwrap();
    let seqs_b: Vec<i64> = sqlx::query_scalar(
        "SELECT trx_seq FROM trx_line WHERE pool_id = $1 ORDER BY trx_seq",
    )
    .bind(pool_b)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(seqs_a, vec![1, 2, 3]);
    assert_eq!(seqs_b, vec![1, 2]);
}
