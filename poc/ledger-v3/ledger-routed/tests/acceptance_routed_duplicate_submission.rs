//! acct-p0d8 acceptance: duplicate (trx_type, source_id) submission.
//!
//! design-v3 §8.3 fifth bullet: "Duplicate submission: same (trx_type,
//! source_id) submitted twice; the second submission to reach Step 9
//! of §5.4 fails on UNIQUE constraint at INSERT trx; the committer
//! logs the conflict; affected commit_group's tx rolls back;
//! pristine-replay excludes the duplicate and re-runs."
//!
//! Verifies:
//!   - exactly one trx row exists after both submissions
//!   - pool_state reflects exactly ONE receipt's effects (not double)
//!   - the second submission is silently swallowed (no error to the
//!     enqueue caller — enqueue just stages the payload; the duplicate
//!     is detected inside the committer where pristine-replay handles
//!     it transparently)

mod common;

use common::*;
use std::time::Duration;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn duplicate_source_id_yields_one_trx_one_layer() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    // First submission lands cleanly.
    let _ = enqueue(
        &pool,
        "po_receipt",
        401,
        "2026-05-22T12:00:00+00:00",
        &po_receipt_line(pool_id, 7, 100, inv, ap),
    )
    .await
    .expect("first enqueue");

    let trx_id_first = poll_for_trx(&pool, "po_receipt", 401, Duration::from_secs(10))
        .await
        .expect("first trx materializes");

    // Second submission with identical (trx_type, source_id). The
    // enqueue itself succeeds (shmem staging is duplicate-blind); the
    // committer's bulk INSERT trx fires the UNIQUE constraint and
    // pristine-replay excludes this submission on retry.
    let _ = enqueue(
        &pool,
        "po_receipt",
        401,
        "2026-05-22T12:00:01+00:00",
        &po_receipt_line(pool_id, 7, 100, inv, ap),
    )
    .await
    .expect("second enqueue (shmem staging is duplicate-blind)");

    // Wait long enough for the duplicate to be picked up + rejected.
    // Several router ticks (50ms) + committer pipeline (low ms).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Exactly one trx row for (po_receipt, 401).
    let trx_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx WHERE trx_type = 'po_receipt'::trx_type AND source_id = 401",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        trx_count, 1,
        "exactly one trx must exist for (po_receipt, 401); got {trx_count}"
    );

    // The surviving trx is the first one (its id is preserved; the
    // duplicate's enqueue never reaches the trx table).
    let surviving_id: i64 = sqlx::query_scalar(
        "SELECT id FROM trx WHERE trx_type = 'po_receipt'::trx_type AND source_id = 401",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(surviving_id, trx_id_first);

    // Exactly one trx_line on this pool.
    let line_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM trx_line WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(line_count, 1, "duplicate must NOT produce a second trx_line");

    // pool_state shows ONE receipt's effect (qty=7, NOT 14).
    let (ps_qty, ps_uc): (i64, i64) =
        sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(ps_qty, 7, "pool_state qty must NOT double-count");
    assert_eq!(ps_uc, 100);
}

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn duplicate_does_not_block_subsequent_distinct_submission() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    let _ = enqueue(
        &pool,
        "po_receipt",
        501,
        "2026-05-22T12:00:00+00:00",
        &po_receipt_line(pool_id, 3, 10, inv, ap),
    )
    .await
    .unwrap();
    let _ = poll_for_trx(&pool, "po_receipt", 501, Duration::from_secs(10))
        .await
        .expect("first lands");

    // Send a duplicate of 501 AND a new distinct submission 502 in
    // quick succession. The committer should reject 501 (UNIQUE) and
    // process 502 normally — pristine-replay's exclude-and-retry must
    // not poison the rest of the commit_group.
    let _ = enqueue(
        &pool,
        "po_receipt",
        501,
        "2026-05-22T12:00:01+00:00",
        &po_receipt_line(pool_id, 3, 10, inv, ap),
    )
    .await
    .unwrap();
    let _ = enqueue(
        &pool,
        "po_receipt",
        502,
        "2026-05-22T12:00:02+00:00",
        &po_receipt_line(pool_id, 2, 20, inv, ap),
    )
    .await
    .unwrap();

    let id_502 = poll_for_trx(&pool, "po_receipt", 502, Duration::from_secs(10))
        .await
        .expect("502 must materialize despite duplicate 501 in the same window");
    assert!(id_502 > 0);

    let trx_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx WHERE trx_type = 'po_receipt'::trx_type \
                                      AND source_id IN (501, 502)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(trx_count, 2, "exactly 501 + 502, no extras");
}
