//! Acceptance tests for slot-loss detection and recovery (acct-1vur.2; 0026,
//! design-v3.2 §6/§7).
//!
//! `max_slot_wal_keep_size` is deliberately finite, so the cluster may
//! invalidate the feed slot rather than let an abandoned consumer pin WAL
//! without bound. `ensure_slot` then recreates it at the CURRENT WAL
//! position, silently skipping every event decodable in the gap: those events
//! never lower a recost floor and never mark their pool dirty, so they are
//! never re-folded. The alarm the design relies on only fires if the event is
//! eventually delivered — so this is the one genuinely silent path left.
//!
//! This binary covers the DETECTION half (acct-1vur.2 a/c): the gauge reports
//! absence as a value rather than an empty result, and an unusable slot fails
//! loud instead of spinning in a retry loop. The recovery half is held on a
//! design decision (acct-1vur.2b) — see 0026's header.

mod common;

use common::*;

/// The gauge must report a MISSING slot as a value, not as an empty result —
/// absence is the state an operator most needs to see, and 0014's `feed_lag`
/// returns zero rows for it.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn health_gauge_reports_absence_as_a_row() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    drop_feed_slot(&pool).await;

    let (present, unhealthy): (bool, bool) =
        sqlx::query_as("SELECT present, unhealthy FROM feed_slot_health")
            .fetch_one(&pool)
            .await
            .expect("gauge emits a row with no slot");
    assert!(!present, "absence is reported");
    assert!(unhealthy, "and flagged unhealthy");
    // 0014's gauge, by contrast, is empty — which is the bug this fixes.
    assert_eq!(count(&pool, "SELECT count(*) FROM feed_lag").await, 0);

    let consumer = reset_feed(&pool).await;
    let (present, unhealthy, lag): (bool, bool, Option<i64>) =
        sqlx::query_as("SELECT present, unhealthy, lag_bytes FROM feed_slot_health")
            .fetch_one(&pool)
            .await
            .expect("gauge with a slot");
    assert!(present && !unhealthy, "a live slot reads healthy");
    assert!(lag.is_some(), "and carries a real lag reading");
    let _ = consumer;
    drop_feed_slot(&pool).await;
}

/// An invalidated slot is terminal, not transient: retrying the peek can only
/// spin, because the WAL it never delivered has been discarded.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn an_absent_slot_fails_loud_rather_than_retrying() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    drop_feed_slot(&pool).await;

    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    let err = consumer.ingest_once(10_000).await.expect_err("absent slot must fail loud");
    assert!(
        matches!(err, ledger_feed::FeedError::SlotLost { .. }),
        "distinguishable from a transient error: {err}"
    );
    assert!(format!("{err}").contains("cannot recover"), "says why retrying is futile: {err}");
}
