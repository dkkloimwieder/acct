//! Acceptance tests for the feed consumer (phase 3, acct-qm7o.3).
//!
//! Covers the D8 advance-on-ingestion contract end-to-end against a live
//! slot: dirty-set population, recost-floor lowering with the R-1
//! `(posted_at, id)` tiebreak, at-least-once redelivery idempotency
//! (crash-between-apply-and-advance), the empty-batch cursor advance that
//! keeps G1 honest, and the three gauge views.

mod common;

use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn ensure_slot_is_idempotent() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    assert!(!consumer.ensure_slot().await.expect("second ensure_slot"));
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn single_receipt_marks_pool_once() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    receipt_at(&pool, f.pool_id, 1001, T12, 5, 100).await;

    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.inserts, 1);
    assert_eq!(report.pools_marked, 1);
    assert_eq!(report.floors_lowered, 0);
    assert!(report.advanced_to.is_some());
    assert_eq!(queue_pools(&pool).await, vec![1]);

    let first_enqueued: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT enqueued_at FROM recalc_queue WHERE pool_id = 1")
            .fetch_one(&pool)
            .await
            .expect("enqueued_at");

    // Nothing new: the cursor is at the frontier and the mark is untouched.
    let report = consumer.ingest_once(10_000).await.expect("re-ingest");
    assert_eq!(report.inserts, 0);
    assert_eq!(report.pools_marked, 0);
    let second_enqueued: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT enqueued_at FROM recalc_queue WHERE pool_id = 1")
            .fetch_one(&pool)
            .await
            .expect("enqueued_at");
    assert_eq!(first_enqueued, second_enqueued, "re-ingest must not touch the mark");
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn multi_pool_batch_dedupes_marks() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    for pid in 1..=3 {
        seed_pool(&pool, pid, pid, pid, "fifo", "running_avg").await;
    }
    let consumer = reset_feed(&pool).await;

    // Pool 2 is touched by both transactions (and twice in the second): the
    // dirty-set still ends up with exactly one row per pool.
    submit_at(
        &pool,
        "po_receipt",
        2001,
        T12,
        json!([
            line(1, "po_receipt_line", 5, 100),
            line(2, "po_receipt_line", 7, 110),
        ]),
    )
    .await;
    submit_at(
        &pool,
        "po_receipt",
        2002,
        T12,
        json!([
            line(2, "po_receipt_line", 3, 120),
            line(2, "po_receipt_line", 4, 130),
            line(3, "po_receipt_line", 9, 140),
        ]),
    )
    .await;

    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.inserts, 5);
    assert_eq!(report.pools_marked, 3);
    assert_eq!(queue_pools(&pool).await, vec![1, 2, 3]);
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn staging_drain_events_feed_identically() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_pool(&pool, 2, 2, 2, "wac", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    // The feed is entry-point-agnostic: events landing via the staging
    // committer are trx_line inserts like any other.
    enqueue(&pool, "po_receipt", 3001, json!([line(1, "po_receipt_line", 5, 100)])).await;
    enqueue(&pool, "po_receipt", 3002, json!([line(2, "po_receipt_line", 8, 200)])).await;
    assert_eq!(drain(&pool, 10).await, 2);

    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.inserts, 2);
    assert_eq!(queue_pools(&pool).await, vec![1, 2]);
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn backdated_event_lowers_recost_floor() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    // The pool is settled through T13; ids far above anything this test makes.
    set_settled_through(&pool, f.pool_id, T13, 1_000_000).await;

    // Backdated to T11 → floor lands on that event.
    let ev1 = receipt_at(&pool, f.pool_id, 4001, T11, 5, 100).await;
    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.floors_lowered, 1);
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T11), ev1)));

    // Deeper backdate to T09 → floor lowers again.
    let ev2 = receipt_at(&pool, f.pool_id, 4002, T09, 5, 100).await;
    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.floors_lowered, 1);
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T09), ev2)));

    // Between the floor and the frontier (T11 < T12 < T13): behind the
    // frontier but not below the floor → floor unchanged.
    receipt_at(&pool, f.pool_id, 4003, T12, 5, 100).await;
    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.floors_lowered, 0);
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T09), ev2)));

    // Ahead of the frontier (T14 > T13) → not backdated, floor unchanged,
    // pool still dirty.
    receipt_at(&pool, f.pool_id, 4004, T14, 5, 100).await;
    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.floors_lowered, 0);
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T09), ev2)));
    assert_eq!(queue_pools(&pool).await, vec![1]);
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn same_timestamp_uses_id_tiebreak() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    // Frontier at (T12, 0): an event AT T12 has id > 0, so
    // (T12, id) > (T12, 0) — not backdated.
    set_settled_through(&pool, f.pool_id, T12, 0).await;
    receipt_at(&pool, f.pool_id, 5001, T12, 5, 100).await;
    consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(floor_of(&pool, f.pool_id).await, None);

    // Frontier at (T12, huge): the same-timestamp event now sorts BEFORE the
    // frontier → floor lands on it.
    set_settled_through(&pool, f.pool_id, T12, 1_000_000).await;
    let ev = receipt_at(&pool, f.pool_id, 5002, T12, 5, 100).await;
    consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T12), ev)));
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn pool_without_settlement_row_gets_no_floor() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    // Never settled → nothing is "backdated"; the mark is the whole signal.
    receipt_at(&pool, f.pool_id, 6001, T09, 5, 100).await;
    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.floors_lowered, 0);
    assert_eq!(queue_pools(&pool).await, vec![1]);
    assert_eq!(count(&pool, "SELECT count(*) FROM pool_settlement").await, 0);
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn crash_between_apply_and_advance_redelivers_idempotently() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;
    set_settled_through(&pool, f.pool_id, T13, 1_000_000).await;

    let ev = receipt_at(&pool, f.pool_id, 7001, T11, 5, 100).await;
    receipt_at(&pool, f.pool_id, 7002, T12, 3, 110).await;

    // Simulated crash: apply the batch durably but never advance the cursor.
    let batch = consumer.peek(10_000).await.expect("peek");
    assert_eq!(batch.events.len(), 2);
    consumer.apply(&batch.events).await.expect("apply");
    assert_eq!(queue_pools(&pool).await, vec![1]);
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T11), ev)));

    // Recovery tick: the slot re-delivers the same events; re-applying them
    // changes nothing, and the cursor finally advances.
    let report = consumer.ingest_once(10_000).await.expect("recovery ingest");
    assert_eq!(report.inserts, 2, "un-advanced cursor must re-deliver");
    assert_eq!(report.pools_marked, 0);
    assert_eq!(report.floors_lowered, 0);
    assert!(report.advanced_to.is_some());
    assert_eq!(queue_pools(&pool).await, vec![1]);
    assert_eq!(floor_of(&pool, f.pool_id).await, Some((ts(T11), ev)));

    // And now the stream is clean.
    let report = consumer.ingest_once(10_000).await.expect("post-recovery ingest");
    assert_eq!(report.inserts, 0);
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn empty_batch_still_advances_cursor() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    // WAL with nothing published in it: unpublished-table writes.
    for source_id in 8001..8020 {
        enqueue(&pool, "po_receipt", source_id, json!([])).await;
    }

    let before: String =
        sqlx::query_scalar("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1")
            .bind(SLOT)
            .fetch_one(&pool)
            .await
            .expect("confirmed_flush before");

    let report = consumer.ingest_once(10_000).await.expect("ingest");
    assert_eq!(report.inserts, 0);
    assert!(report.advanced_to.is_some(), "cursor must advance past unpublished WAL");

    let moved: bool = sqlx::query_scalar(
        "SELECT confirmed_flush_lsn > $2::pg_lsn \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(SLOT)
    .bind(&before)
    .fetch_one(&pool)
    .await
    .expect("confirmed_flush after");
    assert!(moved, "confirmed_flush_lsn must move past unpublished WAL (G1 honesty)");

    // Loss-free: a published event after the empty-batch advance still arrives.
    receipt_at(&pool, f.pool_id, 8500, T12, 5, 100).await;
    let report = consumer.ingest_once(10_000).await.expect("ingest after advance");
    assert_eq!(report.inserts, 1);
    assert_eq!(queue_pools(&pool).await, vec![1]);
    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn gauges_read_correctly_under_lag() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f1 = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let f2 = seed_pool(&pool, 2, 2, 2, "fifo", "running_avg").await;
    let consumer = reset_feed(&pool).await;

    // G1 exists as soon as the slot does.
    let (slot_name, lag_bytes): (String, i64) =
        sqlx::query_as("SELECT slot_name, lag_bytes FROM feed_lag")
            .fetch_one(&pool)
            .await
            .expect("feed_lag row");
    assert_eq!(slot_name, SLOT);
    assert!(lag_bytes >= 0);

    // Pool 1: settled between its second and third events → 1 unsettled.
    let ev1 = receipt_at(&pool, f1.pool_id, 9001, T10, 5, 100).await;
    receipt_at(&pool, f1.pool_id, 9002, T11, 3, 110).await;
    receipt_at(&pool, f1.pool_id, 9003, T13, 2, 120).await;
    set_settled_through(&pool, f1.pool_id, T12, 0).await;
    // Pool 2: never settled → everything unsettled.
    receipt_at(&pool, f2.pool_id, 9004, T12, 7, 90).await;

    consumer.ingest_once(10_000).await.expect("ingest");

    // G2c: both pools dirty; the oldest mark timestamp is populated.
    let (dirty, oldest): (i64, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT dirty_pools, oldest_enqueued_at FROM recalc_queue_depth")
            .fetch_one(&pool)
            .await
            .expect("recalc_queue_depth");
    assert_eq!(dirty, 2);
    assert!(oldest.is_some());

    // G2a/G2b for pool 1: only the T13 event is past the frontier.
    let (unsettled, gross, tip_id): (i64, i64, i64) = sqlx::query_as(
        "SELECT unsettled_events, unsettled_gross_value, tip_id \
         FROM recalc_pool_lag WHERE pool_id = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("recalc_pool_lag pool 1");
    assert_eq!(unsettled, 1);
    assert_eq!(gross, 2 * 120);
    assert_eq!(tip_id, ev1 + 2, "tip is the latest event in (posted_at, id) order");

    // Pool 2 (no settlement row): everything counts as unsettled.
    let (unsettled, gross): (i64, i64) = sqlx::query_as(
        "SELECT unsettled_events, unsettled_gross_value \
         FROM recalc_pool_lag WHERE pool_id = 2",
    )
    .fetch_one(&pool)
    .await
    .expect("recalc_pool_lag pool 2");
    assert_eq!(unsettled, 1);
    assert_eq!(gross, 7 * 90);
    drop_feed_slot(&pool).await;
}
