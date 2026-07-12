//! Acceptance tests for lazy per-pool backpressure (recalc-c §5,
//! acct-qm7o.7) — the §10 assertions, deterministically:
//!
//! 1. `engage_at_bound_rejects_both_paths_per_pool` — the feed engages a pool
//!    EXACTLY when its unsettled-event counter reaches the bound; from that
//!    point both admission surfaces reject with SQLSTATE 53400 (direct
//!    `ledger_submit_trx`, and the `ledger_inbox` BEFORE INSERT trigger), a
//!    multi-line submission touching the throttled pool rejects atomically,
//!    zero-qty lines don't count as appends, and a sibling pool is untouched
//!    (per-pool granularity, not global).
//! 2. `pre_admitted_envelopes_apply_during_throttle` — the staging drain
//!    applies envelopes admitted before engagement; admission is the only
//!    gate, accepted work is never retroactively failed.
//! 3. `settle_releases_at_low_water` — draining the pool resets the counter
//!    to the exact tail (0) and releases at the low-water mark; appends flow
//!    again.
//! 4. `mid_pass_arrivals_hold_throttle_above_low_water` — the hysteresis
//!    band, with a real interleaving (lock-wait polling on `pg_stat_activity`
//!    as the rendezvous): an engine pass is parked inside `settle()` while
//!    fresh events land and the FEED engages the pool mid-pass; when the pass
//!    resumes, its exact-count reset wipes the feed's skewed counter down to
//!    the true tail — which sits INSIDE the hysteresis band (low_water <
//!    tail < bound), so the throttle is retained. A second parked pass then
//!    pins the release boundary: a reset landing EXACTLY at the low-water
//!    mark releases (≤, not <), and the reset is the exact committed tail,
//!    not a decrement by the scanned count.
//! 5. `scope_and_stays_off` — wac pools never enter the counter or the
//!    throttled set (fifo/lifo scoping); a fifo pool below the bound never
//!    engages; and the engine's own `cost_adjustment_line` output is not
//!    counted when its feed delivery loops back (a counter-residue bug would
//!    show up here as a nonzero backlog at rest).
//! 6. `engine_engages_when_reset_lands_at_bound` — the reset writer applies
//!    the bound to its own value: events committed but NOT yet delivered by
//!    the feed (the feed-lag window) are counted by the pass's exact-tail
//!    reset, which must engage at the bound without any feed involvement;
//!    the late delivery then lands its stale counts on a released pool and
//!    the guaranteed follow-up pass wipes them (release after spurious
//!    re-engage).
//! 7. `late_delivery_of_settled_events_heals_via_trailing_mark` — the
//!    swallowed-mark interleaving, deterministically: a pass settles events
//!    whose feed delivery is still in flight; the delivery's counter bump is
//!    parked behind the pass's reset and lands on top of it (stale residue ≥
//!    bound ⇒ spurious engage), while its dirty mark — evaluated AFTER the
//!    bump — sees the queue row the pass deleted and re-inserts it. The
//!    healing pass the mark guarantees resets the counter to the true tail
//!    (0) and releases. Marks running before bumps would leave the pool
//!    permanently throttled with an empty tail and no queue row.

mod common;

use common::*;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool};
use std::time::Duration;

/// A dedicated session connection + its backend pid (for lock-wait polling).
async fn session() -> (PgConnection, i32) {
    let mut c = PgConnection::connect(POC_DSN).await.expect("session connect");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut c)
        .await
        .expect("pg_backend_pid");
    (c, pid)
}

/// Poll until the backend is provably parked on a heavyweight lock. Bounded:
/// a session that never parks means the choreography broke — fail, don't hang.
async fn wait_until_lock_blocked(pool: &PgPool, pid: i32) {
    for _ in 0..400 {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
              WHERE pid = $1 AND wait_event_type = 'Lock')",
        )
        .bind(pid)
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity");
        if blocked {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("backend {pid} never parked on a lock");
}

/// Raw ledger_inbox enqueue expecting the trigger's admission reject.
async fn enqueue_expect_err(pool: &PgPool, source_id: i64, lines: Value) -> sqlx::Error {
    sqlx::query(
        "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
         VALUES ('po_receipt', $1, $2::timestamptz, $3::jsonb)",
    )
    .bind(source_id)
    .bind(TS)
    .bind(lines)
    .execute(pool)
    .await
    .expect_err("enqueue to a throttled pool must be rejected")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn engage_at_bound_rejects_both_paths_per_pool() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f1 = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let f2 = seed_pool(&pool, 2, 2, 1, "fifo", "running_avg").await;
    set_backpressure_bounds(&pool, 5, 1).await;

    // Four events: one short of the bound.
    for i in 0..4 {
        receipt_at(&pool, f1.pool_id, 101 + i, T10, 10, 100).await;
    }
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, f1.pool_id).await, Some(4));
    assert_eq!(throttled(&pool, f1.pool_id).await, None, "below the bound stays open");

    // The fifth event's delivery crosses the bound: the feed engages in the
    // same apply transaction that recorded the mark.
    receipt_at(&pool, f1.pool_id, 105, T10, 10, 100).await;
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, f1.pool_id).await, Some(5));
    assert_eq!(throttled(&pool, f1.pool_id).await, Some(5), "engages exactly at the bound");

    // Direct path: single-line reject with the distinct SQLSTATE.
    let err = submit(&pool, "po_receipt", 106, json!([line(f1.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect_err("throttled pool must reject");
    assert_eq!(sqlstate(&err), "53400");

    // Direct path: a multi-line submission touching the throttled pool
    // rejects atomically — nothing lands, the clean pool's line included.
    let trx_before = count(&pool, "SELECT count(*) FROM trx").await;
    let err = submit(
        &pool,
        "po_receipt",
        107,
        json!([
            line(f2.pool_id, "po_receipt_line", 5, 100),
            line(f1.pool_id, "po_receipt_line", 5, 100),
        ]),
    )
    .await
    .expect_err("mixed submission touching a throttled pool must reject");
    assert_eq!(sqlstate(&err), "53400");
    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, trx_before, "atomic reject");

    // Zero-qty lines are no-op appends: they don't trip the gate.
    submit(
        &pool,
        "po_receipt",
        108,
        json!([
            line(f1.pool_id, "po_receipt_line", 0, 100),
            line(f2.pool_id, "po_receipt_line", 5, 100),
        ]),
    )
    .await
    .expect("zero-qty line on the throttled pool is not an append");

    // Staging path: the enqueue trigger rejects with the same SQLSTATE.
    let err =
        enqueue_expect_err(&pool, 109, json!([line(f1.pool_id, "po_receipt_line", 5, 100)])).await;
    assert_eq!(sqlstate(&err), "53400");

    // The admission check runs before ANY write: replaying an existing
    // (trx_type, source_id) into the throttled pool raises the throttle code,
    // not the idempotency backstop's 23505.
    let err = submit(&pool, "po_receipt", 101, json!([line(f1.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect_err("duplicate source into a throttled pool still rejects");
    assert_eq!(sqlstate(&err), "53400", "gate precedes the trx insert");

    // Per-pool granularity: the sibling pool flows on both paths.
    submit(&pool, "po_receipt", 110, json!([line(f2.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect("clean pool direct submit");
    enqueue(&pool, "po_receipt", 111, json!([line(f2.pool_id, "po_receipt_line", 5, 100)])).await;
    assert_eq!(drain(&pool, 10).await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'failed'").await, 0);

    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn pre_admitted_envelopes_apply_during_throttle() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    set_backpressure_bounds(&pool, 5, 1).await;

    // Admitted while the pool is open.
    let inbox_id =
        enqueue(&pool, "po_receipt", 201, json!([line(f.pool_id, "po_receipt_line", 2, 100)]))
            .await;

    // Engage via five direct events.
    for i in 0..5 {
        receipt_at(&pool, f.pool_id, 202 + i, T10, 10, 100).await;
    }
    ingest_all(&consumer).await;
    assert!(throttled(&pool, f.pool_id).await.is_some());

    // The drain applies the pre-admitted envelope unchecked.
    assert_eq!(drain(&pool, 10).await, 1);
    let status: String =
        sqlx::query_scalar("SELECT status FROM ledger_inbox WHERE id = $1")
            .bind(inbox_id)
            .fetch_one(&pool)
            .await
            .expect("inbox status");
    assert_eq!(status, "done", "admitted work is never retroactively failed");
    let (qty, _, _) = aggregate(&pool, f.pool_id).await.expect("aggregate");
    assert_eq!(qty, 52, "five direct receipts of 10 plus the pre-admitted 2");

    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn settle_releases_at_low_water() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    set_backpressure_bounds(&pool, 5, 1).await;

    for i in 0..5 {
        receipt_at(&pool, f.pool_id, 301 + i, T10, 10, 100).await;
    }
    ingest_all(&consumer).await;
    assert_eq!(throttled(&pool, f.pool_id).await, Some(5));

    // Draining to head resets the counter to the exact tail (0) and releases.
    let report = settle_pool(&pool, f.pool_id).await;
    assert_eq!(report["settled"], json!(true));
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0), "exact-count reset at settle");
    assert_eq!(throttled(&pool, f.pool_id).await, None, "released at the low-water mark");

    submit(&pool, "po_receipt", 306, json!([line(f.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect("released pool accepts appends again");

    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn mid_pass_arrivals_hold_throttle_above_low_water() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    // Hysteresis band: engage at 8, release at ≤ 1.
    set_backpressure_bounds(&pool, 8, 1).await;

    // Staged while the pool is open, for the second leg: the drain path is
    // admission-exempt, so these can land while the pool is throttled.
    for i in 0..3 {
        sqlx::query(
            "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
             VALUES ('po_receipt', $1, $2::timestamptz, $3::jsonb)",
        )
        .bind(410 + i)
        .bind(T14)
        .bind(json!([line(f.pool_id, "po_receipt_line", 10, 100)]))
        .execute(&pool)
        .await
        .expect("pre-stage second-leg envelope");
    }

    // Pre-settle once so the pool_settlement row exists to park on.
    receipt_at(&pool, f.pool_id, 401, T10, 10, 100).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));

    // Five more events: counter 5, inside the open band.
    for i in 0..5 {
        receipt_at(&pool, f.pool_id, 402 + i, T11, 10, 100).await;
    }
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(5));
    assert_eq!(throttled(&pool, f.pool_id).await, None);

    // s3 parks the settlement row so the engine pass blocks inside settle().
    let (mut c3, _) = session().await;
    sqlx::query("BEGIN").execute(&mut c3).await.unwrap();
    sqlx::query("SELECT pool_id FROM pool_settlement WHERE pool_id = $1 FOR UPDATE")
        .bind(f.pool_id)
        .execute(&mut c3)
        .await
        .unwrap();

    // s1: one engine pass — claims the queue row, scans the 5 events, parks
    // at settle() on s3's row lock.
    let (mut c1, pid1) = session().await;
    let t1 = tokio::spawn(async move {
        let report: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut c1)
            .await
            .expect("ledger_recalc_step");
        report
    });
    wait_until_lock_blocked(&pool, pid1).await;

    // Mid-park: three fresh events land and their delivery pushes the FEED's
    // counter to 8 — the feed engages while the pass is still in flight. The
    // feed's counter upsert doesn't block: the pass hasn't reached its own
    // backlog write yet. The events MUST be forward-dated (above the parked
    // pass's frontier): a backdated mid-park event would make the feed's
    // floor UPDATE block on s3's pool_settlement lock while this thread is
    // what unparks s3 — a harness-level deadlock PG cannot detect.
    for i in 0..3 {
        receipt_at(&pool, f.pool_id, 407 + i, T13, 10, 100).await;
    }
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(8));
    assert_eq!(throttled(&pool, f.pool_id).await, Some(8), "feed engages mid-pass");

    // Unpark. The pass settles through its scan snapshot (the 5 events) and
    // resets the counter to the exact committed tail — the 3 mid-park events,
    // wiping the feed's skew (8 → 3). Three sits inside the hysteresis band,
    // so the throttle is retained.
    sqlx::query("COMMIT").execute(&mut c3).await.unwrap();
    let report = t1.await.expect("engine pass task");
    assert_eq!(report["requeued"], json!(true), "the mid-park tail keeps the pool dirty");
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(3), "reset to the exact tail");
    assert_eq!(
        throttled(&pool, f.pool_id).await,
        Some(8),
        "3 > low_water: hysteresis retains the throttle below the bound"
    );

    // Second parked pass: the release boundary is ≤ low_water, pinned at
    // equality. With low_water raised to 3, a reset landing EXACTLY at 3
    // must release — and the reset must be the exact committed tail (the
    // three drained T14 envelopes, deliberately NOT ingested, so a
    // decrement-by-scanned-count would compute 3 − 3 = 0 instead).
    set_backpressure_bounds(&pool, 8, 3).await;
    sqlx::query("BEGIN").execute(&mut c3).await.unwrap();
    sqlx::query("SELECT pool_id FROM pool_settlement WHERE pool_id = $1 FOR UPDATE")
        .bind(f.pool_id)
        .execute(&mut c3)
        .await
        .unwrap();
    let (mut c1b, pid1b) = session().await;
    let t1b = tokio::spawn(async move {
        let report: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut c1b)
            .await
            .expect("ledger_recalc_step");
        report
    });
    wait_until_lock_blocked(&pool, pid1b).await;
    assert_eq!(drain(&pool, 10).await, 3, "the pre-staged envelopes apply mid-park");
    sqlx::query("COMMIT").execute(&mut c3).await.unwrap();
    let report = t1b.await.expect("engine pass task");
    assert_eq!(report["requeued"], json!(true));
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(3), "reset is the exact tail");
    assert_eq!(
        throttled(&pool, f.pool_id).await,
        None,
        "a reset landing exactly at the low-water mark releases"
    );

    // The next pass drains the tail to 0 ≤ low_water: still released.
    let report = recalc_step(&pool).await;
    assert_eq!(report["requeued"], json!(false));
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));
    assert_eq!(throttled(&pool, f.pool_id).await, None, "released at the low-water mark");

    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn scope_and_stays_off() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let ff = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let fw = seed_pool(&pool, 2, 2, 1, "wac", "running_avg").await;
    set_backpressure_bounds(&pool, 5, 1).await;

    // wac pools are outside the lever entirely: no counter, no throttle, even
    // past the bound (their hot path is the authoritative costing plane).
    for i in 0..6 {
        receipt_at(&pool, fw.pool_id, 501 + i, T10, 10, 100).await;
    }
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, fw.pool_id).await, None, "wac pools have no counter");
    assert_eq!(throttled(&pool, fw.pool_id).await, None);

    // A fifo pool below the bound never engages; its settle produces a GL
    // adjustment (authoritative 133 vs provisional 150) whose feed loopback
    // must NOT re-inflate the counter.
    receipt_at(&pool, ff.pool_id, 511, T10, 10, 100).await;
    receipt_at(&pool, ff.pool_id, 512, T11, 10, 200).await;
    deplete_at(&pool, ff.pool_id, 513, T12, 15).await;
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, ff.pool_id).await, Some(3));
    assert_eq!(throttled(&pool, ff.pool_id).await, None, "stays off below the bound");

    let reports = drain_recalc(&pool).await;
    assert!(
        reports.iter().any(|r| r["adjustments_posted"].as_u64().unwrap_or(0) > 0),
        "the fixture must exercise a GL adjustment"
    );
    assert_eq!(backlog_of(&pool, ff.pool_id).await, Some(0), "exact-count reset at settle");

    // Deliver the engine's own cost_adjustment_line output: it marks the pool
    // dirty (a free no-op pass) but never counts as an unsettled event.
    ingest_all(&consumer).await;
    assert_eq!(
        backlog_of(&pool, ff.pool_id).await,
        Some(0),
        "adjustment-line loopback does not inflate the counter"
    );
    drain_recalc(&pool).await;
    assert_eq!(backlog_of(&pool, ff.pool_id).await, Some(0));
    assert_eq!(throttled(&pool, ff.pool_id).await, None);
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_backpressure").await, 0);

    drop_feed_slot(&pool).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn engine_engages_when_reset_lands_at_bound() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    set_backpressure_bounds(&pool, 3, 1).await;

    receipt_at(&pool, f.pool_id, 601, T10, 10, 100).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));

    // A dirty pool with one delivered event, and a pass parked at settle.
    receipt_at(&pool, f.pool_id, 602, T11, 10, 100).await;
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(1));

    let (mut c3, _) = session().await;
    sqlx::query("BEGIN").execute(&mut c3).await.unwrap();
    sqlx::query("SELECT pool_id FROM pool_settlement WHERE pool_id = $1 FOR UPDATE")
        .bind(f.pool_id)
        .execute(&mut c3)
        .await
        .unwrap();
    let (mut c1, pid1) = session().await;
    let t1 = tokio::spawn(async move {
        let report: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut c1)
            .await
            .expect("ledger_recalc_step");
        report
    });
    wait_until_lock_blocked(&pool, pid1).await;

    // The feed-lag window: three events commit mid-park and are NOT ingested.
    // The resumed pass's exact-tail reset counts them (the feed never did)
    // and must engage — the reset lands exactly AT the bound.
    for i in 0..3 {
        receipt_at(&pool, f.pool_id, 603 + i, T13, 10, 100).await;
    }
    sqlx::query("COMMIT").execute(&mut c3).await.unwrap();
    let report = t1.await.expect("engine pass task");
    assert_eq!(report["requeued"], json!(true));
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(3));
    assert_eq!(
        throttled(&pool, f.pool_id).await,
        Some(3),
        "the reset writer applies the bound to its own value"
    );

    // The next pass settles the tail and releases.
    recalc_step(&pool).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));
    assert_eq!(throttled(&pool, f.pool_id).await, None);

    // The late delivery lands its stale counts on the released pool — a
    // spurious re-engage — and the trailing mark guarantees the pass that
    // wipes it.
    ingest_all(&consumer).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(3), "stale counts land");
    assert_eq!(throttled(&pool, f.pool_id).await, Some(3), "spurious re-engage");
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 1);
    drain_recalc(&pool).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));
    assert_eq!(throttled(&pool, f.pool_id).await, None, "the healing pass releases");

    drop_feed_slot(&pool).await;
}

/// Poll until some backend is parked on the feed's counter-bump statement.
async fn wait_until_feed_bump_blocked(pool: &PgPool) {
    for _ in 0..400 {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
              WHERE wait_event_type = 'Lock' \
                AND query LIKE '%INSERT INTO recalc_backlog%')",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity");
        if blocked {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("feed bump never parked on the counter lock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn late_delivery_of_settled_events_heals_via_trailing_mark() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let consumer = reset_feed(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    set_backpressure_bounds(&pool, 3, 1).await;

    // Settle once so the counter row exists to park on.
    receipt_at(&pool, f.pool_id, 701, T10, 10, 100).await;
    ingest_all(&consumer).await;
    drain_recalc(&pool).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));

    // Three committed events whose slot delivery is still pending; the queue
    // row comes from a direct mark, so a pass can claim without the feed.
    for i in 0..3 {
        receipt_at(&pool, f.pool_id, 702 + i, T12, 10, 100).await;
    }
    mark_dirty(&pool, f.pool_id).await;

    // s3 parks the COUNTER row: the pass scans and settles the three events,
    // then parks at its backlog reset; the feed's delivery then parks its
    // bump BEHIND the pass in the same lock queue.
    let (mut c3, _) = session().await;
    sqlx::query("BEGIN").execute(&mut c3).await.unwrap();
    sqlx::query("SELECT pool_id FROM recalc_backlog WHERE pool_id = $1 FOR UPDATE")
        .bind(f.pool_id)
        .execute(&mut c3)
        .await
        .unwrap();
    let (mut c1, pid1) = session().await;
    let t1 = tokio::spawn(async move {
        let report: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
            .fetch_one(&mut c1)
            .await
            .expect("ledger_recalc_step");
        report
    });
    wait_until_lock_blocked(&pool, pid1).await;
    let t_feed = tokio::spawn(async move {
        ingest_all(&consumer).await;
        consumer
    });
    wait_until_feed_bump_blocked(&pool).await;

    // Unpark. FIFO lock grant: the pass resets to the true tail (0) and
    // consumes the queue row; the feed's bump then lands ON TOP of the reset
    // (stale 3 ≥ bound ⇒ spurious engage), and its trailing mark re-creates
    // the queue row the pass deleted — the healing guarantee. Marks running
    // before bumps would leave: engaged, counter 3, true tail 0, NO queue
    // row — a permanent wedge, since admission now rejects the appends that
    // would re-mark the pool.
    sqlx::query("COMMIT").execute(&mut c3).await.unwrap();
    let report = t1.await.expect("engine pass task");
    let _consumer = t_feed.await.expect("feed task");
    assert_eq!(report["requeued"], json!(false), "the pass itself saw a clean pool");
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(3), "stale bump landed on the reset");
    assert_eq!(throttled(&pool, f.pool_id).await, Some(3), "spurious engage from stale counts");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM recalc_queue").await,
        1,
        "the trailing mark re-created the queue row the pass deleted"
    );

    drain_recalc(&pool).await;
    assert_eq!(backlog_of(&pool, f.pool_id).await, Some(0));
    assert_eq!(throttled(&pool, f.pool_id).await, None, "the healing pass releases");
    submit(&pool, "po_receipt", 705, json!([line(f.pool_id, "po_receipt_line", 5, 100)]))
        .await
        .expect("appends flow after the heal");

    drop_feed_slot(&pool).await;
}
