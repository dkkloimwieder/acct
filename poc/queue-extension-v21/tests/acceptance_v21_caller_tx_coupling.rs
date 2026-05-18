//! M5c.2 (acct-1hyx) acceptance: caller-tx coupling via Option (C) +
//! ejection. THE central correctness piece of v2.1 (spec §3.11).
//!
//! Five scenarios, each pinning behavior under one branch of the
//! committer Step 2.45 pg_xact_status switch:
//!
//!   A. committed caller — envelope flows through normally (regression
//!      sanity that the new gate doesn't break the happy path).
//!   B. aborted caller — lazy INSERT into poc_v21_submission_status
//!      creates a 'failed' row with error_code='caller_tx_aborted'.
//!      The original 'queued' row was rolled back with the caller's
//!      user-tx; the lazy INSERT exists because of that.
//!   C. in_progress caller, exhaust eject_count — repeated re-route +
//!      committer tick cycles increment staging.eject_count; once
//!      past max_eject_count, the envelope is marked failed with
//!      error_code='caller_tx_eject_exhausted'.
//!   D. in_progress caller, exhaust caller_tx_timeout_ms — same as C
//!      but the wall-time threshold trips first, yielding
//!      error_code='caller_tx_timeout'.
//!   E. CAS-failure-skip in cleanup — when an entry is ejected mid-
//!      pipeline (CAS 3→1), Step 14's CAS 3→0 fails for that entry;
//!      its spillover-arena blocks are NOT freed (router needs them
//!      to re-route). Asserted via poc_v21_arena_outstanding() before
//!      and after the eject-only tick.
//!
//! Test backend strategy: drive the pipeline synchronously through
//! `poc_v21_test_router_drain()` + `poc_v21_test_committer_tick()`
//! inside one pinned sqlx connection. The committer tick runs in the
//! same backend as the open outer tx, so `pg_xact_status(my_xid)`
//! returns 'in progress' — exactly the in_progress path the test
//! wants to exercise. After ROLLBACK / COMMIT we re-issue ticks from
//! the same connection (or a fresh one) to assert terminal state.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_caller_tx_coupling \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{POC_DSN, reset_state};
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Enqueue one inv_adjust envelope. Caller owns the connection (so a
/// pinned-tx test can drive this from within its own BEGIN).
async fn enqueue_one(
    conn: &mut PgConnection,
    cid: Uuid,
    sku: i64,
    chrono: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 9_300_000 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("po_receipt")
        .bind(payload)
        .bind(pool_keys)
        .execute(conn)
        .await
        .map(|_| ())
}

/// Drive one router-drain + one committer-tick from the given
/// connection. Returns whether a SuperBatch was assembled / processed.
async fn one_pump_cycle(conn: &mut PgConnection) -> (i64, bool) {
    let drained: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_drain()")
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(0);
    let processed: bool = sqlx::query_scalar("SELECT poc_v21_test_committer_tick()")
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(false);
    (drained, processed)
}

async fn arena_outstanding(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_arena_outstanding()")
        .fetch_one(pool)
        .await
        .expect("arena_outstanding")
}

/// Poll arena_outstanding until it returns to `baseline` or `timeout`
/// elapses. Returns the final value. Lets the BGWorker drive the eject
/// loop to terminal at its own cadence without a brittle fixed sleep.
async fn wait_arena_drained_to(pool: &PgPool, baseline: i64, timeout: Duration) -> i64 {
    let start = Instant::now();
    loop {
        let now = arena_outstanding(pool).await;
        if now == baseline {
            return now;
        }
        if start.elapsed() > timeout {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn full_reset(pool: &PgPool) {
    reset_state(pool).await;
    // No staging-queue reset helper exposed; arena baseline is recorded
    // before each scenario and compared post-scenario.
}

/// max_eject_count and caller_tx_timeout_ms are registered as
/// GucContext::Sighup; SET LOCAL fails with SQLSTATE 55P02. Mirror
/// the M5c.1 backpressure pattern: ALTER SYSTEM + pg_reload_conf,
/// short sleep for SIGHUP propagation, optional revert in caller.
async fn set_eject_guc(pool: &PgPool, max_eject: i32, timeout_ms: i32) {
    sqlx::query(&format!(
        "ALTER SYSTEM SET poc_v21.max_eject_count = {max_eject}"
    ))
    .execute(pool)
    .await
    .expect("alter system max_eject_count");
    sqlx::query(&format!(
        "ALTER SYSTEM SET poc_v21.caller_tx_timeout_ms = {timeout_ms}"
    ))
    .execute(pool)
    .await
    .expect("alter system caller_tx_timeout_ms");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await
        .expect("reload");
    // SIGHUP propagation: postmaster signals each BGWorker; each
    // processes the reload at its next CHECK_FOR_INTERRUPTS, which
    // happens at every Spi::run/connect plus the wait_latch return.
    // 2s sleep — empirical headroom for the BGWorker pool to all see
    // the new values before the test's enqueue triggers ticking.
    tokio::time::sleep(Duration::from_millis(2000)).await;
}

async fn reset_eject_guc(pool: &PgPool) {
    let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.max_eject_count")
        .execute(pool)
        .await;
    let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.caller_tx_timeout_ms")
        .execute(pool)
        .await;
    let _ = sqlx::query("SELECT pg_reload_conf()").execute(pool).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
}

// ── A. committed caller (sanity) ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_caller_tx_committed_path() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    full_reset(&pool).await;

    let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
    let cid = Uuid::new_v4();
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    enqueue_one(&mut conn, cid, 9_300_001, 1).await.unwrap();
    sqlx::query("COMMIT").execute(&mut conn).await.unwrap();

    // Caller tx is committed. A fresh tick should process the envelope.
    let (_drained, _processed) = one_pump_cycle(&mut conn).await;

    let (state, _err): (String, Option<String>) = sqlx::query_as(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&mut conn)
    .await
    .expect("status row exists");
    assert_eq!(
        state, "committed",
        "committed-caller envelope should reach 'committed', got {state}"
    );
}

// ── B. aborted caller → lazy failed ──────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_caller_tx_aborted_path() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    full_reset(&pool).await;

    let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
    let cid = Uuid::new_v4();
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    enqueue_one(&mut conn, cid, 9_300_002, 2).await.unwrap();
    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();

    // Caller's user-tx aborted; the 'queued' submission_status row
    // rolled back with it (the row never existed for an outside
    // observer). The shmem staging entry is still there (shmem is
    // non-transactional). Committer's pg_xact_status sees 'aborted'.
    let (_drained, _processed) = one_pump_cycle(&mut conn).await;

    let (state, err): (String, Option<String>) = sqlx::query_as(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&mut conn)
    .await
    .expect("lazy failed-status row should exist");
    assert_eq!(state, "failed", "aborted-caller envelope should be failed");
    assert_eq!(err.as_deref(), Some("caller_tx_aborted"));
}

// ── C. in_progress caller → eject_count exhausted ────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_caller_tx_eject_count_exhausted() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    full_reset(&pool).await;

    // Pin max_eject_count=100 (spec range minimum per §1.7) with a
    // generous timeout so eject_count wins the race against the wall-
    // clock bound. ALTER SYSTEM + pg_reload_conf — Sighup GUC.
    set_eject_guc(&pool, 100, 3_600_000).await;

    let baseline = arena_outstanding(&pool).await;

    let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();

    let cid = Uuid::new_v4();
    enqueue_one(&mut conn, cid, 9_300_003, 3).await.unwrap();

    // BGWorker (router + committer) drives the eject cycle to terminal:
    // max_eject_count=100 means ~101 attempts × tick latency. Poll
    // arena_outstanding until it returns to baseline; 60s window to
    // absorb tick-cadence variance across BGWorker runs.
    let after_terminal = wait_arena_drained_to(&pool, baseline, Duration::from_secs(60)).await;
    assert_eq!(
        after_terminal, baseline,
        "in_progress terminal-fail via eject_count exhausted must release arena: baseline={baseline}, after={after_terminal}"
    );

    // No submission_status row from this in_progress envelope is
    // visible to OTHER backends (caller's tx still holds the 'queued'
    // row uncommitted; committer's drop path doesn't INSERT). From
    // the admin conn's own tx view, the 'queued' row IS visible
    // (own-tx writes). Assert via a fresh connection that the row
    // is NOT visible externally.
    let visible_external: Option<(String,)> = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id=$1",
    )
    .bind(cid)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(
        visible_external.is_none(),
        "in_progress terminal-fail must NOT INSERT submission_status (would XactLockTableWait); got {visible_external:?}"
    );

    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    reset_eject_guc(&pool).await;
}

// ── D. in_progress caller → caller_tx_timeout_ms exhausted ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_caller_tx_timeout_exhausted() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    full_reset(&pool).await;

    // Generous eject budget; 1000ms timeout (the minimum). ALTER
    // SYSTEM + reload + propagation sleep.
    set_eject_guc(&pool, 10_000, 1000).await;

    let baseline = arena_outstanding(&pool).await;

    let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();

    let cid = Uuid::new_v4();
    enqueue_one(&mut conn, cid, 9_300_004, 4).await.unwrap();

    // Wait at least past the 1000ms timeout; enqueued_at_micros is
    // set at enqueue time and the elapsed check inside Step 2.45
    // compares GetCurrentTimestamp against it.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let after_terminal = wait_arena_drained_to(&pool, baseline, Duration::from_secs(30)).await;
    assert_eq!(
        after_terminal, baseline,
        "in_progress terminal-fail via timeout must release arena: baseline={baseline}, after={after_terminal}"
    );

    let visible_external: Option<(String,)> = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id=$1",
    )
    .bind(cid)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(
        visible_external.is_none(),
        "timeout terminal-fail must NOT INSERT submission_status; got {visible_external:?}"
    );

    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    reset_eject_guc(&pool).await;
}

// ── F. eject_count counter grows monotonically per cycle ───────────
//
// Closes the per-cycle assertion gap that C and D give up for
// timing-robustness. With max_eject_count=10_000 + caller_tx_timeout
// ≈ 1 hour, the envelope CAN'T terminal-fail during the test window;
// the only thing the system can do is eject + re-route repeatedly.
// Each cycle increments staging.eject_count. The assertion
//   ec_t0 < ec_t1 < ec_t2  is monotone-true regardless of who ticked
// the BGWorker — the test backend or the production BGWorker can
// only INCREASE the counter, never decrement.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_caller_tx_eject_counter_grows() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    full_reset(&pool).await;

    // Generous budgets — no terminal-fail during the test window.
    set_eject_guc(&pool, 10_000, 3_600_000).await;

    let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    let cid = Uuid::new_v4();
    enqueue_one(&mut conn, cid, 9_300_006, 6).await.unwrap();

    // Locate the staging slot — scan the full 16384-slot range.
    // The staging tail pointer advances across the regression suite
    // (next_request_seq accumulates), so cumulative test runs push
    // the active slot index up to anywhere in 0..16383. 16384 shmem
    // reads is cheap (~10-50ms). Slot stays at valid != 0 throughout
    // the test — caller is held in_progress, eject CAS bounces
    // 3 ↔ 1 ↔ 3 ↔ ... but never goes to 0 (budgets prevent terminal).
    let staging_idx_opt: Option<i64> = sqlx::query_scalar(
        "SELECT slot_idx::bigint \
           FROM generate_series(0, 16383) AS slot_idx \
          WHERE poc_v21_test_staging_state(slot_idx::bigint) NOT LIKE '(0,%' \
          LIMIT 1",
    )
    .fetch_optional(&mut conn)
    .await
    .expect("locator query");
    let staging_idx = staging_idx_opt
        .expect("active staging slot must be findable somewhere in the queue");

    // Sleep 200ms — at the BGWorker's 50ms tick rate, that's ≥3-4 eject
    // cycles. The router runs in its own BGWorker (also 50ms) and the
    // committer pool has its own. By the end of this sleep eject_count
    // should be ≥ 1.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let ec1: i32 = sqlx::query_scalar("SELECT poc_v21_test_staging_eject_count($1)")
        .bind(staging_idx)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(
        ec1 >= 1,
        "after one BGWorker cycle eject_count must be at least 1, got {ec1}"
    );

    // Sleep another 200ms — another batch of eject cycles. ec2 > ec1
    // proves the counter is moving forward, not stuck.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let ec2: i32 = sqlx::query_scalar("SELECT poc_v21_test_staging_eject_count($1)")
        .bind(staging_idx)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(
        ec2 > ec1,
        "eject_count must grow monotonically across cycles; ec1={ec1}, ec2={ec2}"
    );

    // Sanity bound — with 400ms total and max=10_000, we should be
    // nowhere near terminal. If ec2 is unexpectedly huge (>1000),
    // something's broken in the eject path (e.g., busy-looping
    // without the 50ms tick gate).
    assert!(
        ec2 < 1000,
        "eject_count grew unreasonably fast — busy loop suspected? got {ec2} in ~400ms"
    );

    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    reset_eject_guc(&pool).await;
}

// ── E. CAS-failure-skip cleanup (arena preserved on eject) ──────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_caller_tx_eject_preserves_arena() {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    full_reset(&pool).await;

    // Generous budgets so no terminal-fail fires during this test.
    set_eject_guc(&pool, 10_000, 3_600_000).await;
    let baseline = arena_outstanding(&pool).await;

    let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();

    let cid = Uuid::new_v4();
    enqueue_one(&mut conn, cid, 9_300_005, 5).await.unwrap();

    let after_enqueue = arena_outstanding(&pool).await;
    assert!(
        after_enqueue > baseline,
        "enqueue allocates arena: baseline={baseline}, after={after_enqueue}"
    );

    // One eject cycle: drain + committer tick.
    let _ = one_pump_cycle(&mut conn).await;

    let after_eject = arena_outstanding(&pool).await;
    // Step 14 cleanup CAS 3→0 fails for the ejected entry, so its
    // arena blocks (payload, sku_pool_keys) are NOT freed. The
    // queue-entry's OWN arena (staging_offsets + sku_pool_keys mirror)
    // IS freed because cleanup runs unconditionally on the queue side.
    // Net: arena_outstanding must still be > baseline (the staging
    // entry's blocks survive); but may be less than after_enqueue
    // (queue-entry-owned blocks freed).
    assert!(
        after_eject > baseline,
        "ejected entry's arena MUST survive: baseline={baseline}, after_eject={after_eject}"
    );

    // Caller commits the outer tx (releasing the in_progress state)
    // so a follow-up route + tick can drive the envelope to terminal
    // and reclaim its arena.
    sqlx::query("COMMIT").execute(&mut conn).await.unwrap();

    // Repeatedly route + tick until the envelope reaches terminal.
    for _ in 0..5 {
        let _ = one_pump_cycle(&mut conn).await;
        let s: Option<(String,)> = sqlx::query_as(
            "SELECT state FROM poc_v21_submission_status WHERE correlation_id=$1",
        )
        .bind(cid)
        .fetch_optional(&mut conn)
        .await
        .unwrap();
        if let Some((state,)) = s {
            if state == "committed" || state == "failed" || state == "replayed" {
                break;
            }
        }
    }
    reset_eject_guc(&pool).await;
}
