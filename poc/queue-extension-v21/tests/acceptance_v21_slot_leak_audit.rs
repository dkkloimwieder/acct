//! M5d.2 (acct-3rc1) acceptance: periodic slot-leak audit.
//!
//! Manufactures slot leaks in shmem and confirms the audit pass
//! reclaims each pattern, increments the corresponding counter, and
//! frees arena blocks.
//!
//! Each test calls `poc_v21_test_force_reset_all_shmem()` after
//! `reset_state` to nuke any inter-test pollution (leftover staging /
//! queue entries from upstream concurrent tests in the regression run).
//! That gives us a strict zero baseline so arena outstanding equality
//! assertions hold deterministically.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_slot_leak_audit \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, pause_bgworker, reset_state};
use sqlx::PgPool;
use uuid::Uuid;

async fn audit_counters(pool: &PgPool) -> (i64, i64, i64) {
    let reclaims: i64 = sqlx::query_scalar("SELECT poc_v21_audit_reclaims_count()")
        .fetch_one(pool)
        .await
        .unwrap();
    let lost: i64 = sqlx::query_scalar("SELECT poc_v21_audit_lost_envelopes_count()")
        .fetch_one(pool)
        .await
        .unwrap();
    let orphans: i64 = sqlx::query_scalar("SELECT poc_v21_audit_orphans_recovered_count()")
        .fetch_one(pool)
        .await
        .unwrap();
    (reclaims, lost, orphans)
}

async fn arena_outstanding(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_arena_outstanding()")
        .fetch_one(pool)
        .await
        .unwrap_or(0)
}

async fn run_audit(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_test_run_audit_sweep()")
        .fetch_one(pool)
        .await
        .expect("run audit sweep")
}

/// Drain prior-test traffic via reset_state, pause the BGWorker pool so
/// no concurrent tick races the test's synchronous slot-state assertions,
/// then nuke shmem to give the next assertion block a strict zero baseline.
/// The BGWorker stays paused for the rest of the test — every test in this
/// binary needs synchronous control, and docker-restart between binaries
/// (run-tests-v21.sh) re-zeros the pause flag. See acct-gx1z.2.
async fn clean_slate(pool: &PgPool) {
    reset_state(pool).await;
    pause_bgworker(pool).await;
    sqlx::query("SELECT poc_v21_test_force_reset_all_shmem()")
        .execute(pool)
        .await
        .expect("force_reset_all_shmem");
}

/// Pattern A: staging valid=2 stuck > 60s with dead backend_pid →
/// CAS valid 2→1 (pending). Arena NOT freed (entry remains pending
/// for re-route).
#[tokio::test]
#[ignore]
async fn test_v21_slot_leak_audit_pattern_a_processing_orphan() {
    let pool = connect_pool().await;
    clean_slate(&pool).await;

    assert_eq!(arena_outstanding(&pool).await, 0, "clean slate must have 0 outstanding allocs");
    assert_eq!(audit_counters(&pool).await, (0, 0, 0), "counters must be 0 at clean slate");

    let cid = Uuid::new_v4();
    let slot_idx: i64 = 100;

    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_staging_orphan($1::bigint, 0::bigint, $2::uuid)",
    )
    .bind(slot_idx)
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("inject_staging_orphan");
    assert!(injected, "inject_staging_orphan returned false");

    sqlx::query("SELECT poc_v21_test_backdate_staging_enqueued_at($1::bigint, 90000::bigint)")
        .bind(slot_idx)
        .execute(&pool)
        .await
        .expect("backdate");
    sqlx::query("SELECT poc_v21_test_set_staging_backend_pid($1::bigint, $2::int)")
        .bind(slot_idx)
        .bind(i32::MAX)
        .execute(&pool)
        .await
        .expect("set backend_pid");

    let pre_state: String =
        sqlx::query_scalar("SELECT poc_v21_test_staging_state($1::bigint)")
            .bind(slot_idx)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        pre_state.starts_with("(2,"),
        "pre-audit state should be valid=2; got {pre_state}"
    );

    run_audit(&pool).await;

    let post_state: String =
        sqlx::query_scalar("SELECT poc_v21_test_staging_state($1::bigint)")
            .bind(slot_idx)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        post_state.starts_with("(1,"),
        "post-audit state should be valid=1 (pending); got {post_state}"
    );

    let counters = audit_counters(&pool).await;
    assert_eq!(counters, (1, 0, 0), "expected (reclaims=1, lost=0, orphans=0); got {counters:?}");

    // Pattern A doesn't alloc or free anything (the injection set zero
    // offsets; audit just CASes valid 2→1). Outstanding stays 0.
    assert_eq!(arena_outstanding(&pool).await, 0, "Pattern A must not touch arena");
}

/// Pattern B: staging valid=3 stuck > 5min with no live queue entry →
/// mark superbatch_lost, free arena, CAS valid 3→0.
#[tokio::test]
#[ignore]
async fn test_v21_slot_leak_audit_pattern_b_lost_envelope() {
    let pool = connect_pool().await;
    clean_slate(&pool).await;

    assert_eq!(arena_outstanding(&pool).await, 0, "clean slate must have 0 outstanding allocs");
    assert_eq!(audit_counters(&pool).await, (0, 0, 0), "counters must be 0 at clean slate");

    let cid = Uuid::new_v4();
    let slot_idx: i64 = 200;
    let fake_sb_id: i64 = u32::MAX as i64;
    let payload_bytes = 128i64;

    sqlx::query(
        "INSERT INTO poc_v21_submission_status (correlation_id, state, enqueued_at) \
         VALUES ($1::uuid, 'queued', now()) ON CONFLICT (correlation_id) DO NOTHING",
    )
    .bind(cid)
    .execute(&pool)
    .await
    .expect("insert status_row");

    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_staging_with_arena($1::bigint, $2::bigint, $3::uuid, $4::bigint)",
    )
    .bind(slot_idx)
    .bind(fake_sb_id)
    .bind(cid)
    .bind(payload_bytes)
    .fetch_one(&pool)
    .await
    .expect("inject_staging_with_arena");
    assert!(injected);

    let promoted: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_promote_staging_to_routed($1::bigint, $2::bigint)",
    )
    .bind(slot_idx)
    .bind(fake_sb_id)
    .fetch_one(&pool)
    .await
    .expect("promote_to_routed");
    assert!(promoted);

    // Injection allocated 1 arena block (the payload).
    assert_eq!(
        arena_outstanding(&pool).await,
        1,
        "injection should leave exactly 1 outstanding alloc"
    );

    sqlx::query("SELECT poc_v21_test_backdate_staging_enqueued_at($1::bigint, 360000::bigint)")
        .bind(slot_idx)
        .execute(&pool)
        .await
        .unwrap();

    run_audit(&pool).await;

    let post_state: String =
        sqlx::query_scalar("SELECT poc_v21_test_staging_state($1::bigint)")
            .bind(slot_idx)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        post_state.starts_with("(0,"),
        "post-audit state should be empty (valid=0); got {post_state}"
    );

    let (state, error_code): (String, Option<String>) = sqlx::query_as(
        "SELECT state, error_code FROM poc_v21_submission_status \
         WHERE correlation_id = $1::uuid",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status row lookup");
    assert_eq!(state, "failed");
    assert_eq!(error_code.as_deref(), Some("superbatch_lost"));

    let counters = audit_counters(&pool).await;
    assert_eq!(counters, (0, 1, 0), "expected (reclaims=0, lost=1, orphans=0); got {counters:?}");

    // Arena strictly back to 0 — the payload block must have been freed.
    assert_eq!(
        arena_outstanding(&pool).await,
        0,
        "Pattern B must free the injected payload block"
    );
}

/// Pattern C: completed-but-committer-died. BOTH the queue entry's
/// arena (staging_entry_offsets) AND each linked staging entry's
/// arena (payload) must be freed. The queue-entry-owned arena is the
/// easy-to-miss leak source spec §3.3 calls out.
#[tokio::test]
#[ignore]
async fn test_v21_slot_leak_audit_pattern_c_post_commit_death() {
    let pool = connect_pool().await;
    clean_slate(&pool).await;

    assert_eq!(arena_outstanding(&pool).await, 0, "clean slate must have 0 outstanding allocs");
    assert_eq!(audit_counters(&pool).await, (0, 0, 0), "counters must be 0 at clean slate");

    let cid = Uuid::new_v4();
    let staging_idx: i64 = 300;
    let queue_idx: i64 = 100;
    let sb_id: i64 = (u32::MAX as i64) - 1;
    let payload_bytes = 128i64;

    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_orphaned_queue($1::bigint, $2::bigint, $3::int, $4::bigint[])",
    )
    .bind(queue_idx)
    .bind(sb_id)
    .bind(i32::MAX)
    .bind(vec![staging_idx])
    .fetch_one(&pool)
    .await
    .expect("inject_orphaned_queue");
    assert!(injected);

    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_staging_with_arena($1::bigint, $2::bigint, $3::uuid, $4::bigint)",
    )
    .bind(staging_idx)
    .bind(sb_id)
    .bind(cid)
    .bind(payload_bytes)
    .fetch_one(&pool)
    .await
    .expect("inject_staging_with_arena");
    assert!(injected);

    let promoted: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_promote_staging_to_routed($1::bigint, $2::bigint)",
    )
    .bind(staging_idx)
    .bind(sb_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(promoted);

    // Both injections allocated arena: queue's staging_entry_offsets (4B
    // for a 1-entry list) + staging's payload (128B). 2 outstanding blocks.
    assert_eq!(
        arena_outstanding(&pool).await,
        2,
        "both injections should leave exactly 2 outstanding allocs"
    );

    sqlx::query("SELECT poc_v21_test_backdate_staging_enqueued_at($1::bigint, 360000::bigint)")
        .bind(staging_idx)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("SELECT poc_v21_test_backdate_queue_enqueued_at($1::bigint, 360000::bigint)")
        .bind(queue_idx)
        .execute(&pool)
        .await
        .unwrap();

    run_audit(&pool).await;

    let s_state: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1::bigint)")
        .bind(staging_idx)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        s_state.starts_with("(0,"),
        "staging should be reclaimed (valid=0); got {s_state}"
    );

    let q_state: String = sqlx::query_scalar("SELECT poc_v21_test_queue_state($1::bigint)")
        .bind(queue_idx)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        q_state.starts_with("(0,"),
        "queue should be reclaimed (valid=0); got {q_state}"
    );

    let counters = audit_counters(&pool).await;
    assert_eq!(counters, (1, 0, 0), "expected (reclaims=1, lost=0, orphans=0); got {counters:?}");

    // Arena strictly back to 0 — BOTH staging-owned (payload) AND
    // queue-entry-owned (staging_entry_offsets) freed.
    assert_eq!(
        arena_outstanding(&pool).await,
        0,
        "Pattern C must free both staging-owned and queue-entry-owned arena"
    );
}

/// Pattern D: in-flight committer death. Defense-in-depth backstop for
/// M5a.1's per-tick recovery. Audit reuses committer::try_recover_orphan.
#[tokio::test]
#[ignore]
async fn test_v21_slot_leak_audit_pattern_d_inflight_orphan() {
    let pool = connect_pool().await;
    clean_slate(&pool).await;

    assert_eq!(audit_counters(&pool).await, (0, 0, 0), "counters must be 0 at clean slate");

    let slot_idx: i64 = 150;
    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_orphan_into_empty($1::bigint, $2::int, $3::bigint)",
    )
    .bind(slot_idx)
    .bind(i32::MAX)
    .bind(1000i64) // 1s past lease (default lease = 100ms)
    .fetch_one(&pool)
    .await
    .expect("inject_orphan_into_empty");
    assert!(injected);

    run_audit(&pool).await;

    let state: String = sqlx::query_scalar("SELECT poc_v21_test_slot_state($1::bigint)")
        .bind(slot_idx)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        state.starts_with("(1,"),
        "Pattern D should leave slot at valid=1; got {state}"
    );

    let counters = audit_counters(&pool).await;
    assert_eq!(counters, (0, 0, 1), "expected (reclaims=0, lost=0, orphans=1); got {counters:?}");
}

/// Audit pass on a clean state must not increment any counter.
#[tokio::test]
#[ignore]
async fn test_v21_slot_leak_audit_no_spurious_reclaims() {
    let pool = connect_pool().await;
    clean_slate(&pool).await;

    let before = audit_counters(&pool).await;
    assert_eq!(before, (0, 0, 0));
    run_audit(&pool).await;
    let after = audit_counters(&pool).await;
    assert_eq!(after, (0, 0, 0), "clean-state audit must not increment counters");
}

/// audit_last_run_at must be monotone across successive passes.
#[tokio::test]
#[ignore]
async fn test_v21_audit_last_run_at_monotone() {
    let pool = connect_pool().await;
    clean_slate(&pool).await;

    let before_run: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM poc_v21_audit_last_run_at())::float8",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        before_run.is_none(),
        "after force_reset_all_shmem, last_run_at must be NULL; got {:?}",
        before_run
    );

    run_audit(&pool).await;
    let after_first: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM poc_v21_audit_last_run_at())::float8",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(after_first.is_some(), "after run, last_run_at must be set");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    run_audit(&pool).await;
    let after_second: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM poc_v21_audit_last_run_at())::float8",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        after_second >= after_first,
        "last_run_at must be monotone non-decreasing: first={:?} second={:?}",
        after_first,
        after_second
    );
}
