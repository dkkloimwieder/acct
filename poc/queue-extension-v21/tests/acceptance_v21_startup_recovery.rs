//! M5d.1 (acct-y0bp) acceptance: postmaster-startup recovery worker.
//!
//! Per spec §3.6, after a postmaster crash the `poc_v21_submission_status`
//! table may hold 'queued' or 'processing' rows from envelopes that
//! were in flight at crash time. Cost rows are the source of truth:
//! if any exist for a given correlation_id, the committer completed
//! before the crash and the status row should reach 'committed';
//! otherwise the envelope is lost in shmem and the row is marked
//! 'failed' with `postmaster_restart_loss`.
//!
//! Tests use `poc_v21_test_run_startup_recovery()` to exercise the
//! sweep without actually restarting the postmaster — we inject the
//! pre-restart state into submission_status + cost tables, call the
//! synchronous helper, and assert the post-sweep state.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_startup_recovery \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state};
use sqlx::PgPool;
use uuid::Uuid;

async fn insert_status_row(pool: &PgPool, cid: Uuid, state: &str) {
    sqlx::query(
        "INSERT INTO poc_v21_submission_status (correlation_id, state, enqueued_at) \
         VALUES ($1::uuid, $2, now())",
    )
    .bind(cid)
    .bind(state)
    .execute(pool)
    .await
    .expect("insert_status_row");
}

async fn insert_cost_layer(pool: &PgPool, cid: Uuid, sku: i64) {
    sqlx::query(
        "INSERT INTO poc_v21_cost_layers \
           (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, source_ref, \
            correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
         VALUES ($1, 1, 10, 100, now(), 1, 'recv', NULL, $2::uuid, '0'::xid8, 0, 0)",
    )
    .bind(sku)
    .bind(cid)
    .execute(pool)
    .await
    .expect("insert_cost_layer");
}

async fn state_of(pool: &PgPool, cid: Uuid) -> Option<(String, Option<String>)> {
    sqlx::query_as(
        "SELECT state, error_code FROM poc_v21_submission_status \
         WHERE correlation_id = $1::uuid",
    )
    .bind(cid)
    .fetch_optional(pool)
    .await
    .expect("state_of")
}

async fn reset_recovery(pool: &PgPool) {
    sqlx::query("SELECT poc_v21_test_reset_recovery_complete()")
        .execute(pool)
        .await
        .expect("reset_recovery");
}

async fn run_recovery(pool: &PgPool) -> serde_json::Value {
    let json: sqlx::types::Json<serde_json::Value> =
        sqlx::query_scalar("SELECT poc_v21_test_run_startup_recovery()::jsonb")
            .fetch_one(pool)
            .await
            .expect("run_recovery");
    json.0
}

/// M5d.1 primary acceptance: classify in-flight submission_status rows
/// by cost-row existence.
#[tokio::test]
#[ignore]
async fn test_v21_postmaster_restart_non_durable() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_recovery(&pool).await;

    // Construct five scenarios:
    //   queued_with_cost     → should become 'committed'
    //   processing_with_cost → should become 'committed'
    //   queued_no_cost       → should become 'failed' with postmaster_restart_loss
    //   processing_no_cost   → should become 'failed'
    //   already_committed    → must not be touched
    //   already_failed       → must not be touched
    let queued_with_cost = Uuid::new_v4();
    let processing_with_cost = Uuid::new_v4();
    let queued_no_cost = Uuid::new_v4();
    let processing_no_cost = Uuid::new_v4();
    let already_committed = Uuid::new_v4();
    let already_failed = Uuid::new_v4();

    insert_status_row(&pool, queued_with_cost, "queued").await;
    insert_status_row(&pool, processing_with_cost, "processing").await;
    insert_status_row(&pool, queued_no_cost, "queued").await;
    insert_status_row(&pool, processing_no_cost, "processing").await;
    insert_status_row(&pool, already_committed, "committed").await;
    insert_status_row(&pool, already_failed, "failed").await;

    insert_cost_layer(&pool, queued_with_cost, 700_001).await;
    insert_cost_layer(&pool, processing_with_cost, 700_002).await;
    insert_cost_layer(&pool, already_committed, 700_003).await;

    let result = run_recovery(&pool).await;
    let committed = result.get("committed").and_then(|v| v.as_i64()).unwrap_or(-1);
    let failed = result.get("failed").and_then(|v| v.as_i64()).unwrap_or(-1);
    assert_eq!(committed, 2, "expected 2 'committed' transitions; got {committed}");
    assert_eq!(failed, 2, "expected 2 'failed' transitions; got {failed}");

    let s1 = state_of(&pool, queued_with_cost).await.unwrap();
    assert_eq!(s1.0, "committed", "queued+cost → committed");

    let s2 = state_of(&pool, processing_with_cost).await.unwrap();
    assert_eq!(s2.0, "committed", "processing+cost → committed");

    let s3 = state_of(&pool, queued_no_cost).await.unwrap();
    assert_eq!(s3.0, "failed", "queued+no-cost → failed");
    assert_eq!(s3.1.as_deref(), Some("postmaster_restart_loss"));

    let s4 = state_of(&pool, processing_no_cost).await.unwrap();
    assert_eq!(s4.0, "failed", "processing+no-cost → failed");
    assert_eq!(s4.1.as_deref(), Some("postmaster_restart_loss"));

    let s5 = state_of(&pool, already_committed).await.unwrap();
    assert_eq!(s5.0, "committed", "already 'committed' must not be touched");

    let s6 = state_of(&pool, already_failed).await.unwrap();
    assert_eq!(s6.0, "failed", "already 'failed' must not be touched");
    // 'already_failed' had no error_code at insert time; recovery must
    // not have stamped 'postmaster_restart_loss' onto it.
    assert!(
        s6.1.is_none(),
        "already 'failed' must keep its NULL error_code; got {:?}",
        s6.1
    );
}

/// M5d.1 sanity: caller_intx mode pre-restart state.
/// - Caller committed → 'queued' row exists pre-restart with cost rows
///   (committer completed before crash) → recovery moves to 'committed'.
/// - Caller aborted → 'queued' row never existed (rolled back with caller)
///   → recovery sees no row, no spurious entry created.
#[tokio::test]
#[ignore]
async fn test_v21_postmaster_restart_caller_intx() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_recovery(&pool).await;

    let committed_caller = Uuid::new_v4();
    let aborted_caller = Uuid::new_v4();

    // Pre-restart: committed caller's row + matching cost row.
    insert_status_row(&pool, committed_caller, "queued").await;
    insert_cost_layer(&pool, committed_caller, 700_010).await;
    // aborted_caller has NO row at all (caller_intx INSERT rolled back).

    run_recovery(&pool).await;

    let s_committed = state_of(&pool, committed_caller).await.unwrap();
    assert_eq!(s_committed.0, "committed");

    let s_aborted = state_of(&pool, aborted_caller).await;
    assert!(
        s_aborted.is_none(),
        "aborted caller's correlation_id must not appear in submission_status; got {:?}",
        s_aborted
    );
}

/// M5d.1 idempotency: re-running recovery on already-classified rows
/// is a no-op.
#[tokio::test]
#[ignore]
async fn test_v21_postmaster_restart_recovery_idempotent() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_recovery(&pool).await;

    let cid = Uuid::new_v4();
    insert_status_row(&pool, cid, "queued").await;
    insert_cost_layer(&pool, cid, 700_020).await;

    let r1 = run_recovery(&pool).await;
    assert_eq!(r1.get("committed").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(r1.get("failed").and_then(|v| v.as_i64()), Some(0));

    reset_recovery(&pool).await;
    let r2 = run_recovery(&pool).await;
    assert_eq!(
        r2.get("committed").and_then(|v| v.as_i64()),
        Some(0),
        "re-run must not re-classify already-committed rows"
    );
    assert_eq!(r2.get("failed").and_then(|v| v.as_i64()), Some(0));

    let s = state_of(&pool, cid).await.unwrap();
    assert_eq!(s.0, "committed");
}

/// M5d.1 gate: the router/committer wait on recovery_complete before
/// processing traffic. After the recovery worker completes, the flag
/// is set; tests can observe via poc_v21_recovery_complete().
#[tokio::test]
#[ignore]
async fn test_v21_recovery_complete_flag_observable() {
    let pool = connect_pool().await;

    // After docker restart, recovery_complete should be 1 (the startup
    // worker ran). For test isolation, reset then re-run.
    reset_recovery(&pool).await;
    let before: bool = sqlx::query_scalar("SELECT poc_v21_recovery_complete()")
        .fetch_one(&pool)
        .await
        .expect("recovery_complete");
    assert!(!before, "after reset, flag must read false");

    run_recovery(&pool).await;
    let after: bool = sqlx::query_scalar("SELECT poc_v21_recovery_complete()")
        .fetch_one(&pool)
        .await
        .expect("recovery_complete");
    assert!(after, "after recovery, flag must read true");
}
