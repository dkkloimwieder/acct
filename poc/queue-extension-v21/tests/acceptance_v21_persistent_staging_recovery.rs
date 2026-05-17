//! Acceptance tests for M5e.3 (acct-6isq): postmaster-restart durable
//! recovery sweep + mixed-mode classification.
//!
//! Spec §1.9 recovery + §3.6 Step 1.
//!
//! Tests directly invoke `poc_v21_test_run_startup_recovery`, which runs
//! the M5e.3 persistent-staging sweep followed by the M5d.1 non-durable
//! sweep — same ordering as the production recovery worker post-restart.
//!
//! Tests inject synthetic persistent_staging rows using captured XIDs
//! from real (committed or aborted) transactions; pg_xact_status reads
//! the SLRU committed-xact log so the recovery sweep observes the
//! actual outcomes.
//!
//! Scenarios:
//!   - durable + cost rows present (committer ran pre-crash; state-flip
//!     never got persisted) → persistent_staging.state='completed',
//!     submission_status='committed'.
//!   - durable + no cost rows + XID committed → re-enqueued; runs through
//!     committer pipeline; ends as 'completed'.
//!   - durable + no cost rows + XID aborted → persistent_staging row
//!     DELETEd; submission_status='failed' with caller_tx_aborted.
//!   - non-durable in-flight (no persistent_staging row) → M5d.1 path:
//!     'queued' + no cost rows → 'failed' postmaster_restart_loss.
//!   - mixed: all four scenarios in one sweep; per-correlation-id
//!     classification correct.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_persistent_staging_recovery \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";
const TERMINAL_TIMEOUT_SECS: u64 = 10;

/// Open a tx, force XID assignment, COMMIT — returns the now-committed XID.
async fn capture_committed_xid() -> u64 {
    let mut conn = PgConnection::connect(POC_DSN).await.expect("connect");
    let mut tx = conn.begin().await.expect("BEGIN");
    let row = sqlx::query("SELECT pg_current_xact_id()::text AS x")
        .fetch_one(&mut *tx)
        .await
        .expect("xid");
    let xid: String = row.get("x");
    tx.commit().await.expect("COMMIT");
    xid.parse::<u64>().unwrap()
}

/// Open a tx, force XID assignment, ROLLBACK — returns the now-aborted XID.
async fn capture_aborted_xid() -> u64 {
    let mut conn = PgConnection::connect(POC_DSN).await.expect("connect");
    let mut tx = conn.begin().await.expect("BEGIN");
    let row = sqlx::query("SELECT pg_current_xact_id()::text AS x")
        .fetch_one(&mut *tx)
        .await
        .expect("xid");
    let xid: String = row.get("x");
    tx.rollback().await.expect("ROLLBACK");
    xid.parse::<u64>().unwrap()
}

/// Inject a synthetic persistent_staging row directly (bypassing enqueue).
/// Mimics a post-crash state where the persistent_staging row exists
/// from the caller's user-tx INSERT but the shmem entry is gone.
async fn inject_persistent_staging_row(
    pool: &PgPool,
    cid: Uuid,
    user_tx_xid: u64,
    state: &str,
) {
    let payload = serde_json::json!({
        "sku_id": 9001,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 20221,
        "doc_chrono": 1,
        "document_id": 9_999_000_i64,
    });
    sqlx::query(
        "INSERT INTO poc_v21_persistent_staging \
            (correlation_id, user_tx_xid, event_type, payload, sku_pool_keys, business_date, state) \
         VALUES ($1, $2::text::xid8, 'po_receipt', $3, '[[9001,1]]'::jsonb, DATE '2025-05-13', $4)",
    )
    .bind(cid)
    .bind(user_tx_xid.to_string())
    .bind(&payload)
    .bind(state)
    .execute(pool)
    .await
    .expect("inject persistent_staging row");
}

/// Inject a 'queued' submission_status row mimicking M5d.1's non-durable
/// in-flight scenario.
async fn inject_status_row(pool: &PgPool, cid: Uuid, state: &str) {
    sqlx::query(
        "INSERT INTO poc_v21_submission_status (correlation_id, state, enqueued_at) \
         VALUES ($1, $2, now())",
    )
    .bind(cid)
    .bind(state)
    .execute(pool)
    .await
    .expect("inject status row");
}

/// Inject a cost_layer row for `cid` so the recovery sweep classifies
/// it as committer-committed.
async fn inject_cost_layer(pool: &PgPool, cid: Uuid) {
    sqlx::query(
        "INSERT INTO poc_v21_cost_layers \
            (correlation_id, sku_id, location_id, born_at, born_seq, qty, unit_cost, \
             source_kind, user_tx_xid, committer_tx_id, superbatch_id) \
         VALUES ($1, 9001, 1, now(), 1, 5, 100, 'po_receipt', '1'::xid8, 1, 1)",
    )
    .bind(cid)
    .execute(pool)
    .await
    .expect("inject cost_layer");
}

async fn run_recovery(pool: &PgPool) -> serde_json::Value {
    sqlx::query_scalar::<_, serde_json::Value>("SELECT poc_v21_test_run_startup_recovery()")
        .fetch_one(pool)
        .await
        .expect("run recovery")
}

#[tokio::test]
#[ignore]
async fn test_v21_recovery_durable_with_cost_rows_becomes_completed() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let xid = capture_committed_xid().await;
    inject_persistent_staging_row(&pool, cid, xid, "in_shmem").await;
    inject_status_row(&pool, cid, "queued").await;
    inject_cost_layer(&pool, cid).await;

    let result = run_recovery(&pool).await;
    let ps_completed = result.get("ps_completed").and_then(|v| v.as_i64()).unwrap_or(0);
    assert!(ps_completed >= 1, "expected ps_completed >=1; got {result}");

    let ps_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ps state");
    assert_eq!(ps_state.0, "completed");

    let ss_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ss state");
    assert_eq!(ss_state.0, "committed");
}

#[tokio::test]
#[ignore]
async fn test_v21_recovery_durable_no_cost_committed_xid_re_enqueues() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let xid = capture_committed_xid().await;
    inject_persistent_staging_row(&pool, cid, xid, "staged").await;
    inject_status_row(&pool, cid, "queued").await;

    let result = run_recovery(&pool).await;
    let ps_re_enqueued = result.get("ps_re_enqueued").and_then(|v| v.as_i64()).unwrap_or(0);
    assert!(ps_re_enqueued >= 1, "expected ps_re_enqueued >=1; got {result}");

    // Re-enqueued envelope goes through the committer pipeline post-recovery
    // and eventually reaches 'committed' (po_receipt with FIFO default).
    let reached = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1, "re-enqueued envelope must reach terminal");

    let ss_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ss state");
    assert_eq!(ss_state.0, "committed");

    let ps_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ps state");
    assert_eq!(ps_state.0, "completed");
}

#[tokio::test]
#[ignore]
async fn test_v21_recovery_durable_no_cost_aborted_xid_deletes_row() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let xid = capture_aborted_xid().await;
    inject_persistent_staging_row(&pool, cid, xid, "staged").await;
    inject_status_row(&pool, cid, "queued").await;

    let result = run_recovery(&pool).await;
    let ps_aborted = result.get("ps_aborted").and_then(|v| v.as_i64()).unwrap_or(0);
    assert!(ps_aborted >= 1, "expected ps_aborted >=1; got {result}");

    // persistent_staging row deleted.
    let ps_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ps count");
    assert_eq!(ps_count.0, 0, "aborted-caller row must be deleted");

    // submission_status marked failed with caller_tx_aborted.
    let row = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ss row");
    let state: String = row.get(0);
    let error_code: Option<String> = row.get(1);
    assert_eq!(state, "failed");
    assert_eq!(error_code.as_deref(), Some("caller_tx_aborted"));
}

#[tokio::test]
#[ignore]
async fn test_v21_recovery_non_durable_in_flight_marked_failed() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // No persistent_staging row — pure M5d.1 non-durable path.
    let cid = Uuid::new_v4();
    inject_status_row(&pool, cid, "queued").await;

    let result = run_recovery(&pool).await;
    let failed = result.get("failed").and_then(|v| v.as_i64()).unwrap_or(0);
    assert!(failed >= 1, "expected failed >=1; got {result}");

    let row = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ss row");
    let state: String = row.get(0);
    let error_code: Option<String> = row.get(1);
    assert_eq!(state, "failed");
    assert_eq!(error_code.as_deref(), Some("postmaster_restart_loss"));
}

#[tokio::test]
#[ignore]
async fn test_v21_recovery_mixed_per_correlation_classification() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Scenario A: durable + cost rows → completed (no re-enqueue).
    let cid_a = Uuid::new_v4();
    let xid_a = capture_committed_xid().await;
    inject_persistent_staging_row(&pool, cid_a, xid_a, "in_shmem").await;
    inject_status_row(&pool, cid_a, "queued").await;
    inject_cost_layer(&pool, cid_a).await;

    // Scenario B: durable + no cost + committed xid → re-enqueue.
    let cid_b = Uuid::new_v4();
    let xid_b = capture_committed_xid().await;
    inject_persistent_staging_row(&pool, cid_b, xid_b, "staged").await;
    inject_status_row(&pool, cid_b, "queued").await;

    // Scenario C: durable + no cost + aborted xid → delete row + fail status.
    let cid_c = Uuid::new_v4();
    let xid_c = capture_aborted_xid().await;
    inject_persistent_staging_row(&pool, cid_c, xid_c, "staged").await;
    inject_status_row(&pool, cid_c, "queued").await;

    // Scenario D: non-durable in-flight → M5d.1 failed.
    let cid_d = Uuid::new_v4();
    inject_status_row(&pool, cid_d, "queued").await;

    let result = run_recovery(&pool).await;
    eprintln!("mixed recovery counters: {result}");
    assert!(result.get("ps_completed").and_then(|v| v.as_i64()).unwrap_or(0) >= 1);
    assert!(result.get("ps_re_enqueued").and_then(|v| v.as_i64()).unwrap_or(0) >= 1);
    assert!(result.get("ps_aborted").and_then(|v| v.as_i64()).unwrap_or(0) >= 1);
    assert!(result.get("failed").and_then(|v| v.as_i64()).unwrap_or(0) >= 1);

    // Per-correlation-id terminal classification:
    // Wait for B to flow through the committer pipeline.
    let _ = wait_for_terminal(
        &pool,
        &[cid_a, cid_b, cid_d],
        Duration::from_secs(TERMINAL_TIMEOUT_SECS),
    )
    .await;

    let states: Vec<(Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT correlation_id, state, error_code \
           FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[]) \
          ORDER BY correlation_id",
    )
    .bind(vec![cid_a, cid_b, cid_c, cid_d])
    .fetch_all(&pool)
    .await
    .expect("fetch states");

    let by_cid: std::collections::HashMap<Uuid, (String, Option<String>)> =
        states.into_iter().map(|(c, s, e)| (c, (s, e))).collect();

    let (sa, _) = by_cid.get(&cid_a).expect("A row");
    assert_eq!(sa, "committed", "Scenario A");

    let (sb, _) = by_cid.get(&cid_b).expect("B row");
    assert_eq!(sb, "committed", "Scenario B");

    let (sc, ec_c) = by_cid.get(&cid_c).expect("C row");
    assert_eq!(sc, "failed", "Scenario C");
    assert_eq!(ec_c.as_deref(), Some("caller_tx_aborted"), "Scenario C error");

    let (sd, ec_d) = by_cid.get(&cid_d).expect("D row");
    assert_eq!(sd, "failed", "Scenario D");
    assert_eq!(ec_d.as_deref(), Some("postmaster_restart_loss"), "Scenario D error");

    // C's persistent_staging row gone.
    let c_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid_c)
    .fetch_one(&pool)
    .await
    .expect("c count");
    assert_eq!(c_count.0, 0);
}

#[tokio::test]
#[ignore]
async fn test_v21_recovery_pass2_filters_durable_in_flight() {
    // Regression: M5d.1 Pass 2's failed-pass must NOT clobber a 'queued'
    // status row whose envelope is being re-enqueued via M5e.3. The filter
    // 'AND NOT EXISTS (… persistent_staging IN staged/in_shmem)' is the
    // load-bearing check.
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let xid = capture_committed_xid().await;
    inject_persistent_staging_row(&pool, cid, xid, "staged").await;
    inject_status_row(&pool, cid, "queued").await;

    // Recovery: persistent sweep first transitions to 'in_shmem' +
    // re-enqueues; non-durable Pass 2 must then SKIP this correlation_id
    // (the filter sees 'in_shmem' persistent_staging row).
    let result = run_recovery(&pool).await;
    eprintln!("recovery counters: {result}");

    // The state should NOT be 'failed' immediately post-sweep — should
    // remain 'queued' until the committer pipeline completes it.
    // Wait for terminal via the normal committer path.
    let _ = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;

    let row = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("ss row");
    let state: String = row.get(0);
    let error_code: Option<String> = row.get(1);
    assert_eq!(state, "committed", "re-enqueued envelope must complete");
    assert!(error_code.is_none(), "must not have failure error_code");
}
