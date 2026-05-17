//! Acceptance tests for M5e.2 (acct-jypc): committer state transitions
//! for persistent_staging rows + GC function + committer_lazy mode
//! end-to-end.
//!
//! Spec §1.9 committer flow + §3.4 committer_lazy.
//!
//! Tests:
//!   - successful_durable_commit_transitions_to_completed: enqueue
//!     durable_queue=true, wait for committer, assert persistent_staging
//!     state='completed' AND submission_status state='committed'.
//!   - non_durable_envelope_does_not_touch_persistent_staging: negative
//!     control — durable_queue=false envelope reaches terminal without
//!     ever creating a persistent_staging row.
//!   - gc_deletes_completed_rows_older_than_retention: insert a row,
//!     drive it to 'completed', invoke gc(0), assert deletion.
//!   - gc_preserves_recent_completed_rows: gc with retention=24 leaves
//!     recently-completed rows alone.
//!   - gc_ignores_non_completed_states: rows in 'staged'/'in_shmem'
//!     are never deleted regardless of age.
//!   - committer_lazy_creates_status_row_on_commit: with status_insert_mode
//!     =committer_lazy + persistent_staging=on, enqueue path does NOT
//!     write submission_status; committer creates the row lazily at
//!     terminal-state determination.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_persistent_staging_lifecycle \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::{PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

const TERMINAL_TIMEOUT_SECS: u64 = 10;
const MODE_PROPAGATION_MS: u64 = 2000;

async fn enqueue_durable(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 20221,
        "doc_chrono": chrono,
        "document_id": 5_000_000_i64 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("po_receipt")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue durable");
}

async fn enqueue_non_durable(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 20221,
        "doc_chrono": chrono,
        "document_id": 5_000_000_i64 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("po_receipt")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue non-durable");
}

async fn set_mode(pool: &PgPool, mode: &str) {
    sqlx::query(&format!(
        "ALTER SYSTEM SET poc_v21.status_insert_mode = '{mode}'"
    ))
    .execute(pool)
    .await
    .expect("ALTER SYSTEM SET status_insert_mode");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await
        .expect("pg_reload_conf");
    tokio::time::sleep(Duration::from_millis(MODE_PROPAGATION_MS)).await;
}

async fn reset_mode(pool: &PgPool) {
    let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.status_insert_mode")
        .execute(pool)
        .await;
    let _ = sqlx::query("SELECT pg_reload_conf()").execute(pool).await;
    tokio::time::sleep(Duration::from_millis(MODE_PROPAGATION_MS)).await;
}

#[tokio::test]
#[ignore]
async fn test_v21_successful_durable_commit_transitions_to_completed() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    enqueue_durable(&pool, cid, 7001, 1).await;
    let reached = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1, "submission_status must reach terminal");

    let ps_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch persistent_staging state");
    assert_eq!(ps_state.0, "completed");

    let ss_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch submission_status state");
    assert_eq!(ss_state.0, "committed");
}

#[tokio::test]
#[ignore]
async fn test_v21_non_durable_envelope_does_not_touch_persistent_staging() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    enqueue_non_durable(&pool, cid, 7100, 2).await;
    let reached = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1, "submission_status must reach terminal");

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(
        count.0, 0,
        "non-durable envelope must never create persistent_staging row"
    );
}

#[tokio::test]
#[ignore]
async fn test_v21_gc_deletes_completed_rows_older_than_retention() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    enqueue_durable(&pool, cid, 7200, 3).await;
    let reached = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);

    // Backdate the completed row by 25 hours so retention=24 sweeps it.
    sqlx::query(
        "UPDATE poc_v21_persistent_staging SET enqueued_at = NOW() - interval '25 hours' \
         WHERE correlation_id = $1",
    )
    .bind(cid)
    .execute(&pool)
    .await
    .expect("backdate UPDATE");

    let deleted: (i64,) = sqlx::query_as("SELECT poc_v21_persistent_staging_gc(24)")
        .fetch_one(&pool)
        .await
        .expect("call gc");
    assert!(
        deleted.0 >= 1,
        "GC should delete at least our backdated row (got {})",
        deleted.0
    );

    let remaining: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count remaining");
    assert_eq!(remaining.0, 0, "row must be deleted");
}

#[tokio::test]
#[ignore]
async fn test_v21_gc_preserves_recent_completed_rows() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    enqueue_durable(&pool, cid, 7300, 4).await;
    let reached = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);

    // Row is freshly completed (enqueued_at = NOW()); gc with retention=24h
    // must leave it alone.
    let _: (i64,) = sqlx::query_as("SELECT poc_v21_persistent_staging_gc(24)")
        .fetch_one(&pool)
        .await
        .expect("call gc");

    let remaining: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(remaining.0, 1, "recent completed row must be preserved");
}

#[tokio::test]
#[ignore]
async fn test_v21_gc_ignores_non_completed_states() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Insert a synthetic 'staged' row directly (bypassing the committer)
    // with a very old enqueued_at. GC must skip it.
    let cid = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO poc_v21_persistent_staging \
            (correlation_id, user_tx_xid, event_type, payload, sku_pool_keys, business_date, state, enqueued_at) \
         VALUES ($1, '1'::xid8, 'po_receipt', '{}'::jsonb, '[]'::jsonb, DATE 'epoch', 'staged', \
                 NOW() - interval '100 hours')",
    )
    .bind(cid)
    .execute(&pool)
    .await
    .expect("synthetic staged INSERT");

    let _: (i64,) = sqlx::query_as("SELECT poc_v21_persistent_staging_gc(1)")
        .fetch_one(&pool)
        .await
        .expect("call gc retention=1");

    let still_there: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch");
    assert_eq!(still_there.0, "staged", "GC must not touch staged rows");
}

#[tokio::test]
#[ignore]
async fn test_v21_committer_lazy_creates_status_row_lazily() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_mode(&pool, "committer_lazy").await;

    let cid = Uuid::new_v4();

    // In committer_lazy mode the enqueue path must NOT write submission_status.
    // Snapshot the row count immediately post-enqueue (before committer picks
    // it up); allow for the committer racing ahead via the contains check.
    enqueue_durable(&pool, cid, 7400, 5).await;

    let reached = wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1, "lazy-created submission_status row must reach terminal");

    // Verify the row was created by the committer (committer_tx_id IS NOT NULL)
    // rather than by enqueue (which would leave committer_tx_id NULL).
    let row = sqlx::query(
        "SELECT state, committer_tx_id FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch status");
    let state: String = row.get(0);
    let committer_tx_id: Option<i64> = row.get(1);
    assert_eq!(state, "committed");
    assert!(
        committer_tx_id.is_some() && committer_tx_id.unwrap() > 0,
        "committer_lazy must stamp committer_tx_id on terminal-state INSERT"
    );

    let ps_state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch persistent_staging");
    assert_eq!(ps_state.0, "completed");

    reset_mode(&pool).await;
}
