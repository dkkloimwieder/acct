//! Acceptance tests for M5c.3 (acct-nidw): status_insert_mode dispatch.
//!
//! Two modes total post-M5c.3 (caller_subtx dropped — see bd note):
//!
//!   caller_intx (default)
//!     INSERT inside the caller's user-tx. Cheapest. On caller abort,
//!     the row is lost; the committer's pg_xact check + lazy INSERT
//!     (M5c.2 acct-1hyx) creates a 'failed' row with caller_tx_aborted.
//!     Covered end-to-end by tests/acceptance_v21_caller_tx_coupling.rs.
//!
//!   committer_lazy
//!     Startup-gated on persistent_staging=on (Postmaster scope; the
//!     gate at src/lib.rs::_PG_init refuses to load the extension
//!     otherwise). When enabled, enqueue skips the status INSERT and
//!     the committer creates the row via INSERT ON CONFLICT DO UPDATE
//!     at terminal-state determination. End-to-end coverage deferred
//!     to M5e.3 once persistent_staging recovery lands (we can't
//!     toggle the Postmaster-scope GUC at runtime).
//!
//! caller_subtx was specified pre-M5c.3 but dropped: BeginInternalSubTransaction
//! is a PG savepoint, not an autonomous tx — writes still fold into
//! the parent and are lost on parent abort. The pre-abort visibility
//! check in test_v21_caller_subtx_survives_caller_abort exposed this
//! during M5c.3 implementation. The residual differentiator (error
//! isolation on the status INSERT) is absorbed by ON CONFLICT DO
//! NOTHING. See bd acct-nidw resolution.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_status_insert_modes \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{POC_DSN, connect_pool, reset_state};
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

const MODE_PROPAGATION_MS: u64 = 2000;

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

async fn enqueue_one(conn: &mut PgConnection, cid: Uuid, sku: i64) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": 1,
        "document_id": 9_500_000,
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

/// M5c.3 primary acceptance: the obsolete caller_subtx mode raises
/// ERRCODE_INVALID_PARAMETER_VALUE on the next enqueue read.
/// status_insert_mode is Sighup-scope: ALTER SYSTEM accepts the bad
/// string, pg_reload_conf propagates it, and the extension's match
/// arm rejects it the next time enqueue runs (not at SET time).
#[tokio::test]
#[ignore]
async fn test_v21_status_insert_mode_caller_subtx_rejected_at_enqueue() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    set_mode(&pool, "caller_subtx").await;

    let mut caller = PgConnection::connect(POC_DSN).await.expect("caller conn");
    let cid = Uuid::new_v4();
    let result = enqueue_one(&mut caller, cid, 9_500_001).await;

    reset_mode(&pool).await;

    let err = result.expect_err("enqueue must reject obsolete caller_subtx mode");
    let msg = format!("{err}");
    assert!(
        msg.contains("unknown status_insert_mode")
            || msg.contains("caller_subtx")
            || msg.contains("invalid_parameter_value"),
        "expected unknown-mode error, got: {msg}"
    );
}

/// M5c.3 sanity: caller_intx (default) continues to work after the
/// caller_subtx removal. Asserts the post-cleanup match arm structure
/// still routes the default mode correctly.
#[tokio::test]
#[ignore]
async fn test_v21_status_insert_mode_caller_intx_still_works() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    // Defensive: explicit set to caller_intx in case a prior failed
    // test run left ALTER SYSTEM SET status_insert_mode = <bad-value>
    // in postgresql.auto.conf (docker restart preserves it).
    set_mode(&pool, "caller_intx").await;

    let mut caller = PgConnection::connect(POC_DSN).await.expect("caller conn");
    let cid = Uuid::new_v4();
    sqlx::query("BEGIN").execute(&mut caller).await.unwrap();
    enqueue_one(&mut caller, cid, 9_500_002).await.unwrap();

    let mut observer = PgConnection::connect(POC_DSN).await.expect("observer conn");
    let pre_commit_state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1::uuid",
    )
    .bind(cid)
    .fetch_optional(&mut observer)
    .await
    .unwrap();
    assert_eq!(
        pre_commit_state, None,
        "caller_intx: row is part of caller's tx; invisible to observer before commit"
    );

    sqlx::query("COMMIT").execute(&mut caller).await.unwrap();

    let post_commit_state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1::uuid",
    )
    .bind(cid)
    .fetch_optional(&mut observer)
    .await
    .unwrap();
    assert_eq!(
        post_commit_state.as_deref(),
        Some("queued"),
        "caller_intx: row visible to observer after caller commit"
    );
}

/// M5c.3 documentation marker: committer_lazy mode requires
/// persistent_staging=on (Postmaster scope). The startup gate at
/// src/lib.rs::_PG_init refuses to load otherwise. End-to-end
/// validation of the enqueue-skip + committer-side lazy INSERT flow
/// is deferred to M5e.3 (persistent_staging recovery).
#[tokio::test]
#[ignore]
async fn test_v21_committer_lazy_gate_documented() {
    let pool = connect_pool().await;
    let mode: String = sqlx::query_scalar("SHOW poc_v21.status_insert_mode")
        .fetch_one(&pool)
        .await
        .expect("SHOW status_insert_mode");
    let persistent: String = sqlx::query_scalar("SHOW poc_v21.persistent_staging")
        .fetch_one(&pool)
        .await
        .expect("SHOW persistent_staging");

    if mode == "committer_lazy" {
        assert_eq!(
            persistent, "on",
            "extension loaded with committer_lazy mode → persistent_staging must be 'on' (startup gate at lib.rs::_PG_init)"
        );
    }
    // No bad-combination state should reach this test: if it did, the
    // extension would have failed to load and the entire test binary
    // would have hit a connection error.
}
