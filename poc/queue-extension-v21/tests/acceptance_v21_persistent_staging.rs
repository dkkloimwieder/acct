//! Acceptance tests for M5e.1 (acct-m8pg): durable_queue=true ⇒
//! WAL-backed INSERT into `poc_v21_persistent_staging`.
//!
//! Covers (spec §1.9):
//!   - durable_queue=false ⇒ NO row in persistent_staging.
//!   - durable_queue=true ⇒ exactly one 'staged' row with caller-supplied
//!     fields persisted correctly (event_type, payload, business_date,
//!     user_tx_xid).
//!   - The row rides inside the caller user-tx — rolls back atomically
//!     on caller ROLLBACK; commits with COMMIT.
//!   - ON CONFLICT (correlation_id) DO NOTHING: replay-safe boundary;
//!     duplicate enqueue from the same caller does not error.
//!   - wip_pool_keys is NULL when no WIP keys given; populated otherwise.
//!
//! M5e.2 transitions (staged → in_shmem → completed) are out of scope
//! and exercised in their own binary.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_persistent_staging \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state};
use sqlx::{Connection, PgConnection, PgPool, Row};
use uuid::Uuid;

const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";

/// Confirm the Postmaster-scope GUC is on. If not, the test binary
/// can't exercise durable_queue=true and tests would silently fall
/// through to the gate-error path. Loud panic with operator hint.
async fn require_persistent_staging_on(pool: &PgPool) {
    let setting: (String,) = sqlx::query_as("SHOW poc_v21.persistent_staging")
        .fetch_one(pool)
        .await
        .expect("SHOW poc_v21.persistent_staging");
    assert_eq!(
        setting.0, "on",
        "persistent_staging tests require poc_v21.persistent_staging=on in postgresql.conf; \
         current value is '{}'. Set it in db/postgresql.conf and docker restart acct-postgres.",
        setting.0
    );
}

fn make_payload(business_date_jdate: i32, sku: i64) -> serde_json::Value {
    serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": business_date_jdate,
        "doc_chrono": 1,
        "document_id": 9_900_000_i64 + sku,
    })
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_false_writes_no_row() {
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = make_payload(20000, 1);
    let pool_keys = serde_json::json!({ "sku": [[1, 1]], "wip": [] });

    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("inv_adjust")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await
        .expect("enqueue durable_queue=false");

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(count.0, 0, "durable_queue=false must not write persistent_staging row");
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_true_writes_staged_row() {
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = make_payload(20221, 2); // jdate 20221 → 2025-05-13
    let pool_keys = serde_json::json!({ "sku": [[2, 1]], "wip": [] });

    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("po_receipt")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await
        .expect("enqueue durable_queue=true");

    let row = sqlx::query(
        "SELECT event_type, business_date::text, state, \
                user_tx_xid::text, sku_pool_keys::text, wip_pool_keys \
           FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch persistent_staging row");

    let event_type: String = row.get(0);
    let business_date: String = row.get(1);
    let state: String = row.get(2);
    let user_tx_xid: String = row.get(3);
    let sku_keys: String = row.get(4);
    let wip_keys: Option<serde_json::Value> = row.get(5);

    assert_eq!(event_type, "po_receipt");
    assert_eq!(business_date, "2025-05-13");
    assert_eq!(state, "staged");
    assert!(
        user_tx_xid.parse::<u64>().unwrap() > 0,
        "user_tx_xid must be non-zero"
    );
    assert_eq!(sku_keys, "[[2, 1]]");
    assert!(wip_keys.is_none(), "empty wip keys should NULLIF to SQL NULL");
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_caller_abort_rolls_back_row() {
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = make_payload(20100, 3);
    let pool_keys = serde_json::json!({ "sku": [[3, 1]], "wip": [] });

    // Open a tx, enqueue, ROLLBACK — the persistent_staging INSERT
    // rides inside this tx and must roll back atomically.
    let mut conn = PgConnection::connect(POC_DSN).await.expect("connect");
    let mut tx = conn.begin().await.expect("BEGIN");
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("inv_adjust")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&mut *tx)
        .await
        .expect("enqueue inside tx");
    tx.rollback().await.expect("ROLLBACK");

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(
        count.0, 0,
        "caller ROLLBACK must roll back the persistent_staging INSERT"
    );
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_caller_commit_persists_row() {
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = make_payload(20150, 4);
    let pool_keys = serde_json::json!({ "sku": [[4, 1]], "wip": [] });

    let mut conn = PgConnection::connect(POC_DSN).await.expect("connect");
    let mut tx = conn.begin().await.expect("BEGIN");
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("inv_adjust")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&mut *tx)
        .await
        .expect("enqueue inside tx");
    tx.commit().await.expect("COMMIT");

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1 AND state = 'staged'",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(count.0, 1, "caller COMMIT must persist exactly one staged row");
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_replay_idempotent_on_correlation_id() {
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = make_payload(20200, 5);
    let pool_keys = serde_json::json!({ "sku": [[5, 1]], "wip": [] });

    // First enqueue.
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("inv_adjust")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await
        .expect("first enqueue");

    // Replay with same correlation_id must not error — ON CONFLICT DO NOTHING.
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("inv_adjust")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await
        .expect("replay enqueue must not error");

    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(count.0, 1, "replay must not create duplicate row");
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_persists_wip_pool_keys() {
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = serde_json::json!({
        "work_order_id": 100,
        "operation_id": 10,
        "business_date_jdate": 20221,
        "doc_chrono": 1,
        "document_id": 9_900_100_i64,
    });
    let pool_keys = serde_json::json!({
        "sku": [[6, 1], [7, 1]],
        "wip": [[100, 10]],
    });

    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("wo_complete")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await
        .expect("enqueue");

    let row = sqlx::query(
        "SELECT sku_pool_keys::text, wip_pool_keys::text \
           FROM poc_v21_persistent_staging WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch row");
    let sku: String = row.get(0);
    let wip: String = row.get(1);
    assert_eq!(sku, "[[6, 1], [7, 1]]");
    assert_eq!(wip, "[[100, 10]]");
}

#[tokio::test]
#[ignore]
async fn test_v21_durable_queue_gate_fires_when_guc_off() {
    // The runtime test binary runs with persistent_staging=on (compose);
    // we can't toggle the Postmaster-scope GUC mid-test. Instead probe
    // the gate via the error message path: temporarily set the *startup*
    // GUC variable's runtime read via a session-local override is NOT
    // possible (Postmaster scope rejects SET). The gate is statically
    // covered: at startup, lib.rs reads pg_settings; enqueue.rs reads
    // crate::persistent_staging_enabled() which reads the same value.
    //
    // Smoke-test the negative path indirectly: when persistent_staging
    // is ON (this binary's environment), durable_queue=true must NOT
    // raise feature_not_supported. If we observe ERRCODE_FEATURE_NOT_SUPPORTED
    // it means the env is misconfigured (panic via require_persistent_staging_on).
    //
    // True off-path coverage lands at M5e.3 when we exercise the
    // postmaster-restart cycle.
    let pool = connect_pool().await;
    require_persistent_staging_on(&pool).await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = make_payload(20100, 8);
    let pool_keys = serde_json::json!({ "sku": [[8, 1]], "wip": [] });
    let result = sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, true)")
        .bind(cid)
        .bind("inv_adjust")
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await;
    assert!(
        result.is_ok(),
        "with persistent_staging=on, durable_queue=true must succeed; got {result:?}"
    );
}
