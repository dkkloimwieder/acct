//! Shared test helpers for ledger-routed-c acceptance binaries.
//!
//! Tests run against `poc_v3_1` with `ledger_routed_c` preloaded
//! (shared_preload_libraries) so the shmem regions + BGWorkers exist. The
//! staging queue / arena live in shmem and are NOT reset by TRUNCATE; tests that
//! care about counts therefore measure deltas around their own enqueues. DB
//! tables (trx, …) are reset via `reset_state`.

#![allow(dead_code)]

use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3_1";
pub const TS: &str = "2026-05-25T12:00:00+00:00";

pub async fn connect_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect to poc_v3_1")
}

/// Reset only the DB tables (shmem staging/arena persist across the test). Used
/// so the "enqueue writes no trx row" assertion starts from a clean trx table.
pub async fn reset_state(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, trx_line, trx, \
                       pool_state, pool_lock, pool, standard_cost, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
}

/// A single po_receipt line (no variance account).
pub fn receipt_line(pool_id: i64, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
        "debit_account": 1000,
        "credit_account": 2000,
    })
}

/// A line carrying the optional STD variance account (exercises the v3.1 payload
/// delta end-to-end through JSON → arena → (future) committer).
pub fn receipt_line_with_variance(pool_id: i64, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
        "debit_account": 1000,
        "credit_account": 2000,
        "variance_account": 3000,
    })
}

/// Call `ledger_enqueue_trx_c`, returning the shmem submission_id (or SQL error).
pub async fn enqueue(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    lines: Vec<Value>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT ledger_enqueue_trx_c($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(TS)
        .bind(Value::Array(lines))
        .fetch_one(pool)
        .await
}

pub async fn staging_pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count FROM ledger_routed_c_staging_state_counts() WHERE state = 'pending'",
    )
    .fetch_one(pool)
    .await
    .expect("staging pending count")
}

pub async fn arena_outstanding(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_routed_c_arena_outstanding()")
        .fetch_one(pool)
        .await
        .expect("arena outstanding")
}

pub async fn request_seq_max(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_routed_c_staging_request_seq_max()")
        .fetch_one(pool)
        .await
        .expect("request seq max")
}

pub async fn trx_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM trx")
        .fetch_one(pool)
        .await
        .expect("trx count")
}
