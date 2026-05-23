//! Shared test helpers for ledger-direct acceptance binaries.
//!
//! Each binary is a separate `tokio` integration test. Tests run with
//! `--test-threads=1` because they TRUNCATE shared tables and the
//! cluster-per-binary runner relies on between-binary docker restart
//! for stronger isolation.
//!
//! Connection: poc_v3 DB. Path A is shmem-free so no BGWorker drain
//! needed before TRUNCATE — synchronous tx commit is the sufficient
//! barrier.

#![allow(dead_code)]

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3";

pub async fn connect_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect to poc_v3")
}

/// TRUNCATE all ledger + reference tables RESTART IDENTITY. The
/// CASCADE on the truncation chains through the FK graph:
///   posting_line / posting_line_dimension ← trx_line ← trx
///   pool_state / pool_lock ← pool ← sku, location
///   account is independent.
pub async fn reset_state(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE posting_lines_provisional, posting_line_dimension, posting_line, \
                       trx_line, trx, pool_state, pool_lock, pool, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
}

/// Insert an accounting_period in state='open'. Returns the period id.
pub async fn seed_period(pool: &PgPool, start_date: &str, end_date: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounting_period (start_date, end_date, state) \
         VALUES ($1::date, $2::date, 'open') RETURNING id",
    )
    .bind(start_date)
    .bind(end_date)
    .fetch_one(pool)
    .await
    .expect("insert accounting_period")
}

/// Call ledger_close_period and return the JSONB summary as serde_json::Value.
pub async fn close_period(
    pool: &PgPool,
    period_id: i64,
) -> sqlx::types::Json<serde_json::Value> {
    sqlx::query_scalar("SELECT ledger_close_period($1)")
        .bind(period_id)
        .fetch_one(pool)
        .await
        .expect("ledger_close_period")
}

/// Seed a single FIFO pool plus the two accounts the standard PO
/// receipt + WO scrap fixtures use. Returns (sku_id, location_id,
/// pool_id, inv_acct_id, ap_acct_id).
pub async fn seed_fifo_fixture(pool: &PgPool) -> (i64, i64, i64, i64, i64) {
    seed_fixture(pool, "fifo").await
}

pub async fn seed_fixture(pool: &PgPool, method: &str) -> (i64, i64, i64, i64, i64) {
    let sku_id: i64 = sqlx::query_scalar(
        "INSERT INTO sku (code, name) VALUES ('SKU-1', 'Test SKU') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("insert sku");
    let loc_id: i64 = sqlx::query_scalar(
        "INSERT INTO location (code, name) VALUES ('LOC-1', 'Test Loc') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("insert location");
    let inv_acct: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('1000', 'Inventory', 'asset') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("insert inv acct");
    let ap_acct: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('2000', 'AP', 'liability') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("insert ap acct");
    let pool_id: i64 = sqlx::query_scalar(
        "INSERT INTO pool (sku_id, location_id, method) \
         VALUES ($1, $2, $3::pool_method) RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(method)
    .fetch_one(pool)
    .await
    .expect("insert pool");
    (sku_id, loc_id, pool_id, inv_acct, ap_acct)
}

/// Build a single po_receipt_line JSONB for the standard fixture.
pub fn po_receipt_line(pool_id: i64, qty: i64, unit_cost: i64, inv: i64, ap: i64) -> String {
    format!(
        "[{{\"pool_id\":{pool_id},\"line_type\":\"po_receipt_line\",\
          \"source_id\":1,\"qty\":{qty},\"unit_cost\":{unit_cost},\
          \"debit_account\":{inv},\"credit_account\":{ap}}}]"
    )
}

/// Build a single transfer_shipment_line (depletion: negative qty) JSONB.
pub fn depletion_line(pool_id: i64, qty: i64, unit_cost: i64, inv: i64, ap: i64) -> String {
    format!(
        "[{{\"pool_id\":{pool_id},\"line_type\":\"transfer_shipment_line\",\
          \"source_id\":2,\"qty\":-{qty},\"unit_cost\":{unit_cost},\
          \"debit_account\":{ap},\"credit_account\":{inv}}}]"
    )
}

/// Call ledger_submit_trx and return the new trx.id.
pub async fn submit(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines_json: &str,
) -> Result<i64, sqlx::Error> {
    let json: serde_json::Value = serde_json::from_str(lines_json).expect("valid JSON");
    sqlx::query_scalar("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(posted_at)
        .bind(json)
        .fetch_one(pool)
        .await
}
