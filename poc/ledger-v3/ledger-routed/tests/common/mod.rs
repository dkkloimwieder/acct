//! Shared test helpers for ledger-routed acceptance binaries.
//!
//! Path B (routed) uses shmem staging + router + committer BGWorkers,
//! so cross-binary cleanliness depends on docker-restart between
//! binaries (run-tests.sh handles this). Within a binary,
//! --test-threads=1 + reset_state TRUNCATE keeps tests honest. The
//! fixture chain mirrors ledger-direct/tests/common/mod.rs verbatim;
//! the divergent surface is the enqueue + poll helpers and the
//! observability accessors that target Path B's shmem counters.

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

/// TRUNCATE the ledger + reference tables RESTART IDENTITY. Path B's
/// shmem queues are NOT touched here — between-binary isolation is
/// docker-restart's responsibility. Within a binary, pending shmem
/// entries from prior tests will continue to drain into the now-empty
/// schema (causing FK violations in the committer) UNLESS each test
/// waits for quiescence before TRUNCATE. Single-test binaries that
/// reset once at the start are fine.
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

/// Seed a single pool of the requested method plus the two accounts
/// the standard PO receipt fixture uses. Returns (sku_id, location_id,
/// pool_id, inv_acct_id, ap_acct_id).
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

/// Build a single po_receipt_line JSONB string for the standard fixture.
pub fn po_receipt_line(pool_id: i64, qty: i64, unit_cost: i64, inv: i64, ap: i64) -> String {
    format!(
        "[{{\"pool_id\":{pool_id},\"line_type\":\"po_receipt_line\",\
          \"source_id\":1,\"qty\":{qty},\"unit_cost\":{unit_cost},\
          \"debit_account\":{inv},\"credit_account\":{ap}}}]"
    )
}

/// Build a single transfer_shipment_line (depletion: negative qty) JSONB.
/// Mirrors ledger-direct's helper for property-test parity.
pub fn depletion_line(pool_id: i64, qty: i64, unit_cost: i64, inv: i64, ap: i64) -> String {
    format!(
        "[{{\"pool_id\":{pool_id},\"line_type\":\"transfer_shipment_line\",\
          \"source_id\":2,\"qty\":-{qty},\"unit_cost\":{unit_cost},\
          \"debit_account\":{ap},\"credit_account\":{inv}}}]"
    )
}

/// Path B enqueue. Returns the shmem submission_seq (NOT trx.id —
/// trx.id is allocated by the committer at COMMIT time and must be
/// discovered via `poll_for_trx`).
pub async fn enqueue(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines_json: &str,
) -> Result<i64, sqlx::Error> {
    let json: serde_json::Value = serde_json::from_str(lines_json).expect("valid JSON");
    sqlx::query_scalar("SELECT ledger_enqueue_trx($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(posted_at)
        .bind(json)
        .fetch_one(pool)
        .await
}

/// Poll for an existing trx row keyed by (trx_type, source_id) until
/// the deadline elapses. Returns Some(trx.id) on success, None on
/// timeout. Polls at 50ms (matches the committer's wait_latch cadence).
pub async fn poll_for_trx(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    timeout: Duration,
) -> Option<i64> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let id: Option<i64> = sqlx::query_scalar(
            "SELECT id FROM trx WHERE trx_type = $1::trx_type AND source_id = $2",
        )
        .bind(trx_type)
        .bind(source_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if id.is_some() {
            return id;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// Read the cumulative `ledger_routed_eject_total_count` SPI counter.
pub async fn eject_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_routed_eject_total_count()")
        .fetch_one(pool)
        .await
        .unwrap_or(0)
}

/// Read the cumulative `ledger_routed_router_ticks_total` SPI counter.
pub async fn router_ticks_total(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_routed_router_ticks_total()")
        .fetch_one(pool)
        .await
        .unwrap_or(0)
}

/// Wait until the routed extension reports recovery_complete = true.
/// Returns true if the signal arrives before timeout. Tests that
/// enqueue immediately after a fresh docker restart should call this
/// first — otherwise the router holds candidates until recovery's
/// boot-sweep finishes (and the test sees an apparent stall).
pub async fn wait_for_recovery_complete(pool: &PgPool, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let ready: bool = sqlx::query_scalar("SELECT ledger_routed_recovery_complete()")
            .fetch_one(pool)
            .await
            .unwrap_or(false);
        if ready {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}
