//! Shared test helpers for the ledger-feed acceptance + property binaries.
//!
//! Same harness posture as ledger-direct's tests: each binary runs against
//! `poc_v3_2` (extension `ledger_direct` installed — the feed's events come
//! from `ledger_submit_trx` / `ledger_staging_drain`) with `--test-threads=1`.
//! Feed-specific addition: `reset_feed` drops and recreates the replication
//! slot so every test starts from a clean cursor, immune to WAL written by
//! other tests or binaries.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use ledger_feed::FeedConsumer;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3_2";
pub const SLOT: &str = "ledger_feed";
pub const PUBLICATION: &str = "ledger_feed";

/// Timestamp grid for backdating scenarios (RFC3339, one per hour).
pub const T09: &str = "2026-07-11T09:00:00+00:00";
pub const T10: &str = "2026-07-11T10:00:00+00:00";
pub const T11: &str = "2026-07-11T11:00:00+00:00";
pub const T12: &str = "2026-07-11T12:00:00+00:00";
pub const T13: &str = "2026-07-11T13:00:00+00:00";
pub const T14: &str = "2026-07-11T14:00:00+00:00";

pub const INV_ACCT: i64 = 100;
pub const AP_ACCT: i64 = 200;
pub const VAR_ACCT: i64 = 300;
pub const ADJ_ACCT: i64 = 400;

pub async fn connect_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect to poc_v3_2")
}

/// TRUNCATE ledger + reference + staging + recalc tables RESTART IDENTITY.
/// recalc_queue rides on the pool CASCADE but is listed explicitly.
pub async fn reset_state(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, \
                       cost_settlement, cost_layer_consumption, pool_settlement, \
                       recalc_queue, ledger_inbox, trx_line, trx, \
                       pool_state, pool, standard_cost, posting_account_map, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
}

/// Fresh feed cursor: drop the slot if present, recreate it, return the
/// consumer. Call BEFORE submitting the events a test wants delivered.
pub async fn reset_feed(pool: &PgPool) -> FeedConsumer {
    drop_feed_slot(pool).await;
    let consumer = FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
    assert!(consumer.ensure_slot().await.expect("create feed slot"));
    consumer
}

/// Best-effort slot cleanup so a finished binary doesn't leave a lagging slot
/// pinning cluster WAL between test runs.
pub async fn drop_feed_slot(pool: &PgPool) {
    sqlx::query(
        "SELECT pg_drop_replication_slot(slot_name) \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(SLOT)
    .execute(pool)
    .await
    .expect("drop feed slot");
}

pub struct Fixture {
    pub sku_id: i64,
    pub loc_id: i64,
    pub pool_id: i64,
}

/// Seed a pool with explicit ids (sku, location, shared chart accounts, and
/// the posting_account_map row inserted idempotently so pools compose).
pub async fn seed_pool(
    pool: &PgPool,
    pool_id: i64,
    sku_id: i64,
    loc_id: i64,
    method: &str,
    basis: &str,
) -> Fixture {
    sqlx::query("INSERT INTO sku (id, code, name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
        .bind(sku_id)
        .bind(format!("SKU-{sku_id}"))
        .bind(format!("Test SKU {sku_id}"))
        .execute(pool)
        .await
        .expect("insert sku");
    sqlx::query("INSERT INTO location (id, code, name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
        .bind(loc_id)
        .bind(format!("LOC-{loc_id}"))
        .bind(format!("Test location {loc_id}"))
        .execute(pool)
        .await
        .expect("insert location");
    for (id, code, ty) in [
        (INV_ACCT, "INV", "asset"),
        (AP_ACCT, "AP", "liability"),
        (VAR_ACCT, "VARIANCE", "expense"),
        (ADJ_ACCT, "ADJ", "expense"),
    ] {
        sqlx::query(
            "INSERT INTO account (id, code, name, type) VALUES ($1, $2, $2, $3::account_type) \
             ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .bind(code)
        .bind(ty)
        .execute(pool)
        .await
        .expect("insert account");
    }
    sqlx::query(
        "INSERT INTO posting_account_map \
           (sku_id, location_id, receipt_debit, receipt_credit, transfer_debit, transfer_credit, \
            build_debit, build_credit, scrap_debit, scrap_credit, adjustment_debit, \
            adjustment_credit, revaluation_debit, revaluation_credit, variance_acct) \
         VALUES ($1, $2, $3, $4, $3, $5, $3, $5, $3, $5, $3, $5, $3, $5, $6) \
         ON CONFLICT DO NOTHING",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(INV_ACCT)
    .bind(AP_ACCT)
    .bind(ADJ_ACCT)
    .bind(VAR_ACCT)
    .execute(pool)
    .await
    .expect("insert posting_account_map");
    sqlx::query(
        "INSERT INTO pool (id, sku_id, location_id, identity_key, method, provisional_basis) \
         VALUES ($1, $2, $3, CASE WHEN $4 = 'specific' THEN $1 ELSE 0 END, $4::pool_method, \
                 $5::pool_provisional_basis) \
         ON CONFLICT DO NOTHING",
    )
    .bind(pool_id)
    .bind(sku_id)
    .bind(loc_id)
    .bind(method)
    .bind(basis)
    .execute(pool)
    .await
    .expect("insert pool");

    Fixture { sku_id, loc_id, pool_id }
}

/// One-line JSONB payload builder.
pub fn line(pool_id: i64, line_type: &str, qty: i64, unit_cost: i64) -> Value {
    json!({"pool_id": pool_id, "line_type": line_type, "source_id": 1,
           "qty": qty, "unit_cost": unit_cost})
}

/// Call ledger_submit_trx with an explicit posted_at; returns the trx id.
pub async fn submit_at(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines: Value,
) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(posted_at)
        .bind(lines)
        .fetch_one(pool)
        .await
        .expect("ledger_submit_trx")
}

/// Submit a single-line po_receipt at `posted_at`; returns the trx_line id.
pub async fn receipt_at(
    pool: &PgPool,
    pool_id: i64,
    source_id: i64,
    posted_at: &str,
    qty: i64,
    unit_cost: i64,
) -> i64 {
    let trx_id = submit_at(
        pool,
        "po_receipt",
        source_id,
        posted_at,
        json!([line(pool_id, "po_receipt_line", qty, unit_cost)]),
    )
    .await;
    sqlx::query_scalar::<_, i64>("SELECT id FROM trx_line WHERE trx_id = $1")
        .bind(trx_id)
        .fetch_one(pool)
        .await
        .expect("trx_line id of receipt")
}

/// Enqueue one submission into ledger_inbox; returns the inbox row id.
pub async fn enqueue(pool: &PgPool, trx_type: &str, source_id: i64, lines: Value) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
         VALUES ($1::trx_type, $2, $3::timestamptz, $4::jsonb) RETURNING id",
    )
    .bind(trx_type)
    .bind(source_id)
    .bind(T12)
    .bind(lines)
    .fetch_one(pool)
    .await
    .expect("enqueue into ledger_inbox")
}

/// Run one ledger_staging_drain(limit); returns the claimed count.
pub async fn drain(pool: &PgPool, limit: i32) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT ledger_staging_drain($1)")
        .bind(limit)
        .fetch_one(pool)
        .await
        .expect("ledger_staging_drain")
}

/// Seed (or overwrite) a pool_settlement frontier — stands in for the phase-4
/// recalc worker having settled the pool through this position.
pub async fn set_settled_through(pool: &PgPool, pool_id: i64, posted_at: &str, id: i64) {
    sqlx::query(
        "INSERT INTO pool_settlement (pool_id, settled_through_posted_at, settled_through_id) \
         VALUES ($1, $2::timestamptz, $3) \
         ON CONFLICT (pool_id) DO UPDATE \
            SET settled_through_posted_at = EXCLUDED.settled_through_posted_at, \
                settled_through_id        = EXCLUDED.settled_through_id, \
                recost_floor_posted_at    = NULL, \
                recost_floor_id           = NULL",
    )
    .bind(pool_id)
    .bind(posted_at)
    .bind(id)
    .execute(pool)
    .await
    .expect("seed pool_settlement");
}

/// The pool's recost floor; None when unset or the settlement row is absent.
pub async fn floor_of(pool: &PgPool, pool_id: i64) -> Option<(DateTime<Utc>, i64)> {
    sqlx::query_as::<_, (Option<DateTime<Utc>>, Option<i64>)>(
        "SELECT recost_floor_posted_at, recost_floor_id FROM pool_settlement WHERE pool_id = $1",
    )
    .bind(pool_id)
    .fetch_optional(pool)
    .await
    .expect("read recost floor")
    .and_then(|(ts, id)| Some((ts?, id?)))
}

/// All dirty pool ids, ascending.
pub async fn queue_pools(pool: &PgPool) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>("SELECT pool_id FROM recalc_queue ORDER BY pool_id")
        .fetch_all(pool)
        .await
        .expect("read recalc_queue")
}

pub async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql).fetch_one(pool).await.expect("count query")
}

pub fn ts(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339).expect("test timestamp").with_timezone(&Utc)
}
