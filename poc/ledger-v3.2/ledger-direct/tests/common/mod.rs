//! Shared test helpers for ledger-direct acceptance + property binaries.
//!
//! Each binary is a separate `tokio` integration test against the `poc_v3_2`
//! database with the `ledger_direct` extension installed. Tests run with
//! `--test-threads=1` because they TRUNCATE shared tables; the extension is
//! shmem-free, so synchronous tx commit is a sufficient barrier.
//!
//! Reference rows (sku/location/account/pool) use application-assigned BIGINT
//! ids per design-v3.1 §2.2/§2.4 — the helpers below supply explicit ids so
//! tests can assert against them directly.

#![allow(dead_code)]

use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3_2";

/// Fixed RFC3339 posted_at used across fixtures.
pub const TS: &str = "2026-07-11T12:00:00+00:00";

/// Chart-of-accounts ids every fixture seeds.
pub const INV_ACCT: i64 = 100;
pub const AP_ACCT: i64 = 200;
pub const VAR_ACCT: i64 = 300;
pub const ADJ_ACCT: i64 = 400;

pub async fn connect_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(32)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect to poc_v3_2")
}

/// TRUNCATE all ledger + reference + staging + recalc tables RESTART IDENTITY,
/// and reset the backpressure config to its migration defaults (tests lower
/// the bounds to build cheap fixtures).
pub async fn reset_state(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, \
                       cost_settlement, cost_layer_consumption, pool_settlement, \
                       recalc_queue, recalc_backlog, recalc_backpressure, \
                       ledger_inbox, trx_line, trx, \
                       pool_state, pool, standard_cost, posting_account_map, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
    sqlx::query("UPDATE recalc_backpressure_config SET bound_events = DEFAULT, low_water = DEFAULT")
        .execute(pool)
        .await
        .expect("reset backpressure config");
}

/// A seeded single-pool fixture with deterministic ids.
pub struct Fixture {
    pub sku_id: i64,
    pub loc_id: i64,
    pub pool_id: i64,
}

/// Seed one pool of the given method + provisional basis (basis is only
/// meaningful for FIFO/LIFO).
pub async fn seed_fixture(pool: &PgPool, method: &str, basis: &str) -> Fixture {
    seed_pool(pool, 1, 1, 1, method, basis).await
}

/// Seed a pool with explicit ids (lets a test create several pools). Inserts
/// the sku, location, the shared chart accounts, and the posting_account_map
/// row idempotently so multiple pools in one test compose.
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

/// Insert a standard_cost row for a fixture's (sku, location).
pub async fn set_standard_cost(pool: &PgPool, f: &Fixture, unit_cost: i64) {
    sqlx::query(
        "INSERT INTO standard_cost (sku_id, location_id, unit_cost) VALUES ($1, $2, $3) \
         ON CONFLICT (sku_id, location_id) DO UPDATE SET unit_cost = EXCLUDED.unit_cost",
    )
    .bind(f.sku_id)
    .bind(f.loc_id)
    .bind(unit_cost)
    .execute(pool)
    .await
    .expect("insert standard_cost");
}

/// One-line JSONB payload builder.
pub fn line(pool_id: i64, line_type: &str, qty: i64, unit_cost: i64) -> Value {
    json!({"pool_id": pool_id, "line_type": line_type, "source_id": 1,
           "qty": qty, "unit_cost": unit_cost})
}

/// Call ledger_submit_trx; Ok(trx_id) or the raw sqlx error.
pub async fn submit(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    lines: Value,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(TS)
        .bind(lines)
        .fetch_one(pool)
        .await
}

/// SQLSTATE of an sqlx error (empty string when not a database error).
pub fn sqlstate(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::Database(db) => db.code().map(|c| c.to_string()).unwrap_or_default(),
        _ => String::new(),
    }
}

/// Enqueue one submission into ledger_inbox; returns the inbox row id.
pub async fn enqueue(pool: &PgPool, trx_type: &str, source_id: i64, lines: Value) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
         VALUES ($1::trx_type, $2, $3::timestamptz, $4::jsonb) RETURNING id",
    )
    .bind(trx_type)
    .bind(source_id)
    .bind(TS)
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

/// The aggregate row (qty, unit_cost, value_sum) for a pool; None when absent.
pub async fn aggregate(pool: &PgPool, pool_id: i64) -> Option<(i64, i64, i64)> {
    sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT qty, unit_cost, value_sum FROM pool_state WHERE pool_id = $1 AND layer_id = 0",
    )
    .bind(pool_id)
    .fetch_optional(pool)
    .await
    .expect("read aggregate")
}

pub async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql).fetch_one(pool).await.expect("count query")
}

// ── Recalc-engine helpers (feed loop + ledger_recalc_step) ──────────────────

pub const SLOT: &str = "ledger_feed";
pub const PUBLICATION: &str = "ledger_feed";

/// Timestamp grid for business-date scenarios (RFC3339, one per hour).
pub const T09: &str = "2026-07-11T09:00:00+00:00";
pub const T10: &str = "2026-07-11T10:00:00+00:00";
pub const T11: &str = "2026-07-11T11:00:00+00:00";
pub const T12: &str = "2026-07-11T12:00:00+00:00";
pub const T13: &str = "2026-07-11T13:00:00+00:00";
pub const T14: &str = "2026-07-11T14:00:00+00:00";

/// Fresh feed cursor: drop the slot if present, recreate it, return the
/// consumer. Call BEFORE submitting the events a test wants delivered.
pub async fn reset_feed(pool: &PgPool) -> ledger_feed::FeedConsumer {
    drop_feed_slot(pool).await;
    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), SLOT, PUBLICATION);
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

/// Ingest everything currently in the slot (loop until an empty tick).
pub async fn ingest_all(consumer: &ledger_feed::FeedConsumer) {
    loop {
        let report = consumer.ingest_once(10_000).await.expect("feed ingest");
        if report.messages == 0 {
            break;
        }
    }
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

/// Submit a single-line inv_adjustment depletion at `posted_at`; returns the
/// trx_line id.
pub async fn deplete_at(
    pool: &PgPool,
    pool_id: i64,
    source_id: i64,
    posted_at: &str,
    qty: i64,
) -> i64 {
    let trx_id = submit_at(
        pool,
        "inv_adjustment",
        source_id,
        posted_at,
        json!([line(pool_id, "inv_adjustment_line", -qty, 0)]),
    )
    .await;
    sqlx::query_scalar::<_, i64>("SELECT id FROM trx_line WHERE trx_id = $1")
        .bind(trx_id)
        .fetch_one(pool)
        .await
        .expect("trx_line id of depletion")
}

/// One ledger_recalc_step() tick; returns its JSONB report.
pub async fn recalc_step(pool: &PgPool) -> Value {
    sqlx::query_scalar::<_, Value>("SELECT ledger_recalc_step()")
        .fetch_one(pool)
        .await
        .expect("ledger_recalc_step")
}

/// Step until the queue is empty; returns the claimed-pass reports.
pub async fn drain_recalc(pool: &PgPool) -> Vec<Value> {
    let mut reports = Vec::new();
    loop {
        let r = recalc_step(pool).await;
        if r["claimed"] == Value::Bool(false) {
            return reports;
        }
        reports.push(r);
    }
}

/// Mark a pool dirty directly (what a feed apply does), for tests that don't
/// need the real slot delivery.
pub async fn mark_dirty(pool: &PgPool, pool_id: i64) {
    sqlx::query("INSERT INTO recalc_queue (pool_id) VALUES ($1) ON CONFLICT (pool_id) DO NOTHING")
        .bind(pool_id)
        .execute(pool)
        .await
        .expect("mark pool dirty");
}

/// (recalc_generation, settled_through_id, floor set?) for a pool.
pub async fn settlement_of(pool: &PgPool, pool_id: i64) -> Option<(i64, Option<i64>, bool)> {
    sqlx::query_as::<_, (i64, Option<i64>, bool)>(
        "SELECT recalc_generation, settled_through_id, recost_floor_posted_at IS NOT NULL \
           FROM pool_settlement WHERE pool_id = $1",
    )
    .bind(pool_id)
    .fetch_optional(pool)
    .await
    .expect("read pool_settlement")
}

/// Max-generation authoritative unit cost per depletion trx_line.
pub async fn authoritative_of(pool: &PgPool, depletion_id: i64) -> Option<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT authoritative_unit_cost FROM cost_settlement \
          WHERE depletion_trx_line_id = $1 \
          ORDER BY recalc_generation DESC LIMIT 1",
    )
    .bind(depletion_id)
    .fetch_optional(pool)
    .await
    .expect("read cost_settlement")
}

// ── Close-phase helpers (ledger_close_period / ledger_settle_pool) ──────────

/// The T09–T14 grid's canonical accounting period: DATE bounds that cover the
/// whole grid day (a period covers posted_at ∈ [start, end + 1) in DB time).
pub const PERIOD_START: &str = "2026-07-01";
pub const PERIOD_END: &str = "2026-07-11";

/// Next-period timestamps — the day after the grid, outside PERIOD_END.
pub const N09: &str = "2026-07-12T09:00:00+00:00";
pub const N10: &str = "2026-07-12T10:00:00+00:00";

pub async fn create_period(pool: &PgPool, id: i64, start: &str, end: &str) {
    sqlx::query(
        "INSERT INTO accounting_period (id, start_date, end_date, state) \
         VALUES ($1, $2::date, $3::date, 'open')",
    )
    .bind(id)
    .bind(start)
    .bind(end)
    .execute(pool)
    .await
    .expect("insert accounting_period");
}

/// Call ledger_close_period; returns its JSONB close report.
pub async fn close_period(pool: &PgPool, id: i64, actor: &str, force: bool) -> Value {
    sqlx::query_scalar::<_, Value>("SELECT ledger_close_period($1, $2, $3)")
        .bind(id)
        .bind(actor)
        .bind(force)
        .fetch_one(pool)
        .await
        .expect("ledger_close_period")
}

/// Call ledger_settle_pool; returns its JSONB report.
pub async fn settle_pool(pool: &PgPool, pool_id: i64) -> Value {
    sqlx::query_scalar::<_, Value>("SELECT ledger_settle_pool($1)")
        .bind(pool_id)
        .fetch_one(pool)
        .await
        .expect("ledger_settle_pool")
}

/// (state, closed_by) of an accounting period.
pub async fn period_state(pool: &PgPool, id: i64) -> (String, Option<String>) {
    sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT state, closed_by FROM accounting_period WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read accounting_period")
}

/// Σ open-layer value for a pool (the authoritative composition the sweep
/// trues the aggregate to).
pub async fn layer_value(pool: &PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(sum(value_sum), 0)::bigint FROM pool_state \
          WHERE pool_id = $1 AND layer_id > 0",
    )
    .bind(pool_id)
    .fetch_one(pool)
    .await
    .expect("sum layer value")
}

// ── Backpressure helpers (recalc-c §5, acct-qm7o.7) ─────────────────────────

/// Set the backpressure bound + low-water (per-test tunables; reset_state
/// restores the migration defaults).
pub async fn set_backpressure_bounds(pool: &PgPool, bound: i64, low_water: i64) {
    sqlx::query("UPDATE recalc_backpressure_config SET bound_events = $1, low_water = $2")
        .bind(bound)
        .bind(low_water)
        .execute(pool)
        .await
        .expect("set backpressure bounds");
}

/// The pool's unsettled-event counter; None when no backlog row exists.
pub async fn backlog_of(pool: &PgPool, pool_id: i64) -> Option<i64> {
    sqlx::query_scalar::<_, i64>("SELECT pending_events FROM recalc_backlog WHERE pool_id = $1")
        .bind(pool_id)
        .fetch_optional(pool)
        .await
        .expect("read recalc_backlog")
}

/// The pool's throttle entry (engage_events); None when not throttled.
pub async fn throttled(pool: &PgPool, pool_id: i64) -> Option<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT engage_events FROM recalc_backpressure WHERE pool_id = $1",
    )
    .bind(pool_id)
    .fetch_optional(pool)
    .await
    .expect("read recalc_backpressure")
}
