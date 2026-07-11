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

/// TRUNCATE all ledger + reference + staging + recalc tables RESTART IDENTITY.
pub async fn reset_state(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, \
                       cost_settlement, cost_layer_consumption, pool_settlement, \
                       ledger_inbox, trx_line, trx, \
                       pool_state, pool, standard_cost, posting_account_map, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
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
