//! Shared test helpers for ledger-direct-c acceptance + property binaries.
//!
//! Each binary is a separate `tokio` integration test against the `poc_v3_1`
//! database with the `ledger_direct_c` extension installed. Tests run with
//! `--test-threads=1` because they TRUNCATE shared tables; Path C direct is
//! shmem-free, so synchronous tx commit is a sufficient barrier (no BGWorker
//! drain needed before TRUNCATE).
//!
//! Reference rows (sku/location/account/pool/accounting_period) use
//! application-assigned BIGINT ids per design-v3.1 §2.2/§2.4 — the helpers below
//! supply explicit ids rather than relying on IDENTITY.

#![allow(dead_code)]

use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3_1";

/// Fixed RFC3339 posted_at used across fixtures.
pub const TS: &str = "2026-05-25T12:00:00+00:00";

pub async fn connect_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(32)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect to poc_v3_1")
}

/// TRUNCATE all ledger + reference tables RESTART IDENTITY. CASCADE chains
/// through the FK graph: posting_line(_dimension) ← trx_line ← trx;
/// pool_state / pool_lock ← pool. v3.1 has no posting_lines_provisional table.
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

/// A seeded single-pool fixture. Ids are deterministic so tests can assert
/// against them directly.
pub struct Fixture {
    pub sku_id: i64,
    pub loc_id: i64,
    pub pool_id: i64,
    pub inv_acct: i64,
    pub ap_acct: i64,
    pub var_acct: i64,
}

/// Seed one pool of the given method + provisional basis, plus the three
/// accounts the fixtures post against. `basis` is only meaningful for FIFO/LIFO.
pub async fn seed_fixture(pool: &PgPool, method: &str, basis: &str) -> Fixture {
    seed_pool(pool, 1, 1, 1, method, basis).await
}

/// Seed a pool with explicit ids (lets a test create several pools). Inserts the
/// sku, location, and the inv/ap/variance accounts idempotently (ON CONFLICT)
/// so multiple pools in one test share the account chart.
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
        .bind(format!("Test Loc {loc_id}"))
        .execute(pool)
        .await
        .expect("insert location");

    let inv_acct = 1000i64;
    let ap_acct = 2000i64;
    let var_acct = 3000i64;
    seed_account(pool, inv_acct, "1000", "Inventory", "asset").await;
    seed_account(pool, ap_acct, "2000", "AP", "liability").await;
    seed_account(pool, var_acct, "3000", "PPV", "expense").await;

    // Specific pools require identity_key != 0 (CHECK constraint, §2.2); use the
    // pool_id so multiple specific pools sharing a (sku, location) stay unique.
    let identity_key = if method == "specific" { pool_id } else { 0 };
    sqlx::query(
        "INSERT INTO pool (id, sku_id, location_id, identity_key, method, provisional_basis) \
         VALUES ($1, $2, $3, $4, $5::pool_method, $6::pool_provisional_basis)",
    )
    .bind(pool_id)
    .bind(sku_id)
    .bind(loc_id)
    .bind(identity_key)
    .bind(method)
    .bind(basis)
    .execute(pool)
    .await
    .expect("insert pool");

    Fixture { sku_id, loc_id, pool_id, inv_acct, ap_acct, var_acct }
}

async fn seed_account(pool: &PgPool, id: i64, code: &str, name: &str, ty: &str) {
    sqlx::query(
        "INSERT INTO account (id, code, name, type) \
         VALUES ($1, $2, $3, $4::account_type) ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(code)
    .bind(name)
    .bind(ty)
    .execute(pool)
    .await
    .expect("insert account");
}

/// Establish a standard_cost for a (sku, location).
pub async fn seed_standard_cost(pool: &PgPool, sku_id: i64, loc_id: i64, unit_cost: i64) {
    sqlx::query(
        "INSERT INTO standard_cost (sku_id, location_id, unit_cost) VALUES ($1, $2, $3) \
         ON CONFLICT (sku_id, location_id) DO UPDATE SET unit_cost = EXCLUDED.unit_cost",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(unit_cost)
    .execute(pool)
    .await
    .expect("insert standard_cost");
}

// ── line builders ──────────────────────────────────────────────────

/// A receipt line (positive qty). debit inv / credit ap.
pub fn receipt(f: &Fixture, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": f.pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
        "debit_account": f.inv_acct,
        "credit_account": f.ap_acct,
    })
}

/// An STD receipt line carrying a variance account for the PPV leg (§3.3).
pub fn receipt_std(f: &Fixture, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": f.pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
        "debit_account": f.inv_acct,
        "credit_account": f.ap_acct,
        "variance_account": f.var_acct,
    })
}

/// A depletion line (negative qty). debit ap / credit inv. `unit_cost` is the
/// caller's asserted cost; Path C overrides the recorded cost with the
/// provisional (running avg or standard).
pub fn depletion(f: &Fixture, qty: i64) -> Value {
    json!({
        "pool_id": f.pool_id,
        "line_type": "transfer_shipment_line",
        "qty": -qty,
        "unit_cost": 0,
        "debit_account": f.ap_acct,
        "credit_account": f.inv_acct,
    })
}

/// Call `ledger_submit_trx_c` and return the new trx.id (or the SQL error).
pub async fn submit(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    lines: Vec<Value>,
) -> Result<i64, sqlx::Error> {
    let arr = Value::Array(lines);
    sqlx::query_scalar("SELECT ledger_submit_trx_c($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(TS)
        .bind(arr)
        .fetch_one(pool)
        .await
}

// ── read-back helpers ───────────────────────────────────────────────

/// Aggregate row `(qty, unit_cost)` for a pool, or None if no aggregate exists.
pub async fn aggregate(pool: &PgPool, pool_id: i64) -> Option<(i64, i64)> {
    sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_id = 0")
        .bind(pool_id)
        .fetch_optional(pool)
        .await
        .expect("read aggregate")
}

/// Count of materialized layer rows (`layer_id > 0`) for a pool. Path C
/// FIFO/LIFO must keep this at zero.
pub async fn layer_count(pool: &PgPool, pool_id: i64) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM pool_state WHERE pool_id = $1 AND layer_id > 0")
        .bind(pool_id)
        .fetch_one(pool)
        .await
        .expect("count layers")
}

/// All trx_line rows for a trx: (qty, unit_cost, source_trx_line_id), ordered by id.
pub async fn trx_lines(pool: &PgPool, trx_id: i64) -> Vec<(i64, i64, Option<i64>)> {
    sqlx::query_as(
        "SELECT qty, unit_cost, source_trx_line_id FROM trx_line WHERE trx_id = $1 ORDER BY id",
    )
    .bind(trx_id)
    .fetch_all(pool)
    .await
    .expect("read trx_lines")
}

/// All posting_line rows for a trx: (event_type, amount, debit_account,
/// credit_account), ordered by id.
pub async fn posting_lines(pool: &PgPool, trx_id: i64) -> Vec<(String, i64, i64, i64)> {
    sqlx::query_as(
        "SELECT pl.event_type::text, pl.amount, pl.debit_account, pl.credit_account \
           FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE tl.trx_id = $1 \
          ORDER BY pl.id",
    )
    .bind(trx_id)
    .fetch_all(pool)
    .await
    .expect("read posting_lines")
}

/// Shared structural-invariant catch-net for aggregate-method (fifo / lifo / wac
/// / std) workloads — the I1-I7 subset that holds independent of which ops ran
/// (acct-1cer). Property tests call this, then add their workload-specific
/// assertions (aggregate qty == expected, pool_lock presence) inline. Returns
/// Err(msg) on the first violation so it composes with the proptest closures.
///
///   I1 no orphan trx — every trx has >=1 trx_line and >=1 posting_line
///   I2 receipt/depletion posting amount == |qty| * unit_cost (variance legs excluded)
///   I4 zero materialized layers (layer_id > 0) — Path C never iterates layers
///      for these methods (§3.5)
///   I5 no trx_line carries source_trx_line_id (no specific-style layer linkage)
///   I7 no aggregate (layer_id = 0) qty is negative (§3.6)
///
/// Scoped to the aggregate methods: I5 does NOT hold for the `specific` method
/// (its depletions link the consumed layer), so do not call this on a specific
/// workload.
pub async fn assert_aggregate_method_invariants(pool: &PgPool) -> Result<(), String> {
    async fn count(pool: &PgPool, sql: &str) -> Result<i64, String> {
        sqlx::query_scalar(sql).fetch_one(pool).await.map_err(|e| e.to_string())
    }

    let orphan = count(
        pool,
        "SELECT count(*) FROM trx t \
           WHERE NOT EXISTS (SELECT 1 FROM trx_line WHERE trx_id = t.id) \
              OR NOT EXISTS (SELECT 1 FROM posting_line pl \
                              JOIN trx_line tl ON tl.id = pl.trx_line_id \
                             WHERE tl.trx_id = t.id)",
    )
    .await?;
    if orphan != 0 {
        return Err(format!("I1: {orphan} orphan trx (missing trx_line or posting_line)"));
    }

    let bad_amount = count(
        pool,
        "SELECT count(*) FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE pl.event_type IN ('inventory_receipt','inventory_depletion') \
            AND pl.amount <> ABS(tl.qty) * tl.unit_cost",
    )
    .await?;
    if bad_amount != 0 {
        return Err(format!("I2: {bad_amount} posting rows where amount != |qty|*unit_cost"));
    }

    let layers = count(pool, "SELECT count(*) FROM pool_state WHERE layer_id > 0").await?;
    if layers != 0 {
        return Err(format!("I4: {layers} layer rows (layer_id>0) on the hot path"));
    }

    let linked = count(pool, "SELECT count(*) FROM trx_line WHERE source_trx_line_id IS NOT NULL").await?;
    if linked != 0 {
        return Err(format!("I5: {linked} trx_line rows carry source_trx_line_id"));
    }

    let negative = count(pool, "SELECT count(*) FROM pool_state WHERE layer_id = 0 AND qty < 0").await?;
    if negative != 0 {
        return Err(format!("I7: {negative} aggregate rows with negative qty"));
    }

    Ok(())
}
