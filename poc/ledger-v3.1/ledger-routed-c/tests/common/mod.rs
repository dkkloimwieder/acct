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

// ── Router affinity-grouping helpers (P3.2) ─────────────────────────

/// A single line touching `pool_id` (qty/cost are immaterial to routing —
/// the router groups purely on the per-submission pool union).
pub fn line_on(pool_id: i64) -> Value {
    json!({
        "pool_id": pool_id,
        "line_type": "po_receipt_line",
        "qty": 1,
        "unit_cost": 1,
        "debit_account": 1000,
        "credit_account": 2000,
    })
}

/// Enqueue one submission whose lines touch each of `pool_ids` (one line
/// per pool). Enqueue does not validate pool existence, so arbitrary ids
/// can be used to drive the affinity logic without seeding the DB.
pub async fn enqueue_pools(
    pool: &PgPool,
    source_id: i64,
    pool_ids: &[i64],
) -> Result<i64, sqlx::Error> {
    let lines: Vec<Value> = pool_ids.iter().map(|&p| line_on(p)).collect();
    enqueue(pool, "po_receipt", source_id, lines).await
}

/// Pause/resume the router tick loop. Requires the `test_hooks` build.
pub async fn set_router_paused(pool: &PgPool, paused: bool) {
    sqlx::query("SELECT ledger_routed_c_test_set_bgworker_paused($1)")
        .bind(paused)
        .execute(pool)
        .await
        .expect("set_bgworker_paused (needs test_hooks build)");
}

/// Pause/resume just the committer pool (leaves the router running). Requires
/// the `test_hooks` build.
pub async fn set_committer_paused(pool: &PgPool, paused: bool) {
    sqlx::query("SELECT ledger_routed_c_test_set_committer_paused($1)")
        .bind(paused)
        .execute(pool)
        .await
        .expect("set_committer_paused (needs test_hooks build)");
}

/// The live `batch_size_max` GUC (max submissions per commit_group).
pub async fn batch_size_max(pool: &PgPool) -> i64 {
    let s: String = sqlx::query_scalar("SHOW ledger_routed_c.batch_size_max")
        .fetch_one(pool)
        .await
        .expect("show batch_size_max");
    s.parse().expect("batch_size_max int")
}

/// Ready (valid==1) commit_groups whose pool_keys intersect `mine`,
/// as (commit_group_id, submission_count, sorted pool_keys). Filtering by
/// the caller's own pool_ids isolates the assertion from groups other
/// tests left in the never-drained committer queue.
pub async fn ready_groups_for(pool: &PgPool, mine: &[i64]) -> Vec<(i64, i64, Vec<i64>)> {
    let rows: Vec<(i64, i64, String)> = sqlx::query_as(
        "SELECT commit_group_id, submission_count, pool_keys \
         FROM ledger_routed_c_ready_commit_groups()",
    )
    .fetch_all(pool)
    .await
    .expect("ready_commit_groups");
    let mine_set: std::collections::HashSet<i64> = mine.iter().copied().collect();
    rows.into_iter()
        .filter_map(|(cg, sc, keys)| {
            let parsed: Vec<i64> = if keys.is_empty() {
                Vec::new()
            } else {
                keys.split(',').map(|s| s.parse().unwrap()).collect()
            };
            parsed
                .iter()
                .any(|k| mine_set.contains(k))
                .then_some((cg, sc, parsed))
        })
        .collect()
}

/// Poll until the total submission_count of ready groups touching `mine`
/// reaches `expected` (router has flushed the batch), or panic on timeout.
/// Returns the final ready groups for that pool set.
pub async fn await_routed(
    pool: &PgPool,
    mine: &[i64],
    expected: i64,
) -> Vec<(i64, i64, Vec<i64>)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let groups = ready_groups_for(pool, mine).await;
        let total: i64 = groups.iter().map(|g| g.1).sum();
        if total >= expected {
            return groups;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for {expected} routed submissions on pools {mine:?}; \
                 got {total} across {} groups: {groups:?}",
                groups.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Committer-queue state counts (empty/ready/in_flight/done). Used to
/// confirm the P3.2 committer shell never advances a group past `ready`.
pub async fn committer_queue_count(pool: &PgPool, state: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count FROM ledger_routed_c_committer_queue_state_counts() WHERE state = $1",
    )
    .bind(state)
    .fetch_one(pool)
    .await
    .expect("committer_queue_state_counts")
}

// ── Committer pipeline helpers (P3.3) ───────────────────────────────

/// A seeded single-pool fixture (deterministic ids).
pub struct Fixture {
    pub sku_id: i64,
    pub loc_id: i64,
    pub pool_id: i64,
    pub inv_acct: i64,
    pub ap_acct: i64,
    pub var_acct: i64,
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

/// Seed a pool of `method` + `basis` plus its sku/location/accounts. The
/// committer hydrates routing from these rows and FK-references the pool when it
/// writes pool_state, so the committer tests (unlike the affinity test) must
/// seed the DB.
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

    let (inv_acct, ap_acct, var_acct) = (1000i64, 2000i64, 3000i64);
    seed_account(pool, inv_acct, "1000", "Inventory", "asset").await;
    seed_account(pool, ap_acct, "2000", "AP", "liability").await;
    seed_account(pool, var_acct, "3000", "PPV", "expense").await;

    let identity_key = if method == "specific" { pool_id } else { 0 };
    sqlx::query(
        "INSERT INTO pool (id, sku_id, location_id, identity_key, method, provisional_basis) \
         VALUES ($1, $2, $3, $4, $5::pool_method, $6::pool_provisional_basis) ON CONFLICT DO NOTHING",
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

/// Seed the aggregate (`layer_id = 0`) pool_state row so depletions have a
/// starting balance to draw down.
pub async fn seed_aggregate(pool: &PgPool, pool_id: i64, qty: i64, unit_cost: i64) {
    sqlx::query(
        "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost) VALUES ($1, 0, $2, $3) \
         ON CONFLICT (pool_id, layer_id) DO UPDATE SET qty = EXCLUDED.qty, unit_cost = EXCLUDED.unit_cost",
    )
    .bind(pool_id)
    .bind(qty)
    .bind(unit_cost)
    .execute(pool)
    .await
    .expect("seed aggregate");
}

/// A depletion line (negative qty) against `f`'s pool. Path C overrides the
/// recorded cost with the provisional basis (running avg or standard).
pub fn depletion_line(f: &Fixture, qty: i64) -> Value {
    json!({
        "pool_id": f.pool_id,
        "line_type": "transfer_shipment_line",
        "qty": -qty,
        "unit_cost": 0,
        "debit_account": f.ap_acct,
        "credit_account": f.inv_acct,
    })
}

/// A receipt line (positive qty) against `f`'s pool.
pub fn receipt_line_for(f: &Fixture, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": f.pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
        "debit_account": f.inv_acct,
        "credit_account": f.ap_acct,
    })
}

/// Aggregate row `(qty, unit_cost)` for a pool, or None.
pub async fn aggregate(pool: &PgPool, pool_id: i64) -> Option<(i64, i64)> {
    sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_id = 0")
        .bind(pool_id)
        .fetch_optional(pool)
        .await
        .expect("read aggregate")
}

/// Count of trx rows for a given source_id (0 = the submission was dropped /
/// deduped and produced no trx).
pub async fn trx_count_for_source(pool: &PgPool, source_id: i64) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM trx WHERE source_id = $1")
        .bind(source_id)
        .fetch_one(pool)
        .await
        .expect("trx count for source")
}

/// Read a committer stat accessor `ledger_routed_c_committer_<name>()`.
pub async fn committer_stat(pool: &PgPool, name: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT ledger_routed_c_committer_{name}()"))
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("committer stat {name}: {e}"))
}

/// Poll until the total trx count reaches `expected`, or panic on timeout.
pub async fn await_trx_count(pool: &PgPool, expected: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let n = trx_count(pool).await;
        if n >= expected {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {expected} trx rows; got {n}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until `committer_drains_total` advances by at least `delta` beyond
/// `base`, confirming the committer finished processing this test's groups.
pub async fn await_committer_drains(pool: &PgPool, base: i64, delta: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if committer_stat(pool, "drains_total").await - base >= delta {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {delta} committer drains beyond {base}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
