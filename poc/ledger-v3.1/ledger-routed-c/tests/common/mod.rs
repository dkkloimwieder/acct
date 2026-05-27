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
                       pool_state, pool_lock, pool, standard_cost, posting_account_map, \
                       sku, location, account, accounting_period \
                       RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
}

/// A single po_receipt line. Posting accounts are resolved ledger-side from
/// posting_account_map (§3.7), not carried on the line.
pub fn receipt_line(pool_id: i64, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
    })
}

/// Same wire shape as `receipt_line` now that the STD variance account is
/// ledger-resolved (§3.7) rather than carried per line. Kept as a named alias
/// for the enqueue acceptance test's intent.
pub fn receipt_line_with_variance(pool_id: i64, qty: i64, unit_cost: i64) -> Value {
    receipt_line(pool_id, qty, unit_cost)
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

/// Set the committer stall (µs) honored between pool_lock acquire and the write —
/// a window for a test to mutate `trx` mid-pipeline (after pre-flight dedup ran).
/// Requires the `test_hooks` build.
pub async fn set_committer_stall_us(pool: &PgPool, us: i32) {
    sqlx::query("SELECT ledger_routed_c_test_set_committer_stall_us($1)")
        .bind(us)
        .execute(pool)
        .await
        .expect("set_committer_stall_us (needs test_hooks build)");
}

/// Times the committer entered its stall path (cumulative since load).
pub async fn committer_stall_hits(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_routed_c_test_committer_stall_hits()")
        .fetch_one(pool)
        .await
        .expect("committer_stall_hits (needs test_hooks build)")
}

/// Arm a one-shot raw 23505 in the next committer write phase (no real duplicate
/// behind it). Drives the §6.8 re-drive safety valve. Requires the `test_hooks` build.
pub async fn set_inject_unique(pool: &PgPool, on: bool) {
    sqlx::query("SELECT ledger_routed_c_test_set_inject_unique($1)")
        .bind(on)
        .execute(pool)
        .await
        .expect("set_inject_unique (needs test_hooks build)");
}

/// Poll until `committer_stall_hits` advances past `base`, or panic on timeout —
/// confirms the committer is parked in its stall (past pre-flight dedup + locks).
pub async fn await_stall_hit(pool: &PgPool, base: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if committer_stall_hits(pool).await > base {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for committer to enter its stall");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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

    // Posting accounts for this (sku, location), resolved ledger-side (§3.7). All
    // operation pairs stored receipt-direction (debit inv, credit ap); depletions
    // swap, recovering (debit ap, credit inv) on shipments.
    sqlx::query(
        "INSERT INTO posting_account_map ( \
            sku_id, location_id, \
            receipt_debit, receipt_credit, transfer_debit, transfer_credit, \
            build_debit, build_credit, scrap_debit, scrap_credit, \
            adjustment_debit, adjustment_credit, revaluation_debit, revaluation_credit, \
            variance_acct) \
         VALUES ($1, $2, $3,$4, $3,$4, $3,$4, $3,$4, $3,$4, $3,$4, $5) \
         ON CONFLICT (sku_id, location_id) DO NOTHING",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(inv_acct)
    .bind(ap_acct)
    .bind(var_acct)
    .execute(pool)
    .await
    .expect("insert posting_account_map");

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
        "INSERT INTO pool_state (pool_id, layer_id, qty, unit_cost, value_sum) VALUES ($1, 0, $2, $3, $2 * $3) \
         ON CONFLICT (pool_id, layer_id) DO UPDATE \
            SET qty = EXCLUDED.qty, unit_cost = EXCLUDED.unit_cost, value_sum = EXCLUDED.value_sum",
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
    })
}

/// A receipt line (positive qty) against `f`'s pool.
pub fn receipt_line_for(f: &Fixture, qty: i64, unit_cost: i64) -> Value {
    json!({
        "pool_id": f.pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": unit_cost,
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

// ── Recovery + SQL-error-handling helpers (P3.4) ────────────────────

/// Whether the postmaster-startup recovery sweep has signalled complete (§6.5).
pub async fn recovery_complete(pool: &PgPool) -> bool {
    sqlx::query_scalar("SELECT ledger_routed_c_recovery_complete()")
        .fetch_one(pool)
        .await
        .expect("recovery_complete")
}

/// Poll until `recovery_complete` signals, returning whether it did within
/// `timeout`.
pub async fn await_recovery_complete(pool: &PgPool, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if recovery_complete(pool).await {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Invoke the router boot-recovery sweep synchronously (test_hooks). Returns the
/// number of shmem slots reconciled.
pub async fn run_router_recovery_sweep(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT ledger_routed_c_test_run_router_recovery_sweep()")
        .fetch_one(pool)
        .await
        .expect("run_router_recovery_sweep (needs test_hooks build)")
}

/// Inject a synthetic orphaned CommitterQueueEntry (valid==2, dead owner). Returns
/// the CQ index, or -1 if no free slot. Pause the BGWorkers first.
pub async fn inject_orphan_cq(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT ledger_routed_c_test_inject_orphan_cq()")
        .fetch_one(pool)
        .await
        .expect("inject_orphan_cq (needs test_hooks build)")
}

/// Arm `n` synthetic deadlocks (SQLSTATE 40P01) for the committer write phase.
pub async fn set_inject_deadlock_count(pool: &PgPool, n: i32) {
    sqlx::query("SELECT ledger_routed_c_test_set_inject_deadlock_count($1)")
        .bind(n)
        .execute(pool)
        .await
        .expect("set_inject_deadlock_count (needs test_hooks build)");
}

/// Arm a one-shot non-retryable SQL error in the next committer write phase.
pub async fn set_inject_fatal(pool: &PgPool, on: bool) {
    sqlx::query("SELECT ledger_routed_c_test_set_inject_fatal($1)")
        .bind(on)
        .execute(pool)
        .await
        .expect("set_inject_fatal (needs test_hooks build)");
}

/// Pause both BGWorkers, stage `stage`, unpause ONLY the router, and wait until
/// at least one commit_group is `ready` — leaving the committer paused so a test
/// can arm an injection before the group drains. Returns once routing settled.
pub async fn route_with_committer_paused<F, Fut>(pool: &PgPool, stage: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    set_committer_paused(pool, true).await;
    set_router_paused(pool, true).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    stage().await;
    set_router_paused(pool, false).await;
    // Wait for the router to emit the commit_group (committer still parked).
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if committer_queue_count(pool, "ready").await >= 1 {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for a ready commit_group");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until no commit_group sits at `ready` (the committer has claimed/drained
/// them all), or panic on timeout.
pub async fn await_no_ready_groups(pool: &PgPool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if committer_queue_count(pool, "ready").await == 0 {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for ready commit_groups to drain");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── Shared structural-invariant catch-net (acct-yojk.8) ──────────────

/// Shared structural-invariant catch-net for aggregate-method (fifo / lifo / wac
/// / std) workloads — the I1-I7 subset that holds independent of which ops ran
/// (acct-1cer). Property tests call this, then add their workload-specific
/// assertions (aggregate qty == expected) inline. Returns Err(msg) on the first
/// violation so it composes with the proptest closures.
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
