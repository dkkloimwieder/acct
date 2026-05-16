//! Shared test helpers for the v2.1 PoC integration suite.
//!
//! Connects to `acct_poc_queue_v21` via sqlx. Tests run with
//! `--test-threads=1` because they share the cluster's BGWorker pool
//! and shmem state.

#![allow(dead_code)]

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};

pub const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";

pub async fn connect_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect to acct_poc_queue_v21")
}

/// TRUNCATE all PoC tables; RESTART IDENTITY so layer_id / etc. start
/// fresh per test. `sku_method_assignments` is NOT truncated — its
/// rows are configuration that persists across scenarios; tests that
/// need specific assignments call `seed_method_assignments` once.
pub async fn reset_state(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE poc_v21_cost_layers, poc_v21_cost_depletions, \
                  poc_v21_cost_consumptions, poc_v21_posting_lines, \
                  poc_v21_posting_line_inventory, poc_v21_submission_status, \
                  poc_v21_pool_locks, poc_v21_wip_pool_locks, poc_v21_avg_pool_state \
                  RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("reset_state TRUNCATE");
}

/// UPSERT per-SKU method assignments. Idempotent — callers can invoke
/// once at test start or per scenario; pre-existing rows are updated.
pub async fn seed_method_assignments(pool: &PgPool, assignments: &[(i64, &str)]) {
    let sku_ids: Vec<i64> = assignments.iter().map(|(s, _)| *s).collect();
    let methods: Vec<String> = assignments.iter().map(|(_, m)| m.to_string()).collect();
    sqlx::query(
        "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
         SELECT sku_id, method_id FROM UNNEST($1::bigint[], $2::text[]) AS t(sku_id, method_id) \
         ON CONFLICT (sku_id) DO UPDATE SET method_id = EXCLUDED.method_id",
    )
    .bind(&sku_ids)
    .bind(&methods)
    .execute(pool)
    .await
    .expect("seed_method_assignments UPSERT");
}

/// Seed standard costs for STD-method SKUs. Each row uses
/// `effective_from = now() - 1 day` so it's always live for the test
/// window. Caller passes `[(sku, location, unit_cost), ...]`.
pub async fn seed_standard_costs(pool: &PgPool, costs: &[(i64, i64, i64)]) {
    // Clear first so the test's DISTINCT ON read returns a deterministic
    // single cost per (sku, location).
    sqlx::query("TRUNCATE poc_v21_standard_costs")
        .execute(pool)
        .await
        .expect("seed_standard_costs TRUNCATE");
    let sku_ids: Vec<i64> = costs.iter().map(|(s, _, _)| *s).collect();
    let loc_ids: Vec<i64> = costs.iter().map(|(_, l, _)| *l).collect();
    let unit_costs: Vec<i64> = costs.iter().map(|(_, _, c)| *c).collect();
    sqlx::query(
        "INSERT INTO poc_v21_standard_costs (sku_id, location_id, unit_cost, effective_from) \
         SELECT sku_id, location_id, unit_cost, now() - interval '1 day' \
           FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[]) AS t(sku_id, location_id, unit_cost)",
    )
    .bind(&sku_ids)
    .bind(&loc_ids)
    .bind(&unit_costs)
    .execute(pool)
    .await
    .expect("seed_standard_costs INSERT");
}

/// Poll submission_status until all `correlation_ids` are in a terminal
/// state (committed / failed / replayed) or until `timeout` elapses.
/// Returns the count of rows reaching terminal state.
pub async fn wait_for_terminal(
    pool: &PgPool,
    correlation_ids: &[uuid::Uuid],
    timeout: Duration,
) -> i64 {
    let start = Instant::now();
    loop {
        let terminal: (i64,) = sqlx::query_as(
            "SELECT COUNT(*)::BIGINT \
               FROM poc_v21_submission_status \
              WHERE correlation_id = ANY($1::uuid[]) \
                AND state IN ('committed', 'failed', 'replayed')",
        )
        .bind(correlation_ids)
        .fetch_one(pool)
        .await
        .expect("wait_for_terminal query");

        if terminal.0 >= correlation_ids.len() as i64 {
            return terminal.0;
        }
        if start.elapsed() > timeout {
            return terminal.0;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
