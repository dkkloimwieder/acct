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
///
/// Before TRUNCATE, polls the shmem staging queue until pending +
/// processing + routed == 0 — i.e., the previous test's BGWorker
/// activity has drained. This prevents test-ordering flakes where
/// leftover envelopes get processed mid-TRUNCATE or post-reset_state,
/// bumping router stats / cost rows under the next test's feet.
pub async fn reset_state(pool: &PgPool) {
    let drain_start = Instant::now();
    let mut last_total_envelopes: i64 = -1;
    loop {
        let in_flight: i64 = sqlx::query_scalar(
            "SELECT (COALESCE((poc_v21_staging_state_counts()->>'pending')::bigint, 0) \
                   + COALESCE((poc_v21_staging_state_counts()->>'processing')::bigint, 0) \
                   + COALESCE((poc_v21_staging_state_counts()->>'routed')::bigint, 0))",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        // Read router_total_envelopes to detect any in-flight Phase 7
        // record_superbatch_stats. The router_metrics flake comes from
        // a prior test's last SuperBatch finishing Phase 6 (CAS valid
        // 2→3, in_flight visible) → committer cleans up (valid 3→0,
        // in_flight=0) → router Phase 7 still pending. Without
        // catching Phase 7, reset_router_stats can race with the
        // record_superbatch_stats increment. total_envelopes only
        // changes on actual work (not idle 50ms ticks), so two
        // consecutive identical readings with in_flight=0 confirm
        // the router has fully settled.
        let total_envelopes: i64 = sqlx::query_scalar("SELECT poc_v21_router_total_envelopes()")
            .fetch_one(pool)
            .await
            .unwrap_or(-1);
        if in_flight == 0 && total_envelopes == last_total_envelopes {
            // Belt-and-suspenders: one more sleep + recheck to catch
            // a Phase 7 that fires AFTER our stable observation. The
            // router can have Phase 6 + Phase 7 separated such that
            // committer cleanup runs between them, dropping in_flight
            // to 0 BEFORE the stats counter is bumped. Verify across
            // a wider window (150ms total since last_total_envelopes
            // was first sampled at value `total_envelopes`).
            tokio::time::sleep(Duration::from_millis(100)).await;
            let recheck: i64 =
                sqlx::query_scalar("SELECT poc_v21_router_total_envelopes()")
                    .fetch_one(pool)
                    .await
                    .unwrap_or(-1);
            if recheck == total_envelopes {
                break;
            }
            last_total_envelopes = recheck;
        } else {
            last_total_envelopes = total_envelopes;
        }
        if drain_start.elapsed() > Duration::from_secs(5) {
            eprintln!(
                "reset_state: staging didn't drain+settle within 5s (in_flight={in_flight}, \
                 total_envelopes={total_envelopes}); proceeding with TRUNCATE — downstream tests may flake"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
