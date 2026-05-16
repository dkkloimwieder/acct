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
/// fresh per test.
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
