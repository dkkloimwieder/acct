//! Outbox drain worker for the acct-tyq shape-G benchmark. Single-writer
//! by design: callers spawn ONE task running `drain_loop`, which pulls
//! pending rows from `ledger_outbox` in batches and drives them through
//! `post_transfers`. Multi-worker variants are out of scope here — the
//! ORDER BY id + FOR UPDATE SKIP LOCKED pattern is forward-compatible
//! with multiple drainers (acct-dtv).
//!
//! Per-row error isolation uses sqlx nested transactions, which compile
//! to PostgreSQL savepoints. Without the savepoint, a failing
//! post_transfers (e.g. P0001 / 23514) would abort the entire batch tx
//! and force every previously-drained row in the iteration to be retried.

use sqlx::{Acquire, PgPool, Row};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct DrainConfig {
    pub batch_size: i64,
    pub idle_sleep_ms: u64,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self { batch_size: 1000, idle_sleep_ms: 1 }
    }
}

#[derive(Default, Debug, Clone)]
pub struct DrainStats {
    pub batches: u64,
    pub rows_committed: u64,
    pub rows_failed: u64,
    pub duration_ms: u64,
}

/// Run the drain loop until `drain_to_empty` is set AND a 0-row iteration
/// occurs. While `drain_to_empty` is false, the loop sleeps `idle_sleep_ms`
/// on empty iterations and keeps polling.
pub async fn drain_loop(
    pool: PgPool,
    cfg: DrainConfig,
    drain_to_empty: Arc<AtomicBool>,
) -> sqlx::Result<DrainStats> {
    let started = Instant::now();
    let mut stats = DrainStats::default();
    loop {
        let processed = drain_one_batch(&pool, cfg.batch_size, &mut stats).await?;
        if processed == 0 {
            if drain_to_empty.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(cfg.idle_sleep_ms)).await;
        }
    }
    stats.duration_ms = started.elapsed().as_millis() as u64;
    Ok(stats)
}

async fn drain_one_batch(
    pool: &PgPool,
    batch_size: i64,
    stats: &mut DrainStats,
) -> sqlx::Result<u64> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, events, override_closed_period
           FROM ledger_outbox
          WHERE status = 'pending'
          ORDER BY id
          FOR UPDATE SKIP LOCKED
          LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    let n = rows.len() as u64;
    stats.batches += 1;

    for r in &rows {
        let id: i64 = r.try_get("id")?;
        let events: serde_json::Value = r.try_get("events")?;
        let override_closed: bool = r.try_get("override_closed_period")?;

        let mut sp = tx.begin().await?;
        let result: sqlx::Result<serde_json::Value> =
            sqlx::query_scalar("SELECT post_transfers($1, $2)")
                .bind(&events)
                .bind(override_closed)
                .fetch_one(&mut *sp)
                .await;

        match result {
            Ok(_) => {
                sp.commit().await?;
                sqlx::query(
                    "UPDATE ledger_outbox
                        SET status = 'committed',
                            committed_at = clock_timestamp(),
                            attempt_count = attempt_count + 1
                      WHERE id = $1",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
                stats.rows_committed += 1;
            }
            Err(e) => {
                sp.rollback().await?;
                let (sqlstate, msg) = match e.as_database_error() {
                    Some(db) => (db.code().map(|c| c.to_string()), db.message().to_string()),
                    None => (None, e.to_string()),
                };
                sqlx::query(
                    "UPDATE ledger_outbox
                        SET status = 'failed',
                            error_sqlstate = $2,
                            error_text = $3,
                            attempt_count = attempt_count + 1
                      WHERE id = $1",
                )
                .bind(id)
                .bind(sqlstate)
                .bind(msg)
                .execute(&mut *tx)
                .await?;
                stats.rows_failed += 1;
            }
        }
    }

    tx.commit().await?;
    Ok(n)
}
