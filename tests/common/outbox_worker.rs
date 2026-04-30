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
    /// Super-batch attempts (only nonzero in `super_batched_drain_loop`).
    pub super_batch_attempts: u64,
    /// Super-batch successes — one `post_transfers` call committed N rows.
    pub super_batch_successes: u64,
    /// Super-batch failures — fell back to per-row drain for that batch.
    pub super_batch_fallbacks: u64,
}

/// Run the drain loop. Two stop signals:
///   * `hard_stop` — exits before the next iteration regardless of queue state.
///     Used to bound a benchmark's drain phase when the queue would take
///     too long to fully drain.
///   * `drain_to_empty` — exits after the next 0-row iteration. Used in
///     the seeded-queue smoke test where the queue is bounded.
///
/// While both flags are false, the loop sleeps `idle_sleep_ms` on empty
/// iterations and keeps polling.
pub async fn drain_loop(
    pool: PgPool,
    cfg: DrainConfig,
    drain_to_empty: Arc<AtomicBool>,
    hard_stop: Arc<AtomicBool>,
) -> sqlx::Result<DrainStats> {
    let started = Instant::now();
    let mut stats = DrainStats::default();
    loop {
        if hard_stop.load(Ordering::Relaxed) {
            break;
        }
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

// ============================================================
// Super-batched drain loop (acct-hbg).
// ============================================================
//
// Variant of drain_loop that, on each iteration, attempts to commit ALL
// drained rows via a SINGLE post_transfers call by concatenating their
// event arrays. On success — one fsync, one set of FOR UPDATE acquires,
// one function entry — recovers shape B's per-batch amortization. On
// any error from the merged call, falls back to the per-row savepoint
// drain (existing drain_one_batch logic) for that iteration's rows so
// per-row error attribution is preserved.
//
// Caveat: rows with mixed `override_closed_period` flags are split into
// two super-batches (one per flag value), since `post_transfers`'s
// override is a single function argument applied uniformly across the
// merged events. In Phase 0 our load workload always has override=false,
// so the split rarely triggers.

/// Same shape as `drain_loop` but each iteration tries an optimistic
/// super-batch first. Falls back to per-row drain on error.
pub async fn super_batched_drain_loop(
    pool: PgPool,
    cfg: DrainConfig,
    drain_to_empty: Arc<AtomicBool>,
    hard_stop: Arc<AtomicBool>,
) -> sqlx::Result<DrainStats> {
    let started = Instant::now();
    let mut stats = DrainStats::default();
    loop {
        if hard_stop.load(Ordering::Relaxed) {
            break;
        }
        let processed = drain_one_super_batch(&pool, cfg.batch_size, &mut stats).await?;
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

async fn drain_one_super_batch(
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

    // Split rows into two groups by override flag — each becomes its
    // own super-batch attempt.
    let mut group_false: Vec<(i64, serde_json::Value)> = Vec::new();
    let mut group_true: Vec<(i64, serde_json::Value)> = Vec::new();
    for r in &rows {
        let id: i64 = r.try_get("id")?;
        let events: serde_json::Value = r.try_get("events")?;
        let override_closed: bool = r.try_get("override_closed_period")?;
        if override_closed {
            group_true.push((id, events));
        } else {
            group_false.push((id, events));
        }
    }

    for (group, override_flag) in [
        (group_false, false),
        (group_true, true),
    ] {
        if group.is_empty() {
            continue;
        }
        try_super_batch_or_fallback(&mut tx, &group, override_flag, stats).await?;
    }

    tx.commit().await?;
    Ok(n)
}

async fn try_super_batch_or_fallback(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    group: &[(i64, serde_json::Value)],
    override_flag: bool,
    stats: &mut DrainStats,
) -> sqlx::Result<()> {
    // Concatenate every row's events array into one big JSONB array.
    let mut merged: Vec<serde_json::Value> = Vec::new();
    for (_id, events) in group {
        if let Some(arr) = events.as_array() {
            merged.extend(arr.iter().cloned());
        }
    }
    let merged_value = serde_json::Value::Array(merged);

    stats.super_batch_attempts += 1;
    let mut sp = tx.begin().await?;
    let result: sqlx::Result<serde_json::Value> = sqlx::query_scalar(
        "SELECT post_transfers($1, $2)",
    )
    .bind(&merged_value)
    .bind(override_flag)
    .fetch_one(&mut *sp)
    .await;

    match result {
        Ok(_) => {
            sp.commit().await?;
            let ids: Vec<i64> = group.iter().map(|(id, _)| *id).collect();
            sqlx::query(
                "UPDATE ledger_outbox
                    SET status = 'committed',
                        committed_at = clock_timestamp(),
                        attempt_count = attempt_count + 1
                  WHERE id = ANY($1)",
            )
            .bind(&ids)
            .execute(&mut **tx)
            .await?;
            stats.rows_committed += group.len() as u64;
            stats.super_batch_successes += 1;
        }
        Err(_) => {
            sp.rollback().await?;
            stats.super_batch_fallbacks += 1;
            // Per-row fallback: each row gets its own savepoint + post_transfers.
            for (id, events) in group {
                let mut sp2 = tx.begin().await?;
                let res: sqlx::Result<serde_json::Value> =
                    sqlx::query_scalar("SELECT post_transfers($1, $2)")
                        .bind(events)
                        .bind(override_flag)
                        .fetch_one(&mut *sp2)
                        .await;
                match res {
                    Ok(_) => {
                        sp2.commit().await?;
                        sqlx::query(
                            "UPDATE ledger_outbox
                                SET status = 'committed',
                                    committed_at = clock_timestamp(),
                                    attempt_count = attempt_count + 1
                              WHERE id = $1",
                        )
                        .bind(id)
                        .execute(&mut **tx)
                        .await?;
                        stats.rows_committed += 1;
                    }
                    Err(e) => {
                        sp2.rollback().await?;
                        let (sqlstate, msg) = match e.as_database_error() {
                            Some(db) => {
                                (db.code().map(|c| c.to_string()), db.message().to_string())
                            }
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
                        .execute(&mut **tx)
                        .await?;
                        stats.rows_failed += 1;
                    }
                }
            }
        }
    }
    Ok(())
}
