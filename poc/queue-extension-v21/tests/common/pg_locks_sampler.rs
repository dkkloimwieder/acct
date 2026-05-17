//! M9.2 (acct-4d4n.21) — postgres-side lock contention sampler.
//!
//! Adapted from `tests/common/pg_locks_sampler.rs` in the acct repo
//! (acct-8hv2, 2026-05-11). Spawns a background task that polls
//! `pg_locks` + `pg_stat_activity` at a fixed interval (default 100 ms
//! = 10 Hz) during a load cell. Accumulates per-(relation, locktype,
//! mode, granted) wait counts + per-(wait_event_type, wait_event)
//! counts. On shutdown emits a `SamplerReport`.
//!
//! ## Why it's here
//!
//! M9.2 spec §5.3 requires per-cell lock-contention evidence and a
//! sampler perturbation sanity check (re-run one cell with the
//! sampler off; combined p99 must stay inside the sampler-on IQR).
//! This is the same shape acct-8hv2 used to falsify the "1s6r is
//! spread across accounts" hypothesis.
//!
//! ## Perturbation
//!
//! The sampler uses 1 dedicated connection (caller's pool must size
//! for it). Sampling cost at 10 Hz is a fraction of a percent of
//! typical workload CPU.

use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
struct LockSample {
    relation: String,
    locktype: String,
    mode: String,
    granted: bool,
    waiter_count: i64,
}

#[derive(Debug, Clone)]
struct WaitSample {
    wait_event_type: String,
    wait_event: String,
    backend_count: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SamplerReport {
    pub samples_taken: u64,
    pub duration_ms: u64,
    pub lock_observations: HashMap<(String, String, String, bool), i64>,
    pub wait_observations: HashMap<(String, String), i64>,
    pub poll_errors: u64,
}

impl SamplerReport {
    pub fn format(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "--- pg_locks sampler report (samples={} dur={}ms errors={}) ---\n",
            self.samples_taken, self.duration_ms, self.poll_errors
        ));
        let mut locks: Vec<_> = self.lock_observations.iter().collect();
        locks.sort_by(|a, b| b.1.cmp(a.1));
        out.push_str(&format!(
            "{:<32} {:<18} {:<28} {:>9} {:>14}\n",
            "relation", "locktype", "mode", "granted", "sum_waiters"
        ));
        for ((rel, lt, mode, granted), count) in locks.iter().take(40) {
            out.push_str(&format!(
                "{:<32} {:<18} {:<28} {:>9} {:>14}\n",
                rel, lt, mode, granted, count
            ));
        }
        let mut waits: Vec<_> = self.wait_observations.iter().collect();
        waits.sort_by(|a, b| b.1.cmp(a.1));
        out.push_str("--- pg_stat_activity wait_event histogram ---\n");
        out.push_str(&format!(
            "{:<22} {:<32} {:>16}\n",
            "wait_event_type", "wait_event", "sum_backends"
        ));
        for ((wet, we), count) in waits.iter().take(30) {
            out.push_str(&format!("{:<22} {:<32} {:>16}\n", wet, we, count));
        }
        out
    }

    /// Compact one-line wait_event summary for the JSON record.
    pub fn top_wait_event(&self) -> Option<(String, String, i64)> {
        self.wait_observations
            .iter()
            .max_by_key(|(_, c)| **c)
            .map(|((wet, we), c)| (wet.clone(), we.clone(), *c))
    }
}

pub struct PgLocksSampler {
    handle: Option<JoinHandle<SamplerReport>>,
    stop: Arc<AtomicBool>,
}

impl PgLocksSampler {
    pub async fn spawn(pool: PgPool, interval_ms: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let pool_clone = pool.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(interval_ms);
            let started = Instant::now();
            let mut report = SamplerReport::default();

            loop {
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                let tick_started = Instant::now();

                let lock_rows: Result<Vec<LockSample>, sqlx::Error> =
                    sqlx::query_as::<_, (Option<String>, String, String, bool, i64)>(
                        r#"
                        SELECT COALESCE(c.relname, '<no_rel>')::text AS relation,
                               l.locktype::text                       AS locktype,
                               l.mode::text                           AS mode,
                               l.granted                              AS granted,
                               COUNT(*)::BIGINT                       AS waiter_count
                          FROM pg_locks l
                          LEFT JOIN pg_class c ON c.oid = l.relation
                         WHERE l.pid <> pg_backend_pid()
                         GROUP BY c.relname, l.locktype, l.mode, l.granted
                        "#,
                    )
                    .fetch_all(&pool_clone)
                    .await
                    .map(|rows| {
                        rows.into_iter()
                            .map(|(rel, locktype, mode, granted, count)| {
                                LockSample {
                                    relation: rel
                                        .unwrap_or_else(|| "<no_rel>".to_string()),
                                    locktype,
                                    mode,
                                    granted,
                                    waiter_count: count,
                                }
                            })
                            .collect()
                    });

                match lock_rows {
                    Ok(rows) => {
                        for s in rows {
                            *report
                                .lock_observations
                                .entry((s.relation, s.locktype, s.mode, s.granted))
                                .or_insert(0) += s.waiter_count;
                        }
                    }
                    Err(_) => {
                        report.poll_errors += 1;
                    }
                }

                let wait_rows: Result<Vec<WaitSample>, sqlx::Error> =
                    sqlx::query_as::<_, (Option<String>, Option<String>, i64)>(
                        r#"
                        SELECT wait_event_type::text,
                               wait_event::text,
                               COUNT(*)::BIGINT
                          FROM pg_stat_activity
                         WHERE wait_event_type IS NOT NULL
                           AND state = 'active'
                           AND pid <> pg_backend_pid()
                         GROUP BY wait_event_type, wait_event
                        "#,
                    )
                    .fetch_all(&pool_clone)
                    .await
                    .map(|rows| {
                        rows.into_iter()
                            .map(|(wet, we, count)| WaitSample {
                                wait_event_type: wet.unwrap_or_default(),
                                wait_event: we.unwrap_or_default(),
                                backend_count: count,
                            })
                            .collect()
                    });

                match wait_rows {
                    Ok(rows) => {
                        for s in rows {
                            *report
                                .wait_observations
                                .entry((s.wait_event_type, s.wait_event))
                                .or_insert(0) += s.backend_count;
                        }
                    }
                    Err(_) => {
                        report.poll_errors += 1;
                    }
                }

                report.samples_taken += 1;
                let elapsed = tick_started.elapsed();
                if elapsed < interval {
                    tokio::time::sleep(interval - elapsed).await;
                }
            }

            report.duration_ms = started.elapsed().as_millis() as u64;
            report
        });

        Self {
            handle: Some(handle),
            stop,
        }
    }

    pub async fn shutdown(mut self) -> SamplerReport {
        self.stop.store(true, Ordering::Relaxed);
        match self.handle.take() {
            Some(h) => h.await.unwrap_or_default(),
            None => SamplerReport::default(),
        }
    }
}

impl Drop for PgLocksSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}
