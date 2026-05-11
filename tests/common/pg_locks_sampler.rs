//! acct-8hv2 — postgres-side lock contention sampler.
//!
//! ## What it does
//!
//! Spawns a background task that polls `pg_locks` + `pg_stat_activity`
//! at a fixed interval (default 100 ms = 10 Hz) during a load run.
//! Accumulates per-(relation, locktype, mode) wait counts and per-
//! (wait_event_type, wait_event) wait counts. On shutdown, emits a
//! `SamplerReport` that callers print alongside the existing T4 timings.
//!
//! ## Why it exists
//!
//! Prior to this module, the codebase had no postgres-side contention
//! measurement. Statements like "1s6r workload is spread across many
//! accounts" were *inferred* from low deadlock rates rather than
//! observed from lock distribution. The audit (acct-8hv2) introduces
//! this module to give Phase D (per-row hot-row histogram + revised
//! contention model) an evidence base.
//!
//! ## Usage
//!
//! ```ignore
//! let sampler = PgLocksSampler::spawn(pool.clone(), 100).await;
//! // ... run workload ...
//! let report = sampler.shutdown().await;
//! eprintln!("{}", report.format());
//! ```
//!
//! ## Perturbation
//!
//! The sampler uses 1 dedicated connection (caller must size their pool
//! for this). Sampling cost is one `SELECT count(*)`-shaped query per
//! interval, executed by a single background task. At 10 Hz the load is
//! a fraction of a percent of typical workload CPU. The audit's Phase B
//! validates this empirically by re-running Phase A's baseline with
//! `T4_LOCK_SAMPLE=1` and confirming combined p99 stays inside Phase A's
//! IQR.

use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// One observation of a (relation, locktype, mode) waiter group.
#[derive(Debug, Clone)]
struct LockSample {
    relation: String,
    locktype: String,
    mode: String,
    granted: bool,
    waiter_count: i64,
}

/// One observation of a wait event seen on a backend.
#[derive(Debug, Clone)]
struct WaitSample {
    wait_event_type: String,
    wait_event: String,
    backend_count: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SamplerReport {
    /// Total polling samples taken during the run.
    pub samples_taken: u64,
    /// Wall-clock duration the sampler was active.
    pub duration_ms: u64,
    /// (relation, locktype, mode, granted) → sum of waiter_count across all samples.
    pub lock_observations: HashMap<(String, String, String, bool), i64>,
    /// (wait_event_type, wait_event) → sum of backend_count across all samples.
    pub wait_observations: HashMap<(String, String), i64>,
    /// Polling errors (e.g., transient connection issues). Non-zero is
    /// suspicious but not fatal; reported in the summary.
    pub poll_errors: u64,
}

impl SamplerReport {
    /// Render the report as a human-readable block, ready to eprintln.
    pub fn format(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "--- acct-8hv2: pg_locks sampler report (samples={} dur={}ms errors={}) ---\n",
            self.samples_taken, self.duration_ms, self.poll_errors
        ));

        // Sort lock observations by total waiter_count descending.
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

        // Sort wait observations by backend_count descending.
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
}

/// Spawned sampler handle.
pub struct PgLocksSampler {
    handle: Option<JoinHandle<SamplerReport>>,
    stop: Arc<AtomicBool>,
}

impl PgLocksSampler {
    /// Spawn the sampling task. Uses 1 connection from the caller's
    /// pool. Polls at `interval_ms` (typical: 100).
    pub async fn spawn(pool: PgPool, interval_ms: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let pool_clone = pool.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(interval_ms);
            let started = Instant::now();
            let mut report = SamplerReport::default();

            // Pre-fetch the OID → relname map once. Avoids a join per
            // sample. (oid::regclass cast is cheap but adds noise to
            // pg_stat_statements; do it once.)
            //
            // We resolve relation names lazily inside the loop via
            // pg_locks JOIN pg_class because partition children produce
            // many distinct relids and we want them visible separately.

            loop {
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                let tick_started = Instant::now();

                // Query 1: pg_locks grouped. Includes BOTH granted and
                // not-granted because granted FOR UPDATEs on hot rows
                // are also signal (someone is holding the row; that's
                // why others are waiting).
                let lock_rows: Result<Vec<LockSample>, sqlx::Error> = sqlx::query_as::<
                    _,
                    (Option<String>, String, String, bool, i64),
                >(
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
                        .map(|(rel, locktype, mode, granted, count)| LockSample {
                            relation: rel.unwrap_or_else(|| "<no_rel>".to_string()),
                            locktype,
                            mode,
                            granted,
                            waiter_count: count,
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

                // Query 2: pg_stat_activity wait events.
                let wait_rows: Result<Vec<WaitSample>, sqlx::Error> = sqlx::query_as::<
                    _,
                    (Option<String>, Option<String>, i64),
                >(
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

                // Pace by tick_started so we approximate a real interval
                // even when queries take some time.
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

    /// Signal the sampler to stop and await its final report.
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
        // Best-effort cleanup if caller forgot to shutdown.
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}
