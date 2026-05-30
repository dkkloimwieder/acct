//! Postgres-side lock contention sampler for ledger-v3.1 measurement runs.
//!
//! 10 Hz dedicated-connection sampler accumulating per-(relation, locktype,
//! mode, granted) wait counts and per-(wait_event_type, wait_event) backend
//! counts. On shutdown emits a `SamplerReport`. Wired by the `run` drivers;
//! `--no-sampler` suppresses spawn for perturbation checks.
//!
//! `LEDGER_V3_1_PRINT_SAMPLER=1` dumps `SamplerReport::format()` to a sibling
//! `.sampler.txt` file at end of run.

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
    /// Committer-BGWorker-only wait histogram (acct-0usf STEP 1b). Keyed by
    /// (wait_event_type, wait_event), summed over ticks. UNLIKE `wait_observations`
    /// this does NOT filter `state='active'` (committer BGWorkers report
    /// state=NULL even mid-pipeline) and maps a NULL wait_event to ('Running',
    /// 'Running') so on-CPU samples are counted. The idle bucket is
    /// ('Extension','Extension') — a committer parked on its 50ms latch between
    /// drains. The discriminator the lever-2 pass lacked: of the non-idle
    /// samples, the ('Lock','transactionid') share is the cross-committer
    /// pool_lock handoff (what affinity targets); the ('Running','Running') share
    /// is on-CPU query cost (which affinity cannot reduce).
    pub committer_wait_observations: HashMap<(String, String), i64>,
    pub poll_errors: u64,
}

/// Committer wait-time decomposition (acct-0usf STEP 1b), derived from
/// `committer_wait_observations`. All counts are backend-samples (sum over ticks
/// of committer backends in that bucket), not wall-time — but at a fixed sample
/// rate the sample share IS the time share.
#[derive(Debug, Clone, Default)]
pub struct CommitterWaitSummary {
    /// Total committer backend-samples across the run.
    pub total: i64,
    /// Idle on the latch between drains (('Extension','Extension')).
    pub idle: i64,
    /// Blocked on a heavyweight row lock (wait_event_type='Lock'): the
    /// cross-committer pool_lock handoff. Split in the raw histogram into
    /// Lock/transactionid (waiting on the pool_lock row's writer xid) and
    /// Lock/tuple (waiting on the tuple itself) — both are the handoff affinity
    /// targets, summed here.
    pub lock_wait: i64,
    /// On-CPU (NULL wait_event → ('Running','Running')): query execution cost.
    pub running: i64,
    /// Lightweight-lock contention on the shmem structures (wait_event_type=
    /// 'LWLock', e.g. ledger_v31_staging_queue / _spillover_arena). A SEPARATE
    /// bottleneck from the row-lock handoff: committers contending on the staging
    /// ring / arena, which committer→pool affinity does NOT address. Kept distinct
    /// so it isn't mistaken for either the handoff or on-CPU cost.
    pub lwlock_wait: i64,
    /// Disk / WAL IO waits (wait_event_type in ('IO','WALSync')).
    pub io_wait: i64,
}

impl CommitterWaitSummary {
    /// Non-idle samples: total − idle. The committer pool's actual work.
    pub fn busy(&self) -> i64 {
        (self.total - self.idle).max(0)
    }
    /// Fraction of all committer samples spent NOT idle — committer pool
    /// utilization. Low ⇒ committers have spare capacity (the ceiling is upstream:
    /// router formation or arrival rate), so committer-side affinity cannot help.
    pub fn busy_frac(&self) -> f64 {
        if self.total > 0 { self.busy() as f64 / self.total as f64 } else { 0.0 }
    }
    /// Of the BUSY samples, the share blocked on a row lock. High ⇒ the
    /// cross-committer handoff is real and affinity has something to remove.
    pub fn lock_frac_of_busy(&self) -> f64 {
        let b = self.busy();
        if b > 0 { self.lock_wait as f64 / b as f64 } else { 0.0 }
    }
    /// Of the BUSY samples, the share on-CPU (query execution). Affinity cannot
    /// reduce this — it's the irreducible per-group apply/commit cost.
    pub fn running_frac_of_busy(&self) -> f64 {
        let b = self.busy();
        if b > 0 { self.running as f64 / b as f64 } else { 0.0 }
    }
    /// Of the BUSY samples, the share on shmem LWLock contention (staging ring /
    /// arena). A secondary bottleneck affinity does NOT touch.
    pub fn lwlock_frac_of_busy(&self) -> f64 {
        let b = self.busy();
        if b > 0 { self.lwlock_wait as f64 / b as f64 } else { 0.0 }
    }
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
            out.push_str(&format!("{:<32} {:<18} {:<28} {:>9} {:>14}\n", rel, lt, mode, granted, count));
        }
        let mut waits: Vec<_> = self.wait_observations.iter().collect();
        waits.sort_by(|a, b| b.1.cmp(a.1));
        out.push_str("--- pg_stat_activity wait_event histogram ---\n");
        out.push_str(&format!("{:<22} {:<32} {:>16}\n", "wait_event_type", "wait_event", "sum_backends"));
        for ((wet, we), count) in waits.iter().take(30) {
            out.push_str(&format!("{:<22} {:<32} {:>16}\n", wet, we, count));
        }

        // Committer-segmented wait histogram + summary (acct-0usf STEP 1b).
        let mut cwaits: Vec<_> = self.committer_wait_observations.iter().collect();
        cwaits.sort_by(|a, b| b.1.cmp(a.1));
        out.push_str("--- committer-only wait_event histogram (no state filter; NULL→Running) ---\n");
        out.push_str(&format!("{:<22} {:<32} {:>16}\n", "wait_event_type", "wait_event", "sum_samples"));
        for ((wet, we), count) in cwaits.iter().take(30) {
            out.push_str(&format!("{:<22} {:<32} {:>16}\n", wet, we, count));
        }
        let cs = self.committer_wait_summary();
        out.push_str(&format!(
            "committer summary: total={} idle={} busy={} ({:.1}% util) | of busy: lock={:.1}% running={:.1}% lwlock={:.1}% io={:.1}%\n",
            cs.total,
            cs.idle,
            cs.busy(),
            100.0 * cs.busy_frac(),
            100.0 * cs.lock_frac_of_busy(),
            100.0 * cs.running_frac_of_busy(),
            100.0 * cs.lwlock_frac_of_busy(),
            if cs.busy() > 0 { 100.0 * cs.io_wait as f64 / cs.busy() as f64 } else { 0.0 },
        ));
        out
    }

    #[allow(dead_code)]
    pub fn top_wait_event(&self) -> Option<(String, String, i64)> {
        self.wait_observations
            .iter()
            .max_by_key(|(_, c)| **c)
            .map(|((wet, we), c)| (wet.clone(), we.clone(), *c))
    }

    /// Bucket `committer_wait_observations` into the acct-0usf STEP 1b summary.
    /// Bucketing rules (by wait_event_type, then wait_event):
    ///   ('Extension','Extension') → idle (parked on the latch between drains)
    ///   wait_event_type 'Lock'    → lock_wait (row-lock / transactionid handoff)
    ///   ('Running','Running')      → running (on-CPU, NULL wait_event)
    ///   wait_event_type 'IO' | 'WALSync' | 'LWLock' → io_wait
    /// Everything else still counts toward `total` but no sub-bucket (so the four
    /// sub-buckets need not sum to total; `busy − lock − running − io` is "other").
    pub fn committer_wait_summary(&self) -> CommitterWaitSummary {
        let mut s = CommitterWaitSummary::default();
        for ((wet, we), c) in &self.committer_wait_observations {
            s.total += *c;
            match (wet.as_str(), we.as_str()) {
                ("Extension", "Extension") => s.idle += *c,
                ("Running", "Running") => s.running += *c,
                ("Lock", _) => s.lock_wait += *c,
                ("LWLock", _) => s.lwlock_wait += *c,
                ("IO", _) | ("WALSync", _) | ("IPC", _) => s.io_wait += *c,
                _ => {}
            }
        }
        s
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
                    Err(_) => report.poll_errors += 1,
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
                    Err(_) => report.poll_errors += 1,
                }

                // Committer-segmented wait histogram (acct-0usf STEP 1b). NO
                // state filter (committer BGWorkers report state=NULL even
                // mid-pipeline) and NULL wait_event → 'Running' (on-CPU) so the
                // idle / lock-wait / on-CPU split is complete. Scoped to the
                // committer backend_type so caller/router/autovacuum noise is
                // excluded.
                let committer_rows: Result<Vec<WaitSample>, sqlx::Error> =
                    sqlx::query_as::<_, (String, String, i64)>(
                        r#"
                        SELECT COALESCE(wait_event_type, 'Running')::text,
                               COALESCE(wait_event, 'Running')::text,
                               COUNT(*)::BIGINT
                          FROM pg_stat_activity
                         WHERE backend_type LIKE 'ledger_routed_c_committer%'
                           AND pid <> pg_backend_pid()
                         GROUP BY wait_event_type, wait_event
                        "#,
                    )
                    .fetch_all(&pool_clone)
                    .await
                    .map(|rows| {
                        rows.into_iter()
                            .map(|(wet, we, count)| WaitSample {
                                wait_event_type: wet,
                                wait_event: we,
                                backend_count: count,
                            })
                            .collect()
                    });
                match committer_rows {
                    Ok(rows) => {
                        for s in rows {
                            *report
                                .committer_wait_observations
                                .entry((s.wait_event_type, s.wait_event))
                                .or_insert(0) += s.backend_count;
                        }
                    }
                    Err(_) => report.poll_errors += 1,
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

        Self { handle: Some(handle), stop }
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

/// Read `LEDGER_V3_1_PRINT_SAMPLER` ("1"/"true" → enabled).
pub fn print_sampler_enabled() -> bool {
    std::env::var("LEDGER_V3_1_PRINT_SAMPLER")
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true"
        })
        .unwrap_or(false)
}
