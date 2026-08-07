//! Throughput / WAL / wait-event measurement collection (design-v3.1 §10/§11).
//!
//! Three surfaces:
//!
//! 1. `LatencyHistogram` — per-task hdrhistogram. Drivers create one per caller,
//!    record(nanos) per submission, merge at end. For the direct modes this
//!    captures the per-trx ack latency whose insensitivity to pool depth is the
//!    §11.2 headline (the in-function critical section is dominated by pool_lock
//!    hold time; depth is the only thing that varies across a seed sweep).
//!
//! 2. `CounterSnapshot` + `take_snapshot` — foreground xact_commit/rollback +
//!    WAL insert LSN. Caller diffs start/end. We use `pg_current_wal_insert_lsn()`
//!    rather than `pg_stat_wal.wal_bytes` because PG 18's cumulative-stats views
//!    only flush at session end (zero from a long-lived driver connection).
//!
//! 3. `MeasureCollector` — 1 Hz background wait-event sampler on active backends.
//!
//! Why not fsync count: PG 18 dropped `pg_stat_wal.wal_sync`; the replacement
//! `pg_stat_io.fsyncs` only updates at backend session end. The report surfaces
//! WAL bytes per commit instead (proxy for group-commit amortization — the §5.5
//! batched-mode win).

use hdrhistogram::Histogram;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

// ── Per-task latency histogram ─────────────────────────────────────

pub struct LatencyHistogram {
    pub hist: Histogram<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    /// 1ns precision, range 1ns..=1h, 3 significant digits.
    pub fn new() -> Self {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("valid HdrHistogram bounds");
        Self { hist }
    }

    pub fn record(&mut self, nanos: u64) {
        let _ = self.hist.record(nanos);
    }

    pub fn merge_all(parts: Vec<LatencyHistogram>) -> Histogram<u64> {
        let mut agg = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("valid HdrHistogram bounds");
        for p in parts {
            agg.add(&p.hist).expect("merge histograms");
        }
        agg
    }
}

// ── Counter snapshots ──────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct CounterSnapshot {
    pub xact_commit: i64,
    pub xact_rollback: i64,
    /// Bytes consumed by WAL inserts up to this point. Always live (PG 18's
    /// pg_stat_wal flushes are not).
    pub wal_lsn_bytes: i64,
}

/// Foreground snapshot. Diff start/end for deltas.
pub async fn take_snapshot(pool: &PgPool) -> Result<CounterSnapshot, sqlx::Error> {
    let (xc, xr): (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(xact_commit, 0)::bigint, COALESCE(xact_rollback, 0)::bigint \
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await?;

    let lsn: i64 = sqlx::query_scalar("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint")
        .fetch_one(pool)
        .await?;

    Ok(CounterSnapshot { xact_commit: xc, xact_rollback: xr, wal_lsn_bytes: lsn })
}

#[derive(Debug, Clone, Default)]
pub struct MeasureReport {
    pub samples_taken: u64,
    pub duration_ms: u64,
    pub poll_errors: u64,
    pub xact_commit_delta: i64,
    pub xact_rollback_delta: i64,
    pub wal_lsn_bytes_delta: i64,
    pub wait_events: HashMap<(String, String), i64>,
}

impl MeasureReport {
    /// Bytes of WAL emitted per committed tx (or NaN).
    pub fn wal_bytes_per_commit(&self) -> f64 {
        if self.xact_commit_delta <= 0 {
            f64::NAN
        } else {
            self.wal_lsn_bytes_delta as f64 / self.xact_commit_delta as f64
        }
    }
}

// ── Background wait-event collector ─────────────────────────────────

pub struct MeasureCollector {
    handle: Option<JoinHandle<MeasureReport>>,
    stop: Arc<AtomicBool>,
}

impl MeasureCollector {
    pub fn spawn(pool: PgPool, interval_ms: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let pool_clone = pool.clone();

        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis(interval_ms);
            let started = Instant::now();
            let mut report = MeasureReport::default();

            loop {
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                let tick_started = Instant::now();

                let waits: Result<Vec<(Option<String>, Option<String>, i64)>, sqlx::Error> =
                    sqlx::query_as(
                        "SELECT wait_event_type::text, wait_event::text, COUNT(*)::bigint \
                           FROM pg_stat_activity \
                          WHERE wait_event_type IS NOT NULL \
                            AND state = 'active' \
                            AND pid <> pg_backend_pid() \
                          GROUP BY wait_event_type, wait_event",
                    )
                    .fetch_all(&pool_clone)
                    .await;
                match waits {
                    Ok(rows) => {
                        for (wet, we, count) in rows {
                            *report
                                .wait_events
                                .entry((wet.unwrap_or_default(), we.unwrap_or_default()))
                                .or_insert(0) += count;
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

    pub async fn shutdown(mut self) -> MeasureReport {
        self.stop.store(true, Ordering::Relaxed);
        match self.handle.take() {
            Some(h) => h.await.unwrap_or_default(),
            None => MeasureReport::default(),
        }
    }
}

impl Drop for MeasureCollector {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_histogram_records_and_merges() {
        let mut a = LatencyHistogram::new();
        let mut b = LatencyHistogram::new();
        a.record(1_000);
        a.record(2_000);
        b.record(3_000);
        let agg = LatencyHistogram::merge_all(vec![a, b]);
        assert_eq!(agg.len(), 3);
        assert!(agg.min() <= 1_000);
        assert!(agg.max() >= 3_000);
    }

    #[test]
    fn wal_bytes_per_commit_nan_on_no_commits() {
        assert!(MeasureReport::default().wal_bytes_per_commit().is_nan());
    }

    #[test]
    fn wal_bytes_per_commit_computes_ratio() {
        let r = MeasureReport {
            xact_commit_delta: 100,
            wal_lsn_bytes_delta: 500_000,
            ..Default::default()
        };
        assert_eq!(r.wal_bytes_per_commit(), 5_000.0);
    }
}
