//! JSON report output for the `run` subcommand (design-v3.1 §10/§11).
//!
//! One file per run invocation. The `mode` field records which of the three
//! §10.0 submission modes produced it; `pool_depth` records the seeded depth so
//! the §11.2 lock-hold-vs-depth sweep can be tabulated across runs (ack latency
//! must stay flat as depth grows — that is the architectural premise). The
//! `routed` block (mode = routed) carries the committer counters that quantify
//! the §6.7 batching win: commit_group_size_avg and the ratio of trx committed
//! to pool_lock acquisitions / aggregate upserts.

use std::path::Path;

use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::Serialize;

use crate::measure::MeasureReport;
use crate::sampler::SamplerReport;

#[derive(Debug, Serialize)]
pub struct LatencyPercentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    /// The tail the open-loop / coordinated-omission methodology cares about
    /// (acct-0at4.8): a p99-only headline hides multi-second stalls that a
    /// closed-loop generator omits entirely. Reported alongside p99 so the
    /// throughput-at-SLO ramp can gate on either.
    pub p999: u64,
    pub min: u64,
    pub max: u64,
    pub mean: u64,
    pub count: u64,
}

impl LatencyPercentiles {
    /// Microsecond-valued percentiles from a nanosecond-valued histogram.
    pub fn from_ns_hist(hist: &Histogram<u64>) -> Self {
        Self {
            p50: hist.value_at_quantile(0.50) / 1_000,
            p95: hist.value_at_quantile(0.95) / 1_000,
            p99: hist.value_at_quantile(0.99) / 1_000,
            p999: hist.value_at_quantile(0.999) / 1_000,
            min: hist.min() / 1_000,
            max: hist.max() / 1_000,
            mean: (hist.mean() / 1_000.0) as u64,
            count: hist.len(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TopWaitEvent {
    pub wait_event_type: String,
    pub wait_event: String,
    pub samples: i64,
}

/// Routed-mode committer counters (deltas over the run window). Sourced from
/// the `ledger_routed_c_committer_*` accessors. Null on direct-mode reports.
#[derive(Debug, Serialize, Default)]
pub struct RoutedReport {
    /// commit_groups drained by the committer pool.
    pub drains_total: u64,
    /// trx rows committed (≈ submissions successfully processed).
    pub trx_committed_total: u64,
    /// Mean submissions per commit_group (trx_committed / drains) — the §6.7
    /// cross-caller batching ratio.
    pub commit_group_size_avg: f64,
    /// pool_lock FOR UPDATE acquisitions. The batching win: 1000 concurrent
    /// submissions to one hot pool collapse toward one acquisition per group.
    pub pool_lock_acquisitions_total: u64,
    /// Aggregate-row UPSERTs (one per pool per commit_group in steady state).
    pub aggregate_upserts_total: u64,
    /// Pre-flight dedup skips (already-recorded (trx_type, source_id)).
    pub dedup_skips_total: u64,
    /// Submissions dropped (failed plan_apply_provisional) but siblings committed.
    pub dropped_submissions_total: u64,
    /// Subtransaction write-phase failures.
    pub tx_failures_total: u64,
    /// Commit_groups poisoned (non-retryable error / exhausted retry budget, §6.8).
    pub poisoned_total: u64,
    /// Deadlock/serialization retries (40P01/40001 with backoff, §6.8).
    pub deadlock_retries_total: u64,
    /// Dead-committer commit_group takeovers (§6.5 recovery).
    pub takeover_count: u64,

    // ── Committer pipeline-span decomposition (acct-0usf STEP 1) ──────
    // Cumulative ns over the window, summed across all committer workers. The
    // breakdown answers "where does committer wall-time go" directly, instead of
    // inferring the bottleneck from throughput. `pool_lock_ns_total` is the
    // cross-committer hot-pool FOR UPDATE handoff span affinity targets.
    pub txn_ns_total: u64,
    pub pipeline_ns_total: u64,
    pub pool_lock_ns_total: u64,
    pub hydrate_ns_total: u64,
    pub apply_ns_total: u64,
    /// Derived: BEGIN/COMMIT/fsync wrapper cost = txn − pipeline (clamped ≥0).
    pub commit_ns_total: u64,
    /// Derived: decode + triage + dedup + line-decode = pipeline − (pool_lock +
    /// hydrate + apply) (clamped ≥0).
    pub prep_ns_total: u64,
    /// Each span as a fraction of `txn_ns_total` (0.0 if no transactions). The
    /// per-scenario row of the "where does committer wall-time go" table.
    pub pool_lock_frac: f64,
    pub hydrate_frac: f64,
    pub apply_frac: f64,
    pub commit_frac: f64,
    pub prep_frac: f64,

    // ── Committer wait-event segmentation (acct-0usf STEP 1b) ─────────
    // Sourced from the committer-only pg_stat_activity histogram, NOT the
    // in-process spans. Cross-checks the spans: span pool_lock_frac tells you how
    // much wall-time is inside acquire_pool_locks; these tell you whether that
    // time is spent BLOCKED on another committer's row lock (committer_lock_frac_
    // of_busy, the affinity-addressable handoff) vs running the SELECT on-CPU
    // (committer_running_frac_of_busy, irreducible). Zero when --no-sampler.
    pub committer_samples_total: i64,
    pub committer_idle_samples: i64,
    /// Non-idle / total — committer pool utilization. Low ⇒ spare capacity, the
    /// ceiling is upstream (router/arrival), affinity is a priori pointless.
    pub committer_busy_frac: f64,
    /// Of busy samples, share blocked on a row lock (the cross-committer handoff
    /// — the only span committer→pool affinity can shrink).
    pub committer_lock_frac_of_busy: f64,
    /// Of busy samples, share on-CPU (query execution — affinity-irreducible).
    pub committer_running_frac_of_busy: f64,
    /// Of busy samples, share on shmem LWLock contention (staging ring / arena —
    /// a separate bottleneck affinity does not address).
    pub committer_lwlock_frac_of_busy: f64,
}

/// Multi-touch overlay parameters in effect for the run (acct-34ce). Null when
/// the workload was distinct-pool (multi_touch_pct = 0), so every report
/// self-documents whether it exercised the coalesce-under-load path.
#[derive(Debug, Serialize)]
pub struct MultiTouchReport {
    /// Percent of submissions eligible to repeat a pool.
    pub pct: u8,
    /// Per-pool group-size distribution, `touches:weight,…`.
    pub touch_dist: String,
}

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub scenario: String,
    /// Submission mode label (§10.0): direct-per-call | direct-batched | routed.
    pub mode: String,
    /// Pool depth the universe was seeded to (§10.5); None if unspecified.
    pub pool_depth: Option<usize>,
    /// Per-caller workload RNG seed (acct-0at4.10.4) this run was driven with, so
    /// each result JSON self-documents which sample it is in a multi-rep sweep.
    /// Set via `with_seed`; the constructors default it to the historical stream.
    pub seed: u64,
    /// Multi-touch overlay (acct-34ce); None for distinct-pool runs.
    pub multi_touch: Option<MultiTouchReport>,
    /// Total offered arrival rate (trx/s) when driven open-loop (acct-0at4.8);
    /// None for full-blast closed-loop runs. Paired with `throughput_trx_per_sec`
    /// (the *achieved* rate) it is the offered-vs-achieved signal the saturation
    /// ramp plots — the point where achieved falls below offered is saturation.
    pub offered_rate_trx_per_sec: Option<u64>,
    /// Arrival process driving the open-loop schedule: "poisson" | "uniform".
    /// None for closed-loop.
    pub arrival_process: Option<String>,
    pub duration_secs: f64,
    pub callers: usize,
    pub throughput_trx_per_sec: f64,
    pub attempts_total: u64,
    pub errors_total: u64,
    pub ack_latency_us: LatencyPercentiles,
    pub committed_latency_us: LatencyPercentiles,
    pub commits_observed: i64,
    pub rollbacks_observed: i64,
    pub wal_bytes_per_trx: f64,
    pub top_wait_events: Vec<TopWaitEvent>,
    pub routed: Option<RoutedReport>,
    pub sampler_report_path: Option<String>,
    pub started_at: DateTime<Utc>,
}

impl RunReport {
    /// Direct-mode constructor (per-call or batched). ack and committed
    /// latencies are the same (synchronous tx).
    #[allow(clippy::too_many_arguments)]
    pub fn new_direct(
        scenario: impl Into<String>,
        mode: impl Into<String>,
        pool_depth: Option<usize>,
        callers: usize,
        duration_secs: f64,
        ack: &Histogram<u64>,
        errors_total: u64,
        measure: &MeasureReport,
        sampler: &SamplerReport,
        sampler_report_path: Option<String>,
        started_at: DateTime<Utc>,
    ) -> Self {
        let attempts_total = ack.len() + errors_total;
        Self {
            scenario: scenario.into(),
            mode: mode.into(),
            pool_depth,
            seed: 0xDEAD_BEEF_u64,
            multi_touch: None,
            offered_rate_trx_per_sec: None,
            arrival_process: None,
            duration_secs,
            callers,
            throughput_trx_per_sec: if duration_secs > 0.0 {
                ack.len() as f64 / duration_secs
            } else {
                0.0
            },
            attempts_total,
            errors_total,
            ack_latency_us: LatencyPercentiles::from_ns_hist(ack),
            committed_latency_us: LatencyPercentiles::from_ns_hist(ack),
            commits_observed: measure.xact_commit_delta,
            rollbacks_observed: measure.xact_rollback_delta,
            wal_bytes_per_trx: measure.wal_bytes_per_commit(),
            top_wait_events: top_wait_events(sampler, 5),
            routed: None,
            sampler_report_path,
            started_at,
        }
    }

    /// Routed-mode constructor. Throughput derives from `trx_count` (rows the
    /// observer saw materialize), independent of caller blocking shape.
    #[allow(clippy::too_many_arguments)]
    pub fn new_routed(
        scenario: impl Into<String>,
        pool_depth: Option<usize>,
        callers: usize,
        duration_secs: f64,
        trx_count: u64,
        ack: &Histogram<u64>,
        committed: &Histogram<u64>,
        errors_total: u64,
        measure: &MeasureReport,
        sampler: &SamplerReport,
        sampler_report_path: Option<String>,
        started_at: DateTime<Utc>,
        routed: RoutedReport,
    ) -> Self {
        let attempts_total = ack.len() + errors_total;
        Self {
            scenario: scenario.into(),
            mode: "routed".into(),
            pool_depth,
            seed: 0xDEAD_BEEF_u64,
            multi_touch: None,
            offered_rate_trx_per_sec: None,
            arrival_process: None,
            duration_secs,
            callers,
            throughput_trx_per_sec: if duration_secs > 0.0 {
                trx_count as f64 / duration_secs
            } else {
                0.0
            },
            attempts_total,
            errors_total,
            ack_latency_us: LatencyPercentiles::from_ns_hist(ack),
            committed_latency_us: LatencyPercentiles::from_ns_hist(committed),
            commits_observed: measure.xact_commit_delta,
            rollbacks_observed: measure.xact_rollback_delta,
            wal_bytes_per_trx: measure.wal_bytes_per_commit(),
            top_wait_events: top_wait_events(sampler, 5),
            routed: Some(routed),
            sampler_report_path,
            started_at,
        }
    }

    /// Staging-mode constructor (SPIKE-A / acct-0at4.11.1). Like `new_routed`,
    /// throughput derives from `trx_count` (rows the observer saw materialize) and
    /// committed latency from the observer-timestamped `committed` histogram —
    /// the caller's INSERT ack is decoupled from materialization by the drain. No
    /// routed-committer counter block (that machinery is exactly what staging
    /// deletes); `mode` is "staging".
    #[allow(clippy::too_many_arguments)]
    pub fn new_staging(
        scenario: impl Into<String>,
        pool_depth: Option<usize>,
        callers: usize,
        duration_secs: f64,
        trx_count: u64,
        ack: &Histogram<u64>,
        committed: &Histogram<u64>,
        errors_total: u64,
        measure: &MeasureReport,
        sampler: &SamplerReport,
        sampler_report_path: Option<String>,
        started_at: DateTime<Utc>,
    ) -> Self {
        let attempts_total = ack.len() + errors_total;
        Self {
            scenario: scenario.into(),
            mode: "staging".into(),
            pool_depth,
            seed: 0xDEAD_BEEF_u64,
            multi_touch: None,
            offered_rate_trx_per_sec: None,
            arrival_process: None,
            duration_secs,
            callers,
            throughput_trx_per_sec: if duration_secs > 0.0 {
                trx_count as f64 / duration_secs
            } else {
                0.0
            },
            attempts_total,
            errors_total,
            ack_latency_us: LatencyPercentiles::from_ns_hist(ack),
            committed_latency_us: LatencyPercentiles::from_ns_hist(committed),
            commits_observed: measure.xact_commit_delta,
            rollbacks_observed: measure.xact_rollback_delta,
            wal_bytes_per_trx: measure.wal_bytes_per_commit(),
            top_wait_events: top_wait_events(sampler, 5),
            routed: None,
            sampler_report_path,
            started_at,
        }
    }

    /// Attach the multi-touch overlay parameters (acct-34ce). Pass None for a
    /// distinct-pool run.
    pub fn with_multi_touch(mut self, mt: Option<MultiTouchReport>) -> Self {
        self.multi_touch = mt;
        self
    }

    /// Record the open-loop arrival parameters (acct-0at4.8) so the result
    /// self-documents its offered rate and arrival process. Pass `None` offered
    /// rate for a closed-loop (full-blast) run.
    pub fn with_open_loop(
        mut self,
        offered_rate: Option<u64>,
        arrival: crate::pacing::ArrivalProcess,
    ) -> Self {
        self.offered_rate_trx_per_sec = offered_rate;
        self.arrival_process = offered_rate.map(|_| arrival.label().to_string());
        self
    }

    /// Record the per-caller workload RNG seed (acct-0at4.10.4) so each result
    /// JSON self-identifies its sample in a multi-rep re-measurement sweep.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }
}

fn top_wait_events(s: &SamplerReport, n: usize) -> Vec<TopWaitEvent> {
    let mut v: Vec<_> = s
        .wait_observations
        .iter()
        .map(|((wet, we), c)| TopWaitEvent {
            wait_event_type: wet.clone(),
            wait_event: we.clone(),
            samples: *c,
        })
        .collect();
    v.sort_by(|a, b| b.samples.cmp(&a.samples));
    v.truncate(n);
    v
}

/// Serialize `report` as pretty JSON to `path`.
pub fn write_to_path(report: &RunReport, path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(path, json)
}

/// Default output path builder: `results/<scenario>-<mode>-<ts>.json`.
pub fn default_output_path(scenario: &str, mode: &str, when: DateTime<Utc>) -> std::path::PathBuf {
    let ts = when.format("%Y-%m-%dT%H-%M-%S");
    std::path::PathBuf::from(format!("results/{scenario}-{mode}-{ts}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn hist_with_values(vs: &[u64]) -> Histogram<u64> {
        let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap();
        for &v in vs {
            h.record(v).unwrap();
        }
        h
    }

    #[test]
    fn percentiles_match_simple_distribution() {
        let h = hist_with_values(&[100_000, 200_000, 300_000, 400_000, 500_000]);
        let p = LatencyPercentiles::from_ns_hist(&h);
        assert_eq!(p.count, 5);
        assert!(p.min <= 100 && p.max >= 500);
        assert!(p.p50 >= 200 && p.p50 <= 400);
    }

    #[test]
    fn top_wait_events_sorted_desc_and_truncated() {
        let mut sr = SamplerReport::default();
        sr.wait_observations.insert(("LWLock".into(), "LockManager".into()), 100);
        sr.wait_observations.insert(("Client".into(), "ClientRead".into()), 50);
        sr.wait_observations.insert(("IO".into(), "WALSync".into()), 75);
        let top = top_wait_events(&sr, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].wait_event, "LockManager");
        assert_eq!(top[1].wait_event, "WALSync");
    }

    #[test]
    fn default_output_path_shape() {
        let when = DateTime::parse_from_rfc3339("2026-05-25T12:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let p = default_output_path("s7", "direct-batched", when);
        assert_eq!(p.to_str().unwrap(), "results/s7-direct-batched-2026-05-25T12-00-00.json");
    }

    #[test]
    fn write_to_path_round_trips_through_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested").join("r.json");

        let when = Utc::now();
        let ack = hist_with_values(&[100, 200, 300]);
        let measure = MeasureReport {
            xact_commit_delta: 3,
            wal_lsn_bytes_delta: 1000,
            wait_events: HashMap::new(),
            ..Default::default()
        };
        let sampler = SamplerReport::default();

        let r = RunReport::new_direct(
            "s7", "direct-per-call", Some(1000), 10, 1.0, &ack, 7, &measure, &sampler, None, when,
        );
        write_to_path(&r, &path).expect("write");
        let raw = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["scenario"], "s7");
        assert_eq!(parsed["mode"], "direct-per-call");
        assert_eq!(parsed["pool_depth"], 1000);
        assert_eq!(parsed["commits_observed"], 3);
        assert_eq!(parsed["attempts_total"], 10);
        assert_eq!(parsed["errors_total"], 7);
        assert!(parsed["routed"].is_null());
    }
}
