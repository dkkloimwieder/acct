//! JSON report output for the `run` subcommand (acct-giun).
//!
//! One file per run invocation. Schema per plan §F:
//!
//! ```json
//! {
//!   "scenario": "s1",
//!   "path": "direct",
//!   "duration_secs": 30,
//!   "callers": 10,
//!   "throughput_trx_per_sec": 4523.1,
//!   "ack_latency_us": { "p50": 120, "p95": 480, "p99": 980 },
//!   "committed_latency_us": { "p50": 120, "p95": 480, "p99": 980 },
//!   "commits_observed": 135693,
//!   "rollbacks_observed": 0,
//!   "wal_bytes_per_trx": 938.2,
//!   "top_wait_events": [
//!     { "wait_event_type": "LWLock", "wait_event": "LockManager", "samples": 42 }
//!   ],
//!   "routed": null,
//!   "sampler_report_path": "results/s1-direct-2026-05-21T12-00-00.sampler.txt",
//!   "started_at": "2026-05-21T12:00:00Z"
//! }
//! ```
//!
//! Path B (Phase 5) fills in the `routed` block; Path A leaves it null.

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
    pub min: u64,
    pub max: u64,
    pub mean: u64,
    pub count: u64,
}

impl LatencyPercentiles {
    /// Build microsecond-valued percentiles from a nanosecond-valued
    /// histogram. Drivers record `elapsed.as_nanos()` for sub-microsecond
    /// precision; the JSON schema reports µs to match plan §F.
    pub fn from_ns_hist(hist: &Histogram<u64>) -> Self {
        Self {
            p50: hist.value_at_quantile(0.50) / 1_000,
            p95: hist.value_at_quantile(0.95) / 1_000,
            p99: hist.value_at_quantile(0.99) / 1_000,
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

/// Path-B-specific counters; null on Path A reports.
#[derive(Debug, Serialize, Default)]
pub struct RoutedReport {
    pub eject_count_total: u64,
    pub commit_group_size_avg: f64,
    pub commit_group_size_p99: u64,
}

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub scenario: String,
    pub path: String,
    pub duration_secs: f64,
    pub callers: usize,
    pub throughput_trx_per_sec: f64,
    /// Total submission attempts across all callers (successes + errors).
    /// Disambiguates throughput=0 (all errored) from throughput=0 (no
    /// attempts) — cs5k first-pass showed s3/s4 with 0 successful trx
    /// but tens of thousands of attempts.
    pub attempts_total: u64,
    /// Submissions that returned an error from the SPI call.
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
    /// Path A constructor: ack and committed latencies are the same
    /// (synchronous tx). Path B uses `new_routed` with separate ack +
    /// committed histograms.
    pub fn new_direct(
        scenario: impl Into<String>,
        callers: usize,
        duration_secs: f64,
        ack: &Histogram<u64>,
        errors_total: u64,
        measure: &MeasureReport,
        sampler: &SamplerReport,
        sampler_report_path: Option<String>,
        started_at: DateTime<Utc>,
    ) -> Self {
        let percentiles = LatencyPercentiles::from_ns_hist(ack);
        let attempts_total = ack.len() + errors_total;
        Self {
            scenario: scenario.into(),
            path: "direct".into(),
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
            committed_latency_us: percentiles,
            commits_observed: measure.xact_commit_delta,
            rollbacks_observed: measure.xact_rollback_delta,
            wal_bytes_per_trx: measure.wal_bytes_per_commit(),
            top_wait_events: top_wait_events(sampler, 5),
            routed: None,
            sampler_report_path,
            started_at,
        }
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

/// Serialize `report` as pretty JSON to `path`. Caller is responsible
/// for choosing a stable name like `results/<scenario>-<path>-<ts>.json`.
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

/// Default output path builder: `results/<scenario>-<path>-<ts>.json`
/// where `<ts>` is `YYYY-mm-ddTHH-MM-SS` (filesystem-safe).
pub fn default_output_path(scenario: &str, path: &str, when: DateTime<Utc>) -> std::path::PathBuf {
    let ts = when.format("%Y-%m-%dT%H-%M-%S");
    std::path::PathBuf::from(format!("results/{scenario}-{path}-{ts}.json"))
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
        // Values in ns; assertions on the ÷1000 µs output.
        let h = hist_with_values(&[100_000, 200_000, 300_000, 400_000, 500_000]);
        let p = LatencyPercentiles::from_ns_hist(&h);
        assert_eq!(p.count, 5);
        assert!(p.min <= 100 && p.max >= 500);
        assert!(p.p50 >= 200 && p.p50 <= 400);
    }

    #[test]
    fn top_wait_events_sorted_desc_and_truncated() {
        let mut sr = SamplerReport::default();
        sr.wait_observations
            .insert(("LWLock".into(), "LockManager".into()), 100);
        sr.wait_observations
            .insert(("Client".into(), "ClientRead".into()), 50);
        sr.wait_observations
            .insert(("IO".into(), "WALSync".into()), 75);
        let top = top_wait_events(&sr, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].wait_event, "LockManager");
        assert_eq!(top[1].wait_event, "WALSync");
    }

    #[test]
    fn default_output_path_shape() {
        let when = DateTime::parse_from_rfc3339("2026-05-21T12:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let p = default_output_path("s1", "direct", when);
        assert_eq!(p.to_str().unwrap(), "results/s1-direct-2026-05-21T12-00-00.json");
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
            "s1",
            10,
            1.0,
            &ack,
            7,
            &measure,
            &sampler,
            None,
            when,
        );
        write_to_path(&r, &path).expect("write");
        assert!(path.exists());

        let raw = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["scenario"], "s1");
        assert_eq!(parsed["path"], "direct");
        assert_eq!(parsed["callers"], 10);
        assert_eq!(parsed["commits_observed"], 3);
        assert_eq!(parsed["attempts_total"], 10); // 3 ack + 7 errors
        assert_eq!(parsed["errors_total"], 7);
        assert!(parsed["routed"].is_null());
        assert!(parsed["ack_latency_us"]["p50"].is_number());
    }
}
