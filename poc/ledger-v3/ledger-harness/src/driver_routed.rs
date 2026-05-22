//! Routed-path driver for the `run` subcommand (acct-qiaz).
//!
//! Mirrors `driver_direct` but targets Path B: each caller enqueues
//! via `SELECT ledger_enqueue_trx(...)` and then polls for `trx` row
//! existence at 1ms tick until `poll_deadline`. Two histograms per
//! caller — ack (enqueue→return) and committed (enqueue→trx-row
//! observed) — because Path B decouples the two.
//!
//! Routed-specific counters (eject_total_count, router_envelope_histogram,
//! router_total_submissions, router_superbatch_count) are sampled
//! foreground pre/post run; deltas land in the JSON's `routed` block.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::Barrier;

use crate::measure::{take_snapshot, LatencyHistogram, MeasureCollector};
use crate::pool_universe::PoolUniverse;
use crate::report::{self, RoutedReport, RunReport};
use crate::sampler::{print_sampler_enabled, PgLocksSampler};
use crate::scenarios;
use crate::workload::LineParam;

pub struct RunOptions {
    pub dsn: String,
    pub scenario: String,
    pub duration: Duration,
    pub output: Option<PathBuf>,
    pub no_sampler: bool,
    pub max_callers: Option<usize>,
    /// Cap for the per-submission `committed_latency` poll. A trx that
    /// hasn't materialized by this deadline is counted as an attempt but
    /// not recorded in the committed histogram (effectively lost — in
    /// production the caller would resubmit). Match `caller_tx_timeout_ms`
    /// GUC default.
    pub poll_deadline: Duration,
}

pub async fn run(opts: RunOptions) -> Result<(), String> {
    let started_at = Utc::now();

    let pool = PgPoolOptions::new()
        .max_connections(2048)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let universe = load_universe(&pool).await?;
    let mut spec = scenarios::by_id(&opts.scenario, universe).ok_or_else(|| {
        format!("unknown scenario '{}' (try s1..s6)", opts.scenario)
    })?;
    if let Some(cap) = opts.max_callers {
        let capped = spec.callers.min(cap);
        if capped != spec.callers {
            eprintln!(
                "scenario {}: capping callers {} -> {} via --max-callers",
                spec.id, spec.callers, capped
            );
            spec.callers = capped;
            spec.workload.caller_count = capped;
        }
    }
    eprintln!(
        "scenario {}: {} (callers={}, duration={:?})",
        spec.id, spec.description, spec.callers, opts.duration
    );

    drop(pool);
    let pool = PgPoolOptions::new()
        .max_connections(spec.callers as u32 + 8)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("reconnect: {e}"))?;

    let sampler = if opts.no_sampler {
        None
    } else {
        Some(PgLocksSampler::spawn(pool.clone(), 100).await)
    };
    let collector = MeasureCollector::spawn(pool.clone(), 1000);

    let start_snap = take_snapshot(&pool).await.map_err(|e| format!("start snapshot: {e}"))?;
    let pre_routed = read_routed_counters(&pool).await?;

    let driver_started = Instant::now();
    let deadline = driver_started + opts.duration;

    let run_prefix: i64 = (started_at.timestamp() as i64 % 1_000_000) * 1_000_000_000_000;

    let workload = Arc::new(spec.workload.clone());
    let barrier = Arc::new(Barrier::new(spec.callers));
    let mut handles = Vec::with_capacity(spec.callers);
    for caller_id in 0..spec.callers {
        let pool = pool.clone();
        let workload = workload.clone();
        let barrier = barrier.clone();
        let poll_deadline = opts.poll_deadline;
        handles.push(tokio::spawn(async move {
            caller_loop(
                pool, workload, barrier, caller_id, run_prefix, deadline, poll_deadline,
            )
            .await
        }));
    }

    let mut ack_hists: Vec<LatencyHistogram> = Vec::with_capacity(spec.callers);
    let mut committed_hists: Vec<LatencyHistogram> = Vec::with_capacity(spec.callers);
    let mut errors_total: u64 = 0;
    for h in handles {
        let (ack, committed, errors) = h.await.map_err(|e| format!("task join: {e}"))?;
        ack_hists.push(ack);
        committed_hists.push(committed);
        errors_total += errors;
    }
    let elapsed = driver_started.elapsed();

    wait_for_committer_quiet(&pool).await;
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let end_snap = take_snapshot(&pool).await.map_err(|e| format!("end snapshot: {e}"))?;
    let post_routed = read_routed_counters(&pool).await?;

    let mut measure = collector.shutdown().await;
    measure.xact_commit_delta = end_snap.xact_commit - start_snap.xact_commit;
    measure.xact_rollback_delta = end_snap.xact_rollback - start_snap.xact_rollback;
    measure.wal_lsn_bytes_delta = end_snap.wal_lsn_bytes - start_snap.wal_lsn_bytes;

    let sampler_report = match sampler {
        Some(s) => s.shutdown().await,
        None => Default::default(),
    };

    let ack = LatencyHistogram::merge_all(ack_hists);
    let committed = LatencyHistogram::merge_all(committed_hists);

    let output_path = opts
        .output
        .clone()
        .unwrap_or_else(|| report::default_output_path(&spec.id, "routed", started_at));

    let sampler_dump_path = if print_sampler_enabled() && !opts.no_sampler {
        let p = output_path.with_extension("sampler.txt");
        if let Err(e) = std::fs::write(&p, sampler_report.format()) {
            eprintln!("warn: failed to write sampler dump: {e}");
        }
        Some(p.to_string_lossy().into_owned())
    } else {
        None
    };

    let routed_report = derive_routed_report(&pre_routed, &post_routed);
    let report = RunReport::new_routed(
        spec.id.to_string(),
        spec.callers,
        elapsed.as_secs_f64(),
        &ack,
        &committed,
        errors_total,
        &measure,
        &sampler_report,
        sampler_dump_path,
        started_at,
        routed_report,
    );

    report::write_to_path(&report, &output_path).map_err(|e| format!("write report: {e}"))?;
    let routed = report.routed.as_ref().expect("routed block populated");
    println!(
        "{{\"scenario\":\"{}\",\"path\":\"routed\",\"throughput_trx_per_sec\":{:.1},\
        \"ack_p99_us\":{},\"committed_p99_us\":{},\"commits\":{},\"attempts\":{},\
        \"errors\":{},\"eject_total\":{},\"commit_group_avg\":{:.2},\
        \"commit_group_p99\":{},\"output\":\"{}\"}}",
        spec.id,
        report.throughput_trx_per_sec,
        report.ack_latency_us.p99,
        report.committed_latency_us.p99,
        report.commits_observed,
        report.attempts_total,
        report.errors_total,
        routed.eject_count_total,
        routed.commit_group_size_avg,
        routed.commit_group_size_p99,
        output_path.display()
    );
    Ok(())
}

async fn caller_loop(
    pool: PgPool,
    workload: Arc<crate::workload::Workload>,
    barrier: Arc<Barrier>,
    caller_id: usize,
    run_prefix: i64,
    deadline: Instant,
    poll_deadline: Duration,
) -> (LatencyHistogram, LatencyHistogram, u64) {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(caller_id as u64));
    let mut ack_hist = LatencyHistogram::new();
    let mut committed_hist = LatencyHistogram::new();
    let mut errors: u64 = 0;
    let caller_base: i64 = run_prefix + (caller_id as i64) * 1_000_000;
    let mut tick: i64 = 0;

    barrier.wait().await;
    let posted_at = "2026-05-21T12:00:00+00:00";

    while Instant::now() < deadline {
        let lines = workload.next_lines(&mut rng, caller_id);
        let lines_json = build_lines_json(&lines);
        let source_id = caller_base + tick;
        tick += 1;

        let started = Instant::now();
        let res = sqlx::query("SELECT ledger_enqueue_trx('po_receipt', $1, $2, $3::jsonb)")
            .bind(source_id)
            .bind(posted_at)
            .bind(&lines_json)
            .execute(&pool)
            .await;
        let ack_ns = started.elapsed().as_nanos() as u64;

        match res {
            Ok(_) => {
                ack_hist.record(ack_ns);
                let poll_started = Instant::now();
                let poll_until = poll_started + poll_deadline;
                loop {
                    if Instant::now() >= poll_until {
                        break;
                    }
                    let exists: Option<bool> = sqlx::query_scalar(
                        "SELECT EXISTS(SELECT 1 FROM trx \
                          WHERE trx_type = 'po_receipt'::trx_type AND source_id = $1)",
                    )
                    .bind(source_id)
                    .fetch_optional(&pool)
                    .await
                    .ok()
                    .flatten();
                    if exists == Some(true) {
                        let committed_ns = started.elapsed().as_nanos() as u64;
                        committed_hist.record(committed_ns);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            Err(_) => errors += 1,
        }
    }
    (ack_hist, committed_hist, errors)
}

fn build_lines_json(lines: &[LineParam]) -> Value {
    let arr: Vec<Value> = lines
        .iter()
        .map(|l| {
            json!({
                "pool_id": l.pool_id,
                "line_type": l.line_type,
                "source_id": l.source_id,
                "qty": l.qty,
                "unit_cost": l.unit_cost,
                "debit_account": l.debit_account,
                "credit_account": l.credit_account,
            })
        })
        .collect();
    Value::Array(arr)
}

async fn load_universe(pool: &PgPool) -> Result<PoolUniverse, String> {
    let pool_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM pool ORDER BY id")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("load pool universe: {e}"))?;
    if pool_ids.is_empty() {
        return Err("pool universe is empty — run `seed-pools` first".into());
    }
    let inv: i64 = sqlx::query_scalar("SELECT id FROM account WHERE code = '1000-inv' LIMIT 1")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("load inv account: {e}"))?;
    let ap: i64 = sqlx::query_scalar("SELECT id FROM account WHERE code = '2000-ap' LIMIT 1")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("load ap account: {e}"))?;
    Ok(PoolUniverse {
        pool_ids,
        inv_account: inv,
        ap_account: ap,
    })
}

/// Snapshot of the four routed shmem counters the Phase 5 measurement
/// contract needs. Pre/post deltas → RoutedReport.
#[derive(Debug, Default, Clone)]
struct RoutedCounterSnapshot {
    eject_total: i64,
    superbatch_count: i64,
    total_submissions: i64,
    /// (bucket, lower, upper, count) — 8 entries, log2-spaced per
    /// ledger_routed_router_envelope_histogram().
    envelope_histogram: Vec<(i32, i32, i32, i64)>,
}

async fn read_routed_counters(pool: &PgPool) -> Result<RoutedCounterSnapshot, String> {
    let eject_total: i64 = sqlx::query_scalar("SELECT ledger_routed_eject_total_count()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("read eject_total: {e}"))?;
    let superbatch_count: i64 = sqlx::query_scalar("SELECT ledger_routed_router_superbatch_count()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("read superbatch_count: {e}"))?;
    let total_submissions: i64 =
        sqlx::query_scalar("SELECT ledger_routed_router_total_submissions()")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read total_submissions: {e}"))?;
    let envelope_histogram: Vec<(i32, i32, i32, i64)> = sqlx::query_as(
        "SELECT bucket, lower, upper, count FROM ledger_routed_router_envelope_histogram() \
          ORDER BY bucket",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("read envelope_histogram: {e}"))?;
    Ok(RoutedCounterSnapshot {
        eject_total,
        superbatch_count,
        total_submissions,
        envelope_histogram,
    })
}

fn derive_routed_report(pre: &RoutedCounterSnapshot, post: &RoutedCounterSnapshot) -> RoutedReport {
    let eject_delta = (post.eject_total - pre.eject_total).max(0) as u64;
    let sb_delta = post.superbatch_count - pre.superbatch_count;
    let sub_delta = post.total_submissions - pre.total_submissions;
    let avg = if sb_delta > 0 {
        sub_delta as f64 / sb_delta as f64
    } else {
        0.0
    };

    let p99 = commit_group_p99_from_buckets(&pre.envelope_histogram, &post.envelope_histogram);

    RoutedReport {
        eject_count_total: eject_delta,
        commit_group_size_avg: avg,
        commit_group_size_p99: p99,
    }
}

/// Compute the p99 commit_group size from log2-spaced bucket deltas.
/// Picks the bucket whose cumulative count crosses 99%; returns its
/// upper bound (capped at 128 because the top bucket's upper is
/// i32::MAX). Returns 0 if no commit_groups were observed.
fn commit_group_p99_from_buckets(
    pre: &[(i32, i32, i32, i64)],
    post: &[(i32, i32, i32, i64)],
) -> u64 {
    use std::collections::HashMap;
    let pre_by_bucket: HashMap<i32, i64> = pre.iter().map(|(b, _, _, c)| (*b, *c)).collect();
    let mut deltas: Vec<(i32, i32, i64)> = post
        .iter()
        .map(|(b, _l, u, c)| (*b, *u, *c - pre_by_bucket.get(b).copied().unwrap_or(0)))
        .collect();
    deltas.sort_by_key(|d| d.0);
    let total: i64 = deltas.iter().map(|d| d.2).sum();
    if total <= 0 {
        return 0;
    }
    let threshold = ((total as f64) * 0.99).ceil() as i64;
    let mut cumulative: i64 = 0;
    for (_b, upper, count) in &deltas {
        cumulative += count;
        if cumulative >= threshold {
            if *upper == i32::MAX {
                return 128;
            }
            return *upper as u64;
        }
    }
    128
}

/// Wait for the routed committer to go quiet after callers stop
/// submitting. Polls `committer_drains_total` at 200ms; declares
/// drained after 3 consecutive equal reads (≈600ms of inactivity).
/// Hard caps at 10s to bound the end-of-run window.
async fn wait_for_committer_quiet(pool: &PgPool) {
    let poll = Duration::from_millis(200);
    let cap = Instant::now() + Duration::from_secs(10);
    let mut last: i64 = -1;
    let mut stable: u32 = 0;
    while Instant::now() < cap {
        let now: i64 = sqlx::query_scalar("SELECT ledger_routed_committer_drains_total()")
            .fetch_one(pool)
            .await
            .unwrap_or(last);
        if now == last {
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
            last = now;
        }
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::LineParam;

    fn bucket(b: i32, lower: i32, upper: i32, count: i64) -> (i32, i32, i32, i64) {
        (b, lower, upper, count)
    }

    #[test]
    fn build_lines_json_shape_matches_spi_contract() {
        let lines = vec![LineParam {
            pool_id: 7,
            line_type: "po_receipt_line",
            source_id: Some(11),
            qty: 4,
            unit_cost: 50,
            debit_account: 100,
            credit_account: 200,
        }];
        let v = build_lines_json(&lines);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["pool_id"], 7);
        assert_eq!(arr[0]["line_type"], "po_receipt_line");
        assert_eq!(arr[0]["qty"], 4);
    }

    #[test]
    fn p99_is_zero_when_no_commit_groups() {
        let buckets: Vec<_> = (0..8).map(|i| bucket(i, 1, 1, 0)).collect();
        let r = commit_group_p99_from_buckets(&buckets, &buckets);
        assert_eq!(r, 0);
    }

    #[test]
    fn p99_picks_bucket_whose_cumulative_crosses_threshold() {
        let pre: Vec<_> = (0..8).map(|i| bucket(i, 1, 1, 0)).collect();
        let mut post = pre.clone();
        // 100 commit_groups: 99 of size 1, 1 of size 32-63 → p99 = 1.
        post[0] = bucket(0, 1, 1, 99);
        post[5] = bucket(5, 32, 63, 1);
        let r = commit_group_p99_from_buckets(&pre, &post);
        assert_eq!(r, 1);

        // Shift weight: 50 in bucket 0, 50 in bucket 5 → p99 sits in
        // bucket 5 (50/100 cumulative at bucket 0; threshold ceil(99)=99
        // is crossed at bucket 5 since cumulative 100).
        post[0] = bucket(0, 1, 1, 50);
        post[5] = bucket(5, 32, 63, 50);
        let r = commit_group_p99_from_buckets(&pre, &post);
        assert_eq!(r, 63);
    }

    #[test]
    fn p99_top_bucket_clamps_to_128() {
        let pre: Vec<_> = (0..8).map(|i| bucket(i, 1, 1, 0)).collect();
        let mut post = pre.clone();
        post[7] = (7, 128, i32::MAX, 100);
        let r = commit_group_p99_from_buckets(&pre, &post);
        assert_eq!(r, 128);
    }

    #[test]
    fn derive_report_handles_zero_superbatches() {
        let s = RoutedCounterSnapshot::default();
        let r = derive_routed_report(&s, &s);
        assert_eq!(r.eject_count_total, 0);
        assert_eq!(r.commit_group_size_avg, 0.0);
        assert_eq!(r.commit_group_size_p99, 0);
    }

    #[test]
    fn derive_report_computes_avg_from_deltas() {
        let mut pre = RoutedCounterSnapshot::default();
        let mut post = RoutedCounterSnapshot::default();
        pre.envelope_histogram = (0..8).map(|i| (i, 1, 1, 0)).collect();
        post.envelope_histogram = pre.envelope_histogram.clone();
        pre.superbatch_count = 100;
        post.superbatch_count = 110;
        pre.total_submissions = 200;
        post.total_submissions = 250; // 50 envelopes / 10 sb = 5.0 avg
        pre.eject_total = 5;
        post.eject_total = 12; // delta = 7
        let r = derive_routed_report(&pre, &post);
        assert_eq!(r.eject_count_total, 7);
        assert_eq!(r.commit_group_size_avg, 5.0);
    }
}
