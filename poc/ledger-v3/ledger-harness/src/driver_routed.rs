//! Routed-path driver for the `run` subcommand (acct-qiaz + acct-tk58).
//!
//! Path B callers are fire-and-forget — each enqueues via
//! `SELECT ledger_enqueue_trx(...)` and immediately submits the next
//! one, recording `(source_id, enqueue_instant)` in memory. A single
//! dedicated observer task polls the `trx` table incrementally
//! (WHERE id > last_seen_id) at 10ms cadence and timestamps first
//! appearance of each source_id.
//!
//! Throughput = observer's seen-count / window_duration (real
//! materialization rate, not caller-polling-bound). Ack latency
//! recorded per submission at enqueue return. Committed latency
//! derived post-window by joining `submission_log[sid] = enqueue_inst`
//! with `seen[sid] = materialize_inst`.
//!
//! Routed-specific shmem counters (eject_total_count,
//! router_submission_histogram, router_total_submissions,
//! router_commit_group_count, committer_pipeline_ns_total/count,
//! committer_drains_total, router_window_defers_total) sampled
//! foreground pre/post run; deltas land in the JSON's `routed` block.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// Hard cap on how long the observer + drain wait may run after
    /// callers stop, before declaring measurement complete.
    pub drain_deadline: Duration,
}

/// One enqueued submission's bookkeeping for post-window join.
type SubmissionMark = (i64, Instant);

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
        .max_connections(spec.callers as u32 + 16)
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

    let run_prefix: i64 = (started_at.timestamp() as i64 % 1_000_000) * 1_000_000_000_000;
    let run_lo = run_prefix;
    let run_hi = run_prefix + (spec.callers as i64) * 1_000_000 + 1_000_000;

    // Observer must start BEFORE the callers — if it starts late, the
    // first wave's trx rows are timestamped at observer-start, not at
    // their true materialize_instant, inflating committed latency.
    let observer_stop = Arc::new(AtomicBool::new(false));
    let observer_pool = pool.clone();
    let observer_stop_clone = observer_stop.clone();
    let observer_handle =
        tokio::spawn(async move { observer_loop(observer_pool, run_lo, run_hi, observer_stop_clone).await });

    let driver_started = Instant::now();
    let deadline = driver_started + opts.duration;

    let workload = Arc::new(spec.workload.clone());
    let barrier = Arc::new(Barrier::new(spec.callers));
    let mut handles = Vec::with_capacity(spec.callers);
    for caller_id in 0..spec.callers {
        let pool = pool.clone();
        let workload = workload.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            caller_loop(pool, workload, barrier, caller_id, run_prefix, deadline).await
        }));
    }

    let mut ack_hists: Vec<LatencyHistogram> = Vec::with_capacity(spec.callers);
    let mut submission_log: Vec<SubmissionMark> = Vec::new();
    let mut errors_total: u64 = 0;
    for h in handles {
        let (ack, sublog, errors) = h.await.map_err(|e| format!("task join: {e}"))?;
        ack_hists.push(ack);
        submission_log.extend(sublog);
        errors_total += errors;
    }
    let elapsed = driver_started.elapsed();

    // Drain wait → observer keeps polling during this window so it
    // timestamps in-flight trx materializations after callers stop.
    wait_for_committer_quiet(&pool, opts.drain_deadline).await;
    // Tiny settle for pg_stat_database flush + any final observer tick.
    tokio::time::sleep(Duration::from_millis(500)).await;

    observer_stop.store(true, Ordering::Relaxed);
    let seen: HashMap<i64, Instant> = observer_handle
        .await
        .map_err(|e| format!("observer join: {e}"))?;

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

    // Committed latency: join submission_log with seen. Misses
    // (enqueued but never materialized within drain_deadline) are
    // tracked separately as `errors_total += ...` here is wrong —
    // they're not enqueue errors. They count as `submitted_but_unseen`
    // diagnostic but don't fold into errors_total.
    let mut committed_hist = LatencyHistogram::new();
    let mut submitted_but_unseen: u64 = 0;
    for (sid, enq) in &submission_log {
        if let Some(seen_at) = seen.get(sid) {
            let dur = seen_at.saturating_duration_since(*enq).as_nanos() as u64;
            committed_hist.record(dur);
        } else {
            submitted_but_unseen += 1;
        }
    }

    let trx_count = seen.len() as u64;
    let ack = LatencyHistogram::merge_all(ack_hists);

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
        trx_count,
        &ack,
        &committed_hist.hist,
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
        \"ack_p99_us\":{},\"committed_p99_us\":{},\"trx_materialized\":{},\
        \"attempts\":{},\"enqueue_errors\":{},\"submitted_but_unseen\":{},\
        \"eject_total\":{},\"commit_group_avg\":{:.2},\"commit_group_p99\":{},\
        \"pipeline_ns_avg\":{:.0},\"drains\":{},\"window_defers\":{},\
        \"output\":\"{}\"}}",
        spec.id,
        report.throughput_trx_per_sec,
        report.ack_latency_us.p99,
        report.committed_latency_us.p99,
        trx_count,
        report.attempts_total,
        report.errors_total,
        submitted_but_unseen,
        routed.eject_count_total,
        routed.commit_group_size_avg,
        routed.commit_group_size_p99,
        routed.committer_pipeline_ns_avg,
        routed.committer_drains_total,
        routed.router_window_defers_total,
        output_path.display()
    );
    Ok(())
}

/// One caller's fire-and-forget enqueue loop. Records ack latency in
/// `ack_hist` and `(source_id, enqueue_instant)` in `submission_log`.
/// Errors counted; do not affect submission_log.
async fn caller_loop(
    pool: PgPool,
    workload: Arc<crate::workload::Workload>,
    barrier: Arc<Barrier>,
    caller_id: usize,
    run_prefix: i64,
    deadline: Instant,
) -> (LatencyHistogram, Vec<SubmissionMark>, u64) {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(caller_id as u64));
    let mut ack_hist = LatencyHistogram::new();
    let mut submission_log: Vec<SubmissionMark> = Vec::with_capacity(4096);
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
                submission_log.push((source_id, started));
            }
            Err(_) => errors += 1,
        }
    }
    (ack_hist, submission_log, errors)
}

/// Single dedicated polling task that timestamps first appearance of
/// each source_id in the run range. Incremental scan (`WHERE id >
/// last_seen_id`) keeps per-tick cost O(new rows) regardless of
/// accumulated trx volume. 10ms cadence trades latency resolution
/// against background load.
async fn observer_loop(
    pool: PgPool,
    run_lo: i64,
    run_hi: i64,
    stop: Arc<AtomicBool>,
) -> HashMap<i64, Instant> {
    let mut seen: HashMap<i64, Instant> = HashMap::with_capacity(65_536);
    let mut last_id: i64 = 0;
    let tick = Duration::from_millis(10);

    loop {
        let rows: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT id, source_id FROM trx \
              WHERE trx_type = 'po_receipt'::trx_type \
                AND source_id BETWEEN $1 AND $2 \
                AND id > $3 \
              ORDER BY id",
        )
        .bind(run_lo)
        .bind(run_hi)
        .bind(last_id)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
        let now = Instant::now();
        for (id, sid) in &rows {
            seen.entry(*sid).or_insert(now);
            if *id > last_id {
                last_id = *id;
            }
        }
        if stop.load(Ordering::Relaxed) {
            break;
        }
        tokio::time::sleep(tick).await;
    }
    seen
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

#[derive(Debug, Default, Clone)]
struct RoutedCounterSnapshot {
    eject_total: i64,
    commit_group_count: i64,
    total_submissions: i64,
    pipeline_ns_total: i64,
    pipeline_count: i64,
    committer_drains_total: i64,
    router_window_defers_total: i64,
    submission_histogram: Vec<(i32, i32, i32, i64)>,
}

async fn read_routed_counters(pool: &PgPool) -> Result<RoutedCounterSnapshot, String> {
    let eject_total: i64 = sqlx::query_scalar("SELECT ledger_routed_eject_total_count()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("read eject_total: {e}"))?;
    let commit_group_count: i64 = sqlx::query_scalar("SELECT ledger_routed_router_commit_group_count()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("read commit_group_count: {e}"))?;
    let total_submissions: i64 =
        sqlx::query_scalar("SELECT ledger_routed_router_total_submissions()")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read total_submissions: {e}"))?;
    let pipeline_ns_total: i64 =
        sqlx::query_scalar("SELECT ledger_routed_committer_pipeline_ns_total()")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read pipeline_ns_total: {e}"))?;
    let pipeline_count: i64 = sqlx::query_scalar("SELECT ledger_routed_committer_pipeline_count()")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("read pipeline_count: {e}"))?;
    let committer_drains_total: i64 =
        sqlx::query_scalar("SELECT ledger_routed_committer_drains_total()")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read committer_drains_total: {e}"))?;
    let router_window_defers_total: i64 =
        sqlx::query_scalar("SELECT ledger_routed_router_window_defers_total()")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read router_window_defers_total: {e}"))?;
    let submission_histogram: Vec<(i32, i32, i32, i64)> = sqlx::query_as(
        "SELECT bucket, lower, upper, count FROM ledger_routed_router_submission_histogram() \
          ORDER BY bucket",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("read submission_histogram: {e}"))?;
    Ok(RoutedCounterSnapshot {
        eject_total,
        commit_group_count,
        total_submissions,
        pipeline_ns_total,
        pipeline_count,
        committer_drains_total,
        router_window_defers_total,
        submission_histogram,
    })
}

fn derive_routed_report(pre: &RoutedCounterSnapshot, post: &RoutedCounterSnapshot) -> RoutedReport {
    let eject_delta = (post.eject_total - pre.eject_total).max(0) as u64;
    let cg_delta = post.commit_group_count - pre.commit_group_count;
    let sub_delta = post.total_submissions - pre.total_submissions;
    let avg = if cg_delta > 0 {
        sub_delta as f64 / cg_delta as f64
    } else {
        0.0
    };

    let p99 = commit_group_p99_from_buckets(&pre.submission_histogram, &post.submission_histogram);

    let pipeline_ns_delta = post.pipeline_ns_total - pre.pipeline_ns_total;
    let pipeline_count_delta = post.pipeline_count - pre.pipeline_count;
    let pipeline_ns_avg = if pipeline_count_delta > 0 {
        pipeline_ns_delta as f64 / pipeline_count_delta as f64
    } else {
        0.0
    };
    let drains_delta = (post.committer_drains_total - pre.committer_drains_total).max(0) as u64;
    let defers_delta =
        (post.router_window_defers_total - pre.router_window_defers_total).max(0) as u64;

    RoutedReport {
        eject_count_total: eject_delta,
        commit_group_size_avg: avg,
        commit_group_size_p99: p99,
        committer_pipeline_ns_avg: pipeline_ns_avg,
        committer_drains_total: drains_delta,
        router_window_defers_total: defers_delta,
    }
}

fn commit_group_p99_from_buckets(
    pre: &[(i32, i32, i32, i64)],
    post: &[(i32, i32, i32, i64)],
) -> u64 {
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

/// Wait for the routed committer to go quiet after callers stop.
/// Polls `committer_drains_total` at 200ms; declares drained after 3
/// consecutive equal reads (≈600ms of inactivity). Hard caps via
/// `drain_deadline`.
async fn wait_for_committer_quiet(pool: &PgPool, deadline: Duration) {
    let poll = Duration::from_millis(200);
    let cap = Instant::now() + deadline;
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
        post[0] = bucket(0, 1, 1, 99);
        post[5] = bucket(5, 32, 63, 1);
        let r = commit_group_p99_from_buckets(&pre, &post);
        assert_eq!(r, 1);

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
    fn derive_report_handles_zero_commit_groups() {
        let s = RoutedCounterSnapshot::default();
        let r = derive_routed_report(&s, &s);
        assert_eq!(r.eject_count_total, 0);
        assert_eq!(r.commit_group_size_avg, 0.0);
        assert_eq!(r.commit_group_size_p99, 0);
        assert_eq!(r.committer_pipeline_ns_avg, 0.0);
    }

    #[test]
    fn derive_report_computes_avg_from_deltas() {
        let mut pre = RoutedCounterSnapshot::default();
        let mut post = RoutedCounterSnapshot::default();
        pre.submission_histogram = (0..8).map(|i| (i, 1, 1, 0)).collect();
        post.submission_histogram = pre.submission_histogram.clone();
        pre.commit_group_count = 100;
        post.commit_group_count = 110;
        pre.total_submissions = 200;
        post.total_submissions = 250;
        pre.eject_total = 5;
        post.eject_total = 12;
        let r = derive_routed_report(&pre, &post);
        assert_eq!(r.eject_count_total, 7);
        assert_eq!(r.commit_group_size_avg, 5.0);
    }

    #[test]
    fn derive_report_computes_pipeline_avg_and_diagnostic_deltas() {
        let mut pre = RoutedCounterSnapshot::default();
        let mut post = RoutedCounterSnapshot::default();
        pre.submission_histogram = (0..8).map(|i| (i, 1, 1, 0)).collect();
        post.submission_histogram = pre.submission_histogram.clone();
        pre.pipeline_ns_total = 1_000_000;
        post.pipeline_ns_total = 6_000_000;
        pre.pipeline_count = 100;
        post.pipeline_count = 150;
        pre.committer_drains_total = 80;
        post.committer_drains_total = 130;
        pre.router_window_defers_total = 5;
        post.router_window_defers_total = 12;
        let r = derive_routed_report(&pre, &post);
        assert_eq!(r.committer_pipeline_ns_avg, 100_000.0);
        assert_eq!(r.committer_drains_total, 50);
        assert_eq!(r.router_window_defers_total, 7);
    }
}
