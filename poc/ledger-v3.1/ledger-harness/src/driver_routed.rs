//! Routed-flavor driver for `run` (§6, §10.0).
//!
//! Callers are fire-and-forget: each enqueues via `ledger_enqueue_trx_c` and
//! immediately submits the next, recording `(source_id, enqueue_instant)`. A
//! single observer task polls `trx` incrementally (WHERE id > last_seen) at
//! 10ms and timestamps first appearance of each source_id (§6.1 harness
//! polling). Throughput = observer's seen-count / window. Committed latency =
//! materialize_instant − enqueue_instant.
//!
//! The `routed` JSON block carries committer-counter deltas
//! (`ledger_routed_c_committer_*`): drains, trx committed, pool_lock
//! acquisitions, aggregate upserts — these quantify the §6.7 batching win
//! (1000 concurrent submissions → ~one acquisition + one upsert per pool per
//! commit_group) — plus the P3.4 poison / deadlock-retry / takeover counts.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::SeedableRng;
use rand::rngs::StdRng;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::Barrier;

use crate::driver_common::{
    apply_multi_touch, build_lines_json, cap_callers, multi_touch_report, run_prefix,
};
use crate::measure::{take_snapshot, LatencyHistogram, MeasureCollector};
use crate::report::{self, RoutedReport, RunReport};
use crate::sampler::{print_sampler_enabled, PgLocksSampler};
use crate::scenarios;

pub struct RunOptions {
    pub dsn: String,
    pub scenario: String,
    pub pool_depth: Option<usize>,
    pub duration: Duration,
    pub output: Option<PathBuf>,
    pub no_sampler: bool,
    pub max_callers: Option<usize>,
    /// Hard cap on the post-callers observer + drain wait.
    pub drain_deadline: Duration,
    /// Multi-touch overlay (acct-34ce); None leaves the scenario's own setting.
    pub multi_touch_pct: Option<u8>,
    pub touch_dist: Option<crate::workload::TouchDistribution>,
}

type SubmissionMark = (i64, Instant);

const POSTED_AT: &str = "2026-05-25T12:00:00+00:00";
const TRX_TYPE: &str = "po_receipt";

pub async fn run(opts: RunOptions) -> Result<(), String> {
    let started_at = Utc::now();

    let pool = PgPoolOptions::new()
        .max_connections(64)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let universe = crate::pool_universe::load(&pool).await?;
    let mut spec = scenarios::by_id(&opts.scenario, universe)
        .ok_or_else(|| format!("unknown scenario '{}' (try s1..s8)", opts.scenario))?;
    cap_callers(&mut spec, opts.max_callers);
    apply_multi_touch(&mut spec, opts.multi_touch_pct, opts.touch_dist);
    eprintln!(
        "scenario {} [routed]: {} (callers={}, duration={:?})",
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

    let prefix = run_prefix(started_at);
    let run_lo = prefix;
    let run_hi = prefix + (spec.callers as i64) * 1_000_000 + 1_000_000;

    // Observer must start BEFORE callers so first-wave trx are timestamped at
    // their true materialize instant.
    let observer_stop = Arc::new(AtomicBool::new(false));
    let observer_pool = pool.clone();
    let observer_stop_clone = observer_stop.clone();
    let observer_handle = tokio::spawn(async move {
        observer_loop(observer_pool, run_lo, run_hi, observer_stop_clone).await
    });

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
            caller_loop(pool, workload, barrier, caller_id, prefix, deadline).await
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

    wait_for_committer_quiet(&pool, opts.drain_deadline).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    observer_stop.store(true, Ordering::Relaxed);
    let mut seen: HashMap<i64, Instant> =
        observer_handle.await.map_err(|e| format!("observer join: {e}"))?;

    // Final reconciling sweep. The N concurrent committers allocate trx.id then
    // commit out of order, so the incremental (id > last_id) observer can skip a
    // row whose id sits below an already-seen id. One full-range scan after
    // drain captures every committed source_id for an accurate throughput count;
    // stragglers it adds get a best-effort end-of-run timestamp (their committed
    // latency is the tail anyway).
    {
        let now = Instant::now();
        let final_rows: Vec<i64> = sqlx::query_scalar(
            "SELECT source_id FROM trx \
              WHERE trx_type = 'po_receipt'::trx_type AND source_id BETWEEN $1 AND $2",
        )
        .bind(run_lo)
        .bind(run_hi)
        .fetch_all(&pool)
        .await
        .map_err(|e| format!("final reconciling sweep: {e}"))?;
        for sid in final_rows {
            seen.entry(sid).or_insert(now);
        }
    }

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

    let mut committed_hist = LatencyHistogram::new();
    let mut submitted_but_unseen: u64 = 0;
    for (sid, enq) in &submission_log {
        if let Some(seen_at) = seen.get(sid) {
            committed_hist.record(seen_at.saturating_duration_since(*enq).as_nanos() as u64);
        } else {
            submitted_but_unseen += 1;
        }
    }

    let trx_count = seen.len() as u64;
    let ack = LatencyHistogram::merge_all(ack_hists);
    let output_path = opts
        .output
        .clone()
        .unwrap_or_else(|| report::default_output_path(spec.id, "routed", started_at));

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
        opts.pool_depth.or(Some(spec.depth_hint)),
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
    )
    .with_multi_touch(multi_touch_report(&spec.workload));

    report::write_to_path(&report, &output_path).map_err(|e| format!("write report: {e}"))?;
    let routed = report.routed.as_ref().expect("routed block populated");
    println!(
        "{{\"scenario\":\"{}\",\"mode\":\"routed\",\"depth\":{},\"throughput_trx_per_sec\":{:.1},\
        \"ack_p99_us\":{},\"committed_p99_us\":{},\"trx_materialized\":{},\"attempts\":{},\
        \"enqueue_errors\":{},\"submitted_but_unseen\":{},\"drains\":{},\"commit_group_avg\":{:.2},\
        \"pool_lock_acq\":{},\"agg_upserts\":{},\"dropped\":{},\"poisoned\":{},\
        \"deadlock_retries\":{},\"takeovers\":{},\"output\":\"{}\"}}",
        spec.id,
        report.pool_depth.map(|d| d as i64).unwrap_or(-1),
        report.throughput_trx_per_sec,
        report.ack_latency_us.p99,
        report.committed_latency_us.p99,
        trx_count,
        report.attempts_total,
        report.errors_total,
        submitted_but_unseen,
        routed.drains_total,
        routed.commit_group_size_avg,
        routed.pool_lock_acquisitions_total,
        routed.aggregate_upserts_total,
        routed.dropped_submissions_total,
        routed.poisoned_total,
        routed.deadlock_retries_total,
        routed.takeover_count,
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
) -> (LatencyHistogram, Vec<SubmissionMark>, u64) {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(caller_id as u64));
    let mut ack_hist = LatencyHistogram::new();
    let mut submission_log: Vec<SubmissionMark> = Vec::with_capacity(4096);
    let mut errors: u64 = 0;
    let caller_base: i64 = run_prefix + (caller_id as i64) * 1_000_000;
    let mut tick: i64 = 0;

    barrier.wait().await;

    while Instant::now() < deadline {
        let lines = workload.next_lines(&mut rng, caller_id);
        let lines_json = build_lines_json(&lines);
        let source_id = caller_base + tick;
        tick += 1;

        let started = Instant::now();
        let res = sqlx::query("SELECT ledger_enqueue_trx_c($1, $2, $3, $4::jsonb)")
            .bind(TRX_TYPE)
            .bind(source_id)
            .bind(POSTED_AT)
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

#[derive(Debug, Default, Clone)]
struct RoutedCounterSnapshot {
    drains: i64,
    trx_committed: i64,
    pool_lock_acquisitions: i64,
    aggregate_upserts: i64,
    dedup_skips: i64,
    dropped_submissions: i64,
    tx_failures: i64,
    poisoned: i64,
    deadlock_retries: i64,
    takeover_count: i64,
}

async fn read_routed_counters(pool: &PgPool) -> Result<RoutedCounterSnapshot, String> {
    async fn scalar(pool: &PgPool, sql: &str) -> Result<i64, String> {
        sqlx::query_scalar(sql)
            .fetch_one(pool)
            .await
            .map_err(|e| format!("read {sql}: {e}"))
    }
    Ok(RoutedCounterSnapshot {
        drains: scalar(pool, "SELECT ledger_routed_c_committer_drains_total()").await?,
        trx_committed: scalar(pool, "SELECT ledger_routed_c_committer_trx_committed_total()").await?,
        pool_lock_acquisitions: scalar(
            pool,
            "SELECT ledger_routed_c_committer_pool_lock_acquisitions_total()",
        )
        .await?,
        aggregate_upserts: scalar(pool, "SELECT ledger_routed_c_committer_aggregate_upserts_total()")
            .await?,
        dedup_skips: scalar(pool, "SELECT ledger_routed_c_committer_dedup_skips_total()").await?,
        dropped_submissions: scalar(
            pool,
            "SELECT ledger_routed_c_committer_dropped_submissions_total()",
        )
        .await?,
        tx_failures: scalar(pool, "SELECT ledger_routed_c_committer_tx_failures_total()").await?,
        poisoned: scalar(pool, "SELECT ledger_routed_c_committer_poisoned_total()").await?,
        deadlock_retries: scalar(pool, "SELECT ledger_routed_c_committer_deadlock_retries_total()")
            .await?,
        takeover_count: scalar(pool, "SELECT ledger_routed_c_committer_takeover_count()").await?,
    })
}

fn derive_routed_report(pre: &RoutedCounterSnapshot, post: &RoutedCounterSnapshot) -> RoutedReport {
    let d = |a: i64, b: i64| (b - a).max(0) as u64;
    let drains = d(pre.drains, post.drains);
    let trx_committed = d(pre.trx_committed, post.trx_committed);
    let commit_group_size_avg = if drains > 0 {
        trx_committed as f64 / drains as f64
    } else {
        0.0
    };
    RoutedReport {
        drains_total: drains,
        trx_committed_total: trx_committed,
        commit_group_size_avg,
        pool_lock_acquisitions_total: d(pre.pool_lock_acquisitions, post.pool_lock_acquisitions),
        aggregate_upserts_total: d(pre.aggregate_upserts, post.aggregate_upserts),
        dedup_skips_total: d(pre.dedup_skips, post.dedup_skips),
        dropped_submissions_total: d(pre.dropped_submissions, post.dropped_submissions),
        tx_failures_total: d(pre.tx_failures, post.tx_failures),
        poisoned_total: d(pre.poisoned, post.poisoned),
        deadlock_retries_total: d(pre.deadlock_retries, post.deadlock_retries),
        takeover_count: d(pre.takeover_count, post.takeover_count),
    }
}

/// Wait for the committer to go quiet after callers stop: poll
/// `committer_drains_total` at 200ms; declare drained after 3 consecutive equal
/// reads (~600ms idle). Hard-capped by `deadline`.
async fn wait_for_committer_quiet(pool: &PgPool, deadline: Duration) {
    let poll = Duration::from_millis(200);
    let cap = Instant::now() + deadline;
    let mut last: i64 = -1;
    let mut stable: u32 = 0;
    while Instant::now() < cap {
        let now: i64 = sqlx::query_scalar("SELECT ledger_routed_c_committer_drains_total()")
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

    #[test]
    fn derive_report_handles_zero_drains() {
        let s = RoutedCounterSnapshot::default();
        let r = derive_routed_report(&s, &s);
        assert_eq!(r.drains_total, 0);
        assert_eq!(r.commit_group_size_avg, 0.0);
    }

    #[test]
    fn derive_report_computes_commit_group_avg_and_deltas() {
        let pre = RoutedCounterSnapshot {
            drains: 100,
            trx_committed: 500,
            pool_lock_acquisitions: 120,
            aggregate_upserts: 130,
            poisoned: 1,
            deadlock_retries: 3,
            takeover_count: 0,
            ..Default::default()
        };
        let post = RoutedCounterSnapshot {
            drains: 110,
            trx_committed: 1500,
            pool_lock_acquisitions: 140,
            aggregate_upserts: 160,
            poisoned: 2,
            deadlock_retries: 8,
            takeover_count: 1,
            ..Default::default()
        };
        let r = derive_routed_report(&pre, &post);
        assert_eq!(r.drains_total, 10);
        assert_eq!(r.trx_committed_total, 1000);
        assert_eq!(r.commit_group_size_avg, 100.0); // 1000 trx / 10 drains
        assert_eq!(r.pool_lock_acquisitions_total, 20);
        assert_eq!(r.aggregate_upserts_total, 30);
        assert_eq!(r.poisoned_total, 1);
        assert_eq!(r.deadlock_retries_total, 5);
        assert_eq!(r.takeover_count, 1);
    }
}
