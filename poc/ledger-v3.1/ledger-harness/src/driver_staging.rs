//! Staging-flavor driver for `run` (SPIKE-A / acct-0at4.11.1).
//!
//! The alternative-A transport (FEEDBACK-ARCH.md #3 / alt A): each caller INSERTs
//! one row into `ledger_inbox` inside its own tx (fire-and-forget), and K
//! committer connections drain the table via `ledger_staging_drain_c`
//! (`FOR UPDATE SKIP LOCKED`). Throughput and committed latency are measured
//! exactly as in the routed driver — a `trx`-polling observer timestamps each
//! source_id's materialization — so staging vs routed is apples-to-apples on the
//! same measurement path. The only differences from routed are the caller call
//! (INSERT vs `ledger_enqueue_trx_c`) and the committer transport (K looping
//! connections vs the shmem BGWorker pool); the ledger-core apply is identical.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::rngs::StdRng;
use rand::SeedableRng;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::Barrier;

use crate::driver_common::{
    apply_multi_touch, apply_pareto, build_lines_json, cap_callers, multi_touch_report, run_prefix,
};
use crate::measure::{take_snapshot, LatencyHistogram, MeasureCollector};
use crate::report::{self, RunReport};
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
    /// Open-loop offered rate (trx/s) across the caller pool (acct-0at4.8);
    /// None = closed-loop full-blast. Caller INSERT ack latency AND observer-
    /// timestamped committed latency are both measured from the INTENDED
    /// send-time so a drain stall shows up as a growing tail.
    pub target_rate: Option<u64>,
    /// Arrival process for `target_rate` (ignored closed-loop).
    pub arrival: crate::pacing::ArrivalProcess,
    /// Number of committer connections looping `ledger_staging_drain_c` (default
    /// 4 = routed's COMMITTER_COUNT, so the drain parallelism matches).
    pub committers: usize,
    /// Rows per drain call (the SKIP LOCKED LIMIT; default 200 = batch_size_max).
    pub drain_batch: usize,
    /// Hard cap on the post-callers inbox-drain wait.
    pub drain_deadline: Duration,
    pub multi_touch_pct: Option<u8>,
    pub touch_dist: Option<crate::workload::TouchDistribution>,
    pub pareto_hot_pool_pct: Option<u8>,
    pub pareto_hot_traffic_pct: Option<u8>,
    /// Per-caller workload RNG seed (acct-0at4.10.4): each caller seeds from
    /// `seed + caller_id`. The CLI default reproduces the historical stream.
    pub seed: u64,
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
        .ok_or_else(|| format!("unknown scenario '{}' (try s1..s21)", opts.scenario))?;
    cap_callers(&mut spec, opts.max_callers);
    apply_multi_touch(&mut spec, opts.multi_touch_pct, opts.touch_dist);
    apply_pareto(&mut spec, opts.pareto_hot_pool_pct, opts.pareto_hot_traffic_pct);
    let committers = opts.committers.max(1);
    let drain_batch = opts.drain_batch.max(1) as i32;
    eprintln!(
        "scenario {} [staging]: {} (callers={}, committers={}, drain_batch={}, duration={:?})",
        spec.id, spec.description, spec.callers, committers, drain_batch, opts.duration
    );

    drop(pool);
    let pool = PgPoolOptions::new()
        .max_connections(spec.callers as u32 + committers as u32 + 16)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("reconnect: {e}"))?;

    // Fresh queue for this run.
    sqlx::query("TRUNCATE ledger_inbox RESTART IDENTITY")
        .execute(&pool)
        .await
        .map_err(|e| {
            format!("truncate ledger_inbox (did you apply bench/spike-a-inbox.sql?): {e}")
        })?;

    let sampler = if opts.no_sampler {
        None
    } else {
        Some(PgLocksSampler::spawn(pool.clone(), 100).await)
    };
    let collector = MeasureCollector::spawn(pool.clone(), 1000);

    let start_snap = take_snapshot(&pool).await.map_err(|e| format!("start snapshot: {e}"))?;

    let prefix = run_prefix(started_at);
    let run_lo = prefix;
    let run_hi = prefix + (spec.callers as i64) * 1_000_000 + 1_000_000;

    // Observer starts before callers so first-wave trx get their true materialize
    // instant.
    let observer_stop = Arc::new(AtomicBool::new(false));
    let observer_handle = {
        let p = pool.clone();
        let stop = observer_stop.clone();
        tokio::spawn(async move { observer_loop(p, run_lo, run_hi, stop).await })
    };

    // Committer pool: K connections looping the drain. They run until callers stop
    // AND the inbox drains (signalled via committer_stop, set only after the
    // inbox is confirmed empty).
    let committer_stop = Arc::new(AtomicBool::new(false));
    let drains_total = Arc::new(AtomicU64::new(0));
    let claimed_total = Arc::new(AtomicU64::new(0));
    let mut committer_handles = Vec::with_capacity(committers);
    for _ in 0..committers {
        let p = pool.clone();
        let stop = committer_stop.clone();
        let drains = drains_total.clone();
        let claimed = claimed_total.clone();
        committer_handles
            .push(tokio::spawn(
                async move { committer_loop(p, drain_batch, stop, drains, claimed).await },
            ));
    }

    let driver_started = Instant::now();
    let deadline = driver_started + opts.duration;
    let workload = Arc::new(spec.workload.clone());
    let barrier = Arc::new(Barrier::new(spec.callers));
    let callers = spec.callers;
    let target_rate = opts.target_rate;
    let arrival = opts.arrival;
    let seed = opts.seed;
    if let Some(r) = target_rate.filter(|r| *r > 0) {
        eprintln!(
            "[run] open-loop: target_rate={r} trx/s, arrival={}, {callers} callers",
            arrival.label()
        );
    }
    let mut handles = Vec::with_capacity(spec.callers);
    for caller_id in 0..spec.callers {
        let pool = pool.clone();
        let workload = workload.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            caller_loop(pool, workload, barrier, caller_id, callers, prefix, deadline, target_rate, arrival, seed)
                .await
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

    // Wait for the inbox to drain (all rows done), then stop the committers.
    wait_for_inbox_drain(&pool, opts.drain_deadline).await;
    committer_stop.store(true, Ordering::Relaxed);
    for h in committer_handles {
        let _ = h.await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    observer_stop.store(true, Ordering::Relaxed);
    let (mut seen, observer_poll_errors): (HashMap<i64, Instant>, u64) =
        observer_handle.await.map_err(|e| format!("observer join: {e}"))?;
    if observer_poll_errors > 0 {
        eprintln!(
            "warn: observer had {observer_poll_errors} poll error(s); submitted_but_unseen \
             may be inflated by missed reads rather than genuinely uncommitted trx"
        );
    }

    // Final reconciling sweep — committers commit trx.id out of order, so the
    // incremental observer can skip a row whose id sits below an already-seen id.
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
        .unwrap_or_else(|| report::default_output_path(spec.id, "staging", started_at));

    let sampler_dump_path = if print_sampler_enabled() && !opts.no_sampler {
        let p = output_path.with_extension("sampler.txt");
        if let Err(e) = std::fs::write(&p, sampler_report.format()) {
            eprintln!("warn: failed to write sampler dump: {e}");
        }
        Some(p.to_string_lossy().into_owned())
    } else {
        None
    };

    let report = RunReport::new_staging(
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
    )
    .with_multi_touch(multi_touch_report(&spec.workload))
    .with_open_loop(opts.target_rate, opts.arrival)
    .with_seed(opts.seed);

    report::write_to_path(&report, &output_path).map_err(|e| format!("write report: {e}"))?;

    let drains = drains_total.load(Ordering::Relaxed);
    let claimed = claimed_total.load(Ordering::Relaxed);
    let claim_group_avg = if drains > 0 { claimed as f64 / drains as f64 } else { 0.0 };
    println!(
        "{{\"scenario\":\"{}\",\"mode\":\"staging\",\"depth\":{},\"offered_rate\":{},\
        \"throughput_trx_per_sec\":{:.1},\
        \"ack_p99_us\":{},\"committed_p99_us\":{},\"committed_p999_us\":{},\
        \"slo_p99_us\":{},\"slo_p999_us\":{},\"trx_materialized\":{},\"attempts\":{},\
        \"insert_errors\":{},\"submitted_but_unseen\":{},\"observer_poll_errors\":{},\
        \"committers\":{},\"drain_batch\":{},\"drains\":{},\"claimed_total\":{},\
        \"claim_group_avg\":{:.2},\"output\":\"{}\"}}",
        spec.id,
        report.pool_depth.map(|d| d as i64).unwrap_or(-1),
        opts.target_rate.map(|r| r as i64).unwrap_or(-1),
        report.throughput_trx_per_sec,
        report.ack_latency_us.p99,
        report.committed_latency_us.p99,
        report.committed_latency_us.p999,
        // Staging materialization latency IS the end-to-end SLO headline.
        report.committed_latency_us.p99,
        report.committed_latency_us.p999,
        trx_count,
        report.attempts_total,
        report.errors_total,
        submitted_but_unseen,
        observer_poll_errors,
        committers,
        drain_batch,
        drains,
        claimed,
        claim_group_avg,
        output_path.display()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn caller_loop(
    pool: PgPool,
    workload: Arc<crate::workload::Workload>,
    barrier: Arc<Barrier>,
    caller_id: usize,
    callers: usize,
    run_prefix: i64,
    deadline: Instant,
    target_rate: Option<u64>,
    arrival: crate::pacing::ArrivalProcess,
    seed: u64,
) -> (LatencyHistogram, Vec<SubmissionMark>, u64) {
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(caller_id as u64));
    let mut ack_hist = LatencyHistogram::new();
    let mut submission_log: Vec<SubmissionMark> = Vec::with_capacity(4096);
    let mut errors: u64 = 0;
    let caller_base: i64 = run_prefix + (caller_id as i64) * 1_000_000;
    let mut tick: i64 = 0;

    barrier.wait().await;

    // Coordinated-omission-free open-loop schedule (acct-0at4.8); epoch = barrier
    // release. The intended send-time is stamped into submission_log so the
    // observer-timestamped committed latency is CO-free, not just the ack.
    let mut pacer = crate::pacing::Pacer::new(
        target_rate, callers, caller_id, 1, arrival, tokio::time::Instant::now(),
    );

    while Instant::now() < deadline {
        let intended = match pacer.as_mut() {
            Some(p) => Some(p.wait_next(&mut rng).await),
            None => None,
        };
        let lines = workload.next_lines(&mut rng, caller_id);
        let lines_json = build_lines_json(&lines);
        let source_id = caller_base + tick;
        tick += 1;

        let started = Instant::now();
        let res = sqlx::query(
            "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
             VALUES ($1, $2, $3, $4::jsonb)",
        )
        .bind(TRX_TYPE)
        .bind(source_id)
        .bind(POSTED_AT)
        .bind(&lines_json)
        .execute(&pool)
        .await;
        let origin = intended.unwrap_or(started);
        let ack_ns = Instant::now().saturating_duration_since(origin).as_nanos() as u64;

        match res {
            Ok(_) => {
                ack_hist.record(ack_ns);
                submission_log.push((source_id, origin));
            }
            Err(_) => errors += 1,
        }
    }
    (ack_hist, submission_log, errors)
}

/// One committer connection: loop `ledger_staging_drain_c` until the queue is
/// empty and `stop` is set. `n > 0` (rows claimed) = made progress; `n == 0` =
/// queue empty (idle backoff, or exit when stopping).
async fn committer_loop(
    pool: PgPool,
    drain_batch: i32,
    stop: Arc<AtomicBool>,
    drains: Arc<AtomicU64>,
    claimed: Arc<AtomicU64>,
) {
    loop {
        let n: i64 = match sqlx::query_scalar("SELECT ledger_staging_drain_c($1)")
            .bind(drain_batch)
            .fetch_one(&pool)
            .await
        {
            Ok(n) => n,
            Err(_) => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
        };
        if n > 0 {
            drains.fetch_add(1, Ordering::Relaxed);
            claimed.fetch_add(n as u64, Ordering::Relaxed);
        } else {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

async fn observer_loop(
    pool: PgPool,
    run_lo: i64,
    run_hi: i64,
    stop: Arc<AtomicBool>,
) -> (HashMap<i64, Instant>, u64) {
    let mut seen: HashMap<i64, Instant> = HashMap::with_capacity(65_536);
    let mut poll_errors: u64 = 0;
    let mut last_id: i64 = 0;
    let tick = Duration::from_millis(10);

    loop {
        let rows: Vec<(i64, i64)> = match sqlx::query_as(
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
        {
            Ok(rows) => rows,
            Err(_) => {
                poll_errors += 1;
                Vec::new()
            }
        };
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
    (seen, poll_errors)
}

/// Poll `count(*) WHERE NOT done` at 50ms until the inbox is empty or `deadline`.
async fn wait_for_inbox_drain(pool: &PgPool, deadline: Duration) {
    let poll = Duration::from_millis(50);
    let cap = Instant::now() + deadline;
    while Instant::now() < cap {
        let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM ledger_inbox WHERE NOT done")
            .fetch_one(pool)
            .await
            .unwrap_or(1);
        if pending == 0 {
            return;
        }
        tokio::time::sleep(poll).await;
    }
}
