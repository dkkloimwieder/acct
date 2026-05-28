//! Direct-flavor driver for `run` — handles both §10.0 direct modes:
//!   direct-per-call : each `ledger_submit_trx_c` in its own user-tx (autocommit)
//!   direct-batched  : N calls wrapped in one BEGIN..COMMIT user-tx (§5.5)
//!
//! Opens one PG session per caller, spawns one tokio task per caller, each
//! looping until the deadline. Per-call ack latency feeds a per-task
//! LatencyHistogram (the §11.2 lock-hold-vs-depth signal in per-call mode).
//! For batched mode commit cost is amortized across the batch and surfaces as
//! lower commit count / WAL-per-commit, not in per-call latency.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use rand::SeedableRng;
use rand::rngs::StdRng;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::Barrier;

use crate::cli::Mode;
use crate::driver_common::{
    apply_multi_touch, apply_pareto, build_lines_json, cap_callers, multi_touch_report, run_prefix,
};
use crate::measure::{take_snapshot, LatencyHistogram, MeasureCollector};
use crate::pool_universe;
use crate::report::{self, RunReport};
use crate::sampler::{print_sampler_enabled, PgLocksSampler};
use crate::scenarios;

pub struct RunOptions {
    pub dsn: String,
    pub scenario: String,
    pub mode: Mode,
    pub batch_size: usize,
    pub pool_depth: Option<usize>,
    pub duration: Duration,
    pub output: Option<PathBuf>,
    pub no_sampler: bool,
    pub max_callers: Option<usize>,
    /// Multi-touch overlay (acct-34ce); None leaves the scenario's own setting.
    pub multi_touch_pct: Option<u8>,
    pub touch_dist: Option<crate::workload::TouchDistribution>,
    /// Pareto overlap overlay (acct-s90k); either knob being Some switches the
    /// scenario's overlap to Pareto. Both unset leaves the native overlap.
    pub pareto_hot_pool_pct: Option<u8>,
    pub pareto_hot_traffic_pct: Option<u8>,
}

const POSTED_AT: &str = "2026-05-25T12:00:00+00:00";
const TRX_TYPE: &str = "po_receipt";

pub async fn run(opts: RunOptions) -> Result<(), String> {
    let started_at = Utc::now();
    let mode_label = opts.mode.label();

    let pool = PgPoolOptions::new()
        .max_connections(64)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let universe = pool_universe::load(&pool).await?;
    let mut spec = scenarios::by_id(&opts.scenario, universe)
        .ok_or_else(|| format!("unknown scenario '{}' (try s1..s21)", opts.scenario))?;
    cap_callers(&mut spec, opts.max_callers);
    apply_multi_touch(&mut spec, opts.multi_touch_pct, opts.touch_dist);
    apply_pareto(&mut spec, opts.pareto_hot_pool_pct, opts.pareto_hot_traffic_pct);
    eprintln!(
        "scenario {} [{}]: {} (callers={}, batch_size={}, duration={:?})",
        spec.id, mode_label, spec.description, spec.callers,
        if opts.mode == Mode::DirectBatched { opts.batch_size } else { 1 },
        opts.duration
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
    let driver_started = Instant::now();
    let deadline = driver_started + opts.duration;
    let prefix = run_prefix(started_at);

    let workload = Arc::new(spec.workload.clone());
    let barrier = Arc::new(Barrier::new(spec.callers));
    let batched = opts.mode == Mode::DirectBatched;
    let batch_size = if batched { opts.batch_size.max(1) } else { 1 };

    let mut handles = Vec::with_capacity(spec.callers);
    for caller_id in 0..spec.callers {
        let pool = pool.clone();
        let workload = workload.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            caller_loop(pool, workload, barrier, caller_id, prefix, deadline, batched, batch_size)
                .await
        }));
    }

    let mut hists: Vec<LatencyHistogram> = Vec::with_capacity(spec.callers);
    let mut errors_total: u64 = 0;
    for h in handles {
        let (hist, errors) = h.await.map_err(|e| format!("task join: {e}"))?;
        hists.push(hist);
        errors_total += errors;
    }
    let elapsed = driver_started.elapsed();

    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let end_snap = take_snapshot(&pool).await.map_err(|e| format!("end snapshot: {e}"))?;

    let mut measure = collector.shutdown().await;
    measure.xact_commit_delta = end_snap.xact_commit - start_snap.xact_commit;
    measure.xact_rollback_delta = end_snap.xact_rollback - start_snap.xact_rollback;
    measure.wal_lsn_bytes_delta = end_snap.wal_lsn_bytes - start_snap.wal_lsn_bytes;

    let sampler_report = match sampler {
        Some(s) => s.shutdown().await,
        None => Default::default(),
    };

    let ack = LatencyHistogram::merge_all(hists);
    let output_path = opts
        .output
        .clone()
        .unwrap_or_else(|| report::default_output_path(spec.id, mode_label, started_at));

    let sampler_dump_path = if print_sampler_enabled() && !opts.no_sampler {
        let p = output_path.with_extension("sampler.txt");
        if let Err(e) = std::fs::write(&p, sampler_report.format()) {
            eprintln!("warn: failed to write sampler dump: {e}");
        }
        Some(p.to_string_lossy().into_owned())
    } else {
        None
    };

    let report = RunReport::new_direct(
        spec.id.to_string(),
        mode_label,
        opts.pool_depth.or(Some(spec.depth_hint)),
        spec.callers,
        elapsed.as_secs_f64(),
        &ack,
        errors_total,
        &measure,
        &sampler_report,
        sampler_dump_path,
        started_at,
    )
    .with_multi_touch(multi_touch_report(&spec.workload));

    report::write_to_path(&report, &output_path).map_err(|e| format!("write report: {e}"))?;
    println!(
        "{{\"scenario\":\"{}\",\"mode\":\"{}\",\"depth\":{},\"throughput_trx_per_sec\":{:.1},\
        \"p50_us\":{},\"p99_us\":{},\"commits\":{},\"attempts\":{},\"errors\":{},\
        \"wal_bytes_per_commit\":{:.0},\"output\":\"{}\"}}",
        spec.id,
        mode_label,
        report.pool_depth.map(|d| d as i64).unwrap_or(-1),
        report.throughput_trx_per_sec,
        report.ack_latency_us.p50,
        report.ack_latency_us.p99,
        report.commits_observed,
        report.attempts_total,
        report.errors_total,
        report.wal_bytes_per_trx,
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
    run_prefix: i64,
    deadline: Instant,
    batched: bool,
    batch_size: usize,
) -> (LatencyHistogram, u64) {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(caller_id as u64));
    let mut hist = LatencyHistogram::new();
    let mut errors: u64 = 0;
    let caller_base: i64 = run_prefix + (caller_id as i64) * 1_000_000;
    let mut tick: i64 = 0;

    barrier.wait().await;

    while Instant::now() < deadline {
        if batched {
            // One user-tx wrapping `batch_size` submissions (§5.5). A failed
            // submission aborts the tx; we count it and roll the batch back.
            let mut tx = match pool.begin().await {
                Ok(t) => t,
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let mut aborted = false;
            for _ in 0..batch_size {
                let lines = workload.next_lines(&mut rng, caller_id);
                let lines_json = build_lines_json(&lines);
                let source_id = caller_base + tick;
                tick += 1;
                let started = Instant::now();
                let res = sqlx::query("SELECT ledger_submit_trx_c($1, $2, $3, $4::jsonb)")
                    .bind(TRX_TYPE)
                    .bind(source_id)
                    .bind(POSTED_AT)
                    .bind(&lines_json)
                    .execute(&mut *tx)
                    .await;
                match res {
                    Ok(_) => hist.record(started.elapsed().as_nanos() as u64),
                    Err(_) => {
                        errors += 1;
                        aborted = true;
                        break;
                    }
                }
            }
            if aborted {
                let _ = tx.rollback().await;
            } else if tx.commit().await.is_err() {
                errors += 1;
            }
        } else {
            let lines = workload.next_lines(&mut rng, caller_id);
            let lines_json = build_lines_json(&lines);
            let source_id = caller_base + tick;
            tick += 1;
            let started = Instant::now();
            let res = sqlx::query("SELECT ledger_submit_trx_c($1, $2, $3, $4::jsonb)")
                .bind(TRX_TYPE)
                .bind(source_id)
                .bind(POSTED_AT)
                .bind(&lines_json)
                .execute(&pool)
                .await;
            match res {
                Ok(_) => hist.record(started.elapsed().as_nanos() as u64),
                Err(_) => errors += 1,
            }
        }
    }
    (hist, errors)
}
