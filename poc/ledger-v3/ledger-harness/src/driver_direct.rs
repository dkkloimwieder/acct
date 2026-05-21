//! Direct-path driver for the `run` subcommand (acct-ykyl).
//!
//! Opens N concurrent sessions against poc_v3, spawns one tokio task
//! per caller, each task loops `SELECT ledger_submit_trx(...)` until
//! the deadline elapses. Per-task hdr histograms aggregate at end.
//! Spawns the pg_locks sampler (unless --no-sampler) and the
//! 1 Hz measure collector alongside.
//!
//! At end: foreground take_snapshot delta + sampler shutdown + collector
//! shutdown → assemble a RunReport → write JSON to the resolved output
//! path. Sampler's format() dump goes to the sibling `.sampler.txt`
//! path when LEDGER_V3_PRINT_SAMPLER is set.

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
use crate::report::{self, RunReport};
use crate::sampler::{print_sampler_enabled, PgLocksSampler};
use crate::scenarios;
use crate::workload::LineParam;

pub struct RunOptions {
    pub dsn: String,
    pub scenario: String,
    pub duration: Duration,
    pub output: Option<PathBuf>,
    pub no_sampler: bool,
}

pub async fn run(opts: RunOptions) -> Result<(), String> {
    let started_at = Utc::now();

    // ── Connect + load universe + resolve scenario ──
    let pool = PgPoolOptions::new()
        .max_connections(2048)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    let universe = load_universe(&pool).await?;
    let spec = scenarios::by_id(&opts.scenario, universe).ok_or_else(|| {
        format!("unknown scenario '{}' (try s1..s6)", opts.scenario)
    })?;
    eprintln!(
        "scenario {}: {} (callers={}, duration={:?})",
        spec.id, spec.description, spec.callers, opts.duration
    );

    // ── Resize pool now that we know caller count ──
    drop(pool); // close oversized pool; re-open with the tighter cap
    let pool = PgPoolOptions::new()
        .max_connections(spec.callers as u32 + 8)
        .acquire_timeout(Duration::from_secs(15))
        .connect(&opts.dsn)
        .await
        .map_err(|e| format!("reconnect: {e}"))?;

    // ── Sampler + collector (best-effort spawn; failure is fatal so
    // measurement integrity is enforced) ──
    let sampler = if opts.no_sampler {
        None
    } else {
        Some(PgLocksSampler::spawn(pool.clone(), 100).await)
    };
    let collector = MeasureCollector::spawn(pool.clone(), 1000);

    let start_snap = take_snapshot(&pool).await.map_err(|e| format!("start snapshot: {e}"))?;
    let driver_started = Instant::now();
    let deadline = driver_started + opts.duration;

    // ── Per-caller tasks ──
    let workload = Arc::new(spec.workload.clone());
    let barrier = Arc::new(Barrier::new(spec.callers));
    let mut handles = Vec::with_capacity(spec.callers);
    for caller_id in 0..spec.callers {
        let pool = pool.clone();
        let workload = workload.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            caller_loop(pool, workload, barrier, caller_id, deadline).await
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

    // PG 18 pg_stat_database flushes per-backend every ~1s — give all
    // pool connections time to drain before the end snapshot.
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

    // ── Assemble + write report ──
    let ack = LatencyHistogram::merge_all(hists);
    let output_path = opts
        .output
        .clone()
        .unwrap_or_else(|| report::default_output_path(&spec.id, "direct", started_at));

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
        spec.callers,
        elapsed.as_secs_f64(),
        &ack,
        &measure,
        &sampler_report,
        sampler_dump_path,
        started_at,
    );

    report::write_to_path(&report, &output_path).map_err(|e| format!("write report: {e}"))?;
    println!(
        "{{\"scenario\":\"{}\",\"path\":\"direct\",\"throughput_trx_per_sec\":{:.1},\
        \"p99_us\":{},\"commits\":{},\"errors\":{},\"output\":\"{}\"}}",
        spec.id,
        report.throughput_trx_per_sec,
        report.ack_latency_us.p99,
        report.commits_observed,
        errors_total,
        output_path.display()
    );
    Ok(())
}

/// One caller's tight submit loop. Records per-submit ack latency in
/// the returned LatencyHistogram. Errors are counted (and logged in
/// debug) but don't kill the loop — measurement runs tolerate
/// transient duplicates / oversold etc.
async fn caller_loop(
    pool: PgPool,
    workload: Arc<crate::workload::Workload>,
    barrier: Arc<Barrier>,
    caller_id: usize,
    deadline: Instant,
) -> (LatencyHistogram, u64) {
    let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF_u64.wrapping_add(caller_id as u64));
    let mut hist = LatencyHistogram::new();
    let mut errors: u64 = 0;
    // Per-caller source_id space: (caller_id+1) * 1e9 + tick. Keeps the
    // trx UNIQUE (trx_type, source_id) constraint from colliding across
    // callers up to 10^9 submissions per caller.
    let caller_base: i64 = (caller_id as i64 + 1) * 1_000_000_000;
    let mut tick: i64 = 0;

    barrier.wait().await;
    let posted_at = "2026-05-21T12:00:00+00:00";

    while Instant::now() < deadline {
        let lines = workload.next_lines(&mut rng, caller_id);
        let lines_json = build_lines_json(&lines);
        let source_id = caller_base + tick;
        tick += 1;

        let started = Instant::now();
        let res = sqlx::query(
            "SELECT ledger_submit_trx('po_receipt', $1, $2, $3::jsonb)",
        )
        .bind(source_id)
        .bind(posted_at)
        .bind(&lines_json)
        .execute(&pool)
        .await;
        let elapsed_ns = started.elapsed().as_nanos() as u64;

        match res {
            Ok(_) => hist.record(elapsed_ns),
            Err(_) => errors += 1,
        }
    }
    (hist, errors)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::LineParam;

    #[test]
    fn build_lines_json_shape_matches_spi_contract() {
        let lines = vec![LineParam {
            pool_id: 1,
            line_type: "po_receipt_line",
            source_id: Some(42),
            qty: 10,
            unit_cost: 50,
            debit_account: 100,
            credit_account: 200,
        }];
        let v = build_lines_json(&lines);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["pool_id"], 1);
        assert_eq!(arr[0]["line_type"], "po_receipt_line");
        assert_eq!(arr[0]["qty"], 10);
        assert_eq!(arr[0]["debit_account"], 100);
    }
}
