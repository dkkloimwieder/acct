//! The load driver: N callers submitting the generated workload through the
//! direct path (`ledger_submit_trx`) or the staging path (`ledger_inbox`
//! enqueue + in-process `ledger_staging_drain` committers), paced open-loop.
//!
//! Latency is measured from the pacer's INTENDED send-time (coordinated-
//! omission-free); for the staging path the acked instant is the enqueue
//! commit (the caller's durability point — apply is asynchronous by design).
//!
//! Error accounting:
//! - SQLSTATE 23000 (wac strict qty gate) — an expected reject; counted.
//! - SQLSTATE 40P01 (deadlock, documented benign under close-fence overlap) —
//!   retried up to 3 times; retries counted.
//! - anything else — recorded and surfaced; a run with unexplained errors
//!   fails.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Serialize;
use serde_json::json;
use sqlx::PgPool;
use tokio::time::Instant as TokioInstant;

use crate::pacing::{ArrivalProcess, Pacer};
use crate::workload::Workload;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum SubmitPath {
    #[default]
    Direct,
    Staging,
}

impl SubmitPath {
    pub fn label(self) -> &'static str {
        match self {
            SubmitPath::Direct => "direct",
            SubmitPath::Staging => "staging",
        }
    }
}

#[derive(Clone)]
pub struct LoadCfg {
    pub path: SubmitPath,
    pub callers: usize,
    /// Offered rate (trx/s) across all callers; None/0 = closed loop.
    pub rate: Option<u64>,
    pub arrival: ArrivalProcess,
    pub duration: Duration,
    pub workload: Workload,
    /// Staging committers (staging path only).
    pub drainers: usize,
    pub drain_batch: i32,
    pub seed: u64,
    /// The (trx_type, source_id)-unique id allocator. Concurrent drivers
    /// against one database MUST share one sequence.
    pub source_seq: Arc<AtomicI64>,
}

#[derive(Debug, Default, Serialize)]
pub struct LoadOutcome {
    pub path: String,
    pub offered_rate: Option<u64>,
    pub arrival_process: String,
    pub duration_secs: f64,
    pub submitted: u64,
    pub acked: u64,
    pub rejected_qty_gate: u64,
    pub deadlock_retries: u64,
    pub errors: Vec<String>,
    pub achieved_rate: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub backdated_submitted: u64,
    /// Staging path: rows the in-process committers applied / saw fail.
    pub drained_done: u64,
    pub drained_failed: u64,
    pub xact_commit_delta: i64,
    pub wal_bytes_delta: i64,
    pub wal_bytes_per_commit: f64,
}

struct CallerTally {
    hist: Histogram<u64>,
    submitted: u64,
    acked: u64,
    rejected: u64,
    retries: u64,
    backdated: u64,
    errors: Vec<String>,
}

impl CallerTally {
    fn new() -> Self {
        Self {
            hist: Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("hdr bounds"),
            submitted: 0,
            acked: 0,
            rejected: 0,
            retries: 0,
            backdated: 0,
            errors: Vec::new(),
        }
    }
}

fn sqlstate(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::Database(db) => db.code().map(|c| c.to_string()).unwrap_or_default(),
        _ => String::new(),
    }
}

/// (xact_commit, wal_insert_lsn_bytes) counters; diff start/end.
pub async fn counters(pool: &PgPool) -> (i64, i64) {
    let xc: i64 = sqlx::query_scalar(
        "SELECT COALESCE(xact_commit, 0)::bigint FROM pg_stat_database \
          WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let lsn: i64 =
        sqlx::query_scalar("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    (xc, lsn)
}

/// Next source_id base that cannot collide with anything already recorded
/// (trx and ledger_inbox share the (trx_type, source_id) uniqueness story).
pub async fn source_id_base(pool: &PgPool) -> i64 {
    let a: i64 = sqlx::query_scalar("SELECT COALESCE(max(source_id), 0) FROM trx")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let b: i64 = sqlx::query_scalar("SELECT COALESCE(max(source_id), 0) FROM ledger_inbox")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    a.max(b) + 1_000
}

pub async fn run_load(pool: &PgPool, cfg: &LoadCfg) -> LoadOutcome {
    let source_seq = cfg.source_seq.clone();
    let (xc0, wal0) = counters(pool).await;
    let start_tokio = TokioInstant::now();
    let start = Instant::now();
    let deadline = start + cfg.duration;

    let mut tasks = Vec::new();
    for caller_id in 0..cfg.callers {
        let pool = pool.clone();
        let cfg = cfg.clone();
        let source_seq = source_seq.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(cfg.seed ^ (caller_id as u64).wrapping_mul(0x9e37));
            let mut pacer =
                Pacer::new(cfg.rate, cfg.callers, caller_id, cfg.arrival, start_tokio);
            let mut tally = CallerTally::new();
            let total = cfg.duration.as_secs_f64();
            loop {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let intended = match pacer.as_mut() {
                    Some(p) => p.wait_next(&mut rng).await,
                    None => Instant::now(),
                };
                if Instant::now() >= deadline {
                    break;
                }
                let progress = (start.elapsed().as_secs_f64() / total).clamp(0.0, 1.0);
                let op = cfg.workload.next_op(&mut rng, progress);
                let source_id = source_seq.fetch_add(1, Ordering::Relaxed);
                let lines = json!([{
                    "pool_id": op.pool_id, "line_type": op.line_type,
                    "source_id": source_id, "qty": op.qty, "unit_cost": op.unit_cost,
                }]);
                let posted_at = op.posted_at.to_rfc3339();
                tally.submitted += 1;
                if op.backdated {
                    tally.backdated += 1;
                }
                let mut attempts = 0;
                loop {
                    attempts += 1;
                    let res = match cfg.path {
                        SubmitPath::Direct => sqlx::query_scalar::<_, i64>(
                            "SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)",
                        )
                        .bind(op.trx_type)
                        .bind(source_id)
                        .bind(&posted_at)
                        .bind(&lines)
                        .fetch_one(&pool)
                        .await
                        .map(|_| ()),
                        SubmitPath::Staging => sqlx::query(
                            "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
                             VALUES ($1::trx_type, $2, $3::timestamptz, $4::jsonb)",
                        )
                        .bind(op.trx_type)
                        .bind(source_id)
                        .bind(&posted_at)
                        .bind(&lines)
                        .execute(&pool)
                        .await
                        .map(|_| ()),
                    };
                    match res {
                        Ok(()) => {
                            tally.acked += 1;
                            let lat = Instant::now().duration_since(intended);
                            let _ = tally.hist.record(lat.as_nanos() as u64);
                            break;
                        }
                        Err(e) if sqlstate(&e) == "23000" => {
                            tally.rejected += 1;
                            break;
                        }
                        Err(e) if sqlstate(&e) == "40P01" && attempts <= 3 => {
                            tally.retries += 1;
                        }
                        Err(e) => {
                            if tally.errors.len() < 20 {
                                tally.errors.push(format!(
                                    "caller {caller_id} sqlstate {} : {e}",
                                    sqlstate(&e)
                                ));
                            }
                            break;
                        }
                    }
                }
            }
            tally
        }));
    }

    // Staging committers: drain until told to stop AND the backlog is empty.
    let stop_drain = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut drain_tasks = Vec::new();
    if cfg.path == SubmitPath::Staging {
        for _ in 0..cfg.drainers.max(1) {
            let pool = pool.clone();
            let stop = stop_drain.clone();
            let batch = cfg.drain_batch;
            drain_tasks.push(tokio::spawn(async move {
                loop {
                    let claimed: i64 =
                        sqlx::query_scalar("SELECT ledger_staging_drain($1)")
                            .bind(batch)
                            .fetch_one(&pool)
                            .await
                            .unwrap_or(0);
                    if claimed == 0 {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }));
        }
    }

    let mut outcome = LoadOutcome {
        path: cfg.path.label().to_string(),
        offered_rate: cfg.rate.filter(|r| *r > 0),
        arrival_process: cfg.arrival.label().to_string(),
        ..Default::default()
    };
    let mut hist: Histogram<u64> =
        Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("hdr bounds");
    for t in tasks {
        let tally = t.await.expect("caller task");
        hist.add(&tally.hist).expect("merge hist");
        outcome.submitted += tally.submitted;
        outcome.acked += tally.acked;
        outcome.rejected_qty_gate += tally.rejected;
        outcome.deadlock_retries += tally.retries;
        outcome.backdated_submitted += tally.backdated;
        outcome.errors.extend(tally.errors);
    }
    let elapsed = start.elapsed();

    stop_drain.store(true, Ordering::Relaxed);
    for t in drain_tasks {
        t.await.expect("drain task");
    }
    if cfg.path == SubmitPath::Staging {
        outcome.drained_done = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ledger_inbox WHERE status = 'done'",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(0) as u64;
        outcome.drained_failed = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ledger_inbox WHERE status = 'failed'",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(0) as u64;
    }

    let (xc1, wal1) = counters(pool).await;
    outcome.duration_secs = elapsed.as_secs_f64();
    outcome.achieved_rate = outcome.acked as f64 / elapsed.as_secs_f64();
    outcome.p50_us = hist.value_at_quantile(0.50) / 1_000;
    outcome.p99_us = hist.value_at_quantile(0.99) / 1_000;
    outcome.p999_us = hist.value_at_quantile(0.999) / 1_000;
    outcome.max_us = hist.max() / 1_000;
    outcome.xact_commit_delta = xc1 - xc0;
    outcome.wal_bytes_delta = wal1 - wal0;
    outcome.wal_bytes_per_commit = if xc1 > xc0 {
        (wal1 - wal0) as f64 / (xc1 - xc0) as f64
    } else {
        f64::NAN
    };
    outcome
}
