//! The cadence-vs-load soak: the whole surviving architecture running
//! concurrently — open-loop load on the direct + staging hot paths, the
//! logical-decoding feed consumer, N recalc workers on continuous cadence,
//! the two-gauge sampler — then quiesce, the conservation/oracle verify, and
//! the orchestrated period close.
//!
//! Knobs exercise the quiet-backlog chain (recalc-c):
//! - `--pause-feed A..B` stalls the feed consumer for a window: G1
//!   (confirmed_flush_lsn lag / retained WAL) climbs while G2 stays flat —
//!   the ingestion-health axis.
//! - `--pause-workers A..B` stalls the recalc workers: G1 stays low
//!   (advance-on-ingestion, D8) while G2a/b/c climb — the valuation-staleness
//!   axis. Resuming must converge (drain catches up).
//! - `--midrun-close-at T` runs an unforced `ledger_close_period` INSIDE A
//!   ROLLED-BACK TRANSACTION at offset T: a dry-run gate probe that records
//!   the per-pool lag report (G2a) and the G2b gross-value bound without
//!   mutating period state. Advisory locks are xact-scoped, so the rollback
//!   releases them.
//!
//! After load: quiesce (feed idle + queue empty + floors clear, bounded — a
//! livelock fails the run), verify at settled state, close the period for
//! real (unforced — the gate must pass at quiesce), verify post-sweep
//! exactness, then probe immutability (in-period submit → PeriodClosed 55000;
//! next-day submit → flows).

use serde::Serialize;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

use crate::budget::{
    spawn_footprint_watchdog, AbortSignal, ConsecutiveErrorTrip, DEFAULT_MAX_FOOTPRINT_GB,
};
use crate::driver::{self, LoadCfg, LoadOutcome, SubmitPath};
use crate::gauges::{GaugeSummary, Sampler};
use crate::universe::{self, PERIOD_ID};
use crate::verify::{run_verify, AckCounts, VerifyReport};
use crate::workload::Workload;

#[derive(Clone)]
pub struct SoakCfg {
    pub dsn: String,
    pub pools: i64,
    pub direct_callers: usize,
    pub staging_callers: usize,
    pub rate: Option<u64>,
    pub arrival: crate::pacing::ArrivalProcess,
    pub duration: Duration,
    pub workload: Workload,
    pub workers: usize,
    pub worker_poll_ms: u64,
    pub drainers: usize,
    pub drain_batch: i32,
    pub pause_workers: Option<(u64, u64)>,
    pub pause_feed: Option<(u64, u64)>,
    pub midrun_close_at: Option<u64>,
    pub gauge_interval_ms: u64,
    pub quiesce_timeout: Duration,
    pub out_dir: std::path::PathBuf,
    pub seed: u64,
    pub skip_seed: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct WorkerTally {
    pub passes: u64,
    pub settlements_written: u64,
    pub adjustments_posted: u64,
    pub errors: u64,
}

#[derive(Serialize)]
pub struct SoakReport {
    pub pools: i64,
    pub duration_secs: f64,
    pub direct: Option<LoadOutcome>,
    pub staging: Option<LoadOutcome>,
    pub workers: WorkerTally,
    pub gauges: GaugeSummary,
    pub midrun_close_probe: Option<Value>,
    pub quiesce_secs: f64,
    pub verify_settled: VerifyReport,
    pub close_report: Value,
    pub verify_post_close: VerifyReport,
    pub immutability_probe_sqlstate: String,
    pub next_period_flows: bool,
    pub reclose_already_closed: bool,
    pub passed: bool,
}

fn sqlstate(err: &sqlx::Error) -> String {
    match err {
        sqlx::Error::Database(db) => db.code().map(|c| c.to_string()).unwrap_or_default(),
        _ => String::new(),
    }
}

pub async fn run_soak(cfg: SoakCfg) -> Result<SoakReport, String> {
    std::fs::create_dir_all(&cfg.out_dir).map_err(|e| format!("out dir: {e}"))?;
    let conns = (cfg.direct_callers + cfg.staging_callers + cfg.drainers + cfg.workers + 8) as u32;
    let pool = PgPoolOptions::new()
        .max_connections(conns)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&cfg.dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    // ── setup: universe + slot BEFORE any load event commits ─────────────
    if !cfg.skip_seed {
        universe::reset(&pool).await.map_err(|e| format!("reset: {e}"))?;
        universe::seed(&pool, cfg.pools, 60).await.map_err(|e| format!("seed: {e}"))?;
    }
    let consumer = ledger_feed::FeedConsumer::new(pool.clone(), "ledger_feed", "ledger_feed");
    consumer.ensure_slot().await.map_err(|e| format!("ensure slot: {e}"))?;

    let sampler = Sampler::spawn(
        pool.clone(),
        Duration::from_millis(cfg.gauge_interval_ms),
        cfg.out_dir.join("gauges.jsonl"),
    );

    // Hard footprint cap: db+wal over the budget aborts the run loudly.
    let abort = AbortSignal::new();
    let _watchdog = spawn_footprint_watchdog(
        pool.clone(),
        (DEFAULT_MAX_FOOTPRINT_GB * 1_000_000_000) as i64,
        abort.clone(),
    );

    let (stop_tx, stop_rx) = watch::channel(false);
    let (feed_pause_tx, feed_pause_rx) = watch::channel(false);
    let (worker_pause_tx, worker_pause_rx) = watch::channel(false);
    let feed_idle = Arc::new(AtomicBool::new(false));

    // ── feed consumer task ────────────────────────────────────────────────
    let feed_task = {
        let mut stop = stop_rx.clone();
        let pause = feed_pause_rx.clone();
        let idle = feed_idle.clone();
        let mut trip = ConsecutiveErrorTrip::new("feed consumer", 10, abort.clone());
        let abort = abort.clone();
        tokio::spawn(async move {
            loop {
                if *stop.borrow() || abort.is_tripped() {
                    break;
                }
                if *pause.borrow() {
                    idle.store(false, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                match consumer.ingest_once(10_000).await {
                    Ok(report) => {
                        trip.ok();
                        idle.store(report.messages == 0, Ordering::Relaxed);
                        if report.messages == 0 {
                            tokio::select! {
                                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                                _ = stop.changed() => break,
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("feed: ingest error (retrying): {e}");
                        trip.error(e);
                        idle.store(false, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        })
    };

    // ── recalc workers ────────────────────────────────────────────────────
    let passes = Arc::new(AtomicU64::new(0));
    let settlements = Arc::new(AtomicU64::new(0));
    let adjustments = Arc::new(AtomicU64::new(0));
    let worker_errors = Arc::new(AtomicU64::new(0));
    let mut worker_tasks = Vec::new();
    for _ in 0..cfg.workers {
        let pool = pool.clone();
        let stop = stop_rx.clone();
        let pause = worker_pause_rx.clone();
        let poll = Duration::from_millis(cfg.worker_poll_ms);
        let (passes, settlements, adjustments, errors) = (
            passes.clone(),
            settlements.clone(),
            adjustments.clone(),
            worker_errors.clone(),
        );
        let mut trip = ConsecutiveErrorTrip::new("recalc worker", 20, abort.clone());
        let abort = abort.clone();
        worker_tasks.push(tokio::spawn(async move {
            loop {
                if *stop.borrow() || abort.is_tripped() {
                    break;
                }
                if *pause.borrow() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                match sqlx::query_scalar::<_, Value>("SELECT ledger_recalc_step()")
                    .fetch_one(&pool)
                    .await
                {
                    Ok(report) => {
                        trip.ok();
                        if report["claimed"] == json!(false) {
                            tokio::time::sleep(poll).await;
                        } else {
                            passes.fetch_add(1, Ordering::Relaxed);
                            settlements.fetch_add(
                                report["settlements_written"].as_u64().unwrap_or(0),
                                Ordering::Relaxed,
                            );
                            adjustments.fetch_add(
                                report["adjustments_posted"].as_u64().unwrap_or(0),
                                Ordering::Relaxed,
                            );
                        }
                    }
                    Err(e) => {
                        // Deadlock-detector aborts under the close fence are
                        // documented benign; count and continue. Persistent
                        // errors (ENOSPC, dropped slot) trip the run abort.
                        errors.fetch_add(1, Ordering::Relaxed);
                        trip.error(e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }));
    }

    // ── controller: pause windows + the dry-run close probe ──────────────
    let midrun_probe: Arc<tokio::sync::Mutex<Option<Value>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let controller = {
        let pool = pool.clone();
        let cfg2 = cfg.clone();
        let probe_slot = midrun_probe.clone();
        let feed_pause = feed_pause_tx.clone();
        let worker_pause = worker_pause_tx.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            let mut events: Vec<(u64, &str)> = Vec::new();
            if let Some((a, b)) = cfg2.pause_feed {
                events.push((a, "feed_pause"));
                events.push((b, "feed_resume"));
            }
            if let Some((a, b)) = cfg2.pause_workers {
                events.push((a, "worker_pause"));
                events.push((b, "worker_resume"));
            }
            if let Some(t) = cfg2.midrun_close_at {
                events.push((t, "close_probe"));
            }
            events.sort();
            for (at, what) in events {
                let target = Duration::from_secs(at);
                let elapsed = start.elapsed();
                if target > elapsed {
                    tokio::time::sleep(target - elapsed).await;
                }
                match what {
                    "feed_pause" => {
                        println!("soak: pausing feed @{at}s (G1 should climb)");
                        let _ = feed_pause.send(true);
                    }
                    "feed_resume" => {
                        println!("soak: resuming feed @{at}s");
                        let _ = feed_pause.send(false);
                    }
                    "worker_pause" => {
                        println!("soak: pausing recalc workers @{at}s (G2 should climb)");
                        let _ = worker_pause.send(true);
                    }
                    "worker_resume" => {
                        println!("soak: resuming recalc workers @{at}s");
                        let _ = worker_pause.send(false);
                    }
                    "close_probe" => {
                        println!("soak: dry-run close probe @{at}s (rolled back)");
                        let probe = async {
                            let mut tx = pool.begin().await.ok()?;
                            let report: Value = sqlx::query_scalar(
                                "SELECT ledger_close_period($1, 'soak-dry-run', false)",
                            )
                            .bind(PERIOD_ID)
                            .fetch_one(&mut *tx)
                            .await
                            .ok()?;
                            tx.rollback().await.ok()?;
                            Some(report)
                        }
                        .await;
                        *probe_slot.lock().await = probe;
                    }
                    _ => unreachable!(),
                }
            }
        })
    };

    // ── load: both paths share ONE source-id sequence ─────────────────────
    let source_seq = Arc::new(std::sync::atomic::AtomicI64::new(
        driver::source_id_base(&pool).await,
    ));
    let direct_cfg = (cfg.direct_callers > 0).then(|| LoadCfg {
        path: SubmitPath::Direct,
        callers: cfg.direct_callers,
        rate: cfg.rate,
        arrival: cfg.arrival,
        duration: cfg.duration,
        workload: cfg.workload.clone(),
        drainers: 0,
        drain_batch: cfg.drain_batch,
        seed: cfg.seed,
        source_seq: source_seq.clone(),
    });
    let staging_cfg = (cfg.staging_callers > 0).then(|| LoadCfg {
        path: SubmitPath::Staging,
        callers: cfg.staging_callers,
        rate: cfg.rate,
        arrival: cfg.arrival,
        duration: cfg.duration,
        workload: cfg.workload.clone(),
        drainers: cfg.drainers,
        drain_batch: cfg.drain_batch,
        seed: cfg.seed.wrapping_add(1),
        source_seq: source_seq.clone(),
    });
    let start = Instant::now();
    let (direct_out, staging_out) = tokio::join!(
        async {
            match &direct_cfg {
                Some(c) => Some(driver::run_load(&pool, c).await),
                None => None,
            }
        },
        async {
            match &staging_cfg {
                Some(c) => Some(driver::run_load(&pool, c).await),
                None => None,
            }
        },
    );
    controller.await.expect("controller task");
    // Load is done; make sure nothing is left paused for the quiesce.
    let _ = feed_pause_tx.send(false);
    let _ = worker_pause_tx.send(false);

    // ── quiesce: feed idle + queue empty + floors clear, stable across 3
    // consecutive polls; bounded (a floor/requeue livelock fails the run) ──
    let quiesce_start = Instant::now();
    let mut stable = 0;
    loop {
        if abort.is_tripped() {
            return Err(abort.reason().unwrap_or_else(|| "run aborted".into()));
        }
        if quiesce_start.elapsed() > cfg.quiesce_timeout {
            return Err(format!(
                "quiesce did not converge within {:?} — floor/requeue livelock?",
                cfg.quiesce_timeout
            ));
        }
        let queued: i64 = sqlx::query_scalar("SELECT count(*) FROM recalc_queue")
            .fetch_one(&pool)
            .await
            .unwrap_or(-1);
        let floors: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pool_settlement WHERE recost_floor_posted_at IS NOT NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap_or(-1);
        if queued == 0 && floors == 0 && feed_idle.load(Ordering::Relaxed) {
            stable += 1;
            if stable >= 3 {
                break;
            }
        } else {
            stable = 0;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let quiesce_secs = quiesce_start.elapsed().as_secs_f64();

    // ── stop the machinery, collect ───────────────────────────────────────
    let _ = stop_tx.send(true);
    feed_task.await.expect("feed task");
    for t in worker_tasks {
        t.await.expect("worker task");
    }
    let gauge_summary = sampler.shutdown().await;
    let worker_tally = WorkerTally {
        passes: passes.load(Ordering::Relaxed),
        settlements_written: settlements.load(Ordering::Relaxed),
        adjustments_posted: adjustments.load(Ordering::Relaxed),
        errors: worker_errors.load(Ordering::Relaxed),
    };

    let acked = AckCounts {
        direct_acked: direct_out.as_ref().map(|o| o.acked).unwrap_or(0),
        staging_done: staging_out.as_ref().map(|o| o.drained_done).unwrap_or(0),
    };

    // ── verify at settled state ───────────────────────────────────────────
    let verify_settled = run_verify(&pool, true, false, Some(acked))
        .await
        .map_err(|e| format!("verify: {e}"))?;

    // The close gate requires a current feed (0020) and the consumer task is
    // stopped by now, so nothing else can advance the cursor: drain the slot
    // to its anchor here, or the gate can never pass. Quiesce already proved
    // the feed idle, so this is the post-verify WAL tail plus the empty-tick
    // anchor, not real backlog.
    let closing_consumer =
        ledger_feed::FeedConsumer::new(pool.clone(), "ledger_feed", "ledger_feed");
    loop {
        let report = closing_consumer
            .ingest_once(10_000)
            .await
            .map_err(|e| format!("pre-close feed catch-up: {e}"))?;
        if report.messages == 0 {
            break;
        }
    }

    // ── the real close (unforced — the gate must pass at quiesce) ────────
    let close_report: Value =
        sqlx::query_scalar("SELECT ledger_close_period($1, 'ledger-bench', false)")
            .bind(PERIOD_ID)
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("close: {e}"))?;

    let verify_post_close = run_verify(&pool, true, true, Some(acked))
        .await
        .map_err(|e| format!("verify post-close: {e}"))?;

    // ── immutability probes ───────────────────────────────────────────────
    let probe_sid = driver::source_id_base(&pool).await;
    let in_period = sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind("po_receipt")
        .bind(probe_sid)
        .bind(chrono::Utc::now().to_rfc3339())
        .bind(json!([{"pool_id": 1, "line_type": "po_receipt_line", "source_id": probe_sid,
                      "qty": 1, "unit_cost": 100}]))
        .fetch_one(&pool)
        .await;
    let immutability_probe_sqlstate = match in_period {
        Ok(_) => "NONE".to_string(),
        Err(e) => sqlstate(&e),
    };
    let next_day = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
    let next_period_flows =
        sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
            .bind("po_receipt")
            .bind(probe_sid + 1)
            .bind(&next_day)
            .bind(json!([{"pool_id": 1, "line_type": "po_receipt_line",
                          "source_id": probe_sid + 1, "qty": 1, "unit_cost": 100}]))
            .fetch_one(&pool)
            .await
            .is_ok();

    // ── re-close is a no-op ───────────────────────────────────────────────
    let reclose: Value =
        sqlx::query_scalar("SELECT ledger_close_period($1, 'ledger-bench', false)")
            .bind(PERIOD_ID)
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("re-close: {e}"))?;

    let midrun_close_probe = midrun_probe.lock().await.take();
    let passed = verify_settled.ok()
        && verify_post_close.ok()
        && close_report["closed"] == json!(true)
        && immutability_probe_sqlstate == "55000"
        && next_period_flows
        && reclose["already_closed"] == json!(true)
        && direct_out.as_ref().map(|o| o.errors.is_empty()).unwrap_or(true)
        && staging_out.as_ref().map(|o| o.errors.is_empty()).unwrap_or(true);

    let report = SoakReport {
        pools: cfg.pools,
        duration_secs: start.elapsed().as_secs_f64(),
        direct: direct_out,
        staging: staging_out,
        workers: worker_tally,
        gauges: gauge_summary,
        midrun_close_probe,
        quiesce_secs,
        verify_settled,
        close_report,
        verify_post_close,
        immutability_probe_sqlstate,
        next_period_flows,
        reclose_already_closed: reclose["already_closed"] == json!(true),
        passed,
    };
    let path = cfg.out_dir.join("soak-report.json");
    std::fs::write(&path, serde_json::to_string_pretty(&report).expect("serialize report"))
        .map_err(|e| format!("write report: {e}"))?;
    println!("soak: report written to {}", path.display());
    Ok(report)
}
