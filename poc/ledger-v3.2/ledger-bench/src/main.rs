//! ledger-bench — the v3.2 measurement + soak harness (acct-qm7o.6).
//!
//! Reuses the architecture-agnostic survivors of the v3.1 test program:
//! open-loop CO-free pacing + throughput-at-SLO methodology (acct-0at4.8),
//! the conservation-invariant sweep (acct-0at4.5) and the strict-replay
//! oracle equivalence (acct-0at4.7) ported v3.2-native, plus the two knobs
//! the replay oracle flagged missing: receipt-cost volatility and backdated-
//! event injection.
//!
//! Subcommands:
//!   seed    reset + seed the pool universe and the bench accounting period
//!   load    one open-loop load rung against direct or staging (SLO sweeps
//!           call this repeatedly; JSON report on stdout)
//!   soak    the full concurrent run: load + feed + recalc workers + gauges,
//!           then quiesce → verify → close → immutability probes
//!   verify  standalone conservation sweep against current DB state
//!   gauges  one-shot G1/G2 gauge sample (JSON on stdout)
//!
//! Operator entry points: scripts/soak.sh and scripts/slo-sweep.sh.

mod budget;
mod driver;
mod gauges;
mod pacing;
mod soak;
mod universe;
mod verify;
mod workload;

use clap::{Args, Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

const DEFAULT_DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3_2";

#[derive(Parser)]
#[command(name = "ledger-bench", about = "ledger-v3.2 soak / load / verify harness")]
struct Cli {
    #[arg(long, default_value = DEFAULT_DSN, global = true)]
    dsn: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct WorkloadArgs {
    /// Pool universe size (methods cycle fifo/lifo/wac).
    #[arg(long, default_value_t = 300)]
    pools: i64,
    /// Pareto-hot pool pick (80% of ops on the first 20% of ids).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pareto: bool,
    /// Percent of ops that are depletions.
    #[arg(long, default_value_t = 45)]
    deplete_pct: u8,
    /// Receipt-cost volatility profile (the oracle's missing knob).
    #[arg(long, value_enum, default_value_t = workload::CostProfile::Med)]
    profile: workload::CostProfile,
    /// Base receipt cost, µ-units.
    #[arg(long, default_value_t = 500_000)]
    base_cost: i64,
    /// Percent of submissions stamped behind the wall clock.
    #[arg(long, default_value_t = 5)]
    backdate_pct: u8,
    /// Backdate offset window, seconds.
    #[arg(long, default_value_t = 3600)]
    backdate_window_secs: u64,
    #[arg(long, default_value_t = 100)]
    receipt_qty_max: i64,
    #[arg(long, default_value_t = 60)]
    deplete_qty_max: i64,
    /// RNG seed for workload + pacing.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

impl WorkloadArgs {
    fn build(&self) -> workload::Workload {
        workload::Workload {
            n_pools: self.pools,
            pareto: self.pareto,
            deplete_pct: self.deplete_pct,
            profile: self.profile,
            base_cost: self.base_cost,
            backdate_pct: self.backdate_pct,
            backdate_window_secs: self.backdate_window_secs,
            receipt_qty_max: self.receipt_qty_max,
            deplete_qty_max: self.deplete_qty_max,
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Reset the database and seed the bench universe.
    Seed {
        #[arg(long, default_value_t = 300)]
        pools: i64,
        /// Days before today the bench period starts.
        #[arg(long, default_value_t = 60)]
        period_back_days: i64,
    },
    /// One open-loop load rung (hot path only — no feed/workers). JSON report
    /// on stdout; run `seed` first.
    Load {
        #[command(flatten)]
        workload: WorkloadArgs,
        #[arg(long, value_enum, default_value_t = driver::SubmitPath::Direct)]
        path: driver::SubmitPath,
        #[arg(long, default_value_t = 32)]
        callers: usize,
        /// Offered rate trx/s across all callers; 0 = closed loop.
        #[arg(long, default_value_t = 0)]
        rate: u64,
        #[arg(long, value_enum, default_value_t = pacing::ArrivalProcess::Poisson)]
        arrival: pacing::ArrivalProcess,
        #[arg(long, default_value_t = 30)]
        duration_secs: u64,
        /// Staging committers (staging path only).
        #[arg(long, default_value_t = 2)]
        drainers: usize,
        /// Rows per drain transaction. A drain tx holds every touched pool's
        /// aggregate lock until commit, so a large batch over a Pareto-hot
        /// universe convoys concurrent direct-path submitters — keep it small
        /// when both paths run together.
        #[arg(long, default_value_t = 25)]
        drain_batch: i32,
    },
    /// The full concurrent soak (see module docs).
    Soak {
        #[command(flatten)]
        workload: WorkloadArgs,
        #[arg(long, default_value_t = 24)]
        direct_callers: usize,
        #[arg(long, default_value_t = 8)]
        staging_callers: usize,
        /// Offered rate trx/s per path; 0 = closed loop.
        #[arg(long, default_value_t = 400)]
        rate: u64,
        #[arg(long, value_enum, default_value_t = pacing::ArrivalProcess::Poisson)]
        arrival: pacing::ArrivalProcess,
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        #[arg(long, default_value_t = 4)]
        workers: usize,
        #[arg(long, default_value_t = 100)]
        worker_poll_ms: u64,
        #[arg(long, default_value_t = 2)]
        drainers: usize,
        /// Rows per drain transaction (see `load --drain-batch`: large
        /// batches convoy the concurrent direct path).
        #[arg(long, default_value_t = 25)]
        drain_batch: i32,
        /// Pause recalc workers over [A,B) seconds (G2 climbs; "A:B").
        #[arg(long, value_parser = parse_window)]
        pause_workers: Option<(u64, u64)>,
        /// Pause the feed consumer over [A,B) seconds (G1 climbs; "A:B").
        #[arg(long, value_parser = parse_window)]
        pause_feed: Option<(u64, u64)>,
        /// Dry-run (rolled-back) unforced close probe at this offset, seconds.
        #[arg(long)]
        midrun_close_at: Option<u64>,
        #[arg(long, default_value_t = 1000)]
        gauge_interval_ms: u64,
        #[arg(long, default_value_t = 180)]
        quiesce_timeout_secs: u64,
        /// Reuse the existing universe instead of reset+seed.
        #[arg(long, default_value_t = false)]
        skip_seed: bool,
        #[arg(long, default_value = "bench/out")]
        out_dir: std::path::PathBuf,
    },
    /// Standalone conservation sweep against current DB state.
    Verify {
        /// Also require every FL depletion settled + oracle equivalence
        /// (only meaningful on a quiesced database).
        #[arg(long, default_value_t = false)]
        settled: bool,
        /// Also require post-sweep value_sum == Σ layer value (closed period).
        #[arg(long, default_value_t = false)]
        swept: bool,
    },
    /// One-shot gauge sample (JSON on stdout).
    Gauges,
}

fn parse_window(s: &str) -> Result<(u64, u64), String> {
    let (a, b) = s.split_once(':').ok_or_else(|| format!("expected A:B, got {s}"))?;
    let a: u64 = a.parse().map_err(|e| format!("bad window start: {e}"))?;
    let b: u64 = b.parse().map_err(|e| format!("bad window end: {e}"))?;
    if b <= a {
        return Err(format!("window end {b} must be after start {a}"));
    }
    Ok((a, b))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Seed { pools, period_back_days } => {
            let pool = connect(&cli.dsn, 4).await;
            universe::reset(&pool).await.expect("reset");
            universe::seed(&pool, pools, period_back_days).await.expect("seed");
            println!("seeded {pools} pools (methods cycle fifo/lifo/wac), period id 1 open");
        }
        Cmd::Load {
            workload,
            path,
            callers,
            rate,
            arrival,
            duration_secs,
            drainers,
            drain_batch,
        } => {
            let pool = connect(&cli.dsn, (callers + drainers + 4) as u32).await;
            let source_seq = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
                driver::source_id_base(&pool).await,
            ));
            let cfg = driver::LoadCfg {
                path,
                callers,
                rate: (rate > 0).then_some(rate),
                arrival,
                duration: Duration::from_secs(duration_secs),
                workload: workload.build(),
                drainers,
                drain_batch,
                seed: workload.seed,
                source_seq,
            };
            let outcome = driver::run_load(&pool, &cfg).await;
            println!("{}", serde_json::to_string_pretty(&outcome).expect("serialize"));
            if !outcome.errors.is_empty() {
                std::process::exit(1);
            }
        }
        Cmd::Soak {
            workload,
            direct_callers,
            staging_callers,
            rate,
            arrival,
            duration_secs,
            workers,
            worker_poll_ms,
            drainers,
            drain_batch,
            pause_workers,
            pause_feed,
            midrun_close_at,
            gauge_interval_ms,
            quiesce_timeout_secs,
            skip_seed,
            out_dir,
        } => {
            let pools = workload.pools;
            let cfg = soak::SoakCfg {
                dsn: cli.dsn.clone(),
                pools,
                direct_callers,
                staging_callers,
                rate: (rate > 0).then_some(rate),
                arrival,
                duration: Duration::from_secs(duration_secs),
                workload: workload.build(),
                workers,
                worker_poll_ms,
                drainers,
                drain_batch,
                pause_workers,
                pause_feed,
                midrun_close_at,
                gauge_interval_ms,
                quiesce_timeout: Duration::from_secs(quiesce_timeout_secs),
                out_dir,
                seed: workload.seed,
                skip_seed,
            };
            match soak::run_soak(cfg).await {
                Ok(report) => {
                    print_soak_summary(&report);
                    if !report.passed {
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("soak FAILED: {e}");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Verify { settled, swept } => {
            let pool = connect(&cli.dsn, 4).await;
            let report =
                verify::run_verify(&pool, settled, swept, None).await.expect("verify");
            println!("{}", serde_json::to_string_pretty(&report).expect("serialize"));
            if !report.ok() {
                std::process::exit(1);
            }
        }
        Cmd::Gauges => {
            let pool = connect(&cli.dsn, 2).await;
            let sample = gauges::sample(&pool, 0).await;
            println!("{}", serde_json::to_string_pretty(&sample).expect("serialize"));
        }
    }
}

async fn connect(dsn: &str, max: u32) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(30))
        .connect(dsn)
        .await
        .expect("connect to database")
}

fn print_soak_summary(r: &soak::SoakReport) {
    println!("── soak summary ──────────────────────────────────────────────");
    if let Some(d) = &r.direct {
        println!(
            "direct : {} acked ({:.0}/s), {} qty-gate rejects, {} throttled, p99 {}µs",
            d.acked, d.achieved_rate, d.rejected_qty_gate, d.rejected_backpressure, d.p99_us
        );
    }
    if let Some(s) = &r.staging {
        println!(
            "staging: {} enqueued ({:.0}/s), {} applied, {} failed, {} throttled, p99 {}µs",
            s.acked, s.achieved_rate, s.drained_done, s.drained_failed,
            s.rejected_backpressure, s.p99_us
        );
    }
    println!(
        "workers: {} passes, {} settlements, {} adjustments, {} errors",
        r.workers.passes,
        r.workers.settlements_written,
        r.workers.adjustments_posted,
        r.workers.errors
    );
    println!(
        "gauges : max G1 lag {}B / retained {}B; max G2 dirty {} pools, \
         unsettled {} events / {} gross",
        r.gauges.max_g1_lag_bytes,
        r.gauges.max_g1_retained_wal_bytes,
        r.gauges.max_g2c_dirty_pools,
        r.gauges.max_g2b_unsettled_events,
        r.gauges.max_g2b_unsettled_gross
    );
    println!(
        "backpr : max {} throttled pools, max per-pool backlog {}",
        r.gauges.max_bp_throttled_pools, r.gauges.max_bp_max_backlog
    );
    println!(
        "quiesce: {:.1}s; verify settled: {} failures; close: {}; post-close: {} failures",
        r.quiesce_secs,
        r.verify_settled.failures.len(),
        if r.close_report["closed"] == serde_json::json!(true) { "closed" } else { "NOT CLOSED" },
        r.verify_post_close.failures.len()
    );
    for d in &r.verify_settled.drift {
        println!(
            "drift  : {} n={} bias {:+.0}µ mean|Δ| {:.0}µ p99|Δ| {}µ rel {:.2}% exact {:.1}%",
            d.method, d.depletions, d.mean_delta, d.mean_abs_delta, d.p99_abs_delta,
            d.rel_pct, d.exact_pct
        );
    }
    println!(
        "probes : in-period submit → {} (want 55000); next-day flows: {}; re-close no-op: {}",
        r.immutability_probe_sqlstate, r.next_period_flows, r.reclose_already_closed
    );
    println!("PASSED : {}", r.passed);
}
