//! ledger-harness: multi-session measurement binary for ledger-v3.1 (Path C).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §10 / §11.
//!
//! Subcommands (P4, acct-2ttr.8):
//!   seed-pools   pool universe + optional deep-pool layer seeding (§10.5)
//!   run          drive a scenario in one of three submission modes (§10.0):
//!                  direct-per-call | direct-batched | routed
//!   equivalence  cross-flavor §11.1 aggregate-qty diff (direct-c vs routed-c)
//!
//! This is a plain sqlx + tokio client binary (NOT pgrx) that talks to an
//! already-installed poc_v3_1 (ledger_direct_c + ledger_routed_c). For the
//! 1000-caller scenarios point `--dsn` at a pgbouncer/pgcat transaction pool
//! (bench/setup-pgbouncer.sh) — see acct-8cn2.

mod cli;
mod driver_common;
mod driver_direct;
mod driver_routed;
mod driver_staging;
mod equivalence;
mod measure;
mod pacing;
mod pool_universe;
mod report;
mod sampler;
mod scenarios;
mod seed;
mod workload;

use std::time::Duration;

use clap::Parser;
use sqlx::postgres::PgPoolOptions;

use cli::{Cli, Cmd, Mode};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Cli::parse();

    match args.cmd {
        Cmd::SeedPools { count, skus, locations, method_mix, depth } => {
            let pool = match connect(&args.dsn, 8).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("connect failed: {e}");
                    return std::process::ExitCode::from(1);
                }
            };
            let universe = match pool_universe::seed(&pool, count, skus, locations, method_mix).await {
                Ok(u) => u,
                Err(e) => {
                    eprintln!("seed-pools failed: {e}");
                    return std::process::ExitCode::from(1);
                }
            };
            if let Err(e) = seed::deepen(&pool, &universe.pool_ids, depth).await {
                eprintln!("deep-seed failed: {e}");
                return std::process::ExitCode::from(1);
            }
            println!(
                "{{\"pools\":{},\"inv_account\":{},\"ap_account\":{},\"variance_account\":{},\"depth\":{}}}",
                universe.pool_ids.len(),
                universe.inv_account,
                universe.ap_account,
                universe.variance_account,
                depth
            );
            std::process::ExitCode::SUCCESS
        }

        Cmd::Run {
            scenario,
            mode,
            duration,
            output,
            no_sampler,
            max_callers,
            batch_size,
            target_rate,
            arrival,
            committers,
            drain_batch,
            depth,
            method_mix,
            seed_count,
            seed_skus,
            seed_locations,
            seed_depth,
            multi_touch_pct,
            touch_dist,
            pareto_hot_pool_pct,
            pareto_hot_traffic_pct,
            verify,
            seed,
        } => {
            // Kept for the optional post-run conservation sweep (--verify); the
            // driver calls below move args.dsn into their options.
            let dsn_for_verify = args.dsn.clone();
            // Parse the multi-touch distribution overlay up front so a bad spec
            // fails before any reseed/load work (acct-34ce).
            let touch_dist = match touch_dist {
                Some(s) => match workload::TouchDistribution::parse(&s) {
                    Ok(d) => Some(d),
                    Err(e) => {
                        eprintln!("--touch-dist parse failed: {e}");
                        return std::process::ExitCode::from(1);
                    }
                },
                None => None,
            };
            if let Some(mix) = method_mix {
                let pool = match connect(&args.dsn, 8).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("connect (reseed) failed: {e}");
                        return std::process::ExitCode::from(1);
                    }
                };
                if let Err(e) = pool_universe::reset_ledger_tables(&pool).await {
                    eprintln!("reseed TRUNCATE failed: {e}");
                    return std::process::ExitCode::from(1);
                }
                match pool_universe::seed(&pool, seed_count, seed_skus, seed_locations, mix).await {
                    Ok(u) => {
                        if let Err(e) = seed::deepen(&pool, &u.pool_ids, seed_depth).await {
                            eprintln!("reseed deep-seed failed: {e}");
                            return std::process::ExitCode::from(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("reseed seed-pools failed: {e}");
                        return std::process::ExitCode::from(1);
                    }
                }
                eprintln!("[run] reseeded {seed_count} pools mix={mix:?} depth={seed_depth} for bench");
            }

            let result = match mode {
                Mode::DirectPerCall | Mode::DirectBatched | Mode::DirectSingle => {
                    driver_direct::run(driver_direct::RunOptions {
                        dsn: args.dsn,
                        scenario,
                        mode,
                        batch_size,
                        pool_depth: depth,
                        duration: duration.into(),
                        output,
                        no_sampler,
                        max_callers,
                        target_rate,
                        arrival,
                        multi_touch_pct,
                        touch_dist,
                        pareto_hot_pool_pct,
                        pareto_hot_traffic_pct,
                        seed,
                    })
                    .await
                }
                Mode::Routed => {
                    driver_routed::run(driver_routed::RunOptions {
                        dsn: args.dsn,
                        scenario,
                        pool_depth: depth,
                        duration: duration.into(),
                        output,
                        no_sampler,
                        max_callers,
                        batch_size,
                        target_rate,
                        arrival,
                        drain_deadline: Duration::from_secs(30),
                        multi_touch_pct,
                        touch_dist,
                        pareto_hot_pool_pct,
                        pareto_hot_traffic_pct,
                        seed,
                    })
                    .await
                }
                Mode::Staging => {
                    driver_staging::run(driver_staging::RunOptions {
                        dsn: args.dsn,
                        scenario,
                        pool_depth: depth,
                        duration: duration.into(),
                        output,
                        no_sampler,
                        max_callers,
                        target_rate,
                        arrival,
                        committers,
                        drain_batch,
                        drain_deadline: Duration::from_secs(30),
                        multi_touch_pct,
                        touch_dist,
                        pareto_hot_pool_pct,
                        pareto_hot_traffic_pct,
                        seed,
                    })
                    .await
                }
            };
            match result {
                Ok(()) => {
                    if verify {
                        // Bench-harness wiring of the conservation sweep: the
                        // just-driven end state must satisfy every invariant.
                        conservation_verify(&dsn_for_verify).await
                    } else {
                        std::process::ExitCode::SUCCESS
                    }
                }
                Err(e) => {
                    eprintln!("run failed: {e}");
                    std::process::ExitCode::from(1)
                }
            }
        }

        Cmd::Verify {} => conservation_verify(&args.dsn).await,

        Cmd::Equivalence { scenario, submissions_per_caller, callers, method_mix, depth } => {
            match equivalence::run(equivalence::EquivalenceOptions {
                dsn: args.dsn,
                scenario,
                submissions_per_caller,
                callers,
                method_mix,
                depth,
            })
            .await
            {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("equivalence failed: {e}");
                    std::process::ExitCode::from(1)
                }
            }
        }
    }
}

async fn connect(dsn: &str, max_conns: u32) -> Result<sqlx::PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
}

/// Run the conservation-invariant sweep (acct-0at4.5) against `dsn`, print the
/// result as one JSON line (violations detailed on stderr), and map it to a
/// process exit code: SUCCESS when clean, 1 on any violation or query error.
/// Shared by the `verify` subcommand and `run --verify`.
async fn conservation_verify(dsn: &str) -> std::process::ExitCode {
    let pool = match connect(dsn, 4).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("verify connect failed: {e}");
            return std::process::ExitCode::from(1);
        }
    };
    match ledger_verify::run_conservation_sweep(&pool).await {
        Ok(violations) => {
            let items: Vec<String> = violations
                .iter()
                .map(|v| {
                    let detail = serde_json::to_string(&v.detail).unwrap_or_else(|_| "\"\"".into());
                    format!("{{\"check\":\"{}\",\"detail\":{}}}", v.check, detail)
                })
                .collect();
            println!(
                "{{\"sweep\":\"conservation\",\"violations\":{},\"detail\":[{}],\"verdict\":\"{}\"}}",
                violations.len(),
                items.join(","),
                if violations.is_empty() { "PASS" } else { "FAIL" }
            );
            for v in violations.iter().take(20) {
                eprintln!("  [{}] {}", v.check, v.detail);
            }
            if violations.is_empty() {
                std::process::ExitCode::SUCCESS
            } else {
                std::process::ExitCode::from(1)
            }
        }
        Err(e) => {
            eprintln!("verify sweep query error: {e}");
            std::process::ExitCode::from(1)
        }
    }
}
