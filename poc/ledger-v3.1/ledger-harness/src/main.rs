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
mod equivalence;
mod measure;
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
        } => {
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
                Mode::DirectPerCall | Mode::DirectBatched => {
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
                        multi_touch_pct,
                        touch_dist,
                        pareto_hot_pool_pct,
                        pareto_hot_traffic_pct,
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
                        drain_deadline: Duration::from_secs(30),
                        multi_touch_pct,
                        touch_dist,
                        pareto_hot_pool_pct,
                        pareto_hot_traffic_pct,
                    })
                    .await
                }
            };
            match result {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("run failed: {e}");
                    std::process::ExitCode::from(1)
                }
            }
        }

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
