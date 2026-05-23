//! ledger-harness: multi-session measurement binary for ledger-v3.
//!
//! Authoritative spec: `poc/design_research/design-v3.md` §8.5 / §9.
//!
//! Three subcommands per plan §F: seed-pools, run, equivalence. CLI
//! surface (this file + cli.rs) lands here in acct-bitp; per-subcommand
//! bodies follow:
//!   seed-pools   acct-llt2
//!   run          acct-ykyl (direct) / acct-qiaz (routed)
//!   equivalence  acct-t9lo

mod cli;
mod driver_direct;
mod driver_routed;
mod equivalence;
mod measure;
mod pool_universe;
mod report;
mod sampler;
mod scenarios;
mod workload;

use std::time::Duration;

use clap::Parser;
use sqlx::postgres::PgPoolOptions;

use cli::{Cli, Cmd, Path};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args = Cli::parse();

    match args.cmd {
        Cmd::SeedPools {
            count,
            skus,
            locations,
            method_mix,
        } => {
            let pool = match PgPoolOptions::new()
                .max_connections(4)
                .acquire_timeout(Duration::from_secs(10))
                .connect(&args.dsn)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("connect failed: {e}");
                    return std::process::ExitCode::from(1);
                }
            };
            match pool_universe::seed(&pool, count, skus, locations, method_mix).await {
                Ok(u) => {
                    println!(
                        "{{\"pools\":{},\"inv_account\":{},\"ap_account\":{}}}",
                        u.pool_ids.len(),
                        u.inv_account,
                        u.ap_account
                    );
                    std::process::ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("seed-pools failed: {e}");
                    std::process::ExitCode::from(1)
                }
            }
        }
        Cmd::Run {
            scenario,
            path,
            duration,
            output,
            no_sampler,
            max_callers,
        } => match path {
            Path::Direct => {
                let opts = driver_direct::RunOptions {
                    dsn: args.dsn,
                    scenario,
                    duration: duration.into(),
                    output,
                    no_sampler,
                    max_callers,
                };
                match driver_direct::run(opts).await {
                    Ok(()) => std::process::ExitCode::SUCCESS,
                    Err(e) => {
                        eprintln!("run direct failed: {e}");
                        std::process::ExitCode::from(1)
                    }
                }
            }
            Path::Routed => {
                let opts = driver_routed::RunOptions {
                    dsn: args.dsn,
                    scenario,
                    duration: duration.into(),
                    output,
                    no_sampler,
                    max_callers,
                    drain_deadline: Duration::from_secs(30),
                };
                match driver_routed::run(opts).await {
                    Ok(()) => std::process::ExitCode::SUCCESS,
                    Err(e) => {
                        eprintln!("run routed failed: {e}");
                        std::process::ExitCode::from(1)
                    }
                }
            }
        },
        Cmd::Equivalence {
            scenario,
            submissions_per_caller,
            strict,
            method_mix,
        } => {
            let opts = equivalence::EquivalenceOptions {
                dsn: args.dsn,
                scenario,
                submissions_per_caller,
                strict,
                method_mix,
            };
            match equivalence::run(opts).await {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("equivalence failed: {e}");
                    std::process::ExitCode::from(1)
                }
            }
        }
    }
}
