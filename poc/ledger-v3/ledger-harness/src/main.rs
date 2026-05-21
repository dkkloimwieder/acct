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
mod pool_universe;
#[allow(dead_code)] // wired by run-subcommand drivers (acct-ykyl, acct-qiaz)
mod sampler;

use std::time::Duration;

use clap::Parser;
use sqlx::postgres::PgPoolOptions;

use cli::{Cli, Cmd};

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
        } => {
            eprintln!(
                "run: scenario={scenario} path={path:?} duration={duration} output={output:?} no_sampler={no_sampler}"
            );
            eprintln!("[stub] direct driver lands in acct-ykyl, routed in acct-qiaz");
            std::process::ExitCode::from(2)
        }
        Cmd::Equivalence { scenario, duration } => {
            eprintln!("equivalence: scenario={scenario} duration={duration}");
            eprintln!("[stub] body lands in acct-t9lo");
            std::process::ExitCode::from(2)
        }
    }
}
