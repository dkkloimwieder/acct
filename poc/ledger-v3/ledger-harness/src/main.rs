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
#[allow(dead_code)] // wired by run-subcommand drivers (acct-ykyl, acct-qiaz)
mod sampler;

use clap::Parser;

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
            eprintln!(
                "seed-pools: count={count} skus={skus} locations={locations} method_mix={method_mix:?}"
            );
            eprintln!("[stub] body lands in acct-llt2");
            std::process::ExitCode::from(2)
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
