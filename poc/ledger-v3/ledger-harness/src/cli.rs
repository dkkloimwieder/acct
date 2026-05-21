//! clap-derived CLI surface for `ledger-harness`.
//!
//! Three subcommands per plan §F:
//!   seed-pools   one-time pool universe setup (acct-llt2 wires the body)
//!   run          drive a scenario against direct or routed (acct-ykyl / acct-qiaz)
//!   equivalence  cross-path §8.4 diff (acct-t9lo)

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "ledger-harness",
    about = "ledger-v3 multi-session measurement binary (design-v3 §8.5 / §9)",
    version,
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,

    /// Postgres DSN. Defaults to the dev poc_v3 DB.
    #[arg(long, global = true, default_value = "postgres://acct:acct_dev@localhost:5111/poc_v3")]
    pub dsn: String,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Seed a pool universe (idempotent — skips on existing fixture).
    SeedPools {
        /// Number of pools to create.
        #[arg(long, default_value_t = 10_000)]
        count: usize,
        /// Number of distinct SKUs.
        #[arg(long, default_value_t = 1_000)]
        skus: usize,
        /// Number of distinct locations.
        #[arg(long, default_value_t = 10)]
        locations: usize,
        /// Pool method assignment for the seeded pools.
        #[arg(long, value_enum, default_value_t = MethodMix::AllWac)]
        method_mix: MethodMix,
    },
    /// Drive a scenario against a path and collect measurements.
    Run {
        /// Scenario id (s1..s6 per plan §F).
        #[arg(long)]
        scenario: String,
        /// Execution path.
        #[arg(long, value_enum)]
        path: Path,
        /// How long to drive load.
        #[arg(long, default_value = "30s")]
        duration: humantime::Duration,
        /// JSON results output path. Default: results/<scenario>-<path>-<ts>.json.
        #[arg(long)]
        output: Option<std::path::PathBuf>,
        /// Disable the pg_locks sampler.
        #[arg(long)]
        no_sampler: bool,
    },
    /// Run Path A and Path B against an identical workload and diff
    /// the resulting trx + pool_state tables (design-v3 §8.4).
    Equivalence {
        #[arg(long)]
        scenario: String,
        #[arg(long, default_value = "30s")]
        duration: humantime::Duration,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Path {
    Direct,
    Routed,
}

#[derive(Copy, Clone, Debug, ValueEnum, Default)]
pub enum MethodMix {
    #[default]
    AllWac,
    AllFifo,
    Mixed,
}
