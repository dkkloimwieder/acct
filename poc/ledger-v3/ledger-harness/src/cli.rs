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
        /// Cap caller count at most this many — useful when PG
        /// max_connections cannot accommodate the scenario's natural
        /// caller count (e.g. dev container at 320 vs S5/S6 wanting 1000).
        #[arg(long)]
        max_callers: Option<usize>,
        /// If set, TRUNCATE all ledger tables and re-seed the pool
        /// universe with this method assignment before driving. Used
        /// for cross-method bench (acct-9mgx.{1..5}) to switch the
        /// existing AllWac universe to AllFifo / AllWacPeriodic / etc.
        /// without manually invoking seed-pools.
        #[arg(long, value_enum)]
        method_mix: Option<MethodMix>,
        /// Pool universe size for --method-mix reseed (ignored otherwise).
        #[arg(long, default_value_t = 10_000)]
        seed_count: usize,
        /// Distinct SKU count for --method-mix reseed.
        #[arg(long, default_value_t = 1_000)]
        seed_skus: usize,
        /// Distinct location count for --method-mix reseed.
        #[arg(long, default_value_t = 10)]
        seed_locations: usize,
    },
    /// Run Path A and Path B against an identical workload and diff
    /// the resulting trx + pool_state tables (design-v3 §8.4).
    Equivalence {
        #[arg(long)]
        scenario: String,
        /// Submissions per caller. Total = callers × this. Default 50
        /// keeps each run well under a minute.
        #[arg(long, default_value_t = 50)]
        submissions_per_caller: usize,
        /// Upgrade WAC unit_cost drift on shared pools (acct-mcey) from
        /// informational to error. Use with Path B configured for a
        /// single committer (ALTER SYSTEM SET ledger_routed.committer_count
        /// = 1 + postmaster restart) to require byte equivalence.
        #[arg(long)]
        strict: bool,
        /// Pool method assignment for the seeded universe. AllWacPeriodic
        /// additionally seeds an accounting_period and triggers
        /// ledger_close_period after each path's submissions (acct-9mgx.6).
        #[arg(long, value_enum, default_value_t = MethodMix::AllWac)]
        method_mix: MethodMix,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Path {
    Direct,
    Routed,
}

#[derive(Copy, Clone, Debug, ValueEnum, Default, PartialEq, Eq)]
pub enum MethodMix {
    #[default]
    AllWac,
    AllWacPeriodic,
    AllFifo,
    Mixed,
}
