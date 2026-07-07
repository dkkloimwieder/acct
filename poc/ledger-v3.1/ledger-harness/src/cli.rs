//! clap-derived CLI surface for `ledger-harness` (design-v3.1 §10, P4 acct-2ttr.8).
//!
//! Three subcommands:
//!   seed-pools   pool universe + optional deep-pool layer seeding (§10.5)
//!   run          drive a scenario in one of three submission modes (§10.0)
//!   equivalence  cross-flavor §11.1 aggregate-qty diff (direct-c vs routed-c)
//!
//! Unlike ledger-v3 (which had a `--path direct|routed`), v3.1 selects among
//! THREE submission modes via `--mode`: direct-per-call, direct-batched, and
//! routed (§10.0). The DSN defaults to the v3.1 PoC database `poc_v3_1`.

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "ledger-harness",
    about = "ledger-v3.1 (Path C) multi-session measurement binary (design-v3.1 §10)",
    version,
)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,

    /// Postgres DSN. Defaults to the dev poc_v3_1 DB. For the 1000-caller
    /// scenarios (S5/S7/S8) point this at a pgbouncer/pgcat transaction pool
    /// fronting poc_v3_1 (see bench/setup-pgbouncer.sh) — 1000 direct backends
    /// exceed the dev container's io_uring memlock ceiling (acct-8cn2).
    #[arg(long, global = true, default_value = "postgres://acct:acct_dev@localhost:5111/poc_v3_1")]
    pub dsn: String,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Seed a pool universe (idempotent on the universe), optionally deepening
    /// each pool to `--depth` layer rows (§10.5 deep-pool seeding).
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
        #[arg(long, value_enum, default_value_t = MethodMix::AllFifo)]
        method_mix: MethodMix,
        /// Pre-seed each pool to this many layer rows + a matching aggregate
        /// (§10.5). 0 = no deep seeding (pools start empty; receipt workloads
        /// self-seed the aggregate). 10 = shallow, 1000 = deep. Establishes the
        /// pool depth the §11.2 lock-hold-vs-depth measurement sweeps over.
        #[arg(long, default_value_t = 0)]
        depth: usize,
    },
    /// Drive a scenario in one submission mode and collect measurements.
    Run {
        /// Scenario id (s1..s21 per §10.6; s9 is the canned multi-touch mix;
        /// s10-s21 are the Pareto 80/20 receipts/builds/mixed family block).
        #[arg(long)]
        scenario: String,
        /// Submission mode (§10.0).
        #[arg(long, value_enum)]
        mode: Mode,
        /// How long to drive load.
        #[arg(long, default_value = "30s")]
        duration: humantime::Duration,
        /// JSON results output path. Default: results/<scenario>-<mode>-<ts>.json.
        #[arg(long)]
        output: Option<std::path::PathBuf>,
        /// Disable the pg_locks sampler.
        #[arg(long)]
        no_sampler: bool,
        /// Cap caller count at most this many — useful when PG max_connections
        /// cannot accommodate the scenario's natural caller count. Prefer
        /// pointing --dsn at a pooler over capping (acct-8cn2); capping changes
        /// the measured concurrency and is recorded in the report.
        #[arg(long)]
        max_callers: Option<usize>,
        /// Submissions per user-tx in `direct-batched` mode (§5.5). Ignored in
        /// the other modes.
        #[arg(long, default_value_t = 50)]
        batch_size: usize,
        /// Pace the routed caller pool to this total offered rate (trx/s),
        /// split evenly across callers with staggered absolute-schedule
        /// interval pacing. Callers that fall behind (enqueue blocked on
        /// staging backpressure) shed debt rather than burst-catch-up, so
        /// achieved rate degrades gracefully past saturation; offered vs
        /// achieved are both observable. Unset/0 = full-blast open-loop (the
        /// original behavior). Routed mode only.
        #[arg(long)]
        target_rate: Option<u64>,
        /// The pool depth the universe was seeded to (informational; recorded
        /// in the report so the §11.2 lock-hold-vs-depth sweep can label runs).
        /// Establish the depth itself with `seed-pools --depth`.
        #[arg(long)]
        depth: Option<usize>,
        /// If set, TRUNCATE all ledger tables and re-seed the pool universe with
        /// this method assignment (and `--seed-depth`) before driving.
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
        /// Deep-seed depth applied during a --method-mix reseed.
        #[arg(long, default_value_t = 0)]
        seed_depth: usize,
        /// Overlay multi-touch (same-pool-twice) generation onto ANY scenario
        /// (acct-34ce): the percent of submissions eligible to touch one pool
        /// more than once. Overrides the scenario's own value (s9 presets 40).
        /// 0/unset keeps distinct-pool generation. Only multi-line (Complex)
        /// submissions can repeat — Simple is always single-touch.
        #[arg(long)]
        multi_touch_pct: Option<u8>,
        /// Overlay the per-pool group-size distribution for multi-touch
        /// submissions, as `touches:weight,…` (e.g. `1:60,2:30,3:10`). Only
        /// consulted when multi-touch is active. Overrides the scenario's value.
        #[arg(long)]
        touch_dist: Option<String>,
        /// Overlay the Pareto hot-pool fraction (acct-s90k): percent of the
        /// universe that forms the hot population. Switches the scenario's
        /// overlap to `Pareto` even if it was previously Uniform/Zipf/Disjoint;
        /// the cold half is the complement. Pairs with `--pareto-hot-traffic-pct`
        /// (defaults to 80 when only the pool fraction is set). 0/unset leaves
        /// the scenario's overlap untouched.
        #[arg(long)]
        pareto_hot_pool_pct: Option<u8>,
        /// Overlay the Pareto hot-traffic fraction (acct-s90k): percent of
        /// submissions that target the hot population. Pairs with
        /// `--pareto-hot-pool-pct` (defaults to 20 when only the traffic
        /// fraction is set). Unset leaves the scenario's overlap untouched.
        #[arg(long)]
        pareto_hot_traffic_pct: Option<u8>,
    },
    /// Drive direct-c and routed-c against an identical input sequence and diff
    /// the resulting pool_state aggregate qty (§11.1). Aggregate qty must match
    /// exactly; provisional unit_costs may differ (within-batch ordering, by
    /// design — not a bug).
    Equivalence {
        #[arg(long)]
        scenario: String,
        /// Submissions per caller. Total = callers × this.
        #[arg(long, default_value_t = 50)]
        submissions_per_caller: usize,
        /// Caller count for the deterministic replay (capped small so the run
        /// stays well under a minute).
        #[arg(long, default_value_t = 8)]
        callers: usize,
        /// Pool method assignment for the seeded universe.
        #[arg(long, value_enum, default_value_t = MethodMix::AllFifo)]
        method_mix: MethodMix,
        /// Deep-seed depth for the equivalence universe (gives depletions
        /// headroom so both flavors always succeed regardless of order).
        #[arg(long, default_value_t = 5)]
        depth: usize,
    },
}

/// Submission mode (§10.0). Replaces ledger-v3's `--path direct|routed`.
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum Mode {
    /// Each `ledger_submit_trx_c` runs in its own user-tx.
    DirectPerCall,
    /// Caller wraps N `ledger_submit_trx_c` calls in one user-tx (§5.5).
    DirectBatched,
    /// Caller calls `ledger_enqueue_trx_c`; work batched across callers (§6).
    Routed,
}

impl Mode {
    /// Stable label used in report filenames and the `mode` JSON field.
    pub fn label(self) -> &'static str {
        match self {
            Mode::DirectPerCall => "direct-per-call",
            Mode::DirectBatched => "direct-batched",
            Mode::Routed => "routed",
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum, Default, PartialEq, Eq)]
pub enum MethodMix {
    #[default]
    AllFifo,
    AllLifo,
    AllWac,
    AllStd,
    AllSpecific,
    /// 50% fifo, 30% wac, 20% std (deterministic, no RNG) per §10.4.
    Mixed,
}
