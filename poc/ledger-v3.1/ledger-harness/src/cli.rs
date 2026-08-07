//! clap-derived CLI surface for `ledger-harness` (design-v3.1 §10, P4 acct-2ttr.8).
//!
//! Subcommands:
//!   seed-pools   pool universe + optional deep-pool layer seeding (§10.5)
//!   run          drive a scenario in one of three submission modes (§10.0)
//!   equivalence  cross-flavor §11.1 aggregate-qty diff (direct-c vs routed-c)
//!   verify       conservation-invariant SQL sweep as a post-run post-condition
//!                (acct-0at4.5) — also runnable inline via `run --verify`
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
        /// Drive an OPEN-LOOP arrival process at this total offered rate
        /// (trx/s), split across callers on a staggered absolute schedule
        /// (acct-0at4.8). Per-submission latency is measured from the INTENDED
        /// send-time, so a stall shows up as a growing tail instead of being
        /// omitted (coordinated-omission-free). The schedule is never rebased
        /// to now: a caller that falls behind degrades to as-fast-as-possible
        /// while its measured latency climbs — so offered vs achieved
        /// (`throughput_trx_per_sec`) is the saturation signal. Works in all
        /// modes (direct-*, routed, staging). Unset/0 = full-blast closed-loop
        /// (peak-throughput measurement only).
        #[arg(long)]
        target_rate: Option<u64>,
        /// Arrival process for `--target-rate` open-loop pacing (ignored
        /// closed-loop). `poisson` (default) draws memoryless exponential
        /// inter-arrivals — the realistic model the saturation methodology
        /// assumes; `uniform` fires on a fixed interval (lower-variance A/B).
        #[arg(long, value_enum, default_value_t = crate::pacing::ArrivalProcess::Poisson)]
        arrival: crate::pacing::ArrivalProcess,
        /// Staging mode only (SPIKE-A): number of committer connections that loop
        /// `ledger_staging_drain_c`. Defaults to routed's COMMITTER_COUNT (4) so
        /// the drain-loop parallelism matches the shmem flavor's BGWorker pool.
        #[arg(long, default_value_t = 4)]
        committers: usize,
        /// Staging mode only (SPIKE-A): rows claimed per `ledger_staging_drain_c`
        /// call (the SKIP LOCKED `LIMIT`). Defaults to routed's batch_size_max
        /// (200) so each drain commit group matches routed's chunk size.
        #[arg(long, default_value_t = 200)]
        drain_batch: usize,
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
        /// After the bench completes, run the conservation-invariant sweep
        /// (acct-0at4.5) over the resulting DB state and fail the run (nonzero
        /// exit) if any invariant is violated. The bench-harness wiring of the
        /// same sweep exposed standalone as the `verify` subcommand.
        #[arg(long)]
        verify: bool,
        /// PRNG seed for the per-caller workload RNG (acct-0at4.10.4). Each
        /// caller derives its stream as `seed + caller_id`, so two runs with
        /// different `--seed` draw INDEPENDENT workload sequences — the
        /// prerequisite for treating repeated runs as independent samples in the
        /// statistics-discipline re-measurement (bootstrap CI / Mann-Whitney).
        /// The default reproduces the historical fixed stream, so every existing
        /// test and bench stays behaviorally identical unless `--seed` is passed.
        #[arg(long, default_value_t = 0xDEAD_BEEF_u64)]
        seed: u64,
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
    /// Run the conservation-invariant SQL sweep (acct-0at4.5) against the DB as
    /// a post-run / post-test post-condition and report PASS/FAIL as JSON.
    /// Reconciles pool_state against the trx_line / posting_line stream; a
    /// nonzero exit means a reconciliation failed. Used by CI test teardown
    /// (scripts/run-tests.sh) and, inline, by `run --verify`.
    Verify {},
}

/// Submission mode (§10.0). Replaces ledger-v3's `--path direct|routed`.
#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum Mode {
    /// Each `ledger_submit_trx_c` runs in its own user-tx.
    DirectPerCall,
    /// Caller wraps N `ledger_submit_trx_c` calls in one user-tx (§5.5).
    DirectBatched,
    /// SPIKE-B (acct-0at4.11.2): each `ledger_submit_trx_single_c` in its own
    /// user-tx. Same direct autocommit shape as DirectPerCall, but the aggregate
    /// read-modify-write is a single commutative SQL statement under PG's own row
    /// lock (no pool_lock table, no Rust round-trip) — the alt-B variant measured
    /// head-to-head against DirectPerCall's RMW-across-SPI.
    DirectSingle,
    /// Caller calls `ledger_enqueue_trx_c`; work batched across callers (§6).
    Routed,
    /// SPIKE-A (acct-0at4.11.1): caller `INSERT`s into `ledger_inbox` in its own
    /// tx; K committer connections drain it via `ledger_staging_drain_c` with
    /// `FOR UPDATE SKIP LOCKED`. The staging-table alternative to routed's shmem
    /// queue — same ledger-core apply, table transport instead of shmem.
    Staging,
}

impl Mode {
    /// Stable label used in report filenames and the `mode` JSON field.
    pub fn label(self) -> &'static str {
        match self {
            Mode::DirectPerCall => "direct-per-call",
            Mode::DirectBatched => "direct-batched",
            Mode::DirectSingle => "direct-single",
            Mode::Routed => "routed",
            Mode::Staging => "staging",
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
