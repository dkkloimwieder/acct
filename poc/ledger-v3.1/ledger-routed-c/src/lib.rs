//! ledger-routed-c: pgrx 0.18 extension for ledger-v3.1 Path C (routed flavor).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §6.
//!
//! Caller invokes `ledger_enqueue_trx_c(trx_type, source_id, posted_at, lines)`
//! inside their own user-tx. The function stages a descriptor (incl. the caller's
//! user_tx_xid) into the shmem staging queue and returns a shmem-local
//! submission_id — NO DB write. A router BGWorker groups submissions by pool
//! overlap into commit_groups; a committer BGWorker pool claims a commit_group,
//! dispatches each submission to `ledger_core::plan_apply_provisional`, bulk-writes,
//! and COMMITs (drop-and-continue on per-submission failure — Path C has no
//! cross-trx hot-path state, so there is no pristine-snapshot replay, §6.4 / §14.2).
//!
//! Phases: P3.1 (`acct-2ttr.4`) shmem layout (staging + committer queues +
//! spillover arena + committer identity registry, §6.2) + arena allocator +
//! payload codec + `ledger_enqueue_trx_c` + GUCs + BGWorker registration; P3.2
//! (`acct-2ttr.5`) the router (§6.3); P3.3 (`acct-2ttr.6`) the committer pipeline
//! (§6.4); P3.4 (`acct-2ttr.7`) recovery (§6.5: router boot sweep + committer-death
//! takeover) + committer SQL error handling (§6.8: retry-on-deadlock + poison).

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, pg_shmem_init};
use std::ffi::CString;
use std::sync::atomic::Ordering::Relaxed;

::pgrx::pg_module_magic!();

pub(crate) mod affinity; // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
pub(crate) mod arena;
pub(crate) mod cleanup;
pub(crate) mod committer;
pub(crate) mod enqueue;
pub(crate) mod identity;
pub(crate) mod payload;
pub(crate) mod recovery;
pub(crate) mod router;
pub(crate) mod shmem;

use shmem::{
    COMMITTER_QUEUE, LEDGER_V3_COMMITTER_QUEUE_SIZE, LEDGER_V3_SPILLOVER_ARENA_MB,
    LEDGER_V3_STAGING_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE,
};

// ── GUCs ────────────────────────────────────────────────────────────

static STAGING_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(16384);
static COMMITTER_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(2048);
static SPILLOVER_ARENA_MB: GucSetting<i32> = GucSetting::<i32>::new(128);
static QUEUE_FULL_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);
static ROUTER_WINDOW_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1000);
static BATCH_SIZE_MAX: GucSetting<i32> = GucSetting::<i32>::new(200);
static BATCH_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
static MAX_EJECT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static CALLER_TX_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static COMMITTER_COUNT: GucSetting<i32> = GucSetting::<i32>::new(4);
static EJECT_COOLDOWN_MS: GucSetting<i32> = GucSetting::<i32>::new(10);
static ROUTER_PACK_DISJOINT: GucSetting<bool> = GucSetting::<bool>::new(true);
// [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
static AFFINITY_SCHEME: GucSetting<i32> = GucSetting::<i32>::new(0);
static AFFINITY_STEAL_MS: GucSetting<i32> = GucSetting::<i32>::new(5);
static TARGET_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"poc_v3_1"));

#[allow(dead_code)]
pub(crate) fn target_database_str() -> String {
    TARGET_DATABASE
        .get()
        .as_ref()
        .map(|c| c.to_string_lossy().to_string())
        .unwrap_or_else(|| "poc_v3_1".to_string())
}

#[allow(dead_code)]
pub(crate) fn queue_full_timeout_ms_now() -> i32 {
    QUEUE_FULL_TIMEOUT_MS.get()
}
#[allow(dead_code)]
pub(crate) fn router_window_size_now() -> i32 {
    ROUTER_WINDOW_SIZE.get()
}
#[allow(dead_code)]
pub(crate) fn batch_size_max_now() -> i32 {
    BATCH_SIZE_MAX.get()
}
#[allow(dead_code)]
pub(crate) fn batch_window_us_now() -> i32 {
    BATCH_WINDOW_US.get()
}
#[allow(dead_code)]
pub(crate) fn max_eject_count_now() -> i32 {
    MAX_EJECT_COUNT.get()
}
#[allow(dead_code)]
pub(crate) fn caller_tx_timeout_ms_now() -> i32 {
    CALLER_TX_TIMEOUT_MS.get()
}
#[allow(dead_code)]
pub(crate) fn committer_count_now() -> i32 {
    COMMITTER_COUNT.get().max(1)
}
#[allow(dead_code)]
pub(crate) fn eject_cooldown_ms_now() -> i32 {
    EJECT_COOLDOWN_MS.get().max(0)
}
#[allow(dead_code)]
pub(crate) fn router_pack_disjoint_now() -> bool {
    ROUTER_PACK_DISJOINT.get()
}
// [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
#[allow(dead_code)]
pub(crate) fn affinity_scheme_now() -> i32 {
    AFFINITY_SCHEME.get()
}
// [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
#[allow(dead_code)]
pub(crate) fn affinity_steal_ms_now() -> i32 {
    AFFINITY_STEAL_MS.get().max(0)
}

// ── _PG_init ────────────────────────────────────────────────────────

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(STAGING_QUEUE);
    pg_shmem_init!(COMMITTER_QUEUE);
    pg_shmem_init!(SPILLOVER_ARENA);

    GucRegistry::define_int_guc(
        c"ledger_routed_c.staging_queue_size",
        c"Staging queue entry capacity (compile-time)",
        c"Number of slots in the StagingQueue ring. Compile-time constant (16384); the GUC documents the surface and bounds the range. Resizing requires recompile.",
        &STAGING_QUEUE_SIZE,
        1024,
        262_144,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.committer_queue_size",
        c"Committer queue entry capacity (compile-time)",
        c"Number of slots in the CommitterQueue ring. Compile-time constant (2048).",
        &COMMITTER_QUEUE_SIZE,
        256,
        16_384,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.spillover_arena_mb",
        c"Spillover arena size in MB",
        c"Byte arena holding payloads and pool-key arrays. Compile-time constant (128 MB).",
        &SPILLOVER_ARENA_MB,
        16,
        1024,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.queue_full_timeout_ms",
        c"Caller backpressure timeout (staging queue full)",
        c"When the staging queue is full, callers block on a condition variable up to this duration before raising ERRCODE_INSUFFICIENT_RESOURCES.",
        &QUEUE_FULL_TIMEOUT_MS,
        100,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.router_window_size",
        c"Greedy router scan window (entries per tick)",
        c"How many pending staging entries the router considers per tick when packing the next commit_group.",
        &ROUTER_WINDOW_SIZE,
        100,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.batch_size_max",
        c"Maximum submissions per commit_group",
        c"Hard cap on submissions packed into a single commit_group. Bounds committer lock-hold time on contended pool tails.",
        &BATCH_SIZE_MAX,
        1,
        100_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.batch_window_us",
        c"Router batch coalesce window in microseconds",
        c"How long the router waits to coalesce submissions before emitting a commit_group. 0 disables the gate.",
        &BATCH_WINDOW_US,
        0,
        5_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.max_eject_count",
        c"Caller-tx eject loop bound",
        c"Maximum times a single submission may be ejected (committer observes caller user-tx in_progress) before being marked failed.",
        &MAX_EJECT_COUNT,
        100,
        1_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.caller_tx_timeout_ms",
        c"Caller user-tx total timeout",
        c"Once now - enqueued_at_micros exceeds this threshold, ejected submissions are marked failed regardless of remaining eject budget.",
        &CALLER_TX_TIMEOUT_MS,
        1000,
        3_600_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.committer_count",
        c"Number of committer BGWorkers",
        c"Pool of committer BGWorkers competing for commit_group ownership via CAS election. Read once at _PG_init; a runtime SIGHUP change requires a restart to respawn workers.",
        &COMMITTER_COUNT,
        1,
        64,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed_c.eject_cooldown_ms",
        c"Router skip window for recently-ejected staging entries",
        c"After the committer ejects a staging entry (caller user-tx still in_progress), the router skips re-packing it until this many ms have elapsed since the eject (design-v3.1 §6.3 step 2). 0 disables the filter.",
        &EJECT_COOLDOWN_MS,
        0,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
    GucRegistry::define_int_guc(
        c"ledger_routed_c.affinity_scheme",
        c"Committer→pool affinity scheme (acct-0usf STEP 3, default off)",
        c"0=off (production default: first-come committer claim, no affinity). 1=min_pool: a commit_group is owned by mix(min pool_id) % committer_count; non-owner committers skip it on the claim scan until it ages past affinity_steal_ms, then steal. EXPERIMENTAL — measures whether committer→pool affinity shrinks the cross-committer pool_lock handoff on lock-bound scenarios; the reverted acct-xdwk lever 2, rebuilt for rigorous measurement.",
        &AFFINITY_SCHEME,
        0,
        1,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
    GucRegistry::define_int_guc(
        c"ledger_routed_c.affinity_steal_ms",
        c"Age (ms) after which a non-owner committer may steal a group (acct-0usf)",
        c"Only meaningful when affinity_scheme != 0. A queued commit_group whose owner committer has not claimed it within this many ms becomes claimable by any committer (age-gated steal). Lower → affinity engages less (more stealing, closer to off); higher → stronger pinning but worse tail latency if an owner is backed up. Keyed off the entry's enqueued_at_micros.",
        &AFFINITY_STEAL_MS,
        0,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        c"ledger_routed_c.router_pack_disjoint",
        c"Pack disjoint pool-components into one commit_group",
        c"Production default: on. When on, the router greedily bin-packs disjoint affinity components (no shared pool_id) into a single commit_group up to batch_size_max, amortizing per-group commit/fsync on spread/Pareto workloads where same-pool coalescing alone leaves commit_groups small. Safe by construction: disjoint pools share no row lock, so one committer takes them sequentially without cross-pool FOR UPDATE contention; packing only co-locates DISJOINT pools, so each pool still takes one pool_lock + one aggregate UPSERT — no new same-pool contention. acct-p1al measured it win-or-neutral across spread (2x), deep-zipf (+170%), mixed (neutral) and single-hot-pool (inert). Off preserves one-commit_group-per-component.",
        &ROUTER_PACK_DISJOINT,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        c"ledger_routed_c.target_database",
        c"Database the router + committer BGWorkers attach to via SPI",
        c"Set this to the PoC DB created via scripts/create-poc-v3-1-db.sh (default: poc_v3_1).",
        &TARGET_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    // ── BGWorker registration ────────────────────────────────────────
    //
    // Recovery runs ONCE at postmaster start (set_restart_time None), flips
    // COMMITTER_QUEUE.recovery_complete 0→1 so router + committers open for
    // traffic. Router + committers restart after 5 s on crash. The worker
    // BODIES are lifecycle-only shells in P3.1 (routing = P3.2, commit = P3.3).
    use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
    use std::time::Duration;
    BackgroundWorkerBuilder::new("ledger_routed_c_recovery")
        .set_function("ledger_routed_c_recovery_main")
        .set_library("ledger_routed_c")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(None)
        .enable_spi_access()
        .load();
    BackgroundWorkerBuilder::new("ledger_routed_c_router")
        .set_function("ledger_routed_c_router_main")
        .set_library("ledger_routed_c")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
    let n_committers = COMMITTER_COUNT.get().max(1) as usize;
    for i in 0..n_committers {
        BackgroundWorkerBuilder::new(&format!("ledger_routed_c_committer_{}", i))
            .set_function("ledger_routed_c_committer_main")
            .set_library("ledger_routed_c")
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
    }
}

// ── Arena observability ─────────────────────────────────────────────

#[pg_extern]
fn ledger_routed_c_arena_total_allocs() -> i64 {
    SPILLOVER_ARENA.share().allocs_total() as i64
}

#[pg_extern]
fn ledger_routed_c_arena_total_frees() -> i64 {
    SPILLOVER_ARENA.share().frees_total() as i64
}

/// Currently outstanding allocations = total_allocs − total_frees. Converges to
/// 0 at rest; a persistent non-zero value across idle periods signals a leak.
#[pg_extern]
fn ledger_routed_c_arena_outstanding() -> i64 {
    SPILLOVER_ARENA.share().outstanding_allocs() as i64
}

#[pg_extern]
fn ledger_routed_c_arena_bump_offset() -> i64 {
    SPILLOVER_ARENA.share().bump_offset_now() as i64
}

/// O(n) walk of the freelist — debug / observability only.
#[pg_extern]
fn ledger_routed_c_arena_freelist_count() -> i64 {
    SPILLOVER_ARENA.share().freelist_count() as i64
}

// ── Committer observability ─────────────────────────────────────────
//
// Cumulative since extension load (cluster lifetime). Tests measure deltas /
// run against a freshly-restarted container. The pool-lock + aggregate-upsert
// counters verify the §6.7 batching win: a hot-pool commit_group adds one each,
// where direct flavor adds one per submission.

#[pg_extern]
fn ledger_routed_c_committer_drains_total() -> i64 {
    COMMITTER_QUEUE.share().committer_drains_total.load(Relaxed) as i64
}

// [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] claim-path engagement counters.
#[pg_extern]
fn ledger_routed_c_affinity_owned_claims_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .affinity_owned_claims_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_affinity_steals_total() -> i64 {
    COMMITTER_QUEUE.share().affinity_steals_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_pool_lock_acquisitions_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_pool_lock_acquisitions_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_aggregate_upserts_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_aggregate_upserts_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_trx_committed_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_trx_committed_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_dedup_skips_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_dedup_skips_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_dropped_submissions_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_dropped_submissions_total
        .load(Relaxed) as i64
}

/// Write-phase 23505 re-drives (§6.8): one per UNIQUE violation that survived
/// pre-flight dedup. The racer is re-dedup'd out and the rest of the group
/// re-driven; an irresolvable 23505 increments this then poisons.
#[pg_extern]
fn ledger_routed_c_committer_duplicate_redrives_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_duplicate_redrives_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_tx_failures_total() -> i64 {
    COMMITTER_QUEUE.share().committer_tx_failures.load(Relaxed) as i64
}

/// commit_groups moved to the terminal `poisoned` state (§6.8): a non-retryable
/// SQL error or a deadlock that exhausted its retry budget. Their submissions
/// are lost (no trx); the CQ slot is a dead-letter at valid==4.
#[pg_extern]
fn ledger_routed_c_committer_poisoned_total() -> i64 {
    COMMITTER_QUEUE.share().committer_poisoned_total.load(Relaxed) as i64
}

/// Cumulative deadlock-driven write-phase retries (§6.8): one per re-attempt
/// after a 40P01 / 40001.
#[pg_extern]
fn ledger_routed_c_committer_deadlock_retries_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_deadlock_retries_total
        .load(Relaxed) as i64
}

/// commit_groups reclaimed from a dead committer by the router boot sweep
/// (§6.5 Phase 2): each reverted in_flight→ready entry increments this.
#[pg_extern]
fn ledger_routed_c_committer_takeover_count() -> i64 {
    COMMITTER_QUEUE.share().committer_takeover_count.load(Relaxed) as i64
}

/// Staging entries ejected back to pending (valid 3→1) by committer triage: an
/// in-progress caller re-checked after cooldown, or (acct-mvq4.30) every
/// submission of a group whose `pg_xact_status` probe failed — fail-closed, so
/// the next tick re-triages rather than committing unconfirmed work.
#[pg_extern]
fn ledger_routed_c_committer_eject_total_count() -> i64 {
    COMMITTER_QUEUE.share().eject_total_count.load(Relaxed) as i64
}

/// Whether the postmaster-startup recovery sweep has completed (§6.5). Router +
/// committers block until this is set; tests poll it before driving traffic.
#[pg_extern]
fn ledger_routed_c_recovery_complete() -> bool {
    use std::sync::atomic::Ordering::Acquire;
    COMMITTER_QUEUE.share().recovery_complete.load(Acquire) != 0
}

// ── Router observability (bench profiling) ──────────────────────────
//
// Cumulative since extension load (cluster lifetime). Sample as deltas over a
// timed routed load to derive ticks/s, groups/tick, entries-scanned/tick, and
// the window-defer rate — the inputs to locating the Path C throughput ceiling
// (acct-235v: throughput is pinned at ~110 commit-groups/s independent of
// batch_window_us, committer_count, and caller count, so the bound is the
// single router worker's group-formation loop; these counters say where).

#[pg_extern]
fn ledger_routed_c_router_ticks_total() -> i64 {
    COMMITTER_QUEUE.share().router_ticks_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_router_commit_group_count() -> i64 {
    COMMITTER_QUEUE.share().router_commit_group_count.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_router_total_submissions() -> i64 {
    COMMITTER_QUEUE.share().router_total_submissions.load(Relaxed) as i64
}

/// Staging entries inspected across all router_tick scans. Divided by ticks it
/// gives the per-tick scan cost; divided by elapsed it gives the scan rate —
/// the tell for a scan-bound (vs cadence-bound) router.
#[pg_extern]
fn ledger_routed_c_router_entries_scanned_total() -> i64 {
    COMMITTER_QUEUE.share().router_entries_scanned_total.load(Relaxed) as i64
}

/// Ticks that produced no group because the oldest candidate had not yet aged
/// past batch_window_us. High share => the window gate is throttling formation.
#[pg_extern]
fn ledger_routed_c_router_window_defers_total() -> i64 {
    COMMITTER_QUEUE.share().router_window_defers_total.load(Relaxed) as i64
}

/// Chunk-split events where the router carved a large affinity component across
/// multiple commit_groups, so one pool's FOR UPDATE handoff crosses commit_group
/// boundaries. Sampled as a delta over a timed load, this is the direct rate of
/// the cross-group provisional-cost order divergence characterized in §14.2 / A1
/// — the observability the order-divergence story otherwise had to infer.
#[pg_extern]
fn ledger_routed_c_router_cross_commit_group_for_update_waits() -> i64 {
    COMMITTER_QUEUE
        .share()
        .router_cross_commit_group_for_update_waits
        .load(Relaxed) as i64
}

/// High-water mark: the largest submission count any single CommitGroup reached
/// since cluster start. With batch_size_max held non-binding, this is the
/// time-window-only formation's worst-case group size — the tell for whether a
/// pure time gate can form an unbounded group under an arrival spike.
#[pg_extern]
fn ledger_routed_c_router_max_group_size() -> i64 {
    COMMITTER_QUEUE
        .share()
        .router_max_submission_count_per_group
        .load(Relaxed) as i64
}

/// Log2-spaced CommitGroup-size histogram as 8 comma-joined bucket counts:
/// [1],[2-3],[4-7],[8-15],[16-31],[32-63],[64-127],[128+]. Exposes the group-size
/// DISTRIBUTION (not just the mean) — the tail a balance window must bound.
#[pg_extern]
fn ledger_routed_c_router_submission_histogram() -> String {
    let q = COMMITTER_QUEUE.share();
    let mut parts = Vec::with_capacity(8);
    for b in 0..8 {
        parts.push(q.router_submission_histogram[b].load(Relaxed).to_string());
    }
    parts.join(",")
}

/// Cumulative committer apply-pipeline wall time (ns) and the count of groups it
/// covers. The ratio is mean per-group committer work; multiplied by groups/s
/// and divided by committer_count it gives committer pool utilization — the
/// proof of whether committers have spare capacity at the ceiling.
#[pg_extern]
fn ledger_routed_c_committer_pipeline_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_pipeline_ns_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_pipeline_count() -> i64 {
    COMMITTER_QUEUE.share().committer_pipeline_count.load(Relaxed) as i64
}

// Scoped pipeline-span totals (acct-0usf STEP 1). Decompose the single
// `committer_pipeline_ns_total` span so the affinity question is answered from a
// measured per-span breakdown, not inferred from throughput. Cumulative ns since
// extension load; sample as deltas over a timed routed load. Derived views:
//   commit/fsync ns ≈ txn_ns_total − pipeline_ns_total
//   prep (decode+triage+dedup+line-decode) ns ≈ pipeline_ns_total − (pool_lock + hydrate + apply)
// pool_lock_ns_total is the cross-committer hot-pool FOR UPDATE handoff span — the
// quantity affinity is hypothesized to shrink.

#[pg_extern]
fn ledger_routed_c_committer_pool_lock_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_pool_lock_ns_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_hydrate_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_hydrate_ns_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_apply_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_apply_ns_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_txn_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_txn_ns_total.load(Relaxed) as i64
}

// Prep-span refold (acct-e95d). Split the "prep" residual
// (pipeline − pool_lock − hydrate − apply) into its components so the prep floor
// is targeted from a measured breakdown. decode = payload + line decode (Rust,
// per-trx/line); xact = pg_xact_status triage SPI (per group); dedup = dedup
// SELECT against trx (per group). prep − (decode + xact + dedup) = staging-index
// read + subtx/retry framing.

#[pg_extern]
fn ledger_routed_c_committer_decode_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_decode_ns_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_xact_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_xact_ns_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_c_committer_dedup_ns_total() -> i64 {
    COMMITTER_QUEUE.share().committer_dedup_ns_total.load(Relaxed) as i64
}

// ── Smoke entry point ───────────────────────────────────────────────

#[pg_extern]
fn ledger_routed_c_hello() -> String {
    format!(
        "ledger_routed_c 0.0.1 — Path C routed: shmem staging={} committer={} arena_mb={} target_db={}",
        LEDGER_V3_STAGING_QUEUE_SIZE,
        LEDGER_V3_COMMITTER_QUEUE_SIZE,
        LEDGER_V3_SPILLOVER_ARENA_MB,
        target_database_str(),
    )
}
