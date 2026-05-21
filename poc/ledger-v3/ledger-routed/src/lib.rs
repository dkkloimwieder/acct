//! ledger-routed: pgrx 0.18 extension for ledger-v3 Path B.
//!
//! Authoritative spec: `poc/design_research/design-v3.md` §5.
//!
//! Caller invokes `ledger_enqueue_trx(trx_type, source_id, posted_at,
//! lines)` inside their own user-tx (or as a standalone call). The
//! function pushes a descriptor into the shmem staging queue and
//! returns. A router BGWorker scans the staging window, groups
//! submissions by pool-overlap into commit_groups via union-find, and
//! dispatches to a committer BGWorker pool. Each committer claims a
//! commit_group, hydrates a snapshot, runs `ledger_core::plan_apply`
//! per submission (with pristine-snapshot replay on per-submission
//! failure), bulk-writes trx/trx_line/pool_state/posting_line, and
//! COMMITs. One fsync per commit_group; amortizes by N.
//!
//! Module roadmap:
//! - `shmem.rs`        — acct-29a1 (this commit; StagingQueue +
//!                       CommitterQueue + arena structs + statics)
//! - `identity.rs`     — acct-29a1 (this commit; CommitterIdentitySlot
//!                       operations)
//! - `arena.rs`        — acct-17p5 (bump + LIFO freelist)
//! - `payload.rs`      — acct-damm (PocV3Submission + PocV3Line
//!                       serde_json encoding)
//! - `enqueue.rs`      — acct-mgb7 (ledger_enqueue_trx pg_extern)
//! - `router.rs`       — acct-zedi (window scan + union-find + commit_group emit)
//! - `committer.rs`    — acct-usn2 (claim + hydrate + pristine-replay +
//!                       bulk-write + COMMIT)
//! - `bulk_write.rs`   — acct-me64 (UNNEST helpers, same SQL shape as
//!                       ledger-direct)
//! - `cleanup.rs`      — acct-qsga (three-case CAS)
//! - `recovery.rs`     — acct-r2ud (router-orphan boot sweep)
//! - `stats.rs`        — observability pg_externs
//!
//! BGWorker registration (`BackgroundWorkerBuilder::new(...).load()`)
//! lands when the router + committer modules land (acct-zedi /
//! acct-usn2). `_PG_init` here only wires shmem + GUCs.

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, pg_shmem_init};
use std::ffi::CString;

pgrx::pg_module_magic!();

pub(crate) mod arena;
pub(crate) mod identity;
pub(crate) mod payload;
pub(crate) mod shmem;

use shmem::{
    COMMITTER_QUEUE, LEDGER_V3_COMMITTER_QUEUE_SIZE, LEDGER_V3_SPILLOVER_ARENA_MB,
    LEDGER_V3_STAGING_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE,
};

// ── GUCs ────────────────────────────────────────────────────────────
//
// Plan §E retained list: 9 operational + 3 shmem-size + 1 target_database.
// Dropped from v21: persistent_staging, persistent_staging_gc_retention_hours,
// status_insert_mode, skip_wip_locks, drain_batch_size,
// router_starvation_threshold_ticks (starvation backstop deferred).

static STAGING_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(16384);
static COMMITTER_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(2048);
static SPILLOVER_ARENA_MB: GucSetting<i32> = GucSetting::<i32>::new(128);
static QUEUE_FULL_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);
static COMMITTER_LEASE_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static ROUTER_WINDOW_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1000);
static BATCH_SIZE_MAX: GucSetting<i32> = GucSetting::<i32>::new(50);
static BATCH_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
static SNAPSHOT_LAYER_LIMIT_PER_POOL: GucSetting<i32> = GucSetting::<i32>::new(1000);
static MAX_EJECT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static CALLER_TX_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static COMMITTER_COUNT: GucSetting<i32> = GucSetting::<i32>::new(4);
static TARGET_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"poc_v3"));

#[allow(dead_code)]
pub(crate) fn target_database_str() -> String {
    TARGET_DATABASE
        .get()
        .as_ref()
        .map(|c| c.to_string_lossy().to_string())
        .unwrap_or_else(|| "poc_v3".to_string())
}

#[allow(dead_code)]
pub(crate) fn queue_full_timeout_ms_now() -> i32 {
    QUEUE_FULL_TIMEOUT_MS.get()
}
#[allow(dead_code)]
pub(crate) fn committer_lease_ms_now() -> i32 {
    COMMITTER_LEASE_MS.get()
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
pub(crate) fn snapshot_layer_limit_per_pool_now() -> i32 {
    SNAPSHOT_LAYER_LIMIT_PER_POOL.get()
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

// ── _PG_init ────────────────────────────────────────────────────────

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(STAGING_QUEUE);
    pg_shmem_init!(COMMITTER_QUEUE);
    pg_shmem_init!(SPILLOVER_ARENA);

    GucRegistry::define_int_guc(
        c"ledger_routed.staging_queue_size",
        c"Staging queue entry capacity (compile-time)",
        c"Number of slots in the StagingQueue ring. Compile-time constant (16384); the GUC documents the surface and bounds the range. Resizing requires recompile.",
        &STAGING_QUEUE_SIZE,
        1024,
        262_144,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.committer_queue_size",
        c"Committer queue entry capacity (compile-time)",
        c"Number of slots in the CommitterQueue ring. Compile-time constant (2048).",
        &COMMITTER_QUEUE_SIZE,
        256,
        16_384,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.spillover_arena_mb",
        c"Spillover arena size in MB",
        c"Byte arena holding payloads and pool-key arrays. Compile-time constant (128 MB).",
        &SPILLOVER_ARENA_MB,
        16,
        1024,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.queue_full_timeout_ms",
        c"Caller backpressure timeout (staging queue full)",
        c"When the staging queue is full, callers block on a condition variable up to this duration before raising ERRCODE_INSUFFICIENT_RESOURCES.",
        &QUEUE_FULL_TIMEOUT_MS,
        100,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.committer_lease_ms",
        c"Committer lease duration (orphan-recovery threshold)",
        c"A committer's CAS-acquired lease stays valid for this long before a contender may attempt takeover (after pid_alive verification).",
        &COMMITTER_LEASE_MS,
        10,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.router_window_size",
        c"Greedy router scan window (entries per tick)",
        c"How many pending staging entries the router considers per tick when packing the next commit_group.",
        &ROUTER_WINDOW_SIZE,
        100,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.batch_size_max",
        c"Maximum submissions per commit_group",
        c"Hard cap on submissions packed into a single commit_group. Larger batches amortize sub-tx + WAL costs; smaller batches keep per-batch wall-time below committer_lease_ms.",
        &BATCH_SIZE_MAX,
        1,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.batch_window_us",
        c"Router batch coalesce window in microseconds",
        c"How long the router waits to coalesce submissions before emitting a commit_group. Trades latency for batch size. 0 disables the gate.",
        &BATCH_WINDOW_US,
        0,
        5_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.snapshot_layer_limit_per_pool",
        c"Bounded snapshot layer cap per pool",
        c"Maximum layers hydrated per pool during committer snapshot read. Bounds memory and the FIFO walk's worst-case.",
        &SNAPSHOT_LAYER_LIMIT_PER_POOL,
        100,
        100_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.max_eject_count",
        c"Caller-tx eject loop bound",
        c"Maximum times a single submission may be ejected (committer observes caller user-tx in_progress) before being marked failed.",
        &MAX_EJECT_COUNT,
        100,
        1_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.caller_tx_timeout_ms",
        c"Caller user-tx total timeout",
        c"Once now - enqueued_at_micros exceeds this threshold, ejected submissions are marked failed regardless of remaining eject budget.",
        &CALLER_TX_TIMEOUT_MS,
        1000,
        3_600_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"ledger_routed.committer_count",
        c"Number of committer BGWorkers",
        c"Pool of committer BGWorkers competing for commit_group ownership via CAS election.",
        &COMMITTER_COUNT,
        1,
        64,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        c"ledger_routed.target_database",
        c"Database the router + committer BGWorkers attach to via SPI",
        c"Set this to the PoC DB created via scripts/create-poc-v3-db.sh (default: poc_v3).",
        &TARGET_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
}

// ── Arena observability ─────────────────────────────────────────────
//
// Slot-lifecycle leak audit needs visibility into the spillover-arena
// allocator state. All accessors are atomic loads; no locks held
// (PgLwLock.share() takes the read side; the atomic reads inside are
// Relaxed, sequenced by the LWLock).

#[pg_extern]
fn ledger_routed_arena_total_allocs() -> i64 {
    SPILLOVER_ARENA.share().allocs_total() as i64
}

#[pg_extern]
fn ledger_routed_arena_total_frees() -> i64 {
    SPILLOVER_ARENA.share().frees_total() as i64
}

/// Currently outstanding allocations = total_allocs − total_frees.
/// Converges to 0 at rest; a persistent non-zero value across idle
/// periods signals an arena leak.
#[pg_extern]
fn ledger_routed_arena_outstanding() -> i64 {
    SPILLOVER_ARENA.share().outstanding_allocs() as i64
}

#[pg_extern]
fn ledger_routed_arena_bump_offset() -> i64 {
    SPILLOVER_ARENA.share().bump_offset_now() as i64
}

/// O(n) walk of the freelist — debug / observability only.
#[pg_extern]
fn ledger_routed_arena_freelist_count() -> i64 {
    SPILLOVER_ARENA.share().freelist_count() as i64
}

// ── Smoke entry point ───────────────────────────────────────────────

#[pg_extern]
fn ledger_routed_hello() -> String {
    format!(
        "ledger_routed 0.0.1 (acct-29a1 scaffolding) — Path B: shmem staging={} committer={} arena_mb={} target_db={}",
        LEDGER_V3_STAGING_QUEUE_SIZE,
        LEDGER_V3_COMMITTER_QUEUE_SIZE,
        LEDGER_V3_SPILLOVER_ARENA_MB,
        target_database_str(),
    )
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn hello_pg_extern_reachable() {
        let banner = ledger_routed_hello();
        assert!(banner.starts_with("ledger_routed 0.0.1"));
        assert!(banner.contains("staging=16384"));
        assert!(banner.contains("arena_mb=128"));
    }

    #[pg_test]
    fn shmem_regions_visible() {
        // Reaching into the LwLock-guarded regions proves the
        // pg_shmem_init wiring succeeded and the structs are
        // accessible. Default fields are zero-init from
        // PGRXSharedMemory.
        use std::sync::atomic::Ordering::Relaxed;
        let staging = STAGING_QUEUE.share();
        let _head: u32 = staging.head.load(Relaxed);
        drop(staging);
        let committer = COMMITTER_QUEUE.share();
        let _superbatch_id: u64 = committer.next_superbatch_id.load(Relaxed);
        drop(committer);
        let arena = SPILLOVER_ARENA.share();
        let _bump: u32 = arena.bump_offset.load(Relaxed);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries='ledger_routed'",
        ]
    }
}
