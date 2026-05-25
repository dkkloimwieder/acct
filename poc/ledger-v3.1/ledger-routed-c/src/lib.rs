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
//! **P3.1 scope (`acct-2ttr.4`)**: shmem layout (staging + committer queues +
//! spillover arena + committer identity registry, §6.2), the arena allocator, the
//! payload codec, `ledger_enqueue_trx_c`, GUCs, and BGWorker registration with
//! lifecycle-only worker shells. Router logic is P3.2, the committer pipeline P3.3,
//! recovery/error handling P3.4.

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, pg_shmem_init};
use std::ffi::CString;

::pgrx::pg_module_magic!();

pub(crate) mod arena;
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
static COMMITTER_LEASE_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static ROUTER_WINDOW_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1000);
static BATCH_SIZE_MAX: GucSetting<i32> = GucSetting::<i32>::new(50);
static BATCH_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
static MAX_EJECT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static CALLER_TX_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static COMMITTER_COUNT: GucSetting<i32> = GucSetting::<i32>::new(4);
static EJECT_COOLDOWN_MS: GucSetting<i32> = GucSetting::<i32>::new(10);
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
        c"ledger_routed_c.committer_lease_ms",
        c"Committer lease duration (orphan-recovery threshold)",
        c"A committer's CAS-acquired lease stays valid for this long before a contender may attempt takeover (after pid_alive verification).",
        &COMMITTER_LEASE_MS,
        10,
        10_000,
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn hello_pg_extern_reachable() {
        let banner = ledger_routed_c_hello();
        assert!(banner.starts_with("ledger_routed_c 0.0.1"));
        assert!(banner.contains("staging=16384"));
        assert!(banner.contains("arena_mb=128"));
    }

    #[pg_test]
    fn shmem_regions_visible() {
        use std::sync::atomic::Ordering::Relaxed;
        let staging = STAGING_QUEUE.share();
        let _head: u32 = staging.head.load(Relaxed);
        drop(staging);
        let committer = COMMITTER_QUEUE.share();
        let _commit_group_id: u64 = committer.next_commit_group_id.load(Relaxed);
        drop(committer);
        let arena = SPILLOVER_ARENA.share();
        let _bump: u32 = arena.bump_offset.load(Relaxed);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries='ledger_routed_c'"]
    }
}
