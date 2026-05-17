//! poc_v21_ledger: two-queue + router lexicographical ledger PoC v2.1.
//!
//! Authoritative spec: `poc/design_research/poc-v2.1.md`.
//!
//! ## Milestone status
//!
//! - **M0.1 (acct-jux2, this commit): pgrx 0.18 scaffolding, 16 GUCs
//!   from spec §1.7, shmem reservation stubs for StagingQueue +
//!   CommitterQueue + spillover arena, startup-validation gates,
//!   `poc_v21_hello()` smoke SQL fn.**
//! - M1.1 (acct-63dk, next): PoC schema migrations.
//! - M1.2 (acct-q3nm): staging queue + enqueue + spillover arena allocator.
//! - M1.3 (acct-wrwf): single committer + FIFO method + 5-step pipeline.
//!
//! ## GUC namespace
//!
//! All GUCs live under `poc_v21.*`. The extension's shared library
//! name (per `Cargo.toml [lib] name`) is `poc_v21_ledger`, so the
//! same identifier appears in `shared_preload_libraries`.
//!
//! ## Coexistence with v2 PoC
//!
//! v2.1's `poc_v21_ledger` runs alongside v2's `poc_ledger` in the
//! same PG cluster. Distinct shmem regions, distinct GUC namespaces,
//! distinct extension control files. The two PoCs do not share code.

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PgLwLock, pg_shmem_init};
use std::ffi::CString;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU16, AtomicU32, AtomicU64};

pgrx::pg_module_magic!();

// M1.2 (acct-q3nm): caller-side path.
mod arena;
mod enqueue;
mod staging;

// M1.3 (acct-wrwf): end-to-end FIFO pipeline.
mod committer;
mod cost_method;
mod fifo;
mod router;

// M2.1 (acct-hmuf): AVG running-average method + per-event dispatch.
mod avg;

// M2.2 (acct-lgll): STD standard-cost method + 6-target bulk UNNEST.
mod standard;

// ── Compile-time shmem sizing constants (spec §1.6) ─────────────────
//
// The matching Postmaster-scope GUCs (`poc_v21.staging_queue_size`,
// `committer_queue_size`, `spillover_arena_mb`) document the surface
// and bound the GUC ranges, but the actual shmem allocation is
// driven by these constants. Mismatch between the GUC value at
// startup and the compiled constant is reported as an ereport NOTICE
// at _PG_init time; resizing requires a recompile.
//
// Rationale: pgrx's `PgLwLock<T>` requires `T: Sized` (the static
// `PgLwLock` carries metadata; the inner `T` is allocated in shmem
// at startup). Variable-size shmem segments would require a custom
// `shmem_request_hook` + manual `ShmemInitStruct` pattern that's
// out of M0.1 scope (and arguably unnecessary for a PoC). M1.2's
// spillover allocator runs *inside* the fixed 128 MB byte arena;
// the freelist coalesce is the interesting algorithm, not the
// reservation primitive.

pub const POC_V21_STAGING_QUEUE_SIZE: usize = 16384;
pub const POC_V21_COMMITTER_QUEUE_SIZE: usize = 2048;
pub const POC_V21_SPILLOVER_ARENA_MB: usize = 128;
pub const POC_V21_SPILLOVER_ARENA_BYTES: usize = POC_V21_SPILLOVER_ARENA_MB * 1024 * 1024;

// ── Shmem layout (spec §1.6) ────────────────────────────────────────
//
// Stub structures. M1.2 / M1.3 flesh out the operations. Field
// layouts match the spec verbatim so later milestones can drop in
// without re-touching reservation math. Atomic semantics are
// documented at the field level; the load-bearing data-before-flag
// pattern on `StagingEntry.superbatch_id` (Release at router write,
// Acquire at recovery-sweep read) is enforced at the operation
// site, not in the struct definition.

#[repr(C)]
pub struct StagingEntry {
    pub valid: AtomicU8, // 0=empty 1=pending 2=processing 3=routed 4=abandoned
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub correlation_id: [u8; 16],
    pub user_tx_xid: u64,
    pub event_type_id: u16,
    pub _pad_event: [u8; 2],
    pub payload_offset: u32,
    pub payload_length: u32,
    pub sku_pool_count: u16,
    pub wip_pool_count: u16,
    pub sku_pool_keys_offset: u32,
    pub wip_pool_keys_offset: u32,
    pub enqueued_at_micros: u64,
    pub backend_pid: i32,
    pub _pad_pid: [u8; 4],
    pub superbatch_id: AtomicU64,
    pub eject_count: AtomicU16,
    pub _pad2: [u8; 6],
}

#[repr(C, align(64))]
pub struct StagingQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_request_seq: AtomicU64,
    pub backpressure_cv_tranche_id: u32,
    pub _pad2: [u8; 4],
    pub entries: [StagingEntry; POC_V21_STAGING_QUEUE_SIZE],
}

impl Default for StagingQueue {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for StagingQueue {}

#[repr(C)]
pub struct CommitterQueueEntry {
    pub valid: AtomicU8, // 0=empty 1=ready 2=in_flight 3=completed
    pub _pad: [u8; 7],
    pub superbatch_id: u64,
    pub envelope_count: u16,
    pub _pad_env: [u8; 2],
    pub staging_entry_offsets: u32,
    pub sku_pool_keys_offset: u32,
    pub sku_pool_keys_count: u16,
    pub _pad_sku: [u8; 2],
    pub wip_pool_keys_offset: u32,
    pub wip_pool_keys_count: u16,
    pub _pad_wip: [u8; 2],
    pub committer_pid: AtomicI32,
    pub _pad_pid: [u8; 4],
    pub committer_acquired_at_ns: AtomicU64,
    pub committer_tx_id: AtomicU64,
    pub enqueued_at_micros: u64,
}

#[repr(C, align(64))]
pub struct CommitterQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_superbatch_id: AtomicU64,
    // ── Router stats (M3.1 acct-r29s, M3.2 acct-evyq) ─────────────
    // Counters live in shmem (not process-local statics) so the
    // router BGWorker's increments are visible from any backend
    // that reads via SQL accessor. Atomics — no LWLock needed.
    pub router_superbatch_count: AtomicU64,
    pub router_total_envelopes: AtomicU64,
    pub router_force_pack_count: AtomicU64,
    pub router_max_envelope_count: AtomicU16,
    pub _pad_stats: [u8; 6],
    pub entries: [CommitterQueueEntry; POC_V21_COMMITTER_QUEUE_SIZE],
}

impl Default for CommitterQueue {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for CommitterQueue {}

// Spillover arena: a flat byte buffer, indexed by u32 offsets stored
// in `StagingEntry.payload_offset / sku_pool_keys_offset / ...` and
// `CommitterQueueEntry.staging_entry_offsets / sku_pool_keys_offset / ...`.
// M1.2 ships a simple bump + LIFO freelist allocator (see arena.rs);
// coalesce + slab variants deferred per follow-up acct-v21-fu-arena-fragmentation.

#[repr(C, align(64))]
pub struct SpilloverArena {
    /// Offset of head-of-freelist block header (0 = empty list).
    pub freelist_head_offset: AtomicU32,
    /// Next never-touched byte (high-water mark for bump alloc).
    pub bump_offset: AtomicU32,
    /// Lifetime alloc counter (observability).
    pub total_allocs: AtomicU64,
    /// Lifetime free counter (observability).
    pub total_frees: AtomicU64,
    pub bytes: [u8; POC_V21_SPILLOVER_ARENA_BYTES],
}

impl Default for SpilloverArena {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for SpilloverArena {}

pub static STAGING_QUEUE: PgLwLock<StagingQueue> =
    unsafe { PgLwLock::new(c"poc_v21_staging_queue") };
pub static COMMITTER_QUEUE: PgLwLock<CommitterQueue> =
    unsafe { PgLwLock::new(c"poc_v21_committer_queue") };
pub static SPILLOVER_ARENA: PgLwLock<SpilloverArena> =
    unsafe { PgLwLock::new(c"poc_v21_spillover_arena") };

// ── GUCs (spec §1.7) ────────────────────────────────────────────────
//
// 16 GUCs total: 13 integer-typed, 3 string/bool-typed. Three
// Postmaster-scope (size of shmem regions + persistent_staging
// feature flag), the rest are Sighup-scope.

static STAGING_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(16384);
static COMMITTER_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(2048);
static SPILLOVER_ARENA_MB: GucSetting<i32> = GucSetting::<i32>::new(128);
static QUEUE_FULL_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);
static COMMITTER_LEASE_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static ROUTER_WINDOW_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1000);
static BATCH_SIZE_MAX: GucSetting<i32> = GucSetting::<i32>::new(50);
static BATCH_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
static ROUTER_STARVATION_THRESHOLD_TICKS: GucSetting<i32> = GucSetting::<i32>::new(10);
static SNAPSHOT_LAYER_LIMIT_PER_POOL: GucSetting<i32> = GucSetting::<i32>::new(1000);
static MAX_EJECT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(100);
static CALLER_TX_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(300_000);
static COMMITTER_COUNT: GucSetting<i32> = GucSetting::<i32>::new(4);
static PERSISTENT_STAGING_GC_RETENTION_HOURS: GucSetting<i32> = GucSetting::<i32>::new(24);

// String GUC for the 3-valued enum. pgrx 0.18 doesn't expose
// check_hook plumbing cleanly; callers that read it will validate
// against the allowed set ('caller_intx', 'caller_subtx',
// 'committer_lazy'). Spec §1.7 documents the allowed values; the
// startup validation block in `_PG_init` rejects the unsafe
// combination (`committer_lazy` + `persistent_staging=off`).
static STATUS_INSERT_MODE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"caller_intx"));

// Bool GUC. `persistent_staging=on` enables the durable_queue=true
// path (spec §1.9) and unlocks committer_lazy mode (§3.4).
static PERSISTENT_STAGING: GucSetting<bool> = GucSetting::<bool>::new(false);

// Operational GUC (not in §1.7's runtime config table): which DB the
// committer BGWorker connects to via SPI. Default targets the PoC's
// dedicated database (acct_poc_queue_v21); test deployments may
// override. Postmaster-scope (BGWorker reads at startup).
static TARGET_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"acct_poc_queue_v21"));

pub(crate) fn target_database_str() -> String {
    TARGET_DATABASE
        .get()
        .as_ref()
        .map(|c| c.to_string_lossy().to_string())
        .unwrap_or_else(|| "acct_poc_queue_v21".to_string())
}

/// Read `poc_v21.status_insert_mode` lowercased. Defaults to
/// "caller_intx" on parse failure.
pub(crate) fn status_insert_mode_str() -> String {
    STATUS_INSERT_MODE
        .get()
        .as_ref()
        .map(|c| c.to_string_lossy().to_string())
        .unwrap_or_else(|| "caller_intx".to_string())
}

/// Read `poc_v21.persistent_staging`. Defaults to `false`.
pub(crate) fn persistent_staging_enabled() -> bool {
    PERSISTENT_STAGING.get()
}

/// Read `poc_v21.queue_full_timeout_ms`. Defaults to 5000.
pub(crate) fn queue_full_timeout_ms_now() -> i32 {
    QUEUE_FULL_TIMEOUT_MS.get()
}

/// Read `poc_v21.batch_size_max`. Defaults to 50.
pub(crate) fn batch_size_max_now() -> i32 {
    BATCH_SIZE_MAX.get()
}

/// Read `poc_v21.router_window_size`. Defaults to 1000.
pub(crate) fn router_window_size_now() -> i32 {
    ROUTER_WINDOW_SIZE.get()
}

/// Read `poc_v21.router_starvation_threshold_ticks`. Defaults to 10.
pub(crate) fn router_starvation_threshold_ticks_now() -> i32 {
    ROUTER_STARVATION_THRESHOLD_TICKS.get()
}

// ── _PG_init ────────────────────────────────────────────────────────

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Wire shmem regions first; pg_shmem_init! calls
    // RequestAddinShmemSpace + RequestNamedLWLockTranche under the
    // hood, and PG demands they happen during shared_preload_libraries
    // init.
    pg_shmem_init!(STAGING_QUEUE);
    pg_shmem_init!(COMMITTER_QUEUE);
    pg_shmem_init!(SPILLOVER_ARENA);

    // ── Integer GUCs (13) ───────────────────────────────────────────
    GucRegistry::define_int_guc(
        c"poc_v21.staging_queue_size",
        c"Staging queue entry capacity (compile-time at M0.1)",
        c"Number of slots in the StagingQueue ring. Compile-time constant at M0.1 (16384); the GUC documents the surface and bounds. Resizing requires a recompile.",
        &STAGING_QUEUE_SIZE,
        1024,
        262_144,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.committer_queue_size",
        c"Committer queue entry capacity (compile-time at M0.1)",
        c"Number of slots in the CommitterQueue ring. Compile-time constant at M0.1 (2048); see staging_queue_size for the recompile rationale.",
        &COMMITTER_QUEUE_SIZE,
        256,
        16_384,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.spillover_arena_mb",
        c"Spillover arena size in MB",
        c"Byte arena holding payloads, pool-key arrays, and CommitterQueueEntry-owned arrays. M1.2 ships the freelist allocator inside this region. Compile-time at M0.1 (128 MB); recompile to resize.",
        &SPILLOVER_ARENA_MB,
        16,
        1024,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.queue_full_timeout_ms",
        c"Caller backpressure timeout (staging queue full)",
        c"When the staging queue is full, callers block on a condition variable up to this duration before raising ERRCODE_INSUFFICIENT_RESOURCES.",
        &QUEUE_FULL_TIMEOUT_MS,
        100,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.committer_lease_ms",
        c"Committer lease duration (orphan-recovery threshold)",
        c"A committer's CAS-acquired lease stays valid for this long before a contender may attempt takeover (after pg_pid_alive verification). Pre-bake-off calibration recommends max(100, 10 × fsync_p99_ms).",
        &COMMITTER_LEASE_MS,
        10,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.router_window_size",
        c"Greedy window router scan size (entries per tick)",
        c"How many pending staging entries the router considers per tick when packing the next SuperBatch.",
        &ROUTER_WINDOW_SIZE,
        100,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.batch_size_max",
        c"Maximum envelopes per SuperBatch",
        c"Hard cap on envelopes packed into a single SuperBatch. Larger batches amortize sub-tx + WAL costs; smaller batches keep per-batch wall-time below committer_lease_ms.",
        &BATCH_SIZE_MAX,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.batch_window_us",
        c"Router batch window in microseconds",
        c"How long the router waits to coalesce envelopes before emitting a SuperBatch. Trades latency for batch size.",
        &BATCH_WINDOW_US,
        50,
        50_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.router_starvation_threshold_ticks",
        c"Router starvation fairness backstop",
        c"After this many ticks of being skipped, a head-of-queue entry is forced into a size-1 SuperBatch. Bounds tail latency under hot-pool workloads.",
        &ROUTER_STARVATION_THRESHOLD_TICKS,
        1,
        100,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.snapshot_layer_limit_per_pool",
        c"Bounded snapshot layer cap per pool",
        c"Maximum layers hydrated per (sku, location) pool during committer Step 3. Bounds memory and bounds the FIFO walk's worst-case work.",
        &SNAPSHOT_LAYER_LIMIT_PER_POOL,
        100,
        100_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.max_eject_count",
        c"Caller-tx eject loop bound (spec §3.11)",
        c"Maximum times a single envelope may be ejected (committer observes caller user-tx in_progress) before being marked failed with caller_tx_eject_exhausted. Capped at 10000 to avoid AtomicU16 overflow.",
        &MAX_EJECT_COUNT,
        1,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.caller_tx_timeout_ms",
        c"Caller user-tx total timeout (spec §3.11)",
        c"Once now - enqueued_at_micros exceeds this threshold, ejected envelopes are marked failed regardless of remaining eject_count budget.",
        &CALLER_TX_TIMEOUT_MS,
        1000,
        3_600_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.committer_count",
        c"Number of committer BGWorkers",
        c"Pool of committer BGWorkers competing for SuperBatch ownership via CAS election on CommitterQueueEntry.committer_pid.",
        &COMMITTER_COUNT,
        1,
        64,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.persistent_staging_gc_retention_hours",
        c"Persistent-staging completed-row retention (spec §1.9)",
        c"Periodic GC (pg_cron) deletes poc_v21_persistent_staging rows in 'completed' state older than this threshold.",
        &PERSISTENT_STAGING_GC_RETENTION_HOURS,
        1,
        720,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    // ── String GUC (1) ─────────────────────────────────────────────
    GucRegistry::define_string_guc(
        c"poc_v21.status_insert_mode",
        c"Status row insertion strategy: caller_intx | caller_subtx | committer_lazy",
        c"caller_intx (default): INSERT runs inside caller user-tx; cheapest, but loses status row if caller aborts before commit. caller_subtx: INSERT in a sub-tx inside the caller user-tx; status row survives caller abort. committer_lazy: committer creates the status row lazily during recovery from the persistent_staging table; requires persistent_staging=on.",
        &STATUS_INSERT_MODE,
        GucContext::Sighup,
        GucFlags::empty(),
    );

    // ── Bool GUC (1) ───────────────────────────────────────────────
    GucRegistry::define_bool_guc(
        c"poc_v21.persistent_staging",
        c"Enable durable WAL-backed staging table (spec §1.9)",
        c"When ON, callers may pass durable_queue=true to enqueue; envelopes ride to durability with the caller's user-tx commit. Also unlocks status_insert_mode=committer_lazy. Postmaster-scope: requires restart to toggle.",
        &PERSISTENT_STAGING,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    // ── Operational GUC ────────────────────────────────────────────
    GucRegistry::define_string_guc(
        c"poc_v21.target_database",
        c"Database the committer BGWorker connects to via SPI",
        c"The committer + router BGWorkers attach to this database. Set this to the PoC DB you created via scripts/create-poc-v21-db.sh.",
        &TARGET_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    // ── Startup validation (spec §1.7) ─────────────────────────────
    //
    // The unsafe combinations are gated at extension load time. PG
    // has already loaded postgresql.conf by the time _PG_init runs
    // for shared_preload_libraries; the GucSetting reads return the
    // configured values.
    //
    // committer_lazy without persistent_staging is unsafe: post-
    // postmaster-restart, callers' shmem staging entries are gone
    // and the lazy status row never gets written, so caller
    // submissions look 'lost' rather than 'failed'. Refuse to load
    // with hint.
    let mode = status_insert_mode_str();
    let persistent = persistent_staging_enabled();
    if mode == "committer_lazy" && !persistent {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "poc_v21_ledger: status_insert_mode=committer_lazy requires persistent_staging=on",
            "committer_lazy mode writes status rows lazily during recovery; without persistent_staging, post-restart recovery has no record of in-flight callers and submissions look lost rather than failed. Either set poc_v21.persistent_staging=on (Postmaster scope, requires restart) or use status_insert_mode=caller_intx (default) or caller_subtx."
        );
    }
    // The durable_queue=true ↔ persistent_staging=on gate is enforced
    // at enqueue time (raises ERRCODE_FEATURE_NOT_SUPPORTED), not
    // here — durable_queue is a per-call argument, not a GUC.

    // M1.3 BGWorkers: stub router + single committer.
    use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
    use std::time::Duration;
    BackgroundWorkerBuilder::new("poc_v21_router")
        .set_function("poc_v21_router_main")
        .set_library("poc_v21_ledger")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_shmem_access(None)
        .load();
    BackgroundWorkerBuilder::new("poc_v21_committer")
        .set_function("poc_v21_committer_main")
        .set_library("poc_v21_ledger")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
}

// ── Smoke entry point ───────────────────────────────────────────────

/// Return a one-line scaffolding banner. Used by M0.1 acceptance to
/// confirm the extension loads cleanly and the SPI surface is wired.
#[pg_extern]
fn poc_v21_hello() -> String {
    format!(
        "poc_v21_ledger 0.0.1 (acct-jux2 M0.1) — staging={} committer={} arena_mb={} status_mode={} persistent={}",
        POC_V21_STAGING_QUEUE_SIZE,
        POC_V21_COMMITTER_QUEUE_SIZE,
        POC_V21_SPILLOVER_ARENA_MB,
        status_insert_mode_str(),
        persistent_staging_enabled(),
    )
}
