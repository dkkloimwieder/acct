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

use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PgLwLock, pg_shmem_init};
use std::ffi::CString;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU16, AtomicU32, AtomicU64};

pgrx::pg_module_magic!();

/// Current PG timestamp in microseconds since 2000-01-01, clamped at
/// zero. `pg_sys::GetCurrentTimestamp()` returns `TimestampTz` (i64);
/// a negative pre-2000 value cast `as u64` silently wraps to ~2^63
/// and corrupts every downstream arithmetic (lease windows, stale
/// staging detection, etc.). Clamping with `.max(0)` keeps the
/// invariant `now_us() >= 0` so saturating multiplication with 1000
/// for nanoseconds remains safe.
///
/// Acct-gx1z.1.4 (B4): replaces the 16 inline
/// `unsafe { pg_sys::GetCurrentTimestamp() as u64 ... }` sites that
/// previously had this bug.
#[inline]
pub(crate) fn now_us() -> u64 {
    let t = unsafe { pg_sys::GetCurrentTimestamp() };
    t.max(0) as u64
}

/// Current PG timestamp in nanoseconds since 2000-01-01, clamped.
/// Saturates at `u64::MAX` rather than wrapping (impossible in
/// practice for the next ~292 years given GetCurrentTimestamp's
/// microsecond resolution, but cheap insurance).
#[inline]
pub(crate) fn now_ns() -> u64 {
    now_us().saturating_mul(1000)
}

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

// M5d.1 (acct-y0bp): postmaster-startup recovery worker (non-durable path).
mod recovery;

// M7.1 (acct-byue): per-queue + per-method + router observability surface.
mod stats;

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
    pub eject_count: AtomicU32,
    pub _pad2: [u8; 4],
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
    // ── M5c.1 (acct-r0aa): backpressure signal mechanism ───────────
    // `backpressure_cv` is PG's ConditionVariable — zero-initialized
    // at shmem allocation; `ConditionVariableInit` is idempotent and
    // not required because both `slock_t` and `proclist_head` are
    // zero-init-valid. Waiters call ConditionVariableTimedSleep;
    // committer cleanup + router rollback call ConditionVariableBroadcast
    // when a slot becomes empty.
    //
    // `free_slot_wake_count` is an observability counter — incremented
    // each time a signaler broadcasts. Tests assert it grew during a
    // drain to confirm the signal mechanism actually fired.
    pub free_slot_wake_count: AtomicU64,
    /// 3-state CAS gate for lazy ConditionVariableInit. 0=uninit,
    /// 1=init-in-progress, 2=initialized. The CV's `wakeup` field
    /// (proclist_head) is NOT valid zero-init — its sentinels are
    /// INVALID_PROC_NUMBER (-1 in PG18), not 0. Without explicit
    /// ConditionVariableInit, Broadcast walks proclist starting from
    /// head=0 which interprets as a real PGPROC slot → corrupt state /
    /// hung backends. ensure_backpressure_cv_initialized() does the
    /// CAS-gated init at first use.
    pub backpressure_cv_initialized: AtomicU8,
    pub _pad_cv_init: [u8; 7],
    pub backpressure_cv: pg_sys::ConditionVariable,
    pub _pad_cv: [u8; 8],
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
    /// Owning committer's identity-slot index in `CommitterQueue.identity_slots`.
    /// Only meaningful when `committer_bgw_generation > 0`; generation == 0 is
    /// the "unclaimed" sentinel (matches shmem zero-init).
    pub committer_bgw_slot: AtomicU32,
    /// Owning committer's slot generation, captured at claim time. Compared
    /// against `identity_slots[committer_bgw_slot].generation` to detect a
    /// recycled slot (PID-recycling-safe per spec §1.6).
    pub committer_bgw_generation: AtomicU32,
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
    // ── Router stats (M3.1 acct-r29s, M3.2 acct-evyq, M3.3 acct-8xyj) ─
    // Counters live in shmem (not process-local statics) so the
    // router BGWorker's increments are visible from any backend
    // that reads via SQL accessor. Atomics — no LWLock needed.
    pub router_superbatch_count: AtomicU64,
    pub router_total_envelopes: AtomicU64,
    pub router_force_pack_count: AtomicU64,
    pub router_ticks_total: AtomicU64,
    pub router_entries_scanned_total: AtomicU64,
    pub committer_drains_total: AtomicU64,
    /// Cross-SuperBatch FOR UPDATE wait count (spec §4.4 O3). Each
    /// chunk emitted from a component that overflowed batch_size_max
    /// shares pool_keys with its sibling chunks; sibling SuperBatches
    /// claim the same `poc_v21_pool_locks` rows in committer Step 2
    /// and serialize via FOR UPDATE. Incremented per overflow chunk
    /// (chunks beyond the first within one connected component).
    pub router_cross_sb_for_update_waits: AtomicU64,
    /// Envelope-count histogram for SuperBatches (log2-spaced 8 buckets):
    /// 0:[1], 1:[2-3], 2:[4-7], 3:[8-15], 4:[16-31], 5:[32-63], 6:[64-127], 7:[128+].
    pub router_envelope_histogram: [AtomicU64; 8],
    pub router_max_envelope_count: AtomicU16,
    pub _pad_stats: [u8; 6],
    // ── M5b.2 (acct-ud4h): release/acquire invariant test surface ──
    // Shmem-resident (not GUCs) so a test backend's setter call is
    // visible to the router BGWorker without pg_reload_conf gymnastics.
    // Production binaries default both to 0/false (no-op fast path).
    pub test_inject_router_delay_us: AtomicU32,
    pub test_reorder_router_stores: AtomicU8,
    /// acct-gx1z.2 / acct-iypm: test-only BGWorker pause flag. When set
    /// to 1, both router_main and committer_main skip their per-tick
    /// work (audit + router_tick + committer claim loop). Tests that
    /// need deterministic synchronous control over slot transitions
    /// (slot_leak_audit, orphan_recovery) flip this on for the test
    /// window so concurrent BGWorker ticks don't race the test's
    /// manually-driven `poc_v21_test_*` operations. Default 0.
    pub test_bgworker_paused: AtomicU8,
    /// M5d.1 (acct-y0bp): postmaster-startup recovery flag.
    /// 0 = recovery not yet complete; router + committer BGWorkers
    /// spin at startup until set. 1 = recovery sweep finished
    /// (Release stored by the recovery worker; router/committers
    /// Acquire-load before opening for traffic).
    pub recovery_complete: AtomicU8,
    pub _pad_test: [u8; 2],
    // ── M5d.2 (acct-3rc1): periodic slot-leak audit counters ──────────
    // Incremented by the router BGWorker's periodic audit pass (every
    // 60s). `audit_last_run_at_ns` doubles as the next-due timestamp
    // and as an observability surface (poc_v21_audit_last_run_at).
    pub audit_reclaims_count: AtomicU64,
    pub audit_orphans_recovered_count: AtomicU64,
    pub audit_lost_envelopes_count: AtomicU64,
    pub audit_last_run_at_ns: AtomicU64,
    // ── M7.1 (acct-byue): committer pool + per-method observability ──
    // Counters live in shmem so any backend can read via SQL accessor
    // without holding a lock. Increments use Relaxed; reads use Relaxed
    // — eventual consistency is acceptable for observability.
    /// CAS wins on `claim_next_committer_entry`. Counts envelopes
    /// claimed by ANY committer (the pool's aggregate throughput
    /// trigger). One increment per successful Step 2 claim.
    pub committer_claim_count: AtomicU64,
    /// Orphan rescues from `try_recover_orphan` (M5a.1). One increment
    /// per stale-lease + dead-pid takeover.
    pub committer_takeover_count: AtomicU64,
    /// Step-5 sub-tx aborts (process_superbatch returning Err). Distinct
    /// from per-envelope `state='failed'` rows — this counts whole-batch
    /// failures (constraint violations, SPI errors during INSERT).
    pub committer_tx_failures: AtomicU64,
    /// Per-method dispatch counters: index 0=FIFO, 1=AVG, 2=STD.
    /// Incremented once per `apply_one` call in committer Step 4.
    pub method_dispatch_counts: [AtomicU64; 3],
    /// Per-method error counters (same indexing). Incremented when
    /// `apply_one` returns a non-None error_code.
    pub method_error_counts: [AtomicU64; 3],
    /// Per-method nanosecond latency histogram. 16 log2-spaced buckets;
    /// bucket i covers [2^(9+i), 2^(10+i)) ns — i.e., 512ns..16ms. Bucket
    /// 15 is the overflow (>= 2^24 ns = 16ms). Sized for typical apply_one
    /// latencies; bake-off can re-bucket if measured distribution sits in
    /// a different band. Stored as [method][bucket].
    pub method_latency_hist: [[AtomicU64; 16]; 3],
    // ── M7.2 (acct-fln5): bottleneck-classifier instrumentation ──
    /// Total ejections across all staging entries since extension load.
    /// Each eject (committer caller-tx coupling, in_progress branch)
    /// increments. Used by §5.6 B6-or-B5 classification — a high eject
    /// rate suggests caller-side contention forcing repeated re-routes.
    pub eject_total_count: AtomicU64,
    /// Cumulative committer pipeline wall-clock ns across all
    /// SuperBatches. Spans Step 2 (lex-lock) through Step 12 (status
    /// UPSERTs), all inside the sub-tx. Most of this time is SPI;
    /// in-memory dispatch is recoverable separately from method_latency_hist
    /// totals. Used by §5.6 B2 classifier as the SPI-time numerator
    /// (SPI_time / wall_time per committer).
    pub committer_pipeline_ns_total: AtomicU64,
    /// Number of pipeline executions that contributed to
    /// `committer_pipeline_ns_total`. Pairs with the ns counter to give
    /// avg pipeline ns per batch.
    pub committer_pipeline_count: AtomicU64,
    // ── acct-pl3b stage-timing breakdown ──
    /// Per-stage cumulative ns across all committer workers since
    /// extension load. Sum to verify ~= committer_pipeline_ns_total.
    /// Pair with committer_pipeline_count for per-stage avg ns/SB.
    /// Stage boundaries:
    ///   parse_ns      — read staging arena + deserialize PocV21Event
    ///   pre_apply_ns  — Steps 2 (locks), 2.45 (caller_tx), 2.5
    ///                   (dedup), 3 (snapshot hydration)
    ///   apply_ns      — Step 4 per-event dispatch via apply_one
    ///   bulk_insert_ns— Step 5 bulk INSERTs (6 tables)
    ///   post_ns       — Step 12 status UPSERTs + Step 14 cleanup
    pub committer_stage_parse_ns: AtomicU64,
    pub committer_stage_pre_apply_ns: AtomicU64,
    pub committer_stage_apply_ns: AtomicU64,
    pub committer_stage_bulk_insert_ns: AtomicU64,
    pub committer_stage_post_ns: AtomicU64,
    /// acct-ed7u: monotonic version counter for the committer's
    /// process-local `standard_costs` hydration cache. Anyone who
    /// INSERTs/UPDATEs `poc_v21_standard_costs` MUST call
    /// `poc_v21_invalidate_committer_caches()` (which fetch_adds this);
    /// each committer worker compares its last-known version against
    /// this on every SB and clears the cache on mismatch. Relaxed
    /// ordering is correct: the cache is a perf optimization, not
    /// a correctness barrier — worst case is one SB observing the
    /// pre-bump version and reading stale, which is the same window
    /// as the without-cache code path between SELECT and FOR UPDATE.
    pub standard_costs_cache_version: AtomicU64,
    // ── Committer identity slots (acct-l7k8, spec §1.6) ──
    // Application-level analog of PG's BackgroundWorkerData.slot[]:
    // each running committer worker claims one entry at startup and
    // releases at shutdown. CommitterQueueEntry.committer_bgw_slot and
    // committer_bgw_generation point into this array. PID-recycling-safe
    // liveness: a contender compares the entry's stored generation
    // against this slot's current generation. If the slot was released
    // and re-claimed by a different committer, the generation has
    // advanced and the stored identity no longer matches → orphan.
    pub identity_slots: [CommitterIdentitySlot; POC_V21_COMMITTER_IDENTITY_SLOTS],
    pub entries: [CommitterQueueEntry; POC_V21_COMMITTER_QUEUE_SIZE],
}

/// One per running committer worker. 64-byte-aligned so the per-slot
/// atomics don't false-share with neighbors.
#[repr(C, align(64))]
pub struct CommitterIdentitySlot {
    /// OS PID of the worker currently occupying this slot. 0 = free.
    /// The PID is informational + a fast-path liveness gate; the
    /// authoritative liveness signal is `generation`.
    pub pid: AtomicI32,
    /// Monotonic claim counter. Starts at 0 (zero-init); first claim
    /// bumps to 1, release bumps again. Generation == 0 only at fresh
    /// shmem init — never a valid identity. A real committer identity
    /// always has generation >= 1.
    pub generation: AtomicU32,
    pub _pad: [u8; 56],
}

/// Number of identity slots — must be >= committer_count GUC max (64).
pub const POC_V21_COMMITTER_IDENTITY_SLOTS: usize = 64;

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
static MAX_EJECT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static CALLER_TX_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static COMMITTER_COUNT: GucSetting<i32> = GucSetting::<i32>::new(4);
static DRAIN_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(2);
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

// Test-only Bool GUC (M4.1 acct-1c23). `skip_wip_locks=on` causes
// committers to SKIP the WIP pool_locks INSERT ON CONFLICT + SELECT
// FOR UPDATE in Step 2 of the pipeline. Default off (defensive).
// M8.3 sweeps {false, true} on shape S2 to measure the WIP-locks
// overhead per spec §5.5 / §7 Q-G; production deployment decision
// filed as acct-v21-fu-skip-wip-locks-prod.
static SKIP_WIP_LOCKS: GucSetting<bool> = GucSetting::<bool>::new(false);

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

/// Read `poc_v21.committer_lease_ms`. Defaults to 100. Used by M5a.1
/// orphan-recovery to declare a committer's claim stale.
pub(crate) fn committer_lease_ms_now() -> i32 {
    COMMITTER_LEASE_MS.get()
}

/// Read `poc_v21.batch_size_max`. Defaults to 50.
pub(crate) fn batch_size_max_now() -> i32 {
    BATCH_SIZE_MAX.get()
}

/// Read `poc_v21.drain_batch_size`. Defaults to 2 (acct-6kcx, measured
/// sweet spot at N=8 committer workers + 100-SKU normal-distribution
/// workload: N=2 gives +38% e2e lift over N=1 with CV=2.0%; N=4 captures
/// similar median but introduces a tail with occasional 120s timeouts
/// from lock-extension contention on poc_v21_pool_locks).
///
/// 1 = legacy (one outer txn per SB). N > 1 amortizes txn-fixed cost
/// (snapshot, XID, WAL commit record, fsync) across N SBs. Each SB still
/// runs in its own internal sub-tx for per-pipeline failure isolation.
pub(crate) fn drain_batch_size_now() -> i32 {
    DRAIN_BATCH_SIZE.get().max(1)
}

/// Read `poc_v21.router_window_size`. Defaults to 1000.
pub(crate) fn router_window_size_now() -> i32 {
    ROUTER_WINDOW_SIZE.get()
}

/// Read `poc_v21.router_starvation_threshold_ticks`. Defaults to 10.
pub(crate) fn router_starvation_threshold_ticks_now() -> i32 {
    ROUTER_STARVATION_THRESHOLD_TICKS.get()
}

/// Read `poc_v21.skip_wip_locks`. Defaults to `false`. When true, the
/// committer pipeline skips the WIP pool_locks INSERT + SELECT FOR
/// UPDATE; test-only knob for M8.3 measurement of WIP-locking
/// overhead.
pub(crate) fn skip_wip_locks() -> bool {
    SKIP_WIP_LOCKS.get()
}

/// Read `poc_v21.max_eject_count`. Defaults to 10000 per spec §1.7. Used
/// by M5c.2 caller-tx coupling: an envelope ejected this many times is
/// marked failed with `caller_tx_eject_exhausted` rather than re-queued.
pub(crate) fn max_eject_count_now() -> i32 {
    MAX_EJECT_COUNT.get()
}

/// Read `poc_v21.caller_tx_timeout_ms`. Defaults to 30000 (30 s) per
/// spec §1.7 — realistic upper bound for user-tx duration in costing-
/// ledger workloads. Once `now - enqueued_at_micros` exceeds this,
/// ejected envelopes are marked failed regardless of remaining
/// eject_count budget.
pub(crate) fn caller_tx_timeout_ms_now() -> i32 {
    CALLER_TX_TIMEOUT_MS.get()
}

/// Read `poc_v21.batch_window_us`. Used by the startup calibration
/// check (spec §1.7 line 563) and by router_tick's coalesce loop.
pub(crate) fn batch_window_us_now() -> i32 {
    BATCH_WINDOW_US.get()
}

// ── M5c.1 (acct-r0aa): backpressure CV signal mechanism ─────────────

/// CAS-gated lazy initialization of the backpressure ConditionVariable.
/// pgrx's PGRXSharedMemory zero-fills shmem at allocation, but PG's
/// `proclist_head` (inside ConditionVariable.wakeup) is NOT valid
/// zero-init — its sentinel for "empty list" is INVALID_PROC_NUMBER
/// (-1 in PG18), not 0. Without explicit ConditionVariableInit,
/// ConditionVariableBroadcast would walk a "proclist" starting from
/// head=0 (ProcNumber 0 = a real PGPROC slot), corrupt state, and
/// hang random backends.
///
/// Three-state CAS:
///   0 = uninitialized
///   1 = init in progress (some backend won the CAS; others spin)
///   2 = initialized (skip)
///
/// Called from every caller that touches the CV (enqueue's wait path,
/// signal_staging_slot_freed). Cheap fast path: single Acquire load,
/// no-op if state==2.
fn ensure_backpressure_cv_initialized(queue: &StagingQueue) {
    use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
    loop {
        let cur = queue.backpressure_cv_initialized.load(Acquire);
        if cur == 2 {
            return;
        }
        if cur == 0
            && queue
                .backpressure_cv_initialized
                .compare_exchange(0, 1, AcqRel, Acquire)
                .is_ok()
        {
            // We own the init. CV is shmem-resident; cast to *mut for FFI.
            let cv = &queue.backpressure_cv as *const pg_sys::ConditionVariable
                as *mut pg_sys::ConditionVariable;
            unsafe {
                pg_sys::ConditionVariableInit(cv);
            }
            queue.backpressure_cv_initialized.store(2, Release);
            return;
        }
        // cur == 1: another backend is initializing. Spin until done.
        std::hint::spin_loop();
    }
}

/// Raw pointer to the shmem-resident backpressure ConditionVariable,
/// after ensuring it has been initialized. The shmem address is stable
/// for the cluster lifetime, so this can be used outside the LWLock
/// guard for FFI calls (CV operations have their own internal slock;
/// the outer LWLock is NOT held while a backend is in
/// ConditionVariableTimedSleep — otherwise the signaler would deadlock
/// trying to free a slot).
pub(crate) fn backpressure_cv_ptr() -> *mut pg_sys::ConditionVariable {
    let queue = STAGING_QUEUE.share();
    ensure_backpressure_cv_initialized(&queue);
    &queue.backpressure_cv as *const pg_sys::ConditionVariable as *mut pg_sys::ConditionVariable
}

/// Signal "a staging slot was freed" — increment the wake counter for
/// observability + broadcast on the CV so any backend in
/// poc_v21_enqueue's backpressure wait loop wakes immediately. Idempotent;
/// safe to call whether or not any waiters are sleeping.
///
/// Caller passes an already-held `&StagingQueue` (via share OR exclusive
/// guard's deref) — the function does NOT re-acquire the LWLock to
/// avoid recursive-acquire deadlocks. The CV has its own slock internally,
/// so the outer LWLock is irrelevant to broadcast correctness.
pub fn signal_staging_slot_freed(queue: &StagingQueue) {
    queue
        .free_slot_wake_count
        .fetch_add(1, std::sync::atomic::Ordering::Release);
    ensure_backpressure_cv_initialized(queue);
    let cv = &queue.backpressure_cv as *const pg_sys::ConditionVariable
        as *mut pg_sys::ConditionVariable;
    unsafe {
        pg_sys::ConditionVariableBroadcast(cv);
    }
}

/// Claim a free identity slot for the calling committer (spec §1.6).
/// Returns (slot_idx, generation). Two-pass scan: first reclaims slots
/// marked free (pid == 0); second takes over slots whose occupant is
/// dead per kill(pid, 0). Panics if every slot is occupied by a live
/// PID — the GUC `committer_count` cap of 64 matches the array size.
pub(crate) fn claim_committer_identity() -> (u32, u32) {
    use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
    let queue = COMMITTER_QUEUE.share();
    let my_pid = unsafe { pg_sys::MyProcPid };
    // First pass: claim slots with pid == 0.
    for (idx, slot) in queue.identity_slots.iter().enumerate() {
        if slot
            .pid
            .compare_exchange(0, my_pid, AcqRel, Relaxed)
            .is_ok()
        {
            let generation = slot.generation.fetch_add(1, AcqRel) + 1;
            return (idx as u32, generation);
        }
    }
    // Second pass: take over slots with definitely-dead occupants.
    for (idx, slot) in queue.identity_slots.iter().enumerate() {
        let occupant = slot.pid.load(Acquire);
        if occupant == 0 || occupant == my_pid {
            continue;
        }
        let alive = unsafe { libc::kill(occupant, 0) } == 0;
        if alive {
            continue;
        }
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(0);
        if errno != libc::ESRCH {
            continue;
        }
        if slot
            .pid
            .compare_exchange(occupant, my_pid, AcqRel, Relaxed)
            .is_ok()
        {
            let generation = slot.generation.fetch_add(1, AcqRel) + 1;
            return (idx as u32, generation);
        }
    }
    panic!(
        "no free committer identity slot (capacity {})",
        POC_V21_COMMITTER_IDENTITY_SLOTS
    );
}

/// Release the calling committer's identity slot on clean shutdown.
/// Bumps generation first so any stale CommitterQueueEntry referencing
/// the prior identity no longer matches; then clears pid to 0.
pub(crate) fn release_committer_identity(slot_idx: u32) {
    use std::sync::atomic::Ordering::{AcqRel, Release};
    if (slot_idx as usize) >= POC_V21_COMMITTER_IDENTITY_SLOTS {
        return;
    }
    let queue = COMMITTER_QUEUE.share();
    let slot = &queue.identity_slots[slot_idx as usize];
    slot.generation.fetch_add(1, AcqRel);
    slot.pid.store(0, Release);
}

/// Liveness check (spec §1.6 step 1-4). Returns true iff the committer
/// identified by `(slot_idx, generation)` is currently alive: slot has
/// a non-zero pid, generation matches, and `kill(pid, 0)` succeeds.
/// Returns false on any sentinel (generation == 0 → unclaimed) or
/// out-of-bounds slot. Treats kill EPERM as alive (conservative).
pub(crate) fn is_committer_alive(slot_idx: u32, expected_generation: u32) -> bool {
    use std::sync::atomic::Ordering::Acquire;
    if expected_generation == 0 {
        return false;
    }
    if (slot_idx as usize) >= POC_V21_COMMITTER_IDENTITY_SLOTS {
        return false;
    }
    let queue = COMMITTER_QUEUE.share();
    let slot = &queue.identity_slots[slot_idx as usize];
    let pid = slot.pid.load(Acquire);
    if pid == 0 {
        return false;
    }
    let generation = slot.generation.load(Acquire);
    if generation != expected_generation {
        return false;
    }
    let kill_rc = unsafe { libc::kill(pid, 0) };
    if kill_rc == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(0);
    // EPERM (process exists but we lack signal permission) → alive.
    errno != libc::ESRCH
}

/// Observability: cumulative count of `signal_staging_slot_freed` calls
/// since cluster startup. Tests assert this grew during a drain to
/// confirm the signal actually fired.
#[pg_extern]
fn poc_v21_backpressure_wake_count() -> i64 {
    STAGING_QUEUE
        .share()
        .free_slot_wake_count
        .load(std::sync::atomic::Ordering::Acquire) as i64
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
        10_000,
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
        c"Caller-tx eject loop bound (spec §1.7)",
        c"Maximum times a single envelope may be ejected (committer observes caller user-tx in_progress) before being marked failed with caller_tx_eject_exhausted. Default 10000 sits comfortably above the wall-clock bound (~1500-6000 ejects at 30s/5-20ms-per-cycle).",
        &MAX_EJECT_COUNT,
        100,
        1_000_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_v21.caller_tx_timeout_ms",
        c"Caller user-tx total timeout (spec §1.7)",
        c"Once now - enqueued_at_micros exceeds this threshold, ejected envelopes are marked failed regardless of remaining eject_count budget. Default 30000 (30 s) — realistic upper bound for user-tx duration in costing-ledger workloads.",
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
        c"poc_v21.drain_batch_size",
        c"SuperBatches per committer outer transaction (acct-6kcx)",
        c"How many SuperBatches the committer drains per outer BGWorker transaction. Default 2 (measured sweet spot: +38% e2e at N=2; N=4 has same median but introduces stalls from lock-extension contention). Set 1 for legacy one-SB-per-txn behavior. N > 1 amortizes txn-fixed cost (snapshot, XID, WAL commit record, fsync) across N SBs; each SB still runs in its own internal sub-tx for per-pipeline failure isolation. Trade-off: longer outer-txn lifetime extends row-lock hold on pool_locks rows (the locks promote from sub-tx to outer on each release), which can increase cross-committer contention when SBs touch overlapping pools.",
        &DRAIN_BATCH_SIZE,
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

    // ── Bool GUCs ──────────────────────────────────────────────────
    GucRegistry::define_bool_guc(
        c"poc_v21.persistent_staging",
        c"Enable durable WAL-backed staging table (spec §1.9)",
        c"When ON, callers may pass durable_queue=true to enqueue; envelopes ride to durability with the caller's user-tx commit. Also unlocks status_insert_mode=committer_lazy. Postmaster-scope: requires restart to toggle.",
        &PERSISTENT_STAGING,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        c"poc_v21.skip_wip_locks",
        c"Test-only: skip WIP pool_locks acquisition in committer Step 2 (M4.1 acct-1c23)",
        c"When ON, the committer pipeline skips the INSERT ON CONFLICT + SELECT FOR UPDATE against poc_v21_wip_pool_locks. Spec §1.5 reasons WIP pools are uncontended by construction; this GUC lets M8.3 measure the cost of being defensive. Production decision deferred to acct-v21-fu-skip-wip-locks-prod.",
        &SKIP_WIP_LOCKS,
        GucContext::Sighup,
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

    // Eject-loop calibration warning (spec §1.7 line 563).
    //
    // theoretical_max_ejects bounds how many ejects a single envelope
    // could accumulate if every router tick cycled it back through.
    // If that bound is above max_eject_count / 2, the eject counter
    // — not the wall-clock timeout — becomes the primary termination
    // bound, inverting the spec's design intent. Surface a WARNING so
    // operators can either raise max_eject_count or accept the swap.
    let timeout_ms = CALLER_TX_TIMEOUT_MS.get() as i64;
    let window_us = BATCH_WINDOW_US.get() as i64;
    let max_ej = MAX_EJECT_COUNT.get() as i64;
    if window_us > 0 {
        let theoretical_max = timeout_ms * 1000 / window_us;
        if theoretical_max > max_ej / 2 {
            pgrx::warning!(
                "poc_v21: max_eject_count={} may not be the primary bound under tight cycling; \
                 theoretical_max_ejects={} (caller_tx_timeout_ms={} ms × 1000 / batch_window_us={} us). \
                 Verify the wall-clock timeout is the intended bound, or raise max_eject_count to >= {}.",
                max_ej, theoretical_max, timeout_ms, window_us, 2 * theoretical_max
            );
        }
    }

    // M1.3 BGWorkers: router + committer pool (M4.1 acct-1c23).
    //
    // committer_count is read here at _PG_init; runtime SIGHUP changes
    // do NOT respawn workers. Restart required to apply a new value.
    use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
    use std::time::Duration;
    // M5d.1 (acct-y0bp): postmaster-startup recovery worker. Runs once
    // at startup, classifies in-flight submission_status rows via
    // cost-row existence, sets recovery_complete=1 with Release. Router
    // + committers Acquire-load that flag and spin until set before
    // opening for traffic. set_restart_time(None) so PG won't relaunch
    // the worker after it exits.
    BackgroundWorkerBuilder::new("poc_v21_recovery")
        .set_function("poc_v21_recovery_main")
        .set_library("poc_v21_ledger")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(None)
        .enable_spi_access()
        .load();
    // M5b.1 (acct-k7b2): router now needs SPI access for the boot
    // recovery sweep's submission_status UPSERT (Phase 2 take-over of
    // a completed queue entry whose committer died mid-cleanup).
    // enable_spi_access() implies shmem access, so the pure-shmem
    // router_tick path keeps working.
    BackgroundWorkerBuilder::new("poc_v21_router")
        .set_function("poc_v21_router_main")
        .set_library("poc_v21_ledger")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
    let n_committers = COMMITTER_COUNT.get().max(1) as usize;
    for i in 0..n_committers {
        BackgroundWorkerBuilder::new(&format!("poc_v21_committer_{}", i))
            .set_function("poc_v21_committer_main")
            .set_library("poc_v21_ledger")
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
    }
}

// ── Smoke entry point ───────────────────────────────────────────────

// ── Arena observability (M5a.2 acct-b6bw) ───────────────────────────
//
// Slot-lifecycle leak audit per spec §3.10 needs visibility into the
// spillover-arena allocator state. All accessors are atomic loads;
// no locks held.

/// All-time allocation count (monotone).
#[pg_extern]
fn poc_v21_arena_total_allocs() -> i64 {
    SPILLOVER_ARENA.share().allocs_total() as i64
}

/// All-time free count (monotone).
#[pg_extern]
fn poc_v21_arena_total_frees() -> i64 {
    SPILLOVER_ARENA.share().frees_total() as i64
}

/// Currently outstanding allocations = total_allocs − total_frees.
/// Should converge to 0 at rest. A persistent non-zero value across
/// idle periods signals an arena leak.
#[pg_extern]
fn poc_v21_arena_outstanding() -> i64 {
    SPILLOVER_ARENA.share().outstanding_allocs() as i64
}

/// Bump high-water mark (bytes ever allocated linearly before the
/// freelist absorbed the demand).
#[pg_extern]
fn poc_v21_arena_bump_offset() -> i64 {
    SPILLOVER_ARENA.share().bump_offset_now() as i64
}

/// Count of free blocks on the freelist (O(n) — debug only).
#[pg_extern]
fn poc_v21_arena_freelist_count() -> i64 {
    SPILLOVER_ARENA.share().freelist_count() as i64
}

/// acct-ed7u: invalidate every committer worker's process-local
/// hydration caches. Bumps `standard_costs_cache_version` so the next
/// SB on each worker observes the change and clears its cache before
/// hydrating Step 3. Callers: anyone that mutates
/// `poc_v21_standard_costs` (test seeders, future
/// `post_standard_cost_roll`). Cheap — one Relaxed fetch_add.
#[pg_extern]
fn poc_v21_invalidate_committer_caches() {
    use std::sync::atomic::Ordering::Relaxed;
    COMMITTER_QUEUE
        .share()
        .standard_costs_cache_version
        .fetch_add(1, Relaxed);
}

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
