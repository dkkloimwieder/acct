//! Shmem layout for ledger-routed (Path B). Ported from
//! `poc/queue-extension-v21/src/lib.rs` 105-427 with v3 renames per
//! plan §E:
//!
//!   v21                                     v3
//!   ----------------------------------------------
//!   event_type_id                           trx_type_id
//!   sku_pool_count / wip_pool_count         pool_count
//!   sku_pool_keys_offset                    pool_keys_offset
//!   wip_pool_keys_offset                    dropped (single domain)
//!   sku_pool_keys_count + wip_pool_keys_count   pool_keys_count
//!   sku_pool_keys_offset + wip_pool_keys_offset (in CommitterQueueEntry)  pool_keys_offset
//!   sku_pool_keys_count + wip_pool_keys_count (in CommitterQueueEntry)    pool_keys_count
//!
//! Field bag is unchanged otherwise — v21's StagingEntry/Queue +
//! CommitterEntry/Queue + SpilloverArena carry the load-bearing
//! atomics, CV plumbing, identity slots, observability counters, and
//! BGWorker rendezvous fields verbatim. v3 inherits all of that.
//!
//! The PgLwLock statics + their wiring live in `lib.rs::_PG_init`.

use pgrx::pg_sys;
use pgrx::shmem::PGRXSharedMemory;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU16, AtomicU32, AtomicU64};

// ── Compile-time shmem sizing ───────────────────────────────────────
//
// Matching Postmaster-scope GUCs (`ledger_routed.staging_queue_size`,
// `committer_queue_size`, `spillover_arena_mb`) document the surface
// + bound the GUC ranges, but the actual shmem allocation is driven
// by these constants. Mismatch between GUC value at startup and
// compiled constant is reported as a NOTICE at _PG_init time;
// resizing requires recompile. v3 keeps v21's sizes; the bake-off
// alternates that didn't pan out for v21 don't change for v3.

pub const LEDGER_V3_STAGING_QUEUE_SIZE: usize = 16384;
pub const LEDGER_V3_COMMITTER_QUEUE_SIZE: usize = 2048;
pub const LEDGER_V3_SPILLOVER_ARENA_MB: usize = 128;
pub const LEDGER_V3_SPILLOVER_ARENA_BYTES: usize =
    LEDGER_V3_SPILLOVER_ARENA_MB * 1024 * 1024;
pub const LEDGER_V3_COMMITTER_IDENTITY_SLOTS: usize = 64;

// ── StagingEntry / StagingQueue ─────────────────────────────────────

#[repr(C)]
pub struct StagingEntry {
    /// 0=empty 1=pending 2=processing 3=routed 4=abandoned
    pub valid: AtomicU8,
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub correlation_id: [u8; 16],
    pub user_tx_xid: u64,
    /// Renamed from v21's `event_type_id`. Index into a per-extension
    /// trx-type table the caller maps the SQL `trx_type` enum into at
    /// enqueue time. The shmem layer is enum-agnostic; the dispatch
    /// per-trx-type lives in the committer (Path B) / SPI surface
    /// (Path A).
    pub trx_type_id: u16,
    pub _pad_trx: [u8; 2],
    pub payload_offset: u32,
    pub payload_length: u32,
    /// Renamed from v21's `sku_pool_count + wip_pool_count`. v3
    /// collapses the dual-domain (SKU pools + WIP pools) into a
    /// single `pool_id` namespace per design-v3 §1.
    pub pool_count: u16,
    pub _pad_pool: [u8; 2],
    pub pool_keys_offset: u32,
    pub enqueued_at_micros: u64,
    pub backend_pid: i32,
    pub _pad_pid: [u8; 4],
    pub superbatch_id: AtomicU64,
    pub eject_count: AtomicU32,
    pub _pad2: [u8; 4],
    /// Timestamp of the most recent eject (`now_ns()` units, PG epoch).
    /// Written by the committer's eject path
    /// (`fetch_add` on `eject_count` Release THEN `store` here
    /// Release). Read by the router's `collect_candidates`
    /// cooldown filter (Acquire-load); paired against
    /// `eject_cooldown_ms` to skip recently-ejected slots
    /// (design-v3 §5.3 step 2). 0 = never ejected.
    pub last_eject_at_ns: AtomicU64,
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
    /// Cumulative count of `signal_staging_slot_freed` calls — tests
    /// assert this grew during a drain to confirm the signal fired.
    pub free_slot_wake_count: AtomicU64,
    /// 3-state CAS gate for lazy `ConditionVariableInit`. 0=uninit,
    /// 1=init-in-progress, 2=initialized. The CV's `wakeup` field
    /// (proclist_head) is NOT valid zero-init — its sentinel for
    /// "empty list" is `INVALID_PROC_NUMBER` (-1 in PG18), not 0.
    /// Without explicit `ConditionVariableInit`, `Broadcast` would
    /// walk a proclist starting from head=0 (a real PGPROC slot),
    /// corrupting state and hanging random backends.
    pub backpressure_cv_initialized: AtomicU8,
    pub _pad_cv_init: [u8; 7],
    pub backpressure_cv: pg_sys::ConditionVariable,
    pub _pad_cv: [u8; 8],
    pub entries: [StagingEntry; LEDGER_V3_STAGING_QUEUE_SIZE],
}

impl Default for StagingQueue {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for StagingQueue {}

// ── CommitterQueueEntry / CommitterQueue ────────────────────────────

#[repr(C)]
pub struct CommitterQueueEntry {
    /// 0=empty 1=ready 2=in_flight 3=completed
    pub valid: AtomicU8,
    pub _pad: [u8; 7],
    pub superbatch_id: u64,
    pub envelope_count: u16,
    pub _pad_env: [u8; 2],
    pub staging_entry_offsets: u32,
    /// Renamed from v21's `sku_pool_keys_offset + wip_pool_keys_offset`.
    /// Single domain in v3 per plan §E.
    pub pool_keys_offset: u32,
    pub pool_keys_count: u16,
    pub _pad_pool: [u8; 2],
    /// Owning committer's identity-slot index in
    /// `CommitterQueue.identity_slots`. Only meaningful when
    /// `committer_bgw_generation > 0`; generation == 0 is the
    /// "unclaimed" sentinel (matches shmem zero-init).
    pub committer_bgw_slot: AtomicU32,
    /// Owning committer's slot generation, captured at claim time.
    /// Compared against `identity_slots[committer_bgw_slot].generation`
    /// to detect a recycled slot (PID-recycling-safe).
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

    // ── Router stats ───────────────────────────────────────────────
    pub router_superbatch_count: AtomicU64,
    pub router_total_envelopes: AtomicU64,
    /// Number of commit_groups emitted without applying the
    /// `batch_size_max` chunking step because at least one pool in the
    /// group is order-sensitive (fifo / lifo / specific). Read by
    /// `ledger_routed_router_order_sensitive_groups_total`. See acct-aywu.
    pub router_order_sensitive_groups_total: AtomicU64,
    pub router_ticks_total: AtomicU64,
    pub router_entries_scanned_total: AtomicU64,
    pub committer_drains_total: AtomicU64,
    pub router_cross_sb_for_update_waits: AtomicU64,
    pub router_window_defers_total: AtomicU64,
    /// Envelope-count histogram for SuperBatches (log2-spaced 8
    /// buckets): 0:[1], 1:[2-3], 2:[4-7], 3:[8-15], 4:[16-31],
    /// 5:[32-63], 6:[64-127], 7:[128+].
    pub router_envelope_histogram: [AtomicU64; 8],
    pub router_max_envelope_count: AtomicU16,
    pub _pad_stats: [u8; 6],

    // ── Test injection ─────────────────────────────────────────────
    pub test_inject_router_delay_us: AtomicU32,
    /// Microseconds the committer pipeline sleeps under
    /// `cfg(feature = "test_hooks")` after pool_lock acquisition and
    /// before bulk_write. Production builds never reach the read site;
    /// the field exists unconditionally so shmem layout doesn't drift
    /// across feature variants. Tests set this via
    /// `ledger_routed_test_set_committer_stall_us` to force the
    /// committer to remain in flight long enough for pg_terminate_backend
    /// to land mid-pipeline (acct-p0d8 orphan_recovery).
    pub test_inject_committer_stall_us: AtomicU32,
    /// Number of times the committer pipeline entered the stall sleep
    /// path under nonzero `test_inject_committer_stall_us`. Read by
    /// the test_hooks `ledger_routed_test_committer_stall_hits` SPI.
    pub test_committer_stall_hits: AtomicU64,
    /// Router worker's OS PID, stored once at router_main startup.
    /// 0 = router not yet started (or restarting). Read by the
    /// test_hooks `ledger_routed_test_router_pid` SPI so tests can
    /// pg_terminate_backend the router to trigger boot sweep on restart.
    pub router_pid: AtomicI32,
    pub test_reorder_router_stores: AtomicU8,
    pub test_bgworker_paused: AtomicU8,
    /// Postmaster-startup recovery flag. 0 = recovery not yet
    /// complete; router + committer BGWorkers spin at startup until
    /// set. 1 = recovery sweep finished (Release stored by the
    /// recovery worker; router/committers Acquire-load before opening
    /// for traffic).
    pub recovery_complete: AtomicU8,
    pub _pad_test: [u8; 2],

    // ── Slot-leak audit counters ───────────────────────────────────
    pub audit_reclaims_count: AtomicU64,
    pub audit_orphans_recovered_count: AtomicU64,
    pub audit_lost_envelopes_count: AtomicU64,
    pub audit_last_run_at_ns: AtomicU64,

    // ── Committer pool counters ────────────────────────────────────
    pub committer_claim_count: AtomicU64,
    pub committer_takeover_count: AtomicU64,
    pub committer_tx_failures: AtomicU64,

    // ── Eject + pipeline observability ─────────────────────────────
    pub eject_total_count: AtomicU64,
    pub committer_pipeline_ns_total: AtomicU64,
    pub committer_pipeline_count: AtomicU64,
    /// Per-stage cumulative ns across all committer workers since
    /// extension load. Stages match v21's acct-pl3b breakdown adapted
    /// to v3's 8-step pipeline (design-v3 §5.4):
    ///   parse_ns        — Step 3 deserialize submissions from arena
    ///   pre_apply_ns    — Steps 4-7 eject loop + lock acquire +
    ///                     snapshot hydrate
    ///   apply_ns        — Step 8 per-submission `plan_apply` with
    ///                     pristine-replay loop
    ///   bulk_insert_ns  — Step 9 bulk-write (trx + trx_line +
    ///                     pool_state + posting_line)
    ///   post_ns         — Step 10 COMMIT + Step 11 cleanup
    pub committer_stage_parse_ns: AtomicU64,
    pub committer_stage_pre_apply_ns: AtomicU64,
    pub committer_stage_apply_ns: AtomicU64,
    pub committer_stage_bulk_insert_ns: AtomicU64,
    pub committer_stage_post_ns: AtomicU64,

    // ── Committer identity slots ───────────────────────────────────
    pub identity_slots: [CommitterIdentitySlot; LEDGER_V3_COMMITTER_IDENTITY_SLOTS],
    pub entries: [CommitterQueueEntry; LEDGER_V3_COMMITTER_QUEUE_SIZE],
}

impl Default for CommitterQueue {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for CommitterQueue {}

/// One per running committer worker. 64-byte-aligned so per-slot
/// atomics don't false-share with neighbors. Operations on this struct
/// live in `identity.rs` (claim / release / liveness check).
#[repr(C, align(64))]
pub struct CommitterIdentitySlot {
    /// OS PID of the worker currently occupying this slot. 0 = free.
    /// Informational + fast-path liveness gate; authoritative
    /// liveness signal is `generation`.
    pub pid: AtomicI32,
    /// Monotonic claim counter. Starts at 0 (zero-init); first claim
    /// bumps to 1, release bumps again. Generation == 0 only at fresh
    /// shmem init — never a valid identity. A real committer always
    /// has generation >= 1.
    pub generation: AtomicU32,
    pub _pad: [u8; 56],
}

// ── SpilloverArena ──────────────────────────────────────────────────

/// Flat byte buffer indexed by u32 offsets stored in
/// `StagingEntry.payload_offset / pool_keys_offset` and
/// `CommitterQueueEntry.staging_entry_offsets / pool_keys_offset`.
/// Allocator (bump + LIFO freelist) lands in `arena.rs` (acct-17p5);
/// this struct only holds the byte region + the allocator's anchor
/// atomics + observability counters.
#[repr(C, align(64))]
pub struct SpilloverArena {
    /// Offset of head-of-freelist block header (0 = empty list).
    pub freelist_head_offset: AtomicU32,
    /// Next never-touched byte (high-water mark for bump alloc).
    pub bump_offset: AtomicU32,
    pub total_allocs: AtomicU64,
    pub total_frees: AtomicU64,
    pub bytes: [u8; LEDGER_V3_SPILLOVER_ARENA_BYTES],
}

impl Default for SpilloverArena {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for SpilloverArena {}

// ── PgLwLock statics ────────────────────────────────────────────────

use pgrx::PgLwLock;

pub static STAGING_QUEUE: PgLwLock<StagingQueue> =
    unsafe { PgLwLock::new(c"ledger_v3_staging_queue") };
pub static COMMITTER_QUEUE: PgLwLock<CommitterQueue> =
    unsafe { PgLwLock::new(c"ledger_v3_committer_queue") };
pub static SPILLOVER_ARENA: PgLwLock<SpilloverArena> =
    unsafe { PgLwLock::new(c"ledger_v3_spillover_arena") };

// ── Backpressure CV plumbing ────────────────────────────────────────
//
// CAS-gated lazy initialization. pgrx's `PGRXSharedMemory` zero-fills
// shmem at allocation, but `proclist_head` (inside
// `ConditionVariable.wakeup`) is NOT valid zero-init — its sentinel
// for "empty list" is `INVALID_PROC_NUMBER` (-1 in PG18), not 0.
// Without explicit `ConditionVariableInit`, `ConditionVariableBroadcast`
// walks a proclist starting from head=0 (a real PGPROC slot), corrupts
// state, hangs random backends.
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
            let cv = &queue.backpressure_cv as *const pg_sys::ConditionVariable
                as *mut pg_sys::ConditionVariable;
            unsafe {
                pg_sys::ConditionVariableInit(cv);
            }
            queue.backpressure_cv_initialized.store(2, Release);
            return;
        }
        std::hint::spin_loop();
    }
}

/// Raw pointer to the shmem-resident backpressure CV, after ensuring
/// it has been initialized. The shmem address is stable for the
/// cluster lifetime, so this can be used outside the LWLock guard for
/// FFI calls (CV operations have their own internal slock; the outer
/// LWLock is NOT held while a backend is in
/// `ConditionVariableTimedSleep` — otherwise the signaler would
/// deadlock trying to free a slot).
#[allow(dead_code)]
pub(crate) fn backpressure_cv_ptr() -> *mut pg_sys::ConditionVariable {
    let queue = STAGING_QUEUE.share();
    ensure_backpressure_cv_initialized(&queue);
    &queue.backpressure_cv as *const pg_sys::ConditionVariable
        as *mut pg_sys::ConditionVariable
}

/// Signal "a staging slot was freed" — increment the wake counter for
/// observability + broadcast on the CV so any backend in the enqueue
/// backpressure wait loop wakes immediately. Idempotent; safe whether
/// or not any waiters are sleeping. Caller passes an already-held
/// `&StagingQueue`; the function does NOT re-acquire the LWLock to
/// avoid recursive-acquire deadlocks (CV has its own internal slock).
#[allow(dead_code)]
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

// ── Time helpers ────────────────────────────────────────────────────

/// Current PG timestamp in microseconds since 2000-01-01, clamped at
/// zero. `pg_sys::GetCurrentTimestamp()` returns `TimestampTz` (i64);
/// a negative pre-2000 value cast `as u64` silently wraps and corrupts
/// every downstream arithmetic (lease windows, stale staging
/// detection, etc.). Clamping keeps the invariant `now_us() >= 0`.
#[inline]
#[allow(dead_code)]
pub(crate) fn now_us() -> u64 {
    let t = unsafe { pg_sys::GetCurrentTimestamp() };
    t.max(0) as u64
}

/// Current PG timestamp in nanoseconds since 2000-01-01, clamped.
/// Saturates at `u64::MAX` rather than wrapping.
#[inline]
#[allow(dead_code)]
pub(crate) fn now_ns() -> u64 {
    now_us().saturating_mul(1000)
}
