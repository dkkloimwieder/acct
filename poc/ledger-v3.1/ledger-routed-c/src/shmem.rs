//! Shmem layout for ledger-routed-c (Path C, design-v3.1 §6.2).
//!
//! StagingEntry/Queue + CommitterEntry/Queue + SpilloverArena carry the
//! load-bearing atomics, CV plumbing, identity slots, observability
//! counters, and BGWorker rendezvous fields. Single `pool_id` namespace.
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
// resizing requires recompile.

pub const LEDGER_V3_STAGING_QUEUE_SIZE: usize = 16384;
pub const LEDGER_V3_COMMITTER_QUEUE_SIZE: usize = 2048;
pub const LEDGER_V3_SPILLOVER_ARENA_MB: usize = 128;
pub const LEDGER_V3_SPILLOVER_ARENA_BYTES: usize =
    LEDGER_V3_SPILLOVER_ARENA_MB * 1024 * 1024;
pub const LEDGER_V3_COMMITTER_IDENTITY_SLOTS: usize = 64;

// design-v3.1 §6.2 has no per-pool sequence table: Path C drops the strict
// cross-window FIFO ordering the strict path needed. Routed Path C preserves
// order within a commit_group via enqueue order and serializes cross-group
// hot-pool contention at the pool_lock; provisional unit_costs are allowed to
// differ across orderings (§9.4, §14.2). So no per-pool sequence region exists
// here.

// ── StagingEntry / StagingQueue ─────────────────────────────────────

#[repr(C)]
pub struct StagingEntry {
    /// 0=empty 1=pending 2=processing 3=routed 4=abandoned
    pub valid: AtomicU8,
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub user_tx_xid: u64,
    pub payload_offset: u32,
    pub payload_length: u32,
    /// Arena offset of the lines blob — `encode_submission`'s first
    /// allocation (the submission blob, at `payload_offset`, records the
    /// same value as its `line_offset`). Stored on the slot so the
    /// committer cleanup path can free it without decoding the submission.
    /// 0 = no block.
    pub line_offset: u32,
    /// Number of pool_ids this submission touches.
    pub pool_count: u16,
    pub _pad_pool: [u8; 2],
    pub pool_keys_offset: u32,
    pub _pad_pk: [u8; 4],
    pub enqueued_at_micros: u64,
    pub commit_group_id: AtomicU64,
    pub eject_count: AtomicU32,
    pub _pad2: [u8; 4],
    /// Timestamp of the most recent eject (`now_ns()` units, PG epoch).
    /// Written by the committer's eject path
    /// (`fetch_add` on `eject_count` Release THEN `store` here
    /// Release). Read by the router's `collect_candidates`
    /// cooldown filter (Acquire-load); paired against
    /// `eject_cooldown_ms` to skip recently-ejected slots
    /// (§6.3 router collect_candidates). 0 = never ejected.
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
    /// Cumulative count of `signal_staging_slot_freed` calls (one per staged-slot
    /// free broadcast). Incremented for observability but not currently read —
    /// reserved for a future SQL getter, like the router/committer stat counters
    /// that grew accessors as a measurement need arose.
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
    /// Test-only backpressure force-fail hooks (acct-mvq4.37). They drive the two
    /// enqueue ERROR exits (queue-full timeout, arena-full deadline) that no test
    /// or bench ever executes, without filling the 16384-slot ring or waiting the
    /// 5 s `queue_full_timeout_ms` (a Sighup GUC with no per-session SET). All
    /// zero-init = disabled; production (non-`test_hooks`) builds never read them.
    /// Read under the `STAGING_QUEUE` guard already held on the push path (or via
    /// `.share()` in `past_deadline`), so no nested LWLock is introduced.
    ///
    /// When 1, every `push_entry_into_queue` returns `PushError::QueueFull`.
    pub test_force_queue_full: AtomicU8,
    /// When 1, `past_deadline` reports the backpressure deadline elapsed, so the
    /// wait loop raises immediately rather than CV-waiting the full timeout.
    pub test_deadline_expired: AtomicU8,
    pub _pad_test: [u8; 2],
    /// Countdown for the batch PARTIAL-push case, +1-encoded: 0 = disabled;
    /// value N (N>=1) allows N-1 pushes to succeed then fails the rest. The setter
    /// stores k+1 so a zero-init field reads as disabled. Decremented once per
    /// allowed push under the exclusive guard (single writer).
    pub test_queue_full_after: AtomicU32,
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
    /// 0=empty 1=ready 2=in_flight 3=completed 4=poisoned. `poisoned` is a
    /// terminal dead-letter state (§6.8): a committer hit a non-retryable SQL
    /// error (UNIQUE survived dedup, or a deadlock that exhausted its retry
    /// budget). The slot is never re-claimed (claim only takes valid==1) and is
    /// left at 4 for observability; `committer_poisoned_total` counts it.
    pub valid: AtomicU8,
    pub _pad: [u8; 7],
    pub commit_group_id: u64,
    pub submission_count: u16,
    pub _pad_submission: [u8; 2],
    pub staging_entry_offsets: u32,
    pub pool_keys_offset: u32,
    pub pool_keys_count: u16,
    pub _pad_pool: [u8; 2],
    /// [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] Router-stamped owner
    /// ordinal = mix(min pool_id) % committer_count. Published together with the
    /// other router fields by the `valid` 0→1 Release; a committer reads it (via
    /// an Acquire load of `valid`) on the claim scan when `affinity_scheme != 0`,
    /// and ignores it (dead field) when affinity is off.
    pub affinity_owner: u32,
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
    pub enqueued_at_micros: u64,
}

#[repr(C, align(64))]
pub struct CommitterQueue {
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub lock_tranche_id: u32,
    pub _pad: [u8; 4],
    pub next_commit_group_id: AtomicU64,

    // ── Router stats ───────────────────────────────────────────────
    pub router_commit_group_count: AtomicU64,
    pub router_total_submissions: AtomicU64,
    pub router_ticks_total: AtomicU64,
    pub router_entries_scanned_total: AtomicU64,
    pub committer_drains_total: AtomicU64,
    pub router_cross_commit_group_for_update_waits: AtomicU64,
    pub router_window_defers_total: AtomicU64,
    /// Submission-count histogram for CommitGroups (log2-spaced 8
    /// buckets): 0:[1], 1:[2-3], 2:[4-7], 3:[8-15], 4:[16-31],
    /// 5:[32-63], 6:[64-127], 7:[128+].
    pub router_submission_histogram: [AtomicU64; 8],
    pub router_max_submission_count_per_group: AtomicU16,
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
    /// to land mid-pipeline (acct-2ttr.7 orphan recovery).
    pub test_inject_committer_stall_us: AtomicU32,
    /// Test-only: number of synthetic deadlocks (SQLSTATE 40P01) the committer
    /// write phase should raise before letting the write proceed. Decremented
    /// once per injected raise. Lets the §6.8 retry-on-deadlock path be
    /// exercised deterministically. Production builds never set it nonzero.
    pub test_inject_committer_deadlock_count: AtomicU32,
    /// Number of times the committer pipeline entered the stall sleep
    /// path under nonzero `test_inject_committer_stall_us`. Read by
    /// the test_hooks `ledger_routed_test_committer_stall_hits` SPI.
    pub test_committer_stall_hits: AtomicU64,
    /// Router worker's OS PID, stored once at router_main startup.
    /// 0 = router not yet started (or restarting). Diagnostic state — the live
    /// router PID is inspectable from shmem by an operator/debugger.
    pub router_pid: AtomicI32,
    pub test_reorder_router_stores: AtomicU8,
    /// Test-only: when 1, the router skips its tick body (stays parked on the
    /// latch). Lets a test stage a whole batch before the router groups it.
    pub test_bgworker_paused: AtomicU8,
    /// Test-only: when 1, the committer skips claiming/draining commit_groups.
    /// Independent of `test_bgworker_paused` so the router-affinity test can run
    /// the router while keeping emitted groups at `ready` for inspection.
    pub test_committer_paused: AtomicU8,
    /// Test-only one-shot: when 1, the next committer to enter its write phase
    /// raises a non-retryable SQL error (forces the §6.8 poison path), then
    /// clears the flag (swap-to-0). Production builds never set it nonzero.
    pub test_inject_committer_fatal: AtomicU8,
    /// Test-only one-shot: when 1, the next committer write phase raises a raw
    /// 23505 UNIQUE violation (no real duplicate behind it), then clears the flag
    /// (swap-to-0). Exercises the §6.8 re-drive safety valve — a 23505 whose
    /// duplicate is not resolvable in `trx` poisons rather than looping. Production
    /// builds never set it nonzero.
    pub test_inject_committer_unique: AtomicU8,
    /// Test-only one-shot: when 1, the next committer triage (classify_and_eject)
    /// treats its `pg_xact_status` probe as failed — skips the real query and takes
    /// the fail-closed eject-all path (acct-mvq4.30) — then clears the flag
    /// (swap-to-0). Lets a test reach the probe-failure branch without a real
    /// OOM/interrupt. Production builds never set it nonzero.
    pub test_inject_xact_probe_fail: AtomicU8,
    /// Postmaster-startup recovery flag. 0 = recovery not yet
    /// complete; router + committer BGWorkers spin at startup until
    /// set. 1 = recovery sweep finished (Release stored by the
    /// recovery worker; router/committers Acquire-load before opening
    /// for traffic).
    pub recovery_complete: AtomicU8,
    pub _pad_test: [u8; 0],

    // ── Committer pool counters ────────────────────────────────────
    pub committer_takeover_count: AtomicU64,
    pub committer_tx_failures: AtomicU64,
    /// commit_groups moved to the terminal `poisoned` state (valid==4) after a
    /// non-retryable SQL error or a deadlock that exhausted its retry budget
    /// (§6.8). Their submissions are lost (no trx); the CQ slot is a dead-letter.
    pub committer_poisoned_total: AtomicU64,
    /// Cumulative count of deadlock-driven write-phase retries (§6.8): one per
    /// re-attempt after a 40P01 / 40001, across all commit_groups.
    pub committer_deadlock_retries_total: AtomicU64,
    /// Per-pool FOR UPDATE acquisitions across all committed commit_groups
    /// (incremented by `pool_ids.len()` per `acquire_pool_locks`). The routed
    /// batching win: a whole commit_group's submissions to one hot pool add 1
    /// here, where direct flavor would add one per submission (§6.7).
    pub committer_pool_lock_acquisitions_total: AtomicU64,
    /// Aggregate (`layer_id = 0`) rows UPSERTed across all committed
    /// commit_groups (one per touched aggregate-bearing pool per group — the
    /// final-snapshot collapse, NOT one per submission).
    pub committer_aggregate_upserts_total: AtomicU64,
    /// trx rows committed across all commit_groups (one per included
    /// submission). The harness polls trx existence; this is the in-shmem
    /// mirror for fast cross-checks.
    pub committer_trx_committed_total: AtomicU64,
    /// Submissions excluded by pre-flight dedup (already in trx, or a
    /// within-batch (trx_type, source_id) duplicate). §6.4 step 5.
    pub committer_dedup_skips_total: AtomicU64,
    /// Submissions dropped by drop-and-continue (per-submission
    /// plan_apply_provisional Err — e.g. insufficient inventory). §6.4 step 9.
    pub committer_dropped_submissions_total: AtomicU64,
    /// Write-phase 23505 (UNIQUE survived pre-flight dedup) re-drives (§6.8): one
    /// per caught unique violation. The racing duplicate is re-dedup'd out and the
    /// rest of the group re-driven, so an innocent submission isn't dead-lettered
    /// alongside the offender. Counts both the resolved re-drives and the safety-
    /// valve poison (23505 with no resolvable duplicate).
    pub committer_duplicate_redrives_total: AtomicU64,

    // ── Eject + pipeline observability ─────────────────────────────
    pub eject_total_count: AtomicU64,
    pub committer_pipeline_ns_total: AtomicU64,
    pub committer_pipeline_count: AtomicU64,
    /// Wall-time (ns) the committer spends inside `pool_lock::acquire_pool_locks`
    /// — the per-pool FOR UPDATE acquisition, summed across all committed
    /// commit_groups. This is the cross-committer hot-pool-handoff span: when a
    /// hot pool's groups land on different committers across ticks, they serialize
    /// here on each other's row lock. The affinity question (acct-0usf) is
    /// precisely whether pinning a pool to one committer shrinks this span. Timed
    /// at the call site (not test_hooks-gated) so it is live in bench builds.
    pub committer_pool_lock_ns_total: AtomicU64,
    /// Wall-time (ns) in `hydration::hydrate_snapshot` — one aggregate read per
    /// touched pool, summed across committed commit_groups.
    pub committer_hydrate_ns_total: AtomicU64,
    /// Wall-time (ns) in `plan_and_write` — drop-and-continue apply + batch write
    /// (trx / trx_line / layer-mutations / posting_line / aggregate UPSERT),
    /// summed across committed commit_groups. Excludes the COMMIT itself.
    pub committer_apply_ns_total: AtomicU64,
    /// Wall-time (ns) of the whole `BackgroundWorker::transaction` wrapper around
    /// `process_commit_group` — BEGIN + pipeline + COMMIT (the group-commit /
    /// fsync). `committer_txn_ns_total − committer_pipeline_ns_total` isolates the
    /// commit/fsync cost; `committer_pipeline_ns_total − (pool_lock + hydrate +
    /// apply)` isolates decode + triage + dedup + line-decode (the "prep" span).
    pub committer_txn_ns_total: AtomicU64,
    /// Prep-span refold (acct-e95d). The "prep" residual above lumps four costs;
    /// these split it so the prep floor is targeted from a measured breakdown, not
    /// guessed. `decode` = `decode_submissions` + `decode_lines` (Rust payload /
    /// line decode, scales per-trx/per-line); `xact` = `classify_and_eject` (one
    /// `pg_xact_status` SPI query per group); `dedup` = `dedup_against_trx` (one
    /// dedup SELECT per group). prep − (decode + xact + dedup) = staging-index read
    /// + subtx/retry framing. Cumulative ns since load; sample as deltas.
    pub committer_decode_ns_total: AtomicU64,
    pub committer_xact_ns_total: AtomicU64,
    pub committer_dedup_ns_total: AtomicU64,
    // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] claim-path engagement
    // counters. owned = a committer claimed a group it owns; steals = a
    // committer claimed a non-owned group after it aged past affinity_steal_ms.
    // steal_fraction = steals / (owned + steals) shows whether affinity is
    // actually pinning groups to owners (≈0 steals) or degenerating to OFF
    // (steals ≈ owned). Zero when affinity_scheme = 0.
    pub affinity_owned_claims_total: AtomicU64,
    pub affinity_steals_total: AtomicU64,

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
/// Allocator (bump + LIFO freelist) lands in `arena.rs` (acct-2ttr.4);
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
    /// Test-only (acct-mvq4.37): when 1, `try_alloc_blocks` reports `ArenaFull`
    /// without allocating — drives the arena-exhaustion ERROR exits. Zero-init =
    /// disabled; production (non-`test_hooks`) builds never read it. Read under the
    /// `SPILLOVER_ARENA` guard already held in `try_alloc_blocks`.
    pub test_force_arena_full: AtomicU8,
    pub _pad_test: [u8; 7],
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
    unsafe { PgLwLock::new(c"ledger_v31_staging_queue") };
pub static COMMITTER_QUEUE: PgLwLock<CommitterQueue> =
    unsafe { PgLwLock::new(c"ledger_v31_committer_queue") };
pub static SPILLOVER_ARENA: PgLwLock<SpilloverArena> =
    unsafe { PgLwLock::new(c"ledger_v31_spillover_arena") };

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
pub(crate) fn now_us() -> u64 {
    let t = unsafe { pg_sys::GetCurrentTimestamp() };
    t.max(0) as u64
}

/// Current PG timestamp in nanoseconds since 2000-01-01, clamped.
/// Saturates at `u64::MAX` rather than wrapping.
#[inline]
pub(crate) fn now_ns() -> u64 {
    now_us().saturating_mul(1000)
}
