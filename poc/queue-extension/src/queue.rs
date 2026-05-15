//! acct-4d4n.2 (M1.1) + acct-4d4n.3 (M1.2): shmem primitives + committer.
//!
//! Implements the shmem data structures from spec §1.4 — PocQueueShard
//! header, PocPendingRequest ring, PocResultSlot pool — plus slot
//! allocation per §1.6 step 5 (atomic fetch_add for index reservation,
//! CAS state transition free→allocated, linear probe ≤16) and ring
//! buffer push/drain discipline under a PgLwLock.
//!
//! ## What M1.1 + M1.2 covers
//!
//! - `PocQueueShard` header (head, tail, capacity, committer state,
//!   per-shard `committer_tx_seq`, slot/request sequence counters).
//! - `PocPendingRequest` ring buffer with `valid` state machine
//!   (empty/filled/in-flight/abandoned) and Apply-variant body layout.
//! - `PocResultSlot` pool with state machine (free/allocated/filled/
//!   abandoned) plus `committer_tx_id` audit field (added by M1.2 so
//!   callers can read the stamping committer_tx without a secondary
//!   SPI query into `poc_test_rows`).
//! - Slot allocation OUTSIDE the LWLock per spec §1.6 step 5; linear
//!   probe bounded by `POC_MAX_SLOT_PROBE = 16`.
//! - Ring push/drain under a single global `PgLwLock` (M3.1 swaps to
//!   per-shard tranches).
//! - SQL surface for driving the M1.1 acceptance tests from psql.
//!
//! ## What M1.2 deliberately defers
//!
//! - GUC-driven runtime sizing. M1.1 uses compile-time const values;
//!   spec §1.4 defaults (256×4096×4096) would require a
//!   shmem_request_hook FFI dance that's out of M1.x scope. The
//!   `poc_ledger.*` GUCs registered by M0.1 remain advisory.
//! - Per-shard LWLock tranches. M1.x has ONE global lock; M3.1 splits.
//! - `WaitLatch`-based batching (spec §1.6 step 11). M1.2 is
//!   single-backend so there's no peer push to wait for; the drain
//!   immediately processes whatever is in the ring. M3.1 introduces
//!   real batching when multi-backend pushes coexist.
//! - PocCompensatePayload field semantics (M6.1).
//! - Real cost-method dispatch + dedup-lookup (M2.x). M1.2 stamps a
//!   fixed `applied_unit_cost = 100` and writes one row per request
//!   into `poc_test_rows` to demonstrate the committer round-trip.

// M1.1 reserves several spec-defined constants (REQ_ABANDONED,
// REQ_KIND_APPLY, REQ_KIND_COMPENSATE) for M2.x / M5+ consumers. The
// dead_code allow keeps them visible without warnings until those
// milestones land.
#![allow(dead_code)]

use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{PgLwLock, pg_shmem_init};
use std::sync::atomic::{
    AtomicI32, AtomicI64, AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering,
};

// ── PoC sizing (compile-time const for M1.1) ──────────────────────────
//
// Spec §1.4 defaults are 256 × 4096 × 4096 ≈ 430 MB shmem. M1.1 uses
// reduced values so the dev cluster starts cheaply. The algorithms
// tested in M1.1 don't depend on absolute sizes; multi-shard M3/M4 has
// headroom under these values. Real GUC-driven sizing is a separate
// piece of work once the bake-off in M9 needs spec-default scale.

/// Number of shards. Spec default 256; M1.1 const = 16.
pub const POC_SHARD_COUNT: usize = 16;

/// Per-shard ring buffer capacity. Spec default 4096; M1.1 const = 512.
pub const POC_REQUESTS_PER_SHARD: usize = 512;

/// Per-shard result-slot pool size. Spec default 4096; M1.1 const = 512.
pub const POC_SLOTS_PER_SHARD: usize = 512;

/// Spec §1.6 step 5 linear-probe bound.
pub const POC_MAX_SLOT_PROBE: usize = 16;

// ── State enum values ─────────────────────────────────────────────────

/// `PocResultSlot.state` values (spec §1.4 line 344).
pub const SLOT_FREE: u8 = 0;
pub const SLOT_ALLOCATED: u8 = 1;
pub const SLOT_FILLED: u8 = 2;
pub const SLOT_ABANDONED: u8 = 3;

/// `PocPendingRequest.valid` values (spec §1.4 line 291).
pub const REQ_EMPTY: u8 = 0;
pub const REQ_FILLED: u8 = 1;
pub const REQ_ABANDONED: u8 = 2;
pub const REQ_IN_FLIGHT: u8 = 3;

/// `PocRequestKind.tag` values (spec §1.4 line 305).
pub const REQ_KIND_APPLY: u8 = 0;
pub const REQ_KIND_COMPENSATE: u8 = 1;

// ── PocQueueShard ─────────────────────────────────────────────────────
//
// Per spec §1.4 lines 267-287. Field order verbatim. `lock_tranche_id`
// is reserved for M3.1's per-shard LWLock tranche allocation;
// currently unused (single global lock).

#[repr(C, align(64))]
pub struct PocQueueShard {
    pub lock_tranche_id: AtomicU32,
    pub _pad0: [u8; 4],
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub capacity: AtomicU32,
    pub _pad1: [u8; 4],
    pub committer_pid: AtomicI32,
    pub _pad2: [u8; 4],
    pub committer_acquired_at_ns: AtomicU64,
    pub committer_tx_seq: AtomicU64,
    pub next_request_seq: AtomicU64,
    pub next_slot_seq: AtomicU64,
    /// M5c.1 (acct-4d4n.14): PID of the backend waiting on the ring
    /// for a slot to free up (backpressure). Single-slot for MVP; a
    /// drain that advances head signals this PID via SetLatch on its
    /// `PGPROC.procLatch`. Multi-waiter coalescing is acct-4d4n.14
    /// follow-up territory if M9 benches surface contention.
    pub ring_full_waiter_pid: AtomicI32,
    pub _pad3: [u8; 4],
    /// M8.1 (acct-4d4n.17): per-shard count of slot-fill errors
    /// (SlotResolution::Error in drain_and_commit's per-event loop).
    /// Narrow scope per Q4 lean: counts only plan_apply-derived errors
    /// surfaced to slots. Broader recovery / backpressure counters are
    /// M8.2's scope. Reset to 0 by `shard_reset`.
    pub error_count: AtomicU64,
}

// ── PocPendingRequest ─────────────────────────────────────────────────
//
// Per spec §1.4 lines 289-298. Body is stored as 9 opaque AtomicI64
// slots; M2.x defines Apply/Compensate variant layouts via accessor
// methods. (9 × 8 = 72 bytes covers the max payload of 56 bytes from
// either variant.)

#[repr(C, align(64))]
pub struct PocPendingRequest {
    pub valid: AtomicU8,
    pub _pad0: [u8; 7],
    pub request_seq: AtomicU64,
    pub pool_hash: AtomicU64,
    pub backend_pid: AtomicI32,
    pub slot_idx: AtomicU32,
    pub kind_tag: AtomicU8,
    pub _pad1: [u8; 7],
    pub body0: AtomicI64,
    pub body1: AtomicI64,
    pub body2: AtomicI64,
    pub body3: AtomicI64,
    pub body4: AtomicI64,
    pub body5: AtomicI64,
    pub body6: AtomicI64,
    pub body7: AtomicI64,
    pub body8: AtomicI64,
}

// ── PocResultSlot ─────────────────────────────────────────────────────
//
// Per spec §1.4 lines 342-354. `depletion_ids_inline` is fixed at 32
// entries (spec); overflow goes to spillover arena (M5c.2).
//
// `committer_tx_id` is an M1.2 audit-field addition (not in the spec's
// PocResultSlot table). It mirrors the value stamped onto each row in
// `poc_test_rows` / future `poc_cost_*` tables so callers can read it
// from the slot without a secondary SPI query. M5b recovery (which scans
// cost tables by `(user_tx_xid, committer_tx_id)` pairs) needs the same
// value reachable via the slot for the abandoned-but-committed case.

#[repr(C, align(64))]
pub struct PocResultSlot {
    pub state: AtomicU8,
    pub _pad_s: [u8; 3],
    /// PID of the backend waiting on this slot. Stamped at acquire time
    /// by the apply path; the committer reads this after `fill_slot_*`
    /// and calls `SetLatch` on the matching `PGPROC.procLatch` to wake
    /// the waiter. Zero = no waiter (push-only paths don't stamp;
    /// signaller skips). Reset to zero by `recycle_slot`/`shard_reset`.
    /// (acct-4d4n.7, M3.1)
    pub waiter_pid: AtomicI32,
    pub applied_unit_cost: AtomicI64,
    pub applied_total_cost: AtomicI64,
    pub committer_tx_id: AtomicI64,
    pub error_code: AtomicU16,
    pub _pad1: [u8; 6],
    pub depletion_count: AtomicU16,
    pub _pad2: [u8; 6],
    pub depletion_ids_inline: [AtomicI64; 32],
    pub spillover_offset: AtomicU32,
    pub _pad3: [u8; 4],
    /// Stamped at `ring_push_apply` with the request's `event_issue_id`
    /// so orphan recovery can look up cost rows for this slot by issue
    /// id (matches the dedup-lookup shape) without re-reading the
    /// ring entry — which may be a stale REQ_IN_FLIGHT pointing past
    /// head. Zero = no request pushed yet for this slot.
    /// (acct-4d4n.10, M5a.1)
    pub issue_id: AtomicI64,
    /// Stamped at `ring_push_apply` with the request's `request_seq`.
    /// Disambiguates "slot has a live request" from "slot was acquired
    /// but the caller errored before push". Cleared at recycle.
    /// (acct-4d4n.10, M5a.1)
    pub current_request_seq: AtomicU64,
    /// Wall-clock at `acquire_slot` time (nanoseconds since UNIX
    /// epoch). The M5c.2 slot-leak audit consults this to identify
    /// SLOT_ALLOCATED entries whose age exceeds
    /// `poc_ledger.slot_audit_min_age_ms` AND whose `waiter_pid` is
    /// dead, and reclaims them. Stamped exactly once per acquire;
    /// cleared at recycle.
    /// (acct-4d4n.15, M5c.2)
    pub acquired_at_ns: AtomicU64,
    /// Wall-clock at the moment the committer stamps SLOT_FILLED and
    /// is about to `SetLatch` the waiter. The waiting backend reads
    /// this AFTER its `WaitLatch` returns and buckets
    /// `now_ns - set_latched_at_ns` into the B5 wake-latency
    /// histogram. Zero = no committer-side latch yet stamped (push-only
    /// paths, ABANDONED slots). Cleared at recycle.
    /// (acct-4d4n.19, M8.3)
    pub set_latched_at_ns: AtomicU64,
}

// ── Arena (entire shmem segment, one PgLwLock) ────────────────────────

#[repr(C)]
pub struct PocShardArena {
    pub shards: [PocQueueShard; POC_SHARD_COUNT],
    pub requests: [[PocPendingRequest; POC_REQUESTS_PER_SHARD]; POC_SHARD_COUNT],
    pub slots: [[PocResultSlot; POC_SLOTS_PER_SHARD]; POC_SHARD_COUNT],
}

impl Default for PocShardArena {
    fn default() -> Self {
        // Per ledger-extension's h_layer_arena pattern: mem::zeroed for
        // large shmem structs avoids stack-materializing the value.
        // All atomic primitives have well-defined zero bit patterns and
        // the zero values match the desired initial state (SLOT_FREE = 0,
        // REQ_EMPTY = 0, head = tail = 0, etc.).
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for PocShardArena {}

pub static POC_SHARD_ARENA: PgLwLock<PocShardArena> =
    unsafe { PgLwLock::new(c"poc_shard_arena") };

// ── M8.1 (acct-4d4n.17) PocMethodStatsArena ──────────────────────────
//
// Top-level per-method telemetry (Q2 lean: top-level — methods are
// global, the 4-element table is ~1 KiB vs ~16 KiB if per-shard ×
// per-method). 30 exponential latency buckets per method cover
// elapsed_ns ∈ [1, 2^30) split as bucket_i = floor(log2(elapsed_ns));
// values ≥ 2^29 ns (≈ 537 ms) saturate into bucket 29.
//
// All fields are AtomicU64; updates are wait-free fetch_add. Readers
// snapshot under acquire ordering without locking — small skew across
// the (dispatch_count, error_count, buckets) tuple is acceptable for
// telemetry-grade observability.

pub const POC_METHOD_COUNT: usize = 4;
pub const POC_LATENCY_BUCKETS: usize = 30;

#[repr(C, align(64))]
pub struct PocMethodStats {
    /// Total events the method has processed (per-event count; each
    /// event in a batch counts independently). Drives error_rate
    /// denominator.
    pub dispatch_count: AtomicU64,
    /// Total events the method returned with `error.is_some()`. Narrow
    /// per Q4 lean — does not include orphan recovery, ring-full,
    /// takeover, or other transport-level failures (M8.2's scope).
    pub error_count: AtomicU64,
    /// log2-ns latency histogram. Each `plan_apply` call increments the
    /// bucket matching its elapsed wall time. Total call_count =
    /// SUM(buckets).
    pub latency_buckets: [AtomicU64; POC_LATENCY_BUCKETS],
}

#[repr(C)]
pub struct PocMethodStatsArena {
    pub methods: [PocMethodStats; POC_METHOD_COUNT],
}

impl Default for PocMethodStatsArena {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for PocMethodStatsArena {}

pub static POC_METHOD_STATS_ARENA: PgLwLock<PocMethodStatsArena> =
    unsafe { PgLwLock::new(c"poc_method_stats_arena") };

/// Wire up all four shmem segments. Called from `_PG_init`.
pub fn init() {
    pg_shmem_init!(POC_SHARD_ARENA);
    pg_shmem_init!(POC_METHOD_STATS_ARENA);
    pg_shmem_init!(POC_RECOVERY_STATS);
    pg_shmem_init!(POC_BOTTLENECK_STATS);
}

/// Map an elapsed_ns to its log2 bucket index in [0, POC_LATENCY_BUCKETS).
fn latency_bucket(elapsed_ns: u64) -> usize {
    if elapsed_ns < 2 {
        return 0;
    }
    let bits = 63 - elapsed_ns.leading_zeros() as usize; // floor(log2)
    if bits >= POC_LATENCY_BUCKETS {
        POC_LATENCY_BUCKETS - 1
    } else {
        bits
    }
}

/// Record one `plan_apply` call's outcome: bump per-event dispatch +
/// error counters, then bucket the elapsed wall time. `dispatch_n` and
/// `error_n` are per-event counts (a single plan_apply may process N
/// events with K errors). `method_tag` out-of-range is silently
/// dropped — method_tag is bounded by the push path.
pub fn record_dispatch(method_tag: u8, elapsed_ns: u64, dispatch_n: u64, error_n: u64) {
    let idx = method_tag as usize;
    if idx >= POC_METHOD_COUNT {
        return;
    }
    let arena = POC_METHOD_STATS_ARENA.share();
    let m = &arena.methods[idx];
    if dispatch_n > 0 {
        m.dispatch_count.fetch_add(dispatch_n, Ordering::AcqRel);
    }
    if error_n > 0 {
        m.error_count.fetch_add(error_n, Ordering::AcqRel);
    }
    let bucket = latency_bucket(elapsed_ns);
    m.latency_buckets[bucket].fetch_add(1, Ordering::AcqRel);
}

/// Snapshot one method's (dispatch_count, error_count, p50_ns, p99_ns).
/// Percentiles return 0 when no call has been recorded yet. p50/p99 are
/// the bucket upper bound (2^(bucket+1)) so successive ranks compare
/// monotonically; absolute accuracy is within 2× per bucket width.
pub fn read_method_stats(method_tag: u8) -> (u64, u64, u64, u64) {
    let idx = method_tag as usize;
    if idx >= POC_METHOD_COUNT {
        return (0, 0, 0, 0);
    }
    let arena = POC_METHOD_STATS_ARENA.share();
    let m = &arena.methods[idx];
    let dispatch = m.dispatch_count.load(Ordering::Acquire);
    let errors = m.error_count.load(Ordering::Acquire);
    let buckets: [u64; POC_LATENCY_BUCKETS] = std::array::from_fn(|i| {
        m.latency_buckets[i].load(Ordering::Acquire)
    });
    let total: u64 = buckets.iter().sum();
    let (p50, p99) = if total == 0 {
        (0, 0)
    } else {
        let target_p50 = total.div_ceil(2);
        let target_p99 = ((total as u128 * 99 + 99) / 100) as u64;
        let mut cum: u64 = 0;
        let mut p50_b: usize = 0;
        let mut p99_b: usize = 0;
        let mut p50_set = false;
        let mut p99_set = false;
        for (i, &v) in buckets.iter().enumerate() {
            cum = cum.saturating_add(v);
            if !p50_set && cum >= target_p50 {
                p50_b = i;
                p50_set = true;
            }
            if !p99_set && cum >= target_p99 {
                p99_b = i;
                p99_set = true;
            }
        }
        // Bucket upper bound: 2^(bucket+1). Clamp the top bucket so we
        // don't overflow u64 on bucket 29.
        let bound = |b: usize| -> u64 {
            if b + 1 >= 63 {
                u64::MAX
            } else {
                1u64 << (b + 1)
            }
        };
        (bound(p50_b), bound(p99_b))
    };
    (dispatch, errors, p50, p99)
}

/// M8.1: bump per-shard error_count by 1 for each slot-fill error.
pub fn bump_shard_error(shard_idx: usize, n: u64) {
    if n == 0 {
        return;
    }
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .error_count
        .fetch_add(n, Ordering::AcqRel);
}

/// M8.1: read per-shard error_count.
pub fn read_shard_error_count(shard_idx: usize) -> u64 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .error_count
        .load(Ordering::Acquire)
}

/// M8.1: zero all per-method shmem counters. Mirrors the
/// `shard_reset` semantics for the per-shard ring/slots. Useful for
/// bench harnesses that need a clean baseline between cases.
pub fn method_stats_reset() {
    let arena = POC_METHOD_STATS_ARENA.share();
    for m in &arena.methods {
        m.dispatch_count.store(0, Ordering::Release);
        m.error_count.store(0, Ordering::Release);
        for b in &m.latency_buckets {
            b.store(0, Ordering::Release);
        }
    }
}

// ── M8.2 (acct-4d4n.18) PocRecoveryStats ─────────────────────────────
//
// Separate top-level arena from PocMethodStatsArena per Q1 lean: these
// counters are operationally distinct (transport / recovery events,
// not per-method dispatch). Clean reset boundary.
//
// Four global AtomicU64 counters + per-shard ring of recent batch
// sizes. Ring is fixed POC_BATCH_SAMPLE_WINDOW = 16 entries per Q3
// (powers-of-2 modulo, sufficient resolution for telemetry).

pub const POC_BATCH_SAMPLE_WINDOW: usize = 16;

#[repr(C, align(64))]
pub struct PocPerShardBatchRing {
    /// Monotonic write index; bucket = (head as usize) & MASK.
    pub head: AtomicU32,
    pub _pad: [u8; 4],
    pub samples: [AtomicU32; POC_BATCH_SAMPLE_WINDOW],
}

#[repr(C)]
pub struct PocRecoveryStats {
    /// Number of `ring_push_apply` calls that entered the WaitLatch
    /// backpressure loop at least once. Per Q4 lean: counts waiter
    /// instances (one bump per apply call that went to sleep), not
    /// wake cycles.
    pub backpressure_count: AtomicU64,
    /// Number of `orphan_recovery` invocations that reclaimed ≥1 slot —
    /// i.e. how many committer-tx aborts the recovery path observed
    /// and patched up. Per Q5 lean: SPI/sub-tx aborts only; SLOT_ABANDONED
    /// CAS misses on the cancel path are NOT counted here (those are
    /// the normal cancel-cleanup flow, not committer failures).
    pub committer_tx_failures: AtomicU64,
    /// Number of compensations the startup-recovery worker (Phase B)
    /// enqueued for aborted user-tx xids it found by scanning cost
    /// tables. Bumped once per `ring_push_compensate` call.
    pub orphan_compensations: AtomicU64,
    /// Number of successful `try_acquire_or_takeover` takeovers
    /// (TakeoverOutcome::TookOver). Counted at the function boundary
    /// so every caller (apply path, bench helper, slot-audit) attributes
    /// uniformly.
    pub lease_takeovers: AtomicU64,
    /// Per-shard rolling-window batch-size samples. Drain calls write
    /// at `head++ & MASK`; readers compute mean over POC_BATCH_SAMPLE_WINDOW
    /// most-recent non-zero samples (or zero if window is empty).
    pub batch_rings: [PocPerShardBatchRing; POC_SHARD_COUNT],
}

impl Default for PocRecoveryStats {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for PocRecoveryStats {}

pub static POC_RECOVERY_STATS: PgLwLock<PocRecoveryStats> =
    unsafe { PgLwLock::new(c"poc_recovery_stats") };

pub fn bump_backpressure() {
    POC_RECOVERY_STATS
        .share()
        .backpressure_count
        .fetch_add(1, Ordering::AcqRel);
}

pub fn bump_committer_tx_failure() {
    POC_RECOVERY_STATS
        .share()
        .committer_tx_failures
        .fetch_add(1, Ordering::AcqRel);
}

pub fn bump_orphan_compensation() {
    POC_RECOVERY_STATS
        .share()
        .orphan_compensations
        .fetch_add(1, Ordering::AcqRel);
}

pub fn bump_lease_takeover() {
    POC_RECOVERY_STATS
        .share()
        .lease_takeovers
        .fetch_add(1, Ordering::AcqRel);
}

pub fn read_backpressure_count() -> u64 {
    POC_RECOVERY_STATS
        .share()
        .backpressure_count
        .load(Ordering::Acquire)
}

pub fn read_committer_tx_failures() -> u64 {
    POC_RECOVERY_STATS
        .share()
        .committer_tx_failures
        .load(Ordering::Acquire)
}

pub fn read_orphan_compensations() -> u64 {
    POC_RECOVERY_STATS
        .share()
        .orphan_compensations
        .load(Ordering::Acquire)
}

pub fn read_lease_takeovers() -> u64 {
    POC_RECOVERY_STATS
        .share()
        .lease_takeovers
        .load(Ordering::Acquire)
}

/// Record one drain's batch size for the per-shard rolling window.
/// Drain calls of size 0 are skipped (no work means no representative
/// sample — they'd dilute the mean).
pub fn record_batch_size(shard_idx: usize, batch_size: u32) {
    if batch_size == 0 || shard_idx >= POC_SHARD_COUNT {
        return;
    }
    let ring = &POC_RECOVERY_STATS.share().batch_rings[shard_idx];
    let h = ring.head.fetch_add(1, Ordering::AcqRel);
    let idx = (h as usize) & (POC_BATCH_SAMPLE_WINDOW - 1);
    ring.samples[idx].store(batch_size, Ordering::Release);
}

/// Read the per-shard mean batch size. Returns 0 when the ring is
/// empty (no drains recorded). Mean is over only the non-zero samples
/// (a fresh ring starts at all zeros; partial fills don't dilute).
pub fn read_avg_batch_size(shard_idx: usize) -> u32 {
    if shard_idx >= POC_SHARD_COUNT {
        return 0;
    }
    let ring = &POC_RECOVERY_STATS.share().batch_rings[shard_idx];
    let mut total: u64 = 0;
    let mut count: u64 = 0;
    for s in &ring.samples {
        let v = s.load(Ordering::Acquire);
        if v > 0 {
            total += v as u64;
            count += 1;
        }
    }
    if count == 0 {
        0
    } else {
        (total / count) as u32
    }
}

/// Zero all recovery stats counters + batch-size rings. Bench harness
/// helper; mirrors `method_stats_reset`.
pub fn recovery_stats_reset() {
    let arena = POC_RECOVERY_STATS.share();
    arena.backpressure_count.store(0, Ordering::Release);
    arena.committer_tx_failures.store(0, Ordering::Release);
    arena.orphan_compensations.store(0, Ordering::Release);
    arena.lease_takeovers.store(0, Ordering::Release);
    for ring in &arena.batch_rings {
        ring.head.store(0, Ordering::Release);
        for s in &ring.samples {
            s.store(0, Ordering::Release);
        }
    }
}

// ── M8.3 (acct-4d4n.19) PocBottleneckStats ───────────────────────────
//
// Two extension-tracked dimensions for the bake-off classifier:
//
//   B3 (plan_apply CPU): cumulative ns spent inside method.plan_apply
//     since the last reset. Already bucketed in PocMethodStatsArena
//     for p50/p99, but the classifier needs the SUM for the share
//     calculation (B3_ns / wall_ms × 1e6 = B3 fraction of cell wall time).
//
//   B5 (wake latency): per-sample histogram of `now_ns - set_latched_at_ns`
//     captured by waiters after WaitLatch returns. 30 log2-ns buckets
//     mirroring PocMethodStats so percentile math is uniform.
//
// B1 (LWLock) and B2 (WAL fsync) are caller-sampled — the bench harness
// reads `pg_stat_activity` / `pg_stat_wal` directly. No extension state.

pub const POC_B5_BUCKETS: usize = 30;

#[repr(C)]
pub struct PocBottleneckStats {
    /// Cumulative ns spent inside plan_apply since reset (B3 source).
    pub plan_apply_total_ns: AtomicU64,
    /// Cumulative ns observed in wake-latency samples (B5 source for
    /// mean = total / count).
    pub wake_total_ns: AtomicU64,
    /// Number of wake-latency samples recorded.
    pub wake_sample_count: AtomicU64,
    /// 30 log2-ns buckets for wake latency, identical scheme to
    /// PocMethodStats.latency_buckets.
    pub wake_buckets: [AtomicU64; POC_B5_BUCKETS],
}

impl Default for PocBottleneckStats {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for PocBottleneckStats {}

pub static POC_BOTTLENECK_STATS: PgLwLock<PocBottleneckStats> =
    unsafe { PgLwLock::new(c"poc_bottleneck_stats") };

/// B3 add: accumulate ns into plan_apply_total_ns. Called from
/// process_group after Instant::now() delta capture (M8.1 already
/// records per-bucket; this is the parallel SUM for the share calc).
pub fn record_b3_total_ns(elapsed_ns: u64) {
    POC_BOTTLENECK_STATS
        .share()
        .plan_apply_total_ns
        .fetch_add(elapsed_ns, Ordering::AcqRel);
}

/// B5 add: record one wake-latency sample. Called from the apply path
/// after WaitLatch returns and the slot is observed SLOT_FILLED.
pub fn record_b5_wake_sample(elapsed_ns: u64) {
    let arena = POC_BOTTLENECK_STATS.share();
    arena.wake_total_ns.fetch_add(elapsed_ns, Ordering::AcqRel);
    arena.wake_sample_count.fetch_add(1, Ordering::AcqRel);
    let bucket = latency_bucket(elapsed_ns);
    let b_idx = bucket.min(POC_B5_BUCKETS - 1);
    arena.wake_buckets[b_idx].fetch_add(1, Ordering::AcqRel);
}

/// Read B3 cumulative ns.
pub fn read_b3_total_ns() -> u64 {
    POC_BOTTLENECK_STATS
        .share()
        .plan_apply_total_ns
        .load(Ordering::Acquire)
}

/// Read B5 stats: (total_ns, sample_count, p50_ns, p99_ns). Percentiles
/// return 0 when no samples; bucket upper bound (2^(bucket+1)) used so
/// readings compare monotonically with M8.1 method stats.
pub fn read_b5_wake_stats() -> (u64, u64, u64, u64) {
    let arena = POC_BOTTLENECK_STATS.share();
    let total_ns = arena.wake_total_ns.load(Ordering::Acquire);
    let count = arena.wake_sample_count.load(Ordering::Acquire);
    let buckets: [u64; POC_B5_BUCKETS] = std::array::from_fn(|i| {
        arena.wake_buckets[i].load(Ordering::Acquire)
    });
    let total_buckets: u64 = buckets.iter().sum();
    let (p50, p99) = if total_buckets == 0 {
        (0, 0)
    } else {
        let target_p50 = total_buckets.div_ceil(2);
        let target_p99 = ((total_buckets as u128 * 99 + 99) / 100) as u64;
        let mut cum: u64 = 0;
        let mut p50_b: usize = 0;
        let mut p99_b: usize = 0;
        let mut p50_set = false;
        let mut p99_set = false;
        for (i, &v) in buckets.iter().enumerate() {
            cum = cum.saturating_add(v);
            if !p50_set && cum >= target_p50 {
                p50_b = i;
                p50_set = true;
            }
            if !p99_set && cum >= target_p99 {
                p99_b = i;
                p99_set = true;
            }
        }
        let bound = |b: usize| -> u64 {
            if b + 1 >= 63 { u64::MAX } else { 1u64 << (b + 1) }
        };
        (bound(p50_b), bound(p99_b))
    };
    (total_ns, count, p50, p99)
}

/// Bench-harness helper: zero B3/B5 counters + buckets.
pub fn bottleneck_stats_reset() {
    let arena = POC_BOTTLENECK_STATS.share();
    arena.plan_apply_total_ns.store(0, Ordering::Release);
    arena.wake_total_ns.store(0, Ordering::Release);
    arena.wake_sample_count.store(0, Ordering::Release);
    for b in &arena.wake_buckets {
        b.store(0, Ordering::Release);
    }
}

/// Stamp `set_latched_at_ns` on a slot just before the committer fires
/// `SetLatch` on the waiter. The waiter reads this after `WaitLatch`
/// returns and computes the roundtrip latency for B5.
pub fn stamp_set_latched_at_ns(shard_idx: usize, slot_idx: u32, now_ns: u64) {
    if shard_idx >= POC_SHARD_COUNT {
        return;
    }
    let arena = POC_SHARD_ARENA.share();
    arena.slots[shard_idx][slot_idx as usize]
        .set_latched_at_ns
        .store(now_ns, Ordering::Release);
}

/// Read the set_latched_at_ns timestamp. Returns 0 if no committer
/// has latched the slot yet.
pub fn read_set_latched_at_ns(shard_idx: usize, slot_idx: u32) -> u64 {
    if shard_idx >= POC_SHARD_COUNT {
        return 0;
    }
    let arena = POC_SHARD_ARENA.share();
    arena.slots[shard_idx][slot_idx as usize]
        .set_latched_at_ns
        .load(Ordering::Acquire)
}

/// Stamp `capacity` on each shard once shmem is up. Called from a
/// backend's first touch (idempotent — CAS-set-if-zero).
fn ensure_capacity_stamped() {
    let arena = POC_SHARD_ARENA.share();
    for shard in &arena.shards {
        // Race-free: many backends may attempt this; CAS-from-0 succeeds
        // exactly once.
        let _ = shard.capacity.compare_exchange(
            0,
            POC_REQUESTS_PER_SHARD as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }
}

// ── Slot allocation ───────────────────────────────────────────────────
//
// Spec §1.6 step 5: atomic fetch_add reserves a slot index seed;
// CAS `slot.state` from free → allocated; on CAS failure, fetch_add
// again (linear probe up to POC_MAX_SLOT_PROBE = 16). All probes
// exhausted → caller signals slot-pressure (backpressure in M5c.1).

/// Acquire a free slot. Returns `Some(slot_idx)` or `None` if 16
/// consecutive probes hit non-free slots.
pub fn acquire_slot(shard_idx: usize) -> Option<u32> {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    let slots = &arena.slots[shard_idx];
    for _probe in 0..POC_MAX_SLOT_PROBE {
        let seq = shard.next_slot_seq.fetch_add(1, Ordering::AcqRel);
        let idx = (seq as usize) % POC_SLOTS_PER_SHARD;
        if slots[idx]
            .state
            .compare_exchange(
                SLOT_FREE,
                SLOT_ALLOCATED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // M5c.2 (acct-4d4n.15): stamp the acquire timestamp so the
            // slot-leak audit can age the slot. Using SystemTime is
            // robust across postmaster restart (the audit's age
            // threshold is a relative ms count; both sides read the
            // same wall clock).
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            slots[idx]
                .acquired_at_ns
                .store(now_ns, Ordering::Release);
            return Some(idx as u32);
        }
    }
    None
}

/// Recycle a slot from a terminal state back to free. Returns the
/// previous state on success, or `Err(actual)` if the slot wasn't in a
/// terminal state.
pub fn recycle_slot(shard_idx: usize, slot_idx: u32) -> Result<u8, u8> {
    let arena = POC_SHARD_ARENA.share();
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    let cur = slot.state.load(Ordering::Acquire);
    if cur != SLOT_FILLED && cur != SLOT_ABANDONED {
        return Err(cur);
    }
    // Zero per-slot result fields before flipping state — a subsequent
    // acquirer reads a clean slate.
    slot.applied_unit_cost.store(0, Ordering::Release);
    slot.applied_total_cost.store(0, Ordering::Release);
    slot.committer_tx_id.store(0, Ordering::Release);
    slot.error_code.store(0, Ordering::Release);
    slot.depletion_count.store(0, Ordering::Release);
    slot.spillover_offset.store(0, Ordering::Release);
    slot.waiter_pid.store(0, Ordering::Release);
    slot.issue_id.store(0, Ordering::Release);
    slot.current_request_seq.store(0, Ordering::Release);
    slot.acquired_at_ns.store(0, Ordering::Release);
    slot.set_latched_at_ns.store(0, Ordering::Release);
    for i in 0..32 {
        slot.depletion_ids_inline[i].store(0, Ordering::Release);
    }
    slot.state.store(SLOT_FREE, Ordering::Release);
    Ok(cur)
}

/// Stamp the PID of the waiter on a slot. Called by the apply path
/// right after `acquire_slot` so the committer can `SetLatch` the
/// waiter once the slot is filled (spec §1.6 step 11/18 — wait/wake).
/// Push-only and other non-waiting paths skip this call; the field
/// stays 0 and the signaller treats 0 as "no waiter".
/// (acct-4d4n.7, M3.1)
pub fn set_slot_waiter_pid(shard_idx: usize, slot_idx: u32, pid: i32) {
    let arena = POC_SHARD_ARENA.share();
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    slot.waiter_pid.store(pid, Ordering::Release);
}

/// Read the waiter PID stamped on a slot. Returns 0 if no waiter.
/// (acct-4d4n.7, M3.1)
pub fn read_slot_waiter_pid(shard_idx: usize, slot_idx: u32) -> i32 {
    let arena = POC_SHARD_ARENA.share();
    arena.slots[shard_idx][slot_idx as usize]
        .waiter_pid
        .load(Ordering::Acquire)
}

/// M5c.1 (acct-4d4n.14): claim the per-shard ring-full waiter slot via
/// CAS-from-0. Returns Ok(()) on claim; Err(prior_pid) if another
/// backend already occupies the slot. Single-waiter-per-shard at MVP;
/// a contender that loses the CAS falls back to the WaitLatch timeout
/// loop and retries the push, which is correct if eventually-slow.
pub fn set_ring_full_waiter(shard_idx: usize, pid: i32) -> Result<(), i32> {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    shard
        .ring_full_waiter_pid
        .compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire)
        .map(|_| ())
        .map_err(|actual| actual)
}

/// Clear the per-shard ring-full waiter slot IFF this backend owns it.
/// Safe to call even if another backend has it (CAS-from-`my_pid`
/// silently fails). Idempotent: clearing an already-zero slot returns
/// Err but is harmless.
pub fn clear_ring_full_waiter_if_self(shard_idx: usize, my_pid: i32) {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    let _ = shard.ring_full_waiter_pid.compare_exchange(
        my_pid,
        0,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
}

/// Read the per-shard ring-full waiter PID. Returns 0 if no waiter.
pub fn read_ring_full_waiter(shard_idx: usize) -> i32 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .ring_full_waiter_pid
        .load(Ordering::Acquire)
}

pub fn mark_slot_filled(shard_idx: usize, slot_idx: u32) -> Result<(), u8> {
    let arena = POC_SHARD_ARENA.share();
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    slot.state
        .compare_exchange(
            SLOT_ALLOCATED,
            SLOT_FILLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map(|_| ())
        .map_err(|actual| actual)
}

pub fn mark_slot_abandoned(shard_idx: usize, slot_idx: u32) -> Result<(), u8> {
    let arena = POC_SHARD_ARENA.share();
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    slot.state
        .compare_exchange(
            SLOT_ALLOCATED,
            SLOT_ABANDONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map(|_| ())
        .map_err(|actual| actual)
}

pub fn slot_state(shard_idx: usize, slot_idx: u32) -> u8 {
    let arena = POC_SHARD_ARENA.share();
    arena.slots[shard_idx][slot_idx as usize]
        .state
        .load(Ordering::Acquire)
}

// ── Ring buffer push / drain ──────────────────────────────────────────
//
// Spec §1.6 steps 6-9 (push under EXCLUSIVE LWLock; CAS tail forward)
// and 12-14 (drain: walk [head, tail), copy out + mark in_flight,
// advance head). M1.1 drains one request at a time for test purposes;
// M1.2 implements the full committer batch drain.

/// Push a request onto the shard's ring. `Ok(request_seq)` on success,
/// `Err(())` if the ring is full.
pub fn ring_push(
    shard_idx: usize,
    slot_idx: u32,
    pool_hash: u64,
    kind_tag: u8,
    backend_pid: i32,
) -> Result<u64, ()> {
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    let head = shard.head.load(Ordering::Acquire);
    let tail = shard.tail.load(Ordering::Acquire);
    if tail.wrapping_sub(head) >= POC_REQUESTS_PER_SHARD as u32 {
        return Err(());
    }
    let idx = (tail as usize) % POC_REQUESTS_PER_SHARD;
    let req = &arena.requests[shard_idx][idx];
    let req_seq = shard.next_request_seq.fetch_add(1, Ordering::AcqRel);
    req.request_seq.store(req_seq, Ordering::Release);
    req.pool_hash.store(pool_hash, Ordering::Release);
    req.backend_pid.store(backend_pid, Ordering::Release);
    req.slot_idx.store(slot_idx, Ordering::Release);
    req.kind_tag.store(kind_tag, Ordering::Release);
    req.body0.store(0, Ordering::Release);
    req.body1.store(0, Ordering::Release);
    req.body2.store(0, Ordering::Release);
    req.body3.store(0, Ordering::Release);
    req.body4.store(0, Ordering::Release);
    req.body5.store(0, Ordering::Release);
    req.body6.store(0, Ordering::Release);
    req.body7.store(0, Ordering::Release);
    req.body8.store(0, Ordering::Release);
    // valid stamped last — establishes happens-before for any future
    // reader observing `valid == REQ_FILLED`.
    req.valid.store(REQ_FILLED, Ordering::Release);
    shard.tail.store(tail.wrapping_add(1), Ordering::Release);
    Ok(req_seq)
}

/// Drain one request (oldest first), mark it in-flight, advance head.
/// Returns `Some(slot_idx)` of the drained request, `None` if empty.
pub fn ring_drain_one(shard_idx: usize) -> Option<u32> {
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    let head = shard.head.load(Ordering::Acquire);
    let tail = shard.tail.load(Ordering::Acquire);
    if head == tail {
        return None;
    }
    let idx = (head as usize) % POC_REQUESTS_PER_SHARD;
    let req = &arena.requests[shard_idx][idx];
    let slot_idx = req.slot_idx.load(Ordering::Acquire);
    req.valid.store(REQ_IN_FLIGHT, Ordering::Release);
    shard.head.store(head.wrapping_add(1), Ordering::Release);
    Some(slot_idx)
}

// ── Apply-variant body layout ─────────────────────────────────────────
//
// Per spec §1.4 lines 316-329. M1.2 introduces the first concrete body
// layout. M2.x will likely refine field meanings (e.g., add issue_id
// generation) but the slot offsets stay stable.
//
//   body0 → method_tag (0 = FIFO; spec §1.4 line 305 enumerates kinds)
//   body1 → event_qty (signed; positive = receipt, negative = issue)
//   body2 → event_at_micros (caller-supplied timestamp; M1.2 = 0)
//   body3 → event_issue_id (M1.2 = 0; M2.x: caller-allocated)
//   body4 → event_sku_id
//   body5 → event_location_id
//   body6 → user_tx_xid (TransactionId as i64; M5b needs this)
//   body7, body8 → reserved for M2.x

/// Spec §1.4 line 305 — `PocRequestKind` discriminator for cost methods.
/// M1.2 only emits `FIFO` (0). M2.2/M2.3 add AVG/STD.
pub const METHOD_FIFO: u8 = 0;
pub const METHOD_AVG: u8 = 1;
pub const METHOD_STD: u8 = 2;
// M2.1 test-fixture tag. Routed only via the "mock" caller-supplied
// name. Kept distinct from FIFO so M2.2's FifoMethod swap doesn't
// capture mock callers.
pub const METHOD_MOCK: u8 = 3;

/// Snapshot of an Apply-variant request copied out of the ring under
/// LWLock. The committer iterates these without re-touching shmem.
#[derive(Debug, Clone)]
pub struct DrainedApply {
    pub slot_idx: u32,
    pub request_seq: u64,
    pub pool_hash: u64,
    pub backend_pid: i32,
    pub method_tag: u8,
    pub event_qty: i64,
    pub event_at_micros: i64,
    pub event_issue_id: i64,
    pub event_sku_id: i64,
    pub event_location_id: i64,
    pub user_tx_xid: i64,
}

/// Push an Apply request onto the ring with the M1.2 body layout. Same
/// fundamental shape as `ring_push` (LWLock, head/tail check, valid
/// stamped LAST) but writes the Apply payload into body0..body6.
#[allow(clippy::too_many_arguments)]
pub fn ring_push_apply(
    shard_idx: usize,
    slot_idx: u32,
    pool_hash: u64,
    backend_pid: i32,
    method_tag: u8,
    event_qty: i64,
    event_at_micros: i64,
    event_issue_id: i64,
    event_sku_id: i64,
    event_location_id: i64,
    user_tx_xid: i64,
) -> Result<u64, ()> {
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    let head = shard.head.load(Ordering::Acquire);
    let tail = shard.tail.load(Ordering::Acquire);
    if tail.wrapping_sub(head) >= POC_REQUESTS_PER_SHARD as u32 {
        return Err(());
    }
    let idx = (tail as usize) % POC_REQUESTS_PER_SHARD;
    let req = &arena.requests[shard_idx][idx];
    let req_seq = shard.next_request_seq.fetch_add(1, Ordering::AcqRel);
    req.request_seq.store(req_seq, Ordering::Release);
    req.pool_hash.store(pool_hash, Ordering::Release);
    req.backend_pid.store(backend_pid, Ordering::Release);
    req.slot_idx.store(slot_idx, Ordering::Release);
    req.kind_tag.store(REQ_KIND_APPLY, Ordering::Release);
    // Apply payload — see body layout note above.
    req.body0.store(method_tag as i64, Ordering::Release);
    req.body1.store(event_qty, Ordering::Release);
    req.body2.store(event_at_micros, Ordering::Release);
    req.body3.store(event_issue_id, Ordering::Release);
    req.body4.store(event_sku_id, Ordering::Release);
    req.body5.store(event_location_id, Ordering::Release);
    req.body6.store(user_tx_xid, Ordering::Release);
    req.body7.store(0, Ordering::Release);
    req.body8.store(0, Ordering::Release);
    // M5a.1 (acct-4d4n.10): also stamp the slot with issue_id and the
    // request_seq so orphan recovery can find this slot's work without
    // re-reading the ring (which can return stale REQ_IN_FLIGHT
    // entries from prior cycles).
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    slot.issue_id.store(event_issue_id, Ordering::Release);
    slot.current_request_seq.store(req_seq, Ordering::Release);
    // valid stamped LAST — establishes happens-before for the committer
    // reader observing REQ_FILLED, matching ring_push's discipline.
    req.valid.store(REQ_FILLED, Ordering::Release);
    shard.tail.store(tail.wrapping_add(1), Ordering::Release);
    Ok(req_seq)
}

/// Drain up to `max_batch` Apply requests from the ring under one
/// LWLock-EXCLUSIVE critical section. Spec §1.6 steps 12-15: walk
/// [head, tail), copy out, mark each `REQ_IN_FLIGHT`, advance head.
///
/// Returns the drained requests in arrival order. Caller is responsible
/// for releasing the LWLock implicitly via the function return (the
/// `exclusive()` guard is scoped to the inner block); subsequent SPI
/// work runs OUTSIDE the LWLock per spec discipline (LWLocks must not
/// be held across SPI calls — see PG src/backend/storage/lmgr/lwlock.c
/// notes on subsystem reentrancy).
pub fn drain_apply_batch(shard_idx: usize, max_batch: usize) -> Vec<DrainedApply> {
    let mut out: Vec<DrainedApply> = Vec::new();
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    let head_start = shard.head.load(Ordering::Acquire);
    let tail = shard.tail.load(Ordering::Acquire);
    let available = tail.wrapping_sub(head_start) as usize;
    let n = available.min(max_batch);
    if n == 0 {
        return out;
    }
    // M6.1 (acct-4d4n.16): stop at the first non-APPLY entry rather
    // than walk past it. Compensate-kind entries stay at the head for
    // `drain_compensate_batch` to pick up. Advancing head only by the
    // contiguous APPLY prefix preserves FIFO ordering between APPLY
    // and COMPENSATE in the same shard.
    let mut consumed: u32 = 0;
    for i in 0..n {
        let pos = head_start.wrapping_add(i as u32);
        let idx = (pos as usize) % POC_REQUESTS_PER_SHARD;
        let req = &arena.requests[shard_idx][idx];
        // Skip entries whose `valid` is still REQ_EMPTY — covers the
        // M5c.1 synthetic ring-full test surface (which advances tail
        // without writing) and any future tail-stamp race between
        // `ring_push_apply` and a competing drain. Real producers
        // stamp `valid = REQ_FILLED` BEFORE advancing tail. Advance
        // head past these too (otherwise the synthetic entry would
        // wedge the ring permanently).
        let v = req.valid.load(Ordering::Acquire);
        if v == REQ_EMPTY {
            consumed = consumed.wrapping_add(1);
            continue;
        }
        let kind = req.kind_tag.load(Ordering::Acquire);
        if kind != REQ_KIND_APPLY {
            // M6.1: stop here. Don't touch valid; don't advance head
            // past this entry. The Compensate drain will process it.
            break;
        }
        out.push(DrainedApply {
            slot_idx: req.slot_idx.load(Ordering::Acquire),
            request_seq: req.request_seq.load(Ordering::Acquire),
            pool_hash: req.pool_hash.load(Ordering::Acquire),
            backend_pid: req.backend_pid.load(Ordering::Acquire),
            method_tag: req.body0.load(Ordering::Acquire) as u8,
            event_qty: req.body1.load(Ordering::Acquire),
            event_at_micros: req.body2.load(Ordering::Acquire),
            event_issue_id: req.body3.load(Ordering::Acquire),
            event_sku_id: req.body4.load(Ordering::Acquire),
            event_location_id: req.body5.load(Ordering::Acquire),
            user_tx_xid: req.body6.load(Ordering::Acquire),
        });
        req.valid.store(REQ_IN_FLIGHT, Ordering::Release);
        consumed = consumed.wrapping_add(1);
    }
    shard
        .head
        .store(head_start.wrapping_add(consumed), Ordering::Release);
    out
}

// ── M6.1 (acct-4d4n.16) compensate-variant body layout ───────────────
//
// Per spec §1.4 lines 331-342 / §1.7. Compensate entries are
// fire-and-forget — no slot is allocated and no waiter is registered
// because the user backend that triggered the abort has already moved
// on (or died). The committer drains them and INSERTs reversal rows
// without writing back to a slot.
//
//   body0 → user_tx_xid (TransactionId as i64)
//   body1..8 → reserved (zero)

#[derive(Debug, Clone)]
pub struct DrainedCompensate {
    pub request_seq: u64,
    pub user_tx_xid: i64,
}

/// Push a Compensate request onto a shard's ring. Caller picks shard
/// via `hash(user_tx_xid) % POC_SHARD_COUNT` to spread load; correctness
/// is independent of shard choice because compensation scans cost
/// tables across all shards by `user_tx_xid` (not partitioned by pool).
pub fn ring_push_compensate(
    shard_idx: usize,
    user_tx_xid: i64,
) -> Result<u64, ()> {
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    let head = shard.head.load(Ordering::Acquire);
    let tail = shard.tail.load(Ordering::Acquire);
    if tail.wrapping_sub(head) >= POC_REQUESTS_PER_SHARD as u32 {
        return Err(());
    }
    let idx = (tail as usize) % POC_REQUESTS_PER_SHARD;
    let req = &arena.requests[shard_idx][idx];
    let req_seq = shard.next_request_seq.fetch_add(1, Ordering::AcqRel);
    req.request_seq.store(req_seq, Ordering::Release);
    req.pool_hash.store(0, Ordering::Release);
    req.backend_pid.store(0, Ordering::Release);
    // u32::MAX sentinel = "no slot" — drain skips slot_idx-side work.
    req.slot_idx.store(u32::MAX, Ordering::Release);
    req.kind_tag.store(REQ_KIND_COMPENSATE, Ordering::Release);
    req.body0.store(user_tx_xid, Ordering::Release);
    req.body1.store(0, Ordering::Release);
    req.body2.store(0, Ordering::Release);
    req.body3.store(0, Ordering::Release);
    req.body4.store(0, Ordering::Release);
    req.body5.store(0, Ordering::Release);
    req.body6.store(0, Ordering::Release);
    req.body7.store(0, Ordering::Release);
    req.body8.store(0, Ordering::Release);
    req.valid.store(REQ_FILLED, Ordering::Release);
    shard.tail.store(tail.wrapping_add(1), Ordering::Release);
    Ok(req_seq)
}

/// Drain up to `max_batch` Compensate requests from the head of the
/// shard's ring. Mirrors `drain_apply_batch`'s discipline: stops at
/// the first non-COMPENSATE entry so FIFO ordering with APPLY is
/// preserved across alternating calls in `drain_and_commit`.
pub fn drain_compensate_batch(
    shard_idx: usize,
    max_batch: usize,
) -> Vec<DrainedCompensate> {
    let mut out: Vec<DrainedCompensate> = Vec::new();
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    let head_start = shard.head.load(Ordering::Acquire);
    let tail = shard.tail.load(Ordering::Acquire);
    let available = tail.wrapping_sub(head_start) as usize;
    let n = available.min(max_batch);
    if n == 0 {
        return out;
    }
    let mut consumed: u32 = 0;
    for i in 0..n {
        let pos = head_start.wrapping_add(i as u32);
        let idx = (pos as usize) % POC_REQUESTS_PER_SHARD;
        let req = &arena.requests[shard_idx][idx];
        let v = req.valid.load(Ordering::Acquire);
        if v == REQ_EMPTY {
            // Same defensive skip as drain_apply_batch.
            consumed = consumed.wrapping_add(1);
            continue;
        }
        let kind = req.kind_tag.load(Ordering::Acquire);
        if kind != REQ_KIND_COMPENSATE {
            break;
        }
        out.push(DrainedCompensate {
            request_seq: req.request_seq.load(Ordering::Acquire),
            user_tx_xid: req.body0.load(Ordering::Acquire),
        });
        req.valid.store(REQ_IN_FLIGHT, Ordering::Release);
        consumed = consumed.wrapping_add(1);
    }
    shard
        .head
        .store(head_start.wrapping_add(consumed), Ordering::Release);
    out
}

// ── Result-slot writeback (committer-side) ────────────────────────────
//
// After the committer commits its sub-tx, it writes the per-request
// result into each slot and CAS-flips state SLOT_ALLOCATED → SLOT_FILLED
// (spec §1.6 step 18). M1.2's result is the trio
// (applied_unit_cost, applied_total_cost, committer_tx_id); M2.x adds
// depletion_ids_inline / depletion_count populated from FIFO output.

pub fn fill_slot_result(
    shard_idx: usize,
    slot_idx: u32,
    applied_unit_cost: i64,
    applied_total_cost: i64,
    committer_tx_id: i64,
) -> Result<(), u8> {
    fill_slot_result_with_error(
        shard_idx,
        slot_idx,
        applied_unit_cost,
        applied_total_cost,
        committer_tx_id,
        0,
    )
}

/// M2.1 variant: also stamps `error_code` so per-event errors can ride
/// alongside the cost fields. `error_code = 0` means "success" — the
/// cost fields carry the applied result. `error_code != 0` means the
/// method emitted a per-event error and the cost fields are 0 by
/// convention (callers should treat them as undefined).
pub fn fill_slot_result_with_error(
    shard_idx: usize,
    slot_idx: u32,
    applied_unit_cost: i64,
    applied_total_cost: i64,
    committer_tx_id: i64,
    error_code: u16,
) -> Result<(), u8> {
    let arena = POC_SHARD_ARENA.share();
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    // Write result + error fields BEFORE flipping state — caller
    // observing SLOT_FILLED needs to see the values atomically. The
    // CAS is the happens-before barrier.
    slot.applied_unit_cost
        .store(applied_unit_cost, Ordering::Release);
    slot.applied_total_cost
        .store(applied_total_cost, Ordering::Release);
    slot.committer_tx_id
        .store(committer_tx_id, Ordering::Release);
    slot.error_code.store(error_code, Ordering::Release);
    slot.state
        .compare_exchange(
            SLOT_ALLOCATED,
            SLOT_FILLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .map(|_| ())
        .map_err(|actual| actual)
}

/// Read a slot's result tuple (no error_code). Kept for backward-compat
/// with M1.1 callers; M2.1+ should prefer `read_slot_result_with_error`.
pub fn read_slot_result(shard_idx: usize, slot_idx: u32) -> (u8, i64, i64, i64) {
    let (state, unit, total, ctx, _err) =
        read_slot_result_with_error(shard_idx, slot_idx);
    (state, unit, total, ctx)
}

/// M2.1 variant: returns
/// `(state, applied_unit_cost, applied_total_cost, committer_tx_id,
///   error_code)`. state loaded LAST under Acquire so the matching
/// Release in `fill_slot_result_with_error` establishes happens-before
/// on the data fields.
pub fn read_slot_result_with_error(
    shard_idx: usize,
    slot_idx: u32,
) -> (u8, i64, i64, i64, u16) {
    let arena = POC_SHARD_ARENA.share();
    let slot = &arena.slots[shard_idx][slot_idx as usize];
    let unit = slot.applied_unit_cost.load(Ordering::Acquire);
    let total = slot.applied_total_cost.load(Ordering::Acquire);
    let ctx = slot.committer_tx_id.load(Ordering::Acquire);
    let err = slot.error_code.load(Ordering::Acquire);
    let state = slot.state.load(Ordering::Acquire);
    (state, unit, total, ctx, err)
}

// ── Committer election ────────────────────────────────────────────────
//
// Spec §1.6 step 10: single-word CAS on `committer_pid` from 0 to
// `MyProcPid`. On success the winner publishes
// `committer_acquired_at_ns` via a separate Release store. Readers that
// observe pid != 0 but timestamp == 0 are inside the "just-acquired"
// window and must treat the lease as fresh (spec §1.6 step 10 note).

pub fn try_acquire_committer(shard_idx: usize, my_pid: i32, now_ns: u64) -> bool {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    match shard.committer_pid.compare_exchange(
        0,
        my_pid,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            // Release stamp AFTER CAS win; the "fresh" window is the
            // gap between the CAS and this store.
            shard
                .committer_acquired_at_ns
                .store(now_ns, Ordering::Release);
            true
        }
        Err(_) => false,
    }
}

pub fn release_committer(shard_idx: usize) {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    // Clear timestamp first, then pid. A subsequent winner overwriting
    // pid → MyProcPid races their own timestamp-store; either way the
    // "pid != 0, timestamp == 0" window is benign per spec.
    shard
        .committer_acquired_at_ns
        .store(0, Ordering::Release);
    shard.committer_pid.store(0, Ordering::Release);
}

/// Stamp the next committer_tx_id for this shard. Per-shard counter
/// (spec Q-E resolved 2026-05-11 — see saved memory). Monotonic; never
/// decreases; reset only by `shard_reset` for tests.
pub fn next_committer_tx_id(shard_idx: usize) -> i64 {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    // fetch_add returns the pre-add value; first call returns 0 → +1 → 1.
    (shard
        .committer_tx_seq
        .fetch_add(1, Ordering::AcqRel)
        .wrapping_add(1)) as i64
}

pub fn shard_head_tail(shard_idx: usize) -> (u32, u32) {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    (
        shard.head.load(Ordering::Acquire),
        shard.tail.load(Ordering::Acquire),
    )
}

/// Read-only accessor for the per-shard committer-PID slot. Returns 0
/// when no committer is currently elected. Used by M4.1's stats
/// surface to observe cross-shard parallelism — a snapshot taken
/// mid-load with multiple non-zero values indicates committers running
/// independently on multiple shards.
pub fn read_committer_pid(shard_idx: usize) -> i32 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .committer_pid
        .load(Ordering::Acquire)
}

/// M5b.1 (acct-4d4n.12): seed the per-shard committer_tx counter so
/// the next call to `next_committer_tx_id` returns a value strictly
/// greater than `min_seen`. Called by the startup recovery worker
/// after scanning durable cost rows: post-restart shmem starts at 0,
/// new applies would collide with the committer_tx_ids that survived
/// the restart on disk. Idempotent; uses fetch_max so concurrent
/// live committers cannot drive the counter backwards.
pub fn seed_committer_tx_at_least(shard_idx: usize, min_seen: i64) {
    if min_seen <= 0 || shard_idx >= POC_SHARD_COUNT {
        return;
    }
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    let _ = shard
        .committer_tx_seq
        .fetch_max(min_seen as u64, Ordering::AcqRel);
}

/// Read-only accessor for the current per-shard `committer_tx_seq`.
/// Distinct from `next_committer_tx_id` which fetch_adds.
pub fn read_committer_tx_seq(shard_idx: usize) -> u64 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .committer_tx_seq
        .load(Ordering::Acquire)
}

/// Read-only accessor for `next_request_seq`.
pub fn read_next_request_seq(shard_idx: usize) -> u64 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .next_request_seq
        .load(Ordering::Acquire)
}

/// Read-only accessor for `next_slot_seq`.
pub fn read_next_slot_seq(shard_idx: usize) -> u64 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .next_slot_seq
        .load(Ordering::Acquire)
}

/// M5c.2 (acct-4d4n.15): scan `shard_idx`'s slot pool for leaked
/// slots — SLOT_ALLOCATED entries whose acquire-age exceeds
/// `min_age_ns` AND whose `waiter_pid` is dead. Reclaim each via
/// two-step CAS: state ALLOCATED → ABANDONED, then ABANDONED → FREE
/// via `recycle_slot`. The ABANDONED intermediate keeps a concurrent
/// committer's slot-fill CAS from rolling back the audit's work
/// (M5a.2 logs SLOT_ABANDONED fill misses benignly).
///
/// Returns (scanned, reclaimed). `scanned` is the number of
/// ALLOCATED slots observed; `reclaimed` is the number actually
/// state-flipped + recycled by this pass.
pub fn slot_leak_audit_one_shard(
    shard_idx: usize,
    now_ns: u64,
    min_age_ns: u64,
) -> (usize, usize) {
    let arena = POC_SHARD_ARENA.share();
    let slots = &arena.slots[shard_idx];
    let mut scanned: usize = 0;
    let mut reclaimed: usize = 0;
    for (i, slot) in slots.iter().enumerate() {
        let state = slot.state.load(Ordering::Acquire);
        if state != SLOT_ALLOCATED {
            continue;
        }
        scanned += 1;
        let acquired = slot.acquired_at_ns.load(Ordering::Acquire);
        if acquired == 0 || now_ns.saturating_sub(acquired) < min_age_ns {
            continue;
        }
        let pid = slot.waiter_pid.load(Ordering::Acquire);
        // Zero waiter_pid means push-only-then-die OR pre-stamp race:
        // both are legitimate "no live caller" cases. Treat as dead.
        let dead = pid == 0 || !pg_pid_alive(pid);
        if !dead {
            continue;
        }
        // Step 1: ALLOCATED → ABANDONED.
        if slot
            .state
            .compare_exchange(
                SLOT_ALLOCATED,
                SLOT_ABANDONED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // Step 2: ABANDONED → FREE via recycle_slot (clears all
            // payload fields).
            let _ = recycle_slot(shard_idx, i as u32);
            reclaimed += 1;
        }
        // CAS failure means a concurrent path (committer fill,
        // explicit cancel cleanup, prior audit pass) already moved
        // the slot — count it as scanned but not reclaimed.
    }
    (scanned, reclaimed)
}

/// Read the per-shard `committer_acquired_at_ns` timestamp. Returns 0
/// when no committer is currently elected. Combined with the
/// `poc_ledger.committer_lease_ms` GUC, this is the lease-expiry
/// signal: `now_ns - acquired_at_ns > lease_ms * 1e6` → lease stale.
/// (acct-4d4n.10, M5a.1)
pub fn read_committer_acquired_at_ns(shard_idx: usize) -> u64 {
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx]
        .committer_acquired_at_ns
        .load(Ordering::Acquire)
}

// ── pg_pid_alive ──────────────────────────────────────────────────────
//
// Spec §3.2 + bd acct-4d4n.10 description: `kill(pid, 0)` returns 0 if
// the process exists (regardless of whether we can signal it) and -1
// with errno=ESRCH if it does not. We treat any non-zero return as
// "dead" for the M5a.1 takeover decision; M5b.2 (sibling) layers in
// the EPERM (process exists, owned by other user) refinement. In our
// single-user PG cluster EPERM should not arise for backend PIDs.

unsafe extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Returns true if `pid` is a live process this process can identify.
/// Uses `kill(pid, 0)` — never sends a signal, just probes existence.
/// `pid <= 0` is treated as dead (kill(0, ...) targets the process
/// group, kill(-1, ...) signals all permitted processes — neither is a
/// useful probe here). (acct-4d4n.10, M5a.1)
pub fn pg_pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    let rc = unsafe { kill(pid, 0) };
    rc == 0
}

// ── Committer takeover (M5a.1) ────────────────────────────────────────
//
// Two-step decision:
//   1. Lease expired? `now_ns - committer_acquired_at_ns > lease_ns`.
//   2. PID dead? `pg_pid_alive(stored_pid) == false`.
//
// Both must hold before takeover. Either alone is insufficient:
//   - Lease expired but pid alive → slow committer; do NOT steal
//     (M5b.2 refines the slow-committer mitigation via shrunk
//     batch_size_max). M5a.1's conservative behavior is to wait.
//   - PID dead but lease still warm → committer crashed within the
//     lease window. Still safe to take over since the committer can't
//     resume; we just need to wait until the lease expires OR detect
//     the death immediately. M5a.1 chooses immediate-on-dead: if the
//     PID is dead, take over regardless of lease state. This is the
//     responsive behavior that meets the "lease takeover within
//     2 × committer_lease_ms" acceptance.

#[derive(Debug, Clone, Copy)]
pub enum TakeoverOutcome {
    /// No committer held; standard CAS-from-0 acquired.
    Acquired,
    /// Stale committer took over; the previous (dead) PID is returned
    /// so the caller can attribute orphan recovery to it.
    TookOver { stale_pid: i32 },
    /// Committer held but lease and PID are valid → no takeover.
    Held,
    /// CAS lost to a concurrent acquirer.
    Lost,
}

/// Try to acquire the committer either via standard CAS-from-0 OR via
/// lease-takeover-on-dead-pid. Returns the outcome so the caller can
/// decide whether to run orphan recovery (TookOver) or proceed normally
/// (Acquired).
pub fn try_acquire_or_takeover(
    shard_idx: usize,
    my_pid: i32,
    now_ns: u64,
    lease_ns: u64,
) -> TakeoverOutcome {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    // Fast path: empty slot.
    if shard
        .committer_pid
        .compare_exchange(0, my_pid, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        shard
            .committer_acquired_at_ns
            .store(now_ns, Ordering::Release);
        return TakeoverOutcome::Acquired;
    }
    // Slow path: someone holds it. Check if takeover is warranted.
    let cur_pid = shard.committer_pid.load(Ordering::Acquire);
    if cur_pid == 0 {
        // Window between observation and CAS — race lost.
        return TakeoverOutcome::Lost;
    }
    let acquired_at = shard.committer_acquired_at_ns.load(Ordering::Acquire);
    let lease_expired = acquired_at == 0
        || now_ns.saturating_sub(acquired_at) > lease_ns;
    let pid_dead = !pg_pid_alive(cur_pid);
    // M5a.1 + M5b.2 (acct-4d4n.13) policy: dead PID is sufficient AND
    // necessary; lease state is informational only. A slow but alive
    // committer (per-batch wall time > committer_lease_ms) returns
    // Held → contender backs off via outer wait loop → no takeover.
    // The lease_ms guidance lives at lib.rs DRAIN_SLEEP_US doc block:
    // size batch_size_max so worst-case drain stays inside lease_ms;
    // the pg_pid_alive gate keeps misconfiguration harmless (wasted
    // CPU on contender, no correctness violation).
    if !pid_dead {
        return TakeoverOutcome::Held;
    }
    let _ = lease_expired;
    match shard.committer_pid.compare_exchange(
        cur_pid,
        my_pid,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => {
            shard
                .committer_acquired_at_ns
                .store(now_ns, Ordering::Release);
            // M8.2 (acct-4d4n.18): bump at the function boundary so
            // every caller — apply path, bench helper, slot-audit —
            // attributes uniformly. Counts only successful takeovers
            // (TookOver outcome), not Acquired-from-empty or Lost/Held.
            POC_RECOVERY_STATS
                .share()
                .lease_takeovers
                .fetch_add(1, Ordering::AcqRel);
            TakeoverOutcome::TookOver { stale_pid: cur_pid }
        }
        Err(_) => TakeoverOutcome::Lost,
    }
}

/// Test-only: inject a stale committer PID + acquired_at_ns so a
/// concurrent backend's takeover path is exercised without needing an
/// actual proc_exit. Sets `committer_pid` and `committer_acquired_at_ns`
/// unconditionally — caller is responsible for picking a fake_pid that
/// is genuinely not a live PG backend (spawn a `true` subprocess, wait
/// for it to exit, then poke its now-dead PID).
pub fn inject_dead_committer(shard_idx: usize, fake_pid: i32, fake_acquired_at_ns: u64) {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    shard.committer_pid.store(fake_pid, Ordering::Release);
    shard
        .committer_acquired_at_ns
        .store(fake_acquired_at_ns, Ordering::Release);
}

// ── Slot iteration for orphan recovery (M5a.1) ────────────────────────
//
// A snapshot of all SLOT_ALLOCATED slots on a shard that carry a
// non-zero issue_id — i.e. slots with a live request that was already
// pushed (`current_request_seq > 0`) but not yet filled by a
// committer. The orphan-recovery path looks these up in the cost
// tables to decide backfill vs abandoned.

#[derive(Debug, Clone)]
pub struct OrphanSlot {
    pub slot_idx: u32,
    pub issue_id: i64,
    pub current_request_seq: u64,
    pub waiter_pid: i32,
}

/// Walk `slots[shard_idx]` and collect every slot whose
/// `state == SLOT_ALLOCATED` AND `issue_id != 0`. The caller (typically
/// the orphan-recovery path inside the new committer) is responsible
/// for deciding which of these are genuinely orphaned vs concurrently
/// in-flight; M5a.1's policy is "all of them, since takeover only
/// happens when the old committer is dead".
pub fn collect_orphan_slots(shard_idx: usize) -> Vec<OrphanSlot> {
    let arena = POC_SHARD_ARENA.share();
    let mut out: Vec<OrphanSlot> = Vec::new();
    for (idx, slot) in arena.slots[shard_idx].iter().enumerate() {
        let state = slot.state.load(Ordering::Acquire);
        if state != SLOT_ALLOCATED {
            continue;
        }
        let issue_id = slot.issue_id.load(Ordering::Acquire);
        if issue_id == 0 {
            continue;
        }
        out.push(OrphanSlot {
            slot_idx: idx as u32,
            issue_id,
            current_request_seq: slot.current_request_seq.load(Ordering::Acquire),
            waiter_pid: slot.waiter_pid.load(Ordering::Acquire),
        });
    }
    out
}

/// Test-only: reset a shard's state. Real workloads never call this.
pub fn shard_reset(shard_idx: usize) {
    let arena = POC_SHARD_ARENA.exclusive();
    let shard = &arena.shards[shard_idx];
    shard.head.store(0, Ordering::Release);
    shard.tail.store(0, Ordering::Release);
    shard.committer_pid.store(0, Ordering::Release);
    shard
        .committer_acquired_at_ns
        .store(0, Ordering::Release);
    shard.committer_tx_seq.store(0, Ordering::Release);
    shard.next_request_seq.store(0, Ordering::Release);
    shard.next_slot_seq.store(0, Ordering::Release);
    shard
        .capacity
        .store(POC_REQUESTS_PER_SHARD as u32, Ordering::Release);
    shard.ring_full_waiter_pid.store(0, Ordering::Release);
    shard.error_count.store(0, Ordering::Release);
    for req in &arena.requests[shard_idx] {
        req.valid.store(REQ_EMPTY, Ordering::Release);
        req.request_seq.store(0, Ordering::Release);
        req.pool_hash.store(0, Ordering::Release);
        req.backend_pid.store(0, Ordering::Release);
        req.slot_idx.store(0, Ordering::Release);
        req.kind_tag.store(0, Ordering::Release);
    }
    for slot in &arena.slots[shard_idx] {
        slot.state.store(SLOT_FREE, Ordering::Release);
        slot.applied_unit_cost.store(0, Ordering::Release);
        slot.applied_total_cost.store(0, Ordering::Release);
        slot.committer_tx_id.store(0, Ordering::Release);
        slot.error_code.store(0, Ordering::Release);
        slot.depletion_count.store(0, Ordering::Release);
        slot.spillover_offset.store(0, Ordering::Release);
        slot.waiter_pid.store(0, Ordering::Release);
        slot.issue_id.store(0, Ordering::Release);
        slot.current_request_seq.store(0, Ordering::Release);
        slot.acquired_at_ns.store(0, Ordering::Release);
        slot.set_latched_at_ns.store(0, Ordering::Release);
    }
}

// ── SQL surface (test harness for M1.1 acceptance) ────────────────────
//
// All functions take a `shard_idx` as i32; out-of-range returns NULL
// (Option::None) or -1 sentinel where the return type is non-nullable.

#[pg_extern]
fn poc_ledger_shard_count() -> i32 {
    POC_SHARD_COUNT as i32
}

#[pg_extern]
fn poc_ledger_requests_per_shard() -> i32 {
    POC_REQUESTS_PER_SHARD as i32
}

#[pg_extern]
fn poc_ledger_slots_per_shard() -> i32 {
    POC_SLOTS_PER_SHARD as i32
}

#[pg_extern]
fn poc_ledger_max_slot_probe() -> i32 {
    POC_MAX_SLOT_PROBE as i32
}

/// M5c.1 (acct-4d4n.14) test-only: synthesize a ring-full state on
/// `shard_idx` by advancing `tail` past `head + capacity`. The slot
/// pool stays at its current occupancy — this is the only way to
/// exercise the ring-full backpressure path in isolation, since
/// `push_only` saturates both ring and slot pool in lockstep. Returns
/// the new `tail` value, or -1 if `shard_idx` is out of range.
#[pg_extern]
fn poc_ledger_test_force_ring_full(shard_idx: i32) -> i32 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx as usize];
    let head = shard.head.load(Ordering::Acquire);
    let new_tail = head.wrapping_add(POC_REQUESTS_PER_SHARD as u32);
    shard.tail.store(new_tail, Ordering::Release);
    new_tail as i32
}

/// M5c.1 (acct-4d4n.14) test-only: simulate a committer drain that
/// advanced `head` by `n` (freeing `n` ring slots). Calls
/// `set_ring_full_waiter`'s wake (via `SetLatch` on the registered
/// waiter PID) so a backend currently blocked in
/// `poc_ledger_apply`'s backpressure loop is woken to retry the push.
/// Returns the new `head` value.
#[pg_extern]
fn poc_ledger_test_advance_head_and_signal(shard_idx: i32, n: i32) -> i32 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT || n < 0 {
        return -1;
    }
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx as usize];
    let head = shard.head.load(Ordering::Acquire);
    let new_head = head.wrapping_add(n as u32);
    shard.head.store(new_head, Ordering::Release);
    // Wake the per-shard backpressure waiter, if any. Inline because
    // committer.rs's `signal_ring_full_waiter` isn't visible here.
    let pid = shard.ring_full_waiter_pid.load(Ordering::Acquire);
    if pid != 0 {
        unsafe {
            let proc_ = pgrx::pg_sys::BackendPidGetProc(pid);
            if !proc_.is_null() {
                pgrx::pg_sys::SetLatch(&mut (*proc_).procLatch);
            }
        }
    }
    new_head as i32
}

#[pg_extern]
fn poc_ledger_shard_reset(shard_idx: i32) -> bool {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return false;
    }
    ensure_capacity_stamped();
    shard_reset(shard_idx as usize);
    true
}

#[pg_extern]
fn poc_ledger_slot_acquire(shard_idx: i32) -> Option<i32> {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return None;
    }
    acquire_slot(shard_idx as usize).map(|s| s as i32)
}

#[pg_extern]
fn poc_ledger_slot_state(shard_idx: i32, slot_idx: i32) -> i32 {
    if shard_idx < 0
        || (shard_idx as usize) >= POC_SHARD_COUNT
        || slot_idx < 0
        || (slot_idx as usize) >= POC_SLOTS_PER_SHARD
    {
        return -1;
    }
    slot_state(shard_idx as usize, slot_idx as u32) as i32
}

// Encoding: 0 = success; negative = CAS failed (slot not in
// SLOT_ALLOCATED). -100 = out-of-range shard/slot. Encoding mirrors
// poc_ledger_slot_recycle so failure cases are always negative.

#[pg_extern]
fn poc_ledger_slot_mark_filled(shard_idx: i32, slot_idx: i32) -> i32 {
    if shard_idx < 0
        || (shard_idx as usize) >= POC_SHARD_COUNT
        || slot_idx < 0
        || (slot_idx as usize) >= POC_SLOTS_PER_SHARD
    {
        return -100;
    }
    match mark_slot_filled(shard_idx as usize, slot_idx as u32) {
        Ok(()) => 0,
        Err(actual) => -(actual as i32) - 1,
    }
}

#[pg_extern]
fn poc_ledger_slot_mark_abandoned(shard_idx: i32, slot_idx: i32) -> i32 {
    if shard_idx < 0
        || (shard_idx as usize) >= POC_SHARD_COUNT
        || slot_idx < 0
        || (slot_idx as usize) >= POC_SLOTS_PER_SHARD
    {
        return -100;
    }
    match mark_slot_abandoned(shard_idx as usize, slot_idx as u32) {
        Ok(()) => 0,
        Err(actual) => -(actual as i32) - 1,
    }
}

#[pg_extern]
fn poc_ledger_slot_recycle(shard_idx: i32, slot_idx: i32) -> i32 {
    if shard_idx < 0
        || (shard_idx as usize) >= POC_SHARD_COUNT
        || slot_idx < 0
        || (slot_idx as usize) >= POC_SLOTS_PER_SHARD
    {
        return -1;
    }
    match recycle_slot(shard_idx as usize, slot_idx as u32) {
        Ok(prev) => prev as i32,
        Err(actual) => -(actual as i32) - 1,
    }
}

#[pg_extern]
fn poc_ledger_ring_push(
    shard_idx: i32,
    slot_idx: i32,
    pool_hash: i64,
    kind_tag: i32,
) -> Option<i64> {
    if shard_idx < 0
        || (shard_idx as usize) >= POC_SHARD_COUNT
        || slot_idx < 0
        || (slot_idx as usize) >= POC_SLOTS_PER_SHARD
        || kind_tag < 0
        || kind_tag > u8::MAX as i32
    {
        return None;
    }
    let backend_pid = unsafe { pgrx::pg_sys::MyProcPid };
    ring_push(
        shard_idx as usize,
        slot_idx as u32,
        pool_hash as u64,
        kind_tag as u8,
        backend_pid,
    )
    .ok()
    .map(|seq| seq as i64)
}

#[pg_extern]
fn poc_ledger_ring_drain_one(shard_idx: i32) -> Option<i32> {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return None;
    }
    ring_drain_one(shard_idx as usize).map(|s| s as i32)
}

#[pg_extern]
fn poc_ledger_shard_head(shard_idx: i32) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    shard_head_tail(shard_idx as usize).0 as i64
}

#[pg_extern]
fn poc_ledger_shard_tail(shard_idx: i32) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    shard_head_tail(shard_idx as usize).1 as i64
}

#[pg_extern]
fn poc_ledger_shard_depth(shard_idx: i32) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    let (h, t) = shard_head_tail(shard_idx as usize);
    t.wrapping_sub(h) as i64
}

#[pg_extern]
fn poc_ledger_shard_next_request_seq(shard_idx: i32) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx as usize]
        .next_request_seq
        .load(Ordering::Acquire) as i64
}

#[pg_extern]
fn poc_ledger_shard_next_slot_seq(shard_idx: i32) -> i64 {
    if shard_idx < 0 || (shard_idx as usize) >= POC_SHARD_COUNT {
        return -1;
    }
    let arena = POC_SHARD_ARENA.share();
    arena.shards[shard_idx as usize]
        .next_slot_seq
        .load(Ordering::Acquire) as i64
}
