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
    pub _pad3: [u8; 8],
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

/// Wire up the arena's shmem segment. Called from `_PG_init`.
pub fn init() {
    pg_shmem_init!(POC_SHARD_ARENA);
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
    for i in 0..n {
        let pos = head_start.wrapping_add(i as u32);
        let idx = (pos as usize) % POC_REQUESTS_PER_SHARD;
        let req = &arena.requests[shard_idx][idx];
        // M1.2 only emits Apply-kind requests. Compensate is M6.1.
        let kind = req.kind_tag.load(Ordering::Acquire);
        if kind != REQ_KIND_APPLY {
            // Defensive: skip but still mark in_flight + advance head
            // so the unknown variant doesn't wedge the ring. M6.1 will
            // dispatch by kind here.
            req.valid.store(REQ_IN_FLIGHT, Ordering::Release);
            continue;
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
    }
    shard
        .head
        .store(head_start.wrapping_add(n as u32), Ordering::Release);
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
