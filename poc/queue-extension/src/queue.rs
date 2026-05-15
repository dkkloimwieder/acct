//! acct-4d4n.2 (M1.1): single-shard queue primitive.
//!
//! Implements the shmem data structures from spec §1.4 — PocQueueShard
//! header, PocPendingRequest ring, PocResultSlot pool — plus slot
//! allocation per §1.6 step 5 (atomic fetch_add for index reservation,
//! CAS state transition free→allocated, linear probe ≤16) and ring
//! buffer push/drain discipline under a PgLwLock.
//!
//! ## What M1.1 covers
//!
//! - `PocQueueShard` header (head, tail, capacity, committer-state
//!   placeholders, slot/request sequence counters).
//! - `PocPendingRequest` ring buffer with `valid` state machine
//!   (empty/filled/in-flight/abandoned).
//! - `PocResultSlot` pool with state machine (free/allocated/filled/
//!   abandoned).
//! - Slot allocation OUTSIDE the LWLock per spec §1.6 step 5; linear
//!   probe bounded by `POC_MAX_SLOT_PROBE = 16`.
//! - Ring push/drain under a single global `PgLwLock` (M3.1 swaps to
//!   per-shard tranches).
//! - SQL surface for driving the M1.1 acceptance tests from psql.
//!
//! ## What M1.1 deliberately defers
//!
//! - Committer election (M1.2, acct-4d4n.3).
//! - Real INSERT into cost tables (M1.2).
//! - GUC-driven runtime sizing. M1.1 uses compile-time const values;
//!   spec §1.4 defaults (256×4096×4096) would require a
//!   shmem_request_hook FFI dance that's out of M1.1 scope. The
//!   `poc_ledger.*` GUCs registered by M0.1 are advisory at this point.
//!   Re-baseline if M9 demands spec defaults (file as
//!   acct-4d4n-followup if so).
//! - Per-shard LWLock tranches. M1.1 has ONE global lock; M3.1 splits.
//! - PocApplyPayload / PocCompensatePayload field semantics. M1.1
//!   stores the body as opaque AtomicI64 slots; M2.x defines the
//!   variant-specific layout.

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

#[repr(C, align(64))]
pub struct PocResultSlot {
    pub state: AtomicU8,
    pub _pad0: [u8; 7],
    pub applied_unit_cost: AtomicI64,
    pub applied_total_cost: AtomicI64,
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
    slot.error_code.store(0, Ordering::Release);
    slot.depletion_count.store(0, Ordering::Release);
    slot.spillover_offset.store(0, Ordering::Release);
    for i in 0..32 {
        slot.depletion_ids_inline[i].store(0, Ordering::Release);
    }
    slot.state.store(SLOT_FREE, Ordering::Release);
    Ok(cur)
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

pub fn shard_head_tail(shard_idx: usize) -> (u32, u32) {
    let arena = POC_SHARD_ARENA.share();
    let shard = &arena.shards[shard_idx];
    (
        shard.head.load(Ordering::Acquire),
        shard.tail.load(Ordering::Acquire),
    )
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
        slot.error_code.store(0, Ordering::Release);
        slot.depletion_count.store(0, Ordering::Release);
        slot.spillover_offset.store(0, Ordering::Release);
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
