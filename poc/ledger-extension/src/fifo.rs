//! acct-e9tf — FIFO maximal F (revised) — sub 1: per-cell LWLock tranche
//! + FifoBucket layout.
//!
//! Foundation for the FIFO arena. This module ships:
//!
//! - `Layer` / `FifoBucket` / `FifoArena` data layout
//! - `FIFO_ARENA: PgLwLock<FifoArena>` (table-wide insert serialization,
//!   mirrors WAC's `HASH_TABLE`)
//! - Per-cell LWLock tranche `fifo_cell` (one LWLock per bucket;
//!   `FIFO_N_BUCKETS` locks under a single named tranche)
//! - `acquire_fifo_cell` returning an RAII Drop-guard
//! - SQL-callable test surface (`fifo_arena_capacity`,
//!   `fifo_test_acquire_release`, `fifo_test_acquire_two_sorted`,
//!   `fifo_test_cell_lock_addr`) so end-to-end tests in
//!   `poc/batch-ledger/tests/` exercise the primitives.
//!
//! NO SQL apply path here. The `fifo_apply_batch` pg_extern lands in
//! sub 2 (acct-uy4p). NO ring-buffer mutation either — `push_layer` /
//! `consume_layers` arrive with sub 2.
//!
//! ## Two atomicity domains (mirrors WAC)
//!
//! - `FIFO_ARENA` global LWLock serializes inserts (occupied flag 0→1).
//!   SHARED for probe/hit, EXCLUSIVE for insert.
//! - Per-cell LWLocks protect the ring buffer (`layers`, `head`,
//!   `n_layers`) during multi-step apply critical sections (FIFO walk +
//!   consume + push). Atomic key/seq fields let the drain identify
//!   dirty cells lock-free.
//!
//! ## Cell layout — ring buffer with `MAX_LAYERS` cap
//!
//! - `layers[MAX_LAYERS]` indexed via `head` + `n_layers` (head outward
//!   is oldest; tail = `(head + n_layers) % MAX_LAYERS`).
//! - `head` / `n_layers` are u16, protected by the per-cell LWLock.
//! - On overflow at receipt time (sub 3 / acct-b8ub), the overflowing
//!   receipt INSERTs to durable `cost_layers` only — the ring is left
//!   untouched. `overflow_active` flips to 1 on the bucket; while set,
//!   subsequent receipts also bypass the ring even if drains have made
//!   room (sticky-spill preserves the FIFO watermark). The first
//!   spilled layer's id is captured in `overflow_first_spilled_layer_id`
//!   so spill consumes can locate the spilled tail without scanning the
//!   ring contents. Strict per-unit FIFO preserved: issues consume the
//!   in-shmem head first, then walk durable rows with
//!   `id >= overflow_first_spilled_layer_id` in `(receipt_date, id)`
//!   order via SPI `FOR UPDATE`.
//!
//! ## Bucket-exhaustion handling (sub 8 / acct-cpe8)
//!
//! When all `FIFO_N_BUCKETS` buckets are occupied at insert time
//! (no key match, open-addressing probe walked the whole table),
//! the pool is routed through a **durable-only** path: it gets no
//! shmem cell, all its envelopes apply directly against
//! `cost_layers` via SPI. Receipts INSERT a new layer (no ring
//! push, no `pending_drain` staging). Issues SPI-walk the pool's
//! durable rows under `FOR UPDATE` in `(receipt_date, id)` order
//! and stage their `qty_remaining` decrement via the existing
//! `accum_drain` → Phase 9e UPDATE path. Mirrors the
//! spill-to-durable philosophy of sub 3 but at the bucket
//! dimension rather than the layer dimension. Same-pool cell
//! reclamation when arena occupancy drops is future polish.
//!
//! ## Cell-lock acquisition discipline
//!
//! Multi-cell batches MUST acquire cell locks in cell-index ascending
//! order. Sub 2's `fifo_apply_batch` enforces this via sort-then-acquire.
//! Single-cell acquisitions don't need the discipline; the helper here
//! doesn't enforce ordering itself — that's the caller's job (sort
//! upstream, then call this in sequence).

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::shmem::{PGRXSharedMemory, PgSharedMemoryInitialization};
use pgrx::{PgLwLock, pg_shmem_init};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicI64, AtomicPtr, AtomicU8, AtomicU64, Ordering};

// ── constants ────────────────────────────────────────────────────────

/// FIFO arena bucket count. Sized so the 5000-pool fan-out PoC bench
/// lands at ~30% load factor (open addressing degrades past ~70%).
/// Future: GUC-tunable via raw `RequestAddinShmemSpace` +
/// `ShmemInitStruct` shape (pgrx's `pg_shmem_init!` requires a
/// compile-time-sized struct).
pub const FIFO_N_BUCKETS: usize = 16384;

/// Maximum layers per cell. At 256 layers × 24-byte Layer = 6144 bytes
/// payload per cell + 64-byte header + 1024-byte pending_drain = ~7232
/// bytes per cell (padded to next 64-byte cache line = 7296 bytes).
/// 16384 buckets = ~117 MB arena.
///
/// Sized for bursty-receipt ERP workloads: a typical PO match day adds
/// ~100-200 receipts per pool in a burst, then sustained issues drain
/// the ring back down between bursts. 256 layers = ~50% headroom above
/// the burst size. Sustained net-receipt-positive flow eventually
/// overflows regardless of cap — overflow handling routes the
/// overflowing receipt directly to durable `cost_layers`
/// (spill-to-durable, sub 3 / acct-b8ub). Strict per-unit FIFO
/// preserved; slow-path SPI walk consumes the spilled tail after the
/// in-shmem range exhausts. Currently parked pending a workload driver
/// that actually overflows 256.
pub const MAX_LAYERS: usize = 256;

/// Max pending-drain entries per cell between bgworker ticks (sub 4 /
/// acct-0450). Each entry is 16 bytes (layer_id + qty_consumed); the
/// ring is fixed-size for shmem allocation and 64 keeps the per-cell
/// overhead at 1 KiB. At default drain cadence (100 ms) and realistic
/// FIFO workloads (single-digit layers consumed per issue), 64 is
/// comfortable headroom. Overflow falls back to inline UPDATE in
/// `fifo_apply_batch_maximal` Phase 9e — pending_drain stays the fast
/// path; inline is the safety net.
pub const PENDING_DRAIN_CAP: usize = 64;

// ── data layout ──────────────────────────────────────────────────────

/// One FIFO layer (cost layer in the ring buffer).
///
/// Fields:
/// - `qty`: remaining quantity in this layer (mutated by issue).
/// - `unit_cost`: per-unit cost at receipt time (immutable post-push).
/// - `layer_id`: PK of the corresponding `cost_layers` row. Pre-allocated
///   via `nextval('cost_layers_id_seq')` BEFORE the cell lock is taken
///   (sub 2) so the apply path doesn't need post-INSERT sentinel resolution.
///   For lazy-seeded layers (loaded from cost_layers in pre-scan), this
///   is the existing row's id.
///
/// `unit_cost` is BIGINT to match `posting_lines.amount` / `cost_layers`
/// integer-money convention in acct's production schema.
///
/// 24 bytes raw, 8-byte aligned. 64 layers per bucket = 1536 bytes
/// ring buffer. Bucket header 64 bytes + ring 1536 = 1600 bytes per
/// bucket. 16384 buckets = 25.6 MB arena.
#[repr(C, align(8))]
#[derive(Copy, Clone, Default, Debug)]
pub struct Layer {
    pub qty: i64,
    pub unit_cost: i64,
    pub layer_id: i64,
}

unsafe impl PGRXSharedMemory for Layer {}

/// One pending drain entry — staged by `fifo_apply_batch_maximal` Phase 8
/// when an issue consumes from a layer. The bgworker (sub 4 / acct-0450)
/// snapshots + clears these every tick and bulk-UPSERTs the cumulative
/// drains into `cost_layers.qty_remaining`. Same-layer entries do NOT
/// dedupe in shmem (CPU under cell-lock); the drain UPSERT aggregates
/// via `SUM` per layer_id.
///
/// 16 bytes, 8-byte aligned. `PENDING_DRAIN_CAP=64` per cell = 1 KiB
/// per-cell pending area.
#[repr(C, align(8))]
#[derive(Copy, Clone, Default, Debug)]
pub struct PendingDrain {
    pub layer_id: i64,
    pub qty_consumed: i64,
}

unsafe impl PGRXSharedMemory for PendingDrain {}

/// Ring-buffer fields protected by the cell's per-cell LWLock.
/// Lives behind an `UnsafeCell` inside `FifoBucket` so the apply path
/// can obtain `&mut RingFields` without violating Rust's aliasing rules
/// (the cell LWLock is the runtime exclusion mechanism). Atomic
/// fields stay outside this struct because they're touched lock-free
/// by probe / drain / recon paths.
///
/// `pending_drain` + `pending_drain_n` (sub 4 / acct-0450): same lock
/// discipline as `layers` + `n_layers`. Apply phase pushes via
/// `push_pending_drain` under EXCLUSIVE; bgworker snapshots + resets to
/// 0 also under EXCLUSIVE; readers DO NOT touch this without the lock.
#[repr(C)]
pub struct RingFields {
    /// Oldest layer index. Walks `(head + 1) % MAX_LAYERS` as layers
    /// fully consume.
    pub head: u16,
    /// Number of valid layers. Tail = `(head + n_layers) % MAX_LAYERS`.
    pub n_layers: u16,
    /// Number of valid pending_drain entries; tail = `pending_drain_n`.
    /// Drained-to-disk by the bgworker; reset to 0 after snapshot.
    pub pending_drain_n: u16,
    pub _pad: [u8; 2],
    pub layers: [Layer; MAX_LAYERS],
    pub pending_drain: [PendingDrain; PENDING_DRAIN_CAP],
}

impl Default for RingFields {
    fn default() -> Self {
        // SAFETY: u16/i64/padding all valid as zero.
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for RingFields {}

/// One FIFO arena slot. Cache-line aligned (`align(64)`) so concurrent
/// SHARED-lock probes against different buckets don't false-share.
///
/// Two atomicity domains live in this struct:
///
/// - **Atomic fields** (`occupied`, `seeded`, `key_hi`, `key_lo`,
///   `last_seq`, `drained_seq`): read lock-free by probe, recon, and
///   drain paths. Mirrors the WAC `Bucket` layout.
/// - **LWLock-protected `RingFields`** (`head`, `n_layers`, `layers`):
///   mutated via `ring_mut` only by code holding this cell's per-cell
///   LWLock EXCLUSIVE. Wrapped in `UnsafeCell` so the cast to
///   `&mut RingFields` is sound under Rust aliasing rules.
///
/// `occupied` vs `seeded` semantics:
/// - `occupied = 0` → cell is empty; probe walks past it.
/// - `occupied = 1, seeded = 0` → cell was inserted (key reserved) but
///   no layers yet loaded from durable `cost_layers`. First issue
///   triggers lazy-seed.
/// - `occupied = 1, seeded = 1` → ring is authoritative for layer
///   state. Subsequent issues walk shmem; durable `cost_layers` is the
///   audit trail (drift-detected by recon).
///
/// `overflow_active` (sub 3 / acct-b8ub): set to 1 the first time a
/// receipt or lazy-seed push hits the `MAX_LAYERS` cap. While set,
/// subsequent receipts spill directly to durable `cost_layers` without
/// touching the ring (sticky-spill). Issues consume the in-shmem head
/// first, then SPI-walk durable rows with
/// `id >= overflow_first_spilled_layer_id`. The flag stays sticky for
/// the PoC — clearing on full drain is a hardening follow-up.
///
/// `overflow_first_spilled_layer_id`: 0 = unset; > 0 = `cost_layers.id`
/// of the first layer that was forced out of the ring. CAS-set on
/// first overflow; subsequent overflows preserve the original value.
/// All in-shmem layers have id strictly less than this watermark.
///
/// Header layout fits in 64 bytes; the 1544-byte `RingFields` follows.
#[repr(C, align(64))]
pub struct FifoBucket {
    pub occupied: AtomicU8,
    /// Lazy-seed flag. `0` = ring empty / unseeded; `1` = ring is the
    /// authoritative layer state for this cell. Transition gated by
    /// the cell's EXCLUSIVE LWLock during sub 2's apply phase.
    pub seeded: AtomicU8,
    /// Spill-to-durable active flag (sub 3 / acct-b8ub). `0` = ring is
    /// the sole layer store; `1` = ring is full / capped, subsequent
    /// receipts and the spill tail consume via durable. Read lock-free
    /// during Phase 8 spill-routing; writes are CAS / Release stores
    /// from inside the cell's EXCLUSIVE critical section.
    pub overflow_active: AtomicU8,
    pub _pad0: [u8; 5],
    pub key_hi: AtomicU64,
    pub key_lo: AtomicU64,
    /// Monotone seq stamped by the last apply that mutated this cell's
    /// layers. Bgworker (sub 4) compares to `drained_seq` to identify
    /// dirty cells without taking the per-cell lock.
    pub last_seq: AtomicU64,
    /// Highest `last_seq` value the bgworker has UPSERTed to durable
    /// `cost_layers`. Dirty iff `last_seq > drained_seq`.
    pub drained_seq: AtomicU64,
    /// First spilled layer's `cost_layers.id` (sub 3 / acct-b8ub).
    /// `0` = unset; `> 0` = watermark for spill walks. CAS-set on
    /// first overflow; all in-shmem layers have id strictly less.
    pub overflow_first_spilled_layer_id: AtomicI64,
    pub _pad1: [u8; 16],
    /// Cell-lock-protected ring buffer. Access ONLY via `ring_mut`
    /// while holding this cell's LWLock EXCLUSIVE.
    pub ring: UnsafeCell<RingFields>,
}

// SAFETY: `UnsafeCell<RingFields>` opts out of Sync. We assert it by
// hand because the cell LWLock is the actual exclusion mechanism; the
// type-level !Sync is too conservative for the shmem use case.
unsafe impl Sync for FifoBucket {}
unsafe impl PGRXSharedMemory for FifoBucket {}

/// SAFETY: caller MUST hold cell's LWLock EXCLUSIVE. Returns a unique
/// `&mut RingFields` valid for the lifetime of the borrow. Multiple
/// concurrent EXCLUSIVE holders is prevented by PG's LWLock; multiple
/// SHARED holders should NOT call this (they'd UB-race a writer).
#[inline]
pub unsafe fn ring_mut(bucket: &FifoBucket) -> &mut RingFields {
    // UnsafeCell::get returns *mut T; the caller's LWLock guarantees
    // unique access.
    unsafe { &mut *bucket.ring.get() }
}

/// SAFETY: caller MUST hold cell's LWLock SHARED or EXCLUSIVE. Returns
/// a `&RingFields` for read-only snapshot (sub 4 drain, sub 6 recon).
#[inline]
#[allow(dead_code)]
pub unsafe fn ring_ref(bucket: &FifoBucket) -> &RingFields {
    unsafe { &*bucket.ring.get() }
}

/// FIFO arena — the buckets array. Wrapped in `PgLwLock` for table-wide
/// insert serialization (occupied 0→1 transitions). Hot-path probes /
/// reads use SHARED; new-cell inserts use EXCLUSIVE.
#[repr(C)]
pub struct FifoArena {
    pub buckets: [FifoBucket; FIFO_N_BUCKETS],
}

impl Default for FifoArena {
    fn default() -> Self {
        // SAFETY: every field is atomic / integer / Layer (i64+i64);
        // all valid as zero. Padding bytes are zero. Resulting arena
        // has every bucket occupied=0, all other fields = 0.
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for FifoArena {}

pub static FIFO_ARENA: PgLwLock<FifoArena> =
    unsafe { PgLwLock::new(c"fifo_arena") };

// ── per-cell LWLock tranche ──────────────────────────────────────────

/// Registration target for the `fifo_cell` LWLock tranche. The `()`
/// payload is a sentinel — this type carries no shmem data, only the
/// shmem_request_hook side effect of requesting the tranche and the
/// shmem_startup_hook side effect of capturing the tranche base pointer
/// into `FIFO_CELL_LOCK_BASE`.
///
/// Sub 1 design call: register exactly `FIFO_N_BUCKETS` locks under one
/// tranche so the kernel maps them adjacently in shmem. Per-cell
/// padding via PG's `LWLockPadded` (cache-line aligned by definition)
/// means concurrent acquires on different buckets don't false-share.
pub struct FifoCellTranche {
    _private: (),
}

unsafe impl Sync for FifoCellTranche {}
unsafe impl PGRXSharedMemory for FifoCellTranche {}

impl FifoCellTranche {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl PgSharedMemoryInitialization for FifoCellTranche {
    type Value = ();

    unsafe fn on_shmem_request(&'static self) {
        unsafe {
            pg_sys::RequestNamedLWLockTranche(
                c"fifo_cell".as_ptr(),
                FIFO_N_BUCKETS as i32,
            );
        }
    }

    unsafe fn on_shmem_startup(&'static self, _value: ()) {
        unsafe {
            let base = pg_sys::GetNamedLWLockTranche(c"fifo_cell".as_ptr());
            FIFO_CELL_LOCK_BASE.store(base, Ordering::Release);
        }
    }
}

pub static FIFO_CELL_TRANCHE: FifoCellTranche = FifoCellTranche::new();

/// Base pointer to the contiguous `[LWLockPadded; FIFO_N_BUCKETS]`
/// allocated by PG for the `fifo_cell` tranche. Set once at shmem
/// startup; never reassigned. Backends inherit by virtue of shared
/// memory mapping — the pointer is shmem-resident.
static FIFO_CELL_LOCK_BASE: AtomicPtr<pg_sys::LWLockPadded> =
    AtomicPtr::new(std::ptr::null_mut());

/// Returns the `*mut LWLock` for cell `idx`. Panics if `idx` is out of
/// range or if the tranche hasn't been initialized (caller bug — should
/// only happen pre-`_PG_init` completion).
///
/// Safety contract for callers: the returned pointer is valid for the
/// lifetime of the backend (shmem-resident). It must be used only via
/// PG's LWLockAcquire / LWLockRelease (or the `FifoCellGuard` wrapper
/// below); raw dereferencing is unsound.
#[inline]
fn cell_lock_ptr(idx: usize) -> *mut pg_sys::LWLock {
    assert!(idx < FIFO_N_BUCKETS, "fifo cell idx out of range");
    let base = FIFO_CELL_LOCK_BASE.load(Ordering::Acquire);
    assert!(
        !base.is_null(),
        "fifo_cell tranche not yet initialized — \
         caller invoked before shmem_startup_hook completed",
    );
    // SAFETY: tranche guarantees `FIFO_N_BUCKETS` consecutive
    // LWLockPadded entries starting at base; `idx < FIFO_N_BUCKETS`
    // by the assert above.
    unsafe { &raw mut (*base.add(idx)).lock }
}

/// Lock mode for `acquire_fifo_cell`.
#[derive(Copy, Clone, Debug)]
pub enum FifoCellMode {
    Shared,
    Exclusive,
}

impl FifoCellMode {
    #[inline]
    fn as_pg(self) -> pg_sys::LWLockMode::Type {
        match self {
            FifoCellMode::Shared => pg_sys::LWLockMode::LW_SHARED,
            FifoCellMode::Exclusive => pg_sys::LWLockMode::LW_EXCLUSIVE,
        }
    }
}

/// RAII guard for a held cell LWLock. Drop releases unless PG is
/// unwinding from an `elog(ERROR)` (mirrors pgrx's
/// `release_unless_elog_unwinding` pattern — if `InterruptHoldoffCount
/// == 0` we skip release because the abort path will clean up).
pub struct FifoCellGuard {
    lock: *mut pg_sys::LWLock,
    // Field is consumed by sub 2's `fifo_apply_batch` (acct-uy4p) so
    // multi-cell guard arrays can be keyed back to their cell indices
    // for the bulk-INSERT phase. Tagged `#[allow]` here so the sub 1
    // build is warning-clean.
    #[allow(dead_code)]
    idx: usize,
    _not_send: UnsafeCell<()>, // bind to current thread; lock release
                               // must happen on the same backend
}

impl FifoCellGuard {
    /// The cell index this guard protects. Useful for callers that
    /// need to keep guards alongside cell-index keys (sub 2).
    #[allow(dead_code)]
    #[inline]
    pub fn idx(&self) -> usize {
        self.idx
    }
}

impl Drop for FifoCellGuard {
    fn drop(&mut self) {
        // SAFETY: `lock` was acquired via `LWLockAcquire` and is valid
        // for the lifetime of the backend (shmem-resident). Skipping
        // release on elog-unwind matches pgrx convention.
        unsafe {
            if pg_sys::InterruptHoldoffCount > 0 {
                pg_sys::LWLockRelease(self.lock);
            }
        }
    }
}

/// Acquire `FIFO_ARENA.buckets[idx]`'s per-cell LWLock in the requested
/// mode. Returns an RAII guard that releases on drop.
///
/// Discipline (NOT enforced here; caller's responsibility):
/// multi-cell acquisitions in one critical section MUST be made in
/// cell-index ascending order to prevent deadlock between two callers
/// touching the same set of cells in different order.
pub fn acquire_fifo_cell(idx: usize, mode: FifoCellMode) -> FifoCellGuard {
    let lock = cell_lock_ptr(idx);
    unsafe {
        pg_sys::LWLockAcquire(lock, mode.as_pg());
    }
    FifoCellGuard {
        lock,
        idx,
        _not_send: UnsafeCell::new(()),
    }
}

/// Initialize the FIFO subsystem. Called once from the parent crate's
/// `_PG_init`. Wires both shmem hooks (request + startup) via pgrx's
/// `pg_shmem_init!` chain — the arena's buckets allocation and the
/// per-cell tranche registration ride the same hook list.
pub fn init() {
    pg_shmem_init!(FIFO_ARENA);
    pg_shmem_init!(FIFO_CELL_TRANCHE = ());
    // sub 4 (acct-0450) — FIFO drain observability counters.
    pg_shmem_init!(FIFO_DRAIN_CONSECUTIVE_FAILS);
    pg_shmem_init!(FIFO_DRAIN_TOTAL_FAILURES);
    pg_shmem_init!(FIFO_DRAIN_TICKS_TOTAL);
    pg_shmem_init!(FIFO_DRAIN_ROWS_TOTAL);
}

// ── SQL-callable test surface ────────────────────────────────────────
//
// These pg_externs exist so end-to-end tests in
// `poc/batch-ledger/tests/fifo_lwlock_tranche_t1.rs` can exercise the
// primitives from sqlx. They are NOT load-bearing for production use;
// sub 2's `fifo_apply_batch` is the only intended consumer of
// `acquire_fifo_cell` outside this crate.

/// Returns `FIFO_N_BUCKETS`. Smoke check that the static is reachable.
#[pg_extern]
pub fn fifo_arena_capacity() -> i64 {
    FIFO_N_BUCKETS as i64
}

/// Returns `MAX_LAYERS`. Useful for tests that want to know the
/// per-cell ring depth cap without hardcoding.
#[pg_extern]
pub fn fifo_max_layers() -> i64 {
    MAX_LAYERS as i64
}

/// Acquire + immediately release one cell's lock in the requested mode.
/// Smoke test for tranche init + acquire + release. Returns `true` on
/// success, raises on bad `idx` or uninitialized tranche.
///
/// `mode_excl=TRUE` → EXCLUSIVE, FALSE → SHARED.
#[pg_extern]
pub fn fifo_test_acquire_release(idx: i64, mode_excl: bool) -> bool {
    let mode = if mode_excl {
        FifoCellMode::Exclusive
    } else {
        FifoCellMode::Shared
    };
    let _guard = acquire_fifo_cell(idx as usize, mode);
    drop(_guard);
    true
}

/// Acquire two cells in sorted order, holding both simultaneously,
/// then release in LIFO order. Smoke test for the sorted-acquisition
/// discipline (sub 2's apply path will use the same shape).
///
/// Caller MUST pass `idx_a < idx_b`; raises otherwise (this is the
/// discipline check, not a runtime sort).
#[pg_extern]
pub fn fifo_test_acquire_two_sorted(idx_a: i64, idx_b: i64) -> bool {
    if !(idx_a < idx_b) {
        pgrx::error!(
            "fifo_test_acquire_two_sorted: idx_a ({}) must be \
             strictly less than idx_b ({})",
            idx_a,
            idx_b
        );
    }
    let g_a = acquire_fifo_cell(idx_a as usize, FifoCellMode::Exclusive);
    let g_b = acquire_fifo_cell(idx_b as usize, FifoCellMode::Exclusive);
    // Release in LIFO order (drop ordering matches declaration reverse).
    drop(g_b);
    drop(g_a);
    true
}

/// Returns the `*mut LWLock` address for `idx` as an i64 (raw pointer
/// bits). Lets tests verify that:
///
/// - Two calls for the same idx return the same address (stable).
/// - Two calls for different idx return different addresses
///   (per-cell, not table-wide).
/// - Addresses are non-zero (tranche was initialized).
///
/// NOT for use outside tests; raw pointer leakage is unsafe.
#[pg_extern]
pub fn fifo_test_cell_lock_addr(idx: i64) -> i64 {
    let ptr = cell_lock_ptr(idx as usize);
    ptr as usize as i64
}

// ── arena helpers (sub 2) ─────────────────────────────────────────────

use crate::{next_seq, stage_apply};
use pgrx::Spi;
use pgrx::PgAtomic;
use std::collections::{BTreeMap, HashMap};

// ── sub 4 (acct-0450) — FIFO drain observability ─────────────────────
//
// Mirrors the WAC drain counters in lib.rs. _CONSECUTIVE resets on the
// first clean tick after a failure run; _TOTAL never resets without
// `fifo_arena_reset`. Drain emits `warning!` when _CONSECUTIVE crosses
// FIFO_DRAIN_WARN_AFTER (5 ticks = 500 ms at the default 100 ms
// cadence — matches the WAC threshold).

pub(crate) static FIFO_DRAIN_CONSECUTIVE_FAILS: PgAtomic<
    std::sync::atomic::AtomicU64,
> = unsafe { PgAtomic::new(c"fifo_drain_consecutive_fails") };
pub(crate) static FIFO_DRAIN_TOTAL_FAILURES: PgAtomic<
    std::sync::atomic::AtomicU64,
> = unsafe { PgAtomic::new(c"fifo_drain_total_failures") };
pub(crate) static FIFO_DRAIN_TICKS_TOTAL: PgAtomic<
    std::sync::atomic::AtomicU64,
> = unsafe { PgAtomic::new(c"fifo_drain_ticks_total") };
pub(crate) static FIFO_DRAIN_ROWS_TOTAL: PgAtomic<
    std::sync::atomic::AtomicU64,
> = unsafe { PgAtomic::new(c"fifo_drain_rows_total") };
const FIFO_DRAIN_WARN_AFTER: u64 = 5;

/// Pack a (cost_book, product, location) tuple into the cell key. For
/// the PoC schema (one account per pool, no cost-book / product /
/// location dimensions yet), use `pack_fifo_key_poc` which puts the
/// pool_account_id in the upper 64 bits.
#[inline]
pub const fn pack_fifo_key_poc(pool_account_id: i64) -> u128 {
    (pool_account_id as u64 as u128) << 64
}

/// splitmix64-mixed hash over both u128 halves, masked to the bucket
/// count. Avoids cluster-on-sequential-account-ids pathologies.
#[inline]
pub fn slot_for_fifo(key: u128) -> usize {
    let mut z = (key as u64) ^ ((key >> 64) as u64);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z as usize) & (FIFO_N_BUCKETS - 1)
}

/// Probe (read-only) for `key` in the FIFO arena. Returns the cell
/// index on hit, `None` on miss (empty bucket terminates the probe
/// chain). Caller must hold `FIFO_ARENA.share()`.
#[inline]
pub fn probe_fifo_cell(arena: &FifoArena, key: u128) -> Option<usize> {
    let start = slot_for_fifo(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..FIFO_N_BUCKETS {
        let idx = (start + probe) & (FIFO_N_BUCKETS - 1);
        let b = &arena.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return None;
        }
        if b.key_hi.load(Ordering::Acquire) == key_hi
            && b.key_lo.load(Ordering::Acquire) == key_lo
        {
            return Some(idx);
        }
    }
    None
}

/// Insert (or find) `key` in the FIFO arena. Caller MUST hold
/// `FIFO_ARENA.exclusive()` so no concurrent inserter races us. Returns
/// the cell index on success, `None` if the table is full.
///
/// Newly inserted cells have `occupied=1, seeded=0` — the apply path's
/// cell-lock critical section detects `seeded==0` and triggers the
/// lazy-seed push.
#[inline]
pub fn insert_fifo_cell(arena: &FifoArena, key: u128) -> Option<usize> {
    let start = slot_for_fifo(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..FIFO_N_BUCKETS {
        let idx = (start + probe) & (FIFO_N_BUCKETS - 1);
        let b = &arena.buckets[idx];
        if b.occupied.load(Ordering::Relaxed) == 0 {
            b.key_hi.store(key_hi, Ordering::Relaxed);
            b.key_lo.store(key_lo, Ordering::Relaxed);
            b.occupied.store(1, Ordering::Release);
            // seeded stays 0 by default — apply path lazy-seeds.
            return Some(idx);
        }
        if b.key_hi.load(Ordering::Relaxed) == key_hi
            && b.key_lo.load(Ordering::Relaxed) == key_lo
        {
            return Some(idx);
        }
    }
    None
}

/// Push a layer to the tail of the ring. **Caller MUST hold the cell's
/// LWLock EXCLUSIVE.** Returns `false` if the ring is full
/// (`MAX_LAYERS` reached). The caller (sub 3 / acct-b8ub) handles
/// overflow by setting the bucket's `overflow_active` flag, CAS-setting
/// `overflow_first_spilled_layer_id` to the overflowing layer's id,
/// and still INSERTing the durable `cost_layers` row — the layer
/// becomes a spilled tail entry consumed via SPI walk after the ring
/// drains.
#[inline]
pub fn push_layer(ring: &mut RingFields, layer: Layer) -> bool {
    if ring.n_layers as usize >= MAX_LAYERS {
        return false;
    }
    let tail = (ring.head as usize + ring.n_layers as usize) % MAX_LAYERS;
    ring.layers[tail] = layer;
    ring.n_layers += 1;
    true
}

/// One consumed slice from an issue walk.
#[derive(Copy, Clone, Debug)]
pub struct ConsumedSlice {
    pub layer_id: i64,
    pub qty_consumed: i64,
    pub unit_cost: i64,
}

/// Result of an issue consumption walk.
pub struct ConsumeResult {
    /// Sum of `qty_consumed * unit_cost` over `consumed`.
    pub total_cost: i64,
    /// One entry per layer touched, in head-to-tail order. Drives the
    /// bulk-INSERT into `cost_layer_depletions`.
    pub consumed: Vec<ConsumedSlice>,
    /// > 0 if the ring couldn't satisfy the requested qty. Caller
    /// surfaces as a P0006-style error.
    pub shortage: i64,
}

/// Stage a `(layer_id, qty_consumed)` entry into the cell's
/// pending_drain ring (sub 4 / acct-0450). **Caller MUST hold the
/// cell's LWLock EXCLUSIVE.** Returns `false` on overflow
/// (PENDING_DRAIN_CAP reached); caller falls back to inline UPDATE.
#[inline]
pub fn push_pending_drain(ring: &mut RingFields, layer_id: i64, qty_consumed: i64) -> bool {
    if ring.pending_drain_n as usize >= PENDING_DRAIN_CAP {
        return false;
    }
    let tail = ring.pending_drain_n as usize;
    ring.pending_drain[tail] = PendingDrain {
        layer_id,
        qty_consumed,
    };
    ring.pending_drain_n += 1;
    true
}

/// Snapshot + reset the cell's pending_drain ring. **Caller MUST hold
/// the cell's LWLock EXCLUSIVE.** Returns the entries that were
/// staged since the last snapshot. Used by the bgworker drain.
#[inline]
pub fn drain_pending_into(ring: &mut RingFields, out: &mut Vec<PendingDrain>) {
    let n = ring.pending_drain_n as usize;
    if n == 0 {
        return;
    }
    out.reserve(n);
    for i in 0..n {
        out.push(ring.pending_drain[i]);
    }
    ring.pending_drain_n = 0;
}

/// Walk the ring from head and consume `qty` units in FIFO order.
/// **Caller MUST hold the cell's LWLock EXCLUSIVE.**
#[inline]
pub fn consume_from_head(ring: &mut RingFields, qty: i64) -> ConsumeResult {
    let mut remaining = qty;
    let mut total_cost: i64 = 0;
    let mut consumed: Vec<ConsumedSlice> = Vec::new();
    while remaining > 0 && ring.n_layers > 0 {
        let head_idx = ring.head as usize;
        let layer = ring.layers[head_idx];
        let take = remaining.min(layer.qty);
        total_cost = total_cost.saturating_add(take.saturating_mul(layer.unit_cost));
        consumed.push(ConsumedSlice {
            layer_id: layer.layer_id,
            qty_consumed: take,
            unit_cost: layer.unit_cost,
        });
        if take == layer.qty {
            // fully consumed; advance head
            ring.head = ((ring.head as usize + 1) % MAX_LAYERS) as u16;
            ring.n_layers -= 1;
        } else {
            ring.layers[head_idx].qty -= take;
        }
        remaining -= take;
    }
    ConsumeResult {
        total_cost,
        consumed,
        shortage: remaining,
    }
}

/// Get a shared reference to bucket `idx`. Caller may hold
/// `FIFO_ARENA.share()`; atomic fields are touched via Atomic*
/// methods. The ring-protected fields require the cell LWLock; use
/// `ring_mut` to enter that critical section.
#[inline]
fn bucket(arena: &FifoArena, idx: usize) -> &FifoBucket {
    &arena.buckets[idx]
}

// ── fifo_apply_batch_maximal pg_extern ────────────────────────────────

/// Sub 2 — synchronous-apply FIFO batch function. The F-shmem path:
/// layer state lives in the FIFO arena, not in `cost_layers`.
/// Durable `cost_layers` + `cost_layer_depletions` are written inline
/// as the audit trail; the bgworker (sub 4) keeps them current with
/// `qty_remaining`.
///
/// Flow (per design):
///
/// 1. Parse envelopes.
/// 2. Replay-detect (SPI; no locks).
/// 3. Probe cells under `FIFO_ARENA.share()`. Hits get cell_idx;
///    misses go to step 4.
/// 4. If any misses: `FIFO_ARENA.exclusive()`, re-probe + insert
///    missing keys. Drop EXCLUSIVE.
/// 5. Pre-allocate `layer_id` values for receipt envelopes via
///    `nextval('cost_layers_id_seq')` (SPI; no cell locks).
/// 6. Lazy-seed scan: for each cell with at least one issue envelope
///    AND `seeded == 0`, SPI-fetch active `cost_layers` rows into a
///    local cache. (No cell locks held.)
/// 7. Group envelopes by cell_idx in `BTreeMap` (auto-sorted).
/// 8. Apply phase: for each cell in sorted order, acquire cell
///    EXCLUSIVE, push lazy-seeded layers if `seeded == 0`, then apply
///    each envelope (push for receipts, consume for issues). Stamp
///    `last_seq`. Release.
/// 9. Bulk INSERTs (SPI; no cell locks): posting_lines via UNNEST,
///    cost_layers via UNNEST with explicit pre-alloc ids,
///    cost_layer_depletions via UNNEST, UPDATE cost_layers
///    qty_remaining for pre-existing drained layers.
/// 10. `stage_apply` for balance/qty deltas (existing shmem path).
/// 11. Return per-envelope results.
///
/// Scope (matches `fifo_apply_batch_inline`): only `fifo_receipt` /
/// `fifo_issue` kinds; transfers excluded. `qty` always non-null.
#[pg_extern]
pub fn fifo_apply_batch_maximal(
    envelopes: pgrx::JsonB,
) -> TableIterator<
    'static,
    (
        name!(envelope_idx, i32),
        name!(status, String),
        name!(posting_line_id, Option<i64>),
        name!(error_code, Option<String>),
        name!(error_message, Option<String>),
    ),
> {
    let arr = match envelopes.0.as_array() {
        Some(a) => a,
        None => pgrx::error!("fifo_apply_batch_maximal: envelopes must be a JSONB array"),
    };

    #[derive(Clone, Copy, PartialEq)]
    enum Kind {
        FifoReceipt,
        FifoIssue,
    }
    struct Parsed {
        envelope_idx: i32,
        kind: Kind,
        debit: i64,
        credit: i64,
        qty: i64,
        unit_cost: Option<i64>,
        idempotency_key: String,
        business_date: String,
    }

    // Phase 1 — parse + validate.
    let mut parsed: Vec<Parsed> = Vec::with_capacity(arr.len());
    for (idx, env) in arr.iter().enumerate() {
        let obj = match env.as_object() {
            Some(o) => o,
            None => pgrx::error!(
                "fifo_apply_batch_maximal: envelope[{}] is not an object",
                idx
            ),
        };
        let kind_str = obj
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("fifo_receipt");
        let kind = match kind_str {
            "fifo_receipt" => Kind::FifoReceipt,
            "fifo_issue" => Kind::FifoIssue,
            other => pgrx::error!(
                "fifo_apply_batch_maximal: envelope[{}] unsupported kind '{}'",
                idx,
                other
            ),
        };
        let envelope_idx = obj
            .get("envelope_idx")
            .and_then(|v| v.as_i64())
            .unwrap_or(idx as i64) as i32;
        let debit = obj
            .get("debit_account_id")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| {
                pgrx::error!(
                    "fifo_apply_batch_maximal: envelope[{}] missing debit_account_id",
                    idx
                )
            });
        let credit = obj
            .get("credit_account_id")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| {
                pgrx::error!(
                    "fifo_apply_batch_maximal: envelope[{}] missing credit_account_id",
                    idx
                )
            });
        let qty = obj.get("qty").and_then(|v| v.as_i64()).unwrap_or_else(|| {
            pgrx::error!("fifo_apply_batch_maximal: envelope[{}] missing qty", idx)
        });
        if qty <= 0 {
            pgrx::error!(
                "fifo_apply_batch_maximal: envelope[{}] qty must be > 0",
                idx
            );
        }
        let unit_cost = obj.get("unit_cost").and_then(|v| v.as_i64());
        if kind == Kind::FifoReceipt && unit_cost.unwrap_or(0) <= 0 {
            pgrx::error!(
                "fifo_apply_batch_maximal: envelope[{}] fifo_receipt missing/invalid unit_cost",
                idx
            );
        }
        let idempotency_key = obj
            .get("idempotency_key")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                pgrx::error!(
                    "fifo_apply_batch_maximal: envelope[{}] missing idempotency_key",
                    idx
                )
            })
            .to_string();
        let business_date = obj
            .get("business_date")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                pgrx::error!(
                    "fifo_apply_batch_maximal: envelope[{}] missing business_date",
                    idx
                )
            })
            .to_string();
        parsed.push(Parsed {
            envelope_idx,
            kind,
            debit,
            credit,
            qty,
            unit_cost,
            idempotency_key,
            business_date,
        });
    }

    // Phase 2 — replay detect via SPI (no locks).
    let idemp_keys: Vec<String> = parsed.iter().map(|p| p.idempotency_key.clone()).collect();
    let replay_map: HashMap<String, i64> = Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![idemp_keys.clone().into()];
        let tup = client
            .select(
                "SELECT idempotency_key::TEXT AS k, id FROM posting_lines \
                 WHERE idempotency_key = ANY($1::text[]::uuid[])",
                None,
                &args,
            )
            .expect("maximal: replay SELECT");
        let mut m: HashMap<String, i64> = HashMap::new();
        for row in tup {
            let k: String = row["k"].value().unwrap().unwrap();
            let id: i64 = row["id"].value().unwrap().unwrap();
            m.insert(k, id);
        }
        m
    });

    // Phase 3 — for each non-replay envelope, identify the FIFO pool
    // (receipt debit; issue credit) and compute the cell key. Probe.
    let mut env_pool: Vec<Option<i64>> = vec![None; parsed.len()];
    let mut env_cell: Vec<Option<usize>> = vec![None; parsed.len()];
    let mut pool_to_cell: HashMap<i64, usize> = HashMap::new();
    let mut missing_pools: Vec<i64> = Vec::new();
    {
        let arena = FIFO_ARENA.share();
        for (i, p) in parsed.iter().enumerate() {
            if replay_map.contains_key(&p.idempotency_key) {
                continue;
            }
            let pool_account_id = match p.kind {
                Kind::FifoReceipt => p.debit,
                Kind::FifoIssue => p.credit,
            };
            env_pool[i] = Some(pool_account_id);
            if let Some(&cell) = pool_to_cell.get(&pool_account_id) {
                env_cell[i] = Some(cell);
                continue;
            }
            let key = pack_fifo_key_poc(pool_account_id);
            match probe_fifo_cell(&arena, key) {
                Some(cell) => {
                    env_cell[i] = Some(cell);
                    pool_to_cell.insert(pool_account_id, cell);
                }
                None => {
                    if !missing_pools.contains(&pool_account_id) {
                        missing_pools.push(pool_account_id);
                    }
                }
            }
        }
    }

    // Phase 4 — if any pools missed, take EXCLUSIVE + re-probe + insert.
    //
    // sub 8 (acct-cpe8): if `insert_fifo_cell` returns None (arena full,
    // open-addressing probe walked all FIFO_N_BUCKETS without finding a
    // free slot), the pool is marked `durable_only` rather than raising.
    // Its envelopes apply via SPI in Phase 8.6 — no shmem cell, no ring,
    // no pending_drain. `env_cell[i]` stays None for those envelopes.
    let mut durable_only_pools: std::collections::HashSet<i64> =
        std::collections::HashSet::new();
    if !missing_pools.is_empty() {
        let arena = FIFO_ARENA.exclusive();
        for pool_account_id in &missing_pools {
            let key = pack_fifo_key_poc(*pool_account_id);
            match insert_fifo_cell(&arena, key) {
                Some(cell) => {
                    pool_to_cell.insert(*pool_account_id, cell);
                }
                None => {
                    durable_only_pools.insert(*pool_account_id);
                }
            }
        }
        // Backfill env_cell for envelopes whose pool was in missing_pools.
        // durable_only pools have no entry in pool_to_cell, so env_cell
        // stays None for them — Phase 7 routes them into durable_only_envs.
        for (i, p) in parsed.iter().enumerate() {
            if env_cell[i].is_some() {
                continue;
            }
            if let Some(pool) = env_pool[i] {
                env_cell[i] = pool_to_cell.get(&pool).copied();
            }
            // replays: env_pool[i] is None; env_cell stays None.
            // durable_only: env_pool[i] is Some(pool) but pool_to_cell
            // doesn't have it; env_cell stays None.
            let _ = p; // unused
        }
    }

    // Phase 5 — pre-alloc layer_ids for non-replay receipts via SPI.
    let n_receipts = parsed
        .iter()
        .filter(|p| {
            p.kind == Kind::FifoReceipt && !replay_map.contains_key(&p.idempotency_key)
        })
        .count();
    let layer_ids: Vec<i64> = if n_receipts > 0 {
        Spi::connect(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![(n_receipts as i64).into()];
            let tup = client
                .select(
                    "SELECT nextval('cost_layers_id_seq')::bigint AS lid \
                     FROM generate_series(1, $1::bigint)",
                    None,
                    &args,
                )
                .expect("maximal: nextval pre-alloc");
            let mut v: Vec<i64> = Vec::with_capacity(n_receipts);
            for row in tup {
                v.push(row["lid"].value().unwrap().unwrap());
            }
            v
        })
    } else {
        Vec::new()
    };
    // Map each receipt envelope to its pre-allocated id.
    let mut env_layer_id: Vec<Option<i64>> = vec![None; parsed.len()];
    {
        let mut k = 0usize;
        for (i, p) in parsed.iter().enumerate() {
            if p.kind == Kind::FifoReceipt && !replay_map.contains_key(&p.idempotency_key) {
                env_layer_id[i] = Some(layer_ids[k]);
                k += 1;
            }
        }
    }

    // Phase 6 — lazy-seed scan: cells with seeded==0 that this batch
    // will touch (receipt OR issue). SPI-fetch ordered active layers
    // per pool.
    //
    // Originally the filter required `kind == FifoIssue` on the
    // theory that receipts don't need to know existing layers. That
    // missed a case: if a cell's FIRST batch is receipt-only, Phase 8
    // unconditionally stamps `seeded=1` even though no durable seed
    // ran — durable cost_layers rows from prior sessions / bench
    // pre-seed are silently abandoned, and the next batch's issue
    // sees an under-populated ring → "fifo_issue short" against a
    // durable pool that actually has plenty. Queueing seed on any
    // first-touch (regardless of kind) ensures durable layers land
    // in the ring HEAD before the receipts append at the tail.
    let mut cells_needing_seed: Vec<(usize, i64)> = Vec::new(); // (cell_idx, pool_account_id)
    {
        let arena = FIFO_ARENA.share();
        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for (i, p) in parsed.iter().enumerate() {
            if replay_map.contains_key(&p.idempotency_key) {
                continue;
            }
            // sub 8 (acct-cpe8): durable_only envelopes have no cell —
            // they apply via SPI in Phase 8.6 and bypass the lazy-seed
            // scan entirely.
            let cell = match env_cell[i] {
                Some(c) => c,
                None => continue,
            };
            if !seen.insert(cell) {
                continue;
            }
            let b = &arena.buckets[cell];
            if b.seeded.load(Ordering::Acquire) == 0 {
                cells_needing_seed.push((cell, env_pool[i].unwrap()));
            }
        }
    }

    struct SeedLayer {
        layer_id: i64,
        qty_remaining: i64,
        unit_cost: i64,
    }
    let mut seed_cache: HashMap<usize, Vec<SeedLayer>> = HashMap::new();
    if !cells_needing_seed.is_empty() {
        let pool_ids: Vec<i64> = cells_needing_seed.iter().map(|(_, p)| *p).collect();
        let fetched: Vec<(i64, i64, i64, i64)> = Spi::connect(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![pool_ids.clone().into()];
            let tup = client
                .select(
                    "SELECT cl.pool_account_id, cl.id, cl.qty_remaining, cl.unit_cost \
                     FROM cost_layers cl \
                     WHERE cl.pool_account_id = ANY($1::bigint[]) AND cl.qty_remaining > 0 \
                     ORDER BY cl.pool_account_id, cl.receipt_date, cl.id",
                    None,
                    &args,
                )
                .expect("maximal: SELECT cost_layers for seed");
            let mut v: Vec<(i64, i64, i64, i64)> = Vec::new();
            for row in tup {
                v.push((
                    row["pool_account_id"].value().unwrap().unwrap(),
                    row["id"].value().unwrap().unwrap(),
                    row["qty_remaining"].value().unwrap().unwrap(),
                    row["unit_cost"].value().unwrap().unwrap(),
                ));
            }
            v
        });
        // Bucket by cell_idx.
        let pool_to_cell_map: HashMap<i64, usize> = cells_needing_seed
            .iter()
            .map(|(c, p)| (*p, *c))
            .collect();
        for (pid, lid, qr, uc) in fetched {
            if let Some(&cell) = pool_to_cell_map.get(&pid) {
                seed_cache.entry(cell).or_insert_with(Vec::new).push(SeedLayer {
                    layer_id: lid,
                    qty_remaining: qr,
                    unit_cost: uc,
                });
            }
        }
        // Ensure every cell in cells_needing_seed has a (possibly empty) entry.
        for (cell, _) in &cells_needing_seed {
            seed_cache.entry(*cell).or_insert_with(Vec::new);
        }
    }

    // Phase 7 — group envelopes by cell_idx in sorted order.
    //
    // sub 8 (acct-cpe8): durable_only envelopes (env_cell[i] is None
    // because the pool got no shmem cell in Phase 4) are routed into
    // `durable_only_envs` in original envelope_idx order; Phase 8.6
    // handles them via SPI after Phase 8.5.
    let mut cells_to_envelopes: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut durable_only_envs: Vec<usize> = Vec::new();
    for (i, p) in parsed.iter().enumerate() {
        if replay_map.contains_key(&p.idempotency_key) {
            continue;
        }
        match env_cell[i] {
            Some(cell) => {
                cells_to_envelopes.entry(cell).or_default().push(i);
            }
            None => {
                durable_only_envs.push(i);
            }
        }
    }

    // Result + INSERT payload accumulators.
    let mut env_total_cost: Vec<Option<i64>> = vec![None; parsed.len()];
    // posting_lines:
    let mut pl_debit: Vec<i64> = Vec::new();
    let mut pl_credit: Vec<i64> = Vec::new();
    let mut pl_amount: Vec<i64> = Vec::new();
    let mut pl_currency: Vec<String> = Vec::new(); // resolved post-apply
    let mut pl_idemp: Vec<String> = Vec::new();
    let mut pl_bdate: Vec<String> = Vec::new();
    let mut pl_qty: Vec<i64> = Vec::new();
    let mut pl_env_idx: Vec<usize> = Vec::new(); // parallel; for env_idx lookup
    // cost_layers (new from receipts):
    let mut nl_id: Vec<i64> = Vec::new();
    let mut nl_pool: Vec<i64> = Vec::new();
    let mut nl_qty: Vec<i64> = Vec::new();
    let mut nl_uc: Vec<i64> = Vec::new();
    let mut nl_bdate: Vec<String> = Vec::new();
    let mut nl_idemp: Vec<String> = Vec::new(); // for receipt_posting_line_id lookup
    // cost_layer_depletions:
    let mut dp_layer_id: Vec<i64> = Vec::new();
    let mut dp_issue_idemp: Vec<String> = Vec::new();
    let mut dp_qty: Vec<i64> = Vec::new();
    let mut dp_cost: Vec<i64> = Vec::new();
    // pre-existing layer drains (UPDATE qty_remaining) — INLINE
    // FALLBACK only. sub 4 (acct-0450): the default path stages drains
    // into the cell's `pending_drain` ring inside the cell lock; the
    // bgworker UPDATEs cost_layers.qty_remaining at drain_interval_ms
    // cadence. If `push_pending_drain` overflows
    // (PENDING_DRAIN_CAP reached between drain ticks), the overflow
    // entries fall back here for inline UPDATE in Phase 9e.
    let mut accum_drain: HashMap<i64, i64> = HashMap::new();
    // balance + qty deltas (stage_apply):
    let mut bal_deltas: HashMap<i64, (i64, i64)> = HashMap::new();

    // sub 3 (acct-b8ub) — spill-deferred issue queue. An issue whose
    // `consume_from_head` returned `shortage > 0` AND whose cell is
    // `overflow_active = 1` cannot satisfy entirely from the in-shmem
    // ring; the remainder lives in durable `cost_layers` rows that
    // never resided in the ring. Phase 8.5 walks them via SPI under
    // `FOR UPDATE` and folds the spill slices into the same envelope's
    // `dp_*` + `pl_*` + `bal_deltas` accumulators. While the envelope
    // sits in `spill_queue`, its `pl_*` / `bal_deltas` updates are
    // DEFERRED — Phase 8 only pushes the in-shmem `dp_*` slices and
    // stages pending_drain. The final amount cannot be computed until
    // the spill walk completes.
    struct SpillIssueState {
        env_i: usize,
        shortage: i64,
        in_shmem_total_cost: i64,
        watermark: i64,
    }
    let mut spill_queue: Vec<SpillIssueState> = Vec::new();

    // Phase 8 — sorted cell-lock acquisition + apply.
    {
        let arena = FIFO_ARENA.share();
        for (&cell_idx, env_indices) in &cells_to_envelopes {
            let _guard = acquire_fifo_cell(cell_idx, FifoCellMode::Exclusive);
            let b = bucket(&arena, cell_idx);
            // SAFETY: cell LWLock held EXCLUSIVE; sole writer of ring.
            let ring = unsafe { ring_mut(b) };

            // Lazy-seed if needed (under cell lock so transition is atomic).
            //
            // Seeds arrive ordered by `(receipt_date, id)` ascending. The
            // first `MAX_LAYERS` are pushed into the ring (head); on the
            // (MAX_LAYERS+1)-th `push_layer` returns false. sub 3
            // (acct-b8ub) handles this by flipping `overflow_active=1`,
            // CAS-setting the watermark to the first un-pushed seed's
            // `layer_id`, and breaking out of the seed loop — the
            // remaining durable rows stay as the spilled tail and get
            // consumed via Phase 8.5 SPI walk when issues drain past
            // the ring.
            if b.seeded.load(Ordering::Acquire) == 0 {
                if let Some(seeds) = seed_cache.get(&cell_idx) {
                    for s in seeds {
                        let ok = push_layer(
                            ring,
                            Layer {
                                qty: s.qty_remaining,
                                unit_cost: s.unit_cost,
                                layer_id: s.layer_id,
                            },
                        );
                        if !ok {
                            b.overflow_active.store(1, Ordering::Release);
                            // CAS 0 -> s.layer_id; preserves the FIRST
                            // overflowing layer's id if multiple seeds
                            // would have overflowed across multiple
                            // sessions (sticky-monotone watermark).
                            let _ = b
                                .overflow_first_spilled_layer_id
                                .compare_exchange(
                                    0,
                                    s.layer_id,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                );
                            break;
                        }
                    }
                }
                b.seeded.store(1, Ordering::Release);
            }

            // Apply envelopes in original envelope order within the cell.
            for &i in env_indices.iter() {
                let p = &parsed[i];
                match p.kind {
                    Kind::FifoReceipt => {
                        let uc = p.unit_cost.expect("validated");
                        let lid = env_layer_id[i].expect("pre-allocated");
                        let amt = p.qty.saturating_mul(uc);
                        // sub 3 (acct-b8ub) sticky-spill: if the cell
                        // is already overflow_active, skip the ring
                        // push regardless of whether prior drains have
                        // made room — preserves the watermark invariant
                        // (all in-shmem layers have id < watermark).
                        // Otherwise try push; on overflow flip the flag
                        // and capture the watermark. The durable
                        // `cost_layers` INSERT (Phase 9c), posting_line
                        // emission, and bal_deltas update happen
                        // unconditionally — the receipt occurred, the
                        // ledger records it, only the ring is bypassed.
                        let already_overflow =
                            b.overflow_active.load(Ordering::Acquire) == 1;
                        let pushed = if already_overflow {
                            false
                        } else {
                            push_layer(
                                ring,
                                Layer {
                                    qty: p.qty,
                                    unit_cost: uc,
                                    layer_id: lid,
                                },
                            )
                        };
                        if !pushed {
                            b.overflow_active.store(1, Ordering::Release);
                            let _ = b
                                .overflow_first_spilled_layer_id
                                .compare_exchange(
                                    0,
                                    lid,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                );
                        }
                        env_total_cost[i] = Some(amt);
                        pl_debit.push(p.debit);
                        pl_credit.push(p.credit);
                        pl_amount.push(amt);
                        pl_currency.push(String::new()); // filled post-apply
                        pl_idemp.push(p.idempotency_key.clone());
                        pl_bdate.push(p.business_date.clone());
                        pl_qty.push(p.qty);
                        pl_env_idx.push(i);
                        nl_id.push(lid);
                        nl_pool.push(p.debit);
                        nl_qty.push(p.qty);
                        nl_uc.push(uc);
                        nl_bdate.push(p.business_date.clone());
                        nl_idemp.push(p.idempotency_key.clone());
                        let e = bal_deltas.entry(p.debit).or_insert((0, 0));
                        e.0 += amt;
                        e.1 += p.qty;
                        let e = bal_deltas.entry(p.credit).or_insert((0, 0));
                        e.0 -= amt;
                    }
                    Kind::FifoIssue => {
                        let res = consume_from_head(ring, p.qty);
                        // In-shmem slices ALWAYS post to dp_* + stage
                        // pending_drain — they're real consumes the
                        // ring already mutated. This holds whether or
                        // not we go to Phase 8.5 for the spill tail.
                        for slice in &res.consumed {
                            dp_layer_id.push(slice.layer_id);
                            dp_issue_idemp.push(p.idempotency_key.clone());
                            dp_qty.push(slice.qty_consumed);
                            dp_cost.push(slice.qty_consumed.saturating_mul(slice.unit_cost));
                            // sub 4: stage drain into pending_drain ring
                            // under cell lock. Overflow → inline fallback.
                            let staged = push_pending_drain(
                                ring,
                                slice.layer_id,
                                slice.qty_consumed,
                            );
                            if !staged {
                                *accum_drain.entry(slice.layer_id).or_insert(0) +=
                                    slice.qty_consumed;
                            }
                        }

                        if res.shortage > 0 {
                            let overflow_active =
                                b.overflow_active.load(Ordering::Acquire) == 1;
                            if !overflow_active {
                                // Pool truly exhausted (no shmem, no
                                // spill tail) — error as today.
                                pgrx::error!(
                                    "fifo_apply_batch_maximal: envelope[{}] fifo_issue short by {} units \
                                     (pool {} exhausted in shmem ring)",
                                    p.envelope_idx,
                                    res.shortage,
                                    p.credit
                                );
                            }
                            // Spill-defer to Phase 8.5. We still hold
                            // the cell lock here; we capture the
                            // watermark snapshot under the lock so the
                            // SPI walk's `id >= watermark` predicate
                            // is consistent with cell state at this
                            // envelope's point in the batch order.
                            let watermark = b
                                .overflow_first_spilled_layer_id
                                .load(Ordering::Acquire);
                            spill_queue.push(SpillIssueState {
                                env_i: i,
                                shortage: res.shortage,
                                in_shmem_total_cost: res.total_cost,
                                watermark,
                            });
                            // DO NOT push pl_* / bal_deltas here —
                            // total_cost is still incomplete (spill
                            // contribution unknown). Phase 8.5 pushes
                            // them with the final amount.
                        } else {
                            env_total_cost[i] = Some(res.total_cost);
                            pl_debit.push(p.debit);
                            pl_credit.push(p.credit);
                            pl_amount.push(res.total_cost);
                            pl_currency.push(String::new()); // filled post-apply
                            pl_idemp.push(p.idempotency_key.clone());
                            pl_bdate.push(p.business_date.clone());
                            pl_qty.push(p.qty);
                            pl_env_idx.push(i);
                            let e = bal_deltas.entry(p.debit).or_insert((0, 0));
                            e.0 += res.total_cost;
                            let e = bal_deltas.entry(p.credit).or_insert((0, 0));
                            e.0 -= res.total_cost;
                            e.1 -= p.qty;
                        }
                    }
                }
            }

            // Stamp last_seq for drain.
            let seq = next_seq();
            b.last_seq.fetch_max(seq, Ordering::AcqRel);
            // guard drops here, releasing the cell lock.
        }
    }

    // Phase 8.5 (sub 3 / acct-b8ub) — spill consume for issues whose
    // in-shmem consumption ran short on an overflow_active cell.
    //
    // Discipline: NO FifoCellGuards are held in this scope (Phase 8's
    // closing `}` released the last one). The methodology rule "NEVER
    // do SPI under cell-lock" is preserved — spill walks acquire
    // row-level locks via `FOR UPDATE` on `cost_layers` and rely on
    // those for serialization across concurrent backends.
    //
    // Per-pool FIFO ordering within this batch is preserved because
    // (a) `cells_to_envelopes` iterates cells in ascending order and
    // (b) `env_indices` inside each cell preserves the original
    // envelope_idx order — so `spill_queue` is pushed in envelope
    // order for a given pool, and `Vec` iteration here matches.
    //
    // Same-pool spill walks from different envelopes in this batch
    // see prior envelopes' UPDATEs because both reads and writes
    // execute in the same Postgres txn (SPI runs in the apply call's
    // own snapshot).
    if !spill_queue.is_empty() {
        for spill in &spill_queue {
            let p = &parsed[spill.env_i];

            // SPI walk: durable rows for the pool whose id is at or
            // beyond the spill watermark and still have residual qty.
            // FOR UPDATE serializes against other backends mid-spill
            // on the same pool. The select happens via `connect_mut`
            // because FOR UPDATE counts as a writer for SPI's purposes.
            let candidates: Vec<(i64, i64, i64)> = Spi::connect_mut(|client| {
                let args: Vec<pgrx::datum::DatumWithOid> =
                    vec![p.credit.into(), spill.watermark.into()];
                let tup = client
                    .update(
                        "SELECT id, qty_remaining, unit_cost FROM cost_layers \
                         WHERE pool_account_id = $1::bigint \
                           AND id >= $2::bigint \
                           AND qty_remaining > 0 \
                         ORDER BY receipt_date, id \
                         FOR UPDATE",
                        None,
                        &args,
                    )
                    .expect("maximal: spill SELECT cost_layers FOR UPDATE");
                let mut v: Vec<(i64, i64, i64)> = Vec::new();
                for row in tup {
                    let lid: i64 = row["id"].value().unwrap().unwrap();
                    let qr: i64 = row["qty_remaining"].value().unwrap().unwrap();
                    let uc: i64 = row["unit_cost"].value().unwrap().unwrap();
                    v.push((lid, qr, uc));
                }
                v
            });

            // Walk candidates head-to-tail, consume up to `shortage`.
            let mut remaining = spill.shortage;
            let mut spill_total_cost: i64 = 0;
            // (layer_id, qty_taken, unit_cost) for dp_* + accum_drain.
            let mut spill_consumed: Vec<(i64, i64, i64)> = Vec::new();
            for (lid, qty_rem, uc) in candidates {
                if remaining == 0 {
                    break;
                }
                let take = remaining.min(qty_rem);
                spill_total_cost =
                    spill_total_cost.saturating_add(take.saturating_mul(uc));
                spill_consumed.push((lid, take, uc));
                remaining -= take;
            }

            if remaining > 0 {
                pgrx::error!(
                    "fifo_apply_batch_maximal: envelope[{}] fifo_issue short by {} units \
                     (pool {} exhausted in shmem ring + spilled tail; \
                      watermark={})",
                    p.envelope_idx,
                    remaining,
                    p.credit,
                    spill.watermark
                );
            }

            // Append spill slices to dp_* and stage their durable
            // UPDATE via accum_drain (Phase 9e). Spilled layers never
            // sat in the shmem ring, so they never went through
            // pending_drain; the inline UPDATE is the only path to
            // decrement their qty_remaining.
            for (lid, qty, uc) in &spill_consumed {
                dp_layer_id.push(*lid);
                dp_issue_idemp.push(p.idempotency_key.clone());
                dp_qty.push(*qty);
                dp_cost.push(qty.saturating_mul(*uc));
                *accum_drain.entry(*lid).or_insert(0) += qty;
            }

            // Now we know the final total_cost — push pl_* and bal_deltas.
            let total_cost = spill
                .in_shmem_total_cost
                .saturating_add(spill_total_cost);
            env_total_cost[spill.env_i] = Some(total_cost);
            pl_debit.push(p.debit);
            pl_credit.push(p.credit);
            pl_amount.push(total_cost);
            pl_currency.push(String::new()); // filled post-apply
            pl_idemp.push(p.idempotency_key.clone());
            pl_bdate.push(p.business_date.clone());
            pl_qty.push(p.qty);
            pl_env_idx.push(spill.env_i);
            let e = bal_deltas.entry(p.debit).or_insert((0, 0));
            e.0 += total_cost;
            let e = bal_deltas.entry(p.credit).or_insert((0, 0));
            e.0 -= total_cost;
            e.1 -= p.qty;
        }
    }

    // Phase 8.6 (sub 8 / acct-cpe8) — durable-only envelopes.
    //
    // Pools that got no shmem cell in Phase 4 (arena bucket exhaustion)
    // apply directly against `cost_layers` via SPI. No cell lock is
    // taken; the FOR UPDATE row locks on `cost_layers` provide
    // serialization with other concurrent backends. Receipts INSERT
    // a new layer (via the existing Phase 9c bulk path — push to
    // `nl_*` here). Issues walk the pool's durable rows in
    // `(receipt_date, id)` order and stage the consumed qty via
    // `accum_drain` for Phase 9e UPDATE.
    //
    // Discipline: NO FifoCellGuards are held here (durable_only pools
    // have no cell). Within a pool, envelopes apply in `env_idx`
    // order. Across pools, ordering is irrelevant because pools
    // don't share durable rows. The walk uses the existing
    // half-α-architecture pattern (Phase 8.5-shaped SPI call).
    //
    // Limitation (matches Phase 8.5): same-batch receipts whose
    // `cost_layers` row is inserted at Phase 9c are NOT visible to
    // a subsequent same-batch issue's SPI walk here (it executes
    // before Phase 9c). Workloads that need same-batch RYW on
    // durable_only pools must split into separate batches. Same
    // constraint applies to same-batch multi-issue: the second
    // issue's SPI walk sees the same `qty_remaining` snapshot as
    // the first (accum_drain not flushed until Phase 9e), and may
    // double-consume layers the first already drained. Filed as
    // acct-cpe8-followup if a workload driver surfaces.
    if !durable_only_envs.is_empty() {
        for &i in &durable_only_envs {
            let p = &parsed[i];
            match p.kind {
                Kind::FifoReceipt => {
                    let uc = p.unit_cost.expect("validated");
                    let lid = env_layer_id[i].expect("pre-allocated");
                    let amt = p.qty.saturating_mul(uc);
                    env_total_cost[i] = Some(amt);
                    pl_debit.push(p.debit);
                    pl_credit.push(p.credit);
                    pl_amount.push(amt);
                    pl_currency.push(String::new());
                    pl_idemp.push(p.idempotency_key.clone());
                    pl_bdate.push(p.business_date.clone());
                    pl_qty.push(p.qty);
                    pl_env_idx.push(i);
                    nl_id.push(lid);
                    nl_pool.push(p.debit);
                    nl_qty.push(p.qty);
                    nl_uc.push(uc);
                    nl_bdate.push(p.business_date.clone());
                    nl_idemp.push(p.idempotency_key.clone());
                    let e = bal_deltas.entry(p.debit).or_insert((0, 0));
                    e.0 += amt;
                    e.1 += p.qty;
                    let e = bal_deltas.entry(p.credit).or_insert((0, 0));
                    e.0 -= amt;
                }
                Kind::FifoIssue => {
                    // SPI walk: all durable rows for the pool with
                    // residual qty, in FIFO order, FOR UPDATE.
                    let candidates: Vec<(i64, i64, i64)> = Spi::connect_mut(|client| {
                        let args: Vec<pgrx::datum::DatumWithOid> = vec![p.credit.into()];
                        let tup = client
                            .update(
                                "SELECT id, qty_remaining, unit_cost FROM cost_layers \
                                 WHERE pool_account_id = $1::bigint \
                                   AND qty_remaining > 0 \
                                 ORDER BY receipt_date, id \
                                 FOR UPDATE",
                                None,
                                &args,
                            )
                            .expect("maximal: durable_only SELECT cost_layers FOR UPDATE");
                        let mut v: Vec<(i64, i64, i64)> = Vec::new();
                        for row in tup {
                            let lid: i64 = row["id"].value().unwrap().unwrap();
                            let qr: i64 = row["qty_remaining"].value().unwrap().unwrap();
                            let uc: i64 = row["unit_cost"].value().unwrap().unwrap();
                            v.push((lid, qr, uc));
                        }
                        v
                    });

                    let mut remaining = p.qty;
                    let mut total_cost: i64 = 0;
                    let mut consumed: Vec<(i64, i64, i64)> = Vec::new();
                    for (lid, qty_rem, uc) in candidates {
                        if remaining == 0 {
                            break;
                        }
                        let take = remaining.min(qty_rem);
                        total_cost = total_cost.saturating_add(take.saturating_mul(uc));
                        consumed.push((lid, take, uc));
                        remaining -= take;
                    }

                    if remaining > 0 {
                        pgrx::error!(
                            "fifo_apply_batch_maximal: envelope[{}] fifo_issue short by {} units \
                             (pool {} durable-only path exhausted; arena bucket exhaustion)",
                            p.envelope_idx,
                            remaining,
                            p.credit
                        );
                    }

                    // Stage durable UPDATE via accum_drain (Phase 9e).
                    // durable_only layers never sat in the ring; this is
                    // the only path to decrement their qty_remaining.
                    for (lid, qty, uc) in &consumed {
                        dp_layer_id.push(*lid);
                        dp_issue_idemp.push(p.idempotency_key.clone());
                        dp_qty.push(*qty);
                        dp_cost.push(qty.saturating_mul(*uc));
                        *accum_drain.entry(*lid).or_insert(0) += qty;
                    }

                    env_total_cost[i] = Some(total_cost);
                    pl_debit.push(p.debit);
                    pl_credit.push(p.credit);
                    pl_amount.push(total_cost);
                    pl_currency.push(String::new());
                    pl_idemp.push(p.idempotency_key.clone());
                    pl_bdate.push(p.business_date.clone());
                    pl_qty.push(p.qty);
                    pl_env_idx.push(i);
                    let e = bal_deltas.entry(p.debit).or_insert((0, 0));
                    e.0 += total_cost;
                    let e = bal_deltas.entry(p.credit).or_insert((0, 0));
                    e.0 -= total_cost;
                    e.1 -= p.qty;
                }
            }
        }
    }

    // Phase 9a — resolve currencies for all touched accounts.
    let mut all_acct_ids: Vec<i64> = parsed
        .iter()
        .flat_map(|p| [p.debit, p.credit])
        .collect();
    all_acct_ids.sort_unstable();
    all_acct_ids.dedup();
    let currency_map: HashMap<i64, String> = Spi::connect(|client| {
        let args: Vec<pgrx::datum::DatumWithOid> = vec![all_acct_ids.clone().into()];
        let tup = client
            .select(
                "SELECT id, currency::TEXT AS c FROM accounts WHERE id = ANY($1::bigint[])",
                None,
                &args,
            )
            .expect("maximal: SELECT currency");
        let mut m: HashMap<i64, String> = HashMap::new();
        for row in tup {
            let id: i64 = row["id"].value().unwrap().unwrap();
            let c: String = row["c"].value().unwrap().unwrap();
            m.insert(id, c);
        }
        m
    });
    // Fill pl_currency now that the map is resolved.
    for (i_pl, env_idx) in pl_env_idx.iter().enumerate() {
        let p = &parsed[*env_idx];
        let c = currency_map.get(&p.debit).cloned().unwrap_or_else(|| {
            pgrx::error!(
                "maximal: envelope[{}] debit_account_id={} has no currency",
                p.envelope_idx,
                p.debit
            )
        });
        pl_currency[i_pl] = c;
    }

    // Phase 9b — INSERT posting_lines.
    let new_pl_map: HashMap<String, i64> = if pl_idemp.is_empty() {
        HashMap::new()
    } else {
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![
                pl_debit.clone().into(),
                pl_credit.clone().into(),
                pl_amount.clone().into(),
                pl_currency.clone().into(),
                pl_idemp.clone().into(),
                pl_bdate.clone().into(),
                pl_qty.clone().into(),
            ];
            let tup = client
                .update(
                    "INSERT INTO posting_lines \
                       (debit_account_id, credit_account_id, amount, currency, \
                        idempotency_key, business_date, qty) \
                     SELECT u.debit, u.credit, u.amount, u.curr, u.idemp::uuid, u.bdate::date, u.qty \
                     FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], \
                                 $4::text[], $5::text[], $6::text[], $7::bigint[]) \
                              AS u(debit, credit, amount, curr, idemp, bdate, qty) \
                     RETURNING id, idempotency_key::TEXT AS k",
                    None,
                    &args,
                )
                .expect("maximal: INSERT posting_lines");
            let mut m: HashMap<String, i64> = HashMap::new();
            for row in tup {
                let id: i64 = row["id"].value().unwrap().unwrap();
                let k: String = row["k"].value().unwrap().unwrap();
                m.insert(k, id);
            }
            m
        })
    };

    // Phase 9c — INSERT new cost_layers (receipts) with explicit ids.
    if !nl_id.is_empty() {
        let nl_pl_ids: Vec<i64> = nl_idemp
            .iter()
            .map(|k| {
                *new_pl_map.get(k).unwrap_or_else(|| {
                    pgrx::error!(
                        "maximal: receipt idempotency_key '{}' missing from posting_lines INSERT",
                        k
                    )
                })
            })
            .collect();
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![
                nl_id.clone().into(),
                nl_pool.clone().into(),
                nl_qty.clone().into(),
                nl_uc.clone().into(),
                nl_bdate.clone().into(),
                nl_pl_ids.clone().into(),
            ];
            client
                .update(
                    "INSERT INTO cost_layers \
                       (id, pool_account_id, qty_remaining, unit_cost, receipt_date, \
                        receipt_posting_line_id) \
                     SELECT u.lid, u.pool, u.qty, u.uc, u.bdate::date, u.pl \
                     FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::text[], $6::bigint[]) \
                              AS u(lid, pool, qty, uc, bdate, pl)",
                    None,
                    &args,
                )
                .expect("maximal: INSERT cost_layers");
        });
    }

    // Phase 9d — INSERT cost_layer_depletions. `dp_cost` was computed
    // per-slice during the apply phase from `slice.unit_cost` captured
    // out of the ring; no post-hoc SPI lookup needed.
    if !dp_layer_id.is_empty() {
        let dp_issue_pl_ids: Vec<i64> = dp_issue_idemp
            .iter()
            .map(|k| {
                *new_pl_map.get(k).unwrap_or_else(|| {
                    pgrx::error!(
                        "maximal: issue idempotency_key '{}' missing from posting_lines INSERT",
                        k
                    )
                })
            })
            .collect();
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> = vec![
                dp_layer_id.clone().into(),
                dp_issue_pl_ids.clone().into(),
                dp_qty.clone().into(),
                dp_cost.clone().into(),
            ];
            client
                .update(
                    "INSERT INTO cost_layer_depletions \
                       (layer_id, issue_posting_line_id, qty_consumed, cost_amount) \
                     SELECT u.lid, u.pl, u.qty, u.cost \
                     FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) \
                              AS u(lid, pl, qty, cost)",
                    None,
                    &args,
                )
                .expect("maximal: INSERT cost_layer_depletions");
        });
    }

    // Phase 9e — INLINE FALLBACK for pending_drain overflow.
    // sub 4 (acct-0450): the steady-state path stages drains into the
    // per-cell pending_drain ring; the `ledger_drain` bgworker flushes
    // those to `cost_layers.qty_remaining` at `drain_interval_ms`
    // cadence. `accum_drain` here contains only the entries that
    // OVERFLOWED `PENDING_DRAIN_CAP` between drain ticks — the
    // safety net so a rare burst doesn't lose audit-state coherence.
    // Empty `accum_drain` is the common case post-sub-4.
    if !accum_drain.is_empty() {
        let ld_id: Vec<i64> = accum_drain.keys().copied().collect();
        let ld_drain: Vec<i64> = ld_id.iter().map(|id| accum_drain[id]).collect();
        Spi::connect_mut(|client| {
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![ld_id.clone().into(), ld_drain.clone().into()];
            client
                .update(
                    "UPDATE cost_layers SET qty_remaining = qty_remaining - u.d \
                     FROM unnest($1::bigint[], $2::bigint[]) AS u(id, d) \
                     WHERE cost_layers.id = u.id",
                    None,
                    &args,
                )
                .expect("maximal: UPDATE cost_layers");
        });
    }

    // Phase 10 — stage_apply balance/qty deltas (existing shmem path).
    for (account_id, (dbal, dqty)) in bal_deltas.iter() {
        stage_apply(*account_id, 1, 1, 1, *dbal, *dqty);
    }

    // Phase 11 — build per-envelope results.
    let results: Vec<(i32, String, Option<i64>, Option<String>, Option<String>)> = parsed
        .iter()
        .map(|p| {
            let pl_id = replay_map
                .get(&p.idempotency_key)
                .copied()
                .or_else(|| new_pl_map.get(&p.idempotency_key).copied());
            let status = if replay_map.contains_key(&p.idempotency_key) {
                "idempotent_replay".to_string()
            } else {
                "committed".to_string()
            };
            (p.envelope_idx, status, pl_id, None, None)
        })
        .collect();

    TableIterator::new(results)
}

// ── sub 4 (acct-0450) — FIFO bgworker drain ──────────────────────────

/// One FIFO drain pass. Called from `ledger_drain_main` after the WAC
/// drain. Half-α architecture: durable `cost_layers` INSERTs +
/// `cost_layer_depletions` INSERTs stay inline on the apply path; only
/// `UPDATE cost_layers SET qty_remaining` moves here.
///
/// Flow:
///
/// 1. Walk all FIFO buckets under `FIFO_ARENA.share()`; gather the
///    cell indices that are dirty (`last_seq > drained_seq`). Read
///    `last_seq` lock-free; do NOT touch `pending_drain` here (that
///    requires the cell LWLock).
/// 2. For each dirty cell: acquire the cell LWLock EXCLUSIVE briefly,
///    snapshot `pending_drain` into a local vec, reset
///    `pending_drain_n` to 0, capture the post-lock `last_seq`,
///    release. The brief EXCLUSIVE hold is the only synchronization
///    with apply writers.
/// 3. Aggregate all snapshots: `HashMap<layer_id, sum_qty_consumed>`.
/// 4. Bulk SPI `UPDATE cost_layers SET qty_remaining = qty_remaining -
///    sum_qty_consumed` via UNNEST. Wrapped in `PgTryBuilder` so a
///    missing table / schema-error logs+counts but doesn't crash the
///    tick.
/// 5. CAS-stamp each successfully-drained cell's `drained_seq` up to
///    its captured `last_seq`.
///
/// Cells with `pending_drain_n == 0` at snapshot are still stamped:
/// `last_seq > drained_seq` can be true because of receipts (which
/// don't pend any drain) — we still want to clear the dirty bit so
/// the cell isn't re-scanned every tick.
pub(crate) fn do_fifo_drain_tick() {
    // Phase 1 — lock-free dirty scan.
    let mut dirty_cells: Vec<usize> = Vec::new();
    {
        let arena = FIFO_ARENA.share();
        for i in 0..FIFO_N_BUCKETS {
            let b = &arena.buckets[i];
            if b.occupied.load(Ordering::Acquire) == 0 {
                continue;
            }
            let last_pre = b.last_seq.load(Ordering::Acquire);
            let drained = b.drained_seq.load(Ordering::Acquire);
            if last_pre <= drained {
                continue;
            }
            dirty_cells.push(i);
        }
    }
    if dirty_cells.is_empty() {
        return;
    }

    FIFO_DRAIN_TICKS_TOTAL
        .get()
        .fetch_add(1, Ordering::AcqRel);

    // Phase 2 — per-cell brief EXCLUSIVE: snapshot + reset.
    // Carries (cell_idx, captured_last_seq, drained_entries) per cell.
    let mut snapshots: Vec<(usize, u64, Vec<PendingDrain>)> =
        Vec::with_capacity(dirty_cells.len());
    {
        let arena = FIFO_ARENA.share();
        for &cell_idx in &dirty_cells {
            let _guard = acquire_fifo_cell(cell_idx, FifoCellMode::Exclusive);
            let b = &arena.buckets[cell_idx];
            // SAFETY: cell LWLock held EXCLUSIVE; sole writer of ring.
            let ring = unsafe { ring_mut(b) };
            let mut entries: Vec<PendingDrain> = Vec::new();
            drain_pending_into(ring, &mut entries);
            let last_seq = b.last_seq.load(Ordering::Acquire);
            snapshots.push((cell_idx, last_seq, entries));
            // guard releases here.
        }
    }

    // Phase 3 — aggregate per layer_id across all cells (same-layer
    // multiple-cell deltas don't happen in the PoC schema, but the
    // SUM is cheap insurance).
    let mut per_layer: HashMap<i64, i64> = HashMap::new();
    for (_cell, _seq, entries) in &snapshots {
        for e in entries {
            *per_layer.entry(e.layer_id).or_insert(0) += e.qty_consumed;
        }
    }

    // Phase 4 — bulk UPDATE cost_layers via UNNEST.
    let mut failed_this_tick: u64 = 0;
    let mut last_err: Option<String> = None;
    let rows_updated = per_layer.len() as u64;
    if !per_layer.is_empty() {
        let ld_id: Vec<i64> = per_layer.keys().copied().collect();
        let ld_drain: Vec<i64> = ld_id.iter().map(|id| per_layer[id]).collect();
        let outcome: Result<(), String> = pgrx::PgTryBuilder::new(|| {
            let args: Vec<pgrx::datum::DatumWithOid> =
                vec![ld_id.clone().into(), ld_drain.clone().into()];
            // Consume the SpiTupleTable inside the connect_mut closure
            // so its lifetime stays bounded by the client borrow.
            Spi::connect_mut(|client| {
                client
                    .update(
                        "UPDATE cost_layers SET qty_remaining = qty_remaining - u.d \
                         FROM unnest($1::bigint[], $2::bigint[]) AS u(id, d) \
                         WHERE cost_layers.id = u.id",
                        None,
                        &args,
                    )
                    .map(|_| ())
                    .map_err(|e| format!("{e}"))
            })
        })
        .catch_others(|caught| Err(format!("{caught:?}")))
        .execute();

        match outcome {
            Ok(()) => {
                FIFO_DRAIN_ROWS_TOTAL
                    .get()
                    .fetch_add(rows_updated, Ordering::AcqRel);
            }
            Err(e) => {
                failed_this_tick = ld_id.len() as u64;
                last_err = Some(e.clone());
                log!(
                    "fifo_drain: UPDATE cost_layers failed for {} layers: {e}",
                    ld_id.len()
                );
            }
        }
    }

    // Phase 5 — stamp drained_seq on every dirty cell whose UPDATE
    // succeeded. If the UPDATE failed, leave drained_seq alone so the
    // next tick re-snapshots. Cells with empty pending_drain at
    // snapshot still get stamped — they were dirty for non-drain
    // reasons (e.g., pure-receipt last_seq bump) and we don't want to
    // re-scan them every tick.
    if failed_this_tick == 0 {
        let arena = FIFO_ARENA.share();
        for (cell_idx, last_seq, _entries) in &snapshots {
            let b = &arena.buckets[*cell_idx];
            let mut cur = b.drained_seq.load(Ordering::Acquire);
            while cur < *last_seq {
                match b.drained_seq.compare_exchange(
                    cur,
                    *last_seq,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
        }
    } else {
        // Re-stage the snapshots we already drained (entries are gone
        // from the ring — push them BACK into pending_drain so next
        // tick retries). This is the only path where the bgworker
        // mutates pending_drain without snapshotting it first; gated
        // on the UPDATE having failed so the ring isn't double-counted.
        let arena = FIFO_ARENA.share();
        for (cell_idx, _seq, entries) in &snapshots {
            if entries.is_empty() {
                continue;
            }
            let _guard = acquire_fifo_cell(*cell_idx, FifoCellMode::Exclusive);
            let b = &arena.buckets[*cell_idx];
            let ring = unsafe { ring_mut(b) };
            for e in entries {
                // Best-effort: if the ring overfilled during this
                // tick (apply path pushed more entries after our
                // snapshot), drop the overflow — caller's inline
                // fallback already handled overflow on the apply
                // side, and the durable UPDATE will catch up next
                // tick when the apply path overflows again.
                let _ = push_pending_drain(ring, e.layer_id, e.qty_consumed);
            }
        }
    }

    // Phase 6 — escalation counters.
    if failed_this_tick > 0 {
        FIFO_DRAIN_TOTAL_FAILURES
            .get()
            .fetch_add(failed_this_tick, Ordering::AcqRel);
        let prev = FIFO_DRAIN_CONSECUTIVE_FAILS
            .get()
            .fetch_add(1, Ordering::AcqRel);
        let now = prev + 1;
        if now == FIFO_DRAIN_WARN_AFTER {
            pgrx::warning!(
                "fifo_drain: {now} consecutive ticks with SPI errors \
                 (this tick: {failed_this_tick} layer rows failed of {} total); \
                 last error: {}. Check that `cost_layers` exists in the \
                 target DB and the bgworker has access.",
                rows_updated,
                last_err.as_deref().unwrap_or("<none>")
            );
        }
    } else {
        FIFO_DRAIN_CONSECUTIVE_FAILS
            .get()
            .store(0, Ordering::Release);
    }
}

// ── SQL surface for sub 4 testing ────────────────────────────────────

/// Force one FIFO drain pass. Useful for tests that want to flush
/// `pending_drain` rings to `cost_layers.qty_remaining` deterministically
/// rather than waiting `drain_interval_ms`. NOT for production use.
#[pg_extern]
pub fn fifo_force_drain_tick() -> bool {
    do_fifo_drain_tick();
    true
}

/// Returns the cell's pending_drain count (under SHARED-only read of
/// FIFO_ARENA; the field is read without acquiring the cell lock
/// because pending_drain_n is u16 atomic on the wire — under TSO and
/// AcqRel mutators on the writer side the read is racy but
/// monotonic-bounded by `[0, PENDING_DRAIN_CAP]`. Tests can rely on
/// this for "approximately N pending after Phase 8" assertions.).
///
/// Returns -1 if cell idx is out of range. Implementation note: we
/// take the cell lock SHARED briefly to get a consistent read of
/// `pending_drain_n`, then release. This isn't the production
/// observability shape (which would expose an atomic) but it's fine
/// for the test surface.
#[pg_extern]
pub fn fifo_pending_drain_n(idx: i64) -> i64 {
    if (idx as usize) >= FIFO_N_BUCKETS {
        return -1;
    }
    let _guard = acquire_fifo_cell(idx as usize, FifoCellMode::Shared);
    let arena = FIFO_ARENA.share();
    let b = &arena.buckets[idx as usize];
    // SAFETY: SHARED lock held; readers of pending_drain_n must not
    // race a writer. Apply path takes EXCLUSIVE for writes; SHARED is
    // sufficient for read.
    let ring = unsafe { ring_ref(b) };
    ring.pending_drain_n as i64
}

/// Returns the total number of rows the FIFO drain has UPDATEd since
/// last reset. Counter is process-lifetime; reset only via
/// `fifo_arena_reset` (sub-6 territory).
#[pg_extern]
pub fn fifo_drain_rows_total() -> i64 {
    FIFO_DRAIN_ROWS_TOTAL
        .get()
        .load(Ordering::Acquire) as i64
}

/// Returns the total number of FIFO drain ticks that touched at least
/// one dirty cell. Lock-free reader.
#[pg_extern]
pub fn fifo_drain_ticks_total() -> i64 {
    FIFO_DRAIN_TICKS_TOTAL
        .get()
        .load(Ordering::Acquire) as i64
}

/// Returns the number of consecutive ticks the FIFO drain SPI UPDATE
/// has failed. Zero in steady state; non-zero signals durable-write
/// trouble (missing table / RLS / etc.).
#[pg_extern]
pub fn fifo_drain_consecutive_failures() -> i64 {
    FIFO_DRAIN_CONSECUTIVE_FAILS
        .get()
        .load(Ordering::Acquire) as i64
}

/// Reset every FIFO arena cell back to empty. Intended for **bench
/// setup or test teardown only** — there is no synchronization with
/// concurrent apply paths. The caller guarantees no
/// `fifo_apply_batch_maximal` is in flight (single-threaded bench
/// setup phase; quiescent test).
///
/// Acquires `FIFO_ARENA.exclusive()` to serialize against insertions
/// and probes that take `share()`, but DOES NOT take cell LWLocks —
/// quiescence is the load-bearing precondition. Wipes occupied,
/// seeded, overflow_active, overflow_first_spilled_layer_id, key,
/// last_seq, drained_seq, and the ring state
/// (head/n_layers/pending_drain_n). Layer/pending_drain arrays are
/// not zeroed; n=0 makes them unreachable.
#[pg_extern]
pub fn fifo_arena_reset() -> bool {
    let arena = FIFO_ARENA.exclusive();
    for i in 0..FIFO_N_BUCKETS {
        let b = &arena.buckets[i];
        b.occupied.store(0, Ordering::Relaxed);
        b.seeded.store(0, Ordering::Relaxed);
        b.overflow_active.store(0, Ordering::Relaxed);
        b.key_hi.store(0, Ordering::Relaxed);
        b.key_lo.store(0, Ordering::Relaxed);
        b.last_seq.store(0, Ordering::Relaxed);
        b.drained_seq.store(0, Ordering::Relaxed);
        b.overflow_first_spilled_layer_id
            .store(0, Ordering::Relaxed);
        // SAFETY: caller-guaranteed quiescence (no concurrent apply).
        let ring = unsafe { ring_mut(b) };
        ring.head = 0;
        ring.n_layers = 0;
        ring.pending_drain_n = 0;
    }
    true
}

// ── sub 8 (acct-cpe8) — test helpers for arena bucket exhaustion ────

/// Test-only: mark every currently-unoccupied FIFO bucket as occupied
/// by a sentinel key (`key_lo = 1`, `key_hi = idx`), forcing any
/// subsequent real pool insert via `insert_fifo_cell` to return None
/// after a full probe walk. Real PoC pool keys always have
/// `key_lo = 0` (`pack_fifo_key_poc` packs the pool_account_id into
/// the upper 64 bits), so:
///
/// - The sentinel keys cannot collide with any real pool by key match
///   (the equality check in `insert_fifo_cell` fails on `key_lo`).
/// - `fifo_arena_recon` already skips non-PoC-shape keys
///   (`if key_lo != 0 { continue; }`), so sentinels are invisible to
///   that recon's output.
///
/// Each sentinel slot gets a unique `key_hi` (the bucket index) so the
/// probe loop doesn't short-circuit on a same-key match before
/// exhausting the table.
///
/// Returns the number of buckets newly marked. Pair with
/// `fifo_release_sentinels` for cleanup so the helper doesn't leak
/// across test binaries when the arena is shared.
#[pg_extern]
pub fn fifo_force_arena_full() -> i64 {
    let arena = FIFO_ARENA.exclusive();
    let mut marked: i64 = 0;
    for i in 0..FIFO_N_BUCKETS {
        let b = &arena.buckets[i];
        if b.occupied.load(Ordering::Relaxed) != 0 {
            continue;
        }
        // Sentinel: distinct key_hi per bucket avoids `insert_fifo_cell`
        // probe-loop key-match short-circuit; key_lo = 1 marks the row
        // as test-only (real PoC keys always have key_lo = 0).
        b.key_hi.store(i as u64, Ordering::Relaxed);
        b.key_lo.store(1, Ordering::Relaxed);
        // seeded=1 so the apply path never tries to lazy-seed this
        // sentinel (would SPI-fetch garbage). Doesn't matter for
        // correctness — apply skips it because env_cell never points
        // here — but it's defense in depth if a future code path
        // iterates all occupied cells.
        b.seeded.store(1, Ordering::Release);
        b.occupied.store(1, Ordering::Release);
        marked += 1;
    }
    marked
}

/// Test-only: clear `occupied` (and the sentinel key bits) on every
/// bucket whose `key_lo == 1` — the marker set by
/// `fifo_force_arena_full`. Returns the number of buckets cleared.
/// Real PoC pool cells (key_lo == 0) are left untouched, so this is
/// safe to call in tests that share the arena with concurrent
/// non-cpe8 tests.
#[pg_extern]
pub fn fifo_release_sentinels() -> i64 {
    let arena = FIFO_ARENA.exclusive();
    let mut cleared: i64 = 0;
    for i in 0..FIFO_N_BUCKETS {
        let b = &arena.buckets[i];
        if b.occupied.load(Ordering::Relaxed) == 0 {
            continue;
        }
        if b.key_lo.load(Ordering::Relaxed) != 1 {
            continue;
        }
        b.key_hi.store(0, Ordering::Relaxed);
        b.key_lo.store(0, Ordering::Relaxed);
        b.seeded.store(0, Ordering::Relaxed);
        b.occupied.store(0, Ordering::Release);
        cleared += 1;
    }
    cleared
}

// ── sub 6 (acct-6g2u) — FIFO arena recon ─────────────────────────────

/// Per-cell shmem-vs-durable layer recon. For each occupied FIFO arena
/// cell, returns:
///
/// - `pool_account_id`     — the pool tracked by this cell (PoC key shape).
/// - `shmem_live_qty`      — SUM(`layers[].qty`) in the ring (live layers only;
///   fully-consumed layers are evicted via head-advance in `consume_from_head`).
/// - `shmem_pending_qty`   — SUM(`pending_drain[].qty_consumed`) staged for
///   the next bgworker tick (sub 4 half-α architecture).
/// - `shmem_total_qty`     — `shmem_live_qty + shmem_pending_qty`. At drain
///   quiescence this is the canonical shmem position for the pool.
/// - `shmem_spilled_qty`   — SUM(`cost_layers.qty_remaining`) for the cell's
///   spilled tail (sub 3 / acct-b8ub): rows whose `id >= watermark` for
///   overflow_active cells; 0 for non-overflow cells. Surfaces the
///   portion of pool qty that lives durable-only because it never fit
///   in the ring.
/// - `durable_qty`         — SUM(`cost_layers.qty_remaining`) WHERE
///   `pool_account_id = cell's pool`. NULL if the pool has no rows in
///   `cost_layers` (signals: post-restart pre-lazy-seed, rollback-leaked
///   shmem cell, or test scaffolding leak).
/// - `drift`               — `durable_qty - shmem_spilled_qty - shmem_total_qty`.
///   NULL when `durable_qty` is NULL (cannot compare). For non-overflow
///   cells `shmem_spilled_qty=0` and this reduces to the original
///   `durable - shmem_total` form. For overflow cells the spilled tail
///   is excluded from the equation because it isn't shmem-managed —
///   the half-α invariant only constrains the shmem-owned portion to
///   match its durable shadow.
///
/// ## Quiescence semantics
///
/// At drain quiescence (no in-flight `fifo_apply_batch_maximal`, bgworker
/// tick completed, `pending_drain_n == 0` per cell), `drift = 0` exactly
/// for every cell whose pool has durable rows.
///
/// Under load each row is internally coherent (the cell's SHARED LWLock
/// is held during the in-shmem snapshot), but `drift` may reflect
/// in-flight transient states. The `shmem_pending_qty` term in the
/// equation eliminates the bgworker-flush lag specifically; it does not
/// eliminate the apply-in-progress lag between Phase 9 of
/// `fifo_apply_batch_maximal` (durable INSERT/UPDATE) and the eventual
/// txn commit. Best practice: call at quiescence for an exact verdict;
/// under load, treat `|drift| > 0` as advisory unless it persists across
/// successive snapshots after `fifo_force_drain_tick()`.
///
/// ## What this recon does NOT detect
///
/// - **Orphan durable pools.** Pools with rows in `cost_layers` but no
///   shmem cell (post-restart pre-lazy-seed state) are silently skipped —
///   the iteration is shmem-side. The lazy-seed path repopulates the cell
///   on first apply; until then, the pool is invisible to this recon. A
///   future shape could optionally include a `WHERE pool_account_id NOT
///   IN (occupied cell keys)` subquery for fuller coverage.
/// - **Per-layer drift.** This rolls up to the pool level. Per-layer
///   `qty_remaining` skew is detectable by joining `cost_layers` to the
///   shmem layer array on `layer_id`, but the PoC's recon contract
///   (per-cell SUM equality) is sufficient for the half-α correctness
///   guarantee. File as a follow-up shape if a workload driver surfaces.
///
/// ## Lock discipline
///
/// Takes `FIFO_ARENA.share()` for the outer scan to prevent concurrent
/// insertions from advancing occupied flags mid-iteration, then
/// `acquire_fifo_cell(idx, Shared)` per-cell to coexist with apply
/// writers (which take EXCLUSIVE). Cell-locks are acquired in ascending
/// index order so the recon never deadlocks against apply, which takes
/// cell-locks in batch-sorted (also ascending) order.
#[pg_extern]
pub fn fifo_arena_recon() -> TableIterator<
    'static,
    (
        name!(pool_account_id, i64),
        name!(shmem_live_qty, i64),
        name!(shmem_pending_qty, i64),
        name!(shmem_total_qty, i64),
        name!(shmem_spilled_qty, i64),
        name!(durable_qty, Option<i64>),
        name!(drift, Option<i64>),
    ),
> {
    // Phase 1 — snapshot occupied cells. Each cell snapshot is coherent
    // because the SHARED LWLock blocks the EXCLUSIVE-holding writers
    // (push_layer, consume_from_head, drain_pending_into).
    //
    // sub 3 (acct-b8ub): also capture `overflow_active` + `watermark`
    // so the SPI side can split durable rows into shmem-owned (id <
    // watermark) and spilled-tail (id >= watermark) for overflow cells.
    let mut cells: Vec<(
        i64, /* pool */
        i64, /* live */
        i64, /* pending */
        i64, /* watermark (0 = no overflow) */
        i32, /* overflow_active 0/1 */
    )> = Vec::new();
    {
        let arena = FIFO_ARENA.share();
        for i in 0..FIFO_N_BUCKETS {
            let b = &arena.buckets[i];
            if b.occupied.load(Ordering::Acquire) == 0 {
                continue;
            }
            let key_hi = b.key_hi.load(Ordering::Acquire);
            let key_lo = b.key_lo.load(Ordering::Acquire);
            // PoC key shape: `pack_fifo_key_poc(pool_account_id)` puts
            // pool in upper 64 bits, lower is 0. Skip non-PoC-shape
            // keys defensively so a future multi-dimensional key
            // doesn't silently misreport pool_account_id as stale bits.
            if key_lo != 0 {
                continue;
            }
            let pool_account_id = key_hi as i64;

            let _guard = acquire_fifo_cell(i, FifoCellMode::Shared);
            // SAFETY: cell LWLock held SHARED; ring mutators all take
            // EXCLUSIVE, so the read cannot observe torn layers /
            // pending_drain state.
            let ring = unsafe { ring_ref(b) };

            let mut live: i64 = 0;
            for offset in 0..(ring.n_layers as usize) {
                let lidx = (ring.head as usize + offset) % MAX_LAYERS;
                live = live.saturating_add(ring.layers[lidx].qty);
            }
            let mut pending: i64 = 0;
            for k in 0..(ring.pending_drain_n as usize) {
                pending = pending.saturating_add(ring.pending_drain[k].qty_consumed);
            }
            let overflow_active = b.overflow_active.load(Ordering::Acquire) as i32;
            let watermark = b
                .overflow_first_spilled_layer_id
                .load(Ordering::Acquire);
            cells.push((pool_account_id, live, pending, watermark, overflow_active));
            // guard releases here; next iteration acquires the next cell.
        }
    }

    if cells.is_empty() {
        return TableIterator::new(Vec::new());
    }

    // Phase 2 — bulk SPI: per-pool SUM(qty_remaining) joined to the
    // cells snapshot. One round-trip regardless of cell count (mirrors
    // ledger_shmem_recon's acct-dav7 #7 refactor).
    //
    // `pool_exists` separates "pool absent from cost_layers" (NULL
    // durable_qty + NULL drift) from "pool present but every layer
    // qty_remaining=0" (durable_qty=0 + drift = -shmem_total). The
    // first signals a structural state (post-restart, rollback leak);
    // the second is normal (drained pool).
    //
    // sub 3: `spilled_per_pool` sums `qty_remaining` only over rows
    // with `id >= watermark` for cells where `overflow_active = 1` and
    // `watermark > 0`. For non-overflow cells the join produces 0
    // (LEFT JOIN, COALESCE). Drift then subtracts the spilled portion
    // so the half-α invariant (shmem-owned == ring) is the assertion
    // being checked.
    let pool_ids: Vec<i64> = cells.iter().map(|(p, _, _, _, _)| *p).collect();
    let live_qtys: Vec<i64> = cells.iter().map(|(_, l, _, _, _)| *l).collect();
    let pend_qtys: Vec<i64> = cells.iter().map(|(_, _, p, _, _)| *p).collect();
    let watermarks: Vec<i64> = cells.iter().map(|(_, _, _, w, _)| *w).collect();
    let overflows: Vec<i32> = cells.iter().map(|(_, _, _, _, o)| *o).collect();

    let sql = "
        WITH cells_in AS (
            SELECT pool_account_id, shmem_live, shmem_pending,
                   watermark, overflow_active
              FROM unnest($1::bigint[], $2::bigint[], $3::bigint[],
                          $4::bigint[], $5::int[])
                AS u(pool_account_id, shmem_live, shmem_pending,
                     watermark, overflow_active)
        ),
        pool_exists AS (
            SELECT DISTINCT pool_account_id
              FROM cost_layers
             WHERE pool_account_id = ANY($1::bigint[])
        ),
        durable AS (
            SELECT pool_account_id, SUM(qty_remaining)::bigint AS q
              FROM cost_layers
             WHERE pool_account_id = ANY($1::bigint[])
             GROUP BY pool_account_id
        ),
        spilled AS (
            SELECT cl.pool_account_id, SUM(cl.qty_remaining)::bigint AS q
              FROM cost_layers cl
              JOIN cells_in c ON c.pool_account_id = cl.pool_account_id
             WHERE cl.pool_account_id = ANY($1::bigint[])
               AND c.overflow_active = 1
               AND c.watermark > 0
               AND cl.id >= c.watermark
             GROUP BY cl.pool_account_id
        )
        SELECT c.pool_account_id,
               c.shmem_live,
               c.shmem_pending,
               (c.shmem_live + c.shmem_pending)::bigint AS shmem_total,
               COALESCE(s.q, 0)::bigint AS shmem_spilled_qty,
               CASE WHEN p.pool_account_id IS NOT NULL
                    THEN COALESCE(d.q, 0)::bigint
                    ELSE NULL
               END AS durable_qty,
               CASE WHEN p.pool_account_id IS NOT NULL
                    THEN (COALESCE(d.q, 0)
                          - COALESCE(s.q, 0)
                          - (c.shmem_live + c.shmem_pending))::bigint
                    ELSE NULL
               END AS drift
          FROM cells_in c
          LEFT JOIN pool_exists p ON p.pool_account_id = c.pool_account_id
          LEFT JOIN durable d     ON d.pool_account_id = c.pool_account_id
          LEFT JOIN spilled s     ON s.pool_account_id = c.pool_account_id
         ORDER BY c.pool_account_id
    ";

    let out: Vec<(i64, i64, i64, i64, i64, Option<i64>, Option<i64>)> =
        pgrx::PgTryBuilder::new(|| {
            Spi::connect(|client| {
                let args: Vec<pgrx::datum::DatumWithOid> = vec![
                    pool_ids.clone().into(),
                    live_qtys.clone().into(),
                    pend_qtys.clone().into(),
                    watermarks.clone().into(),
                    overflows.clone().into(),
                ];
                let tup = client
                    .select(sql, None, &args)
                    .expect("fifo_arena_recon SPI select");
                let mut rows = Vec::with_capacity(cells.len());
                for row in tup {
                    let p: i64 = row["pool_account_id"].value().unwrap().unwrap();
                    let l: i64 = row["shmem_live"].value().unwrap().unwrap();
                    let pd: i64 = row["shmem_pending"].value().unwrap().unwrap();
                    let t: i64 = row["shmem_total"].value().unwrap().unwrap();
                    let sp: i64 = row["shmem_spilled_qty"].value().unwrap().unwrap();
                    let d: Option<i64> = row["durable_qty"].value().unwrap();
                    let dr: Option<i64> = row["drift"].value().unwrap();
                    rows.push((p, l, pd, t, sp, d, dr));
                }
                rows
            })
        })
        .catch_others(|_| {
            // SPI error path: surface one row per cell with shmem-side
            // values populated and durable_qty / drift NULL. Matches
            // ledger_shmem_recon's legacy fallback shape. shmem_spilled
            // is shmem-side (durable-derived, not snapshotted) so it
            // collapses to 0 on SPI failure — drift is NULL anyway.
            cells
                .iter()
                .map(|(p, l, pd, _, _)| {
                    (*p, *l, *pd, *l + *pd, 0i64, None::<i64>, None::<i64>)
                })
                .collect()
        })
        .execute();

    TableIterator::new(out)
}
