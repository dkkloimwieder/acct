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
//! `consume_layers` arrive with sub 2 + sub 3 (acct-b8ub coalescing).
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
//! - On overflow at receipt time, sub 3 coalesces the two oldest layers
//!   into a weighted-average. Cost-preserving, lossy on FIFO order.
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
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, Ordering};

// ── constants ────────────────────────────────────────────────────────

/// FIFO arena bucket count. Sized so the 5000-pool fan-out PoC bench
/// lands at ~30% load factor (open addressing degrades past ~70%).
/// Future: GUC-tunable via raw `RequestAddinShmemSpace` +
/// `ShmemInitStruct` shape (pgrx's `pg_shmem_init!` requires a
/// compile-time-sized struct).
pub const FIFO_N_BUCKETS: usize = 16384;

/// Maximum layers per cell. 64 keeps each bucket at 1088 bytes
/// (16-byte Layer × 64 = 1024 bytes payload + 64-byte header). Total
/// arena = 17 MB. Coalescing on insert kicks in at this cap (sub 3).
pub const MAX_LAYERS: usize = 64;

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
/// Header layout fits in 64 bytes; the 1544-byte `RingFields` follows.
#[repr(C, align(64))]
pub struct FifoBucket {
    pub occupied: AtomicU8,
    /// Lazy-seed flag. `0` = ring empty / unseeded; `1` = ring is the
    /// authoritative layer state for this cell. Transition gated by
    /// the cell's EXCLUSIVE LWLock during sub 2's apply phase.
    pub seeded: AtomicU8,
    pub _pad0: [u8; 6],
    pub key_hi: AtomicU64,
    pub key_lo: AtomicU64,
    /// Monotone seq stamped by the last apply that mutated this cell's
    /// layers. Bgworker (sub 4) compares to `drained_seq` to identify
    /// dirty cells without taking the per-cell lock.
    pub last_seq: AtomicU64,
    /// Highest `last_seq` value the bgworker has UPSERTed to durable
    /// `cost_layers`. Dirty iff `last_seq > drained_seq`.
    pub drained_seq: AtomicU64,
    pub _pad1: [u8; 24],
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

/// Returns `MAX_LAYERS`. Useful for tests that want to know coalesce
/// threshold without hardcoding.
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
/// (MAX_LAYERS reached); sub 3 (acct-b8ub) adds coalescing.
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
    if !missing_pools.is_empty() {
        let arena = FIFO_ARENA.exclusive();
        for pool_account_id in &missing_pools {
            let key = pack_fifo_key_poc(*pool_account_id);
            let cell = insert_fifo_cell(&arena, key).unwrap_or_else(|| {
                pgrx::error!(
                    "fifo_apply_batch_maximal: FIFO arena full inserting pool_account_id={}",
                    pool_account_id
                )
            });
            pool_to_cell.insert(*pool_account_id, cell);
        }
        // Backfill env_cell for envelopes whose pool was in missing_pools.
        for (i, p) in parsed.iter().enumerate() {
            if env_cell[i].is_some() {
                continue;
            }
            if let Some(pool) = env_pool[i] {
                env_cell[i] = pool_to_cell.get(&pool).copied();
            }
            // replays: env_pool[i] is None; env_cell stays None.
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

    // Phase 6 — lazy-seed scan: cells that have at least one issue
    // envelope AND seeded==0. SPI-fetch ordered active layers per pool.
    let mut cells_needing_seed: Vec<(usize, i64)> = Vec::new(); // (cell_idx, pool_account_id)
    {
        let arena = FIFO_ARENA.share();
        let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for (i, p) in parsed.iter().enumerate() {
            if replay_map.contains_key(&p.idempotency_key) {
                continue;
            }
            if p.kind != Kind::FifoIssue {
                continue;
            }
            let cell = env_cell[i].unwrap();
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
    let mut cells_to_envelopes: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (i, p) in parsed.iter().enumerate() {
        if replay_map.contains_key(&p.idempotency_key) {
            continue;
        }
        let cell = env_cell[i].unwrap();
        cells_to_envelopes.entry(cell).or_default().push(i);
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

    // Phase 8 — sorted cell-lock acquisition + apply.
    {
        let arena = FIFO_ARENA.share();
        for (&cell_idx, env_indices) in &cells_to_envelopes {
            let _guard = acquire_fifo_cell(cell_idx, FifoCellMode::Exclusive);
            let b = bucket(&arena, cell_idx);
            // SAFETY: cell LWLock held EXCLUSIVE; sole writer of ring.
            let ring = unsafe { ring_mut(b) };

            // Lazy-seed if needed (under cell lock so transition is atomic).
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
                            pgrx::error!(
                                "fifo_apply_batch_maximal: lazy-seed overflow at cell {} \
                                 (>{} layers in durable cost_layers; coalescing is sub 3)",
                                cell_idx,
                                MAX_LAYERS
                            );
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
                        let ok = push_layer(
                            ring,
                            Layer {
                                qty: p.qty,
                                unit_cost: uc,
                                layer_id: lid,
                            },
                        );
                        if !ok {
                            pgrx::error!(
                                "fifo_apply_batch_maximal: envelope[{}] receipt overflowed cell {} \
                                 (MAX_LAYERS={}; coalescing is sub 3)",
                                p.envelope_idx,
                                cell_idx,
                                MAX_LAYERS
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
                        if res.shortage > 0 {
                            pgrx::error!(
                                "fifo_apply_batch_maximal: envelope[{}] fifo_issue short by {} units \
                                 (pool {} exhausted in shmem ring)",
                                p.envelope_idx,
                                res.shortage,
                                p.credit
                            );
                        }
                        env_total_cost[i] = Some(res.total_cost);
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

            // Stamp last_seq for drain.
            let seq = next_seq();
            b.last_seq.fetch_max(seq, Ordering::AcqRel);
            // guard drops here, releasing the cell lock.
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
/// seeded, key, last_seq, drained_seq, and the ring state
/// (head/n_layers/pending_drain_n). Layer/pending_drain arrays are
/// not zeroed; n=0 makes them unreachable.
#[pg_extern]
pub fn fifo_arena_reset() -> bool {
    let arena = FIFO_ARENA.exclusive();
    for i in 0..FIFO_N_BUCKETS {
        let b = &arena.buckets[i];
        b.occupied.store(0, Ordering::Relaxed);
        b.seeded.store(0, Ordering::Relaxed);
        b.key_hi.store(0, Ordering::Relaxed);
        b.key_lo.store(0, Ordering::Relaxed);
        b.last_seq.store(0, Ordering::Relaxed);
        b.drained_seq.store(0, Ordering::Relaxed);
        // SAFETY: caller-guaranteed quiescence (no concurrent apply).
        let ring = unsafe { ring_mut(b) };
        ring.head = 0;
        ring.n_layers = 0;
        ring.pending_drain_n = 0;
    }
    true
}
