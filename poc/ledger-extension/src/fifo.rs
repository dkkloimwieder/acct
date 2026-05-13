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

// ── data layout ──────────────────────────────────────────────────────

/// One FIFO layer (cost layer in the ring buffer).
///
/// `unit_cost` is BIGINT to match `posting_lines.amount` / `cost_layers`
/// integer-money convention in acct's production schema. 16-byte
/// alignment so 4 layers fit in a single 64-byte cache line; the ring
/// buffer is contiguous so a walk over n_layers consecutive layers
/// scans sequential memory.
#[repr(C, align(16))]
#[derive(Copy, Clone, Default, Debug)]
pub struct Layer {
    pub qty: i64,
    pub unit_cost: i64,
}

unsafe impl PGRXSharedMemory for Layer {}

/// One FIFO arena slot. Cache-line aligned (`align(64)`) so concurrent
/// SHARED-lock probes against different buckets don't false-share.
///
/// Two atomicity domains live in this struct:
///
/// - **Atomic fields** (`occupied`, `key_hi`, `key_lo`, `last_seq`,
///   `drained_seq`): read lock-free by probe, recon, and drain paths.
///   Mirrors the WAC `Bucket` layout.
/// - **LWLock-protected fields** (`head`, `n_layers`, `layers[...]`):
///   mutated only by an apply holding this cell's per-cell LWLock
///   EXCLUSIVE (or read-snapshotted under SHARED). Sub 2's apply path
///   enforces.
///
/// Memory layout (offsets are illustrative for x86_64 / Linux):
///
/// ```text
///   0   AtomicU8  occupied            (1)
///   1   [u8; 1]   _pad0               (1)
///   2   u16       head                (2)  // cell-lock protected
///   4   u16       n_layers            (2)  // cell-lock protected
///   6   [u8; 2]   _pad1               (2)
///   8   AtomicU64 key_hi              (8)
///  16   AtomicU64 key_lo              (8)
///  24   AtomicU64 last_seq            (8)
///  32   AtomicU64 drained_seq         (8)
///  40   [u8; 24]  _pad2               (24) // pad to 64-byte header
///  64   [Layer; MAX_LAYERS] layers    (1024 at MAX_LAYERS=64)
/// 1088  (struct end, 64-byte aligned)
/// ```
///
/// Total: 1088 bytes per bucket at `MAX_LAYERS=64`. 16384 buckets =
/// 17 MB arena.
#[repr(C, align(64))]
pub struct FifoBucket {
    pub occupied: AtomicU8,
    pub _pad0: [u8; 1],
    /// Ring-buffer head index. Oldest layer lives at `layers[head]`.
    /// **Protected by this cell's LWLock.**
    pub head: u16,
    /// Ring-buffer occupancy. Tail (next push position) =
    /// `(head + n_layers) % MAX_LAYERS`. **Protected by this cell's
    /// LWLock.**
    pub n_layers: u16,
    pub _pad1: [u8; 2],
    pub key_hi: AtomicU64,
    pub key_lo: AtomicU64,
    /// Monotone seq stamped by the last apply that mutated this cell's
    /// layers. Bgworker (sub 4) compares to `drained_seq` to identify
    /// dirty cells without taking the per-cell lock.
    pub last_seq: AtomicU64,
    /// Highest `last_seq` value the bgworker has UPSERTed to durable
    /// `cost_layers`. Dirty iff `last_seq > drained_seq`.
    pub drained_seq: AtomicU64,
    pub _pad2: [u8; 24],
    pub layers: [Layer; MAX_LAYERS],
}

unsafe impl PGRXSharedMemory for FifoBucket {}

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
