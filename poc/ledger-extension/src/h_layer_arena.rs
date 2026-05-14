//! acct-1grr / zm69.h9 — H per-layer arena.
//!
//! Per-layer residual_qty in shmem. Companion to `h_arena` (group-level
//! invariant). Used by Path 2 of the H+ext cost-attribution evaluation:
//! FIFO walks decrement layer residuals via CAS, not FOR UPDATE on
//! durable rows.
//!
//! ## Layout
//!
//! `H_LAYER_TABLE`: hash table keyed by `layer_id` → `(residual_qty)`.
//! `H_LAYER_RESET_SEQ`: monotone counter incremented on each
//! `h_layer_reset()` call. Cells are not deleted; the reset bumps the
//! sequence so stale cells from prior generations are ignored. (Same
//! "no deletion path" discipline as the WAC arena.)
//!
//! Layer ordering is NOT maintained here. Wrappers query the durable
//! `cost_layers_h_ext` table by `(layer_group_id, ORDER BY born_at,
//! layer_id)` to determine FIFO walk order; this module only provides
//! atomic decrement of per-layer residuals.
//!
//! ## Atomic decrement semantics
//!
//! `h_layer_decrement(layer_id, requested)` returns the actual qty
//! taken — possibly less than requested if the cell was partially
//! drained or empty. Caller writes a depletion row for the actual
//! amount, advances to the next layer for any remainder. Lock-free
//! across backends; concurrent decrements serialize through CAS
//! retries on the same cell.
//!
//! ## Rollback
//!
//! Same PENDING_STACK pattern as h_arena. Per-backend
//! `H_LAYER_PRE_APPLIED` tracks each (layer_id, signed_delta) eager
//! mutation. PRE_COMMIT is a no-op (already applied). ABORT walks
//! the list and reverses.
//!
//! ## Eager apply
//!
//! Unlike h_arena's PRE_COMMIT-deferred apply, h_layer_arena applies
//! eagerly at call time. Reason: same-txn receipt-then-issue must let
//! the issue's FIFO walk see the freshly-created layer cell. Deferring
//! receipt apply to PRE_COMMIT would make this shape impossible.
//!
//! Cost of eager apply: other backends can see uncommitted state for a
//! window. The durable cost_layers_h_ext is still the source of truth
//! for layer enumeration; readers consulting only shmem would see
//! dirty state. For the bench's purposes (writing depletion rows
//! against shmem state) the dirty read is irrelevant — the writes are
//! reversed on ABORT, restoring shmem to a consistent view.

use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{PgLwLock, pg_shmem_init};
use std::cell::RefCell;
use std::ffi::c_void;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};

// acct-xeee: bumped 2^16 → 2^22 (256MiB) so 60s fan_out benches at
// path 4's measured ~80K transfers/s (~24K new layer cells per second
// at 30% receipts) sustain under 40% hash-table load.
//
// h_layer_arena has no cell reclamation — cells live until the next
// h_layer_arena_reset() — so capacity must cover the per-bench-run
// receipt count. Production usage would either: (a) reset between
// reporting windows, (b) add a tombstone-on-drained-and-quiescent
// reclamation path, or (c) partition by lot/period so reset is
// naturally scoped. None of those are needed for the PoC bench.
//
// At 4M buckets × 64 byte cache-aligned cell = 256 MiB shmem.
pub const HL_N_BUCKETS: usize = 1 << 22;

#[repr(C, align(64))]
pub struct HLBucket {
    pub occupied: AtomicU8,
    pub _pad0: [u8; 7],
    pub layer_id: AtomicI64,
    pub residual_qty: AtomicI64,
    pub _pad1: [u8; 32],
}

unsafe impl PGRXSharedMemory for HLBucket {}

#[repr(C)]
pub struct HLHashTable {
    pub buckets: [HLBucket; HL_N_BUCKETS],
}

impl Default for HLHashTable {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for HLHashTable {}

pub static H_LAYER_TABLE: PgLwLock<HLHashTable> =
    unsafe { PgLwLock::new(c"h_layer_arena_table") };

#[inline]
fn hl_slot_for(layer_id: i64) -> usize {
    let mut z = layer_id as u64;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z as usize) & (HL_N_BUCKETS - 1)
}

thread_local! {
    /// Per-backend list of (layer_id, signed_delta) eager mutations.
    /// Reversed on ABORT.
    static H_LAYER_PRE_APPLIED: RefCell<Vec<(i64, i64)>> =
        const { RefCell::new(Vec::new()) };
}

/// Create or seed a per-layer shmem cell. Used by the wrapper at
/// receipt time — caller passes the layer_id obtained from the
/// durable INSERT RETURNING. If a cell already exists for that
/// layer_id (shouldn't, since layer_id is BIGSERIAL), the qty is
/// added to its residual.
#[pg_extern]
pub fn h_layer_create(layer_id: i64, qty: i64) {
    if layer_id <= 0 {
        error!("h_layer_create: layer_id must be > 0; got {layer_id}");
    }
    if qty < 0 {
        error!("h_layer_create: qty must be >= 0; got {qty}");
    }
    let inserted = {
        let table = H_LAYER_TABLE.share();
        try_create_existing(&table, layer_id, qty)
    };
    if !inserted {
        let table = H_LAYER_TABLE.exclusive();
        if !try_create_existing(&table, layer_id, qty) {
            insert_new(&table, layer_id, qty);
        }
    }
    H_LAYER_PRE_APPLIED.with(|p| p.borrow_mut().push((layer_id, qty)));
}

/// Atomic CAS decrement on layer's residual. Takes up to `requested`
/// qty from the cell; returns the actual amount taken (`0..=requested`).
///
/// Returns 0 if the cell doesn't exist (layer was never seeded) — caller
/// should advance to the next layer.
#[pg_extern]
pub fn h_layer_decrement(layer_id: i64, requested: i64) -> i64 {
    if requested <= 0 {
        return 0;
    }
    let table = H_LAYER_TABLE.share();
    let start = hl_slot_for(layer_id);
    for probe in 0..HL_N_BUCKETS {
        let idx = (start + probe) & (HL_N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return 0;
        }
        if b.layer_id.load(Ordering::Acquire) == layer_id {
            loop {
                let cur = b.residual_qty.load(Ordering::Acquire);
                if cur <= 0 {
                    return 0;
                }
                let take = cur.min(requested);
                let new = cur - take;
                if b.residual_qty
                    .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    H_LAYER_PRE_APPLIED.with(|p| p.borrow_mut().push((layer_id, -take)));
                    return take;
                }
                // CAS retry.
            }
        }
    }
    0
}

#[pg_extern]
fn h_layer_lookup(layer_id: i64) -> i64 {
    let table = H_LAYER_TABLE.share();
    let start = hl_slot_for(layer_id);
    for probe in 0..HL_N_BUCKETS {
        let idx = (start + probe) & (HL_N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return 0;
        }
        if b.layer_id.load(Ordering::Acquire) == layer_id {
            return b.residual_qty.load(Ordering::Acquire);
        }
    }
    0
}

#[pg_extern]
fn h_layer_arena_reset() {
    let table = H_LAYER_TABLE.exclusive();
    for i in 0..HL_N_BUCKETS {
        let b = &table.buckets[i];
        b.occupied.store(0, Ordering::Release);
        b.layer_id.store(0, Ordering::Release);
        b.residual_qty.store(0, Ordering::Release);
    }
    H_LAYER_PRE_APPLIED.with(|p| p.borrow_mut().clear());
}

#[pg_extern]
fn h_layer_arena_occupied_count() -> i64 {
    let table = H_LAYER_TABLE.share();
    let mut n = 0i64;
    for i in 0..HL_N_BUCKETS {
        if table.buckets[i].occupied.load(Ordering::Acquire) != 0 {
            n += 1;
        }
    }
    n
}

fn try_create_existing(table: &HLHashTable, layer_id: i64, qty: i64) -> bool {
    let start = hl_slot_for(layer_id);
    for probe in 0..HL_N_BUCKETS {
        let idx = (start + probe) & (HL_N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return false;
        }
        if b.layer_id.load(Ordering::Acquire) == layer_id {
            b.residual_qty.fetch_add(qty, Ordering::AcqRel);
            return true;
        }
    }
    false
}

fn insert_new(table: &HLHashTable, layer_id: i64, qty: i64) {
    let start = hl_slot_for(layer_id);
    for probe in 0..HL_N_BUCKETS {
        let idx = (start + probe) & (HL_N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            b.layer_id.store(layer_id, Ordering::Release);
            b.residual_qty.store(qty, Ordering::Release);
            b.occupied.store(1, Ordering::Release);
            return;
        }
        if b.layer_id.load(Ordering::Acquire) == layer_id {
            b.residual_qty.fetch_add(qty, Ordering::AcqRel);
            return;
        }
    }
    error!("h_layer_arena: hash table full (cap={})", HL_N_BUCKETS);
}

fn hl_xact_commit() {
    H_LAYER_PRE_APPLIED.with(|p| p.borrow_mut().clear());
}

fn hl_xact_abort() {
    let to_reverse: Vec<(i64, i64)> =
        H_LAYER_PRE_APPLIED.with(|p| std::mem::take(&mut *p.borrow_mut()));
    if to_reverse.is_empty() {
        return;
    }
    let table = H_LAYER_TABLE.share();
    for (layer_id, delta) in to_reverse {
        let start = hl_slot_for(layer_id);
        for probe in 0..HL_N_BUCKETS {
            let idx = (start + probe) & (HL_N_BUCKETS - 1);
            let b = &table.buckets[idx];
            if b.occupied.load(Ordering::Acquire) == 0 {
                break;
            }
            if b.layer_id.load(Ordering::Acquire) == layer_id {
                // Reverse: subtract the previously-applied delta.
                b.residual_qty.fetch_sub(delta, Ordering::AcqRel);
                break;
            }
        }
    }
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn hl_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => hl_xact_commit(),
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => hl_xact_abort(),
        _ => {}
    }
}

pub fn init() {
    pg_shmem_init!(H_LAYER_TABLE);
}
