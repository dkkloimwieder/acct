//! acct-zm69 / zm69.h6-followup — H arena.
//!
//! Shmem-mediated invariant enforcement for Candidate H (append-only
//! `cost_layers_h_ext` + `cost_consumptions_h_ext` from PoC mig 0029).
//!
//! The pure-SQL H schema (mig 0026 + 0027) failed at production batch
//! size: SERIALIZABLE's predicate-read tracking on the deferred SUM
//! trigger amplified conflicts to 70-74% abort rate even at fan_out
//! g=5000. See `bench/results-h-batched-2026-05-14.md`.
//!
//! This module replaces the trigger with a shmem-resident per-group
//! `effective_qty` (= `SUM(layers.qty) - SUM(consumptions.qty)`).
//! Apply functions stage deltas per-backend; the XactCallback's
//! `XACT_EVENT_PRE_COMMIT` phase performs the invariant check + apply
//! under SHARED-with-CAS (or EXCLUSIVE for new cells). On `ABORT`,
//! pre-applied deltas are reversed.
//!
//! ## Why this avoids the pure-SQL trap
//!
//! Pure-SQL H's bottleneck was SSI predicate-read amplification: every
//! deferred trigger fire generated `SELECT SUM(qty) FROM …` predicate
//! reads that SSI tracks for conflict. Concurrent insert sets to the
//! same `cost_consumptions_h` rows created rw-dependency cycles → 40001
//! avalanche. Moving the invariant out of MVCC and into shmem CAS:
//!
//! - Durable INSERTs into `cost_layers_h_ext` / `cost_consumptions_h_ext`
//!   stay pure-SQL, but carry NO trigger and NO predicate reads. They
//!   compose cleanly under READ COMMITTED.
//! - The invariant check happens via shmem `compare_exchange` — not
//!   MVCC-tracked, not SSI-tracked. Concurrent backends serialize
//!   through CAS retries on the same cell, not through the SSI
//!   predicate graph.
//!
//! ## Net-delta semantics
//!
//! H's invariant is "SUM(layers) >= SUM(consumptions) per group at
//! commit time". Order of receipts/consumptions within a transaction
//! does NOT matter — only the net. The wrapper aggregates per-group
//! signed deltas and calls `h_apply_delta(group_id, net_delta)` once
//! per touched group. The PRE_COMMIT phase walks the per-backend
//! `H_PENDING` map and applies each via CAS check.
//!
//! ## Rollback semantics
//!
//! PRE_COMMIT applies eagerly (shmem mutation happens at PRE_COMMIT,
//! NOT at COMMIT). Per the PG `XactEvent` flow, PRE_COMMIT failure
//! raises `error!` → ABORT runs. If PRE_COMMIT raised after applying
//! some prior groups in the same batch, those eager applies are
//! reversed in the ABORT callback via `H_PRE_APPLIED`. Successful
//! PRE_COMMIT is essentially committed (COMMIT phase is a no-op).

use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{PgLwLock, pg_shmem_init};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};

/// 16384 cells × 64-byte cache-aligned bucket = 1 MiB shmem.
/// Same sizing as the parent WAC arena.
pub const H_N_BUCKETS: usize = 16384;

/// One cell. Cache-line aligned. `occupied=0` means empty bucket
/// (probe-chain terminator). `group_id=0` is treated as "not a valid
/// key" so callers must use group_ids >= 1 (the PoC bench seeds
/// 1..=groups_count).
#[repr(C, align(64))]
pub struct HBucket {
    pub occupied: AtomicU8,
    pub _pad0: [u8; 7],
    pub group_id: AtomicI64,
    pub effective_qty: AtomicI64,
    pub _pad1: [u8; 32],
}

unsafe impl PGRXSharedMemory for HBucket {}

#[repr(C)]
pub struct HHashTable {
    pub buckets: [HBucket; H_N_BUCKETS],
}

impl Default for HHashTable {
    fn default() -> Self {
        // SAFETY: atomic + zero-init padding all valid from zero bytes.
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for HHashTable {}

pub static H_HASH_TABLE: PgLwLock<HHashTable> =
    unsafe { PgLwLock::new(c"h_arena_hash_table") };

#[inline]
fn h_slot_for(group_id: i64) -> usize {
    // splitmix64.
    let mut z = group_id as u64;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z as usize) & (H_N_BUCKETS - 1)
}

thread_local! {
    /// Per-backend pending deltas, aggregated by group_id.
    /// Drained at PRE_COMMIT; cleared at COMMIT or ABORT.
    /// (Lazy initializer — HashMap::new() is not const.)
    static H_PENDING: RefCell<HashMap<i64, i64>> = RefCell::new(HashMap::new());

    /// Per-backend list of (group_id, delta) that were applied during
    /// PRE_COMMIT. Reversed in ABORT if the txn rolls back.
    static H_PRE_APPLIED: RefCell<Vec<(i64, i64)>> =
        const { RefCell::new(Vec::new()) };
}

/// Stage a signed qty delta against `group_id`. Positive for receipt
/// (layer), negative for consumption.
///
/// Applies at PRE_COMMIT; does NOT mutate shmem here.
#[pg_extern]
pub fn h_apply_delta(group_id: i64, qty_delta: i64) {
    if group_id <= 0 {
        error!("h_apply_delta: group_id must be > 0; got {group_id}");
    }
    H_PENDING.with(|p| {
        let mut map = p.borrow_mut();
        let entry = map.entry(group_id).or_insert(0);
        *entry = entry.saturating_add(qty_delta);
    });
}

/// Read current effective_qty for `group_id`. Includes both committed
/// shmem state and same-txn staged pending. Returns 0 if no cell and
/// no pending.
#[pg_extern]
fn h_arena_lookup(group_id: i64) -> i64 {
    let shmem = {
        let table = H_HASH_TABLE.share();
        let start = h_slot_for(group_id);
        let mut found = 0i64;
        for probe in 0..H_N_BUCKETS {
            let idx = (start + probe) & (H_N_BUCKETS - 1);
            let b = &table.buckets[idx];
            if b.occupied.load(Ordering::Acquire) == 0 {
                break;
            }
            if b.group_id.load(Ordering::Acquire) == group_id {
                found = b.effective_qty.load(Ordering::Acquire);
                break;
            }
        }
        found
    };
    let pending = H_PENDING.with(|p| p.borrow().get(&group_id).copied().unwrap_or(0));
    shmem + pending
}

/// Wipe all shmem cells and per-backend pending state. Test/bench
/// helper. EXCLUSIVE-locked.
#[pg_extern]
fn h_arena_reset() {
    let table = H_HASH_TABLE.exclusive();
    for i in 0..H_N_BUCKETS {
        let b = &table.buckets[i];
        b.occupied.store(0, Ordering::Release);
        b.group_id.store(0, Ordering::Release);
        b.effective_qty.store(0, Ordering::Release);
    }
    H_PENDING.with(|p| p.borrow_mut().clear());
    H_PRE_APPLIED.with(|p| p.borrow_mut().clear());
}

/// Count of occupied cells. Diagnostic.
#[pg_extern]
fn h_arena_occupied_count() -> i64 {
    let table = H_HASH_TABLE.share();
    let mut n = 0i64;
    for i in 0..H_N_BUCKETS {
        if table.buckets[i].occupied.load(Ordering::Acquire) != 0 {
            n += 1;
        }
    }
    n
}

/// CAS-loop apply with invariant check.
/// Returns:
///   - `Some(true)`  — applied OK (cell existed, CAS succeeded, new effective >= 0)
///   - `Some(false)` — cell existed but the resulting effective would be < 0
///   - `None`        — cell does not exist (caller must insert)
fn h_try_apply_check(table: &HHashTable, group_id: i64, delta: i64) -> Option<bool> {
    let start = h_slot_for(group_id);
    for probe in 0..H_N_BUCKETS {
        let idx = (start + probe) & (H_N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return None;
        }
        if b.group_id.load(Ordering::Acquire) == group_id {
            loop {
                let cur = b.effective_qty.load(Ordering::Acquire);
                let new = match cur.checked_add(delta) {
                    Some(v) => v,
                    None => {
                        error!(
                            "h_arena: effective_qty overflow group_id={} cur={} delta={}",
                            group_id, cur, delta
                        );
                    }
                };
                if new < 0 {
                    return Some(false);
                }
                if b.effective_qty
                    .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Some(true);
                }
                // CAS lost the race; retry with fresh cur.
            }
        }
    }
    None
}

fn h_xact_pre_commit() {
    let pending: Vec<(i64, i64)> = H_PENDING.with(|p| {
        let mut map = p.borrow_mut();
        map.drain().collect()
    });
    if pending.is_empty() {
        return;
    }

    // Phase 1: SHARED lock for existing cells.
    let mut needs_insert: Vec<(i64, i64)> = Vec::new();
    {
        let table = H_HASH_TABLE.share();
        for (group_id, delta) in pending {
            match h_try_apply_check(&table, group_id, delta) {
                Some(true) => {
                    H_PRE_APPLIED.with(|p| p.borrow_mut().push((group_id, delta)));
                }
                Some(false) => {
                    error!(
                        "h_arena: would over-consume group_id={} (net_delta={} would push effective negative)",
                        group_id, delta
                    );
                }
                None => needs_insert.push((group_id, delta)),
            }
        }
    }

    if needs_insert.is_empty() {
        return;
    }

    // Phase 2: EXCLUSIVE lock for new-cell inserts. Re-probe each in
    // case another backend's PRE_COMMIT created the cell since our
    // SHARED-phase miss.
    let table = H_HASH_TABLE.exclusive();
    for (group_id, delta) in needs_insert {
        match h_try_apply_check(&table, group_id, delta) {
            Some(true) => {
                H_PRE_APPLIED.with(|p| p.borrow_mut().push((group_id, delta)));
                continue;
            }
            Some(false) => {
                error!(
                    "h_arena: would over-consume group_id={} (net_delta={} after concurrent insert)",
                    group_id, delta
                );
            }
            None => {}
        }
        // Cell still doesn't exist. Insert new cell with the delta.
        // If delta < 0, that's an over-consume from a zero starting
        // effective — raise.
        if delta < 0 {
            error!(
                "h_arena: would over-consume group_id={} (no layers; net_delta={})",
                group_id, delta
            );
        }
        let start = h_slot_for(group_id);
        let mut inserted = false;
        for probe in 0..H_N_BUCKETS {
            let idx = (start + probe) & (H_N_BUCKETS - 1);
            let b = &table.buckets[idx];
            if b.occupied.load(Ordering::Acquire) == 0 {
                b.group_id.store(group_id, Ordering::Release);
                b.effective_qty.store(delta, Ordering::Release);
                b.occupied.store(1, Ordering::Release);
                H_PRE_APPLIED.with(|p| p.borrow_mut().push((group_id, delta)));
                inserted = true;
                break;
            }
            // Bucket occupied — same group_id? Concurrent insert won.
            if b.group_id.load(Ordering::Acquire) == group_id {
                match h_try_apply_check(&table, group_id, delta) {
                    Some(true) => {
                        H_PRE_APPLIED.with(|p| p.borrow_mut().push((group_id, delta)));
                        inserted = true;
                        break;
                    }
                    Some(false) => {
                        error!(
                            "h_arena: would over-consume group_id={}",
                            group_id
                        );
                    }
                    None => {}
                }
            }
        }
        if !inserted {
            error!("h_arena: hash table full (cap={})", H_N_BUCKETS);
        }
    }
}

fn h_xact_commit() {
    // PRE_COMMIT already applied. Just clear bookkeeping.
    H_PRE_APPLIED.with(|p| p.borrow_mut().clear());
    H_PENDING.with(|p| p.borrow_mut().clear());
}

fn h_xact_abort() {
    // Reverse any deltas that were already applied during PRE_COMMIT.
    let to_reverse: Vec<(i64, i64)> =
        H_PRE_APPLIED.with(|p| std::mem::take(&mut *p.borrow_mut()));
    if !to_reverse.is_empty() {
        let table = H_HASH_TABLE.share();
        for (group_id, delta) in to_reverse {
            let start = h_slot_for(group_id);
            for probe in 0..H_N_BUCKETS {
                let idx = (start + probe) & (H_N_BUCKETS - 1);
                let b = &table.buckets[idx];
                if b.occupied.load(Ordering::Acquire) == 0 {
                    break;
                }
                if b.group_id.load(Ordering::Acquire) == group_id {
                    b.effective_qty.fetch_sub(delta, Ordering::AcqRel);
                    break;
                }
            }
        }
    }
    H_PENDING.with(|p| p.borrow_mut().clear());
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn h_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_PRE_COMMIT => h_xact_pre_commit(),
        pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => h_xact_commit(),
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => h_xact_abort(),
        _ => {}
    }
}

pub fn init() {
    pg_shmem_init!(H_HASH_TABLE);
}
