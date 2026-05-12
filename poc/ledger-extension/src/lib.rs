//! acct-sw4i: shared-memory ledger balance rollup with bgworker drain.
//!
//! ## Milestone status
//!
//! - M1: pgrx scaffolding + host→container CREATE EXTENSION (validated).
//! - M2: shmem hash table + PgLwLock + counters (validated).
//! - **M3 (this commit): per-bucket atomics + packed u128 key + dual-lock
//!   `ledger_apply_balance_delta` hot path.**
//! - M4: SQL reader `balance(account_id)` (shmem-first, durable fallback).
//! - M5: bgworker drain to `account_balances_rollup`.
//! - M6: custom WAL RM + redo for crash recovery.
//! - M7: recon hook (shmem vs `SUM(posting_lines)` at quiescence).
//! - M8: integrate with PoC `post_batch`.
//! - M9: bench validation vs fan-in / fan-out / WAC-fan shapes.
//!
//! ## M3 concurrency design
//!
//! The hot path is `ledger_apply_balance_delta`. It does NOT take an
//! exclusive lock for the common case (apply against an already-allocated
//! `(account, period, currency, ledger_kind)` cell). Instead:
//!
//! 1. Acquire the global `HASH_TABLE` LWLock in **SHARED** mode. PG's
//!    LWLock allows unbounded concurrent shared holders. The shared lock
//!    only serializes against the rare "insert new key" path below.
//! 2. Probe (open addressing, splitmix64-hashed start). At each visited
//!    bucket, perform atomic loads of `occupied` + key. If we hit an empty
//!    bucket, the probe chain ends — key isn't in the table.
//! 3. If the key is found, do `balance.fetch_add(amount_delta)` +
//!    `qty.fetch_add(qty_delta)` + `last_seq.store(next_seq)`. Multiple
//!    concurrent shared holders updating the SAME bucket compose
//!    correctly because each fetch_add is atomic.
//! 4. If the key wasn't found, fall to the cold path: acquire the
//!    LWLock in **EXCLUSIVE** mode. Re-probe (a racing inserter may
//!    have created the cell while we waited). If still not found,
//!    INSERT a new bucket.
//!
//! Why dual-locking instead of per-bucket spinlocks: open-addressing
//! makes per-bucket locking subtle (the probe chain can race against
//! concurrent inserts that move the empty-bucket terminator). The
//! shared-lock-for-updates / exclusive-lock-for-inserts split keeps the
//! semantics rigorous while preserving lock-free update throughput for
//! the steady-state workload (where all accounts are pre-allocated and
//! every apply is an UPDATE, not an INSERT).
//!
//! ## Packed key layout (u128)
//!
//! ```text
//! bits   meaning                width
//! 127..64  account_id (i64)     64
//!  63..32  period_id  (i32)     32
//!  31..16  currency_id (i16)    16
//!  15..8   ledger_kind (u8)      8
//!   7..0   reserved              8
//! ```

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{PgAtomic, PgLwLock, pg_shmem_init};
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};

pgrx::pg_module_magic!();

pub const N_BUCKETS: usize = 4096;

/// One slot. Cache-line aligned so concurrent shared-lock updates to
/// different buckets don't false-share. AtomicU8/U64/I64 are all
/// zero-init valid, so `mem::zeroed()` gives a valid empty bucket.
#[repr(C, align(64))]
pub struct Bucket {
    pub occupied: AtomicU8,
    pub _pad0: [u8; 7],
    pub key_hi: AtomicU64,
    pub key_lo: AtomicU64,
    pub balance: AtomicI64,
    pub qty: AtomicI64,
    pub last_seq: AtomicU64,
}

unsafe impl PGRXSharedMemory for Bucket {}

#[repr(C)]
pub struct HashTable {
    pub buckets: [Bucket; N_BUCKETS],
}

impl Default for HashTable {
    fn default() -> Self {
        // SAFETY: AtomicU8/U64/I64 all init to 0 from zero bytes. Padding
        // bytes are zero. The resulting HashTable has every bucket
        // occupied=0, all other fields = 0.
        unsafe { std::mem::zeroed() }
    }
}

unsafe impl PGRXSharedMemory for HashTable {}

static HASH_TABLE: PgLwLock<HashTable> = unsafe { PgLwLock::new(c"ledger_hash_table") };
static OCCUPIED_COUNT: PgAtomic<AtomicU64> = unsafe { PgAtomic::new(c"ledger_occupied_count") };
static APPLY_SEQ: PgAtomic<AtomicU64> = unsafe { PgAtomic::new(c"ledger_apply_seq") };

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(HASH_TABLE);
    pg_shmem_init!(OCCUPIED_COUNT);
    pg_shmem_init!(APPLY_SEQ);
}

#[inline]
const fn pack_key(account_id: i64, period_id: i32, currency_id: i16, ledger_kind: u8) -> u128 {
    ((account_id as u64 as u128) << 64)
        | ((period_id as u32 as u128) << 32)
        | ((currency_id as u16 as u128) << 16)
        | ((ledger_kind as u128) << 8)
}

#[inline]
fn slot_for(key: u128) -> usize {
    // splitmix64 mixed over both halves of the u128. Avoids the
    // cluster-on-sequential-account_ids pathology of plain modulo.
    let mut z = (key as u64) ^ ((key >> 64) as u64);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z as usize) & (N_BUCKETS - 1)
}

#[inline]
fn next_seq() -> u64 {
    APPLY_SEQ.get().fetch_add(1, Ordering::AcqRel) + 1
}

/// Try to apply deltas to an existing bucket. Returns Some(seq) on hit,
/// None on probe-chain miss (empty bucket reached or table walked).
#[inline]
fn try_update_existing(
    table: &HashTable,
    key: u128,
    amount_delta: i64,
    qty_delta: i64,
) -> Option<u64> {
    let start = slot_for(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return None;
        }
        if b.key_hi.load(Ordering::Acquire) == key_hi
            && b.key_lo.load(Ordering::Acquire) == key_lo
        {
            b.balance.fetch_add(amount_delta, Ordering::AcqRel);
            b.qty.fetch_add(qty_delta, Ordering::AcqRel);
            let seq = next_seq();
            b.last_seq.store(seq, Ordering::Release);
            return Some(seq);
        }
    }
    None
}

/// Insert a new bucket. Caller MUST hold the exclusive LWLock so no
/// concurrent inserter can race us into a duplicate slot.
#[inline]
fn insert_new(table: &HashTable, key: u128, amount_delta: i64, qty_delta: i64) -> u64 {
    let start = slot_for(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Relaxed) == 0 {
            b.key_hi.store(key_hi, Ordering::Relaxed);
            b.key_lo.store(key_lo, Ordering::Relaxed);
            b.balance.store(amount_delta, Ordering::Relaxed);
            b.qty.store(qty_delta, Ordering::Relaxed);
            let seq = next_seq();
            b.last_seq.store(seq, Ordering::Relaxed);
            b.occupied.store(1, Ordering::Release);
            OCCUPIED_COUNT.get().fetch_add(1, Ordering::AcqRel);
            return seq;
        }
    }
    error!(
        "ledger_apply_balance_delta: hash table full (capacity {})",
        N_BUCKETS
    );
}

// ── SQL surface ───────────────────────────────────────────────────────

#[pg_extern]
fn ledger_extension_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pg_extern]
fn ledger_shmem_capacity() -> i64 {
    N_BUCKETS as i64
}

#[pg_extern]
fn ledger_shmem_occupied() -> i64 {
    OCCUPIED_COUNT.get().load(Ordering::Acquire) as i64
}

#[pg_extern]
fn ledger_shmem_apply_seq() -> i64 {
    APPLY_SEQ.get().load(Ordering::Acquire) as i64
}

/// The hot-path apply. Adds (`amount_delta`, `qty_delta`) to the cell
/// keyed by (`account_id`, `period_id`, `currency_id`, `ledger_kind`),
/// creating the cell with the deltas as initial values if absent.
/// Returns the new `last_seq`.
#[pg_extern]
fn ledger_apply_balance_delta(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
    amount_delta: i64,
    qty_delta: i64,
) -> i64 {
    let key = pack_key(account_id, period_id, currency_id, ledger_kind as u8);
    {
        let table = HASH_TABLE.share();
        if let Some(seq) = try_update_existing(&table, key, amount_delta, qty_delta) {
            return seq as i64;
        }
    }
    let table = HASH_TABLE.exclusive();
    // Re-probe inside exclusive: a concurrent inserter may have placed
    // this key while we were upgrading.
    if let Some(seq) = try_update_existing(&table, key, amount_delta, qty_delta) {
        return seq as i64;
    }
    insert_new(&table, key, amount_delta, qty_delta) as i64
}

/// Read the cell at (`account_id`, `period_id`, `currency_id`,
/// `ledger_kind`). Returns one row of NULLs if absent. Takes the SHARED
/// LWLock; concurrent appliers can proceed without blocking the reader.
#[pg_extern]
fn ledger_balance_lookup(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
) -> TableIterator<
    'static,
    (
        name!(balance, Option<i64>),
        name!(qty, Option<i64>),
        name!(last_seq, Option<i64>),
    ),
> {
    let key = pack_key(account_id, period_id, currency_id, ledger_kind as u8);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    let table = HASH_TABLE.share();
    let start = slot_for(key);
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return TableIterator::new(vec![(None, None, None)]);
        }
        if b.key_hi.load(Ordering::Acquire) == key_hi
            && b.key_lo.load(Ordering::Acquire) == key_lo
        {
            return TableIterator::new(vec![(
                Some(b.balance.load(Ordering::Acquire)),
                Some(b.qty.load(Ordering::Acquire)),
                Some(b.last_seq.load(Ordering::Acquire) as i64),
            )]);
        }
    }
    TableIterator::new(vec![(None, None, None)])
}

/// Wipe the table. Useful for tests and benchmarking baselines. Takes
/// the exclusive lock for the duration.
#[pg_extern]
fn ledger_shmem_reset() {
    let table = HASH_TABLE.exclusive();
    for b in table.buckets.iter() {
        b.occupied.store(0, Ordering::Relaxed);
        b.key_hi.store(0, Ordering::Relaxed);
        b.key_lo.store(0, Ordering::Relaxed);
        b.balance.store(0, Ordering::Relaxed);
        b.qty.store(0, Ordering::Relaxed);
        b.last_seq.store(0, Ordering::Relaxed);
    }
    OCCUPIED_COUNT.get().store(0, Ordering::Release);
    APPLY_SEQ.get().store(0, Ordering::Release);
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn version_string_is_nonempty() {
        let v = crate::ledger_extension_version();
        assert!(!v.is_empty());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries='ledger_extension'"]
    }
}
