//! acct-sw4i: shared-memory ledger balance rollup with bgworker drain.
//!
//! ## Milestone status
//!
//! - M1: pgrx scaffolding + host→container CREATE EXTENSION (validated).
//! - **M2 (this commit): shmem hash table + LWLock + occupied counter.**
//! - M3: per-bucket spinlocks + `ledger_apply_balance_delta` CAS path.
//! - M4: SQL reader `balance(account_id)` (shmem-first, durable rollback).
//! - M5: bgworker drain to `account_balances_rollup`.
//! - M6: custom WAL RM + redo for crash recovery.
//! - M7: recon hook (shmem vs `SUM(posting_lines)` at quiescence).
//! - M8: integrate with PoC `post_batch`.
//! - M9: bench validation vs fan-in / fan-out / WAC-fan shapes.
//!
//! ## M2 design notes
//!
//! Fixed-size open-addressing hash table with linear probing, sized at
//! compile time to `N_BUCKETS = 4096` (~228 KB on the postmaster stack
//! when `Default::default()` constructs the initial value in
//! `shmem_startup_hook`). Runtime GUC-driven sizing requires
//! `RequestAddinShmemSpace` + `ShmemInitStruct` and is deferred until the
//! per-bucket-spinlock design lands in M3.
//!
//! Single `PgLwLock<HashTable>` protects the whole table for M2. This is
//! deliberately the wrong concurrency primitive for the eventual hot path
//! — M3 splits it into per-bucket atomic flags so concurrent writers to
//! different buckets don't serialize. For M2 the single lock is enough
//! to prove allocation, cross-backend visibility, and the SQL surface.
//!
//! Key is a single `i64` for now. M3 packs `(account_id, period_id,
//! currency_id, ledger_kind)` into `u128` as the design doc specifies.
//!
//! ## Load
//!
//! Requires `shared_preload_libraries = 'ledger_extension'` in
//! `postgresql.conf` + PG restart. Without it `_PG_init()` doesn't run,
//! shmem isn't allocated, and any of the SQL functions below will panic
//! with "PgLwLock was not initialized".

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::{PgAtomic, PgLwLock, pg_shmem_init};
use pgrx::shmem::PGRXSharedMemory;
use std::sync::atomic::{AtomicU64, Ordering};

pgrx::pg_module_magic!();

pub const N_BUCKETS: usize = 4096;

/// Per-slot payload. Wider tuple — `(key, balance, qty, last_seq)` —
/// in M3 will move to a `u128` packed key and per-bucket atomic header.
/// For M2, plain `i64` key + flat layout keeps the smoke test simple.
#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct Slot {
    /// 0 = empty, 1 = occupied. Plain `u8` because the M2 design uses a
    /// single `PgLwLock` to serialize all access; the slot itself doesn't
    /// need an atomic until M3 adds per-bucket CAS.
    pub occupied: u8,
    pub _pad: [u8; 7],
    pub key: i64,
    pub balance: i64,
    pub qty: i64,
    pub last_seq: i64,
}

unsafe impl PGRXSharedMemory for Slot {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct HashTable {
    pub slots: [Slot; N_BUCKETS],
}

impl Default for HashTable {
    fn default() -> Self {
        Self { slots: [Slot::default(); N_BUCKETS] }
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
fn slot_for(key: i64) -> usize {
    // splitmix64 -> truncate. Avoids the cluster-on-sequential-keys
    // pathology that plain modulo would exhibit on bigserial account_ids.
    let mut z = key as u64;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z as usize) & (N_BUCKETS - 1)
}

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
    OCCUPIED_COUNT.get().load(Ordering::Relaxed) as i64
}

#[pg_extern]
fn ledger_shmem_apply_seq() -> i64 {
    APPLY_SEQ.get().load(Ordering::Relaxed) as i64
}

/// Upsert a single balance row. Returns whether a new slot was occupied
/// (`true`) vs an existing key updated (`false`). Linear probe up to
/// `N_BUCKETS` slots; raises if the table is full.
#[pg_extern]
fn ledger_balance_set(key: i64, balance: i64, qty: i64) -> bool {
    let seq = APPLY_SEQ.get().fetch_add(1, Ordering::AcqRel) + 1;
    let start = slot_for(key);
    let mut table = HASH_TABLE.exclusive();
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let s = &mut table.slots[idx];
        if s.occupied == 0 {
            s.occupied = 1;
            s.key = key;
            s.balance = balance;
            s.qty = qty;
            s.last_seq = seq as i64;
            OCCUPIED_COUNT.get().fetch_add(1, Ordering::AcqRel);
            return true;
        }
        if s.key == key {
            s.balance = balance;
            s.qty = qty;
            s.last_seq = seq as i64;
            return false;
        }
    }
    error!(
        "ledger_balance_set: hash table full (capacity {}, all slots occupied with different keys)",
        N_BUCKETS
    );
}

/// Returns (balance, qty, last_seq) or NULL if key not found.
#[pg_extern]
fn ledger_balance_get(
    key: i64,
) -> TableIterator<'static, (name!(balance, Option<i64>), name!(qty, Option<i64>), name!(last_seq, Option<i64>))>
{
    let start = slot_for(key);
    let table = HASH_TABLE.share();
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let s = &table.slots[idx];
        if s.occupied == 0 {
            return TableIterator::new(vec![(None, None, None)]);
        }
        if s.key == key {
            return TableIterator::new(vec![(Some(s.balance), Some(s.qty), Some(s.last_seq))]);
        }
    }
    TableIterator::new(vec![(None, None, None)])
}

/// Wipe the table. Useful for tests and for benchmarking baselines.
#[pg_extern]
fn ledger_shmem_reset() {
    let mut table = HASH_TABLE.exclusive();
    for s in table.slots.iter_mut() {
        s.occupied = 0;
        s.key = 0;
        s.balance = 0;
        s.qty = 0;
        s.last_seq = 0;
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
