//! acct-sw4i: shared-memory ledger balance rollup with bgworker drain.
//!
//! ## Milestone status
//!
//! - M1: pgrx scaffolding + host→container CREATE EXTENSION (validated).
//! - M2: shmem hash table + PgLwLock + counters (validated).
//! - M3: per-bucket atomics + packed u128 key + dual-lock apply (validated).
//! - M4: balance() reader + account_balances_rollup durable projection.
//! - M5: bgworker drain of dirty shmem cells → rollup.
//! - M6: lazy-load from rollup on insert.
//! - M7: `ledger_shmem_recon()` cross-checks shmem vs `posting_lines`.
//! - **M8 (this commit): `post_batch_shmem` integration — drop-in
//!   replacement for `post_batch`'s `UPDATE accounts SET balance`
//!   path. Insert posting_lines + per-leg `ledger_apply_balance_delta`.
//!   Recon shows drift=0 against PoC truth.**
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

use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::prelude::*;
use pgrx::shmem::PGRXSharedMemory;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PgAtomic, PgLwLock, Spi, pg_shmem_init};
use std::ffi::CString;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

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
    /// Monotone APPLY_SEQ value stamped by the last apply that mutated
    /// this cell. M5's bgworker compares it to `drained_seq` to find
    /// dirty cells.
    pub last_seq: AtomicU64,
    /// Highest `last_seq` value the bgworker has successfully UPSERTed
    /// into `account_balances_rollup`. A cell is dirty when
    /// `last_seq > drained_seq`.
    pub drained_seq: AtomicU64,
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

// M5 — bgworker drain configuration.
static DRAIN_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static DRAIN_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"acct_poc"));

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(HASH_TABLE);
    pg_shmem_init!(OCCUPIED_COUNT);
    pg_shmem_init!(APPLY_SEQ);

    GucRegistry::define_int_guc(
        c"ledger.drain_interval_ms",
        c"Bgworker drain wake interval (ms)",
        c"How often the ledger drain bgworker wakes to copy dirty shmem cells to account_balances_rollup",
        &DRAIN_INTERVAL_MS,
        10,
        3_600_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        c"ledger.drain_database",
        c"Database the bgworker connects to via SPI",
        c"Bgworker connects to a single database; the rollup table must exist there",
        &DRAIN_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    BackgroundWorkerBuilder::new("ledger_drain")
        .set_function("ledger_drain_main")
        .set_library("ledger_extension")
        .set_argument(None)
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .set_restart_time(Some(Duration::from_secs(1)))
        .enable_spi_access()
        .load();
}

// ── M5: bgworker drain ────────────────────────────────────────────────

/// Bgworker entrypoint. Connects to the configured database via SPI,
/// then loops: wait `drain_interval_ms` on the latch, run one drain
/// tick wrapped in a transaction. Exits cleanly on SIGTERM.
///
/// `#[unsafe(no_mangle)]` is required because PG's bgworker launcher
/// looks the function up via `dlsym` against the exact name passed to
/// `set_function("ledger_drain_main")`. pgrx's `#[pg_guard]` only
/// auto-exports `_PG_init`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_drain_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );
    let dbname = DRAIN_DATABASE
        .get()
        .and_then(|c| c.into_string().ok())
        .unwrap_or_else(|| "acct_poc".to_string());
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    loop {
        let interval = DRAIN_INTERVAL_MS.get().max(10) as u64;
        let alive = BackgroundWorker::wait_latch(Some(Duration::from_millis(interval)));
        if !alive {
            break;
        }
        // Each tick is its own transaction so a failed UPSERT doesn't
        // leave the worker in an uncommitted state.
        BackgroundWorker::transaction(|| {
            do_drain_tick();
        });
    }
}

/// One drain pass. Three phases under best-effort consistency:
///
/// 1. Walk all buckets under SHARED lock, gather (key, balance, qty,
///    last_seq) tuples where `last_seq > drained_seq`. Re-read
///    last_seq after the data reads; if it changed, skip this cell
///    (next tick will catch the new state).
/// 2. UPSERT each dirty cell into `account_balances_rollup` via SPI.
///    The `WHERE last_seq < EXCLUDED.last_seq` guard defends against
///    out-of-order delivery (defensive — the bgworker is currently
///    the only writer).
/// 3. CAS-max each successfully-upserted cell's `drained_seq` up to
///    the captured `last_seq`. Subsequent applies will bump `last_seq`
///    above this watermark and become eligible for the next tick.
fn do_drain_tick() {
    let mut dirty: Vec<(i64, i32, i16, i16, i64, i64, u64, u128)> = Vec::new();
    {
        let table = HASH_TABLE.share();
        for i in 0..N_BUCKETS {
            let b = &table.buckets[i];
            if b.occupied.load(Ordering::Acquire) == 0 {
                continue;
            }
            let last_pre = b.last_seq.load(Ordering::Acquire);
            let drained = b.drained_seq.load(Ordering::Acquire);
            if last_pre <= drained {
                continue;
            }
            let key_hi = b.key_hi.load(Ordering::Acquire);
            let key_lo = b.key_lo.load(Ordering::Acquire);
            let bal = b.balance.load(Ordering::Acquire);
            let qty = b.qty.load(Ordering::Acquire);
            let last_post = b.last_seq.load(Ordering::Acquire);
            if last_post != last_pre {
                continue;
            }
            let key = ((key_hi as u128) << 64) | (key_lo as u128);
            let (account_id, period_id, currency_id, ledger_kind) = unpack_key(key);
            dirty.push((
                account_id,
                period_id,
                currency_id,
                ledger_kind as i16,
                bal,
                qty,
                last_pre,
                key,
            ));
        }
    }
    if dirty.is_empty() {
        return;
    }

    let sql = "INSERT INTO account_balances_rollup
                (account_id, period_id, currency_id, ledger_kind,
                 balance, qty, last_seq, drained_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7, clock_timestamp())
              ON CONFLICT (account_id, period_id, currency_id, ledger_kind)
              DO UPDATE SET balance = EXCLUDED.balance,
                            qty = EXCLUDED.qty,
                            last_seq = EXCLUDED.last_seq,
                            drained_at = EXCLUDED.drained_at
                WHERE account_balances_rollup.last_seq < EXCLUDED.last_seq";
    let mut succeeded: Vec<(u128, u64)> = Vec::with_capacity(dirty.len());
    for (a, p, c, l, bal, qty, last, key) in &dirty {
        let res = Spi::run_with_args(
            sql,
            &[
                (*a).into(),
                (*p).into(),
                (*c).into(),
                (*l).into(),
                (*bal).into(),
                (*qty).into(),
                (*last as i64).into(),
            ],
        );
        if res.is_ok() {
            succeeded.push((*key, *last));
        } else {
            // Rollup table missing in this DB, or transient error.
            // Don't propagate — next tick re-tries. (Bgworker restarts
            // on uncaught error per `set_restart_time(1s)`.)
            log!("ledger_drain: upsert failed for ({a}, {p}, {c}, {l})");
        }
    }

    let table = HASH_TABLE.share();
    for (key, last) in &succeeded {
        stamp_drained(&table, *key, *last);
    }
}

#[inline]
const fn unpack_key(key: u128) -> (i64, i32, i16, i16) {
    let account_id = (key >> 64) as u64 as i64;
    let period_id = (((key >> 32) & 0xFFFF_FFFF) as u32) as i32;
    let currency_id = (((key >> 16) & 0xFFFF) as u16) as i16;
    let ledger_kind = (((key >> 8) & 0xFF) as u8) as i16;
    (account_id, period_id, currency_id, ledger_kind)
}

fn stamp_drained(table: &HashTable, key: u128, last_seq: u64) {
    let start = slot_for(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return;
        }
        if b.key_hi.load(Ordering::Acquire) == key_hi
            && b.key_lo.load(Ordering::Acquire) == key_lo
        {
            let mut cur = b.drained_seq.load(Ordering::Acquire);
            while cur < last_seq {
                match b.drained_seq.compare_exchange(
                    cur,
                    last_seq,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(actual) => cur = actual,
                }
            }
            return;
        }
    }
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

/// Insert a new bucket, optionally seeded with a prior rollup state.
/// Caller MUST hold the exclusive LWLock so no concurrent inserter
/// can race us into a duplicate slot.
///
/// `rollup_seed = Some((bal, qty, last_seq))` means a durable row
/// exists in `account_balances_rollup` from a prior process lifetime
/// (or a manually-seeded test): the new cell starts at `(bal + delta,
/// qty + qty_delta)` and `drained_seq` is set to the rollup's
/// `last_seq`, so the next bgworker tick correctly reports this cell
/// as dirty (its new `last_seq` is advanced via `APPLY_SEQ.fetch_max`
/// to ensure `last_seq > drained_seq`).
///
/// `rollup_seed = None` means the cell is genuinely new; behave as
/// pre-M6: `(delta, qty_delta)` initial values, `drained_seq = 0`.
#[inline]
fn insert_new_seeded(
    table: &HashTable,
    key: u128,
    rollup_seed: Option<(i64, i64, u64)>,
    amount_delta: i64,
    qty_delta: i64,
) -> u64 {
    let (init_balance, init_qty, init_drained_seq) = match rollup_seed {
        Some((bal, qty, last_seq)) => {
            // Bump global APPLY_SEQ above rollup's watermark so the
            // next_seq() call below produces last_seq > drained_seq.
            APPLY_SEQ.get().fetch_max(last_seq, Ordering::AcqRel);
            (bal + amount_delta, qty + qty_delta, last_seq)
        }
        None => (amount_delta, qty_delta, 0),
    };

    let start = slot_for(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Relaxed) == 0 {
            b.key_hi.store(key_hi, Ordering::Relaxed);
            b.key_lo.store(key_lo, Ordering::Relaxed);
            b.balance.store(init_balance, Ordering::Relaxed);
            b.qty.store(init_qty, Ordering::Relaxed);
            let seq = next_seq();
            b.last_seq.store(seq, Ordering::Relaxed);
            b.drained_seq.store(init_drained_seq, Ordering::Relaxed);
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

/// SPI lookup against `account_balances_rollup`. Returns `None` on
/// "no row," missing table, or any SPI error — callers treat that as
/// "no prior state" and the new cell starts at delta-only.
fn lookup_rollup_seed(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
) -> Option<(i64, i64, u64)> {
    let res = Spi::get_three_with_args::<i64, i64, i64>(
        "SELECT balance, qty, last_seq
           FROM account_balances_rollup
          WHERE account_id = $1 AND period_id = $2
            AND currency_id = $3 AND ledger_kind = $4",
        &[
            account_id.into(),
            period_id.into(),
            currency_id.into(),
            ledger_kind.into(),
        ],
    );
    match res {
        Ok((Some(bal), Some(qty), Some(seq))) => Some((bal, qty, seq as u64)),
        _ => None,
    }
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

/// Count of occupied cells whose `last_seq > drained_seq` —
/// pending bgworker drain.
#[pg_extern]
fn ledger_shmem_dirty_count() -> i64 {
    let table = HASH_TABLE.share();
    let mut n: i64 = 0;
    for i in 0..N_BUCKETS {
        let b = &table.buckets[i];
        if b.occupied.load(Ordering::Acquire) == 0 {
            continue;
        }
        if b.last_seq.load(Ordering::Acquire) > b.drained_seq.load(Ordering::Acquire) {
            n += 1;
        }
    }
    n
}

/// Count of occupied cells already drained to rollup at their current
/// `last_seq` watermark.
#[pg_extern]
fn ledger_shmem_drained_count() -> i64 {
    let table = HASH_TABLE.share();
    let mut n: i64 = 0;
    for i in 0..N_BUCKETS {
        let b = &table.buckets[i];
        if b.occupied.load(Ordering::Acquire) == 0 {
            continue;
        }
        let last = b.last_seq.load(Ordering::Acquire);
        let drained = b.drained_seq.load(Ordering::Acquire);
        if drained > 0 && last == drained {
            n += 1;
        }
    }
    n
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

    // M6: cell not in shmem. Look up rollup BEFORE taking exclusive lock
    // so the rare lazy-load path doesn't lengthen the lock hold time. A
    // concurrent inserter racing us between SPI lookup and exclusive
    // acquire is harmless — re-probe inside exclusive catches it and
    // routes through the update path; our SPI result is discarded.
    let rollup_seed = lookup_rollup_seed(account_id, period_id, currency_id, ledger_kind);

    let table = HASH_TABLE.exclusive();
    if let Some(seq) = try_update_existing(&table, key, amount_delta, qty_delta) {
        return seq as i64;
    }
    insert_new_seeded(&table, key, rollup_seed, amount_delta, qty_delta) as i64
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

/// Cross-check shmem balances against the authoritative ledger
/// truth in the PoC `posting_lines` table. Uses the PoC's
/// **debit-positive convention** (matching `post_batch`'s `accounts.
/// balance` semantics): for every account, `ledger_balance =
/// SUM(debits) - SUM(credits)`. This is consistent with how the M8
/// `post_batch_shmem` integration applies deltas:
/// `+amount` on the debit leg, `-amount` on the credit leg.
///
/// Returns one row per occupied shmem cell at the PoC convention
/// `(period_id, currency_id, ledger_kind) = (1, 1, 1)`. Other
/// dimensions are filtered out — M8/acct integration parameterizes
/// the filter.
///
/// `drift = shmem_balance - ledger_balance`. Drift=0 after a
/// `post_batch_shmem` call means the extension hot path produced
/// the same state `post_batch`'s `UPDATE accounts SET balance`
/// would have. Non-zero drift is a real apply-path bug or a
/// scenario where shmem and `posting_lines` were written
/// independently (in which case it's an integration mismatch, not
/// an extension bug).
///
/// `ledger_balance` is NULL when no matching `accounts` row exists
/// for the shmem `account_id`.
#[pg_extern]
fn ledger_shmem_recon() -> TableIterator<
    'static,
    (
        name!(account_id, i64),
        name!(shmem_balance, i64),
        name!(shmem_qty, i64),
        name!(ledger_balance, Option<i64>),
        name!(drift, Option<i64>),
    ),
> {
    // Phase 1: snapshot occupied cells under SHARED lock.
    let mut cells: Vec<(i64, i64, i64)> = Vec::new();
    {
        let table = HASH_TABLE.share();
        for i in 0..N_BUCKETS {
            let b = &table.buckets[i];
            if b.occupied.load(Ordering::Acquire) == 0 {
                continue;
            }
            let key_hi = b.key_hi.load(Ordering::Acquire);
            let key_lo = b.key_lo.load(Ordering::Acquire);
            let bal = b.balance.load(Ordering::Acquire);
            let qty = b.qty.load(Ordering::Acquire);
            let key = ((key_hi as u128) << 64) | (key_lo as u128);
            let (account_id, period_id, currency_id, ledger_kind) = unpack_key(key);
            if period_id != 1 || currency_id != 1 || ledger_kind != 1 {
                continue;
            }
            cells.push((account_id, bal, qty));
        }
    }

    // Phase 2: ledger lookup per cell. SPI outside the LWLock so the
    // recon doesn't block applies.
    // Debit-positive convention: SUM(debits) - SUM(credits) for ALL
    // accounts, regardless of accounts.kind. Matches what post_batch
    // and post_batch_shmem do (accounts.balance and shmem cell store
    // the same signed value). SUM(amount BIGINT) returns numeric; the
    // overall expression is cast to bigint so SPI can decode as i64.
    let sql = "SELECT (
                   COALESCE((SELECT SUM(amount) FROM posting_lines WHERE debit_account_id = a.id), 0)
                 - COALESCE((SELECT SUM(amount) FROM posting_lines WHERE credit_account_id = a.id), 0)
                 )::bigint
                FROM accounts a WHERE a.id = $1";

    let mut out = Vec::with_capacity(cells.len());
    for (account_id, shmem_balance, shmem_qty) in cells {
        let res = Spi::get_one_with_args::<i64>(sql, &[account_id.into()]);
        match res {
            Ok(Some(lb)) => {
                let drift = shmem_balance - lb;
                out.push((account_id, shmem_balance, shmem_qty, Some(lb), Some(drift)));
            }
            _ => out.push((account_id, shmem_balance, shmem_qty, None, None)),
        }
    }

    TableIterator::new(out)
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
        b.drained_seq.store(0, Ordering::Relaxed);
    }
    OCCUPIED_COUNT.get().store(0, Ordering::Release);
    APPLY_SEQ.get().store(0, Ordering::Release);
}

// ── M4: durable rollup table + balance() reader ───────────────────────
//
// `account_balances_rollup` is the eventually-consistent durable
// projection. M5's bgworker will write to it; M4 leaves it as a plain
// table that callers (and tests) can populate manually.
//
// `balance(account_id, period_id, currency_id, ledger_kind)` consults
// shmem first via `ledger_balance_lookup`, then falls back to the
// rollup, then returns zeros + source='none'. The source label lets
// callers and tests reason about freshness.
//
// Semantic note for M5+ — currently shmem cells hold the full running
// total (every apply since CREATE EXTENSION / PG start). When the
// bgworker lands, the contract is "shmem stays authoritative as long
// as it has the cell; rollup is the write-through copy lagging the
// shmem state." If shmem is missing the cell (post-restart, pre-WAL-
// recovery), rollup is authoritative. The `balance()` selection logic
// in this file already implements that contract correctly.
pgrx::extension_sql!(
    r#"
    CREATE TABLE account_balances_rollup (
        account_id  BIGINT NOT NULL,
        period_id   INT NOT NULL,
        currency_id SMALLINT NOT NULL,
        ledger_kind SMALLINT NOT NULL,
        balance     BIGINT NOT NULL DEFAULT 0,
        qty         BIGINT NOT NULL DEFAULT 0,
        last_seq    BIGINT NOT NULL DEFAULT 0,
        drained_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (account_id, period_id, currency_id, ledger_kind)
    );

    CREATE FUNCTION balance(
        p_account_id BIGINT,
        p_period_id INT,
        p_currency_id SMALLINT,
        p_ledger_kind SMALLINT
    ) RETURNS TABLE(balance BIGINT, qty BIGINT, last_seq BIGINT, source TEXT) AS $body$
    DECLARE
        s_balance BIGINT;
        s_qty BIGINT;
        s_last_seq BIGINT;
    BEGIN
        SELECT lookup.balance, lookup.qty, lookup.last_seq
          INTO s_balance, s_qty, s_last_seq
          FROM ledger_balance_lookup(p_account_id, p_period_id, p_currency_id, p_ledger_kind) lookup;

        IF s_balance IS NOT NULL THEN
            balance := s_balance;
            qty := s_qty;
            last_seq := s_last_seq;
            source := 'shmem';
            RETURN NEXT;
            RETURN;
        END IF;

        SELECT r.balance, r.qty, r.last_seq
          INTO balance, qty, last_seq
          FROM account_balances_rollup r
         WHERE r.account_id = p_account_id
           AND r.period_id = p_period_id
           AND r.currency_id = p_currency_id
           AND r.ledger_kind = p_ledger_kind;

        IF FOUND THEN
            source := 'rollup';
            RETURN NEXT;
            RETURN;
        END IF;

        balance := 0;
        qty := 0;
        last_seq := 0;
        source := 'none';
        RETURN NEXT;
    END;
    $body$ LANGUAGE plpgsql STABLE;
    "#,
    name = "rollup_schema",
    requires = [ledger_balance_lookup],
);

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
