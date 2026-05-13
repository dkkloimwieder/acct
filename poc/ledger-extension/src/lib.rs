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
//! - M8: `post_batch_shmem` integration; recon drift=0.
//! - M9: bench validation. Fan-in 2.16× over mutable `post_batch`;
//!   fan-out 5.55× over mutable. N_BUCKETS bumped 4096→16384.
//! - M10.A1: confirmed rollback correctness gap (acct-2733).
//! - M10.D1: INVARIANTS.md catalog + pin tests (acct-w88b).
//! - **M10.A2 (this commit): deferred-apply via XactCallback +
//!   SubXactCallback. `ledger_apply_balance_delta` now STAGES into
//!   a per-backend PENDING_STACK; commit applies, rollback discards.
//!   SAVEPOINT support via SubXactCallback. (acct-4e91)**
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
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CString, c_void};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

// acct-zo4t / M10.B4-prep — atomic 128-bit pair for (balance, qty).
// Single 16-byte load returns a real coupled snapshot; writers CAS-loop.
// Cf. seqlock_torn_read_t1::t2_torn_read_probe (falsification gate).
use portable_atomic::AtomicU128;

pgrx::pg_module_magic!();

// 16384 slots × 64-byte cache-aligned bucket = 1 MiB shmem.
// Sized so the fan-out PoC bench (5000 accounts) lands at a sub-30%
// load factor with comfortable headroom. Open-addressing probes
// degrade past ~70% load, so this caps useful capacity at ~11K cells.
// Future: GUC-driven sizing via RequestAddinShmemSpace + ShmemInitStruct
// (M3 used a const because pgrx's pg_shmem_init! can't size against a
// runtime GUC — the Bucket array shape must be known at compile time).
pub const N_BUCKETS: usize = 16384;

/// One slot. Cache-line aligned so concurrent shared-lock updates to
/// different buckets don't false-share. AtomicU8/U64/U128 are all
/// zero-init valid, so `mem::zeroed()` gives a valid empty bucket.
///
/// `balance_qty` packs `(balance as i64) << 64 | (qty as i64 as u64)`
/// into a single AtomicU128 so readers observe a real coupled pair
/// (acct-zo4t). Helpers `pack_bal_qty` / `unpack_bal_qty` convert.
/// Pre-zo4t this was `balance: AtomicI64, qty: AtomicI64` — separate
/// loads were torn-readable under concurrent SHARED-LWLock writers
/// (the M9 lock-free hot path's race).
///
/// **No deletion path.** Accounts are not removed from a ledger;
/// probe chains in this open-addressing table grow monotonically
/// over a backend's lifetime (modulo `ledger_shmem_reset()` which
/// wipes the entire table). There is no `remove`, `unlink`, or
/// tombstone helper. If you need to reset bucket state, use
/// `ledger_shmem_reset()` for the whole table. Per-cell deletion
/// would require tombstones to preserve probe-chain integrity
/// (acct-layd-style accounting), and we'd rather assert "no
/// deletion" than build the machinery.
#[repr(C, align(64))]
pub struct Bucket {
    pub occupied: AtomicU8,
    pub _pad0: [u8; 7],
    pub key_hi: AtomicU64,
    pub key_lo: AtomicU64,
    /// 16-byte aligned; Rust inserts implicit 8-byte pad before this
    /// field (offset 32 from struct base). Cache-line layout: header
    /// (24) + pad (8) + balance_qty (16) + last_seq (8) + drained_seq
    /// (8) = 64 bytes.
    pub balance_qty: AtomicU128,
    /// Monotone APPLY_SEQ value stamped by the last apply that mutated
    /// this cell. M5's bgworker compares it to `drained_seq` to find
    /// dirty cells.
    pub last_seq: AtomicU64,
    /// Highest `last_seq` value the bgworker has successfully UPSERTed
    /// into `account_balances_rollup`. A cell is dirty when
    /// `last_seq > drained_seq`.
    pub drained_seq: AtomicU64,
}

/// Pack a signed `(balance, qty)` pair into a single u128.
/// High 64 bits = balance (reinterpreted i64→u64), low 64 = qty.
#[inline]
const fn pack_bal_qty(balance: i64, qty: i64) -> u128 {
    ((balance as u64 as u128) << 64) | (qty as u64 as u128)
}

/// Unpack a u128 back to `(balance, qty)`.
#[inline]
const fn unpack_bal_qty(packed: u128) -> (i64, i64) {
    let balance = (packed >> 64) as u64 as i64;
    let qty = packed as u64 as i64;
    (balance, qty)
}

/// CAS-loop fetch_add equivalent for the packed (balance, qty) atom.
/// Lock-free: one writer always makes progress per CAS round. Returns
/// the new (balance, qty) post-add.
#[inline]
fn balance_qty_fetch_add(slot: &AtomicU128, amount_delta: i64, qty_delta: i64) -> (i64, i64) {
    let mut cur = slot.load(Ordering::Acquire);
    loop {
        let (bal, q) = unpack_bal_qty(cur);
        let new_bal = bal.wrapping_add(amount_delta);
        let new_q = q.wrapping_add(qty_delta);
        let new_packed = pack_bal_qty(new_bal, new_q);
        match slot.compare_exchange_weak(
            cur,
            new_packed,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return (new_bal, new_q),
            Err(actual) => cur = actual,
        }
    }
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
// acct-3ee2: counter for the rare post-pre_commit hash-full race.
// Should stay 0 in production; non-zero signals a workload that's
// pushing N_BUCKETS and needs sizing.
static LEDGER_SHMEM_INSERT_FAILURES: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"ledger_shmem_insert_failures") };

// acct-3ovt (M10.C2): bgworker drain SPI failure observability.
// `_CONSECUTIVE` counts ticks with ≥1 failed upsert (resets to 0 on
// the first clean tick); `_TOTAL` counts every failed upsert (never
// resets without `ledger_shmem_reset`). The bgworker emits
// `warning!` when `_CONSECUTIVE` reaches `LEDGER_DRAIN_WARN_AFTER`
// (default 5 ticks = 500ms at the default 100ms cadence).
static LEDGER_DRAIN_CONSECUTIVE_FAILS: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"ledger_drain_consecutive_fails") };
static LEDGER_DRAIN_TOTAL_FAILURES: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"ledger_drain_total_failures") };
const LEDGER_DRAIN_WARN_AFTER: u64 = 5;

// M5 — bgworker drain configuration.
static DRAIN_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static DRAIN_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"acct_poc"));

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(HASH_TABLE);
    pg_shmem_init!(OCCUPIED_COUNT);
    pg_shmem_init!(APPLY_SEQ);
    pg_shmem_init!(LEDGER_SHMEM_INSERT_FAILURES);
    pg_shmem_init!(LEDGER_DRAIN_CONSECUTIVE_FAILS);
    pg_shmem_init!(LEDGER_DRAIN_TOTAL_FAILURES);

    // acct-17vr: register XactCallback + SubXactCallback unconditionally
    // at postmaster init. Each forked backend inherits the callback list,
    // so callbacks fire from the very first transaction event regardless
    // of whether `ledger_apply_balance_delta` is ever called. Eliminates
    // the lazy-registration edge case where the first apply happens
    // inside an already-open subxact (we'd miss its SUBXACT_EVENT_START_SUB
    // and on RELEASE would conflate subxact deltas into the top frame).
    // Cost: a no-op callback dispatch per transaction in every backend
    // even when the ledger is untouched. PG callback dispatch walks a
    // short static list; the no-op branch is a tiny match arm.
    unsafe {
        pg_sys::RegisterXactCallback(Some(ledger_xact_callback), std::ptr::null_mut());
        pg_sys::RegisterSubXactCallback(
            Some(ledger_subxact_callback),
            std::ptr::null_mut(),
        );
    }

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
        // acct-vd74 (M10.C4): when SIGHUP is delivered, the signal
        // handler latches `GOT_SIGHUP` and wakes `wait_latch`. Without
        // calling `ProcessConfigFile(PGC_SIGHUP)` here, the bgworker
        // would keep using stale `DRAIN_INTERVAL_MS` (and `DRAIN_DATABASE`
        // would be ignored even at restart). pgrx exposes `sighup_received`
        // as a one-shot flag.
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
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
            // Atomic 128-bit load — (balance, qty) is a real coupled
            // snapshot from one real instant (acct-zo4t).
            let (bal, qty) = unpack_bal_qty(b.balance_qty.load(Ordering::Acquire));
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
    let mut failed_this_tick: u64 = 0;
    let mut last_err: Option<String> = None;
    for (a, p, c, l, bal, qty, last, key) in &dirty {
        // acct-3ovt: wrap the SPI call in PgTryBuilder so a missing-
        // table / schema-error (which raises an FFI-level ERROR that
        // crosses Rust boundaries as a long-jump) is caught locally.
        // Without this, the error aborts the tick's transaction
        // BEFORE we increment the failure counter — the escalation
        // path stays silent because the panicking tick never reaches
        // its tail.
        let outcome: Result<(), String> = pgrx::PgTryBuilder::new(|| {
            match Spi::run_with_args(
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
            ) {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("{e}")),
            }
        })
        .catch_others(|caught| Err(format!("{caught:?}")))
        .execute();

        match outcome {
            Ok(()) => succeeded.push((*key, *last)),
            Err(e) => {
                failed_this_tick += 1;
                last_err = Some(e.clone());
                log!("ledger_drain: upsert failed for ({a}, {p}, {c}, {l}): {e}");
            }
        }
    }

    let table = HASH_TABLE.share();
    for (key, last) in &succeeded {
        stamp_drained(&table, *key, *last);
    }

    // acct-3ovt: escalation. Update consecutive-failure counter and
    // emit a single rich warning when crossing LEDGER_DRAIN_WARN_AFTER.
    // The warning is per-threshold-crossing (not per-tick) so log
    // volume stays bounded under sustained outages.
    if failed_this_tick > 0 {
        LEDGER_DRAIN_TOTAL_FAILURES
            .get()
            .fetch_add(failed_this_tick, Ordering::AcqRel);
        let prev = LEDGER_DRAIN_CONSECUTIVE_FAILS
            .get()
            .fetch_add(1, Ordering::AcqRel);
        let now = prev + 1;
        if now == LEDGER_DRAIN_WARN_AFTER {
            pgrx::warning!(
                "ledger_drain: {now} consecutive ticks with SPI errors \
                 (this tick: {failed_this_tick} failed of {} dirty cells); \
                 last error: {}. Check that `account_balances_rollup` \
                 exists in the target DB and the bgworker has access.",
                dirty.len(),
                last_err.as_deref().unwrap_or("<none>")
            );
        }
    } else {
        // First clean tick after a failure run: reset to 0.
        LEDGER_DRAIN_CONSECUTIVE_FAILS.get().store(0, Ordering::Release);
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
            // CAS-loop on the packed (balance, qty). Atomic 128-bit RMW
            // — readers observing this cell get a real coupled pair.
            balance_qty_fetch_add(&b.balance_qty, amount_delta, qty_delta);
            let seq = next_seq();
            // fetch_max not store: two writers racing on this cell each
            // pull a monotone seq from next_seq(), but their last_seq
            // stores are not ordered with the global APPLY_SEQ increment.
            // A plain store can land out-of-order (T2's seq=11 stored,
            // then T1's seq=10 stored), leaving last_seq trailing
            // APPLY_SEQ. The drain would briefly see the cell as "clean"
            // at the stale watermark even though APPLY_SEQ moved past.
            // fetch_max guarantees last_seq only ever moves forward per
            // cell regardless of inter-thread reordering.
            b.last_seq.fetch_max(seq, Ordering::AcqRel);
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
/// Returns `Some(seq)` on success, `None` when the hash table has no
/// empty slot within `N_BUCKETS` probes (table is full).
///
/// Pre-`acct-3ee2` this raised `error!()` on overflow. That signature
/// was load-bearing for the synchronous-apply hot path, where the
/// caller's transaction would abort cleanly. Post-A2 the apply runs
/// inside `xact_commit` — the user transaction has already committed
/// in the durable sense, so an error here would be logged but the
/// commit can't be undone. The graceful contract is to return None
/// and let the commit callback emit a WARNING; the `xact_pre_commit`
/// hook is the load-bearing capacity gate (it runs PRE_COMMIT, where
/// raising `error!` properly aborts the tx).
#[inline]
fn insert_new_seeded(
    table: &HashTable,
    key: u128,
    rollup_seed: Option<(i64, i64, u64)>,
    amount_delta: i64,
    qty_delta: i64,
) -> Option<u64> {
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
            // Single atomic store of the packed pair; caller holds
            // EXCLUSIVE so no concurrent inserter contends.
            b.balance_qty
                .store(pack_bal_qty(init_balance, init_qty), Ordering::Relaxed);
            let seq = next_seq();
            b.last_seq.store(seq, Ordering::Relaxed);
            b.drained_seq.store(init_drained_seq, Ordering::Relaxed);
            b.occupied.store(1, Ordering::Release);
            OCCUPIED_COUNT.get().fetch_add(1, Ordering::AcqRel);
            return Some(seq);
        }
    }
    None
}

/// SPI lookup against `account_balances_rollup`. Returns `None` on
/// "no row," missing table, or any SPI error — callers treat that as
/// "no prior state" and the new cell starts at delta-only.
///
/// Wrapped in `PgTryBuilder` so a missing rollup table (or any other
/// Postgres-side error inside SPI) returns `None` instead of
/// propagating an ERROR back to the user transaction. This matters
/// for acct-3ovt-style outages where the bgworker test wants to break
/// the rollup table to exercise the drain-failure escalation path —
/// without this guard, the user's `ledger_apply_balance_delta` call
/// also fails because of the lazy-load SPI.
fn lookup_rollup_seed(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
) -> Option<(i64, i64, u64)> {
    pgrx::PgTryBuilder::new(|| {
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
    })
    .catch_others(|_caught| None)
    .execute()
}

// ── M10.A2: deferred-apply via XactCallback + SubXactCallback ─────────
//
// Pre-A2, `ledger_apply_balance_delta` mutated shmem synchronously. A
// `BEGIN; apply; ROLLBACK;` left the delta in shmem — the bug confirmed
// by `tests/rollback_correctness_t1.rs` (M10.A1).
//
// A2 makes the apply transactional. Each call STAGES `(amount_delta,
// qty_delta, captured_rollup_seed)` into a per-backend thread-local
// PENDING_STACK. The actual shmem mutation happens in the Commit
// callback. Abort callback discards. SubXact callbacks support
// SAVEPOINT / ROLLBACK TO semantics: START_SUB pushes a fresh frame;
// COMMIT_SUB merges into parent; ABORT_SUB discards.
//
// Eager rollup_seed capture: SPI to `account_balances_rollup` is only
// safe in user-transaction context, not in commit callbacks. So when
// the staging path notices a cell missing from shmem at apply time, it
// captures the rollup seed eagerly and stores it alongside the delta.
// At commit time, the apply re-probes shmem under SHARED/EXCLUSIVE; if
// the cell has appeared (another backend's commit raced in), it does
// fetch_add and the captured seed is unused.
//
// Same-key collapse: PENDING_STACK[top] is a HashMap keyed by packed
// u128; subsequent applies to the same key sum deltas. SubXact COMMIT
// merges the popped frame into its parent, summing same-key deltas
// again.
//
// RYW LIMITATION: in-txn `ledger_balance_lookup` returns PRE-apply
// state because the cell isn't mutated until Commit. Existing PoC
// integration (`post_batch_shmem`) does not RYW within a txn; no
// load-bearing impact.

#[derive(Clone, Copy, Default, Debug)]
struct PendingEntry {
    amount_delta: i64,
    qty_delta: i64,
    /// Captured eagerly when no cell existed in shmem at apply time. Used
    /// by `insert_new_seeded` if the cell still doesn't exist at commit.
    rollup_seed: Option<(i64, i64, u64)>,
    /// Set after the first apply did its rollup_seed lookup. Prevents
    /// repeat SPI on collapse + subxact-merge paths.
    rollup_seed_captured: bool,
}

thread_local! {
    /// Per-backend pending deltas. Vec of frames: index 0 is the
    /// top-level transaction; subxacts push/pop. Initialized lazily
    /// (the const initializer below runs at thread_local first-access
    /// per backend).
    ///
    /// XactCallback + SubXactCallback are registered once in `_PG_init`
    /// (acct-17vr) so the callback path is wired up before any backend
    /// transaction begins. Frame management on this stack is still
    /// lazy: backends that never call `ledger_apply_balance_delta`
    /// keep the stack empty and the callbacks become no-ops.
    static PENDING_STACK: RefCell<Vec<HashMap<u128, PendingEntry>>> =
        const { RefCell::new(Vec::new()) };
}

/// Read-only probe. Returns true if a cell for `key` exists in shmem.
#[inline]
fn cell_exists(table: &HashTable, key: u128) -> bool {
    let start = slot_for(key);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return false;
        }
        if b.key_hi.load(Ordering::Acquire) == key_hi
            && b.key_lo.load(Ordering::Acquire) == key_lo
        {
            return true;
        }
    }
    false
}

/// Ensure PENDING_STACK has at least one frame (the top-level txn
/// frame). Called before any push/insert.
///
/// Callbacks are registered in `_PG_init` (acct-17vr); frame setup
/// here is the only remaining lazy step. SubXactCallback events
/// arriving before the first apply find an empty stack and skip
/// safely (push/pop guards check `is_empty()`).
fn ensure_top_frame() {
    PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        if stack.is_empty() {
            stack.push(HashMap::new());
        }
    });
}

#[pg_guard]
unsafe extern "C-unwind" fn ledger_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_PRE_COMMIT => xact_pre_commit(),
        pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => xact_commit(),
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => xact_abort(),
        _ => {}
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn ledger_subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut c_void,
) {
    match event {
        pg_sys::SubXactEvent::SUBXACT_EVENT_START_SUB => subxact_start_sub(),
        pg_sys::SubXactEvent::SUBXACT_EVENT_COMMIT_SUB => subxact_commit_sub(),
        pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB => subxact_abort_sub(),
        _ => {}
    }
}

/// PreCommit: count distinct new keys that would require insert_new
/// at commit time. If `current_occupied + new_keys > N_BUCKETS`, raise
/// `error!` — PG aborts the COMMIT cleanly.
fn xact_pre_commit() {
    // Collect new keys from all frames (defensive; subxact callbacks
    // should have merged everything into frame 0 by this point).
    let new_keys: Vec<u128> = PENDING_STACK.with(|s| {
        let stack = s.borrow();
        let mut keys: Vec<u128> = Vec::new();
        for frame in stack.iter() {
            for &key in frame.keys() {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
        keys
    });

    if new_keys.is_empty() {
        return;
    }

    let mut new_inserts = 0usize;
    {
        let table = HASH_TABLE.share();
        for key in &new_keys {
            if !cell_exists(&table, *key) {
                new_inserts += 1;
            }
        }
    }

    if new_inserts > 0 {
        let current = OCCUPIED_COUNT.get().load(Ordering::Acquire) as usize;
        if current + new_inserts > N_BUCKETS {
            error!(
                "ledger_xact_pre_commit: hash table would overflow at commit \
                 (occupied={current}, new_keys={new_inserts}, cap={N_BUCKETS})"
            );
        }
    }
}

/// Commit: apply all staged deltas. Two-pass: SHARED for existing
/// cells, EXCLUSIVE for inserts.
///
/// MUST succeed. Any error here will be reported by PG but the
/// transaction has already committed in the durable sense — the user
/// SQL state is the source of truth, shmem just lags. The PreCommit
/// hook is responsible for catching capacity overflow; commit-phase
/// errors should be vanishingly rare (race against reset, OOM).
fn xact_commit() {
    let entries = drain_pending_stack();
    if entries.is_empty() {
        return;
    }

    let mut needs_insert: Vec<(u128, PendingEntry)> = Vec::new();
    {
        let table = HASH_TABLE.share();
        for (key, entry) in entries {
            if try_update_existing(&table, key, entry.amount_delta, entry.qty_delta).is_some() {
                continue;
            }
            needs_insert.push((key, entry));
        }
    }

    if !needs_insert.is_empty() {
        let table = HASH_TABLE.exclusive();
        for (key, entry) in needs_insert {
            // Re-probe under EXCLUSIVE: a concurrent backend's commit
            // may have created the cell since our SHARED-probe miss.
            if try_update_existing(&table, key, entry.amount_delta, entry.qty_delta).is_some() {
                continue;
            }
            if insert_new_seeded(
                &table,
                key,
                entry.rollup_seed,
                entry.amount_delta,
                entry.qty_delta,
            )
            .is_none()
            {
                // acct-3ee2 — hash table full at commit time. The
                // pre_commit hook should have caught this; reaching
                // here means a concurrent backend's commit pushed
                // OCCUPIED_COUNT past N_BUCKETS between our
                // pre_commit projection and now. Log + count;
                // can't raise `error!` here because the tx is
                // already committed in PG's durable sense.
                LEDGER_SHMEM_INSERT_FAILURES
                    .get()
                    .fetch_add(1, Ordering::AcqRel);
                pgrx::warning!(
                    "ledger_xact_commit: hash table full at insert (post-pre_commit race); \
                     cell skipped, recon will flag drift. key_hi={:#x} key_lo={:#x}",
                    (key >> 64) as u64,
                    key as u64
                );
            }
        }
    }
}

/// Abort: discard all staged deltas. Reset stack to a single empty
/// frame. Idempotent across nested aborts because PG only fires
/// XACT_EVENT_ABORT at top-level rollback.
fn xact_abort() {
    PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        stack.clear();
        stack.push(HashMap::new());
    });
}

/// Take all pending frames, merge into one map collapsing same-key
/// deltas, leave a fresh empty top-level frame.
fn drain_pending_stack() -> HashMap<u128, PendingEntry> {
    PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let mut merged: HashMap<u128, PendingEntry> = HashMap::new();
        for frame in stack.drain(..) {
            for (key, entry) in frame {
                merge_entry(&mut merged, key, entry);
            }
        }
        stack.push(HashMap::new());
        merged
    })
}

fn merge_entry(map: &mut HashMap<u128, PendingEntry>, key: u128, entry: PendingEntry) {
    map.entry(key)
        .and_modify(|e| {
            e.amount_delta = e.amount_delta.saturating_add(entry.amount_delta);
            e.qty_delta = e.qty_delta.saturating_add(entry.qty_delta);
            // First captured seed wins — represents the durable state at
            // earliest staging time, which is what insert_new_seeded
            // would have used for a then-missing cell.
            if !e.rollup_seed_captured {
                e.rollup_seed = entry.rollup_seed;
                e.rollup_seed_captured = entry.rollup_seed_captured;
            }
        })
        .or_insert(entry);
}

fn subxact_start_sub() {
    PENDING_STACK.with(|s| {
        s.borrow_mut().push(HashMap::new());
    });
}

fn subxact_commit_sub() {
    PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let popped = match stack.pop() {
            Some(f) => f,
            None => {
                // No-op: subxact COMMIT_SUB fired without a matching
                // START_SUB (would indicate a callback registered
                // mid-subxact). Re-establish invariant defensively.
                stack.push(HashMap::new());
                return;
            }
        };
        if stack.is_empty() {
            // Restore single-frame invariant; carry the merged result
            // forward as the new top-level frame.
            stack.push(popped);
            return;
        }
        let parent = stack.last_mut().unwrap();
        for (key, entry) in popped {
            merge_entry(parent, key, entry);
        }
    });
}

fn subxact_abort_sub() {
    PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let _ = stack.pop();
        if stack.is_empty() {
            stack.push(HashMap::new());
        }
    });
}

/// Stage `(amount_delta, qty_delta)` for the cell keyed by
/// `(account_id, period_id, currency_id, ledger_kind)`. On the first
/// stage in a backend, registers the XactCallback + SubXactCallback so
/// the staged deltas apply at COMMIT and discard on ROLLBACK.
///
/// Same-key applies within one (sub)transaction are collapsed into a
/// single pending entry (deltas summed). SubXact START pushes a fresh
/// frame; COMMIT_SUB merges into parent; ABORT_SUB discards.
fn stage_apply(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
    amount_delta: i64,
    qty_delta: i64,
) {
    ensure_top_frame();
    let key = pack_key(account_id, period_id, currency_id, ledger_kind as u8);

    // Fast path: if entry already exists in the top frame, just sum.
    let need_seed = PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let top = stack.last_mut().unwrap();
        if let Some(existing) = top.get_mut(&key) {
            existing.amount_delta = existing.amount_delta.saturating_add(amount_delta);
            existing.qty_delta = existing.qty_delta.saturating_add(qty_delta);
            return false;
        }
        // Reserve the entry; rollup_seed populated below if needed.
        top.insert(
            key,
            PendingEntry {
                amount_delta,
                qty_delta,
                rollup_seed: None,
                rollup_seed_captured: false,
            },
        );
        true
    });

    if !need_seed {
        return;
    }

    // Probe shmem under SHARED. If the cell already exists, no
    // rollup_seed needed — at commit we'll just fetch_add against the
    // existing cell.
    let exists_in_shmem = {
        let table = HASH_TABLE.share();
        cell_exists(&table, key)
    };

    let seed = if exists_in_shmem {
        None
    } else {
        lookup_rollup_seed(account_id, period_id, currency_id, ledger_kind)
    };

    PENDING_STACK.with(|s| {
        let mut stack = s.borrow_mut();
        let top = stack.last_mut().unwrap();
        if let Some(entry) = top.get_mut(&key) {
            entry.rollup_seed = seed;
            entry.rollup_seed_captured = true;
        }
    });
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

/// acct-3ee2: count of commit-time hash-full failures. Expected to
/// stay 0 in production — `xact_pre_commit` is the load-bearing
/// capacity gate. Non-zero values signal a workload that pushed
/// `N_BUCKETS` between pre-commit projection and commit, suggesting
/// the table is sized too tightly.
#[pg_extern]
fn ledger_shmem_insert_failure_count() -> i64 {
    LEDGER_SHMEM_INSERT_FAILURES
        .get()
        .load(Ordering::Acquire) as i64
}

/// acct-3ovt: count of consecutive bgworker drain ticks with at least
/// one failed UPSERT. Resets to 0 on the first fully-successful tick.
/// Emits a `warning!` (visible in `pg_stat_activity` and logs) when
/// it reaches `LEDGER_DRAIN_WARN_AFTER` (default 5).
#[pg_extern]
fn ledger_drain_consecutive_fails() -> i64 {
    LEDGER_DRAIN_CONSECUTIVE_FAILS
        .get()
        .load(Ordering::Acquire) as i64
}

/// acct-3ovt: cumulative count of failed UPSERTs across all drain
/// ticks since the bgworker started (or last `ledger_shmem_reset`).
#[pg_extern]
fn ledger_drain_total_failures() -> i64 {
    LEDGER_DRAIN_TOTAL_FAILURES
        .get()
        .load(Ordering::Acquire) as i64
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

/// The hot-path apply. STAGES `(amount_delta, qty_delta)` into the
/// per-backend pending stack; the actual shmem mutation happens at
/// COMMIT via the XactCallback. ROLLBACK / ROLLBACK TO discard the
/// staged delta.
///
/// **Return value semantics changed in M10.A2.** Pre-A2, the function
/// returned the cell's new `last_seq` (synchronous shmem mutation).
/// Post-A2, it returns `0` — the cell's actual `last_seq` is not
/// known until COMMIT fires. Callers that need to observe the
/// post-commit seq should query `ledger_balance_lookup` AFTER the
/// transaction commits.
///
/// **Read-your-writes limitation.** Within a transaction,
/// `ledger_balance_lookup` returns PRE-staging state because the
/// cell isn't mutated until COMMIT. The PoC integration
/// (`post_batch_shmem`) doesn't RYW in a transaction, so this is
/// non-load-bearing. A future RYW-requiring caller would need a
/// TX-local sidecar cache or the more invasive "provisional shmem
/// delta + commit-confirm" pattern.
#[pg_extern]
fn ledger_apply_balance_delta(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
    amount_delta: i64,
    qty_delta: i64,
) -> i64 {
    stage_apply(
        account_id,
        period_id,
        currency_id,
        ledger_kind,
        amount_delta,
        qty_delta,
    );
    0
}

/// acct-r8xv (M10 followup) — batch entry point. Takes a JSONB array of
/// pre-computed legs; stages all of them via the same A2 PENDING_STACK
/// path as `ledger_apply_balance_delta`, but with ONE cross-boundary
/// call per batch instead of one per leg.
///
/// Envelope shape (short keys to minimize JSONB size):
/// ```json
/// [
///   {"a": 123, "amt":  1000, "qty": 100},
///   {"a": 124, "amt": -1000, "qty":   0}
/// ]
/// ```
///
/// Optional per-leg dimension keys (default to the PoC convention 1):
///   `"p"` period_id (i32), `"c"` currency_id (i16), `"k"` ledger_kind (i16).
///
/// Returns the number of legs staged. Errors raise via `pgrx::error!`
/// before staging anything (atomicity: a malformed envelope aborts the
/// whole batch, the caller's txn rolls back, no partial state leaks).
#[pg_extern]
fn ledger_apply_batch(envelopes: pgrx::JsonB) -> i64 {
    let arr = match envelopes.0.as_array() {
        Some(a) => a,
        None => pgrx::error!("ledger_apply_batch: envelopes must be a JSONB array"),
    };

    // Two-pass: validate everything first, then stage. Avoids partial
    // PENDING_STACK pollution if a malformed envelope appears mid-batch.
    // (The caller's txn would roll back via xact_abort, but doing it
    // up-front keeps the failure mode crisp.)
    let mut parsed: Vec<(i64, i32, i16, i16, i64, i64)> = Vec::with_capacity(arr.len());
    for (idx, env) in arr.iter().enumerate() {
        let obj = match env.as_object() {
            Some(o) => o,
            None => pgrx::error!(
                "ledger_apply_batch: envelope[{}] is not an object",
                idx
            ),
        };
        let get_i64 = |k: &str| -> i64 {
            match obj.get(k).and_then(|v| v.as_i64()) {
                Some(v) => v,
                None => pgrx::error!(
                    "ledger_apply_batch: envelope[{}] missing/invalid integer key '{}'",
                    idx,
                    k
                ),
            }
        };
        let get_i64_or = |k: &str, default: i64| -> i64 {
            obj.get(k).and_then(|v| v.as_i64()).unwrap_or(default)
        };
        let account_id = get_i64("a");
        let period_id = get_i64_or("p", 1) as i32;
        let currency_id = get_i64_or("c", 1) as i16;
        let ledger_kind = get_i64_or("k", 1) as i16;
        let amount_delta = get_i64("amt");
        let qty_delta = get_i64("qty");
        parsed.push((
            account_id,
            period_id,
            currency_id,
            ledger_kind,
            amount_delta,
            qty_delta,
        ));
    }

    for (a, p, c, k, amt, qty) in &parsed {
        stage_apply(*a, *p, *c, *k, *amt, *qty);
    }
    parsed.len() as i64
}

/// acct-2g9w helper: probe shmem for `(value, qty)` at the PoC convention
/// `(period_id=1, currency_id=1, ledger_kind=1)`. Returns `(0, 0)` if no
/// cell exists. Inlined into the WAC dispatcher to avoid the
/// `TableIterator<Vec>` allocation that `ledger_balance_lookup` does per
/// call.
#[inline]
fn probe_shmem_pool(pool_id: i64) -> (i64, i64) {
    let key = pack_key(pool_id, 1, 1, 1);
    let key_hi = (key >> 64) as u64;
    let key_lo = key as u64;
    let table = HASH_TABLE.share();
    let start = slot_for(key);
    for probe in 0..N_BUCKETS {
        let idx = (start + probe) & (N_BUCKETS - 1);
        let b = &table.buckets[idx];
        if b.occupied.load(Ordering::Acquire) == 0 {
            return (0, 0);
        }
        if b.key_hi.load(Ordering::Acquire) == key_hi
            && b.key_lo.load(Ordering::Acquire) == key_lo
        {
            return unpack_bal_qty(b.balance_qty.load(Ordering::Acquire));
        }
    }
    (0, 0)
}

/// acct-2g9w — maximal r8xv. Push WAC running-avg dispatch fully into
/// Rust. Wraps the per-envelope work of mig 0014's plpgsql
/// `post_batch_wac_shmem`: shmem pool seed, in-batch running-avg map,
/// per-leg amount/qty computation, and `stage_apply`. Returns per-
/// envelope priced legs so the SQL wrapper can do a single set-based
/// `INSERT INTO posting_lines`.
///
/// Envelope shape mirrors mig 0014:
/// ```json
/// {
///   "envelope_idx": 0,
///   "kind": "transfer" | "wac_receipt" | "wac_issue",
///   "debit_account_id": ...,
///   "credit_account_id": ...,
///   "amount": ...,        // required for transfer
///   "qty": ...,           // required for wac_*
///   "unit_cost": ...      // required for wac_receipt
/// }
/// ```
///
/// Returned rows:
///   transfer    : qty=NULL, amount=caller-supplied
///   wac_receipt : qty=Some(qty), amount=qty*unit_cost
///   wac_issue   : qty=Some(qty), amount=qty*running_avg
///
/// `period_id`, `currency_id`, `ledger_kind` are hardcoded to the PoC
/// convention (1, 1, 1). Idempotency replays are pre-filtered by the
/// SQL wrapper BEFORE invoking this fn — every envelope here is a
/// fresh posting that will both INSERT a posting_line and stage_apply
/// its legs.
///
/// Two-pass: validates ALL envelopes (raises on any malformed input)
/// before any `stage_apply`. Atomicity matches `ledger_apply_batch`.
#[pg_extern]
fn ledger_dispatch_wac_batch(
    envelopes: pgrx::JsonB,
) -> TableIterator<
    'static,
    (
        name!(envelope_idx, i32),
        name!(debit_account_id, i64),
        name!(credit_account_id, i64),
        name!(amount, i64),
        name!(qty, Option<i64>),
    ),
> {
    let arr = match envelopes.0.as_array() {
        Some(a) => a,
        None => pgrx::error!("ledger_dispatch_wac_batch: envelopes must be a JSONB array"),
    };

    #[derive(Clone, Copy)]
    enum Kind {
        Transfer,
        WacReceipt,
        WacIssue,
    }
    struct Parsed {
        envelope_idx: i32,
        kind: Kind,
        debit: i64,
        credit: i64,
        amount: Option<i64>,
        qty: Option<i64>,
        unit_cost: Option<i64>,
    }

    // Pass 1: parse + structural validate.
    let mut parsed: Vec<Parsed> = Vec::with_capacity(arr.len());
    for (idx, env) in arr.iter().enumerate() {
        let obj = match env.as_object() {
            Some(o) => o,
            None => pgrx::error!(
                "ledger_dispatch_wac_batch: envelope[{}] is not an object",
                idx
            ),
        };
        let kind_str = obj
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("transfer");
        let kind = match kind_str {
            "transfer" => Kind::Transfer,
            "wac_receipt" => Kind::WacReceipt,
            "wac_issue" => Kind::WacIssue,
            other => pgrx::error!(
                "ledger_dispatch_wac_batch: envelope[{}] unknown kind '{}'",
                idx,
                other
            ),
        };
        let env_idx = obj
            .get("envelope_idx")
            .and_then(|v| v.as_i64())
            .unwrap_or(idx as i64) as i32;
        let debit = match obj.get("debit_account_id").and_then(|v| v.as_i64()) {
            Some(v) => v,
            None => pgrx::error!(
                "ledger_dispatch_wac_batch: envelope[{}] missing debit_account_id",
                idx
            ),
        };
        let credit = match obj.get("credit_account_id").and_then(|v| v.as_i64()) {
            Some(v) => v,
            None => pgrx::error!(
                "ledger_dispatch_wac_batch: envelope[{}] missing credit_account_id",
                idx
            ),
        };
        let amount = obj.get("amount").and_then(|v| v.as_i64());
        let qty = obj.get("qty").and_then(|v| v.as_i64());
        let unit_cost = obj.get("unit_cost").and_then(|v| v.as_i64());

        match kind {
            Kind::Transfer => {
                if amount.is_none() {
                    pgrx::error!(
                        "ledger_dispatch_wac_batch: envelope[{}] transfer missing amount",
                        idx
                    );
                }
            }
            Kind::WacReceipt => {
                if qty.unwrap_or(0) <= 0 {
                    pgrx::error!(
                        "ledger_dispatch_wac_batch: envelope[{}] wac_receipt missing/invalid qty",
                        idx
                    );
                }
                if unit_cost.unwrap_or(0) <= 0 {
                    pgrx::error!(
                        "ledger_dispatch_wac_batch: envelope[{}] wac_receipt missing/invalid unit_cost",
                        idx
                    );
                }
            }
            Kind::WacIssue => {
                if qty.unwrap_or(0) <= 0 {
                    pgrx::error!(
                        "ledger_dispatch_wac_batch: envelope[{}] wac_issue missing/invalid qty",
                        idx
                    );
                }
            }
        }
        parsed.push(Parsed {
            envelope_idx: env_idx,
            kind,
            debit,
            credit,
            amount,
            qty,
            unit_cost,
        });
    }

    // Local in-batch running-avg map: account_id -> (value, qty).
    // Seeded lazily from shmem (one SHARED probe per distinct pool on
    // first reference). PoC convention pins period=1, currency=1, kind=1.
    let mut pool_map: HashMap<i64, (i64, i64)> = HashMap::with_capacity(arr.len().min(64));

    // Pass 2: price each envelope, building output rows + collecting
    // legs to stage_apply. Defer stage_apply until pricing succeeds for
    // all envelopes (matches `ledger_apply_batch`'s validate-then-stage
    // atomicity: if pricing raises mid-batch, no PENDING_STACK pollution).
    let mut rows: Vec<(i32, i64, i64, i64, Option<i64>)> = Vec::with_capacity(parsed.len());
    // Legs to stage: (account_id, amount_delta, qty_delta).
    let mut legs: Vec<(i64, i64, i64)> = Vec::with_capacity(parsed.len() * 2);

    for p in &parsed {
        let (out_amount, out_qty_opt) = match p.kind {
            Kind::Transfer => {
                let amt = p.amount.expect("validated");
                // debit +amt/0; credit -amt/0
                legs.push((p.debit, amt, 0));
                legs.push((p.credit, -amt, 0));
                (amt, None)
            }
            Kind::WacReceipt => {
                let qty = p.qty.expect("validated");
                let uc = p.unit_cost.expect("validated");
                let amt = qty.saturating_mul(uc);
                let pool_id = p.debit;
                let entry = pool_map
                    .entry(pool_id)
                    .or_insert_with(|| probe_shmem_pool(pool_id));
                entry.0 = entry.0.saturating_add(amt);
                entry.1 = entry.1.saturating_add(qty);
                // pool gets +amount/+qty; counterparty gets -amount/0
                legs.push((p.debit, amt, qty));
                legs.push((p.credit, -amt, 0));
                (amt, Some(qty))
            }
            Kind::WacIssue => {
                let qty = p.qty.expect("validated");
                // Pool is the credit account.
                let pool_id = p.credit;
                let entry = pool_map
                    .entry(pool_id)
                    .or_insert_with(|| probe_shmem_pool(pool_id));
                let running_value = entry.0;
                let running_qty = entry.1;
                if running_qty <= 0 {
                    pgrx::error!(
                        "ledger_dispatch_wac_batch: envelope[{}] wac_issue from empty pool {} (running qty={})",
                        p.envelope_idx,
                        p.credit,
                        running_qty
                    );
                }
                if qty > running_qty {
                    pgrx::error!(
                        "ledger_dispatch_wac_batch: envelope[{}] wac_issue qty={} exceeds running qty={}",
                        p.envelope_idx,
                        qty,
                        running_qty
                    );
                }
                // Integer division: matches mig 0014 / mig 0006 semantics.
                let unit_cost = running_value / running_qty;
                let amt = unit_cost.saturating_mul(qty);
                entry.0 = running_value - amt;
                entry.1 = running_qty - qty;
                // pool (credit) gets -amount/-qty; counterparty (debit) gets +amount/0
                legs.push((p.credit, -amt, -qty));
                legs.push((p.debit, amt, 0));
                (amt, Some(qty))
            }
        };
        rows.push((p.envelope_idx, p.debit, p.credit, out_amount, out_qty_opt));
    }

    // Pass 3: stage every leg. PENDING_STACK fast-path collapses same-key
    // legs across envelopes (e.g., fan-in batches with one shared pool).
    for (account_id, amt, qty) in &legs {
        stage_apply(*account_id, 1, 1, 1, *amt, *qty);
    }

    TableIterator::new(rows)
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
            // Single atomic 128-bit load — (balance, qty) is a real
            // coupled snapshot (acct-zo4t).
            let (bal, qty) = unpack_bal_qty(b.balance_qty.load(Ordering::Acquire));
            return TableIterator::new(vec![(
                Some(bal),
                Some(qty),
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
///
/// # Concurrency semantics
///
/// Phase 1 walks occupied buckets under SHARED LWLock and per-cell
/// loads `balance_qty` as a single 128-bit atomic (acct-zo4t /
/// M10.B4-prep). Each row's `(shmem_balance, shmem_qty)` is therefore
/// a real coupled snapshot from one instant — never a torn pair
/// (I11). Different rows in one recon call may reflect different
/// instants; the function reports per-cell consistency, not cross-cell
/// linearizability.
///
/// Phase 2 (SPI lookup of `ledger_balance`) runs outside the LWLock,
/// after Phase 1 snapshots. A concurrent applier between Phase 1 and
/// Phase 2 can mutate the cell, but does NOT affect the recon row —
/// the row carries the Phase 1 snapshot verbatim.
///
/// Best practice: call at quiescence for fully-comparable
/// `(shmem, ledger)` pairs. Under load, individual rows are coherent
/// `(balance, qty)` snapshots but `drift` may reflect mid-batch
/// transient states.
///
/// **Pinned by** `poc/batch-ledger/tests/recon_under_load_t1.rs`:
/// R1 (concurrent-writer torn-read absence) and R2 (post-quiescence
/// exactness).
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
            // Single atomic 128-bit load — coupled (balance, qty)
            // observation (acct-zo4t).
            let (bal, qty) = unpack_bal_qty(b.balance_qty.load(Ordering::Acquire));
            let key = ((key_hi as u128) << 64) | (key_lo as u128);
            let (account_id, period_id, currency_id, ledger_kind) = unpack_key(key);
            if period_id != 1 || currency_id != 1 || ledger_kind != 1 {
                continue;
            }
            cells.push((account_id, bal, qty));
        }
    }

    // Phase 2 (acct-dav7 #7): one SPI call instead of N. Pass cells as
    // three parallel bigint[] arrays + a LEFT JOIN against an aggregation
    // CTE over posting_lines. The N-SPI version (one query per cell) cost
    // a planner+executor round-trip × cell count; with 5001 cells in the
    // B4 bench's post-sweep state, recon was visibly slow even though
    // correctness was clean.
    //
    // Debit-positive convention: SUM(debits) - SUM(credits) for ALL
    // accounts, regardless of accounts.kind. Matches what post_batch and
    // post_batch_shmem do (accounts.balance and shmem cell store the
    // same signed value).
    //
    // Account-existence semantics preserved from the per-cell version:
    // if a shmem cell's account_id has NO row in `accounts`, the
    // ledger_balance + drift columns return NULL ("can't verify because
    // the account doesn't exist") rather than 0 ("ledger balance is
    // zero"). The `acct_exists` CTE + CASE branches enforce this.
    if cells.is_empty() {
        return TableIterator::new(Vec::new());
    }

    let account_ids: Vec<i64> = cells.iter().map(|(a, _, _)| *a).collect();
    let shmem_balances: Vec<i64> = cells.iter().map(|(_, b, _)| *b).collect();
    let shmem_qtys: Vec<i64> = cells.iter().map(|(_, _, q)| *q).collect();

    let sql = "
        WITH cells_in AS (
            SELECT account_id, shmem_balance, shmem_qty
              FROM unnest($1::bigint[], $2::bigint[], $3::bigint[])
                AS u(account_id, shmem_balance, shmem_qty)
        ),
        acct_exists AS (
            SELECT id FROM accounts WHERE id = ANY($1::bigint[])
        ),
        debits AS (
            SELECT debit_account_id, SUM(amount) AS s
              FROM posting_lines
             WHERE debit_account_id = ANY($1::bigint[])
             GROUP BY debit_account_id
        ),
        credits AS (
            SELECT credit_account_id, SUM(amount) AS s
              FROM posting_lines
             WHERE credit_account_id = ANY($1::bigint[])
             GROUP BY credit_account_id
        )
        SELECT c.account_id,
               c.shmem_balance,
               c.shmem_qty,
               CASE WHEN a.id IS NOT NULL
                    THEN (COALESCE(d.s, 0) - COALESCE(cr.s, 0))::bigint
                    ELSE NULL
               END AS ledger_balance,
               CASE WHEN a.id IS NOT NULL
                    THEN (c.shmem_balance - (COALESCE(d.s, 0) - COALESCE(cr.s, 0)))::bigint
                    ELSE NULL
               END AS drift
        FROM cells_in c
        LEFT JOIN acct_exists a ON a.id = c.account_id
        LEFT JOIN debits d      ON d.debit_account_id  = c.account_id
        LEFT JOIN credits cr    ON cr.credit_account_id = c.account_id
        ORDER BY c.account_id
    ";

    let out: Vec<(i64, i64, i64, Option<i64>, Option<i64>)> =
        pgrx::PgTryBuilder::new(|| {
            Spi::connect(|client| {
                let args: Vec<pgrx::datum::DatumWithOid> = vec![
                    account_ids.clone().into(),
                    shmem_balances.clone().into(),
                    shmem_qtys.clone().into(),
                ];
                let tup = client
                    .select(sql, None, &args)
                    .expect("recon Phase 2 SPI select");
                let mut rows = Vec::with_capacity(cells.len());
                for row in tup {
                    let a: i64 = row["account_id"].value().unwrap().unwrap();
                    let sb: i64 = row["shmem_balance"].value().unwrap().unwrap();
                    let sq: i64 = row["shmem_qty"].value().unwrap().unwrap();
                    let lb: Option<i64> = row["ledger_balance"].value().unwrap();
                    let dr: Option<i64> = row["drift"].value().unwrap();
                    rows.push((a, sb, sq, lb, dr));
                }
                rows
            })
        })
        .catch_others(|_| {
            // SPI error path: fall back to per-cell NULLs so recon still
            // emits one row per cell (matches the legacy fallback shape
            // where any per-cell SPI failure produced (None, None)).
            cells
                .iter()
                .map(|(a, sb, sq)| (*a, *sb, *sq, None::<i64>, None::<i64>))
                .collect()
        })
        .execute();

    TableIterator::new(out)
}

// ── M10.C6 (acct-plle) — panic cleanup test helpers ──────────────────
//
// These functions deliberately panic at well-defined moments so a Rust
// test can verify that pgrx's `#[pg_guard]` panic catcher:
//   1. Converts the panic into a clean SQL ERROR (no backend crash).
//   2. Releases the LWLock guard via Drop on the unwind path — i.e.,
//      subsequent operations proceed without deadlocking.
//
// They are plain `#[pg_extern]` (not `#[cfg(test)]`) because the
// extension .so is installed into a live PG and called over SQL from
// the test binary. Naming convention `ledger_test_panic_*` makes the
// test-only purpose explicit. They are NOT load-bearing for the
// non-test apply path.

/// Acquire SHARED lock, mutate the cell at `(account_id, period_id,
/// currency_id, ledger_kind)` via the same path the post-A2 commit
/// callback uses (insert if absent, fetch_add if present), then panic
/// AFTER the mutation. Verifies: (a) mutation persists (atomic write
/// already landed), (b) LWLock guard drops on unwind, (c) the SQL
/// ERROR is reported with the panic message.
#[pg_extern]
fn ledger_test_panic_after_fetch_add(
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
        // Mutate if cell exists; if absent, escalate to EXCLUSIVE so
        // the test scenario is deterministic (caller usually pre-seeds).
        if try_update_existing(&table, key, amount_delta, qty_delta).is_none() {
            drop(table);
            let table = HASH_TABLE.exclusive();
            if try_update_existing(&table, key, amount_delta, qty_delta).is_none() {
                insert_new_seeded(&table, key, None, amount_delta, qty_delta);
            }
        }
    }
    panic!(
        "ledger_test_panic_after_fetch_add: deliberate panic after mutation \
         on (account_id={account_id}, period_id={period_id})"
    );
}

/// Acquire SHARED lock, panic BEFORE any mutation. Verifies the
/// LWLock guard drops on unwind even when nothing was modified.
#[pg_extern]
fn ledger_test_panic_before_fetch_add(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
) -> i64 {
    let _key = pack_key(account_id, period_id, currency_id, ledger_kind as u8);
    let _table = HASH_TABLE.share();
    panic!(
        "ledger_test_panic_before_fetch_add: deliberate panic before any mutation \
         on (account_id={account_id})"
    );
}

/// Acquire EXCLUSIVE lock, then panic immediately. Verifies the
/// exclusive guard drops on unwind — load-bearing because if it didn't,
/// ALL subsequent applies (which take SHARED) would deadlock against
/// the leaked EXCLUSIVE.
#[pg_extern]
fn ledger_test_panic_in_exclusive(
    account_id: i64,
    period_id: i32,
    currency_id: i16,
    ledger_kind: i16,
) -> i64 {
    let _key = pack_key(account_id, period_id, currency_id, ledger_kind as u8);
    let _table = HASH_TABLE.exclusive();
    panic!(
        "ledger_test_panic_in_exclusive: deliberate panic holding EXCLUSIVE \
         on (account_id={account_id})"
    );
}

/// Wipe the table. Useful for tests and benchmarking baselines. Takes
/// the exclusive lock for the duration.
///
/// **Does NOT clear per-backend `PENDING_STACK`.** Reset wipes shmem
/// (buckets, OCCUPIED_COUNT, APPLY_SEQ, failure counters) but leaves
/// every backend's thread-local staging stack untouched. If a test
/// or benchmark interleaves `stage_apply` with `reset()` in the same
/// transaction — e.g.:
///
/// ```sql
/// BEGIN;
///   SELECT ledger_apply_balance_delta(...);  -- stages into PENDING_STACK
///   SELECT ledger_shmem_reset();             -- wipes shmem only
/// COMMIT;                                    -- xact_commit re-populates
///                                            -- from the staged delta
/// ```
///
/// the commit-time `xact_commit` callback drains `PENDING_STACK` and
/// re-creates the cell the reset just zeroed. The behaviour is
/// correct (staged work is committed) but surprises tests that
/// assume reset means "empty everything." Reset BEFORE any staging
/// in the same txn, or AFTER the txn that staged commits. Better
/// still, do reset at the start of a test fixture before any apply
/// runs.
///
/// `account_balances_rollup` (the durable SQL table) is also NOT
/// cleared by `reset()`; subsequent applies trigger M6 lazy-load
/// which seeds new cells from rollup state. Tests that want a truly
/// clean slate must `TRUNCATE account_balances_rollup` separately.
#[pg_extern]
fn ledger_shmem_reset() {
    let table = HASH_TABLE.exclusive();
    for b in table.buckets.iter() {
        b.occupied.store(0, Ordering::Relaxed);
        b.key_hi.store(0, Ordering::Relaxed);
        b.key_lo.store(0, Ordering::Relaxed);
        b.balance_qty.store(0, Ordering::Relaxed);
        b.last_seq.store(0, Ordering::Relaxed);
        b.drained_seq.store(0, Ordering::Relaxed);
    }
    OCCUPIED_COUNT.get().store(0, Ordering::Release);
    APPLY_SEQ.get().store(0, Ordering::Release);
    LEDGER_SHMEM_INSERT_FAILURES.get().store(0, Ordering::Release);
    LEDGER_DRAIN_CONSECUTIVE_FAILS.get().store(0, Ordering::Release);
    LEDGER_DRAIN_TOTAL_FAILURES.get().store(0, Ordering::Release);
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
