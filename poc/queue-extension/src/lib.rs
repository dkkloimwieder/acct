//! poc_ledger: queue+committer costing ledger PoC primitive.
//!
//! Authoritative spec: `poc/design_research/poc-validation-spec.md`.
//!
//! ## Milestone status
//!
//! - M0.1 (acct-4d4n.1, shipped @ b8ddacb): pgrx scaffolding, 9 GUCs
//!   from spec §1.5, placeholder shmem, `poc_ledger_hello()` SQL fn.
//! - **M1.1 (acct-4d4n.2, this commit): single-shard primitive — see
//!   `queue` module for the data structures and slot/ring algorithms;
//!   spec §1.4 (shmem layout) and §1.6 step 5 + steps 6-9, 12-14.**
//!
//! ## GUC namespace
//!
//! All GUCs live under `poc_ledger.*`. The extension's shared library
//! name (per `Cargo.toml [lib] name`) is also `poc_ledger`, so the
//! same identifier appears in `shared_preload_libraries`.

#![allow(unexpected_cfgs)]

use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

pgrx::pg_module_magic!();

// M1.1 — single-shard queue primitive (PocQueueShard, ring, slot pool).
mod queue;

// M2.1 — PocCostMethod trait + types + MockMethod + cost-table schema.
mod cost_method;

// M2.2 — FifoMethod (walks PocSnapshot.layers; emits per-layer depletions).
mod fifo;

// M2.3 — AvgMethod + StdMethod (running aggregate / standard-cost lookup).
mod avg;
mod std_cost;

// M1.2 + M2.1 — committer election, batch drain, dedup-lookup, dispatch.
mod committer;

// ── GUCs (spec §1.5) ────────────────────────────────────────────────
//
// Mapping from the spec's table to GucSetting + GucRegistry calls below.
// Defaults, ranges, and reload contexts are copied verbatim from §1.5;
// any divergence is a spec violation and should be fixed in either
// place before further milestones build on the values.

static SHARD_COUNT: GucSetting<i32> = GucSetting::<i32>::new(256);
static REQUESTS_PER_SHARD: GucSetting<i32> = GucSetting::<i32>::new(4096);
static SLOTS_PER_SHARD: GucSetting<i32> = GucSetting::<i32>::new(4096);
static SPILLOVER_ARENA_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static BATCH_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
static BATCH_SIZE_MAX: GucSetting<i32> = GucSetting::<i32>::new(1024);
static COMMITTER_LEASE_MS: GucSetting<i32> = GucSetting::<i32>::new(100);
static QUEUE_FULL_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);

// `semantics` is a string GUC. Enum-of-{compensation, reservation}
// enforcement isn't done at registration time (pgrx 0.16 doesn't expose
// the PG `check_hook` plumbing cleanly); callers that read it will
// validate. Spec §1.5 documents the allowed values.
static SEMANTICS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"compensation"));

// M3.2 (acct-4d4n.8) Q-A switch. Values: 'none' (default — committer
// relies on row-level FOR UPDATE inside the snapshot builders) /
// 'pool_locks' (FOR UPDATE on `poc_pool_locks` row with audit column) /
// 'pool_lock_anchors' (FOR UPDATE on minimal `poc_pool_lock_anchors`
// row). Read via `pool_lock_mode_str()` from committer hot path.
static POOL_LOCK_MODE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"none"));

/// Read the current `poc_ledger.pool_lock_mode` GUC value, lowercased.
/// Returns "none" if unset or unparseable.
pub(crate) fn pool_lock_mode_str() -> String {
    POOL_LOCK_MODE
        .get()
        .as_ref()
        .map(|c| c.to_string_lossy().to_string())
        .unwrap_or_else(|| "none".to_string())
}

// M5b.1 (acct-4d4n.12): Database the startup-recovery bgworker
// connects to via SPI to scan durable cost tables. Postmaster-scope
// because the worker is registered at _PG_init and reads the value
// once when it starts.
static RECOVERY_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"acct_poc_queue"));

/// Read the recovery worker's target database name. Returns the
/// default ("acct_poc_queue") if unset.
pub(crate) fn recovery_database_str() -> String {
    RECOVERY_DATABASE
        .get()
        .as_ref()
        .map(|c| c.to_string_lossy().to_string())
        .unwrap_or_else(|| "acct_poc_queue".to_string())
}

// ── M5b.2 (acct-4d4n.13) lease false-positive protection ────────────
//
// Failure mode (spec §3.6): a legitimately slow committer whose per-
// batch wall time exceeds `committer_lease_ms` gets its lease stolen
// by a contender that observed lease expiry. Result: two committers
// race on the same shard, one drains a partial batch, the slot-fill
// CAS path produces log floods, and the stale committer's INSERTs
// either collide on dedup or are silently overwritten by the
// takeover's snapshot read.
//
// Protection in place (M5a.1):
//
//   try_acquire_or_takeover ALWAYS calls pg_pid_alive(committer_pid)
//   before the takeover CAS. Alive committer → returns Held; takeover
//   does NOT happen. This is the load-bearing guard.
//
// Operator guidance — `committer_lease_ms` ↔ `batch_size_max`:
//
//   The two GUCs must be sized together. Pick `batch_size_max` so
//   that a single drain's worst-case wall time stays well inside
//   `committer_lease_ms`. Rough budget:
//
//     per_batch_wall_time ≈ batch_size_max × per_event_SPI_us
//
//   With the M2 SPI shape (~15-25 µs per consumption/depletion row at
//   the default isolation), the default `batch_size_max=1024` and
//   `committer_lease_ms=100` give:
//
//     1024 × 20 µs = 20.5 ms per batch
//     20.5 ms ≪ 100 ms lease     → ~5× safety margin
//
//   If you raise `batch_size_max` you should raise `committer_lease_ms`
//   proportionally. The pg_pid_alive guard makes lease expiry
//   harmless on a healthy cluster (alive committer = no takeover), but
//   the contender's WaitLatch wake/back-off loop still spins every
//   ~100 ms during a long drain; tuning the lease keeps that loop
//   from flapping.
//
//   Failure surface if you misconfigure: lease_ms ≪ per-batch
//   wall time → contender wakes, observes lease expired, pg_pid_alive
//   reports alive, returns Held, sleeps 100 ms, repeats. No
//   correctness violation, only wasted CPU on the contender. The
//   committer completes its batch and signals the contender's slot
//   normally.
//
// `drain_sleep_us` is a TEST-ONLY switch that deterministically
// extends a single drain's wall time past `committer_lease_ms` so the
// slow-committer scenario can be reproduced in a bench/test harness.
// Production deployments leave it at 0. Userset-scope (PGC_USERSET)
// so bench scripts can SET it per-session, matching how
// `pool_lock_mode` works for the M3.2 fan-in bench; Sighup would
// require an ALTER SYSTEM + pg_reload_conf round-trip and apply
// cluster-wide, which is the wrong shape for a test helper.
static DRAIN_SLEEP_US: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Read the test-only drain-sleep GUC (microseconds). Returns 0 (the
/// default — no sleep) on parse failure or unset.
pub(crate) fn drain_sleep_us_now() -> i32 {
    DRAIN_SLEEP_US.get()
}

// ── _PG_init ────────────────────────────────────────────────────────

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    queue::init();

    GucRegistry::define_int_guc(
        c"poc_ledger.shard_count",
        c"Number of queue shards (power of two)",
        c"Total queue shards across which (sku_id, location_id) keys hash. Must be a power of two; M1.1 will enforce at startup.",
        &SHARD_COUNT,
        16,
        4096,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.requests_per_shard",
        c"Pending-request ring buffer size per shard",
        c"Capacity of the per-shard ring buffer that holds pending apply requests awaiting committer drain.",
        &REQUESTS_PER_SHARD,
        256,
        65536,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.slots_per_shard",
        c"Result-slot pool size per shard",
        c"Capacity of the per-shard result-slot pool. Callers acquire a slot via atomic fetch_add + CAS state machine before pushing a request.",
        &SLOTS_PER_SHARD,
        256,
        65536,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.spillover_arena_mb",
        c"Spillover arena size in MB (per-shard? — global; see spec §1.4)",
        c"Arena for result rows whose depletion count exceeds the inline ResultSlot capacity (32). Block size and allocation policy are PoC implementation details.",
        &SPILLOVER_ARENA_MB,
        1,
        2048,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.batch_window_us",
        c"Committer batch window in microseconds",
        c"How long the committer waits to coalesce requests before draining a batch. Trades latency for batch size.",
        &BATCH_WINDOW_US,
        50,
        50_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.batch_size_max",
        c"Maximum requests per committer batch",
        c"Hard cap on requests drained in one committer batch. Larger batches amortize sub-tx + WAL costs; smaller batches keep per-batch wall-time below committer_lease_ms.",
        &BATCH_SIZE_MAX,
        16,
        65_536,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.committer_lease_ms",
        c"Committer lease duration in milliseconds",
        c"Time a committer's CAS-acquired lease stays valid before a contender may attempt takeover (after pg_pid_alive verification, M5b.2).",
        &COMMITTER_LEASE_MS,
        10,
        10_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.queue_full_timeout_ms",
        c"Backpressure timeout in milliseconds",
        c"When the per-shard ring is full, push blocks on a condition variable up to this duration before returning queue_full to the caller (M5c.1).",
        &QUEUE_FULL_TIMEOUT_MS,
        100,
        60_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        c"poc_ledger.semantics",
        c"Apply semantics: compensation (default) or reservation",
        c"Primary PoC bake-off surface is compensation. Reservation is filed as standalone follow-up acct-7fom (M7); not exercised in the M9 bake-off.",
        &SEMANTICS,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        c"poc_ledger.pool_lock_mode",
        c"Pool-lock granularity (M3.2 Q-A): none | pool_locks | pool_lock_anchors",
        c"Selects how the committer serializes committer-to-committer work on the same (sku_id, location_id) pool. 'none' relies on the existing FOR UPDATE inside the per-method snapshot builders. 'pool_locks' takes a row lock on `poc_pool_locks` (audit-column flavour) at the top of process_group. 'pool_lock_anchors' takes a row lock on the minimal `poc_pool_lock_anchors` table. M3.2 benches all three under fan_in. Userset so the bench harness can SET it per psql session.",
        &POOL_LOCK_MODE,
        GucContext::Userset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        c"poc_ledger.recovery_database",
        c"Database the startup-recovery bgworker connects to (M5b.1)",
        c"On postmaster startup the recovery worker connects to this database via SPI, scans the cost tables for per-shard max(committer_tx_id) and seeds the shmem counters so post-restart applies cannot collide with pre-restart durable rows. Must be the database where the poc_ledger extension lives.",
        &RECOVERY_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        c"poc_ledger.drain_sleep_us",
        c"TEST-ONLY: extend committer drain wall time by this many microseconds (M5b.2)",
        c"Test-only switch for reproducing the spec §3.6 slow-committer scenario. When > 0, the committer's drain_and_commit path sleeps this many microseconds after draining the ring but before per-event INSERTs. Combined with a small committer_lease_ms it lets a bench harness verify that a live-but-slow committer is NOT preempted by a contender (the pg_pid_alive guard in try_acquire_or_takeover returns Held). Production deployments leave at 0.",
        &DRAIN_SLEEP_US,
        0,
        10_000_000,
        GucContext::Userset,
        GucFlags::empty(),
    );

    // M5b.1 (acct-4d4n.12): one-shot startup recovery worker.
    // `restart_time = None` means PG does NOT relaunch after the worker
    // exits — recovery runs once per postmaster start.
    BackgroundWorkerBuilder::new("poc_ledger_startup_recovery")
        .set_function("poc_ledger_startup_recovery_main")
        .set_library("poc_ledger")
        .set_argument(None)
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .set_restart_time(None)
        .enable_spi_access()
        .load();
}

// ── SQL surface ─────────────────────────────────────────────────────

/// Build identifier — updated as milestones land so the acceptance gate
/// can confirm the .so the cluster loaded is the one this code shipped.
#[pg_extern]
fn poc_ledger_hello() -> &'static str {
    "poc_ledger v0.0.1 — M5b.2 lease false-positive protection + slow-committer mitigation (acct-4d4n.13)"
}
