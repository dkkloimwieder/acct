//! acct-4d4n.1 (M0.1): scaffolding for the queue+committer costing PoC.
//!
//! Authoritative spec: `poc/design_research/poc-validation-spec.md`.
//!
//! ## What M0.1 covers
//!
//! - pgrx crate that builds against PG18.
//! - `_PG_init` registers the 9 GUCs from spec §1.5 with the exact
//!   defaults / ranges / reload contexts documented there.
//! - Shmem reservation wiring exercised via `pg_shmem_init!` on a
//!   placeholder atomic counter (the real sizing — `PocQueueShard`
//!   sized from GUCs — lands in M1.1, acct-4d4n.2).
//! - One `#[pg_extern]` function `poc_ledger_hello()` that returns
//!   a build identifier, used to confirm SQL surface works.
//!
//! ## What M0.1 deliberately does NOT do
//!
//! - No PocQueueShard struct (M1.1).
//! - No committer election or batch drain (M1.2).
//! - No cost methods (M2.x).
//! - No bgworker, no recovery worker, no XactCallbacks (later milestones).
//! - GUC values are visible via `pg_settings` but nothing reads them yet.
//!
//! ## GUC namespace
//!
//! All GUCs live under `poc_ledger.*`. The extension's shared library
//! name (per `Cargo.toml [lib] name`) is also `poc_ledger`, so the
//! same identifier appears in `shared_preload_libraries`.

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PgAtomic, pg_shmem_init};
use std::ffi::CString;
use std::sync::atomic::AtomicU64;

pgrx::pg_module_magic!();

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

// ── Shmem placeholder ───────────────────────────────────────────────
//
// pg_shmem_init! wires up a shmem segment via PG's RequestAddinShmemSpace
// + ShmemInitStruct mechanism. M0.1 reserves a single AtomicU64 just to
// validate the path; M1.1 (acct-4d4n.2) replaces this with the real
// PocQueueShard array sized from SHARD_COUNT.

static PLACEHOLDER_HEARTBEAT: PgAtomic<AtomicU64> =
    unsafe { PgAtomic::new(c"poc_ledger_placeholder_heartbeat") };

// ── _PG_init ────────────────────────────────────────────────────────

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pg_shmem_init!(PLACEHOLDER_HEARTBEAT);

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
}

// ── SQL surface ─────────────────────────────────────────────────────

/// Build identifier for the scaffolding milestone. Lets the acceptance
/// gate confirm the extension is actually loaded vs the SQL coming back
/// from some other source.
#[pg_extern]
fn poc_ledger_hello() -> &'static str {
    "poc_ledger v0.0.1 — M0.1 scaffolding (acct-4d4n.1)"
}
