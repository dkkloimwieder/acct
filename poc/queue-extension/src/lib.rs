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
}

// ── SQL surface ─────────────────────────────────────────────────────

/// Build identifier — updated as milestones land so the acceptance gate
/// can confirm the .so the cluster loaded is the one this code shipped.
#[pg_extern]
fn poc_ledger_hello() -> &'static str {
    "poc_ledger v0.0.1 — M2.3 AvgMethod + StdMethod + snapshot helpers (acct-4d4n.6)"
}
