//! ledger-direct-c: pgrx 0.18 extension for ledger-v3.1 Path C (direct flavor).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §5.
//!
//! Exposes one SPI function — `ledger_submit_trx_c(trx_type, source_id, posted_at, lines)`
//! — which executes the full ledger work synchronously inside the caller's user-tx:
//! sorted `pool_lock` acquisition, bulk aggregate hydration, ledger-core dispatch
//! (`plan_apply_provisional` for FIFO/LIFO; strict for WAC/STD/specific), ordered bulk
//! writes. One PG transaction per submission, one fsync, caller-visible failures.
//!
//! Path C direct allocates no shared memory: every submission's work happens entirely
//! in the caller's backend, so `_PG_init` is a no-op beyond pgrx wiring. The routed
//! flavor (ledger-routed-c) is the one that needs shmem regions.
//!
//! It also hosts `ledger_staging_drain_c` (SPIKE-A / acct-0at4.11.1): the
//! staging-table alternative committer that draws pending work from a
//! `ledger_inbox` table via `FOR UPDATE SKIP LOCKED` and applies it through the
//! same ledger-core path — benchmarked against the shmem routed flavor.
//!
//! Module map:
//! - `pool_lock`        — singleton-loop optimistic FOR UPDATE (§5.1 step 2)
//! - `hydration`        — snapshot read: pool routing + aggregate + standard_cost (§5.1 steps 3-5)
//! - `bulk_write`       — UNNEST INSERT/UPSERT/DELETE helpers (§5.1 step 8)
//! - `ledger_error_map` — LedgerError → ereport!(ERROR, ...) at the SPI boundary
//! - `submit`           — `ledger_submit_trx_c` orchestration (§5.1 steps 1-9)
//! - `submit_single`    — `ledger_submit_trx_single_c` single-statement commutative variant (SPIKE-B)
//! - `drain`            — `ledger_staging_drain_c` staging-table committer (SPIKE-A)

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

pub(crate) mod drain;
pub(crate) mod ledger_error_map;
pub(crate) mod submit;
pub(crate) mod submit_single;

// ── _PG_init ────────────────────────────────────────────────────────
//
// Path C direct is shmem-free, so this hook only exists to give PG
// something to invoke when the .so loads. No GUCs, no
// shared_preload_libraries — there is no shmem to allocate.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {}

/// One-line smoke banner. Used by Phase 2 acceptance to confirm the
/// extension loads cleanly and the SPI surface is wired.
#[pg_extern]
fn ledger_direct_c_hello() -> String {
    format!(
        "ledger_direct_c {} — Path C: synchronous in-tx ledger_submit_trx_c (provisional cost)",
        env!("CARGO_PKG_VERSION"),
    )
}
