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
//! Path C direct allocates no shared memory. Scaffold stub (P1.1, acct-2ttr.1);
//! `ledger_submit_trx_c` and its helper modules (pool_lock, hydration, bulk_write,
//! ledger_error_map, submit) land in P2 (acct-2ttr.3).

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

/// Scaffold smoke function; replaced by the real SPI surface in P2.
#[pg_extern]
fn ledger_direct_c_scaffold_version() -> &'static str {
    "ledger-v3.1 direct-c scaffold (P1.1)"
}
