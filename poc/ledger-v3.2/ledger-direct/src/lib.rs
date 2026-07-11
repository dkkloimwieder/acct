//! ledger-direct: pgrx 0.18 extension for the ledger-v3.2 hot path.
//!
//! Authoritative spec: `poc/design_research/design-v3.2.md` §3 (hot-path
//! semantics) / §4 (SPI surface).
//!
//! Two SPI entry points:
//!
//! - `ledger_submit_trx(trx_type, source_id, posted_at, lines)` — the direct
//!   path. Callers send inventory facts only (`pool_id, line_type, qty,
//!   unit_cost`); the ledger resolves GL accounts from `posting_account_map`
//!   (§3.7). Dispatch is per pool method:
//!     - WAC — SPIKE-B single-statement commutative CTEs (qty gate in the
//!       UPDATE's WHERE, running average via PG 18 `RETURNING old`, cost leg
//!       posted). No `pool_lock`, no Rust round-trip.
//!     - FIFO / LIFO — alt-C appends (design-v3.1 §16): record the physical
//!       event with an observed cost, fire the commutative aggregate delta
//!       unconditionally (negative qty allowed — a flagged signal), post NO
//!       cost leg. The recalc engine is their sole costing plane.
//!     - STD / specific — the ledger-core dispatch (stateful / multi-leg):
//!       pool-row locks, hydrate, `plan_apply`, bulk write.
//!
//! - `ledger_staging_drain(limit)` — the SPIKE-A staging-table committer:
//!   claims pending `ledger_inbox` rows FOR UPDATE SKIP LOCKED in id order and
//!   applies each through the same dispatch inside a per-submission
//!   subtransaction (failures mark the row 'failed' and roll back only that
//!   submission).
//!
//! The extension allocates no shared memory: every submission's work happens in
//! the calling backend, so `_PG_init` is a no-op beyond pgrx wiring.
//!
//! Module map:
//! - `cte`              — the four commutative single-statement CTEs (kept plans)
//! - `submit`           — `ledger_submit_trx` + the shared `apply_submission`
//! - `drain`            — `ledger_staging_drain`
//! - `ledger_error_map` — LedgerError → ereport!(ERROR, ...) at the SPI boundary

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

pub(crate) mod cte;
pub(crate) mod drain;
pub(crate) mod ledger_error_map;
pub(crate) mod submit;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {}

/// One-line smoke banner: confirms the extension loads and the SPI surface is
/// wired.
#[pg_extern]
fn ledger_direct_hello() -> String {
    format!(
        "ledger_direct {} — v3.2 hot path: ledger_submit_trx (per-method dispatch) + ledger_staging_drain",
        env!("CARGO_PKG_VERSION"),
    )
}
