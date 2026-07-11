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
//! - `ledger_recalc_step()` — one recalc-engine worker tick (design-v3.2 §5):
//!   claims ONE dirty pool from `recalc_queue` FOR UPDATE SKIP LOCKED, replays
//!   its physical stream in R-1 `(posted_at, id)` order through the
//!   ledger-core strict FIFO/LIFO fold (incremental from the persisted layer
//!   checkpoint; full-opening on a recost floor), and writes the
//!   generation-keyed settlement + GL cost adjustments in the same
//!   transaction. N looping connections = N workers (continuous cadence).
//!
//! - `ledger_close_period(period_id, actor, force)` — the orchestrated period
//!   close (design-v3.2 §5e / recalc-e): recalc-drain gate on the period-scoped
//!   G2a, synchronous drain (forced close drains, never skips), the
//!   variance-into-empty-pool residue sweep that makes each pool's aggregate
//!   `value_sum` exactly equal its authoritative open-layer value, and the
//!   immutable finalize stamp (`state → closed`; the 0017 triggers make the
//!   period lock a schema invariant; PeriodClosed = SQLSTATE 55000).
//!
//! - `ledger_settle_pool(pool_id)` — force-drain ONE pool to its stream head
//!   synchronously (the on-demand worker shape; recalc-e §7).
//!
//! The extension allocates no shared memory: every submission's work happens in
//! the calling backend, so `_PG_init` is a no-op beyond pgrx wiring.
//!
//! Module map:
//! - `cte`              — the four commutative single-statement CTEs (kept plans)
//! - `submit`           — `ledger_submit_trx` + the shared `apply_submission`
//! - `drain`            — `ledger_staging_drain`
//! - `recalc`           — `ledger_recalc_step` + the claimed-pass machinery
//! - `close`            — `ledger_close_period` / `ledger_settle_pool`
//! - `ledger_error_map` — LedgerError → ereport!(ERROR, ...) at the SPI boundary

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

pub(crate) mod close;
pub(crate) mod cte;
pub(crate) mod drain;
pub(crate) mod ledger_error_map;
pub(crate) mod recalc;
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
