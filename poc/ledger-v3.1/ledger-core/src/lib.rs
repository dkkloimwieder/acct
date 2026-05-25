//! ledger-core: pure-Rust cost-ledger transformation core for ledger-v3.1 (Path C).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §8.
//!
//! No pgrx, no DB — unit-testable in isolation. The two pgrx extensions
//! (`ledger-direct-c`, `ledger-routed-c`) depend on this crate and feed it a
//! [`Snapshot`] of the touched pools, then apply the returned [`PlanResult`].
//!
//! Two entry points:
//!  - [`plan_apply`] — strict dispatcher. WAC/STD/specific run their real logic;
//!    FIFO/LIFO hit `MethodMismatch` stubs (Path C never calls strict layer math;
//!    a future recalc/close phase replaces the stubs). Exists so every PoolMethod
//!    variant typechecks.
//!  - [`plan_apply_provisional`] — the Path C hot-path dispatcher. FIFO/LIFO run
//!    provisional (aggregate-only) mode; WAC/STD/specific delegate to their strict
//!    modules. This is what `ledger_submit_trx_c` / the routed committer call.
//!
//! Numeric model (§3.0): BIGINT at 1e-6 fixed precision; the WAC running-average
//! division uses [`numeric::banker_div`] (round-half-to-even, i128 numerator).

pub mod error;
pub mod method;
pub mod numeric;
pub mod plan;
pub mod provisional;
pub mod snapshot;

// Strict per-method implementations (shared by both entry points).
//   wac      — §3.1 running-average aggregate
//   standard — §3.3 (file named `standard` to avoid `mod std` shadowing ::std)
//   specific — §3.4 strict K=1 layer
pub(crate) mod specific;
pub(crate) mod standard;
pub(crate) mod wac;

// FIFO/LIFO: MethodMismatch stubs in PoC scope (§8). Path C dispatches FIFO/LIFO
// through `provisional`, never here; recalc/close (deferred) will supply the real
// strict layer math.
pub(crate) mod fifo;
pub(crate) mod lifo;

/// Implicit fixed-point precision: 1 BIGINT unit = 1e-6 currency units (§3.0).
pub const MICRO_UNITS_PER_CURRENCY_UNIT: i64 = 1_000_000;

pub use error::LedgerError;
pub use method::{plan_apply, PoolMethod, ProvisionalBasis};
pub use numeric::banker_div;
pub use plan::{
    LineType, PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
pub use provisional::plan_apply_provisional;
pub use snapshot::{PoolStateRow, Snapshot};
