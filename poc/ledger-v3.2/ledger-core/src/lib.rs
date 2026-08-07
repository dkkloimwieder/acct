//! ledger-core: pure-Rust cost-ledger transformation core for ledger-v3.2.
//!
//! Authoritative spec: `poc/design_research/design-v3.2.md` (§1 carries the crate
//! over from design-v3.1 §8; §3 fixes the hot-path shapes; §5 decomposes the
//! recalc engine).
//!
//! No pgrx, no DB — unit-testable in isolation. SPI-side callers feed it a
//! [`Snapshot`] of the touched pools and apply the returned [`PlanResult`].
//!
//! Hot-path entry point: [`plan_apply`], the strict dispatcher. WAC/STD/
//! specific produce their final cost directly on the hot path and need no
//! reconciliation (design-v3.2 §1). FIFO/LIFO never dispatch there — under the
//! alt-C posture (design-v3.1 §16) their hot path is an insert-only append and
//! the recalc engine is their sole costing plane; dispatching them raises
//! `MethodMismatch`. Their strict layer-walk is [`fifo::strict_fold`] /
//! [`lifo::strict_fold`] (module `strict`), run only by the recalc engine
//! (design-v3.2 §5 child (a)).
//!
//! Numeric model (design-v3.1 §3.0): BIGINT at 1e-6 fixed precision; the WAC
//! running-average division uses [`numeric::banker_div`] (round-half-to-even,
//! i128 numerator).

pub mod error;
pub mod method;
pub mod numeric;
pub mod plan;
pub mod snapshot;
pub mod strict;

// Strict per-method implementations.
//   wac      — design-v3.1 §3.1 running-average aggregate
//   standard — design-v3.1 §3.3 (file named `standard` to avoid `mod std` shadowing ::std)
//   specific — design-v3.1 §3.4 strict K=1 layer
pub(crate) mod specific;
pub(crate) mod standard;
pub(crate) mod wac;

// FIFO/LIFO: the recalc engine's strict layer-walk (`strict_fold`, fed in R-1
// order) plus fail-loud hot-path guards — the hot path never dispatches
// FIFO/LIFO here (design-v3.2 §5).
pub mod fifo;
pub mod lifo;

/// Implicit fixed-point precision: 1 BIGINT unit = 1e-6 currency units (design-v3.1 §3.0).
pub const MICRO_UNITS_PER_CURRENCY_UNIT: i64 = 1_000_000;

pub use error::LedgerError;
pub use method::{plan_apply, PoolMethod, ProvisionalBasis};
pub use numeric::banker_div;
pub use plan::{
    LineType, PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
pub use snapshot::{PoolStateRow, PostingAccounts, Snapshot};
pub use strict::{DepletionCosting, LayerDraw, StrictEvent, StrictFoldOutput, StrictLayer};
