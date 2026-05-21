//! ledger-core: pure-Rust cost-ledger transformation core for ledger-v3.
//!
//! Authoritative spec: `poc/design_research/design-v3.md`.
//!
//! The single public entry point is [`plan_apply`], shared by both execution
//! paths (ledger-direct / ledger-routed). It walks a `&[TrxLineRequest]` over a
//! mutable [`Snapshot`] and returns a [`PlanResult`] of trx_line outputs,
//! pool_state mutations, and posting_line requests. The caller's bulk-write
//! step turns these into UNNEST INSERT/UPSERT/UPDATE/DELETE statements in FK
//! order per design-v3 §4.2 step 7 / §5.4 step 9.
//!
//! Pristine-snapshot replay (design-v3 §7) is the caller's responsibility —
//! see `method.rs` for rationale.

pub mod error;
pub mod method;
pub mod plan;
pub mod snapshot;

// Per-method implementations. Empty stubs today; filled in by:
//   fifo     — acct-ddnu
//   lifo     — acct-stvf
//   wac      — acct-evpw  (load-bearing §3.1 divide-by-zero guard lives here)
//   standard — acct-ll79  (file named `standard` to avoid shadowing ::std)
//   specific — acct-qnpq
//
// Dispatched from `method::plan_apply` (wired by acct-nmlc).
pub(crate) mod fifo;
pub(crate) mod lifo;
pub(crate) mod specific;
pub(crate) mod standard;
pub(crate) mod wac;

// Shared logic for FIFO / LIFO / specific-id (layered methods).
pub(crate) mod layered;

// Per-pool monotonic trx_seq counter helper. Used by the per-method modules.
pub(crate) mod seq;

pub use error::LedgerError;
pub use method::{plan_apply, PoolMethod};
pub use plan::{
    LineType, PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
pub use snapshot::{PoolStateRow, Snapshot};
