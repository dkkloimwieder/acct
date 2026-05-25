//! ledger-core: pure-Rust cost-ledger transformation core for ledger-v3.1 (Path C).
//!
//! Authoritative spec: `poc/design_research/design-v3.1.md` §8.
//!
//! No pgrx, no DB — unit-testable in isolation. Both pgrx extensions
//! (`ledger-direct-c`, `ledger-routed-c`) depend on this crate and invoke the same
//! `plan_apply` / `plan_apply_provisional` functions over the same `Snapshot` types.
//!
//! Scaffold stub (P1.1, acct-2ttr.1). The real modules land in P1.2 (acct-2ttr.2):
//!   - numeric.rs      — banker_div (round-half-to-even) + precision constants
//!   - snapshot.rs     — Snapshot (HashMap<pool_id, PoolStateRows>)
//!   - plan.rs         — PlanResult (trx_line, pool_state mutations, posting_line)
//!   - error.rs        — LedgerError
//!   - method.rs       — PoolMethod enum + plan_apply dispatcher
//!   - wac.rs / std.rs / specific.rs — strict implementations
//!   - provisional.rs  — plan_apply_provisional for FIFO/LIFO (aggregate-only)
//!   - fifo.rs / lifo.rs — MethodMismatch stubs (recalc/close replaces them later)

/// Implicit fixed-point precision: 1 BIGINT unit = 1e-6 currency units (design-v3.1 §3.0).
pub const MICRO_UNITS_PER_CURRENCY_UNIT: i64 = 1_000_000;
