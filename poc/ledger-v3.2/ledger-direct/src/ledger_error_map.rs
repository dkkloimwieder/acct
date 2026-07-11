//! Map [`ledger_core::LedgerError`] to PG `ereport!(ERROR, ...)`.
//!
//! Raising ereport! at the SPI boundary aborts the caller's user-tx and the
//! SQLSTATE surfaces to the client.
//!
//! SQLSTATE assignment:
//!   InsufficientInventory  → INTEGRITY_CONSTRAINT_VIOLATION (23000) — a
//!     strict-method depletion (WAC/STD/specific) exceeds on-hand; the
//!     commutative WAC gate raises the same SQLSTATE from submit.rs.
//!   MethodMismatch         → DATA_EXCEPTION (22000) — a FIFO/LIFO pool reached
//!     plan_apply; their costing belongs to the recalc engine (dispatch bug).
//!   UnknownPool            → UNDEFINED_OBJECT (42704) — line references a pool
//!     with no `pool` row.
//!   MissingStandardCost    → DATA_EXCEPTION (22000) — STD pool (or a
//!     'standard'-basis FIFO/LIFO depletion) with no standard_cost row (§3.3).
//!   MissingPostingAccounts → DATA_EXCEPTION (22000) — touched pool's (sku_id,
//!     location_id) has no posting_account_map row (§3.7/§4).
//!   MissingVarianceAccount → DATA_EXCEPTION (22000) — STD receipt with
//!     actual != standard but the pool's posting_account_map.variance_acct is NULL (§3.3).
//!   SpecificPoolOccupied   → INTEGRITY_CONSTRAINT_VIOLATION (23000) — a second
//!     receipt to a specific pool that already holds a unit; K=1 (§3.4).
//!   Overflow               → NUMERIC_VALUE_OUT_OF_RANGE (22003) — BIGINT
//!     arithmetic overflowed during plan math (§3.0).
//!
//! `raise_ledger_error` longjmps out via ereport!; its expansion is typed `()`,
//! so the signature returns `()` and callers follow it with `unreachable!()`.

use ledger_core::LedgerError;
use pgrx::prelude::*;

pub fn raise_ledger_error(err: LedgerError) {
    let msg = err.to_string();
    match err {
        LedgerError::InsufficientInventory { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION,
                msg,
                "Strict methods (WAC/STD/specific) gate depletions on on-hand qty. \
                 Reduce the qty, receive more inventory first, or use an alt-C \
                 FIFO/LIFO pool if negative-on-hand recording is intended."
            );
        }
        LedgerError::MethodMismatch { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "A FIFO/LIFO pool reached the strict plan_apply dispatcher. Their \
                 hot path is the commutative append; costing is the recalc \
                 engine's — likely a dispatch bug."
            );
        }
        LedgerError::UnknownPool(_) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                msg,
                "The submission referenced a pool_id with no row in `pool`. Create \
                 the pool before submitting lines against it."
            );
        }
        LedgerError::MissingStandardCost { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "STD pools (and 'standard'-basis FIFO/LIFO depletions) require a \
                 standard_cost row for the pool's (sku_id, location_id). Insert one and retry."
            );
        }
        LedgerError::MissingPostingAccounts { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "Every touched pool's (sku_id, location_id) needs a posting_account_map \
                 row resolving debit/credit (and the STD variance) accounts. Insert one and retry."
            );
        }
        LedgerError::MissingVarianceAccount { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "An STD receipt whose actual cost differs from standard needs \
                 posting_account_map.variance_acct set for the pool's (sku_id, location_id)."
            );
        }
        LedgerError::SpecificPoolOccupied { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION,
                msg,
                "A specific-method pool is K=1 and already holds a unit. Deplete it \
                 before re-stocking, or use a distinct identity_key (a separate pool)."
            );
        }
        LedgerError::Overflow { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                msg,
                "BIGINT arithmetic overflowed inside plan_apply. Reduce qty or \
                 unit_cost magnitudes."
            );
        }
    }
}
