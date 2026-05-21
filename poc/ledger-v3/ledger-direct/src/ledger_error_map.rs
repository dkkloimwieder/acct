//! Map [`ledger_core::LedgerError`] to PG `ereport!(ERROR, ...)`.
//!
//! Called by `submit::ledger_submit_trx` (acct-v4xz) at step 6 of the
//! 8-step pipeline (design-v3 §4.2 step 6). Raising ereport! at the SPI
//! boundary aborts the caller's user-tx and the SQLSTATE surfaces to
//! the client.
//!
//! SQLSTATE assignment per acct-d74b:
//!   InsufficientInventory  → INTEGRITY_CONSTRAINT_VIOLATION (23000) —
//!     "ledger invariant: cannot deplete more than is on hand."
//!   MethodMismatch         → DATA_EXCEPTION (22000) —
//!     "snapshot's method_of disagrees with the per-method apply_X."
//!   UnknownPool            → UNDEFINED_OBJECT (42704) —
//!     "line references a pool_id that wasn't hydrated."
//!   MissingStandardCost    → DATA_EXCEPTION (22000) —
//!     "STD pool needs a standard cost; none in scope for sku+location."
//!   Overflow               → NUMERIC_VALUE_OUT_OF_RANGE (22003) —
//!     "BIGINT arithmetic overflowed during weighted-average or qty math."
//!
//! `raise_ledger_error` does not return — `ereport!(ERROR, ...)` longjmps
//! out of the SPI invocation back to PG's transaction-abort handler — but
//! the macro's expansion is typed `()`, so the function signature returns
//! `()` and the caller treats the call as a return point.

use ledger_core::LedgerError;
use pgrx::prelude::*;

#[allow(dead_code)] // wired by submit::ledger_submit_trx (acct-v4xz)
pub fn raise_ledger_error(err: LedgerError) {
    let msg = err.to_string();
    match err {
        LedgerError::InsufficientInventory { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION,
                msg,
                "Reduce the depletion qty, split it across multiple submissions, \
                 or receive more inventory into the pool before retrying."
            );
        }
        LedgerError::MethodMismatch { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "Snapshot hydration returned a method that disagrees with the \
                 per-method apply_X branch — likely a hydration bug."
            );
        }
        LedgerError::UnknownPool(_) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
                msg,
                "The submission referenced a pool_id that was not hydrated. \
                 Caller must populate Snapshot.method_of for every pool_id \
                 that appears in `lines`."
            );
        }
        LedgerError::MissingStandardCost { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "STD-method pool requires a current standard_costs entry for \
                 (sku_id, location_id). Insert one effective on or before \
                 posted_at and retry."
            );
        }
        LedgerError::Overflow { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                msg,
                "BIGINT arithmetic overflowed inside plan_apply. Inputs are \
                 likely outside the supported range for this method's running \
                 totals; reduce qty or unit_cost magnitudes."
            );
        }
    }
}
