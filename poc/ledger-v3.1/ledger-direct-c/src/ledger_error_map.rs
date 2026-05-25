//! Map [`ledger_core::LedgerError`] to PG `ereport!(ERROR, ...)`.
//!
//! Called by `submit::ledger_submit_trx_c` at §5.1 step 7. Raising ereport! at
//! the SPI boundary aborts the caller's user-tx and the SQLSTATE surfaces to the
//! client.
//!
//! SQLSTATE assignment:
//!   InsufficientInventory  → INTEGRITY_CONSTRAINT_VIOLATION (23000) — cannot
//!     deplete more than the aggregate holds (§3.6, no negative inventory).
//!   MethodMismatch         → DATA_EXCEPTION (22000) — a FIFO/LIFO pool reached
//!     strict layer math; Path C must route those through plan_apply_provisional.
//!   UnknownPool            → UNDEFINED_OBJECT (42704) — line references a pool
//!     that wasn't hydrated.
//!   MissingStandardCost    → DATA_EXCEPTION (22000) — STD / 'standard'-basis pool
//!     with no standard_cost row (§3.3).
//!   MissingVarianceAccount → DATA_EXCEPTION (22000) — STD receipt with
//!     actual != standard but no variance account supplied on the line (§3.3).
//!   Overflow               → NUMERIC_VALUE_OUT_OF_RANGE (22003) — BIGINT
//!     arithmetic overflowed during the running-average math (§3.0).
//!
//! `raise_ledger_error` longjmps out via ereport!; its expansion is typed `()`,
//! so the signature returns `()` and the caller treats the call as a return point.

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
                "Reduce the depletion qty, split it across submissions, or receive \
                 more inventory into the pool before retrying."
            );
        }
        LedgerError::MethodMismatch { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "A FIFO/LIFO pool reached strict layer math. Path C must dispatch \
                 FIFO/LIFO through plan_apply_provisional — likely a dispatch bug."
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
                "STD or 'standard'-basis pool requires a standard_cost row for its \
                 (sku_id, location_id). Insert one and retry."
            );
        }
        LedgerError::MissingVarianceAccount { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "An STD receipt whose actual cost differs from standard needs a \
                 variance_account on the line to absorb the purchase-price variance."
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
