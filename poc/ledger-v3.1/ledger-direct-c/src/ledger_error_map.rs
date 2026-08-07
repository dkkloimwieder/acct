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
//!   MissingPostingAccounts → DATA_EXCEPTION (22000) — touched pool's (sku_id,
//!     location_id) has no posting_account_map row (§3.7).
//!   MissingVarianceAccount → DATA_EXCEPTION (22000) — STD receipt with
//!     actual != standard but the pool's posting_account_map.variance_acct is NULL (§3.3).
//!   SpecificPoolOccupied   → INTEGRITY_CONSTRAINT_VIOLATION (23000) — a second
//!     receipt to a specific pool that already holds a unit; K=1 (§3.4).
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
        LedgerError::MissingPostingAccounts { .. } => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                msg,
                "Every pool's (sku_id, location_id) needs a posting_account_map row \
                 resolving debit/credit (and the STD variance) accounts. Insert one and retry."
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
