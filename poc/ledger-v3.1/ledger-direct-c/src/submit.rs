//! Path C direct SPI entry point: `ledger_submit_trx_c` (design-v3.1 §5.1).
//!
//! Executes the §5.1 pipeline synchronously inside the caller's user-tx:
//!   1. compute touched pool_ids (sort + dedup)
//!   2. acquire pool_lock FOR UPDATE in ascending order (optimistic, §5.1 step 2)
//!   3-5. hydrate Snapshot from pool / pool_state / standard_cost
//!   6. ledger_core::plan_apply_provisional (FIFO/LIFO provisional; WAC/STD/specific strict)
//!   7. on Err: raise_ledger_error → ereport!(ERROR, ...) → caller's tx aborts
//!   8. bulk-write trx, trx_line, pool_state mutations, posting_line
//!   9. return trx.id

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use ledger_core::{plan_apply_provisional, LineType, TrxLineRequest};
use pgrx::prelude::*;
use serde::Deserialize;

use crate::bulk_write;
use crate::hydration;
use crate::ledger_error_map;
use crate::pool_lock;

/// JSONB element shape for `lines[]`. Mirror of `TrxLineRequest` with a
/// stringly-typed line_type — the caller sends the SQL enum text and we decode
/// it here. `variance_account` is optional (only STD receipts with a non-zero
/// purchase-price variance require it, §3.3).
#[derive(Deserialize)]
struct LineJson {
    pool_id: i64,
    line_type: String,
    #[serde(default)]
    source_id: Option<i64>,
    qty: i64,
    unit_cost: i64,
    debit_account: i64,
    credit_account: i64,
    #[serde(default)]
    variance_account: Option<i64>,
}

fn decode_line_type(s: &str) -> Option<LineType> {
    match s {
        "po_receipt_line" => Some(LineType::PoReceiptLine),
        "wo_output" => Some(LineType::WoOutput),
        "wo_backflush" => Some(LineType::WoBackflush),
        "wo_scrap" => Some(LineType::WoScrap),
        "inv_adjustment_line" => Some(LineType::InvAdjustmentLine),
        "transfer_shipment_line" => Some(LineType::TransferShipmentLine),
        "transfer_receipt_line" => Some(LineType::TransferReceiptLine),
        "manual_adjustment_line" => Some(LineType::ManualAdjustmentLine),
        "revaluation_line" => Some(LineType::RevaluationLine),
        _ => None,
    }
}

/// Caller-facing SPI: submit one transaction (Path C direct flavor).
///
/// `trx_type` is the SQL trx_type enum text (po_receipt, transfer_shipment, …).
/// `posted_at` is RFC3339 text (e.g. `'2026-05-25T12:00:00+00:00'`).
/// `lines` is a JSONB array of `LineJson` objects.
///
/// Returns the new trx.id. Raises ereport!(ERROR, ...) on any LedgerError or
/// argument-validation failure; the caller's user-tx then aborts.
#[pg_extern]
fn ledger_submit_trx_c(
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines: pgrx::JsonB,
) -> i64 {
    let posted_at: DateTime<Utc> = match DateTime::parse_from_rfc3339(posted_at) {
        Ok(t) => t.with_timezone(&Utc),
        Err(e) => ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT,
            format!("ledger_submit_trx_c: posted_at not RFC3339: {e}"),
            "Use ISO 8601 with offset, e.g. '2026-05-25T12:00:00+00:00'.",
        ),
    };

    let line_jsons: Vec<LineJson> = match serde_json::from_value(lines.0) {
        Ok(v) => v,
        Err(e) => ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("ledger_submit_trx_c: lines JSONB decode failed: {e}"),
            "Expected an array of {pool_id, line_type, source_id?, qty, unit_cost, \
             debit_account, credit_account, variance_account?}.",
        ),
    };

    let line_requests: Vec<TrxLineRequest> = line_jsons
        .iter()
        .map(|l| {
            let lt = match decode_line_type(&l.line_type) {
                Some(t) => t,
                None => ereport_invalid_arg(
                    PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                    format!("ledger_submit_trx_c: unknown line_type '{}'", l.line_type),
                    "See migration 0001 line_type enum for valid values.",
                ),
            };
            TrxLineRequest {
                pool_id: l.pool_id,
                line_type: lt,
                source_id: l.source_id,
                qty: l.qty,
                unit_cost: l.unit_cost,
                debit_account: l.debit_account,
                credit_account: l.credit_account,
                variance_account: l.variance_account,
            }
        })
        .collect();

    // 1. Touched pool_ids.
    let touched: Vec<i64> = line_requests
        .iter()
        .map(|l| l.pool_id)
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();

    // 2. pool_lock FOR UPDATE.
    if let Err(e) = pool_lock::acquire_pool_locks(&touched) {
        ereport_internal(format!("ledger_submit_trx_c: pool_lock acquisition failed: {e}"));
    }

    // 3-5. Hydrate snapshot.
    let mut snapshot = match hydration::hydrate_snapshot(&touched) {
        Ok(s) => s,
        Err(e) => ereport_internal(format!("ledger_submit_trx_c: snapshot hydration failed: {e}")),
    };

    // 6. Dispatch — provisional for FIFO/LIFO, strict for WAC/STD/specific.
    let plan = match plan_apply_provisional(&mut snapshot, &line_requests, posted_at) {
        Ok(p) => p,
        Err(err) => {
            // 7. LedgerError → ereport!(ERROR, ...). raise_ledger_error diverges
            // via ereport! but its return type is (); the explicit unreachable!
            // satisfies the type checker for this match arm.
            ledger_error_map::raise_ledger_error(err);
            unreachable!()
        }
    };

    // 8. Bulk-write in FK order. 9. trx.id is the return value.
    match bulk_write::apply_plan_result(trx_type, source_id, posted_at, &plan) {
        Ok(id) => id,
        Err(e) => ereport_internal(format!("ledger_submit_trx_c: bulk-write failed: {e}")),
    }
}

/// Wrap `ereport!(ERROR, ...)` so its diverging behavior propagates through to
/// match arms via the `-> !` return type.
#[allow(unreachable_code)]
fn ereport_invalid_arg(code: PgSqlErrorCode, msg: String, hint: &str) -> ! {
    ereport!(ERROR, code, msg, hint);
    unreachable!()
}

#[allow(unreachable_code)]
fn ereport_internal(msg: String) -> ! {
    ereport!(ERROR, PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, msg);
    unreachable!()
}
