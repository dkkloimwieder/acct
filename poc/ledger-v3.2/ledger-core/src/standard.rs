//! STD method (design-v3.1 §3.3).
//!
//! Both receipts and depletions record at the standard cost `C_std` from
//! `standard_cost` (resolved into `snapshot.standard_cost_of`). Receipts post
//! the actual-vs-standard purchase-price variance to a variance account.
//!
//! STD maintains an aggregate row (`layer_id = 0`) for qty tracking; depletions
//! gate synchronously on available qty (strict methods keep their gate,
//! design-v3.2 §3). The aggregate's unit_cost mirrors `C_std`.
//!
//! File named `standard` (not `std`) to avoid `mod std` shadowing `::std`.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{
    PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
use crate::snapshot::Snapshot;
use crate::wac::expect_method;

/// Resolve `C_std` for a pool, or `MissingStandardCost` (with sku/location for
/// the message).
pub(crate) fn resolve_standard_cost(snapshot: &Snapshot, pool_id: i64) -> Result<i64, LedgerError> {
    snapshot
        .standard_cost_of
        .get(&pool_id)
        .copied()
        .ok_or_else(|| {
            let (sku_id, location_id) =
                snapshot.sku_location_of.get(&pool_id).copied().unwrap_or((0, 0));
            LedgerError::MissingStandardCost { sku_id, location_id }
        })
}

pub(crate) fn apply_std(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    expect_method(snapshot, line.pool_id, PoolMethod::Std)?;
    if line.qty == 0 {
        return Ok(());
    }
    let c_std = resolve_standard_cost(snapshot, line.pool_id)?;
    let (old_qty, _old_uc, _old_vs, _existed) = snapshot.read_aggregate(line.pool_id);

    if line.qty > 0 {
        receipt(snapshot, line, result, posted_at, c_std, old_qty)
    } else {
        deplete(snapshot, line, result, posted_at, c_std, old_qty)
    }
}

fn receipt(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
    c_std: i64,
    old_qty: i64,
) -> Result<(), LedgerError> {
    // Receipt direction: debit inventory, credit contra. The variance leg flips
    // between the contra (credit_account) and the configured variance account.
    let accounts = snapshot.resolve_posting_accounts(line.pool_id)?;
    let (debit_account, credit_account) = accounts.pair_for(line.line_type);

    let q = line.qty; // > 0
    let c_actual = line.unit_cost;
    let new_qty = old_qty
        .checked_add(q)
        .ok_or_else(|| overflow(format!("std receipt qty on pool {}", line.pool_id)))?;

    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        qty: q,
        unit_cost: c_std, // STD records the standard, not the actual
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    // STD's book value mirrors the standard: value_sum = qty*C_std, so the
    // derived unit_cost stays exactly C_std and value_sum tracks the posted std
    // value (Q*C_std main leg).
    let value_sum: i64 = (new_qty as i128 * c_std as i128)
        .try_into()
        .map_err(|_| overflow(format!("std receipt value_sum on pool {}", line.pool_id)))?;
    result.pool_state_mutations.push(PoolStateMutation::UpsertAggregate {
        pool_id: line.pool_id,
        qty: new_qty,
        unit_cost: c_std, // aggregate mirrors the standard
        value_sum,
    });
    snapshot.put_aggregate(line.pool_id, new_qty, c_std, value_sum);

    // Main leg: inventory (debit) <- source (credit) at standard value Q*C_std.
    let std_value: i64 = (q as i128 * c_std as i128)
        .try_into()
        .map_err(|_| overflow(format!("std value on pool {}", line.pool_id)))?;
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryReceipt,
        amount: std_value,
        debit_account,
        credit_account,
        posted_at,
    });

    // Variance leg: Q*(C_actual - C_std). Source's net credit becomes Q*C_actual.
    let delta_unit = (c_actual as i128) - (c_std as i128);
    if delta_unit != 0 {
        let variance_account = accounts
            .variance
            .ok_or(LedgerError::MissingVarianceAccount { pool_id: line.pool_id })?;
        let variance: i128 = (q as i128) * delta_unit;
        // delta > 0 (paid more than standard, unfavorable): debit variance,
        // credit source (the contra). delta < 0 (favorable): debit source, credit variance.
        let (var_debit, var_credit, amount) = if variance > 0 {
            (variance_account, credit_account, variance)
        } else {
            (credit_account, variance_account, -variance)
        };
        let amount: i64 = amount
            .try_into()
            .map_err(|_| overflow(format!("std variance on pool {}", line.pool_id)))?;
        result.posting_lines.push(PostingLineRequest {
            trx_line_idx,
            event_type: PostingEventType::Variance,
            amount,
            debit_account: var_debit,
            credit_account: var_credit,
            posted_at,
        });
    }
    Ok(())
}

fn deplete(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
    c_std: i64,
    old_qty: i64,
) -> Result<(), LedgerError> {
    // Depletion direction: swap the receipt-direction pair (debit contra, credit
    // inventory).
    let (rcv_debit, rcv_credit) =
        snapshot.resolve_posting_accounts(line.pool_id)?.pair_for(line.line_type);
    let (debit_account, credit_account) = (rcv_credit, rcv_debit);

    let qty_to_deplete = line
        .qty
        .checked_abs()
        .ok_or_else(|| overflow(format!("std deplete qty.abs() on pool {}", line.pool_id)))?;
    if old_qty < qty_to_deplete {
        return Err(LedgerError::InsufficientInventory {
            pool_id: line.pool_id,
            requested: qty_to_deplete,
            available: old_qty,
        });
    }
    let new_qty = old_qty - qty_to_deplete;

    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        qty: line.qty,
        unit_cost: c_std,
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    let value_sum: i64 = (new_qty as i128 * c_std as i128)
        .try_into()
        .map_err(|_| overflow(format!("std deplete value_sum on pool {}", line.pool_id)))?;
    result.pool_state_mutations.push(PoolStateMutation::UpsertAggregate {
        pool_id: line.pool_id,
        qty: new_qty,
        unit_cost: c_std,
        value_sum,
    });
    snapshot.put_aggregate(line.pool_id, new_qty, c_std, value_sum);

    let amount: i64 = (qty_to_deplete as i128 * c_std as i128)
        .try_into()
        .map_err(|_| overflow(format!("std deplete amount on pool {}", line.pool_id)))?;
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryDepletion,
        amount,
        debit_account,
        credit_account,
        posted_at,
    });
    Ok(())
}

fn overflow(detail: String) -> LedgerError {
    LedgerError::Overflow { detail }
}
