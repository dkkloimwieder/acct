//! WAC method — running-average aggregate (design-v3.1 §3.1).
//!
//! pool_state has exactly one row per pool at `layer_id = 0` carrying total
//! `qty`, the cumulative book value `value_sum`, and the running-average
//! `unit_cost` derived from them as `banker_div(value_sum, qty)`. Receipts
//! accumulate `value_sum += qty × unit_cost` exactly (i128 product, no
//! intermediate rounding); the average is a rounded view of `value_sum`, not an
//! independent accumulator, so a receipts-only pool's final unit_cost is exactly
//! `banker_div(Σ qty×cost, Σ qty)` and order-independent (§3.0/§3.1).
//!
//! The `aggregate_receipt` / `aggregate_deplete` helpers are shared with Path C's
//! provisional FIFO/LIFO mode (§3.5: "Receipts behave identically to WAC's
//! running-aggregate update"), so the receipt math lives here once.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::numeric::banker_div;
use crate::plan::{
    PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
use crate::snapshot::Snapshot;

pub(crate) fn apply_wac(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    expect_method(snapshot, line.pool_id, PoolMethod::Wac)?;
    match line.qty.cmp(&0) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => aggregate_receipt(snapshot, line, result, posted_at),
        std::cmp::Ordering::Less => {
            // WAC depletes at the running average (the current aggregate unit_cost).
            let (_, applied_unit_cost, _, _) = snapshot.read_aggregate(line.pool_id);
            aggregate_deplete(snapshot, line, result, posted_at, applied_unit_cost)
        }
    }
}

/// Defensive method check; the dispatcher already routed here, but a buggy
/// dispatcher should fail loud rather than corrupt state.
pub(crate) fn expect_method(
    snapshot: &Snapshot,
    pool_id: i64,
    expected: PoolMethod,
) -> Result<(), LedgerError> {
    let got = snapshot
        .method_of
        .get(&pool_id)
        .copied()
        .ok_or(LedgerError::UnknownPool(pool_id))?;
    if got != expected {
        return Err(LedgerError::MethodMismatch { pool_id, expected, got });
    }
    Ok(())
}

/// Running-average receipt on the aggregate row (§3.1). Shared by WAC and Path C
/// provisional FIFO/LIFO. `line.qty > 0` assumed (caller dispatched on sign).
pub(crate) fn aggregate_receipt(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    // Receipt direction: debit inventory, credit contra — the stored pair as-is.
    let (debit_account, credit_account) =
        snapshot.resolve_posting_accounts(line.pool_id)?.pair_for(line.line_type);

    let (old_qty, old_unit_cost, old_value_sum, _existed) = snapshot.read_aggregate(line.pool_id);

    let new_qty = old_qty
        .checked_add(line.qty)
        .ok_or_else(|| overflow(format!("receipt qty on pool {}", line.pool_id)))?;

    // value_sum accumulates the EXACT book value (§3.0): old book value plus this
    // receipt's qty*cost. The i128 cast is mandatory — i64*i64 overflows here. The
    // running-average unit_cost is then DERIVED from the exact value_sum, never
    // re-rounded off the prior (already-rounded) average — so a receipts-only pool
    // lands unit_cost == banker_div(Σ qty*cost, Σ qty), exact and order-independent.
    let new_value_sum_i128: i128 =
        (old_value_sum as i128) + (line.qty as i128) * (line.unit_cost as i128);
    let new_value_sum: i64 = new_value_sum_i128
        .try_into()
        .map_err(|_| overflow(format!("receipt value_sum on pool {}", line.pool_id)))?;
    // new_qty == 0 guard: defensive against a degenerate qty=0 receipt against an
    // empty pool, which would divide by zero (§3.1). Preserve old_unit_cost.
    let new_unit_cost =
        if new_qty > 0 { banker_div(new_value_sum_i128, new_qty) } else { old_unit_cost };

    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        qty: line.qty,
        unit_cost: line.unit_cost, // receipts record the actual asserted cost
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    result.pool_state_mutations.push(PoolStateMutation::UpsertAggregate {
        pool_id: line.pool_id,
        qty: new_qty,
        unit_cost: new_unit_cost,
        value_sum: new_value_sum,
    });
    snapshot.put_aggregate(line.pool_id, new_qty, new_unit_cost, new_value_sum);

    let amount = (line.qty as i128 * line.unit_cost as i128)
        .try_into()
        .map_err(|_| overflow(format!("receipt amount on pool {}", line.pool_id)))?;
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryReceipt,
        amount,
        debit_account,
        credit_account,
        posted_at,
    });
    Ok(())
}

/// Aggregate depletion at a caller-chosen `applied_unit_cost` (§3.1 / §3.5).
/// WAC passes the running average; provisional 'standard' basis passes the
/// standard cost. `line.qty < 0` assumed. unit_cost on the aggregate is unchanged
/// (the average only moves on receipts).
pub(crate) fn aggregate_deplete(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
    applied_unit_cost: i64,
) -> Result<(), LedgerError> {
    // Depletion direction: swap the receipt-direction pair (debit contra, credit
    // inventory).
    let (rcv_debit, rcv_credit) =
        snapshot.resolve_posting_accounts(line.pool_id)?.pair_for(line.line_type);
    let (debit_account, credit_account) = (rcv_credit, rcv_debit);

    let qty_to_deplete = line
        .qty
        .checked_abs()
        .ok_or_else(|| overflow(format!("deplete qty.abs() on pool {}", line.pool_id)))?;

    let (current_qty, current_unit_cost, current_value_sum, _existed) =
        snapshot.read_aggregate(line.pool_id);
    if current_qty < qty_to_deplete {
        return Err(LedgerError::InsufficientInventory {
            pool_id: line.pool_id,
            requested: qty_to_deplete,
            available: current_qty,
        });
    }
    let new_qty = current_qty - qty_to_deplete;

    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        qty: line.qty,
        unit_cost: applied_unit_cost,
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    // Depletion posts qty*applied_unit_cost; value_sum drops by exactly that
    // amount so it stays == net posted book value (GL-reconcilable). The average
    // is re-derived from the reduced value_sum (preserved up to rounding). When
    // the pool empties, force value_sum = 0 to clear the rounding residual and
    // avoid a stranded value with zero qty (acct-0qps).
    //
    // value_sum may legitimately go NEGATIVE while qty > 0: a provisional
    // standard-basis depletion (§3.5) prices at standard_cost, decoupled from
    // the book average, so depleting deep while standard_cost > avg posts more
    // value out than the pool carries (banker-rounded running-average
    // depletions can do the same at ±1 scale). The derived average follows it
    // below zero — standard-basis pools never price off it. The GL stays the
    // truth; the deferred recalc tier (§7) trues the pool up (acct-mvq4.22;
    // migration 0009 permits the negative aggregate).
    let amount: i64 = (qty_to_deplete as i128 * applied_unit_cost as i128)
        .try_into()
        .map_err(|_| overflow(format!("deplete amount on pool {}", line.pool_id)))?;
    let new_value_sum = if new_qty == 0 {
        0
    } else {
        current_value_sum
            .checked_sub(amount)
            .ok_or_else(|| overflow(format!("deplete value_sum on pool {}", line.pool_id)))?
    };
    let new_unit_cost =
        if new_qty > 0 { banker_div(new_value_sum as i128, new_qty) } else { current_unit_cost };

    result.pool_state_mutations.push(PoolStateMutation::UpsertAggregate {
        pool_id: line.pool_id,
        qty: new_qty,
        unit_cost: new_unit_cost,
        value_sum: new_value_sum,
    });
    snapshot.put_aggregate(line.pool_id, new_qty, new_unit_cost, new_value_sum);

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
