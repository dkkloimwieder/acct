//! Specific-id method (design-v3.1 §3.4).
//!
//! Each unit is its own pool (`pool.identity_key = unit_id`, K=1 by construction).
//! Always strict — there's only one layer to read, so there is nothing to defer
//! (specific-id is orthogonal to the alt-C posture and unchanged, design-v3.1
//! §16). A receipt materializes the layer (`layer_id > 0`, equal to
//! the receipt's trx_line.id); a depletion reads that layer, records its cost
//! with `source_trx_line_id` pointing at it, and deletes it. An aggregate row
//! tracks qty for the no-negative invariant.
//!
//! The K=1 single-receipt / no-additional-inflow invariant is the caller's
//! responsibility (§3.4); this module supports full-layer consumption only.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{
    PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
use crate::snapshot::Snapshot;
use crate::wac::expect_method;

pub(crate) fn apply_specific(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    expect_method(snapshot, line.pool_id, PoolMethod::Specific)?;
    match line.qty.cmp(&0) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => receipt(snapshot, line, result, posted_at),
        std::cmp::Ordering::Less => deplete(snapshot, line, result, posted_at),
    }
}

fn receipt(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    // Receipt direction: debit inventory, credit contra — the stored pair as-is.
    let (debit_account, credit_account) =
        snapshot.resolve_posting_accounts(line.pool_id)?.pair_for(line.line_type);

    let (old_qty, _old_uc, _old_vs, _existed) = snapshot.read_aggregate(line.pool_id);
    // K=1 (§3.4): a specific pool holds at most one materialized unit. A second
    // receipt while the pool is stocked would push a co-existing layer that the
    // single-layer `deplete` (lowest layer_id) cannot reason about. The aggregate
    // qty mirrors layer presence here (specific supports full-layer consumption
    // only, so qty is either 0 or the live layer's qty), making qty > 0 the
    // signal that the pool is already stocked — for a hydrated prior receipt and
    // for an earlier receipt within this same batch alike.
    if old_qty > 0 {
        return Err(LedgerError::SpecificPoolOccupied {
            pool_id: line.pool_id,
            existing_qty: old_qty,
        });
    }
    let new_qty = old_qty
        .checked_add(line.qty)
        .ok_or_else(|| overflow(format!("specific receipt qty on pool {}", line.pool_id)))?;

    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        qty: line.qty,
        unit_cost: line.unit_cost,
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    // Materialize the layer. layer_id = this receipt's trx_line.id, resolved by
    // the caller from INSERT ... RETURNING (the idx points at trx_lines).
    let layer_value_sum: i64 = (line.qty as i128 * line.unit_cost as i128)
        .try_into()
        .map_err(|_| overflow(format!("specific layer value_sum on pool {}", line.pool_id)))?;
    result.pool_state_mutations.push(PoolStateMutation::InsertLayer {
        pool_id: line.pool_id,
        layer_trx_line_idx: trx_line_idx,
        qty: line.qty,
        unit_cost: line.unit_cost,
        value_sum: layer_value_sum,
    });
    // Aggregate qty tracking (unit_cost + value_sum mirror are informational for
    // specific: book value = qty * the single layer's cost).
    let value_sum: i64 = (new_qty as i128 * line.unit_cost as i128)
        .try_into()
        .map_err(|_| overflow(format!("specific receipt value_sum on pool {}", line.pool_id)))?;
    result.pool_state_mutations.push(PoolStateMutation::UpsertAggregate {
        pool_id: line.pool_id,
        qty: new_qty,
        unit_cost: line.unit_cost,
        value_sum,
    });
    snapshot.put_aggregate(line.pool_id, new_qty, line.unit_cost, value_sum);

    let amount: i64 = (line.qty as i128 * line.unit_cost as i128)
        .try_into()
        .map_err(|_| overflow(format!("specific receipt amount on pool {}", line.pool_id)))?;
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

fn deplete(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    // Depletion direction: swap the receipt-direction pair (debit contra, credit
    // inventory).
    let (rcv_debit, rcv_credit) =
        snapshot.resolve_posting_accounts(line.pool_id)?.pair_for(line.line_type);
    let (debit_account, credit_account) = (rcv_credit, rcv_debit);

    let qty_to_deplete = line
        .qty
        .checked_abs()
        .ok_or_else(|| overflow(format!("specific deplete qty.abs() on pool {}", line.pool_id)))?;

    // Locate the single materialized layer (lowest layer_id > 0).
    let layer = snapshot
        .pools
        .get(&line.pool_id)
        .and_then(|rows| rows.iter().filter(|r| r.layer_id > 0).min_by_key(|r| r.layer_id))
        .cloned();

    let layer = match layer {
        Some(l) if l.qty >= qty_to_deplete => l,
        _ => {
            let available = layer.as_ref().map(|l| l.qty).unwrap_or(0);
            return Err(LedgerError::InsufficientInventory {
                pool_id: line.pool_id,
                requested: qty_to_deplete,
                available,
            });
        }
    };

    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        qty: line.qty,
        unit_cost: layer.unit_cost,
        source_trx_line_id: Some(layer.layer_id), // consumes this specific layer
        line_type: line.line_type,
        source_id: line.source_id,
    });

    // K=1: full consumption deletes the layer row.
    result.pool_state_mutations.push(PoolStateMutation::DeleteLayer {
        pool_id: line.pool_id,
        layer_id: layer.layer_id,
    });
    // Aggregate qty decrement (cannot go below zero — guarded by the check above
    // plus the aggregate invariant).
    let (agg_qty, agg_uc, _agg_vs, _) = snapshot.read_aggregate(line.pool_id);
    let new_agg_qty = (agg_qty - qty_to_deplete).max(0);
    // K=1 full consumption empties the pool: book value follows qty to 0.
    let new_value_sum: i64 = (new_agg_qty as i128 * agg_uc as i128)
        .try_into()
        .map_err(|_| overflow(format!("specific deplete value_sum on pool {}", line.pool_id)))?;
    result.pool_state_mutations.push(PoolStateMutation::UpsertAggregate {
        pool_id: line.pool_id,
        qty: new_agg_qty,
        unit_cost: agg_uc,
        value_sum: new_value_sum,
    });

    // In-memory: remove the consumed layer, update aggregate, so a second
    // depletion in the same submission finds nothing -> InsufficientInventory.
    if let Some(rows) = snapshot.pools.get_mut(&line.pool_id) {
        rows.retain(|r| r.layer_id != layer.layer_id);
    }
    snapshot.put_aggregate(line.pool_id, new_agg_qty, agg_uc, new_value_sum);

    let amount: i64 = (qty_to_deplete as i128 * layer.unit_cost as i128)
        .try_into()
        .map_err(|_| overflow(format!("specific deplete amount on pool {}", line.pool_id)))?;
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
