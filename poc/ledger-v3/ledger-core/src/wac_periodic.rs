//! WAC-periodic method — Oracle PAC convention (acct-s6fa).
//!
//! Same cumulative-sum storage and depletion math as `wac` (acct-h5gs).
//! The only behavioral difference is that depletions **also** push a
//! `ProvisionalPostingRequest` onto `PlanResult.provisional_postings`,
//! which the caller's bulk_write turns into a `posting_lines_provisional`
//! row. The period-close hook (`ledger_close_period`) walks those rows
//! to recompute `final_avg = Σ(in-period receipt value) / Σ(in-period qty)`
//! per pool and posts variance.
//!
//! # Per-method storage contract
//!
//! Identical to WAC (per `crate::wac` module head): `pool_state.qty` =
//! qty_sum (total), `pool_state.unit_cost` = value_sum (total). The same
//! footgun and the same display rule `running_avg = unit_cost / qty`.
//!
//! # Receipts
//!
//! No provisional flag. Receipts in periodic-WAC are exact additive value
//! ops; their amount is `qty × unit_cost` directly. The close hook
//! consumes the receipt rows from `posting_line` (event_type =
//! 'inventory_receipt') joined to `trx_line` for qty, summing per pool to
//! derive the period's `final_avg`.
//!
//! # Depletions
//!
//! Posted at the running pool average (same math as `wac`). The amount
//! is the provisional cost. The depletion is flagged in
//! `provisional_postings` so the close hook can recompute variance against
//! the actual `final_avg` at period close.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{
    PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, ProvisionalPostingRequest,
    TrxLineOutput, TrxLineRequest,
};
use crate::seq::next_trx_seq;
use crate::snapshot::{PoolStateRow, Snapshot};

pub(crate) fn apply_wac_periodic(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    let method = snapshot
        .method_of
        .get(&line.pool_id)
        .copied()
        .ok_or(LedgerError::UnknownPool(line.pool_id))?;
    if method != PoolMethod::WacPeriodic {
        return Err(LedgerError::MethodMismatch {
            pool_id: line.pool_id,
            expected: PoolMethod::WacPeriodic,
            got: method,
        });
    }

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
    let (existing_qty, existing_value_sum, layer_existed) = read_layer0(snapshot, line.pool_id);

    let added_value = line
        .qty
        .checked_mul(line.unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac_periodic receipt value: {} * {} (pool {})",
                line.qty, line.unit_cost, line.pool_id
            ),
        })?;

    let new_qty = existing_qty
        .checked_add(line.qty)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac_periodic receipt qty overflow on pool {}: {} + {}",
                line.pool_id, existing_qty, line.qty
            ),
        })?;
    let new_value_sum = existing_value_sum
        .checked_add(added_value)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac_periodic receipt value_sum overflow on pool {}: {} + {}",
                line.pool_id, existing_value_sum, added_value
            ),
        })?;

    let trx_seq = next_trx_seq(snapshot, line.pool_id)?;
    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        trx_seq,
        qty: line.qty,
        unit_cost: line.unit_cost,
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    let mutation = if layer_existed {
        PoolStateMutation::Upsert {
            pool_id: line.pool_id,
            layer_seq: 0,
            qty: new_qty,
            unit_cost: new_value_sum,
            last_trx_line_idx: trx_line_idx,
        }
    } else {
        PoolStateMutation::Insert {
            pool_id: line.pool_id,
            layer_seq: 0,
            qty: new_qty,
            unit_cost: new_value_sum,
            last_trx_line_idx: trx_line_idx,
        }
    };
    result.pool_state_mutations.push(mutation);

    upsert_in_memory_layer0(snapshot, line.pool_id, new_qty, new_value_sum);

    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryReceipt,
        amount: added_value,
        debit_account: line.debit_account,
        credit_account: line.credit_account,
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
    let qty_to_deplete = line.qty.checked_abs().ok_or_else(|| LedgerError::Overflow {
        detail: format!(
            "wac_periodic deplete qty.abs() on pool {}: qty={}",
            line.pool_id, line.qty
        ),
    })?;

    let (current_qty, current_value_sum, layer_existed) = read_layer0(snapshot, line.pool_id);
    if current_qty < qty_to_deplete {
        return Err(LedgerError::InsufficientInventory {
            pool_id: line.pool_id,
            requested: qty_to_deplete,
            available: current_qty,
        });
    }

    // SINGLE BOUNDED ROUND per depletion: amount = (Q × value_sum) / qty.
    // Matches `wac` exactly. The amount becomes the provisional value
    // captured on posting_lines_provisional for later variance recompute.
    let numerator = qty_to_deplete
        .checked_mul(current_value_sum)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac_periodic deplete amount num: {} * {} (pool {})",
                qty_to_deplete, current_value_sum, line.pool_id
            ),
        })?;
    let amount = numerator / current_qty;

    let new_qty = current_qty - qty_to_deplete;
    let new_value_sum = current_value_sum - amount;

    let display_unit_cost = amount / qty_to_deplete;

    let trx_seq = next_trx_seq(snapshot, line.pool_id)?;
    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        trx_seq,
        qty: line.qty,
        unit_cost: display_unit_cost,
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    result.pool_state_mutations.push(PoolStateMutation::Upsert {
        pool_id: line.pool_id,
        layer_seq: 0,
        qty: new_qty,
        unit_cost: new_value_sum,
        last_trx_line_idx: trx_line_idx,
    });

    if layer_existed {
        upsert_in_memory_layer0(snapshot, line.pool_id, new_qty, new_value_sum);
    }

    let posting_line_idx = result.posting_lines.len();
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryDepletion,
        amount,
        debit_account: line.debit_account,
        credit_account: line.credit_account,
        posted_at,
    });

    // The provisional flag — the periodic-vs-perpetual difference.
    result
        .provisional_postings
        .push(ProvisionalPostingRequest {
            posting_line_idx,
            pool_id: line.pool_id,
            qty: qty_to_deplete,
            provisional_amount: amount,
        });

    Ok(())
}

fn read_layer0(snapshot: &Snapshot, pool_id: i64) -> (i64, i64, bool) {
    match snapshot
        .pools
        .get(&pool_id)
        .and_then(|v| v.iter().find(|l| l.layer_seq == 0))
    {
        Some(row) => (row.qty, row.unit_cost, true),
        None => (0, 0, false),
    }
}

fn upsert_in_memory_layer0(snapshot: &mut Snapshot, pool_id: i64, qty: i64, value_sum: i64) {
    let layers = snapshot.pools.entry(pool_id).or_default();
    if let Some(row) = layers.iter_mut().find(|l| l.layer_seq == 0) {
        row.qty = qty;
        row.unit_cost = value_sum;
    } else {
        layers.push(PoolStateRow {
            layer_seq: 0,
            qty,
            unit_cost: value_sum,
            last_trx_line_id: 0,
        });
    }
}
