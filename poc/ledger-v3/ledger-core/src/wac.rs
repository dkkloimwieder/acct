//! WAC method (design-v3 §3.1).
//!
//! Single pool_state row at layer_seq = 0 carrying the pool's running
//! qty + weighted-average unit_cost.
//!
//! Receipt of qty Q at unit_cost C:
//!   new_qty = old_qty + Q
//!   IF new_qty > 0: new_unit_cost = (old_qty * old_unit_cost + Q * C) / new_qty
//!   ELSE: preserve old_unit_cost  ← LOAD-BEARING §3.1 GUARD
//! Emits Insert (first touch) or Upsert (subsequent) on layer_seq=0.
//!
//! Depletion of qty Q:
//!   Reads pool's running unit_cost, emits trx_line at that cost,
//!   emits Update with new_qty = old_qty - Q. unit_cost unchanged.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{
    PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
use crate::seq::next_trx_seq;
use crate::snapshot::{PoolStateRow, Snapshot};

pub(crate) fn apply_wac(
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
    if method != PoolMethod::Wac {
        return Err(LedgerError::MethodMismatch {
            pool_id: line.pool_id,
            expected: PoolMethod::Wac,
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
    let (existing_qty, existing_unit_cost, layer_existed) = read_layer0(snapshot, line.pool_id);

    let new_qty = existing_qty
        .checked_add(line.qty)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac receipt qty overflow on pool {}: {} + {}",
                line.pool_id, existing_qty, line.qty
            ),
        })?;

    // §3.1 LOAD-BEARING DIVIDE-BY-ZERO GUARD.
    //
    // Naive implementations divide (old_qty*old_unit_cost + Q*C) by new_qty
    // unconditionally, which panics (Rust) or raises (SQL) when new_qty == 0
    // and produces nonsense averages when new_qty < 0 (oversold position).
    // The spec mandates: when new_qty <= 0, preserve the previous unit_cost
    // as the basis — the cost survives the trip through zero, ready to be
    // weighted against the next receipt that brings the pool positive again.
    //
    // First-touch case (no existing layer) skips the guard entirely:
    // new_unit_cost = line.unit_cost directly.
    let new_unit_cost = if !layer_existed {
        line.unit_cost
    } else if new_qty > 0 {
        weighted_average(existing_qty, existing_unit_cost, line.qty, line.unit_cost, new_qty)?
    } else {
        existing_unit_cost
    };

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
            unit_cost: new_unit_cost,
            last_trx_line_idx: trx_line_idx,
        }
    } else {
        PoolStateMutation::Insert {
            pool_id: line.pool_id,
            layer_seq: 0,
            qty: new_qty,
            unit_cost: new_unit_cost,
            last_trx_line_idx: trx_line_idx,
        }
    };
    result.pool_state_mutations.push(mutation);

    upsert_in_memory_layer0(snapshot, line.pool_id, new_qty, new_unit_cost);

    let amount = line
        .qty
        .checked_mul(line.unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac receipt amount: {} * {} (pool {})",
                line.qty, line.unit_cost, line.pool_id
            ),
        })?;
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryReceipt,
        amount,
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
            "wac deplete qty.abs() on pool {}: qty={}",
            line.pool_id, line.qty
        ),
    })?;

    let (current_qty, current_unit_cost, layer_existed) = read_layer0(snapshot, line.pool_id);
    if current_qty < qty_to_deplete {
        return Err(LedgerError::InsufficientInventory {
            pool_id: line.pool_id,
            requested: qty_to_deplete,
            available: current_qty,
        });
    }

    let new_qty = current_qty - qty_to_deplete;
    let trx_seq = next_trx_seq(snapshot, line.pool_id)?;
    let trx_line_idx = result.trx_lines.len();
    result.trx_lines.push(TrxLineOutput {
        pool_id: line.pool_id,
        trx_seq,
        qty: line.qty,
        unit_cost: current_unit_cost,
        source_trx_line_id: None,
        line_type: line.line_type,
        source_id: line.source_id,
    });

    // Depletion always Updates (qty -= Q); never Delete (WAC keeps the row at
    // qty=0 to preserve the basis through the zero crossing per §3.1 +
    // §13.3 — stale rows after total depletion are intentional).
    result.pool_state_mutations.push(PoolStateMutation::Update {
        pool_id: line.pool_id,
        layer_seq: 0,
        qty: new_qty,
    });

    // Mirror in snapshot. layer_existed must be true here (current_qty >=
    // qty_to_deplete >= 1), but write defensively.
    if layer_existed {
        upsert_in_memory_layer0(snapshot, line.pool_id, new_qty, current_unit_cost);
    }

    let amount = qty_to_deplete
        .checked_mul(current_unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac deplete amount: {} * {} (pool {})",
                qty_to_deplete, current_unit_cost, line.pool_id
            ),
        })?;
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type: PostingEventType::InventoryDepletion,
        amount,
        debit_account: line.debit_account,
        credit_account: line.credit_account,
        posted_at,
    });

    Ok(())
}

/// Returns (qty, unit_cost, layer_existed). layer_existed=false means the pool
/// has no layer_seq=0 row yet (first WAC receipt).
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

fn upsert_in_memory_layer0(snapshot: &mut Snapshot, pool_id: i64, qty: i64, unit_cost: i64) {
    let layers = snapshot.pools.entry(pool_id).or_default();
    if let Some(row) = layers.iter_mut().find(|l| l.layer_seq == 0) {
        row.qty = qty;
        row.unit_cost = unit_cost;
    } else {
        layers.push(PoolStateRow {
            layer_seq: 0,
            qty,
            unit_cost,
            last_trx_line_id: 0,
        });
    }
}

/// (old_qty * old_unit_cost + line_qty * line_unit_cost) / new_qty,
/// with checked arithmetic. Caller has already verified new_qty > 0.
fn weighted_average(
    old_qty: i64,
    old_unit_cost: i64,
    line_qty: i64,
    line_unit_cost: i64,
    new_qty: i64,
) -> Result<i64, LedgerError> {
    let term1 = old_qty
        .checked_mul(old_unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!("wac avg term1: {} * {}", old_qty, old_unit_cost),
        })?;
    let term2 = line_qty
        .checked_mul(line_unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!("wac avg term2: {} * {}", line_qty, line_unit_cost),
        })?;
    let numerator = term1.checked_add(term2).ok_or_else(|| LedgerError::Overflow {
        detail: format!("wac avg numerator: {} + {}", term1, term2),
    })?;
    Ok(numerator / new_qty)
}
