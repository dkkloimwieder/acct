//! Shared layered-method logic for FIFO, LIFO, and specific-id.
//!
//! All three methods share:
//!  - Receipt creates a new `pool_state` layer at `layer_seq = trx_seq`,
//!    with caller-supplied qty + unit_cost.
//!  - Depletion walks live layers in a method-specific order, consuming
//!    `min(remaining, layer.qty)` from each. Each touched layer emits its
//!    own `TrxLineOutput` (with `source_trx_line_id` set to the layer's
//!    receipt trx_line) plus an `Update` or `Delete` mutation.
//!  - Posting_line per emitted trx_line: `amount = consumed * layer.unit_cost`,
//!    event = InventoryReceipt / InventoryDepletion by sign.
//!
//! The only inter-method difference is the depletion walk order:
//!  - FIFO + specific-id: ascending by `layer_seq` (oldest first)
//!  - LIFO: descending (newest first)
//!
//! Specific-id (§3.5) is "FIFO with K=1" — same logic, K=1 invariant enforced
//! by usage convention, not by this helper.
//!
//! IN-SUBMISSION SOURCE RESOLUTION (acknowledged limitation, deferred):
//! When a receipt and a depletion of the same pool appear in the same
//! submission, the receipt's `trx_line.id` isn't known until after the
//! caller's bulk INSERT. The in-memory layer this helper pushes carries
//! `last_trx_line_id = 0` as a placeholder. If a subsequent depletion in
//! the SAME submission consumes that layer, the emitted
//! `TrxLineOutput.source_trx_line_id` will be `None` rather than referencing
//! the in-submission receipt. The PoC's documented test set
//! (acct-ddnu / acct-stvf / acct-qnpq) does not exercise this case;
//! production would need a sibling `source_trx_line_idx: Option<usize>` field
//! on TrxLineOutput for caller-side index translation.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::plan::{
    PlanResult, PoolStateMutation, PostingEventType, PostingLineRequest, TrxLineOutput,
    TrxLineRequest,
};
use crate::seq::next_trx_seq;
use crate::snapshot::{PoolStateRow, Snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LayerOrder {
    Ascending,
    Descending,
}

pub(crate) fn apply_layered(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
    order: LayerOrder,
) -> Result<(), LedgerError> {
    match line.qty.cmp(&0) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => receipt(snapshot, line, result, posted_at),
        std::cmp::Ordering::Less => deplete(snapshot, line, result, posted_at, order),
    }
}

fn receipt(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
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

    result.pool_state_mutations.push(PoolStateMutation::Insert {
        pool_id: line.pool_id,
        layer_seq: trx_seq,
        qty: line.qty,
        unit_cost: line.unit_cost,
        last_trx_line_idx: trx_line_idx,
    });

    // In-memory snapshot: push layer so subsequent lines see it.
    // last_trx_line_id = 0 sentinel — see module docstring.
    snapshot
        .pools
        .entry(line.pool_id)
        .or_default()
        .push(PoolStateRow {
            layer_seq: trx_seq,
            qty: line.qty,
            unit_cost: line.unit_cost,
            last_trx_line_id: 0,
        });

    let amount = line.qty.checked_mul(line.unit_cost).ok_or_else(|| {
        LedgerError::Overflow {
            detail: format!(
                "layered receipt amount: {} * {} (pool {})",
                line.qty, line.unit_cost, line.pool_id
            ),
        }
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
    order: LayerOrder,
) -> Result<(), LedgerError> {
    let total_to_deplete = line.qty.checked_abs().ok_or_else(|| LedgerError::Overflow {
        detail: format!(
            "layered deplete qty.abs() on pool {}: qty={}",
            line.pool_id, line.qty
        ),
    })?;

    // PLAN PHASE: figure out which layers to consume, by how much, without
    // mutating anything yet. Releases the snapshot borrow before EXECUTE phase
    // calls next_trx_seq (which needs &mut snapshot).
    //
    // Each plan entry: (layer_idx_in_pools_vec, layer_seq, consumed_qty,
    //                   unit_cost, source_last_trx_line_id, layer_qty_after)
    let plan: Vec<(usize, i64, i64, i64, i64, i64)> = {
        let layers = snapshot.pools.get(&line.pool_id);
        let available: i64 = layers.map(|v| v.iter().map(|l| l.qty).sum()).unwrap_or(0);
        if available < total_to_deplete {
            return Err(LedgerError::InsufficientInventory {
                pool_id: line.pool_id,
                requested: total_to_deplete,
                available,
            });
        }
        let layers = layers.expect("available > 0 implies pool has layers");
        let mut sorted_idx: Vec<usize> =
            (0..layers.len()).filter(|&i| layers[i].qty > 0).collect();
        match order {
            LayerOrder::Ascending => sorted_idx.sort_by_key(|&i| layers[i].layer_seq),
            LayerOrder::Descending => {
                sorted_idx.sort_by_key(|&i| std::cmp::Reverse(layers[i].layer_seq))
            }
        }

        let mut remaining = total_to_deplete;
        let mut p = Vec::new();
        for idx in sorted_idx {
            if remaining == 0 {
                break;
            }
            let layer = &layers[idx];
            let consumed = remaining.min(layer.qty);
            let new_qty = layer.qty - consumed;
            p.push((
                idx,
                layer.layer_seq,
                consumed,
                layer.unit_cost,
                layer.last_trx_line_id,
                new_qty,
            ));
            remaining -= consumed;
        }
        p
    };

    // EXECUTE PHASE: emit outputs + mutations per planned layer touch.
    for &(layer_idx, layer_seq, consumed, unit_cost, last_trx_line_id, new_qty) in &plan {
        let trx_seq = next_trx_seq(snapshot, line.pool_id)?;
        let trx_line_idx = result.trx_lines.len();
        result.trx_lines.push(TrxLineOutput {
            pool_id: line.pool_id,
            trx_seq,
            qty: -consumed,
            unit_cost,
            source_trx_line_id: if last_trx_line_id > 0 {
                Some(last_trx_line_id)
            } else {
                None
            },
            line_type: line.line_type,
            source_id: line.source_id,
        });

        if new_qty == 0 {
            result.pool_state_mutations.push(PoolStateMutation::Delete {
                pool_id: line.pool_id,
                layer_seq,
            });
        } else {
            result.pool_state_mutations.push(PoolStateMutation::Update {
                pool_id: line.pool_id,
                layer_seq,
                qty: new_qty,
            });
        }

        let amount = consumed
            .checked_mul(unit_cost)
            .ok_or_else(|| LedgerError::Overflow {
                detail: format!(
                    "layered deplete amount: {} * {} (pool {})",
                    consumed, unit_cost, line.pool_id
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

        // Mirror the mutation in the in-memory snapshot.
        snapshot
            .pools
            .get_mut(&line.pool_id)
            .expect("layer existed during plan phase")[layer_idx]
            .qty = new_qty;
    }

    // Prune zero-qty layers so subsequent lines / submissions skip them.
    if let Some(layers) = snapshot.pools.get_mut(&line.pool_id) {
        layers.retain(|l| l.qty > 0);
    }

    Ok(())
}
