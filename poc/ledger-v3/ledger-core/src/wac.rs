//! WAC method — cumulative-sum form (acct-h5gs, supersedes acct-mcey running-avg).
//!
//! # Per-method storage contract
//!
//! On WAC pool_state rows, the schema columns carry **per-method semantics**
//! that differ from FIFO/LIFO/Specific:
//!
//! | Column                  | FIFO/LIFO/Specific (per-layer) | WAC (cumulative)              |
//! |-------------------------|--------------------------------|-------------------------------|
//! | `pool_state.qty`        | layer qty                      | qty_sum (total over pool)     |
//! | `pool_state.unit_cost`  | per-layer cost (immutable)     | value_sum (total value)       |
//!
//! For WAC, the "running per-unit cost" is computed on demand as
//! `unit_cost / qty` — it is **never stored**. This is the load-bearing
//! difference vs the pre-h5gs running-average model where `unit_cost` was
//! stored as the rounded average and read back on the next receipt.
//!
//! Migration 0006 documents this in the catalog. Apologetic naming —
//! see the migration body for the footgun discussion.
//!
//! # Receipt of qty Q at unit_cost C  (Q > 0)
//!
//! ```text
//! qty       += Q                          [exact]
//! unit_cost += Q × C   (i.e. value_sum)   [exact, commutative]
//! ```
//!
//! No rounding. The order of receipts on a shared pool does not affect the
//! final `(qty, unit_cost)` pair — additive commutative.
//!
//! The §3.1 "preserve cost through zero crossing" guard collapses to a
//! tautology here: with cumulative storage, the per-unit cost is derived
//! from `unit_cost / qty` on the fly, so zero qty just makes the derived
//! value undefined (`unit_cost / NULLIF(qty, 0)`); the underlying
//! `unit_cost` (value_sum) is preserved naturally because we never
//! overwrite it with a rounded derivative.
//!
//! # Depletion of qty Q  (Q > 0)
//!
//! ```text
//! amount    = (Q × unit_cost) / qty        [single bounded round]
//! qty      -= Q                            [exact]
//! unit_cost -= amount                      [exact]
//! trx_line.unit_cost = amount / Q          [derived display value]
//! posting_line.amount = amount             [source-of-truth value]
//! ```
//!
//! One rounding event per depletion, on `amount`. The rounding does NOT
//! propagate into subsequent receipts because receipts re-add exact
//! quantities to `(qty, unit_cost)` — there is no read-back of rounded
//! state on the receipt path. This is the structural fix for acct-mcey.
//!
//! # Equivalence implication
//!
//! - **Receipt-only workloads** (e.g. s5: 1000 receipts on 1 pool): two
//!   paths that apply the same submission set in any order converge to
//!   identical `(qty, unit_cost)`. equivalence --strict expected byte-pass.
//! - **Workloads with depletions** (e.g. s2/s4): post-depletion
//!   `(qty, unit_cost)` is exact subtraction of an `amount` that may differ
//!   across paths when concurrent commit_groups see different intermediate
//!   `(qty, unit_cost)` at depletion time. Per-depletion drift is bounded;
//!   the drift does not compound. trx_line.unit_cost on depletion rows may
//!   diverge between paths within the per-depletion bound — equivalence
//!   classifies these into a `wac_depletion_drifts` bucket distinct from
//!   the now-empty `wac_drifts` bucket.

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
    let (existing_qty, existing_value_sum, layer_existed) = read_layer0(snapshot, line.pool_id);

    let added_value = line
        .qty
        .checked_mul(line.unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac receipt value: {} * {} (pool {})",
                line.qty, line.unit_cost, line.pool_id
            ),
        })?;

    let new_qty = existing_qty
        .checked_add(line.qty)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac receipt qty overflow on pool {}: {} + {}",
                line.pool_id, existing_qty, line.qty
            ),
        })?;
    let new_value_sum = existing_value_sum
        .checked_add(added_value)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac receipt value_sum overflow on pool {}: {} + {}",
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
            "wac deplete qty.abs() on pool {}: qty={}",
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

    // SINGLE BOUNDED ROUND per depletion. amount = (Q × value_sum) / qty.
    // Integer division truncates toward zero. We compute the full
    // numerator first so the round happens at the very last step.
    let numerator = qty_to_deplete
        .checked_mul(current_value_sum)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "wac deplete amount num: {} * {} (pool {})",
                qty_to_deplete, current_value_sum, line.pool_id
            ),
        })?;
    let amount = numerator / current_qty;

    let new_qty = current_qty - qty_to_deplete;
    let new_value_sum = current_value_sum - amount;

    // trx_line.unit_cost is a derived per-unit display value; rounded.
    // Derives from amount (the already-rounded source-of-truth) rather
    // than from value_sum/qty so that qty * trx_line.unit_cost ≤ amount
    // holds, no surprise overflow on a re-derivation downstream.
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

    // Depletion updates qty AND value_sum together — `unit_cost` field on
    // the WAC row is value_sum, so the Update mutation needs to carry it.
    // PoolStateMutation::Update doesn't take unit_cost today (only qty);
    // we use Upsert with the new (qty, value_sum) instead, riding the same
    // ON CONFLICT path that receipts use.
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

/// Returns (qty, value_sum, layer_existed). For WAC, the row's
/// `unit_cost` column IS the value_sum per the per-method storage contract
/// documented at the module head. layer_existed=false means the pool has
/// no layer_seq=0 row yet (first WAC receipt).
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
