//! STD method (design-v3 §3.4).
//!
//! No pool_state rows. Receipt and depletion both INSERT trx_line with
//! unit_cost taken from the caller's TrxLineRequest.unit_cost (per locked
//! plan decision Q4 — ledger-core trusts the caller for the PoC; future
//! revision could look up from `standard_costs` via Snapshot.std_cost_of).
//! Variance posting_lines on receipts are deferred per design-v3 §3.4.
//!
//! File named `standard.rs` (not `std.rs`) to avoid `mod std;` shadowing the
//! Rust standard library inside the crate — same convention as
//! poc/queue-extension-v21/src/standard.rs.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{PlanResult, PostingEventType, PostingLineRequest, TrxLineOutput, TrxLineRequest};
use crate::snapshot::Snapshot;

/// Apply one STD line. Mutates `snapshot.max_trx_seq_of` (per-pool counter)
/// and appends to `result.trx_lines` + `result.posting_lines`. Does NOT
/// emit a `PoolStateMutation` — STD pools have no pool_state rows.
pub(crate) fn apply_std(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    // Defensive: the dispatcher should have already verified this is an STD pool.
    // Re-check so a buggy dispatcher fails loudly instead of corrupting state.
    let method = snapshot
        .method_of
        .get(&line.pool_id)
        .copied()
        .ok_or(LedgerError::UnknownPool(line.pool_id))?;
    if method != PoolMethod::Std {
        return Err(LedgerError::MethodMismatch {
            pool_id: line.pool_id,
            expected: PoolMethod::Std,
            got: method,
        });
    }

    // Allocate trx_seq from the pool's running max (design-v3 §13.1 option a).
    let seq_entry = snapshot.max_trx_seq_of.entry(line.pool_id).or_insert(0);
    *seq_entry = seq_entry
        .checked_add(1)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!("std trx_seq overflow on pool {}", line.pool_id),
        })?;
    let trx_seq = *seq_entry;

    // Emit one TrxLineOutput; STD never splits across layers.
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

    // posting_line.amount is unsigned magnitude. checked_abs guards i64::MIN.
    let abs_qty = line.qty.checked_abs().ok_or_else(|| LedgerError::Overflow {
        detail: format!("std qty.abs() overflow: qty={}", line.qty),
    })?;
    let amount = abs_qty
        .checked_mul(line.unit_cost)
        .ok_or_else(|| LedgerError::Overflow {
            detail: format!(
                "std posting amount overflow: {} * {}",
                abs_qty, line.unit_cost
            ),
        })?;
    let event_type = if line.qty >= 0 {
        PostingEventType::InventoryReceipt
    } else {
        PostingEventType::InventoryDepletion
    };
    result.posting_lines.push(PostingLineRequest {
        trx_line_idx,
        event_type,
        amount,
        debit_account: line.debit_account,
        credit_account: line.credit_account,
        posted_at,
    });

    Ok(())
}
