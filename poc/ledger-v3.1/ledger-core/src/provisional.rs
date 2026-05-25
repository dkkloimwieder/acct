//! Provisional cost mode + the Path C hot-path dispatcher (design-v3.1 §3.5, §8).
//!
//! [`plan_apply_provisional`] is what `ledger_submit_trx_c` and the routed
//! committer call. It dispatches per pool method:
//!  - FIFO / LIFO → provisional, aggregate-only (this module). Receipts run the
//!    WAC running-average update; depletions record a provisional cost from the
//!    running average or the standard cost, leave `source_trx_line_id` NULL, and
//!    mutate only the aggregate row — no per-layer iteration. This is the
//!    constant-lock-hold behavior the PoC measures.
//!  - WAC / STD / specific → their strict modules, unchanged (no provisional
//!    divergence applies).

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::{PoolMethod, ProvisionalBasis};
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

/// Path C entry point. Mutates `snapshot` in place so later lines see earlier
/// lines' effects. Returns Err on the first failing line.
pub fn plan_apply_provisional(
    snapshot: &mut Snapshot,
    lines: &[TrxLineRequest],
    posted_at: DateTime<Utc>,
) -> Result<PlanResult, LedgerError> {
    let mut result = PlanResult::default();
    for line in lines {
        let method = snapshot
            .method_of
            .get(&line.pool_id)
            .copied()
            .ok_or(LedgerError::UnknownPool(line.pool_id))?;
        match method {
            PoolMethod::Fifo | PoolMethod::Lifo => {
                apply_provisional(snapshot, line, &mut result, posted_at)?
            }
            PoolMethod::Wac => crate::wac::apply_wac(snapshot, line, &mut result, posted_at)?,
            PoolMethod::Std => {
                crate::standard::apply_std(snapshot, line, &mut result, posted_at)?
            }
            PoolMethod::Specific => {
                crate::specific::apply_specific(snapshot, line, &mut result, posted_at)?
            }
        }
    }
    Ok(result)
}

/// Provisional FIFO/LIFO line (§3.5). Aggregate-only; no layer rows touched.
fn apply_provisional(
    snapshot: &mut Snapshot,
    line: &TrxLineRequest,
    result: &mut PlanResult,
    posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    match line.qty.cmp(&0) {
        std::cmp::Ordering::Equal => Ok(()),
        // Receipts behave identically to WAC's running-aggregate update.
        std::cmp::Ordering::Greater => {
            crate::wac::aggregate_receipt(snapshot, line, result, posted_at)
        }
        std::cmp::Ordering::Less => {
            let basis = snapshot
                .provisional_basis_of
                .get(&line.pool_id)
                .copied()
                .unwrap_or_default();
            let applied_unit_cost = match basis {
                ProvisionalBasis::RunningAvg => {
                    let (_, unit_cost, _) = snapshot.read_aggregate(line.pool_id);
                    unit_cost
                }
                ProvisionalBasis::Standard => {
                    crate::standard::resolve_standard_cost(snapshot, line.pool_id)?
                }
            };
            crate::wac::aggregate_deplete(snapshot, line, result, posted_at, applied_unit_cost)
        }
    }
}
