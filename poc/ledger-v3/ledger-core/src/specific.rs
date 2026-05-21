//! Specific-id method (design-v3 §3.5).
//!
//! Each unit is its own pool (pool.identity_key = unit_id). The pool has at
//! most one layer of qty=1 (or 0 after depletion). Per spec: "Same shape as
//! FIFO with K=1." The K=1 invariant is a usage convention enforced by the
//! caller (one pool per unit_id), not by this helper — so apply_specific is
//! literally `apply_fifo` with a different method check.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::layered::{apply_layered, LayerOrder};
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

pub(crate) fn apply_specific(
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
    if method != PoolMethod::Specific {
        return Err(LedgerError::MethodMismatch {
            pool_id: line.pool_id,
            expected: PoolMethod::Specific,
            got: method,
        });
    }
    apply_layered(snapshot, line, result, posted_at, LayerOrder::Ascending)
}
