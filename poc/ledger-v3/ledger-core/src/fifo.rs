//! FIFO method (design-v3 §3.2).
//!
//! Thin wrapper over `layered::apply_layered` with `LayerOrder::Ascending`.
//! All the heavy lifting (receipt → push layer; depletion → walk ASC, emit
//! per-layer trx_line + Update/Delete) lives in `crate::layered`.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::layered::{apply_layered, LayerOrder};
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

pub(crate) fn apply_fifo(
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
    if method != PoolMethod::Fifo {
        return Err(LedgerError::MethodMismatch {
            pool_id: line.pool_id,
            expected: PoolMethod::Fifo,
            got: method,
        });
    }
    apply_layered(snapshot, line, result, posted_at, LayerOrder::Ascending)
}
