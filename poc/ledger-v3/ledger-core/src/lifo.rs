//! LIFO method (design-v3 §3.3).
//!
//! Same shape as FIFO but depletion walks layers DESC by layer_seq
//! (newest first). All shared logic lives in `crate::layered`.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::layered::{apply_layered, LayerOrder};
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

pub(crate) fn apply_lifo(
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
    if method != PoolMethod::Lifo {
        return Err(LedgerError::MethodMismatch {
            pool_id: line.pool_id,
            expected: PoolMethod::Lifo,
            got: method,
        });
    }
    apply_layered(snapshot, line, result, posted_at, LayerOrder::Descending)
}
