//! FIFO method — fail-loud stub (design-v3.2 §5a).
//!
//! FIFO pools have no hot-path cost dispatch: under the alt-C posture their hot
//! path is an insert-only append and the recalc engine is the sole costing plane
//! (design-v3.1 §16 / design-v3.2 §5). The strict FIFO layer-walk lands here
//! with the recalc engine (design-v3.2-recalc-a.md), replacing this stub.
//! Reaching it before then is a misroute, surfaced as MethodMismatch.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

pub(crate) fn apply_fifo(
    snapshot: &Snapshot,
    line: &TrxLineRequest,
    _result: &mut PlanResult,
    _posted_at: DateTime<Utc>,
) -> Result<(), LedgerError> {
    let got = snapshot
        .method_of
        .get(&line.pool_id)
        .copied()
        .ok_or(LedgerError::UnknownPool(line.pool_id))?;
    Err(LedgerError::MethodMismatch {
        pool_id: line.pool_id,
        expected: PoolMethod::Fifo,
        got,
    })
}
