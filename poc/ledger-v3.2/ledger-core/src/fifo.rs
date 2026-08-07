//! FIFO method — strict layer-walk for the recalc engine, plus the hot-path
//! guard.
//!
//! FIFO pools have no hot-path cost dispatch: under the alt-C posture their hot
//! path is an insert-only append and the recalc engine is the sole costing
//! plane (design-v3.1 §16 / design-v3.2 §5). [`strict_fold`] is that engine's
//! per-pool fold (recalc-a §7): oldest layer first, fed in R-1
//! `(posted_at, id)` order. The `plan_apply` dispatch entry below stays a
//! fail-loud guard — reaching it from the hot path is a misroute, surfaced as
//! MethodMismatch.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;
use crate::strict::{self, StrictEvent, StrictFoldOutput, StrictLayer};

/// Strict FIFO fold: consume the OLDEST open layer first (recalc-a §2).
pub fn strict_fold(opening: Vec<StrictLayer>, events: &[StrictEvent]) -> StrictFoldOutput {
    strict::strict_fold(false, opening, events)
}

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
