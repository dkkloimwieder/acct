//! LIFO method — strict layer-walk for the recalc engine, plus the hot-path
//! guard.
//!
//! Same shape as `fifo.rs`: [`strict_fold`] is the recalc engine's per-pool
//! fold (newest layer first); the `plan_apply` dispatch entry stays a
//! fail-loud MethodMismatch guard keeping the strict hot-path dispatcher
//! total.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;
use crate::strict::{self, StrictEvent, StrictFoldOutput, StrictLayer};

/// Strict LIFO fold: consume the NEWEST open layer first (recalc-a §2).
pub fn strict_fold(opening: Vec<StrictLayer>, events: &[StrictEvent]) -> StrictFoldOutput {
    strict::strict_fold(true, opening, events)
}

pub(crate) fn apply_lifo(
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
        expected: PoolMethod::Lifo,
        got,
    })
}
