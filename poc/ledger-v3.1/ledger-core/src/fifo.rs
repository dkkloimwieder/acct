//! FIFO method — MethodMismatch stub in PoC scope (design-v3.1 §8).
//!
//! Path C never invokes strict FIFO layer math: the hot path routes FIFO pools
//! through `provisional::plan_apply_provisional` (aggregate-only). This stub
//! exists only so the strict `plan_apply` dispatcher typechecks for every
//! PoolMethod variant. Reaching it in PoC scope is a configuration bug (strict
//! FIFO is recalc/close work, deferred), which the MethodMismatch surfaces.

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
    // Strict FIFO is out of PoC scope; recalc/close (deferred) supplies it.
    Err(LedgerError::MethodMismatch {
        pool_id: line.pool_id,
        expected: PoolMethod::Fifo,
        got,
    })
}
