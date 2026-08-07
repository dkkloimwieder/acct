//! LIFO method — MethodMismatch stub in PoC scope (design-v3.1 §8).
//!
//! Same rationale as `fifo.rs`: Path C routes LIFO pools through
//! `provisional::plan_apply_provisional`; strict LIFO layer math is recalc/close
//! work (deferred). This stub keeps the strict `plan_apply` dispatcher total.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::method::PoolMethod;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

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
