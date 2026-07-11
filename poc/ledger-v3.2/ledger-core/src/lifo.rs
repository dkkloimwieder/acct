//! LIFO method — fail-loud stub (design-v3.2 §5a).
//!
//! Same rationale as `fifo.rs`: LIFO costing is the recalc engine's job; the
//! strict LIFO layer-walk replaces this stub when the recalc engine lands
//! (design-v3.2-recalc-a.md). This stub keeps the strict `plan_apply`
//! dispatcher total.

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
