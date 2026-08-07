//! PoolMethod, ProvisionalBasis, and the strict `plan_apply` dispatcher.
//!
//! `plan_apply` runs strict mode for WAC/STD/specific and hits MethodMismatch
//! stubs for FIFO/LIFO (design-v3.1 §8). Path C's hot path does NOT call this for
//! FIFO/LIFO — it calls [`crate::plan_apply_provisional`], which handles FIFO/LIFO
//! in aggregate-only provisional mode and delegates the other methods to the same
//! strict modules. `plan_apply` exists so every PoolMethod variant typechecks and
//! so a misroute (FIFO/LIFO reaching strict math in PoC scope) fails loud.

use chrono::{DateTime, Utc};

use crate::error::LedgerError;
use crate::plan::{PlanResult, TrxLineRequest};
use crate::snapshot::Snapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolMethod {
    Fifo,
    Lifo,
    Wac,
    Std,
    Specific,
}

/// Provisional cost basis for FIFO/LIFO depletions under Path C (`pool.provisional_basis`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProvisionalBasis {
    /// Use the aggregate row's running average unit_cost (default).
    #[default]
    RunningAvg,
    /// Use `standard_cost.unit_cost` for the pool's (sku_id, location_id).
    Standard,
}

/// Strict dispatcher. WAC/STD/specific run real logic; FIFO/LIFO hit
/// MethodMismatch stubs in PoC scope (recalc/close replaces them).
///
/// `snapshot` is mutated in place so later lines see earlier lines' effects.
pub fn plan_apply(
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
            PoolMethod::Wac => crate::wac::apply_wac(snapshot, line, &mut result, posted_at)?,
            PoolMethod::Std => {
                crate::standard::apply_std(snapshot, line, &mut result, posted_at)?
            }
            PoolMethod::Specific => {
                crate::specific::apply_specific(snapshot, line, &mut result, posted_at)?
            }
            PoolMethod::Fifo => crate::fifo::apply_fifo(snapshot, line, &mut result, posted_at)?,
            PoolMethod::Lifo => crate::lifo::apply_lifo(snapshot, line, &mut result, posted_at)?,
        }
    }
    Ok(result)
}
