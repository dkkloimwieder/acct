//! PoolMethod, ProvisionalBasis, and the strict `plan_apply` dispatcher.
//!
//! `plan_apply` runs strict mode for WAC/STD/specific — the methods that produce
//! their final cost directly on the hot path (design-v3.2 §1). FIFO/LIFO hit
//! MethodMismatch stubs: their costing is the recalc engine's job (design-v3.2
//! §5a), so a FIFO/LIFO pool reaching this dispatcher is a misroute that fails
//! loud.

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

/// Mirror of the SQL `pool_provisional_basis` enum (`pool.provisional_basis`).
/// Selects the observed-cost basis a FIFO/LIFO depletion append records on its
/// `trx_line.unit_cost`. Informational under the alt-C posture (design-v3.1
/// §16): recalc assigns the authoritative cost regardless of what was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProvisionalBasis {
    /// Use the aggregate row's running average unit_cost (default).
    #[default]
    RunningAvg,
    /// Use `standard_cost.unit_cost` for the pool's (sku_id, location_id).
    Standard,
}

/// Strict dispatcher. WAC/STD/specific run real logic; FIFO/LIFO fail loud
/// (their strict layer math belongs to the recalc engine, design-v3.2 §5a).
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
