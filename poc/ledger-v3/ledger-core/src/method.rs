//! PoolMethod + plan_apply (top-level dispatch).
//!
//! plan_apply is the single entry point shared by ledger-direct (Path A) and
//! ledger-routed (Path B). Static dispatch via match on
//! `snapshot.method_of[&pool_id]` — no trait object, since PoolMethod is closed
//! (no third-party methods extend the enum).
//!
//! Pristine-snapshot replay (design-v3 §7) is the CALLER's responsibility,
//! not plan_apply's:
//!  - Path A: N=1 per call; failing line propagates LedgerError, caller's tx
//!    aborts. No replay possible or needed.
//!  - Path B: ledger-routed's committer owns the replay loop in
//!    process_commit_group: clone pristine snapshot, run plan_apply per
//!    non-excluded submission, on Err exclude + retry from pristine.

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

/// The single entry point shared by ledger-direct and ledger-routed.
///
/// `snapshot` is mutated in-place as plan_apply walks lines so subsequent
/// lines in the same submission see prior lines' effects.
///
/// Returns Err on the first failing line. Caller decides what to do:
///  - Path A: convert to `ereport!(ERROR, ...)`; caller's user-tx aborts.
///  - Path B: exclude the submission and pristine-replay (design-v3 §7).
///
/// Per-method bodies live in `fifo.rs` / `lifo.rs` / `wac.rs` / `standard.rs` /
/// `specific.rs` (filled in by acct-ddnu / acct-stvf / acct-evpw / acct-ll79 /
/// acct-qnpq). Each method impl wires its own dispatch branch as it lands;
/// acct-nmlc verifies coverage + ships the idempotent-replay property test.
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
            PoolMethod::Std => {
                crate::standard::apply_std(snapshot, line, &mut result, posted_at)?
            }
            PoolMethod::Fifo => crate::fifo::apply_fifo(snapshot, line, &mut result, posted_at)?,
            PoolMethod::Lifo => crate::lifo::apply_lifo(snapshot, line, &mut result, posted_at)?,
            PoolMethod::Wac => crate::wac::apply_wac(snapshot, line, &mut result, posted_at)?,
            PoolMethod::Specific => {
                crate::specific::apply_specific(snapshot, line, &mut result, posted_at)?
            }
        }
    }
    Ok(result)
}
