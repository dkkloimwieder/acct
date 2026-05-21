//! Per-pool row lock acquisition (design-v3 §4.2 step 3).
//!
//! Each submission touches a set of pools; before reading pool_state we
//! acquire `pool_lock` FOR UPDATE on every touched pool. Lazy-create per
//! locked-plan Q3: the pool_lock row is INSERTed on first touch
//! (ON CONFLICT DO NOTHING), then FOR UPDATE locked.
//!
//! Deadlock avoidance: we always lock in ascending `pool_id` order, so
//! two callers touching overlapping pool sets agree on lock order and
//! cycle-free serialize through PG's row-lock manager.
//!
//! Per-pool 2 SPI (INSERT then SELECT FOR UPDATE) matches the spec
//! literal. A future Phase 6 optimization (v21's acct-a2sj approach)
//! can cache "row already exists" to skip the INSERT.

use pgrx::prelude::*;

/// Acquire `pool_lock` FOR UPDATE on every pool_id in `pool_ids`.
///
/// Defensively sorts ascending. Caller is expected to sort + dedup first
/// (BTreeSet upstream), but the in-function sort prevents a deadlock if
/// the caller forgets.
///
/// Returns once all locks are held. Lock survives until the caller's
/// user-tx commits or aborts.
#[allow(dead_code)] // wired by submit::ledger_submit_trx (acct-v4xz)
pub fn acquire_pool_locks(pool_ids: &[i64]) -> Result<(), pgrx::spi::Error> {
    let mut sorted: Vec<i64> = pool_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    for pid in sorted {
        // Lazy create the lock row (idempotent).
        Spi::run_with_args(
            "INSERT INTO pool_lock (pool_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[pid.into()],
        )?;
        // Take the row lock — blocks until any concurrent holder commits.
        Spi::run_with_args(
            "SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE",
            &[pid.into()],
        )?;
    }
    Ok(())
}
