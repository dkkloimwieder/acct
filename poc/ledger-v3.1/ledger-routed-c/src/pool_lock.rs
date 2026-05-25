//! Per-pool row lock acquisition (design-v3.1 §6.4 step 7).
//!
//! Shared shape with ledger-direct-c §5.1 step 2 — the same optimistic FOR
//! UPDATE pattern, here keyed across the whole commit_group's deduped pool set
//! (one acquisition for the batch, vs direct flavor's one per submission).
//!
//! Each submission touches a set of pools; before reading pool_state we acquire
//! `pool_lock` FOR UPDATE on every touched pool. We use the spec's **optimistic**
//! pattern: in the steady state (the pool_lock row already exists, which is the
//! case after a pool's first-ever touch) acquisition is one SPI —
//! `SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE`. Only on a pool's very
//! first touch does the SELECT find no row; then we lazy-create
//! (`INSERT ... ON CONFLICT DO NOTHING`) and re-SELECT FOR UPDATE (+2 SPI, once
//! per pool over its lifetime).
//!
//! Deadlock avoidance: locks are always taken in ascending `pool_id` order, so
//! two callers touching overlapping pool sets agree on lock order and serialize
//! cycle-free through PG's row-lock manager.
//!
//! The FOR UPDATE lock is held until the caller's user-tx commits or aborts —
//! SPI does not open a subtransaction for these reads, so the lock outlives the
//! `Spi::connect` scope.

use pgrx::prelude::*;

/// Acquire `pool_lock` FOR UPDATE on every pool_id in `pool_ids`.
///
/// Defensively sorts ascending + dedups (the caller already does via BTreeSet,
/// but the in-function sort prevents a deadlock if a future caller forgets).
pub fn acquire_pool_locks(pool_ids: &[i64]) -> Result<(), pgrx::spi::Error> {
    let mut sorted: Vec<i64> = pool_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    // FOR UPDATE requires a read-write SPI context: `client.update` calls
    // `Spi::mark_mutable()` and so permits row locking, whereas the read-only
    // `client.select` rejects it ("not allowed in a non-volatile function").
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        for pid in sorted {
            // Optimistic: one SPI in the steady state where the lock row exists.
            let exists = !client
                .update(
                    "SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE",
                    None,
                    &[pid.into()],
                )?
                .is_empty();

            if !exists {
                // First-ever touch of this pool: lazy-create the lock row, then
                // re-acquire. ON CONFLICT DO NOTHING absorbs a concurrent creator.
                client.update(
                    "INSERT INTO pool_lock (pool_id) VALUES ($1) ON CONFLICT DO NOTHING",
                    None,
                    &[pid.into()],
                )?;
                client.update(
                    "SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE",
                    None,
                    &[pid.into()],
                )?;
            }
        }
        Ok(())
    })
}
