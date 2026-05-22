//! Per-pool row lock acquisition (design-v3 §5.4 step 6).
//!
//! Port of `ledger-direct/src/pool_lock.rs` (locked plan §G Q3:
//! copy-paste, resist premature abstraction; if both paths stabilize an
//! `acquire_pool_locks` shared helper may move into `ledger-core`).
//!
//! Each commit_group touches a set of pools; before reading pool_state
//! we acquire `pool_lock` FOR UPDATE on every touched pool. Lazy-create
//! per locked-plan Q3: the pool_lock row is INSERTed on first touch
//! (ON CONFLICT DO NOTHING), then FOR UPDATE locked.
//!
//! Deadlock avoidance: we always lock in ascending `pool_id` order, so
//! two callers touching overlapping pool sets agree on lock order and
//! cycle-free serialize through PG's row-lock manager.

use pgrx::prelude::*;

#[allow(dead_code)] // wired by committer::process_commit_group (acct-usn2)
pub fn acquire_pool_locks(pool_ids: &[i64]) -> Result<(), pgrx::spi::Error> {
    let mut sorted: Vec<i64> = pool_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    for pid in sorted {
        Spi::run_with_args(
            "INSERT INTO pool_lock (pool_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[pid.into()],
        )?;
        Spi::run_with_args(
            "SELECT 1 FROM pool_lock WHERE pool_id = $1 FOR UPDATE",
            &[pid.into()],
        )?;
    }
    Ok(())
}
