//! Per-pool serialization for the ledger-core dispatch paths (STD / specific).
//!
//! There is no `pool_lock` table in v3.2 (deleted with the SPIKE-B verdict,
//! design-v3.2 §3). The commutative aggregate paths need no explicit lock at all
//! — PG's own tuple lock on the `pool_state` aggregate row serializes them. The
//! genuinely stateful methods (STD's read-then-write with a variance leg,
//! specific's K=1 layer lifecycle) still need a per-pool mutex covering the
//! hydrate-plan-write span; the `pool` row itself is that mutex — it always
//! exists (unlike the aggregate `pool_state` row before a pool's first receipt),
//! so there is no lazy-create protocol.
//!
//! Deadlock avoidance: one statement, `ORDER BY id FOR UPDATE` — the LockRows
//! node sits above the Sort, so rows lock in ascending id order and concurrent
//! callers with overlapping pool sets agree on acquisition order. Callers must
//! take these locks BEFORE any commutative-CTE statement in the same
//! transaction (core locks first, CTE tuples second — a fixed two-tier global
//! order that keeps mixed-method submissions cycle-free).
//!
//! The FOR UPDATE lock is held until the caller's user-tx commits or aborts.

use pgrx::prelude::*;

/// Lock the `pool` rows for `pool_ids` in ascending id order. A pool_id with no
/// `pool` row locks nothing here; it surfaces as `UnknownPool` downstream.
pub fn lock_pool_rows(pool_ids: &[i64]) -> Result<(), pgrx::spi::Error> {
    if pool_ids.is_empty() {
        return Ok(());
    }
    let mut sorted: Vec<i64> = pool_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    // FOR UPDATE requires a read-write SPI context (`client.update` marks the
    // xact mutable; read-only `client.select` rejects row locking).
    Spi::connect_mut(|client| -> Result<(), pgrx::spi::Error> {
        client.update(
            "SELECT id FROM pool WHERE id = ANY($1::bigint[]) ORDER BY id FOR UPDATE",
            None,
            &[sorted.into()],
        )?;
        Ok(())
    })
}
