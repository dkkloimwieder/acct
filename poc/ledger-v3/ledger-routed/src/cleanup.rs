//! Cleanup step (design-v3 §5.4 step 11). Verbatim port of v21's
//! `committer.rs::cleanup_after_superbatch` adapted to v3's single
//! pool_keys domain.
//!
//! ## Three CAS cases per staging entry
//!
//! The router's Phase 6 ordering is `superbatch_id.store(sb_id,
//! Release)` THEN `valid.compare_exchange(2, 3, Release, ...)` (see
//! `router::emit_superbatch_chunk`). The committer's pristine-replay
//! eject path is `eject_count.fetch_add(1, Release)` THEN
//! `valid.compare_exchange(3, 1, Release, ...)` + Release-store
//! `sb_id = 0`. Cleanup reads both atomics with Acquire to pair
//! against those Release stores:
//!
//!   1. **Success.** Slot is at `valid == 3`, `superbatch_id == our_sb_id`,
//!      `eject_count == 0`. CAS 3→0 succeeds; arena blocks freed.
//!
//!   2. **Ejected.** Slot is at `valid == 1`, `superbatch_id == 0`,
//!      `eject_count > 0`. Both CAS attempts fail (3→0 because
//!      valid != 3; 2→0 because eject_count != 0). Arena blocks
//!      left for the router to re-claim on the next tick.
//!
//!   3. **Router-died-mid-stamp.** Router crashed between
//!      `sb_id.store(...)` and CAS 2→3, so the slot is stuck at
//!      `valid == 2` with possibly-stale sb_id. eject_count is still
//!      0 because no committer has touched it. CAS 3→0 fails (valid
//!      != 3); CAS 2→0 fallback succeeds. Arena blocks freed —
//!      otherwise the slot leaks because the recovery sweep can't
//!      tell whether the entry made it into the CommitterQueue.
//!
//! ## sb_id guard
//!
//! Case 1's CAS is gated on `observed_sb == superbatch_id` to avoid
//! a race where: cleanup runs AFTER the committer's eject sub-tx
//! release; a concurrent router re-packs the entry (CAS 1→2 + new
//! sb_id + CAS 2→3); blind CAS 3→0 would free the NEW SuperBatch's
//! arena and silently abandon the staging entry. The guard makes
//! cleanup a no-op for entries that have already moved on to another
//! SuperBatch.
//!
//! ## Backpressure broadcast
//!
//! Each successful slot release calls `signal_staging_slot_freed`,
//! which Broadcasts on the queue's backpressure ConditionVariable so
//! a `ledger_enqueue_trx` caller blocked in `cv_wait_for_slot`
//! wakes immediately and retries.

#![allow(dead_code)]

use crate::shmem::{
    COMMITTER_QUEUE, SPILLOVER_ARENA, STAGING_QUEUE, SpilloverArena, StagingQueue,
    signal_staging_slot_freed,
};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// Attempt to release one staging entry's slot + arena ownership.
///
/// Returns `true` if the slot was released (CAS 3→0 success on
/// sb-match, or CAS 2→0 fallback when `eject_count == 0`); `false`
/// otherwise (ejected, or already abandoned, or re-routed under a
/// different sb_id).
///
/// Frees the slot's `payload_offset` + `pool_keys_offset` arena
/// blocks on success. Does NOT broadcast on the backpressure CV —
/// that's the caller's responsibility (the live wrapper performs the
/// broadcast via `signal_staging_slot_freed` under the share-lock;
/// unit tests can call this helper directly without invoking PG-side
/// CV machinery).
pub(crate) fn try_release_staging_entry(
    staging_queue: &StagingQueue,
    arena: &mut SpilloverArena,
    s_idx: u32,
    superbatch_id: u64,
) -> bool {
    let slot = &staging_queue.entries[s_idx as usize];
    let payload_off = slot.payload_offset;
    let pool_keys_off = slot.pool_keys_offset;
    let observed_sb = slot.superbatch_id.load(Acquire);
    let observed_eject = slot.eject_count.load(Acquire);
    let cas_ok = (observed_sb == superbatch_id
        && slot
            .valid
            .compare_exchange(3, 0, Release, Relaxed)
            .is_ok())
        || (observed_eject == 0
            && slot
                .valid
                .compare_exchange(2, 0, Release, Relaxed)
                .is_ok());
    if cas_ok {
        if payload_off != 0 {
            arena.free(payload_off);
        }
        if pool_keys_off != 0 {
            arena.free(pool_keys_off);
        }
    }
    cas_ok
}

/// Release one CommitterQueueEntry: free its arena-owned blocks
/// (`staging_offsets_off` + `pool_keys_off`), CAS valid 2→3 (completed)
/// then 3→0 (empty), and clear the identity / lease atomics. The
/// completed→empty CAS sequence mirrors v21 and gives a brief window
/// where observability can see `completed` before reuse.
pub(crate) fn release_committer_queue_entry(
    committer_queue: &crate::shmem::CommitterQueue,
    arena: &mut SpilloverArena,
    cq_idx: u32,
    staging_offsets_off: u32,
    pool_keys_off: u32,
) {
    if staging_offsets_off != 0 {
        arena.free(staging_offsets_off);
    }
    if pool_keys_off != 0 {
        arena.free(pool_keys_off);
    }
    let slot = &committer_queue.entries[cq_idx as usize];
    let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
    let _ = slot.valid.compare_exchange(3, 0, Release, Relaxed);
    slot.committer_bgw_slot.store(u32::MAX, Relaxed);
    slot.committer_bgw_generation.store(0, Relaxed);
    slot.committer_acquired_at_ns.store(0, Relaxed);
    slot.committer_tx_id.store(0, Relaxed);
}

/// Live wrapper: called by the committer after `COMMIT` on a
/// commit_group. Iterates `staging_indices`, attempts to release each
/// slot (under the shared staging-queue LWLock), broadcasts on the
/// backpressure CV for each successful release, then frees the
/// CommitterQueueEntry's own arena blocks and CAS-cycles its valid
/// 2→3→0 with the identity / lease atomics cleared.
pub(crate) fn cleanup_after_superbatch(
    cq_idx: u32,
    superbatch_id: u64,
    staging_indices: &[u32],
    staging_offsets_off: u32,
    pool_keys_off: u32,
) {
    for &s_idx in staging_indices {
        let released = {
            let staging_guard = STAGING_QUEUE.share();
            let mut arena_guard = SPILLOVER_ARENA.exclusive();
            let released =
                try_release_staging_entry(&staging_guard, &mut arena_guard, s_idx, superbatch_id);
            if released {
                signal_staging_slot_freed(&staging_guard);
            }
            released
        };
        // released is informational — false means ejected or already
        // moved on; no further action needed on this entry.
        let _ = released;
    }

    {
        let committer_guard = COMMITTER_QUEUE.share();
        let mut arena_guard = SPILLOVER_ARENA.exclusive();
        release_committer_queue_entry(
            &committer_guard,
            &mut arena_guard,
            cq_idx,
            staging_offsets_off,
            pool_keys_off,
        );
    }
}

// ── Unit tests for the three CAS cases ──────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::CommitterQueue;
    use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8};

    fn fresh_arena() -> Box<SpilloverArena> {
        unsafe { Box::<SpilloverArena>::new_zeroed().assume_init() }
    }

    fn fresh_staging_queue() -> Box<StagingQueue> {
        unsafe { Box::<StagingQueue>::new_zeroed().assume_init() }
    }

    fn fresh_committer_queue() -> Box<CommitterQueue> {
        unsafe { Box::<CommitterQueue>::new_zeroed().assume_init() }
    }

    /// Populate one staging slot to a known state. Allocates `payload`
    /// + `pool_keys` arena blocks (returns their offsets) so the
    /// caller can assert the arena outstanding count moves expected.
    fn populate_slot(
        staging_queue: &mut StagingQueue,
        arena: &mut SpilloverArena,
        s_idx: u32,
        valid: u8,
        sb_id: u64,
        eject_count: u32,
    ) -> (u32, u32) {
        let payload_off = arena.alloc(64).expect("alloc payload");
        let pool_keys_off = arena.alloc(16).expect("alloc pool_keys");
        let slot = &mut staging_queue.entries[s_idx as usize];
        slot.valid = AtomicU8::new(valid);
        slot.superbatch_id = AtomicU64::new(sb_id);
        slot.eject_count = AtomicU32::new(eject_count);
        slot.payload_offset = payload_off;
        slot.payload_length = 64;
        slot.pool_keys_offset = pool_keys_off;
        slot.pool_count = 2;
        (payload_off, pool_keys_off)
    }

    #[test]
    fn case_1_success_releases_slot_and_arena() {
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let sb_id: u64 = 42;
        let (_p, _pk) = populate_slot(&mut staging, &mut arena, 7, 3, sb_id, 0);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_entry(&staging, &mut arena, 7, sb_id);

        assert!(released, "CAS 3→0 should succeed on matching sb_id");
        assert_eq!(staging.entries[7].valid.load(Relaxed), 0);
        assert_eq!(arena.outstanding_allocs(), 0, "both arena blocks freed");
    }

    #[test]
    fn case_2_ejected_leaves_slot_at_pending_and_arena_in_place() {
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let sb_id: u64 = 42;
        // After eject: valid=1 (pending), sb_id reset to 0, eject_count incremented.
        let (p, pk) = populate_slot(&mut staging, &mut arena, 5, 1, 0, 1);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_entry(&staging, &mut arena, 5, sb_id);

        assert!(!released, "ejected entry should not be released by cleanup");
        assert_eq!(
            staging.entries[5].valid.load(Relaxed),
            1,
            "valid must remain 1 (pending) for router re-pack"
        );
        assert_eq!(
            arena.outstanding_allocs(),
            2,
            "arena blocks belong to next router pack, must not be freed"
        );
        // Sanity: offsets unchanged so router can re-pack.
        assert_eq!(staging.entries[5].payload_offset, p);
        assert_eq!(staging.entries[5].pool_keys_offset, pk);
    }

    #[test]
    fn case_3_router_died_mid_stamp_falls_back_to_cas_2_to_0() {
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let cleanup_sb_id: u64 = 42;
        // Router stamped sb_id but crashed before CAS 2→3 — slot is
        // still at valid=2; sb_id may be either the new value or 0
        // depending on where the crash landed. Use 0 to model the
        // CAS-failed-before-store edge; either way the 3→0 guard
        // must miss and the 2→0 fallback must take.
        let (_p, _pk) = populate_slot(&mut staging, &mut arena, 9, 2, 0, 0);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_entry(&staging, &mut arena, 9, cleanup_sb_id);

        assert!(released, "CAS 2→0 fallback should succeed");
        assert_eq!(staging.entries[9].valid.load(Relaxed), 0);
        assert_eq!(
            arena.outstanding_allocs(),
            0,
            "arena blocks freed by 2→0 fallback to prevent slot leak"
        );
    }

    #[test]
    fn case_3_with_eject_in_flight_skips_2_to_0() {
        // Defensive variant: if the slot is at valid=2 BUT
        // eject_count > 0 (an eject is mid-flight; the eject path
        // bumps eject_count Release BEFORE flipping valid 3→1), the
        // 2→0 fallback must be skipped to avoid race-freeing an
        // entry the eject is about to push back to pending.
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let cleanup_sb_id: u64 = 42;
        populate_slot(&mut staging, &mut arena, 3, 2, cleanup_sb_id, 1);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_entry(&staging, &mut arena, 3, cleanup_sb_id);

        assert!(!released, "in-flight eject must block 2→0 fallback");
        assert_eq!(staging.entries[3].valid.load(Relaxed), 2);
        assert_eq!(arena.outstanding_allocs(), 2);
    }

    #[test]
    fn sb_id_mismatch_on_valid_3_skips_3_to_0_and_falls_through() {
        // Concurrent re-routing race: the entry was completed by an
        // earlier SuperBatch (sb_id=42) then re-packed by the router
        // with a new sb_id=99. Cleanup running for the OLD sb_id=42
        // must NOT CAS 3→0 (would free the new pack's arena and
        // silently abandon the slot). With valid=3 + sb_id=99 +
        // eject_count=0, the 2→0 fallback also skips because valid
        // != 2.
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let stale_sb_id: u64 = 42;
        let new_sb_id: u64 = 99;
        populate_slot(&mut staging, &mut arena, 1, 3, new_sb_id, 0);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_entry(&staging, &mut arena, 1, stale_sb_id);

        assert!(
            !released,
            "stale cleanup must not free a re-routed slot's arena"
        );
        assert_eq!(staging.entries[1].valid.load(Relaxed), 3);
        assert_eq!(arena.outstanding_allocs(), 2);
    }

    #[test]
    fn release_committer_queue_entry_frees_arena_and_clears_identity() {
        let mut committer = fresh_committer_queue();
        let mut arena = fresh_arena();
        let staging_off = arena.alloc(32).expect("alloc staging_offsets");
        let pool_off = arena.alloc(48).expect("alloc pool_keys");
        let cq_idx = 11u32;
        {
            let slot = &mut committer.entries[cq_idx as usize];
            slot.valid = AtomicU8::new(2);
            slot.staging_entry_offsets = staging_off;
            slot.pool_keys_offset = pool_off;
            slot.committer_bgw_slot = AtomicU32::new(5);
            slot.committer_bgw_generation = AtomicU32::new(17);
            slot.committer_acquired_at_ns = AtomicU64::new(12345);
            slot.committer_tx_id = AtomicU64::new(99);
        }
        assert_eq!(arena.outstanding_allocs(), 2);

        release_committer_queue_entry(&committer, &mut arena, cq_idx, staging_off, pool_off);

        assert_eq!(arena.outstanding_allocs(), 0);
        let slot = &committer.entries[cq_idx as usize];
        assert_eq!(slot.valid.load(Relaxed), 0);
        assert_eq!(slot.committer_bgw_slot.load(Relaxed), u32::MAX);
        assert_eq!(slot.committer_bgw_generation.load(Relaxed), 0);
        assert_eq!(slot.committer_acquired_at_ns.load(Relaxed), 0);
        assert_eq!(slot.committer_tx_id.load(Relaxed), 0);
    }
}
