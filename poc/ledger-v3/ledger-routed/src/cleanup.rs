//! Cleanup step (design-v3 §5.4 step 11).
//!
//! ## Three CAS cases per staging entry
//!
//! The router's Phase 6 ordering is `commit_group_id.store(cg_id,
//! Release)` THEN `valid.compare_exchange(2, 3, Release, ...)` (see
//! `router::emit_commit_group`). The committer's pristine-replay
//! eject path is `eject_count.fetch_add(1, Release)` THEN
//! `valid.compare_exchange(3, 1, Release, ...)` + Release-store
//! `cg_id = 0`. Cleanup reads both atomics with Acquire to pair
//! against those Release stores:
//!
//!   1. **Success.** Slot is at `valid == 3`, `commit_group_id == our_cg_id`,
//!      `eject_count == 0`. CAS 3→0 succeeds; arena blocks freed.
//!
//!   2. **Ejected.** Slot is at `valid == 1`, `commit_group_id == 0`,
//!      `eject_count > 0`. Both CAS attempts fail (3→0 because
//!      valid != 3; 2→0 because eject_count != 0). Arena blocks
//!      left for the router to re-claim on the next tick.
//!
//!   3. **Router-died-mid-stamp.** Router crashed between
//!      `cg_id.store(...)` and CAS 2→3, so the slot is stuck at
//!      `valid == 2` with possibly-stale cg_id. eject_count is still
//!      0 because no committer has touched it. CAS 3→0 fails (valid
//!      != 3); CAS 2→0 fallback succeeds. Arena blocks freed —
//!      otherwise the slot leaks because the recovery sweep can't
//!      tell whether the entry made it into the CommitterQueue.
//!
//! ## cg_id guard
//!
//! Case 1's CAS is gated on `observed_cg == commit_group_id` to avoid
//! a race where: cleanup runs AFTER the committer's eject sub-tx
//! release; a concurrent router re-packs the entry (CAS 1→2 + new
//! cg_id + CAS 2→3); blind CAS 3→0 would free the NEW CommitGroup's
//! arena and silently abandon the staging entry. The guard makes
//! cleanup a no-op for entries that have already moved on to another
//! CommitGroup.
//!
//! ## Backpressure broadcast
//!
//! Each successful slot release calls `signal_staging_slot_freed`,
//! which Broadcasts on the queue's backpressure ConditionVariable so
//! a `ledger_enqueue_trx` caller blocked in `cv_wait_for_slot`
//! wakes immediately and retries.
//!
//! ## Lock ordering
//!
//! STAGING_QUEUE / COMMITTER_QUEUE and SPILLOVER_ARENA are held
//! SEQUENTIALLY, never nested. Each helper takes a single ref so
//! callers can scope guards independently:
//!
//!   - staging.share() guard → CAS + capture offsets → drop guard
//!   - arena.exclusive() guard → free captured offsets → drop guard
//!
//! Same shape for the CQ-cleanup branch (arena free first, then
//! committer.share() for the slot-finalization CAS sequence).

#![allow(dead_code)]

use crate::shmem::{
    COMMITTER_QUEUE, CommitterQueue, SPILLOVER_ARENA, STAGING_QUEUE, SpilloverArena, StagingQueue,
    signal_staging_slot_freed,
};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// CAS-release one staging entry's slot. Returns `Some((payload_off,
/// pool_keys_off))` if the slot was released (CAS 3→0 success on
/// sb-match, or CAS 2→0 fallback when `eject_count == 0`); `None`
/// otherwise (ejected, or already abandoned, or re-routed under a
/// different cg_id). Caller frees the returned arena offsets in a
/// separate `SPILLOVER_ARENA.exclusive()` scope.
pub(crate) fn try_release_staging_slot(
    staging_queue: &StagingQueue,
    s_idx: u32,
    commit_group_id: u64,
) -> Option<(u32, u32)> {
    let slot = &staging_queue.entries[s_idx as usize];
    let payload_off = slot.payload_offset;
    let pool_keys_off = slot.pool_keys_offset;
    let observed_cg = slot.commit_group_id.load(Acquire);
    let observed_eject = slot.eject_count.load(Acquire);
    let cas_ok = (observed_cg == commit_group_id
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
        Some((payload_off, pool_keys_off))
    } else {
        None
    }
}

/// Free the two arena blocks owned by a staging entry's payload.
/// Either offset may be 0 (sentinel for "no block allocated"); free
/// is skipped for those.
pub(crate) fn free_staging_arena_blocks(
    arena: &mut SpilloverArena,
    payload_off: u32,
    pool_keys_off: u32,
) {
    if payload_off != 0 {
        arena.free(payload_off);
    }
    if pool_keys_off != 0 {
        arena.free(pool_keys_off);
    }
}

/// Free the arena blocks owned by a CommitterQueueEntry
/// (staging_indices + deduplicated pool_keys union + optional tm09
/// pool_seqs). Any offset may be 0 (sentinel for "no block allocated").
pub(crate) fn free_committer_queue_arena(
    arena: &mut SpilloverArena,
    staging_offsets_off: u32,
    pool_keys_off: u32,
    pool_seqs_off: u32,
) {
    if staging_offsets_off != 0 {
        arena.free(staging_offsets_off);
    }
    if pool_keys_off != 0 {
        arena.free(pool_keys_off);
    }
    if pool_seqs_off != 0 {
        arena.free(pool_seqs_off);
    }
}

/// Finalize a CommitterQueueEntry: CAS valid 2→3 (completed) then
/// 3→0 (empty), and clear the identity / lease atomics. The
/// completed→empty CAS sequence gives a brief window where
/// observability can see `completed` before reuse.
pub(crate) fn finalize_committer_queue_slot(committer_queue: &CommitterQueue, cq_idx: u32) {
    let slot = &committer_queue.entries[cq_idx as usize];
    let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
    let _ = slot.valid.compare_exchange(3, 0, Release, Relaxed);
    slot.committer_bgw_slot.store(u32::MAX, Relaxed);
    slot.committer_bgw_generation.store(0, Relaxed);
    slot.committer_acquired_at_ns.store(0, Relaxed);
    slot.committer_tx_id.store(0, Relaxed);
}

/// Live wrapper: called by the committer after `COMMIT` on a
/// commit_group. For each staging entry: under STAGING_QUEUE.share(),
/// do the CAS + (on success) broadcast the backpressure CV + capture
/// the arena offsets; then drop the staging guard; then acquire
/// SPILLOVER_ARENA.exclusive() to free the captured offsets. After
/// all staging entries: free the CQ's own arena blocks, then under
/// COMMITTER_QUEUE.share() finalize the slot's valid CAS + identity
/// atomics.
///
/// Lock acquisition is strictly sequential — staging or committer
/// guards never wrap the arena guard. This avoids cross-lock deadlocks
/// against any future caller that might acquire arena.exclusive()
/// outer (e.g., the recovery sweep audit pass).
pub(crate) fn cleanup_after_commit_group(
    cq_idx: u32,
    commit_group_id: u64,
    staging_indices: &[u32],
    staging_offsets_off: u32,
    pool_keys_off: u32,
    pool_seqs_off: u32,
) {
    for &s_idx in staging_indices {
        let offsets = {
            let staging_guard = STAGING_QUEUE.share();
            let offsets = try_release_staging_slot(&staging_guard, s_idx, commit_group_id);
            if offsets.is_some() {
                signal_staging_slot_freed(&staging_guard);
            }
            offsets
        };
        if let Some((payload_off, pool_keys_off_staging)) = offsets {
            let mut arena_guard = SPILLOVER_ARENA.exclusive();
            free_staging_arena_blocks(&mut arena_guard, payload_off, pool_keys_off_staging);
        }
    }

    {
        let mut arena_guard = SPILLOVER_ARENA.exclusive();
        free_committer_queue_arena(
            &mut arena_guard,
            staging_offsets_off,
            pool_keys_off,
            pool_seqs_off,
        );
    }
    {
        let committer_guard = COMMITTER_QUEUE.share();
        finalize_committer_queue_slot(&committer_guard, cq_idx);
    }
}

// ── Unit tests for the three CAS cases ──────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
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
        cg_id: u64,
        eject_count: u32,
    ) -> (u32, u32) {
        let payload_off = arena.alloc(64).expect("alloc payload");
        let pool_keys_off = arena.alloc(16).expect("alloc pool_keys");
        let slot = &mut staging_queue.entries[s_idx as usize];
        slot.valid = AtomicU8::new(valid);
        slot.commit_group_id = AtomicU64::new(cg_id);
        slot.eject_count = AtomicU32::new(eject_count);
        slot.payload_offset = payload_off;
        slot.payload_length = 64;
        slot.pool_keys_offset = pool_keys_off;
        slot.pool_count = 2;
        (payload_off, pool_keys_off)
    }

    #[test]
    fn case_1_success_releases_slot_then_caller_frees_arena() {
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let cg_id: u64 = 42;
        populate_slot(&mut staging, &mut arena, 7, 3, cg_id, 0);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_slot(&staging, 7, cg_id);
        let (p, pk) = released.expect("CAS 3→0 should succeed on matching cg_id");

        assert_eq!(staging.entries[7].valid.load(Relaxed), 0);
        assert_eq!(arena.outstanding_allocs(), 2, "release does not free arena");

        free_staging_arena_blocks(&mut arena, p, pk);
        assert_eq!(arena.outstanding_allocs(), 0, "caller-side arena free");
    }

    #[test]
    fn case_2_ejected_leaves_slot_at_pending_and_arena_in_place() {
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let cg_id: u64 = 42;
        let (p, pk) = populate_slot(&mut staging, &mut arena, 5, 1, 0, 1);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_slot(&staging, 5, cg_id);

        assert!(released.is_none(), "ejected entry should not release");
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
        assert_eq!(staging.entries[5].payload_offset, p);
        assert_eq!(staging.entries[5].pool_keys_offset, pk);
    }

    #[test]
    fn case_3_router_died_mid_stamp_falls_back_to_cas_2_to_0() {
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let cleanup_cg_id: u64 = 42;
        populate_slot(&mut staging, &mut arena, 9, 2, 0, 0);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_slot(&staging, 9, cleanup_cg_id);
        let (p, pk) = released.expect("CAS 2→0 fallback should succeed");

        assert_eq!(staging.entries[9].valid.load(Relaxed), 0);
        free_staging_arena_blocks(&mut arena, p, pk);
        assert_eq!(arena.outstanding_allocs(), 0);
    }

    #[test]
    fn case_3_with_eject_in_flight_skips_2_to_0() {
        // Defensive variant: valid=2 BUT eject_count > 0 means an
        // eject is mid-flight (eject path bumps eject_count Release
        // BEFORE flipping valid 3→1). 2→0 must skip.
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let cleanup_cg_id: u64 = 42;
        populate_slot(&mut staging, &mut arena, 3, 2, cleanup_cg_id, 1);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_slot(&staging, 3, cleanup_cg_id);

        assert!(released.is_none(), "in-flight eject must block 2→0 fallback");
        assert_eq!(staging.entries[3].valid.load(Relaxed), 2);
        assert_eq!(arena.outstanding_allocs(), 2);
    }

    #[test]
    fn cg_id_mismatch_on_valid_3_skips_3_to_0_and_falls_through() {
        // Re-routing race: entry was completed by an earlier CommitGroup
        // (cg_id=42) then re-packed under a new cg_id=99. Cleanup for
        // the OLD cg_id=42 must NOT CAS 3→0 (would free the new
        // pack's arena). valid==3 also skips the 2→0 fallback.
        let mut staging = fresh_staging_queue();
        let mut arena = fresh_arena();
        let stale_cg_id: u64 = 42;
        let new_cg_id: u64 = 99;
        populate_slot(&mut staging, &mut arena, 1, 3, new_cg_id, 0);
        assert_eq!(arena.outstanding_allocs(), 2);

        let released = try_release_staging_slot(&staging, 1, stale_cg_id);

        assert!(released.is_none(), "stale cleanup must not free a re-routed slot");
        assert_eq!(staging.entries[1].valid.load(Relaxed), 3);
        assert_eq!(arena.outstanding_allocs(), 2);
    }

    #[test]
    fn free_committer_queue_arena_releases_all_blocks() {
        let mut arena = fresh_arena();
        let so = arena.alloc(32).expect("alloc staging_offsets");
        let pk = arena.alloc(48).expect("alloc pool_keys");
        let ps = arena.alloc(48).expect("alloc pool_seqs");
        assert_eq!(arena.outstanding_allocs(), 3);

        free_committer_queue_arena(&mut arena, so, pk, ps);
        assert_eq!(arena.outstanding_allocs(), 0);
    }

    #[test]
    fn free_committer_queue_arena_skips_zero_offsets() {
        let mut arena = fresh_arena();
        let so = arena.alloc(32).expect("alloc staging_offsets");
        assert_eq!(arena.outstanding_allocs(), 1);

        free_committer_queue_arena(&mut arena, so, 0, 0);
        assert_eq!(arena.outstanding_allocs(), 0);
    }

    #[test]
    fn free_committer_queue_arena_releases_seqs_independently() {
        // tm09: split-safe groups have pool_seqs_off = 0; free still works.
        let mut arena = fresh_arena();
        let so = arena.alloc(32).expect("alloc staging_offsets");
        let pk = arena.alloc(48).expect("alloc pool_keys");
        assert_eq!(arena.outstanding_allocs(), 2);

        free_committer_queue_arena(&mut arena, so, pk, 0);
        assert_eq!(arena.outstanding_allocs(), 0);
    }

    #[test]
    fn finalize_committer_queue_slot_cas_cycles_and_zeros_identity() {
        let mut committer = fresh_committer_queue();
        let cq_idx = 11u32;
        {
            let slot = &mut committer.entries[cq_idx as usize];
            slot.valid = AtomicU8::new(2);
            slot.committer_bgw_slot = AtomicU32::new(5);
            slot.committer_bgw_generation = AtomicU32::new(17);
            slot.committer_acquired_at_ns = AtomicU64::new(12345);
            slot.committer_tx_id = AtomicU64::new(99);
        }

        finalize_committer_queue_slot(&committer, cq_idx);

        let slot = &committer.entries[cq_idx as usize];
        assert_eq!(slot.valid.load(Relaxed), 0);
        assert_eq!(slot.committer_bgw_slot.load(Relaxed), u32::MAX);
        assert_eq!(slot.committer_bgw_generation.load(Relaxed), 0);
        assert_eq!(slot.committer_acquired_at_ns.load(Relaxed), 0);
        assert_eq!(slot.committer_tx_id.load(Relaxed), 0);
    }
}
