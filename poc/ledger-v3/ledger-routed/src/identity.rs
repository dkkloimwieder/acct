//! Committer identity slot registry — verbatim port from v21
//! `src/lib.rs` 651-743.
//!
//! Application-level analog of PG's `BackgroundWorkerData.slot[]`: each
//! running committer worker claims one entry at startup and releases at
//! shutdown. `CommitterQueueEntry.committer_bgw_slot` +
//! `committer_bgw_generation` (see `shmem.rs`) point into this array.
//! PID-recycling-safe liveness: a contender compares the entry's stored
//! generation against this slot's current generation. If the slot was
//! released and re-claimed by a different committer between then and
//! now, the generation has advanced and the stored identity no longer
//! matches → orphan.
//!
//! See `shmem.rs::CommitterIdentitySlot` for the struct definition (it
//! lives in shmem.rs because `CommitterQueue.identity_slots: [Slot; N]`
//! is a fixed-size array embedded in the shmem region).

use pgrx::pg_sys;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

use crate::shmem::{LEDGER_V3_COMMITTER_IDENTITY_SLOTS, COMMITTER_QUEUE};

/// Claim a free identity slot for the calling committer. Returns
/// `(slot_idx, generation)`. Two-pass scan: first reclaims slots marked
/// free (pid == 0); second takes over slots whose occupant is dead per
/// `kill(pid, 0)`. Panics if every slot is occupied by a live PID — the
/// `committer_count` GUC cap of 64 matches the array size.
pub(crate) fn claim_committer_identity() -> (u32, u32) {
    let queue = COMMITTER_QUEUE.share();
    let my_pid = unsafe { pg_sys::MyProcPid };
    for (idx, slot) in queue.identity_slots.iter().enumerate() {
        if slot
            .pid
            .compare_exchange(0, my_pid, AcqRel, Relaxed)
            .is_ok()
        {
            let generation = slot.generation.fetch_add(1, AcqRel) + 1;
            return (idx as u32, generation);
        }
    }
    for (idx, slot) in queue.identity_slots.iter().enumerate() {
        let occupant = slot.pid.load(Acquire);
        if occupant == 0 || occupant == my_pid {
            continue;
        }
        let alive = unsafe { libc::kill(occupant, 0) } == 0;
        if alive {
            continue;
        }
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(0);
        if errno != libc::ESRCH {
            continue;
        }
        if slot
            .pid
            .compare_exchange(occupant, my_pid, AcqRel, Relaxed)
            .is_ok()
        {
            let generation = slot.generation.fetch_add(1, AcqRel) + 1;
            return (idx as u32, generation);
        }
    }
    panic!(
        "no free committer identity slot (capacity {})",
        LEDGER_V3_COMMITTER_IDENTITY_SLOTS
    );
}

/// Release the calling committer's identity slot on clean shutdown.
/// Bumps generation first so any stale `CommitterQueueEntry`
/// referencing the prior identity no longer matches; then clears pid
/// to 0.
pub(crate) fn release_committer_identity(slot_idx: u32) {
    if (slot_idx as usize) >= LEDGER_V3_COMMITTER_IDENTITY_SLOTS {
        return;
    }
    let queue = COMMITTER_QUEUE.share();
    let slot = &queue.identity_slots[slot_idx as usize];
    slot.generation.fetch_add(1, AcqRel);
    slot.pid.store(0, Release);
}

/// Liveness check. Returns true iff the committer identified by
/// `(slot_idx, generation)` is currently alive: slot has a non-zero
/// pid, generation matches, and `kill(pid, 0)` succeeds. Returns false
/// on any sentinel (generation == 0 → unclaimed) or out-of-bounds
/// slot. Treats `kill` `EPERM` as alive (conservative).
pub(crate) fn is_committer_alive(slot_idx: u32, expected_generation: u32) -> bool {
    if expected_generation == 0 {
        return false;
    }
    if (slot_idx as usize) >= LEDGER_V3_COMMITTER_IDENTITY_SLOTS {
        return false;
    }
    let queue = COMMITTER_QUEUE.share();
    let slot = &queue.identity_slots[slot_idx as usize];
    let pid = slot.pid.load(Acquire);
    if pid == 0 {
        return false;
    }
    let generation = slot.generation.load(Acquire);
    if generation != expected_generation {
        return false;
    }
    let kill_rc = unsafe { libc::kill(pid, 0) };
    if kill_rc == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(0);
    errno != libc::ESRCH
}

#[cfg(not(feature = "pg_test"))]
#[allow(dead_code)]
fn _silence_unused_warnings_when_no_callers_yet() {
    let _ = claim_committer_identity;
    let _ = release_committer_identity;
    let _ = is_committer_alive;
    let _ = Relaxed;
}
