//! Staging queue push helper + observer utilities.
//!
//! M1.2 (acct-q3nm). The staging queue is a ring buffer of
//! `POC_V21_STAGING_QUEUE_SIZE` (=16384) `StagingEntry` slots.
//!
//! ## State machine (`StagingEntry.valid`)
//!
//! ```text
//!   0 empty       — slot is reusable
//!   1 pending     — enqueued by caller; router not yet aware
//!   2 processing  — router has claimed for SuperBatch assembly
//!   3 routed      — router has finalized SuperBatch; committer claims
//!   4 abandoned   — caller backend died or eject-exhausted
//! ```
//!
//! M1.2 ships only the 0→1 transition. Router (M3.1) drives 1→2 and
//! 2→3; committer (M1.3+) drives 3→0 at Step 14 post-commit cleanup.
//!
//! ## Find-free strategy
//!
//! Under the staging queue's exclusive `PgLwLock`, scan `entries[]`
//! linearly starting from `tail % capacity` looking for the first
//! slot with `valid==0`. Linear scan is O(capacity) worst-case;
//! M3+ may switch to a free-slot bitmap if measurement justifies.
//! For M1.2 the simple scan is sufficient and exact.

use crate::{POC_V21_STAGING_QUEUE_SIZE, StagingEntry, StagingQueue};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

/// Push a fully-prepared StagingEntry into the queue. Returns the
/// slot index on success, or `Err(StagingPushError::QueueFull)` if
/// no slot is currently free.
///
/// Caller holds the staging queue LWLock in EXCLUSIVE mode.
///
/// Slot fields are written under the lock; the CAS that flips
/// `valid` from 0→1 publishes the row to readers. M3+ readers observe
/// the slot via the ring head/tail and the per-slot valid byte.
pub fn push_entry(
    queue: &mut StagingQueue,
    entry: StagingEntry,
) -> Result<u32, StagingPushError> {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as u32;
    let tail = queue.tail.load(Relaxed);
    // Scan capacity slots starting from tail. The ring is a free-slot
    // pool, not a strict FIFO — the router consumes in seq order, not
    // slot order.
    for i in 0..capacity {
        let idx = ((tail + i) % capacity) as usize;
        let slot = &mut queue.entries[idx];
        let cur = slot.valid.load(Relaxed);
        if cur == 0 {
            // Write all non-atomic fields first, then publish via CAS
            // with Release. The router reads valid with Acquire so the
            // field writes happen-before the read.
            slot.request_seq = entry.request_seq;
            slot.correlation_id = entry.correlation_id;
            slot.user_tx_xid = entry.user_tx_xid;
            slot.event_type_id = entry.event_type_id;
            slot.payload_offset = entry.payload_offset;
            slot.payload_length = entry.payload_length;
            slot.sku_pool_count = entry.sku_pool_count;
            slot.wip_pool_count = entry.wip_pool_count;
            slot.sku_pool_keys_offset = entry.sku_pool_keys_offset;
            slot.wip_pool_keys_offset = entry.wip_pool_keys_offset;
            slot.enqueued_at_micros = entry.enqueued_at_micros;
            slot.backend_pid = entry.backend_pid;
            slot.superbatch_id.store(0, Relaxed);
            slot.eject_count.store(0, Relaxed);

            // CAS 0→1 with Release: any reader observing valid=1 with
            // Acquire sees all the above field writes.
            match slot.valid.compare_exchange(0, 1, Release, Relaxed) {
                Ok(_) => {
                    queue.tail.store((tail + i + 1) % capacity, Relaxed);
                    return Ok(idx as u32);
                }
                Err(_) => continue, // Lost race — try next slot.
            }
        }
    }
    Err(StagingPushError::QueueFull)
}

/// Linear scan for the next staging entry in state `pending` (valid==1).
/// Used by the dequeue-one helper in tests (simulates the router's
/// take-by-FIFO consumer side).
///
/// Returns Some(slot_index) on hit. Marks the slot via CAS 1→2
/// (Acquire on success).
pub fn take_pending(queue: &mut StagingQueue) -> Option<u32> {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as u32;
    let head = queue.head.load(Relaxed);
    for i in 0..capacity {
        let idx = ((head + i) % capacity) as usize;
        let slot = &queue.entries[idx];
        if slot.valid.load(Relaxed) == 1
            && slot
                .valid
                .compare_exchange(1, 2, Acquire, Relaxed)
                .is_ok()
        {
            queue.head.store((head + i + 1) % capacity, Relaxed);
            return Some(idx as u32);
        }
    }
    None
}

/// Reset a slot to empty (valid==0), freeing it for re-use. Called by
/// the committer at Step 14 post-commit cleanup; exposed for tests.
pub fn release_slot(queue: &mut StagingQueue, slot_index: u32) {
    let slot = &queue.entries[slot_index as usize];
    slot.valid.store(0, Release);
}

/// Count entries by state. Returns (empty, pending, processing, routed, abandoned).
pub fn state_counts(queue: &StagingQueue) -> (u32, u32, u32, u32, u32) {
    let mut counts = (0u32, 0u32, 0u32, 0u32, 0u32);
    for slot in queue.entries.iter() {
        match slot.valid.load(Relaxed) {
            0 => counts.0 += 1,
            1 => counts.1 += 1,
            2 => counts.2 += 1,
            3 => counts.3 += 1,
            4 => counts.4 += 1,
            _ => {}
        }
    }
    counts
}

#[derive(Debug, Clone, Copy)]
pub enum StagingPushError {
    QueueFull,
}
