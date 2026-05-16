//! Stub router BGWorker. M1.3 only — Greedy Window Router at M3.1.
//!
//! Pulls ONE pending StagingEntry per tick, assembles a size-1
//! CommitterQueueEntry. Real packing (router_window_size,
//! batch_size_max, fairness backstop) lands at M3.x.
//!
//! ## Data-before-flag invariant (spec §1.6, §3.3)
//!
//! The router stores `superbatch_id` with **Release** semantics BEFORE
//! CAS-ing `valid` from 2→3. Readers (recovery sweep) use **Acquire**
//! when loading `superbatch_id` AFTER confirming `valid==3`. This is
//! the load-bearing ordering that makes recovery correct under router
//! crash between push and CAS. M3.1 preserves the same invariant; M5b
//! adds the crash-recovery sweep.

use crate::{
    COMMITTER_QUEUE, CommitterQueueEntry, POC_V21_COMMITTER_QUEUE_SIZE,
    POC_V21_STAGING_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::time::Duration;

/// Router BGWorker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_router_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // No SPI access required for the router; pure shmem orchestration.
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        loop {
            if !try_route_one() {
                break;
            }
        }
    }
}

/// Try to route exactly one pending staging entry into a size-1
/// SuperBatch. Returns true if work was done; false if no pending
/// entry or no free CommitterQueue slot.
fn try_route_one() -> bool {
    let staging_capacity = POC_V21_STAGING_QUEUE_SIZE as u32;
    let committer_capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;

    // 1. Scan staging for a pending (valid==1) entry; CAS 1→2 (processing).
    let claim = {
        let queue = STAGING_QUEUE.share();
        let head = queue.head.load(Relaxed);
        let mut found: Option<u32> = None;
        for i in 0..staging_capacity {
            let idx = ((head + i) % staging_capacity) as usize;
            let slot = &queue.entries[idx];
            if slot.valid.load(Relaxed) == 1
                && slot
                    .valid
                    .compare_exchange(1, 2, Acquire, Relaxed)
                    .is_ok()
            {
                found = Some(idx as u32);
                break;
            }
        }
        if let Some(idx) = found {
            queue.head.store((head + 1) % staging_capacity, Relaxed);
            // Read the fields we need before releasing the share lock.
            let slot = &queue.entries[idx as usize];
            let sku_pool_count = slot.sku_pool_count;
            let wip_pool_count = slot.wip_pool_count;
            let sku_pool_keys_offset = slot.sku_pool_keys_offset;
            let wip_pool_keys_offset = slot.wip_pool_keys_offset;
            Some((idx, sku_pool_count, wip_pool_count, sku_pool_keys_offset, wip_pool_keys_offset))
        } else {
            None
        }
    };

    let (staging_idx, sku_pool_count, wip_pool_count, sku_keys_off, wip_keys_off) =
        match claim {
            Some(t) => t,
            None => return false,
        };

    // 2. Allocate spillover-arena storage for the CommitterQueueEntry's
    // arrays: (a) staging_entry_offsets (single u32 = 4 bytes;
    // round up to 8), (b) deduplicated/sorted sku_pool_keys mirror
    // of the staging entry's, (c) same for wip_pool_keys.
    let (staging_offsets_off, sku_keys_copy_off, wip_keys_copy_off) = {
        let mut arena = SPILLOVER_ARENA.exclusive();
        let staging_off = arena.alloc(8); // single u32 padded
        let sku_off = if sku_pool_count > 0 {
            arena.alloc(sku_pool_count as u32 * 16)
        } else {
            Some(0u32)
        };
        let wip_off = if wip_pool_count > 0 {
            arena.alloc(wip_pool_count as u32 * 16)
        } else {
            Some(0u32)
        };
        match (staging_off, sku_off, wip_off) {
            (Some(s_off), Some(sku), Some(wip)) => {
                // Copy staging_idx (size-1 batch) into the staging_offsets
                // arena block.
                let idx_bytes = (staging_idx as u32).to_le_bytes();
                arena.write_bytes(s_off, &idx_bytes);
                // Copy pool keys from the staging-entry's blocks. For
                // size-1 SuperBatch these are already sorted and dedup'd
                // by the caller; M3.1 dedups across multiple envelopes.
                if sku_pool_count > 0 && sku_keys_off != 0 {
                    let bytes = arena.read_bytes(sku_keys_off, sku_pool_count as u32 * 16);
                    arena.write_bytes(sku, &bytes);
                }
                if wip_pool_count > 0 && wip_keys_off != 0 {
                    let bytes = arena.read_bytes(wip_keys_off, wip_pool_count as u32 * 16);
                    arena.write_bytes(wip, &bytes);
                }
                (s_off, sku, wip)
            }
            _ => {
                // Arena exhausted; CAS staging back to pending. The next
                // tick will retry.
                let queue = STAGING_QUEUE.share();
                let _ = queue.entries[staging_idx as usize]
                    .valid
                    .compare_exchange(2, 1, Release, Relaxed);
                return false;
            }
        }
    };

    // 3. Find free CommitterQueueEntry; write fields; CAS valid 0→1 (ready).
    let cq_idx: Option<u32> = {
        let mut queue = COMMITTER_QUEUE.exclusive();
        let tail = queue.tail.load(Relaxed);
        let mut found: Option<u32> = None;
        for i in 0..committer_capacity {
            let idx = ((tail + i) % committer_capacity) as usize;
            let slot = &queue.entries[idx];
            if slot.valid.load(Relaxed) == 0 {
                found = Some(idx as u32);
                break;
            }
        }
        if let Some(idx) = found {
            let superbatch_id = queue.next_superbatch_id.fetch_add(1, Relaxed) + 1;
            let entry = CommitterQueueEntry {
                valid: std::sync::atomic::AtomicU8::new(0),
                _pad: [0; 7],
                superbatch_id,
                envelope_count: 1,
                _pad_env: [0; 2],
                staging_entry_offsets: staging_offsets_off,
                sku_pool_keys_offset: sku_keys_copy_off,
                sku_pool_keys_count: sku_pool_count,
                _pad_sku: [0; 2],
                wip_pool_keys_offset: wip_keys_copy_off,
                wip_pool_keys_count: wip_pool_count,
                _pad_wip: [0; 2],
                committer_pid: std::sync::atomic::AtomicI32::new(0),
                _pad_pid: [0; 4],
                committer_acquired_at_ns: std::sync::atomic::AtomicU64::new(0),
                committer_tx_id: std::sync::atomic::AtomicU64::new(0),
                enqueued_at_micros: unsafe { pg_sys::GetCurrentTimestamp() as u64 },
            };
            write_committer_entry(&mut queue.entries[idx as usize], &entry);
            // CAS valid 0→1 (ready). M3.1 packed router does this once
            // per SuperBatch; here it's once per staging entry.
            let _ = queue.entries[idx as usize]
                .valid
                .compare_exchange(0, 1, Release, Relaxed);
            queue.tail.store((tail + 1) % committer_capacity, Relaxed);
            // Read superbatch_id back for staging stamping.
            queue.entries[idx as usize].committer_pid.store(0, Relaxed);
            // Return the idx so staging stamping below uses the right superbatch_id.
            // We need superbatch_id; read it back.
            let sb = queue.entries[idx as usize].superbatch_id;
            // Store it on the staging entry with Release semantics
            // BEFORE the CAS valid 2→3. This is the data-before-flag
            // invariant for the recovery sweep.
            {
                let staging = STAGING_QUEUE.share();
                let s = &staging.entries[staging_idx as usize];
                s.superbatch_id.store(sb, Release);
                let _ = s.valid.compare_exchange(2, 3, Release, Relaxed);
            }
            Some(idx)
        } else {
            None
        }
    };

    if cq_idx.is_none() {
        // CommitterQueue full; CAS staging back to pending.
        let queue = STAGING_QUEUE.share();
        let _ = queue.entries[staging_idx as usize]
            .valid
            .compare_exchange(2, 1, Release, Relaxed);
        // Free the arena blocks we allocated.
        let mut arena = SPILLOVER_ARENA.exclusive();
        if staging_offsets_off != 0 {
            arena.free(staging_offsets_off);
        }
        if sku_keys_copy_off != 0 {
            arena.free(sku_keys_copy_off);
        }
        if wip_keys_copy_off != 0 {
            arena.free(wip_keys_copy_off);
        }
        return false;
    }

    true
}

fn write_committer_entry(dst: &mut CommitterQueueEntry, src: &CommitterQueueEntry) {
    dst.superbatch_id = src.superbatch_id;
    dst.envelope_count = src.envelope_count;
    dst.staging_entry_offsets = src.staging_entry_offsets;
    dst.sku_pool_keys_offset = src.sku_pool_keys_offset;
    dst.sku_pool_keys_count = src.sku_pool_keys_count;
    dst.wip_pool_keys_offset = src.wip_pool_keys_offset;
    dst.wip_pool_keys_count = src.wip_pool_keys_count;
    dst.committer_pid.store(0, Relaxed);
    dst.committer_acquired_at_ns.store(0, Relaxed);
    dst.committer_tx_id.store(0, Relaxed);
    dst.enqueued_at_micros = src.enqueued_at_micros;
}

/// Synchronous router-tick helper exposed to tests + SQL: drains one
/// pending entry into a SuperBatch. Returns true if work was done.
#[pg_extern]
fn poc_v21_test_router_tick() -> bool {
    try_route_one()
}
