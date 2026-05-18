//! Committer BGWorker + 5-step pipeline per spec §1.8.
//!
//! M1.3 (acct-wrwf). Single committer, FIFO only. Size-1 SuperBatches
//! (stub router). AVG + STD method dispatch lands at M2.1 + M2.2;
//! committer pool + CAS election at M4.1; per-envelope failure
//! isolation via sub-tx at M6.2.
//!
//! ## Atomic ordering policy (acct-gx1z.1.7 — Q9/Q8/B7)
//!
//! Authoritative reference: `poc/design_research/poc-v2.1.md` §5.2.
//! This block is the per-field cheatsheet; if it disagrees with the
//! spec, the spec wins.
//!
//! ### CommitterQueueEntry.valid (u8 state machine: 0→1→2→3→0)
//!
//! - `Release` on every CAS that advances the state (1→2, 2→3, 3→0,
//!   and the 2→0 router-mid-Phase-6-death fallback). Pairs with the
//!   `Acquire` reads in `claim_next_committer_entry` and
//!   `try_recover_orphan` so the claimer/rescuer sees every field
//!   the previous owner published.
//! - `Relaxed` on the cheap probe read at the top of
//!   `claim_next_committer_entry` (we re-confirm with the Acquire
//!   CAS before any state-affecting work).
//! - `Release` on the rare race-release in `claim_next_committer_entry`
//!   (slot.valid is left untouched; we only release committer_pid).
//!
//! ### CommitterQueueEntry.committer_pid (i32 ownership claim)
//!
//! - `Acquire` on the claim CAS (0 → MyProcPid). Pairs with the
//!   orphan-recovery rescuer's `Acquire` read of `valid==2` —
//!   together they form the "ownership before state advance"
//!   pattern. Collapsing to a single CAS on `valid` would lose this
//!   invariant; see acct-gx1z.1's pushback on reviewer claim B2.
//! - `Relaxed` on the race-release-after-failure path: no other
//!   thread observes the timing, and the next claim's Acquire CAS
//!   re-establishes happens-before. Documented intentional weakening.
//!
//! ### StagingEntry / CommitterQueueEntry.eject_count (u32)
//!
//! - `Release` on `fetch_add(1, …)` (router-side increment). Ensures
//!   the matching `Acquire` load in Step 14's CAS-2→0 fallback sees
//!   every concurrent eject. Invariant: a reader that observes
//!   `eject_count == 0` AND `valid == 2` is guaranteed no eject is
//!   in flight, because the eject path bumps the counter BEFORE
//!   touching valid. See the inline comment around the 2→0 CAS.
//!
//! ### Stats counters (method_dispatch_counts, method_latency_hist,
//!     committer_pipeline_ns_total, committer_pipeline_count, …)
//!
//! - `Relaxed` everywhere. Counters are monotonic; readers
//!   (`poc_v21_*_stats` SQL fns) are eventually-consistent
//!   observability surfaces, not synchronization points. Reading
//!   them with stronger ordering would add cost without correctness
//!   benefit.
//!
//! ### StagingEntry.superbatch_id (u64 + AtomicU64)
//!
//! - `Release` on router-write, `Acquire` on committer-read. The
//!   load-bearing data-before-flag rule: router writes
//!   `payload_offset`, `sku_pool_keys_offset`, etc. THEN releases
//!   `superbatch_id`; committer acquires `superbatch_id` THEN reads
//!   the data fields. Cross-module — see also `router.rs` §1.6.

use crate::avg::AVG_METHOD;
use crate::cost_method::{
    PocV21ApplyResult, PocV21CostMethod, PocV21Event, PocV21EventResult, PocV21EventType,
    PocV21Snapshot, SkuPoolState,
};
use crate::fifo::FIFO_METHOD;
use crate::standard::STANDARD_METHOD;
use crate::{
    COMMITTER_QUEUE, POC_V21_COMMITTER_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE,
    caller_tx_timeout_ms_now, committer_lease_ms_now, max_eject_count_now,
    signal_staging_slot_freed, skip_wip_locks, target_database_str,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::time::Duration;

/// M7.1 (acct-byue): map a method string ("fifo" / "avg" / "std") to the
/// per-method histogram index. Returns None for unknown methods (defensive
/// — keeps the unknown-method case out of the histogram rather than
/// silently bucketing into FIFO).
fn method_index(method: &str) -> Option<usize> {
    match method {
        "fifo" => Some(0),
        "avg" => Some(1),
        "std" => Some(2),
        _ => None,
    }
}

/// M7.1 (acct-byue): bucket a nanosecond latency into the per-method
/// log2-spaced histogram. Bucket i covers [2^(9+i), 2^(10+i)) ns; bucket
/// 15 is the >= 16ms overflow. Increments dispatch_count + (optionally)
/// error_count + the latency bucket.
fn record_method_latency(method: &str, elapsed_ns: u64, errored: bool) {
    let Some(mi) = method_index(method) else {
        return;
    };
    let queue = COMMITTER_QUEUE.share();
    queue.method_dispatch_counts[mi].fetch_add(1, Relaxed);
    if errored {
        queue.method_error_counts[mi].fetch_add(1, Relaxed);
    }
    // Bucket index: clamp(floor(log2(ns)) - 9, 0..=15). For ns < 512 we
    // floor to bucket 0; for ns >= 16ms we cap at 15.
    let bucket = if elapsed_ns < 512 {
        0
    } else {
        let lg = 63 - elapsed_ns.leading_zeros() as usize;
        let raw = lg.saturating_sub(9);
        raw.min(15)
    };
    queue.method_latency_hist[mi][bucket].fetch_add(1, Relaxed);
}

/// Committer BGWorker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_committer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // M5d.1 (acct-y0bp): wait for postmaster-startup recovery to finish
    // before claiming any work. See router.rs for rationale.
    crate::router::wait_for_recovery_complete();
    let dbname = target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        loop {
            let claim = claim_next_committer_entry();
            match claim {
                Some(cq_idx) => {
                    BackgroundWorker::transaction(|| {
                        let _ = process_superbatch(cq_idx);
                    });
                    // M3.3 (acct-8xyj): committer drain observability.
                    // Increment regardless of process_superbatch outcome
                    // — a failed batch still consumed a CommitterQueueEntry.
                    let queue = COMMITTER_QUEUE.share();
                    queue.committer_drains_total.fetch_add(1, Relaxed);
                }
                None => break,
            }
        }
        // M5a.1 (acct-lefr): scan for orphaned in-flight entries
        // whose owning committer died. Recovery uses lease staleness
        // + kill(pid, 0) liveness as the gating signal. On rescue
        // success the queue entry is reset to valid=1 so the next
        // committer claim can re-execute Steps 2-5 (dedup-lookup at
        // Step 2.5 catches any rows the dead committer already
        // committed before death).
        while try_recover_orphan() > 0 {}
    }
}

/// Scan the committer queue for orphaned entries (valid==2 with a
/// dead committer_pid) and reset them to valid==1 for re-processing.
/// Returns the number of entries recovered. Spec §3.2.
///
/// Detection signal (both must hold):
///   1. Lease staleness: now - committer_acquired_at_ns >
///      committer_lease_ms × 1_000_000.
///   2. Process liveness: kill(committer_pid, 0) returns ESRCH
///      (errno=3). EPERM (errno=1) is treated as alive (process
///      exists but we lack permission to signal) — safer to assume
///      alive than risk a false-positive rescue.
///
/// Rescue action: CAS committer_pid old_dead → MyProcPid (single CAS;
/// loses to other rescuers gracefully). On success, reset the entry
/// to valid=1 and committer_pid=0. The next BGWorker tick claims it
/// normally via claim_next_committer_entry; Step 2.5 dedup-lookup
/// guards against duplicates if the dead committer's tx happened to
/// commit before death (rare but possible).
///
/// Idempotent under concurrent rescuers (CAS election, same shape as
/// M4.1 committer claim).
pub(crate) fn try_recover_orphan() -> u32 {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let my_pid = unsafe { pg_sys::MyProcPid };
    let now_ns = crate::now_ns();
    let lease_ns = committer_lease_ms_now().max(1) as u64 * 1_000_000;

    let mut recovered: u32 = 0;
    for i in 0..capacity {
        let slot = &queue.entries[i as usize];
        if slot.valid.load(Relaxed) != 2 {
            continue;
        }
        let old_pid = slot.committer_pid.load(Relaxed);
        if old_pid == 0 || old_pid == my_pid {
            continue;
        }
        let acquired_at = slot.committer_acquired_at_ns.load(Relaxed);
        if now_ns.saturating_sub(acquired_at) <= lease_ns {
            continue; // not stale yet
        }
        // Liveness check via signal-0. ESRCH (errno 3) → process gone.
        let kill_rc = unsafe { libc::kill(old_pid, 0) };
        if kill_rc == 0 {
            continue; // still alive
        }
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        if errno != libc::ESRCH {
            // EPERM or other — be conservative, leave alone.
            continue;
        }
        // Try to claim the orphan. CAS old_pid → my_pid.
        if slot
            .committer_pid
            .compare_exchange(old_pid, my_pid, Acquire, Relaxed)
            .is_err()
        {
            continue; // another rescuer won
        }
        // We own the rescue. Reset the entry for re-processing.
        slot.committer_acquired_at_ns.store(0, Relaxed);
        slot.committer_tx_id.store(0, Relaxed);
        slot.committer_pid.store(0, Release);
        // valid 2→1 with Release ensures the pid reset is visible
        // before any subsequent claimer sees valid==1.
        let _ = slot
            .valid
            .compare_exchange(2, 1, Release, Relaxed);
        recovered += 1;
        // M7.1 (acct-byue): record the takeover for committer-pool
        // observability (distinct from claim_count — takeovers indicate
        // committer death + rescue, not normal claim path).
        queue.committer_takeover_count.fetch_add(1, Relaxed);
    }
    recovered
}

/// Test-only: synthetically inject an orphaned entry into an EMPTY
/// slot by stamping valid=2 + fake committer_pid + stale acquired_at.
/// Bypasses the valid==1 precondition so tests can exercise the
/// orphan-recovery mechanism without racing live router/committer
/// flow. Returns false if the target slot is not currently valid==0.
///
/// `fake_pid` should be a definitely-dead PID (i32::MAX is the
/// conventional choice — exceeds typical Linux PID_MAX). The kill(0)
/// liveness check will return ESRCH and the recovery path engages.
///
/// `lease_offset_ms` is subtracted from `now` to force lease
/// staleness; pass `committer_lease_ms × 2` for safety.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_inject_orphan_into_empty(
    slot_idx: i64,
    fake_pid: i32,
    lease_offset_ms: i64,
) -> bool {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if slot_idx < 0 || slot_idx >= capacity {
        return false;
    }
    let slot = &queue.entries[slot_idx as usize];
    if slot.valid.compare_exchange(0, 2, Acquire, Relaxed).is_err() {
        return false;
    }
    slot.committer_pid.store(fake_pid, Relaxed);
    let now_ns = crate::now_ns();
    let stale_ns = now_ns.saturating_sub((lease_offset_ms.max(0) as u64) * 1_000_000);
    slot.committer_acquired_at_ns.store(stale_ns, Relaxed);
    true
}

/// Test-only: read a CommitterQueueEntry's current state as a string
/// tuple "(valid, committer_pid)". Useful for asserting state
/// transitions in orphan-recovery tests.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_slot_state(slot_idx: i64) -> String {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if slot_idx < 0 || slot_idx >= capacity {
        return "out_of_range".to_string();
    }
    let slot = &queue.entries[slot_idx as usize];
    format!(
        "({}, {})",
        slot.valid.load(Relaxed),
        slot.committer_pid.load(Relaxed)
    )
}

/// Test-only: force-reset a slot to valid==0 (empty). Used in
/// test cleanup after synthetic orphan injection so the real
/// committer pool doesn't try to process an artifact slot. Returns
/// the previous valid value.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_force_reset_slot(slot_idx: i64) -> i32 {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if slot_idx < 0 || slot_idx >= capacity {
        return -1;
    }
    let slot = &queue.entries[slot_idx as usize];
    let prev = slot.valid.swap(0, Release);
    slot.committer_pid.store(0, Relaxed);
    slot.committer_acquired_at_ns.store(0, Relaxed);
    slot.committer_tx_id.store(0, Relaxed);
    prev as i32
}

/// Test-only: synchronously run one orphan-recovery sweep. Returns
/// the count of entries recovered.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_orphan_recover_tick() -> i64 {
    try_recover_orphan() as i64
}

/// Test-only: read the committer queue head. Used by E1's
/// head-advance acceptance test (acct-gx1z.1.1).
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_committer_head_get() -> i64 {
    COMMITTER_QUEUE.share().head.load(Relaxed) as i64
}

/// Test-only: set the committer queue head to a specific value.
/// Used by E1's head-advance acceptance test to fix the starting
/// position before injecting a claimable entry off-head.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_committer_head_set(value: i64) {
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let v = (value.rem_euclid(capacity as i64)) as u32;
    COMMITTER_QUEUE.share().head.store(v, Relaxed);
}

/// Test-only: inject a claimable (valid==1) entry at the given slot
/// with the minimum fields claim_next_committer_entry consults
/// (valid + committer_pid). All other CommitterQueueEntry fields
/// stay at their resident defaults. Returns false if the slot is
/// not currently valid==0.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_inject_claimable_entry(slot_idx: i64) -> bool {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if slot_idx < 0 || slot_idx >= capacity {
        return false;
    }
    let slot = &queue.entries[slot_idx as usize];
    slot.committer_pid.store(0, Relaxed);
    slot.valid
        .compare_exchange(0, 1, Release, Relaxed)
        .is_ok()
}

/// Test-only: run claim_next_committer_entry once. Returns the
/// slot index claimed, or -1 if no claim was made. Used by E1's
/// head-advance acceptance test to exercise claim without invoking
/// the full pipeline (which expects staging-entry data).
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_claim_committer_entry() -> i64 {
    match claim_next_committer_entry() {
        Some(idx) => idx as i64,
        None => -1,
    }
}

/// CAS-claim the next valid==1 CommitterQueueEntry. Returns its
/// slot index on success.
fn claim_next_committer_entry() -> Option<u32> {
    let queue = COMMITTER_QUEUE.share();
    let head = queue.head.load(Relaxed);
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let my_pid = unsafe { pg_sys::MyProcPid };
    for i in 0..capacity {
        let idx = ((head + i) % capacity) as usize;
        let slot = &queue.entries[idx];
        if slot.valid.load(Relaxed) == 1
            && slot.committer_pid.compare_exchange(0, my_pid, Acquire, Relaxed).is_ok()
        {
            if slot
                .valid
                .compare_exchange(1, 2, Acquire, Relaxed)
                .is_ok()
            {
                let now_ns = crate::now_ns();
                slot.committer_acquired_at_ns.store(now_ns, Relaxed);
                // Advance past the winning slot (head + i + 1), not just
                // head + 1. Otherwise head drifts behind the actual claim
                // position when i > 0 and the next tick re-scans the
                // empty slots between [head, idx).
                queue.head.store((head + i + 1) % capacity, Relaxed);
                // M7.1 (acct-byue): record the CAS-win for committer-pool
                // throughput observability.
                queue.committer_claim_count.fetch_add(1, Relaxed);
                return Some(idx as u32);
            } else {
                // Rare race: another committer flipped valid first; release
                // our pid claim.
                slot.committer_pid.store(0, Relaxed);
            }
        }
    }
    None
}

/// Run the 5-step pipeline for one CommitterQueueEntry.
fn process_superbatch(cq_idx: u32) -> Result<(), String> {
    // Snapshot the CommitterQueueEntry's fields we need.
    let (superbatch_id, envelope_count, staging_offsets_off, sku_keys_off, sku_count, wip_off, wip_count) = {
        let queue = COMMITTER_QUEUE.share();
        let slot = &queue.entries[cq_idx as usize];
        (
            slot.superbatch_id,
            slot.envelope_count,
            slot.staging_entry_offsets,
            slot.sku_pool_keys_offset,
            slot.sku_pool_keys_count,
            slot.wip_pool_keys_offset,
            slot.wip_pool_keys_count,
        )
    };

    // Read staging-entry indices from spillover arena.
    let staging_indices: Vec<u32> = {
        let arena = SPILLOVER_ARENA.share();
        let bytes = arena.read_bytes(staging_offsets_off, envelope_count as u32 * 4);
        bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect()
    };

    // Read sku_pool_keys from arena.
    let sku_pool_keys: Vec<(i64, i64)> = if sku_count > 0 {
        let arena = SPILLOVER_ARENA.share();
        let bytes = arena.read_bytes(sku_keys_off, sku_count as u32 * 16);
        bytes
            .chunks_exact(16)
            .map(|c| {
                let a = i64::from_le_bytes(c[0..8].try_into().unwrap());
                let b = i64::from_le_bytes(c[8..16].try_into().unwrap());
                (a, b)
            })
            .collect()
    } else {
        Vec::new()
    };

    // Read wip_pool_keys from arena (M4.1 acct-1c23 — two-domain
    // lex-locking adds the symmetric WIP path).
    let wip_pool_keys: Vec<(i64, i64)> = if wip_count > 0 && wip_off != 0 {
        let arena = SPILLOVER_ARENA.share();
        let bytes = arena.read_bytes(wip_off, wip_count as u32 * 16);
        bytes
            .chunks_exact(16)
            .map(|c| {
                let a = i64::from_le_bytes(c[0..8].try_into().unwrap());
                let b = i64::from_le_bytes(c[8..16].try_into().unwrap());
                (a, b)
            })
            .collect()
    } else {
        Vec::new()
    };

    // Build events from each staging entry's payload. M6.1: wo_complete
    // expands into K+1 events per envelope. Parse errors per-envelope
    // mark just that correlation_id failed; surviving envelopes still
    // run through the pipeline. The submission_status INSERT happens
    // outside the sub-tx because (a) the caller user-tx is committed
    // (we passed the Step 2.45 check is moot — parse runs before that),
    // and (b) keeping it in the worker-tx ensures it survives sub-tx
    // rollback.
    let mut events: Vec<PocV21Event> = Vec::with_capacity(staging_indices.len());
    let mut parse_errors: Vec<(pgrx::Uuid, String)> = Vec::new();
    for &s_idx in &staging_indices {
        let cid = {
            let queue = STAGING_QUEUE.share();
            pgrx::Uuid::from_bytes(queue.entries[s_idx as usize].correlation_id)
        };
        match read_event_from_staging(s_idx) {
            Ok(v) => events.extend(v),
            Err(e) => parse_errors.push((cid, e)),
        }
    }
    for (cid, err) in &parse_errors {
        let detail =
            pgrx::JsonB(serde_json::json!({ "phase": "payload_parse", "detail": err }));
        let _ = Spi::run_with_args(
            "INSERT INTO poc_v21_submission_status \
                (correlation_id, state, enqueued_at, processed_at, error_code, error_detail) \
             VALUES ($1, 'failed', now(), now(), $2, $3) \
             ON CONFLICT (correlation_id) DO UPDATE SET \
                state='failed', processed_at=now(), \
                error_code=EXCLUDED.error_code, error_detail=EXCLUDED.error_detail",
            &[(*cid).into(), "payload_parse_error".into(), detail.into()],
        );
    }

    // Open the sub-tx that owns committer_tx_id + the bulk INSERTs.
    let savepoint_name = CString::new(format!("poc_v21_committer_sb_{superbatch_id}")).unwrap();
    unsafe {
        pg_sys::BeginInternalSubTransaction(savepoint_name.as_ptr());
    }

    // M7.2 (acct-fln5): time the whole pipeline body (Step 2 through
    // Step 12) for §5.6 B2 (SPI-time) classifier. Most of this duration
    // is SPI; in-memory dispatch time is recoverable separately via
    // method_latency_hist sums.
    let pipeline_t0 = std::time::Instant::now();
    let pipeline_result = run_pipeline_inside_subtx(
        cq_idx,
        superbatch_id,
        &staging_indices,
        &sku_pool_keys,
        &wip_pool_keys,
        &events,
    );
    let pipeline_ns = pipeline_t0.elapsed().as_nanos() as u64;
    let cq = COMMITTER_QUEUE.share();
    cq.committer_pipeline_ns_total.fetch_add(pipeline_ns, Relaxed);
    cq.committer_pipeline_count.fetch_add(1, Relaxed);

    match pipeline_result {
        Ok(_) => {
            unsafe { pg_sys::ReleaseCurrentSubTransaction() };
        }
        Err(err) => {
            unsafe { pg_sys::RollbackAndReleaseCurrentSubTransaction() };
            // M7.1 (acct-byue): whole-batch failure (sub-tx aborted on
            // bulk INSERT error). Distinct from per-envelope failures
            // which mark individual `submission_status` rows but don't
            // abort the sub-tx.
            COMMITTER_QUEUE
                .share()
                .committer_tx_failures
                .fetch_add(1, Relaxed);
            // Mark all envelopes failed.
            for event in &events {
                let corr = event.correlation_id;
                let detail = pgrx::JsonB(serde_json::json!({ "phase": "pipeline", "detail": err }));
                let _ = Spi::run_with_args(
                    "INSERT INTO poc_v21_submission_status \
                       (correlation_id, state, enqueued_at, processed_at, error_code, error_detail) \
                     VALUES ($1, 'failed', now(), now(), $2, $3) \
                     ON CONFLICT (correlation_id) DO UPDATE SET \
                       state='failed', processed_at=now(), \
                       error_code=EXCLUDED.error_code, error_detail=EXCLUDED.error_detail",
                    &[corr.into(), "pipeline_error".into(), detail.into()],
                );
            }
        }
    }

    // STEP 14: cleanup. Free staging arena blocks, reset staging slots,
    // mark CommitterQueueEntry completed. superbatch_id passed in so
    // the cleanup can distinguish our slots from slots a concurrent
    // router has re-routed into a NEW SuperBatch (M5c.2 race fix).
    cleanup_after_superbatch(
        cq_idx,
        superbatch_id,
        &staging_indices,
        staging_offsets_off,
        sku_keys_off,
    );
    Ok(())
}

/// Steps 2–13 inside the active sub-transaction.
fn run_pipeline_inside_subtx(
    cq_idx: u32,
    superbatch_id: u64,
    staging_indices: &[u32],
    sku_pool_keys: &[(i64, i64)],
    wip_pool_keys: &[(i64, i64)],
    events: &[PocV21Event],
) -> Result<(), String> {
    // STEP 2: locks. Two-domain lex-locking — SKU pool keys always,
    // WIP pool keys gated on poc_v21.skip_wip_locks (M4.1 acct-1c23,
    // §1.5). For each domain: INSERT ON CONFLICT DO NOTHING (creates
    // row if missing), then SELECT FOR UPDATE in lex order.
    if !sku_pool_keys.is_empty() {
        let sku_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.0).collect();
        let location_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.1).collect();
        Spi::run_with_args(
            "INSERT INTO poc_v21_pool_locks (sku_id, location_id) \
             SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id) \
             ON CONFLICT (sku_id, location_id) DO NOTHING",
            &[sku_ids.clone().into(), location_ids.clone().into()],
        )
        .map_err(|e| format!("pool_locks INSERT: {e}"))?;
        Spi::run_with_args(
            "SELECT 1 FROM poc_v21_pool_locks \
             WHERE (sku_id, location_id) IN (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
             ORDER BY sku_id, location_id \
             FOR UPDATE",
            &[sku_ids.into(), location_ids.into()],
        )
        .map_err(|e| format!("pool_locks SELECT FOR UPDATE: {e}"))?;
    }
    if !wip_pool_keys.is_empty() && !skip_wip_locks() {
        let wo_ids: Vec<i64> = wip_pool_keys.iter().map(|k| k.0).collect();
        let op_ids: Vec<i64> = wip_pool_keys.iter().map(|k| k.1).collect();
        Spi::run_with_args(
            "INSERT INTO poc_v21_wip_pool_locks (work_order_id, operation_id) \
             SELECT work_order_id, operation_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(work_order_id, operation_id) \
             ON CONFLICT (work_order_id, operation_id) DO NOTHING",
            &[wo_ids.clone().into(), op_ids.clone().into()],
        )
        .map_err(|e| format!("wip_pool_locks INSERT: {e}"))?;
        Spi::run_with_args(
            "SELECT 1 FROM poc_v21_wip_pool_locks \
             WHERE (work_order_id, operation_id) IN (SELECT work_order_id, operation_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(work_order_id, operation_id)) \
             ORDER BY work_order_id, operation_id \
             FOR UPDATE",
            &[wo_ids.into(), op_ids.into()],
        )
        .map_err(|e| format!("wip_pool_locks SELECT FOR UPDATE: {e}"))?;
    }

    // Force XID allocation via a tiny no-op write (option Q-D(b) — robust
    // across pg_current_xact_id_if_assigned signature ambiguity). The
    // pool_locks INSERT above already triggers XID assignment under
    // ordinary cases (ON CONFLICT DO NOTHING with a non-conflicting row),
    // but the all-conflict case (re-entering a pool) is also possible.
    // Use a dummy LOCAL temp table or a session_lock; simplest is
    // pg_current_xact_id() (always returns; allocates if needed).
    let committer_tx_id: u64 = Spi::get_one("SELECT pg_current_xact_id()::text::bigint")
        .map_err(|e| format!("get committer_tx_id: {e}"))?
        .unwrap_or(0i64) as u64;

    // Store committer_tx_id on the CommitterQueueEntry.
    {
        let queue = COMMITTER_QUEUE.share();
        queue.entries[cq_idx as usize]
            .committer_tx_id
            .store(committer_tx_id, Relaxed);
    }

    // STEP 2.45 (M5c.2 acct-1hyx): caller-tx coupling check.
    //
    // THE central correctness rule of v2.1 (spec §3.11): the committer
    // NEVER sleeps waiting for a caller's user-tx. For each distinct
    // user_tx_xid in this batch we read pg_xact_status:
    //
    //   - committed  → keep envelope; downstream Steps 3–5 process it.
    //   - aborted    → mark envelope failed via lazy INSERT into
    //                  poc_v21_submission_status (the caller_intx-mode
    //                  'queued' row was rolled back with the caller's
    //                  user-tx, so we create the 'failed' row now).
    //   - in_progress → EJECT. CAS staging.valid 3→1, reset
    //                  superbatch_id to 0 (Release), increment
    //                  staging.eject_count. The router will re-pick
    //                  on a future tick. Once eject_count exceeds
    //                  max_eject_count OR enqueued duration exceeds
    //                  caller_tx_timeout_ms, terminal-fail the envelope
    //                  with caller_tx_eject_exhausted / caller_tx_timeout.
    //
    // Filter `events` to the kept (caller_tx='committed') set so
    // every downstream step (dedup, hydrate, dispatch, bulk insert,
    // status update) operates on the surviving envelopes only.
    //
    // CAS-3→1 rollback on the staging side pairs with Step 14's
    // CAS-failure-skip cleanup: ejected staging entries leave their
    // arena blocks in place so the next router pick can reuse them.
    let kept_events: Vec<PocV21Event> = {
        let mut unique_xids: std::collections::HashSet<u64> =
            std::collections::HashSet::new();
        for e in events {
            unique_xids.insert(e.user_tx_xid);
        }
        let xid_strs: Vec<String> = unique_xids.iter().map(|x| x.to_string()).collect();
        let mut xid_status: HashMap<u64, &'static str> = HashMap::new();
        if !xid_strs.is_empty() {
            let rows: Vec<(String, Option<String>)> = Spi::connect(|client| {
                let mut out: Vec<(String, Option<String>)> = Vec::new();
                let mut t = client
                    .select(
                        "SELECT x.s::xid8::text, pg_xact_status(x.s::xid8) \
                           FROM UNNEST($1::text[]) AS x(s)",
                        None,
                        &[xid_strs.into()],
                    )
                    .ok()?;
                while let Some(row) = t.next() {
                    let x: String = row.get::<String>(1).ok()??;
                    let s: Option<String> = row.get::<String>(2).ok()?;
                    out.push((x, s));
                }
                Some(out)
            })
            .unwrap_or_default();
            for (x, s) in rows {
                let xid: u64 = x.parse().unwrap_or(0);
                // pg_xact_status returns 'in progress' / 'committed' /
                // 'aborted' / NULL. NULL = unknown (clog wraparound or
                // very recent xid); treat as 'committed' defensively
                // — re-processing a committed-but-unknown xid is safe
                // (dedup-lookup catches duplicates).
                let status: &'static str = match s.as_deref() {
                    Some("committed") => "committed",
                    Some("aborted") => "aborted",
                    Some("in progress") => "in_progress",
                    _ => "committed",
                };
                xid_status.insert(xid, status);
            }
        }

        let max_ej = max_eject_count_now() as i32;
        let caller_timeout_us = (caller_tx_timeout_ms_now() as u64) * 1000;
        let now_us = crate::now_us();

        let mut kept: Vec<PocV21Event> = Vec::with_capacity(events.len());
        // Aborted callers get a lazy INSERT into submission_status —
        // their 'queued' INSERT rolled back with the caller's tx so
        // PG's unique-index check sees a dead tuple, no XactLockTableWait,
        // INSERT proceeds cleanly.
        let mut aborted_failed: Vec<(pgrx::Uuid, &'static str)> = Vec::new();
        let mut requeue: Vec<u32> = Vec::new();
        for (i, event) in events.iter().enumerate() {
            let status = xid_status
                .get(&event.user_tx_xid)
                .copied()
                .unwrap_or("committed");
            match status {
                "aborted" => {
                    aborted_failed.push((event.correlation_id, "caller_tx_aborted"));
                }
                "in_progress" => {
                    let s_idx = staging_indices[i];
                    let queue = STAGING_QUEUE.share();
                    let slot = &queue.entries[s_idx as usize];
                    let prev = slot.eject_count.fetch_add(1, Release);
                    // M7.2 (acct-fln5): global eject counter for the
                    // classifier (high eject rate suggests caller-side
                    // contention forcing committer re-routes).
                    COMMITTER_QUEUE
                        .share()
                        .eject_total_count
                        .fetch_add(1, Relaxed);
                    let new_count = (prev as i32).saturating_add(1);
                    let enqueued_at = slot.enqueued_at_micros;
                    drop(queue);
                    let elapsed_us = now_us.saturating_sub(enqueued_at);
                    if new_count > max_ej || elapsed_us > caller_timeout_us {
                        // Terminal-fail an in_progress caller. We do NOT
                        // INSERT into submission_status here: the caller's
                        // 'queued' row is still uncommitted in their tx,
                        // PG's unique-index conflict check would acquire
                        // XactLockTableWait on the caller's xid, and the
                        // committer would block — violating
                        // I-committer-non-blocking (spec §3.11). Instead
                        // we drop the envelope: no requeue (no CAS 3→1),
                        // no submission_status write. Step 14 cleanup's
                        // CAS 3→0 frees the staging slot's arena
                        // normally. The caller's 'queued' row, if it
                        // eventually materializes (caller commits), is
                        // an orphan — application-layer reconciliation
                        // handles that. This is a documented PoC
                        // limitation; production may add an out-of-band
                        // committer_terminal_decisions table to surface
                        // the failure to callers post-commit.
                    } else {
                        requeue.push(s_idx);
                    }
                }
                _ => kept.push(event.clone()),
            }
        }

        for (corr, code) in &aborted_failed {
            let detail =
                pgrx::JsonB(serde_json::json!({ "phase": "caller_tx_check", "detail": code }));
            Spi::run_with_args(
                "INSERT INTO poc_v21_submission_status \
                   (correlation_id, state, enqueued_at, processed_at, error_code, error_detail, committer_tx_id, superbatch_id) \
                 VALUES ($1, 'failed', now(), now(), $2, $3, $4, $5) \
                 ON CONFLICT (correlation_id) DO UPDATE SET \
                   state='failed', processed_at=now(), \
                   error_code=EXCLUDED.error_code, error_detail=EXCLUDED.error_detail, \
                   committer_tx_id=EXCLUDED.committer_tx_id, superbatch_id=EXCLUDED.superbatch_id",
                &[
                    (*corr).into(),
                    (*code).into(),
                    detail.into(),
                    (committer_tx_id as i64).into(),
                    (superbatch_id as i64).into(),
                ],
            )
            .map_err(|e| format!("caller_tx_check status INSERT: {e}"))?;
        }

        // Roll non-terminal ejected staging entries back to pending.
        //
        // Just CAS valid 3→1 (Release). DO NOT reset superbatch_id —
        // writing sb_id=0 (Release) BEFORE the CAS creates a brief
        // (valid=3, sb_id=0) intermediate which the M5b.2 acct-ud4h
        // invariant test (rightly) flags as a Release/Acquire pairing
        // violation; writing it AFTER the CAS creates a brief
        // (valid=2, sb_id=0) window during re-route that the cleanup
        // 2→0 fallback would wrongly free. Leaving sb_id at its old
        // value is harmless: no production consumer reads sb_id under
        // valid==1 (collect_candidates only checks valid==1, ignores
        // sb_id; boot sweep Phase 1 only reads sb_id under valid==2;
        // committer cleanup's sb_id guard correctly skips entries with
        // stale sb_id via the first-branch mismatch). The next router
        // pack will overwrite sb_id in Phase 6 anyway.
        //
        // M5b.2 acct-kt61: gating cleanup_after_superbatch's 2→0
        // fallback on eject_count==0 protects against the cleanup
        // race when an ejected entry is re-routed BEFORE the old
        // committer's Step 14 reads it (then valid=2 with old or
        // partial sb_id, but eject_count>0 marks it as not-our-mess).
        for s_idx in requeue {
            let queue = STAGING_QUEUE.share();
            let slot = &queue.entries[s_idx as usize];
            let _ = slot.valid.compare_exchange(3, 1, Release, Relaxed);
        }

        kept
    };
    let events: &[PocV21Event] = &kept_events;

    // STEP 2.5 (M5e.2 acct-jypc): persistent_staging state transition.
    // For envelopes whose caller user-tx committed AND that have a
    // durable persistent_staging row: flip 'staged' → 'in_shmem'.
    // The UPDATE WHERE state='staged' is no-op for non-durable
    // envelopes (no row exists) and for re-processed envelopes
    // (already 'in_shmem' or 'completed'). Single batched UPDATE per
    // SuperBatch.
    if !events.is_empty() {
        let corrs: Vec<pgrx::Uuid> = events.iter().map(|e| e.correlation_id).collect();
        Spi::run_with_args(
            "UPDATE poc_v21_persistent_staging \
                SET state='in_shmem' \
              WHERE correlation_id = ANY($1::uuid[]) AND state='staged'",
            &[corrs.into()],
        )
        .map_err(|e| format!("persistent_staging in_shmem UPDATE: {e}"))?;
    }

    // STEP 2.4 (new at M2.1): hydrate per-SKU method assignments from
    // poc_v21_sku_method_assignments. SKUs without a row default to
    // "fifo" — this preserves M1.3 test convenience and matches the
    // bake-off setup convention (only AVG/STD SKUs need explicit rows).
    let mut method_for_sku: HashMap<i64, &'static str> = HashMap::new();
    if !sku_pool_keys.is_empty() {
        let sku_ids: Vec<i64> = sku_pool_keys.iter().map(|k| k.0).collect();
        let assigned: Vec<(i64, String)> = Spi::connect(|client| {
            let mut out: Vec<(i64, String)> = Vec::new();
            let mut t = client
                .select(
                    "SELECT sku_id, method_id FROM poc_v21_sku_method_assignments \
                       WHERE sku_id = ANY($1::bigint[])",
                    None,
                    &[sku_ids.into()],
                )
                .ok()?;
            while let Some(row) = t.next() {
                let sku_id: i64 = row.get::<i64>(1).ok()??;
                let method: String = row.get::<String>(2).ok()??;
                out.push((sku_id, method));
            }
            Some(out)
        })
        .unwrap_or_default();
        for (sku_id, method) in assigned {
            let m: &'static str = match method.as_str() {
                "avg" => "avg",
                "std" => "std",
                _ => "fifo",
            };
            method_for_sku.insert(sku_id, m);
        }
    }
    let method_of = |sku_id: i64| -> &'static str {
        *method_for_sku.get(&sku_id).unwrap_or(&"fifo")
    };

    // STEP 2.5: dedup. Collect (issue_id, method_used) pairs per event's
    // SKU method assignment (AVG events check cost_consumptions; FIFO
    // checks cost_depletions; UNION below covers both).
    let dedup_pairs: Vec<(i64, &'static str)> = events
        .iter()
        .filter(|e| matches!(
            e.event_type,
            PocV21EventType::InvIssue | PocV21EventType::SoShipment
        ) || (matches!(e.event_type, PocV21EventType::InvAdjust) && e.qty < 0))
        .map(|e| (e.issue_id, method_of(e.sku_id)))
        .collect();

    let mut replayed_correlation_ids: Vec<pgrx::Uuid> = Vec::new();
    if !dedup_pairs.is_empty() {
        let issue_ids: Vec<i64> = dedup_pairs.iter().map(|p| p.0).collect();
        let methods_owned: Vec<String> = dedup_pairs.iter().map(|p| p.1.to_string()).collect();

        // Check both cost_depletions and cost_consumptions for prior writes.
        let existing: Vec<i64> = Spi::connect(|client| {
            let mut out = Vec::new();
            let mut t = client
                .select(
                    "SELECT DISTINCT issue_id FROM poc_v21_cost_depletions \
                     WHERE (issue_id, method_used) IN (\
                       SELECT issue_id, method_used FROM UNNEST($1::bigint[], $2::text[]) AS t(issue_id, method_used)\
                     ) \
                     UNION \
                     SELECT DISTINCT issue_id FROM poc_v21_cost_consumptions \
                     WHERE (issue_id, method_used) IN (\
                       SELECT issue_id, method_used FROM UNNEST($1::bigint[], $2::text[]) AS t(issue_id, method_used)\
                     )",
                    None,
                    &[issue_ids.into(), methods_owned.into()],
                )
                .ok()?;
            while let Some(row) = t.next() {
                if let Ok(Some(id)) = row.get::<i64>(1) {
                    out.push(id);
                }
            }
            Some(out)
        })
        .unwrap_or_default();

        if !existing.is_empty() {
            for event in events {
                if matches!(
                    event.event_type,
                    PocV21EventType::InvIssue | PocV21EventType::SoShipment
                ) || (matches!(event.event_type, PocV21EventType::InvAdjust) && event.qty < 0)
                {
                    if existing.contains(&event.issue_id) {
                        replayed_correlation_ids.push(event.correlation_id);
                    }
                }
            }
        }
    }

    // STEP 2.5b (acct-0frn): within-SB cross-envelope dedup.
    //
    // Under the grouped router rule (`poc-v2.1-amendment-0.2`), two
    // envelopes sharing an `(issue_id, method_used)` pair can land in
    // the SAME SuperBatch — the DB-side check above only catches
    // replays against persisted state. Without this pass both
    // envelopes' depletions would mutate the in-memory pool while the
    // table-level `INSERT ... ON CONFLICT (issue_id, method_used) DO
    // NOTHING` silently drops the second `cost_consumptions` /
    // `cost_depletions` row — leaving the pool drained twice with
    // only one consumption row persisted (P6 invariant violation
    // surfaced by `tests/property_v21_enqueue_commit.rs`).
    //
    // Iterate events in chrono order (matching Step 4's dispatch
    // order so "first arrival" is well-defined); keep the first
    // occurrence of each `(issue_id, method_used)`; mark subsequent
    // occurrences' correlation_ids as replayed. They get UPSERTed
    // to `state='replayed'` in Step 12 alongside the DB-replayed set.
    //
    // Limitation: this marks the WHOLE envelope replayed on the
    // first duplicate event. wo_complete envelopes whose K
    // depletions use distinct issue_ids (the standard generator
    // shape) are unaffected; an envelope that mixed duplicate +
    // non-duplicate issue_ids would lose the legitimate depletions
    // — out of scope for E1, would require per-event-rather-than-
    // per-envelope replay marking.
    {
        let mut chrono_indices: Vec<usize> = (0..events.len()).collect();
        chrono_indices.sort_by_key(|&i| {
            let e = &events[i];
            (
                e.business_date_jdate,
                e.doc_chrono,
                e.document_id,
                e.sub_priority,
            )
        });
        let mut seen_in_sb: HashSet<(i64, &'static str)> = HashSet::new();
        for idx in chrono_indices {
            let event = &events[idx];
            let is_depletion = matches!(
                event.event_type,
                PocV21EventType::InvIssue | PocV21EventType::SoShipment
            ) || (matches!(event.event_type, PocV21EventType::InvAdjust) && event.qty < 0);
            if !is_depletion {
                continue;
            }
            if replayed_correlation_ids.contains(&event.correlation_id) {
                continue;
            }
            let key = (event.issue_id, method_of(event.sku_id));
            if !seen_in_sb.insert(key) {
                replayed_correlation_ids.push(event.correlation_id);
            }
        }
    }

    let events_to_plan: Vec<PocV21Event> = events
        .iter()
        .filter(|e| !replayed_correlation_ids.contains(&e.correlation_id))
        .cloned()
        .collect();

    // STEP 3: snapshot hydration. FIFO loads cost_layers; AVG loads
    // avg_pool_state. Each only hydrates the pools its method covers
    // (the dispatch on event.sku_id at Step 4 reads the right slot).
    let mut snapshot = PocV21Snapshot::default();
    for (sku, &m) in method_for_sku.iter() {
        snapshot.method_assignments.insert(*sku, m);
    }
    if !sku_pool_keys.is_empty() {
        let fifo_pools: Vec<(i64, i64)> = sku_pool_keys
            .iter()
            .filter(|(s, _)| method_of(*s) == "fifo")
            .copied()
            .collect();
        if !fifo_pools.is_empty() {
            let sku_ids: Vec<i64> = fifo_pools.iter().map(|k| k.0).collect();
            let location_ids: Vec<i64> = fifo_pools.iter().map(|k| k.1).collect();
            hydrate_fifo_layers(&mut snapshot, &sku_ids, &location_ids)?;
        }
        let avg_pools: Vec<(i64, i64)> = sku_pool_keys
            .iter()
            .filter(|(s, _)| method_of(*s) == "avg")
            .copied()
            .collect();
        if !avg_pools.is_empty() {
            let sku_ids: Vec<i64> = avg_pools.iter().map(|k| k.0).collect();
            let location_ids: Vec<i64> = avg_pools.iter().map(|k| k.1).collect();
            hydrate_avg_pools(&mut snapshot, &sku_ids, &location_ids)?;
        }
        let std_pools: Vec<(i64, i64)> = sku_pool_keys
            .iter()
            .filter(|(s, _)| method_of(*s) == "std")
            .copied()
            .collect();
        if !std_pools.is_empty() {
            let sku_ids: Vec<i64> = std_pools.iter().map(|k| k.0).collect();
            let location_ids: Vec<i64> = std_pools.iter().map(|k| k.1).collect();
            hydrate_standard_costs(&mut snapshot, &sku_ids, &location_ids)?;
        }
    }

    // STEP 4: per-event dispatch grouped by envelope for per-envelope
    // failure isolation (M6.2 acct-p0mu; spec §1.8 Step 4 + §3.7).
    //
    // Events are first sorted by chronological key, then grouped by
    // correlation_id preserving first-appearance order. Each envelope's
    // events dispatch as a unit: pre-envelope snapshot of touched pool
    // entries is captured; on ANY event failure within the envelope, the
    // snapshot is restored and the envelope's accumulated result rows are
    // truncated. Other envelopes in the SuperBatch commit normally.
    //
    // Within-envelope ordering: events of one wo_complete share
    // (business_date, doc_chrono, document_id) and only differ in
    // sub_priority — they sort consecutively. Single-event envelopes are
    // singleton groups.
    //
    // Cross-envelope ordering: groups iterate by first-appearance in the
    // chrono sort. Two envelopes targeting the same pool process
    // serially (FIFO bills the first against pre-batch layers, then the
    // second against post-mutation layers; AVG roll_in updates running
    // state in chrono order). Under the grouped router rule (acct-0frn
    // / `poc-v2.1-amendment-0.2`) shared-pool envelopes are packed INTO
    // one SuperBatch by design, so this cross-envelope sequencing is
    // the load-bearing path for overlapping work, not just an
    // intra-`wo_complete` K+1 special case.
    //
    // Snapshot rollback fixes the post-M6.1 polluted-snapshot risk: FIFO
    // `deplete` partially mutates layer effective_qty before signalling
    // insufficient_inventory, AVG `roll_in` mutates running state before
    // any subsequent same-envelope component fails. Without rollback,
    // succeeded components of a failed envelope leak mutations into
    // unrelated envelopes (and into subsequent SuperBatches that hydrate
    // from cost_layers/avg_pool_state — though those reads come from the
    // committed DB state, not the snapshot, so DB persistence is already
    // gated on succeeded_set filtering). The risk is purely in-memory:
    // envelope N+1 within the same SuperBatch sees envelope N's partial
    // mutation. Rollback closes that hole.
    let mut sorted_events = events_to_plan.clone();
    sorted_events.sort_by_key(|e| {
        (
            e.business_date_jdate,
            e.doc_chrono,
            e.document_id,
            e.sub_priority,
        )
    });

    let mut envelope_groups: Vec<(pgrx::Uuid, Vec<PocV21Event>)> = Vec::new();
    let mut corr_to_group_idx: HashMap<pgrx::Uuid, usize> = HashMap::new();
    for event in &sorted_events {
        if let Some(&idx) = corr_to_group_idx.get(&event.correlation_id) {
            envelope_groups[idx].1.push(event.clone());
        } else {
            corr_to_group_idx.insert(event.correlation_id, envelope_groups.len());
            envelope_groups.push((event.correlation_id, vec![event.clone()]));
        }
    }

    let mut result = PocV21ApplyResult::default();

    for (_corr, group) in &envelope_groups {
        // Checkpoint: which (sku, location) pool entries does this envelope
        // touch? Save their current state (or None if absent) so rollback
        // can restore exactly the pre-envelope shape — including the case
        // where this envelope was the FIRST to .entry(...).or_insert(...) a
        // pool, in which case rollback must `remove` rather than `insert`.
        let touched: std::collections::HashSet<(i64, i64)> =
            group.iter().map(|e| (e.sku_id, e.location_id)).collect();
        let pool_checkpoint: HashMap<(i64, i64), Option<SkuPoolState>> = touched
            .iter()
            .map(|key| (*key, snapshot.sku_pools.get(key).cloned()))
            .collect();
        let layer_inserts_before = result.layer_inserts.len();
        let depletion_inserts_before = result.depletion_inserts.len();
        let consumption_inserts_before = result.consumption_inserts.len();
        let posting_line_inserts_before = result.posting_line_inserts.len();
        let posting_line_inventory_inserts_before =
            result.posting_line_inventory_inserts.len();

        // M6.1 (acct-o1yv): WoComplete cost accumulator is envelope-local.
        // Components push cost in; output reads it. Discarded automatically
        // when the loop body ends, so a rolled-back envelope leaves no
        // accumulator residue for subsequent envelopes.
        let mut wo_cost_local: i64 = 0;
        let mut envelope_failed = false;

        for event in group {
            // Inject unit_cost into WoComplete output (qty>0).
            let mut event_for_dispatch = event.clone();
            if matches!(event.event_type, PocV21EventType::WoComplete) && event.qty > 0 {
                event_for_dispatch.unit_cost = if event.qty != 0 {
                    wo_cost_local / event.qty
                } else {
                    0
                };
            }

            let depl_before = result.depletion_inserts.len();
            let cons_before = result.consumption_inserts.len();

            let m = method_of(event_for_dispatch.sku_id);
            // M7.1 (acct-byue): per-method dispatch latency + count. Time
            // the apply_one call only — outside the result-vector accounting.
            // std::time::Instant ~25ns/call on x86_64; acceptable overhead
            // relative to apply_one's ~µs-scale work.
            let t_start = std::time::Instant::now();
            let event_result = match m {
                "avg" => AVG_METHOD.apply_one(&event_for_dispatch, &mut snapshot, &mut result),
                "std" => {
                    STANDARD_METHOD.apply_one(&event_for_dispatch, &mut snapshot, &mut result)
                }
                _ => FIFO_METHOD.apply_one(&event_for_dispatch, &mut snapshot, &mut result),
            };
            let elapsed_ns = t_start.elapsed().as_nanos() as u64;
            record_method_latency(m, elapsed_ns, event_result.error_code.is_some());

            if event_result.error_code.is_some() {
                // Mark the envelope failed; record the failing per_event
                // entry so failed_map below can surface the error_code for
                // status update; stop dispatching remaining events of this
                // envelope (downstream events would see the failure-leg
                // snapshot mid-mutation, and rollback below restores the
                // pre-envelope state regardless).
                envelope_failed = true;
                result.per_event.push(event_result);
                break;
            }

            // Accumulate component cost on a successful WoComplete consume.
            // E5 (acct-gx1z.1.5): use checked_* so silent overflow can't
            // produce a clamped-at-i64::MAX wo_cost_local that then divides
            // by output qty to give a wildly wrong unit_cost. On overflow,
            // route the envelope through the per-envelope failure path
            // with error_code='cost_overflow' so the snapshot rollback
            // restores pre-envelope pool state and the caller sees the
            // problem in submission_status.
            if matches!(event.event_type, PocV21EventType::WoComplete) && event.qty < 0 {
                let mut overflowed = false;
                let mut cost: i64 = 0;
                'cost_accum: {
                    for d in &result.depletion_inserts[depl_before..] {
                        match d.qty.checked_mul(d.unit_cost).and_then(|p| cost.checked_add(p)) {
                            Some(v) => cost = v,
                            None => {
                                overflowed = true;
                                break 'cost_accum;
                            }
                        }
                    }
                    for c in &result.consumption_inserts[cons_before..] {
                        match c.qty.checked_mul(c.unit_cost).and_then(|p| cost.checked_add(p)) {
                            Some(v) => cost = v,
                            None => {
                                overflowed = true;
                                break 'cost_accum;
                            }
                        }
                    }
                    match wo_cost_local.checked_add(cost) {
                        Some(v) => wo_cost_local = v,
                        None => overflowed = true,
                    }
                }
                if overflowed {
                    envelope_failed = true;
                    result.per_event.push(PocV21EventResult {
                        correlation_id: event.correlation_id,
                        error_code: Some("cost_overflow".to_string()),
                    });
                    break;
                }
            }

            result.per_event.push(event_result);
        }

        if envelope_failed {
            // Snapshot rollback: restore each touched pool to its
            // pre-envelope state (or remove it if this envelope created it).
            for (key, saved) in &pool_checkpoint {
                match saved {
                    Some(state) => {
                        snapshot.sku_pools.insert(*key, state.clone());
                    }
                    None => {
                        snapshot.sku_pools.remove(key);
                    }
                }
            }
            // Result-vector rollback: drop rows accumulated by this
            // envelope. per_event entries are kept (failed_map below reads
            // them to emit the status='failed' update).
            result.layer_inserts.truncate(layer_inserts_before);
            result.depletion_inserts.truncate(depletion_inserts_before);
            result.consumption_inserts.truncate(consumption_inserts_before);
            result.posting_line_inserts.truncate(posting_line_inserts_before);
            result
                .posting_line_inventory_inserts
                .truncate(posting_line_inventory_inserts_before);
        }
    }

    // Drop envelopes whose per_event reported an error. M6.1 (acct-o1yv):
    // wo_complete envelopes expand into K+1 per_event entries; if ANY of
    // them fails, the whole envelope is failed. Dedupe correlation_ids
    // (one row per envelope in submission_status UPSERTs to avoid
    // "ON CONFLICT DO UPDATE command cannot affect row a second time").
    let mut failed_map: HashMap<pgrx::Uuid, String> = HashMap::new();
    for er in &result.per_event {
        if let Some(code) = &er.error_code {
            failed_map.entry(er.correlation_id).or_insert_with(|| code.clone());
        }
    }
    let failed_correlation_ids: Vec<(pgrx::Uuid, String)> =
        failed_map.iter().map(|(c, e)| (*c, e.clone())).collect();
    // Succeeded = unique correlation_ids whose ALL per_event entries are
    // error-free (i.e., not in failed_map).
    let mut succeeded_dedupe: std::collections::HashSet<pgrx::Uuid> =
        std::collections::HashSet::new();
    for er in &result.per_event {
        if !failed_map.contains_key(&er.correlation_id) {
            succeeded_dedupe.insert(er.correlation_id);
        }
    }
    let succeeded: Vec<pgrx::Uuid> = succeeded_dedupe.iter().copied().collect();

    // Filter row vectors to succeeded envelopes only.
    let succeeded_set: std::collections::HashSet<pgrx::Uuid> = succeeded.iter().copied().collect();
    // Track each layer_row's original index in result.layer_inserts so
    // Step 5b can map RETURNING ids back to that position. Depletions
    // emitted earlier in this SB carry that position via
    // `layer_insert_index`; Step 5c translates the placeholder
    // layer_id=0 to the real BIGSERIAL (acct-shpc.9).
    let layer_rows_indexed: Vec<(usize, crate::cost_method::PocV21LayerRow)> = result
        .layer_inserts
        .iter()
        .enumerate()
        .filter(|(_, r)| succeeded_set.contains(&r.correlation_id))
        .map(|(i, r)| (i, r.clone()))
        .collect();
    let layer_rows: Vec<crate::cost_method::PocV21LayerRow> =
        layer_rows_indexed.iter().map(|(_, r)| r.clone()).collect();
    let depletion_rows: Vec<_> = result
        .depletion_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    let posting_line_rows: Vec<_> = result
        .posting_line_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    let posting_inventory_rows: Vec<_> = result
        .posting_line_inventory_inserts
        .iter()
        .filter_map(|r| {
            let pl = result.posting_line_inserts.get(r.posting_line_ordinal)?;
            if succeeded_set.contains(&pl.correlation_id) {
                Some((r.clone(), pl.correlation_id))
            } else {
                None
            }
        })
        .collect();

    // STEP 5: bulk UNNEST inserts. Six target tables; this is the
    // load-bearing throughput primitive measured by P5 (spec §4.2):
    // 5a posting_lines, 5b cost_layers, 5c cost_depletions,
    // 5c' cost_consumptions, 5d posting_line_inventory, 5e
    // avg_pool_state UPSERT. Each writes one SuperBatch's rows in
    // a single bulk statement; per-row chatter is gone.
    //
    // posting_lines + cost_layers use RETURNING to surface BIGSERIAL
    // ids for downstream linking (posting_line_id → inventory; new
    // layer_id → inv.layer_id for receipt-side rows). PG returns
    // RETURNING rows in insertion order; the input array order is
    // preserved.
    //
    // posting_line_inventory carries posting_line_ordinal (the index
    // into result.posting_line_inserts where the originating
    // posting line lives). We build an `ordinal → posting_line_id`
    // resolver from the RETURNING output, then UNNEST-INSERT the
    // inventory rows with resolved ids.

    let (posting_line_ords_for_rows, posting_line_input): (
        Vec<usize>,
        Vec<&crate::cost_method::PocV21PostingLineRow>,
    ) = result
        .posting_line_inserts
        .iter()
        .enumerate()
        .filter(|(_, r)| succeeded_set.contains(&r.correlation_id))
        .map(|(i, r)| (i, r))
        .unzip();

    // L3 invariant (acct-gx1z.1.7): ordinal_to_pl_id maps each
    // input position in `result.posting_line_inserts` to its
    // RETURNING-assigned posting_line_id. The mapping is stable
    // because (a) we issue ONE INSERT … RETURNING id that PG
    // guarantees yields rows in input order, and (b) the
    // `posting_line_ords_for_rows` Vec built via the same filter
    // above carries the original indices. Sized to the full
    // posting_line_inserts length so filtered-out (failed-envelope)
    // ordinals stay at 0 and are never indexed downstream.
    let mut ordinal_to_pl_id: Vec<i64> = vec![0; result.posting_line_inserts.len()];
    if !posting_line_input.is_empty() {
        // Build parallel arrays from the input rows.
        let bd: Vec<i32> = posting_line_input.iter().map(|r| r.business_date_jdate).collect();
        let chrono: Vec<i64> = posting_line_input.iter().map(|r| r.doc_chrono).collect();
        let doc_id: Vec<i64> = posting_line_input.iter().map(|r| r.document_id).collect();
        let sub_pri: Vec<i32> = posting_line_input.iter().map(|r| r.sub_priority).collect();
        let event_type_v: Vec<String> = posting_line_input.iter().map(|r| r.event_type.to_string()).collect();
        let amount: Vec<i64> = posting_line_input.iter().map(|r| r.amount).collect();
        let debit_acct: Vec<Option<i64>> = posting_line_input.iter().map(|r| r.debit_account).collect();
        let credit_acct: Vec<Option<i64>> = posting_line_input.iter().map(|r| r.credit_account).collect();
        let corr: Vec<pgrx::Uuid> = posting_line_input.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = posting_line_input.iter().map(|r| r.user_tx_xid as i64).collect();

        let returned: Vec<i64> = Spi::connect(|client| -> Option<Vec<i64>> {
            let mut out: Vec<i64> = Vec::with_capacity(posting_line_input.len());
            let mut t = client
                .select(
                    "INSERT INTO poc_v21_posting_lines \
                       (business_date, doc_chrono, document_id, sub_priority, event_type, amount, \
                        debit_account, credit_account, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
                     SELECT '2000-01-01'::date + bd, chrono, doc_id, sub_pri, et, amt, deb, cred, c, uxid::text::xid8, $11, $12 \
                       FROM UNNEST($1::int[], $2::bigint[], $3::bigint[], $4::int[], $5::text[], $6::bigint[], \
                                   $7::bigint[], $8::bigint[], $9::uuid[], $10::bigint[]) \
                            AS t(bd, chrono, doc_id, sub_pri, et, amt, deb, cred, c, uxid) \
                     RETURNING posting_line_id",
                    None,
                    &[
                        bd.into(),
                        chrono.into(),
                        doc_id.into(),
                        sub_pri.into(),
                        event_type_v.into(),
                        amount.into(),
                        debit_acct.into(),
                        credit_acct.into(),
                        corr.into(),
                        user_xid.into(),
                        (committer_tx_id as i64).into(),
                        (superbatch_id as i64).into(),
                    ],
                )
                .ok()?;
            while let Some(row) = t.next() {
                if let Ok(Some(id)) = row.get::<i64>(1) {
                    out.push(id);
                }
            }
            Some(out)
        })
        .ok_or_else(|| "posting_lines bulk INSERT failed".to_string())?;
        if returned.len() != posting_line_input.len() {
            return Err(format!(
                "posting_lines: expected {} ids, got {}",
                posting_line_input.len(),
                returned.len()
            ));
        }
        for (ordinal, id) in posting_line_ords_for_rows.iter().zip(returned.iter()) {
            ordinal_to_pl_id[*ordinal] = *id;
        }
    }

    // 5b. cost_layers — bulk INSERT RETURNING layer_id. We need
    // per-correlation queues of new layer ids so posting_line_inventory
    // can pop one per receipt-side row.
    let mut new_layer_ids_by_corr: HashMap<pgrx::Uuid, Vec<i64>> = HashMap::new();
    // Position-keyed map for Step 5c depletion translation
    // (acct-shpc.9): depletion's layer_insert_index → real BIGSERIAL.
    let mut layer_db_id_by_insert_index: HashMap<usize, i64> = HashMap::new();
    if !layer_rows.is_empty() {
        let sku: Vec<i64> = layer_rows.iter().map(|r| r.sku_id).collect();
        let loc: Vec<i64> = layer_rows.iter().map(|r| r.location_id).collect();
        let qty: Vec<i64> = layer_rows.iter().map(|r| r.qty).collect();
        let unit_cost: Vec<i64> = layer_rows.iter().map(|r| r.unit_cost).collect();
        let born_at: Vec<i64> = layer_rows.iter().map(|r| r.born_at_micros).collect();
        let born_seq: Vec<i64> = layer_rows.iter().map(|r| r.born_seq).collect();
        let source_kind: Vec<String> = layer_rows.iter().map(|r| r.source_kind.to_string()).collect();
        let source_ref: Vec<Option<i64>> = layer_rows.iter().map(|r| r.source_ref).collect();
        let corr: Vec<pgrx::Uuid> = layer_rows.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = layer_rows.iter().map(|r| r.user_tx_xid as i64).collect();

        let returned_pairs: Vec<(i64, pgrx::Uuid)> = Spi::connect(
            |client| -> Option<Vec<(i64, pgrx::Uuid)>> {
                let mut out: Vec<(i64, pgrx::Uuid)> = Vec::with_capacity(layer_rows.len());
                let mut t = client
                    .select(
                        "INSERT INTO poc_v21_cost_layers \
                           (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, source_ref, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
                         SELECT sku, loc, q, u, to_timestamp(b::double precision / 1000000), bs, sk, sr, c, uxid::text::xid8, $11, $12 \
                           FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], \
                                       $7::text[], $8::bigint[], $9::uuid[], $10::bigint[]) \
                                AS t(sku, loc, q, u, b, bs, sk, sr, c, uxid) \
                         RETURNING layer_id, correlation_id",
                        None,
                        &[
                            sku.into(),
                            loc.into(),
                            qty.into(),
                            unit_cost.into(),
                            born_at.into(),
                            born_seq.into(),
                            source_kind.into(),
                            source_ref.into(),
                            corr.into(),
                            user_xid.into(),
                            (committer_tx_id as i64).into(),
                            (superbatch_id as i64).into(),
                        ],
                    )
                    .ok()?;
                while let Some(row) = t.next() {
                    let id = row.get::<i64>(1).ok()??;
                    let c = row.get::<pgrx::Uuid>(2).ok()??;
                    out.push((id, c));
                }
                Some(out)
            },
        )
        .ok_or_else(|| "cost_layers bulk INSERT failed".to_string())?;
        if returned_pairs.len() != layer_rows_indexed.len() {
            return Err(format!(
                "cost_layers: expected {} ids, got {}",
                layer_rows_indexed.len(),
                returned_pairs.len()
            ));
        }
        for ((id, c), (orig_idx, _)) in returned_pairs.into_iter().zip(layer_rows_indexed.iter()) {
            new_layer_ids_by_corr.entry(c).or_default().push(id);
            layer_db_id_by_insert_index.insert(*orig_idx, id);
        }
    }

    // 5c. cost_depletions — bulk UNNEST INSERT. No RETURNING needed.
    // Translate in-SB-emitted layer placeholders (`layer_insert_index`)
    // to real BIGSERIAL ids stamped by Step 5b's RETURNING. Hydrated
    // layers carry their real DB id on `layer_id` directly
    // (acct-shpc.9).
    if !depletion_rows.is_empty() {
        let layer: Vec<i64> = depletion_rows
            .iter()
            .map(|r| -> Result<i64, String> {
                if let Some(idx) = r.layer_insert_index {
                    layer_db_id_by_insert_index.get(&idx).copied().ok_or_else(|| {
                        format!(
                            "cost_depletions: in-SB layer_insert_index={} \
                             has no DB id (layer creator's correlation_id failed?)",
                            idx
                        )
                    })
                } else {
                    Ok(r.layer_id)
                }
            })
            .collect::<Result<Vec<i64>, String>>()?;
        let qty: Vec<i64> = depletion_rows.iter().map(|r| r.qty).collect();
        let unit_cost: Vec<i64> = depletion_rows.iter().map(|r| r.unit_cost).collect();
        let consumed_at: Vec<i64> = depletion_rows.iter().map(|r| r.consumed_at_micros).collect();
        let consumed_seq: Vec<i64> = depletion_rows.iter().map(|r| r.consumed_seq).collect();
        let issue: Vec<i64> = depletion_rows.iter().map(|r| r.issue_id).collect();
        let method: Vec<String> = depletion_rows.iter().map(|r| r.method_used.to_string()).collect();
        let corr: Vec<pgrx::Uuid> = depletion_rows.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = depletion_rows.iter().map(|r| r.user_tx_xid as i64).collect();

        Spi::run_with_args(
            "INSERT INTO poc_v21_cost_depletions \
               (layer_id, qty, unit_cost, consumed_at, consumed_seq, issue_id, method_used, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             SELECT layer, q, u, to_timestamp(ca::double precision / 1000000), cs, i, m, c, uxid::text::xid8, $10, $11 \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], \
                           $7::text[], $8::uuid[], $9::bigint[]) \
                    AS t(layer, q, u, ca, cs, i, m, c, uxid)",
            &[
                layer.into(),
                qty.into(),
                unit_cost.into(),
                consumed_at.into(),
                consumed_seq.into(),
                issue.into(),
                method.into(),
                corr.into(),
                user_xid.into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("cost_depletions bulk INSERT: {e}"))?;
    }

    // 5c'. cost_consumptions — bulk UNNEST with ON CONFLICT
    // (issue_id, method_used) DO NOTHING for replay-safety.
    let consumption_rows: Vec<_> = result
        .consumption_inserts
        .iter()
        .filter(|r| succeeded_set.contains(&r.correlation_id))
        .cloned()
        .collect();
    if !consumption_rows.is_empty() {
        let sku: Vec<i64> = consumption_rows.iter().map(|r| r.sku_id).collect();
        let loc: Vec<i64> = consumption_rows.iter().map(|r| r.location_id).collect();
        let qty: Vec<i64> = consumption_rows.iter().map(|r| r.qty).collect();
        let unit_cost: Vec<i64> = consumption_rows.iter().map(|r| r.unit_cost).collect();
        let consumed_at: Vec<i64> = consumption_rows.iter().map(|r| r.consumed_at_micros).collect();
        let consumed_seq: Vec<i64> = consumption_rows.iter().map(|r| r.consumed_seq).collect();
        let issue: Vec<i64> = consumption_rows.iter().map(|r| r.issue_id).collect();
        let method: Vec<String> = consumption_rows.iter().map(|r| r.method_used.to_string()).collect();
        let corr: Vec<pgrx::Uuid> = consumption_rows.iter().map(|r| r.correlation_id).collect();
        let user_xid: Vec<i64> = consumption_rows.iter().map(|r| r.user_tx_xid as i64).collect();

        Spi::run_with_args(
            "INSERT INTO poc_v21_cost_consumptions \
               (sku_id, location_id, qty, applied_unit_cost, consumed_at, consumed_seq, issue_id, method_used, correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             SELECT sku, loc, q, u, to_timestamp(ca::double precision / 1000000), cs, i, m, c, uxid::text::xid8, $11, $12 \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[], \
                           $7::bigint[], $8::text[], $9::uuid[], $10::bigint[]) \
                    AS t(sku, loc, q, u, ca, cs, i, m, c, uxid) \
             ON CONFLICT (issue_id, method_used) DO NOTHING",
            &[
                sku.into(),
                loc.into(),
                qty.into(),
                unit_cost.into(),
                consumed_at.into(),
                consumed_seq.into(),
                issue.into(),
                method.into(),
                corr.into(),
                user_xid.into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("cost_consumptions bulk INSERT: {e}"))?;
    }

    // 5d. posting_line_inventory — resolve posting_line_id via the
    // ordinal map + layer_id via the per-correlation new-layer queue;
    // bulk UNNEST INSERT the resolved rows.
    let mut inv_pl_ids: Vec<i64> = Vec::new();
    let mut inv_sku: Vec<i64> = Vec::new();
    let mut inv_loc: Vec<i64> = Vec::new();
    let mut inv_qty: Vec<i64> = Vec::new();
    let mut inv_layer: Vec<Option<i64>> = Vec::new();
    for (inv_row, corr) in &posting_inventory_rows {
        let pl_id = ordinal_to_pl_id[inv_row.posting_line_ordinal];
        if pl_id == 0 {
            continue;
        }
        let layer_id = if let Some(real_id) = inv_row.layer_id {
            Some(real_id)
        } else {
            new_layer_ids_by_corr.get_mut(corr).and_then(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.remove(0))
                }
            })
        };
        inv_pl_ids.push(pl_id);
        inv_sku.push(inv_row.sku_id);
        inv_loc.push(inv_row.location_id);
        inv_qty.push(inv_row.qty);
        inv_layer.push(layer_id);
    }
    if !inv_pl_ids.is_empty() {
        Spi::run_with_args(
            "INSERT INTO poc_v21_posting_line_inventory \
               (posting_line_id, sku_id, location_id, qty, layer_id) \
             SELECT pl, sku, loc, q, layer \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[]) \
                    AS t(pl, sku, loc, q, layer)",
            &[
                inv_pl_ids.into(),
                inv_sku.into(),
                inv_loc.into(),
                inv_qty.into(),
                inv_layer.into(),
            ],
        )
        .map_err(|e| format!("posting_line_inventory bulk INSERT: {e}"))?;
    }

    // 5e. avg_pool_state UPSERT — persist running average state for
    // every AVG-method pool that this batch touched. Race-free: pool_lock
    // held in Step 2 covers concurrent committer access on the same pool.
    // "NEVER reconstruct AVG from cost_layers history" — this UPSERT is
    // the contract (spec §1.3).
    let mut avg_dirty_pools: Vec<(i64, i64, i64, i64)> = Vec::new(); // (sku, loc, avg_unit_cost, total_qty)
    for ((sku, loc), pool) in snapshot.sku_pools.iter() {
        if pool.avg_dirty {
            avg_dirty_pools.push((*sku, *loc, pool.avg_unit_cost, pool.avg_total_qty));
        }
    }
    if !avg_dirty_pools.is_empty() {
        let sku_ids: Vec<i64> = avg_dirty_pools.iter().map(|t| t.0).collect();
        let loc_ids: Vec<i64> = avg_dirty_pools.iter().map(|t| t.1).collect();
        let units: Vec<i64> = avg_dirty_pools.iter().map(|t| t.2).collect();
        let qtys: Vec<i64> = avg_dirty_pools.iter().map(|t| t.3).collect();
        Spi::run_with_args(
            "INSERT INTO poc_v21_avg_pool_state \
               (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
             SELECT sku_id, location_id, avg_unit_cost, total_qty, now(), $5::bigint \
               FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[]) \
                    AS t(sku_id, location_id, avg_unit_cost, total_qty) \
             ON CONFLICT (sku_id, location_id) DO UPDATE SET \
               avg_unit_cost=EXCLUDED.avg_unit_cost, \
               total_qty=EXCLUDED.total_qty, \
               last_updated_at=EXCLUDED.last_updated_at, \
               last_committer_tx_id=EXCLUDED.last_committer_tx_id",
            &[
                sku_ids.into(),
                loc_ids.into(),
                units.into(),
                qtys.into(),
                (committer_tx_id as i64).into(),
            ],
        )
        .map_err(|e| format!("avg_pool_state UPSERT: {e}"))?;
    }

    // STEP 12: status updates. INSERT ON CONFLICT DO UPDATE for
    // committed + replayed paths so committer_lazy mode (no
    // pre-existing 'queued' row from enqueue) creates a row at
    // terminal state. caller_intx / caller_subtx modes hit the
    // ON CONFLICT branch which UPDATEs the pre-existing row.
    if !succeeded.is_empty() {
        Spi::run_with_args(
            "INSERT INTO poc_v21_submission_status \
               (correlation_id, state, enqueued_at, processed_at, committed_at, committer_tx_id, superbatch_id) \
             SELECT corr, 'committed', now(), now(), now(), $1, $2 \
               FROM UNNEST($3::uuid[]) AS t(corr) \
             ON CONFLICT (correlation_id) DO UPDATE SET \
               state='committed', committed_at=now(), processed_at=now(), \
               committer_tx_id=EXCLUDED.committer_tx_id, \
               superbatch_id=EXCLUDED.superbatch_id",
            &[
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
                succeeded.clone().into(),
            ],
        )
        .map_err(|e| format!("status committed UPSERT: {e}"))?;

        // M5e.2 (acct-jypc): persistent_staging staged|in_shmem → completed.
        // No-op for non-durable envelopes (no row exists). Commits atomically
        // with the cost rows + status UPSERT inside Step 5's sub-tx.
        Spi::run_with_args(
            "UPDATE poc_v21_persistent_staging \
                SET state='completed' \
              WHERE correlation_id = ANY($1::uuid[]) AND state IN ('staged','in_shmem')",
            &[succeeded.clone().into()],
        )
        .map_err(|e| format!("persistent_staging completed UPDATE: {e}"))?;
    }
    for (corr, code) in &failed_correlation_ids {
        let detail = pgrx::JsonB(serde_json::json!({ "phase": "plan_apply", "detail": code }));
        Spi::run_with_args(
            "INSERT INTO poc_v21_submission_status \
               (correlation_id, state, enqueued_at, processed_at, error_code, error_detail, committer_tx_id, superbatch_id) \
             VALUES ($1, 'failed', now(), now(), $2, $3, $4, $5) \
             ON CONFLICT (correlation_id) DO UPDATE SET \
               state='failed', processed_at=now(), \
               error_code=EXCLUDED.error_code, error_detail=EXCLUDED.error_detail, \
               committer_tx_id=EXCLUDED.committer_tx_id, superbatch_id=EXCLUDED.superbatch_id",
            &[
                (*corr).into(),
                code.as_str().into(),
                detail.into(),
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
            ],
        )
        .map_err(|e| format!("status failed UPDATE: {e}"))?;
    }
    if !replayed_correlation_ids.is_empty() {
        Spi::run_with_args(
            "INSERT INTO poc_v21_submission_status \
               (correlation_id, state, enqueued_at, processed_at, committer_tx_id, superbatch_id) \
             SELECT corr, 'replayed', now(), now(), $1, $2 \
               FROM UNNEST($3::uuid[]) AS t(corr) \
             ON CONFLICT (correlation_id) DO UPDATE SET \
               state='replayed', processed_at=now(), \
               committer_tx_id=EXCLUDED.committer_tx_id, \
               superbatch_id=EXCLUDED.superbatch_id",
            &[
                (committer_tx_id as i64).into(),
                (superbatch_id as i64).into(),
                replayed_correlation_ids.clone().into(),
            ],
        )
        .map_err(|e| format!("status replayed UPSERT: {e}"))?;
    }

    Ok(())
}

fn hydrate_standard_costs(
    snapshot: &mut PocV21Snapshot,
    sku_ids: &[i64],
    location_ids: &[i64],
) -> Result<(), String> {
    // DISTINCT ON returns the latest effective row per (sku, location).
    // `effective_from <= now()` filter excludes future-dated rolls.
    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "SELECT DISTINCT ON (sku_id, location_id) \
                        sku_id, location_id, unit_cost \
                   FROM poc_v21_standard_costs \
                  WHERE (sku_id, location_id) IN \
                        (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
                    AND effective_from <= now() \
                  ORDER BY sku_id, location_id, effective_from DESC",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT standard_costs: {e}"))?;
        while let Some(row) = t.next() {
            let sku_id: i64 = row.get::<i64>(1).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(2).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let unit_cost: i64 = row.get::<i64>(3).map_err(|e| format!("unit_cost: {e}"))?.unwrap_or(0);
            snapshot.standard_costs.insert((sku_id, location_id), unit_cost);
        }
        Ok(())
    })?;
    Ok(())
}

fn hydrate_avg_pools(
    snapshot: &mut PocV21Snapshot,
    sku_ids: &[i64],
    location_ids: &[i64],
) -> Result<(), String> {
    // Load avg_unit_cost + total_qty for AVG-method pools. Pools with
    // no row in avg_pool_state get default (0, 0) — first receipt
    // initializes; first consumption hits the insufficient-inventory
    // path.
    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "SELECT sku_id, location_id, avg_unit_cost, total_qty \
                   FROM poc_v21_avg_pool_state \
                  WHERE (sku_id, location_id) IN \
                        (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id))",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT avg_pool_state: {e}"))?;
        while let Some(row) = t.next() {
            let sku_id: i64 = row.get::<i64>(1).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(2).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let avg_unit_cost: i64 = row.get::<i64>(3).map_err(|e| format!("avg_unit_cost: {e}"))?.unwrap_or(0);
            let total_qty: i64 = row.get::<i64>(4).map_err(|e| format!("total_qty: {e}"))?.unwrap_or(0);

            let pool = snapshot
                .sku_pools
                .entry((sku_id, location_id))
                .or_insert_with(SkuPoolState::default);
            pool.avg_unit_cost = avg_unit_cost;
            pool.avg_total_qty = total_qty;
        }
        Ok(())
    })?;
    Ok(())
}

fn hydrate_fifo_layers(
    snapshot: &mut PocV21Snapshot,
    sku_ids: &[i64],
    location_ids: &[i64],
) -> Result<(), String> {
    use crate::cost_method::LayerView;

    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "WITH layers AS (\
                   SELECT layer_id, sku_id, location_id, qty, unit_cost, \
                          EXTRACT(EPOCH FROM born_at)::bigint * 1000000 AS born_at_micros, \
                          born_seq, correlation_id, \
                          (qty - COALESCE((SELECT SUM(qty)::bigint FROM poc_v21_cost_depletions d WHERE d.layer_id = poc_v21_cost_layers.layer_id), 0))::bigint AS effective_qty \
                     FROM poc_v21_cost_layers \
                    WHERE (sku_id, location_id) IN (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
                 ) \
                 SELECT layer_id, sku_id, location_id, unit_cost, born_at_micros, born_seq, correlation_id, effective_qty \
                   FROM layers \
                  WHERE effective_qty > 0 \
                  ORDER BY sku_id, location_id, born_at_micros, born_seq",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT cost_layers: {e}"))?;
        while let Some(row) = t.next() {
            let layer_id: i64 = row.get::<i64>(1).map_err(|e| format!("layer_id: {e}"))?.unwrap_or(0);
            let sku_id: i64 = row.get::<i64>(2).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(3).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let unit_cost: i64 = row.get::<i64>(4).map_err(|e| format!("unit_cost: {e}"))?.unwrap_or(0);
            let born_at_micros: i64 = row.get::<i64>(5).map_err(|e| format!("born_at_micros: {e}"))?.unwrap_or(0);
            let born_seq: i64 = row.get::<i64>(6).map_err(|e| format!("born_seq: {e}"))?.unwrap_or(0);
            let correlation_id: pgrx::Uuid = row.get::<pgrx::Uuid>(7).map_err(|e| format!("correlation_id: {e}"))?.unwrap();
            let effective_qty: i64 = row.get::<i64>(8).map_err(|e| format!("effective_qty: {e}"))?.unwrap_or(0);

            let pool = snapshot
                .sku_pools
                .entry((sku_id, location_id))
                .or_insert_with(SkuPoolState::default);
            if born_seq > pool.max_born_seq {
                pool.max_born_seq = born_seq;
            }
            pool.layers.push(LayerView {
                layer_id,
                layer_insert_index: None,
                unit_cost,
                effective_qty,
                born_at_micros,
                born_seq,
                correlation_id,
            });
        }
        Ok(())
    })?;

    // Seed max_born_seq for pools with no live layers (rolled-down). The
    // SELECT above filtered effective_qty > 0; ensure max_born_seq still
    // reflects the highest born_seq written. For M1.3 this is conservative
    // — we'll re-fetch separately.
    Spi::connect(|client| -> Result<(), String> {
        let mut t = client
            .select(
                "SELECT sku_id, location_id, MAX(born_seq) AS max_seq \
                   FROM poc_v21_cost_layers \
                  WHERE (sku_id, location_id) IN (SELECT sku_id, location_id FROM UNNEST($1::bigint[], $2::bigint[]) AS t(sku_id, location_id)) \
                  GROUP BY sku_id, location_id",
                None,
                &[sku_ids.to_vec().into(), location_ids.to_vec().into()],
            )
            .map_err(|e| format!("snapshot SELECT max(born_seq): {e}"))?;
        while let Some(row) = t.next() {
            let sku_id: i64 = row.get::<i64>(1).map_err(|e| format!("sku_id: {e}"))?.unwrap_or(0);
            let location_id: i64 = row.get::<i64>(2).map_err(|e| format!("location_id: {e}"))?.unwrap_or(0);
            let max_seq: i64 = row.get::<i64>(3).map_err(|e| format!("max_seq: {e}"))?.unwrap_or(0);
            let pool = snapshot
                .sku_pools
                .entry((sku_id, location_id))
                .or_insert_with(SkuPoolState::default);
            if max_seq > pool.max_born_seq {
                pool.max_born_seq = max_seq;
            }
        }
        Ok(())
    })?;

    Ok(())
}

fn read_event_from_staging(staging_idx: u32) -> Result<Vec<PocV21Event>, String> {
    let queue = STAGING_QUEUE.share();
    let slot = &queue.entries[staging_idx as usize];
    let payload_offset = slot.payload_offset;
    let payload_length = slot.payload_length;
    let correlation_id = pgrx::Uuid::from_bytes(slot.correlation_id);
    let user_tx_xid = slot.user_tx_xid;
    let event_type_id = slot.event_type_id;
    let enqueued_at = slot.enqueued_at_micros;
    drop(queue);

    let payload_bytes = {
        let arena = SPILLOVER_ARENA.share();
        arena.read_bytes(payload_offset, payload_length)
    };
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| format!("payload parse: {e}"))?;

    // M6.1 (acct-o1yv): wo_complete expands into K components + 1 output.
    if event_type_id == 2 {
        return expand_wo_complete_payload(
            &payload,
            correlation_id,
            user_tx_xid,
            enqueued_at as i64,
        );
    }

    let event_type = match event_type_id {
        1 => PocV21EventType::InvAdjust,
        3 => return Err("wo_start not supported at M1.3".to_string()),
        5 => PocV21EventType::PoReceipt,
        6 => PocV21EventType::SoShipment,
        7 => PocV21EventType::InvIssue,
        _ => return Err(format!("unknown event_type_id={event_type_id}")),
    };

    // E3 (acct-gx1z.1.3): fail-loud on universally-required fields
    // (sku_id, location_id, qty, doc_chrono, document_id). The
    // previous .unwrap_or(0) silently defaulted missing fields to 0,
    // routing envelopes to pool (0, 0) and corrupting audit trails.
    // Keep .unwrap_or(default) for event-type-conditional fields
    // (issue_id only present on inv_issue; unit_cost computed by
    // dispatcher for issue/shipment) and explicit defaults
    // (business_date_jdate = 9999, sub_priority = 0).
    let sku_id = payload
        .get("sku_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "payload missing or malformed field 'sku_id'".to_string())?;
    let location_id = payload
        .get("location_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "payload missing or malformed field 'location_id'".to_string())?;
    let qty = payload
        .get("qty")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "payload missing or malformed field 'qty'".to_string())?;
    let unit_cost = payload.get("unit_cost").and_then(|v| v.as_i64()).unwrap_or(0);
    let issue_id = payload.get("issue_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let business_date_jdate = payload
        .get("business_date_jdate")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(9999);
    let doc_chrono = payload
        .get("doc_chrono")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "payload missing or malformed field 'doc_chrono'".to_string())?;
    let document_id = payload
        .get("document_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "payload missing or malformed field 'document_id'".to_string())?;
    let sub_priority = payload
        .get("sub_priority")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(0);

    Ok(vec![PocV21Event {
        correlation_id,
        issue_id,
        event_type,
        sku_id,
        location_id,
        qty,
        unit_cost,
        business_date_jdate,
        doc_chrono,
        document_id,
        sub_priority,
        user_tx_xid,
        at_micros: enqueued_at as i64,
        wo_id: 0,
        op_id: 0,
    }])
}

/// M6.1 (acct-o1yv): expand a wo_complete payload into K+1 PocV21Events.
///
/// Payload shape:
/// ```json
/// {
///   "wip_account": [wo_id, op_id],
///   "components": [[sku_id, location_id, qty], ...],
///   "output": [sku_id, location_id, qty],
///   "business_date_jdate": ..., "doc_chrono": ..., "document_id": ...
/// }
/// ```
///
/// Each component yields a WoComplete event with qty<0 (consumption);
/// the output yields a WoComplete event with qty>0 (receipt). sub_priority
/// orders components first (0..K-1) then output (K) so the dispatcher's
/// sort processes all components before the output. The output event's
/// `unit_cost` is left at 0 — the dispatcher fills it in from accumulated
/// component cost before calling the method's apply_one.
fn expand_wo_complete_payload(
    payload: &serde_json::Value,
    correlation_id: pgrx::Uuid,
    user_tx_xid: u64,
    at_micros: i64,
) -> Result<Vec<PocV21Event>, String> {
    let wip = payload
        .get("wip_account")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "wo_complete payload missing wip_account".to_string())?;
    if wip.len() != 2 {
        return Err(format!("wo_complete wip_account must be [wo_id, op_id]"));
    }
    let wo_id = wip[0]
        .as_i64()
        .ok_or_else(|| "wo_complete wip_account[0] (wo_id) not an integer".to_string())?;
    let op_id = wip[1]
        .as_i64()
        .ok_or_else(|| "wo_complete wip_account[1] (op_id) not an integer".to_string())?;

    let components = payload
        .get("components")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "wo_complete payload missing components".to_string())?;
    if components.is_empty() {
        return Err("wo_complete components array must be non-empty".to_string());
    }

    let output = payload
        .get("output")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "wo_complete payload missing output".to_string())?;
    if output.len() != 3 {
        return Err("wo_complete output must be [sku_id, location_id, qty]".to_string());
    }
    let output_sku = output[0]
        .as_i64()
        .ok_or_else(|| "wo_complete output[0] (sku_id) not an integer".to_string())?;
    let output_loc = output[1]
        .as_i64()
        .ok_or_else(|| "wo_complete output[1] (location_id) not an integer".to_string())?;
    let output_qty = output[2]
        .as_i64()
        .ok_or_else(|| "wo_complete output[2] (qty) not an integer".to_string())?;
    if output_qty <= 0 {
        return Err(format!("wo_complete output qty must be positive: {output_qty}"));
    }

    let business_date_jdate = payload
        .get("business_date_jdate")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(9999);
    let doc_chrono = payload
        .get("doc_chrono")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "wo_complete payload missing or malformed 'doc_chrono'".to_string())?;
    let document_id = payload
        .get("document_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| "wo_complete payload missing or malformed 'document_id'".to_string())?;

    let mut events = Vec::with_capacity(components.len() + 1);
    for (i, comp) in components.iter().enumerate() {
        let arr = comp
            .as_array()
            .ok_or_else(|| "component entry must be array".to_string())?;
        if arr.len() != 3 {
            return Err("component entry must be [sku_id, location_id, qty]".to_string());
        }
        let sku = arr[0]
            .as_i64()
            .ok_or_else(|| format!("component[{i}][0] (sku_id) not an integer"))?;
        let loc = arr[1]
            .as_i64()
            .ok_or_else(|| format!("component[{i}][1] (location_id) not an integer"))?;
        let cq = arr[2]
            .as_i64()
            .ok_or_else(|| format!("component[{i}][2] (qty) not an integer"))?;
        if cq <= 0 {
            return Err(format!("component qty must be positive: {cq}"));
        }
        events.push(PocV21Event {
            correlation_id,
            issue_id: 0,
            event_type: PocV21EventType::WoComplete,
            sku_id: sku,
            location_id: loc,
            qty: -cq, // negative = consumption
            unit_cost: 0,
            business_date_jdate,
            doc_chrono,
            document_id,
            sub_priority: i as i32,
            user_tx_xid,
            at_micros,
            wo_id,
            op_id,
        });
    }
    // Output: sub_priority strictly greater than any component's, so the
    // dispatch sort places it after all components for this envelope.
    events.push(PocV21Event {
        correlation_id,
        issue_id: 0,
        event_type: PocV21EventType::WoComplete,
        sku_id: output_sku,
        location_id: output_loc,
        qty: output_qty,
        unit_cost: 0, // dispatcher fills in: total_component_cost / output_qty
        business_date_jdate,
        doc_chrono,
        document_id,
        sub_priority: 1_000_000, // higher than any plausible component sub_priority
        user_tx_xid,
        at_micros,
        wo_id,
        op_id,
    });
    Ok(events)
}

fn cleanup_after_superbatch(
    cq_idx: u32,
    superbatch_id: u64,
    staging_indices: &[u32],
    staging_offsets_off: u32,
    sku_keys_off: u32,
) {
    // Free staging-entry arena blocks; CAS staging.valid 3→0.
    //
    // sb_id guard (M5c.2 acct-1hyx): cleanup must verify this committer
    // OWNS the slot before CAS 3→0. The M5c.2 eject path writes
    // staging.valid 3→1 + sb_id=0 INSIDE the sub-tx; cleanup runs
    // AFTER sub-tx commit. In the gap (~10ms of sub-tx release), a
    // concurrent router can re-pack the entry: CAS 1→2, store new
    // sb_id, CAS 2→3. If cleanup then blindly CAS 3→0, it would
    // wrongly free the NEW SuperBatch's arena and leave the staging
    // entry at valid=0 — router stops re-routing, committer stops
    // cycling, the entry is silently abandoned with garbage
    // payload_offset. Guard: only CAS 3→0 when staging.sb_id matches
    // our sb_id (Acquire-load pairs with the router's Release store
    // and the eject path's Release store of 0).
    //
    // R7-shaped invariant (M5b.1 acct-k7b2): the router's Phase 6
    // stamps superbatch_id (Release) then CAS valid 2→3 (Release).
    // If the router dies between those two stores for SOME of the
    // packed entries, the queue still references them via
    // staging_entry_offsets and the committer can read their payload
    // from arena, BUT the CAS 3→0 in cleanup will fail because they
    // are still at valid==2. Fall back to CAS 2→0 so we still free
    // the arena for those entries — otherwise the slot leaks. The
    // sb_id guard does NOT apply to this 2→0 fallback: by definition
    // the router died mid-Phase-6 with sb_id potentially un-stored,
    // so checking sb_id-match would refuse to cleanup these recovery
    // entries.
    for &s_idx in staging_indices {
        let (payload_off, sku_off, wip_off, cas_ok) = {
            let queue = STAGING_QUEUE.share();
            let slot = &queue.entries[s_idx as usize];
            let p = slot.payload_offset;
            let s = slot.sku_pool_keys_offset;
            let w = slot.wip_pool_keys_offset;
            let observed_sb = slot.superbatch_id.load(Acquire);
            // 3→0 path: this entry processed through Phase 6 normally
            // and belongs to our SuperBatch (sb_id guard).
            // 2→0 fallback: M5b.1 router-mid-Phase-6-death recovery —
            // the entry stuck at valid=2 because Phase 6's CAS 2→3
            // didn't complete. Gate on eject_count==0 so we don't
            // race-free an entry that was ejected by THIS committer's
            // Step 2.45 and re-routed by another router tick before
            // we reached cleanup (M5b.2/acct-kt61 race).
            //
            // B7 invariant (acct-gx1z.1.7): the eject path is
            // `eject_count.fetch_add(1, Release)` THEN
            // `valid.compare_exchange(3, 1, Release, …)`. With our
            // `Acquire`-load of eject_count below paired against
            // that Release-increment, observing `eject_count == 0`
            // AND `valid == 2` proves no eject is in flight — the
            // bump-then-flip ordering means we'd see eject_count > 0
            // first if one had started. The read-then-CAS gap is
            // tolerated: a concurrent eject AFTER our load can only
            // cause our CAS(2→0) to fail (valid is now != 2), which
            // is the desired no-op outcome. Dead-PID rescue in
            // try_recover_orphan (recovery.rs) covers any residual.
            let observed_eject = slot.eject_count.load(Acquire);
            let ok = (observed_sb == superbatch_id
                && slot.valid.compare_exchange(3, 0, Release, Relaxed).is_ok())
                || (observed_eject == 0
                    && slot.valid.compare_exchange(2, 0, Release, Relaxed).is_ok());
            if ok {
                // M5c.1 (acct-r0aa): wake any waiter on backpressure CV.
                // Broadcast inside the share guard — the CV has its own
                // internal slock and re-acquiring STAGING_QUEUE.share()
                // here would be a no-op-but-confusing nested lock.
                signal_staging_slot_freed(&queue);
            }
            (p, s, w, ok)
        };
        if cas_ok {
            let mut arena = SPILLOVER_ARENA.exclusive();
            if payload_off != 0 {
                arena.free(payload_off);
            }
            if sku_off != 0 {
                arena.free(sku_off);
            }
            if wip_off != 0 {
                arena.free(wip_off);
            }
        }
        // CAS failure = ejected; leave arena blocks for the router to re-claim.
    }

    // Free CommitterQueueEntry's own arena blocks (staging_offsets +
    // sku_pool_keys + wip_pool_keys mirror).
    //
    // B6 invariant (acct-gx1z.1.7): wip_pool_keys_offset is
    // written ONCE by the router during Phase 1 SuperBatch assembly
    // and never mutated thereafter. The committer reads it here
    // post-sub-tx-release without a fence because there's no
    // concurrent writer to synchronize against. If a future
    // refactor introduces router re-mutation between Phase 1 and
    // committer cleanup, this read becomes fragile — re-audit.
    let wip_keys_off = {
        let queue = COMMITTER_QUEUE.share();
        queue.entries[cq_idx as usize].wip_pool_keys_offset
    };
    {
        let mut arena = SPILLOVER_ARENA.exclusive();
        if staging_offsets_off != 0 {
            arena.free(staging_offsets_off);
        }
        if sku_keys_off != 0 {
            arena.free(sku_keys_off);
        }
        if wip_keys_off != 0 {
            arena.free(wip_keys_off);
        }
    }

    // CAS CommitterQueueEntry valid 2→3 (completed), then 3→0.
    {
        let queue = COMMITTER_QUEUE.share();
        let slot = &queue.entries[cq_idx as usize];
        let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
        let _ = slot.valid.compare_exchange(3, 0, Release, Relaxed);
        slot.committer_pid.store(0, Relaxed);
        slot.committer_acquired_at_ns.store(0, Relaxed);
        slot.committer_tx_id.store(0, Relaxed);
    }
}

/// Synchronous committer-tick helper for tests: claims one entry and
/// runs the full pipeline. Returns true if work was done.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_committer_tick() -> bool {
    let claim = claim_next_committer_entry();
    match claim {
        Some(cq_idx) => {
            // Run inside an outer transaction so SPI works the same way
            // as in the BGWorker.
            let _ = process_superbatch(cq_idx);
            true
        }
        None => false,
    }
}
