//! Greedy Window Router BGWorker (M3.1, acct-r29s; packing rule
//! inverted at acct-0frn per amendment `poc-v2.1-amendment-0.2`).
//!
//! Scans up to `router_window_size` pending staging entries per tick,
//! packs them into one SuperBatch in head-order up to `batch_size_max`
//! (regardless of pool-key overlap), and pushes that SuperBatch onto
//! the committer queue. WIP pool keys are carried through but NOT
//! consulted at routing (spec §1.5 — WIP pools uncontended by
//! construction).
//!
//! ## Packing rule — grouped, not disjoint (acct-0frn)
//!
//! The original rule was "greedy disjoint pool-key cover": envelopes
//! that shared a SKU pool key were sent to different SuperBatches.
//! That created cross-SB contention — two committers would each claim
//! one of the overlapping SBs and serialize on the shared `pool_locks`
//! row, manifesting as hangs in M8.1 shapes S3 / S4 / overlap-on-wrap
//! S6 / S7 / S8.
//!
//! The current rule packs overlapping envelopes INTO ONE SuperBatch.
//! One committer processes the batch sequentially in chrono order
//! (Step 4 in committer.rs); running-avg / FIFO are naturally
//! sequential operations on a shared pool. Disjoint envelopes are
//! also packed together up to `batch_size_max` — multi-committer
//! parallelism comes across ticks, not within a tick. The
//! per-cluster split design (one SB per disjoint cluster within one
//! tick) is a future optimization tracked in the meta-epic `acct-shpc`.
//!
//! ## Data-before-flag invariant (spec §1.6, §3.3)
//!
//! For every packed staging entry, `superbatch_id` is stored with
//! **Release** semantics BEFORE the CAS `valid` 2→3 (also Release).
//! Readers (recovery sweep) use **Acquire** when loading
//! `superbatch_id` AFTER confirming `valid==3`. Reversing the order
//! creates a window where a router crash leaves `valid=3` with
//! `superbatch_id=0`; the sweep's classification logic depends on
//! seeing them consistent. M5b.2 (`acct-rxax`) ships the dedicated
//! ordering stress test.
//!
//! ## Q-E (head-scan vs sliding cursor)
//!
//! M3.1 lean: head-scan per spec §1.8. Sliding-cursor alternative is
//! filed as `acct-v21-fu-router-sliding-window` for measurement.
//! Head-scan walks the ring from `staging.head` for at most
//! `staging_capacity` slots, collecting up to `router_window_size`
//! pending candidates per tick. Routed/empty slots are O(1) skips
//! (atomic load + compare).
//!
//! ## Q-F (committer/router architecture)
//!
//! M3.1 lean: option (a) — dedicated router BGWorker + N committer
//! BGWorkers (each a separate process; no caller-promotion).

use crate::{
    COMMITTER_QUEUE, POC_V21_COMMITTER_QUEUE_SIZE, POC_V21_STAGING_QUEUE_SIZE, SPILLOVER_ARENA,
    STAGING_QUEUE, batch_size_max_now, router_starvation_threshold_ticks_now,
    router_window_size_now, signal_staging_slot_freed, target_database_str,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Spin until M5d.1 startup-recovery has finished its sweep and set
/// the `recovery_complete` flag. Sleep in 50ms increments and respect
/// SIGTERM by exiting if the postmaster asks the worker to quit before
/// recovery finishes. Called from router_main + committer_main at
/// startup, exactly once per worker process.
pub(crate) fn wait_for_recovery_complete() {
    loop {
        if COMMITTER_QUEUE.share().recovery_complete.load(Acquire) != 0 {
            return;
        }
        // wait_latch returns false on SIGTERM. Honor it so we don't
        // leave a worker spinning if the postmaster is shutting down
        // mid-recovery.
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
            return;
        }
    }
}

/// Router BGWorker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_router_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // M5d.1 (acct-y0bp): wait for the startup-recovery worker to finish
    // classifying in-flight submission_status rows. Both workers start at
    // RecoveryFinished; without this gate, the router could route new
    // traffic for a stale correlation_id concurrent with the recovery
    // sweep, leading to (cost rows exist, status='failed') outcomes.
    wait_for_recovery_complete();

    // M5b.1 (acct-k7b2): boot-time recovery sweep. Run once before
    // entering the wait-latch loop so any router-orphaned state from
    // a prior router crash is reconciled before the new router starts
    // packing. The committer pool may already be running and claiming
    // queue entries; the cleanup CAS 2→0 fallback in
    // cleanup_after_superbatch (mirrored here) handles staging
    // entries the old router never reached in Phase 6.
    BackgroundWorker::transaction(|| {
        let _ = try_recover_router_orphan();
    });
    // M5d.2 (acct-3rc1): seed the audit timestamp so the first periodic
    // pass fires ~60s after the boot sweep, not immediately.
    {
        let now_ns = crate::now_ns();
        COMMITTER_QUEUE
            .share()
            .audit_last_run_at_ns
            .store(now_ns, Release);
    }

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        // M5d.2 (acct-3rc1): periodic slot-leak audit. Runs at most
        // once every AUDIT_INTERVAL_NS; bounded pass (iterates fixed
        // staging + committer queue sizes) so it doesn't perturb
        // router tick latency on the off-cycle. Wrapped in a
        // BackgroundWorker::transaction because Pattern B + Pattern C
        // UPSERT submission_status.
        if audit_due() {
            BackgroundWorker::transaction(|| {
                run_audit_sweep();
            });
        }
        // Drain SuperBatches until the queue is empty (or committer
        // queue is full / arena is exhausted; those conditions roll
        // back partial work and return 0 from this tick).
        while router_tick() > 0 {}
    }
}

/// One scan-and-pack iteration. Returns the number of SuperBatches
/// produced (0 or 1 — looping happens in the BGWorker).
fn router_tick() -> u32 {
    let staging_capacity = POC_V21_STAGING_QUEUE_SIZE as u32;
    let committer_capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let window_limit = router_window_size_now().max(1) as u32;
    let batch_max = batch_size_max_now().max(1) as u16;
    let starvation_threshold = router_starvation_threshold_ticks_now().max(1) as u32;

    // Always increment ticks_total — observability for empty ticks too.
    {
        let queue = COMMITTER_QUEUE.share();
        queue.router_ticks_total.fetch_add(1, Relaxed);
    }

    // --- Phase 1: scan staging head; collect candidate metadata. ---
    let candidates_meta = collect_candidates(staging_capacity, window_limit);
    if candidates_meta.is_empty() {
        return 0;
    }
    {
        let queue = COMMITTER_QUEUE.share();
        queue
            .router_entries_scanned_total
            .fetch_add(candidates_meta.len() as u64, Relaxed);
    }

    // --- Phase 2: hydrate pool keys for each candidate from arena. ---
    let candidates = hydrate_candidates(&candidates_meta);

    // --- Phase 3: grouped pack (head-order) with fairness backstop. ---
    //
    // Per `poc-v2.1-amendment-0.2` (acct-0frn / meta `acct-shpc`):
    // envelopes are packed in head-order into one SuperBatch up to
    // `batch_max`, regardless of pool-key overlap. The shared-pool
    // case (two envelopes touching the same SKU) is intentionally
    // pulled INTO one SB — one committer processes them sequentially
    // in chrono order inside one transaction with one set of
    // lex-locks. The disjoint-pack rule it replaces forced overlapping
    // envelopes into separate SBs and triggered cross-committer hangs
    // on shared `pool_locks` (M8.1 shapes S3 / S4 / overlap-on-wrap
    // S6 / S7 / S8).
    //
    // Spec §1.8: when a candidate's starvation_count >= threshold AND
    // the current SuperBatch is empty, force-pack as size-1 SuperBatch
    // and break (ensure the starved entry gets through). Under the
    // grouped rule starvation is bounded to queue/arena pressure and
    // CAS-race losses (no more disjointness-reject path), so the
    // counter rarely populates beyond 0/1 — but the safety net is
    // retained.
    //
    // Packed candidates have their starvation counter cleared.
    // CAS-race-lost candidates have their starvation counter
    // incremented.
    let mut lock_set: HashSet<(i64, i64)> = HashSet::new();
    let mut wip_union: HashSet<(i64, i64)> = HashSet::new();
    let mut packed: Vec<Candidate> = Vec::new();
    let mut forced = false;
    let mut starv = starvation_map().lock().expect("starvation_map lock");

    for cand in candidates.into_iter() {
        if (packed.len() as u16) >= batch_max {
            break;
        }

        // Force-pack gate (runs only when SuperBatch is still empty).
        if packed.is_empty() {
            let current = *starv.get(&cand.request_seq).unwrap_or(&0);
            if current >= starvation_threshold {
                let cas_ok = {
                    let queue = STAGING_QUEUE.share();
                    queue.entries[cand.staging_idx as usize]
                        .valid
                        .compare_exchange(1, 2, Acquire, Relaxed)
                        .is_ok()
                };
                if cas_ok {
                    for k in &cand.sku_pool_keys {
                        lock_set.insert(*k);
                    }
                    for k in &cand.wip_pool_keys {
                        wip_union.insert(*k);
                    }
                    starv.remove(&cand.request_seq);
                    packed.push(cand);
                    forced = true;
                    break; // size-1 forced SuperBatch
                } else {
                    // CAS race lost (another tick took this entry).
                    starv.remove(&cand.request_seq);
                    continue;
                }
            }
        }

        // CAS valid 1→2 (pending → processing). Acquire on success so
        // subsequent reads from this staging entry see the caller's
        // payload writes.
        //
        // The disjoint-pool reject that used to live here has been
        // deleted: overlapping envelopes are now WANTED inside one SB.
        let cas_ok = {
            let queue = STAGING_QUEUE.share();
            queue.entries[cand.staging_idx as usize]
                .valid
                .compare_exchange(1, 2, Acquire, Relaxed)
                .is_ok()
        };
        if !cas_ok {
            // Lost the race (another router tick? recovery sweep?).
            // Don't extend lock_set since we don't own this entry.
            *starv.entry(cand.request_seq).or_insert(0) += 1;
            continue;
        }
        for k in &cand.sku_pool_keys {
            lock_set.insert(*k);
        }
        for k in &cand.wip_pool_keys {
            wip_union.insert(*k);
        }
        starv.remove(&cand.request_seq);
        packed.push(cand);
    }

    drop(starv); // release before arena/queue work

    if packed.is_empty() {
        return 0;
    }

    // --- Phase 4: arena alloc for SuperBatch-owned blocks. ---
    let envelope_count = packed.len() as u16;
    let mut sku_union_sorted: Vec<(i64, i64)> = lock_set.into_iter().collect();
    sku_union_sorted.sort();
    let mut wip_union_sorted: Vec<(i64, i64)> = wip_union.into_iter().collect();
    wip_union_sorted.sort();
    let sku_count = sku_union_sorted.len() as u16;
    let wip_count = wip_union_sorted.len() as u16;

    let arena_alloc = allocate_superbatch_arena(
        envelope_count,
        &packed,
        sku_count,
        &sku_union_sorted,
        wip_count,
        &wip_union_sorted,
    );
    let (staging_offsets_off, sku_keys_off, wip_keys_off) = match arena_alloc {
        Some(t) => t,
        None => {
            // Arena exhausted; roll back the CAS 1→2's so the next
            // tick (or the next router after free-list churn) can retry.
            rollback_packed_to_pending(&packed);
            return 0;
        }
    };

    // --- Phase 5: claim a free CommitterQueueEntry, write fields, CAS 0→1. ---
    let cq_result = {
        let mut queue = COMMITTER_QUEUE.exclusive();
        let tail = queue.tail.load(Relaxed);
        let mut found: Option<u32> = None;
        for i in 0..committer_capacity {
            let idx = ((tail + i) % committer_capacity) as usize;
            if queue.entries[idx].valid.load(Relaxed) == 0 {
                found = Some(idx as u32);
                break;
            }
        }
        if let Some(cq_idx) = found {
            let sb_id = queue.next_superbatch_id.fetch_add(1, Relaxed) + 1;
            let now_micros = crate::now_us();
            let slot = &mut queue.entries[cq_idx as usize];
            slot.superbatch_id = sb_id;
            slot.envelope_count = envelope_count;
            slot.staging_entry_offsets = staging_offsets_off;
            slot.sku_pool_keys_offset = sku_keys_off;
            slot.sku_pool_keys_count = sku_count;
            slot.wip_pool_keys_offset = wip_keys_off;
            slot.wip_pool_keys_count = wip_count;
            slot.committer_pid.store(0, Relaxed);
            slot.committer_acquired_at_ns.store(0, Relaxed);
            slot.committer_tx_id.store(0, Relaxed);
            slot.enqueued_at_micros = now_micros;
            let _ = slot.valid.compare_exchange(0, 1, Release, Relaxed);
            queue.tail.store((tail + 1) % committer_capacity, Relaxed);
            Some(sb_id)
        } else {
            None
        }
    };

    let sb_id = match cq_result {
        Some(id) => id,
        None => {
            // Committer queue full. Roll back the CAS 1→2's; free the
            // arena blocks we just allocated. Next tick will retry.
            rollback_packed_to_pending(&packed);
            free_superbatch_arena(staging_offsets_off, sku_keys_off, wip_keys_off);
            return 0;
        }
    };

    // --- Phase 6: stamp staging entries with sb_id; CAS valid 2→3. ---
    //
    // Data-before-flag invariant: superbatch_id MUST be stored BEFORE
    // the valid CAS, both with Release ordering. The recovery sweep
    // (§3.3) Acquires `valid` first; if it sees 3, it then Acquires
    // `superbatch_id` and trusts that the store is visible. Reversing
    // these gives a recovery window where valid=3 with stale sb_id=0.
    //
    // M5b.2 (acct-ud4h): the two test-only hooks below are no-ops in
    // production (both atomics default to 0/false).
    //   - test_inject_router_delay_us > 0: sleep between the two
    //     stores, enlarging the window for concurrent readers to
    //     observe whichever interleaving is in effect.
    //   - test_reorder_router_stores=1: reverse the order (CAS first,
    //     then store). NEGATIVE CONTROL — breaks the invariant so the
    //     test can confirm it would observably catch a regression.
    {
        let delay_us = test_inject_router_delay_us();
        let reorder = test_reorder_router_stores();
        let queue = STAGING_QUEUE.share();
        for cand in &packed {
            let slot = &queue.entries[cand.staging_idx as usize];
            if reorder {
                let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
                if delay_us > 0 {
                    std::thread::sleep(Duration::from_micros(delay_us as u64));
                }
                slot.superbatch_id.store(sb_id, Release);
            } else {
                slot.superbatch_id.store(sb_id, Release);
                if delay_us > 0 {
                    std::thread::sleep(Duration::from_micros(delay_us as u64));
                }
                let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
            }
        }
    }

    // --- Phase 7: stats. ---
    record_superbatch_stats(envelope_count, forced);

    // M3.1 punts on SetLatch broadcast — committer BGWorker wakes on
    // its 50ms tick. Spec §1.8 calls for SetLatch on idle committers;
    // proper PID registry is M4.1 / M3.2-followup work since latency
    // here is bounded by the committer tick anyway (50ms p99 vs
    // ~500us router tick).

    1
}

#[derive(Debug)]
struct CandidateMeta {
    staging_idx: u32,
    request_seq: u64,
    sku_pool_keys_offset: u32,
    sku_pool_count: u16,
    wip_pool_keys_offset: u32,
    wip_pool_count: u16,
}

#[derive(Debug)]
struct Candidate {
    staging_idx: u32,
    request_seq: u64,
    sku_pool_keys: Vec<(i64, i64)>,
    wip_pool_keys: Vec<(i64, i64)>,
}

/// Walk the staging ring from `head` and collect up to `window_limit`
/// pending (valid==1) entries' metadata. Bounded at staging_capacity
/// total slot inspections per call.
fn collect_candidates(staging_capacity: u32, window_limit: u32) -> Vec<CandidateMeta> {
    let mut out: Vec<CandidateMeta> = Vec::new();
    let queue = STAGING_QUEUE.share();
    let head = queue.head.load(Relaxed);
    let mut scanned: u32 = 0;
    while scanned < staging_capacity && (out.len() as u32) < window_limit {
        let idx = ((head + scanned) % staging_capacity) as usize;
        let slot = &queue.entries[idx];
        if slot.valid.load(Relaxed) == 1 {
            out.push(CandidateMeta {
                staging_idx: idx as u32,
                request_seq: slot.request_seq,
                sku_pool_keys_offset: slot.sku_pool_keys_offset,
                sku_pool_count: slot.sku_pool_count,
                wip_pool_keys_offset: slot.wip_pool_keys_offset,
                wip_pool_count: slot.wip_pool_count,
            });
        }
        scanned += 1;
    }
    out
}

/// Read each candidate's SKU + WIP pool keys from the arena. Holds
/// the arena share lock once for the whole batch.
fn hydrate_candidates(metas: &[CandidateMeta]) -> Vec<Candidate> {
    let arena = SPILLOVER_ARENA.share();
    metas
        .iter()
        .map(|m| {
            let sku_pool_keys: Vec<(i64, i64)> =
                if m.sku_pool_count > 0 && m.sku_pool_keys_offset != 0 {
                    let bytes = arena.read_bytes(m.sku_pool_keys_offset, m.sku_pool_count as u32 * 16);
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
            let wip_pool_keys: Vec<(i64, i64)> =
                if m.wip_pool_count > 0 && m.wip_pool_keys_offset != 0 {
                    let bytes = arena.read_bytes(m.wip_pool_keys_offset, m.wip_pool_count as u32 * 16);
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
            Candidate {
                staging_idx: m.staging_idx,
                request_seq: m.request_seq,
                sku_pool_keys,
                wip_pool_keys,
            }
        })
        .collect()
}

/// Allocate the three SuperBatch-owned arena blocks atomically — if
/// any fails, free whatever succeeded and return None. Writes the
/// block contents on success.
fn allocate_superbatch_arena(
    envelope_count: u16,
    packed: &[Candidate],
    sku_count: u16,
    sku_sorted: &[(i64, i64)],
    wip_count: u16,
    wip_sorted: &[(i64, i64)],
) -> Option<(u32, u32, u32)> {
    let mut arena = SPILLOVER_ARENA.exclusive();
    let offsets_bytes = (envelope_count as u32) * 4;
    let so_off = arena.alloc(offsets_bytes)?;
    let sku_off = if sku_count > 0 {
        match arena.alloc(sku_count as u32 * 16) {
            Some(v) => v,
            None => {
                arena.free(so_off);
                return None;
            }
        }
    } else {
        0
    };
    let wip_off = if wip_count > 0 {
        match arena.alloc(wip_count as u32 * 16) {
            Some(v) => v,
            None => {
                arena.free(so_off);
                if sku_off != 0 {
                    arena.free(sku_off);
                }
                return None;
            }
        }
    } else {
        0
    };

    // Write staging-entry indices in pack order.
    let mut so_buf: Vec<u8> = Vec::with_capacity(offsets_bytes as usize);
    for cand in packed {
        so_buf.extend_from_slice(&cand.staging_idx.to_le_bytes());
    }
    arena.write_bytes(so_off, &so_buf);

    // Write deduplicated/sorted SKU pool keys.
    if sku_count > 0 {
        let mut sku_buf: Vec<u8> = Vec::with_capacity(sku_count as usize * 16);
        for k in sku_sorted {
            sku_buf.extend_from_slice(&k.0.to_le_bytes());
            sku_buf.extend_from_slice(&k.1.to_le_bytes());
        }
        arena.write_bytes(sku_off, &sku_buf);
    }

    // Write deduplicated/sorted WIP pool keys.
    if wip_count > 0 {
        let mut wip_buf: Vec<u8> = Vec::with_capacity(wip_count as usize * 16);
        for k in wip_sorted {
            wip_buf.extend_from_slice(&k.0.to_le_bytes());
            wip_buf.extend_from_slice(&k.1.to_le_bytes());
        }
        arena.write_bytes(wip_off, &wip_buf);
    }

    Some((so_off, sku_off, wip_off))
}

/// CAS each packed staging entry's valid 2→1 (processing → pending).
/// Used when arena alloc or CommitterQueue claim fails post-CAS.
fn rollback_packed_to_pending(packed: &[Candidate]) {
    let queue = STAGING_QUEUE.share();
    for cand in packed {
        let _ = queue.entries[cand.staging_idx as usize]
            .valid
            .compare_exchange(2, 1, Release, Relaxed);
    }
}

fn free_superbatch_arena(staging_offsets_off: u32, sku_keys_off: u32, wip_keys_off: u32) {
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

// ── Starvation map (M3.2 acct-evyq) ─────────────────────────────────
//
// Per-envelope tick counters keyed by request_seq. Process-local — the
// router BGWorker's instance is the load-bearing one for production
// fairness; the test backend's instance is incidental (cleared on each
// test reset_state since envelopes get fresh request_seqs). Lost on
// router death (acceptable per spec §7 Q-B; production hardening filed
// as acct-v21-fu-router-starvation-persistent).

fn starvation_map() -> &'static Mutex<HashMap<u64, u32>> {
    static MAP: OnceLock<Mutex<HashMap<u64, u32>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

// ── Router stats (lightweight; M3.3 builds the proper API) ──────────
//
// Counters live in shmem on CommitterQueue (not process-local statics)
// so the router BGWorker's increments are visible from any backend
// that reads via the SQL accessors. All atomics — no LWLock contention
// on the hot path.

/// Log2-spaced bucket index for an envelope count (0..=7). Bucket 0 is
/// size-1, bucket 7 is the >=128 overflow.
fn envelope_histogram_bucket(envelope_count: u16) -> usize {
    if envelope_count == 0 {
        return 0;
    }
    // 31 - leading_zeros(n) for n>=1 gives floor(log2(n)).
    let lg = 31 - (envelope_count as u32).leading_zeros();
    (lg as usize).min(7)
}

fn record_superbatch_stats(envelope_count: u16, forced: bool) {
    let queue = COMMITTER_QUEUE.share();
    queue.router_superbatch_count.fetch_add(1, Relaxed);
    queue
        .router_total_envelopes
        .fetch_add(envelope_count as u64, Relaxed);
    if forced {
        queue.router_force_pack_count.fetch_add(1, Relaxed);
    }
    let bucket = envelope_histogram_bucket(envelope_count);
    queue.router_envelope_histogram[bucket].fetch_add(1, Relaxed);
    let mut prev = queue.router_max_envelope_count.load(Relaxed);
    while envelope_count > prev {
        match queue.router_max_envelope_count.compare_exchange(
            prev,
            envelope_count,
            Relaxed,
            Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => prev = observed,
        }
    }
}

/// Reset router counters; for test fixtures.
#[pg_extern]
fn poc_v21_router_stats_reset() {
    let queue = COMMITTER_QUEUE.share();
    queue.router_superbatch_count.store(0, Relaxed);
    queue.router_total_envelopes.store(0, Relaxed);
    queue.router_max_envelope_count.store(0, Relaxed);
    queue.router_force_pack_count.store(0, Relaxed);
    queue.router_ticks_total.store(0, Relaxed);
    queue.router_entries_scanned_total.store(0, Relaxed);
    queue.committer_drains_total.store(0, Relaxed);
    for b in queue.router_envelope_histogram.iter() {
        b.store(0, Relaxed);
    }
    if let Ok(mut m) = starvation_map().lock() {
        m.clear();
    }
}

/// Total SuperBatches assembled since last reset.
#[pg_extern]
fn poc_v21_router_superbatch_count() -> i64 {
    let queue = COMMITTER_QUEUE.share();
    queue.router_superbatch_count.load(Relaxed) as i64
}

/// Largest envelope_count observed in a single SuperBatch.
#[pg_extern]
fn poc_v21_router_max_envelope_count() -> i32 {
    let queue = COMMITTER_QUEUE.share();
    queue.router_max_envelope_count.load(Relaxed) as i32
}

/// Total envelopes packed across all SuperBatches.
#[pg_extern]
fn poc_v21_router_total_envelopes() -> i64 {
    let queue = COMMITTER_QUEUE.share();
    queue.router_total_envelopes.load(Relaxed) as i64
}

/// Total router_tick invocations since last reset. Used by
/// tests/common/mod.rs `reset_state` as an idle-detection signal:
/// two consecutive identical readings with in_flight=0 confirm
/// the router has gone idle and won't increment stats counters
/// during the next test's setup window.
#[pg_extern]
fn poc_v21_router_ticks_total() -> i64 {
    let queue = COMMITTER_QUEUE.share();
    queue.router_ticks_total.load(Relaxed) as i64
}

/// Total force-packs (starvation fairness backstop triggered). Each
/// counts one SuperBatch where the starvation threshold was met and a
/// candidate was force-packed as size-1.
#[pg_extern]
fn poc_v21_router_force_pack_count() -> i64 {
    let queue = COMMITTER_QUEUE.share();
    queue.router_force_pack_count.load(Relaxed) as i64
}

/// Aggregated router + committer observability stats for §4.3 R1/R2
/// validation. Returns one row per metric so callers can `WHERE
/// stat_name=...` to extract specific values. All values are atomic
/// loads (Relaxed) — eventual consistency is acceptable for
/// observability; no LWLock held.
///
/// Stats emitted:
///   superbatch_count          — total SuperBatches assembled
///   total_envelopes           — total envelopes packed across all batches
///   force_pack_count          — fairness-backstop force-packs
///   max_envelope_count        — largest single-batch envelope count seen
///   ticks_total               — every router_tick invocation incl. empty
///   entries_scanned_total     — sum of candidates collected per tick
///   committer_drains_total    — successful committer SuperBatch drains
///   avg_envelopes_per_sb      — total_envelopes / superbatch_count
///   packing_efficiency        — avg_envelopes_per_sb / batch_size_max
///                               (R1; redefinition under grouped rule
///                               tracked as `acct-shpc.2` — singleton-SB
///                               fraction and cluster-merge counters
///                               replace this once they land)
///   pack_yield_per_tick       — superbatch_count / ticks_total
///   histogram_bucket_<N>      — count of SuperBatches with envelope_count in
///                               2^N..2^(N+1)-1 (bucket 7 is the >=128 overflow).
#[pg_extern]
fn poc_v21_router_stats() -> TableIterator<
    'static,
    (
        name!(stat_name, String),
        name!(stat_value, f64),
    ),
> {
    let queue = COMMITTER_QUEUE.share();
    let sb_count = queue.router_superbatch_count.load(Relaxed);
    let total_env = queue.router_total_envelopes.load(Relaxed);
    let force_pack = queue.router_force_pack_count.load(Relaxed);
    let ticks = queue.router_ticks_total.load(Relaxed);
    let scanned = queue.router_entries_scanned_total.load(Relaxed);
    let drains = queue.committer_drains_total.load(Relaxed);
    let max_env = queue.router_max_envelope_count.load(Relaxed);
    let batch_max = batch_size_max_now().max(1) as u64;
    let mut histogram: [u64; 8] = [0; 8];
    for (i, b) in queue.router_envelope_histogram.iter().enumerate() {
        histogram[i] = b.load(Relaxed);
    }

    let avg_envelopes_per_sb = if sb_count > 0 {
        total_env as f64 / sb_count as f64
    } else {
        0.0
    };
    let packing_efficiency = avg_envelopes_per_sb / batch_max as f64;
    let pack_yield_per_tick = if ticks > 0 {
        sb_count as f64 / ticks as f64
    } else {
        0.0
    };

    // M7.1 (acct-byue): envelopes-per-SuperBatch p50/p99 derived from the
    // 8-bucket log2 histogram. Bucket i covers [2^i, 2^(i+1)) envelopes;
    // bucket 7 is the >=128 overflow. Quantile output is the bucket's
    // midpoint; ballpark resolution is adequate for §4.3 R1/R2 reporting.
    let total_sb_in_hist: u64 = histogram.iter().sum();
    let envelopes_per_sb_p50 = router_hist_quantile_midpoint(&histogram, total_sb_in_hist, 0.50);
    let envelopes_per_sb_p99 = router_hist_quantile_midpoint(&histogram, total_sb_in_hist, 0.99);

    let mut rows: Vec<(String, f64)> = vec![
        ("superbatch_count".into(), sb_count as f64),
        ("total_envelopes".into(), total_env as f64),
        ("force_pack_count".into(), force_pack as f64),
        ("max_envelope_count".into(), max_env as f64),
        ("ticks_total".into(), ticks as f64),
        ("entries_scanned_total".into(), scanned as f64),
        ("committer_drains_total".into(), drains as f64),
        ("avg_envelopes_per_sb".into(), avg_envelopes_per_sb),
        ("packing_efficiency".into(), packing_efficiency),
        ("pack_yield_per_tick".into(), pack_yield_per_tick),
        ("batch_size_max_guc".into(), batch_max as f64),
        ("envelopes_per_sb_p50".into(), envelopes_per_sb_p50),
        ("envelopes_per_sb_p99".into(), envelopes_per_sb_p99),
    ];
    for (i, count) in histogram.iter().enumerate() {
        rows.push((format!("histogram_bucket_{}", i), *count as f64));
    }
    TableIterator::new(rows.into_iter())
}

/// M7.1: bucket-quantile midpoint for the 8-bucket SuperBatch envelope
/// histogram. Bucket i covers [2^i, 2^(i+1)) envelopes; midpoint is
/// 1.5 × 2^i. Bucket 7 is the >=128 overflow — its "midpoint" is reported
/// as 192 (a conservative single-step estimate; actual values can be
/// arbitrarily larger but the bake-off's batch_size_max GUC caps real
/// envelopes-per-sb well below this in practice).
fn router_hist_quantile_midpoint(hist: &[u64; 8], total: u64, q: f64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let target = (total as f64 * q).ceil() as u64;
    let mut cum: u64 = 0;
    for (i, &c) in hist.iter().enumerate() {
        cum += c;
        if cum >= target {
            let lo = 1u64 << i;
            let hi = 1u64 << (i + 1);
            return (lo + hi) as f64 / 2.0;
        }
    }
    192.0
}

// ── Test SQL surface ────────────────────────────────────────────────

/// Run one router tick. Returns true if at least one SuperBatch was
/// assembled this call. Used by tests to drive the router
/// deterministically.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_router_tick() -> bool {
    router_tick() > 0
}

/// Drain the router until no pending entry remains. Returns the count
/// of SuperBatches assembled. Used by the property test fixture to
/// flush quickly without waiting on the BGWorker's 50ms tick.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_router_drain() -> i64 {
    let mut total: u64 = 0;
    loop {
        let n = router_tick();
        if n == 0 {
            break;
        }
        total += n as u64;
    }
    total as i64
}

// ── M5b.1 (acct-k7b2): router boot recovery sweep ───────────────────
//
// Recover from router BGWorker SIGKILL or panic per spec §3.3.
// Postmaster restarts the router; the new router runs this sweep
// before resuming packing.
//
// Three-phase shape:
//
//   Phase 1 (queue-driven re-stamp): for each CommitterQueueEntry at
//     valid==1 (ready, no committer claim yet), iterate linked staging
//     entries. Any linked staging at (valid==2, sb_id==0) means the
//     old router pushed the queue entry (Phase 5) but died before
//     completing the per-entry stamp+CAS (Phase 6). Roll FORWARD:
//     store sb_id (Release) + CAS staging 2→3 (Release). The
//     committer's normal cleanup will free arena correctly.
//
//   Phase 2 (queue-driven take-over): for each CommitterQueueEntry at
//     valid==3 (completed) with a dead committer_pid (kill(0)==ESRCH),
//     sweep takes ownership: walks linked staging entries, frees
//     per-entry arena, CAS staging 3→0 (fallback 2→0). Frees the
//     queue-entry-owned arena (staging_entry_offsets + sorted
//     sku/wip pool-keys mirrors). CAS queue 3→0. UPSERT
//     submission_status='committed' for the SuperBatch's
//     correlation_ids (idempotent — typically already set by the
//     committer's pre-death tx commit, but defensive).
//
//   Phase 3 (staging-driven orphan-no-queue): for each StagingEntry
//     at valid==2 where no active CommitterQueueEntry references it
//     (sb_id==0, OR sb_id != 0 but no queue at that sb_id). Either
//     the router died before pushing the queue (Phase 5) or a fast
//     committer drained the queue entirely while this staging slot
//     was somehow stranded. CAS valid 2→1 + reset sb_id to 0;
//     re-routing happens on the next router tick.
//
// Memory ordering: the Acquire load on staging.superbatch_id pairs
// with the Release store in router_tick's Phase 6 (data-before-flag
// invariant per §1.6, §3.3). The Acquire load on staging.valid pairs
// with the Release store in router_tick's Phase 3 CAS 1→2 (Acquire
// on success).
//
// Idempotent under repeated firings — CAS election semantics mean a
// second call finds the slots already at their post-recovery state
// and skips.
//
// Per §3.10 lifecycle-coupling: every arena block allocated by the
// router on behalf of a SuperBatch is freed by this sweep when it
// takes ownership. Verified end-to-end by the acceptance suite's
// arena-leak audit (outstanding_allocs returns to baseline).

/// Run one full router-orphan recovery sweep. Returns the number of
/// shmem slots reconciled (staging entries re-stamped, staging
/// entries reverted to pending, queue entries swept). Idempotent.
pub(crate) fn try_recover_router_orphan() -> u64 {
    let mut recovered: u64 = 0;
    recovered += sweep_queue_phase();
    recovered += sweep_staging_phase();
    recovered
}

fn sweep_queue_phase() -> u64 {
    let mut recovered: u64 = 0;
    let queue_capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let my_pid = unsafe { pg_sys::MyProcPid };

    for q_idx in 0..queue_capacity {
        // Snapshot queue fields under share lock.
        let (q_valid, q_sb_id, q_envelope_count, q_so_off, q_sku_off, q_wip_off, q_committer_pid) = {
            let queue = COMMITTER_QUEUE.share();
            let slot = &queue.entries[q_idx as usize];
            let v = slot.valid.load(Acquire);
            (
                v,
                slot.superbatch_id,
                slot.envelope_count,
                slot.staging_entry_offsets,
                slot.sku_pool_keys_offset,
                slot.wip_pool_keys_offset,
                slot.committer_pid.load(Relaxed),
            )
        };

        match q_valid {
            1 => {
                // Phase 1: re-stamp any linked staging entries the old
                // router never reached in Phase 6.
                if q_envelope_count == 0 || q_so_off == 0 {
                    continue;
                }
                let staging_indices = read_staging_indices(q_so_off, q_envelope_count);
                let queue = STAGING_QUEUE.share();
                for s_idx in staging_indices {
                    let slot = &queue.entries[s_idx as usize];
                    if slot.valid.load(Acquire) != 2 {
                        continue;
                    }
                    let s_sb = slot.superbatch_id.load(Acquire);
                    if s_sb != 0 && s_sb != q_sb_id {
                        continue; // belongs to a different (later) SuperBatch
                    }
                    // Complete the router's interrupted Phase 6 stamp.
                    slot.superbatch_id.store(q_sb_id, Release);
                    if slot.valid.compare_exchange(2, 3, Release, Relaxed).is_ok() {
                        recovered += 1;
                    }
                }
            }
            2 => {
                // In flight; M5a.1 committer-side recovery handles dead
                // committers in this state. Leave alone.
            }
            3 => {
                // Phase 2: completed. Check committer liveness.
                if q_committer_pid == 0 || q_committer_pid == my_pid {
                    continue;
                }
                let kill_rc = unsafe { libc::kill(q_committer_pid, 0) };
                if kill_rc == 0 {
                    continue; // still alive
                }
                let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                if errno != libc::ESRCH {
                    continue; // EPERM etc. — be conservative
                }
                // Sweep takes ownership.
                let staging_indices = if q_so_off != 0 && q_envelope_count > 0 {
                    read_staging_indices(q_so_off, q_envelope_count)
                } else {
                    Vec::new()
                };
                let mut correlation_ids: Vec<pgrx::Uuid> =
                    Vec::with_capacity(staging_indices.len());
                for s_idx in staging_indices {
                    let (payload_off, sku_off, wip_off, corr, did_cas) = {
                        let queue = STAGING_QUEUE.share();
                        let slot = &queue.entries[s_idx as usize];
                        let p = slot.payload_offset;
                        let s = slot.sku_pool_keys_offset;
                        let w = slot.wip_pool_keys_offset;
                        let c = pgrx::Uuid::from_bytes(slot.correlation_id);
                        let did = slot.valid.compare_exchange(3, 0, Release, Relaxed).is_ok()
                            || slot.valid.compare_exchange(2, 0, Release, Relaxed).is_ok();
                        if did {
                            // M5c.1: this is a staging slot becoming
                            // available — wake any backpressure waiter.
                            signal_staging_slot_freed(&queue);
                        }
                        (p, s, w, c, did)
                    };
                    correlation_ids.push(corr);
                    if did_cas {
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
                }
                // Free queue-entry-owned arena blocks (the leak source
                // spec §3.3 explicitly calls out as easy-to-miss).
                {
                    let mut arena = SPILLOVER_ARENA.exclusive();
                    if q_so_off != 0 {
                        arena.free(q_so_off);
                    }
                    if q_sku_off != 0 {
                        arena.free(q_sku_off);
                    }
                    if q_wip_off != 0 {
                        arena.free(q_wip_off);
                    }
                }
                // CAS queue 3→0; clear pid/lease.
                {
                    let queue = COMMITTER_QUEUE.share();
                    let slot = &queue.entries[q_idx as usize];
                    let _ = slot.valid.compare_exchange(3, 0, Release, Relaxed);
                    slot.committer_pid.store(0, Relaxed);
                    slot.committer_acquired_at_ns.store(0, Relaxed);
                    slot.committer_tx_id.store(0, Relaxed);
                }
                // UPSERT submission_status defensively. Idempotent —
                // typically already 'committed' from the committer's
                // pre-death tx, but covers the edge case where the
                // committer aborted but a separate path needs reconciling.
                if !correlation_ids.is_empty() {
                    let _ = Spi::run_with_args(
                        "UPDATE poc_v21_submission_status \
                           SET state='committed', processed_at=now() \
                         WHERE correlation_id = ANY($1::uuid[]) \
                           AND state NOT IN ('committed', 'failed')",
                        &[correlation_ids.into()],
                    );
                }
                recovered += 1;
            }
            _ => {}
        }
    }

    recovered
}

fn sweep_staging_phase() -> u64 {
    let mut recovered: u64 = 0;
    let staging_capacity = POC_V21_STAGING_QUEUE_SIZE as u32;
    let queue_capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;

    // Build a set of sb_ids that currently have a live queue entry.
    // Staging at valid==2 whose sb_id is in this set was handled (or
    // intentionally left alone) by sweep_queue_phase. Anything else
    // is genuinely orphaned and gets reverted to pending.
    let active_sb_ids: HashSet<u64> = {
        let queue = COMMITTER_QUEUE.share();
        let mut s = HashSet::new();
        for q_idx in 0..queue_capacity {
            let slot = &queue.entries[q_idx as usize];
            if slot.valid.load(Acquire) != 0 {
                s.insert(slot.superbatch_id);
            }
        }
        s
    };

    let staging = STAGING_QUEUE.share();
    for s_idx in 0..staging_capacity {
        let slot = &staging.entries[s_idx as usize];
        if slot.valid.load(Acquire) != 2 {
            continue;
        }
        let s_sb = slot.superbatch_id.load(Acquire);
        if s_sb != 0 && active_sb_ids.contains(&s_sb) {
            continue;
        }
        // Revert to pending. CAS 2→1 first, then reset sb_id under the
        // pending-state ownership (no concurrent router pack on boot).
        if slot.valid.compare_exchange(2, 1, Release, Relaxed).is_ok() {
            slot.superbatch_id.store(0, Release);
            recovered += 1;
        }
    }

    recovered
}

/// Read an envelope_count-length array of u32 staging indices from the
/// spillover arena at `offset`.
fn read_staging_indices(offset: u32, envelope_count: u16) -> Vec<u32> {
    let arena = SPILLOVER_ARENA.share();
    let bytes = arena.read_bytes(offset, envelope_count as u32 * 4);
    bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

// ── M5b.1 test SQL surface ──────────────────────────────────────────

/// Test-only: synthetically inject a staging-side orphan into an EMPTY
/// staging slot by stamping (valid=2, superbatch_id=sb_id, correlation_id).
/// Bypasses the normal enqueue path so tests can exercise the recovery
/// sweep without racing the live router/committer flow. Returns false
/// if the target slot is not currently valid==0.
///
/// Use `sb_id=0` for the Phase A scenario (router died pre-Phase-6).
/// Use a non-zero `sb_id` that matches NO live queue entry for the
/// Phase B scenario (orphaned link).
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_inject_staging_orphan(
    staging_idx: i64,
    sb_id: i64,
    correlation_id: pgrx::Uuid,
) -> bool {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return false;
    }
    // Try to claim the empty slot.
    {
        let queue = STAGING_QUEUE.share();
        let slot = &queue.entries[staging_idx as usize];
        if slot.valid.compare_exchange(0, 2, Acquire, Relaxed).is_err() {
            return false;
        }
    }
    // Set non-atomic fields under exclusive lock. payload_offset etc.
    // are left at 0 — Phase A/B scenarios don't allocate arena.
    {
        let mut q = STAGING_QUEUE.exclusive();
        let slot = &mut q.entries[staging_idx as usize];
        slot.correlation_id = *correlation_id.as_bytes();
        slot.payload_offset = 0;
        slot.payload_length = 0;
        slot.sku_pool_keys_offset = 0;
        slot.sku_pool_count = 0;
        slot.wip_pool_keys_offset = 0;
        slot.wip_pool_count = 0;
        slot.event_type_id = 0;
        slot.backend_pid = 0;
        slot.user_tx_xid = 0;
        slot.request_seq = 0;
        slot.enqueued_at_micros = 0;
        slot.eject_count.store(0, Relaxed);
    }
    // Stamp atomic sb_id with Release. Pairs with Acquire in the sweep.
    {
        let queue = STAGING_QUEUE.share();
        let slot = &queue.entries[staging_idx as usize];
        slot.superbatch_id.store(sb_id as u64, Release);
    }
    true
}

/// Test-only: synthetically inject a completed-state CommitterQueueEntry
/// with a definitely-dead committer_pid and arena-backed staging-entry
/// offsets. Each `staging_indices` entry should already be injected via
/// `poc_v21_test_inject_staging_orphan` with sb_id matching this call's
/// sb_id BEFORE this call — so the sweep can walk the chain.
///
/// Allocates real arena blocks (staging_entry_offsets) so the §3.10
/// lifecycle-coupling audit can verify the sweep frees them.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_inject_orphaned_queue(
    queue_idx: i64,
    sb_id: i64,
    fake_pid: i32,
    staging_indices: Vec<i64>,
) -> bool {
    let queue_capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if queue_idx < 0 || queue_idx >= queue_capacity {
        return false;
    }
    if staging_indices.is_empty() || staging_indices.len() > 65535 {
        return false;
    }
    let envelope_count = staging_indices.len() as u16;
    let staging_capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    for &s in &staging_indices {
        if s < 0 || s >= staging_capacity {
            return false;
        }
    }

    // Allocate arena for staging_entry_offsets array.
    let so_off = {
        let mut arena = SPILLOVER_ARENA.exclusive();
        let off = match arena.alloc(envelope_count as u32 * 4) {
            Some(o) => o,
            None => return false,
        };
        let mut buf: Vec<u8> = Vec::with_capacity(envelope_count as usize * 4);
        for &s in &staging_indices {
            buf.extend_from_slice(&(s as u32).to_le_bytes());
        }
        arena.write_bytes(off, &buf);
        off
    };

    // Stamp queue entry under exclusive lock; transition empty→completed.
    {
        let mut q = COMMITTER_QUEUE.exclusive();
        let slot = &mut q.entries[queue_idx as usize];
        if slot.valid.load(Relaxed) != 0 {
            let mut arena = SPILLOVER_ARENA.exclusive();
            arena.free(so_off);
            return false;
        }
        slot.superbatch_id = sb_id as u64;
        slot.envelope_count = envelope_count;
        slot.staging_entry_offsets = so_off;
        slot.sku_pool_keys_offset = 0;
        slot.sku_pool_keys_count = 0;
        slot.wip_pool_keys_offset = 0;
        slot.wip_pool_keys_count = 0;
        slot.committer_pid.store(fake_pid, Relaxed);
        let now_ns = crate::now_ns();
        slot.committer_acquired_at_ns.store(now_ns, Relaxed);
        slot.committer_tx_id.store(0, Relaxed);
        slot.enqueued_at_micros = now_ns / 1000;
        slot.valid.store(3, Release);
    }
    true
}

/// Test-only: read a staging entry's state as
/// "(valid, superbatch_id)". Useful for asserting state transitions.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_staging_state(staging_idx: i64) -> String {
    let queue = STAGING_QUEUE.share();
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return "out_of_range".to_string();
    }
    let slot = &queue.entries[staging_idx as usize];
    format!(
        "({}, {})",
        slot.valid.load(Relaxed),
        slot.superbatch_id.load(Relaxed)
    )
}

/// Test-only: read a staging entry's `eject_count` (M5c.2 acct-1hyx).
/// Used by caller-tx-coupling tests to assert the eject counter grows
/// across re-route cycles when a caller holds its user-tx open.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_staging_eject_count(staging_idx: i64) -> i32 {
    let queue = STAGING_QUEUE.share();
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return -1;
    }
    queue.entries[staging_idx as usize]
        .eject_count
        .load(Relaxed) as i32
}

/// Test-only: force-reset a staging slot to valid==0 (empty).
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_force_reset_staging(staging_idx: i64) -> i32 {
    let queue = STAGING_QUEUE.share();
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return -1;
    }
    let slot = &queue.entries[staging_idx as usize];
    let prev = slot.valid.swap(0, Release);
    slot.superbatch_id.store(0, Release);
    prev as i32
}

/// Test-only: force-reset a queue slot whose state was synthetically
/// injected via `poc_v21_test_inject_orphaned_queue`. Frees the
/// arena-backed staging_entry_offsets, sku_pool_keys, wip_pool_keys
/// blocks the injection allocated, then CASs the slot to valid==0
/// and clears committer fields. Mirrors what the recovery sweep does
/// to the queue's own arena; idempotent if called after the sweep.
/// Returns previous valid value.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_force_reset_injected_queue(queue_idx: i64) -> i32 {
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if queue_idx < 0 || queue_idx >= capacity {
        return -1;
    }
    let (prev, so_off, sku_off, wip_off) = {
        let mut q = COMMITTER_QUEUE.exclusive();
        let slot = &mut q.entries[queue_idx as usize];
        let prev = slot.valid.swap(0, Release);
        let so = slot.staging_entry_offsets;
        let sku = slot.sku_pool_keys_offset;
        let wip = slot.wip_pool_keys_offset;
        slot.staging_entry_offsets = 0;
        slot.sku_pool_keys_offset = 0;
        slot.wip_pool_keys_offset = 0;
        slot.committer_pid.store(0, Relaxed);
        slot.committer_acquired_at_ns.store(0, Relaxed);
        slot.committer_tx_id.store(0, Relaxed);
        (prev, so, sku, wip)
    };
    if prev != 0 {
        let mut arena = SPILLOVER_ARENA.exclusive();
        if so_off != 0 {
            arena.free(so_off);
        }
        if sku_off != 0 {
            arena.free(sku_off);
        }
        if wip_off != 0 {
            arena.free(wip_off);
        }
    }
    prev as i32
}

/// Test-only: read a queue-entry's state as "(valid, superbatch_id,
/// committer_pid)". Distinct from `poc_v21_test_slot_state` which
/// returns only (valid, committer_pid) per M5a.1 convention.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_queue_state(queue_idx: i64) -> String {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if queue_idx < 0 || queue_idx >= capacity {
        return "out_of_range".to_string();
    }
    let slot = &queue.entries[queue_idx as usize];
    format!(
        "({}, {}, {})",
        slot.valid.load(Relaxed),
        slot.superbatch_id,
        slot.committer_pid.load(Relaxed),
    )
}

/// Test-only: run one router-orphan recovery sweep. Returns the count
/// of slots reconciled.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_router_recover_tick() -> i64 {
    try_recover_router_orphan() as i64
}

// ── M5b.2 (acct-ud4h): release/acquire ordering invariant test ──────
//
// The invariant being tested: if a reader Acquire-loads
// staging.valid and sees 3 (routed), then a subsequent Acquire-load
// of staging.superbatch_id MUST return the non-zero value the router
// stored. This is the Release/Acquire pairing the M5b.1 recovery
// sweep depends on — without it, the sweep could observe (valid=3,
// sb_id=0) and mis-classify the entry as "router died pre-stamp",
// CAS-reverting it to pending while a real SuperBatch is mid-flight.
//
// On x86 the natural store-store ordering already enforces the
// invariant for normal-order stores, so a positive-only test would
// pass even with Relaxed. The negative-control (reorder GUC) is what
// makes the test load-bearing: with the stores reversed (CAS first,
// then sb_id store), readers WILL observe (valid=3, sb_id=0) during
// the delay window. The acceptance suite includes a negative-control
// case that asserts the test setup actually exercises the invariant.

pub(crate) fn test_inject_router_delay_us() -> u32 {
    COMMITTER_QUEUE.share().test_inject_router_delay_us.load(Relaxed)
}

pub(crate) fn test_reorder_router_stores() -> bool {
    COMMITTER_QUEUE.share().test_reorder_router_stores.load(Relaxed) != 0
}

/// Test-only: set the router Phase-6 inject delay in microseconds.
/// Visible to the router BGWorker because the field is shmem-resident.
/// Pass 0 to disable.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_set_router_delay_us(delay_us: i32) {
    let d = if delay_us < 0 { 0 } else { delay_us as u32 };
    COMMITTER_QUEUE
        .share()
        .test_inject_router_delay_us
        .store(d, Relaxed);
}

/// Test-only: enable the router Phase-6 store reorder for negative
/// control. When ON, the CAS valid 2→3 happens BEFORE the sb_id store
/// — breaking the data-before-flag invariant intentionally so the
/// test can confirm violations are actually observable.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_set_router_reorder(enabled: bool) {
    COMMITTER_QUEUE
        .share()
        .test_reorder_router_stores
        .store(if enabled { 1 } else { 0 }, Relaxed);
}

/// Test-only: scan all staging entries with Acquire ordering and
/// return the count of (valid==3, superbatch_id==0) observations.
/// Each observation that survives a single backend's two Acquire
/// loads is a violation of the data-before-flag invariant.
///
/// Race window: a writer can transition (valid=2, sb_id=set) →
/// (valid=3, sb_id=set) between this fn's Acquire-load of valid and
/// the subsequent Acquire-load of sb_id. That sequence is the EXPECTED
/// post-pack state — sb_id is non-zero so it doesn't count as a
/// violation. The violation pattern is observing valid=3 with sb_id
/// STILL 0, which can only happen if the router's two stores are
/// reordered (CAS first) OR if the compiler/CPU is allowed to reorder
/// despite the Release annotation.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_check_release_acquire_invariant() -> i64 {
    let mut violations: i64 = 0;
    let queue = STAGING_QUEUE.share();
    for i in 0..POC_V21_STAGING_QUEUE_SIZE {
        let slot = &queue.entries[i];
        if slot.valid.load(Acquire) == 3 && slot.superbatch_id.load(Acquire) == 0 {
            violations += 1;
        }
    }
    violations
}

// ── M5d.2 (acct-3rc1): periodic slot-leak audit ─────────────────────
//
// Defense-in-depth backstop catching slots that get stuck in
// non-terminal states due to bugs the eventful recovery paths
// (M5a.1 committer orphan recovery, M5b.1 router boot sweep,
// M5c.2 eject cleanup, M5d.1 startup recovery) missed.
//
// Piggybacked on the router BGWorker: every ~60s the router's main
// loop calls run_audit_sweep() inside a BackgroundWorker::transaction.
// Bounded pass (iterates fixed staging + committer queue sizes).
//
// Four detection patterns per spec §3.12:
//
//   A: Staging valid=2 stuck > 60s with dead backend_pid (caller).
//      Router should never leave a slot at valid=2 for that long
//      in steady state — router_tick takes <1ms. CAS valid 2→1
//      reverts to pending; re-routing on next tick.
//
//   B: Staging valid=3 stuck > 5min with no live CommitterQueueEntry
//      referencing it. Defensive — should never happen in clean ops.
//      Mark submission_status='failed' / error_code='superbatch_lost',
//      free arena, CAS valid 3→0.
//
//   C: Staging valid=3 stuck > 5min linked to a CommitterQueueEntry
//      at valid=3 (completed) with dead committer_pid. Committer
//      finished Step 5 commit (cost rows + status rows durable) but
//      died before Step 14 cleanup. Reap directly: per-staging arena
//      free + CAS 3→0, queue-entry-owned arena free, CAS queue 3→0.
//      Convergence point with M5b.1's boot sweep (Phase 2) — that runs
//      at router startup; this is the runtime backstop.
//
//   D: CommitterQueueEntry valid=2 stuck with dead committer_pid.
//      M5a.1's per-tick try_recover_orphan handles this at the
//      committer_lease_ms threshold (~100ms). The audit's 60s call
//      is a backstop in case all committers are dead.
//
// Counters exposed via SQL accessors:
//   poc_v21_audit_reclaims_count          (patterns A + C)
//   poc_v21_audit_orphans_recovered_count (pattern D)
//   poc_v21_audit_lost_envelopes_count    (pattern B)
//   poc_v21_audit_last_run_at             (timestamptz of last pass)

/// Audit interval — 60 seconds (per spec §3.12 default cadence).
const AUDIT_INTERVAL_NS: u64 = 60 * 1_000_000_000;

/// Stale threshold for Pattern A (staging valid=2 stuck). Microseconds
/// (matches StagingEntry.enqueued_at_micros).
const AUDIT_STALE_PATTERN_A_US: u64 = 60 * 1_000_000;

/// Stale threshold for Patterns B + C (5min). Microseconds.
const AUDIT_STALE_5MIN_US: u64 = 5 * 60 * 1_000_000;

/// True when the periodic audit timestamp is more than AUDIT_INTERVAL_NS
/// stale. Caller (router_main) gates the audit pass on this; the
/// `poc_v21_test_run_audit_sweep` SQL helper bypasses for tests.
fn audit_due() -> bool {
    let now_ns = crate::now_ns();
    let last = COMMITTER_QUEUE.share().audit_last_run_at_ns.load(Acquire);
    now_ns.saturating_sub(last) >= AUDIT_INTERVAL_NS
}

/// Liveness check for a stored caller / committer pid via `kill(pid, 0)`.
/// Returns true when the pid is definitely dead (ESRCH). Returns false
/// for live (kill returns 0) AND for ambiguous errors (EPERM etc.) —
/// conservative; ambiguous cases leave the slot alone.
///
/// A pid of 0 is treated as "no owner registered" — definitely dead.
/// Only test helpers should ever leave a slot with backend_pid==0
/// while the slot is at valid != empty; production paths always stamp
/// the caller's pid.
fn pid_is_dead(pid: i32) -> bool {
    if pid == 0 {
        return true;
    }
    let kill_rc = unsafe { libc::kill(pid, 0) };
    if kill_rc == 0 {
        return false;
    }
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno == libc::ESRCH
}

/// Top-level audit pass. Idempotent under re-entry; safe to call from
/// tests synchronously OR from the router BGWorker's main loop. Stamps
/// `audit_last_run_at_ns` on completion regardless of whether any
/// pattern fired.
pub(crate) fn run_audit_sweep() {
    let reclaims_a = audit_pattern_a();
    let lost = audit_pattern_b();
    let reclaims_c = audit_pattern_c();
    let orphans = crate::committer::try_recover_orphan() as u64; // Pattern D

    let queue = COMMITTER_QUEUE.share();
    queue
        .audit_reclaims_count
        .fetch_add(reclaims_a + reclaims_c, Relaxed);
    queue
        .audit_orphans_recovered_count
        .fetch_add(orphans, Relaxed);
    queue.audit_lost_envelopes_count.fetch_add(lost, Relaxed);
    let now_ns = crate::now_ns();
    queue.audit_last_run_at_ns.store(now_ns, Release);
}

/// Pattern A: staging valid=2 > 60s with dead backend_pid → CAS 2→1.
/// Arena is NOT freed — the staging entry's payload + pool-keys remain
/// valid; next router pack re-uses them. Resets sb_id to 0 under the
/// pending-state ownership.
fn audit_pattern_a() -> u64 {
    let now_us = crate::now_us();
    let staging_capacity = POC_V21_STAGING_QUEUE_SIZE as u32;
    let mut reclaimed: u64 = 0;

    for s_idx in 0..staging_capacity {
        let (valid, enqueued_us, backend_pid) = {
            let queue = STAGING_QUEUE.share();
            let slot = &queue.entries[s_idx as usize];
            (
                slot.valid.load(Acquire),
                slot.enqueued_at_micros,
                slot.backend_pid,
            )
        };
        if valid != 2 {
            continue;
        }
        if now_us.saturating_sub(enqueued_us) <= AUDIT_STALE_PATTERN_A_US {
            continue;
        }
        if !pid_is_dead(backend_pid) {
            continue;
        }
        // CAS valid 2→1 (processing → pending). On success, reset sb_id
        // (no concurrent router pack on a valid=1 slot once we own it).
        let cas_ok = {
            let queue = STAGING_QUEUE.share();
            queue.entries[s_idx as usize]
                .valid
                .compare_exchange(2, 1, Release, Relaxed)
                .is_ok()
        };
        if cas_ok {
            let queue = STAGING_QUEUE.share();
            queue.entries[s_idx as usize]
                .superbatch_id
                .store(0, Release);
            reclaimed += 1;
        }
    }
    reclaimed
}

/// Pattern B: staging valid=3 > 5min with no live CommitterQueueEntry
/// referencing it → mark submission_status as 'superbatch_lost', free
/// arena, CAS valid 3→0. Defensive — should never fire in clean ops.
fn audit_pattern_b() -> u64 {
    let now_us = crate::now_us();
    let queue_capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let staging_capacity = POC_V21_STAGING_QUEUE_SIZE as u32;

    // Snapshot the set of sb_ids belonging to live queue entries.
    let active_sb_ids: HashSet<u64> = {
        let queue = COMMITTER_QUEUE.share();
        let mut s = HashSet::new();
        for q_idx in 0..queue_capacity {
            let slot = &queue.entries[q_idx as usize];
            if slot.valid.load(Acquire) != 0 {
                s.insert(slot.superbatch_id);
            }
        }
        s
    };

    let mut lost: u64 = 0;
    for s_idx in 0..staging_capacity {
        let (valid, sb_id, enqueued_us, payload_off, sku_off, wip_off, corr) = {
            let queue = STAGING_QUEUE.share();
            let slot = &queue.entries[s_idx as usize];
            (
                slot.valid.load(Acquire),
                slot.superbatch_id.load(Acquire),
                slot.enqueued_at_micros,
                slot.payload_offset,
                slot.sku_pool_keys_offset,
                slot.wip_pool_keys_offset,
                pgrx::Uuid::from_bytes(slot.correlation_id),
            )
        };
        if valid != 3 {
            continue;
        }
        if now_us.saturating_sub(enqueued_us) <= AUDIT_STALE_5MIN_US {
            continue;
        }
        // If sb_id == 0 OR sb_id is not in the active set, this entry's
        // queue link is gone. The sb_id == 0 path catches the rare
        // memory-ordering violation case (caught observably by M5b.2).
        if sb_id != 0 && active_sb_ids.contains(&sb_id) {
            continue;
        }
        let cas_ok = {
            let queue = STAGING_QUEUE.share();
            queue.entries[s_idx as usize]
                .valid
                .compare_exchange(3, 0, Release, Relaxed)
                .is_ok()
        };
        if !cas_ok {
            continue;
        }
        // Free per-entry arena blocks.
        {
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
        // Reset sb_id; wake any backpressure waiter (slot is now empty).
        {
            let queue = STAGING_QUEUE.share();
            queue.entries[s_idx as usize]
                .superbatch_id
                .store(0, Release);
            signal_staging_slot_freed(&queue);
        }
        // Mark status 'failed' with error_code 'superbatch_lost'.
        // ON CONFLICT covers the case where the caller already
        // observed terminal state by some other path.
        let _ = Spi::run_with_args(
            "INSERT INTO poc_v21_submission_status \
               (correlation_id, state, error_code, processed_at, enqueued_at) \
             VALUES ($1, 'failed', 'superbatch_lost', now(), now()) \
             ON CONFLICT (correlation_id) DO UPDATE \
               SET state='failed', \
                   error_code='superbatch_lost', \
                   processed_at=now() \
             WHERE poc_v21_submission_status.state \
                   NOT IN ('committed', 'failed', 'replayed')",
            &[corr.into()],
        );
        lost += 1;
    }
    lost
}

/// Pattern C: CommitterQueueEntry valid=3 (completed) > 5min with dead
/// committer_pid → reap. The committer ran Step 5 (cost rows + status
/// rows durable in PG) but died before Step 14 (arena free + CAS 3→0).
/// Age-gated mirror of M5b.1's `sweep_queue_phase` Phase 2. Idempotent
/// with that boot sweep — both use CAS 3→0 election.
fn audit_pattern_c() -> u64 {
    let now_us = crate::now_us();
    let queue_capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let my_pid = unsafe { pg_sys::MyProcPid };
    let mut reaped: u64 = 0;

    for q_idx in 0..queue_capacity {
        let (
            q_valid,
            _q_sb_id,
            q_envelope_count,
            q_so_off,
            q_sku_off,
            q_wip_off,
            q_committer_pid,
            q_enqueued_us,
        ) = {
            let queue = COMMITTER_QUEUE.share();
            let slot = &queue.entries[q_idx as usize];
            (
                slot.valid.load(Acquire),
                slot.superbatch_id,
                slot.envelope_count,
                slot.staging_entry_offsets,
                slot.sku_pool_keys_offset,
                slot.wip_pool_keys_offset,
                slot.committer_pid.load(Relaxed),
                slot.enqueued_at_micros,
            )
        };
        if q_valid != 3 {
            continue;
        }
        if now_us.saturating_sub(q_enqueued_us) <= AUDIT_STALE_5MIN_US {
            continue;
        }
        if q_committer_pid == 0 || q_committer_pid == my_pid {
            continue;
        }
        if !pid_is_dead(q_committer_pid) {
            continue;
        }
        // Reap. Walk linked staging entries; CAS each 3→0; free per-entry
        // arena. Then free queue-entry-owned arena (staging_entry_offsets,
        // sku_pool_keys, wip_pool_keys — the easy-to-miss leak source).
        // Finally CAS queue 3→0.
        let staging_indices = if q_so_off != 0 && q_envelope_count > 0 {
            read_staging_indices(q_so_off, q_envelope_count)
        } else {
            Vec::new()
        };
        let mut correlation_ids: Vec<pgrx::Uuid> = Vec::with_capacity(staging_indices.len());
        for s_idx in staging_indices {
            let (payload_off, sku_off, wip_off, corr, did_cas) = {
                let queue = STAGING_QUEUE.share();
                let slot = &queue.entries[s_idx as usize];
                let p = slot.payload_offset;
                let s = slot.sku_pool_keys_offset;
                let w = slot.wip_pool_keys_offset;
                let c = pgrx::Uuid::from_bytes(slot.correlation_id);
                let did = slot.valid.compare_exchange(3, 0, Release, Relaxed).is_ok();
                if did {
                    slot.superbatch_id.store(0, Release);
                    signal_staging_slot_freed(&queue);
                }
                (p, s, w, c, did)
            };
            correlation_ids.push(corr);
            if did_cas {
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
        }
        // Free queue-entry-owned arena (separate from per-staging blocks).
        {
            let mut arena = SPILLOVER_ARENA.exclusive();
            if q_so_off != 0 {
                arena.free(q_so_off);
            }
            if q_sku_off != 0 {
                arena.free(q_sku_off);
            }
            if q_wip_off != 0 {
                arena.free(q_wip_off);
            }
        }
        // CAS queue 3→0; clear pid/lease.
        {
            let queue = COMMITTER_QUEUE.share();
            let slot = &queue.entries[q_idx as usize];
            let _ = slot.valid.compare_exchange(3, 0, Release, Relaxed);
            slot.committer_pid.store(0, Relaxed);
            slot.committer_acquired_at_ns.store(0, Relaxed);
            slot.committer_tx_id.store(0, Relaxed);
        }
        // Defensive UPSERT — cost rows are already durable; status row
        // SHOULD already be 'committed' from the pre-death tx commit.
        // Only flip non-terminal states.
        if !correlation_ids.is_empty() {
            let _ = Spi::run_with_args(
                "UPDATE poc_v21_submission_status \
                   SET state='committed', processed_at=now() \
                 WHERE correlation_id = ANY($1::uuid[]) \
                   AND state NOT IN ('committed', 'failed', 'replayed')",
                &[correlation_ids.into()],
            );
        }
        reaped += 1;
    }
    reaped
}

// ── M5d.2 SQL surface ───────────────────────────────────────────────

#[pg_extern]
fn poc_v21_audit_reclaims_count() -> i64 {
    COMMITTER_QUEUE.share().audit_reclaims_count.load(Relaxed) as i64
}

#[pg_extern]
fn poc_v21_audit_orphans_recovered_count() -> i64 {
    COMMITTER_QUEUE
        .share()
        .audit_orphans_recovered_count
        .load(Relaxed) as i64
}

#[pg_extern]
fn poc_v21_audit_lost_envelopes_count() -> i64 {
    COMMITTER_QUEUE
        .share()
        .audit_lost_envelopes_count
        .load(Relaxed) as i64
}

/// Timestamp of the most recent audit pass, or NULL if no pass has run
/// since cluster startup. Returns a PG timestamptz value derived from
/// the audit_last_run_at_ns shmem field (PG epoch microseconds → standard
/// PG timestamp).
#[pg_extern]
fn poc_v21_audit_last_run_at() -> Option<pgrx::datum::TimestampWithTimeZone> {
    let ns = COMMITTER_QUEUE.share().audit_last_run_at_ns.load(Relaxed);
    if ns == 0 {
        return None;
    }
    let us = (ns / 1000) as pg_sys::TimestampTz;
    // pgrx 0.18: TimestampWithTimeZone::try_from(pg_sys::TimestampTz)
    // interprets the i64 as microseconds since the PG epoch
    // (2000-01-01 UTC). Matches pg_sys::GetCurrentTimestamp semantics.
    pgrx::datum::TimestampWithTimeZone::try_from(us).ok()
}

/// Test-only: run one audit pass synchronously (bypasses the 60s gate).
/// Returns the count of slots reconciled across all patterns.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_run_audit_sweep() -> i64 {
    let before_a = COMMITTER_QUEUE.share().audit_reclaims_count.load(Relaxed);
    let before_b = COMMITTER_QUEUE
        .share()
        .audit_lost_envelopes_count
        .load(Relaxed);
    let before_d = COMMITTER_QUEUE
        .share()
        .audit_orphans_recovered_count
        .load(Relaxed);
    run_audit_sweep();
    let after_a = COMMITTER_QUEUE.share().audit_reclaims_count.load(Relaxed);
    let after_b = COMMITTER_QUEUE
        .share()
        .audit_lost_envelopes_count
        .load(Relaxed);
    let after_d = COMMITTER_QUEUE
        .share()
        .audit_orphans_recovered_count
        .load(Relaxed);
    (after_a - before_a + after_b - before_b + after_d - before_d) as i64
}

/// Test-only: reset all audit counters + last_run_at to 0. Used by
/// tests to establish a clean baseline before manufacturing a leak.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_audit_counters_reset() {
    let queue = COMMITTER_QUEUE.share();
    queue.audit_reclaims_count.store(0, Relaxed);
    queue.audit_orphans_recovered_count.store(0, Relaxed);
    queue.audit_lost_envelopes_count.store(0, Relaxed);
    queue.audit_last_run_at_ns.store(0, Relaxed);
}

/// Test-only: backdate a staging entry's `enqueued_at_micros` by
/// `offset_ms` so it appears stale to audit gating. Saturates at 0
/// (no underflow).
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_backdate_staging_enqueued_at(staging_idx: i64, offset_ms: i64) -> bool {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return false;
    }
    let mut q = STAGING_QUEUE.exclusive();
    let slot = &mut q.entries[staging_idx as usize];
    let offset_us = (offset_ms.max(0) as u64) * 1000;
    slot.enqueued_at_micros = slot.enqueued_at_micros.saturating_sub(offset_us);
    true
}

/// Test-only: set a staging entry's `backend_pid` to a known value
/// (used to inject Pattern A's dead-pid condition with a non-zero PID).
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_set_staging_backend_pid(staging_idx: i64, pid: i32) -> bool {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return false;
    }
    let mut q = STAGING_QUEUE.exclusive();
    q.entries[staging_idx as usize].backend_pid = pid;
    true
}

/// Test-only: backdate a queue entry's `enqueued_at_micros` by
/// `offset_ms`. Used for Pattern C age-gating.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_backdate_queue_enqueued_at(queue_idx: i64, offset_ms: i64) -> bool {
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as i64;
    if queue_idx < 0 || queue_idx >= capacity {
        return false;
    }
    let mut q = COMMITTER_QUEUE.exclusive();
    let slot = &mut q.entries[queue_idx as usize];
    let offset_us = (offset_ms.max(0) as u64) * 1000;
    slot.enqueued_at_micros = slot.enqueued_at_micros.saturating_sub(offset_us);
    true
}

/// Test-only: directly stamp a staging slot to the routed state
/// (valid=3, sb_id=given). Mirrors the router's Phase 6 stores
/// (sb_id Release then CAS valid 2→3 Release). Precondition: slot must
/// be at valid=2. Used to construct Patterns B and C scenarios where
/// staging needs to be at valid=3 with a specific sb_id linkage.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_promote_staging_to_routed(staging_idx: i64, sb_id: i64) -> bool {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return false;
    }
    let queue = STAGING_QUEUE.share();
    let slot = &queue.entries[staging_idx as usize];
    // Match router's Phase 6 ordering: store sb_id first (Release),
    // then CAS valid 2→3 (Release).
    slot.superbatch_id.store(sb_id as u64, Release);
    slot.valid
        .compare_exchange(2, 3, Release, Relaxed)
        .is_ok()
}

/// Test-only: comprehensive shmem reset for audit tests that need strong
/// equality assertions on arena outstanding / staging+queue state. Walks
/// every slot in staging + committer queue, forces valid=0; nukes the
/// arena (bump=0, freelist=0, counters=0); resets ring head/tail +
/// next_seq + all router stats counters; resets all audit counters.
///
/// Safety: assumes no BGWorker is actively processing shmem. The audit
/// pass and router_tick check slot state under atomics; forcing valid=0
/// while a tick is mid-pack would cause it to lose track of an entry
/// but not corrupt memory. Tests calling this should not have live
/// enqueues in flight (which is the audit-test invariant).
///
/// Returns the count of (staging, queue) slots that were non-empty
/// before reset — useful for tests that want to verify their setup
/// state was clean.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_force_reset_all_shmem() -> String {
    let mut s_dirty: u32 = 0;
    let mut q_dirty: u32 = 0;

    // Wipe staging entries.
    {
        let queue = STAGING_QUEUE.share();
        for s_idx in 0..POC_V21_STAGING_QUEUE_SIZE {
            let slot = &queue.entries[s_idx];
            let prev = slot.valid.swap(0, Release);
            if prev != 0 {
                s_dirty += 1;
            }
            slot.superbatch_id.store(0, Release);
            slot.eject_count.store(0, Relaxed);
        }
        queue.head.store(0, Relaxed);
        queue.tail.store(0, Relaxed);
        queue.next_request_seq.store(0, Relaxed);
    }

    // Wipe committer queue entries.
    {
        let mut queue = COMMITTER_QUEUE.exclusive();
        for q_idx in 0..POC_V21_COMMITTER_QUEUE_SIZE {
            let slot = &mut queue.entries[q_idx];
            let prev = slot.valid.swap(0, Release);
            if prev != 0 {
                q_dirty += 1;
            }
            slot.superbatch_id = 0;
            slot.envelope_count = 0;
            slot.staging_entry_offsets = 0;
            slot.sku_pool_keys_offset = 0;
            slot.sku_pool_keys_count = 0;
            slot.wip_pool_keys_offset = 0;
            slot.wip_pool_keys_count = 0;
            slot.committer_pid.store(0, Relaxed);
            slot.committer_acquired_at_ns.store(0, Relaxed);
            slot.committer_tx_id.store(0, Relaxed);
            slot.enqueued_at_micros = 0;
        }
        queue.head.store(0, Relaxed);
        queue.tail.store(0, Relaxed);
        queue.next_superbatch_id.store(0, Relaxed);
        // Reset router stats.
        queue.router_superbatch_count.store(0, Relaxed);
        queue.router_total_envelopes.store(0, Relaxed);
        queue.router_force_pack_count.store(0, Relaxed);
        queue.router_ticks_total.store(0, Relaxed);
        queue.router_entries_scanned_total.store(0, Relaxed);
        queue.committer_drains_total.store(0, Relaxed);
        queue.router_max_envelope_count.store(0, Relaxed);
        for b in queue.router_envelope_histogram.iter() {
            b.store(0, Relaxed);
        }
        // Reset audit counters.
        queue.audit_reclaims_count.store(0, Relaxed);
        queue.audit_orphans_recovered_count.store(0, Relaxed);
        queue.audit_lost_envelopes_count.store(0, Relaxed);
        queue.audit_last_run_at_ns.store(0, Relaxed);
    }

    // Wipe spillover arena.
    {
        let mut arena = SPILLOVER_ARENA.exclusive();
        arena.bump_offset.store(0, Release);
        arena.freelist_head_offset.store(0, Release);
        arena.total_allocs.store(0, Relaxed);
        arena.total_frees.store(0, Relaxed);
    }

    // Clear router starvation map (process-local Mutex).
    if let Ok(mut m) = starvation_map().lock() {
        m.clear();
    }

    format!("({}, {})", s_dirty, q_dirty)
}

/// Test-only: directly populate an empty staging slot with an arena-backed
/// payload + pool-keys so audit Pattern B / C tests can verify arena
/// freeing. Stamps valid=2 with the given sb_id; tests can subsequently
/// CAS 2→3 via `poc_v21_test_promote_staging_to_routed`.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_inject_staging_with_arena(
    staging_idx: i64,
    sb_id: i64,
    correlation_id: pgrx::Uuid,
    payload_bytes: i64,
) -> bool {
    let capacity = POC_V21_STAGING_QUEUE_SIZE as i64;
    if staging_idx < 0 || staging_idx >= capacity {
        return false;
    }
    if payload_bytes < 1 || payload_bytes > 65535 {
        return false;
    }
    // Allocate a real arena block for payload.
    let payload_off = {
        let mut arena = SPILLOVER_ARENA.exclusive();
        match arena.alloc(payload_bytes as u32) {
            Some(o) => o,
            None => return false,
        }
    };
    // Try to claim slot.
    {
        let queue = STAGING_QUEUE.share();
        let slot = &queue.entries[staging_idx as usize];
        if slot.valid.compare_exchange(0, 2, Acquire, Relaxed).is_err() {
            let mut arena = SPILLOVER_ARENA.exclusive();
            arena.free(payload_off);
            return false;
        }
    }
    // Stamp non-atomic fields.
    {
        let mut q = STAGING_QUEUE.exclusive();
        let slot = &mut q.entries[staging_idx as usize];
        slot.correlation_id = *correlation_id.as_bytes();
        slot.payload_offset = payload_off;
        slot.payload_length = payload_bytes as u32;
        slot.sku_pool_keys_offset = 0;
        slot.sku_pool_count = 0;
        slot.wip_pool_keys_offset = 0;
        slot.wip_pool_count = 0;
        slot.event_type_id = 0;
        slot.backend_pid = 0;
        slot.user_tx_xid = 0;
        slot.request_seq = 0;
        slot.enqueued_at_micros = crate::now_us();
        slot.eject_count.store(0, Relaxed);
    }
    // Stamp sb_id.
    {
        let queue = STAGING_QUEUE.share();
        let slot = &queue.entries[staging_idx as usize];
        slot.superbatch_id.store(sb_id as u64, Release);
    }
    true
}
