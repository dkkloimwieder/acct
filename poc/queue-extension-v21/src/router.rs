//! Greedy Window Router BGWorker (M3.1, acct-r29s).
//!
//! Scans up to `router_window_size` pending staging entries per tick,
//! packs them into SuperBatches by greedy disjoint pool-key cover, and
//! pushes each SuperBatch onto the committer queue. WIP pool keys are
//! carried through but NOT consulted at routing (spec §1.5 — WIP pools
//! uncontended by construction).
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
    router_window_size_now,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Router BGWorker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_router_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // No SPI access required for the router; pure shmem orchestration.
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
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

    // --- Phase 3: greedy disjoint pack with fairness backstop. ---
    //
    // Per spec §1.8: when a candidate's starvation_count >= threshold
    // AND the current SuperBatch is empty, force-pack as size-1
    // SuperBatch and break (don't add other candidates this tick;
    // ensure the starved entry gets through without further contention).
    // Skipped candidates (intersecting lock_set OR losing the CAS race)
    // get their starvation counter incremented. Packed candidates
    // (normal or forced) have their counter cleared.
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

        // Disjoint check.
        if cand.sku_pool_keys.iter().any(|k| lock_set.contains(k)) {
            *starv.entry(cand.request_seq).or_insert(0) += 1;
            continue;
        }

        // CAS valid 1→2 (pending → processing). Acquire on success so
        // subsequent reads from this staging entry see the caller's
        // payload writes.
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
            let now_micros = unsafe { pg_sys::GetCurrentTimestamp() as u64 };
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
    {
        let queue = STAGING_QUEUE.share();
        for cand in &packed {
            let slot = &queue.entries[cand.staging_idx as usize];
            slot.superbatch_id.store(sb_id, Release);
            let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
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
///                               (R1 target: > 0.7 under disjoint workload)
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
    ];
    for (i, count) in histogram.iter().enumerate() {
        rows.push((format!("histogram_bucket_{}", i), *count as f64));
    }
    TableIterator::new(rows.into_iter())
}

// ── Test SQL surface ────────────────────────────────────────────────

/// Run one router tick. Returns true if at least one SuperBatch was
/// assembled this call. Used by tests to drive the router
/// deterministically.
#[pg_extern]
fn poc_v21_test_router_tick() -> bool {
    router_tick() > 0
}

/// Drain the router until no pending entry remains. Returns the count
/// of SuperBatches assembled. Used by the property test fixture to
/// flush quickly without waiting on the BGWorker's 50ms tick.
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
