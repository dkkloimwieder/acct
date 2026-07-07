//! Router BGWorker for ledger-routed-c (Path C) — design-v3.1 §6.3.
//!
//! ## Per-tick pipeline
//!
//!   1. `collect_candidates` — head-scan up to `router_window_size`
//!      staging entries with `valid == 1` (pending); capture
//!      (staging_idx, request_seq, pool_keys_offset/count,
//!      enqueued_at_micros). Skips slots inside their eject cooldown.
//!   2. `batch_window_us` gate — if the OLDEST candidate has been
//!      pending for less than the window, defer this tick to let more
//!      submissions accumulate. Window=0 disables the gate.
//!   3. `hydrate_candidates` — read each candidate's pool_keys (i64
//!      array) from the spillover arena under one share-lock.
//!   4. `affinity_group` — union-find on pool_id overlap, emitting one
//!      `Vec<Candidate>` per connected component, oldest-first by
//!      `min(request_seq)`, members within each group sorted by
//!      request_seq.
//!   5. `emit_commit_group` per group, chunking oversized components
//!      into batches of `batch_size_max`:
//!        a. CAS staging valid 1→2 (Acquire on success)
//!        b. allocate two arena blocks (`staging_indices` u32 array +
//!           deduplicated/sorted pool-keys u64 array)
//!        c. claim a `CommitterQueueEntry`, stamp fields, CAS valid 0→1
//!           with Release (publishes to committers)
//!        d. data-before-flag: store `commit_group_id` on each staging
//!           entry with Release BEFORE CAS staging valid 2→3 with
//!           Release.
//!
//! ## Path C delta vs a strict path (§6.2, §9.4, §14.2)
//!
//! There is no per-pool sequence table / per-pool sequence assignment
//! (strict cross-window FIFO ordering): Path C records provisional
//! aggregate updates, so any component may be split across
//! commit_groups and provisional unit_costs are allowed to differ
//! across orderings. The chunker therefore treats every component
//! uniformly — there is no order-sensitive no-split case, and the
//! committer needs no predecessor-wait. Cross-group hot-pool contention
//! serializes harmlessly at `pool_lock`.
//!
//! ## Data-before-flag (§6.3)
//!
//! `commit_group_id.store(cg_id, Release)` MUST precede
//! `valid.compare_exchange(2, 3, Release, Relaxed)` on each staging
//! entry. The two test-injection atomics on `CommitterQueue`
//! (`test_inject_router_delay_us`, `test_reorder_router_stores`) let the
//! recovery test (P3.4) stress the ordering window; both default to zero
//! and have no production effect.
//!
//! ## Boot-recovery sweep (§6.5)
//!
//! `try_recover_router_orphan` runs once at `router_main` startup (after the
//! recovery worker flips `recovery_complete`) and reconciles shmem state a prior
//! router / committer left mid-operation: re-stamping interrupted
//! data-before-flag stores, taking over commit_groups whose owning committer
//! died, and reverting orphaned staging entries. It is idempotent and is also
//! callable on demand via the `test_hooks` SPI.

use crate::identity::is_committer_alive;
use crate::shmem::{
    COMMITTER_QUEUE, LEDGER_V3_COMMITTER_QUEUE_SIZE, LEDGER_V3_STAGING_QUEUE_SIZE, SPILLOVER_ARENA,
    STAGING_QUEUE, StagingQueue, signal_staging_slot_freed,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::time::Duration;

/// Cadence for the router's periodic dead-committer reclaim (§6.5 Q10). The boot
/// sweep only runs at router (re)start, so a committer that exits clean-FATAL
/// mid-pipeline would otherwise orphan its in-flight commit_group (CQ valid==2,
/// dead identity) until the next router restart. Re-running the idempotent sweep
/// on this coarse interval bounds reclaim latency while the router stays up. The
/// sweep is a cheap shmem scan; a few-second cadence adds no meaningful overhead
/// and is comparable to the 5 s BGWorker restart_time.
const RECLAIM_SWEEP_INTERVAL_NS: u64 = 2_000_000_000;

/// Block until the recovery worker has flipped `recovery_complete` 0→1, or until
/// a SIGTERM ends the latch wait. design-v3.1 §6.5. Lives here because the
/// committer depends on it too (mirrors the v3 layout).
pub(crate) fn wait_for_recovery_complete() {
    loop {
        if COMMITTER_QUEUE.share().recovery_complete.load(Acquire) != 0 {
            return;
        }
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
            return;
        }
    }
}

/// Router BGWorker entry point. Registered in `_PG_init` with a 5 s restart time.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_c_router_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = crate::target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // Publish PID so the P3.4 orphan-recovery test_hooks SPI can target it;
    // overwritten on every router relaunch.
    COMMITTER_QUEUE
        .share()
        .router_pid
        .store(unsafe { pg_sys::MyProcPid } as i32, Release);

    wait_for_recovery_complete();

    // Boot-recovery sweep (§6.5): reconcile any shmem state a prior router /
    // committer left mid-operation before opening for steady-state routing.
    // Phase ordering is load-bearing (see try_recover_router_orphan).
    try_recover_router_orphan();
    let mut last_reclaim_sweep_ns = crate::shmem::now_ns();

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        // Test-only pause hook: a test backend flips `test_bgworker_paused` to
        // 1 to suspend ticking while synchronous test SQL inspects slot state
        // without racing this worker. Compiled out of production (acct-yojk.11);
        // only test_hooks builds carry the read and can arm it. The periodic
        // reclaim below is gated behind this too, so a parked router never
        // autonomously sweeps under a test's feet.
        #[cfg(feature = "test_hooks")]
        if COMMITTER_QUEUE.share().test_bgworker_paused.load(Acquire) == 1 {
            continue;
        }
        // Periodic dead-committer reclaim (§6.5 Q10). Runs at the TOP of the
        // wake loop, before this wake's emits, so it never races our own tick;
        // the full sweep is idempotent and, in steady state (no router crash),
        // does real work only in its Phase-2 dead-committer branch.
        let now = crate::shmem::now_ns();
        if now.saturating_sub(last_reclaim_sweep_ns) >= RECLAIM_SWEEP_INTERVAL_NS {
            try_recover_router_orphan();
            last_reclaim_sweep_ns = now;
        }
        while router_tick() > 0 {}
    }
}

// ── Per-tick pipeline ───────────────────────────────────────────────

/// One scan-and-pack iteration. Returns the number of commit_groups
/// produced this tick (0+; affinity grouping may emit multiple groups
/// from one window).
fn router_tick() -> u32 {
    let staging_capacity = LEDGER_V3_STAGING_QUEUE_SIZE as u32;
    let committer_capacity = LEDGER_V3_COMMITTER_QUEUE_SIZE as u32;
    let window_limit = crate::router_window_size_now().max(1) as u32;
    let batch_max = crate::batch_size_max_now().max(1) as u16;

    COMMITTER_QUEUE
        .share()
        .router_ticks_total
        .fetch_add(1, Relaxed);

    let cooldown_ms = crate::eject_cooldown_ms_now().max(0) as u32;
    let now_ns = crate::shmem::now_ns();
    let candidates_meta = collect_candidates(staging_capacity, window_limit, now_ns, cooldown_ms);
    if candidates_meta.is_empty() {
        return 0;
    }
    COMMITTER_QUEUE
        .share()
        .router_entries_scanned_total
        .fetch_add(candidates_meta.len() as u64, Relaxed);

    // Time-coalesce gate: defer emission if the OLDEST candidate has been
    // pending for less than batch_window_us, giving more submissions a chance
    // to accumulate in the same commit_group. Window=0 disables the gate.
    let window_us = crate::batch_window_us_now() as u64;
    if window_us > 0 {
        let now = crate::shmem::now_us();
        let oldest_age_us = candidates_meta
            .iter()
            .map(|m| now.saturating_sub(m.enqueued_at_micros))
            .max()
            .unwrap_or(u64::MAX);
        if oldest_age_us < window_us {
            COMMITTER_QUEUE
                .share()
                .router_window_defers_total
                .fetch_add(1, Relaxed);
            return 0;
        }
    }

    let candidates = hydrate_candidates(&candidates_meta);
    let mut groups = affinity_group(candidates);

    // acct-xdwk lever 1b: when router_pack_disjoint is on, greedily first-fit
    // the disjoint affinity components into commit_groups of up to
    // batch_size_max so a tick on a spread/Pareto workload emits a few full
    // groups instead of many tiny one-component groups, amortizing the
    // per-group commit/fsync. Disjoint components share no pool_id, so this
    // adds no cross-pool FOR UPDATE contention. Off → one group per component.
    if crate::router_pack_disjoint_now() {
        groups = pack_disjoint_components(groups, (batch_max as usize).max(1));
    }

    // Path C: every component is chunked uniformly at batch_size_max. No
    // order-sensitive no-split case (§14.2) — splitting a FIFO/LIFO pool's
    // submissions across commit_groups is allowed because the depletions are
    // provisional aggregate updates and cross-group hot-pool contention
    // serializes at pool_lock.
    let chunk_cap = batch_max as usize;
    let mut emitted = 0u32;
    'outer: for mut group in groups {
        let group_chunks = group.len().div_ceil(chunk_cap);
        if group_chunks > 1 {
            COMMITTER_QUEUE
                .share()
                .router_cross_commit_group_for_update_waits
                .fetch_add((group_chunks - 1) as u64, Relaxed);
        }
        while !group.is_empty() {
            let take = group.len().min(chunk_cap);
            let chunk: Vec<Candidate> = group.drain(..take).collect();
            match emit_commit_group(chunk, committer_capacity) {
                EmitOutcome::Emitted => {
                    emitted += 1;
                }
                EmitOutcome::Empty => {
                    // Every member CAS-lost (claimed by another emitter).
                    // Not pending any more; skip.
                }
                EmitOutcome::Exhausted => {
                    // Arena or committer queue full. Chunk rolled back to
                    // pending; stop this tick.
                    break 'outer;
                }
            }
        }
    }

    emitted
}

enum EmitOutcome {
    Emitted,
    Empty,
    Exhausted,
}

/// Emit one commit_group from a single chunk of candidates. Caller
/// guarantees the chunk is sorted by request_seq ascending.
fn emit_commit_group(chunk: Vec<Candidate>, committer_capacity: u32) -> EmitOutcome {
    let mut packed: Vec<Candidate> = Vec::with_capacity(chunk.len());
    let mut pool_union: HashSet<i64> = HashSet::new();
    {
        let queue = STAGING_QUEUE.share();
        for cand in chunk {
            let cas_ok = queue.entries[cand.staging_idx as usize]
                .valid
                .compare_exchange(1, 2, Acquire, Relaxed)
                .is_ok();
            if cas_ok {
                for k in &cand.pool_keys {
                    pool_union.insert(*k);
                }
                packed.push(cand);
            }
        }
    }

    if packed.is_empty() {
        return EmitOutcome::Empty;
    }

    let submission_count = packed.len() as u16;
    let mut pool_union_sorted: Vec<i64> = pool_union.into_iter().collect();
    pool_union_sorted.sort();
    let pool_keys_count = pool_union_sorted.len() as u16;

    let arena_alloc =
        allocate_commit_group_arena(submission_count, &packed, pool_keys_count, &pool_union_sorted);
    let (staging_offsets_off, pool_keys_off) = match arena_alloc {
        Some(t) => t,
        None => {
            rollback_packed_to_pending(&packed);
            return EmitOutcome::Exhausted;
        }
    };

    // Claim a CommitterQueueEntry under exclusive lock; CAS valid 0→1
    // with Release publishes to committers.
    let cq_result = {
        let mut queue_guard = COMMITTER_QUEUE.exclusive();
        let queue = &mut *queue_guard;
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
            let cg_id = queue.next_commit_group_id.fetch_add(1, Relaxed) + 1;
            let now_micros = crate::shmem::now_us();
            let slot = &mut queue.entries[cq_idx as usize];
            slot.commit_group_id = cg_id;
            slot.submission_count = submission_count;
            slot.staging_entry_offsets = staging_offsets_off;
            slot.pool_keys_offset = pool_keys_off;
            slot.pool_keys_count = pool_keys_count;
            // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] stamp the owner
            // ordinal from the group's minimum pool_id. Computed unconditionally
            // (one cheap mix); the committer ignores it when affinity is off.
            slot.affinity_owner = crate::affinity::owner_for_min_pool(
                pool_union_sorted.first().copied().unwrap_or(0),
                crate::committer_count_now(),
            );
            slot.committer_bgw_slot.store(u32::MAX, Relaxed);
            slot.committer_bgw_generation.store(0, Relaxed);
            slot.committer_acquired_at_ns.store(0, Relaxed);
            slot.committer_tx_id.store(0, Relaxed);
            slot.enqueued_at_micros = now_micros;
            let _ = slot.valid.compare_exchange(0, 1, Release, Relaxed);
            queue.tail.store((tail + 1) % committer_capacity, Relaxed);
            Some(cg_id)
        } else {
            None
        }
    };

    let cg_id = match cq_result {
        Some(id) => id,
        None => {
            rollback_packed_to_pending(&packed);
            free_commit_group_arena(staging_offsets_off, pool_keys_off);
            return EmitOutcome::Exhausted;
        }
    };

    // Data-before-flag: commit_group_id Release BEFORE CAS valid 2→3 Release.
    // Production runs this canonical ordering unconditionally; the test_hooks build
    // (below) additionally honors two injection atomics so the P3.4 recovery test
    // can stress the ordering window. The reads are compiled out of production
    // (acct-yojk.11) — they were always 0/0 no-ops there.
    #[cfg(not(feature = "test_hooks"))]
    {
        let queue = STAGING_QUEUE.share();
        for cand in &packed {
            let slot = &queue.entries[cand.staging_idx as usize];
            slot.commit_group_id.store(cg_id, Release);
            let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
        }
    }
    #[cfg(feature = "test_hooks")]
    {
        let delay_us = COMMITTER_QUEUE
            .share()
            .test_inject_router_delay_us
            .load(Relaxed);
        let reorder = COMMITTER_QUEUE
            .share()
            .test_reorder_router_stores
            .load(Relaxed)
            != 0;
        let queue = STAGING_QUEUE.share();
        for cand in &packed {
            let slot = &queue.entries[cand.staging_idx as usize];
            if reorder {
                let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
                if delay_us > 0 {
                    std::thread::sleep(Duration::from_micros(delay_us as u64));
                }
                slot.commit_group_id.store(cg_id, Release);
            } else {
                slot.commit_group_id.store(cg_id, Release);
                if delay_us > 0 {
                    std::thread::sleep(Duration::from_micros(delay_us as u64));
                }
                let _ = slot.valid.compare_exchange(2, 3, Release, Relaxed);
            }
        }
    }

    record_commit_group_stats(submission_count);
    EmitOutcome::Emitted
}

// ── Affinity grouping ───────────────────────────────────────────────

/// Partition `candidates` into connected components by pool_id
/// overlap (union-find). Within each component, members are sorted by
/// request_seq; components themselves are sorted by their min
/// request_seq (oldest-first dispatch).
fn affinity_group(candidates: Vec<Candidate>) -> Vec<Vec<Candidate>> {
    let n = candidates.len();
    if n == 0 {
        return Vec::new();
    }
    let mut uf = UnionFind::new(n);

    let mut pool_to_envs: HashMap<i64, Vec<usize>> = HashMap::new();
    for (idx, cand) in candidates.iter().enumerate() {
        for k in &cand.pool_keys {
            pool_to_envs.entry(*k).or_default().push(idx);
        }
    }
    for envs in pool_to_envs.values() {
        if envs.len() >= 2 {
            let head = envs[0];
            for &other in &envs[1..] {
                uf.union(head, other);
            }
        }
    }

    let roots: Vec<usize> = (0..n).map(|i| uf.find(i)).collect();
    let mut by_root: HashMap<usize, Vec<Candidate>> = HashMap::new();
    for (idx, cand) in candidates.into_iter().enumerate() {
        by_root.entry(roots[idx]).or_default().push(cand);
    }

    let mut groups: Vec<Vec<Candidate>> = by_root.into_values().collect();
    for g in &mut groups {
        g.sort_by_key(|c| c.request_seq);
    }
    groups.sort_by_key(|g| g[0].request_seq);
    groups
}

/// Greedily first-fit the disjoint affinity components from `affinity_group`
/// into commit_group bins of at most `cap` submissions (acct-xdwk lever 1b,
/// gated on `router_pack_disjoint`). Components are pool-disjoint by
/// construction — `affinity_group` unions every same-pool submission into one
/// component — so a bin's members share no pool_id; one committer drains the
/// whole bin sequentially with zero cross-pool FOR UPDATE contention, and the
/// single commit/fsync amortizes over more submissions than the per-component
/// emission would on a spread/Pareto workload.
///
/// A component with `len >= cap` is left standalone so the caller's chunk loop
/// splits it at `cap` and records the cross-group FOR UPDATE waits (those
/// chunks DO share a pool). Smaller components first-fit into the earliest bin
/// with room (a standalone large bin never has room, so it is skipped without
/// special-casing). `cap == 1` packs nothing — every component is its own bin,
/// matching the unbatched baseline. Bins and their members are returned sorted
/// by request_seq so oldest-first dispatch is preserved.
fn pack_disjoint_components(groups: Vec<Vec<Candidate>>, cap: usize) -> Vec<Vec<Candidate>> {
    let mut bins: Vec<Vec<Candidate>> = Vec::with_capacity(groups.len());
    for mut comp in groups {
        if comp.len() >= cap {
            bins.push(comp);
            continue;
        }
        let mut placed = false;
        for bin in bins.iter_mut() {
            if bin.len() + comp.len() <= cap {
                bin.append(&mut comp);
                placed = true;
                break;
            }
        }
        if !placed {
            bins.push(comp);
        }
    }
    for bin in bins.iter_mut() {
        bin.sort_by_key(|c| c.request_seq);
    }
    bins.sort_by_key(|bin| bin[0].request_seq);
    bins
}

/// Disjoint-set with path compression and union-by-rank.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        let mut root = x;
        while self.parent[root] != root {
            root = self.parent[root];
        }
        let mut cur = x;
        while self.parent[cur] != root {
            let next = self.parent[cur];
            self.parent[cur] = root;
            cur = next;
        }
        root
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }
}

// ── Candidate collection + hydration ────────────────────────────────

#[derive(Debug)]
struct CandidateMeta {
    staging_idx: u32,
    request_seq: u64,
    pool_keys_offset: u32,
    pool_count: u16,
    enqueued_at_micros: u64,
}

#[derive(Debug)]
struct Candidate {
    staging_idx: u32,
    request_seq: u64,
    pool_keys: Vec<i64>,
}

/// Walk the staging ring from `head` and collect up to `window_limit`
/// pending (valid==1) entries' metadata. Bounded at `staging_capacity`
/// total slot inspections per call.
///
/// Per design-v3.1 §6.3, skips entries inside their eject cooldown
/// window: `eject_count > 0 AND now_ns - last_eject_at_ns <
/// cooldown_ms × 1_000_000`. `cooldown_ms == 0` disables the filter.
fn collect_candidates(
    staging_capacity: u32,
    window_limit: u32,
    now_ns: u64,
    cooldown_ms: u32,
) -> Vec<CandidateMeta> {
    let mut out: Vec<CandidateMeta> = Vec::new();
    let queue = STAGING_QUEUE.share();
    let head = queue.head.load(Relaxed);
    let mut scanned: u32 = 0;
    while scanned < staging_capacity && (out.len() as u32) < window_limit {
        let idx = ((head + scanned) % staging_capacity) as usize;
        let slot = &queue.entries[idx];
        if slot.valid.load(Relaxed) == 1 {
            let observed_eject = slot.eject_count.load(Acquire);
            let observed_last_eject_ns = slot.last_eject_at_ns.load(Acquire);
            if !is_in_eject_cooldown(observed_eject, observed_last_eject_ns, now_ns, cooldown_ms) {
                out.push(CandidateMeta {
                    staging_idx: idx as u32,
                    request_seq: slot.request_seq,
                    pool_keys_offset: slot.pool_keys_offset,
                    pool_count: slot.pool_count,
                    enqueued_at_micros: slot.enqueued_at_micros,
                });
            }
        }
        scanned += 1;
    }
    out
}

/// Pure helper: returns true if the slot's `eject_count` and
/// `last_eject_at_ns` place it inside the active cooldown window.
/// `cooldown_ms == 0` disables the filter (always returns false).
/// `eject_count == 0` means the slot was never ejected (cooldown
/// inapplicable; returns false).
fn is_in_eject_cooldown(
    eject_count: u32,
    last_eject_at_ns: u64,
    now_ns: u64,
    cooldown_ms: u32,
) -> bool {
    if cooldown_ms == 0 || eject_count == 0 {
        return false;
    }
    let cooldown_ns: u64 = (cooldown_ms as u64).saturating_mul(1_000_000);
    let elapsed = now_ns.saturating_sub(last_eject_at_ns);
    elapsed < cooldown_ns
}

/// Read each candidate's pool_keys (i64 array) from the spillover
/// arena under one share-lock acquire.
fn hydrate_candidates(metas: &[CandidateMeta]) -> Vec<Candidate> {
    let arena = SPILLOVER_ARENA.share();
    metas
        .iter()
        .map(|m| {
            let pool_keys: Vec<i64> = if m.pool_count > 0 && m.pool_keys_offset != 0 {
                let bytes = arena.read_bytes(m.pool_keys_offset, m.pool_count as u32 * 8);
                bytes
                    .chunks_exact(8)
                    .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                    .collect()
            } else {
                Vec::new()
            };
            Candidate {
                staging_idx: m.staging_idx,
                request_seq: m.request_seq,
                pool_keys,
            }
        })
        .collect()
}

// ── Arena ownership ─────────────────────────────────────────────────

/// Allocate the commit_group-owned arena blocks atomically — if any
/// allocation fails, free whatever succeeded and return None. Writes
/// the block contents on success.
///
/// Block 1: staging_indices (submission_count × u32 LE)
/// Block 2: pool_keys (pool_keys_count × i64 LE, sorted, deduplicated)
///
/// Path C has no third pool_seqs block (no per-pool sequence table, §6.2).
fn allocate_commit_group_arena(
    submission_count: u16,
    packed: &[Candidate],
    pool_keys_count: u16,
    pool_sorted: &[i64],
) -> Option<(u32, u32)> {
    let mut arena = SPILLOVER_ARENA.exclusive();
    let offsets_bytes = (submission_count as u32) * 4;
    let so_off = arena.alloc(offsets_bytes.max(1))?;

    let pool_off = if pool_keys_count > 0 {
        match arena.alloc(pool_keys_count as u32 * 8) {
            Some(v) => v,
            None => {
                arena.free(so_off);
                return None;
            }
        }
    } else {
        0
    };

    let mut so_buf: Vec<u8> = Vec::with_capacity(offsets_bytes as usize);
    for cand in packed {
        so_buf.extend_from_slice(&cand.staging_idx.to_le_bytes());
    }
    arena.write_bytes(so_off, &so_buf);

    if pool_keys_count > 0 {
        let mut pool_buf: Vec<u8> = Vec::with_capacity(pool_keys_count as usize * 8);
        for k in pool_sorted {
            pool_buf.extend_from_slice(&k.to_le_bytes());
        }
        arena.write_bytes(pool_off, &pool_buf);
    }

    Some((so_off, pool_off))
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

fn free_commit_group_arena(staging_offsets_off: u32, pool_keys_off: u32) {
    let mut arena = SPILLOVER_ARENA.exclusive();
    if staging_offsets_off != 0 {
        arena.free(staging_offsets_off);
    }
    if pool_keys_off != 0 {
        arena.free(pool_keys_off);
    }
}

// ── Stats ───────────────────────────────────────────────────────────

/// Log2-spaced bucket index for a submission count (0..=7). Bucket 0
/// is size-1, bucket 7 is the >=128 overflow.
fn submission_histogram_bucket(submission_count: u16) -> usize {
    if submission_count == 0 {
        return 0;
    }
    let lg = 31 - (submission_count as u32).leading_zeros();
    (lg as usize).min(7)
}

fn record_commit_group_stats(submission_count: u16) {
    let queue = COMMITTER_QUEUE.share();
    queue.router_commit_group_count.fetch_add(1, Relaxed);
    queue
        .router_total_submissions
        .fetch_add(submission_count as u64, Relaxed);
    let bucket = submission_histogram_bucket(submission_count);
    queue.router_submission_histogram[bucket].fetch_add(1, Relaxed);
    let mut prev = queue.router_max_submission_count_per_group.load(Relaxed);
    while submission_count > prev {
        match queue.router_max_submission_count_per_group.compare_exchange(
            prev,
            submission_count,
            Relaxed,
            Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => prev = observed,
        }
    }
}

// ── Observability SPIs ──────────────────────────────────────────────
//
// The committer (P3.3) is still a shell that never drains the committer
// queue, so commit_groups the router emits remain at valid==1 (ready).
// These accessors let the P3.2 affinity-grouping acceptance suite assert
// the routing shape (which submissions landed in which commit_group);
// P3.3/P3.4 reuse them as queue observability.

/// Per-state counts of `CommitterQueueEntry.valid`
/// (empty / ready / in_flight / done). Mirrors the staging accessor.
#[pg_extern]
fn ledger_routed_c_committer_queue_state_counts()
-> TableIterator<'static, (name!(state, String), name!(count, i64))> {
    let queue = COMMITTER_QUEUE.share();
    let mut counts = [0i64; 5];
    for slot in queue.entries.iter() {
        let v = slot.valid.load(Relaxed) as usize;
        if v < counts.len() {
            counts[v] += 1;
        }
    }
    drop(queue);
    let states = ["empty", "ready", "in_flight", "done", "poisoned"];
    let rows: Vec<(String, i64)> = states
        .iter()
        .enumerate()
        .map(|(i, s)| ((*s).to_string(), counts[i]))
        .collect();
    TableIterator::new(rows.into_iter())
}

/// One row per ready (valid==1) commit_group: its id, submission count,
/// and the sorted/deduplicated pool_keys it owns rendered as a
/// comma-separated string (decoded from the spillover arena). Tests
/// parse `pool_keys` to attribute a group to their own pool_ids,
/// isolating their assertions from groups other tests left in the
/// (never-drained) queue.
#[pg_extern]
fn ledger_routed_c_ready_commit_groups() -> TableIterator<
    'static,
    (
        name!(commit_group_id, i64),
        name!(submission_count, i64),
        name!(pool_keys, String),
    ),
> {
    // Snapshot ready entries' metadata under the committer-queue guard
    // (Acquire-load valid pairs with the router's Release CAS so the
    // plain stamped fields are visible), then decode pool_keys under the
    // arena guard — avoid holding both locks at once.
    let metas: Vec<(u64, u16, u32, u16)> = {
        let queue = COMMITTER_QUEUE.share();
        queue
            .entries
            .iter()
            .filter(|s| s.valid.load(Acquire) == 1)
            .map(|s| {
                (
                    s.commit_group_id,
                    s.submission_count,
                    s.pool_keys_offset,
                    s.pool_keys_count,
                )
            })
            .collect()
    };
    let rows: Vec<(i64, i64, String)> = {
        let arena = SPILLOVER_ARENA.share();
        metas
            .into_iter()
            .map(|(cg, sc, off, cnt)| {
                let pool_keys: Vec<i64> = if cnt > 0 && off != 0 {
                    arena
                        .read_bytes(off, cnt as u32 * 8)
                        .chunks_exact(8)
                        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                        .collect()
                } else {
                    Vec::new()
                };
                let keys_csv = pool_keys
                    .iter()
                    .map(|k| k.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                (cg as i64, sc as i64, keys_csv)
            })
            .collect()
    };
    TableIterator::new(rows.into_iter())
}

// ── Boot recovery sweep (§6.5) ──────────────────────────────────────
//
// Recover from a router BGWorker SIGKILL/panic or a committer death.
// Postmaster restarts the router (set_restart_time(5s) — see lib.rs);
// the new router runs this sweep before entering the tick loop. Also
// invoked on demand by the test_hooks SPI.
//
// Two-phase shape (Path C has no per-pool sequence table, so strict
// per-pool seq rollforward is absent; otherwise this mirrors the strict sweep):
//
//   Phase 1 (queue-driven re-stamp): for each CommitterQueueEntry at
//     valid==1 (ready, no committer claim yet), iterate linked staging
//     entries. Any linked staging at (valid==2, cg_id==0 OR cg_id ==
//     CQ.cg_id) means the old router pushed the queue entry but died
//     before completing the per-entry stamp+CAS (the data-before-flag
//     block in emit_commit_group). Roll FORWARD: store cg_id (Release)
//     + CAS staging 2→3 (Release). The committer's normal claim+drain
//     picks up the now-ready slot.
//
//   Phase 2 (queue-driven take-over dead committer): for each
//     CommitterQueueEntry at valid==2 with a dead committer identity
//     (is_committer_alive returns false), revert the CQ to valid==1 +
//     clear identity. The next committer's claim_next_committer_entry
//     sees valid==1 and re-claims. v3.1 needs no pristine-replay here:
//     the committer's PRE-FLIGHT DEDUP (dedup_against_trx) is the
//     recovery source of truth — any submissions the dead committer
//     already wrote to trx are skipped, the rest reprocessed; the trx
//     UNIQUE constraint backstops races.
//
//   Phase 3 (staging-driven orphan-no-queue): for each StagingEntry at
//     valid==2 whose cg_id isn't backed by an active CQ entry, revert
//     valid 2→1 + reset cg_id to 0. The router crashed before pushing
//     the CQ entry.
//
// Phase ordering is load-bearing: sweep_queue_phase (Phase 1+2) MUST run
// before sweep_staging_phase (Phase 3). A staging entry the queue phase
// is about to re-stamp carries cg_id==0 (the router crashed before the
// cg_id store), so it is never in active_cg_ids — running the staging
// phase first would revert it as an orphan, dropping a submission the
// queue phase was about to recover.
//
// Idempotent: a second call finds slots already at their post-recovery
// state and skips (CAS election semantics).

/// Run one full router-orphan recovery sweep. Returns the number of shmem slots
/// reconciled (re-stamped, reverted, or CQ-cleared). Idempotent.
pub(crate) fn try_recover_router_orphan() -> u64 {
    let mut recovered: u64 = 0;
    recovered += sweep_queue_phase();
    recovered += sweep_staging_phase();
    recovered
}

fn sweep_queue_phase() -> u64 {
    let mut recovered: u64 = 0;
    let queue_capacity = LEDGER_V3_COMMITTER_QUEUE_SIZE as u32;

    for q_idx in 0..queue_capacity {
        let (q_valid, q_cg_id, q_submission_count, q_so_off) = {
            let queue = COMMITTER_QUEUE.share();
            let slot = &queue.entries[q_idx as usize];
            (
                slot.valid.load(Acquire),
                slot.commit_group_id,
                slot.submission_count,
                slot.staging_entry_offsets,
            )
        };

        match q_valid {
            1 => {
                // Phase 1: re-stamp linked staging entries the old router never
                // reached in the data-before-flag block.
                if q_submission_count == 0 || q_so_off == 0 {
                    continue;
                }
                let staging_indices = read_staging_indices_at(q_so_off, q_submission_count);
                let queue = STAGING_QUEUE.share();
                for s_idx in staging_indices {
                    if try_restamp_staging_to_routed(&queue, s_idx, q_cg_id) {
                        recovered += 1;
                    }
                }
            }
            2 => {
                // Phase 2: in-flight with a possibly-dead committer.
                if try_reclaim_dead_committer_entry(q_idx) {
                    recovered += 1;
                }
            }
            _ => {}
        }
    }

    recovered
}

/// Phase-2-only reclaim: scan the committer queue and revert every entry at
/// valid==2 whose owning committer is dead. Unlike `try_recover_router_orphan`
/// this touches ONLY the committer queue (never staging), so it is safe to run
/// concurrently with a live router's emit — a committer calls it at startup to
/// reclaim an orphan a clean-FATAL predecessor left in-flight (§6.5 Q10), rather
/// than waiting for a router restart. Idempotent; returns the count reclaimed.
pub(crate) fn reclaim_dead_committers() -> u64 {
    let mut reclaimed: u64 = 0;
    let queue_capacity = LEDGER_V3_COMMITTER_QUEUE_SIZE as u32;
    for q_idx in 0..queue_capacity {
        if try_reclaim_dead_committer_entry(q_idx) {
            reclaimed += 1;
        }
    }
    reclaimed
}

/// Phase-2 per-entry helper: if CQ[`q_idx`] is at valid==2 with a dead owning
/// committer identity, CAS-revert it to valid==1 and clear the identity so the
/// next `claim_next_committer_entry` takes over. Returns true on reclaim. The
/// unclaimed sentinel (generation == 0) is skipped — it was never owned. Staging
/// entries stay at valid==3; the re-claiming committer re-decodes from the CQ
/// payload and re-runs the pipeline, with pre-flight dedup (`dedup_against_trx`)
/// skipping anything the dead committer's tx committed before dying. CAS-elected,
/// so concurrent sweepers (router + committers) race safely — the loser no-ops.
fn try_reclaim_dead_committer_entry(q_idx: u32) -> bool {
    let (q_valid, q_owner_slot, q_owner_gen) = {
        let queue = COMMITTER_QUEUE.share();
        let slot = &queue.entries[q_idx as usize];
        (
            slot.valid.load(Acquire),
            slot.committer_bgw_slot.load(Acquire),
            slot.committer_bgw_generation.load(Acquire),
        )
    };
    if q_valid != 2 || q_owner_gen == 0 {
        return false;
    }
    if is_committer_alive(q_owner_slot, q_owner_gen) {
        return false;
    }
    let queue = COMMITTER_QUEUE.share();
    let slot = &queue.entries[q_idx as usize];
    if slot.valid.compare_exchange(2, 1, Release, Relaxed).is_ok() {
        slot.committer_bgw_slot.store(u32::MAX, Relaxed);
        slot.committer_bgw_generation.store(0, Relaxed);
        slot.committer_acquired_at_ns.store(0, Relaxed);
        slot.committer_tx_id.store(0, Relaxed);
        queue.committer_takeover_count.fetch_add(1, Relaxed);
        return true;
    }
    false
}

fn sweep_staging_phase() -> u64 {
    let mut recovered: u64 = 0;
    let staging_capacity = LEDGER_V3_STAGING_QUEUE_SIZE as u32;
    let queue_capacity = LEDGER_V3_COMMITTER_QUEUE_SIZE as u32;

    // cg_ids backed by an active CQ entry. Staging at valid==2 whose cg_id is in
    // this set was either handled by sweep_queue_phase or is intentionally
    // in-flight; staging at valid==2 whose cg_id isn't is genuinely orphaned.
    let active_cg_ids: HashSet<u64> = {
        let queue = COMMITTER_QUEUE.share();
        let mut s = HashSet::new();
        for q_idx in 0..queue_capacity {
            let slot = &queue.entries[q_idx as usize];
            if slot.valid.load(Acquire) != 0 {
                s.insert(slot.commit_group_id);
            }
        }
        s
    };

    let staging = STAGING_QUEUE.share();
    for s_idx in 0..staging_capacity {
        if try_revert_orphan_staging(&staging, s_idx, &active_cg_ids) {
            signal_staging_slot_freed(&staging);
            recovered += 1;
        }
    }

    recovered
}

/// Phase 1 pure helper: if `s_idx` is at valid==2 with cg_id==0 or matching the
/// queue's cg_id, complete the router's interrupted data-before-flag stamp
/// (store cg_id Release + CAS valid 2→3 Release). Returns true on re-stamp.
pub(crate) fn try_restamp_staging_to_routed(queue: &StagingQueue, s_idx: u32, q_cg_id: u64) -> bool {
    let slot = &queue.entries[s_idx as usize];
    if slot.valid.load(Acquire) != 2 {
        return false;
    }
    let s_cg = slot.commit_group_id.load(Acquire);
    if s_cg != 0 && s_cg != q_cg_id {
        return false; // belongs to a different (later) commit_group
    }
    slot.commit_group_id.store(q_cg_id, Release);
    slot.valid.compare_exchange(2, 3, Release, Relaxed).is_ok()
}

/// Phase 3 pure helper: if `s_idx` is at valid==2 with a cg_id not in
/// `active_cg_ids`, CAS revert valid 2→1 + reset cg_id Release. Returns true on
/// revert. Caller broadcasts the backpressure CV after each successful revert.
pub(crate) fn try_revert_orphan_staging(
    queue: &StagingQueue,
    s_idx: u32,
    active_cg_ids: &HashSet<u64>,
) -> bool {
    let slot = &queue.entries[s_idx as usize];
    if slot.valid.load(Acquire) != 2 {
        return false;
    }
    let s_cg = slot.commit_group_id.load(Acquire);
    if s_cg != 0 && active_cg_ids.contains(&s_cg) {
        return false;
    }
    if slot.valid.compare_exchange(2, 1, Release, Relaxed).is_ok() {
        slot.commit_group_id.store(0, Release);
        return true;
    }
    false
}

/// Read a `submission_count`-length array of u32 staging indices from the
/// spillover arena at `offset`. Router-local mirror of the committer's reader
/// (small enough to copy-paste; resist premature abstraction).
fn read_staging_indices_at(offset: u32, submission_count: u16) -> Vec<u32> {
    if submission_count == 0 || offset == 0 {
        return Vec::new();
    }
    let arena = SPILLOVER_ARENA.share();
    let bytes = arena.read_bytes(offset, submission_count as u32 * 4);
    bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

// ── Test hooks ──────────────────────────────────────────────────────

/// Pause/resume the router (and committer) tick loops by flipping the
/// shmem `test_bgworker_paused` flag. A paused worker skips its tick
/// body on each latch wakeup but stays alive. Tests pause the router,
/// stage a batch of submissions, then resume so the whole batch is
/// collected in a single router window (deterministic affinity
/// grouping, free of tick-boundary splits).
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_bgworker_paused(paused: bool) {
    COMMITTER_QUEUE
        .share()
        .test_bgworker_paused
        .store(u8::from(paused), Release);
}

/// Pause/resume just the committer pool. The router-affinity test keeps the
/// committer paused so emitted commit_groups stay at `ready` for inspection
/// while the router still runs.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_committer_paused(paused: bool) {
    COMMITTER_QUEUE
        .share()
        .test_committer_paused
        .store(u8::from(paused), Release);
}

/// The router worker's OS PID (published at `router_main` startup). Lets a test
/// pg_terminate_backend the router to trigger the boot sweep on its restart.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_router_pid() -> i32 {
    COMMITTER_QUEUE.share().router_pid.load(Acquire)
}

/// Set the committer stall (µs) the pipeline honors between pool_lock acquire and
/// the write. Tests use it to keep the committer in flight long enough to act on
/// it mid-pipeline (§6.8 / §9.3).
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_committer_stall_us(us: i32) {
    let v = if us < 0 { 0u32 } else { us as u32 };
    COMMITTER_QUEUE
        .share()
        .test_inject_committer_stall_us
        .store(v, Release);
}

/// Read the committer stall-hit counter (times the pipeline entered the stall).
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_committer_stall_hits() -> i64 {
    COMMITTER_QUEUE.share().test_committer_stall_hits.load(Acquire) as i64
}

/// Arm `n` synthetic deadlocks (SQLSTATE 40P01) for the committer write phase to
/// raise (one per re-attempt). Drives the §6.8 retry-on-deadlock path.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_inject_deadlock_count(n: i32) {
    let v = if n < 0 { 0u32 } else { n as u32 };
    COMMITTER_QUEUE
        .share()
        .test_inject_committer_deadlock_count
        .store(v, Release);
}

/// Arm a one-shot non-retryable SQL error in the next committer write phase
/// (forces the §6.8 poison path).
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_inject_fatal(on: bool) {
    COMMITTER_QUEUE
        .share()
        .test_inject_committer_fatal
        .store(u8::from(on), Release);
}

/// Arm a one-shot raw 23505 (UNIQUE) in the next committer write phase with no
/// real duplicate behind it. Drives the §6.8 re-drive safety valve: a 23505 whose
/// offender isn't resolvable in `trx` must poison rather than loop.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_inject_unique(on: bool) {
    COMMITTER_QUEUE
        .share()
        .test_inject_committer_unique
        .store(u8::from(on), Release);
}

/// Invoke the router boot sweep synchronously (without restarting the router).
/// Returns the number of slots reconciled. Pause the BGWorkers first so the
/// router can't race-claim the slots being swept.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_run_router_recovery_sweep() -> i64 {
    try_recover_router_orphan() as i64
}

/// Invoke the Phase-2-only dead-committer reclaim synchronously — the exact call
/// a committer makes at startup (§6.5 Q10). Returns the number of orphaned
/// commit_groups reclaimed. Pause the BGWorkers first so nothing race-claims the
/// reverted slot.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_reclaim_dead_committers() -> i64 {
    reclaim_dead_committers() as i64
}

/// Inject a synthetic orphaned CommitterQueueEntry: find the first CQ slot at
/// valid==0, stamp it valid==2 with a phony owner identity (slot 0, generation
/// u32::MAX) that can never match a live committer. Returns the chosen CQ index
/// (-1 if none free). Exercises the boot-sweep Phase 2 take-over path without a
/// real committer SIGKILL — a real SIGKILL on a BGW makes the postmaster restart
/// ALL backends (shmem may be corrupt), which is indistinguishable from the
/// postmaster-restart scenario. Pause the BGWorkers before calling.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_inject_orphan_cq() -> i32 {
    // Exclusive guard: we mutate the entry's plain (non-atomic) data fields, not
    // just the atomics. BGWorkers are paused by the caller, so there's no
    // contention.
    let mut guard = COMMITTER_QUEUE.exclusive();
    let capacity = LEDGER_V3_COMMITTER_QUEUE_SIZE.min(guard.entries.len());
    for idx in 0..capacity {
        let entry = &mut guard.entries[idx];
        if entry.valid.load(Acquire) != 0 {
            continue;
        }
        // Zero the data fields: a finalized (valid==0) slot retains stale
        // staging_entry_offsets / pool_keys_offset / submission_count /
        // commit_group_id from its prior use, and those arena offsets are already
        // freed. Leaving them would make the committer that later drains this
        // reverted orphan double-free them (freelist corruption → an alloc spin
        // holding the arena LWLock). A real Phase-2 orphan carries VALID offsets
        // (the dead committer died before cleanup), so production never hits this;
        // the synthetic hook must clear them to behave like a freshly-emitted
        // empty group whose drain is a clean no-op.
        entry.commit_group_id = 0;
        entry.submission_count = 0;
        entry.staging_entry_offsets = 0;
        entry.pool_keys_offset = 0;
        entry.pool_keys_count = 0;
        entry.committer_bgw_slot.store(0, Release);
        entry.committer_bgw_generation.store(u32::MAX, Release);
        entry.valid.store(2, Release);
        return idx as i32;
    }
    -1
}

// ── Tests for the pure helpers ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(staging_idx: u32, request_seq: u64, pool_keys: Vec<i64>) -> Candidate {
        Candidate {
            staging_idx,
            request_seq,
            pool_keys,
        }
    }

    #[test]
    fn affinity_group_empty_input_returns_empty() {
        assert!(affinity_group(Vec::new()).is_empty());
    }

    #[test]
    fn affinity_group_disjoint_pools_form_separate_components() {
        let cands = vec![
            cand(0, 10, vec![1, 2]),
            cand(1, 11, vec![3, 4]),
            cand(2, 12, vec![5]),
        ];
        let groups = affinity_group(cands);
        assert_eq!(groups.len(), 3);
        let lens: Vec<usize> = groups.iter().map(|g| g.len()).collect();
        assert_eq!(lens, vec![1, 1, 1]);
    }

    #[test]
    fn affinity_group_shared_pool_merges_two_candidates() {
        let cands = vec![cand(0, 10, vec![1, 2]), cand(1, 11, vec![2, 3])];
        let groups = affinity_group(cands);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[0][0].request_seq, 10);
        assert_eq!(groups[0][1].request_seq, 11);
    }

    #[test]
    fn affinity_group_transitive_chain_merges_three() {
        let cands = vec![
            cand(0, 10, vec![1, 2]),
            cand(1, 11, vec![2, 3]),
            cand(2, 12, vec![3, 4]),
        ];
        let groups = affinity_group(cands);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 3);
    }

    #[test]
    fn affinity_group_orders_groups_by_min_request_seq() {
        let cands = vec![
            cand(0, 50, vec![10]),
            cand(1, 20, vec![20]),
            cand(2, 30, vec![20]),
            cand(3, 40, vec![10]),
        ];
        let groups = affinity_group(cands);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0][0].request_seq, 20);
        assert_eq!(groups[1][0].request_seq, 40);
    }

    #[test]
    fn affinity_group_sorts_members_within_group_by_request_seq() {
        let cands = vec![
            cand(0, 30, vec![1]),
            cand(1, 10, vec![1]),
            cand(2, 20, vec![1]),
        ];
        let groups = affinity_group(cands);
        assert_eq!(groups.len(), 1);
        let seqs: Vec<u64> = groups[0].iter().map(|c| c.request_seq).collect();
        assert_eq!(seqs, vec![10, 20, 30]);
    }

    #[test]
    fn affinity_group_candidate_with_no_pools_is_its_own_component() {
        let cands = vec![cand(0, 10, vec![]), cand(1, 11, vec![1, 2])];
        let groups = affinity_group(cands);
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn union_find_initially_each_index_is_its_own_root() {
        let mut uf = UnionFind::new(5);
        for i in 0..5 {
            assert_eq!(uf.find(i), i);
        }
    }

    #[test]
    fn union_find_union_merges_two_components() {
        let mut uf = UnionFind::new(5);
        uf.union(0, 1);
        assert_eq!(uf.find(0), uf.find(1));
        assert_ne!(uf.find(0), uf.find(2));
    }

    #[test]
    fn union_find_transitive_union_is_path_compressed() {
        let mut uf = UnionFind::new(5);
        uf.union(0, 1);
        uf.union(1, 2);
        uf.union(2, 3);
        let root = uf.find(0);
        assert_eq!(uf.find(1), root);
        assert_eq!(uf.find(2), root);
        assert_eq!(uf.find(3), root);
        assert_ne!(uf.find(4), root);
    }

    #[test]
    fn cooldown_zero_disables_filter() {
        assert!(!is_in_eject_cooldown(5, 100, 100, 0));
        assert!(!is_in_eject_cooldown(1, u64::MAX - 1, u64::MAX, 0));
    }

    #[test]
    fn cooldown_zero_eject_count_returns_false() {
        assert!(!is_in_eject_cooldown(0, 0, 100, 10));
        assert!(!is_in_eject_cooldown(0, 999_999, 1_000_000, 10));
    }

    #[test]
    fn cooldown_recent_eject_returns_true() {
        // 10ms cooldown = 10_000_000 ns. now - last = 5ms inside window.
        assert!(is_in_eject_cooldown(1, 10_000_000, 15_000_000, 10));
        assert!(is_in_eject_cooldown(5, 0, 9_999_999, 10));
    }

    #[test]
    fn cooldown_stale_eject_returns_false() {
        assert!(!is_in_eject_cooldown(1, 10_000_000, 21_000_000, 10));
        assert!(!is_in_eject_cooldown(5, 0, 10_000_000, 10));
        // Equal-to threshold is OUTSIDE the window (strict <).
        assert!(!is_in_eject_cooldown(1, 0, 10_000_000, 10));
    }

    #[test]
    fn cooldown_now_before_last_eject_returns_false_via_saturating_sub() {
        // now_ns < last_eject_at_ns: saturating_sub returns 0; elapsed 0 <
        // cooldown → true (correct defensive choice — treat as just-ejected).
        assert!(is_in_eject_cooldown(1, 100, 50, 10));
    }

    #[test]
    fn submission_histogram_bucket_known_values() {
        assert_eq!(submission_histogram_bucket(0), 0);
        assert_eq!(submission_histogram_bucket(1), 0);
        assert_eq!(submission_histogram_bucket(2), 1);
        assert_eq!(submission_histogram_bucket(3), 1);
        assert_eq!(submission_histogram_bucket(4), 2);
        assert_eq!(submission_histogram_bucket(7), 2);
        assert_eq!(submission_histogram_bucket(8), 3);
        assert_eq!(submission_histogram_bucket(15), 3);
        assert_eq!(submission_histogram_bucket(16), 4);
        assert_eq!(submission_histogram_bucket(64), 6);
        assert_eq!(submission_histogram_bucket(127), 6);
        assert_eq!(submission_histogram_bucket(128), 7);
        assert_eq!(submission_histogram_bucket(u16::MAX), 7);
    }

    // ── Router-orphan sweep helpers (§6.5) ──────────────────────────

    fn fresh_staging_queue() -> Box<StagingQueue> {
        unsafe { Box::<StagingQueue>::new_zeroed().assume_init() }
    }

    fn set_staging(slot_state: u8, cg_id: u64) -> Box<StagingQueue> {
        let mut q = fresh_staging_queue();
        let slot = &mut q.entries[3];
        slot.valid = std::sync::atomic::AtomicU8::new(slot_state);
        slot.commit_group_id = std::sync::atomic::AtomicU64::new(cg_id);
        q
    }

    #[test]
    fn restamp_promotes_valid_2_cg_zero_to_routed_with_cg_id() {
        let q = set_staging(2, 0);
        assert!(try_restamp_staging_to_routed(&q, 3, 42));
        assert_eq!(q.entries[3].valid.load(Relaxed), 3);
        assert_eq!(q.entries[3].commit_group_id.load(Relaxed), 42);
    }

    #[test]
    fn restamp_skips_when_valid_is_not_2() {
        let q = set_staging(1, 0);
        assert!(!try_restamp_staging_to_routed(&q, 3, 42));
        assert_eq!(q.entries[3].valid.load(Relaxed), 1);
    }

    #[test]
    fn restamp_skips_when_cg_id_mismatches() {
        // Staging at valid=2 with cg_id=99 belongs to a later commit_group; the
        // Phase 1 sweep iterating CQ cg_id=42 must NOT re-stamp it.
        let q = set_staging(2, 99);
        assert!(!try_restamp_staging_to_routed(&q, 3, 42));
        assert_eq!(q.entries[3].valid.load(Relaxed), 2);
        assert_eq!(q.entries[3].commit_group_id.load(Relaxed), 99);
    }

    #[test]
    fn restamp_idempotent_second_call_is_noop() {
        let q = set_staging(2, 0);
        assert!(try_restamp_staging_to_routed(&q, 3, 42));
        assert!(!try_restamp_staging_to_routed(&q, 3, 42));
    }

    #[test]
    fn revert_orphan_promotes_valid_2_to_pending_when_cg_not_active() {
        let q = set_staging(2, 0);
        let active: HashSet<u64> = HashSet::new();
        assert!(try_revert_orphan_staging(&q, 3, &active));
        assert_eq!(q.entries[3].valid.load(Relaxed), 1);
        assert_eq!(q.entries[3].commit_group_id.load(Relaxed), 0);
    }

    #[test]
    fn revert_orphan_promotes_when_cg_id_set_but_not_active() {
        let q = set_staging(2, 77);
        let active: HashSet<u64> = HashSet::new(); // 77 not present
        assert!(try_revert_orphan_staging(&q, 3, &active));
        assert_eq!(q.entries[3].valid.load(Relaxed), 1);
        assert_eq!(q.entries[3].commit_group_id.load(Relaxed), 0);
    }

    #[test]
    fn revert_orphan_skips_when_cg_id_active() {
        let q = set_staging(2, 77);
        let active: HashSet<u64> = [77u64].into_iter().collect();
        assert!(!try_revert_orphan_staging(&q, 3, &active));
        assert_eq!(q.entries[3].valid.load(Relaxed), 2);
        assert_eq!(q.entries[3].commit_group_id.load(Relaxed), 77);
    }

    #[test]
    fn revert_orphan_skips_when_valid_is_not_2() {
        let q = set_staging(3, 0);
        let active: HashSet<u64> = HashSet::new();
        assert!(!try_revert_orphan_staging(&q, 3, &active));
        assert_eq!(q.entries[3].valid.load(Relaxed), 3);
    }

    #[test]
    fn revert_orphan_idempotent_second_call_is_noop() {
        let q = set_staging(2, 0);
        let active: HashSet<u64> = HashSet::new();
        assert!(try_revert_orphan_staging(&q, 3, &active));
        assert!(!try_revert_orphan_staging(&q, 3, &active));
    }

    // ── pack_disjoint_components (acct-xdwk lever 1b) ────────────────

    #[test]
    fn pack_empty_input_returns_empty() {
        assert!(pack_disjoint_components(vec![], 50).is_empty());
    }

    #[test]
    fn pack_combines_small_disjoint_components_into_one_bin() {
        let bins = pack_disjoint_components(
            vec![
                vec![cand(0, 1, vec![10])],
                vec![cand(1, 2, vec![20])],
                vec![cand(2, 3, vec![30])],
            ],
            50,
        );
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].len(), 3);
    }

    #[test]
    fn pack_respects_cap_first_fit() {
        // Five 2-submission components, cap 4 → bins of 4, 4, 2.
        let mk2 = |base: u32, seq: u64| vec![cand(base, seq, vec![]), cand(base + 1, seq + 1, vec![])];
        let bins = pack_disjoint_components(
            vec![mk2(0, 1), mk2(10, 3), mk2(20, 5), mk2(30, 7), mk2(40, 9)],
            4,
        );
        for b in &bins {
            assert!(b.len() <= 4, "bin exceeded cap: {}", b.len());
        }
        let mut sizes: Vec<usize> = bins.iter().map(|b| b.len()).collect();
        sizes.sort();
        assert_eq!(sizes, vec![2, 4, 4]);
    }

    #[test]
    fn pack_large_component_stays_standalone_for_chunking() {
        // A component with len >= cap is left whole so the caller's chunk loop
        // splits it (and records cross-group FOR UPDATE waits).
        let big = vec![
            cand(0, 1, vec![10]),
            cand(1, 2, vec![10]),
            cand(2, 3, vec![10]),
            cand(3, 4, vec![10]),
            cand(4, 5, vec![10]),
        ];
        let small = vec![cand(10, 6, vec![20])];
        let bins = pack_disjoint_components(vec![big, small], 3);
        assert_eq!(bins.len(), 2);
        assert!(bins.iter().any(|b| b.len() == 5));
        assert!(bins.iter().any(|b| b.len() == 1));
    }

    #[test]
    fn pack_cap_one_packs_nothing() {
        let bins = pack_disjoint_components(
            vec![vec![cand(0, 1, vec![10])], vec![cand(1, 2, vec![20])]],
            1,
        );
        assert_eq!(bins.len(), 2);
    }

    #[test]
    fn pack_preserves_oldest_first_dispatch_and_member_sort() {
        let bins = pack_disjoint_components(
            vec![
                vec![cand(0, 5, vec![10])],
                vec![cand(1, 2, vec![20])],
                vec![cand(2, 8, vec![30])],
            ],
            50,
        );
        assert_eq!(bins.len(), 1);
        assert_eq!(
            bins[0].iter().map(|c| c.request_seq).collect::<Vec<_>>(),
            vec![2, 5, 8]
        );
    }

    #[test]
    fn pack_orders_bins_by_min_request_seq() {
        // First-fit in input order then sort: comp(100) opens bin0, comp(1)
        // joins bin0 (cap 2), comp(2) opens bin1, comp(101) joins bin1.
        let bins = pack_disjoint_components(
            vec![
                vec![cand(0, 100, vec![10])],
                vec![cand(1, 1, vec![20])],
                vec![cand(2, 2, vec![30])],
                vec![cand(3, 101, vec![40])],
            ],
            2,
        );
        assert_eq!(bins.len(), 2);
        assert_eq!(bins[0][0].request_seq, 1);
        assert_eq!(bins[1][0].request_seq, 2);
    }

    /// Document atomicity (acct-p1al Phase 2 invariant): pack_disjoint_components
    /// moves whole Candidates only. A submission is one Candidate carrying all its
    /// lines in one staging entry (payload.rs encode_submission, line_count u32),
    /// so packing can never split a submission across commit_groups, drop one, or
    /// duplicate one. The output must be an exact partition of the input by
    /// staging_idx: every submission lands in exactly one bin, exactly once.
    #[test]
    fn pack_preserves_document_atomicity_exact_partition() {
        let input = vec![
            vec![cand(0, 1, vec![10, 11, 12])], // multi-line (multi-pool) submission
            vec![cand(1, 2, vec![20])],
            vec![cand(2, 3, vec![30])],
            vec![cand(3, 4, vec![40])],
            vec![cand(4, 5, vec![50])],
        ];
        let input_idxs: HashSet<u32> = input.iter().flatten().map(|c| c.staging_idx).collect();
        let bins = pack_disjoint_components(input, 3);

        let mut seen: HashSet<u32> = HashSet::new();
        for bin in &bins {
            assert!(bin.len() <= 3, "bin exceeded document cap: {}", bin.len());
            for c in bin {
                assert!(
                    seen.insert(c.staging_idx),
                    "submission {} split or duplicated across bins",
                    c.staging_idx
                );
            }
        }
        assert_eq!(seen, input_idxs, "packing dropped or invented a submission");
    }

    /// The cap counts DOCUMENTS, never lines: a submission touching many pools
    /// (a fat multi-line document) occupies exactly one slot toward batch_size_max
    /// and is never broken up to fit.
    #[test]
    fn pack_caps_by_document_not_by_line_count() {
        let fat: Vec<i64> = (0..100).collect(); // one document, 100 pools/lines
        let bins = pack_disjoint_components(
            vec![
                vec![cand(0, 1, fat)],
                vec![cand(1, 2, vec![200])],
                vec![cand(2, 3, vec![201])],
            ],
            2,
        );
        for bin in &bins {
            assert!(bin.len() <= 2, "document cap counted lines, not documents: {}", bin.len());
        }
        let fat_members: Vec<&Candidate> =
            bins.iter().flatten().filter(|c| c.staging_idx == 0).collect();
        assert_eq!(fat_members.len(), 1, "fat submission split or duplicated");
        assert_eq!(
            fat_members[0].pool_keys.len(),
            100,
            "fat submission lost lines/pools during packing"
        );
    }
}
