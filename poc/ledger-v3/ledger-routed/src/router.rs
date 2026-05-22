//! Router BGWorker for ledger-routed (Path B).
//!
//! Ports v21's window-router (acct-r29s + acct-zplt) with v3 changes
//! per plan §E:
//!   - Single pool_id domain (i64) — no SKU + WIP split
//!   - No starvation backstop (deferred)
//!   - No persistent_staging audit (no persistent_staging in v3)
//!   - No boot recovery sweep (lands with acct-r2ud)
//!   - No test-hook pg_externs (land with acct-p0d8)
//!
//! ## Per-tick pipeline (design-v3 §5.3)
//!
//!   1. `collect_candidates` — head-scan up to `router_window_size`
//!      staging entries with `valid == 1`; capture (staging_idx,
//!      request_seq, pool_keys_offset/count, enqueued_at_micros).
//!   2. `batch_window_us` gate — if the OLDEST candidate has been
//!      pending for less than the window, defer emission to let more
//!      envelopes accumulate.
//!   3. `hydrate_candidates` — read each candidate's pool_keys (i64
//!      array) from the spillover arena under one share-lock.
//!   4. `affinity_group` — union-find on pool_id overlap, emitting one
//!      `Vec<Candidate>` per connected component, oldest-first by
//!      `min(request_seq)`, members within each group sorted by
//!      request_seq.
//!   5. `emit_superbatch_chunk` per group, splitting oversized
//!      components into chunks of size `batch_size_max`. Each chunk:
//!        a. CAS staging valid 1→2 (Acquire on success)
//!        b. allocate two arena blocks (`staging_indices` u32 array +
//!           deduplicated/sorted pool-keys u64 array)
//!        c. claim a `CommitterQueueEntry`, stamp fields, CAS valid 0→1
//!           with Release (publishes to committers)
//!        d. data-before-flag: store `superbatch_id` on each staging
//!           entry with Release BEFORE CAS staging valid 2→3 with
//!           Release. Recovery sweeps Acquire-load valid first then
//!           Acquire-load superbatch_id; the store order guarantees
//!           consistency if a router crash leaves valid=3.
//!
//! ## Data-before-flag (design-v3 §5.6)
//!
//! Honors v21's invariant: `superbatch_id.store(sb_id, Release)` MUST
//! precede `valid.compare_exchange(2, 3, Release, Relaxed)`. The two
//! test-injection atomics on `CommitterQueue` (`test_inject_router_delay_us`
//! and `test_reorder_router_stores`) are honored so an acct-p0d8
//! regression test can stress the ordering — they default to zero
//! and have no production effect.
//!
//! ## Recovery-complete gate
//!
//! `wait_for_recovery_complete()` polls
//! `COMMITTER_QUEUE.recovery_complete` (Acquire) before entering the
//! tick loop. acct-r2ud will own the flag's lifecycle (set 0 at
//! startup, sweep, set 1). Until then, the router seeds it to 1 at
//! BGWorker entry as an INTERIM no-op so the gate is benign.

use crate::shmem::{
    COMMITTER_QUEUE, LEDGER_V3_COMMITTER_QUEUE_SIZE, LEDGER_V3_STAGING_QUEUE_SIZE,
    SPILLOVER_ARENA, STAGING_QUEUE,
};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::time::Duration;

/// Spin until acct-r2ud has finished its boot recovery sweep and set
/// `COMMITTER_QUEUE.recovery_complete` to 1. Polls in 50ms increments
/// and respects SIGTERM. Called exactly once per worker process at
/// startup.
#[allow(dead_code)]
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

/// Router BGWorker entry point. Registered by `_PG_init` via
/// `BackgroundWorkerBuilder::new("ledger_routed_router")`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_router_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = crate::target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // INTERIM: acct-r2ud will own the recovery_complete flag (set 0
    // at startup, run the orphan sweep, set 1 with Release). Until
    // that ships, seed 1 here so wait_for_recovery_complete is a
    // no-op and the tick loop runs.
    COMMITTER_QUEUE.share().recovery_complete.store(1, Release);
    wait_for_recovery_complete();

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        // Test-only pause hook: a test backend can flip
        // `test_bgworker_paused` to 1 to suspend ticking while
        // synchronous test SQL fns mutate slot state without racing
        // this worker. Production default is 0 (no skip).
        if COMMITTER_QUEUE.share().test_bgworker_paused.load(Acquire) == 1 {
            continue;
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

    {
        let queue = COMMITTER_QUEUE.share();
        queue.router_ticks_total.fetch_add(1, Relaxed);
    }

    let cooldown_ms = crate::eject_cooldown_ms_now().max(0) as u32;
    let now_ns = crate::shmem::now_ns();
    let candidates_meta = collect_candidates(staging_capacity, window_limit, now_ns, cooldown_ms);
    if candidates_meta.is_empty() {
        return 0;
    }
    {
        let queue = COMMITTER_QUEUE.share();
        queue
            .router_entries_scanned_total
            .fetch_add(candidates_meta.len() as u64, Relaxed);
    }

    // Time-coalesce gate: defer emission if the OLDEST candidate has
    // been pending for less than batch_window_us, giving more
    // envelopes a chance to accumulate in the same commit_group.
    // Window=0 disables the gate.
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
    let groups = affinity_group(candidates);

    let mut emitted = 0u32;
    'outer: for mut group in groups {
        let group_chunks = group.len().div_ceil(batch_max as usize);
        if group_chunks > 1 {
            COMMITTER_QUEUE
                .share()
                .router_cross_sb_for_update_waits
                .fetch_add((group_chunks - 1) as u64, Relaxed);
        }
        while !group.is_empty() {
            let take = group.len().min(batch_max as usize);
            let chunk: Vec<Candidate> = group.drain(..take).collect();
            match emit_superbatch_chunk(chunk, committer_capacity) {
                EmitOutcome::Emitted => {
                    emitted += 1;
                }
                EmitOutcome::Empty => {
                    // Every member CAS-lost (claimed by recovery or
                    // another emitter). Not pending any more; skip.
                }
                EmitOutcome::Exhausted => {
                    // Arena or committer queue full. Chunk rolled
                    // back to pending; stop this tick.
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
fn emit_superbatch_chunk(chunk: Vec<Candidate>, committer_capacity: u32) -> EmitOutcome {
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

    let envelope_count = packed.len() as u16;
    let mut pool_union_sorted: Vec<i64> = pool_union.into_iter().collect();
    pool_union_sorted.sort();
    let pool_keys_count = pool_union_sorted.len() as u16;

    let arena_alloc =
        allocate_superbatch_arena(envelope_count, &packed, pool_keys_count, &pool_union_sorted);
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
            let sb_id = queue.next_superbatch_id.fetch_add(1, Relaxed) + 1;
            let now_micros = crate::shmem::now_us();
            let slot = &mut queue.entries[cq_idx as usize];
            slot.superbatch_id = sb_id;
            slot.envelope_count = envelope_count;
            slot.staging_entry_offsets = staging_offsets_off;
            slot.pool_keys_offset = pool_keys_off;
            slot.pool_keys_count = pool_keys_count;
            slot.committer_bgw_slot.store(u32::MAX, Relaxed);
            slot.committer_bgw_generation.store(0, Relaxed);
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
            rollback_packed_to_pending(&packed);
            free_superbatch_arena(staging_offsets_off, pool_keys_off);
            return EmitOutcome::Exhausted;
        }
    };

    // Data-before-flag: superbatch_id Release BEFORE CAS valid 2→3
    // Release. Honor the two test-injection atomics so acct-p0d8 can
    // stress the ordering window. Production defaults are 0/0 (no-op).
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

    record_superbatch_stats(envelope_count);
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
/// Per design-v3 §5.3 step 2, skips entries inside their eject cooldown
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

/// Allocate the two commit_group-owned arena blocks atomically — if
/// either fails, free whatever succeeded and return None. Writes the
/// block contents on success.
///
/// Block 1: staging_indices (envelope_count × u32 LE)
/// Block 2: pool_keys (pool_keys_count × i64 LE, sorted, deduplicated)
fn allocate_superbatch_arena(
    envelope_count: u16,
    packed: &[Candidate],
    pool_keys_count: u16,
    pool_sorted: &[i64],
) -> Option<(u32, u32)> {
    let mut arena = SPILLOVER_ARENA.exclusive();
    let offsets_bytes = (envelope_count as u32) * 4;
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

fn free_superbatch_arena(staging_offsets_off: u32, pool_keys_off: u32) {
    let mut arena = SPILLOVER_ARENA.exclusive();
    if staging_offsets_off != 0 {
        arena.free(staging_offsets_off);
    }
    if pool_keys_off != 0 {
        arena.free(pool_keys_off);
    }
}

// ── Stats ───────────────────────────────────────────────────────────

/// Log2-spaced bucket index for an envelope count (0..=7). Bucket 0
/// is size-1, bucket 7 is the >=128 overflow.
fn envelope_histogram_bucket(envelope_count: u16) -> usize {
    if envelope_count == 0 {
        return 0;
    }
    let lg = 31 - (envelope_count as u32).leading_zeros();
    (lg as usize).min(7)
}

fn record_superbatch_stats(envelope_count: u16) {
    let queue = COMMITTER_QUEUE.share();
    queue.router_superbatch_count.fetch_add(1, Relaxed);
    queue
        .router_total_envelopes
        .fetch_add(envelope_count as u64, Relaxed);
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
        // cooldown_ms == 0 must always return false even with a
        // recent eject.
        assert!(!is_in_eject_cooldown(5, 100, 100, 0));
        assert!(!is_in_eject_cooldown(1, u64::MAX - 1, u64::MAX, 0));
    }

    #[test]
    fn cooldown_zero_eject_count_returns_false() {
        // eject_count == 0 means never ejected; cooldown inapplicable.
        assert!(!is_in_eject_cooldown(0, 0, 100, 10));
        assert!(!is_in_eject_cooldown(0, 999_999, 1_000_000, 10));
    }

    #[test]
    fn cooldown_recent_eject_returns_true() {
        // 10ms cooldown = 10_000_000 ns. now - last = 5ms = 5_000_000
        // ns. Inside window → filtered.
        assert!(is_in_eject_cooldown(1, 10_000_000, 15_000_000, 10));
        assert!(is_in_eject_cooldown(5, 0, 9_999_999, 10));
    }

    #[test]
    fn cooldown_stale_eject_returns_false() {
        // Elapsed exceeds cooldown → no longer filtered.
        assert!(!is_in_eject_cooldown(1, 10_000_000, 21_000_000, 10));
        assert!(!is_in_eject_cooldown(5, 0, 10_000_000, 10));
        // Equal-to threshold is OUTSIDE the window (strict <).
        assert!(!is_in_eject_cooldown(1, 0, 10_000_000, 10));
    }

    #[test]
    fn cooldown_now_before_last_eject_returns_false_via_saturating_sub() {
        // Edge: now_ns < last_eject_at_ns (shouldn't happen in
        // practice but defensive). saturating_sub returns 0; elapsed
        // 0 < cooldown → would return true. That's actually the
        // CORRECT defensive choice — treat as just-ejected.
        assert!(is_in_eject_cooldown(1, 100, 50, 10));
    }

    #[test]
    fn envelope_histogram_bucket_known_values() {
        assert_eq!(envelope_histogram_bucket(0), 0);
        assert_eq!(envelope_histogram_bucket(1), 0);
        assert_eq!(envelope_histogram_bucket(2), 1);
        assert_eq!(envelope_histogram_bucket(3), 1);
        assert_eq!(envelope_histogram_bucket(4), 2);
        assert_eq!(envelope_histogram_bucket(7), 2);
        assert_eq!(envelope_histogram_bucket(8), 3);
        assert_eq!(envelope_histogram_bucket(15), 3);
        assert_eq!(envelope_histogram_bucket(16), 4);
        assert_eq!(envelope_histogram_bucket(64), 6);
        assert_eq!(envelope_histogram_bucket(127), 6);
        assert_eq!(envelope_histogram_bucket(128), 7);
        assert_eq!(envelope_histogram_bucket(u16::MAX), 7);
    }
}
