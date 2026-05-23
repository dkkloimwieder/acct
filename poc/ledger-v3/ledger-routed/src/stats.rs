//! Observability SQL accessors over `CommitterQueue` shmem counters.
//!
//! Per plan §F measurement list. The harness polls these per
//! measurement window; acceptance tests assert counters move during
//! load. Arena accessors (`ledger_routed_arena_*`) live in lib.rs and
//! cover their own subsection of the surface.
//!
//! All accessors are atomic loads under `COMMITTER_QUEUE.share()` —
//! no LWLock contention beyond the read-side of the queue lock.
//! Returned BIGINT values are non-decreasing (monotonic counters)
//! except `recovery_complete` (0 then 1) and `next_superbatch_id`
//! (also monotonic but reflects allocation index, not throughput).

use crate::shmem::COMMITTER_QUEUE;
use pgrx::iter::TableIterator;
use pgrx::name;
use pgrx::prelude::*;
use std::sync::atomic::Ordering::Relaxed;

// ── Scalar router counters ──────────────────────────────────────────

#[pg_extern]
fn ledger_routed_router_superbatch_count() -> i64 {
    COMMITTER_QUEUE.share().router_superbatch_count.load(Relaxed) as i64
}

/// Total submissions packed into commit_groups since extension load.
/// Maps to `CommitterQueue.router_total_envelopes` (the v3 schema's
/// "envelope" is one enqueued submission).
#[pg_extern]
fn ledger_routed_router_total_submissions() -> i64 {
    COMMITTER_QUEUE.share().router_total_envelopes.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_router_ticks_total() -> i64 {
    COMMITTER_QUEUE.share().router_ticks_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_router_entries_scanned_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .router_entries_scanned_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_router_window_defers_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .router_window_defers_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_router_max_envelope_count() -> i64 {
    COMMITTER_QUEUE
        .share()
        .router_max_envelope_count
        .load(Relaxed) as i64
}

/// Commit_groups emitted whole (chunking step skipped) because at
/// least one pool in the group is order-sensitive (fifo / lifo /
/// specific). Per acct-aywu: order-sensitive groups never split.
#[pg_extern]
fn ledger_routed_router_order_sensitive_groups_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .router_order_sensitive_groups_total
        .load(Relaxed) as i64
}

/// acct-tm09: count of commit_groups whose committer entered the
/// per-pool predecessor wait loop (incremented once per group that
/// actually slept, not once per touched OS pool).
#[pg_extern]
fn ledger_routed_committer_tm09_waits_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_tm09_waits_total
        .load(Relaxed) as i64
}

/// acct-tm09: count of committer waits that hit the committer_lease_ms
/// deadline without seeing predecessor commit. Indicates a stuck or
/// abandoned predecessor seq (router emit roll-forward bug, dead
/// committer awaiting takeover, etc).
#[pg_extern]
fn ledger_routed_committer_tm09_wait_timeouts_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_tm09_wait_timeouts_total
        .load(Relaxed) as i64
}

/// acct-tm09: cumulative nanoseconds spent in the per-pool predecessor
/// wait loop across all committers since extension load.
#[pg_extern]
fn ledger_routed_committer_tm09_wait_ns_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_tm09_wait_ns_total
        .load(Relaxed) as i64
}

// ── Scalar committer counters ───────────────────────────────────────

#[pg_extern]
fn ledger_routed_committer_drains_total() -> i64 {
    COMMITTER_QUEUE.share().committer_drains_total.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_committer_claim_count() -> i64 {
    COMMITTER_QUEUE.share().committer_claim_count.load(Relaxed) as i64
}

/// Number of CQ entries reclaimed by router boot-sweep Phase 2
/// (dead-committer takeover). See `router::sweep_queue_phase`.
#[pg_extern]
fn ledger_routed_committer_takeover_count() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_takeover_count
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_committer_tx_failures() -> i64 {
    COMMITTER_QUEUE.share().committer_tx_failures.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_committer_pipeline_ns_total() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_pipeline_ns_total
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_committer_pipeline_count() -> i64 {
    COMMITTER_QUEUE
        .share()
        .committer_pipeline_count
        .load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_eject_total_count() -> i64 {
    COMMITTER_QUEUE.share().eject_total_count.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_next_superbatch_id() -> i64 {
    COMMITTER_QUEUE.share().next_superbatch_id.load(Relaxed) as i64
}

#[pg_extern]
fn ledger_routed_recovery_complete() -> bool {
    COMMITTER_QUEUE.share().recovery_complete.load(Relaxed) != 0
}

// ── Composite: per-stage pipeline timing ────────────────────────────

/// Cumulative ns per pipeline stage across all committer workers
/// since extension load. Stages match the §5.4 5-step pipeline
/// breakdown documented on `CommitterQueue`:
///   parse        — Step 3 decode submissions from arena
///   pre_apply    — Steps 4–7 eject + pool_lock + hydrate
///   apply        — Step 8 per-submission plan_apply pristine-replay
///   bulk_insert  — Step 9 bulk-write per submission (under subtx)
///   post         — Steps 10–11 COMMIT + cleanup
#[pg_extern]
fn ledger_routed_committer_stage_timings() -> TableIterator<
    'static,
    (
        name!(stage, String),
        name!(ns_total, i64),
    ),
> {
    let q = COMMITTER_QUEUE.share();
    let rows: Vec<(String, i64)> = vec![
        ("parse".into(), q.committer_stage_parse_ns.load(Relaxed) as i64),
        (
            "pre_apply".into(),
            q.committer_stage_pre_apply_ns.load(Relaxed) as i64,
        ),
        ("apply".into(), q.committer_stage_apply_ns.load(Relaxed) as i64),
        (
            "bulk_insert".into(),
            q.committer_stage_bulk_insert_ns.load(Relaxed) as i64,
        ),
        ("post".into(), q.committer_stage_post_ns.load(Relaxed) as i64),
    ];
    TableIterator::new(rows)
}

// ── Composite: router envelope histogram ────────────────────────────

/// Log2-spaced bucket histogram of SuperBatch envelope counts.
/// Bucket 0=[1], 1=[2-3], 2=[4-7], 3=[8-15], 4=[16-31], 5=[32-63],
/// 6=[64-127], 7=[128+]. The harness uses this to characterize the
/// effective batch-size distribution under different workloads.
#[pg_extern]
fn ledger_routed_router_envelope_histogram() -> TableIterator<
    'static,
    (
        name!(bucket, i32),
        name!(lower, i32),
        name!(upper, i32),
        name!(count, i64),
    ),
> {
    let q = COMMITTER_QUEUE.share();
    let bounds: [(i32, i32); 8] = [
        (1, 1),
        (2, 3),
        (4, 7),
        (8, 15),
        (16, 31),
        (32, 63),
        (64, 127),
        (128, i32::MAX),
    ];
    let mut rows: Vec<(i32, i32, i32, i64)> = Vec::with_capacity(8);
    for (bucket, &(lower, upper)) in bounds.iter().enumerate() {
        let count = q.router_envelope_histogram[bucket].load(Relaxed) as i64;
        rows.push((bucket as i32, lower, upper, count));
    }
    TableIterator::new(rows)
}
