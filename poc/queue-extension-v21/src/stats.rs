//! M7.1 (acct-byue): per-queue + per-method + router-aggregated stats
//! SQL functions per spec §4.4 O3.
//!
//! All readers use atomic Relaxed loads — no LWLock, eventual consistency
//! is acceptable for observability. The hot path increments these
//! counters via Relaxed fetch_add; the reader's view may lag a tick
//! behind reality but never tear.
//!
//! Functions:
//!   - `poc_v21_staging_stats()`     — head, tail, depth, per-state count.
//!   - `poc_v21_committer_stats()`   — committer queue depth + per-state
//!                                     count + claim/takeover/tx_failure
//!                                     counters + drains.
//!   - `poc_v21_method_stats()`      — per-method dispatch_count,
//!                                     error_count, error_rate, p50/p99
//!                                     latency from log2 histogram.
//!   - `poc_v21_apply_seq()`         — next staging request_seq.
//!   - `poc_v21_committer_tx_seq()`  — high watermark of committer_tx_id
//!                                     across CommitterQueueEntry slots.
//!   - `poc_v21_queue_depth(name)`   — depth lookup by queue name
//!                                     ('staging' or 'committer').
//!
//! Reuse: `poc_v21_router_stats()` (M3.3, in router.rs) already covers
//! the router-side metrics — `superbatch_count`, `total_envelopes`,
//! `force_pack_count`, the log2 envelope histogram, packing efficiency.
//! M7.1 adds the envelopes-per-SuperBatch p50/p99 quantile rows to that
//! function (separate edit in router.rs), so this file does NOT redefine
//! poc_v21_router_stats.

use crate::{
    COMMITTER_QUEUE, POC_V21_COMMITTER_QUEUE_SIZE, STAGING_QUEUE, staging,
};
use pgrx::prelude::*;
use std::sync::atomic::Ordering::Relaxed;

/// Per-queue staging stats. One row per metric so callers can
/// `WHERE stat_name=...` to extract specific values. Values are f64
/// (DOUBLE PRECISION) for uniformity across raw counts and quantile
/// outputs.
#[pg_extern]
fn poc_v21_staging_stats() -> TableIterator<
    'static,
    (
        name!(stat_name, String),
        name!(stat_value, f64),
    ),
> {
    let queue = STAGING_QUEUE.share();
    let (empty, pending, processing, routed, abandoned) = staging::state_counts(&queue);
    let head = queue.head.load(Relaxed);
    let tail = queue.tail.load(Relaxed);
    // depth = tail - head modulo capacity (logical lap-aware width).
    // For observability, expose the count of non-empty entries which
    // matches the operator's intuition of "queue depth".
    let depth = pending + processing + routed;
    let next_request_seq = queue.next_request_seq.load(Relaxed);
    let backpressure_wakes = queue.free_slot_wake_count.load(Relaxed);

    let rows: Vec<(String, f64)> = vec![
        ("head".into(), head as f64),
        ("tail".into(), tail as f64),
        ("depth".into(), depth as f64),
        ("count_empty".into(), empty as f64),
        ("count_pending".into(), pending as f64),
        ("count_processing".into(), processing as f64),
        ("count_routed".into(), routed as f64),
        ("count_abandoned".into(), abandoned as f64),
        ("next_request_seq".into(), next_request_seq as f64),
        ("backpressure_wake_count".into(), backpressure_wakes as f64),
    ];
    TableIterator::new(rows.into_iter())
}

/// Per-shard committer queue stats. Covers depth, per-state counts,
/// CAS-claim activity, orphan-takeover activity, sub-tx failure count,
/// successful drain count.
#[pg_extern]
fn poc_v21_committer_stats() -> TableIterator<
    'static,
    (
        name!(stat_name, String),
        name!(stat_value, f64),
    ),
> {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let (mut empty, mut ready, mut in_flight, mut completed) = (0u32, 0u32, 0u32, 0u32);
    for i in 0..capacity {
        match queue.entries[i as usize].valid.load(Relaxed) {
            0 => empty += 1,
            1 => ready += 1,
            2 => in_flight += 1,
            3 => completed += 1,
            _ => {}
        }
    }
    let depth = ready + in_flight + completed;
    let head = queue.head.load(Relaxed);
    let tail = queue.tail.load(Relaxed);
    let next_superbatch_id = queue.next_superbatch_id.load(Relaxed);
    let claim_count = queue.committer_claim_count.load(Relaxed);
    let takeover_count = queue.committer_takeover_count.load(Relaxed);
    let tx_failures = queue.committer_tx_failures.load(Relaxed);
    let drains_total = queue.committer_drains_total.load(Relaxed);

    let rows: Vec<(String, f64)> = vec![
        ("head".into(), head as f64),
        ("tail".into(), tail as f64),
        ("depth".into(), depth as f64),
        ("capacity".into(), capacity as f64),
        ("count_empty".into(), empty as f64),
        ("count_ready".into(), ready as f64),
        ("count_in_flight".into(), in_flight as f64),
        ("count_completed".into(), completed as f64),
        ("next_superbatch_id".into(), next_superbatch_id as f64),
        ("committer_claim_count".into(), claim_count as f64),
        ("committer_takeover_count".into(), takeover_count as f64),
        ("committer_tx_failures".into(), tx_failures as f64),
        ("committer_drains_total".into(), drains_total as f64),
    ];
    TableIterator::new(rows.into_iter())
}

/// Per-method dispatch stats. One row per (method, statistic) combination.
/// p50 / p99 latency come from the per-method log2-spaced 16-bucket
/// histogram (bucket i covers [2^(9+i), 2^(10+i)) ns); quantile output is
/// the bucket's midpoint in nanoseconds. Ballpark resolution is adequate
/// for bottleneck classification (spec §5.6); precise per-method tuning
/// uses the bench harness.
#[pg_extern]
fn poc_v21_method_stats() -> TableIterator<
    'static,
    (
        name!(stat_name, String),
        name!(stat_value, f64),
    ),
> {
    let queue = COMMITTER_QUEUE.share();
    let methods = ["fifo", "avg", "std"];
    let mut rows: Vec<(String, f64)> = Vec::with_capacity(methods.len() * 5);
    for (mi, name) in methods.iter().enumerate() {
        let dispatch = queue.method_dispatch_counts[mi].load(Relaxed);
        let errors = queue.method_error_counts[mi].load(Relaxed);
        let mut hist: [u64; 16] = [0; 16];
        for (i, b) in queue.method_latency_hist[mi].iter().enumerate() {
            hist[i] = b.load(Relaxed);
        }
        let total: u64 = hist.iter().sum();
        let error_rate = if dispatch > 0 {
            errors as f64 / dispatch as f64
        } else {
            0.0
        };
        let p50_ns = bucket_quantile_midpoint_ns(&hist, total, 0.50);
        let p99_ns = bucket_quantile_midpoint_ns(&hist, total, 0.99);
        rows.push((format!("{name}_dispatch_count"), dispatch as f64));
        rows.push((format!("{name}_error_count"), errors as f64));
        rows.push((format!("{name}_error_rate"), error_rate));
        rows.push((format!("{name}_p50_ns"), p50_ns));
        rows.push((format!("{name}_p99_ns"), p99_ns));
    }
    TableIterator::new(rows.into_iter())
}

/// Bucket-quantile helper. `hist[i]` is the count of samples with
/// nanosecond latency in [2^(9+i), 2^(10+i)). Bucket 15 is the >=16ms
/// overflow.
fn bucket_quantile_midpoint_ns(hist: &[u64; 16], total: u64, q: f64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let target = (total as f64 * q).ceil() as u64;
    let mut cum: u64 = 0;
    for (i, &c) in hist.iter().enumerate() {
        cum += c;
        if cum >= target {
            // Midpoint of [2^(9+i), 2^(10+i)) is 1.5 × 2^(9+i).
            let lo = 1u64 << (9 + i);
            let hi = 1u64 << (10 + i);
            return (lo + hi) as f64 / 2.0;
        }
    }
    // Shouldn't reach here if total > 0; defensive fallback to overflow
    // bucket midpoint.
    let lo = 1u64 << (9 + 15);
    (lo as f64) * 1.5
}

/// Next staging request_seq (high watermark of enqueued envelopes).
/// Equivalent to extracting `next_request_seq` from `poc_v21_staging_stats`
/// but provided as a scalar for the §4.4 O3 helper-fn list.
#[pg_extern]
fn poc_v21_apply_seq() -> i64 {
    let queue = STAGING_QUEUE.share();
    queue.next_request_seq.load(Relaxed) as i64
}

/// High watermark of `committer_tx_id` across all CommitterQueueEntry
/// slots. Indicates the most recent committer-side transaction that
/// touched the queue. Useful for debugging long-running batches.
#[pg_extern]
fn poc_v21_committer_tx_seq() -> i64 {
    let queue = COMMITTER_QUEUE.share();
    let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
    let mut hi: u64 = 0;
    for i in 0..capacity {
        let v = queue.entries[i as usize].committer_tx_id.load(Relaxed);
        if v > hi {
            hi = v;
        }
    }
    hi as i64
}

/// Depth lookup by queue name. Accepts 'staging' or 'committer'; any
/// other value returns -1 to signal "unknown queue". Implementations
/// reuse the per-queue stats logic but project a single integer.
#[pg_extern]
fn poc_v21_queue_depth(queue_name: &str) -> i64 {
    match queue_name {
        "staging" => {
            let queue = STAGING_QUEUE.share();
            let (_, pending, processing, routed, _) = staging::state_counts(&queue);
            (pending + processing + routed) as i64
        }
        "committer" => {
            let queue = COMMITTER_QUEUE.share();
            let capacity = POC_V21_COMMITTER_QUEUE_SIZE as u32;
            let mut depth: u32 = 0;
            for i in 0..capacity {
                if queue.entries[i as usize].valid.load(Relaxed) != 0 {
                    depth += 1;
                }
            }
            depth as i64
        }
        _ => -1,
    }
}
