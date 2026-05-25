//! Committer BGWorker pool (design-v3.1 §6.4).
//!
//! Pool of `committer_count` worker processes, each claiming
//! CommitterQueueEntry slots via CAS identity election. Per claim, in one PG
//! transaction:
//!
//!   1. `claim_next_committer_entry` — CAS valid 1→2 + identity stamp.
//!   3. Decode submissions from arena (`decode_submissions`).
//!   4. pg_xact_status triage (`classify_and_eject`): keep 'committed' callers,
//!      drop 'aborted', eject 'in progress' (CAS staging 3→1, bump eject_count +
//!      last_eject_at_ns; the router's cooldown filter skips them briefly).
//!   5. Pre-flight dedup (`dedup_against_trx`): drop any (trx_type, source_id)
//!      already in `trx`, and any within-batch duplicate (first wins). The trx
//!      UNIQUE constraint stays as a structural backstop.
//!   6. Pool-id union (sorted, deduped).
//!   7. `pool_lock::acquire_pool_locks` FOR UPDATE, ascending.
//!   8. `hydration::hydrate_snapshot` — one aggregate read per pool.
//!   9. **Drop-and-continue apply** (`apply_and_write`): process submissions in
//!      submission_id (enqueue) order against one evolving working snapshot. A
//!      submission whose `plan_apply_provisional` fails is dropped (its trial
//!      clone is discarded, the snapshot is untouched) and the rest continue —
//!      NO pristine-snapshot replay (Path C has no cross-trx hot-path state, so
//!      a failed submission contributes nothing to back out; §6.4, §14.2).
//!  10. Batch write: per-submission trx / trx_line / posting_line + each
//!      submission's *layer* mutations, then the *aggregate* row once per pool
//!      from the final working snapshot — the collapse that turns a whole
//!      commit_group's depletions into one aggregate UPDATE (§6.7).
//!  11. Implicit COMMIT on `BackgroundWorker::transaction` scope exit.
//!  12. `cleanup::cleanup_after_commit_group` outside the tx scope.
//!
//! Path C deltas vs the v3 strict committer: no tm09 per-pool predecessor wait
//! (no PoolSeqTable), and pristine-replay is replaced by drop-and-continue.
//! Retry-on-deadlock and poison classification (§6.8) are P3.4.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::CString;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ledger_core::{
    LineType, PlanResult, PoolStateMutation, Snapshot, TrxLineRequest, plan_apply_provisional,
};
use pgrx::PgTryBuilder;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::prelude::*;

use crate::bulk_write;
use crate::cleanup;
use crate::hydration;
use crate::identity::{claim_committer_identity, release_committer_identity};
use crate::payload::{self, PocV3Line, PocV3Submission};
use crate::pool_lock;
use crate::router;
use crate::shmem::{
    COMMITTER_QUEUE, LEDGER_V3_COMMITTER_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE, now_ns, now_us,
};

// ── Per-process identity (thread-local) ─────────────────────────────

thread_local! {
    static MY_COMMITTER_IDENTITY: RefCell<Option<(u32, u32)>> = const { RefCell::new(None) };
}

fn set_my_committer_identity(slot: u32, generation: u32) {
    MY_COMMITTER_IDENTITY.with(|cell| *cell.borrow_mut() = Some((slot, generation)));
}

fn clear_my_committer_identity() {
    MY_COMMITTER_IDENTITY.with(|cell| *cell.borrow_mut() = None);
}

fn my_committer_identity() -> Option<(u32, u32)> {
    MY_COMMITTER_IDENTITY.with(|cell| *cell.borrow())
}

// ── BGWorker entry point ────────────────────────────────────────────

/// One committer BGWorker process. Registered N times in `_PG_init`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_c_committer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = crate::target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    router::wait_for_recovery_complete();

    let (slot, generation) = claim_committer_identity();
    set_my_committer_identity(slot, generation);

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        if COMMITTER_QUEUE.share().test_committer_paused.load(Acquire) == 1 {
            continue;
        }

        while let Some(cq_idx) = claim_next_committer_entry() {
            let (cg_id, staging_offsets_off, pool_keys_off) = {
                let cq = COMMITTER_QUEUE.share();
                let entry = &cq.entries[cq_idx as usize];
                (
                    entry.commit_group_id,
                    entry.staging_entry_offsets,
                    entry.pool_keys_offset,
                )
            };

            let outcome = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                process_commit_group(cq_idx, cg_id)
            }));

            let staging_indices = match outcome {
                ProcessOutcome::Committed { staging_indices, .. } => {
                    COMMITTER_QUEUE
                        .share()
                        .committer_drains_total
                        .fetch_add(1, Relaxed);
                    staging_indices
                }
                ProcessOutcome::AllEjected { staging_indices } => staging_indices,
                ProcessOutcome::TxError { staging_indices, .. } => {
                    COMMITTER_QUEUE
                        .share()
                        .committer_tx_failures
                        .fetch_add(1, Relaxed);
                    staging_indices
                }
            };

            // Cleanup runs OUTSIDE the tx scope (shmem CAS + arena, no tracked
            // rows). P3.4 will refine the TxError path to retry/poison; for now
            // every outcome releases its slots so nothing is stranded without a
            // recovery sweep.
            cleanup::cleanup_after_commit_group(
                cq_idx,
                cg_id,
                &staging_indices,
                staging_offsets_off,
                pool_keys_off,
            );
        }
    }

    release_committer_identity(slot);
    clear_my_committer_identity();
}

// ── Claim a CommitterQueueEntry ─────────────────────────────────────

/// Scan the CommitterQueue ring for a valid==1 entry, CAS-claim it via the
/// identity election protocol, return the slot index.
fn claim_next_committer_entry() -> Option<u32> {
    let queue = COMMITTER_QUEUE.share();
    let head = queue.head.load(Relaxed);
    let capacity = LEDGER_V3_COMMITTER_QUEUE_SIZE as u32;
    let (my_slot, my_gen) =
        my_committer_identity().expect("claim_next_committer_entry requires a claimed identity");

    for i in 0..capacity {
        let idx = ((head + i) % capacity) as usize;
        let slot = &queue.entries[idx];
        if slot.valid.load(Relaxed) == 1
            && slot
                .committer_bgw_generation
                .compare_exchange(0, my_gen, AcqRel, Relaxed)
                .is_ok()
        {
            slot.committer_bgw_slot.store(my_slot, Release);
            if slot.valid.compare_exchange(1, 2, Acquire, Relaxed).is_ok() {
                slot.committer_acquired_at_ns.store(now_ns(), Relaxed);
                queue.head.store((head + i + 1) % capacity, Relaxed);
                queue.committer_claim_count.fetch_add(1, Relaxed);
                return Some(idx as u32);
            } else {
                slot.committer_bgw_slot.store(u32::MAX, Relaxed);
                slot.committer_bgw_generation.store(0, Relaxed);
            }
        }
    }
    None
}

// ── Per-commit-group pipeline ───────────────────────────────────────

#[derive(Debug)]
pub(crate) enum ProcessOutcome {
    Committed {
        committed_count: usize,
        staging_indices: Vec<u32>,
    },
    AllEjected {
        staging_indices: Vec<u32>,
    },
    TxError {
        message: String,
        staging_indices: Vec<u32>,
    },
}

struct Decoded {
    staging_idx: u32,
    user_tx_xid: u64,
    submission: PocV3Submission,
    lines: Vec<PocV3Line>,
}

#[derive(Debug, PartialEq, Eq)]
enum CallerTxStatus {
    Committed,
    InProgress,
    Aborted,
    Unknown,
}

fn process_commit_group(cq_idx: u32, _cg_id: u64) -> ProcessOutcome {
    let pipeline_t0 = now_ns();
    let outcome = process_commit_group_inner(cq_idx);
    let pipeline_ns = now_ns().saturating_sub(pipeline_t0);
    let cq = COMMITTER_QUEUE.share();
    cq.committer_pipeline_ns_total.fetch_add(pipeline_ns, Relaxed);
    cq.committer_pipeline_count.fetch_add(1, Relaxed);
    outcome
}

fn process_commit_group_inner(cq_idx: u32) -> ProcessOutcome {
    let staging_indices = read_staging_indices(cq_idx);

    // Step 3: decode submissions from arena.
    let decoded = match decode_submissions(&staging_indices) {
        Ok(d) => d,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("payload decode: {e}"),
                staging_indices,
            };
        }
    };

    // Step 4: pg_xact_status triage + eject.
    let kept = match classify_and_eject(&decoded) {
        Ok(k) => k,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("caller_tx_check: {e}"),
                staging_indices,
            };
        }
    };
    if kept.is_empty() {
        return ProcessOutcome::AllEjected { staging_indices };
    }

    // Step 5: pre-flight dedup against trx + within-batch.
    let (kept, dedup_skips) = match dedup_against_trx(kept) {
        Ok(p) => p,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("dedup: {e}"),
                staging_indices,
            };
        }
    };
    if dedup_skips > 0 {
        COMMITTER_QUEUE
            .share()
            .committer_dedup_skips_total
            .fetch_add(dedup_skips, Relaxed);
    }
    if kept.is_empty() {
        // Everything was a duplicate; nothing to write. Slots get released by
        // cleanup (they sit at valid==3).
        return ProcessOutcome::Committed {
            committed_count: 0,
            staging_indices,
        };
    }

    // Step 6: pool-id union.
    let pool_ids: Vec<i64> = kept
        .iter()
        .flat_map(|d| d.lines.iter().map(|l| l.pool_id))
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();

    // Step 7: pool_lock FOR UPDATE.
    if let Err(e) = pool_lock::acquire_pool_locks(&pool_ids) {
        return ProcessOutcome::TxError {
            message: format!("pool_lock: {e}"),
            staging_indices,
        };
    }
    COMMITTER_QUEUE
        .share()
        .committer_pool_lock_acquisitions_total
        .fetch_add(pool_ids.len() as u64, Relaxed);

    // Step 8: hydrate the working snapshot.
    let snapshot = match hydration::hydrate_snapshot(&pool_ids) {
        Ok(s) => s,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("hydrate_snapshot: {e}"),
                staging_indices,
            };
        }
    };

    // Steps 9 + 10: drop-and-continue apply, then batch write.
    match apply_and_write(snapshot, &kept) {
        Ok(summary) => {
            let cq = COMMITTER_QUEUE.share();
            cq.committer_trx_committed_total
                .fetch_add(summary.committed as u64, Relaxed);
            cq.committer_aggregate_upserts_total
                .fetch_add(summary.aggregate_upserts as u64, Relaxed);
            if summary.dropped > 0 {
                cq.committer_dropped_submissions_total
                    .fetch_add(summary.dropped, Relaxed);
            }
            ProcessOutcome::Committed {
                committed_count: summary.committed,
                staging_indices,
            }
        }
        Err(e) => ProcessOutcome::TxError {
            message: e,
            staging_indices,
        },
    }
}

// ── Drop-and-continue apply + batch write ───────────────────────────

struct PlannedSubmission {
    trx_type: String,
    source_id: i64,
    posted_at: DateTime<Utc>,
    plan: PlanResult,
}

struct WriteSummary {
    committed: usize,
    dropped: u64,
    aggregate_upserts: usize,
}

/// Process `kept` in submission_id order against one evolving working snapshot.
/// Each submission is applied to a trial clone; on success the clone becomes the
/// snapshot and the plan is queued, on `plan_apply_provisional` Err the clone is
/// discarded (drop-and-continue — the snapshot is unchanged, no replay). After
/// the forward pass, write everything in one subtx: per-submission trx /
/// trx_line / layer-mutations / posting_line, then the aggregate row once per
/// touched pool from the final snapshot.
fn apply_and_write(mut snapshot: Snapshot, kept: &[Decoded]) -> Result<WriteSummary, String> {
    let mut planned: Vec<PlannedSubmission> = Vec::with_capacity(kept.len());
    let mut agg_pools: BTreeSet<i64> = BTreeSet::new();
    let mut dropped: u64 = 0;

    for d in kept {
        let posted_at = DateTime::<Utc>::from_timestamp_micros(d.submission.posted_at_micros)
            .ok_or_else(|| {
                format!(
                    "submission posted_at_micros {} out of range",
                    d.submission.posted_at_micros
                )
            })?;
        let line_requests = decode_lines(&d.lines).map_err(|e| format!("decode lines: {e}"))?;

        let mut trial = snapshot.clone();
        match plan_apply_provisional(&mut trial, &line_requests, posted_at) {
            Ok(plan) => {
                for m in &plan.pool_state_mutations {
                    if let PoolStateMutation::UpsertAggregate { pool_id, .. } = m {
                        agg_pools.insert(*pool_id);
                    }
                }
                snapshot = trial;
                planned.push(PlannedSubmission {
                    trx_type: d.submission.trx_type.clone(),
                    source_id: d.submission.source_id,
                    posted_at,
                    plan,
                });
            }
            Err(_) => dropped += 1,
        }
    }

    if planned.is_empty() {
        return Ok(WriteSummary {
            committed: 0,
            dropped,
            aggregate_upserts: 0,
        });
    }

    // Final aggregate per touched pool, read from the post-pass snapshot.
    let agg_muts: Vec<PoolStateMutation> = agg_pools
        .iter()
        .filter_map(|&pid| {
            snapshot.aggregate(pid).map(|r| PoolStateMutation::UpsertAggregate {
                pool_id: pid,
                qty: r.qty,
                unit_cost: r.unit_cost,
            })
        })
        .collect();
    let aggregate_upserts = agg_muts.len();

    write_in_subtx(&planned, &agg_muts)?;

    Ok(WriteSummary {
        committed: planned.len(),
        dropped,
        aggregate_upserts,
    })
}

/// Write the whole commit_group inside one nested savepoint. Per submission:
/// trx → trx_line (RETURNING ids) → that submission's *layer* mutations →
/// posting_line. Then the collapsed aggregate UPSERT batch. On UNIQUE_VIOLATION
/// (a dedup-surviving race — recovery territory, P3.4) the savepoint rolls back
/// and the caller poisons via TxError; on any other SQL error likewise.
fn write_in_subtx(
    planned: &[PlannedSubmission],
    agg_muts: &[PoolStateMutation],
) -> Result<(), String> {
    let savepoint = CString::new("rc_commit_group").expect("savepoint name has no NUL");
    unsafe { pg_sys::BeginInternalSubTransaction(savepoint.as_ptr()) };

    // Ok(true) = wrote cleanly; Ok(false) = UNIQUE violation sentinel.
    let result: Result<bool, pgrx::spi::Error> = PgTryBuilder::new(AssertUnwindSafe(|| {
        for p in planned {
            let trx_id = bulk_write::insert_trx(&p.trx_type, p.source_id, p.posted_at)?;
            let trx_line_ids = bulk_write::insert_trx_lines(trx_id, &p.plan.trx_lines)?;
            // Aggregate (layer_id = 0) mutations are collapsed and written once
            // at the end; here we apply only this submission's layer mutations
            // (specific InsertLayer/DeleteLayer), keyed to its own trx_line ids.
            let layer_muts: Vec<PoolStateMutation> = p
                .plan
                .pool_state_mutations
                .iter()
                .filter(|m| !matches!(m, PoolStateMutation::UpsertAggregate { .. }))
                .cloned()
                .collect();
            bulk_write::apply_pool_state_mutations(&layer_muts, &trx_line_ids)?;
            bulk_write::insert_posting_lines(&p.plan.posting_lines, &trx_line_ids)?;
        }
        // Collapsed aggregate upsert (UpsertAggregate ignores trx_line ids).
        bulk_write::apply_pool_state_mutations(agg_muts, &[])?;
        Ok(true)
    }))
    .catch_when(PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION, |_| Ok(false))
    .execute();

    match result {
        Ok(true) => {
            unsafe { pg_sys::ReleaseCurrentSubTransaction() };
            Ok(())
        }
        Ok(false) => {
            unsafe { pg_sys::RollbackAndReleaseCurrentSubTransaction() };
            Err("unique violation survived pre-flight dedup (caller race)".to_string())
        }
        Err(e) => {
            unsafe { pg_sys::RollbackAndReleaseCurrentSubTransaction() };
            Err(format!("bulk-write: {e}"))
        }
    }
}

// ── Pre-flight dedup (§6.4 step 5) ──────────────────────────────────

/// Drop submissions whose (trx_type, source_id) is already in `trx` (a prior
/// commit_group recorded it) or is a within-batch duplicate (first occurrence in
/// submission order wins). Returns the surviving submissions and the skip count.
fn dedup_against_trx(kept: Vec<Decoded>) -> Result<(Vec<Decoded>, u64), String> {
    if kept.is_empty() {
        return Ok((kept, 0));
    }

    let trx_types: Vec<String> = kept.iter().map(|d| d.submission.trx_type.clone()).collect();
    let source_ids: Vec<i64> = kept.iter().map(|d| d.submission.source_id).collect();

    let existing: HashSet<(String, i64)> =
        Spi::connect(|client| -> Result<HashSet<(String, i64)>, pgrx::spi::Error> {
            let mut set = HashSet::new();
            // Compare the existing enum rendered as text against the input text
            // so an unknown trx_type can't fail an enum cast on the input side.
            let mut t = client.select(
                "SELECT trx.trx_type::text, trx.source_id \
                   FROM trx \
                   JOIN UNNEST($1::text[], $2::bigint[]) AS u(tt, sid) \
                     ON trx.trx_type::text = u.tt AND trx.source_id = u.sid",
                None,
                &[trx_types.into(), source_ids.into()],
            )?;
            while let Some(row) = t.next() {
                let tt: String = row.get::<String>(1)?.unwrap_or_default();
                let sid: i64 = row.get::<i64>(2)?.unwrap_or(0);
                set.insert((tt, sid));
            }
            Ok(set)
        })
        .map_err(|e| format!("trx dedup query: {e}"))?;

    let mut seen: HashSet<(String, i64)> = HashSet::new();
    let mut out: Vec<Decoded> = Vec::with_capacity(kept.len());
    let mut skipped: u64 = 0;
    for d in kept {
        let key = (d.submission.trx_type.clone(), d.submission.source_id);
        if existing.contains(&key) || seen.contains(&key) {
            skipped += 1;
        } else {
            seen.insert(key);
            out.push(d);
        }
    }
    Ok((out, skipped))
}

// ── Caller user-tx classification + eject ───────────────────────────

/// Batched pg_xact_status lookup over the unique XIDs, then per-submission
/// classification. In-progress callers are ejected (eject_count +
/// last_eject_at_ns bumped, cg_id reset, staging CAS 3→1 so the router re-packs
/// after cooldown); aborted callers are dropped silently. Returns the kept set.
fn classify_and_eject(decoded: &[Decoded]) -> Result<Vec<Decoded>, pgrx::spi::Error> {
    let mut unique_xids: HashSet<u64> = HashSet::new();
    for d in decoded {
        unique_xids.insert(d.user_tx_xid);
    }
    let xid_strs: Vec<String> = unique_xids.iter().map(|x| x.to_string()).collect();

    let mut xid_status: HashMap<u64, CallerTxStatus> = HashMap::new();
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
            xid_status.insert(xid, parse_caller_status(s.as_deref()));
        }
    }

    let mut kept: Vec<Decoded> = Vec::with_capacity(decoded.len());
    let mut to_eject: Vec<u32> = Vec::new();
    let max_ejects = crate::max_eject_count_now() as u32;
    let timeout_us = (crate::caller_tx_timeout_ms_now() as u64).saturating_mul(1000);
    let now = now_us();

    for d in decoded {
        let status = xid_status
            .get(&d.user_tx_xid)
            .map(status_copy)
            .unwrap_or(CallerTxStatus::Unknown);
        match status {
            CallerTxStatus::Committed | CallerTxStatus::Unknown => kept.push(decoded_clone(d)),
            CallerTxStatus::Aborted => {
                // Drop silently — no trx will exist, which is the failure signal.
            }
            CallerTxStatus::InProgress => {
                let queue = STAGING_QUEUE.share();
                let slot = &queue.entries[d.staging_idx as usize];
                let prev = slot.eject_count.load(Acquire);
                let elapsed_us = now.saturating_sub(slot.enqueued_at_micros);
                let exceeds_count = prev.saturating_add(1) > max_ejects;
                let exceeds_timeout = elapsed_us > timeout_us;
                if !(exceeds_count || exceeds_timeout) {
                    to_eject.push(d.staging_idx);
                }
                // Else terminal-fail: drop (staging slot cleaned up via 3→0).
            }
        }
    }

    if !to_eject.is_empty() {
        let queue = STAGING_QUEUE.share();
        let now_n = now_ns();
        for &s_idx in &to_eject {
            let slot = &queue.entries[s_idx as usize];
            slot.eject_count.fetch_add(1, Release);
            slot.last_eject_at_ns.store(now_n, Release);
            slot.commit_group_id.store(0, Release);
            let _ = slot.valid.compare_exchange(3, 1, Release, Relaxed);
        }
        COMMITTER_QUEUE
            .share()
            .eject_total_count
            .fetch_add(to_eject.len() as u64, Relaxed);
    }

    Ok(kept)
}

fn parse_caller_status(s: Option<&str>) -> CallerTxStatus {
    match s {
        Some("committed") => CallerTxStatus::Committed,
        Some("aborted") => CallerTxStatus::Aborted,
        Some("in progress") => CallerTxStatus::InProgress,
        Some(_) | None => CallerTxStatus::Unknown,
    }
}

fn status_copy(s: &CallerTxStatus) -> CallerTxStatus {
    match s {
        CallerTxStatus::Committed => CallerTxStatus::Committed,
        CallerTxStatus::InProgress => CallerTxStatus::InProgress,
        CallerTxStatus::Aborted => CallerTxStatus::Aborted,
        CallerTxStatus::Unknown => CallerTxStatus::Unknown,
    }
}

fn decoded_clone(d: &Decoded) -> Decoded {
    Decoded {
        staging_idx: d.staging_idx,
        user_tx_xid: d.user_tx_xid,
        submission: d.submission.clone(),
        lines: d.lines.clone(),
    }
}

// ── Arena reads + line decode ───────────────────────────────────────

fn read_staging_indices(cq_idx: u32) -> Vec<u32> {
    let (staging_offsets_off, submission_count) = {
        let cq = COMMITTER_QUEUE.share();
        let entry = &cq.entries[cq_idx as usize];
        (entry.staging_entry_offsets, entry.submission_count)
    };
    if submission_count == 0 || staging_offsets_off == 0 {
        return Vec::new();
    }
    let arena = SPILLOVER_ARENA.share();
    let bytes = arena.read_bytes(staging_offsets_off, submission_count as u32 * 4);
    bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

fn decode_submissions(staging_indices: &[u32]) -> Result<Vec<Decoded>, String> {
    let mut decoded: Vec<Decoded> = Vec::with_capacity(staging_indices.len());
    for &s_idx in staging_indices {
        let (payload_off, payload_len, user_tx_xid) = {
            let q = STAGING_QUEUE.share();
            let slot = &q.entries[s_idx as usize];
            (slot.payload_offset, slot.payload_length, slot.user_tx_xid)
        };
        let arena = SPILLOVER_ARENA.share();
        let (submission, lines) = payload::decode_submission(&arena, payload_off, payload_len)
            .map_err(|e| format!("staging slot {s_idx}: {e}"))?;
        decoded.push(Decoded {
            staging_idx: s_idx,
            user_tx_xid,
            submission,
            lines,
        });
    }
    Ok(decoded)
}

fn decode_lines(lines: &[PocV3Line]) -> Result<Vec<TrxLineRequest>, String> {
    let mut out: Vec<TrxLineRequest> = Vec::with_capacity(lines.len());
    for l in lines {
        let lt = decode_line_type(&l.line_type)
            .ok_or_else(|| format!("unknown line_type '{}'", l.line_type))?;
        out.push(TrxLineRequest {
            pool_id: l.pool_id,
            line_type: lt,
            source_id: l.source_id,
            qty: l.qty,
            unit_cost: l.unit_cost,
            debit_account: l.debit_account,
            credit_account: l.credit_account,
            variance_account: l.variance_account,
        });
    }
    Ok(out)
}

/// Decode a `line_type` text value to the ledger-core enum. Mirror of the same
/// fn in `ledger-direct-c/src/submit.rs` (copy-paste; resist premature
/// abstraction).
fn decode_line_type(s: &str) -> Option<LineType> {
    match s {
        "po_receipt_line" => Some(LineType::PoReceiptLine),
        "wo_output" => Some(LineType::WoOutput),
        "wo_backflush" => Some(LineType::WoBackflush),
        "wo_scrap" => Some(LineType::WoScrap),
        "inv_adjustment_line" => Some(LineType::InvAdjustmentLine),
        "transfer_shipment_line" => Some(LineType::TransferShipmentLine),
        "transfer_receipt_line" => Some(LineType::TransferReceiptLine),
        "manual_adjustment_line" => Some(LineType::ManualAdjustmentLine),
        "revaluation_line" => Some(LineType::RevaluationLine),
        _ => None,
    }
}

// ── Unit tests for pure helpers ─────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_line_type_maps_all_sql_enum_variants() {
        assert_eq!(decode_line_type("po_receipt_line"), Some(LineType::PoReceiptLine));
        assert_eq!(decode_line_type("wo_output"), Some(LineType::WoOutput));
        assert_eq!(decode_line_type("wo_backflush"), Some(LineType::WoBackflush));
        assert_eq!(decode_line_type("wo_scrap"), Some(LineType::WoScrap));
        assert_eq!(decode_line_type("inv_adjustment_line"), Some(LineType::InvAdjustmentLine));
        assert_eq!(
            decode_line_type("transfer_shipment_line"),
            Some(LineType::TransferShipmentLine)
        );
        assert_eq!(
            decode_line_type("transfer_receipt_line"),
            Some(LineType::TransferReceiptLine)
        );
        assert_eq!(
            decode_line_type("manual_adjustment_line"),
            Some(LineType::ManualAdjustmentLine)
        );
        assert_eq!(decode_line_type("revaluation_line"), Some(LineType::RevaluationLine));
    }

    #[test]
    fn decode_line_type_unknown_returns_none() {
        assert_eq!(decode_line_type(""), None);
        assert_eq!(decode_line_type("not_a_real_type"), None);
        assert_eq!(decode_line_type("PO_RECEIPT_LINE"), None);
    }

    #[test]
    fn decode_lines_carries_variance_account() {
        let lines = vec![
            PocV3Line {
                line_type: "po_receipt_line".into(),
                source_id: Some(42),
                pool_id: 7,
                qty: 10,
                unit_cost: 50,
                debit_account: 1,
                credit_account: 2,
                variance_account: Some(3),
            },
            PocV3Line {
                line_type: "inv_adjustment_line".into(),
                source_id: None,
                pool_id: 8,
                qty: -3,
                unit_cost: 100,
                debit_account: 1,
                credit_account: 2,
                variance_account: None,
            },
        ];
        let req = decode_lines(&lines).expect("decode ok");
        assert_eq!(req.len(), 2);
        assert_eq!(req[0].pool_id, 7);
        assert_eq!(req[0].line_type, LineType::PoReceiptLine);
        assert_eq!(req[0].variance_account, Some(3));
        assert_eq!(req[1].qty, -3);
        assert_eq!(req[1].variance_account, None);
    }

    #[test]
    fn decode_lines_unknown_type_returns_err() {
        let lines = vec![PocV3Line {
            line_type: "not_a_type".into(),
            source_id: None,
            pool_id: 1,
            qty: 1,
            unit_cost: 1,
            debit_account: 1,
            credit_account: 2,
            variance_account: None,
        }];
        assert!(decode_lines(&lines).is_err());
    }

    #[test]
    fn parse_caller_status_maps_all_known_strings() {
        assert_eq!(parse_caller_status(Some("committed")), CallerTxStatus::Committed);
        assert_eq!(parse_caller_status(Some("aborted")), CallerTxStatus::Aborted);
        assert_eq!(parse_caller_status(Some("in progress")), CallerTxStatus::InProgress);
        assert_eq!(parse_caller_status(None), CallerTxStatus::Unknown);
        assert_eq!(parse_caller_status(Some("garbage")), CallerTxStatus::Unknown);
    }

    #[test]
    fn my_committer_identity_set_and_clear_round_trip() {
        clear_my_committer_identity();
        assert_eq!(my_committer_identity(), None);
        set_my_committer_identity(7, 42);
        assert_eq!(my_committer_identity(), Some((7, 42)));
        clear_my_committer_identity();
        assert_eq!(my_committer_identity(), None);
    }
}
