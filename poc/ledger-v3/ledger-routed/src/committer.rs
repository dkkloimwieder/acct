//! Committer BGWorker pool (design-v3 §5.4 steps 1-11).
//!
//! Pool of `committer_count` worker processes, each claiming
//! CommitterQueueEntry slots via CAS election. Per claim:
//!
//!   1. CAS valid 1→2 + identity stamp (`claim_next_committer_entry`)
//!   2. Open one PG transaction (`BackgroundWorker::transaction`)
//!   3. Decode submissions from arena via `payload::decode_submission`
//!   4. pg_xact_status batched lookup → eject in-progress callers
//!      (bump `eject_count` + `last_eject_at_ns` + reset sb_id +
//!      CAS valid 3→1; router cooldown filter from acct-r4sb skips
//!      them on subsequent ticks). Aborted callers excluded silently
//!      (v3 has no submission_status table; "trx exists iff committed").
//!   5. Compute pool_id union
//!   6. `pool_lock::acquire_pool_locks` FOR UPDATE in ascending order
//!   7. `hydration::hydrate_snapshot` builds the pristine Snapshot
//!   8/9. Pristine-replay outer loop: clone pristine, walk kept
//!      submissions, `ledger_core::plan_apply` each. On Err exclude +
//!      restart. After plans clean: bulk-write under one subtx; on
//!      UNIQUE violation (race with a prior commit), the catch_when
//!      handler marks the failing index in a shared Cell and the
//!      outer loop excludes + retries.
//!   10. Implicit COMMIT on `BackgroundWorker::transaction` scope exit
//!   11. `cleanup::cleanup_after_superbatch` outside the tx scope
//!
//! Identity is per-BGWorker-process state held in a thread-local
//! `RefCell<Option<(u32, u32)>>`. Claimed at startup via
//! `identity::claim_committer_identity`, stamped into each
//! `CommitterQueueEntry.committer_bgw_{slot,generation}` at claim
//! time, released on clean shutdown.

#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::CString;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ledger_core::{
    LineType, PlanResult, Snapshot, TrxLineRequest, plan_apply,
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
    /// Set once at BGWorker startup via `set_my_committer_identity`.
    /// `(slot_idx, generation)` from `claim_committer_identity`. Read
    /// by `claim_next_committer_entry` when stamping
    /// `CommitterQueueEntry.committer_bgw_{slot,generation}`.
    static MY_COMMITTER_IDENTITY: RefCell<Option<(u32, u32)>> = const { RefCell::new(None) };
}

fn set_my_committer_identity(slot: u32, generation: u32) {
    MY_COMMITTER_IDENTITY.with(|cell| {
        *cell.borrow_mut() = Some((slot, generation));
    });
}

fn clear_my_committer_identity() {
    MY_COMMITTER_IDENTITY.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

fn my_committer_identity() -> Option<(u32, u32)> {
    MY_COMMITTER_IDENTITY.with(|cell| *cell.borrow())
}

// ── BGWorker entry point ────────────────────────────────────────────

/// One committer BGWorker process. Registered N times in `_PG_init`
/// (one per `committer_count`).
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_committer_main(_arg: pg_sys::Datum) {
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
        if COMMITTER_QUEUE.share().test_bgworker_paused.load(Acquire) == 1 {
            continue;
        }

        while let Some(cq_idx) = claim_next_committer_entry() {
            let (sb_id, staging_offsets_off, pool_keys_off) = {
                let cq = COMMITTER_QUEUE.share();
                let entry = &cq.entries[cq_idx as usize];
                (
                    entry.superbatch_id,
                    entry.staging_entry_offsets,
                    entry.pool_keys_offset,
                )
            };

            let outcome = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                process_commit_group(cq_idx, sb_id)
            }));

            // Cleanup runs OUTSIDE the tx scope: it operates on shmem
            // CAS-state + arena, not on tracked rows. The staging
            // slots that were ejected mid-pipeline are already left
            // at valid==1 with their offsets intact for router
            // re-pack; cleanup just handles the CAS 3→0 / 2→0 cases
            // for slots that reached the routed state.
            let (drained_staging_indices, drained_status) = match outcome {
                ProcessOutcome::Committed {
                    committed_count,
                    staging_indices,
                } => {
                    let cq = COMMITTER_QUEUE.share();
                    cq.committer_drains_total.fetch_add(1, Relaxed);
                    cq.router_total_envelopes
                        .fetch_add(committed_count as u64, Relaxed);
                    (staging_indices, "ok")
                }
                ProcessOutcome::AllEjected { staging_indices } => {
                    // No work done, but the slot itself still needs
                    // CAS 3→0 release for any non-ejected entries (in
                    // this case all entries were ejected and left at
                    // valid==1; the loop is a no-op for those).
                    (staging_indices, "all-ejected")
                }
                ProcessOutcome::TxError {
                    staging_indices, ..
                } => {
                    COMMITTER_QUEUE
                        .share()
                        .committer_tx_failures
                        .fetch_add(1, Relaxed);
                    (staging_indices, "tx-error")
                }
            };
            let _ = drained_status;

            cleanup::cleanup_after_superbatch(
                cq_idx,
                sb_id,
                &drained_staging_indices,
                staging_offsets_off,
                pool_keys_off,
            );
        }
    }

    release_committer_identity(slot);
    clear_my_committer_identity();
}

// ── Claim a CommitterQueueEntry ─────────────────────────────────────

/// Scan the CommitterQueue ring for a valid==1 entry, CAS-claim it
/// via the identity election protocol, return the slot index.
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

fn process_commit_group(cq_idx: u32, _sb_id: u64) -> ProcessOutcome {
    // Step 3: decode all submissions from arena
    let parse_t0 = now_ns();
    let staging_indices = read_staging_indices(cq_idx);
    let decoded = match decode_submissions(&staging_indices) {
        Ok(d) => d,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("payload decode: {e}"),
                staging_indices,
            };
        }
    };
    let parse_ns = now_ns().saturating_sub(parse_t0);
    record_stage_parse(parse_ns);

    // Step 4: eject loop on pg_xact_status
    let pre_apply_t0 = now_ns();
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
        let pre_apply_ns = now_ns().saturating_sub(pre_apply_t0);
        record_stage_pre_apply(pre_apply_ns);
        return ProcessOutcome::AllEjected { staging_indices };
    }

    // Step 5: pool_id union (sort + dedup via BTreeSet)
    let pool_ids: Vec<i64> = kept
        .iter()
        .flat_map(|d| d.lines.iter().map(|l| l.pool_id))
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect();

    // Step 6: pool_lock FOR UPDATE
    if let Err(e) = pool_lock::acquire_pool_locks(&pool_ids) {
        return ProcessOutcome::TxError {
            message: format!("pool_lock: {e}"),
            staging_indices,
        };
    }

    // Step 7: hydrate pristine snapshot
    let pristine = match hydration::hydrate_snapshot(&pool_ids) {
        Ok(s) => s,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("hydrate_snapshot: {e}"),
                staging_indices,
            };
        }
    };
    let pre_apply_ns = now_ns().saturating_sub(pre_apply_t0);
    record_stage_pre_apply(pre_apply_ns);

    // Steps 8 + 9: pristine-replay loop with bulk-write
    let result = run_pristine_replay_loop(&pristine, &kept);
    let committed_count = match result {
        Ok(n) => n,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: e,
                staging_indices,
            };
        }
    };

    let post_t0 = now_ns();
    let post_ns = now_ns().saturating_sub(post_t0);
    record_stage_post(post_ns);

    ProcessOutcome::Committed {
        committed_count,
        staging_indices,
    }
}

fn run_pristine_replay_loop(
    pristine: &Snapshot,
    kept: &[Decoded],
) -> Result<usize, String> {
    let mut excluded: HashSet<usize> = HashSet::new();
    let max_iterations = kept.len().saturating_add(1);

    for _ in 0..max_iterations {
        // Plan stage with inner exclude-on-Err loop
        let plans = match plan_all_with_inner_replay(pristine, kept, &mut excluded) {
            PlanOutcome::Clean(p) => p,
            PlanOutcome::AllExcluded => return Ok(0),
            PlanOutcome::FatalDecode(e) => return Err(e),
        };
        if plans.is_empty() {
            return Ok(0);
        }

        let apply_t0 = now_ns();
        let bulk_t0 = now_ns();
        let write_result = write_plans_in_subtx(&plans);
        let bulk_ns = now_ns().saturating_sub(bulk_t0);
        record_stage_bulk_insert(bulk_ns);
        let apply_ns = now_ns().saturating_sub(apply_t0);
        record_stage_apply(apply_ns);

        match write_result {
            WriteOutcome::Committed { count } => return Ok(count),
            WriteOutcome::UniqueViolationAt { idx } => {
                excluded.insert(idx);
                continue;
            }
            WriteOutcome::SpiError(e) => return Err(format!("bulk-write: {e}")),
        }
    }

    Err(format!(
        "pristine-replay exceeded {max_iterations} iterations without convergence"
    ))
}

enum PlanOutcome {
    Clean(Vec<PlannedSubmission>),
    AllExcluded,
    FatalDecode(String),
}

struct PlannedSubmission {
    idx_in_kept: usize,
    trx_type: String,
    source_id: i64,
    posted_at: DateTime<Utc>,
    plan: PlanResult,
}

/// Plan every non-excluded submission against a fresh clone of
/// pristine. On per-submission plan_apply Err, add to `excluded` and
/// restart the walk. Terminates when one full pass succeeds without
/// any Err (or all submissions are excluded).
fn plan_all_with_inner_replay(
    pristine: &Snapshot,
    kept: &[Decoded],
    excluded: &mut HashSet<usize>,
) -> PlanOutcome {
    let cap = kept.len().saturating_add(1);
    for _ in 0..cap {
        let mut snapshot = pristine.clone();
        let mut plans: Vec<PlannedSubmission> = Vec::new();
        let mut failed: Option<usize> = None;

        for (i, d) in kept.iter().enumerate() {
            if excluded.contains(&i) {
                continue;
            }
            let posted_at = match DateTime::<Utc>::from_timestamp_micros(
                d.submission.posted_at_micros,
            ) {
                Some(t) => t,
                None => {
                    return PlanOutcome::FatalDecode(format!(
                        "submission #{i} posted_at_micros {} out of range",
                        d.submission.posted_at_micros
                    ));
                }
            };

            let line_requests: Vec<TrxLineRequest> = match decode_lines(&d.lines) {
                Ok(v) => v,
                Err(e) => {
                    return PlanOutcome::FatalDecode(format!("submission #{i}: {e}"));
                }
            };

            match plan_apply(&mut snapshot, &line_requests, posted_at) {
                Ok(plan) => plans.push(PlannedSubmission {
                    idx_in_kept: i,
                    trx_type: d.submission.trx_type.clone(),
                    source_id: d.submission.source_id,
                    posted_at,
                    plan,
                }),
                Err(_e) => {
                    failed = Some(i);
                    break;
                }
            }
        }

        if let Some(i) = failed {
            excluded.insert(i);
            // Loop: re-plan from pristine with the new exclusion
            continue;
        }

        if plans.is_empty() {
            return PlanOutcome::AllExcluded;
        }
        return PlanOutcome::Clean(plans);
    }

    PlanOutcome::AllExcluded
}

enum WriteOutcome {
    Committed { count: usize },
    UniqueViolationAt { idx: usize },
    SpiError(pgrx::spi::Error),
}

/// Apply all planned submissions in one nested savepoint. On
/// UNIQUE_VIOLATION (race against a concurrent committer's prior
/// commit of the same (trx_type, source_id)), the savepoint rolls
/// back and the failing index is reported so the outer loop can
/// exclude + re-plan + retry.
fn write_plans_in_subtx(plans: &[PlannedSubmission]) -> WriteOutcome {
    let savepoint_name = match CString::new("usn2_bulk_attempt") {
        Ok(s) => s,
        Err(_) => unreachable!("savepoint name has no NUL"),
    };
    unsafe {
        pg_sys::BeginInternalSubTransaction(savepoint_name.as_ptr());
    }

    // Track which plan is mid-write so the UNIQUE-catch handler can
    // report its index. Cell is UnwindSafe; the AssertUnwindSafe wrap
    // on the closure satisfies pgrx's PgTryBuilder bound.
    let in_flight: Cell<Option<usize>> = Cell::new(None);

    let result: Result<usize, pgrx::spi::Error> = PgTryBuilder::new(AssertUnwindSafe(|| {
        for p in plans {
            in_flight.set(Some(p.idx_in_kept));
            bulk_write::apply_plan_result(&p.trx_type, p.source_id, p.posted_at, &p.plan)?;
        }
        in_flight.set(None);
        Ok(plans.len())
    }))
    .catch_when(PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION, |_| {
        // Reuse the in_flight Cell-captured idx via the outer scope.
        // We can't reach the Cell from inside catch_when directly
        // because the closure captures by FnMut(*mut ErrorData) — but
        // we read it AFTER execute() returns, via the outcome below.
        Ok(usize::MAX)
    })
    .execute();

    let was_unique_violation = matches!(&result, Ok(n) if *n == usize::MAX);
    if was_unique_violation {
        unsafe {
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
        }
        let idx = in_flight.get().unwrap_or(0);
        return WriteOutcome::UniqueViolationAt { idx };
    }
    match result {
        Ok(count) => {
            unsafe {
                pg_sys::ReleaseCurrentSubTransaction();
            }
            WriteOutcome::Committed { count }
        }
        Err(e) => {
            unsafe {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
            }
            WriteOutcome::SpiError(e)
        }
    }
}

// ── Caller user-tx classification + eject ───────────────────────────

/// Batched pg_xact_status lookup over the unique XIDs in `decoded`,
/// then per-submission classification. Submissions whose caller
/// user-tx is in_progress get ejected (eject_count + last_eject_at_ns
/// bumped; sb_id reset; staging valid CAS 3→1 so router re-packs after
/// cooldown). Aborted callers excluded silently (v3 has no
/// submission_status table). Returns the Committed set.
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
            let status = parse_caller_status(s.as_deref());
            xid_status.insert(xid, status);
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
            .copied_with_unknown_default();
        match status {
            CallerTxStatus::Committed | CallerTxStatus::Unknown => kept.push(decoded_clone(d)),
            CallerTxStatus::Aborted => {
                // Drop silently — v3 has no submission_status table to
                // record the failure. Treat like "caller never
                // submitted." Staging slot will be cleaned up via the
                // normal 3→0 CAS path (decoded entry was at valid==3
                // by the time we read it).
            }
            CallerTxStatus::InProgress => {
                let queue = STAGING_QUEUE.share();
                let slot = &queue.entries[d.staging_idx as usize];
                let prev = slot.eject_count.load(Acquire);
                let enqueued_at = slot.enqueued_at_micros;
                let elapsed_us = now.saturating_sub(enqueued_at);
                let new_count = prev.saturating_add(1);
                let exceeds_count = new_count > max_ejects;
                let exceeds_timeout = elapsed_us > timeout_us;
                if exceeds_count || exceeds_timeout {
                    // Terminal-fail: drop the envelope. Staging slot
                    // stays at valid==3 and gets cleaned up by the
                    // normal 3→0 CAS — the caller's user-tx, if it
                    // eventually commits, lacks a corresponding trx
                    // row (which is the failure signal in v3).
                } else {
                    to_eject.push(d.staging_idx);
                }
            }
        }
    }

    // Apply eject CAS for non-terminal in_progress entries.
    if !to_eject.is_empty() {
        let queue = STAGING_QUEUE.share();
        let now_n = now_ns();
        let mut eject_total: u64 = 0;
        for &s_idx in &to_eject {
            let slot = &queue.entries[s_idx as usize];
            slot.eject_count.fetch_add(1, Release);
            slot.last_eject_at_ns.store(now_n, Release);
            slot.superbatch_id.store(0, Release);
            let _ = slot.valid.compare_exchange(3, 1, Release, Relaxed);
            eject_total += 1;
        }
        if eject_total > 0 {
            COMMITTER_QUEUE
                .share()
                .eject_total_count
                .fetch_add(eject_total, Relaxed);
        }
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

trait CopiedWithUnknownDefault {
    fn copied_with_unknown_default(self) -> CallerTxStatus;
}

impl CopiedWithUnknownDefault for Option<&CallerTxStatus> {
    fn copied_with_unknown_default(self) -> CallerTxStatus {
        match self {
            Some(s) => match s {
                CallerTxStatus::Committed => CallerTxStatus::Committed,
                CallerTxStatus::InProgress => CallerTxStatus::InProgress,
                CallerTxStatus::Aborted => CallerTxStatus::Aborted,
                CallerTxStatus::Unknown => CallerTxStatus::Unknown,
            },
            None => CallerTxStatus::Unknown,
        }
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

// ── Arena reads ─────────────────────────────────────────────────────

fn read_staging_indices(cq_idx: u32) -> Vec<u32> {
    let (staging_offsets_off, envelope_count) = {
        let cq = COMMITTER_QUEUE.share();
        let entry = &cq.entries[cq_idx as usize];
        (entry.staging_entry_offsets, entry.envelope_count)
    };
    if envelope_count == 0 || staging_offsets_off == 0 {
        return Vec::new();
    }
    let arena = SPILLOVER_ARENA.share();
    let bytes = arena.read_bytes(staging_offsets_off, envelope_count as u32 * 4);
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
        });
    }
    Ok(out)
}

/// Decode a `line_type` text value to the ledger-core enum. Mirror of
/// the same fn in `ledger-direct/src/submit.rs` (locked plan §G Q3:
/// copy-paste, resist premature abstraction).
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

// ── Stage timing helpers ────────────────────────────────────────────

fn record_stage_parse(ns: u64) {
    COMMITTER_QUEUE
        .share()
        .committer_stage_parse_ns
        .fetch_add(ns, Relaxed);
}
fn record_stage_pre_apply(ns: u64) {
    COMMITTER_QUEUE
        .share()
        .committer_stage_pre_apply_ns
        .fetch_add(ns, Relaxed);
}
fn record_stage_apply(ns: u64) {
    COMMITTER_QUEUE
        .share()
        .committer_stage_apply_ns
        .fetch_add(ns, Relaxed);
}
fn record_stage_bulk_insert(ns: u64) {
    COMMITTER_QUEUE
        .share()
        .committer_stage_bulk_insert_ns
        .fetch_add(ns, Relaxed);
}
fn record_stage_post(ns: u64) {
    COMMITTER_QUEUE
        .share()
        .committer_stage_post_ns
        .fetch_add(ns, Relaxed);
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
        assert_eq!(
            decode_line_type("inv_adjustment_line"),
            Some(LineType::InvAdjustmentLine)
        );
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
        assert_eq!(
            decode_line_type("revaluation_line"),
            Some(LineType::RevaluationLine)
        );
    }

    #[test]
    fn decode_line_type_unknown_returns_none() {
        assert_eq!(decode_line_type(""), None);
        assert_eq!(decode_line_type("not_a_real_type"), None);
        assert_eq!(decode_line_type("PO_RECEIPT_LINE"), None);
    }

    #[test]
    fn decode_lines_builds_trx_line_requests() {
        let lines = vec![
            PocV3Line {
                line_type: "po_receipt_line".into(),
                source_id: Some(42),
                pool_id: 7,
                qty: 10,
                unit_cost: 50,
                debit_account: 1,
                credit_account: 2,
            },
            PocV3Line {
                line_type: "inv_adjustment_line".into(),
                source_id: None,
                pool_id: 8,
                qty: -3,
                unit_cost: 100,
                debit_account: 1,
                credit_account: 2,
            },
        ];
        let req = decode_lines(&lines).expect("decode ok");
        assert_eq!(req.len(), 2);
        assert_eq!(req[0].pool_id, 7);
        assert_eq!(req[0].line_type, LineType::PoReceiptLine);
        assert_eq!(req[0].qty, 10);
        assert_eq!(req[1].pool_id, 8);
        assert_eq!(req[1].line_type, LineType::InvAdjustmentLine);
        assert_eq!(req[1].qty, -3);
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
        }];
        assert!(decode_lines(&lines).is_err());
    }

    #[test]
    fn parse_caller_status_maps_all_known_strings() {
        assert_eq!(parse_caller_status(Some("committed")), CallerTxStatus::Committed);
        assert_eq!(parse_caller_status(Some("aborted")), CallerTxStatus::Aborted);
        assert_eq!(
            parse_caller_status(Some("in progress")),
            CallerTxStatus::InProgress
        );
        assert_eq!(parse_caller_status(None), CallerTxStatus::Unknown);
        assert_eq!(parse_caller_status(Some("garbage")), CallerTxStatus::Unknown);
    }

    #[test]
    fn copied_with_unknown_default_passes_through_known_status() {
        let map: HashMap<u64, CallerTxStatus> =
            [(1u64, CallerTxStatus::Committed), (2u64, CallerTxStatus::Aborted)]
                .into_iter()
                .collect();
        assert_eq!(
            map.get(&1u64).copied_with_unknown_default(),
            CallerTxStatus::Committed
        );
        assert_eq!(
            map.get(&2u64).copied_with_unknown_default(),
            CallerTxStatus::Aborted
        );
        assert_eq!(
            map.get(&99u64).copied_with_unknown_default(),
            CallerTxStatus::Unknown
        );
    }

    #[test]
    fn my_committer_identity_set_and_clear_round_trip() {
        clear_my_committer_identity();
        assert_eq!(my_committer_identity(), None);
        set_my_committer_identity(7, 42);
        assert_eq!(my_committer_identity(), Some((7, 42)));
        set_my_committer_identity(8, 100);
        assert_eq!(my_committer_identity(), Some((8, 100)));
        clear_my_committer_identity();
        assert_eq!(my_committer_identity(), None);
    }
}
