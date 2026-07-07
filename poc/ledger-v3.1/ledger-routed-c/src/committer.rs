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
//! Path C deltas vs a strict committer: no per-pool predecessor wait
//! (no per-pool sequence table), and pristine-replay is replaced by
//! drop-and-continue (§6.4 / §14.2).
//!
//! §6.8 SQL error handling: the whole lock → hydrate → apply → write phase runs
//! in a nested subtransaction. A transient SQLSTATE (40P01 deadlock / 40001
//! serialization) rolls the subtx back and retries with exponential backoff (up
//! to MAX_DEADLOCK_RETRIES) — the pre-flight dedup catches anything other
//! committers finished meanwhile. A 23505 that survived pre-flight dedup (a racer
//! committed one of this group's keys between our dedup and our write) re-runs
//! dedup against `trx`, drops the now-visible offender, and re-drives the rest of
//! the group. A non-retryable SQLSTATE (check / not-null / disk-full, …),
//! exhausting the retry budget, or a UNIQUE re-drive that resolves no offender
//! POISONs the commit_group: its CQ entry moves to the terminal valid==4
//! dead-letter state and its submissions are lost. Committer-death recovery itself
//! is the router boot sweep's job (router.rs §6.5 Phase 2); this committer's
//! pre-flight dedup is the recovery source of truth when a reclaimed group is
//! reprocessed.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::CString;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::time::Duration;

use chrono::{DateTime, Utc};
use ledger_core::{
    PlanResult, PoolStateMutation, PostingLineRequest, Snapshot, TrxLineOutput, TrxLineRequest,
    plan_apply_provisional,
};
use pgrx::PgTryBuilder;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::datum::DatumWithOid;
use pgrx::pg_sys::PgOid;
use pgrx::spi::{OwnedPreparedStatement, SpiTupleTable};

/// §6.8 retry-on-deadlock budget: a commit_group whose write phase hits a
/// transient SQLSTATE (40P01 deadlock / 40001 serialization) is retried up to
/// this many times with exponential backoff before being poisoned.
const MAX_DEADLOCK_RETRIES: u32 = 5;

use crate::cleanup;
use crate::identity::{claim_committer_identity, release_committer_identity};
use crate::payload::{self, PocV3Line, PocV3Submission};
use crate::router;
use ledger_spi_common::line_type::decode_line_type;
use ledger_spi_common::{bulk_write, hydration, pool_lock};
use crate::shmem::{
    COMMITTER_QUEUE, LEDGER_V3_COMMITTER_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE, now_ns, now_us,
};

// ── Per-process identity (thread-local) ─────────────────────────────

thread_local! {
    static MY_COMMITTER_IDENTITY: RefCell<Option<(u32, u32)>> = const { RefCell::new(None) };

    /// Kept (`SPI_keepplan`) plan for the prep-phase dedup SELECT against `trx`
    /// (acct-e95d). One-shot SPI re-parsed/re-planned this query on every commit
    /// group, which dominated the prep span once acct-sczx gutted apply; caching
    /// it for the backend's lifetime removes that re-plan. Read-only — built with
    /// `prepare` (not `prepare_mut`) and run via `select`.
    static PLAN_DEDUP_TRX: RefCell<Option<OwnedPreparedStatement>> = const { RefCell::new(None) };

    /// Valid `trx_type` enum labels, queried once per backend via `enum_range`
    /// and cached. Used to drop unknown trx_types from the dedup probe in Rust so
    /// the probe's `u.tt::trx_type` cast only ever sees valid input and can't
    /// raise 22P02 (acct-e95d) — dedup runs outside the write subtx's
    /// `catch_others`, and `BackgroundWorker::transaction` re-raises an uncaught
    /// ERROR, so a raise here would abort the committer tick, not just the group.
    /// A new enum value added at runtime won't appear until restart — consistent
    /// with the kept-plan cache (both assume a schema stable for the backend).
    static DEDUP_TRX_TYPE_LABELS: RefCell<Option<HashSet<String>>> = const { RefCell::new(None) };
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

    // Startup dead-committer reclaim (§6.5 Q10): a clean-FATAL predecessor
    // (uncaught tick error → FATAL → exit(1), or operator pg_terminate_backend
    // mid-pipeline) leaves its in-flight commit_group at CQ valid==2 with a now-
    // dead identity, and the boot sweep only runs at ROUTER restart. Reclaim it
    // now — after claiming our own identity, so the just-bumped generation makes
    // the stale entry read as dead. Phase-2 only (never touches staging), so it
    // cannot race the live router's emit. The router also sweeps periodically;
    // whichever fires first wins the CAS, the other no-ops.
    router::reclaim_dead_committers();

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        // Test-only pause hook: only test_hooks builds can arm it, and the read is
        // compiled out of production entirely (acct-yojk.11).
        #[cfg(feature = "test_hooks")]
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

            let txn_t0 = now_ns();
            let outcome = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                process_commit_group(cq_idx, cg_id)
            }));
            // Whole-transaction span (BEGIN + pipeline + COMMIT/fsync). The
            // commit/fsync cost is committer_txn_ns_total − committer_pipeline_ns_total
            // (acct-0usf STEP 1). Recorded for every drained group regardless of
            // outcome — the COMMIT happens even for AllEjected / TxError groups.
            COMMITTER_QUEUE
                .share()
                .committer_txn_ns_total
                .fetch_add(now_ns().saturating_sub(txn_t0), Relaxed);

            // Cleanup runs OUTSIDE the tx scope (shmem CAS + arena, no tracked
            // rows). Committed / AllEjected / TxError release their slots back to
            // empty; Poisoned (§6.8) releases the staging slots + arena but leaves
            // the CQ entry at the terminal valid==4 dead-letter state.
            match outcome {
                ProcessOutcome::Committed { staging_indices, .. } => {
                    COMMITTER_QUEUE
                        .share()
                        .committer_drains_total
                        .fetch_add(1, Relaxed);
                    cleanup::cleanup_after_commit_group(
                        cq_idx,
                        cg_id,
                        &staging_indices,
                        staging_offsets_off,
                        pool_keys_off,
                    );
                }
                ProcessOutcome::AllEjected { staging_indices } => {
                    cleanup::cleanup_after_commit_group(
                        cq_idx,
                        cg_id,
                        &staging_indices,
                        staging_offsets_off,
                        pool_keys_off,
                    );
                }
                ProcessOutcome::TxError { staging_indices, .. } => {
                    COMMITTER_QUEUE
                        .share()
                        .committer_tx_failures
                        .fetch_add(1, Relaxed);
                    cleanup::cleanup_after_commit_group(
                        cq_idx,
                        cg_id,
                        &staging_indices,
                        staging_offsets_off,
                        pool_keys_off,
                    );
                }
                ProcessOutcome::Poisoned { staging_indices, .. } => {
                    COMMITTER_QUEUE
                        .share()
                        .committer_poisoned_total
                        .fetch_add(1, Relaxed);
                    cleanup::cleanup_after_poison(
                        cq_idx,
                        cg_id,
                        &staging_indices,
                        staging_offsets_off,
                        pool_keys_off,
                    );
                }
            }
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

    // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] read the scheme once per
    // scan. When off, `aff_scheme == SCHEME_OFF` and the per-entry gate below is
    // a single compare that always passes — the claim path is unchanged.
    let aff_scheme = crate::affinity_scheme_now();
    let aff_steal_us = (crate::affinity_steal_ms_now() as u64).saturating_mul(1000);

    for i in 0..capacity {
        let idx = ((head + i) % capacity) as usize;
        let slot = &queue.entries[idx];
        // Acquire pairs with the router's `valid` 0→1 Release so the plain
        // `affinity_owner` field published before that CAS is visible here.
        if slot.valid.load(Acquire) != 1 {
            continue;
        }
        // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE] owner / age-gated-steal
        // gate. Owners claim immediately; non-owners skip until the entry has
        // aged past affinity_steal_ms, then steal it (no starvation on a
        // backed-up or dead owner). `claim_was_steal` is counted only after a
        // successful claim so the engagement counters stay exact under races.
        let mut claim_was_steal = false;
        if aff_scheme != crate::affinity::SCHEME_OFF && slot.affinity_owner != my_slot {
            let age_us = crate::shmem::now_us().saturating_sub(slot.enqueued_at_micros);
            if age_us < aff_steal_us {
                continue;
            }
            claim_was_steal = true;
        }
        if slot
            .committer_bgw_generation
            .compare_exchange(0, my_gen, AcqRel, Relaxed)
            .is_ok()
        {
            slot.committer_bgw_slot.store(my_slot, Release);
            if slot.valid.compare_exchange(1, 2, Acquire, Relaxed).is_ok() {
                slot.committer_acquired_at_ns.store(now_ns(), Relaxed);
                queue.head.store((head + i + 1) % capacity, Relaxed);
                queue.committer_claim_count.fetch_add(1, Relaxed);
                // [acct-0usf affinity — EXPERIMENTAL/REMOVABLE]
                if aff_scheme != crate::affinity::SCHEME_OFF {
                    if claim_was_steal {
                        queue.affinity_steals_total.fetch_add(1, Relaxed);
                    } else {
                        queue.affinity_owned_claims_total.fetch_add(1, Relaxed);
                    }
                }
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
    /// Terminal: a non-retryable SQL error (NOT-NULL, check, disk-full, …), a
    /// deadlock that exhausted MAX_DEADLOCK_RETRIES, or a UNIQUE that survived
    /// dedup with no resolvable offender to re-drive without (a resolvable one is
    /// `DuplicateRace`, not terminal). The CQ entry is moved to valid==4
    /// (dead-letter); submissions are lost.
    Poisoned {
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
    /// `pg_xact_status` returned NULL — the xid is older than the status horizon
    /// (frozen), which means it must have committed. Safe to keep.
    Unknown,
    /// `pg_xact_status` returned a non-null string we don't recognize — a wording
    /// drift across PG versions. We can't confirm the caller committed, so this is
    /// treated as not-yet-committed (never kept): a rename of the status text fails
    /// loud (WARNING + caller timeouts) instead of silently committing work for a
    /// possibly still-open tx.
    Unrecognized,
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

    // Step 3: decode submissions from arena. (acct-e95d prep refold: decode span.)
    let t_decode0 = now_ns();
    let decoded = match decode_submissions(&staging_indices) {
        Ok(d) => d,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("payload decode: {e}"),
                staging_indices,
            };
        }
    };
    COMMITTER_QUEUE
        .share()
        .committer_decode_ns_total
        .fetch_add(now_ns().saturating_sub(t_decode0), Relaxed);

    // Step 4: pg_xact_status triage + eject. (acct-e95d prep refold: xact span.)
    let t_xact0 = now_ns();
    let kept = match classify_and_eject(&decoded) {
        Ok(k) => k,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("caller_tx_check: {e}"),
                staging_indices,
            };
        }
    };
    COMMITTER_QUEUE
        .share()
        .committer_xact_ns_total
        .fetch_add(now_ns().saturating_sub(t_xact0), Relaxed);
    if kept.is_empty() {
        return ProcessOutcome::AllEjected { staging_indices };
    }

    // Step 5: pre-flight dedup against trx + within-batch. (acct-e95d: dedup span.)
    let t_dedup0 = now_ns();
    let (kept, dedup_skips) = match dedup_against_trx(kept) {
        Ok(p) => p,
        Err(e) => {
            return ProcessOutcome::TxError {
                message: format!("dedup: {e}"),
                staging_indices,
            };
        }
    };
    COMMITTER_QUEUE
        .share()
        .committer_dedup_ns_total
        .fetch_add(now_ns().saturating_sub(t_dedup0), Relaxed);
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

    // Step 6: pre-decode lines (pure — no DB). Malformed submissions are dropped
    // here (consistent with drop-and-continue) and counted once, before the
    // retry loop, so a retry never re-pays the decode.
    let t_decode1 = now_ns();
    let mut prepared: Vec<Prepared> = Vec::with_capacity(kept.len());
    let mut predrop: u64 = 0;
    for d in &kept {
        let posted_at = DateTime::<Utc>::from_timestamp_micros(d.submission.posted_at_micros);
        let lines = decode_lines(&d.lines);
        match (posted_at, lines) {
            (Some(posted_at), Ok(lines)) => prepared.push(Prepared {
                trx_type: d.submission.trx_type.clone(),
                source_id: d.submission.source_id,
                posted_at,
                lines,
            }),
            _ => predrop += 1,
        }
    }
    COMMITTER_QUEUE
        .share()
        .committer_decode_ns_total
        .fetch_add(now_ns().saturating_sub(t_decode1), Relaxed);
    if prepared.is_empty() {
        if predrop > 0 {
            COMMITTER_QUEUE
                .share()
                .committer_dropped_submissions_total
                .fetch_add(predrop, Relaxed);
        }
        return ProcessOutcome::Committed {
            committed_count: 0,
            staging_indices,
        };
    }

    let mut pool_ids = pool_ids_of(&prepared);

    // Steps 7-10 with §6.8 retry-on-deadlock + re-drive-on-unique: the whole lock →
    // hydrate → apply → write phase runs in a nested subtransaction. A transient
    // SQLSTATE (40P01 / 40001) rolls the subtx back and re-attempts after backoff;
    // a 23505 that survived pre-flight dedup (a racer wrote the key meanwhile) drops
    // the now-visible offender and re-drives the rest of the group, so its innocent
    // siblings aren't dead-lettered with it; any other non-retryable SQLSTATE — or
    // exhausting a budget — poisons the commit_group. Each re-drive removes ≥1
    // submission, so the bound is the (initial) submission count plus one.
    let mut attempt: u32 = 0;
    let mut redrives: u32 = 0;
    let max_redrives = prepared.len() as u32 + 1;
    loop {
        match attempt_commit_phase(&pool_ids, &prepared) {
            PhaseOutcome::Committed(summary) => {
                let cq = COMMITTER_QUEUE.share();
                cq.committer_pool_lock_acquisitions_total
                    .fetch_add(pool_ids.len() as u64, Relaxed);
                cq.committer_trx_committed_total
                    .fetch_add(summary.committed as u64, Relaxed);
                cq.committer_aggregate_upserts_total
                    .fetch_add(summary.aggregate_upserts as u64, Relaxed);
                let dropped = summary.dropped + predrop;
                if dropped > 0 {
                    cq.committer_dropped_submissions_total
                        .fetch_add(dropped, Relaxed);
                }
                return ProcessOutcome::Committed {
                    committed_count: summary.committed,
                    staging_indices,
                };
            }
            PhaseOutcome::Retryable(msg) => {
                attempt += 1;
                if attempt > MAX_DEADLOCK_RETRIES {
                    return ProcessOutcome::Poisoned {
                        message: format!("deadlock retries exhausted: {msg}"),
                        staging_indices,
                    };
                }
                COMMITTER_QUEUE
                    .share()
                    .committer_deadlock_retries_total
                    .fetch_add(1, Relaxed);
                std::thread::sleep(retry_backoff(attempt));
            }
            PhaseOutcome::DuplicateRace(msg) => {
                redrives += 1;
                COMMITTER_QUEUE
                    .share()
                    .committer_duplicate_redrives_total
                    .fetch_add(1, Relaxed);
                if redrives > max_redrives {
                    return ProcessOutcome::Poisoned {
                        message: format!("duplicate re-drives exhausted: {msg}"),
                        staging_indices,
                    };
                }
                // Re-dedup against trx: the racer that beat us to the UNIQUE is now
                // committed and visible, so it (and any other newly-arrived
                // duplicate) drops out, leaving the innocent rest to re-drive. A
                // fresh hydrate + replan on the next attempt recomputes the
                // aggregate without the offender.
                let removed = match drop_prepared_already_in_trx(&mut prepared) {
                    Ok(n) => n,
                    Err(e) => {
                        return ProcessOutcome::Poisoned {
                            message: format!("re-drive dedup query failed: {e}"),
                            staging_indices,
                        };
                    }
                };
                if removed == 0 {
                    // 23505 with no resolvable duplicate in trx — re-driving can't
                    // make progress (e.g. a non-(trx_type, source_id) UNIQUE, or a
                    // synthetic injected 23505). This is a genuine fatal: poison.
                    return ProcessOutcome::Poisoned {
                        message: format!("unique violation with no resolvable duplicate: {msg}"),
                        staging_indices,
                    };
                }
                COMMITTER_QUEUE
                    .share()
                    .committer_dedup_skips_total
                    .fetch_add(removed, Relaxed);
                if prepared.is_empty() {
                    // The racing duplicate(s) were the whole group — nothing to write.
                    return ProcessOutcome::Committed {
                        committed_count: 0,
                        staging_indices,
                    };
                }
                pool_ids = pool_ids_of(&prepared);
            }
            PhaseOutcome::Fatal(msg) => {
                return ProcessOutcome::Poisoned {
                    message: msg,
                    staging_indices,
                };
            }
        }
    }
}

/// The sorted, deduped set of pool_ids a prepared batch touches — the lock +
/// hydrate set. Recomputed after a re-drive drops a submission, so a now-untouched
/// pool isn't needlessly locked.
fn pool_ids_of(prepared: &[Prepared]) -> Vec<i64> {
    prepared
        .iter()
        .flat_map(|p| p.lines.iter().map(|l| l.pool_id))
        .collect::<BTreeSet<i64>>()
        .into_iter()
        .collect()
}

// ── Drop-and-continue apply + batch write (§6.4 steps 9-10, §6.8) ────

/// A submission whose payload is decoded and ready to apply against the working
/// snapshot. Pre-decoded once before the retry loop.
struct Prepared {
    trx_type: String,
    source_id: i64,
    posted_at: DateTime<Utc>,
    lines: Vec<TrxLineRequest>,
}

struct WriteSummary {
    committed: usize,
    dropped: u64,
    aggregate_upserts: usize,
}

/// What the subtx closure produced: a clean write, or a caught Postgres ERROR
/// carrying its SQLSTATE (so classification happens after the savepoint unwinds).
enum PhaseStep {
    Wrote(WriteSummary),
    Caught(PgSqlErrorCode),
}

/// Outcome of one write-phase attempt (§6.8 classification).
enum PhaseOutcome {
    Committed(WriteSummary),
    /// Transient SQLSTATE (40P01 deadlock / 40001 serialization): the caller
    /// rolls back, backs off, and retries.
    Retryable(String),
    /// 23505 UNIQUE violation that survived pre-flight dedup — a racing committer
    /// wrote one of this group's (trx_type, source_id) keys between our dedup and
    /// our INSERT. The caller re-dedups (the racer is now visible in `trx`), drops
    /// the offender, and re-drives the rest of the group; it does NOT poison the
    /// innocent submissions alongside the offender.
    DuplicateRace(String),
    /// Non-retryable SQLSTATE (check / not-null, disk-full, …) or a Rust-level SPI
    /// error: the caller poisons the commit_group.
    Fatal(String),
}

/// One write-phase attempt inside a nested subtransaction (§6.4 steps 7-10):
/// acquire pool_locks → (test injection) → hydrate → drop-and-continue plan →
/// batch write. On success the savepoint is released; on any caught Postgres
/// ERROR the savepoint is rolled back and the SQLSTATE classified into
/// Retryable / Fatal. Holding the whole phase in the subtx means a deadlock
/// retry re-acquires locks and re-hydrates a fresh snapshot cleanly.
fn attempt_commit_phase(pool_ids: &[i64], prepared: &[Prepared]) -> PhaseOutcome {
    let savepoint = CString::new("rc_commit_group").expect("savepoint name has no NUL");
    unsafe { pg_sys::BeginInternalSubTransaction(savepoint.as_ptr()) };

    // The SQLSTATE of a caught Postgres ERROR is threaded through the closure's
    // RETURN value (PhaseStep::Caught) rather than a captured cell — the catch
    // closure captures nothing, so it trivially satisfies catch_others's
    // RefUnwindSafe bound. Err = a Rust-level SPI error short-circuited a `?`.
    let result: Result<PhaseStep, pgrx::spi::Error> = PgTryBuilder::new(AssertUnwindSafe(|| {
        // Scoped wall-time spans (acct-0usf STEP 1): decompose the committer
        // pipeline into pool_lock / hydrate / apply so the affinity question can be
        // answered from a *measured* per-span breakdown rather than inferred from
        // throughput. fetch_add is plain shmem-atomic work — it never longjmps, so
        // it is safe inside the catch closure; on an early `?` or a caught Postgres
        // ERROR the post-span add is simply skipped (a failed attempt contributes
        // no span, which is what we want).
        let cq = COMMITTER_QUEUE.share();
        let t_lock0 = now_ns();
        pool_lock::acquire_pool_locks(pool_ids)?;
        let t_lock1 = now_ns();
        cq.committer_pool_lock_ns_total
            .fetch_add(t_lock1.saturating_sub(t_lock0), Relaxed);
        // Test-only injection sites — compiled out of production builds entirely
        // (acct-yojk.11); only test_hooks builds carry them and can arm them.
        #[cfg(feature = "test_hooks")]
        {
            maybe_inject_deadlock();
            maybe_inject_fatal();
            maybe_inject_unique();
            maybe_stall();
        }
        let snapshot = hydration::hydrate_snapshot(pool_ids)?;
        let t_hyd = now_ns();
        cq.committer_hydrate_ns_total
            .fetch_add(t_hyd.saturating_sub(t_lock1), Relaxed);
        let summary = plan_and_write(snapshot, prepared)?;
        cq.committer_apply_ns_total
            .fetch_add(now_ns().saturating_sub(t_hyd), Relaxed);
        Ok(PhaseStep::Wrote(summary))
    }))
    .catch_others(|caught| Ok(PhaseStep::Caught(sqlstate_of(&caught))))
    .execute();

    match result {
        Ok(PhaseStep::Wrote(summary)) => {
            unsafe { pg_sys::ReleaseCurrentSubTransaction() };
            PhaseOutcome::Committed(summary)
        }
        Ok(PhaseStep::Caught(code)) => {
            unsafe { pg_sys::RollbackAndReleaseCurrentSubTransaction() };
            classify_phase_error(Some(code), None)
        }
        Err(e) => {
            unsafe { pg_sys::RollbackAndReleaseCurrentSubTransaction() };
            classify_phase_error(None, Some(e))
        }
    }
}

/// Drop-and-continue forward pass + batch write, run INSIDE the caller's subtx.
/// Each submission is applied to a trial clone; on success the clone becomes the
/// snapshot and the plan is queued, on `plan_apply_provisional` Err the clone is
/// discarded (no replay — Path C has no cross-trx hot-path state). Then writes:
/// per-submission trx / trx_line / layer-mutations / posting_line, and the
/// aggregate row once per touched pool from the final snapshot (the §6.7
/// collapse). A Postgres ERROR (e.g. UNIQUE survived dedup) longjmps past the
/// `?` to the caller's catch_others; a Rust-level SPI error returns via `?`.
fn plan_and_write(
    mut snapshot: Snapshot,
    prepared: &[Prepared],
) -> Result<WriteSummary, pgrx::spi::Error> {
    let mut planned: Vec<(&Prepared, PlanResult)> = Vec::with_capacity(prepared.len());
    let mut agg_pools: BTreeSet<i64> = BTreeSet::new();
    let mut dropped: u64 = 0;

    for p in prepared {
        let mut trial = snapshot.clone();
        match plan_apply_provisional(&mut trial, &p.lines, p.posted_at) {
            Ok(plan) => {
                for m in &plan.pool_state_mutations {
                    if let PoolStateMutation::UpsertAggregate { pool_id, .. } = m {
                        agg_pools.insert(*pool_id);
                    }
                }
                snapshot = trial;
                planned.push((p, plan));
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

    // ── Batched write across the whole commit group (acct-sczx Lever A) ──
    // The PLAN pass above stays per-submission and sequential (WAC running
    // average depends on prior submissions' pool effects); only the WRITE
    // collapses. One multi-row INSERT per table replaces the ~700 one-shot
    // statements/group the flamegraph charged for. The trx.id → trx_line.id →
    // posting_line.trx_line_id FK chain is preserved via UNNEST WITH ORDINALITY
    // + ascending-id recovery (8.1b / 8.2b), the same alignment the
    // per-submission primitives use. A UNIQUE collision anywhere in the batch
    // aborts the whole attempt inside the subtx; the §6.8 re-drive re-queries
    // `trx`, drops the now-visible offender(s), and re-drives the reduced set.
    let trx_types: Vec<String> = planned.iter().map(|(p, _)| p.trx_type.clone()).collect();
    let source_ids: Vec<i64> = planned.iter().map(|(p, _)| p.source_id).collect();
    let posted_ats: Vec<DateTime<Utc>> = planned.iter().map(|(p, _)| p.posted_at).collect();
    let trx_ids = bulk_write::insert_trx_batch(&trx_types, &source_ids, &posted_ats)?;

    // Flatten every submission's trx_lines in submission order, tagging each row
    // with its submission's trx.id, and batch-insert. Returned ids are aligned to
    // this flattened order, so they slice back per submission.
    let total_lines: usize = planned.iter().map(|(_, plan)| plan.trx_lines.len()).sum();
    let mut flat_lines: Vec<TrxLineOutput> = Vec::with_capacity(total_lines);
    let mut line_trx_id: Vec<i64> = Vec::with_capacity(total_lines);
    for (i, (_, plan)) in planned.iter().enumerate() {
        for line in &plan.trx_lines {
            flat_lines.push(line.clone());
            line_trx_id.push(trx_ids[i]);
        }
    }
    let flat_line_ids = bulk_write::insert_trx_lines_batch(&line_trx_id, &flat_lines)?;

    // Per submission: apply its layer mutations (specific FIFO/LIFO; none for WAC,
    // the only active method) against its own slice of the batched trx_line ids,
    // and flatten its posting_lines with pre-resolved trx_line ids for one batched
    // posting INSERT. Aggregate (layer_id = 0) mutations are collapsed below.
    let total_postings: usize = planned.iter().map(|(_, plan)| plan.posting_lines.len()).sum();
    let mut posting_tlid: Vec<i64> = Vec::with_capacity(total_postings);
    let mut flat_postings: Vec<PostingLineRequest> = Vec::with_capacity(total_postings);
    let mut offset = 0usize;
    for (_, plan) in &planned {
        let len = plan.trx_lines.len();
        let slice = &flat_line_ids[offset..offset + len];
        offset += len;

        let layer_muts: Vec<PoolStateMutation> = plan
            .pool_state_mutations
            .iter()
            .filter(|m| !matches!(m, PoolStateMutation::UpsertAggregate { .. }))
            .cloned()
            .collect();
        bulk_write::apply_pool_state_mutations(&layer_muts, slice)?;

        for req in &plan.posting_lines {
            posting_tlid.push(slice[req.trx_line_idx]);
            flat_postings.push(req.clone());
        }
    }
    bulk_write::insert_posting_lines_batch(&posting_tlid, &flat_postings)?;

    // Collapsed aggregate per touched pool, read from the post-pass snapshot.
    let agg_muts: Vec<PoolStateMutation> = agg_pools
        .iter()
        .filter_map(|&pid| {
            snapshot.aggregate(pid).map(|r| PoolStateMutation::UpsertAggregate {
                pool_id: pid,
                qty: r.qty,
                unit_cost: r.unit_cost,
                value_sum: r.value_sum,
            })
        })
        .collect();
    let aggregate_upserts = agg_muts.len();
    bulk_write::apply_pool_state_mutations(&agg_muts, &[])?;

    Ok(WriteSummary {
        committed: planned.len(),
        dropped,
        aggregate_upserts,
    })
}

// ── §6.8 SQL-error classification ───────────────────────────────────

/// Map a caught error into the retry/poison/re-drive decision. 40P01 (deadlock)
/// and 40001 (serialization) are transient → Retryable; 23505 (UNIQUE survived
/// dedup) → DuplicateRace (drop the racer, re-drive the rest); everything else,
/// including a Rust-level SPI error with no caught SQLSTATE, is Fatal. PoC
/// posture: this is the basic resilience measure (§6.8 final para); finer
/// per-SQLSTATE handling is production hardening.
fn classify_phase_error(
    caught: Option<PgSqlErrorCode>,
    spi_err: Option<pgrx::spi::Error>,
) -> PhaseOutcome {
    match caught {
        Some(PgSqlErrorCode::ERRCODE_T_R_DEADLOCK_DETECTED)
        | Some(PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE) => {
            PhaseOutcome::Retryable(format!("{:?}", caught.unwrap()))
        }
        Some(PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION) => {
            PhaseOutcome::DuplicateRace("23505 survived pre-flight dedup".to_string())
        }
        Some(code) => PhaseOutcome::Fatal(format!("non-retryable sql error {code:?}")),
        None => match spi_err {
            Some(e) => PhaseOutcome::Fatal(format!("spi: {e}")),
            None => PhaseOutcome::Fatal("write failed with no caught sqlstate".to_string()),
        },
    }
}

fn sqlstate_of(caught: &CaughtError) -> PgSqlErrorCode {
    match caught {
        CaughtError::PostgresError(e) | CaughtError::ErrorReport(e) => e.sql_error_code(),
        CaughtError::RustPanic { ereport, .. } => ereport.sql_error_code(),
    }
}

/// Exponential backoff for deadlock retries: 10 ms · 2^(attempt-1), capped at 1 s
/// (attempt is 1-based).
fn retry_backoff(attempt: u32) -> Duration {
    let ms = 10u64.saturating_mul(1u64 << (attempt.saturating_sub(1)).min(20));
    Duration::from_millis(ms.min(1000))
}

// ── Test-only injection sites (test_hooks builds only — both the call sites in
//    the commit phase and these bodies are #[cfg]'d out of production, acct-yojk.11;
//    only test_hooks SPIs ever arm them, §6.8 / §9.3) ───────────────────

/// Raise a synthetic 40P01 if the deadlock-injection counter is armed,
/// decrementing it once. The RAISE longjmps to the caller's catch_others.
#[cfg(feature = "test_hooks")]
fn maybe_inject_deadlock() {
    let cq = COMMITTER_QUEUE.share();
    // Claim one injection via CAS (saturating, never underflows below 0).
    let counter = &cq.test_inject_committer_deadlock_count;
    let mut cur = counter.load(Acquire);
    loop {
        if cur == 0 {
            return;
        }
        match counter.compare_exchange_weak(cur, cur - 1, AcqRel, Acquire) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
    let _ = Spi::run(
        "DO $$ BEGIN RAISE EXCEPTION 'injected deadlock (test_hooks)' \
         USING ERRCODE = '40P01'; END $$",
    );
}

/// Raise a synthetic non-retryable error (one-shot) if armed.
#[cfg(feature = "test_hooks")]
fn maybe_inject_fatal() {
    if COMMITTER_QUEUE
        .share()
        .test_inject_committer_fatal
        .swap(0, AcqRel)
        == 1
    {
        let _ = Spi::run(
            "DO $$ BEGIN RAISE EXCEPTION 'injected fatal (test_hooks)' \
             USING ERRCODE = '23514'; END $$",
        );
    }
}

/// Raise a synthetic raw 23505 (one-shot) with no real duplicate behind it, to
/// exercise the re-drive safety valve: a UNIQUE violation whose offender is not
/// resolvable in `trx` must poison (not loop). Production builds never arm it.
#[cfg(feature = "test_hooks")]
fn maybe_inject_unique() {
    if COMMITTER_QUEUE
        .share()
        .test_inject_committer_unique
        .swap(0, AcqRel)
        == 1
    {
        let _ = Spi::run(
            "DO $$ BEGIN RAISE EXCEPTION 'injected unique (test_hooks)' \
             USING ERRCODE = '23505'; END $$",
        );
    }
}

/// Sleep for the injected stall (µs), holding pool_locks + the open subtx, so a
/// recovery test has a window to act on the committer mid-flight (§9.3).
#[cfg(feature = "test_hooks")]
fn maybe_stall() {
    let stall_us = COMMITTER_QUEUE
        .share()
        .test_inject_committer_stall_us
        .load(Acquire);
    if stall_us > 0 {
        COMMITTER_QUEUE
            .share()
            .test_committer_stall_hits
            .fetch_add(1, Release);
        std::thread::sleep(Duration::from_micros(stall_us as u64));
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
    let existing = existing_trx_keys(trx_types, source_ids)?;

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

/// Execute a kept (`SPI_keepplan`) read-only prepared statement, preparing +
/// keeping it on first use for this backend. The read-path twin of
/// `bulk_write::run_prepared` (acct-sczx Lever B): `prepare` / `select`
/// (read_only = true) instead of `prepare_mut` / `update`. Args are built by the
/// caller in the outer memory context (same as bulk_write) and `read_rows`
/// consumes the result table.
fn run_prepared_read<R>(
    slot: &'static std::thread::LocalKey<RefCell<Option<OwnedPreparedStatement>>>,
    sql: &str,
    arg_oids: &[PgOid],
    args: &[DatumWithOid<'_>],
    read_rows: impl FnOnce(SpiTupleTable<'_>) -> Result<R, pgrx::spi::Error>,
) -> Result<R, pgrx::spi::Error> {
    Spi::connect(|client| {
        slot.with_borrow_mut(|opt| -> Result<(), pgrx::spi::Error> {
            if opt.is_none() {
                *opt = Some(client.prepare(sql, arg_oids)?.keep());
            }
            Ok(())
        })?;
        slot.with_borrow(|opt| {
            let plan = opt.as_ref().expect("plan prepared above");
            let table = client.select(plan, None, args)?;
            read_rows(table)
        })
    })
}

/// Populate the per-backend `trx_type` label cache on first use, then return the
/// (trx_type, source_id) pairs whose trx_type is a known enum label as aligned
/// arrays. Unknown labels are dropped here, in Rust, so the dedup probe's enum
/// cast only ever sees valid input (see `DEDUP_TRX_TYPE_LABELS`).
fn filter_to_known_trx_types(
    trx_types: Vec<String>,
    source_ids: Vec<i64>,
) -> Result<(Vec<String>, Vec<i64>), String> {
    if DEDUP_TRX_TYPE_LABELS.with_borrow(|o| o.is_none()) {
        let labels = Spi::connect(|client| -> Result<HashSet<String>, pgrx::spi::Error> {
            let mut set = HashSet::new();
            let mut t = client.select("SELECT unnest(enum_range(NULL::trx_type))::text", None, &[])?;
            while let Some(row) = t.next() {
                if let Some(s) = row.get::<String>(1)? {
                    set.insert(s);
                }
            }
            Ok(set)
        })
        .map_err(|e| format!("trx_type label cache: {e}"))?;
        DEDUP_TRX_TYPE_LABELS.with_borrow_mut(|o| *o = Some(labels));
    }
    Ok(DEDUP_TRX_TYPE_LABELS.with_borrow(|o| {
        let labels = o.as_ref().expect("labels populated above");
        let mut tt = Vec::with_capacity(trx_types.len());
        let mut sid = Vec::with_capacity(source_ids.len());
        for (t, s) in trx_types.into_iter().zip(source_ids) {
            if labels.contains(&t) {
                tt.push(t);
                sid.push(s);
            }
        }
        (tt, sid)
    }))
}

/// Query `trx` for which of the given (trx_type, source_id) keys already exist.
/// Keys are pre-filtered in Rust to valid `trx_type` labels, then compared
/// enum-to-enum (`u.tt::trx_type`) so the `(trx_type, source_id)` unique index
/// drives a 2-column seek (acct-e95d) instead of a `source_id`-only scan. The
/// Rust pre-filter (not a SQL `enum_range` filter, which is STABLE and would be
/// re-evaluated per probe row) keeps the cast unreachable for unknown input so it
/// can't raise 22P02 outside the write subtx's catch. An unknown trx_type can't
/// match an existing row anyway, so dropping it is output-preserving; it surfaces
/// later at `insert_trx`'s `$1::text::trx_type` cast, exactly as before. Shared by
/// pre-flight dedup and the re-drive re-dedup. Runs on a kept plan: the shape is
/// fixed and only its `text[]` / `bigint[]` params vary, so parse+plan is once
/// per backend.
fn existing_trx_keys(
    trx_types: Vec<String>,
    source_ids: Vec<i64>,
) -> Result<HashSet<(String, i64)>, String> {
    let (tt, sid) = filter_to_known_trx_types(trx_types, source_ids)?;
    if tt.is_empty() {
        return Ok(HashSet::new());
    }
    let args: [DatumWithOid<'_>; 2] = [tt.into(), sid.into()];
    run_prepared_read(
        &PLAN_DEDUP_TRX,
        "SELECT trx.trx_type::text, trx.source_id \
           FROM trx \
           JOIN UNNEST($1::text[], $2::bigint[]) AS u(tt, sid) \
             ON trx.trx_type = u.tt::trx_type AND trx.source_id = u.sid",
        &pgrx::oids_of![Vec<String>, Vec<i64>],
        &args,
        |mut t| {
            let mut set = HashSet::new();
            while let Some(row) = t.next() {
                let tt: String = row.get::<String>(1)?.unwrap_or_default();
                let sid: i64 = row.get::<i64>(2)?.unwrap_or(0);
                set.insert((tt, sid));
            }
            Ok(set)
        },
    )
    .map_err(|e| format!("trx dedup query: {e}"))
}

/// Re-drive re-dedup (§6.8): after a 23505 that survived pre-flight dedup, a racing
/// committer has now committed one (or more) of this group's (trx_type, source_id)
/// keys. Re-query `trx` and drop every `prepared` submission whose key is now
/// present, leaving the innocent rest to be re-driven. Returns how many were
/// removed — 0 means the UNIQUE fired with no resolvable duplicate, so the caller
/// poisons (re-driving can't make progress).
fn drop_prepared_already_in_trx(prepared: &mut Vec<Prepared>) -> Result<u64, String> {
    if prepared.is_empty() {
        return Ok(0);
    }
    let trx_types: Vec<String> = prepared.iter().map(|p| p.trx_type.clone()).collect();
    let source_ids: Vec<i64> = prepared.iter().map(|p| p.source_id).collect();
    let existing = existing_trx_keys(trx_types, source_ids)?;
    let before = prepared.len();
    prepared.retain(|p| !existing.contains(&(p.trx_type.clone(), p.source_id)));
    Ok((before - prepared.len()) as u64)
}

// ── Caller user-tx classification + eject ───────────────────────────

/// Batched pg_xact_status lookup over the unique XIDs, then per-submission
/// classification. In-progress callers are ejected (eject_count +
/// last_eject_at_ns bumped, cg_id reset, staging CAS 3→1 so the router re-packs
/// after cooldown); aborted callers are dropped silently; a NULL status (frozen
/// xid = committed) is kept; an unrecognized non-null status (pg_xact_status
/// wording drift) is logged and ejected, never kept. Returns the kept set.
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
            if status == CallerTxStatus::Unrecognized {
                // A pg_xact_status wording drift: surface it loudly. The submission
                // is treated as not-yet-committed below (never kept), so this fails
                // safe — it never commits work for a possibly still-open caller tx.
                pgrx::warning!(
                    "ledger_routed_c: unrecognized pg_xact_status {s:?} for caller xid {xid}; \
                     treating as not-yet-committed (will not keep)"
                );
            }
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
            .map(status_copy)
            .unwrap_or(CallerTxStatus::Unknown);
        match status {
            CallerTxStatus::Committed | CallerTxStatus::Unknown => kept.push(decoded_clone(d)),
            CallerTxStatus::Aborted => {
                // Drop silently — no trx will exist, which is the failure signal.
            }
            // InProgress: caller's tx is still open — re-check after cooldown.
            // Unrecognized: a status-wording drift; we can't confirm commit, so it
            // rides the same not-yet-committed path (eject → retry → terminal-drop),
            // never kept. A persistent drift thus fails loud (WARNING + timeouts).
            CallerTxStatus::InProgress | CallerTxStatus::Unrecognized => {
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
        // NULL = frozen/too-old xid (committed long ago) → safe to keep. A non-null
        // string we don't know = a pg_xact_status wording drift → must NOT be kept.
        None => CallerTxStatus::Unknown,
        Some(_) => CallerTxStatus::Unrecognized,
    }
}

fn status_copy(s: &CallerTxStatus) -> CallerTxStatus {
    match s {
        CallerTxStatus::Committed => CallerTxStatus::Committed,
        CallerTxStatus::InProgress => CallerTxStatus::InProgress,
        CallerTxStatus::Aborted => CallerTxStatus::Aborted,
        CallerTxStatus::Unknown => CallerTxStatus::Unknown,
        CallerTxStatus::Unrecognized => CallerTxStatus::Unrecognized,
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
        });
    }
    Ok(out)
}

// ── In-process apply-path microbench (bench_hooks builds only; acct-q6sx) ──
//
// Measures the committer's single-core *apply* ceiling — `plan_and_write` (plan +
// batched INSERT of trx / trx_line / posting_line + the per-pool aggregate
// UPSERT) — with NO ingress: no staging ring, no router, no pool_lock, no hydrate
// in the timed region. That is precisely the ceiling the end-to-end harness cannot
// reach, because the staging-ring LWLock starves committers as caller count rises
// (acct-ruex). It is the in-process, Docker-native counterpart to a pgrx
// `#[pg_bench]`, which would require a separate pgrx-managed cluster plus a
// hand-injected base schema (the apply path INSERTs into migration-created tables);
// see acct-q6sx.
//
// Each iteration applies a `p_batch`-submission po_receipt commit_group into the
// pre-seeded pool `p_pool_id` inside an internal subtransaction, times ONLY
// plan_and_write (the snapshot clone and the subtx begin/rollback are bench
// artifacts, excluded from the sample), then ROLLS THE SUBTX BACK so the pool's
// base state is byte-identical for every iteration — every sample measures the
// same work. `p_warmup` iterations are discarded first to warm bulk_write's
// per-backend prepared-plan cache (kept plans survive subtx rollback).
//
// Returns a JSONB summary: per-iteration ns stats (min/p50/mean/p99/max/stddev),
// `us_per_trx` (mean / batch), and `committed_per_iter` (a sanity signal — it must
// equal `batch`; 0 means the pool isn't seeded, e.g. no posting_account_map). The
// `us_per_trx` at a large batch is directly comparable to the span-measured apply
// ~44 us/trx from measure-apply-spans.sh.
#[cfg(feature = "bench_hooks")]
#[pg_extern]
fn ledger_routed_c_bench_apply(
    p_pool_id: i64,
    p_batch: i32,
    p_iters: i32,
    p_warmup: i32,
) -> pgrx::JsonB {
    use std::time::Instant;

    let batch = p_batch.max(1) as usize;
    let iters = p_iters.max(1) as usize;
    let warmup = p_warmup.max(0) as usize;

    // Base snapshot read once; every iteration rolls back, so the base never moves.
    let base = match hydration::hydrate_snapshot(&[p_pool_id]) {
        Ok(s) => s,
        Err(e) => return pgrx::JsonB(serde_json::json!({ "error": format!("hydrate: {e}") })),
    };

    // A `batch`-submission commit_group of po_receipts into the pool. Distinct
    // source_ids satisfy the trx (trx_type, source_id) UNIQUE within one
    // plan_and_write; across iterations the rollback discards them, so the same ids
    // are safely reused. posted_at is fixed — apply cost is timestamp-independent.
    let posted_at = DateTime::<Utc>::from_timestamp_micros(0).expect("epoch is a valid timestamp");
    let prepared: Vec<Prepared> = (0..batch as i64)
        .map(|i| Prepared {
            trx_type: "po_receipt".to_string(),
            source_id: 1_000_000_000 + i,
            posted_at,
            lines: vec![TrxLineRequest {
                pool_id: p_pool_id,
                line_type: ledger_core::LineType::PoReceiptLine,
                source_id: Some(1_000_000_000 + i),
                qty: 10,
                unit_cost: 50,
            }],
        })
        .collect();

    // One iteration: time plan_and_write inside a rolled-back subtx. Returns the
    // elapsed ns + committed count, or an error string. The memory-context /
    // resource-owner save+restore around the subtx mirrors pgrx-bench's runtime so a
    // caught apply ERROR cannot leave the backend on a freed context.
    let run_once = |prepared: &[Prepared]| -> Result<(u64, usize), String> {
        let snap = base.clone(); // argument prep — NOT timed
        let sp = CString::new("rc_bench_apply").expect("savepoint name has no NUL");
        unsafe {
            let old_ctx = pg_sys::CurrentMemoryContext;
            let old_ro = pg_sys::CurrentResourceOwner;
            pg_sys::BeginInternalSubTransaction(sp.as_ptr());
            let outcome: Result<(u64, usize), String> = PgTryBuilder::new(AssertUnwindSafe(|| {
                let t0 = Instant::now();
                let summary = plan_and_write(snap, prepared).map_err(|e| e.to_string())?;
                let dt = t0.elapsed().as_nanos() as u64;
                Ok((dt, summary.committed))
            }))
            .catch_others(|caught| Err(format!("apply raised: {:?}", sqlstate_of(&caught))))
            .execute();
            // Always roll back: the base pool state must be unchanged for the next
            // iteration, so we never release/commit the savepoint.
            pg_sys::MemoryContextSwitchTo(old_ctx);
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_ctx);
            pg_sys::CurrentResourceOwner = old_ro;
            outcome
        }
    };

    // Warmup (discarded) — the first call builds bulk_write's prepared plans.
    for _ in 0..warmup {
        if let Err(e) = run_once(&prepared) {
            return pgrx::JsonB(serde_json::json!({ "error": format!("warmup: {e}") }));
        }
    }

    let mut samples: Vec<u64> = Vec::with_capacity(iters);
    let mut committed_seen: usize = 0;
    for _ in 0..iters {
        match run_once(&prepared) {
            Ok((ns, committed)) => {
                samples.push(ns);
                committed_seen = committed;
            }
            Err(e) => return pgrx::JsonB(serde_json::json!({ "error": format!("sample: {e}") })),
        }
    }

    samples.sort_unstable();
    let n = samples.len() as f64;
    let sum: u64 = samples.iter().sum();
    let mean = sum as f64 / n;
    let stddev = (samples.iter().map(|&s| (s as f64 - mean).powi(2)).sum::<f64>() / n).sqrt();
    let pct = |q: f64| -> u64 {
        let idx = ((q * (samples.len() as f64 - 1.0)).round() as usize).min(samples.len() - 1);
        samples[idx]
    };

    pgrx::JsonB(serde_json::json!({
        "pool_id": p_pool_id,
        "batch": batch,
        "iters": iters,
        "warmup": warmup,
        "committed_per_iter": committed_seen,
        "iter_ns": {
            "min": samples[0],
            "p50": pct(0.50),
            "mean": mean,
            "p99": pct(0.99),
            "max": samples[samples.len() - 1],
            "stddev": stddev,
        },
        "us_per_iter_mean": mean / 1000.0,
        "us_per_trx": mean / 1000.0 / batch as f64,
    }))
}

// ── Unit tests for pure helpers ─────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_core::LineType;

    #[test]
    fn decode_lines_maps_line_type_and_fields() {
        let lines = vec![
            PocV3Line {
                line_type: "po_receipt_line".into(),
                source_id: Some(42),
                pool_id: 7,
                qty: 10,
                unit_cost: 50,
            },
            PocV3Line {
                line_type: "inv_adjustment_line".into(),
                source_id: None,
                pool_id: 8,
                qty: -3,
                unit_cost: 100,
            },
        ];
        let req = decode_lines(&lines).expect("decode ok");
        assert_eq!(req.len(), 2);
        assert_eq!(req[0].pool_id, 7);
        assert_eq!(req[0].line_type, LineType::PoReceiptLine);
        assert_eq!(req[0].source_id, Some(42));
        assert_eq!(req[1].qty, -3);
        assert_eq!(req[1].line_type, LineType::InvAdjustmentLine);
    }

    #[test]
    fn decode_lines_unknown_type_returns_err() {
        let lines = vec![PocV3Line {
            line_type: "not_a_type".into(),
            source_id: None,
            pool_id: 1,
            qty: 1,
            unit_cost: 1,
        }];
        assert!(decode_lines(&lines).is_err());
    }

    #[test]
    fn parse_caller_status_maps_all_known_strings() {
        assert_eq!(parse_caller_status(Some("committed")), CallerTxStatus::Committed);
        assert_eq!(parse_caller_status(Some("aborted")), CallerTxStatus::Aborted);
        assert_eq!(parse_caller_status(Some("in progress")), CallerTxStatus::InProgress);
        // NULL = frozen/too-old xid → committed → keep.
        assert_eq!(parse_caller_status(None), CallerTxStatus::Unknown);
    }

    #[test]
    fn parse_caller_status_unrecognized_is_distinct_from_null() {
        // A non-null string we don't know (a pg_xact_status wording drift) must NOT
        // collapse to Unknown-keep — it maps to Unrecognized (handled as
        // not-yet-committed, never kept), so a future rename fails safe.
        assert_eq!(parse_caller_status(Some("garbage")), CallerTxStatus::Unrecognized);
        assert_eq!(parse_caller_status(Some("in-progress")), CallerTxStatus::Unrecognized);
        assert_ne!(parse_caller_status(Some("running")), CallerTxStatus::Unknown);
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
