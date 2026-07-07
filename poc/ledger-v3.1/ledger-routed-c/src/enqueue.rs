//! Routed SPI entry point: `ledger_enqueue_trx_c` (design-v3.1 §6.1, §6.4 step 0).
//!
//! Caller invokes inside their own user-tx. The function:
//!
//!   1. Parses `lines` JSONB into a `Vec<PocV3Line>`.
//!   2. Parses `posted_at` RFC3339 → micros since Unix epoch.
//!   3. Computes touched pool_ids as a sorted, deduplicated `BTreeSet`.
//!   4. Forces user-tx XID allocation — the committer's eject loop
//!      reads `pg_xact_status` on this XID to detect caller
//!      in_progress / aborted / committed; without an allocated XID it
//!      has nothing to read.
//!   5. Under `SPILLOVER_ARENA.exclusive()`, calls
//!      `payload::encode_submission` (two arena allocs: lines blob +
//!      submission blob) and a third arena alloc for the pool-keys
//!      block (`pool_count * 8` bytes, little-endian).
//!   6. Under `STAGING_QUEUE.exclusive()`, scans `entries[]` starting
//!      from `tail` for `valid == 0`, stamps the slot fields, then
//!      CAS-publishes `valid` 0→1 with Release; bumps tail; returns
//!      the slot's `request_seq` (consumed once a free slot is found,
//!      so QueueFull / arena-full retries do not waste seqs).
//!   7. On arena exhaustion or QueueFull, frees any partial allocs
//!      and CV-waits up to `queue_full_timeout_ms`. Re-tries on wake.
//!      Raises `ERRCODE_INSUFFICIENT_RESOURCES` on deadline.
//!
//! Returns the shmem-local `request_seq` (i64) — the locked plan
//! treats this as the v3 submission_id (spec verbatim; internal use,
//! not durable). The committer assigns the durable `trx.id` at COMMIT.

use std::collections::BTreeSet;
use std::sync::atomic::Ordering::{Relaxed, Release};

use chrono::DateTime;
use pgrx::pg_sys;
use pgrx::prelude::*;

use crate::payload::{self, PocV3Line, PocV3Submission};
use crate::shmem::{
    LEDGER_V3_STAGING_QUEUE_SIZE, SPILLOVER_ARENA, STAGING_QUEUE, SpilloverArena, StagingQueue,
    backpressure_cv_ptr, now_us,
};

/// Caller-facing SPI: enqueue a submission for asynchronous commit.
///
/// `trx_type` is the SQL `trx_type` enum text (po_receipt, wo_completion, ...).
/// `posted_at` is RFC3339 text (e.g. `'2026-05-21T12:00:00+00:00'`).
/// `lines` is a JSONB array of `{line_type, source_id?, pool_id, qty,
/// unit_cost}` objects. Posting accounts are resolved ledger-side from
/// posting_account_map (§3.7), not supplied per line.
///
/// Returns the shmem `request_seq` of the published staging slot.
#[pg_extern]
fn ledger_enqueue_trx_c(
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines: pgrx::JsonB,
) -> i64 {
    let posted_at_micros: i64 = match DateTime::parse_from_rfc3339(posted_at) {
        Ok(t) => t.timestamp_micros(),
        Err(e) => ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT,
            format!("ledger_enqueue_trx_c: posted_at not RFC3339: {e}"),
            "Use ISO 8601 with offset, e.g. '2026-05-21T12:00:00+00:00'.",
        ),
    };

    let line_vec: Vec<PocV3Line> = match serde_json::from_value(lines.0) {
        Ok(v) => v,
        Err(e) => ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("ledger_enqueue_trx_c: lines JSONB decode failed: {e}"),
            "Expected an array of {line_type, source_id?, pool_id, qty, unit_cost}.",
        ),
    };
    if line_vec.is_empty() {
        ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "ledger_enqueue_trx_c: lines must be non-empty".to_string(),
            "Submissions with zero lines are no-ops and are rejected.",
        );
    }

    let pool_ids: Vec<i64> = pack_pool_ids(&line_vec);
    let pool_count: u16 = match u16::try_from(pool_ids.len()) {
        Ok(n) => n,
        Err(_) => ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            format!(
                "ledger_enqueue_trx_c: too many distinct pools touched ({} > {})",
                pool_ids.len(),
                u16::MAX
            ),
            "Split into multiple submissions or reduce per-submission pool fan-out.",
        ),
    };

    // Full (epoch-qualified) xid: classify_and_eject feeds this to
    // pg_xact_status(x::xid8), which needs a 64-bit FullTransactionId. A bare
    // 32-bit TransactionId widened to u64 carries epoch 0, so once the xid epoch
    // advances the staged value resolves to a too-old xid → pg_xact_status NULL →
    // CallerTxStatus::Unknown → wrongly kept, committing in-progress caller work
    // (acct-mvq4.34). Same assign-if-none semantics as GetCurrentTransactionId.
    let user_tx_xid: u64 = unsafe { pg_sys::GetCurrentFullTransactionId() }.value;
    let trx_type_id = trx_type_to_id(trx_type);

    let mut sub_shell = PocV3Submission {
        trx_type: trx_type.to_string(),
        source_id,
        posted_at_micros,
        line_count: 0,
        line_offset: 0,
    };

    let timeout_ms = crate::queue_full_timeout_ms_now();
    let deadline_micros: i128 = (now_us() as i128) + (timeout_ms as i128) * 1000;
    let cv = backpressure_cv_ptr();
    unsafe { pg_sys::ConditionVariablePrepareToSleep(cv) };

    let request_seq = loop {
        unsafe { pg_sys::ProcessInterrupts() };

        let alloc_outcome: AllocOutcome = {
            let mut arena_guard = SPILLOVER_ARENA.exclusive();
            try_alloc_blocks(&mut arena_guard, &mut sub_shell, &line_vec, &pool_ids)
        };

        let (payload_offset, payload_length, pool_keys_offset) = match alloc_outcome {
            AllocOutcome::Ok(t) => t,
            AllocOutcome::ArenaFull => {
                if past_deadline(deadline_micros) {
                    unsafe { pg_sys::ConditionVariableCancelSleep() };
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!(
                            "ledger_enqueue_trx_c: spillover arena exhausted and backpressure timeout elapsed ({timeout_ms}ms)"
                        )
                    );
                }
                cv_wait_for_slot(cv, deadline_micros);
                continue;
            }
            AllocOutcome::Internal(detail) => {
                unsafe { pg_sys::ConditionVariableCancelSleep() };
                ereport_internal(format!(
                    "ledger_enqueue_trx_c: payload encode failed: {detail}"
                ));
            }
        };

        let push_result = {
            let mut queue_guard = STAGING_QUEUE.exclusive();
            push_entry_into_queue(
                &mut queue_guard,
                payload_offset,
                payload_length,
                sub_shell.line_offset,
                pool_count,
                pool_keys_offset,
                trx_type_id,
                user_tx_xid,
            )
        };

        match push_result {
            Ok((_slot_index, seq)) => break seq,
            Err(PushError::QueueFull) => {
                {
                    let mut arena_guard = SPILLOVER_ARENA.exclusive();
                    payload::free_submission(
                        &mut arena_guard,
                        payload_offset,
                        sub_shell.line_offset,
                    );
                    if pool_keys_offset != 0 {
                        arena_guard.free(pool_keys_offset);
                    }
                }
                if past_deadline(deadline_micros) {
                    unsafe { pg_sys::ConditionVariableCancelSleep() };
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!(
                            "ledger_enqueue_trx_c: staging queue full and backpressure timeout elapsed ({timeout_ms}ms)"
                        )
                    );
                }
                cv_wait_for_slot(cv, deadline_micros);
                continue;
            }
        }
    };

    unsafe { pg_sys::ConditionVariableCancelSleep() };
    request_seq as i64
}

/// Diagnostic batch enqueue (acct-ruex): publish N submissions under ONE
/// `SPILLOVER_ARENA.exclusive()` (alloc all) followed by ONE
/// `STAGING_QUEUE.exclusive()` (push all), so the per-trx ingress-lock
/// HANDOFFS — where the LWLock contention actually lives — are amortized ~Nx.
/// With handoffs removed, the residual routed throughput is the pure
/// downstream drain ceiling (router grouping + committer commit + WAL); the
/// committer sampler then names whatever that ceiling is.
///
/// `trxs` is a JSONB array of `{trx_type, source_id, posted_at, lines:[...]}`;
/// returns the count of submissions published.
///
/// Deliberately self-contained — it shares no code with the hot single-push
/// `ledger_enqueue_trx_c`, so the production path carries zero risk from this
/// probe. Staging push is BACKPRESSURED for the saturation use-case: each lock
/// acquisition pushes as many entries as currently fit, then CV-waits for the
/// committers to free slots and pushes the rest, honoring
/// `queue_full_timeout_ms` as a per-call deadline. The arena allocs stay live
/// across waits (no free/realloc thrash), so a half-ring batch from several
/// callers keeps the ring saturated without error churn. Arena alloc itself is
/// all-or-nothing (size `spillover_arena_mb` above the in-flight payload set);
/// on `ArenaFull` or a backpressure timeout it frees this batch's
/// not-yet-published allocs and raises `INSUFFICIENT_RESOURCES` (already-pushed
/// slots ride this user-tx; if it aborts, the committer's eject loop reclaims
/// them, same as an aborted single-push caller).
#[pg_extern]
fn ledger_enqueue_trx_batch_c(trxs: pgrx::JsonB) -> i64 {
    #[derive(serde::Deserialize)]
    struct TrxEnvelope {
        trx_type: String,
        source_id: i64,
        posted_at: String,
        lines: Vec<PocV3Line>,
    }

    let envelopes: Vec<TrxEnvelope> = match serde_json::from_value(trxs.0) {
        Ok(v) => v,
        Err(e) => ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("ledger_enqueue_trx_batch_c: trxs JSONB decode failed: {e}"),
            "Expected a JSON array of {trx_type, source_id, posted_at, lines:[...]}.",
        ),
    };
    if envelopes.is_empty() {
        ereport_invalid_arg(
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "ledger_enqueue_trx_batch_c: trxs must be non-empty".to_string(),
            "Empty batches are no-ops and are rejected.",
        );
    }

    // Full (epoch-qualified) xid: classify_and_eject feeds this to
    // pg_xact_status(x::xid8), which needs a 64-bit FullTransactionId. A bare
    // 32-bit TransactionId widened to u64 carries epoch 0, so once the xid epoch
    // advances the staged value resolves to a too-old xid → pg_xact_status NULL →
    // CallerTxStatus::Unknown → wrongly kept, committing in-progress caller work
    // (acct-mvq4.34). Same assign-if-none semantics as GetCurrentTransactionId.
    let user_tx_xid: u64 = unsafe { pg_sys::GetCurrentFullTransactionId() }.value;

    // Per-trx prep — parse + pool-id computation, no lock held.
    struct Prepared {
        sub_shell: PocV3Submission,
        line_vec: Vec<PocV3Line>,
        pool_ids: Vec<i64>,
        pool_count: u16,
        trx_type_id: u16,
    }
    let mut prepared: Vec<Prepared> = Vec::with_capacity(envelopes.len());
    for env in envelopes {
        let posted_at_micros: i64 = match DateTime::parse_from_rfc3339(&env.posted_at) {
            Ok(t) => t.timestamp_micros(),
            Err(e) => ereport_invalid_arg(
                PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT,
                format!("ledger_enqueue_trx_batch_c: posted_at not RFC3339: {e}"),
                "Use ISO 8601 with offset, e.g. '2026-05-21T12:00:00+00:00'.",
            ),
        };
        if env.lines.is_empty() {
            ereport_invalid_arg(
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                "ledger_enqueue_trx_batch_c: each trx's lines must be non-empty".to_string(),
                "Submissions with zero lines are no-ops and are rejected.",
            );
        }
        let pool_ids: Vec<i64> = pack_pool_ids(&env.lines);
        let pool_count: u16 = match u16::try_from(pool_ids.len()) {
            Ok(n) => n,
            Err(_) => ereport_invalid_arg(
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                format!(
                    "ledger_enqueue_trx_batch_c: too many distinct pools touched ({} > {})",
                    pool_ids.len(),
                    u16::MAX
                ),
                "Split into multiple submissions or reduce per-submission pool fan-out.",
            ),
        };
        let sub_shell = PocV3Submission {
            trx_type: env.trx_type.clone(),
            source_id: env.source_id,
            posted_at_micros,
            line_count: 0,
            line_offset: 0,
        };
        let trx_type_id = trx_type_to_id(&env.trx_type);
        prepared.push(Prepared {
            sub_shell,
            line_vec: env.lines,
            pool_ids,
            pool_count,
            trx_type_id,
        });
    }

    // One arena-lock acquisition: alloc payload + pool-keys blocks for all N.
    struct Alloced {
        payload_offset: u32,
        payload_length: u32,
        line_offset: u32,
        pool_keys_offset: u32,
        pool_count: u16,
        trx_type_id: u16,
    }
    let free_alloced = |arena: &mut SpilloverArena, items: &[Alloced]| {
        for a in items {
            payload::free_submission(arena, a.payload_offset, a.line_offset);
            if a.pool_keys_offset != 0 {
                arena.free(a.pool_keys_offset);
            }
        }
    };
    let alloced: Vec<Alloced> = {
        let mut arena_guard = SPILLOVER_ARENA.exclusive();
        let mut out: Vec<Alloced> = Vec::with_capacity(prepared.len());
        for p in prepared.iter_mut() {
            match try_alloc_blocks(&mut arena_guard, &mut p.sub_shell, &p.line_vec, &p.pool_ids) {
                AllocOutcome::Ok((payload_offset, payload_length, pool_keys_offset)) => {
                    out.push(Alloced {
                        payload_offset,
                        payload_length,
                        line_offset: p.sub_shell.line_offset,
                        pool_keys_offset,
                        pool_count: p.pool_count,
                        trx_type_id: p.trx_type_id,
                    });
                }
                AllocOutcome::ArenaFull => {
                    free_alloced(&mut arena_guard, &out);
                    drop(arena_guard);
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!(
                            "ledger_enqueue_trx_batch_c: spillover arena exhausted at {}/{} of batch",
                            out.len(),
                            prepared.len()
                        )
                    );
                }
                AllocOutcome::Internal(detail) => {
                    free_alloced(&mut arena_guard, &out);
                    drop(arena_guard);
                    ereport_internal(format!(
                        "ledger_enqueue_trx_batch_c: payload encode failed: {detail}"
                    ));
                }
            }
        }
        out
    };

    // Backpressured staging push: each acquisition pushes as many as fit, then
    // waits for the committers to free slots and pushes the rest. Allocs stay
    // live across waits, so a saturated ring causes a wait — not error churn.
    let timeout_ms = crate::queue_full_timeout_ms_now();
    let deadline_micros: i128 = (now_us() as i128) + (timeout_ms as i128) * 1000;
    let cv = backpressure_cv_ptr();
    unsafe { pg_sys::ConditionVariablePrepareToSleep(cv) };

    let mut pushed: usize = 0;
    while pushed < alloced.len() {
        {
            let mut queue_guard = STAGING_QUEUE.exclusive();
            while pushed < alloced.len() {
                let a = &alloced[pushed];
                match push_entry_into_queue(
                    &mut queue_guard,
                    a.payload_offset,
                    a.payload_length,
                    a.line_offset,
                    a.pool_count,
                    a.pool_keys_offset,
                    a.trx_type_id,
                    user_tx_xid,
                ) {
                    Ok(_) => pushed += 1,
                    Err(PushError::QueueFull) => break,
                }
            }
        }
        if pushed >= alloced.len() {
            break;
        }
        if past_deadline(deadline_micros) {
            unsafe { pg_sys::ConditionVariableCancelSleep() };
            let mut arena_guard = SPILLOVER_ARENA.exclusive();
            free_alloced(&mut arena_guard, &alloced[pushed..]);
            drop(arena_guard);
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                format!(
                    "ledger_enqueue_trx_batch_c: staging queue full and backpressure timeout elapsed ({timeout_ms}ms) at {}/{} of batch",
                    pushed,
                    alloced.len()
                )
            );
        }
        cv_wait_for_slot(cv, deadline_micros);
    }
    unsafe { pg_sys::ConditionVariableCancelSleep() };

    pushed as i64
}

/// Per-state counts of `StagingEntry.valid`. Returns one row per state
/// label (empty / pending / processing / routed / abandoned) with its
/// current count. Walks `entries[]` under the LWLock-shared guard.
#[pg_extern]
fn ledger_routed_c_staging_state_counts()
-> TableIterator<'static, (name!(state, String), name!(count, i64))> {
    let queue = STAGING_QUEUE.share();
    let mut counts = [0i64; 5];
    for slot in queue.entries.iter() {
        let v = slot.valid.load(Relaxed) as usize;
        if v < counts.len() {
            counts[v] += 1;
        }
    }
    drop(queue);
    let states = ["empty", "pending", "processing", "routed", "abandoned"];
    let rows: Vec<(String, i64)> = states
        .iter()
        .enumerate()
        .map(|(i, s)| ((*s).to_string(), counts[i]))
        .collect();
    TableIterator::new(rows.into_iter())
}

/// High-water mark of `next_request_seq` — diagnostic only. Reads the
/// next-to-allocate value (not the last-allocated value); successive
/// successful enqueues increment this by 1.
#[pg_extern]
fn ledger_routed_c_staging_request_seq_max() -> i64 {
    STAGING_QUEUE.share().next_request_seq.load(Relaxed) as i64
}

// ── Internal helpers ────────────────────────────────────────────────

#[derive(Debug)]
enum AllocOutcome {
    Ok((u32, u32, u32)),
    ArenaFull,
    Internal(String),
}

#[derive(Debug)]
enum PushError {
    QueueFull,
}

fn try_alloc_blocks(
    arena: &mut SpilloverArena,
    sub: &mut PocV3Submission,
    line_vec: &[PocV3Line],
    pool_ids: &[i64],
) -> AllocOutcome {
    // acct-mvq4.37: drive the arena-exhaustion ERROR exits without a 128 MB fill.
    #[cfg(feature = "test_hooks")]
    if arena.test_force_arena_full.load(Relaxed) != 0 {
        return AllocOutcome::ArenaFull;
    }
    let (payload_offset, payload_length) = match payload::encode_submission(arena, sub, line_vec) {
        Ok(t) => t,
        Err(payload::PayloadError::ArenaAllocFailed { .. }) => return AllocOutcome::ArenaFull,
        Err(e) => return AllocOutcome::Internal(e.to_string()),
    };

    let pool_keys_offset = if !pool_ids.is_empty() {
        let bytes = (pool_ids.len() as u32) * 8;
        match arena.alloc(bytes) {
            Some(off) => {
                arena.write_bytes(off, &pool_ids_to_le_bytes(pool_ids));
                off
            }
            None => {
                payload::free_submission(arena, payload_offset, sub.line_offset);
                return AllocOutcome::ArenaFull;
            }
        }
    } else {
        0
    };

    AllocOutcome::Ok((payload_offset, payload_length, pool_keys_offset))
}

fn push_entry_into_queue(
    queue: &mut StagingQueue,
    payload_offset: u32,
    payload_length: u32,
    line_offset: u32,
    pool_count: u16,
    pool_keys_offset: u32,
    trx_type_id: u16,
    user_tx_xid: u64,
) -> Result<(u32, u64), PushError> {
    // acct-mvq4.37: drive the queue-full ERROR exits without filling the ring.
    // Checked before the free-slot scan so a forced failure consumes neither a
    // slot nor a request_seq — faithful to a real ring-full push.
    #[cfg(feature = "test_hooks")]
    {
        if queue.test_force_queue_full.load(Relaxed) != 0 {
            return Err(PushError::QueueFull);
        }
        // +1-encoded countdown (0 = disabled): allow N-1 pushes then fail the
        // rest. Single writer (the exclusive queue guard), so plain load/store.
        let cd = queue.test_queue_full_after.load(Relaxed);
        if cd != 0 {
            if cd == 1 {
                return Err(PushError::QueueFull);
            }
            queue.test_queue_full_after.store(cd - 1, Relaxed);
        }
    }
    let capacity = LEDGER_V3_STAGING_QUEUE_SIZE as u32;
    let tail = queue.tail.load(Relaxed);
    let backend_pid = unsafe { pg_sys::MyProcPid };
    let enqueued_at_micros = now_us();

    for i in 0..capacity {
        let idx = ((tail + i) % capacity) as usize;
        let slot = &mut queue.entries[idx];
        if slot.valid.load(Relaxed) != 0 {
            continue;
        }
        let request_seq = queue.next_request_seq.fetch_add(1, Relaxed);
        slot.request_seq = request_seq;
        slot.correlation_id = [0u8; 16];
        slot.user_tx_xid = user_tx_xid;
        slot.trx_type_id = trx_type_id;
        slot.payload_offset = payload_offset;
        slot.payload_length = payload_length;
        slot.line_offset = line_offset;
        slot.pool_count = pool_count;
        slot.pool_keys_offset = pool_keys_offset;
        slot.enqueued_at_micros = enqueued_at_micros;
        slot.backend_pid = backend_pid;
        slot.commit_group_id.store(0, Relaxed);
        slot.eject_count.store(0, Relaxed);

        match slot.valid.compare_exchange(0, 1, Release, Relaxed) {
            Ok(_) => {
                queue.tail.store((tail + i + 1) % capacity, Relaxed);
                return Ok((idx as u32, request_seq));
            }
            Err(_) => continue,
        }
    }
    Err(PushError::QueueFull)
}

fn pack_pool_ids(lines: &[PocV3Line]) -> Vec<i64> {
    let s: BTreeSet<i64> = lines.iter().map(|l| l.pool_id).collect();
    s.into_iter().collect()
}

fn pool_ids_to_le_bytes(pool_ids: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pool_ids.len() * 8);
    for id in pool_ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

fn trx_type_to_id(trx_type: &str) -> u16 {
    match trx_type {
        "po_receipt" => 1,
        "wo_completion" => 2,
        "inv_adjustment" => 3,
        "transfer_shipment" => 4,
        "transfer_receipt" => 5,
        "manual_adjustment" => 6,
        "revaluation_run" => 7,
        _ => 0,
    }
}

fn cv_wait_for_slot(cv: *mut pg_sys::ConditionVariable, deadline_micros: i128) -> bool {
    let remaining_micros = (deadline_micros - now_us() as i128).max(0) as u64;
    let ms = ((remaining_micros / 1000) as i64).clamp(1, 1000);
    unsafe {
        pg_sys::ConditionVariableTimedSleep(cv, ms as std::ffi::c_long, pg_sys::PG_WAIT_EXTENSION)
    }
}

fn past_deadline(deadline_micros: i128) -> bool {
    // acct-mvq4.37: report the deadline elapsed on demand so a forced-full wait
    // loop raises immediately instead of sleeping the 5 s timeout. Every call site
    // holds no LWLock, so this `.share()` introduces no nesting.
    #[cfg(feature = "test_hooks")]
    if STAGING_QUEUE.share().test_deadline_expired.load(Relaxed) != 0 {
        return true;
    }
    (now_us() as i128) >= deadline_micros
}

#[allow(unreachable_code)]
fn ereport_invalid_arg(code: PgSqlErrorCode, msg: String, hint: &str) -> ! {
    ereport!(ERROR, code, msg, hint);
    unreachable!()
}

#[allow(unreachable_code)]
fn ereport_internal(msg: String) -> ! {
    ereport!(ERROR, PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, msg);
    unreachable!()
}

// ── Test-only backpressure force-fail hooks (acct-mvq4.37) ──────────
//
// These arm the flags read behind `#[cfg(feature = "test_hooks")]` in
// try_alloc_blocks / push_entry_into_queue / past_deadline, so an acceptance
// test can reach enqueue's two never-executed ERROR exits (queue-full timeout,
// arena-full deadline -> ERRCODE_INSUFFICIENT_RESOURCES) with zero ring/arena
// fill and no multi-second wait. Compiled only under `test_hooks`; production
// builds carry neither the setters nor the reads. Stores are `Release` (paired
// with the `Relaxed` reads on the push path) via `.share()`, matching the other
// test-injection setters — the LWLock guards field existence, the atomic carries
// the value.

/// Force every staging push to report the ring full (`on = false` clears it).
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_force_queue_full(on: bool) {
    STAGING_QUEUE
        .share()
        .test_force_queue_full
        .store(u8::from(on), Release);
}

/// Allow `k` staging pushes to succeed, then report the ring full for the rest
/// (drives the batch PARTIAL-push remainder-free path). `k < 0` disables the
/// countdown. Stored +1-encoded so a zero-init field reads as disabled.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_queue_full_after(k: i32) {
    let v = if k < 0 { 0 } else { (k as u32).saturating_add(1) };
    STAGING_QUEUE
        .share()
        .test_queue_full_after
        .store(v, Release);
}

/// Force the backpressure deadline to read elapsed (`on = false` clears it) so a
/// wait loop raises immediately instead of sleeping the full timeout.
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_deadline_expired(on: bool) {
    STAGING_QUEUE
        .share()
        .test_deadline_expired
        .store(u8::from(on), Release);
}

/// Force `try_alloc_blocks` to report the arena exhausted (`on = false` clears it).
#[cfg(feature = "test_hooks")]
#[pg_extern]
fn ledger_routed_c_test_set_force_arena_full(on: bool) {
    SPILLOVER_ARENA
        .share()
        .test_force_arena_full
        .store(u8::from(on), Release);
}

// ── Unit tests for pure helpers ─────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn line(pool_id: i64) -> PocV3Line {
        PocV3Line {
            line_type: "po_receipt_line".into(),
            source_id: None,
            pool_id,
            qty: 1,
            unit_cost: 1,
        }
    }

    #[test]
    fn pack_pool_ids_dedups_and_sorts() {
        let lines = vec![line(5), line(2), line(5), line(7), line(2), line(1)];
        let got = pack_pool_ids(&lines);
        assert_eq!(got, vec![1, 2, 5, 7]);
    }

    #[test]
    fn pack_pool_ids_handles_single_pool() {
        let lines = vec![line(42), line(42), line(42)];
        let got = pack_pool_ids(&lines);
        assert_eq!(got, vec![42]);
    }

    #[test]
    fn pool_ids_to_le_bytes_matches_i64_to_le_bytes() {
        let ids = vec![0i64, 1, -1, i64::MAX, i64::MIN, 0x0123_4567_89AB_CDEFi64];
        let bytes = pool_ids_to_le_bytes(&ids);
        assert_eq!(bytes.len(), ids.len() * 8);
        for (i, id) in ids.iter().enumerate() {
            let chunk: [u8; 8] = bytes[i * 8..(i + 1) * 8].try_into().unwrap();
            assert_eq!(i64::from_le_bytes(chunk), *id, "mismatch at index {i}");
        }
    }

    #[test]
    fn pool_ids_to_le_bytes_empty_is_empty() {
        assert!(pool_ids_to_le_bytes(&[]).is_empty());
    }

    #[test]
    fn trx_type_to_id_covers_all_known_variants() {
        assert_eq!(trx_type_to_id("po_receipt"), 1);
        assert_eq!(trx_type_to_id("wo_completion"), 2);
        assert_eq!(trx_type_to_id("inv_adjustment"), 3);
        assert_eq!(trx_type_to_id("transfer_shipment"), 4);
        assert_eq!(trx_type_to_id("transfer_receipt"), 5);
        assert_eq!(trx_type_to_id("manual_adjustment"), 6);
        assert_eq!(trx_type_to_id("revaluation_run"), 7);
    }

    #[test]
    fn trx_type_to_id_unknown_is_zero() {
        assert_eq!(trx_type_to_id(""), 0);
        assert_eq!(trx_type_to_id("revaluation"), 0);
        assert_eq!(trx_type_to_id("PO_RECEIPT"), 0);
    }
}
