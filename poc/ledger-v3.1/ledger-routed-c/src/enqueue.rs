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
/// unit_cost, debit_account, credit_account}` objects.
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
            "Expected an array of {line_type, source_id?, pool_id, qty, unit_cost, debit_account, credit_account, variance_account?}.",
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

    let user_tx_xid: u64 = unsafe { pg_sys::GetCurrentTransactionId().into_inner() } as u64;
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
    pool_count: u16,
    pool_keys_offset: u32,
    trx_type_id: u16,
    user_tx_xid: u64,
) -> Result<(u32, u64), PushError> {
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
            debit_account: 1,
            credit_account: 2,
            variance_account: None,
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
