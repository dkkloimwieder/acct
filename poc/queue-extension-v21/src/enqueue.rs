//! `poc_v21_enqueue` SQL entry point + supporting helpers.
//!
//! M1.2 (acct-q3nm). Implements the caller-side path of spec §1.8:
//!   1. Validate envelope.
//!   2. Validate durable_queue request.
//!   3. Force user-tx XID allocation.
//!   4. Insert submission_status row (caller_intx default; other modes M5c.3).
//!   5. Allocate spillover-arena blocks for payload + pool_keys arrays.
//!   6. Push StagingEntry under the staging queue LWLock; CAS valid 0→1.
//!   7. CV-wait on full queue up to queue_full_timeout_ms.
//!
//! ## Pool keys JSONB shape
//!
//! Callers pass `pool_keys` as `{"sku":[[sku_id,location_id], ...], "wip":[[wo_id,op_id], ...]}`.
//! Each inner array is a tuple-as-array of two BIGINTs. Empty arrays are valid
//! (some events have no SKU pool keys; many have no WIP pool keys).

use crate::{SPILLOVER_ARENA, STAGING_QUEUE, StagingEntry, staging, status_insert_mode_str};
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::{Json, JsonB, Uuid};
use std::sync::atomic::Ordering::Relaxed;

/// Push an envelope onto the staging queue.
///
/// Returns void; on backpressure timeout raises ERRCODE_INSUFFICIENT_RESOURCES.
/// On durable_queue=true with persistent_staging=off raises
/// ERRCODE_FEATURE_NOT_SUPPORTED.
///
/// Status row INSERT mode is governed by `poc_v21.status_insert_mode`:
/// caller_intx (default) writes the row inside the caller's user-tx;
/// other modes deferred to M5c.3.
#[pg_extern]
fn poc_v21_enqueue(
    correlation_id: Uuid,
    event_type: &str,
    payload: JsonB,
    pool_keys: JsonB,
    durable_queue: default!(bool, false),
) {
    // 2. durable_queue gate (§1.7).
    if durable_queue && !crate::persistent_staging_enabled() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "poc_v21_enqueue: durable_queue=true requires poc_v21.persistent_staging=on",
            "Set poc_v21.persistent_staging=on in postgresql.conf (Postmaster scope, requires restart) before passing durable_queue=true."
        );
    }

    // 3. Force user-tx XID allocation. Allocates an XID if not yet
    // assigned. Always returns a valid XID; never raises.
    let user_tx_xid = unsafe { pg_sys::GetCurrentTransactionId().into_inner() };

    // Parse pool_keys JSONB once on the caller side to fail fast.
    let (sku_pool_keys, wip_pool_keys) = parse_pool_keys(&pool_keys);

    // 4. Status row INSERT — caller_intx mode (default).
    let mode = status_insert_mode_str();
    match mode.as_str() {
        "caller_intx" => {
            insert_status_row_caller_intx(correlation_id);
        }
        "caller_subtx" | "committer_lazy" => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!("poc_v21_enqueue: status_insert_mode={mode} not implemented at M1.2"),
                "M5c.3 (acct-nidw) lands caller_subtx + committer_lazy modes. Set poc_v21.status_insert_mode=caller_intx for now."
            );
        }
        other => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("poc_v21_enqueue: unknown status_insert_mode '{other}'")
            );
        }
    }

    // 5+6. Allocate arena blocks + push staging entry. Both happen
    // under the SPILLOVER_ARENA + STAGING_QUEUE locks respectively.
    // The CV-wait loop wraps the push so we can release the lock
    // between retries.
    let payload_bytes = serde_json::to_vec(&payload.0).expect("payload JSONB serializes");
    let payload_len: u32 = payload_bytes
        .len()
        .try_into()
        .expect("payload length fits in u32");

    // Each pool key is (i64, i64) = 16 bytes.
    let sku_keys_size: u32 = (sku_pool_keys.len() * 16) as u32;
    let wip_keys_size: u32 = (wip_pool_keys.len() * 16) as u32;

    // Try until we succeed in arena+ring, or until the timeout fires.
    let timeout_ms = crate::queue_full_timeout_ms_now();
    let deadline_micros = (now_micros() as i128) + (timeout_ms as i128) * 1000;
    let mut iter_count: u32 = 0;

    loop {
        // Check for caller interrupt (Ctrl-C, query_cancel) before sleeping.
        unsafe { pg_sys::ProcessInterrupts() };

        // 5. Allocate.
        let alloc_result = {
            let mut arena_guard = SPILLOVER_ARENA.exclusive();
            let arena = &mut *arena_guard;
            let payload_offset = arena.alloc(payload_len.max(1));
            if payload_offset.is_none() {
                None
            } else {
                let payload_offset = payload_offset.unwrap();
                arena.write_bytes(payload_offset, &payload_bytes);

                let sku_keys_offset = if sku_keys_size > 0 {
                    let off = arena.alloc(sku_keys_size);
                    match off {
                        Some(off) => {
                            let bytes = pool_keys_to_bytes(&sku_pool_keys);
                            arena.write_bytes(off, &bytes);
                            off
                        }
                        None => {
                            arena.free(payload_offset);
                            return_to_retry();
                            continue;
                        }
                    }
                } else {
                    0
                };

                let wip_keys_offset = if wip_keys_size > 0 {
                    let off = arena.alloc(wip_keys_size);
                    match off {
                        Some(off) => {
                            let bytes = pool_keys_to_bytes(&wip_pool_keys);
                            arena.write_bytes(off, &bytes);
                            off
                        }
                        None => {
                            arena.free(payload_offset);
                            if sku_keys_offset != 0 {
                                arena.free(sku_keys_offset);
                            }
                            return_to_retry();
                            continue;
                        }
                    }
                } else {
                    0
                };

                Some((payload_offset, sku_keys_offset, wip_keys_offset))
            }
        };

        let (payload_offset, sku_keys_offset, wip_keys_offset) = match alloc_result {
            Some(tuple) => tuple,
            None => {
                // Arena exhausted; treat like queue-full backpressure.
                if past_deadline(deadline_micros) {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!("poc_v21_enqueue: spillover arena exhausted and backpressure timeout elapsed ({timeout_ms}ms)")
                    );
                }
                sleep_until_retry(deadline_micros, &mut iter_count);
                continue;
            }
        };

        // 6. Push staging entry.
        let push_result = {
            let mut queue_guard = STAGING_QUEUE.exclusive();
            let queue = &mut *queue_guard;

            let request_seq = queue.next_request_seq.fetch_add(1, Relaxed);
            let entry = StagingEntry {
                valid: std::sync::atomic::AtomicU8::new(0),
                _pad: [0; 7],
                request_seq,
                correlation_id: *correlation_id.as_bytes(),
                user_tx_xid: user_tx_xid as u64,
                event_type_id: event_type_to_id(event_type),
                _pad_event: [0; 2],
                payload_offset,
                payload_length: payload_len,
                sku_pool_count: sku_pool_keys.len() as u16,
                wip_pool_count: wip_pool_keys.len() as u16,
                sku_pool_keys_offset: sku_keys_offset,
                wip_pool_keys_offset: wip_keys_offset,
                enqueued_at_micros: now_micros(),
                backend_pid: unsafe { pg_sys::MyProcPid },
                _pad_pid: [0; 4],
                superbatch_id: std::sync::atomic::AtomicU64::new(0),
                eject_count: std::sync::atomic::AtomicU16::new(0),
                _pad2: [0; 6],
            };
            staging::push_entry(queue, entry)
        };

        match push_result {
            Ok(_) => break,
            Err(staging::StagingPushError::QueueFull) => {
                // Return arena blocks; retry after sleep.
                let mut arena_guard = SPILLOVER_ARENA.exclusive();
                arena_guard.free(payload_offset);
                if sku_keys_offset != 0 {
                    arena_guard.free(sku_keys_offset);
                }
                if wip_keys_offset != 0 {
                    arena_guard.free(wip_keys_offset);
                }
                drop(arena_guard);

                if past_deadline(deadline_micros) {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!("poc_v21_enqueue: staging queue full and backpressure timeout elapsed ({timeout_ms}ms)")
                    );
                }
                sleep_until_retry(deadline_micros, &mut iter_count);
                continue;
            }
        }
    }

    // 7. (Router wake — SetLatch on the router BGWorker — lands at M3.1.)
    // For M1.2 the router is stubbed; setting the latch is a no-op.
}

fn return_to_retry() {
    // Inline; just here for readability of the matching arm above.
}

/// Pop a pending staging entry and mark valid=2. Returns the slot index
/// (or NULL if no pending entry). Test-only helper; mirrors what the
/// router (M3.1) would do for a size-1 SuperBatch.
#[pg_extern]
fn poc_v21_test_take_pending() -> Option<i64> {
    let mut queue_guard = STAGING_QUEUE.exclusive();
    staging::take_pending(&mut queue_guard).map(|idx| idx as i64)
}

/// Reset a slot to empty (valid=0) and free its arena blocks. Test-only;
/// mirrors what the committer (M1.3) would do at Step 14.
#[pg_extern]
fn poc_v21_test_release_slot(slot_index: i64) {
    let queue_guard = STAGING_QUEUE.share();
    let slot = &queue_guard.entries[slot_index as usize];
    let payload_offset = slot.payload_offset;
    let sku_keys_offset = slot.sku_pool_keys_offset;
    let wip_keys_offset = slot.wip_pool_keys_offset;
    drop(queue_guard);

    let mut arena_guard = SPILLOVER_ARENA.exclusive();
    if payload_offset != 0 {
        arena_guard.free(payload_offset);
    }
    if sku_keys_offset != 0 {
        arena_guard.free(sku_keys_offset);
    }
    if wip_keys_offset != 0 {
        arena_guard.free(wip_keys_offset);
    }
    drop(arena_guard);

    let mut queue_guard = STAGING_QUEUE.exclusive();
    staging::release_slot(&mut queue_guard, slot_index as u32);
}

/// Observability: counts of staging entries by state.
#[pg_extern]
fn poc_v21_staging_state_counts() -> Json {
    let queue_guard = STAGING_QUEUE.share();
    let (empty, pending, processing, routed, abandoned) = staging::state_counts(&queue_guard);
    Json(serde_json::json!({
        "empty": empty,
        "pending": pending,
        "processing": processing,
        "routed": routed,
        "abandoned": abandoned,
        "head": queue_guard.head.load(Relaxed),
        "tail": queue_guard.tail.load(Relaxed),
        "next_request_seq": queue_guard.next_request_seq.load(Relaxed),
    }))
}

/// Observability: spillover arena stats.
#[pg_extern]
fn poc_v21_arena_stats() -> Json {
    let arena_guard = SPILLOVER_ARENA.share();
    Json(serde_json::json!({
        "bump_offset": arena_guard.bump_offset.load(Relaxed),
        "freelist_head_offset": arena_guard.freelist_head_offset.load(Relaxed),
        "freelist_count": arena_guard.freelist_count(),
        "total_allocs": arena_guard.total_allocs.load(Relaxed),
        "total_frees": arena_guard.total_frees.load(Relaxed),
        "bytes_capacity": arena_guard.bytes.len(),
    }))
}

// ── helpers ─────────────────────────────────────────────────────────

fn insert_status_row_caller_intx(correlation_id: Uuid) {
    Spi::run_with_args(
        "INSERT INTO poc_v21_submission_status (correlation_id, state, enqueued_at) \
         VALUES ($1, 'queued', now()) \
         ON CONFLICT (correlation_id) DO NOTHING",
        &[correlation_id.into()],
    )
    .expect("submission_status INSERT");
}

fn parse_pool_keys(pool_keys: &JsonB) -> (Vec<(i64, i64)>, Vec<(i64, i64)>) {
    let sku = pool_keys
        .0
        .get("sku")
        .and_then(|v| v.as_array())
        .map(|arr| parse_key_array(arr))
        .unwrap_or_default();
    let wip = pool_keys
        .0
        .get("wip")
        .and_then(|v| v.as_array())
        .map(|arr| parse_key_array(arr))
        .unwrap_or_default();
    (sku, wip)
}

fn parse_key_array(arr: &[serde_json::Value]) -> Vec<(i64, i64)> {
    arr.iter()
        .map(|v| {
            let pair = v.as_array().expect("pool key entry must be array");
            assert_eq!(pair.len(), 2, "pool key entry must be [id, id]");
            let a = pair[0].as_i64().expect("pool key element must be int");
            let b = pair[1].as_i64().expect("pool key element must be int");
            (a, b)
        })
        .collect()
}

fn pool_keys_to_bytes(keys: &[(i64, i64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(keys.len() * 16);
    for (a, b) in keys {
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
    }
    out
}

fn event_type_to_id(event_type: &str) -> u16 {
    match event_type {
        "inv_adjust" => 1,
        "wo_complete" => 2,
        "wo_start" => 3,
        "scrap" => 4,
        "receipt" => 5,
        // Unknown event types pass through; committer (M1.3+) will validate.
        _ => 0,
    }
}

fn now_micros() -> u64 {
    // PG's GetCurrentTimestamp returns microseconds since PG epoch
    // (2000-01-01 UTC). Used for enqueued_at_micros bookkeeping.
    unsafe { pg_sys::GetCurrentTimestamp() as u64 }
}

fn past_deadline(deadline_micros: i128) -> bool {
    (now_micros() as i128) >= deadline_micros
}

fn sleep_until_retry(deadline_micros: i128, iter_count: &mut u32) {
    // Bounded short sleep (10ms); CHECK_FOR_INTERRUPTS via WaitLatch.
    // M3.1 will replace this with a ConditionVariable wait keyed to
    // router-side slot-freed signals.
    *iter_count = iter_count.saturating_add(1);
    let remaining_micros = (deadline_micros - now_micros() as i128).max(0) as u64;
    let sleep_micros = remaining_micros.min(10_000);
    if sleep_micros == 0 {
        return;
    }
    let sleep_ms = (sleep_micros / 1000).max(1) as i64;
    unsafe {
        let _ = pg_sys::WaitLatch(
            pg_sys::MyLatch,
            (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_EXIT_ON_PM_DEATH) as i32,
            sleep_ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(pg_sys::MyLatch);
    }
}

