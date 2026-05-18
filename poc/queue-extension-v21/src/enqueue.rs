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

    // 3b. durable_queue=true ⇒ WAL-backed staging row (spec §1.9, M5e.1).
    //
    // Rides inside caller user-tx — rolls back atomically on caller abort;
    // commits atomically with caller's other work. No extra fsync — the
    // INSERT's WAL is part of the caller's commit record.
    //
    // ON CONFLICT (correlation_id) DO NOTHING handles caller retry with
    // the same UUID (idempotent boundary, mirrors submission_status convention).
    //
    // business_date extracted from payload's business_date_jdate field
    // (integer days since Unix epoch — same convention the committer
    // uses at _extract_payload_meta). Payloads with no business_date_jdate
    // default to 1970-01-01 — caller error visible in the persisted row.
    if durable_queue {
        insert_persistent_staging_row(
            correlation_id,
            user_tx_xid,
            event_type,
            &payload,
            &sku_pool_keys,
            &wip_pool_keys,
        );
    }

    // 4. Status row INSERT — dispatch on poc_v21.status_insert_mode.
    // caller_intx (default): INSERT inside caller user-tx; cheapest.
    //                         On caller abort, the row is lost; the
    //                         committer's pg_xact check + lazy INSERT
    //                         creates a 'failed' row with caller_tx_aborted.
    // committer_lazy: skip — committer creates row at terminal-state
    //                 determination. _PG_init gated this on persistent_staging=on.
    //
    // (caller_subtx was specified pre-M5c.3 but dropped: BeginInternalSubTransaction
    // is a savepoint, not an autonomous tx — writes still fold into the
    // parent and are lost on parent abort. The promise was non-deliverable
    // and the residual differentiator (error isolation on status INSERT)
    // is absorbed by ON CONFLICT DO NOTHING.)
    let mode = status_insert_mode_str();
    match mode.as_str() {
        "caller_intx" => {
            insert_status_row_caller_intx(correlation_id);
        }
        "committer_lazy" => {
            // No-op. Committer's Step 12 INSERT ON CONFLICT path
            // (committed/failed/replayed) creates the row.
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
    //
    // M5c.1 (acct-r0aa): wait loop is signal-driven via PG's
    // ConditionVariable. PrepareToSleep registers this backend in the
    // CV's proclist BEFORE we observe queue state — so a broadcast
    // that fires between our check and TimedSleep still wakes us.
    // The committer (cleanup_after_superbatch) and the router (rollback
    // + recovery sweep) broadcast on slot-free; any waiter in TimedSleep
    // wakes immediately, retries the push, returns.
    let timeout_ms = crate::queue_full_timeout_ms_now();
    let deadline_micros = (now_micros() as i128) + (timeout_ms as i128) * 1000;
    let cv = crate::backpressure_cv_ptr();
    unsafe {
        pg_sys::ConditionVariablePrepareToSleep(cv);
    }

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
                    unsafe { pg_sys::ConditionVariableCancelSleep() };
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!("poc_v21_enqueue: spillover arena exhausted and backpressure timeout elapsed ({timeout_ms}ms)")
                    );
                }
                cv_wait_for_slot(cv, deadline_micros);
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
                    unsafe { pg_sys::ConditionVariableCancelSleep() };
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_RESOURCES,
                        format!("poc_v21_enqueue: staging queue full and backpressure timeout elapsed ({timeout_ms}ms)")
                    );
                }
                cv_wait_for_slot(cv, deadline_micros);
                continue;
            }
        }
    }

    unsafe { pg_sys::ConditionVariableCancelSleep() };
    // 7. (Router wake — SetLatch on the router BGWorker — lands at M3.1.)
    // For M1.2 the router is stubbed; setting the latch is a no-op.
}

/// Wait for a slot-freed signal or the per-batch deadline, whichever
/// comes first. ConditionVariableTimedSleep handles CHECK_FOR_INTERRUPTS
/// internally (a query cancel sets the backend's latch which the CV's
/// inner WaitLatch picks up). Returns true if the wait timed out;
/// returns false if signaled.
fn cv_wait_for_slot(cv: *mut pg_sys::ConditionVariable, deadline_micros: i128) -> bool {
    let remaining_micros = (deadline_micros - now_micros() as i128).max(0) as u64;
    // Cap each TimedSleep at 1s so deadline-passed cases get re-checked
    // even if the signaler is unusually quiet. Inside that cap, the wake
    // is signal-driven (broadcast) so latency is ~OS-scheduler quantum.
    let ms = ((remaining_micros / 1000) as i64).min(1000).max(1);
    unsafe {
        pg_sys::ConditionVariableTimedSleep(
            cv,
            ms as std::ffi::c_long,
            pg_sys::PG_WAIT_EXTENSION,
        )
    }
}

fn return_to_retry() {
    // Inline; just here for readability of the matching arm above.
}

/// Pop a pending staging entry and mark valid=2. Returns the slot index
/// (or NULL if no pending entry). Test-only helper; mirrors what the
/// router (M3.1) would do for a size-1 SuperBatch.
#[cfg(any(test, feature = "test_hooks"))]
#[pg_extern]
fn poc_v21_test_take_pending() -> Option<i64> {
    let mut queue_guard = STAGING_QUEUE.exclusive();
    staging::take_pending(&mut queue_guard).map(|idx| idx as i64)
}

/// Reset a slot to empty (valid=0) and free its arena blocks. Test-only;
/// mirrors what the committer (M1.3) would do at Step 14.
#[cfg(any(test, feature = "test_hooks"))]
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

/// INSERT into `poc_v21_persistent_staging` for the durable_queue=true
/// path. M5e.1 (acct-m8pg). Runs inside caller user-tx so rolls back on
/// caller abort. ON CONFLICT (correlation_id) DO NOTHING for idempotent
/// retry. `business_date` resolved from payload's `business_date_jdate`
/// (integer days since Unix epoch — the same convention the committer
/// uses at `_extract_payload_meta`).
fn insert_persistent_staging_row(
    correlation_id: Uuid,
    user_tx_xid: u32,
    event_type: &str,
    payload: &JsonB,
    sku_pool_keys: &[(i64, i64)],
    wip_pool_keys: &[(i64, i64)],
) {
    let business_date_jdate: i32 = payload
        .0
        .get("business_date_jdate")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32)
        .unwrap_or(0);

    let sku_json =
        serde_json::Value::Array(sku_pool_keys.iter().map(pool_key_to_json).collect());
    let wip_json =
        serde_json::Value::Array(wip_pool_keys.iter().map(pool_key_to_json).collect());

    // wip_pool_keys column is nullable; empty array → SQL NULL via NULLIF.
    // user_tx_xid binds as TEXT then casts to xid8 (no direct pgrx u64→xid8).
    Spi::run_with_args(
        "INSERT INTO poc_v21_persistent_staging \
            (correlation_id, user_tx_xid, event_type, payload, \
             sku_pool_keys, wip_pool_keys, business_date, state) \
         VALUES ($1, $2::text::xid8, $3, $4, $5, \
                 NULLIF($6, '[]'::jsonb), \
                 DATE 'epoch' + ($7::int), 'staged') \
         ON CONFLICT (correlation_id) DO NOTHING",
        &[
            correlation_id.into(),
            user_tx_xid.to_string().into(),
            event_type.into(),
            JsonB(payload.0.clone()).into(),
            JsonB(sku_json).into(),
            JsonB(wip_json).into(),
            (business_date_jdate as i64).into(),
        ],
    )
    .expect("persistent_staging INSERT");
}

fn pool_key_to_json(pair: &(i64, i64)) -> serde_json::Value {
    serde_json::Value::Array(vec![
        serde_json::Value::Number(pair.0.into()),
        serde_json::Value::Number(pair.1.into()),
    ])
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
        "so_shipment" => 6,
        "inv_issue" => 7,
        "po_receipt" => 5,
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

// ── M5e.3 (acct-6isq): re-enqueue from persistent_staging ───────────
//
// Used by the postmaster-restart recovery sweep. The shmem state is empty
// (postmaster crash wiped it). The persistent_staging row carries the full
// envelope. We allocate arena blocks + push a fresh StagingEntry with the
// SAVED user_tx_xid (NOT a new one) and the SAVED correlation_id. Caller
// (recovery.rs) handles state transitions on persistent_staging itself.
//
// No CV wait: post-restart the queues are empty; arena/queue should have
// capacity. If they don't, we return an error and let the caller surface
// it (this is an operational error, not a normal-path backpressure case).
pub(crate) fn re_enqueue_from_persistent_staging(
    correlation_id: Uuid,
    user_tx_xid: u64,
    event_type: &str,
    payload_bytes: &[u8],
    sku_pool_keys: &[(i64, i64)],
    wip_pool_keys: &[(i64, i64)],
) -> Result<u32, String> {
    let payload_len: u32 = payload_bytes
        .len()
        .try_into()
        .map_err(|_| format!("payload too large: {}", payload_bytes.len()))?;
    let sku_keys_size: u32 = (sku_pool_keys.len() * 16) as u32;
    let wip_keys_size: u32 = (wip_pool_keys.len() * 16) as u32;

    // Allocate arena blocks.
    let (payload_offset, sku_keys_offset, wip_keys_offset) = {
        let mut arena = SPILLOVER_ARENA.exclusive();
        let payload_offset = arena
            .alloc(payload_len.max(1))
            .ok_or_else(|| "spillover arena exhausted (payload)".to_string())?;
        arena.write_bytes(payload_offset, payload_bytes);

        let sku_keys_offset = if sku_keys_size > 0 {
            let off = arena.alloc(sku_keys_size).ok_or_else(|| {
                arena.free(payload_offset);
                "spillover arena exhausted (sku_keys)".to_string()
            })?;
            arena.write_bytes(off, &pool_keys_to_bytes(sku_pool_keys));
            off
        } else {
            0
        };

        let wip_keys_offset = if wip_keys_size > 0 {
            let off = arena.alloc(wip_keys_size).ok_or_else(|| {
                arena.free(payload_offset);
                if sku_keys_offset != 0 {
                    arena.free(sku_keys_offset);
                }
                "spillover arena exhausted (wip_keys)".to_string()
            })?;
            arena.write_bytes(off, &pool_keys_to_bytes(wip_pool_keys));
            off
        } else {
            0
        };
        (payload_offset, sku_keys_offset, wip_keys_offset)
    };

    // Push the staging entry.
    let mut queue_guard = STAGING_QUEUE.exclusive();
    let queue = &mut *queue_guard;
    let request_seq = queue.next_request_seq.fetch_add(1, Relaxed);
    let entry = StagingEntry {
        valid: std::sync::atomic::AtomicU8::new(0),
        _pad: [0; 7],
        request_seq,
        correlation_id: *correlation_id.as_bytes(),
        user_tx_xid,
        event_type_id: event_type_to_id(event_type),
        _pad_event: [0; 2],
        payload_offset,
        payload_length: payload_len,
        sku_pool_count: sku_pool_keys.len() as u16,
        wip_pool_count: wip_pool_keys.len() as u16,
        sku_pool_keys_offset: sku_keys_offset,
        wip_pool_keys_offset: wip_keys_offset,
        enqueued_at_micros: now_micros(),
        backend_pid: 0, // No live caller backend; recovery worker is doing this.
        _pad_pid: [0; 4],
        superbatch_id: std::sync::atomic::AtomicU64::new(0),
        eject_count: std::sync::atomic::AtomicU16::new(0),
        _pad2: [0; 6],
    };

    match staging::push_entry(queue, entry) {
        Ok(slot_idx) => Ok(slot_idx),
        Err(staging::StagingPushError::QueueFull) => {
            drop(queue_guard);
            let mut arena = SPILLOVER_ARENA.exclusive();
            arena.free(payload_offset);
            if sku_keys_offset != 0 {
                arena.free(sku_keys_offset);
            }
            if wip_keys_offset != 0 {
                arena.free(wip_keys_offset);
            }
            Err("staging queue full during recovery re-enqueue".to_string())
        }
    }
}

/// Parse `{"sku": [[...]], "wip": [[...]]}` from a serde_json::Value.
/// Crate-internal version for recovery.rs to use after reading the
/// JSONB column from persistent_staging.
pub(crate) fn parse_pool_keys_array(arr: &serde_json::Value) -> Vec<(i64, i64)> {
    arr.as_array()
        .map(|a| parse_key_array(a))
        .unwrap_or_default()
}


