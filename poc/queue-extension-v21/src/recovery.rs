//! M5d.1 (acct-y0bp): postmaster-startup recovery worker.
//!
//! Spec §3.6 — non-durable path. After a postmaster crash + restart,
//! shmem is empty (staging queue, committer queue, spillover arena
//! all wiped). The `poc_v21_submission_status` table may still hold
//! 'queued' or 'processing' rows from envelopes that were in flight
//! at crash time. Cost rows in `poc_v21_cost_*` / `poc_v21_posting_lines`
//! are the source of truth: if any exist for a given correlation_id,
//! the committer completed before the crash and the status row should
//! reach 'committed'; otherwise the envelope is lost in shmem and
//! the status row is marked 'failed' with `postmaster_restart_loss`.
//!
//! Coordination:
//!
//!   The recovery worker is registered at `_PG_init` with
//!   BgWorkerStartTime::RecoveryFinished. The router and committers
//!   start with the same start_time and could run concurrently with
//!   the recovery worker. To prevent a race where the recovery sweep
//!   classifies a row as 'failed' WHILE the committer is mid-flight
//!   on a re-submitted envelope with the same correlation_id, both
//!   the router and committer main loops Acquire-load
//!   `COMMITTER_QUEUE.recovery_complete` at startup and SPIN until it
//!   reaches 1. The recovery worker Release-stores 1 after its sweep
//!   completes.
//!
//!   The flag is set to 1 EVEN IF the sweep fails — the alternative
//!   (refuse to open queues) would render the extension unusable on
//!   recovery error. Operators see the error in logs.
//!
//! Persistent staging (M5e.x):
//!
//!   Spec §3.6 references `poc_v21_persistent_staging` for the durable
//!   path: in-flight envelopes with persistent_staging rows are
//!   recoverable (envelope payload is on disk). For M5d.1's non-durable
//!   scope, that filter is omitted — every in-flight `submission_status`
//!   row is classified. M5e.3 (acct-cv4n) extends this sweep to skip
//!   rows that have persistent_staging coverage and re-enqueue them
//!   into shmem.

use crate::{COMMITTER_QUEUE, target_database_str};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_sys;
use pgrx::prelude::*;
use std::sync::atomic::Ordering::{Acquire, Release};

/// M5e.3 sweep counters: (completed_from_cost_rows, re_enqueued, failed_caller_aborted).
type PersistentSweepCounts = (i64, i64, i64);

/// BGWorker entry point. Connects to the target DB, runs the recovery
/// sweep once inside one transaction, sets `recovery_complete=1`, and
/// returns (the worker exits — set_restart_time(None) on registration).
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_recovery_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // M5e.3: persistent_staging sweep runs FIRST. Re-enqueued envelopes
    // get persistent_staging.state='in_shmem'; the M5d.1 sweep's Pass 2
    // filter then knows to skip their submission_status rows.
    let persistent_result = std::panic::catch_unwind(|| {
        BackgroundWorker::transaction(|| run_persistent_staging_recovery_sweep())
    });
    let (ps_completed, ps_re_enqueued, ps_failed) = match persistent_result {
        Ok(Ok(counts)) => counts,
        Ok(Err(e)) => {
            log!("poc_v21_recovery: persistent_staging sweep error: {e}");
            (0, 0, 0)
        }
        Err(_) => {
            log!("poc_v21_recovery: persistent_staging sweep panicked");
            (0, 0, 0)
        }
    };
    log!(
        "poc_v21_recovery: persistent_staging sweep — completed={} re_enqueued={} caller_aborted={}",
        ps_completed,
        ps_re_enqueued,
        ps_failed
    );

    let sweep_result = std::panic::catch_unwind(|| {
        BackgroundWorker::transaction(|| run_startup_recovery_sweep())
    });

    let (committed, failed) = match sweep_result {
        Ok(Ok(counts)) => counts,
        Ok(Err(e)) => {
            log!("poc_v21_recovery: sweep error: {e}");
            (0, 0)
        }
        Err(_) => {
            log!("poc_v21_recovery: sweep panicked; flag still set so queues open");
            (0, 0)
        }
    };
    log!(
        "poc_v21_recovery: non-durable sweep complete — committed={} failed={}",
        committed,
        failed
    );

    COMMITTER_QUEUE
        .share()
        .recovery_complete
        .store(1, Release);
}

/// Run the recovery classification sweep. Returns (committed_count,
/// failed_count) for telemetry.
///
/// SQL approach: a single UPDATE per outcome.
///
/// Pass 1 (committed): for each `submission_status` row in
/// ('queued', 'processing') with at least one cost-row sibling, set
/// state='committed'. The row's lock ensures the EXISTS evaluation
/// and the UPDATE are atomic per row.
///
/// Pass 2 (failed): for remaining in-flight rows (those Pass 1
/// didn't touch), set state='failed' with error_code='postmaster_restart_loss'.
///
/// A race-mitigating side-note: the recovery_complete coordination
/// guarantees no committer is running while Pass 1/2 execute. So a
/// committer cannot insert cost rows between the two passes.
pub fn run_startup_recovery_sweep() -> Result<(i64, i64), String> {
    let committed: i64 = Spi::get_one(
        "WITH updated AS (\
           UPDATE poc_v21_submission_status \
           SET state='committed', \
               processed_at=COALESCE(processed_at, now()), \
               committed_at=COALESCE(committed_at, now()) \
           WHERE state IN ('queued', 'processing') \
             AND (EXISTS (SELECT 1 FROM poc_v21_cost_layers cl \
                            WHERE cl.correlation_id = poc_v21_submission_status.correlation_id) \
               OR EXISTS (SELECT 1 FROM poc_v21_cost_depletions cd \
                            WHERE cd.correlation_id = poc_v21_submission_status.correlation_id) \
               OR EXISTS (SELECT 1 FROM poc_v21_cost_consumptions cc \
                            WHERE cc.correlation_id = poc_v21_submission_status.correlation_id) \
               OR EXISTS (SELECT 1 FROM poc_v21_posting_lines pl \
                            WHERE pl.correlation_id = poc_v21_submission_status.correlation_id)) \
           RETURNING 1\
         ) SELECT COUNT(*)::BIGINT FROM updated",
    )
    .map_err(|e| format!("recovery committed-pass: {e}"))?
    .unwrap_or(0);

    // M5d.1 Pass 2: in-flight without cost rows AND without persistent_staging
    // coverage. The filter (M5e.3) excludes correlation_ids that were
    // re-enqueued from persistent_staging — those rows are legitimately
    // in-flight again post-recovery and will reach terminal through the
    // committer pipeline.
    let failed: i64 = Spi::get_one(
        "WITH updated AS (\
           UPDATE poc_v21_submission_status \
           SET state='failed', \
               processed_at=now(), \
               error_code='postmaster_restart_loss' \
           WHERE state IN ('queued', 'processing') \
             AND NOT EXISTS ( \
                 SELECT 1 FROM poc_v21_persistent_staging ps \
                  WHERE ps.correlation_id = poc_v21_submission_status.correlation_id \
                    AND ps.state IN ('staged', 'in_shmem') \
             ) \
           RETURNING 1\
         ) SELECT COUNT(*)::BIGINT FROM updated",
    )
    .map_err(|e| format!("recovery failed-pass: {e}"))?
    .unwrap_or(0);

    Ok((committed, failed))
}

/// M5e.3 (acct-6isq): durable-path recovery sweep.
///
/// For each persistent_staging row in ('staged', 'in_shmem'):
///   1. If cost rows exist for correlation_id → committer committed
///      pre-crash. UPDATE state='completed', UPSERT submission_status.
///   2. Else check pg_xact_status(user_tx_xid):
///      - 'committed': caller user-tx durable. Re-INSERT into shmem
///        staging queue; UPDATE state='in_shmem'.
///      - else (aborted / in_progress / unknown): caller never made it.
///        DELETE persistent_staging row; mark submission_status='failed'
///        with error_code='caller_tx_aborted' if a status row exists.
///
/// Returns (completed_from_cost_rows, re_enqueued, failed_caller_aborted).
///
/// Per-row work, not a single batched UPDATE, because each row may
/// require a different action plus the shmem re-push has to happen
/// per-row anyway.
pub fn run_persistent_staging_recovery_sweep() -> Result<PersistentSweepCounts, String> {
    let rows: Vec<(
        pgrx::Uuid,
        String,    // user_tx_xid as text (xid8 → text)
        String,    // event_type
        pgrx::JsonB, // payload
        pgrx::JsonB, // sku_pool_keys
        Option<pgrx::JsonB>, // wip_pool_keys
    )> = Spi::connect(|client| {
        let mut out: Vec<(pgrx::Uuid, String, String, pgrx::JsonB, pgrx::JsonB, Option<pgrx::JsonB>)> =
            Vec::new();
        let mut t = client
            .select(
                "SELECT correlation_id, user_tx_xid::text, event_type, \
                        payload, sku_pool_keys, wip_pool_keys \
                   FROM poc_v21_persistent_staging \
                  WHERE state IN ('staged', 'in_shmem') \
                  ORDER BY request_seq",
                None,
                &[],
            )
            .ok()?;
        while let Some(row) = t.next() {
            let cid: pgrx::Uuid = row.get::<pgrx::Uuid>(1).ok()??;
            let xid: String = row.get::<String>(2).ok()??;
            let event_type: String = row.get::<String>(3).ok()??;
            let payload: pgrx::JsonB = row.get::<pgrx::JsonB>(4).ok()??;
            let sku: pgrx::JsonB = row.get::<pgrx::JsonB>(5).ok()??;
            let wip: Option<pgrx::JsonB> = row.get::<pgrx::JsonB>(6).ok().flatten();
            out.push((cid, xid, event_type, payload, sku, wip));
        }
        Some(out)
    })
    .unwrap_or_default();

    let mut completed_count = 0i64;
    let mut re_enqueued_count = 0i64;
    let mut aborted_count = 0i64;

    for (cid, xid_str, event_type, payload, sku_json, wip_json) in rows {
        // Check cost rows first — source of truth (committer wrote them
        // inside Step 5 sub-tx).
        let has_cost: bool = Spi::get_one_with_args(
            "SELECT EXISTS ( \
               SELECT 1 FROM poc_v21_cost_layers WHERE correlation_id=$1 \
               UNION ALL \
               SELECT 1 FROM poc_v21_cost_depletions WHERE correlation_id=$1 \
               UNION ALL \
               SELECT 1 FROM poc_v21_cost_consumptions WHERE correlation_id=$1 \
               UNION ALL \
               SELECT 1 FROM poc_v21_posting_lines WHERE correlation_id=$1 \
               LIMIT 1 \
             )",
            &[cid.into()],
        )
        .map_err(|e| format!("cost-rows check: {e}"))?
        .unwrap_or(false);

        if has_cost {
            Spi::run_with_args(
                "UPDATE poc_v21_persistent_staging SET state='completed' WHERE correlation_id=$1",
                &[cid.into()],
            )
            .map_err(|e| format!("complete UPDATE: {e}"))?;
            Spi::run_with_args(
                "INSERT INTO poc_v21_submission_status \
                   (correlation_id, state, enqueued_at, processed_at, committed_at) \
                 VALUES ($1, 'committed', now(), now(), now()) \
                 ON CONFLICT (correlation_id) DO UPDATE SET \
                   state='committed', processed_at=now(), \
                   committed_at=COALESCE(poc_v21_submission_status.committed_at, now())",
                &[cid.into()],
            )
            .map_err(|e| format!("status committed UPSERT: {e}"))?;
            completed_count += 1;
            continue;
        }

        // No cost rows — check caller user-tx outcome via pg_xact_status.
        let xact_status: Option<String> = Spi::get_one_with_args(
            "SELECT pg_xact_status($1::text::xid8)",
            &[xid_str.clone().into()],
        )
        .unwrap_or(None);
        let is_committed = matches!(xact_status.as_deref(), Some("committed"));

        if is_committed {
            // Re-enqueue into shmem.
            let sku_keys = crate::enqueue::parse_pool_keys_array(&sku_json.0);
            let wip_keys = wip_json
                .as_ref()
                .map(|j| crate::enqueue::parse_pool_keys_array(&j.0))
                .unwrap_or_default();
            let payload_bytes =
                serde_json::to_vec(&payload.0).map_err(|e| format!("payload serialize: {e}"))?;
            let user_tx_xid: u64 = xid_str.parse().unwrap_or(0);

            crate::enqueue::re_enqueue_from_persistent_staging(
                cid,
                user_tx_xid,
                &event_type,
                &payload_bytes,
                &sku_keys,
                &wip_keys,
            )?;
            Spi::run_with_args(
                "UPDATE poc_v21_persistent_staging SET state='in_shmem' WHERE correlation_id=$1",
                &[cid.into()],
            )
            .map_err(|e| format!("re-enqueue state UPDATE: {e}"))?;
            re_enqueued_count += 1;
        } else {
            // Caller never committed durably. Drop the persistent_staging
            // row; mark any existing submission_status row failed.
            Spi::run_with_args(
                "DELETE FROM poc_v21_persistent_staging WHERE correlation_id=$1",
                &[cid.into()],
            )
            .map_err(|e| format!("DELETE: {e}"))?;
            Spi::run_with_args(
                "UPDATE poc_v21_submission_status \
                    SET state='failed', processed_at=now(), \
                        error_code='caller_tx_aborted' \
                  WHERE correlation_id=$1 AND state IN ('queued', 'processing')",
                &[cid.into()],
            )
            .map_err(|e| format!("status fail UPDATE: {e}"))?;
            aborted_count += 1;
        }
    }

    Ok((completed_count, re_enqueued_count, aborted_count))
}

/// Test-only: run the recovery sweep synchronously and set the
/// recovery_complete flag. Used by acceptance tests to exercise the
/// sweep logic without an actual postmaster restart.
///
/// Runs persistent_staging sweep (M5e.3) FIRST, then the non-durable
/// sweep (M5d.1). Returns JSON with both sets of counters.
#[pg_extern]
fn poc_v21_test_run_startup_recovery() -> pgrx::Json {
    let (ps_completed, ps_re_enqueued, ps_aborted) =
        run_persistent_staging_recovery_sweep().unwrap_or((0, 0, 0));
    let (committed, failed) = run_startup_recovery_sweep().unwrap_or((0, 0));
    COMMITTER_QUEUE
        .share()
        .recovery_complete
        .store(1, Release);
    pgrx::Json(serde_json::json!({
        "committed": committed,
        "failed": failed,
        "ps_completed": ps_completed,
        "ps_re_enqueued": ps_re_enqueued,
        "ps_aborted": ps_aborted,
    }))
}

/// Test-only: clear the recovery_complete flag so a subsequent
/// `poc_v21_test_run_startup_recovery` call can exercise the gate.
#[pg_extern]
fn poc_v21_test_reset_recovery_complete() {
    COMMITTER_QUEUE
        .share()
        .recovery_complete
        .store(0, Release);
}

/// Test-only: observable state of the recovery_complete flag.
#[pg_extern]
fn poc_v21_recovery_complete() -> bool {
    COMMITTER_QUEUE
        .share()
        .recovery_complete
        .load(Acquire)
        != 0
}
