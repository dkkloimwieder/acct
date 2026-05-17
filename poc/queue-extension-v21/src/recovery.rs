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

/// BGWorker entry point. Connects to the target DB, runs the recovery
/// sweep once inside one transaction, sets `recovery_complete=1`, and
/// returns (the worker exits — set_restart_time(None) on registration).
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn poc_v21_recovery_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

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
        "poc_v21_recovery: sweep complete — committed={} failed={}",
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

    let failed: i64 = Spi::get_one(
        "WITH updated AS (\
           UPDATE poc_v21_submission_status \
           SET state='failed', \
               processed_at=now(), \
               error_code='postmaster_restart_loss' \
           WHERE state IN ('queued', 'processing') \
           RETURNING 1\
         ) SELECT COUNT(*)::BIGINT FROM updated",
    )
    .map_err(|e| format!("recovery failed-pass: {e}"))?
    .unwrap_or(0);

    Ok((committed, failed))
}

/// Test-only: run the recovery sweep synchronously and set the
/// recovery_complete flag. Used by acceptance tests to exercise the
/// sweep logic without an actual postmaster restart.
///
/// Returns JSON: {"committed": N, "failed": N}.
#[pg_extern]
fn poc_v21_test_run_startup_recovery() -> pgrx::Json {
    let (committed, failed) = run_startup_recovery_sweep().unwrap_or((0, 0));
    COMMITTER_QUEUE
        .share()
        .recovery_complete
        .store(1, Release);
    pgrx::Json(serde_json::json!({
        "committed": committed,
        "failed": failed,
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
