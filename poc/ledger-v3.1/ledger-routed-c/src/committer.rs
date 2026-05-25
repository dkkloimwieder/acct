//! Committer BGWorker for ledger-routed-c (Path C) — P3.1 shell.
//!
//! P3.1 ships the worker lifecycle only: attach signal handlers, connect SPI,
//! wait for the recovery gate, claim a CommitterIdentitySlot (proving the
//! registry works), then idle on a 50 ms latch and release the slot on exit.
//!
//! The commit pipeline — claim a commit_group, pg_xact_status triage, pre-flight
//! dedup, hydrate, dispatch to `plan_apply_provisional`, bulk write, COMMIT,
//! cleanup, with drop-and-continue (NO pristine replay, design-v3.1 §6.4 / §14.2)
//! — lands in P3.3 (`acct-2ttr.6`).

use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use std::sync::atomic::Ordering::Acquire;

use crate::identity::{claim_committer_identity, release_committer_identity};
use crate::router::wait_for_recovery_complete;
use crate::shmem::COMMITTER_QUEUE;

/// Committer BGWorker entry point. One per `committer_count`; registered in
/// `_PG_init` with a 5 s restart time.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_c_committer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = crate::target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    wait_for_recovery_complete();

    let (slot, _generation) = claim_committer_identity();

    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        if COMMITTER_QUEUE.share().test_bgworker_paused.load(Acquire) == 1 {
            continue;
        }
        // claim_next_committer_entry() → process_commit_group() → cleanup — P3.3.
    }

    release_committer_identity(slot);
}
