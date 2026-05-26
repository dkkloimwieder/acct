//! Recovery BGWorker for ledger-routed-c (Path C, design-v3.1 §6.5).
//!
//! Postmaster-startup recovery worker. Runs ONCE at postmaster start
//! (set_restart_time None on registration), Release-stores
//! `COMMITTER_QUEUE.recovery_complete = 1` so router + committers can
//! open for traffic, exits.
//!
//! ## Why this is trivial for Path C
//!
//! Per design-v3.1 §6.5 / §14.2, in-flight submissions at postmaster
//! restart EVAPORATE cleanly:
//!   - Shmem (StagingQueue + CommitterQueue + SpilloverArena) is
//!     wiped to zero on PG startup — no orphan staging or CQ entries
//!     to sweep.
//!   - There is NO submission_status table to classify ("trx exists
//!     iff committed" semantics make per-submission DB state
//!     unnecessary).
//!   - There is NO persistent_staging durable path (the design doesn't
//!     include it).
//!
//! The recovery worker therefore exists only to be the official
//! owner of `recovery_complete`'s 0→1 lifecycle. Router-orphan
//! sweeps (recover from a router-only restart while postmaster is
//! alive) are router-self-owned via `router::try_recover_router_orphan`
//! called at the router's own restart-time boot — that path runs on
//! every router relaunch, not just at postmaster start, and is
//! where the load-bearing recovery work actually happens.

use crate::shmem::COMMITTER_QUEUE;
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use std::sync::atomic::Ordering::Release;

/// Recovery BGWorker entry point. Registered ONCE in `_PG_init` with
/// `set_restart_time(None)` — runs only at postmaster start.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_c_recovery_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = crate::target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // Postmaster-restart sweep is trivial for v3 (shmem fresh, no
    // submission_status to classify). Just flip the gate.
    COMMITTER_QUEUE.share().recovery_complete.store(1, Release);
}
