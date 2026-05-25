//! Router BGWorker for ledger-routed-c (Path C) — P3.1 shell.
//!
//! P3.1 ships only the worker lifecycle: attach signal handlers, connect SPI to
//! the target database, publish the router PID, wait for the recovery gate, then
//! tick on a 50 ms latch (honoring SIGHUP and the test pause flag). The actual
//! window scan + union-find affinity grouping + commit_group emit (design-v3.1
//! §6.3) lands in P3.2 (`acct-2ttr.5`); the boot-recovery sweep in P3.4.
//!
//! `wait_for_recovery_complete` lives here because the committer depends on it
//! too (mirrors the v3 layout).

use std::time::Duration;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::pg_guard;
use pgrx::pg_sys;
use std::sync::atomic::Ordering::{Acquire, Release};

use crate::shmem::COMMITTER_QUEUE;

/// Block until the recovery worker has flipped `recovery_complete` 0→1, or until
/// a SIGTERM ends the latch wait. design-v3.1 §6.5.
pub(crate) fn wait_for_recovery_complete() {
    loop {
        if COMMITTER_QUEUE.share().recovery_complete.load(Acquire) != 0 {
            return;
        }
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
            return;
        }
    }
}

/// Router BGWorker entry point. Registered in `_PG_init` with a 5 s restart time.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn ledger_routed_c_router_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let dbname = crate::target_database_str();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // Publish PID so a future test_hooks SPI (P3.4 orphan-recovery) can target
    // it; overwritten on every router relaunch.
    COMMITTER_QUEUE
        .share()
        .router_pid
        .store(unsafe { pg_sys::MyProcPid } as i32, Release);

    wait_for_recovery_complete();

    // P3.2 fills the tick body (router_tick) and P3.4 the boot sweep. For now the
    // worker idles on the latch so the lifecycle + registration are exercised.
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
        if BackgroundWorker::sighup_received() {
            unsafe {
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
            }
        }
        if COMMITTER_QUEUE.share().test_bgworker_paused.load(Acquire) == 1 {
            continue;
        }
        // router_tick() — P3.2.
    }
}
