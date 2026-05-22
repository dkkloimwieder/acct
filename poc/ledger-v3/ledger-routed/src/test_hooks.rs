//! Test-only SPIs (acct-p0d8). Compiled in only when the
//! `test_hooks` feature is enabled; production builds elide the
//! module entirely.
//!
//! Surface:
//!   - `ledger_routed_test_committer_pids()` — read CommitterIdentityRegistry
//!     slots (slot, pid, generation, alive).
//!   - `ledger_routed_test_router_pid()` — read the router worker's pid
//!     published at router_main startup.
//!   - `ledger_routed_test_set_committer_stall_us(us INT)` — set the
//!     shmem stall atomic the committer pipeline honors between
//!     hydrate_snapshot and bulk_write. Tests use this to keep the
//!     committer in flight long enough to pg_terminate_backend it.
//!   - `ledger_routed_test_run_router_recovery_sweep()` — invoke the
//!     router boot sweep synchronously without restarting the router.
//!     Returns the number of slots reconciled.
//!
//! All SPIs require superuser (pg_terminate_backend is gated similarly
//! upstream; these expose internal shmem so the test_hooks gate exists
//! both as a build-time guard and an authorization signal).

use crate::shmem::COMMITTER_QUEUE;
use crate::identity::is_committer_alive;
use pgrx::iter::TableIterator;
use pgrx::name;
use pgrx::prelude::*;
use std::sync::atomic::Ordering::{Acquire, Release};

#[pg_extern]
fn ledger_routed_test_committer_pids() -> TableIterator<
    'static,
    (
        name!(slot, i32),
        name!(pid, i32),
        name!(generation, i32),
        name!(alive, bool),
    ),
> {
    let q = COMMITTER_QUEUE.share();
    let mut rows: Vec<(i32, i32, i32, bool)> = Vec::with_capacity(q.identity_slots.len());
    for (i, s) in q.identity_slots.iter().enumerate() {
        let pid = s.pid.load(Acquire);
        let generation = s.generation.load(Acquire) as i32;
        let alive = is_committer_alive(i as u32, generation as u32);
        rows.push((i as i32, pid, generation, alive));
    }
    TableIterator::new(rows)
}

#[pg_extern]
fn ledger_routed_test_router_pid() -> i32 {
    COMMITTER_QUEUE.share().router_pid.load(Acquire)
}

#[pg_extern]
fn ledger_routed_test_set_committer_stall_us(us: i32) {
    let v = if us < 0 { 0u32 } else { us as u32 };
    COMMITTER_QUEUE
        .share()
        .test_inject_committer_stall_us
        .store(v, Release);
}

#[pg_extern]
fn ledger_routed_test_run_router_recovery_sweep() -> i64 {
    crate::router::try_recover_router_orphan() as i64
}

#[pg_extern]
fn ledger_routed_test_committer_stall_hits() -> i64 {
    COMMITTER_QUEUE.share().test_committer_stall_hits.load(Acquire) as i64
}

/// Inject a synthetic orphaned CommitterQueue entry. Finds the first
/// CQ slot at valid==0 (empty) and stamps it with valid==2 + a phony
/// owner identity (slot 0, generation u32::MAX) that can never match a
/// live committer. Returns the chosen CQ index. Use to exercise the
/// router's boot-sweep Phase 2 path (try_recover_router_orphan) without
/// triggering a postmaster crash-recovery cycle: a real SIGKILL on a
/// BGW causes PG to restart ALL backends because shmem might be
/// corrupted, which makes the post-condition observation indistinguishable
/// from the postmaster_restart scenario. The synthetic orphan exercises
/// the same `sweep_queue_phase` Phase 2 CAS path without that side
/// effect. Pause BGWs (`test_bgworker_paused`) before calling so the
/// router can't race-claim the slot.
#[pg_extern]
fn ledger_routed_test_inject_orphan_cq() -> i32 {
    use crate::shmem::{LEDGER_V3_COMMITTER_QUEUE_SIZE};
    let queue = COMMITTER_QUEUE.share();
    for (idx, entry) in queue.entries.iter().enumerate() {
        if entry.valid.load(Acquire) == 0 {
            entry.committer_bgw_slot.store(0, Release);
            entry.committer_bgw_generation.store(u32::MAX, Release);
            entry.valid.store(2, Release);
            return idx as i32;
        }
        if (idx as u32) >= LEDGER_V3_COMMITTER_QUEUE_SIZE as u32 {
            break;
        }
    }
    -1
}

/// Pause the router + committer BGWorkers. The pause flag is checked
/// at the top of each BGW's wait_latch iteration; in-flight pipelines
/// finish, then BGWs spin without doing work. Already exposed via the
/// `test_bgworker_paused` shmem atomic; this SPI is a typed entry
/// point for tests.
#[pg_extern]
fn ledger_routed_test_set_bgworker_paused(paused: bool) {
    COMMITTER_QUEUE
        .share()
        .test_bgworker_paused
        .store(if paused { 1 } else { 0 }, Release);
}
