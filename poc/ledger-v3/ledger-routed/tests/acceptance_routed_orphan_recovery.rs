//! acct-p0d8 acceptance: committer-orphan recovery via boot sweep.
//!
//! design-v3 §5.5 + §8.3 third bullet: "Committer death mid-processing:
//! verify orphan recovery picks up the commit_group; trx rows for
//! submissions in the group either all exist (recovery committer's tx
//! committed) or all don't (recovery committer reprocesses fresh)."
//!
//! Mechanism under test: `try_recover_router_orphan()` Phase 2 — for
//! each CommitterQueueEntry at valid==2 whose stamped owner identity
//! fails `is_committer_alive`, CAS the entry valid 2→1 + clear the
//! identity stamp so the next live committer's claim_next_committer_entry
//! finds it and reprocesses. Production trigger is router boot
//! (postmaster start OR router-only crash + restart); the test_hooks
//! `ledger_routed_test_run_router_recovery_sweep` SPI invokes the same
//! function synchronously without restarting the router.
//!
//! Why a synthetic orphan and not a real committer SIGKILL: PG's
//! crash-recovery contract requires postmaster to restart ALL backends
//! if a BGW exits unclean (SIGSEGV/SIGKILL) — the shmem may be
//! corrupted. That side effect is indistinguishable from the
//! postmaster_restart scenario (covered by its own binary). SIGTERM
//! via pg_terminate_backend doesn't actually abort the committer's
//! tx mid-pipeline because PG's ProcDie handler only fires at the
//! next CHECK_FOR_INTERRUPTS, which the BackgroundWorker::transaction
//! commit path doesn't reliably poll before COMMIT. So we exercise the
//! recovery code path directly by mutating shmem into the orphan
//! configuration via test_hook, then asserting the sweep restores it.
//! The recovery algorithm itself has unit coverage in router.rs (14
//! sweep_queue_phase / sweep_staging_phase tests); this binary proves
//! the end-to-end wiring (sweep callable from SPI, takeover_count
//! observability accurate, post-sweep CQ state recoverable).

mod common;

use common::*;
use std::time::Duration;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed (WITH_TEST_HOOKS=1)"]
async fn synthetic_orphan_cq_recovered_by_boot_sweep() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );

    let before_takeovers: i64 =
        sqlx::query_scalar("SELECT ledger_routed_committer_takeover_count()")
            .fetch_one(&pool)
            .await
            .unwrap();
    let before_sweep_idempotent: i64 =
        sqlx::query_scalar("SELECT ledger_routed_test_run_router_recovery_sweep()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        before_sweep_idempotent, 0,
        "sweep on clean state must reconcile 0 entries"
    );

    sqlx::query("SELECT ledger_routed_test_set_bgworker_paused(true)")
        .execute(&pool)
        .await
        .unwrap();
    // Brief wait so any in-flight tick from before the pause drains.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let cq_idx: i32 = sqlx::query_scalar("SELECT ledger_routed_test_inject_orphan_cq()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(cq_idx >= 0, "must find a free CQ slot to mark as orphan");

    // Sweep synchronously. With the BGWs paused, no racing committer
    // can intervene. Sweep MUST find the orphan and revert it.
    let reclaimed: i64 =
        sqlx::query_scalar("SELECT ledger_routed_test_run_router_recovery_sweep()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        reclaimed >= 1,
        "sweep must reconcile at least 1 entry (the injected orphan): got {reclaimed}"
    );

    let after_takeovers: i64 =
        sqlx::query_scalar("SELECT ledger_routed_committer_takeover_count()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        after_takeovers > before_takeovers,
        "committer_takeover_count must register the orphan reclaim: \
         before={before_takeovers} after={after_takeovers}"
    );

    // Idempotency: a second sweep finds nothing more to reclaim.
    let reclaimed_again: i64 =
        sqlx::query_scalar("SELECT ledger_routed_test_run_router_recovery_sweep()")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reclaimed_again, 0, "sweep is idempotent");

    sqlx::query("SELECT ledger_routed_test_set_bgworker_paused(false)")
        .execute(&pool)
        .await
        .unwrap();

    // After unpausing, the system must still process a real submission
    // end-to-end — proves the synthetic mutation + sweep didn't leave
    // shmem in a corrupted state that breaks subsequent traffic.
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;
    let _ = enqueue(
        &pool,
        "po_receipt",
        9301,
        "2026-05-22T12:00:00+00:00",
        &po_receipt_line(pool_id, 4, 25, inv, ap),
    )
    .await
    .expect("post-recovery enqueue");
    assert!(
        poll_for_trx(&pool, "po_receipt", 9301, Duration::from_secs(15))
            .await
            .is_some(),
        "system must process a fresh submission after orphan recovery"
    );
}
