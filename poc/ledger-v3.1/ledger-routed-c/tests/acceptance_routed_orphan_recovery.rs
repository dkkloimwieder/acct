//! acct-2ttr.7 §6.5 + §6.8 acceptance: recovery + committer SQL error handling.
//!
//! Four scenarios:
//!   (a) committer-death mid-group → the router boot sweep reclaims the orphaned
//!       commit_group (CQ valid 2→1) so a live committer reprocesses it; trx rows
//!       for the group end up all-exist-or-all-absent (pre-flight dedup is the
//!       recovery source of truth — no pristine-replay).
//!   (b) a transient SQLSTATE (40P01 deadlock) during the write phase is retried
//!       with backoff and then commits; `deadlock_retries_total` advances.
//!   (c) a non-retryable SQLSTATE poisons the commit_group: it lands in the
//!       terminal valid==4 dead-letter state, `poisoned_total` advances, the
//!       submissions are lost (no trx).
//!   (d) postmaster-crash semantics (lightweight): the recovery worker signals
//!       `recovery_complete` at boot, no in_flight entry is stuck at rest, and the
//!       system processes fresh submissions.
//!
//! Why a SYNTHETIC orphan and not a real committer SIGKILL (scenario a): PG's
//! crash-recovery contract restarts ALL backends if a BGW exits unclean
//! (SIGSEGV/SIGKILL) — shmem may be corrupt — which is indistinguishable from a
//! postmaster restart. SIGTERM (pg_terminate_backend) doesn't abort the
//! committer's tx mid-pipeline because ProcDie only fires at the next
//! CHECK_FOR_INTERRUPTS, which the commit path doesn't reliably poll before
//! COMMIT. So we drive the recovery CODE PATH directly: mutate shmem into the
//! orphan configuration via a test hook, then assert the sweep restores it. The
//! sweep algorithm has unit coverage in router.rs (the restamp/revert tests);
//! this binary proves the end-to-end wiring (sweep callable from SPI,
//! takeover_count accurate, post-sweep traffic flows).
//!
//! Requires the `test_hooks` build (the cluster-per-binary runner installs it).

mod common;

use common::*;
use sqlx::PgPool;
use std::time::Duration;

/// Poll until `ledger_routed_c_committer_<name>()` advances by ≥ `delta` beyond
/// `base`, or panic on timeout.
async fn await_committer_stat(pool: &PgPool, name: &str, base: i64, delta: i64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if committer_stat(pool, name).await - base >= delta {
            return;
        }
        if std::time::Instant::now() >= deadline {
            let now = committer_stat(pool, name).await;
            panic!("timed out waiting for {name} to advance by {delta} beyond {base} (now {now})");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn synthetic_orphan_cq_recovered_by_boot_sweep() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        await_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal at boot"
    );

    // Park BOTH workers so neither races the synthetic orphan we plant.
    set_router_paused(&pool, true).await;
    set_committer_paused(&pool, true).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let base_takeovers = committer_stat(&pool, "takeover_count").await;

    let cq_idx = inject_orphan_cq(&pool).await;
    assert!(cq_idx >= 0, "must find a free CQ slot to mark as orphan");

    // Sweep synchronously (BGWs parked, no racing committer). It MUST find the
    // orphan (CQ valid==2, dead owner) and revert it to ready (valid==1).
    let reclaimed = run_router_recovery_sweep(&pool).await;
    assert!(
        reclaimed >= 1,
        "sweep must reconcile ≥1 entry (the injected orphan): got {reclaimed}"
    );
    assert!(
        committer_stat(&pool, "takeover_count").await - base_takeovers >= 1,
        "committer_takeover_count must register the orphan reclaim"
    );

    // Idempotent: a second sweep finds nothing more.
    assert_eq!(run_router_recovery_sweep(&pool).await, 0, "sweep is idempotent");

    set_committer_paused(&pool, false).await;
    set_router_paused(&pool, false).await;

    // The synthetic mutation + sweep must not have corrupted shmem: a fresh
    // submission still flows end-to-end.
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;
    enqueue(&pool, "po_receipt", 7_001, vec![receipt_line_for(&f, 4, 25)])
        .await
        .expect("post-recovery enqueue");
    await_trx_count(&pool, 1).await;
    assert_eq!(
        trx_count_for_source(&pool, 7_001).await,
        1,
        "system processes a fresh submission after orphan recovery"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn deadlock_during_write_retries_then_commits() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;

    let base_retries = committer_stat(&pool, "deadlock_retries_total").await;
    let base_poisoned = committer_stat(&pool, "poisoned_total").await;

    // Route the group with the committer parked, THEN arm two deadlocks so the
    // injection lands on exactly this group's write phase.
    route_with_committer_paused(&pool, || async {
        enqueue(&pool, "po_receipt", 7_101, vec![receipt_line_for(&f, 10, 500)])
            .await
            .expect("enqueue receipt");
    })
    .await;

    set_inject_deadlock_count(&pool, 2).await;
    set_committer_paused(&pool, false).await;

    // The first two write attempts raise 40P01 (rolled back + retried); the third
    // commits.
    await_trx_count(&pool, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        trx_count_for_source(&pool, 7_101).await,
        1,
        "the submission commits after the deadlock retries"
    );
    assert_eq!(
        committer_stat(&pool, "deadlock_retries_total").await - base_retries,
        2,
        "exactly two deadlock-driven retries were recorded"
    );
    assert_eq!(
        committer_stat(&pool, "poisoned_total").await - base_poisoned,
        0,
        "a retried-then-committed group is NOT poisoned"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((10, 500)),
        "the receipt applied exactly once despite the retries"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn fatal_error_during_write_poisons_commit_group() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;

    let base_poisoned = committer_stat(&pool, "poisoned_total").await;
    let base_poisoned_slots = committer_queue_count(&pool, "poisoned").await;

    route_with_committer_paused(&pool, || async {
        enqueue(&pool, "po_receipt", 7_201, vec![receipt_line_for(&f, 10, 500)])
            .await
            .expect("enqueue receipt");
    })
    .await;

    set_inject_fatal(&pool, true).await;
    set_committer_paused(&pool, false).await;

    // The write phase raises a non-retryable SQLSTATE → the group is poisoned.
    await_committer_stat(&pool, "poisoned_total", base_poisoned, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        committer_stat(&pool, "poisoned_total").await - base_poisoned,
        1,
        "exactly one commit_group was poisoned"
    );
    assert!(
        committer_queue_count(&pool, "poisoned").await - base_poisoned_slots >= 1,
        "a CQ entry sits at the terminal valid==4 dead-letter state"
    );
    assert_eq!(
        trx_count_for_source(&pool, 7_201).await,
        0,
        "the poisoned submission is lost — no trx"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((0, 0)),
        "nothing was applied to the aggregate"
    );

    // The poison flag is one-shot: the system keeps processing fresh submissions.
    enqueue(&pool, "po_receipt", 7_202, vec![receipt_line_for(&f, 3, 100)])
        .await
        .expect("post-poison enqueue");
    await_trx_count(&pool, 1).await;
    assert_eq!(
        trx_count_for_source(&pool, 7_202).await,
        1,
        "the committer recovers and processes the next submission"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn recovery_complete_at_boot_and_system_operational() {
    // Postmaster-crash semantics, lightweight: the cluster-per-binary runner
    // restarts the container before this binary, so this exercises a genuine
    // postmaster boot. In-flight work from before the (simulated) crash is gone
    // with shmem; the recovery worker signals complete and the system accepts new
    // submissions.
    let pool = connect_pool().await;
    reset_state(&pool).await;

    assert!(
        await_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery worker must signal recovery_complete at postmaster boot"
    );

    // Let any in-flight work from sibling tests settle, then assert nothing is
    // stuck mid-commit at rest.
    await_no_ready_groups(&pool).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        committer_queue_count(&pool, "in_flight").await,
        0,
        "no commit_group is stuck in_flight at rest"
    );

    // Fresh traffic flows.
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    seed_aggregate(&pool, f.pool_id, 0, 0).await;
    enqueue(&pool, "po_receipt", 7_301, vec![receipt_line_for(&f, 7, 250)])
        .await
        .expect("post-boot enqueue");
    await_trx_count(&pool, 1).await;
    assert_eq!(trx_count_for_source(&pool, 7_301).await, 1);
}
