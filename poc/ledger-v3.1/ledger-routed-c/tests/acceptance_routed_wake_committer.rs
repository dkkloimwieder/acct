//! acct-0at4.16 acceptance: wake-committer-on-publish is functionally safe.
//!
//! This axis adds an optional SetLatch on every live committer BGWorker from the
//! router, gated by `ledger_routed_c.wake_committer_on_publish` (default off).
//! When on, the router pokes the committers (via `BackendPidGetProc(pid)` →
//! `SetLatch(procLatch)` over the committer identity registry) right after it
//! CAS-publishes a commit_group, so the group is claimed without waiting for a
//! committer's 50 ms steady tick. It complements axis F (`wake_on_enqueue`,
//! which wakes the router): F closes the enqueue→router leg, this closes the
//! router→committer leg.
//!
//! This binary pins the FUNCTIONAL contract: with the wake ON, the router's
//! per-publish wake path executes and the submission must still round-trip to
//! exactly one correct trx. Because the router does not know which of the N
//! committers will CAS-claim the entry it SetLatches *all* live committers, so
//! this also exercises the wake-losers path — the committers that wake, find no
//! claimable entry, and re-park must not double-process or corrupt the pipeline.
//! The LATENCY win is measured in the bench (results/POC-REPORT.md) at
//! `batch_window_us=0`, which is read by the router BGWorker and needs a restart.
//!
//! Robust to GUC-propagation timing by construction: it asserts COMMIT
//! CORRECTNESS, which holds whether or not the wake actually fired (wake off
//! still materializes via the committer tick). The router adopts the Sighup GUC
//! on its next ProcessConfigFile tick, so across the enqueue + await window the
//! wake path is exercised; a lagging read can only let the assertion pass via the
//! tick, never make it flaky.

mod common;

use common::*;
use sqlx::PgPool;

/// `ALTER SYSTEM SET`/`RESET` + `pg_reload_conf()`. Sighup reaches the router
/// BGWorker (which ProcessConfigFile()s at the top of its tick) as well as
/// regular backends.
async fn alter_system(pool: &PgPool, stmt: &str) {
    sqlx::query(stmt).execute(pool).await.expect("ALTER SYSTEM");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await
        .expect("pg_reload_conf");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn wake_committer_on_publish_round_trips_to_one_correct_trx() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;

    // Clean the knob at entry (a prior panic can't leak it in), then turn the
    // committer wake ON so the router exercises the SetLatch-all-committers path
    // on publish.
    alter_system(&pool, "ALTER SYSTEM RESET ledger_routed_c.wake_committer_on_publish").await;
    alter_system(&pool, "ALTER SYSTEM SET ledger_routed_c.wake_committer_on_publish = on").await;

    let _ = enqueue(&pool, "po_receipt", 4343, vec![receipt_line_for(&f, 10, 100)])
        .await
        .expect("enqueue under committer wake");

    await_trx_count(&pool, 1).await;

    // Reset the knob BEFORE asserting so a failed assert never leaves the ablation
    // flag flipped on for later test binaries in this cluster.
    alter_system(&pool, "ALTER SYSTEM RESET ledger_routed_c.wake_committer_on_publish").await;

    assert_eq!(
        trx_count_for_source(&pool, 4343).await,
        1,
        "committer-wake must commit exactly one trx (no drop, no double)"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((10, 100)),
        "the single receipt materializes as (qty 10, unit_cost 100)"
    );
}
