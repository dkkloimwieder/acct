//! acct-0at4.15 axis F acceptance: wake-on-enqueue is functionally safe.
//!
//! Axis F adds an optional SetLatch on the router BGWorker from the enqueue
//! backend, gated by `ledger_routed_c.wake_on_enqueue` (default off). When on,
//! a successful enqueue pokes the router (via `BackendPidGetProc(router_pid)` →
//! `SetLatch(procLatch)`) so a staged submission is re-scanned without waiting
//! for the router's 50 ms steady tick.
//!
//! This binary pins the FUNCTIONAL contract: with the wake ON, the raw-FFI wake
//! path executes on every enqueue and must still round-trip to exactly one
//! correct trx — the SetLatch neither drops nor double-processes the submission
//! nor corrupts the pipeline. The LATENCY win itself is measured in the bench
//! (results/POC-REPORT.md): the pure wake floor needs `batch_window_us=0`, which
//! is read by the router BGWorker and only takes effect across a restart — out of
//! reach for an in-process cargo test, whereas the bench restarts per arm.
//!
//! Note this test is robust to GUC-propagation timing by construction: it asserts
//! COMMIT CORRECTNESS, which holds whether or not the wake actually fired (wake
//! off still materializes via the tick). So a lagging Sighup read can never make
//! it flaky — a green run means "wake-on does not break the round-trip", a red
//! run means the wake path corrupted something.

mod common;

use common::*;
use sqlx::PgPool;

/// `ALTER SYSTEM SET`/`RESET` + `pg_reload_conf()`. The enqueue backend is a
/// regular backend, so it applies a Sighup change before its next command.
async fn alter_system(pool: &PgPool, stmt: &str) {
    sqlx::query(stmt).execute(pool).await.expect("ALTER SYSTEM");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await
        .expect("pg_reload_conf");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c preloaded"]
async fn wake_on_enqueue_round_trips_to_one_correct_trx() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;

    // Clean the knob at entry (a prior panic can't leak it in), then turn the wake
    // ON so the enqueue exercises the SetLatch path.
    alter_system(&pool, "ALTER SYSTEM RESET ledger_routed_c.wake_on_enqueue").await;
    alter_system(&pool, "ALTER SYSTEM SET ledger_routed_c.wake_on_enqueue = on").await;

    let _ = enqueue(&pool, "po_receipt", 4242, vec![receipt_line_for(&f, 10, 100)])
        .await
        .expect("enqueue under wake");

    await_trx_count(&pool, 1).await;

    // Reset the knob BEFORE asserting so a failed assert never leaves the ablation
    // flag flipped on for later test binaries in this cluster.
    alter_system(&pool, "ALTER SYSTEM RESET ledger_routed_c.wake_on_enqueue").await;

    assert_eq!(
        trx_count_for_source(&pool, 4242).await,
        1,
        "wake-on enqueue must commit exactly one trx (no drop, no double)"
    );
    assert_eq!(
        aggregate(&pool, f.pool_id).await,
        Some((10, 100)),
        "the single receipt materializes as (qty 10, unit_cost 100)"
    );
}
