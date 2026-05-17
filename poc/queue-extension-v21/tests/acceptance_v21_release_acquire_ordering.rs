//! M5b.2 (acct-ud4h) acceptance: data-before-flag (release/acquire)
//! ordering invariant on staging.superbatch_id ↔ staging.valid.
//!
//! The invariant the M5b.1 recovery sweep depends on: if a reader
//! Acquire-loads staging.valid and observes 3 (routed), then a
//! subsequent Acquire-load of staging.superbatch_id MUST return the
//! non-zero value the router stored. Without this pairing, the sweep
//! could observe (valid=3, sb_id=0), mis-classify the entry as
//! "claimed pre-stamp", and CAS-revert it back to pending — causing
//! a live SuperBatch's envelope to be re-routed and re-processed,
//! yielding duplicate cost rows.
//!
//! Two scenarios:
//!
//!   1. Ordering-invariant-holds: with delay-injection enabled (so
//!      the window between the two atomic stores is observable) and
//!      normal order, 0 violations should be observed across a stress
//!      shape of N envelopes routed × M concurrent observer reads.
//!
//!   2. Negative control: with delay-injection AND store-reorder
//!      enabled (CAS valid 2→3 happens BEFORE the sb_id store),
//!      violations should be observable. This proves the test setup
//!      actually exercises the invariant — without the negative
//!      control, the positive case might pass even with Relaxed
//!      ordering on x86's strong memory model.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_release_acquire_ordering \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::reset_state;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";

async fn enqueue_one(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 7_000_000 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("po_receipt")
        .bind(payload)
        .bind(pool_keys)
        .execute(pool)
        .await
        .map(|_| ())
}

async fn reset_test_gucs(pool: &PgPool) {
    sqlx::query("SELECT poc_v21_test_set_router_delay_us(0)")
        .execute(pool)
        .await
        .expect("clear delay");
    sqlx::query("SELECT poc_v21_test_set_router_reorder(false)")
        .execute(pool)
        .await
        .expect("clear reorder");
}

/// Run one stress cycle: set GUCs, enqueue N envelopes, spawn observer
/// + drainer concurrently, return total observed violations.
async fn run_ordering_cycle(
    pool: &PgPool,
    reorder: bool,
    delay_us: i32,
    iterations: usize,
    envs_per_iter: usize,
) -> (i64, usize) {
    sqlx::query("SELECT poc_v21_test_set_router_delay_us($1)")
        .bind(delay_us)
        .execute(pool)
        .await
        .expect("set delay");
    sqlx::query("SELECT poc_v21_test_set_router_reorder($1)")
        .bind(reorder)
        .execute(pool)
        .await
        .expect("set reorder");

    let mut total_violations: i64 = 0;
    let mut total_observer_calls: usize = 0;

    for iter in 0..iterations {
        // Pre-iteration: clear any stale routed state by waiting for
        // committers to drain prior work.
        // Cheap because envelopes are PO receipts with default FIFO.
        let sku_base = 60_000 + (iter * envs_per_iter * 10) as i64;
        let mut cids: Vec<Uuid> = Vec::with_capacity(envs_per_iter);
        for i in 0..envs_per_iter {
            let cid = Uuid::new_v4();
            cids.push(cid);
            let sku = sku_base + i as i64;
            let chrono = iter as i64 * 1000 + i as i64;
            enqueue_one(pool, cid, sku, chrono)
                .await
                .expect("enqueue");
        }

        // Spawn observer task — hot-loops the invariant check until told
        // to stop. Each backend call is its own PG backend (separate
        // sqlx connection), so observer + drainer run concurrently.
        let stop = Arc::new(AtomicBool::new(false));
        let stop_obs = stop.clone();
        let pool_obs = pool.clone();
        let obs_handle = tokio::spawn(async move {
            let mut v: i64 = 0;
            let mut n: usize = 0;
            while !stop_obs.load(Ordering::Relaxed) {
                let r: i64 = sqlx::query_scalar(
                    "SELECT poc_v21_test_check_release_acquire_invariant()",
                )
                .fetch_one(&pool_obs)
                .await
                .unwrap_or(0);
                v += r;
                n += 1;
                if v > 0 && reorder {
                    // Negative control: bail early once we've proven
                    // the invariant is observable; speeds up the test.
                    break;
                }
            }
            (v, n)
        });

        // Drainer: run router_tick synchronously in this backend so we
        // know exactly when the Phase 6 stores happen. The BGWorker may
        // race and pick up envelopes first; both paths exercise the
        // same Phase 6 dispatch on the test GUCs.
        let _ = sqlx::query("SELECT poc_v21_test_router_drain()")
            .execute(pool)
            .await;

        // Give the observer one more pass after the drainer settles so
        // it can catch the final entries' post-CAS state.
        tokio::time::sleep(Duration::from_millis(5)).await;

        stop.store(true, Ordering::Relaxed);
        let (iter_v, iter_n) = obs_handle.await.expect("observer join");
        total_violations += iter_v;
        total_observer_calls += iter_n;

        if reorder && total_violations > 0 {
            // Negative control: one violation is enough. Bail.
            break;
        }
    }

    reset_test_gucs(pool).await;
    (total_violations, total_observer_calls)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn acceptance_v21_release_acquire_invariant_holds() {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    reset_state(&pool).await;
    reset_test_gucs(&pool).await;

    // 1000 iterations × 5 envelopes each = 5000 stamps. With a 200us
    // delay between sb_id.store and CAS 2→3, the per-iteration Phase 6
    // window is ~1ms × 5 = 5ms. Observer hot-loops alongside. Total
    // wall-time ~15-30s depending on dev cluster.
    const ITERATIONS: usize = 1000;
    const ENVS_PER_ITER: usize = 5;
    const DELAY_US: i32 = 200;

    let (violations, observer_calls) =
        run_ordering_cycle(&pool, false, DELAY_US, ITERATIONS, ENVS_PER_ITER).await;
    println!(
        "ordering-invariant-holds: {ITERATIONS} iter × {ENVS_PER_ITER} env / delay={DELAY_US}us \
         → observer_calls={observer_calls} violations={violations}"
    );
    assert!(
        observer_calls > 0,
        "observer must have completed at least one check call; got {observer_calls}"
    );
    assert_eq!(
        violations, 0,
        "release/acquire invariant must hold under normal Phase 6 order; \
         got {violations} violations of (valid=3, sb_id=0)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn acceptance_v21_release_acquire_negative_control_observable() {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    reset_state(&pool).await;
    reset_test_gucs(&pool).await;

    // Negative control: reorder the two stores so CAS valid 2→3 happens
    // BEFORE the sb_id store. Readers should observably catch the
    // (valid=3, sb_id=0) window during the delay. Without this proof
    // the positive test would pass even with weaker ordering on x86's
    // strong memory model.
    //
    // Bigger delay (2ms) + bigger batches so the negative control
    // window is wide. Bails after first observed violation.
    const ITERATIONS: usize = 100;
    const ENVS_PER_ITER: usize = 10;
    const DELAY_US: i32 = 2000;

    let (violations, observer_calls) =
        run_ordering_cycle(&pool, true, DELAY_US, ITERATIONS, ENVS_PER_ITER).await;
    println!(
        "negative-control: {ITERATIONS} iter × {ENVS_PER_ITER} env / delay={DELAY_US}us / reorder=true \
         → observer_calls={observer_calls} violations={violations}"
    );
    assert!(
        violations > 0,
        "negative control with reordered Phase 6 stores must reproduce \
         observable violations; got {violations} — the positive test \
         might pass spuriously"
    );

    // Wait for any residual workload to drain before the next test
    // sees a stained shmem state.
    tokio::time::sleep(Duration::from_millis(200)).await;
}
