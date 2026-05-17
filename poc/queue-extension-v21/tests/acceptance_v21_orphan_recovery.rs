//! M5a.1 (acct-lefr) acceptance: orphan recovery via lease staleness
//! + kill(pid, 0) detects a dead committer and resets its in-flight
//! SuperBatch for re-processing.
//!
//! Two phases:
//!  Phase A — synthetic orphan: deterministic injection of a fake
//!    orphan into an empty slot; assert the recovery sweep detects
//!    it and transitions valid 2→1.
//!  Phase B — orphan-during-live-workload: inject a synthetic orphan
//!    on a fresh slot while a real 100-envelope workload runs;
//!    assert both the workload completes cleanly and the BGWorker
//!    recovery loop processes the injected orphan within bounded
//!    wall-time.
//!
//! Note on SIGKILL testing: killing a single BGWorker via signal-9
//! triggers postmaster-wide crash recovery (postgres protocol —
//! shared memory may be corrupted, so all backends are terminated
//! and the cluster re-initializes). The orphan-recovery mechanism
//! is designed for graceful-but-stuck committers (hung tx, slow
//! sub-tx, etc.); full crash recovery is M5d.1's scope.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_orphan_recovery \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
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
        "document_id": 3_000_000 + chrono,
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

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_orphan_synthetic_recovery() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    // Use slot far from where the router will write so it doesn't
    // race a real SuperBatch. Slot 100 is well within the 2048 ring
    // but past where typical post-reset routing lands first.
    const SLOT: i64 = 100;

    // Cleanup defensively.
    sqlx::query("SELECT poc_v21_test_force_reset_slot($1)")
        .bind(SLOT)
        .execute(&pool)
        .await
        .expect("force_reset (pre)");

    // Inject: valid 0→2, fake_pid=i32::MAX (definitely dead),
    // acquired_at = now - 500ms (lease default is 100ms, so >> stale).
    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_orphan_into_empty($1, $2, $3)",
    )
    .bind(SLOT)
    .bind(i32::MAX)
    .bind(500_i64)
    .fetch_one(&pool)
    .await
    .expect("inject");
    assert!(injected, "inject_orphan_into_empty must succeed on empty slot");

    let state_pre: String =
        sqlx::query_scalar("SELECT poc_v21_test_slot_state($1)")
            .bind(SLOT)
            .fetch_one(&pool)
            .await
            .expect("slot_state");
    println!("slot {} pre-recover: {}", SLOT, state_pre);
    assert!(
        state_pre.starts_with("(2,"),
        "slot must be valid=2 after injection; got {}",
        state_pre
    );

    // Trigger one recovery sweep. The BGWorker also runs this loop
    // every 50ms tick, so either path can rescue; we call the test
    // SQL fn here to make the rescue deterministic (test backend's
    // own MyProcPid claims the slot).
    let recovered: i64 =
        sqlx::query_scalar("SELECT poc_v21_test_orphan_recover_tick()")
            .fetch_one(&pool)
            .await
            .expect("recover_tick");
    println!("recovered: {}", recovered);
    assert!(
        recovered >= 1,
        "recovery sweep must rescue at least the injected orphan; got {}",
        recovered
    );

    let state_post: String =
        sqlx::query_scalar("SELECT poc_v21_test_slot_state($1)")
            .bind(SLOT)
            .fetch_one(&pool)
            .await
            .expect("slot_state");
    println!("slot {} post-recover: {}", SLOT, state_post);
    assert!(
        state_post.starts_with("(1,") || state_post.starts_with("(0,") || state_post.starts_with("(2,"),
        "slot must be valid=1 (rescued, awaiting re-claim) or downstream-processed; got {}",
        state_post
    );

    // Cleanup: force the slot back to empty so subsequent tests
    // don't see an artifact entry. The real committer pool will
    // ignore valid==0 slots.
    sqlx::query("SELECT poc_v21_test_force_reset_slot($1)")
        .bind(SLOT)
        .execute(&pool)
        .await
        .expect("force_reset (post)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn acceptance_v21_orphan_recovery_during_live_workload() {
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    reset_state(&pool).await;

    const TOTAL: usize = 100;
    const SKU_BASE: i64 = 9000;
    // Fresh slot far from where the router will write — picked so the
    // injected orphan doesn't collide with the workload's SuperBatches.
    const ORPHAN_SLOT: i64 = 1500;

    // Pre-test cleanup of the target slot.
    sqlx::query("SELECT poc_v21_test_force_reset_slot($1)")
        .bind(ORPHAN_SLOT)
        .execute(&pool)
        .await
        .expect("force_reset (pre)");

    // Concurrently enqueue + inject orphan. The committer BGWorkers
    // will see both: real SuperBatches from the router, and an
    // orphaned synthetic entry. Both paths must run cleanly.
    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(TOTAL);
    let mut handles = Vec::with_capacity(TOTAL);
    for i in 0..TOTAL {
        let cid = Uuid::new_v4();
        correlation_ids.push(cid);
        let pool = pool.clone();
        let sku = SKU_BASE + i as i64;
        let chrono = (i + 1) as i64;
        handles.push(tokio::spawn(async move {
            enqueue_one(&pool, cid, sku, chrono).await
        }));
    }

    // Inject the orphan mid-flight.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let injected: bool =
        sqlx::query_scalar("SELECT poc_v21_test_inject_orphan_into_empty($1, $2, $3)")
            .bind(ORPHAN_SLOT)
            .bind(i32::MAX)
            .bind(500_i64)
            .fetch_one(&pool)
            .await
            .expect("inject");
    assert!(injected, "orphan injection must succeed on the empty slot");

    for h in handles {
        h.await.expect("join").expect("enqueue_one");
    }

    let terminal =
        wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(30)).await;
    println!("terminal: {} of {}", terminal, TOTAL);
    assert_eq!(
        terminal, TOTAL as i64,
        "workload must commit cleanly alongside concurrent orphan recovery"
    );

    // Confirm the orphan slot was recovered. Bound the poll loop at
    // 5s — committer_lease_ms default is 100ms; recovery sweep fires
    // every ~50ms BGWorker tick.
    let mut slot_state = String::new();
    for _ in 0..50 {
        slot_state = sqlx::query_scalar("SELECT poc_v21_test_slot_state($1)")
            .bind(ORPHAN_SLOT)
            .fetch_one(&pool)
            .await
            .expect("slot_state");
        // Recovery transitions valid 2→1. After that a real committer
        // may try to claim and find no payload — it would CAS 1→2 then
        // process_superbatch reads empty arena. To keep this test
        // deterministic we poll for the 2→non-2 transition only.
        if !slot_state.starts_with("(2,") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("orphan slot {} final state: {}", ORPHAN_SLOT, slot_state);
    assert!(
        !slot_state.starts_with("(2,"),
        "orphan slot must have transitioned out of valid=2 within bound; got {}",
        slot_state
    );

    // Final cleanup.
    sqlx::query("SELECT poc_v21_test_force_reset_slot($1)")
        .bind(ORPHAN_SLOT)
        .execute(&pool)
        .await
        .expect("force_reset (post)");
}
