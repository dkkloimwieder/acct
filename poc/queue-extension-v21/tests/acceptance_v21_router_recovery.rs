//! M5b.1 (acct-k7b2) acceptance: router boot recovery sweep + the
//! 3-state-staging-entry transitions defined in spec §3.3.
//!
//! Five phases:
//!
//!   Phase A — staging at (valid=2, sb_id=0): router died before
//!     reaching Phase 6 of its pack. Sweep CASs staging 2→1. Re-routing
//!     happens on the next router tick.
//!
//!   Phase B — staging at (valid=2, sb_id=<not in any live queue>):
//!     queue was already cleaned by a fast committer but staging slot
//!     somehow stayed routed. Sweep reverts staging 2→1 + resets sb_id.
//!
//!   Phase C — completed queue (valid=3) + dead committer + linked
//!     staging entries: committer committed mid-tx then died before
//!     finishing shmem cleanup. Sweep takes ownership: frees per-staging
//!     arena, CAS staging→0; frees queue's OWN arena (the §3.3 leak
//!     source spec calls out as easy-to-miss); CAS queue 3→0; UPSERTs
//!     submission_status='committed' for the SuperBatch's correlation_ids.
//!
//!   Phase D — completed queue (valid=3) + ALIVE committer (test
//!     backend's MyProcPid): sweep leaves alone. Forming the negative
//!     control: kill(0) returning 0 means "process exists" and we must
//!     not steal a slot from a committer mid-cleanup.
//!
//!   Phase E — live 100-envelope workload + concurrent synthetic
//!     staging-orphan injection mid-flight: workload completes cleanly;
//!     explicit recover_tick reclaims the synthetic orphan; arena
//!     outstanding returns to baseline. Confirms the sweep is safe to
//!     run alongside live committer pool traffic.
//!
//! Note on SIGKILL testing: killing the router BGWorker via signal-9
//! triggers postmaster-wide crash recovery (the same constraint M5a.1
//! documented for committer SIGKILL — postgres treats single-backend
//! death as potential shared-memory corruption). The orphan-recovery
//! mechanism is designed for graceful-but-stuck routers (panic, hung
//! tx, scheduler timeout); full crash recovery is M5d.1's scope. Synthetic
//! injection exercises the recovery logic without triggering that.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_recovery \
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
        "document_id": 5_000_000 + chrono,
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

async fn arena_outstanding(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_arena_outstanding()")
        .fetch_one(pool)
        .await
        .expect("arena_outstanding")
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_phase_a_staging_no_sb_id() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    const SLOT: i64 = 3001;

    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(SLOT)
        .execute(&pool)
        .await
        .expect("pre-cleanup");

    let cid = Uuid::new_v4();
    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_staging_orphan($1, 0, $2::uuid)",
    )
    .bind(SLOT)
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("inject");
    assert!(injected, "inject_staging_orphan must succeed on empty slot");

    let pre: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(SLOT)
        .fetch_one(&pool)
        .await
        .expect("pre state");
    println!("phase A pre: {pre}");
    assert_eq!(pre, "(2, 0)", "must be valid=2, sb_id=0");

    let recovered: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_recover_tick()")
        .fetch_one(&pool)
        .await
        .expect("recover_tick");
    println!("phase A recovered: {recovered}");
    assert!(
        recovered >= 1,
        "sweep must reconcile at least the injected orphan; got {recovered}"
    );

    let post: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(SLOT)
        .fetch_one(&pool)
        .await
        .expect("post state");
    println!("phase A post: {post}");
    assert!(
        post.starts_with("(1,") || post.starts_with("(0,") || post.starts_with("(3,"),
        "must transition out of valid=2; got {post}"
    );

    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(SLOT)
        .execute(&pool)
        .await
        .expect("post-cleanup");
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_phase_b_staging_orphan_link() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    const SLOT: i64 = 3501;
    // sb_id chosen to be far outside any live SuperBatch counter (the
    // router starts from 1 and increments). 999_999_999 is well-past
    // anything the bake-off shapes will hit.
    const ORPHAN_SB_ID: i64 = 999_999_999;

    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(SLOT)
        .execute(&pool)
        .await
        .expect("pre-cleanup");

    let cid = Uuid::new_v4();
    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_staging_orphan($1, $2, $3::uuid)",
    )
    .bind(SLOT)
    .bind(ORPHAN_SB_ID)
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("inject");
    assert!(injected);

    let pre: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(SLOT)
        .fetch_one(&pool)
        .await
        .expect("pre state");
    println!("phase B pre: {pre}");
    assert_eq!(pre, format!("(2, {ORPHAN_SB_ID})"));

    let recovered: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_recover_tick()")
        .fetch_one(&pool)
        .await
        .expect("recover_tick");
    println!("phase B recovered: {recovered}");
    assert!(recovered >= 1, "sweep must reconcile orphan-link; got {recovered}");

    let post: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(SLOT)
        .fetch_one(&pool)
        .await
        .expect("post state");
    println!("phase B post: {post}");
    // Recovery resets sb_id to 0 on revert; valid moves out of 2.
    assert!(
        post == "(1, 0)" || post == "(0, 0)",
        "must transition to valid=1 (pending) or valid=0 (subsequently re-routed) with sb_id=0; got {post}"
    );

    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(SLOT)
        .execute(&pool)
        .await
        .expect("post-cleanup");
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_phase_c_completed_queue_dead_committer() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    const QUEUE_IDX: i64 = 1501;
    const STAGING_A: i64 = 4001;
    const STAGING_B: i64 = 4002;
    const SB_ID: i64 = 42_424_242;

    sqlx::query("SELECT poc_v21_test_force_reset_injected_queue($1)")
        .bind(QUEUE_IDX)
        .execute(&pool)
        .await
        .expect("queue pre-cleanup");
    for s in [STAGING_A, STAGING_B] {
        sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
            .bind(s)
            .execute(&pool)
            .await
            .expect("staging pre-cleanup");
    }
    sqlx::query("TRUNCATE poc_v21_submission_status")
        .execute(&pool)
        .await
        .expect("truncate status");

    let baseline = arena_outstanding(&pool).await;
    let corr_a = Uuid::new_v4();
    let corr_b = Uuid::new_v4();

    sqlx::query("SELECT poc_v21_test_inject_staging_orphan($1, $2, $3::uuid)")
        .bind(STAGING_A)
        .bind(SB_ID)
        .bind(corr_a)
        .execute(&pool)
        .await
        .expect("inject staging A");
    sqlx::query("SELECT poc_v21_test_inject_staging_orphan($1, $2, $3::uuid)")
        .bind(STAGING_B)
        .bind(SB_ID)
        .bind(corr_b)
        .execute(&pool)
        .await
        .expect("inject staging B");

    // Seed submission_status rows in the 'queued' state to simulate
    // the caller_intx default (status row exists pre-commit) so the
    // sweep's UPSERT can transition them. The bd description's
    // "to 'committed'" semantics rely on the row existing.
    sqlx::query(
        "INSERT INTO poc_v21_submission_status (correlation_id, state, enqueued_at) \
         VALUES ($1, 'queued', now()), ($2, 'queued', now())",
    )
    .bind(corr_a)
    .bind(corr_b)
    .execute(&pool)
    .await
    .expect("seed submission_status");

    let injected_queue: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_orphaned_queue($1, $2, $3, $4::bigint[])",
    )
    .bind(QUEUE_IDX)
    .bind(SB_ID)
    .bind(i32::MAX) // definitely dead PID
    .bind(vec![STAGING_A, STAGING_B])
    .fetch_one(&pool)
    .await
    .expect("inject queue");
    assert!(injected_queue);

    let after_inject = arena_outstanding(&pool).await;
    println!(
        "phase C baseline={baseline} after_inject={after_inject}"
    );
    assert!(
        after_inject > baseline,
        "injection must allocate queue arena (staging_entry_offsets); got {after_inject}"
    );

    let pre_queue: String = sqlx::query_scalar("SELECT poc_v21_test_queue_state($1)")
        .bind(QUEUE_IDX)
        .fetch_one(&pool)
        .await
        .expect("pre queue");
    println!("phase C pre queue: {pre_queue}");
    assert!(pre_queue.starts_with("(3,"), "must be valid=3; got {pre_queue}");

    let recovered: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_recover_tick()")
        .fetch_one(&pool)
        .await
        .expect("recover_tick");
    println!("phase C recovered: {recovered}");
    assert!(recovered >= 1, "sweep must take ownership of completed orphan");

    let post_queue: String = sqlx::query_scalar("SELECT poc_v21_test_queue_state($1)")
        .bind(QUEUE_IDX)
        .fetch_one(&pool)
        .await
        .expect("post queue");
    println!("phase C post queue: {post_queue}");
    assert!(
        post_queue.starts_with("(0,"),
        "queue must reset to valid=0; got {post_queue}"
    );
    let post_a: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(STAGING_A)
        .fetch_one(&pool)
        .await
        .expect("post staging A");
    let post_b: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(STAGING_B)
        .fetch_one(&pool)
        .await
        .expect("post staging B");
    println!("phase C post staging A={post_a} B={post_b}");
    assert!(post_a.starts_with("(0,"), "staging A must reset; got {post_a}");
    assert!(post_b.starts_with("(0,"), "staging B must reset; got {post_b}");

    let after_sweep = arena_outstanding(&pool).await;
    println!("phase C arena after sweep: {after_sweep}");
    assert_eq!(
        after_sweep, baseline,
        "queue-owned arena (staging_entry_offsets + sku/wip mirrors) must be \
         freed by sweep — §3.3 calls this out as an easy-to-miss leak source"
    );

    let states: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT correlation_id, state FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[]) \
          ORDER BY correlation_id",
    )
    .bind(vec![corr_a, corr_b])
    .fetch_all(&pool)
    .await
    .expect("status rows");
    for (c, s) in &states {
        println!("phase C status {c}: {s}");
        assert_eq!(s, "committed", "sweep must UPSERT status to committed");
    }
    assert_eq!(states.len(), 2, "both correlation_ids must have committed rows");
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_phase_d_completed_queue_alive_committer() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    const QUEUE_IDX: i64 = 1601;
    const STAGING_IDX: i64 = 5101;
    const SB_ID: i64 = 55_555_555;

    sqlx::query("SELECT poc_v21_test_force_reset_injected_queue($1)")
        .bind(QUEUE_IDX)
        .execute(&pool)
        .await
        .expect("queue pre-cleanup");
    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(STAGING_IDX)
        .execute(&pool)
        .await
        .expect("staging pre-cleanup");

    let cid = Uuid::new_v4();
    sqlx::query("SELECT poc_v21_test_inject_staging_orphan($1, $2, $3::uuid)")
        .bind(STAGING_IDX)
        .bind(SB_ID)
        .bind(cid)
        .execute(&pool)
        .await
        .expect("inject staging");

    // Use the test backend's MyProcPid — definitely alive.
    let my_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("pg_backend_pid");

    let injected: bool = sqlx::query_scalar(
        "SELECT poc_v21_test_inject_orphaned_queue($1, $2, $3, $4::bigint[])",
    )
    .bind(QUEUE_IDX)
    .bind(SB_ID)
    .bind(my_pid)
    .bind(vec![STAGING_IDX])
    .fetch_one(&pool)
    .await
    .expect("inject queue");
    assert!(injected);

    let recovered: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_recover_tick()")
        .fetch_one(&pool)
        .await
        .expect("recover_tick");
    println!("phase D recovered (alive committer): {recovered}");

    let post_queue: String = sqlx::query_scalar("SELECT poc_v21_test_queue_state($1)")
        .bind(QUEUE_IDX)
        .fetch_one(&pool)
        .await
        .expect("post queue");
    let post_staging: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(STAGING_IDX)
        .fetch_one(&pool)
        .await
        .expect("post staging");
    println!("phase D post queue={post_queue} staging={post_staging}");

    // Alive committer = sweep must leave everything alone.
    assert!(
        post_queue.starts_with("(3,"),
        "alive committer => queue stays at valid=3; got {post_queue}"
    );
    assert!(
        post_staging.starts_with("(2,"),
        "alive committer => staging stays at valid=2; got {post_staging}"
    );

    // Manual cleanup: free queue's arena, reset staging.
    sqlx::query("SELECT poc_v21_test_force_reset_injected_queue($1)")
        .bind(QUEUE_IDX)
        .execute(&pool)
        .await
        .expect("queue cleanup");
    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(STAGING_IDX)
        .execute(&pool)
        .await
        .expect("staging cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn acceptance_v21_phase_e_live_workload_with_injection() {
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    reset_state(&pool).await;

    const TOTAL: usize = 100;
    const SKU_BASE: i64 = 12_000;
    const ORPHAN_STAGING_IDX: i64 = 6500;

    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(ORPHAN_STAGING_IDX)
        .execute(&pool)
        .await
        .expect("orphan pre-cleanup");

    let baseline = arena_outstanding(&pool).await;
    println!("phase E baseline arena: {baseline}");

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

    // Inject the staging orphan mid-flight at a slot far from where
    // the router writes (router head starts at 0 and increments).
    tokio::time::sleep(Duration::from_millis(15)).await;
    let orphan_cid = Uuid::new_v4();
    sqlx::query("SELECT poc_v21_test_inject_staging_orphan($1, 0, $2::uuid)")
        .bind(ORPHAN_STAGING_IDX)
        .bind(orphan_cid)
        .execute(&pool)
        .await
        .expect("inject orphan");

    for h in handles {
        h.await.expect("join").expect("enqueue_one");
    }

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(30)).await;
    println!("phase E terminal: {terminal} of {TOTAL}");
    assert_eq!(
        terminal, TOTAL as i64,
        "live workload must complete cleanly alongside concurrent synthetic injection"
    );

    // Run an explicit sweep tick to recover the orphan.
    let recovered: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_recover_tick()")
        .fetch_one(&pool)
        .await
        .expect("recover_tick");
    println!("phase E recovered: {recovered}");

    let post_orphan: String = sqlx::query_scalar("SELECT poc_v21_test_staging_state($1)")
        .bind(ORPHAN_STAGING_IDX)
        .fetch_one(&pool)
        .await
        .expect("post orphan state");
    println!("phase E post orphan: {post_orphan}");
    assert!(
        !post_orphan.starts_with("(2,"),
        "orphan must transition out of valid=2; got {post_orphan}"
    );

    // Force-reset the slot and verify arena returns to baseline.
    sqlx::query("SELECT poc_v21_test_force_reset_staging($1)")
        .bind(ORPHAN_STAGING_IDX)
        .execute(&pool)
        .await
        .expect("orphan post-cleanup");

    let mut after = baseline;
    for _ in 0..50 {
        after = arena_outstanding(&pool).await;
        if after == baseline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("phase E arena after settle: {after}");
    assert_eq!(
        after, baseline,
        "arena must return to baseline after workload + synthetic injection are reconciled"
    );
}
