//! acct-pl3b acceptance: `poc_v21_enqueue_batch` batch-ingress API.
//!
//! Scenarios:
//!
//!   1. happy-path: enqueue a 1000-envelope batch of po_receipt
//!      envelopes; assert all 1000 reach state='committed' within 30s.
//!      Validates the bulk submission_status INSERT, the single
//!      SPILLOVER_ARENA acquire, the single STAGING_QUEUE acquire,
//!      and downstream router/committer drain.
//!
//!   2. fail-atomic: a batch with one malformed envelope (missing
//!      event_type) aborts the whole call. No submission_status rows
//!      are written; no staging entries are created; arena outstanding
//!      bytes are unchanged.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_enqueue_batch \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::reset_state;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};
use uuid::Uuid;

const POC_DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue_v21";

async fn pre_seed_sku_method_assignments(pool: &PgPool, skus: &[i64]) {
    for sku in skus {
        sqlx::query(
            "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
             VALUES ($1, 'std') \
             ON CONFLICT (sku_id) DO NOTHING",
        )
        .bind(*sku)
        .execute(pool)
        .await
        .expect("seed sku_method_assignments");

        sqlx::query(
            "INSERT INTO poc_v21_standard_costs (sku_id, location_id, unit_cost, effective_from) \
             VALUES ($1, 1, 100, now() - interval '1 day') \
             ON CONFLICT (sku_id, location_id, effective_from) DO NOTHING",
        )
        .bind(*sku)
        .execute(pool)
        .await
        .expect("seed standard_costs");
    }
}

async fn arena_outstanding(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_arena_outstanding()")
        .fetch_one(pool)
        .await
        .expect("arena_outstanding")
}

async fn terminal_count(pool: &PgPool) -> (i64, i64) {
    let row: (i64, i64) = sqlx::query_as(
        "SELECT \
           COUNT(*) FILTER (WHERE state = 'committed'), \
           COUNT(*) FILTER (WHERE state IN ('failed', 'replayed')) \
         FROM poc_v21_submission_status",
    )
    .fetch_one(pool)
    .await
    .expect("terminal_count");
    row
}

fn make_envelopes(n: usize) -> serde_json::Value {
    let mut arr = Vec::with_capacity(n);
    for i in 0..n {
        let sku = ((i % 1000) + 1) as i64;
        let cid = Uuid::new_v4();
        arr.push(serde_json::json!({
            "correlation_id": cid.to_string(),
            "event_type": "po_receipt",
            "payload": {
                "sku_id": sku,
                "location_id": 1,
                "qty": 5,
                "unit_cost": 100,
                "business_date_jdate": 9999,
                "doc_chrono": (i + 1) as i64,
                "document_id": 9_000_000 + i as i64,
            },
            "pool_keys": { "sku": [[sku, 1]], "wip": [] },
        }));
    }
    serde_json::Value::Array(arr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn acceptance_v21_enqueue_batch_happy_path_1000() {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    reset_state(&pool).await;

    let skus: Vec<i64> = (1..=1000).collect();
    pre_seed_sku_method_assignments(&pool, &skus).await;

    let envelopes = make_envelopes(1000);

    let t0 = Instant::now();
    let count: i64 = sqlx::query_scalar("SELECT poc_v21_enqueue_batch($1::jsonb, false)")
        .bind(&envelopes)
        .fetch_one(&pool)
        .await
        .expect("enqueue_batch");
    let submit_ms = t0.elapsed().as_millis();
    assert_eq!(count, 1000, "enqueue_batch should return N");
    println!("submitted 1000 envelopes in {submit_ms} ms");

    // Wait for terminal state.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut committed: i64 = 0;
    let mut failed: i64 = 0;
    while Instant::now() < deadline {
        let (c, f) = terminal_count(&pool).await;
        committed = c;
        failed = f;
        if c + f >= 1000 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let drain_ms = t0.elapsed().as_millis();
    println!("terminal: committed={committed} failed={failed} after {drain_ms} ms total");
    assert_eq!(committed, 1000, "all 1000 should commit");
    assert_eq!(failed, 0, "none should fail");

    // Arena should be drained back to ~0 outstanding bytes (some slack
    // from leftover BGWorker activity is fine).
    let outstanding = arena_outstanding(&pool).await;
    assert!(
        outstanding < 1024 * 1024,
        "arena should be ~empty after drain, got {outstanding} bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn acceptance_v21_enqueue_batch_fail_atomic_on_malformed() {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(POC_DSN)
        .await
        .expect("connect");
    reset_state(&pool).await;

    let baseline_outstanding = arena_outstanding(&pool).await;
    let (baseline_committed, baseline_failed) = terminal_count(&pool).await;
    let baseline_status: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM poc_v21_submission_status")
            .fetch_one(&pool)
            .await
            .expect("status count");

    // Build a 5-envelope batch where envelope[2] is missing event_type.
    let mut arr = vec![];
    for i in 0..5 {
        let mut env = serde_json::json!({
            "correlation_id": Uuid::new_v4().to_string(),
            "event_type": "po_receipt",
            "payload": {
                "sku_id": 1, "location_id": 1, "qty": 5, "unit_cost": 100,
                "business_date_jdate": 9999, "doc_chrono": (i + 1) as i64,
                "document_id": 9_500_000 + i as i64,
            },
            "pool_keys": { "sku": [[1, 1]], "wip": [] },
        });
        if i == 2 {
            env.as_object_mut().unwrap().remove("event_type");
        }
        arr.push(env);
    }
    let envelopes = serde_json::Value::Array(arr);

    let result = sqlx::query("SELECT poc_v21_enqueue_batch($1::jsonb, false)")
        .bind(&envelopes)
        .execute(&pool)
        .await;

    assert!(result.is_err(), "malformed envelope should raise ERROR");
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("envelope[2]") && msg.contains("event_type"),
        "error should reference envelope index 2 and event_type, got: {msg}"
    );

    // Nothing should have changed.
    let post_outstanding = arena_outstanding(&pool).await;
    let (post_committed, post_failed) = terminal_count(&pool).await;
    let post_status: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM poc_v21_submission_status")
            .fetch_one(&pool)
            .await
            .expect("status count");

    assert_eq!(
        post_outstanding, baseline_outstanding,
        "arena outstanding bytes should be unchanged"
    );
    assert_eq!(post_committed, baseline_committed);
    assert_eq!(post_failed, baseline_failed);
    assert_eq!(
        post_status, baseline_status,
        "no submission_status rows should be written on fail-atomic"
    );
}
