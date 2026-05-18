//! M3.1 (acct-r29s) / acct-zplt acceptance: under spec §4.3 R1
//! (low-overlap workload), each pool-disjoint envelope lands in its
//! own SuperBatch.
//!
//! Test shape: enqueue N envelopes each touching a distinct (sku,
//! location=1) pool. The affinity-grouping pass produces N singleton
//! connected components → N size-1 SuperBatches. Asserts no envelope
//! is lost and the singleton property holds. The companion test for
//! the high-overlap case (R2 — many envelopes sharing one pool pack
//! into one SuperBatch) lives in acceptance_v21_router_groups_shared_pool.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_packing \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

const SKU_BASE: i64 = 700;
const SKU_COUNT: i64 = 50;

async fn enqueue_one(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 8_000_000 + chrono,
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

async fn reset_router_stats(pool: &PgPool) {
    sqlx::query("SELECT poc_v21_router_stats_reset()")
        .execute(pool)
        .await
        .expect("router_stats_reset");
}

async fn router_total_envelopes(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_router_total_envelopes()")
        .fetch_one(pool)
        .await
        .expect("router_total_envelopes")
}

async fn router_max_envelope_count(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT poc_v21_router_max_envelope_count()")
        .fetch_one(pool)
        .await
        .expect("router_max_envelope_count")
}

async fn router_superbatch_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT poc_v21_router_superbatch_count()")
        .fetch_one(pool)
        .await
        .expect("router_superbatch_count")
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_packs_burst() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_router_stats(&pool).await;

    // Build a pool-disjoint burst. Each envelope touches a different
    // (sku, location=1) pool, so the affinity-grouping pass produces
    // SKU_COUNT singleton connected components → SKU_COUNT size-1
    // SuperBatches (spec §4.3 R1).
    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(SKU_COUNT as usize);
    for _ in 0..SKU_COUNT {
        correlation_ids.push(Uuid::new_v4());
    }

    // Enqueue in parallel across multiple connections so envelopes land
    // in the staging queue within microseconds of each other — gives
    // the router's per-tick window scan something to pack.
    let mut handles = Vec::with_capacity(SKU_COUNT as usize);
    for i in 0..SKU_COUNT {
        let pool = pool.clone();
        let cid = correlation_ids[i as usize];
        let sku = SKU_BASE + i;
        let chrono = (i + 1) as i64;
        handles.push(tokio::spawn(async move {
            enqueue_one(&pool, cid, sku, chrono).await
        }));
    }
    for h in handles {
        h.await.expect("join").expect("enqueue_one");
    }

    // Wait for terminal status on all correlations.
    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(15)).await;
    assert_eq!(
        terminal,
        SKU_COUNT as i64,
        "all enqueued envelopes should reach terminal state"
    );

    // Counter assertions.
    let total = router_total_envelopes(&pool).await;
    let max_env = router_max_envelope_count(&pool).await;
    let sb_count = router_superbatch_count(&pool).await;

    println!(
        "router stats: superbatches={} total_envelopes={} max_envelope_count={}",
        sb_count, total, max_env
    );

    assert_eq!(
        total, SKU_COUNT as i64,
        "router_total_envelopes ({}) should equal enqueued count ({}) — no lost envelope",
        total, SKU_COUNT
    );
    assert_eq!(
        max_env, 1,
        "R1 low-overlap: every component is a singleton, so max_envelope_count == 1; observed {}",
        max_env
    );
    assert_eq!(
        sb_count, SKU_COUNT as i64,
        "R1 low-overlap: each pool-disjoint envelope is its own connected component, so sb_count == enqueued count; observed sb_count={} enqueued={}",
        sb_count, SKU_COUNT
    );
}
