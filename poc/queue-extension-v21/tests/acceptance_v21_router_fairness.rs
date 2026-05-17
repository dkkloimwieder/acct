//! M3.2 (acct-evyq) acceptance: fairness backstop fires under
//! hot-pool burst.
//!
//! Test shape: enqueue 30 envelopes all targeting the same (sku=999,
//! location=1) pool. Since every envelope's pool key intersects every
//! other's, only the head-of-queue packs per tick; the rest accumulate
//! starvation_count by 1 each tick. After
//! `router_starvation_threshold_ticks` (default 10) ticks, force-pack
//! begins firing on subsequent envelopes — verified via
//! `poc_v21_router_force_pack_count()`.
//!
//! The natural-progress guarantee (every envelope reaches terminal
//! within bounded time) holds at M3.1 too — head-of-queue always
//! packs regardless of starvation, so a hot pool drains 1-per-tick
//! at the 50ms BGWorker cadence. M3.2's contribution is force-pack
//! visibility: when an envelope's wait crosses the threshold, the
//! router emits a force-packed size-1 SuperBatch and bumps the
//! counter, giving operations + bake-off measurement (R3) a signal
//! for tail-latency analysis.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_fairness \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

const HOT_SKU: i64 = 999;
const ENVELOPE_COUNT: usize = 30;

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

async fn reset_router_stats(pool: &PgPool) {
    sqlx::query("SELECT poc_v21_router_stats_reset()")
        .execute(pool)
        .await
        .expect("router_stats_reset");
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_fairness_backstop_fires() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_router_stats(&pool).await;

    // Hot pool: all envelopes touch the same (sku, location). Greedy
    // disjoint packing trivially picks the head and rejects every
    // other candidate in the same tick.
    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(ENVELOPE_COUNT);
    let mut handles = Vec::with_capacity(ENVELOPE_COUNT);
    for i in 0..ENVELOPE_COUNT {
        let cid = Uuid::new_v4();
        correlation_ids.push(cid);
        let pool = pool.clone();
        let chrono = (i + 1) as i64;
        handles.push(tokio::spawn(async move {
            enqueue_one(&pool, cid, HOT_SKU, chrono).await
        }));
    }
    for h in handles {
        h.await.expect("join").expect("enqueue_one");
    }

    // Wait for terminal — hot pool processes at ~one envelope per
    // 50ms BGWorker tick.
    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(30)).await;
    assert_eq!(
        terminal,
        ENVELOPE_COUNT as i64,
        "every envelope must reach terminal state under hot-pool drain"
    );

    let total_envelopes: i64 = sqlx::query_scalar("SELECT poc_v21_router_total_envelopes()")
        .fetch_one(&pool)
        .await
        .expect("total_envelopes");
    let sb_count: i64 = sqlx::query_scalar("SELECT poc_v21_router_superbatch_count()")
        .fetch_one(&pool)
        .await
        .expect("sb_count");
    let force_packs: i64 = sqlx::query_scalar("SELECT poc_v21_router_force_pack_count()")
        .fetch_one(&pool)
        .await
        .expect("force_packs");
    let max_env: i32 = sqlx::query_scalar("SELECT poc_v21_router_max_envelope_count()")
        .fetch_one(&pool)
        .await
        .expect("max_env");

    println!(
        "router stats: superbatches={} total_envelopes={} max_envelope_count={} force_packs={}",
        sb_count, total_envelopes, max_env, force_packs
    );

    assert_eq!(
        total_envelopes, ENVELOPE_COUNT as i64,
        "no envelope lost during hot-pool drain"
    );
    // Every SuperBatch under hot pool is size-1 (head packs alone;
    // rest intersect). Strictly: max_envelope_count == 1.
    assert_eq!(
        max_env, 1,
        "hot pool packs strictly size-1 SuperBatches; observed max envelope_count={}",
        max_env
    );
    // Fairness backstop should fire for envelopes past the threshold.
    // With default threshold=10 and 30 envelopes:
    //   - Envelopes 1..=10 pack as head without force (each tick's
    //     head has starv < 10 because it just became head).
    //   - From envelope 11 onward, starv accumulates past threshold
    //     before its turn at the head, so the gate fires on the
    //     tick that promotes it.
    // We expect ~20 force-packs; assert at least 1 to keep the
    // signal robust against scheduling jitter.
    assert!(
        force_packs > 0,
        "M3.2 acceptance: starvation_threshold_ticks=10 must produce \
         at least one force-pack under a 30-envelope hot-pool burst; \
         observed force_pack_count={}",
        force_packs
    );
}
