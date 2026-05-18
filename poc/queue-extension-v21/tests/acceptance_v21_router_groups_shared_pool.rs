//! E1 acceptance (acct-0frn / meta `acct-shpc`): grouped packing — envelopes
//! sharing a SKU pool key are packed INTO one SuperBatch by the router.
//!
//! Pre-fix (the original "greedy disjoint" rule), two envelopes touching the
//! same `(sku, location)` would be split across two SuperBatches that race
//! for committers and serialize on `pool_locks`. The grouped rule (per
//! `poc-v2.1-amendment-0.2`) intentionally pulls them INTO one SuperBatch
//! processed by one committer in chrono order; per-event dispatch in Step 4
//! handles the running-avg / FIFO sequencing inside one transaction.
//!
//! Test shape: enqueue N envelopes all touching the same `(sku=900,
//! location=1)` pool in parallel across the connection pool so they pile
//! into the staging queue within one router-tick window. Wait for terminal;
//! assert the router observed at least one SuperBatch with envelope_count
//! > 1 — under the new rule this is the expected outcome, not coincidence.
//! Under the old rule the same burst would have produced N size-1 SBs.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_groups_shared_pool \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

const SHARED_SKU: i64 = 900;
const SHARED_LOC: i64 = 1;
const N: i64 = 10;

async fn enqueue_one(pool: &PgPool, cid: Uuid, chrono: i64) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": SHARED_SKU,
        "location_id": SHARED_LOC,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 9_000_000 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[SHARED_SKU, SHARED_LOC]], "wip": [] });
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
async fn acceptance_v21_router_groups_shared_pool_burst() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_router_stats(&pool).await;

    // Burst N envelopes all targeting the same (sku, location) pool. Each
    // gets a fresh correlation_id but the same pool_keys array. In the
    // disjoint regime the router would have rejected each successor as
    // "intersects lock_set" and produced N size-1 SuperBatches; under
    // grouped packing they land in one SuperBatch (or, if N exceeded
    // batch_size_max, a handful of multi-envelope SBs).
    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(N as usize);
    for _ in 0..N {
        correlation_ids.push(Uuid::new_v4());
    }

    let mut handles = Vec::with_capacity(N as usize);
    for i in 0..N {
        let pool = pool.clone();
        let cid = correlation_ids[i as usize];
        let chrono = (i + 1) as i64;
        handles.push(tokio::spawn(async move {
            enqueue_one(&pool, cid, chrono).await
        }));
    }
    for h in handles {
        h.await.expect("join").expect("enqueue_one");
    }

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(15)).await;
    assert_eq!(
        terminal, N,
        "all enqueued envelopes should reach terminal state"
    );

    let total = router_total_envelopes(&pool).await;
    let max_env = router_max_envelope_count(&pool).await;
    let sb_count = router_superbatch_count(&pool).await;

    println!(
        "router stats: superbatches={} total_envelopes={} max_envelope_count={}",
        sb_count, total, max_env
    );

    assert_eq!(
        total, N,
        "router_total_envelopes ({}) should equal enqueued count ({}) — no lost envelope",
        total, N
    );
    assert!(
        max_env >= 2,
        "grouped rule: shared-pool burst should produce at least one SuperBatch with envelope_count >= 2 (observed max={}). Pre-fix this assertion would fail because the disjoint rule put each shared-pool envelope into its own size-1 SB.",
        max_env
    );
    assert!(
        sb_count < N,
        "grouped rule: shared-pool burst's superbatch_count ({}) must be strictly less than envelope count ({}) — N size-1 SBs is the disjoint-rule failure mode.",
        sb_count, N
    );
}
