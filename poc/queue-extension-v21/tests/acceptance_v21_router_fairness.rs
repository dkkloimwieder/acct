//! M3.2 (acct-evyq) acceptance: hot-pool burst drains under the grouped
//! router rule, no envelope lost.
//!
//! The original M3.2 assertion (fairness backstop fires under hot pool)
//! is dead under the grouped router rule (acct-0frn /
//! `poc-v2.1-amendment-0.2`): every envelope's pool key intersects every
//! other's, so all envelopes pack INTO one SuperBatch — there's no
//! starvation path for hot pool anymore. The fairness backstop is
//! preserved as a safety net for queue/arena pressure but does not
//! fire on this workload. FU2 (`acct-shpc.8`) re-validates the
//! backstop under grouped semantics on a workload that legitimately
//! exercises it.
//!
//! This test now asserts the load-bearing properties that still hold:
//!
//!   - every envelope reaches terminal state (no loss / no hang)
//!   - the burst packs into a small number of SuperBatches (1 if the
//!     batch fits in `batch_size_max`; 2-3 under tick jitter when the
//!     window splits)
//!   - max_envelope_count is multi-envelope (grouping happened)
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

    // Hot pool: all envelopes touch the same (sku, location). Under the
    // grouped router rule they pack INTO one SuperBatch per tick (up to
    // batch_size_max=50); under the previous disjoint rule the router
    // would have picked the head and rejected every other candidate in
    // the same tick.
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
    // Grouped rule: hot pool's envelopes pack INTO one SuperBatch up to
    // batch_size_max=50. With ENVELOPE_COUNT=30 < batch_max, expect the
    // whole burst to fit in 1-2 SBs (depending on tick boundary jitter
    // — if half land just before a tick and the other half just after,
    // they split). max_envelope_count must be multi-envelope: at least
    // 2, typically all 30.
    assert!(
        max_env >= 2,
        "grouped rule: hot-pool burst must produce at least one multi-envelope SuperBatch; observed max envelope_count={}",
        max_env
    );
    assert!(
        sb_count >= 1 && sb_count <= 3,
        "grouped rule: hot-pool burst packs into 1-3 SuperBatches (1 typical, more under tick jitter); got sb_count={}",
        sb_count
    );
    // Force-pack is preserved as a queue/arena-pressure safety net but
    // does NOT fire on a hot-pool burst under the grouped rule — there
    // are no skipped candidates for starvation to accumulate against
    // (cf. the disjoint-rule trigger this test originally validated).
    // FU2 (`acct-shpc.8`) re-validates the backstop on a workload that
    // exercises queue/arena pressure.
    assert_eq!(
        force_packs, 0,
        "grouped rule: hot-pool burst does not trigger starvation/force-pack; got force_pack_count={}",
        force_packs
    );
}
