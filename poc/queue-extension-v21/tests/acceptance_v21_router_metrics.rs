//! M3.3 (acct-8xyj) / acct-zplt acceptance: poc_v21_router_stats() is
//! callable and emits values consistent with spec §4.3 R1/R2.
//!
//! Two-phase coverage:
//!  - Phase A (pool-disjoint burst): each envelope is a singleton
//!    connected component → one size-1 SuperBatch each. Asserts the
//!    R1 low-overlap surface: avg_envelopes_per_sb == 1, all SBs in
//!    histogram_bucket_0.
//!  - Phase B (hot pool): shared-pool envelopes group into one (or
//!    two under jitter) SuperBatches via affinity grouping; force_pack
//!    is the defensive backstop and stays at 0 for non-saturated
//!    workloads.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_router_metrics \
//!     --features pg18 --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

async fn enqueue_one(pool: &PgPool, cid: Uuid, sku: i64, chrono: i64) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 6_000_000 + chrono,
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

async fn read_router_stats(pool: &PgPool) -> HashMap<String, f64> {
    let rows: Vec<(String, f64)> = sqlx::query_as("SELECT * FROM poc_v21_router_stats()")
        .fetch_all(pool)
        .await
        .expect("poc_v21_router_stats");
    rows.into_iter().collect()
}

fn assert_stat_present(stats: &HashMap<String, f64>, name: &str) {
    assert!(
        stats.contains_key(name),
        "poc_v21_router_stats() must emit stat `{}`; got keys: {:?}",
        name,
        stats.keys().collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_metrics_disjoint_burst() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_router_stats(&pool).await;

    const N: usize = 50;
    const SKU_BASE: i64 = 600;

    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(N);
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let cid = Uuid::new_v4();
        correlation_ids.push(cid);
        let pool = pool.clone();
        let sku = SKU_BASE + i as i64;
        let chrono = (i + 1) as i64;
        handles.push(tokio::spawn(async move {
            enqueue_one(&pool, cid, sku, chrono).await
        }));
    }
    for h in handles {
        h.await.expect("join").expect("enqueue_one");
    }

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(15)).await;
    assert_eq!(
        terminal, N as i64,
        "all envelopes should reach terminal under disjoint burst"
    );

    let stats = read_router_stats(&pool).await;
    println!("disjoint burst stats: {:?}", stats);

    // Required surface — every named stat must be present.
    let required = [
        "superbatch_count",
        "total_envelopes",
        "force_pack_count",
        "max_envelope_count",
        "ticks_total",
        "entries_scanned_total",
        "committer_drains_total",
        "avg_envelopes_per_sb",
        "packing_efficiency",
        "pack_yield_per_tick",
        "batch_size_max_guc",
        "histogram_bucket_0",
        "histogram_bucket_1",
        "histogram_bucket_2",
        "histogram_bucket_3",
        "histogram_bucket_4",
        "histogram_bucket_5",
        "histogram_bucket_6",
        "histogram_bucket_7",
    ];
    for name in &required {
        assert_stat_present(&stats, name);
    }

    // Sanity assertions.
    // Re-route may increment total_envelopes on each pack (acct-011x).
    // Use >= for the "no envelope lost" property.
    assert!(
        stats["total_envelopes"] as i64 >= N as i64,
        "total_envelopes ({}) must be >= enqueued count ({})",
        stats["total_envelopes"] as i64,
        N
    );
    assert!(
        stats["superbatch_count"] > 0.0,
        "at least one SuperBatch must have been assembled"
    );
    assert!(
        stats["committer_drains_total"] > 0.0,
        "committer must have drained at least one SuperBatch"
    );
    assert!(
        stats["ticks_total"] > 0.0,
        "router must have ticked at least once"
    );
    assert!(
        stats["entries_scanned_total"] >= N as f64,
        "entries_scanned_total ({}) must be >= envelopes ({})",
        stats["entries_scanned_total"],
        N
    );
    // R1 low-overlap: each pool-disjoint envelope is its own singleton
    // component → one size-1 SB each. Average envelopes-per-SB == 1.
    assert!(
        (stats["avg_envelopes_per_sb"] - 1.0).abs() < 0.01,
        "R1 low-overlap: disjoint burst yields avg_envelopes_per_sb == 1.0; got {}",
        stats["avg_envelopes_per_sb"]
    );
    assert!(
        stats["packing_efficiency"] > 0.0
            && stats["packing_efficiency"] <= 1.0,
        "packing_efficiency must be in (0, 1]; got {}",
        stats["packing_efficiency"]
    );
    // R1 low-overlap: all SBs are size-1 → every SB lands in bucket_0;
    // buckets 1-7 (size >= 2) must be empty.
    let large_bucket_total: f64 = (1..=7)
        .map(|i| stats[&format!("histogram_bucket_{}", i)])
        .sum();
    assert_eq!(
        large_bucket_total, 0.0,
        "R1 low-overlap: no SuperBatch of size >= 2 expected; histogram buckets 1-7 sum = {}",
        large_bucket_total
    );
    assert!(
        stats["histogram_bucket_0"] >= N as f64,
        "R1 low-overlap: histogram_bucket_0 must hold all {} size-1 SBs; got {}",
        N,
        stats["histogram_bucket_0"]
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_metrics_hot_pool() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_router_stats(&pool).await;

    const N: usize = 25;
    const HOT_SKU: i64 = 555;

    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(N);
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
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

    let terminal = wait_for_terminal(&pool, &correlation_ids, Duration::from_secs(30)).await;
    assert_eq!(terminal, N as i64);

    let stats = read_router_stats(&pool).await;
    println!("hot pool stats: {:?}", stats);

    // Hot pool: every shared-pool envelope packs INTO one SuperBatch,
    // not N separate ones. sb_count is small (1-2 depending on
    // scheduling jitter; up to batch_size_max=50 envelopes per SB) and
    // max_envelope_count == N.
    assert!(
        (stats["superbatch_count"] as i64) >= 1 && (stats["superbatch_count"] as i64) <= 2,
        "hot pool packs into 1 (or 2 under jitter) SuperBatches; got sb_count={}",
        stats["superbatch_count"]
    );
    assert_eq!(
        stats["total_envelopes"] as i64, N as i64,
        "no envelope lost; got total_envelopes={}",
        stats["total_envelopes"]
    );
    assert!(
        (stats["max_envelope_count"] as i64) >= 2,
        "max_envelope_count under hot pool must be >= 2 (grouped rule packs shared-pool envelopes together); got {}",
        stats["max_envelope_count"]
    );
    // size-1 bucket should be near-empty for a hot-pool burst that fits
    // in one tick's window — every envelope shares the pool key with
    // every other, so they pack together rather than dribbling out as
    // singletons. Allow 0 or 1 singletons (boundary-of-tick effects).
    assert!(
        (stats["histogram_bucket_0"] as i64) <= 1,
        "hot pool under grouped rule produces at most 1 size-1 SB (boundary jitter); got bucket_0={}",
        stats["histogram_bucket_0"]
    );
    // Force-pack is a queue/arena-pressure safety net; this
    // 25-envelope hot-pool burst doesn't hit those paths. FU2
    // (`acct-shpc.8`) validates the fairness backstop on a workload
    // that exercises queue/arena pressure.
    assert_eq!(
        stats["force_pack_count"] as i64, 0,
        "hot-pool burst does not trigger starvation/force-pack; got {}",
        stats["force_pack_count"]
    );
}
