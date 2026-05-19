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
    // Names align with spec §4.4 O3.
    let required = [
        "superbatch_count",
        "total_envelopes",
        "force_pack_count",
        "max_envelope_count",
        "ticks_total",
        "entries_scanned_total",
        "committer_drains_total",
        "avg_envelopes_per_sb",
        "cross_sb_for_update_waits",
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
    // R1 low-overlap: components are all singletons, so no chunk
    // overflows batch_size_max. cross_sb_for_update_waits stays 0.
    assert_eq!(
        stats["cross_sb_for_update_waits"] as i64, 0,
        "R1 low-overlap: singleton components don't overflow batch_size_max, so cross_sb_for_update_waits stays 0; got {}",
        stats["cross_sb_for_update_waits"]
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
    // Re-route may increment total_envelopes on each pack (acct-011x).
    // Use >= for the "no envelope lost" property — matches the
    // disjoint_burst test's documented pattern.
    assert!(
        stats["total_envelopes"] as i64 >= N as i64,
        "total_envelopes ({}) must be >= enqueued count ({})",
        stats["total_envelopes"] as i64,
        N
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
    // 25 envelopes against default batch_size_max=50 fit in one chunk —
    // no cluster overflow → cross_sb_for_update_waits stays 0. The
    // overflow path is exercised by acceptance_v21_router_metrics_cluster_overflow.
    assert_eq!(
        stats["cross_sb_for_update_waits"] as i64, 0,
        "hot-pool below batch_size_max yields zero cross-SB FOR UPDATE waits; got {}",
        stats["cross_sb_for_update_waits"]
    );
}

// ── Cluster overflow → cross_sb_for_update_waits > 0 (acct-1gg0) ────
//
// When a connected component exceeds batch_size_max, the router emits
// multiple chunks for that component; sibling SuperBatches share
// pool_keys and serialize on FOR UPDATE in committer Step 2. Spec
// §4.4 O3 mandates `cross_sb_for_update_waits` as an observability
// counter for exactly this case.
//
// Pin batch_size_max=5 + enqueue 25 envelopes all targeting one SKU →
// one connected component of 25 envelopes → 5 chunks → 4 overflow
// chunks past the first. Counter must reach >= 4 in the all-one-tick
// case; under tick jitter the component can split across multiple
// router ticks (each tick's slice is a new component) — but the
// `cross_sb_for_update_waits` counter is shmem-resident and
// monotonic, so the assertion at the end of the run measures the
// total overflow signal across however many ticks consumed the burst.
// Worst-case jitter spreads the 25 envelopes evenly across N ticks;
// 4 overflow signals are produced iff at least one tick observed
// >= 2 chunks-worth of envelopes for the hot pool. With batch_size_max=5
// and 25 envelopes, even spreading across 5 ticks (5 envelopes each)
// would produce 0 overflows. So the assertion is bounded as >= 1
// (any tick with > batch_size_max envelopes produces >= 1 overflow).

async fn set_batch_size_max(pool: &PgPool, n: i32) {
    sqlx::query(&format!("ALTER SYSTEM SET poc_v21.batch_size_max = {n}"))
        .execute(pool)
        .await
        .expect("alter system batch_size_max");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await
        .expect("pg_reload_conf");
    // SIGHUP propagation window — the router's next tick checks
    // sighup_received() and calls ProcessConfigFile() (acct-1gg0).
    tokio::time::sleep(Duration::from_millis(150)).await;
}

async fn reset_batch_size_max(pool: &PgPool) {
    let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.batch_size_max")
        .execute(pool)
        .await;
    let _ = sqlx::query("SELECT pg_reload_conf()").execute(pool).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn acceptance_v21_router_metrics_cluster_overflow() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    reset_router_stats(&pool).await;
    set_batch_size_max(&pool, 5).await;

    const N: usize = 25;
    const HOT_SKU: i64 = 777;

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
    println!("cluster_overflow stats: {:?}", stats);

    // Under tick jitter the 25 envelopes may split across N ticks.
    // Each tick whose hot-SKU component exceeds batch_size_max=5
    // emits >= 1 overflow chunk. The assertion is lower-bounded at 1
    // because at least one tick must absorb > 5 envelopes when 25
    // are enqueued in parallel within the burst window.
    assert!(
        (stats["cross_sb_for_update_waits"] as i64) >= 1,
        "cluster overflow (N={} component, batch_size_max=5) must emit >= 1 overflow chunk; got cross_sb_for_update_waits={}",
        N, stats["cross_sb_for_update_waits"]
    );
    assert_eq!(
        stats["batch_size_max_guc"] as i64, 5,
        "BGWorker must observe the runtime batch_size_max=5 (acct-1gg0 SIGHUP wiring); got {}",
        stats["batch_size_max_guc"]
    );

    reset_batch_size_max(&pool).await;
}
