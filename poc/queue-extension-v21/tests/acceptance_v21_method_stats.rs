//! Acceptance tests for M7.1 (acct-byue): per-queue + per-method + router
//! stats SQL functions.
//!
//! Spec §4.4 O3.
//!
//! Tests cover the observability surface added by M7.1:
//!   - poc_v21_staging_stats()   — TableIterator(stat, value)
//!   - poc_v21_committer_stats() — TableIterator(stat, value)
//!   - poc_v21_method_stats()    — per-method dispatch_count + p50/p99
//!   - poc_v21_apply_seq()       — scalar high watermark
//!   - poc_v21_committer_tx_seq() — scalar high watermark
//!   - poc_v21_queue_depth()     — by-name depth lookup
//!   - poc_v21_router_stats()    — p50/p99 rows added (M3.3 + M7.1)
//!
//! Convention: each stats fn returns rows of (stat_name TEXT, value
//! DOUBLE PRECISION). Tests assert (a) callability, (b) shape, (c)
//! non-degenerate values under load, (d) counter monotonicity.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_method_stats \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{
    connect_pool, reset_state, seed_method_assignments, seed_standard_costs, wait_for_terminal,
};
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

const TERMINAL_TIMEOUT_SECS: u64 = 10;

async fn fetch_stats(pool: &PgPool, fn_name: &str) -> HashMap<String, f64> {
    let rows: Vec<(String, f64)> = sqlx::query_as(&format!(
        "SELECT stat_name, stat_value FROM {fn_name}()"
    ))
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("fetch {fn_name}: {e}"));
    rows.into_iter().collect()
}

async fn seed_fifo_layer(pool: &PgPool, sku: i64, qty: i64, unit_cost: i64) {
    sqlx::query(
        "INSERT INTO poc_v21_cost_layers \
            (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, \
             correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
         VALUES ($1, 1, $2, $3, now() - interval '1 minute', 1, 'po_receipt', \
                 gen_random_uuid(), '1'::xid8, 1, 1)",
    )
    .bind(sku)
    .bind(qty)
    .bind(unit_cost)
    .execute(pool)
    .await
    .expect("seed cost_layer");
}

async fn enqueue_inv_issue(pool: &PgPool, cid: Uuid, sku: i64, qty: i64, issue_id: i64, chrono: i64) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": -qty,
        "unit_cost": 0,
        "issue_id": issue_id,
        "business_date_jdate": 20221,
        "doc_chrono": chrono,
        "document_id": 6_000_000_i64 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'inv_issue', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue");
}

/// All four TableIterator stats fns are callable and return the documented
/// stat names. No assertion on values — pure shape check.
#[tokio::test]
#[ignore]
async fn test_v21_stats_fns_shape() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let staging = fetch_stats(&pool, "poc_v21_staging_stats").await;
    for k in [
        "head",
        "tail",
        "depth",
        "count_empty",
        "count_pending",
        "count_processing",
        "count_routed",
        "count_abandoned",
        "next_request_seq",
        "backpressure_wake_count",
    ] {
        assert!(staging.contains_key(k), "staging missing {k}");
    }

    let committer = fetch_stats(&pool, "poc_v21_committer_stats").await;
    for k in [
        "head",
        "tail",
        "depth",
        "capacity",
        "count_empty",
        "count_ready",
        "count_in_flight",
        "count_completed",
        "next_superbatch_id",
        "committer_claim_count",
        "committer_takeover_count",
        "committer_tx_failures",
        "committer_drains_total",
    ] {
        assert!(committer.contains_key(k), "committer missing {k}");
    }

    let method = fetch_stats(&pool, "poc_v21_method_stats").await;
    for prefix in ["fifo", "avg", "std"] {
        for suffix in ["dispatch_count", "error_count", "error_rate", "p50_ns", "p99_ns"] {
            let k = format!("{prefix}_{suffix}");
            assert!(method.contains_key(&k), "method missing {k}");
        }
    }

    let router = fetch_stats(&pool, "poc_v21_router_stats").await;
    assert!(router.contains_key("envelopes_per_sb_p50"));
    assert!(router.contains_key("envelopes_per_sb_p99"));
}

/// Counters grow under load and stay monotone.
#[tokio::test]
#[ignore]
async fn test_v21_stats_counters_grow_under_load() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    // Reset router stats so the test's expected delta is clean.
    sqlx::query("SELECT poc_v21_router_stats_reset()")
        .execute(&pool)
        .await
        .expect("router_stats_reset");

    let baseline = fetch_stats(&pool, "poc_v21_committer_stats").await;
    let baseline_drains = baseline["committer_drains_total"];
    let baseline_claims = baseline["committer_claim_count"];

    // Drive 5 envelopes through.
    for i in 0..5i64 {
        seed_fifo_layer(&pool, 1100 + i, 100, 10).await;
    }
    let cids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    for (i, cid) in cids.iter().enumerate() {
        enqueue_inv_issue(&pool, *cid, 1100 + i as i64, 5, 11_000 + i as i64, i as i64 + 1).await;
    }

    let reached =
        wait_for_terminal(&pool, &cids, Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 5);

    let after = fetch_stats(&pool, "poc_v21_committer_stats").await;
    assert!(
        after["committer_drains_total"] > baseline_drains,
        "drains_total must grow under load"
    );
    assert!(
        after["committer_claim_count"] > baseline_claims,
        "claim_count must grow under load"
    );

    // apply_seq scalar grows too.
    let apply_seq: i64 = sqlx::query_scalar("SELECT poc_v21_apply_seq()")
        .fetch_one(&pool)
        .await
        .expect("apply_seq");
    assert!(apply_seq >= 5, "apply_seq must reflect enqueues: {apply_seq}");
}

/// Per-method counters increment correctly when a mixed-method batch runs.
#[tokio::test]
#[ignore]
async fn test_v21_method_stats_per_method_dispatch_counts() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    seed_method_assignments(&pool, &[(1201, "avg"), (1202, "std")]).await;
    seed_standard_costs(&pool, &[(1202, 1, 50)]).await;
    sqlx::query(
        "INSERT INTO poc_v21_avg_pool_state (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
         VALUES (1201, 1, 30, 100, now(), 0) \
         ON CONFLICT (sku_id, location_id) DO UPDATE SET avg_unit_cost = EXCLUDED.avg_unit_cost, total_qty = EXCLUDED.total_qty",
    )
    .execute(&pool)
    .await
    .expect("avg pool");
    seed_fifo_layer(&pool, 1200, 100, 20).await;

    let baseline = fetch_stats(&pool, "poc_v21_method_stats").await;

    // One issue per method.
    let cids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    enqueue_inv_issue(&pool, cids[0], 1200, 1, 12_001, 10).await;
    enqueue_inv_issue(&pool, cids[1], 1201, 1, 12_002, 11).await;
    enqueue_inv_issue(&pool, cids[2], 1202, 1, 12_003, 12).await;

    let reached =
        wait_for_terminal(&pool, &cids, Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 3);

    let after = fetch_stats(&pool, "poc_v21_method_stats").await;
    for prefix in ["fifo", "avg", "std"] {
        let key = format!("{prefix}_dispatch_count");
        assert!(
            after[&key] > baseline[&key],
            "{prefix} dispatch_count must grow: baseline={} after={}",
            baseline[&key],
            after[&key]
        );
    }
}

/// queue_depth() returns sensible values for known queue names and -1
/// for unknown.
#[tokio::test]
#[ignore]
async fn test_v21_queue_depth_by_name() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let s_depth: i64 = sqlx::query_scalar("SELECT poc_v21_queue_depth('staging'::text)")
        .fetch_one(&pool)
        .await
        .expect("staging depth");
    let c_depth: i64 = sqlx::query_scalar("SELECT poc_v21_queue_depth('committer'::text)")
        .fetch_one(&pool)
        .await
        .expect("committer depth");
    let unknown: i64 = sqlx::query_scalar("SELECT poc_v21_queue_depth('bogus'::text)")
        .fetch_one(&pool)
        .await
        .expect("unknown depth");
    assert!(s_depth >= 0, "staging depth must be >= 0; got {s_depth}");
    assert!(c_depth >= 0, "committer depth must be >= 0; got {c_depth}");
    assert_eq!(unknown, -1, "unknown queue name must return -1");
}

/// Error-rate is computed correctly when a known-bad envelope hits a
/// method's failure path.
#[tokio::test]
#[ignore]
async fn test_v21_method_stats_error_rate_reflects_failure() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // FIFO SKU with no stock → insufficient_inventory error in FIFO.
    let cid_fail = Uuid::new_v4();
    enqueue_inv_issue(&pool, cid_fail, 1301, 5, 13_001, 20).await;

    // FIFO SKU with stock → success.
    seed_fifo_layer(&pool, 1302, 100, 10).await;
    let cid_ok = Uuid::new_v4();
    enqueue_inv_issue(&pool, cid_ok, 1302, 5, 13_002, 21).await;

    let reached = wait_for_terminal(&pool, &[cid_fail, cid_ok], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 2);

    let stats = fetch_stats(&pool, "poc_v21_method_stats").await;
    // FIFO had at least 1 failure and 1 success ⇒ dispatch >= 2,
    // error_count >= 1, error_rate in (0, 1).
    assert!(
        stats["fifo_dispatch_count"] >= 2.0,
        "fifo dispatched at least 2: {}",
        stats["fifo_dispatch_count"]
    );
    assert!(
        stats["fifo_error_count"] >= 1.0,
        "fifo had at least 1 error: {}",
        stats["fifo_error_count"]
    );
    let r = stats["fifo_error_rate"];
    assert!(r > 0.0 && r < 1.0, "fifo error_rate in (0,1): {r}");
}
