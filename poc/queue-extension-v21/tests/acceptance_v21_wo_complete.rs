//! Acceptance tests for M6.1 (acct-o1yv): WoComplete event type +
//! multi-pool atomic + per-component method dispatch.
//!
//! Spec §1.3 (WoComplete), §1.4 (dispatch), §1.5 (WIP pools).
//!
//! WoComplete payload shape:
//!   {
//!     "wip_account": [wo_id, op_id],
//!     "components": [[sku, location, qty], ...],
//!     "output": [sku, location, qty],
//!     "business_date_jdate": ..., "doc_chrono": ..., "document_id": ...
//!   }
//!
//! At staging-read the payload expands into K+1 synthetic PocV21Events:
//! one per component (qty<0, sub_priority=0..K-1) and one for the output
//! (qty>0, sub_priority=1_000_000). The dispatcher accumulates component
//! cost as each component event flows through its own method's apply_one,
//! then injects unit_cost = total_component_cost / output_qty on the
//! output event before dispatching it to the output method.
//!
//! Tests:
//!   - K=2 FIFO components → FIFO output: standard case; verify cost
//!     flow + posting_lines + output layer.
//!   - K=15 stress: lex-lock chain length holds.
//!   - K=50 stress (per shape S7): ensure no overflow / panic.
//!   - Mixed-method components: FIFO + AVG + STD components into a FIFO
//!     output; per-component method correctness.
//!   - STD output: output emits at component-cost-based unit_cost
//!     (variance vs output's own standard cost is acct's concern, not
//!     extension's).
//!   - Insufficient inventory on a component → whole envelope failed;
//!     no cost rows persisted; submission_status=failed.
//!   - Empty components → enqueue accepts; committer surfaces an error
//!     (parse-time check).
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_wo_complete \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, seed_method_assignments, seed_standard_costs, wait_for_terminal};
use sqlx::{PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

const TERMINAL_TIMEOUT_SECS: u64 = 10;

/// Seed FIFO layers directly so component consumption has stock.
async fn seed_fifo_layers(pool: &PgPool, layers: &[(i64, i64, i64, i64)]) {
    // (sku, location, qty, unit_cost)
    for (sku, loc, qty, uc) in layers {
        sqlx::query(
            "INSERT INTO poc_v21_cost_layers \
                (sku_id, location_id, qty, unit_cost, born_at, born_seq, source_kind, \
                 correlation_id, user_tx_xid, committer_tx_id, superbatch_id) \
             VALUES ($1, $2, $3, $4, now() - interval '1 minute', 1, 'po_receipt', \
                     gen_random_uuid(), '1'::xid8, 1, 1)",
        )
        .bind(sku)
        .bind(loc)
        .bind(qty)
        .bind(uc)
        .execute(pool)
        .await
        .expect("seed cost_layer");
    }
}

/// Seed AVG pool state with a starting avg unit cost.
async fn seed_avg_pool(pool: &PgPool, sku: i64, loc: i64, avg_uc: i64, total_qty: i64) {
    sqlx::query(
        "INSERT INTO poc_v21_avg_pool_state \
            (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
         VALUES ($1, $2, $3, $4, now(), 0) \
         ON CONFLICT (sku_id, location_id) DO UPDATE SET \
             avg_unit_cost = EXCLUDED.avg_unit_cost, total_qty = EXCLUDED.total_qty",
    )
    .bind(sku)
    .bind(loc)
    .bind(avg_uc)
    .bind(total_qty)
    .execute(pool)
    .await
    .expect("seed avg pool");
}

async fn enqueue_wo_complete(
    pool: &PgPool,
    cid: Uuid,
    wo_id: i64,
    op_id: i64,
    components: &[(i64, i64, i64)],
    output: (i64, i64, i64),
    doc_chrono: i64,
) {
    let comps_json: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, q)| serde_json::json!([s, l, q]))
        .collect();
    let payload = serde_json::json!({
        "wip_account": [wo_id, op_id],
        "components": comps_json,
        "output": [output.0, output.1, output.2],
        "business_date_jdate": 20221,
        "doc_chrono": doc_chrono,
        "document_id": 9_990_000_i64 + doc_chrono,
    });

    let mut sku_keys: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, _)| serde_json::json!([s, l]))
        .collect();
    sku_keys.push(serde_json::json!([output.0, output.1]));
    let pool_keys = serde_json::json!({ "sku": sku_keys, "wip": [[wo_id, op_id]] });

    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'wo_complete', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue wo_complete");
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_k2_fifo_basic() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Component 101 has 100 units at $50, 102 has 100 units at $80.
    // Output 999 produced by consuming 5 of 101 and 3 of 102 → 1 output.
    seed_fifo_layers(&pool, &[(101, 1, 100, 50), (102, 1, 100, 80)]).await;

    let cid = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid,
        501,
        10,
        &[(101, 1, 5), (102, 1, 3)],
        (999, 1, 1),
        1,
    )
    .await;

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);

    let row = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch status");
    let state: String = row.get(0);
    let error: Option<String> = row.get(1);
    assert_eq!(state, "committed", "error={error:?}");

    // Output layer at unit_cost=490 (= 5×50 + 3×80) / 1 output_qty.
    let layer = sqlx::query(
        "SELECT qty, unit_cost FROM poc_v21_cost_layers WHERE sku_id=999 AND correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("fetch output layer");
    let layer_qty: i64 = layer.get(0);
    let layer_uc: i64 = layer.get(1);
    assert_eq!(layer_qty, 1);
    assert_eq!(layer_uc, 490);

    // Component depletions: 5 of 101 at 50, 3 of 102 at 80.
    let depletions: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT cl.sku_id, cd.qty, cd.unit_cost \
           FROM poc_v21_cost_depletions cd \
           JOIN poc_v21_cost_layers cl ON cl.layer_id = cd.layer_id \
          WHERE cd.correlation_id = $1 \
          ORDER BY cl.sku_id",
    )
    .bind(cid)
    .fetch_all(&pool)
    .await
    .expect("fetch depletions");
    assert_eq!(depletions.len(), 2);
    assert_eq!(depletions[0], (101, 5, 50));
    assert_eq!(depletions[1], (102, 3, 80));

    // Posting lines: K+1 = 3 total (one per component + one for output).
    let pl_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_posting_lines WHERE correlation_id = $1 AND event_type = 'wo_complete'",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count posting lines");
    assert_eq!(pl_count.0, 3);
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_k15_stress() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let layers: Vec<(i64, i64, i64, i64)> =
        (0..15).map(|i| (200 + i, 1, 100, 10 + i)).collect();
    seed_fifo_layers(&pool, &layers).await;

    let cid = Uuid::new_v4();
    let components: Vec<(i64, i64, i64)> = (0..15).map(|i| (200 + i, 1, 2)).collect();
    enqueue_wo_complete(&pool, cid, 502, 20, &components, (998, 1, 1), 2).await;

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(state.0, "committed");

    // Expected total cost: sum_{i=0..15} (2 × (10 + i)) = 2 × (10×15 + (0+1+...+14)) = 2 × (150 + 105) = 510.
    let layer_uc: (i64,) = sqlx::query_as(
        "SELECT unit_cost FROM poc_v21_cost_layers WHERE sku_id=998 AND correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("output layer");
    assert_eq!(layer_uc.0, 510);
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_k50_stress() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let layers: Vec<(i64, i64, i64, i64)> =
        (0..50).map(|i| (300 + i, 1, 100, 10)).collect();
    seed_fifo_layers(&pool, &layers).await;

    let cid = Uuid::new_v4();
    let components: Vec<(i64, i64, i64)> = (0..50).map(|i| (300 + i, 1, 1)).collect();
    enqueue_wo_complete(&pool, cid, 503, 30, &components, (997, 1, 1), 3).await;

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(state.0, "committed");

    let layer_uc: (i64,) = sqlx::query_as(
        "SELECT unit_cost FROM poc_v21_cost_layers WHERE sku_id=997 AND correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("output layer");
    // 50 components × 1 unit × $10 = $500.
    assert_eq!(layer_uc.0, 500);
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_mixed_method_components() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Component 401: FIFO (default), 50 units at $20.
    seed_fifo_layers(&pool, &[(401, 1, 50, 20)]).await;
    // Component 402: AVG, seeded pool at avg=$30 with 100 qty.
    seed_method_assignments(&pool, &[(402, "avg")]).await;
    seed_avg_pool(&pool, 402, 1, 30, 100).await;
    // Component 403: STD, standard_cost = $40.
    seed_method_assignments(&pool, &[(403, "std")]).await;
    seed_standard_costs(&pool, &[(403, 1, 40)]).await;

    let cid = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid,
        504,
        40,
        &[(401, 1, 2), (402, 1, 2), (403, 1, 2)],
        (996, 1, 1),
        4,
    )
    .await;

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let row = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status");
    let state: String = row.get(0);
    let err: Option<String> = row.get(1);
    assert_eq!(state, "committed", "error={err:?}");

    // Output unit_cost: FIFO 2×20 + AVG 2×30 + STD 2×40 = 40 + 60 + 80 = 180.
    let layer_uc: (i64,) = sqlx::query_as(
        "SELECT unit_cost FROM poc_v21_cost_layers WHERE sku_id=996 AND correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("output layer");
    assert_eq!(layer_uc.0, 180);
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_std_output_uses_component_cost_not_std() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Component 410: FIFO 10 units at $15.
    seed_fifo_layers(&pool, &[(410, 1, 10, 15)]).await;
    // Output 995: STD with standard_cost = $1000 (artificially high to
    // make the test loud). The output layer should emit at COMPONENT cost
    // (4×15 / 1 = $60), NOT the output's own STD ($1000).
    seed_method_assignments(&pool, &[(995, "std")]).await;
    seed_standard_costs(&pool, &[(995, 1, 1000)]).await;

    let cid = Uuid::new_v4();
    enqueue_wo_complete(&pool, cid, 505, 50, &[(410, 1, 4)], (995, 1, 1), 5).await;

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(state.0, "committed");

    // STD output emits a cost_layer at the computed unit_cost (60 from
    // component cost, NOT 1000 from output's own STD).
    let layer_uc: (i64,) = sqlx::query_as(
        "SELECT unit_cost FROM poc_v21_cost_layers \
          WHERE sku_id=995 AND correlation_id=$1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("output layer");
    assert_eq!(layer_uc.0, 60, "STD output must emit at component cost, not own std");
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_insufficient_component_inventory_fails_envelope() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // Component 420 only has 1 unit, but wo_complete asks for 5.
    seed_fifo_layers(&pool, &[(420, 1, 1, 10), (421, 1, 100, 20)]).await;

    let cid = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid,
        506,
        60,
        &[(420, 1, 5), (421, 1, 3)],
        (994, 1, 1),
        6,
    )
    .await;

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let row = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status");
    let state: String = row.get(0);
    let err: Option<String> = row.get(1);
    assert_eq!(state, "failed");
    assert!(
        err.as_deref().map(|e| e.contains("insufficient_inventory")).unwrap_or(false),
        "expected insufficient_inventory error; got {err:?}"
    );

    // No output layer for this correlation_id.
    let layer_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_cost_layers WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(layer_count.0, 0, "failed envelope must not persist any layer rows");
}

#[tokio::test]
#[ignore]
async fn test_v21_wo_complete_empty_components_payload_fails() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let cid = Uuid::new_v4();
    let payload = serde_json::json!({
        "wip_account": [507, 70],
        "components": [],
        "output": [993, 1, 1],
        "business_date_jdate": 20221,
        "doc_chrono": 7,
        "document_id": 9_990_007_i64,
    });
    let pool_keys = serde_json::json!({ "sku": [[993, 1]], "wip": [[507, 70]] });

    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'wo_complete', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(&pool)
        .await
        .expect("enqueue accepts (validation is committer-side)");

    let reached =
        wait_for_terminal(&pool, &[cid], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let state: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid)
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(state.0, "failed");
}
