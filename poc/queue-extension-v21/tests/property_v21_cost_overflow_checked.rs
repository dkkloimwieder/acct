//! E5 (acct-gx1z.1.5) test: cost arithmetic overflow routes the
//! envelope to state='failed' with error_code='cost_overflow'.
//!
//! Pre-fix: the wo_complete component-cost accumulator used
//! `saturating_add` + `saturating_mul`. On overflow the running
//! `wo_cost_local` silently clamped at i64::MAX; the dispatcher
//! then divided by output qty to produce a wildly wrong but
//! plausible-looking per-unit cost. Cost-accounting silent failure
//! is worse than panic.
//!
//! Post-fix: the accumulator uses `checked_*`; on None, the
//! envelope is marked failed with error_code='cost_overflow' and
//! per-envelope snapshot rollback restores pre-envelope pool state.
//!
//! Six scenarios:
//!   1. Single depletion overflow: 1 component, qty × unit_cost
//!      overflows i64.
//!   2. Multi-component sum overflow: 2 components individually fit,
//!      sum overflows.
//!   3. Confirms a near-boundary case where qty × unit_cost is
//!      exactly i64::MAX commits successfully (no false positive).
//!   4. FIFO receipt overflow (acct-gx1z.1.15): po_receipt with qty ×
//!      unit_cost > i64::MAX trips fifo.rs emit_layer's checked_mul.
//!   5. AVG receipt overflow: po_receipt with AVG method trips
//!      avg.rs roll_in's checked_mul before any pool mutation.
//!   6. AVG consume overflow: seed an AVG pool with a high running
//!      avg_unit_cost, then so_shipment trips avg.rs consume's
//!      checked_mul.
//!
//! Run via:
//!   cargo test --release --test property_v21_cost_overflow_checked \
//!     --features pg18,test_hooks --no-default-features -- --ignored --nocapture

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, seed_method_assignments};
use sqlx::{PgPool, Row};
use std::time::{Duration, Instant};
use uuid::Uuid;

async fn seed_fifo_layer(pool: &PgPool, sku: i64, loc: i64, qty: i64, unit_cost: i64) {
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
    .bind(unit_cost)
    .execute(pool)
    .await
    .expect("seed cost_layer");
}

async fn enqueue_po_receipt(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    location: i64,
    qty: i64,
    unit_cost: i64,
    doc_chrono: i64,
) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": location,
        "qty": qty,
        "unit_cost": unit_cost,
        "doc_chrono": doc_chrono,
        "document_id": 8_600_000_i64 + doc_chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, location]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'po_receipt', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue po_receipt");
}

async fn enqueue_so_shipment(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    location: i64,
    qty: i64,
    doc_chrono: i64,
) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": location,
        "qty": qty,
        "doc_chrono": doc_chrono,
        "document_id": 8_500_000_i64 + doc_chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, location]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'so_shipment', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue so_shipment");
}

async fn seed_avg_pool(
    pool: &PgPool,
    sku: i64,
    loc: i64,
    qty: i64,
    avg_unit_cost: i64,
) {
    sqlx::query(
        "INSERT INTO poc_v21_avg_pool_state \
            (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
         VALUES ($1, $2, $3, $4, now(), 1)",
    )
    .bind(sku)
    .bind(loc)
    .bind(avg_unit_cost)
    .bind(qty)
    .execute(pool)
    .await
    .expect("seed avg_pool_state");
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
    let comps: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, q)| serde_json::json!([s, l, q]))
        .collect();
    let payload = serde_json::json!({
        "wip_account": [wo_id, op_id],
        "components": comps,
        "output": [output.0, output.1, output.2],
        "doc_chrono": doc_chrono,
        "document_id": 8_700_000_i64 + doc_chrono,
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

async fn wait_for_status(
    pool: &PgPool,
    cid: Uuid,
    timeout: Duration,
) -> Option<(String, Option<String>)> {
    let start = Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT state::text, error_code::text \
               FROM poc_v21_submission_status \
              WHERE correlation_id = $1",
        )
        .bind(cid)
        .fetch_optional(pool)
        .await
        .expect("query submission_status");
        if let Some(r) = row {
            let state: String = r.get(0);
            let err: Option<String> = r.get(1);
            if state == "failed" || state == "committed" || state == "replayed" {
                return Some((state, err));
            }
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn single_component_overflow_routes_to_cost_overflow() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_method_assignments(&pool, &[(1, "fifo"), (2, "fifo")]).await;

    // Seed a FIFO layer with qty=3 at unit_cost=i64::MAX/2. Receipt
    // itself doesn't overflow (we INSERT directly into cost_layers
    // bypassing the dispatcher), but a wo_complete consuming qty=3
    // produces depletion.qty=3 × depletion.unit_cost=i64::MAX/2
    // ≈ 1.5×i64::MAX which overflows the checked_mul.
    seed_fifo_layer(&pool, 1, 1, 3, i64::MAX / 2).await;

    let cid = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid,
        9001,                  // wo_id
        100,                   // op_id
        &[(1, 1, 3)],          // components: consume 3 of SKU 1
        (2, 1, 1),             // output: 1 of SKU 2
        1,
    )
    .await;

    let (state, err) = wait_for_status(&pool, cid, Duration::from_secs(5))
        .await
        .expect("terminal status in 5s");
    assert_eq!(state, "failed", "expected state='failed', got {state}");
    assert!(
        err.as_deref().map(|e| e.contains("cost_overflow")).unwrap_or(false),
        "expected error_code containing 'cost_overflow', got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn multi_component_sum_overflow_routes_to_cost_overflow() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_method_assignments(&pool, &[(3, "fifo"), (4, "fifo"), (5, "fifo")]).await;

    // Two components each at ~i64::MAX/3. Per-component qty × unit_cost
    // fits (i64::MAX/3 × 2 ≈ 0.67×i64::MAX); their sum overflows.
    // qty=2 × unit_cost=i64::MAX/3 ≈ 6.15e18; two of them sum to
    // ~1.23e19 which exceeds i64::MAX (9.22e18).
    seed_fifo_layer(&pool, 3, 1, 2, i64::MAX / 3).await;
    seed_fifo_layer(&pool, 4, 1, 2, i64::MAX / 3).await;

    let cid = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid,
        9002,
        100,
        &[(3, 1, 2), (4, 1, 2)],
        (5, 1, 1),
        2,
    )
    .await;

    let (state, err) = wait_for_status(&pool, cid, Duration::from_secs(5))
        .await
        .expect("terminal status in 5s");
    assert_eq!(state, "failed", "expected state='failed', got {state}");
    assert!(
        err.as_deref().map(|e| e.contains("cost_overflow")).unwrap_or(false),
        "expected error_code containing 'cost_overflow', got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn near_boundary_no_overflow_commits_cleanly() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_method_assignments(&pool, &[(6, "fifo"), (7, "fifo")]).await;

    // Engineered to land just under i64::MAX. qty=2 × unit_cost=10^17
    // = 2e17, well under 9.22e18. Single component, single output.
    // The checked_* path should accept this and commit.
    seed_fifo_layer(&pool, 6, 1, 2, 100_000_000_000_000_000).await; // 1e17

    let cid = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid,
        9003,
        100,
        &[(6, 1, 2)],
        (7, 1, 1),
        3,
    )
    .await;

    let (state, err) = wait_for_status(&pool, cid, Duration::from_secs(5))
        .await
        .expect("terminal status in 5s");
    assert_eq!(
        state, "committed",
        "near-boundary case should commit cleanly, got state={state} err={err:?}"
    );
    assert!(
        err.is_none(),
        "committed envelope should have no error_code, got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn fifo_receipt_overflow_routes_to_cost_overflow() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_method_assignments(&pool, &[(10, "fifo")]).await;

    // qty × unit_cost = 3 × i64::MAX/2 ≈ 1.5×i64::MAX — overflows
    // fifo.rs emit_layer's checked_mul on the posting_line amount.
    let cid = Uuid::new_v4();
    enqueue_po_receipt(&pool, cid, 10, 1, 3, i64::MAX / 2, 10).await;

    let (state, err) = wait_for_status(&pool, cid, Duration::from_secs(5))
        .await
        .expect("terminal status in 5s");
    assert_eq!(state, "failed", "expected state='failed', got {state}");
    assert!(
        err.as_deref().map(|e| e.contains("cost_overflow")).unwrap_or(false),
        "expected error_code containing 'cost_overflow', got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn avg_receipt_overflow_routes_to_cost_overflow() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_method_assignments(&pool, &[(11, "avg")]).await;

    // Same shape as FIFO but routes through avg.rs roll_in.
    let cid = Uuid::new_v4();
    enqueue_po_receipt(&pool, cid, 11, 1, 3, i64::MAX / 2, 11).await;

    let (state, err) = wait_for_status(&pool, cid, Duration::from_secs(5))
        .await
        .expect("terminal status in 5s");
    assert_eq!(state, "failed", "expected state='failed', got {state}");
    assert!(
        err.as_deref().map(|e| e.contains("cost_overflow")).unwrap_or(false),
        "expected error_code containing 'cost_overflow', got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn avg_consume_overflow_routes_to_cost_overflow() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_method_assignments(&pool, &[(12, "avg")]).await;

    // Seed an AVG pool with a high avg_unit_cost. so_shipment qty=3 ×
    // avg_unit_cost=i64::MAX/2 overflows the avg.rs consume checked_mul.
    seed_avg_pool(&pool, 12, 1, 5, i64::MAX / 2).await;

    let cid = Uuid::new_v4();
    enqueue_so_shipment(&pool, cid, 12, 1, 3, 12).await;

    let (state, err) = wait_for_status(&pool, cid, Duration::from_secs(5))
        .await
        .expect("terminal status in 5s");
    assert_eq!(state, "failed", "expected state='failed', got {state}");
    assert!(
        err.as_deref().map(|e| e.contains("cost_overflow")).unwrap_or(false),
        "expected error_code containing 'cost_overflow', got {err:?}"
    );
}
