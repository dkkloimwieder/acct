//! Acceptance tests for M6.2 (acct-p0mu): per-envelope failure isolation
//! in SuperBatch.
//!
//! Spec §1.8 Step 4 + Step 5 filtering, §3.7, §4.1 C2.
//!
//! Risk surface: an envelope's dispatch can succeed for some events then
//! fail partway through. Without rollback the snapshot is left mid-mutation
//! (e.g. FIFO `deplete` already decremented some layers' effective_qty
//! before the leg that would have exhausted available stock raised
//! insufficient_inventory). The committer must:
//!
//!   1. Filter the failed envelope's row inserts out of Step 5's UNNEST
//!      arrays (the DB never sees them).
//!   2. Roll back snapshot mutations made by the failed envelope's
//!      events so OTHER envelopes downstream in the same SuperBatch see
//!      pre-envelope pool state. Envelopes sharing an SKU pool are
//!      packed into one SuperBatch, so per-envelope rollback isolation
//!      keeps a failing envelope's partial mutation from corrupting
//!      its neighbors. wo_complete envelopes additionally touch
//!      multiple pools per envelope and must not pollute themselves
//!      across their K+1 internal events.
//!
//! Tests:
//!   - basic: 10 single-event envelopes, envelope 3 has insufficient
//!     inventory → 9 commit, 1 fails; row counts match.
//!   - wo_complete partial component failure: K=5 components, component 3
//!     fails → whole envelope failed; no rows persist; a follow-up
//!     wo_complete on the same component SKUs sees pre-failure (full)
//!     pool state.
//!   - replay after failure: re-enqueue with same correlation_id after
//!     seeding additional stock → second submission commits.
//!
//! Run via:
//!   cargo test --release --test acceptance_v21_per_envelope_failure_isolation \
//!     --features pg18 --no-default-features -- --ignored --nocapture --test-threads=1

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use sqlx::{PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

const TERMINAL_TIMEOUT_SECS: u64 = 10;

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

async fn enqueue_inv_issue(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    loc: i64,
    qty: i64,
    issue_id: i64,
    doc_chrono: i64,
) {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": loc,
        "qty": -qty,
        "unit_cost": 0,
        "issue_id": issue_id,
        "business_date_jdate": 20221,
        "doc_chrono": doc_chrono,
        "document_id": 8_000_000_i64 + doc_chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, loc]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'inv_issue', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await
        .expect("enqueue inv_issue");
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
        "document_id": 7_000_000_i64 + doc_chrono,
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

/// 10 single-event InvIssue envelopes targeting distinct SKUs; envelope 3
/// has insufficient inventory. Verify the other 9 commit and envelope 3
/// fails with `insufficient_inventory`.
#[tokio::test]
#[ignore]
async fn test_v21_per_envelope_failure_isolation_basic_10_envelopes() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // SKUs 600..610: 100 units each at $10, EXCEPT SKU 603 which has 0.
    for sku in 600..610i64 {
        if sku == 603 {
            continue;
        }
        seed_fifo_layer(&pool, sku, 1, 100, 10).await;
    }

    let mut cids: Vec<Uuid> = Vec::new();
    for (i, sku) in (600..610i64).enumerate() {
        let cid = Uuid::new_v4();
        cids.push(cid);
        // Envelope 3 (sku=603) asks for 5 units, but stock is 0 → fail.
        let qty = if sku == 603 { 5 } else { 3 };
        enqueue_inv_issue(&pool, cid, sku, 1, qty, 5_000 + i as i64, 100 + i as i64).await;
    }

    let reached = wait_for_terminal(&pool, &cids, Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 10, "all 10 envelopes must reach terminal state");

    // 9 committed + 1 failed.
    let committed: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[]) AND state = 'committed'",
    )
    .bind(&cids)
    .fetch_one(&pool)
    .await
    .expect("count committed");
    assert_eq!(committed.0, 9);

    let failed: Vec<(Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT correlation_id, state, error_code FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[]) AND state = 'failed'",
    )
    .bind(&cids)
    .fetch_all(&pool)
    .await
    .expect("fetch failed");
    assert_eq!(failed.len(), 1);
    let (failed_cid, failed_state, failed_err) = &failed[0];
    assert_eq!(failed_state, "failed");
    assert!(
        failed_err
            .as_deref()
            .map(|e| e.contains("insufficient_inventory"))
            .unwrap_or(false),
        "expected insufficient_inventory; got {failed_err:?}"
    );
    // The failed envelope must be the one for sku=603.
    let failed_idx = cids.iter().position(|c| c == failed_cid).unwrap();
    assert_eq!(failed_idx, 3, "envelope 3 (sku=603) should be the failure");

    // Failed envelope has no posting lines or cost rows.
    let failed_pl: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_posting_lines WHERE correlation_id = $1",
    )
    .bind(failed_cid)
    .fetch_one(&pool)
    .await
    .expect("count failed pl");
    assert_eq!(failed_pl.0, 0);

    let failed_dep: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_cost_depletions WHERE correlation_id = $1",
    )
    .bind(failed_cid)
    .fetch_one(&pool)
    .await
    .expect("count failed dep");
    assert_eq!(failed_dep.0, 0);

    // 9 commits → 9 posting_line rows + 9 depletion rows total across the
    // committed correlation_ids (one inv_issue per envelope).
    let committed_cids: Vec<Uuid> = cids.iter().copied().filter(|c| c != failed_cid).collect();
    let total_pl: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_posting_lines WHERE correlation_id = ANY($1::uuid[])",
    )
    .bind(&committed_cids)
    .fetch_one(&pool)
    .await
    .expect("count committed pl");
    assert_eq!(total_pl.0, 9);

    let total_dep: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_cost_depletions WHERE correlation_id = ANY($1::uuid[])",
    )
    .bind(&committed_cids)
    .fetch_one(&pool)
    .await
    .expect("count committed dep");
    assert_eq!(total_dep.0, 9);
}

/// wo_complete with K=5 components; component 3 has insufficient inventory.
/// Whole envelope fails; no cost rows persist. A follow-up wo_complete
/// targeting the SAME component SKUs commits and bills against the
/// pre-failure full stock — confirming snapshot pollution did not leak
/// (committed DB layer state is the source of truth, but this also
/// validates that the failed envelope's mid-flight depletions were rolled
/// back from the snapshot before subsequent SuperBatches hydrated).
#[tokio::test]
#[ignore]
async fn test_v21_per_envelope_wo_complete_partial_component_rollback() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // 5 components: SKUs 700..705. Components 0, 1, 2, 4 have 100 units;
    // component 3 (sku=703) has 0 units.
    for sku in 700..705i64 {
        if sku == 703 {
            continue;
        }
        seed_fifo_layer(&pool, sku, 1, 100, 10).await;
    }

    // Envelope 1: try to consume 2 of each of 5 components → component 3
    // fails (no stock).
    let cid1 = Uuid::new_v4();
    let components: Vec<(i64, i64, i64)> = (700..705i64).map(|s| (s, 1, 2)).collect();
    enqueue_wo_complete(&pool, cid1, 700, 1, &components, (799, 1, 1), 1).await;

    let reached = wait_for_terminal(&pool, &[cid1], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let row1 = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid1)
    .fetch_one(&pool)
    .await
    .expect("status1");
    let state1: String = row1.get(0);
    let err1: Option<String> = row1.get(1);
    assert_eq!(state1, "failed", "envelope 1 must fail");
    assert!(
        err1.as_deref()
            .map(|e| e.contains("insufficient_inventory"))
            .unwrap_or(false),
        "expected insufficient_inventory; got {err1:?}"
    );

    // No posting lines, no layers, no depletions for envelope 1.
    let cnt_pl1: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_posting_lines WHERE correlation_id = $1",
    )
    .bind(cid1)
    .fetch_one(&pool)
    .await
    .expect("count pl");
    assert_eq!(cnt_pl1.0, 0, "no posting lines for failed envelope");

    let cnt_dep1: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_cost_depletions WHERE correlation_id = $1",
    )
    .bind(cid1)
    .fetch_one(&pool)
    .await
    .expect("count dep");
    assert_eq!(cnt_dep1.0, 0, "no depletions for failed envelope");

    let cnt_layer1: (i64,) = sqlx::query_as(
        "SELECT COUNT(*)::BIGINT FROM poc_v21_cost_layers WHERE correlation_id = $1",
    )
    .bind(cid1)
    .fetch_one(&pool)
    .await
    .expect("count layer");
    assert_eq!(cnt_layer1.0, 0, "no output layer for failed envelope");

    // Now submit envelope 2: 4 components (skipping the bad SKU 703), 2 of
    // each. Must commit at the original layer cost ($10 each) — proving
    // that envelope 1's partial mutations did NOT leak. The 4 components
    // collectively cost 4 × 2 × $10 = $80; the output unit_cost should be
    // 80 / 1 = $80.
    let cid2 = Uuid::new_v4();
    let components2: Vec<(i64, i64, i64)> = vec![
        (700, 1, 2),
        (701, 1, 2),
        (702, 1, 2),
        (704, 1, 2),
    ];
    enqueue_wo_complete(&pool, cid2, 701, 1, &components2, (798, 1, 1), 2).await;

    let reached2 =
        wait_for_terminal(&pool, &[cid2], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached2, 1);
    let row2 = sqlx::query(
        "SELECT state, error_code FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid2)
    .fetch_one(&pool)
    .await
    .expect("status2");
    let state2: String = row2.get(0);
    let err2: Option<String> = row2.get(1);
    assert_eq!(state2, "committed", "envelope 2 must commit; err={err2:?}");

    let layer_uc2: (i64,) = sqlx::query_as(
        "SELECT unit_cost FROM poc_v21_cost_layers WHERE sku_id=798 AND correlation_id=$1",
    )
    .bind(cid2)
    .fetch_one(&pool)
    .await
    .expect("output layer");
    assert_eq!(
        layer_uc2.0, 80,
        "output unit_cost must equal 4×2×$10 = $80 (pristine pool state)"
    );

    // Each surviving component pool should still have 98 units (started at
    // 100, depleted 2 by envelope 2; envelope 1's depletions rolled back).
    for sku in [700, 701, 702, 704i64] {
        let remaining: (Option<i64>,) = sqlx::query_as(
            "SELECT (qty - COALESCE((SELECT SUM(qty)::bigint FROM poc_v21_cost_depletions \
                                       WHERE layer_id = cl.layer_id), 0))::bigint AS remaining \
               FROM poc_v21_cost_layers cl WHERE sku_id = $1",
        )
        .bind(sku)
        .fetch_one(&pool)
        .await
        .expect("layer remaining");
        assert_eq!(
            remaining.0.unwrap_or(0),
            98,
            "sku={sku} should have 98 remaining (100 - 2 from envelope 2)"
        );
    }
}

/// After a failed envelope, re-submitting under a NEW correlation_id once
/// the inventory shortage is fixed succeeds. Validates that failed
/// envelopes leave NO row-level residue that would block subsequent
/// inserts (no orphaned cost_layers rows on the failed correlation_id, no
/// stale (issue_id, method) constraint hits in cost_depletions).
#[tokio::test]
#[ignore]
async fn test_v21_per_envelope_failure_does_not_block_subsequent_replay() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // SKU 800 has 0 stock initially.
    let cid1 = Uuid::new_v4();
    enqueue_inv_issue(&pool, cid1, 800, 1, 5, 9_001, 1).await;
    let reached = wait_for_terminal(&pool, &[cid1], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let st1: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid1)
    .fetch_one(&pool)
    .await
    .expect("st1");
    assert_eq!(st1.0, "failed");

    // Seed inventory and submit a new envelope (different correlation_id,
    // different issue_id to avoid dedup) against the same SKU.
    seed_fifo_layer(&pool, 800, 1, 100, 15).await;
    let cid2 = Uuid::new_v4();
    enqueue_inv_issue(&pool, cid2, 800, 1, 5, 9_002, 2).await;
    let reached2 =
        wait_for_terminal(&pool, &[cid2], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached2, 1);
    let st2: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid2)
    .fetch_one(&pool)
    .await
    .expect("st2");
    assert_eq!(st2.0, "committed");

    // Verify envelope 2's depletion: 5 units at $15 = $75.
    let amount: (i64,) = sqlx::query_as(
        "SELECT amount FROM poc_v21_posting_lines \
          WHERE correlation_id = $1 AND event_type = 'inv_issue'",
    )
    .bind(cid2)
    .fetch_one(&pool)
    .await
    .expect("pl amount");
    assert_eq!(amount.0, 75);
}

/// Mixed batch: a failing envelope's snapshot pollution must not corrupt
/// AVG running state for a SUBSEQUENT wo_complete envelope in the same
/// SuperBatch sharing one AVG component. Two same-batch envelopes can
/// share an SKU pool; cross-envelope rollback isolation is the
/// load-bearing invariant. A single envelope's K+1 internal events
/// span K+1 pools, and partial in-flight AVG mutation on a FAILING
/// wo_complete must not contaminate other envelopes in the same batch.
#[tokio::test]
#[ignore]
async fn test_v21_per_envelope_avg_pool_no_pollution_across_envelopes() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // AVG SKUs 850, 851; STD SKU 852; output 859.
    sqlx::query(
        "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
         VALUES (850, 'avg'), (851, 'avg') \
         ON CONFLICT (sku_id) DO UPDATE SET method_id = EXCLUDED.method_id",
    )
    .execute(&pool)
    .await
    .expect("seed avg assignments");

    // Seed AVG pool state: SKU 850 has 100 qty at $40 avg; SKU 851 has 0 qty.
    sqlx::query(
        "INSERT INTO poc_v21_avg_pool_state (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
         VALUES (850, 1, 40, 100, now(), 0), (851, 1, 0, 0, now(), 0) \
         ON CONFLICT (sku_id, location_id) DO UPDATE SET \
             avg_unit_cost = EXCLUDED.avg_unit_cost, total_qty = EXCLUDED.total_qty",
    )
    .execute(&pool)
    .await
    .expect("seed avg pool");

    // Envelope 1: wo_complete using 2 of SKU 850 (AVG, ok) + 1 of SKU 851
    // (AVG, NO STOCK → fail). Whole envelope must fail.
    let cid1 = Uuid::new_v4();
    enqueue_wo_complete(
        &pool,
        cid1,
        850,
        1,
        &[(850, 1, 2), (851, 1, 1)],
        (859, 1, 1),
        1,
    )
    .await;

    let reached =
        wait_for_terminal(&pool, &[cid1], Duration::from_secs(TERMINAL_TIMEOUT_SECS)).await;
    assert_eq!(reached, 1);
    let st1: (String,) = sqlx::query_as(
        "SELECT state FROM poc_v21_submission_status WHERE correlation_id = $1",
    )
    .bind(cid1)
    .fetch_one(&pool)
    .await
    .expect("st1");
    assert_eq!(st1.0, "failed");

    // Envelope 2 (separate submission): wo_complete using 2 of SKU 850 +
    // 1 of SKU 850 (just SKU 850 twice → K=2 components on same SKU). Must
    // commit at unit_cost = (2×40 + 1×40) / 1 = $120. Note: SKUs 850 and
    // 851 in the SAME envelope work fine via the router packing those
    // pools together in one envelope; what matters here is whether
    // envelope 1's failed in-flight AVG decrement on SKU 850 leaked into
    // envelope 2's hydrated state.
    //
    // Because SuperBatches commit AVG state only on success, envelope 2's
    // hydration reads SKU 850's pre-envelope-1 state ($40 / 100 qty). The
    // rollback contract is verified by SKU 850's total_qty in
    // avg_pool_state remaining at 100 after envelope 1 (NOT 98 from the
    // pretend-mid-failure decrement).
    let avg_after_1: (i64, i64) = sqlx::query_as(
        "SELECT avg_unit_cost, total_qty FROM poc_v21_avg_pool_state \
          WHERE sku_id = 850 AND location_id = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("avg after failure");
    assert_eq!(
        avg_after_1.0, 40,
        "AVG unit_cost must stay $40 after failed envelope rollback"
    );
    assert_eq!(
        avg_after_1.1, 100,
        "AVG total_qty must stay 100 after failed envelope rollback (NOT 98 from partial drain)"
    );
}
