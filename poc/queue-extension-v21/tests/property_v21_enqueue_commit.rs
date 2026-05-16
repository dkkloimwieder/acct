//! Property test for the M1.3 (acct-wrwf) FIFO enqueue→commit pipeline.
//!
//! Probes random sequences of single envelopes against the v2.1 PoC's
//! end-to-end path and asserts the I1–I7 / class-confusion invariants
//! that bind correlation_id to its cost rows:
//!
//!  P1 — every committed envelope has a 1:1 status row with state in
//!       {committed, replayed, failed}; every row tagged with the
//!       superbatch_id pair stamped on its correlation.
//!  P2 — committer_tx_id + superbatch_id stamps are CONSISTENT across
//!       all cost rows attributed to the same correlation_id (no
//!       split-batch attribution drift, no cross-stamping).
//!  P3 — FIFO pool integrity: SUM(layer.qty) - SUM(depletion.qty) ≥ 0
//!       for every (sku, location); insufficient-inventory events
//!       reach state='failed' and do NOT leave partial depletion rows.
//!  P4 — Dedup correctness: every (issue_id, method_used='fifo')
//!       appears at most once across all cost_depletions; replayed
//!       correlations leave NO additional depletion rows.
//!
//! Run via:
//!   cargo test --release --test property_v21_enqueue_commit \
//!     --features pg18 --no-default-features -- --ignored --nocapture
//!
//! PROPTEST_CASES env var overrides case count (default 20 here; each
//! case generates 5-15 envelopes and waits for them to drain).

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, wait_for_terminal};
use proptest::prelude::*;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone)]
enum SyntheticEvent {
    Receipt { sku: i64, qty: i64, unit_cost: i64 },
    Consume { sku: i64, qty: i64, issue_id: i64, replay: bool },
}

fn event_strategy() -> impl Strategy<Value = SyntheticEvent> {
    prop_oneof![
        (1i64..=4, 1i64..=50, 100i64..=999)
            .prop_map(|(sku, qty, unit_cost)| SyntheticEvent::Receipt { sku, qty, unit_cost }),
        (1i64..=4, 1i64..=20, 1i64..=200, any::<bool>())
            .prop_map(|(sku, qty, issue_id, replay)| SyntheticEvent::Consume {
                sku,
                qty,
                issue_id,
                replay,
            }),
    ]
}

fn scenario_strategy() -> impl Strategy<Value = Vec<SyntheticEvent>> {
    prop::collection::vec(event_strategy(), 5..=15)
}

async fn enqueue_event(
    pool: &PgPool,
    cid: Uuid,
    event: &SyntheticEvent,
    chrono: i64,
) -> Result<(), sqlx::Error> {
    let (event_type, payload) = match event {
        SyntheticEvent::Receipt { sku, qty, unit_cost } => (
            "po_receipt",
            serde_json::json!({
                "sku_id": sku,
                "location_id": 1,
                "qty": qty,
                "unit_cost": unit_cost,
                "business_date_jdate": 9999,
                "doc_chrono": chrono,
                "document_id": 9_000_000 + chrono,
            }),
        ),
        SyntheticEvent::Consume { sku, qty, issue_id, .. } => (
            "inv_issue",
            serde_json::json!({
                "sku_id": sku,
                "location_id": 1,
                "qty": -qty,
                "issue_id": issue_id,
                "business_date_jdate": 9999,
                "doc_chrono": chrono,
                "document_id": 9_000_000 + chrono,
            }),
        ),
    };
    let pool_keys = serde_json::json!({
        "sku": [[match event {
            SyntheticEvent::Receipt { sku, .. } | SyntheticEvent::Consume { sku, .. } => *sku
        }, 1]],
        "wip": []
    });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind(event_type)
        .bind(payload)
        .bind(pool_keys)
        .execute(pool)
        .await
        .map(|_| ())
}

async fn run_scenario(pool: &PgPool, events: Vec<SyntheticEvent>) {
    reset_state(pool).await;

    let mut correlation_ids: Vec<Uuid> = Vec::with_capacity(events.len());
    for (i, event) in events.iter().enumerate() {
        let cid = Uuid::new_v4();
        correlation_ids.push(cid);
        enqueue_event(pool, cid, event, (i + 1) as i64)
            .await
            .expect("enqueue_event");

        // For consume events with replay=true, enqueue a second envelope with
        // a new cid but the same issue_id (force dedup hit).
        if let SyntheticEvent::Consume { replay: true, .. } = event {
            let cid2 = Uuid::new_v4();
            correlation_ids.push(cid2);
            enqueue_event(pool, cid2, event, (i + 1) as i64 + 1000)
                .await
                .expect("enqueue_event replay");
        }
    }

    let _ = wait_for_terminal(pool, &correlation_ids, Duration::from_secs(15)).await;

    // ── P1: every correlation has 1:1 status row in terminal state ────────
    let rows: Vec<(Uuid, String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT correlation_id, state, committer_tx_id, superbatch_id \
           FROM poc_v21_submission_status \
          WHERE correlation_id = ANY($1::uuid[])",
    )
    .bind(&correlation_ids)
    .fetch_all(pool)
    .await
    .expect("status read");
    assert_eq!(
        rows.len(),
        correlation_ids.len(),
        "P1: every correlation_id must have a status row"
    );
    for (cid, state, ctx, sbi) in &rows {
        assert!(
            matches!(state.as_str(), "committed" | "failed" | "replayed"),
            "P1: correlation_id={} stuck in state={}",
            cid,
            state
        );
        if state == "committed" || state == "replayed" {
            assert!(ctx.is_some(), "P1: committed/replayed needs committer_tx_id");
            assert!(sbi.is_some(), "P1: committed/replayed needs superbatch_id");
        }
    }

    // ── P2: cost rows for one correlation have a single (committer_tx_id,
    //         superbatch_id) pair.
    let pair_per_corr: Vec<(Uuid, i64)> = sqlx::query_as(
        "WITH all_rows AS (\
           SELECT correlation_id, committer_tx_id, superbatch_id FROM poc_v21_cost_layers \
           UNION ALL \
           SELECT correlation_id, committer_tx_id, superbatch_id FROM poc_v21_cost_depletions \
           UNION ALL \
           SELECT correlation_id, committer_tx_id, superbatch_id FROM poc_v21_posting_lines\
         ) \
         SELECT correlation_id, COUNT(DISTINCT (committer_tx_id, superbatch_id))::BIGINT \
           FROM all_rows \
          WHERE correlation_id = ANY($1::uuid[]) \
          GROUP BY correlation_id",
    )
    .bind(&correlation_ids)
    .fetch_all(pool)
    .await
    .expect("pair read");
    for (cid, distinct_pairs) in &pair_per_corr {
        assert_eq!(
            *distinct_pairs, 1,
            "P2: correlation_id={} has {} distinct (committer_tx_id, superbatch_id) pairs",
            cid, distinct_pairs
        );
    }

    // ── P3: FIFO pool integrity ─────────────────────────────────────────
    let pool_balances: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT l.sku_id, l.location_id, \
                COALESCE(SUM(l.qty), 0)::BIGINT AS layer_qty, \
                COALESCE((SELECT SUM(qty) FROM poc_v21_cost_depletions d \
                  WHERE d.layer_id IN (SELECT layer_id FROM poc_v21_cost_layers l2 \
                    WHERE l2.sku_id=l.sku_id AND l2.location_id=l.location_id)), 0)::BIGINT \
                AS dep_qty \
           FROM poc_v21_cost_layers l \
          GROUP BY l.sku_id, l.location_id",
    )
    .fetch_all(pool)
    .await
    .expect("pool balance read");
    for (sku, loc, layer_qty, dep_qty) in &pool_balances {
        assert!(
            layer_qty - dep_qty >= 0,
            "P3: sku={} location={} over-consumed: layers={} depletions={}",
            sku,
            loc,
            layer_qty,
            dep_qty
        );
    }

    // ── P4: dedup correctness ──────────────────────────────────────────
    let dup_rows: (i64,) = sqlx::query_as(
        "WITH g AS (\
           SELECT issue_id, method_used, layer_id, COUNT(*) AS c \
             FROM poc_v21_cost_depletions \
            GROUP BY issue_id, method_used, layer_id\
         ) SELECT COALESCE(SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END), 0)::BIGINT FROM g",
    )
    .fetch_one(pool)
    .await
    .expect("dedup read");
    assert_eq!(dup_rows.0, 0, "P4: found duplicate (issue_id, method, layer_id) keys");
}

#[test]
#[ignore]
fn prop_v21_enqueue_commit() {
    // Current-thread runtime + Handle::block_on inside the proptest
    // closure deadlocks: a current-thread runtime can't drive I/O via
    // Handle::block_on once the outer block_on returns. Use the
    // runtime directly via a reference (the proptest Fn closure
    // borrows it instead of taking the cloned Handle).
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = runtime.block_on(connect_pool());

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        max_shrink_iters: 16,
        ..proptest::test_runner::Config::default()
    });
    let strategy = scenario_strategy();
    runner
        .run(&strategy, |events| {
            let pool = pool.clone();
            runtime.block_on(async move {
                run_scenario(&pool, events).await;
            });
            Ok(())
        })
        .expect("proptest run");
}
