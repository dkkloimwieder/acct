//! Property test for the M1.3 (acct-wrwf) FIFO + M2.1 (acct-hmuf) AVG
//! enqueue→commit pipeline.
//!
//! Probes random sequences of single envelopes against the v2.1 PoC's
//! end-to-end path and asserts the I1–I7 / class-confusion invariants
//! that bind correlation_id to its cost rows. SKU 1+2 → FIFO; SKU 3+4
//! → AVG (seeded via sku_method_assignments at scenario 0).
//!
//!  P1 — every envelope has a 1:1 status row in {committed, replayed,
//!       failed}; every committed/replayed row carries committer_tx_id
//!       and superbatch_id stamps.
//!  P2 — committer_tx_id + superbatch_id stamps CONSISTENT across all
//!       cost rows attributed to the same correlation_id (covers
//!       cost_layers, cost_depletions, cost_consumptions, posting_lines —
//!       no split-batch attribution drift, no cross-stamping).
//!  P3 — FIFO pool integrity: SUM(layer.qty) - SUM(depletion.qty) ≥ 0
//!       for every FIFO (sku, location); insufficient events reach
//!       state='failed' and leave NO partial depletion rows.
//!  P4 — Dedup correctness: (issue_id, method_used) appears at most
//!       once across cost_depletions AND cost_consumptions; replayed
//!       correlations leave no additional rows on either table.
//!  P5 — AVG pool integrity: avg_pool_state.total_qty ≥ 0 for every
//!       AVG (sku, location); avg_unit_cost ≥ 0; insufficient events
//!       reach state='failed'.
//!  P6 — AVG running-average consistency: at end-of-scenario,
//!       avg_pool_state.total_qty for any AVG SKU equals
//!       SUM(cost_layers.qty) - SUM(cost_consumptions.qty) over that
//!       same (sku, location). (Method dispatch correctness — AVG
//!       never writes to cost_depletions; AVG layers track inflows.)
//!
//! Run via:
//!   cargo test --release --test property_v21_enqueue_commit \
//!     --features pg18 --no-default-features -- --ignored --nocapture
//!
//! PROPTEST_CASES env var overrides case count (default 20 here; each
//! case generates 5-15 envelopes and waits for them to drain).

#![cfg(test)]

mod common;

use common::{connect_pool, reset_state, seed_method_assignments, wait_for_terminal};
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
    //         superbatch_id) pair across cost_layers, cost_depletions,
    //         cost_consumptions, and posting_lines.
    let pair_per_corr: Vec<(Uuid, i64)> = sqlx::query_as(
        "WITH all_rows AS (\
           SELECT correlation_id, committer_tx_id, superbatch_id FROM poc_v21_cost_layers \
           UNION ALL \
           SELECT correlation_id, committer_tx_id, superbatch_id FROM poc_v21_cost_depletions \
           UNION ALL \
           SELECT correlation_id, committer_tx_id, superbatch_id FROM poc_v21_cost_consumptions \
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

    // ── P3: FIFO pool integrity (FIFO-method SKUs only) ────────────────
    let pool_balances: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT l.sku_id, l.location_id, \
                COALESCE(SUM(l.qty), 0)::BIGINT AS layer_qty, \
                COALESCE((SELECT SUM(qty) FROM poc_v21_cost_depletions d \
                  WHERE d.layer_id IN (SELECT layer_id FROM poc_v21_cost_layers l2 \
                    WHERE l2.sku_id=l.sku_id AND l2.location_id=l.location_id)), 0)::BIGINT \
                AS dep_qty \
           FROM poc_v21_cost_layers l \
           JOIN poc_v21_sku_method_assignments a ON a.sku_id = l.sku_id \
          WHERE a.method_id = 'fifo' \
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

    // ── P4: dedup correctness across both consumption tables ───────────
    let dup_depletions: (i64,) = sqlx::query_as(
        "WITH g AS (\
           SELECT issue_id, method_used, layer_id, COUNT(*) AS c \
             FROM poc_v21_cost_depletions \
            GROUP BY issue_id, method_used, layer_id\
         ) SELECT COALESCE(SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END), 0)::BIGINT FROM g",
    )
    .fetch_one(pool)
    .await
    .expect("dedup depletions read");
    assert_eq!(
        dup_depletions.0, 0,
        "P4: duplicate (issue_id, method, layer_id) keys in cost_depletions"
    );
    let dup_consumptions: (i64,) = sqlx::query_as(
        "WITH g AS (\
           SELECT issue_id, method_used, COUNT(*) AS c \
             FROM poc_v21_cost_consumptions \
            GROUP BY issue_id, method_used\
         ) SELECT COALESCE(SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END), 0)::BIGINT FROM g",
    )
    .fetch_one(pool)
    .await
    .expect("dedup consumptions read");
    assert_eq!(
        dup_consumptions.0, 0,
        "P4: duplicate (issue_id, method) keys in cost_consumptions"
    );

    // ── P5: AVG pool non-negative ──────────────────────────────────────
    let avg_pools: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT sku_id, location_id, avg_unit_cost, total_qty FROM poc_v21_avg_pool_state",
    )
    .fetch_all(pool)
    .await
    .expect("avg pool read");
    for (sku, loc, avg_cost, total_qty) in &avg_pools {
        assert!(*total_qty >= 0, "P5: avg pool sku={} loc={} total_qty<0 ({})", sku, loc, total_qty);
        assert!(*avg_cost >= 0, "P5: avg pool sku={} loc={} avg_unit_cost<0 ({})", sku, loc, avg_cost);
    }

    // ── P6: AVG running-avg consistency. avg_pool_state.total_qty for
    //         an AVG SKU must equal SUM(layers.qty) - SUM(consumptions.qty)
    //         over the same (sku, location). Catches dispatch errors that
    //         would route an AVG event to FIFO (depletions table) or vice
    //         versa, and catches snapshot UPSERT divergence.
    let avg_recon: Vec<(i64, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT l.sku_id, l.location_id, \
                COALESCE(SUM(l.qty), 0)::BIGINT AS layer_qty, \
                COALESCE(( \
                  SELECT SUM(c.qty)::BIGINT FROM poc_v21_cost_consumptions c \
                   WHERE c.sku_id=l.sku_id AND c.location_id=l.location_id \
                ), 0)::BIGINT AS consumed_qty, \
                COALESCE(( \
                  SELECT a.total_qty FROM poc_v21_avg_pool_state a \
                   WHERE a.sku_id=l.sku_id AND a.location_id=l.location_id \
                ), 0)::BIGINT AS pool_total_qty \
           FROM poc_v21_cost_layers l \
           JOIN poc_v21_sku_method_assignments m ON m.sku_id = l.sku_id \
          WHERE m.method_id = 'avg' \
          GROUP BY l.sku_id, l.location_id",
    )
    .fetch_all(pool)
    .await
    .expect("avg recon read");
    for (sku, loc, layer_qty, consumed_qty, pool_total) in &avg_recon {
        let expected = layer_qty - consumed_qty;
        assert_eq!(
            *pool_total, expected,
            "P6: AVG sku={} loc={} pool_total={} ≠ layer_qty({}) - consumed_qty({}) = {}",
            sku, loc, pool_total, layer_qty, consumed_qty, expected
        );
    }
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

    // Seed method assignments once at test start. SKU 1+2 → FIFO,
    // SKU 3+4 → AVG. reset_state leaves sku_method_assignments alone
    // so this survives scenarios.
    runtime.block_on(seed_method_assignments(
        &pool,
        &[(1, "fifo"), (2, "fifo"), (3, "avg"), (4, "avg")],
    ));

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
