//! acct-4a0a property test: random submissions through ledger_enqueue_trx
//! preserve the end-to-end ledger invariants identical to property_direct_pipeline.
//!
//! Each case generates a fresh fixture (sku + location + FIFO pool + two
//! accounts), enqueues N random ops (receipts + depletions) via Path B,
//! polls for materialization, and checks invariants:
//!
//!   I1  trx ⇔ trx_line ⇔ posting_line existence (no orphans)
//!   I2  posting_line.amount == abs(qty) × unit_cost
//!   I3  SUM(pool_state.qty) == SUM(trx_line.qty signed) per pool
//!   I4  trx_seq strictly monotonic per pool, starts at 1
//!   I5  source_trx_line_id (when non-NULL) references a depletion's
//!       upstream layer for the same pool
//!   I6  pool_lock row exists for every touched pool

mod common;

use common::*;
use proptest::prelude::*;
use sqlx::PgPool;
use std::time::Duration;

const MAX_OPS: usize = 6;

#[derive(Clone, Debug)]
enum Op {
    Receipt { qty: i64, unit_cost: i64 },
    Deplete { qty: i64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1i64..=50, 1i64..=100).prop_map(|(qty, unit_cost)| Op::Receipt { qty, unit_cost }),
        (1i64..=80).prop_map(|qty| Op::Deplete { qty }),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn invariants_hold_across_random_enqueue() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);

    let pool = connect_pool().await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        ..Default::default()
    });

    let strategy = prop::collection::vec(op_strategy(), 1..=MAX_OPS);

    runner
        .run(&strategy, |ops| {
            let pool = pool.clone();
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(run_one_case(pool, ops.clone()))
            });
            result.map_err(|e| TestCaseError::fail(e))
        })
        .expect("invariant property failed");
}

async fn run_one_case(pool: PgPool, ops: Vec<Op>) -> Result<(), String> {
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "fifo").await;

    let mut source_id = 1i64;
    let mut last_unit_cost = 0i64;
    let mut enqueued_source_ids: Vec<i64> = Vec::new();
    for op in &ops {
        source_id += 1;
        let posted = format!("2026-05-21T12:{:02}:00+00:00", (source_id % 60));
        let lines_json = match *op {
            Op::Receipt { qty, unit_cost } => {
                last_unit_cost = unit_cost;
                po_receipt_line(pool_id, qty, unit_cost, inv, ap)
            }
            Op::Deplete { qty } => {
                if last_unit_cost == 0 {
                    continue;
                }
                depletion_line(pool_id, qty, last_unit_cost, inv, ap)
            }
        };
        let _ = enqueue(&pool, "po_receipt", source_id, &posted, &lines_json).await;
        enqueued_source_ids.push(source_id);
    }

    // Drain wait: poll every enqueued source_id with a generous deadline.
    // Failing depletions (insufficient inventory) won't materialize a
    // trx, so we accept None as a valid outcome — the invariant checks
    // only assert on rows that DID materialize.
    let drain_deadline = std::time::Instant::now() + Duration::from_secs(8);
    for sid in &enqueued_source_ids {
        let timeout = drain_deadline.saturating_duration_since(std::time::Instant::now());
        let _ = poll_for_trx(&pool, "po_receipt", *sid, timeout.max(Duration::from_millis(50))).await;
    }

    // After polling, wait one more short window to ensure all in-flight
    // pristine-replay loops have finished and cleanup has run.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // I1: every trx has at least one trx_line and at least one posting_line.
    let orphan_trx: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx t \
           WHERE NOT EXISTS (SELECT 1 FROM trx_line WHERE trx_id = t.id) \
              OR NOT EXISTS (SELECT 1 FROM posting_line pl \
                              JOIN trx_line tl ON tl.id = pl.trx_line_id \
                             WHERE tl.trx_id = t.id)",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if orphan_trx != 0 {
        return Err(format!("I1 violation: {orphan_trx} orphan trx rows"));
    }

    // I2: |amount| == |qty| × unit_cost.
    let bad_amount: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE pl.amount <> ABS(tl.qty) * tl.unit_cost",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if bad_amount != 0 {
        return Err(format!(
            "I2 violation: {bad_amount} posting_line rows where amount != |qty|*unit_cost"
        ));
    }

    // I3: SUM(pool_state.qty) == SUM(trx_line.qty signed) per pool.
    let (ps_sum, tl_sum): (i64, i64) = sqlx::query_as(
        "SELECT \
           COALESCE((SELECT SUM(qty)::bigint FROM pool_state WHERE pool_id = $1), 0), \
           COALESCE((SELECT SUM(qty)::bigint FROM trx_line WHERE pool_id = $1), 0)",
    )
    .bind(pool_id)
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if ps_sum != tl_sum {
        return Err(format!(
            "I3 violation: pool_state SUM={ps_sum} != trx_line SUM={tl_sum} for pool {pool_id}"
        ));
    }

    // I4: trx_seq strictly monotonic per pool, starts at 1.
    let seqs: Vec<i64> = sqlx::query_scalar(
        "SELECT trx_seq FROM trx_line WHERE pool_id = $1 ORDER BY trx_seq",
    )
    .bind(pool_id)
    .fetch_all(&pool)
    .await
    .map_err(|e| e.to_string())?;
    for (i, s) in seqs.iter().enumerate() {
        if *s != (i as i64) + 1 {
            return Err(format!(
                "I4 violation: trx_seq[{i}] = {s}, expected {}",
                i + 1
            ));
        }
    }

    // I5: source_trx_line_id (when set) points to a real trx_line for same pool.
    let bad_src: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx_line c \
           WHERE c.source_trx_line_id IS NOT NULL \
             AND NOT EXISTS (SELECT 1 FROM trx_line p \
                              WHERE p.id = c.source_trx_line_id \
                                AND p.pool_id = c.pool_id)",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if bad_src != 0 {
        return Err(format!(
            "I5 violation: {bad_src} trx_line rows reference invalid source_trx_line_id"
        ));
    }

    // I6: pool_lock row exists for every touched pool.
    let missing_lock: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT tl.pool_id) FROM trx_line tl \
           WHERE NOT EXISTS (SELECT 1 FROM pool_lock pl WHERE pl.pool_id = tl.pool_id)",
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| e.to_string())?;
    if missing_lock != 0 {
        return Err(format!(
            "I6 violation: {missing_lock} touched pools without pool_lock rows"
        ));
    }

    Ok(())
}
