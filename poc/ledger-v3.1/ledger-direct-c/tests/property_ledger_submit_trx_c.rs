//! acct-2ttr.3 property test for `ledger_submit_trx_c` (per the acct-1cer
//! property-test-per-entry-point convention).
//!
//! Each case picks a random aggregate method (fifo / lifo / wac / std) and runs a
//! random mix of receipts and depletions, then checks the Path C invariants after
//! the workload:
//!
//!   I1  no orphan trx (every trx has ≥1 trx_line and ≥1 posting_line)
//!   I2  inventory_receipt/depletion posting amount == |qty| × unit_cost
//!       (variance legs excluded — those are |qty| × |actual−std|)
//!   I3  aggregate qty (pool_state.layer_id = 0) == Σ signed trx_line.qty
//!   I4  zero layer_id>0 rows — Path C never materializes layers for these
//!       methods (the architectural premise §3.5)
//!   I5  every trx_line has NULL source_trx_line_id (no specific layer linkage)
//!   I6  pool_lock row exists for the touched pool
//!   I7  aggregate qty never negative (no-negative-inventory, §3.6)
//!
//! Random method choice + random op mix is where the class-confusion / dispatch
//! catch-net lives: a depletion priced off the wrong basis, a layer leaking onto
//! the hot path, or a lost aggregate update all surface here.

mod common;

use common::*;
use proptest::prelude::*;
use sqlx::PgPool;

const MAX_OPS: usize = 8;
const STD_COST: i64 = 50;

#[derive(Clone, Debug)]
enum Op {
    Receipt { qty: i64, unit_cost: i64 },
    Deplete { qty: i64 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1i64..=50, 1i64..=100).prop_map(|(qty, unit_cost)| Op::Receipt { qty, unit_cost }),
        (1i64..=60).prop_map(|qty| Op::Deplete { qty }),
    ]
}

fn method_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("fifo"), Just("lifo"), Just("wac"), Just("std")]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn invariants_hold_across_random_submissions() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        ..Default::default()
    });

    let strategy = (method_strategy(), prop::collection::vec(op_strategy(), 1..=MAX_OPS));

    runner
        .run(&strategy, |(method, ops)| {
            let pool = pool.clone();
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(run_one_case(pool, method, ops.clone()))
            });
            result.map_err(TestCaseError::fail)
        })
        .expect("invariant property failed");
}

async fn run_one_case(pool: PgPool, method: &str, ops: Vec<Op>) -> Result<(), String> {
    reset_state(&pool).await;
    let f = seed_fixture(&pool, method, "running_avg").await;
    if method == "std" {
        seed_standard_cost(&pool, f.sku_id, f.loc_id, STD_COST).await;
    }

    // Track expected on-hand so we can let oversold depletions error (a valid
    // outcome) without flagging them, while still asserting I3/I7 hold afterward.
    let mut on_hand = 0i64;
    let mut source_id = 0i64;
    let mut any_receipt = false;
    for op in &ops {
        source_id += 1;
        match *op {
            Op::Receipt { qty, unit_cost } => {
                any_receipt = true;
                let line = if method == "std" {
                    receipt_std(&f, qty, unit_cost)
                } else {
                    receipt(&f, qty, unit_cost)
                };
                let r = submit(&pool, "po_receipt", source_id, vec![line]).await;
                if r.is_ok() {
                    on_hand += qty;
                }
            }
            Op::Deplete { qty } => {
                if !any_receipt {
                    continue; // nothing to deplete yet
                }
                let r = submit(&pool, "transfer_shipment", source_id, vec![depletion(&f, qty)]).await;
                if r.is_ok() {
                    on_hand -= qty;
                } else {
                    // Oversell rejection is a valid outcome; on_hand unchanged.
                }
            }
        }
    }

    // I1 — no orphan trx.
    let orphan: i64 = scalar(
        &pool,
        "SELECT count(*) FROM trx t \
           WHERE NOT EXISTS (SELECT 1 FROM trx_line WHERE trx_id = t.id) \
              OR NOT EXISTS (SELECT 1 FROM posting_line pl \
                              JOIN trx_line tl ON tl.id = pl.trx_line_id \
                             WHERE tl.trx_id = t.id)",
    )
    .await?;
    if orphan != 0 {
        return Err(format!("I1: {orphan} orphan trx (method={method})"));
    }

    // I2 — receipt/depletion amounts. Variance legs are excluded.
    let bad_amount: i64 = scalar(
        &pool,
        "SELECT count(*) FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE pl.event_type IN ('inventory_receipt','inventory_depletion') \
            AND pl.amount <> ABS(tl.qty) * tl.unit_cost",
    )
    .await?;
    if bad_amount != 0 {
        return Err(format!("I2: {bad_amount} posting rows amount != |qty|*unit_cost (method={method})"));
    }

    // I3 — aggregate qty == Σ signed trx_line qty.
    let (agg, tl_sum): (i64, i64) = pair(
        &pool,
        "SELECT \
           COALESCE((SELECT qty FROM pool_state WHERE pool_id = $1 AND layer_id = 0), 0), \
           COALESCE((SELECT SUM(qty)::bigint FROM trx_line WHERE pool_id = $1), 0)",
        f.pool_id,
    )
    .await?;
    if agg != tl_sum {
        return Err(format!("I3: aggregate {agg} != trx_line sum {tl_sum} (method={method})"));
    }
    if agg != on_hand {
        return Err(format!("I3: aggregate {agg} != expected on_hand {on_hand} (method={method})"));
    }

    // I4 — no materialized layers for aggregate methods.
    let layers: i64 = scalar(&pool, "SELECT count(*) FROM pool_state WHERE layer_id > 0").await?;
    if layers != 0 {
        return Err(format!("I4: {layers} layer rows leaked onto the hot path (method={method})"));
    }

    // I5 — no source_trx_line_id linkage.
    let linked: i64 =
        scalar(&pool, "SELECT count(*) FROM trx_line WHERE source_trx_line_id IS NOT NULL").await?;
    if linked != 0 {
        return Err(format!("I5: {linked} trx_line rows carry source_trx_line_id (method={method})"));
    }

    // I6 — pool_lock exists for the touched pool (when any work happened).
    let trx_n: i64 = scalar(&pool, "SELECT count(*) FROM trx").await?;
    if trx_n > 0 {
        let locks: i64 = pair_one(&pool, "SELECT count(*) FROM pool_lock WHERE pool_id = $1", f.pool_id).await?;
        if locks != 1 {
            return Err(format!("I6: expected 1 pool_lock for touched pool, got {locks}"));
        }
    }

    // I7 — aggregate qty never negative.
    if agg < 0 {
        return Err(format!("I7: negative aggregate qty {agg} (method={method})"));
    }

    Ok(())
}

async fn scalar(pool: &PgPool, q: &str) -> Result<i64, String> {
    sqlx::query_scalar(q).fetch_one(pool).await.map_err(|e| e.to_string())
}

async fn pair_one(pool: &PgPool, q: &str, arg: i64) -> Result<i64, String> {
    sqlx::query_scalar(q).bind(arg).fetch_one(pool).await.map_err(|e| e.to_string())
}

async fn pair(pool: &PgPool, q: &str, arg: i64) -> Result<(i64, i64), String> {
    sqlx::query_as(q).bind(arg).fetch_one(pool).await.map_err(|e| e.to_string())
}
