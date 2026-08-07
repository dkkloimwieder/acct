//! Property test for `ledger_submit_trx` (acct-1cer convention).
//!
//! Each case picks a random method (wac / fifo / lifo / std) and a random
//! receipt/depletion mix, tracks the expected strict-gate outcomes Rust-side,
//! and checks the v3.2 invariants after the workload:
//!
//!   I1  every trx has ≥ 1 trx_line (one submission per op; failures roll the
//!       whole submission back, so trx count == successful ops)
//!   I2  inventory_receipt / inventory_depletion posting amount ==
//!       |qty| × trx_line.unit_cost (variance legs excluded)
//!   I3  aggregate qty == Σ signed trx_line.qty
//!   I4  zero layer rows (these methods never materialize layers on the hot
//!       path; layers are specific's and, later, recalc's)
//!   I5  every trx_line has NULL source_trx_line_id
//!   I6  trx_line.posted_at == trx.posted_at (the D1 denorm)
//!   I7  strict-method gate: wac/std depletions beyond on-hand raise 23000 and
//!       write nothing; the aggregate never goes negative
//!   I8  alt-C posture: fifo/lifo depletions NEVER raise, the aggregate may go
//!       negative, and NO posting_line exists (no hot-path cost leg)
//!   I9  whenever the final aggregate qty > 0, unit_cost ==
//!       banker_div(value_sum, qty) — the derived-average identity, checked
//!       through the SQL banker_div itself
//!
//! The random method × op mix is the class-confusion catch-net: a depletion
//! priced off the wrong basis, a gate leaking onto the alt-C path (or vice
//! versa), or a lost aggregate update all surface here.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::json;
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

fn case_strategy() -> impl Strategy<Value = (&'static str, Vec<Op>)> {
    (
        prop_oneof![Just("fifo"), Just("lifo"), Just("wac"), Just("std")],
        proptest::collection::vec(op_strategy(), 1..=MAX_OPS),
    )
}

async fn run_case(pool: &PgPool, method: &str, ops: &[Op]) {
    reset_state(pool).await;
    let f = seed_fixture(pool, method, "running_avg").await;
    if method == "std" {
        set_standard_cost(pool, &f, STD_COST).await;
    }

    let strict = matches!(method, "wac" | "std");
    let mut expected_qty: i64 = 0;
    let mut successes: i64 = 0;

    for (i, op) in ops.iter().enumerate() {
        let source_id = 1000 + i as i64;
        match *op {
            Op::Receipt { qty, unit_cost } => {
                submit(
                    pool,
                    "po_receipt",
                    source_id,
                    json!([line(f.pool_id, "po_receipt_line", qty, unit_cost)]),
                )
                .await
                .unwrap_or_else(|e| panic!("receipt must succeed ({method}, op {i}): {e}"));
                expected_qty += qty;
                successes += 1;
            }
            Op::Deplete { qty } => {
                let res = submit(
                    pool,
                    "inv_adjustment",
                    source_id,
                    json!([line(f.pool_id, "inv_adjustment_line", -qty, 0)]),
                )
                .await;
                if strict && qty > expected_qty {
                    // I7: the gate rejects with 23000 and writes nothing.
                    let err = res.expect_err(&format!(
                        "strict overdraw must raise ({method}, op {i}, qty {qty} > {expected_qty})"
                    ));
                    assert_eq!(sqlstate(&err), "23000", "gate SQLSTATE ({method}, op {i})");
                } else {
                    // I8 for fifo/lifo: never raises; strict in-bounds succeeds.
                    res.unwrap_or_else(|e| {
                        panic!("depletion must succeed ({method}, op {i}): {e}")
                    });
                    expected_qty -= qty;
                    successes += 1;
                }
            }
        }
    }

    // I1 — trx count == successes, every trx has a line.
    assert_eq!(count(pool, "SELECT count(*) FROM trx").await, successes, "I1 trx count");
    assert_eq!(
        count(pool, "SELECT count(*) FROM trx t WHERE NOT EXISTS \
                     (SELECT 1 FROM trx_line l WHERE l.trx_id = t.id)")
        .await,
        0,
        "I1 no orphan trx"
    );

    // I3 — aggregate qty is the signed line sum (and the tracked expectation).
    let agg = aggregate(pool, f.pool_id).await;
    let signed_sum =
        count(pool, "SELECT COALESCE(SUM(qty), 0)::bigint FROM trx_line").await;
    let agg_qty = agg.map(|(q, _, _)| q).unwrap_or(0);
    assert_eq!(agg_qty, signed_sum, "I3 aggregate == signed line sum");
    assert_eq!(agg_qty, expected_qty, "I3 aggregate == tracked expectation");

    // I7 / I8 — strictness vs flagged-negative posture.
    if strict {
        assert!(agg_qty >= 0, "I7 strict aggregate never negative");
    } else {
        assert_eq!(
            count(pool, "SELECT count(*) FROM posting_line").await,
            0,
            "I8 alt-C posts no cost leg"
        );
    }

    // I2 — receipt/depletion posting amounts tie to their line.
    assert_eq!(
        count(
            pool,
            "SELECT count(*) FROM posting_line p JOIN trx_line t ON t.id = p.trx_line_id \
              WHERE p.event_type IN ('inventory_receipt', 'inventory_depletion') \
                AND p.amount <> abs(t.qty) * t.unit_cost"
        )
        .await,
        0,
        "I2 posting amount identity"
    );

    // I4 / I5 — no layers, no specific linkage.
    assert_eq!(
        count(pool, "SELECT count(*) FROM pool_state WHERE layer_id > 0").await,
        0,
        "I4 no layer rows"
    );
    assert_eq!(
        count(pool, "SELECT count(*) FROM trx_line WHERE source_trx_line_id IS NOT NULL").await,
        0,
        "I5 no source linkage"
    );

    // I6 — the D1 denorm.
    assert_eq!(
        count(
            pool,
            "SELECT count(*) FROM trx_line l JOIN trx t ON t.id = l.trx_id \
              WHERE l.posted_at IS DISTINCT FROM t.posted_at"
        )
        .await,
        0,
        "I6 posted_at denorm"
    );

    // I9 — derived-average identity whenever the pool ends positive.
    assert_eq!(
        count(
            pool,
            "SELECT count(*) FROM pool_state \
              WHERE layer_id = 0 AND qty > 0 \
                AND unit_cost <> banker_div(value_sum::numeric, qty)"
        )
        .await,
        0,
        "I9 unit_cost == banker_div(value_sum, qty)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn invariants_hold_across_random_submissions() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = TestRunner::default();
    let strategy = case_strategy();

    for case_no in 0..cases {
        let (method, ops) =
            strategy.new_tree(&mut runner).expect("generate case").current();
        println!("case {case_no}: method={method} ops={ops:?}");
        run_case(&pool, method, &ops).await;
    }
}
