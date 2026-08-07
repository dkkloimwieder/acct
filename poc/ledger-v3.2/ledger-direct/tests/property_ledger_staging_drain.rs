//! Property test for `ledger_staging_drain` (acct-1cer convention): drain
//! equivalence against the direct path.
//!
//! Each case generates a random multi-pool workload (mixed methods, receipts
//! and depletions), applies it twice against identical seeds:
//!
//!   PASS A — direct: one `ledger_submit_trx` per op, recording each op's
//!            success/failure.
//!   PASS B — staged: enqueue every op into `ledger_inbox`, then drain to dry
//!            with random batch sizes.
//!
//! The drain claims in id order == enqueue order == PASS A's order, and applies
//! through the same dispatch, so the two passes must be EQUIVALENT:
//!
//!   E1  identical pool_state (per pool: qty, unit_cost, value_sum, layers)
//!   E2  identical trx_line multiset (pool, line_type, qty, unit_cost)
//!   E3  identical posting_line multiset (event, amount, debit, credit)
//!   E4  drain 'failed' rows == direct-path errors, and every inbox row ends
//!       done or failed (none stuck pending)
//!
//! This pins the transport-equivalence claim: staging changes WHERE the work
//! runs (committer loop, batched commit, subtransaction isolation), never WHAT
//! is written.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::json;
use sqlx::PgPool;

const MAX_OPS: usize = 10;
const STD_COST: i64 = 40;
const METHODS: [&str; 4] = ["wac", "fifo", "lifo", "std"];

#[derive(Clone, Debug)]
struct OpSpec {
    pool_idx: usize, // 0..3
    qty: i64,        // signed: receipt > 0, depletion < 0
    unit_cost: i64,
}

fn case_strategy() -> impl Strategy<Value = (Vec<usize>, Vec<OpSpec>, u8)> {
    (
        proptest::collection::vec(0usize..METHODS.len(), 3), // per-pool method pick
        proptest::collection::vec(
            (0usize..3, prop_oneof![1i64..=40, -40i64..=-1], 1i64..=90),
            1..=MAX_OPS,
        )
        .prop_map(|v| {
            v.into_iter()
                .map(|(pool_idx, qty, unit_cost)| OpSpec { pool_idx, qty, unit_cost })
                .collect()
        }),
        2u8..=5, // drain batch size
    )
}

async fn seed_case(pool: &PgPool, method_picks: &[usize]) -> Vec<Fixture> {
    reset_state(pool).await;
    let mut fixtures = Vec::new();
    for (i, &m) in method_picks.iter().enumerate() {
        let id = i as i64 + 1;
        let f = seed_pool(pool, id, id, id, METHODS[m], "running_avg").await;
        if METHODS[m] == "std" {
            set_standard_cost(pool, &f, STD_COST).await;
        }
        fixtures.push(f);
    }
    fixtures
}

fn op_payload(fixtures: &[Fixture], op: &OpSpec) -> (&'static str, serde_json::Value) {
    let pool_id = fixtures[op.pool_idx].pool_id;
    if op.qty > 0 {
        ("po_receipt", json!([line(pool_id, "po_receipt_line", op.qty, op.unit_cost)]))
    } else {
        ("inv_adjustment", json!([line(pool_id, "inv_adjustment_line", op.qty, 0)]))
    }
}

#[derive(Debug, PartialEq)]
struct StateSnapshot {
    pool_state: Vec<(i64, i64, i64, i64, i64)>,
    trx_lines: Vec<(i64, String, i64, i64)>,
    postings: Vec<(String, i64, i64, i64)>,
}

async fn snapshot(pool: &PgPool) -> StateSnapshot {
    let pool_state = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT pool_id, layer_id, qty, unit_cost, value_sum FROM pool_state \
          ORDER BY pool_id, layer_id",
    )
    .fetch_all(pool)
    .await
    .expect("pool_state snapshot");
    let trx_lines = sqlx::query_as::<_, (i64, String, i64, i64)>(
        "SELECT pool_id, line_type::text, qty, unit_cost FROM trx_line \
          ORDER BY pool_id, line_type, qty, unit_cost, id",
    )
    .fetch_all(pool)
    .await
    .expect("trx_line snapshot");
    let postings = sqlx::query_as::<_, (String, i64, i64, i64)>(
        "SELECT event_type::text, amount, debit_account, credit_account FROM posting_line \
          ORDER BY event_type, amount, debit_account, credit_account, id",
    )
    .fetch_all(pool)
    .await
    .expect("posting snapshot");
    StateSnapshot { pool_state, trx_lines, postings }
}

async fn run_case(pool: &PgPool, method_picks: &[usize], ops: &[OpSpec], batch: u8) {
    // ── PASS A: direct ──────────────────────────────────────────────────
    let fixtures = seed_case(pool, method_picks).await;
    let mut direct_errors = 0i64;
    for (i, op) in ops.iter().enumerate() {
        let (trx_type, payload) = op_payload(&fixtures, op);
        if submit(pool, trx_type, 1000 + i as i64, payload).await.is_err() {
            direct_errors += 1;
        }
    }
    let direct = snapshot(pool).await;

    // ── PASS B: staged ──────────────────────────────────────────────────
    let fixtures = seed_case(pool, method_picks).await;
    for (i, op) in ops.iter().enumerate() {
        let (trx_type, payload) = op_payload(&fixtures, op);
        enqueue(pool, trx_type, 1000 + i as i64, payload).await;
    }
    loop {
        if drain(pool, i32::from(batch)).await == 0 {
            break;
        }
    }
    let staged = snapshot(pool).await;

    // E4 — outcome parity.
    assert_eq!(
        count(pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'pending'").await,
        0,
        "E4 drained dry"
    );
    assert_eq!(
        count(pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'failed'").await,
        direct_errors,
        "E4 failed rows == direct errors"
    );

    // E1/E2/E3 — byte-equal ledger state.
    assert_eq!(staged.pool_state, direct.pool_state, "E1 pool_state");
    assert_eq!(staged.trx_lines, direct.trx_lines, "E2 trx_line multiset");
    assert_eq!(staged.postings, direct.postings, "E3 posting multiset");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn drain_is_equivalent_to_direct() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = TestRunner::default();
    let strategy = case_strategy();

    for case_no in 0..cases {
        let (method_picks, ops, batch) =
            strategy.new_tree(&mut runner).expect("generate case").current();
        println!(
            "case {case_no}: methods={:?} batch={batch} ops={ops:?}",
            method_picks.iter().map(|&m| METHODS[m]).collect::<Vec<_>>()
        );
        run_case(&pool, &method_picks, &ops, batch).await;
    }
}
