//! Property test for lazy per-pool backpressure (recalc-c §5, acct-qm7o.7;
//! acct-1cer convention) — random workloads against the transition rules.
//!
//! Each case seeds one fifo, one lifo, and one wac pool, draws a random
//! (bound, low_water) hysteresis band, and drives random receipts/depletions
//! (direct + staging), feed ingests, engine steps, and targeted settles
//! through the REAL loop. After every op it checks the lever's invariants
//! against a model that mirrors only what the test itself submitted:
//!
//!   B1  exact feed accounting — an ingest raises each fifo/lifo pool's
//!       counter by exactly the physical events committed to it since the
//!       last ingest (wac and the engine's own cost_adjustment_line loopback
//!       never count), and engages exactly the pools whose post-bump counter
//!       reached the bound
//!   B2  engage completeness — at any quiescent point, counter ≥ bound ⇒
//!       throttled (both writers apply the threshold to the value they write)
//!   B3  hysteresis at the engine — a settle on pool P releases iff its
//!       exact-tail reset landed ≤ low_water; inside the band (low_water <
//!       tail < bound) the throttle state is UNCHANGED
//!   B4  admission exactness — a direct submit or staging enqueue touching a
//!       throttled pool rejects with SQLSTATE 53400 and leaves zero residue
//!       (trx / inbox counts unchanged); untouched-pool traffic flows
//!   B5  scope — wac pools never appear in the counter or the throttled set
//!   B6  convergence — after quiescing (drain staged work, ingest, drain the
//!       engine to empty, to a fixpoint), every counter is 0, the throttled
//!       set is empty, no recost floor survives, and the queue is empty —
//!       backpressure can wedge nothing shut
//!
//! The aggregate value identity (R3 in property_recalc_engine) is
//! deliberately NOT asserted here — its known divergence is acct-qm7o.9.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashMap;

const TS_GRID: [&str; 5] = [T09, T10, T11, T12, T13];
/// pool_id 1 = fifo, 2 = lifo, 3 = wac.
const POOLS: [(i64, &str); 3] = [(1, "fifo"), (2, "lifo"), (3, "wac")];

#[derive(Clone, Debug)]
enum Op {
    /// Direct single-line receipt (any pool).
    Receipt { pool_idx: usize, ts_idx: usize, qty: i64, unit_cost: i64 },
    /// Direct single-line depletion (fifo/lifo only — unconditional appends).
    Deplete { fl_idx: usize, ts_idx: usize, qty: i64 },
    /// Staging enqueue of a receipt (fifo/lifo only).
    Enqueue { fl_idx: usize, qty: i64, unit_cost: i64 },
    /// One ledger_staging_drain sweep.
    Drain,
    Ingest,
    Step,
    /// ledger_settle_pool on a fifo/lifo pool.
    Settle { fl_idx: usize },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0..3usize, 0..TS_GRID.len(), 1i64..=20, 1i64..=300)
            .prop_map(|(p, t, qty, unit_cost)| Op::Receipt { pool_idx: p, ts_idx: t, qty, unit_cost }),
        3 => (0..2usize, 0..TS_GRID.len(), 1i64..=15)
            .prop_map(|(p, t, qty)| Op::Deplete { fl_idx: p, ts_idx: t, qty }),
        2 => (0..2usize, 1i64..=20, 1i64..=300)
            .prop_map(|(p, qty, unit_cost)| Op::Enqueue { fl_idx: p, qty, unit_cost }),
        2 => Just(Op::Drain),
        3 => Just(Op::Ingest),
        3 => Just(Op::Step),
        1 => (0..2usize).prop_map(|p| Op::Settle { fl_idx: p }),
    ]
}

fn case_strategy() -> impl Strategy<Value = (i64, i64, Vec<Op>)> {
    (3i64..=8)
        .prop_flat_map(|bound| (Just(bound), 0..=(bound - 1).min(2)))
        .prop_flat_map(|(bound, low)| {
            (Just(bound), Just(low), proptest::collection::vec(op_strategy(), 6..=24))
        })
}

/// The DB-side lever state: (counter, throttled engage_events) per pool.
#[derive(Default, Clone)]
struct BpState {
    counter: HashMap<i64, i64>,
    throttled: HashMap<i64, i64>,
}

async fn bp_state(pool: &PgPool) -> BpState {
    let counter: Vec<(i64, i64)> =
        sqlx::query_as("SELECT pool_id, pending_events FROM recalc_backlog")
            .fetch_all(pool)
            .await
            .expect("read recalc_backlog");
    let throttled: Vec<(i64, i64)> =
        sqlx::query_as("SELECT pool_id, engage_events FROM recalc_backpressure")
            .fetch_all(pool)
            .await
            .expect("read recalc_backpressure");
    BpState { counter: counter.into_iter().collect(), throttled: throttled.into_iter().collect() }
}

/// B2 + B5 — the any-time invariants.
fn assert_global(state: &BpState, bound: i64, ctx: &str) {
    for (pid, method) in POOLS {
        let c = state.counter.get(&pid).copied();
        let t = state.throttled.contains_key(&pid);
        if method == "wac" {
            assert_eq!(c, None, "B5 wac pool {pid} gained a counter ({ctx})");
            assert!(!t, "B5 wac pool {pid} throttled ({ctx})");
        } else if c.unwrap_or(0) >= bound {
            assert!(t, "B2 pool {pid} counter {c:?} ≥ bound {bound} but not throttled ({ctx})");
        }
    }
}

async fn run_case(pool: &PgPool, bound: i64, low: i64, ops: &[Op]) {
    reset_state(pool).await;
    let consumer = reset_feed(pool).await;
    for (pid, method) in POOLS {
        seed_pool(pool, pid, pid, 1, method, "running_avg").await;
    }
    set_backpressure_bounds(pool, bound, low).await;

    let mut source_id = 1_000i64;
    // Physical fifo/lifo events committed but not yet ingested, per pool.
    let mut pending_feed: HashMap<i64, i64> = HashMap::new();
    // Enqueued-but-undrained staging envelopes: (pool_id, qty, unit_cost).
    let mut inbox: Vec<(i64, i64, i64)> = Vec::new();

    for op in ops {
        let prev = bp_state(pool).await;
        match op {
            Op::Receipt { pool_idx, ts_idx, qty, unit_cost } => {
                let (pid, method) = POOLS[*pool_idx];
                source_id += 1;
                let res = submit_lines(
                    pool,
                    "po_receipt",
                    source_id,
                    TS_GRID[*ts_idx],
                    json!([line(pid, "po_receipt_line", *qty, *unit_cost)]),
                )
                .await;
                assert_admission(pool, &prev, pid, res, "Receipt").await;
                if method != "wac" && !prev.throttled.contains_key(&pid) {
                    *pending_feed.entry(pid).or_insert(0) += 1;
                }
            }
            Op::Deplete { fl_idx, ts_idx, qty } => {
                let (pid, _) = POOLS[*fl_idx];
                source_id += 1;
                let res = submit_lines(
                    pool,
                    "inv_adjustment",
                    source_id,
                    TS_GRID[*ts_idx],
                    json!([line(pid, "inv_adjustment_line", -qty, 0)]),
                )
                .await;
                assert_admission(pool, &prev, pid, res, "Deplete").await;
                if !prev.throttled.contains_key(&pid) {
                    *pending_feed.entry(pid).or_insert(0) += 1;
                }
            }
            Op::Enqueue { fl_idx, qty, unit_cost } => {
                let (pid, _) = POOLS[*fl_idx];
                source_id += 1;
                let inbox_before = count(pool, "SELECT count(*) FROM ledger_inbox").await;
                let res = sqlx::query(
                    "INSERT INTO ledger_inbox (trx_type, source_id, posted_at, lines) \
                     VALUES ('po_receipt', $1, $2::timestamptz, $3::jsonb)",
                )
                .bind(source_id)
                .bind(TS)
                .bind(json!([line(pid, "po_receipt_line", *qty, *unit_cost)]))
                .execute(pool)
                .await;
                if prev.throttled.contains_key(&pid) {
                    let err = res.expect_err("B4 enqueue to a throttled pool must reject");
                    assert_eq!(sqlstate(&err), "53400", "B4 trigger SQLSTATE");
                    assert_eq!(
                        count(pool, "SELECT count(*) FROM ledger_inbox").await,
                        inbox_before,
                        "B4 rejected enqueue leaves no inbox residue"
                    );
                } else {
                    res.expect("B4 enqueue to an open pool");
                    inbox.push((pid, *qty, *unit_cost));
                }
            }
            Op::Drain => {
                let claimed = drain(pool, 100).await;
                assert_eq!(claimed as usize, inbox.len(), "drain claims all pending envelopes");
                assert_eq!(
                    count(pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'failed'").await,
                    0,
                    "receipt-only envelopes never fail at apply"
                );
                for (pid, _, _) in inbox.drain(..) {
                    *pending_feed.entry(pid).or_insert(0) += 1;
                }
            }
            Op::Ingest => {
                ingest_all(&consumer).await;
                let now = bp_state(pool).await;
                for (pid, method) in POOLS {
                    if method == "wac" {
                        continue;
                    }
                    let expect =
                        prev.counter.get(&pid).copied().unwrap_or(0)
                            + pending_feed.get(&pid).copied().unwrap_or(0);
                    let got = now.counter.get(&pid).copied().unwrap_or(0);
                    assert_eq!(
                        got, expect,
                        "B1 pool {pid}: counter after ingest (adjustment loopback must not count)"
                    );
                    let was = prev.throttled.contains_key(&pid);
                    let is = now.throttled.contains_key(&pid);
                    assert_eq!(
                        is,
                        was || got >= bound,
                        "B1 pool {pid}: ingest engages iff the post-bump counter reached the bound"
                    );
                }
                pending_feed.clear();
            }
            Op::Step => {
                let report = recalc_step(pool).await;
                if report["claimed"] == json!(true) && report["skipped"] == Value::Null {
                    let pid = report["pool_id"].as_i64().expect("claimed pass has pool_id");
                    assert_settle_transition(pool, &prev, pid, bound, low, "Step").await;
                }
            }
            Op::Settle { fl_idx } => {
                let (pid, _) = POOLS[*fl_idx];
                let report = settle_pool(pool, pid).await;
                assert_eq!(report["settled"], json!(true));
                let now = bp_state(pool).await;
                assert_eq!(
                    now.counter.get(&pid).copied(),
                    Some(0),
                    "B3 settle-to-head resets the counter to the exact (empty) tail"
                );
                assert!(
                    !now.throttled.contains_key(&pid),
                    "B3 settle-to-head (0 ≤ low_water) must release pool {pid}"
                );
            }
        }
        let snap = bp_state(pool).await;
        assert_global(&snap, bound, "after op");
    }

    // ── B6 quiesce: drain staged work, then ingest + engine to a fixpoint ──
    let claimed = drain(pool, 100).await;
    assert_eq!(claimed as usize, inbox.len(), "quiesce drains the leftover envelopes");
    inbox.clear();
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds <= 12, "quiesce did not converge — floor/requeue livelock?");
        ingest_all(&consumer).await;
        drain_recalc(pool).await;
        let queued = count(pool, "SELECT count(*) FROM recalc_queue").await;
        let floors = count(
            pool,
            "SELECT count(*) FROM pool_settlement WHERE recost_floor_posted_at IS NOT NULL",
        )
        .await;
        if queued == 0 && floors == 0 {
            // One more ingest to pick up the final passes' adjustment lines;
            // stable only if it delivers nothing that re-dirties the queue.
            ingest_all(&consumer).await;
            if count(pool, "SELECT count(*) FROM recalc_queue").await == 0 {
                break;
            }
        }
    }
    let end = bp_state(pool).await;
    assert!(end.throttled.is_empty(), "B6 throttled set empty at rest: {:?}", end.throttled);
    for (pid, c) in &end.counter {
        assert_eq!(*c, 0, "B6 pool {pid} counter nonzero at rest");
    }
    assert_global(&end, bound, "at rest");

    drop_feed_slot(pool).await;
}

/// Direct submit with an explicit posted_at, returning the raw result.
async fn submit_lines(
    pool: &PgPool,
    trx_type: &str,
    source_id: i64,
    posted_at: &str,
    lines: Value,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
        .bind(trx_type)
        .bind(source_id)
        .bind(posted_at)
        .bind(lines)
        .fetch_one(pool)
        .await
}

/// B4 — the direct-path admission contract for a single-pool submission.
async fn assert_admission(
    pool: &PgPool,
    prev: &BpState,
    pid: i64,
    res: Result<i64, sqlx::Error>,
    what: &str,
) {
    if prev.throttled.contains_key(&pid) {
        let err = res.expect_err("B4 submit to a throttled pool must reject");
        assert_eq!(sqlstate(&err), "53400", "B4 {what} SQLSTATE");
        // No residue: the reject happened before any write.
        let orphan = count(
            pool,
            "SELECT count(*) FROM trx t WHERE NOT EXISTS \
               (SELECT 1 FROM trx_line l WHERE l.trx_id = t.id)",
        )
        .await;
        assert_eq!(orphan, 0, "B4 rejected {what} left a lineless trx row");
    } else {
        res.expect("B4 submit to an open pool must flow");
    }
}

/// B3 — after an engine settle on `pid`, the reset value dictates the
/// transition: ≥ bound engages, ≤ low_water releases, inside the band the
/// prior state is retained.
async fn assert_settle_transition(
    pool: &PgPool,
    prev: &BpState,
    pid: i64,
    bound: i64,
    low: i64,
    what: &str,
) {
    let now = bp_state(pool).await;
    let c = now.counter.get(&pid).copied().unwrap_or(0);
    let was = prev.throttled.contains_key(&pid);
    let is = now.throttled.contains_key(&pid);
    if c >= bound {
        assert!(is, "B3 {what}: reset to {c} ≥ bound {bound} must engage pool {pid}");
    } else if c <= low {
        assert!(!is, "B3 {what}: reset to {c} ≤ low_water {low} must release pool {pid}");
    } else {
        assert_eq!(is, was, "B3 {what}: reset to {c} inside the band must retain pool {pid}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn backpressure_transitions_hold_across_random_workloads() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let pool = connect_pool().await;
    let mut runner = TestRunner::default();
    for i in 0..cases {
        let (bound, low, ops) = case_strategy()
            .new_tree(&mut runner)
            .expect("generate case")
            .current();
        println!("case {i}: bound={bound} low={low} {} ops", ops.len());
        run_case(&pool, bound, low, &ops).await;
    }
}
