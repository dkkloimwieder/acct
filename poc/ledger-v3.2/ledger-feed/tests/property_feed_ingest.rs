//! Property test for the feed consumer (acct-1cer convention).
//!
//! Each case runs a random multi-pool workload — receipts and depletions at
//! random business times, interleaved with full ingest ticks and simulated
//! crashes (apply without advance) — against randomly pre-seeded settlement
//! frontiers, then checks the feed invariants:
//!
//!   F1  the dirty-set is EXACTLY the set of pools with ≥ 1 trx_line — no
//!       missed pool (a silent gap) and no phantom mark
//!   F2  each settled pool's recost floor is the R-1 minimum (posted_at, id)
//!       over its events strictly behind the settlement frontier, or NULL when
//!       none — independent of how the batches were cut or re-delivered
//!   F3  after the stream drains, another tick is a strict no-op (no inserts,
//!       no marks, no floor movement) — the cursor genuinely advanced
//!
//! F2's batch-independence is the load-bearing property: the guarded-min
//! floor update must converge to the same answer no matter where ingestion
//! pauses, crashes, or re-delivers.

mod common;

use chrono::{DateTime, Utc};
use common::*;
use ledger_feed::FeedConsumer;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::json;
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet};

const MAX_OPS: usize = 24;
const TS_GRID: [&str; 6] = [T09, T10, T11, T12, T13, T14];

#[derive(Clone, Debug)]
enum Op {
    Receipt { pool_idx: usize, qty: i64, unit_cost: i64, ts_idx: usize },
    Deplete { pool_idx: usize, qty: i64, ts_idx: usize },
    /// A full feed tick.
    Ingest,
    /// Apply without advancing the cursor — a crash before the advance.
    IngestNoAdvance,
}

/// Per-pool optional settlement frontier: (timestamp index, frontier id).
/// id 0 vs a huge id exercises both sides of the same-timestamp tiebreak.
type SettlementSeed = Option<(usize, i64)>;

fn op_strategy(n_pools: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..n_pools, 1i64..=50, 1i64..=200, 0..TS_GRID.len())
            .prop_map(|(pool_idx, qty, unit_cost, ts_idx)| Op::Receipt {
                pool_idx, qty, unit_cost, ts_idx
            }),
        2 => (0..n_pools, 1i64..=60, 0..TS_GRID.len())
            .prop_map(|(pool_idx, qty, ts_idx)| Op::Deplete { pool_idx, qty, ts_idx }),
        1 => Just(Op::Ingest),
        1 => Just(Op::IngestNoAdvance),
    ]
}

fn case_strategy() -> impl Strategy<Value = (usize, Vec<SettlementSeed>, Vec<Op>)> {
    (1usize..=4).prop_flat_map(|n_pools| {
        (
            Just(n_pools),
            proptest::collection::vec(
                proptest::option::of((0..TS_GRID.len(), prop_oneof![Just(0i64), Just(1_000_000)])),
                n_pools,
            ),
            proptest::collection::vec(op_strategy(n_pools), 1..=MAX_OPS),
        )
    })
}

async fn run_case(pool: &PgPool, n_pools: usize, settlements: &[SettlementSeed], ops: &[Op]) {
    reset_state(pool).await;
    for i in 0..n_pools {
        let pid = (i + 1) as i64;
        let method = if i % 2 == 0 { "fifo" } else { "lifo" };
        seed_pool(pool, pid, pid, pid, method, "running_avg").await;
    }
    let consumer = reset_feed(pool).await;

    // Frontier seeds stand in for phase-4 settlement state; they must be in
    // place before ingestion so floor checks see them from the first tick.
    let mut frontier: BTreeMap<i64, (DateTime<Utc>, i64)> = BTreeMap::new();
    for (i, seed) in settlements.iter().enumerate() {
        if let Some((ts_idx, frontier_id)) = seed {
            let pid = (i + 1) as i64;
            set_settled_through(pool, pid, TS_GRID[*ts_idx], *frontier_id).await;
            frontier.insert(pid, (ts(TS_GRID[*ts_idx]), *frontier_id));
        }
    }

    let mut source_id = 1000i64;
    for op in ops {
        match op {
            Op::Receipt { pool_idx, qty, unit_cost, ts_idx } => {
                source_id += 1;
                let pid = (*pool_idx + 1) as i64;
                submit_at(
                    pool,
                    "po_receipt",
                    source_id,
                    TS_GRID[*ts_idx],
                    json!([line(pid, "po_receipt_line", *qty, *unit_cost)]),
                )
                .await;
            }
            Op::Deplete { pool_idx, qty, ts_idx } => {
                source_id += 1;
                let pid = (*pool_idx + 1) as i64;
                // fifo/lifo depletions never gate (alt-C posture).
                submit_at(
                    pool,
                    "inv_adjustment",
                    source_id,
                    TS_GRID[*ts_idx],
                    json!([line(pid, "inv_adjustment_line", -qty, 0)]),
                )
                .await;
            }
            Op::Ingest => {
                consumer.ingest_once(10_000).await.expect("mid-run ingest");
            }
            Op::IngestNoAdvance => {
                let batch = consumer.peek(10_000).await.expect("mid-run peek");
                if !batch.events.is_empty() {
                    consumer.apply(&batch.events).await.expect("mid-run apply");
                }
            }
        }
    }

    // Drain the stream, then verify.
    consumer.ingest_once(10_000).await.expect("final ingest");
    check_invariants(pool, &consumer, &frontier).await;
    drop_feed_slot(pool).await;
}

async fn check_invariants(
    pool: &PgPool,
    consumer: &FeedConsumer,
    frontier: &BTreeMap<i64, (DateTime<Utc>, i64)>,
) {
    let events: Vec<(i64, i64, DateTime<Utc>)> =
        sqlx::query_as("SELECT id, pool_id, posted_at FROM trx_line ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("read trx_line");

    // F1 — dirty-set == touched-pool set.
    let touched: BTreeSet<i64> = events.iter().map(|(_, pid, _)| *pid).collect();
    let queued: BTreeSet<i64> = queue_pools(pool).await.into_iter().collect();
    assert_eq!(queued, touched, "F1: recalc_queue must be exactly the touched pools");

    // F2 — floor == min (posted_at, id) behind the frontier, per settled pool.
    for (pid, (st_ts, st_id)) in frontier {
        let expected = events
            .iter()
            .filter(|(id, p, ts)| p == pid && (*ts, *id) < (*st_ts, *st_id))
            .map(|(id, _, ts)| (*ts, *id))
            .min();
        let actual = floor_of(pool, *pid).await;
        assert_eq!(
            actual, expected,
            "F2: pool {pid} floor vs frontier ({st_ts}, {st_id})"
        );
    }
    // ... and pools with no frontier never grow one.
    let phantom_floors = count(
        pool,
        "SELECT count(*) FROM pool_settlement \
         WHERE settled_through_posted_at IS NULL AND recost_floor_posted_at IS NOT NULL",
    )
    .await;
    assert_eq!(phantom_floors, 0, "F2: no floor without a frontier");

    // F3 — the drained stream is quiet.
    let report = consumer.ingest_once(10_000).await.expect("F3 ingest");
    assert_eq!(report.inserts, 0, "F3: no redelivery after advance");
    assert_eq!(report.pools_marked, 0, "F3: no phantom marks");
    assert_eq!(report.floors_lowered, 0, "F3: no floor movement");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn property_feed_ingest() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = TestRunner::default();
    let strategy = case_strategy();

    for case_no in 0..cases {
        let (n_pools, settlements, ops) =
            strategy.new_tree(&mut runner).expect("generate case").current();
        println!("case {case_no}: n_pools={n_pools} settlements={settlements:?} ops={ops:?}");
        run_case(&pool, n_pools, &settlements, &ops).await;
    }
}
