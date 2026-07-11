//! Property test for `ledger_recalc_step` (acct-1cer convention) — the
//! recalc-vs-strict-replay equivalence net.
//!
//! Each case drives the REAL loop — `ledger_submit_trx` → logical slot →
//! dirty-set → `ledger_recalc_step` — with a random pool population (fifo /
//! lifo / wac), random receipt/depletion streams over a random business-date
//! grid (so backdating and same-date id-tiebreaks arise naturally from ops
//! whose business date precedes already-settled events), and randomly
//! interleaved feed ingests, engine steps, and rolled-back steps (crash
//! simulation). After quiescing (ingest+drain until nothing moves — bounded,
//! so a floor/requeue livelock fails the test), it checks:
//!
//!   R1  oracle equivalence — per fifo/lifo pool, an INDEPENDENT in-test
//!       layer walk over the pool's physical events in (posted_at, id) order
//!       (only `banker_div` is shared) matches every depletion's
//!       max-generation authoritative cost, regardless of how the engine
//!       split the work across incremental/full/re-cost passes
//!   R2  layer materialization — persisted `pool_state.layer_id > 0` rows
//!       equal the reference walk's final open layers
//!   R3  value reconciliation — aggregate value_sum == the hot path's
//!       commit-order fold (with its exact-empty residue flush) minus the
//!       telescoped generation deltas Σ (auth_final − observed) × qty — exact
//!   R4  bookkeeping at rest — settled_through == the pool's physical stream
//!       head, no recost floor, empty queue
//!   R5  idempotency — re-marking every pool dirty produces pure no-op
//!       passes: nothing written, no generation bump, no GL
//!   R6  derivability — every max-generation settlement reproduces its
//!       authoritative cost from its consumption rows + uncovered remainder
//!       at the depletion's observed cost
//!   R7  method isolation — wac pools never gain settlement state
//!
//! This is where the engine's plumbing (scan bounds, R-1 ordering, floor
//! races, generation deltas, loopback filtering) has to hold under shapes no
//! hand-written case anticipates.

mod common;

use common::*;
use ledger_core::banker_div;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::{json, Value};
use sqlx::PgPool;

const TS_GRID: [&str; 6] = [T09, T10, T11, T12, T13, T14];
const METHODS: [&str; 3] = ["fifo", "lifo", "wac"];

#[derive(Clone, Debug)]
enum Op {
    Receipt { pool_idx: usize, ts_idx: usize, qty: i64, unit_cost: i64 },
    Deplete { pool_idx: usize, ts_idx: usize, qty: i64 },
    Ingest,
    Step,
    CrashStep,
}

fn op_strategy(n_pools: usize) -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..n_pools, 0..TS_GRID.len(), 1i64..=40, 1i64..=400)
            .prop_map(|(p, t, qty, unit_cost)| Op::Receipt { pool_idx: p, ts_idx: t, qty, unit_cost }),
        3 => (0..n_pools, 0..TS_GRID.len(), 1i64..=50)
            .prop_map(|(p, t, qty)| Op::Deplete { pool_idx: p, ts_idx: t, qty }),
        2 => Just(Op::Ingest),
        2 => Just(Op::Step),
        1 => Just(Op::CrashStep),
    ]
}

fn case_strategy() -> impl Strategy<Value = (usize, Vec<Op>)> {
    (1usize..=3).prop_flat_map(|n_pools| {
        (Just(n_pools), proptest::collection::vec(op_strategy(n_pools), 4..=14))
    })
}

/// Independent strict layer walk (shares only banker_div with the engine).
/// Events: (trx_line_id, signed qty, recorded unit_cost) in (posted_at, id)
/// order. Returns per-depletion authoritative costs and the final open layers.
fn reference_walk(
    newest_first: bool,
    events: &[(i64, i64, i64)],
) -> (Vec<(i64, i64)>, Vec<(i64, i64, i64)>) {
    let mut layers: Vec<(i64, i64, i64)> = Vec::new(); // (layer_id, qty, cost), oldest first
    let mut costings = Vec::new();
    for &(id, qty, cost) in events {
        if qty > 0 {
            layers.push((id, qty, cost));
            continue;
        }
        let want = -qty;
        let mut remaining = want;
        let mut value: i128 = 0;
        while remaining > 0 && !layers.is_empty() {
            let idx = if newest_first { layers.len() - 1 } else { 0 };
            let take = remaining.min(layers[idx].1);
            value += take as i128 * layers[idx].2 as i128;
            layers[idx].1 -= take;
            remaining -= take;
            if layers[idx].1 == 0 {
                layers.remove(idx);
            }
        }
        value += remaining as i128 * cost as i128; // uncovered at observed
        costings.push((id, banker_div(value, want)));
    }
    (costings, layers)
}

async fn run_case(pool: &PgPool, n_pools: usize, ops: &[Op]) {
    reset_state(pool).await;
    let consumer = reset_feed(pool).await;

    for p in 0..n_pools {
        let pid = (p + 1) as i64;
        seed_pool(pool, pid, pid, pid, METHODS[p % METHODS.len()], "running_avg").await;
    }

    // ── drive the random interleaving ────────────────────────────────────
    for (i, op) in ops.iter().enumerate() {
        let source_id = 1000 + i as i64;
        match *op {
            Op::Receipt { pool_idx, ts_idx, qty, unit_cost } => {
                let pid = (pool_idx + 1) as i64;
                submit_at(
                    pool,
                    "po_receipt",
                    source_id,
                    TS_GRID[ts_idx],
                    json!([line(pid, "po_receipt_line", qty, unit_cost)]),
                )
                .await;
            }
            Op::Deplete { pool_idx, ts_idx, qty } => {
                let pid = (pool_idx + 1) as i64;
                // wac keeps its strict hot-path gate; a rejected depletion is
                // that path's contract, not this test's subject.
                let res = sqlx::query_scalar::<_, i64>(
                    "SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)",
                )
                .bind("inv_adjustment")
                .bind(source_id)
                .bind(TS_GRID[ts_idx])
                .bind(json!([line(pid, "inv_adjustment_line", -qty, 0)]))
                .fetch_one(pool)
                .await;
                if let Err(e) = res {
                    assert_eq!(sqlstate(&e), "23000", "only the strict qty gate may reject");
                }
            }
            Op::Ingest => {
                consumer.ingest_once(10_000).await.expect("feed ingest");
            }
            Op::Step => {
                recalc_step(pool).await;
            }
            Op::CrashStep => {
                let mut tx = pool.begin().await.unwrap();
                let _: Value = sqlx::query_scalar("SELECT ledger_recalc_step()")
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
                tx.rollback().await.unwrap();
            }
        }
    }

    // ── quiesce: ingest + drain until nothing moves. Bounded — a pool that
    // never settles (floor/requeue livelock) is a failure, not a wait. ──────
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds <= 12, "quiesce did not converge — floor/requeue livelock?");
        let mut inserts = 0usize;
        loop {
            let r = consumer.ingest_once(10_000).await.expect("feed ingest");
            inserts += r.inserts;
            if r.messages == 0 {
                break;
            }
        }
        let passes = drain_recalc(pool).await;
        let floors = count(
            pool,
            "SELECT count(*) FROM pool_settlement WHERE recost_floor_posted_at IS NOT NULL",
        )
        .await;
        let queued = count(pool, "SELECT count(*) FROM recalc_queue").await;
        if inserts == 0 && passes.is_empty() && floors == 0 && queued == 0 {
            break;
        }
    }

    // ── invariants ────────────────────────────────────────────────────────
    for p in 0..n_pools {
        let pid = (p + 1) as i64;
        let method = METHODS[p % METHODS.len()];

        if method == "wac" {
            // R7 — the engine never grows settlement state for strict-hot-path
            // methods.
            assert_eq!(
                count(
                    pool,
                    &format!("SELECT count(*) FROM pool_settlement WHERE pool_id = {pid}")
                )
                .await,
                0,
                "R7 wac pool has no settlement row"
            );
            continue;
        }

        let events: Vec<(i64, i64, i64)> = sqlx::query_as(
            "SELECT id, qty, unit_cost FROM trx_line \
              WHERE pool_id = $1 AND line_type <> 'cost_adjustment_line' \
              ORDER BY posted_at, id",
        )
        .bind(pid)
        .fetch_all(pool)
        .await
        .unwrap();
        let (ref_costings, ref_layers) = reference_walk(method == "lifo", &events);

        // R1 — every depletion's max-generation authoritative == reference.
        for &(dep_id, ref_auth) in &ref_costings {
            let auth = authoritative_of(pool, dep_id).await;
            assert_eq!(
                auth,
                Some(ref_auth),
                "R1 authoritative mismatch: pool {pid} ({method}) depletion {dep_id}"
            );
        }

        // R2 — persisted layers == reference final layers.
        if !events.is_empty() {
            let mut layers: Vec<(i64, i64, i64)> = sqlx::query_as(
                "SELECT layer_id, qty, unit_cost FROM pool_state \
                  WHERE pool_id = $1 AND layer_id > 0 ORDER BY layer_id",
            )
            .bind(pid)
            .fetch_all(pool)
            .await
            .unwrap();
            let mut expected = ref_layers.clone();
            layers.sort();
            expected.sort();
            assert_eq!(layers, expected, "R2 layer materialization: pool {pid}");
        }

        // R3 — value_sum == the hot path's commit-order (id-order) fold minus
        // the telescoped generation deltas Σ (auth_final − observed) × qty.
        // The hot-path fold must include the FL CTE's exact-empty branch: a
        // depletion that zeroes the qty flushes value_sum to 0 (residue
        // flush), so the naive Σ qty×recorded identity is off by that residue.
        if !events.is_empty() {
            let mut by_commit = events.clone();
            by_commit.sort_by_key(|(id, _, _)| *id);
            let mut hot_qty: i64 = 0;
            let mut hot_value: i128 = 0;
            for &(_, q, c) in &by_commit {
                hot_qty += q;
                if q > 0 {
                    hot_value += q as i128 * c as i128;
                } else if hot_qty == 0 {
                    hot_value = 0;
                } else {
                    hot_value -= (-q) as i128 * c as i128;
                }
            }
            let deltas: i128 = ref_costings
                .iter()
                .map(|&(dep_id, auth)| {
                    let (_, q, observed) =
                        *events.iter().find(|(id, _, _)| *id == dep_id).unwrap();
                    (auth - observed) as i128 * (-q) as i128
                })
                .sum();
            let value_sum = aggregate(pool, pid).await.expect("aggregate exists").2;
            assert_eq!(
                value_sum as i128,
                hot_value - deltas,
                "R3 value reconciliation: pool {pid}"
            );
        }

        // R4 — frontier at the stream head, floor clear.
        if let Some(&(last_id, _, _)) = events.last() {
            let s = settlement_of(pool, pid).await.expect("settled pool has a row");
            assert_eq!(s.1, Some(last_id), "R4 settled_through at stream head: pool {pid}");
            assert!(!s.2, "R4 no floor at rest: pool {pid}");
        }

        // R6 — derivability from the consumption rows.
        for &(dep_id, ref_auth) in &ref_costings {
            let draws: Vec<(i64, i64)> = sqlx::query_as(
                "SELECT c.qty, c.unit_cost FROM cost_layer_consumption c \
                  WHERE c.depletion_trx_line_id = $1 \
                    AND c.recalc_generation = (SELECT max(recalc_generation) \
                                                 FROM cost_settlement \
                                                WHERE depletion_trx_line_id = $1)",
            )
            .bind(dep_id)
            .fetch_all(pool)
            .await
            .unwrap();
            let (_, dep_qty, observed) =
                *events.iter().find(|(id, _, _)| *id == dep_id).unwrap();
            let want = -dep_qty;
            let drawn: i64 = draws.iter().map(|(q, _)| q).sum();
            let value: i128 = draws.iter().map(|(q, c)| *q as i128 * *c as i128).sum::<i128>()
                + (want - drawn) as i128 * observed as i128;
            assert_eq!(
                banker_div(value, want),
                ref_auth,
                "R6 derivability: pool {pid} depletion {dep_id}"
            );
        }
    }

    // R5 — idempotency: re-mark everything, drain, nothing may move.
    let gens_before: Vec<(i64, i64)> =
        sqlx::query_as("SELECT pool_id, recalc_generation FROM pool_settlement ORDER BY pool_id")
            .fetch_all(pool)
            .await
            .unwrap();
    let settlements_before = count(pool, "SELECT count(*) FROM cost_settlement").await;
    for p in 0..n_pools {
        mark_dirty(pool, (p + 1) as i64).await;
    }
    for report in drain_recalc(pool).await {
        if report["skipped"] != Value::Null {
            continue; // wac pools drain as method no-ops
        }
        assert_eq!(report["settlements_written"], json!(0), "R5 re-run writes nothing");
        assert_eq!(report["adjustments_posted"], json!(0), "R5 re-run posts nothing");
    }
    let gens_after: Vec<(i64, i64)> =
        sqlx::query_as("SELECT pool_id, recalc_generation FROM pool_settlement ORDER BY pool_id")
            .fetch_all(pool)
            .await
            .unwrap();
    assert_eq!(gens_before, gens_after, "R5 no generation bump on no-op passes");
    assert_eq!(
        count(pool, "SELECT count(*) FROM cost_settlement").await,
        settlements_before,
        "R5 settlement history unchanged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn recalc_matches_strict_replay_across_random_interleavings() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = TestRunner::default();
    let strategy = case_strategy();

    for case_no in 0..cases {
        let (n_pools, ops) = strategy.new_tree(&mut runner).expect("generate case").current();
        println!("case {case_no}: pools={n_pools} ops={ops:?}");
        run_case(&pool, n_pools, &ops).await;
    }

    drop_feed_slot(&pool).await;
}
