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
//!   R3  value reconciliation — aggregate value_sum == an id-order (apply-
//!       order) replay of ALL the pool's lines: physical lines via the FL
//!       hot-path aggregate formulas (including the exact-empty residue
//!       flush, which discards every correction folded in before it),
//!       `cost_adjustment_line` rows via their settlement's
//!       (authoritative − prior) × qty reconcile — exact on every pool,
//!       including flush-wiped shapes; PLUS the reference anchor: each
//!       depletion's settlement chain telescopes to (ref_auth − observed) ×
//!       qty with GL linkage iff a row moved, so a self-consistent engine
//!       (wrong priors, unfolded corrections) cannot satisfy the fold alone
//!   R4  bookkeeping at rest — settled_through == the pool's physical stream
//!       head, no recost floor, empty queue
//!   R5  idempotency — re-marking every pool dirty produces pure no-op
//!       passes: nothing written, no generation bump, no GL
//!   R6  derivability — every max-generation settlement reproduces its
//!       authoritative cost from its consumption rows + uncovered remainder
//!       at the depletion's observed cost
//!   R7  method isolation — wac pools never gain settlement state
//!   R8  generation monotonicity — sampled after every op, quiesce round,
//!       and the R5 re-drain: a pool's committed recalc_generation never
//!       decreases (committed generations key cost_layer_consumption's
//!       primary key, so a regression wedges the next genuine re-cost)
//!   R9  backpressure stays off — sampled with R8: under the default bounds
//!       (200/20) these case sizes never approach the bound, so the throttled
//!       set must remain empty throughout (recalc-c §5 "stays off otherwise";
//!       a counter-residue bug would surface here as a spurious engage)
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
use std::collections::HashMap;

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

/// R8 — sample every pool's committed generation against its running max.
/// R9 — piggybacked: the throttled set stays empty at these case sizes.
async fn assert_gen_monotonic(pool: &PgPool, max_gen: &mut HashMap<i64, i64>) {
    let gens: Vec<(i64, i64)> =
        sqlx::query_as("SELECT pool_id, recalc_generation FROM pool_settlement")
            .fetch_all(pool)
            .await
            .unwrap();
    for (pid, g) in gens {
        let prior = max_gen.entry(pid).or_insert(0);
        assert!(g >= *prior, "R8 generation regressed on pool {pid}: {prior} -> {g}");
        *prior = g;
    }
    let engaged: i64 = sqlx::query_scalar("SELECT count(*) FROM recalc_backpressure")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(engaged, 0, "R9 backpressure engaged under a workload far below the bound");
}

async fn run_case(pool: &PgPool, n_pools: usize, ops: &[Op]) {
    reset_state(pool).await;
    let consumer = reset_feed(pool).await;
    let mut max_gen: HashMap<i64, i64> = HashMap::new();

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
        assert_gen_monotonic(pool, &mut max_gen).await;
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
        assert_gen_monotonic(pool, &mut max_gen).await;
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

        // R3 — value_sum == an id-order replay of ALL the pool's lines. Ids
        // record apply order here: every op in this harness runs to
        // completion before the next, so trx_line ids interleave hot-path
        // commits and engine passes exactly as value_sum experienced them
        // (a guarantee verify.rs V2 lacks under concurrency — it must skip
        // flush-wiped pools; this fold reconciles them exactly). Physical
        // lines apply the FL hot-path formulas — the exact-empty flush
        // (post-event qty 0 ⇒ value := 0) discards every reconcile folded in
        // before it, which the fold reproduces by construction. Each
        // `cost_adjustment_line` applies its settlement's
        // (authoritative − prior) × qty reconcile; per-line application sums
        // to the engine's per-pass net (subtraction is linear). Adjustment
        // lines never touch the aggregate qty, so they cannot fire the flush.
        if !events.is_empty() {
            let all_lines: Vec<(i64, String, i64, i64)> = sqlx::query_as(
                "SELECT id, line_type::text, qty, unit_cost FROM trx_line \
                  WHERE pool_id = $1 ORDER BY id",
            )
            .bind(pid)
            .fetch_all(pool)
            .await
            .unwrap();
            let recons: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
                "SELECT tl.id, cs.authoritative_unit_cost, cs.prior_unit_cost, -tl.qty \
                   FROM trx_line tl \
                   JOIN posting_line pl ON pl.trx_line_id = tl.id \
                   JOIN cost_settlement cs ON cs.adjustment_posting_line_id = pl.id \
                  WHERE tl.pool_id = $1 AND tl.line_type = 'cost_adjustment_line'",
            )
            .bind(pid)
            .fetch_all(pool)
            .await
            .unwrap();
            let recon: HashMap<i64, i128> = recons
                .iter()
                .map(|&(id, auth, prior, dep_qty)| {
                    (id, (auth - prior) as i128 * dep_qty as i128)
                })
                .collect();
            let adj_lines =
                all_lines.iter().filter(|(_, lt, _, _)| lt == "cost_adjustment_line").count();
            assert_eq!(
                adj_lines,
                recon.len(),
                "R3 every adjustment line carries exactly one settlement reconcile: pool {pid}"
            );
            let mut fold_qty: i64 = 0;
            let mut fold_value: i128 = 0;
            for &(id, ref lt, q, c) in &all_lines {
                if lt == "cost_adjustment_line" {
                    fold_value -= recon[&id];
                    continue;
                }
                fold_qty += q;
                if q > 0 {
                    fold_value += q as i128 * c as i128;
                } else if fold_qty == 0 {
                    fold_value = 0;
                } else {
                    fold_value -= (-q) as i128 * c as i128;
                }
            }
            let value_sum = aggregate(pool, pid).await.expect("aggregate exists").2;
            assert_eq!(value_sum as i128, fold_value, "R3 value reconciliation: pool {pid}");
        }

        // R3 anchor — the settlement chain telescopes to the INDEPENDENT
        // reference: per depletion, Σ (authoritative − prior) × qty over its
        // rows == (ref_auth − observed) × qty, and GL linkage exists iff the
        // row moved. The id-order fold above is self-consistent with whatever
        // the engine recorded — an engine that mis-anchors `prior` on re-cost
        // or records moved settlements without folding/posting would satisfy
        // the fold alone. This anchor never reads value_sum, so it stays
        // exact on flush-wiped pools.
        let chains: Vec<(i64, i64, i64, bool)> = sqlx::query_as(
            "SELECT cs.depletion_trx_line_id, cs.authoritative_unit_cost, \
                    cs.prior_unit_cost, cs.adjustment_posting_line_id IS NOT NULL \
               FROM cost_settlement cs \
               JOIN trx_line tl ON tl.id = cs.depletion_trx_line_id \
              WHERE tl.pool_id = $1",
        )
        .bind(pid)
        .fetch_all(pool)
        .await
        .unwrap();
        let mut chain_sum: HashMap<i64, i128> = HashMap::new();
        for &(dep, auth, prior, has_gl) in &chains {
            assert_eq!(
                has_gl,
                auth != prior,
                "R3 GL linkage iff the settlement moved: pool {pid} depletion {dep}"
            );
            *chain_sum.entry(dep).or_default() += (auth - prior) as i128;
        }
        for &(dep_id, ref_auth) in &ref_costings {
            let (_, q, observed) = *events.iter().find(|(id, _, _)| *id == dep_id).unwrap();
            assert_eq!(
                chain_sum.get(&dep_id).copied().unwrap_or(0) * (-q) as i128,
                (ref_auth - observed) as i128 * (-q) as i128,
                "R3 settlement chain telescopes to the reference: pool {pid} depletion {dep_id}"
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
    assert_gen_monotonic(pool, &mut max_gen).await;
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
