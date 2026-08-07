//! Property test for `ledger_close_period` / `ledger_settle_pool`
//! (acct-1cer convention) — the close-time finalize net.
//!
//! Each case drives the REAL loop — `ledger_submit_trx` → logical slot →
//! dirty-set → `ledger_recalc_step` — over a random pool population (fifo /
//! lifo / wac) and a TWO-day business grid whose first day is covered by the
//! accounting period and whose second day is not, so every case randomizes
//! which events are in-period, which form an unsettled next-period tail at
//! close time, and how much drain the close itself must pay. Ingests, steps,
//! and crash-steps interleave randomly; the close is attempted unforced first
//! (gate-report sanity) and then forced — closing amid arbitrary unsettled
//! state is the point. Post-close it checks:
//!
//!   C1  completeness — every fifo/lifo depletion at or before the period end
//!       has a finalized max-generation settlement (the sweep drains swept
//!       pools to head, so ALL their depletions are settled)
//!   C2  oracle equivalence survives the close — an independent in-test layer
//!       walk (only banker_div shared) matches every settled depletion's
//!       max-generation authoritative cost on swept pools
//!   C3  sweep exactness — per swept pool, aggregate value_sum == Σ open-layer
//!       value EXACTLY, and equals the hot path's commit-order fold minus the
//!       telescoped generation deltas minus the swept residue (the three
//!       residue sources — banker rounding, exact-empty flush, uncovered
//!       depletions — all land in the residue GL)
//!   C4  immutability — random backdate attempts into the closed period all
//!       reject with PeriodClosed (SQLSTATE 55000), for every pool method
//!   C5  re-close is a no-op — no new GL, no new settlement generations
//!   C6  the pool keeps living — next-period activity flows, settles on
//!       demand via ledger_settle_pool, and its authoritative costs still
//!       match the reference walk over the full stream (prefix stability
//!       across the closed boundary)
//!   C7  method isolation — wac pools gain no settlement state and are never
//!       swept

mod common;

use common::*;
use ledger_core::banker_div;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::{json, Value};
use sqlx::PgPool;

/// Day one (in-period) + day two (next period, past PERIOD_END).
const TS_GRID: [&str; 8] = [T09, T10, T11, T12, T13, T14, N09, N10];
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

/// (pools, ops, backdate probes (pool_idx, ts_idx ≤ day one), post-close pool)
fn case_strategy() -> impl Strategy<Value = (usize, Vec<Op>, Vec<(usize, usize)>, usize)> {
    (1usize..=3).prop_flat_map(|n_pools| {
        (
            Just(n_pools),
            proptest::collection::vec(op_strategy(n_pools), 4..=14),
            proptest::collection::vec((0..n_pools, 0..6usize), 1..=3),
            0..n_pools,
        )
    })
}

/// Independent strict layer walk (shares only banker_div with the engine).
fn reference_walk(
    newest_first: bool,
    events: &[(i64, i64, i64)],
) -> (Vec<(i64, i64)>, Vec<(i64, i64, i64)>) {
    let mut layers: Vec<(i64, i64, i64)> = Vec::new();
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
        value += remaining as i128 * cost as i128;
        costings.push((id, banker_div(value, want)));
    }
    (costings, layers)
}

/// The pool's physical stream in R-1 order.
async fn physical_events(pool: &PgPool, pid: i64) -> Vec<(i64, i64, i64)> {
    sqlx::query_as(
        "SELECT id, qty, unit_cost FROM trx_line \
          WHERE pool_id = $1 AND line_type <> 'cost_adjustment_line' \
          ORDER BY posted_at, id",
    )
    .bind(pid)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Signed swept residue for a pool: zero-qty cost_adjustment_lines, positive
/// when the sweep moved value out of inventory into variance.
async fn swept_residue(pool: &PgPool, pid: i64) -> i128 {
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT pl.amount, pl.debit_account FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE tl.pool_id = $1 AND tl.line_type = 'cost_adjustment_line' AND tl.qty = 0",
    )
    .bind(pid)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.iter()
        .map(|&(amount, debit)| if debit == VAR_ACCT { amount as i128 } else { -(amount as i128) })
        .sum()
}

/// Oracle-equivalence + derivable-settlement check over a pool's full stream.
async fn assert_oracle_equivalence(pool: &PgPool, pid: i64, method: &str, tag: &str) {
    let events = physical_events(pool, pid).await;
    let (ref_costings, ref_layers) = reference_walk(method == "lifo", &events);
    for &(dep_id, ref_auth) in &ref_costings {
        assert_eq!(
            authoritative_of(pool, dep_id).await,
            Some(ref_auth),
            "{tag}: authoritative mismatch pool {pid} ({method}) depletion {dep_id}"
        );
    }
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
    assert_eq!(layers, expected, "{tag}: layer materialization pool {pid}");
}

async fn run_case(
    pool: &PgPool,
    n_pools: usize,
    ops: &[Op],
    backdates: &[(usize, usize)],
    post_close_pool: usize,
) {
    reset_state(pool).await;
    let consumer = reset_feed(pool).await;

    for p in 0..n_pools {
        let pid = (p + 1) as i64;
        seed_pool(pool, pid, pid, pid, METHODS[p % METHODS.len()], "running_avg").await;
    }
    create_period(pool, 1, PERIOD_START, PERIOD_END).await;

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

    // ── close: unforced first (gate-report sanity), then forced. The close
    // drains synchronously, so no recalc quiesce is needed — closing amid
    // arbitrary unsettled state is the point. The FEED however must be
    // current (the documented close premise, G1 healthy): the gate reads the
    // floor/frontier state only the feed maintains, so a backdated event
    // whose delivery is still in the slot would be invisible to it. ────────
    ingest_until_current(pool, &consumer).await;
    let unforced = close_period(pool, 1, "prop", false).await;
    if unforced["closed"] == json!(false) {
        assert_eq!(unforced["state"], json!("closing"), "gate-fail persists closing");
        // A blocked gate always names its cause: either per-pool drain lag, or
        // the feed-currency leg (0020) reporting why the slot is not current.
        let drain_lag =
            unforced["gate"]["lagging"].as_array().is_some_and(|l| !l.is_empty());
        let feed_stale = unforced["gate"]["feed"]["current"] == json!(false);
        assert!(
            drain_lag || feed_stale,
            "a blocked gate names its laggards or its feed: {unforced}"
        );
        assert!(
            !feed_stale || unforced["gate"]["feed"]["reason"].is_string(),
            "a stale-feed gate states a reason: {unforced}"
        );
    }
    // The unforced attempt may itself have written WAL (the `closing` stamp),
    // which leaves the slot behind the next gate's captured LSN — the feed
    // must be current for every close, not just the first (0020).
    ingest_until_current(pool, &consumer).await;
    let report = close_period(pool, 1, "prop", true).await;
    assert_eq!(report["closed"], json!(true), "forced close closes: {report}");

    for p in 0..n_pools {
        let pid = (p + 1) as i64;
        let method = METHODS[p % METHODS.len()];

        if method == "wac" {
            // C7 — never settled, never swept.
            assert_eq!(
                count(pool, &format!("SELECT count(*) FROM pool_settlement WHERE pool_id = {pid}"))
                    .await,
                0,
                "C7 wac pool {pid} has no settlement row"
            );
            assert_eq!(swept_residue(pool, pid).await, 0, "C7 wac pool {pid} never swept");
            continue;
        }

        let events = physical_events(pool, pid).await;
        let in_scope = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM trx_line t \
              WHERE t.pool_id = $1 AND t.line_type <> 'cost_adjustment_line' \
                AND t.posted_at < ($2::date + 1)::timestamptz)",
        )
        .bind(pid)
        .bind(PERIOD_END)
        .fetch_one(pool)
        .await
        .unwrap();
        if !in_scope {
            continue; // only next-day activity: not gated, not swept
        }

        // C1 — swept pools are drained to head: every depletion settled.
        for &(dep_id, qty, _) in events.iter().filter(|(_, q, _)| *q < 0) {
            assert!(
                authoritative_of(pool, dep_id).await.is_some(),
                "C1 depletion {dep_id} (qty {qty}) of swept pool {pid} is settled"
            );
        }

        // C2 — the reference walk still matches after the close's drain.
        assert_oracle_equivalence(pool, pid, method, "C2").await;

        // C3 — sweep exactness: value_sum == Σ layer value, and the full
        // conservation identity including the swept residue holds.
        let (_, _, value_sum) = aggregate(pool, pid).await.expect("aggregate exists");
        assert_eq!(
            value_sum,
            layer_value(pool, pid).await,
            "C3 value_sum exact at layer value: pool {pid}"
        );

        let (ref_costings, _) = reference_walk(method == "lifo", &events);
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
                let (_, q, observed) = *events.iter().find(|(id, _, _)| *id == dep_id).unwrap();
                (auth - observed) as i128 * (-q) as i128
            })
            .sum();
        let residue = swept_residue(pool, pid).await;
        assert_eq!(
            value_sum as i128,
            hot_value - deltas - residue,
            "C3 conservation incl. sweep: pool {pid}"
        );
    }

    // C4 — random backdates into the closed period reject for every method.
    for (probe_no, &(pool_idx, ts_idx)) in backdates.iter().enumerate() {
        let pid = (pool_idx + 1) as i64;
        let res = sqlx::query_scalar::<_, i64>("SELECT ledger_submit_trx($1, $2, $3, $4::jsonb)")
            .bind("po_receipt")
            .bind(9000 + probe_no as i64)
            .bind(TS_GRID[ts_idx])
            .bind(json!([line(pid, "po_receipt_line", 1, 7)]))
            .fetch_one(pool)
            .await;
        assert_eq!(
            sqlstate(&res.expect_err("backdate into closed period must reject")),
            "55000",
            "C4 PeriodClosed on backdate probe {probe_no} pool {pid}"
        );
    }

    // C5 — re-close is a pure no-op.
    let gl_before = count(pool, "SELECT count(*) FROM posting_line").await;
    let settlements_before = count(pool, "SELECT count(*) FROM cost_settlement").await;
    let again = close_period(pool, 1, "prop-again", false).await;
    assert_eq!(again["already_closed"], json!(true), "C5 re-close no-ops");
    assert_eq!(count(pool, "SELECT count(*) FROM posting_line").await, gl_before, "C5 no new GL");
    assert_eq!(
        count(pool, "SELECT count(*) FROM cost_settlement").await,
        settlements_before,
        "C5 no new settlement generations"
    );

    // C6 — the pool keeps living past the closed boundary: next-period
    // activity settles on demand and the oracle still matches over the full
    // stream (prefix stability across the close).
    let pid = (post_close_pool + 1) as i64;
    let method = METHODS[post_close_pool % METHODS.len()];
    if method != "wac" {
        submit_at(
            pool,
            "po_receipt",
            9100,
            N09,
            json!([line(pid, "po_receipt_line", 6, 55)]),
        )
        .await;
        submit_at(
            pool,
            "inv_adjustment",
            9101,
            N10,
            json!([line(pid, "inv_adjustment_line", -4, 0)]),
        )
        .await;
        // The on-demand settle rides the same feed-maintained floor state as
        // the workers: ingest first, exactly as the continuously running feed
        // would have (a backdated receipt behind the pool's frontier needs its
        // recost floor applied for the R-2 re-cost).
        ingest_all(&consumer).await;
        let settled = settle_pool(pool, pid).await;
        assert_eq!(settled["settled"], json!(true), "C6 settle_pool settles: {settled}");
        assert_oracle_equivalence(pool, pid, method, "C6").await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn close_finalizes_exactly_across_random_workloads() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = TestRunner::default();
    let strategy = case_strategy();

    for case_no in 0..cases {
        let (n_pools, ops, backdates, post_close_pool) =
            strategy.new_tree(&mut runner).expect("generate case").current();
        println!("case {case_no}: pools={n_pools} post_close_pool={post_close_pool} ops={ops:?}");
        run_case(&pool, n_pools, &ops, &backdates, post_close_pool).await;
    }

    drop_feed_slot(&pool).await;
}
