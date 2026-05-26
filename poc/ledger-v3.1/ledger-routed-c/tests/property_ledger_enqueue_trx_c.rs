//! acct-yojk.3 property test for `ledger_enqueue_trx_c` (per the acct-1cer
//! property-test-per-entry-point convention; AUDIT.md D6.1).
//!
//! The direct entry point already has `property_ledger_submit_trx_c.rs`; this is
//! its routed sibling — the higher-risk surface (shmem staging queue, router
//! batching, multi-committer drop-and-continue). Each case picks a random
//! aggregate method (fifo / lifo / wac / std), a small pool universe, and a
//! random multi-caller op mix, enqueues it through the routed path, drains the
//! committer to quiescence, then checks the §11.1 cross-flavor invariant:
//!
//!   E1  routed final aggregate qty (pool_state.layer_id = 0) == the model's net
//!       signed qty per pool — i.e. exactly what the direct flavor produces. Net
//!       qty per pool is the order-independent sum of signed line qtys, so for a
//!       deep-seeded universe (no oversell drops) the routed batched apply MUST
//!       land the same aggregate qty the caller-serial direct path does. A
//!       mismatch means a submission was dropped, duplicated, or misapplied.
//!       (Aggregate unit_cost MAY diverge across flavors — running-average
//!       provisional cost is order-sensitive — so it is NOT asserted, per §11.1.)
//!   E2  no orphan trx (every trx has ≥1 trx_line and ≥1 posting_line).
//!   E3  zero materialized layers (layer_id > 0) — Path C never iterates layers
//!       on the hot path for these methods (the §3.5 architectural premise).
//!   E4  aggregate qty never negative (no-negative-inventory, §3.6).
//!
//! Coverage of the routed-specific paths the §11.1 invariant must survive:
//!   - **drop-and-continue**: each case may inject "poison" depletions whose qty
//!     exceeds any possible pool balance; the committer drops them and continues
//!     with the rest of the group. The model excludes them from the expected qty,
//!     so a poison that silently *applied* (or that aborted its whole group)
//!     would surface as an E1 mismatch.
//!   - **router multi-submission batching**: the interleaved multi-caller stream
//!     puts several submissions touching shared pools in flight together, so the
//!     router forms multi-submission commit_groups (the §6.7 collapse) rather
//!     than degenerate one-per-group batches.
//!
//! Two routed paths are deliberately NOT probed here and are covered
//! deterministically by the acceptance suite instead, because randomizing them
//! against a 4-committer cluster would race rather than test:
//!   - **pre-flight dedup** — `acceptance_routed_committer::
//!     duplicate_source_caught_by_preflight_dedup`. This test uses globally
//!     unique (trx_type, source_id) by construction; injecting duplicates under
//!     the default `committer_count = 4` could have two workers process the two
//!     copies' groups simultaneously, where the UNIQUE backstop (not dedup)
//!     fires and poisons a group — a real but separately-tested edge, not a
//!     qty-equivalence property.
//!   - **eject** — `acceptance_routed_orphan_recovery` + the committer's
//!     `classify_and_eject` unit tests. Eject is a transient re-batching path
//!     (an in-progress caller XID is bounced and retried after cooldown); it
//!     cannot change the *final* aggregate qty once the cluster has fully
//!     drained, which is exactly the post-drain state E1 asserts.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::{Duration, Instant};

/// Deep starting balance per pool so normal depletions never oversell — keeps net
/// qty order-independent (the precondition for cross-flavor qty equivalence).
const SEED_QTY: i64 = 1_000_000;
const SEED_COST: i64 = 50;
const STD_COST: i64 = 50;
/// A depletion larger than any reachable balance → always dropped, in any order,
/// by any committer. Exercises drop-and-continue without making qty order-sensitive.
const POISON_QTY: i64 = 2_000_000;

#[derive(Clone, Debug)]
enum Op {
    Receipt { pool: usize, qty: i64, cost: i64 },
    Deplete { pool: usize, qty: i64 },
    Poison { pool: usize },
}

#[derive(Clone, Debug)]
struct Case {
    method: &'static str,
    n_pools: usize,
    callers: Vec<Vec<Op>>,
}

fn method_strategy() -> impl Strategy<Value = &'static str> {
    prop_oneof![Just("fifo"), Just("lifo"), Just("wac"), Just("std")]
}

fn op_strategy(n_pools: usize) -> impl Strategy<Value = Op> {
    let pool = 0..n_pools;
    prop_oneof![
        // Weighted so receipts/depletions dominate but poison shows up regularly.
        4 => (pool.clone(), 1i64..=50, 1i64..=100)
            .prop_map(|(pool, qty, cost)| Op::Receipt { pool, qty, cost }),
        3 => (pool.clone(), 1i64..=40).prop_map(|(pool, qty)| Op::Deplete { pool, qty }),
        1 => pool.clone().prop_map(|pool| Op::Poison { pool }),
    ]
}

fn case_strategy() -> impl Strategy<Value = Case> {
    (method_strategy(), 1usize..=3)
        .prop_flat_map(|(method, n_pools)| {
            let callers = prop::collection::vec(
                prop::collection::vec(op_strategy(n_pools), 1..=4),
                2..=3,
            );
            (Just(method), Just(n_pools), callers)
        })
        .prop_map(|(method, n_pools, callers)| Case { method, n_pools, callers })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs running poc_v3_1 with ledger_routed_c (test_hooks) preloaded"]
async fn routed_aggregate_qty_equivalent_to_direct_across_random_submissions() {
    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = connect_pool().await;
    let mut runner = TestRunner::new(Config { cases, ..Default::default() });

    runner
        .run(&case_strategy(), |case| {
            let pool = pool.clone();
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(run_one_case(pool, case.clone()))
            });
            result.map_err(TestCaseError::fail)
        })
        .expect("routed equivalence property failed");
}

async fn run_one_case(pool: PgPool, case: Case) -> Result<(), String> {
    reset_state(&pool).await;

    // ── Seed the pool universe; every pool starts deep so depletions have
    // headroom and the net signed qty stays order-independent. ──
    let mut fixtures = Vec::with_capacity(case.n_pools);
    let mut expected_qty = vec![SEED_QTY; case.n_pools];
    for p in 0..case.n_pools {
        let id = (p + 1) as i64;
        let f = seed_pool(&pool, id, id, id, case.method, "running_avg").await;
        if case.method == "std" {
            seed_standard_cost(&pool, f.sku_id, f.loc_id, STD_COST).await;
        }
        seed_aggregate(&pool, f.pool_id, SEED_QTY, SEED_COST).await;
        fixtures.push(f);
    }

    // ── Interleave callers round-robin into one enqueue stream (models several
    // callers submitting concurrently into the staging queue) and assign each a
    // globally unique source_id. Build the expected per-pool net qty as we go. ──
    let max_len = case.callers.iter().map(Vec::len).max().unwrap_or(0);
    let mut stream: Vec<(&'static str, i64, Value)> = Vec::new();
    let mut next_source = 1i64;
    let mut expected_trx = 0i64;
    for tick in 0..max_len {
        for caller in &case.callers {
            let Some(op) = caller.get(tick) else { continue };
            let source_id = next_source;
            next_source += 1;
            match *op {
                Op::Receipt { pool: pi, qty, cost } => {
                    let f = &fixtures[pi];
                    expected_qty[pi] += qty;
                    expected_trx += 1;
                    stream.push(("po_receipt", source_id, receipt_line(f, qty, cost, case.method)));
                }
                Op::Deplete { pool: pi, qty } => {
                    let f = &fixtures[pi];
                    expected_qty[pi] -= qty;
                    expected_trx += 1;
                    stream.push(("transfer_shipment", source_id, depletion_line(f, qty)));
                }
                Op::Poison { pool: pi } => {
                    // Dropped at commit → contributes nothing to qty or trx count.
                    let f = &fixtures[pi];
                    stream.push(("transfer_shipment", source_id, depletion_line(f, POISON_QTY)));
                }
            }
        }
    }

    // ── Enqueue the whole stream, then drain the committer to quiescence. ──
    for (trx_type, source_id, line) in &stream {
        enqueue(&pool, trx_type, *source_id, vec![line.clone()])
            .await
            .map_err(|e| format!("enqueue source_id={source_id} ({trx_type}): {e}"))?;
    }
    await_quiescent(&pool, expected_trx).await?;

    // ── E2 — no orphan trx. ──
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
        return Err(format!("E2: {orphan} orphan trx (method={})", case.method));
    }

    // ── E3 — no materialized layers leaked onto the hot path. ──
    let layers: i64 = scalar(&pool, "SELECT count(*) FROM pool_state WHERE layer_id > 0").await?;
    if layers != 0 {
        return Err(format!("E3: {layers} layer rows on the hot path (method={})", case.method));
    }

    // ── E1 — routed aggregate qty == model (== direct); E4 — never negative. ──
    for p in 0..case.n_pools {
        let pool_id = (p + 1) as i64;
        let agg = aggregate(&pool, pool_id).await;
        let got = agg.map(|(q, _)| q).unwrap_or(0);
        if got != expected_qty[p] {
            return Err(format!(
                "E1: pool {pool_id} routed qty {got} != model qty {} (method={})",
                expected_qty[p], case.method
            ));
        }
        if got < 0 {
            return Err(format!("E4: pool {pool_id} negative aggregate qty {got}"));
        }
    }

    Ok(())
}

/// A po_receipt line; STD pools carry the variance account for the PPV leg (§3.3).
fn receipt_line(f: &Fixture, qty: i64, cost: i64, method: &str) -> Value {
    let mut v = json!({
        "pool_id": f.pool_id,
        "line_type": "po_receipt_line",
        "qty": qty,
        "unit_cost": cost,
        "debit_account": f.inv_acct,
        "credit_account": f.ap_acct,
    });
    if method == "std" {
        v["variance_account"] = json!(f.var_acct);
    }
    v
}

/// Poll until the committer has materialized every non-dropped submission AND the
/// staging queue is fully drained (no slot in any non-`empty` state), so the next
/// case starts clean — the cluster-per-binary runner does NOT reset shmem between
/// cases within a binary, and reset_state's TRUNCATE only touches DB tables.
///
/// Committed-count + staging-drained are the direct quiescence signals: they tell
/// us every submission has been applied (or dropped) and no slot is still in
/// flight, independent of arena reclaim timing.
async fn await_quiescent(pool: &PgPool, expected_trx: i64) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let committed = trx_count(pool).await;
        let inflight = staging_inflight(pool).await;
        if committed >= expected_trx && inflight == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "drain timeout: committed {committed}/{expected_trx}, staging_inflight {inflight}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Total staging slots in any non-`empty` state (pending / routed / processing /
/// abandoned) — i.e. work still in flight. Zero means the queue has fully drained.
async fn staging_inflight(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(count), 0)::bigint \
           FROM ledger_routed_c_staging_state_counts() WHERE state <> 'empty'",
    )
    .fetch_one(pool)
    .await
    .expect("staging inflight count")
}

async fn scalar(pool: &PgPool, q: &str) -> Result<i64, String> {
    sqlx::query_scalar(q).fetch_one(pool).await.map_err(|e| e.to_string())
}
