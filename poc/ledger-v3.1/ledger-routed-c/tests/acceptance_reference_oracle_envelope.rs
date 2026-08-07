//! Envelope-mode reference-oracle check (acct-0at4.4, design-v3.1 §9.3 / §14.2).
//!
//! The routed flavor batches submissions across callers and reorders within a
//! batch, and drops a submission that under-runs (drop-and-continue, no
//! pristine-replay, §6.8). Exact per-line equality is therefore undefined — so
//! instead of one answer the `ledger-oracle` model supplies the *set of legal
//! answers* and we check the observed `pool_state` is explainable by SOME
//! serialization of the submitted multiset:
//!
//!  - **Receipts-only pools are still byte-exact** — `value_sum` accumulates
//!    exactly and the running average is order-independent (§3.1), so even the
//!    routed reorder must land on `receipts_only_aggregate` to the unit.
//!  - **Final on-hand qty must be reachable** under drop-and-continue — checked
//!    exactly by enumeration for small op counts, and by sound conservation
//!    bounds + a sub-multiset-sum witness for large ones. A DB↔model
//!    cross-check ties which depletions the committer actually dropped (via
//!    per-source trx presence) to the observed qty.
//!
//! `#[ignore]` like its siblings: needs `poc_v3_1` with `ledger_routed_c`
//! installed and its router + committer BGWorkers running.

mod common;

use std::time::{Duration, Instant};

use common::*;
use ledger_oracle::envelope::{explains_final_qty, receipts_only_aggregate, Verdict};
use sqlx::PgPool;

/// Poll until the routed pipeline is fully drained: every enqueued submission has
/// been committed or dropped (arena empty), nothing pending in staging, and no
/// committer group left ready/in-flight. Enqueue bumps `arena_outstanding`
/// synchronously, so calling this after all enqueues is race-free.
async fn await_drained(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let arena = arena_outstanding(pool).await;
        let ready = committer_queue_count(pool, "ready").await;
        let in_flight = committer_queue_count(pool, "in_flight").await;
        let pending = staging_pending(pool).await;
        if arena == 0 && ready == 0 && in_flight == 0 && pending == 0 {
            return;
        }
        if Instant::now() >= deadline {
            panic!("routed pipeline did not drain: arena={arena} ready={ready} in_flight={in_flight} pending={pending}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Aggregate `(qty, unit_cost, value_sum)` for a pool, or None.
async fn aggregate_with_value(pool: &PgPool, pool_id: i64) -> Option<(i64, i64, i64)> {
    sqlx::query_as("SELECT qty, unit_cost, value_sum FROM pool_state WHERE pool_id = $1 AND layer_id = 0")
        .bind(pool_id)
        .fetch_optional(pool)
        .await
        .expect("read aggregate with value_sum")
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c installed + BGWorkers"]
async fn receipts_only_is_byte_exact_through_routed_batching() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;

    // Many receipts with varied cost, enqueued as independent submissions the
    // router is free to batch and reorder. Receipts never drop.
    let receipts: Vec<(i64, i64)> = (1..=30)
        .map(|i| {
            let qty = 1 + (i % 7);
            let cost = 100 + 37 * (i % 13);
            (qty, cost)
        })
        .collect();
    for (i, &(qty, cost)) in receipts.iter().enumerate() {
        enqueue(&pool, "po_receipt", (i + 1) as i64, vec![receipt_line_for(&f, qty, cost)])
            .await
            .expect("enqueue receipt");
    }
    await_drained(&pool).await;

    // All 30 receipts committed; the aggregate must equal the exact order-
    // independent running average to the unit, regardless of batch reorder.
    let expected = receipts_only_aggregate((0, 0), &receipts);
    let observed = aggregate_with_value(&pool, f.pool_id).await.expect("aggregate present");
    assert_eq!(
        observed, expected,
        "receipts-only aggregate diverged from the order-independent model"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c installed + BGWorkers"]
async fn reachable_final_qty_small_n_exact_enumeration() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;

    // 2 receipts (total 15 on hand) then 3 depletions totalling 17 > 15 — at least
    // one must drop under drop-and-continue. 5 ops ≤ enumeration cap → the exact
    // legal set. (source_id, signed_qty).
    let ops: [(i64, i64); 5] = [(1, 10), (2, 5), (3, -6), (4, -7), (5, -4)];
    for &(sid, q) in &ops {
        let line = if q > 0 { receipt_line_for(&f, q, 100) } else { depletion_line(&f, -q) };
        let trx_type = if q > 0 { "po_receipt" } else { "transfer_shipment" };
        enqueue(&pool, trx_type, sid, vec![line]).await.expect("enqueue");
    }
    await_drained(&pool).await;

    let observed_qty = aggregate(&pool, f.pool_id).await.expect("aggregate present").0;
    let signed: Vec<i64> = ops.iter().map(|&(_, q)| q).collect();
    let verdict = explains_final_qty(0, &signed, observed_qty);
    assert!(matches!(verdict, Verdict::Exact { .. }), "small-N should enumerate exactly");
    assert!(verdict.explained(), "observed final qty {observed_qty} is not a reachable serialization outcome");

    // DB↔model cross-check: sum the depletions the committer actually applied
    // (per-source trx present) and confirm it accounts for the drawdown exactly.
    let mut applied_depletion = 0i64;
    for &(sid, q) in &ops {
        if q < 0 && trx_count_for_source(&pool, sid).await == 1 {
            applied_depletion += -q;
        }
        if q > 0 {
            assert_eq!(trx_count_for_source(&pool, sid).await, 1, "receipt {sid} must commit");
        }
    }
    let total_receipts: i64 = ops.iter().filter(|&&(_, q)| q > 0).map(|&(_, q)| q).sum();
    assert_eq!(observed_qty, total_receipts - applied_depletion, "conservation: on-hand = receipts − applied depletions");
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c installed + BGWorkers"]
async fn reachable_final_qty_large_n_bounds_and_subset_sum() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;

    // 12 receipts of 10 (=120 available) and 6 depletions of 25 (=150 demanded):
    // some depletions must drop. 18 ops > enumeration cap → the sound bounds +
    // sub-multiset-sum witness path.
    let mut ops: Vec<(i64, i64)> = Vec::new();
    let mut sid = 0i64;
    for _ in 0..12 {
        sid += 1;
        ops.push((sid, 10));
    }
    for _ in 0..6 {
        sid += 1;
        ops.push((sid, -25));
    }
    for &(s, q) in &ops {
        let line = if q > 0 { receipt_line_for(&f, q, 100) } else { depletion_line(&f, -q) };
        let trx_type = if q > 0 { "po_receipt" } else { "transfer_shipment" };
        enqueue(&pool, trx_type, s, vec![line]).await.expect("enqueue");
    }
    await_drained(&pool).await;

    let observed_qty = aggregate(&pool, f.pool_id).await.expect("aggregate present").0;
    let signed: Vec<i64> = ops.iter().map(|&(_, q)| q).collect();
    let verdict = explains_final_qty(0, &signed, observed_qty);
    assert!(matches!(verdict, Verdict::Bounds { .. }), "large-N should take the bounds path");
    assert!(
        verdict.explained(),
        "observed final qty {observed_qty} failed conservation bounds / subset-sum witness: {verdict:?}"
    );

    // The applied-depletion total must be a multiple of 25 (all depletions are 25),
    // and equal receipts − observed.
    let total_receipts = 120i64;
    let applied = total_receipts - observed_qty;
    assert!(applied >= 0 && applied % 25 == 0, "applied depletion {applied} must be a sum of 25s");
}
