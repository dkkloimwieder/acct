//! Conservation-invariant sweep as a post-run post-condition (acct-0at4.5).
//!
//! Exercises `ledger_verify::run_conservation_sweep` two ways against the direct
//! flavor: (1) after representative workloads it must find NOTHING — including
//! the subtle cases the sweep is designed around (a pool driven empty then
//! refilled, resetting value_sum; a standard-basis pool whose value_sum goes
//! legitimately negative, §15); and (2) after a deliberately-injected imbalance
//! it must catch the specific broken invariant. Each injected case restores a
//! clean slate at the end so a per-binary CI teardown sweep (run-tests.sh) sees
//! consistent state regardless of test order.
//!
//! `#[ignore]` like its siblings: needs `poc_v3_1` with `ledger_direct_c`
//! installed. `--test-threads=1` (shared TRUNCATE) per the binary convention.

mod common;

use common::*;
use ledger_verify::run_conservation_sweep;
use sqlx::PgPool;

/// Assert the sweep is clean; panic with the full violation list otherwise.
async fn assert_clean(pool: &PgPool, ctx: &str) {
    let v = run_conservation_sweep(pool).await.expect("sweep query");
    assert!(v.is_empty(), "{ctx}: expected clean sweep, got {}", ledger_verify::format_violations(&v));
}

/// Run the sweep and assert at least one violation carries `want` as its check
/// slug. Returns the violations for context in a failing assert message.
async fn assert_catches(pool: &PgPool, want: &str) {
    let v = run_conservation_sweep(pool).await.expect("sweep query");
    assert!(
        v.iter().any(|x| x.check == want),
        "expected a `{want}` violation, got {}",
        ledger_verify::format_violations(&v)
    );
}

// ── positive: representative mixed workload leaves a clean sweep ──────

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn mixed_method_workload_is_conservation_clean() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    // One pool per aggregate method + a specific pool, each with receipts and
    // (where headroom allows) depletions.
    let wac = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    let fifo = seed_pool(&pool, 2, 2, 1, "fifo", "running_avg").await;
    let lifo = seed_pool(&pool, 3, 3, 1, "lifo", "running_avg").await;
    let std = seed_pool(&pool, 4, 4, 1, "std", "running_avg").await;
    seed_standard_cost(&pool, 4, 1, 150).await;
    let spec = seed_pool(&pool, 5, 5, 1, "specific", "running_avg").await;

    let mut sid = 0i64;
    let mut recv = |f: &Fixture, q: i64, c: i64| {
        sid += 1;
        (sid, receipt(f, q, c))
    };
    // Receipts.
    for (f, q, c) in [(&wac, 10, 100), (&wac, 5, 130), (&fifo, 8, 90), (&lifo, 8, 90), (&std, 6, 160), (&spec, 4, 250)] {
        let (s, line) = recv(f, q, c);
        submit(&pool, "po_receipt", s, vec![line]).await.expect("receipt");
    }
    // Depletions (partial for aggregates, full for the K=1 specific pool).
    for (f, q) in [(&wac, 6), (&fifo, 3), (&lifo, 3), (&std, 2), (&spec, 4)] {
        sid += 1;
        submit(&pool, "transfer_shipment", sid, vec![depletion(f, q)]).await.expect("depletion");
    }

    assert_clean(&pool, "mixed workload").await;
}

// ── positive: empty-then-refill resets value_sum (the §3.1 subtlety) ─

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn empty_then_refill_is_conservation_clean() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;

    // Receipt, deplete to exactly empty (value_sum reset to 0), refill. A naive
    // "value_sum == Σ over ALL lines" would false-flag; the tail-since-last-empty
    // reconciliation must not.
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r1");
    submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 10)]).await.expect("d1");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((0, 100)), "emptied");
    submit(&pool, "po_receipt", 3, vec![receipt(&f, 7, 250)]).await.expect("r2");
    submit(&pool, "transfer_shipment", 4, vec![depletion(&f, 3)]).await.expect("d2");

    assert_clean(&pool, "empty-then-refill").await;
}

// ── positive: standard-basis over-book drives value_sum negative (§15) ─

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn standard_basis_negative_value_sum_is_conservation_clean() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    // FIFO pool priced on the STANDARD provisional basis: depletions post at
    // standard_cost, decoupled from the running average.
    let f = seed_pool(&pool, 1, 1, 1, "fifo", "standard").await;
    seed_standard_cost(&pool, 1, 1, 200).await;

    // Receive 10 @ 100 (value_sum 1000), deplete 8 @ standard 200 (amount 1600):
    // value_sum = 1000 − 1600 = −600 while qty stays 2 > 0. The sweep must accept
    // this — it reconciles value_sum to net posted, never asserts non-negativity.
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r1");
    submit(&pool, "transfer_shipment", 2, vec![depletion(&f, 8)]).await.expect("d1");
    let (qty, _uc, value_sum) = aggregate_with_value(&pool, f.pool_id).await.expect("agg");
    assert_eq!(qty, 2, "qty");
    assert!(value_sum < 0, "value_sum should be negative here, got {value_sum}");

    assert_clean(&pool, "standard-basis negative value_sum").await;
}

// ── injected imbalances: each broken invariant must be caught ─────────

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn injected_qty_imbalance_is_caught() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r");
    assert_clean(&pool, "pre-injection").await;

    // Corrupt the aggregate on-hand so it no longer equals Σ trx_line.qty.
    sqlx::query("UPDATE pool_state SET qty = qty + 1 WHERE layer_id = 0 AND pool_id = $1")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .expect("inject qty drift");
    assert_catches(&pool, "C1_qty_conservation").await;

    reset_state(&pool).await; // leave a clean slate for the teardown sweep
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn injected_value_sum_imbalance_is_caught() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r");
    assert_clean(&pool, "pre-injection").await;

    // Drift the book value away from the posted line stream.
    sqlx::query("UPDATE pool_state SET value_sum = value_sum + 1000 WHERE layer_id = 0 AND pool_id = $1")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .expect("inject value drift");
    assert_catches(&pool, "C2a_value_accumulator").await;

    reset_state(&pool).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn injected_deleted_posting_is_caught() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r");
    assert_clean(&pool, "pre-injection").await;

    // Delete the receipt's posting_line, orphaning a (non-seed) trx_line.
    sqlx::query("DELETE FROM posting_line WHERE id = (SELECT max(id) FROM posting_line)")
        .execute(&pool)
        .await
        .expect("inject posting delete");
    assert_catches(&pool, "C3_orphan_trx_line").await;

    reset_state(&pool).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn injected_self_posting_is_caught() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    submit(&pool, "po_receipt", 1, vec![receipt(&f, 10, 100)]).await.expect("r");
    assert_clean(&pool, "pre-injection").await;

    // Collapse a posting_line onto a single account: it still "balances" a naive
    // Σdebit==Σcredit but nets that account's movement to zero — a real corruption
    // only the degenerate guard catches.
    sqlx::query("UPDATE posting_line SET debit_account = credit_account WHERE id = (SELECT max(id) FROM posting_line)")
        .execute(&pool)
        .await
        .expect("inject self-posting");
    assert_catches(&pool, "C4_posting_degenerate").await;

    reset_state(&pool).await;
}
