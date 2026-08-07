//! Conservation-invariant sweep against the routed flavor (acct-0at4.5).
//!
//! The sweep is drop-agnostic — a submission the committer drops under
//! drop-and-continue (§6.8) leaves NO trx / trx_line / posting_line and no
//! pool_state delta, so `pool_state.qty == Σ trx_line.qty` (and every value
//! reconciliation) holds on exactly the committed subset. This binary drives a
//! real routed workload through the shmem queue + committer BGWorkers, drains,
//! and asserts the sweep is clean; then it injects an imbalance and asserts it is
//! caught. Injected cases restore a clean DB slate at the end.
//!
//! `#[ignore]` like its siblings: needs `poc_v3_1` with `ledger_routed_c`
//! installed and its router + committer BGWorkers running.

mod common;

use std::time::{Duration, Instant};

use common::*;
use ledger_verify::run_conservation_sweep;
use sqlx::PgPool;

/// Poll until the routed pipeline is fully drained (arena empty, nothing pending
/// in staging, no committer group ready/in-flight). Enqueue bumps
/// `arena_outstanding` synchronously, so this is race-free after all enqueues.
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

async fn assert_clean(pool: &PgPool, ctx: &str) {
    let v = run_conservation_sweep(pool).await.expect("sweep query");
    assert!(v.is_empty(), "{ctx}: expected clean sweep, got {}", ledger_verify::format_violations(&v));
}

async fn assert_catches(pool: &PgPool, want: &str) {
    let v = run_conservation_sweep(pool).await.expect("sweep query");
    assert!(
        v.iter().any(|x| x.check == want),
        "expected a `{want}` violation, got {}",
        ledger_verify::format_violations(&v)
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c installed + BGWorkers"]
async fn routed_workload_is_conservation_clean() {
    let pool = connect_pool().await;
    reset_state(&pool).await;

    let wac = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    let fifo = seed_pool(&pool, 2, 2, 1, "fifo", "running_avg").await;
    let lifo = seed_pool(&pool, 3, 3, 1, "lifo", "running_avg").await;
    let std = seed_pool(&pool, 4, 4, 1, "std", "running_avg").await;
    seed_standard_cost(&pool, 4, 1, 150).await;
    let spec = seed_pool(&pool, 5, 5, 1, "specific", "running_avg").await;

    // Phase 1: receipts. Drain so every pool is stocked before any depletion is
    // routed (routed reorder could otherwise drop a depletion that races ahead of
    // its receipt — harmless to the sweep, but this keeps the workload fully
    // applied so the post-condition covers real depletions).
    let mut sid = 0i64;
    for (f, q, c) in [(&wac, 10, 100), (&wac, 5, 130), (&fifo, 8, 90), (&lifo, 8, 90), (&std, 6, 160), (&spec, 4, 250)] {
        sid += 1;
        enqueue(&pool, "po_receipt", sid, vec![receipt_line_for(f, q, c)]).await.expect("enqueue receipt");
    }
    await_drained(&pool).await;

    // Phase 2: depletions.
    for (f, q) in [(&wac, 6), (&fifo, 3), (&lifo, 3), (&std, 2), (&spec, 4)] {
        sid += 1;
        enqueue(&pool, "transfer_shipment", sid, vec![depletion_line(f, q)]).await.expect("enqueue depletion");
    }
    await_drained(&pool).await;

    assert_clean(&pool, "routed workload").await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c installed + BGWorkers"]
async fn routed_injected_qty_imbalance_is_caught() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    enqueue(&pool, "po_receipt", 1, vec![receipt_line_for(&f, 10, 100)]).await.expect("enqueue");
    await_drained(&pool).await;
    assert_clean(&pool, "pre-injection").await;

    sqlx::query("UPDATE pool_state SET qty = qty + 1 WHERE layer_id = 0 AND pool_id = $1")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .expect("inject qty drift");
    assert_catches(&pool, "C1_qty_conservation").await;

    reset_state(&pool).await;
}

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_routed_c installed + BGWorkers"]
async fn routed_injected_value_sum_imbalance_is_caught() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_pool(&pool, 1, 1, 1, "wac", "running_avg").await;
    enqueue(&pool, "po_receipt", 1, vec![receipt_line_for(&f, 10, 100)]).await.expect("enqueue");
    await_drained(&pool).await;
    assert_clean(&pool, "pre-injection").await;

    sqlx::query("UPDATE pool_state SET value_sum = value_sum + 1000 WHERE layer_id = 0 AND pool_id = $1")
        .bind(f.pool_id)
        .execute(&pool)
        .await
        .expect("inject value drift");
    assert_catches(&pool, "C2a_value_accumulator").await;

    reset_state(&pool).await;
}
