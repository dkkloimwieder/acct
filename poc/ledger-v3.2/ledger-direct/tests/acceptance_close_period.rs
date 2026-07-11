//! Acceptance tests for `ledger_close_period` / `ledger_settle_pool` — the
//! close-time finalize (design-v3.2 §5e; recalc-e).
//!
//! Cases pin: the period-scoped recalc-drain gate (blocks with per-pool G2a/G2b
//! detail, passes once drained); forced close draining synchronously (never
//! skipping); the residue sweep making each pool's aggregate `value_sum`
//! exactly Σ open-layer value across all three residue sources (banker
//! rounding, the hot-path exact-empty flush, uncovered-depletion value);
//! stamp-only finalize + idempotent re-close; closer serialization; the 0017
//! immutability triggers (PeriodClosed, SQLSTATE 55000, for backdated events and
//! post-close settlement generations, next-period flow untouched); in-order
//! close enforcement; and the on-demand `ledger_settle_pool` drain.

mod common;

use common::*;
use serde_json::{json, Value};
use sqlx::PgPool;

async fn try_submit_at(
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

/// The gate blocks while the pool's in-period tail is unsettled (persisting
/// `closing`, during which backdates into the period still flow), then passes
/// once continuous recalc drains it; the pass-path close still sweeps.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn gate_blocks_then_passes_after_drain() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 10, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, T11, 15).await;

    let report = close_period(&pool, 1, "closer", false).await;
    assert_eq!(report["closed"], json!(false));
    assert_eq!(report["state"], json!("closing"));
    assert_eq!(report["gate"]["passed"], json!(false));
    assert_eq!(report["gate"]["lagging"][0]["pool_id"], json!(f.pool_id));
    assert_eq!(report["gate"]["lagging"][0]["unsettled_events"], json!(3));
    assert_eq!(period_state(&pool, 1).await, ("closing".to_string(), None));

    // closing means draining, not frozen: a backdate into the period lands.
    let d2 = deplete_at(&pool, f.pool_id, 4, T12, 1).await;

    mark_dirty(&pool, f.pool_id).await;
    drain_recalc(&pool).await;

    let report = close_period(&pool, 1, "closer", false).await;
    assert_eq!(report["closed"], json!(true));
    assert_eq!(report["gate"]["passed"], json!(true));
    assert_eq!(report["forced"], json!(false));
    assert_eq!(report["sweep"]["pools_swept"], json!(1));
    let (state, by) = period_state(&pool, 1).await;
    assert_eq!((state.as_str(), by.as_deref()), ("closed", Some("closer")));

    // Both depletions carry a finalized max-generation settlement, and the
    // aggregate is exactly the open-layer value.
    assert!(authoritative_of(&pool, d).await.is_some());
    assert!(authoritative_of(&pool, d2).await.is_some());
    let (_, _, value_sum) = aggregate(&pool, f.pool_id).await.unwrap();
    assert_eq!(value_sum, layer_value(&pool, f.pool_id).await);
}

/// Re-closing a closed period is an `already_closed` no-op: no new GL, no new
/// settlement generations, stamp untouched.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn reclose_is_a_noop() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T10, 4).await;

    let first = close_period(&pool, 1, "closer", true).await;
    assert_eq!(first["closed"], json!(true));
    let gl_rows = count(&pool, "SELECT count(*) FROM posting_line").await;
    let settlements = count(&pool, "SELECT count(*) FROM cost_settlement").await;

    let again = close_period(&pool, 1, "someone-else", false).await;
    assert_eq!(again["already_closed"], json!(true));
    assert_eq!(again["closed"], json!(true));
    assert_eq!(count(&pool, "SELECT count(*) FROM posting_line").await, gl_rows);
    assert_eq!(count(&pool, "SELECT count(*) FROM cost_settlement").await, settlements);
    assert_eq!(period_state(&pool, 1).await.1.as_deref(), Some("closer"));
}

/// Concurrent close callers serialize on the closer mutex: exactly one does
/// the work, the other observes `already_closed`.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn concurrent_closers_serialize() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T10, 4).await;

    let pool_a = connect_pool().await;
    let pool_b = connect_pool().await;
    let (a, b) = tokio::join!(
        close_period(&pool_a, 1, "a", true),
        close_period(&pool_b, 1, "b", true),
    );
    let noop_count = [&a, &b]
        .iter()
        .filter(|r| r["already_closed"] == json!(true))
        .count();
    assert_eq!(noop_count, 1, "exactly one caller no-ops: a={a}, b={b}");
    assert_eq!(a["closed"], json!(true));
    assert_eq!(b["closed"], json!(true));
    assert_eq!(period_state(&pool, 1).await.0, "closed");
}

/// Forced close never skips: it drains the lagging pools synchronously in the
/// close call and the report carries the G2b-sized move it paid.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn forced_close_drains_synchronously() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 5, 300).await;
    let d = deplete_at(&pool, f.pool_id, 3, T11, 12).await;

    let report = close_period(&pool, 1, "closer", true).await;
    assert_eq!(report["closed"], json!(true));
    assert_eq!(report["forced"], json!(true));
    assert_eq!(report["gate"]["passed"], json!(false));
    // G2b: 10×100 + 5×300 + 12×167 (observed running average) = 4504.
    assert_eq!(report["gate"]["unsettled_gross_total"], json!(4504));
    assert_eq!(report["drained"]["settlements_written"], json!(1));
    assert_eq!(report["drained"]["adjustments_posted"], json!(1));

    // Draws 10@100 + 2@300 → banker_div(1600, 12) = 133.
    assert_eq!(authoritative_of(&pool, d).await, Some(133));
    assert_eq!(period_state(&pool, 1).await.0, "closed");
}

/// Banker-rounding residue: the drained pool's aggregate sits 4 above the
/// layer value (10@100 + 5@300, deplete 12 → authoritative 133 vs observed
/// 167); the sweep posts it to variance on a zero-qty cost_adjustment_line
/// and the aggregate ends exactly at Σ layer value.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn sweep_absorbs_banker_residue() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 5, 300).await;
    deplete_at(&pool, f.pool_id, 3, T11, 12).await;

    let report = close_period(&pool, 1, "closer", true).await;
    assert_eq!(report["sweep"]["residues"][0]["residue"], json!(4));
    assert_eq!(report["sweep"]["residue_abs_total"], json!(4));

    let (qty, unit_cost, value_sum) = aggregate(&pool, f.pool_id).await.unwrap();
    assert_eq!((qty, unit_cost, value_sum), (3, 300, 900));
    assert_eq!(value_sum, layer_value(&pool, f.pool_id).await);

    // Positive residue: book overstated → debit variance, credit inventory,
    // riding a zero-qty adjustment line.
    let (amount, debit, credit, qty) = sqlx::query_as::<_, (i64, i64, i64, i64)>(
        "SELECT pl.amount, pl.debit_account, pl.credit_account, tl.qty \
           FROM posting_line pl JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE pl.id = $1",
    )
    .bind(report["sweep"]["residues"][0]["posting_line_id"].as_i64().unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((amount, debit, credit, qty), (4, VAR_ACCT, INV_ACCT, 0));
}

/// Exact-empty flush residue: a depletion that zeroes the aggregate qty
/// flushes `value_sum` to 0 on the hot path, while the telescoped recalc
/// deltas then move it off 0 — the sweep trues it back to the (empty) layer
/// value and books the difference.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn sweep_absorbs_exact_empty_flush_residue() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;

    // 10@100 + 5@200 (avg 133); deplete 5 (observed 133, authoritative 100,
    // delta −165); receipt 2@500 (avg banker(2335/12) = 195); deplete 12 —
    // exact empty, hot path flushes value_sum to 0. Authoritative for the
    // last depletion: draws 5@100 + 5@200 + 2@500 = 2500 → banker(2500/12)
    // = 208, delta +156. Net reconcile −(−165 + 156) = +9 off the flushed 0.
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 5, 200).await;
    deplete_at(&pool, f.pool_id, 3, T11, 5).await;
    receipt_at(&pool, f.pool_id, 4, T12, 2, 500).await;
    deplete_at(&pool, f.pool_id, 5, T13, 12).await;

    let report = close_period(&pool, 1, "closer", true).await;
    assert_eq!(report["sweep"]["residues"][0]["residue"], json!(9));

    let (qty, _, value_sum) = aggregate(&pool, f.pool_id).await.unwrap();
    assert_eq!((qty, value_sum), (0, 0));
    assert_eq!(layer_value(&pool, f.pool_id).await, 0);
}

/// Uncovered-depletion residue: an over-depletion's uncovered remainder is
/// costed at its observed cost with no layer backing, leaving a negative
/// aggregate book value that the sweep hands back from variance.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn sweep_absorbs_uncovered_depletion_residue() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 5, 100).await;
    let d = deplete_at(&pool, f.pool_id, 2, T10, 8).await;

    let report = close_period(&pool, 1, "closer", true).await;
    // Covered 5@100 + uncovered 3 @ observed 100 → authoritative 100, zero
    // delta; the −300 book value is pure uncovered residue.
    assert_eq!(authoritative_of(&pool, d).await, Some(100));
    assert_eq!(report["sweep"]["residues"][0]["residue"], json!(-300));

    let (qty, _, value_sum) = aggregate(&pool, f.pool_id).await.unwrap();
    assert_eq!((qty, value_sum), (-3, 0));
    assert_eq!(layer_value(&pool, f.pool_id).await, 0);

    // Negative residue reverses: debit inventory, credit variance.
    let (amount, debit, credit) = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT pl.amount, pl.debit_account, pl.credit_account \
           FROM posting_line pl WHERE pl.id = $1",
    )
    .bind(report["sweep"]["residues"][0]["posting_line_id"].as_i64().unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((amount, debit, credit), (300, INV_ACCT, VAR_ACCT));
}

/// A passing unforced close still drains an unsettled next-period tail before
/// sweeping: the residue is only defined at full settlement.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn unforced_close_drains_next_period_tail_for_the_sweep() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    deplete_at(&pool, f.pool_id, 2, T10, 4).await;
    mark_dirty(&pool, f.pool_id).await;
    drain_recalc(&pool).await;

    // In-period settled; next-period activity unsettled at close time.
    receipt_at(&pool, f.pool_id, 3, N09, 5, 300).await;
    let d_next = deplete_at(&pool, f.pool_id, 4, N10, 7).await;

    let report = close_period(&pool, 1, "closer", false).await;
    assert_eq!(report["closed"], json!(true));
    assert_eq!(report["gate"]["passed"], json!(true));

    let (_, _, value_sum) = aggregate(&pool, f.pool_id).await.unwrap();
    assert_eq!(value_sum, layer_value(&pool, f.pool_id).await);
    // The tail was drained on the way: the next-period depletion is settled.
    assert!(authoritative_of(&pool, d_next).await.is_some());
}

/// The 0017 schema invariant: after close, a backdated physical event and a
/// new settlement generation for an in-period depletion both reject with
/// PeriodClosed (SQLSTATE 55000); next-period activity on the same pool is
/// untouched.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn immutability_rejects_backdate_and_new_generation() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    let d = deplete_at(&pool, f.pool_id, 2, T10, 4).await;
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));

    let backdate = try_submit_at(
        &pool,
        "inv_adjustment",
        90,
        T12,
        json!([line(f.pool_id, "inv_adjustment_line", 1, 50)]),
    )
    .await;
    assert_eq!(sqlstate(&backdate.unwrap_err()), "55000");

    let new_generation = sqlx::query(
        "INSERT INTO cost_settlement \
                (depletion_trx_line_id, recalc_generation, authoritative_unit_cost, \
                 prior_unit_cost, adjustment_posting_line_id) \
         VALUES ($1, 99, 1, 1, NULL)",
    )
    .bind(d)
    .execute(&pool)
    .await;
    assert_eq!(sqlstate(&new_generation.unwrap_err()), "55000");

    // The same pool keeps flowing in the next period.
    receipt_at(&pool, f.pool_id, 91, N09, 3, 200).await;
    let report = settle_pool(&pool, f.pool_id).await;
    assert_eq!(report["settled"], json!(true));
}

/// Guard rails: an empty period closes trivially; a missing period is
/// undefined_object; periods close in start_date order.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn close_guard_rails() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    seed_fixture(&pool, "fifo", "running_avg").await;
    create_period(&pool, 1, "2026-06-01", "2026-06-30").await;
    create_period(&pool, 2, PERIOD_START, PERIOD_END).await;

    let out_of_order = sqlx::query_scalar::<_, Value>("SELECT ledger_close_period(2, 'x', false)")
        .fetch_one(&pool)
        .await;
    assert_eq!(sqlstate(&out_of_order.unwrap_err()), "55000");

    let missing = sqlx::query_scalar::<_, Value>("SELECT ledger_close_period(99, 'x', false)")
        .fetch_one(&pool)
        .await;
    assert_eq!(sqlstate(&missing.unwrap_err()), "42704");

    // No activity at all: both close trivially, in order.
    let june = close_period(&pool, 1, "closer", false).await;
    assert_eq!(june["closed"], json!(true));
    assert_eq!(june["gate"]["pools_in_scope"], json!(0));
    assert_eq!(close_period(&pool, 2, "closer", false).await["closed"], json!(true));
}

/// `ledger_settle_pool` drains one pool to head on demand, no-ops on
/// non-strict pools, and rejects unknown pools.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn settle_pool_drains_on_demand() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let fifo = seed_pool(&pool, 1, 1, 1, "fifo", "running_avg").await;
    let wac = seed_pool(&pool, 2, 2, 2, "wac", "running_avg").await;

    receipt_at(&pool, fifo.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, fifo.pool_id, 2, T10, 10, 300).await;
    let d = deplete_at(&pool, fifo.pool_id, 3, T11, 15).await;
    receipt_at(&pool, wac.pool_id, 4, T09, 5, 100).await;

    let report = settle_pool(&pool, fifo.pool_id).await;
    assert_eq!(report["settled"], json!(true));
    assert_eq!(report["settlements_written"], json!(1));
    assert_eq!(report["adjustments_posted"], json!(1));
    assert_eq!(authoritative_of(&pool, d).await, Some(167));
    assert_eq!(count(&pool, "SELECT count(*) FROM recalc_queue").await, 0);

    let skipped = settle_pool(&pool, wac.pool_id).await;
    assert_eq!(skipped["skipped"], json!("non_strict_method"));

    let unknown = sqlx::query_scalar::<_, Value>("SELECT ledger_settle_pool(999)")
        .fetch_one(&pool)
        .await;
    assert_eq!(sqlstate(&unknown.unwrap_err()), "42704");
}

/// The sweep fails loud (MissingVarianceAccount, whole close aborted) when a
/// pool needs a residue posting but its variance_acct is NULL; the period
/// stays open and a corrected map lets the close succeed.
#[tokio::test]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn sweep_missing_variance_account_fails_loud() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    sqlx::query("UPDATE posting_account_map SET variance_acct = NULL WHERE sku_id = $1")
        .bind(f.sku_id)
        .execute(&pool)
        .await
        .unwrap();
    create_period(&pool, 1, PERIOD_START, PERIOD_END).await;
    receipt_at(&pool, f.pool_id, 1, T09, 10, 100).await;
    receipt_at(&pool, f.pool_id, 2, T10, 5, 300).await;
    deplete_at(&pool, f.pool_id, 3, T11, 12).await;

    let failed = sqlx::query_scalar::<_, Value>("SELECT ledger_close_period(1, 'x', true)")
        .fetch_one(&pool)
        .await;
    assert_eq!(sqlstate(&failed.unwrap_err()), "22000");
    assert_eq!(period_state(&pool, 1).await.0, "open");
    // The aborted close left no partial writes.
    assert_eq!(count(&pool, "SELECT count(*) FROM cost_settlement").await, 0);

    sqlx::query("UPDATE posting_account_map SET variance_acct = $2 WHERE sku_id = $1")
        .bind(f.sku_id)
        .bind(VAR_ACCT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(close_period(&pool, 1, "closer", true).await["closed"], json!(true));
}
