//! acct-s6fa acceptance: wac_periodic depletions are flagged in
//! posting_lines_provisional mid-period; ledger_close_period restates them
//! at final_avg = Σ(in-period receipt value) / Σ(in-period receipt qty),
//! posts variance posting_lines per non-zero-variance flagged depletion,
//! adjusts pool_state.unit_cost (= value_sum for wac_periodic) by
//! -Σ variance, and stamps each provisional row finalized.

mod common;

use common::*;

/// Workload (period 2026-05-01 .. 2026-05-31):
///   5/10 receive 10 @ 50  → pool (10, 500)
///   5/12 deplete 4 at avg 50 → amount=200; pool (6, 300)
///   5/15 receive 10 @ 100 → pool (16, 1300)
///   5/20 deplete 5 at avg 81 (=1300/16, truncated) → amount=406; pool (11, 894)
///
/// In-period receipts:  value=1500, qty=20 → final_avg=75
///   depletion 1 restate: 75*4 = 300; variance = +100 (under-provisioned)
///   depletion 2 restate: 75*5 = 375; variance =  -31 (over-provisioned)
/// Sum variance per pool: +69
/// Post-close pool_state.unit_cost = 894 - 69 = 825 (value_sum)
/// Sanity: 20 received at 1500 - 9 depleted at 75 each (=675) = 825. ✓
#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn wac_periodic_close_finalizes_provisionals_and_posts_variance() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac_periodic").await;
    let period_id = seed_period(&pool, "2026-05-01", "2026-05-31").await;

    // ── 5/10 receipt 1: 10 @ 50
    submit(
        &pool, "po_receipt", 101, "2026-05-10T12:00:00+00:00",
        &po_receipt_line(pool_id, 10, 50, inv, ap),
    ).await.expect("receipt 1");

    // ── 5/12 depletion 1: 4 at running avg 50 → amount 200
    submit(
        &pool, "transfer_shipment", 201, "2026-05-12T12:00:00+00:00",
        &depletion_line(pool_id, 4, 0, inv, ap),
    ).await.expect("depletion 1");

    // ── 5/15 receipt 2: 10 @ 100
    submit(
        &pool, "po_receipt", 102, "2026-05-15T12:00:00+00:00",
        &po_receipt_line(pool_id, 10, 100, inv, ap),
    ).await.expect("receipt 2");

    // ── 5/20 depletion 2: 5 at avg=81 (=1300/16) → amount=406 (truncated)
    submit(
        &pool, "transfer_shipment", 202, "2026-05-20T12:00:00+00:00",
        &depletion_line(pool_id, 5, 0, inv, ap),
    ).await.expect("depletion 2");

    // Pre-close assertions.
    let (ps_qty_pre, ps_uc_pre): (i64, i64) = sqlx::query_as(
        "SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_seq = 0",
    ).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(ps_qty_pre, 11, "pre-close pool qty");
    assert_eq!(ps_uc_pre, 894, "pre-close value_sum");

    let prov_unfinalized: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM posting_lines_provisional \
            WHERE finalized_at IS NULL",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(prov_unfinalized, 2, "two unfinalized provisional rows");

    // Capture provisional amounts before close for variance arithmetic.
    let prov_amounts: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT id, provisional_amount FROM posting_lines_provisional \
            ORDER BY id",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(prov_amounts.len(), 2);
    assert_eq!(prov_amounts[0].1, 200, "provisional 1 amount");
    assert_eq!(prov_amounts[1].1, 406, "provisional 2 amount");

    // ── Close period.
    let summary = close_period(&pool, period_id).await;
    let summary = &summary.0;
    assert_eq!(summary["period_id"].as_i64(), Some(period_id));
    assert_eq!(summary["provisional_finalized"].as_i64(), Some(2));
    assert_eq!(summary["variance_posted"].as_i64(), Some(2));
    assert_eq!(summary["zero_variance"].as_i64(), Some(0));
    assert_eq!(summary["pools_affected"].as_i64(), Some(1));

    // Post-close pool_state.
    let (ps_qty, ps_uc): (i64, i64) = sqlx::query_as(
        "SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1 AND layer_seq = 0",
    ).bind(pool_id).fetch_one(&pool).await.unwrap();
    assert_eq!(ps_qty, 11, "post-close pool qty (unchanged)");
    assert_eq!(ps_uc, 825, "post-close value_sum (894 - sum_variance 69)");

    // Provisional rows: both finalized, variance amounts +100 / -31.
    let prov_finalized: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(
        "SELECT id, variance_amount, variance_posting_line_id \
           FROM posting_lines_provisional \
          WHERE finalized_at IS NOT NULL ORDER BY id",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(prov_finalized.len(), 2);
    assert_eq!(prov_finalized[0].1, 100, "depletion 1 variance");
    assert!(prov_finalized[0].2.is_some(), "depletion 1 has variance posting");
    assert_eq!(prov_finalized[1].1, -31, "depletion 2 variance");
    assert!(prov_finalized[1].2.is_some(), "depletion 2 has variance posting");

    // Variance posting_lines: amounts 100 and 31 with appropriate directions.
    let variance_postings: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT amount, debit_account, credit_account \
           FROM posting_line \
          WHERE event_type = 'variance' ORDER BY id",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(variance_postings.len(), 2);
    // depletion 1 variance > 0: same direction as original depletion
    // (depletion_line emits debit=ap, credit=inv).
    assert_eq!(variance_postings[0], (100, ap, inv), "variance 1 (positive)");
    // depletion 2 variance < 0: SWAPPED direction.
    assert_eq!(variance_postings[1], (31, inv, ap), "variance 2 (negative)");

    // Period state.
    let (state, closed_at_some): (String, bool) = sqlx::query_as(
        "SELECT state, (closed_at IS NOT NULL) \
           FROM accounting_period WHERE id = $1",
    ).bind(period_id).fetch_one(&pool).await.unwrap();
    assert_eq!(state, "closed");
    assert!(closed_at_some, "closed_at populated");

    // Receipt postings should NOT be flagged (no provisional rows for them).
    let receipts_flagged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM posting_lines_provisional plp \
            JOIN posting_line pl ON pl.id = plp.posting_line_id \
           WHERE pl.event_type = 'inventory_receipt'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(receipts_flagged, 0, "no receipts flagged");

    // Cross-check: net inv ledger balance == pool_state.value_sum.
    // inv is debit-normal (asset), so balance = Σ amount(debit=inv) − Σ amount(credit=inv).
    let inv_balance: i64 = sqlx::query_scalar(
        "SELECT COALESCE( \
            SUM(CASE WHEN debit_account = $1 THEN amount \
                     WHEN credit_account = $1 THEN -amount \
                     ELSE 0 END), 0)::bigint \
           FROM posting_line",
    ).bind(inv).fetch_one(&pool).await.unwrap();
    assert_eq!(inv_balance, 825,
        "inv account ledger balance matches post-close pool value_sum");
}

/// A period with no provisional rows closes cleanly with zero counts.
#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn close_period_with_no_provisional_rows_is_trivial() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let period_id = seed_period(&pool, "2026-06-01", "2026-06-30").await;

    let summary = close_period(&pool, period_id).await;
    let summary = &summary.0;
    assert_eq!(summary["provisional_finalized"].as_i64(), Some(0));
    assert_eq!(summary["variance_posted"].as_i64(), Some(0));
    assert_eq!(summary["zero_variance"].as_i64(), Some(0));
    assert_eq!(summary["pools_affected"].as_i64(), Some(0));

    let state: String = sqlx::query_scalar(
        "SELECT state FROM accounting_period WHERE id = $1",
    ).bind(period_id).fetch_one(&pool).await.unwrap();
    assert_eq!(state, "closed");
}

/// Closing an already-closed period raises ERROR (state != 'open').
#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn close_period_rejects_already_closed() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let period_id = seed_period(&pool, "2026-07-01", "2026-07-31").await;

    // First close succeeds.
    close_period(&pool, period_id).await;

    // Second close must raise.
    let res: Result<sqlx::types::Json<serde_json::Value>, sqlx::Error> = sqlx::query_scalar(
        "SELECT ledger_close_period($1)",
    ).bind(period_id).fetch_one(&pool).await;
    assert!(res.is_err(), "expected error closing already-closed period; got {res:?}");
}

/// A wac_periodic depletion in a period with zero in-period receipts cannot
/// compute final_avg → ledger_close_period must raise (main acct P0020 analog).
#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn close_period_no_receipts_raises_for_pool_with_depletions() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac_periodic").await;
    let setup_period = seed_period(&pool, "2026-04-01", "2026-04-30").await;
    let target_period = seed_period(&pool, "2026-05-01", "2026-05-31").await;

    // Stock the pool in a prior period; deplete in the target period (no
    // receipts in target). Closing target must raise.
    submit(
        &pool, "po_receipt", 101, "2026-04-10T12:00:00+00:00",
        &po_receipt_line(pool_id, 10, 50, inv, ap),
    ).await.expect("setup receipt");
    submit(
        &pool, "transfer_shipment", 201, "2026-05-15T12:00:00+00:00",
        &depletion_line(pool_id, 4, 0, inv, ap),
    ).await.expect("target depletion");

    // Setup period closes fine (it has receipts; no provisional rows since
    // depletion fell in May).
    close_period(&pool, setup_period).await;

    // Target period has a provisional row but no in-period receipts → raise.
    let res: Result<sqlx::types::Json<serde_json::Value>, sqlx::Error> = sqlx::query_scalar(
        "SELECT ledger_close_period($1)",
    ).bind(target_period).fetch_one(&pool).await;
    assert!(res.is_err(), "expected error on no-receipts close; got {res:?}");
}
