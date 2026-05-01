//! `acct-c0b` / `acct-qfj.2` — full wac_periodic close-time matrix.
//!
//! Comprehensive integration tests for periodic WAC re-costing. Each
//! test sets up a period of transactions, calls close_period, and
//! verifies the resulting state (account balances, transfers_provisional
//! finalization, variance routing).
//!
//! Smoke tests for the basic flow live in tests/wac_periodic.rs (qfj.1).
//! This file is the compositional matrix: pool partitioning, period
//! boundaries, mixed reasons, force flags, audit trail, regression,
//! and the hook dispatch order test rewritten to use a transaction
//! so DDL rolls back cleanly.

mod common;

use common::*;

// ============================================================
// Helpers
// ============================================================

async fn insert_wac_periodic_sku(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'wac_periodic')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert wac_periodic sku {code}: {e}"))
}

async fn insert_wac_perpetual_sku(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'wac_perpetual')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert wac_perpetual sku {code}: {e}"))
}

async fn open_qty_account(
    pool: &sqlx::PgPool,
    sku_id: &str,
    loc_code: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
           FROM locations l WHERE l.code = $2
         RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_code)
    .fetch_one(pool)
    .await
    .expect("open stock_available")
}

async fn open_value_account(
    pool: &sqlx::PgPool,
    sku_id: &str,
    loc_code: &str,
    kind: &str,
    currency: &str,
) -> i64 {
    sqlx::query_scalar(&format!(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT '{kind}', 'value', $3, 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = $2
         RETURNING id",
    ))
    .bind(sku_id)
    .bind(loc_code)
    .bind(currency)
    .fetch_one(pool)
    .await
    .expect("open value account")
}

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

async fn balance(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("balance")
}

#[allow(clippy::too_many_arguments)]
async fn adjust_with_class(
    pool: &sqlx::PgPool,
    sku: &str,
    loc_code: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    currency: &str,
    inv_class: &str,
    business_date: &str,
) -> sqlx::Result<String> {
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(loc_code)
        .fetch_one(pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, $5,
            $6, $7::DATE, $8::UUID, $9::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(&loc_id)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(currency)
    .bind(inv_class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    loc_code: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    business_date: &str,
) -> sqlx::Result<String> {
    adjust_with_class(pool, sku, loc_code, qty_delta, unit_cost, "USD", "fg", business_date).await
}

async fn close_period(
    pool: &sqlx::PgPool,
    period_code: &str,
    force_provisional: bool,
) -> sqlx::Result<serde_json::Value> {
    let pid = period_id(pool, period_code).await;
    let actor = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, $3, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .bind(force_provisional)
    .fetch_one(pool)
    .await
}

/// Set up a wac_periodic SKU with stock_available + inv_value_fg(USD) at MAIN.
/// Returns (sku_id, qty_acct_id, val_acct_id).
async fn setup_wac_periodic_sku(pool: &sqlx::PgPool, code: &str) -> (String, i64, i64) {
    let sku = insert_wac_periodic_sku(pool, code).await;
    let qty = open_qty_account(pool, &sku, "MAIN").await;
    let val = open_value_account(pool, &sku, "MAIN", "inv_value_fg", "USD").await;
    (sku, qty, val)
}

// ============================================================
// Section 1: Math correctness across receipt/depletion patterns
// ============================================================

#[tokio::test]
async fn one_receipt_one_depletion_no_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, val) = setup_wac_periodic_sku(&pool, "MX-1R1D").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.expect("rcv");
    adjust(&pool, &sku, "MAIN", -40, None, "2026-04-15").await.expect("dep");

    // After: pool=60, value=300. Provisional = 5. Final period avg = 5. Variance = 0.
    assert_eq!(balance(&pool, val).await, 300);
    let _ = close_period(&pool, "2026-04", false).await.expect("close");

    let (variance, finalized): (i64, Option<String>) = sqlx::query_as(
        "SELECT variance_amount, finalized_at::text
           FROM transfers_provisional WHERE cost_method='wac_periodic'",
    )
    .fetch_one(&pool)
    .await
    .expect("audit");
    assert_eq!(variance, 0);
    assert!(finalized.is_some());
    assert_eq!(balance(&pool, val).await, 300, "no variance posted");
}

#[tokio::test]
async fn depletion_between_two_receipts_write_up_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, val) = setup_wac_periodic_sku(&pool, "MX-WRITEUP").await;

    // Receive 100@5 → pool 100, value 500, running avg 5.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.expect("rcv1");
    // Deplete 30 at provisional=5.
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-10").await.expect("dep");
    // Receive 100@9 → pool 170, value 350+900=1250, running avg ~7.35.
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-04-15").await.expect("rcv2");

    let pre_balance = balance(&pool, val).await;
    assert_eq!(pre_balance, 1250);

    // Final period avg = (500+900)/(100+100) = 7.
    // Variance = 30 × (7 - 5) = 60 (write-up).
    let _ = close_period(&pool, "2026-04", false).await.expect("close");

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.expect("variance");
    assert_eq!(variance, 60, "write-up variance");

    // After close, val_acct adjusted by -60 (variance routes from inv_value to itself
    // via variance_wac_period; net effect on inv_value_fg = -60).
    assert_eq!(balance(&pool, val).await, 1250 - 60);
}

#[tokio::test]
async fn depletion_between_two_receipts_write_down_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, val) = setup_wac_periodic_sku(&pool, "MX-WRITEDOWN").await;

    // Receive 100@9, deplete 30 (provisional=9), receive 100@5.
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-04-05").await.expect("rcv1");
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-10").await.expect("dep");
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-15").await.expect("rcv2");

    // Final avg = (900+500)/200 = 7. Variance = 30 × (7 - 9) = -60 (write-down).
    let _ = close_period(&pool, "2026-04", false).await.expect("close");

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.expect("variance");
    assert_eq!(variance, -60, "write-down variance");

    // Pool: pre-close val = 900 - 270 + 500 = 1130. After close: 1130 + 60 = 1190.
    // (write-down reverses the depletion's over-debit on inv_value_fg.)
    assert_eq!(balance(&pool, val).await, 1190);
}

#[tokio::test]
async fn many_depletions_at_running_avg_each_gets_own_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-MANYDEP").await;

    // Receive 100@5. Avg = 5.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.expect("rcv1");
    // Deplete 20 at avg=5.
    adjust(&pool, &sku, "MAIN", -20, None, "2026-04-08").await.expect("dep1");
    // Receive 100@7. Pool: 80+100=180, value: 400+700=1100. Avg = 6.11.
    adjust(&pool, &sku, "MAIN", 100, Some(7), "2026-04-12").await.expect("rcv2");
    // Deplete 30 at avg=6 (1100/180=6.11 truncated to 6).
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-15").await.expect("dep2");

    // Final period avg = (500+700)/(100+100) = 6.
    // Variance per depletion:
    //   dep1: 20 × (6 - 5) = +20
    //   dep2: 30 × (6 - 6) = 0
    let _ = close_period(&pool, "2026-04", false).await.expect("close");

    let variances: Vec<i64> = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional
          WHERE cost_method='wac_periodic'
          ORDER BY transfer_id",
    ).fetch_all(&pool).await.expect("variances");
    assert_eq!(variances, vec![20, 0]);
}

#[tokio::test]
async fn integer_rounding_truncates_provisional_unit_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-ROUND").await;

    // Receive 3@10, deplete 1. Provisional = floor(30/3) = 10. Final = 10. Variance = 0.
    adjust(&pool, &sku, "MAIN", 3, Some(10), "2026-04-05").await.expect("rcv");
    adjust(&pool, &sku, "MAIN", -1, None, "2026-04-10").await.expect("dep");

    let _ = close_period(&pool, "2026-04", false).await.expect("close");
    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.expect("variance");
    assert_eq!(variance, 0);
}

#[tokio::test]
async fn integer_rounding_with_uneven_division() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-UNEVEN").await;

    // Receive 7@10 → pool 7, value 70. Running avg = floor(70/7) = 10.
    adjust(&pool, &sku, "MAIN", 7, Some(10), "2026-04-05").await.expect("rcv1");
    // Deplete 3 at provisional=10.
    adjust(&pool, &sku, "MAIN", -3, None, "2026-04-10").await.expect("dep");
    // Receive 3@13 → pool 7, value 70-30+39=79. Final avg = floor(109/10) = 10.
    // (109 = total value-in across the period: 70 + 39)
    adjust(&pool, &sku, "MAIN", 3, Some(13), "2026-04-15").await.expect("rcv2");

    // Total value_in = 70 + 39 = 109; total qty_in = 7 + 3 = 10. Final = 10 (truncated).
    // Variance = 3 × (10 - 10) = 0.
    let _ = close_period(&pool, "2026-04", false).await.expect("close");
    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.expect("variance");
    assert_eq!(variance, 0);
}

// ============================================================
// Section 2: Pool partitioning
// ============================================================

#[tokio::test]
async fn multi_location_pools_close_independently() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_periodic_sku(&pool, "MX-MULTILOC").await;
    let _q_main = open_qty_account(&pool, &sku, "MAIN").await;
    let v_main = open_value_account(&pool, &sku, "MAIN", "inv_value_fg", "USD").await;
    let _q_alt = open_qty_account(&pool, &sku, "ALT").await;
    let v_alt = open_value_account(&pool, &sku, "ALT", "inv_value_fg", "USD").await;

    // MAIN: receive 100@5, deplete 50, receive 100@7. Final MAIN avg = 6.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.expect("main r1");
    adjust(&pool, &sku, "MAIN", -50, None, "2026-04-10").await.expect("main dep");
    adjust(&pool, &sku, "MAIN", 100, Some(7), "2026-04-15").await.expect("main r2");

    // ALT: receive 100@10, deplete 30, no second receipt. Final ALT avg = 10.
    adjust(&pool, &sku, "ALT", 100, Some(10), "2026-04-05").await.expect("alt r1");
    adjust(&pool, &sku, "ALT", -30, None, "2026-04-10").await.expect("alt dep");

    let _ = close_period(&pool, "2026-04", false).await.expect("close");

    // MAIN variance: 50 × (6 - 5) = +50.
    // ALT variance: 30 × (10 - 10) = 0.
    let main_variance: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.credit_account_id = $1",
    ).bind(v_main).fetch_one(&pool).await.expect("main variance");
    let alt_variance: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.credit_account_id = $1",
    ).bind(v_alt).fetch_one(&pool).await.expect("alt variance");
    assert_eq!(main_variance, 50);
    assert_eq!(alt_variance, 0);
}

#[tokio::test]
async fn raw_class_and_fg_class_close_independently_via_separate_skus() {
    // Sister of raw_class_and_fg_class_close_independently_via_one_sku
    // (below). This version uses two SKUs — one for each class — and
    // verifies the close hook handles each class identically and
    // independently. The single-SKU version proves the same fix when
    // both classes are open on ONE SKU (was an "architectural assumption"
    // pre-acct-1vr; fixed by migration 0030's per-class qty SUM).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let raw_sku = insert_wac_periodic_sku(&pool, "MX-RAWSKU").await;
    let _ = open_qty_account(&pool, &raw_sku, "MAIN").await;
    let v_raw = open_value_account(&pool, &raw_sku, "MAIN", "inv_value_raw", "USD").await;

    let fg_sku = insert_wac_periodic_sku(&pool, "MX-FGSKU").await;
    let _ = open_qty_account(&pool, &fg_sku, "MAIN").await;
    let v_fg = open_value_account(&pool, &fg_sku, "MAIN", "inv_value_fg", "USD").await;

    // raw_sku: 100@5, deplete 20, 100@9. Final = (500+900)/200 = 7. Variance = 20×(7-5)=40.
    adjust_with_class(&pool, &raw_sku, "MAIN", 100, Some(5), "USD", "raw", "2026-04-05").await.unwrap();
    adjust_with_class(&pool, &raw_sku, "MAIN", -20, None, "USD", "raw", "2026-04-10").await.unwrap();
    adjust_with_class(&pool, &raw_sku, "MAIN", 100, Some(9), "USD", "raw", "2026-04-15").await.unwrap();

    // fg_sku: 100@5, deplete 50. Final = 5. Variance = 0.
    adjust_with_class(&pool, &fg_sku, "MAIN", 100, Some(5), "USD", "fg", "2026-04-05").await.unwrap();
    adjust_with_class(&pool, &fg_sku, "MAIN", -50, None, "USD", "fg", "2026-04-10").await.unwrap();

    let _ = close_period(&pool, "2026-04", false).await.expect("close");

    let raw_var: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.credit_account_id = $1",
    ).bind(v_raw).fetch_one(&pool).await.expect("raw variance");
    let fg_var: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.credit_account_id = $1",
    ).bind(v_fg).fetch_one(&pool).await.expect("fg variance");
    assert_eq!(raw_var, 40);
    assert_eq!(fg_var, 0);
}

#[tokio::test]
async fn raw_class_and_fg_class_close_independently_via_one_sku() {
    // Single SKU has BOTH inv_value_raw AND inv_value_fg pools open.
    // Both classes have receipts and depletions in 2026-04. Close
    // verifies per-class avgs are computed independently — the raw
    // class's variance is unaffected by fg class's avg, and vice versa.
    //
    // This test is the proof that acct-1vr (migration 0030) fixed the
    // structural coupling. Pre-fix, the close hook would have computed
    // qty_in via stock_available which pools both classes' qty —
    // poisoning per-class avgs. Post-fix, qty_in reads transfers.qty
    // tagged on the specific value pool.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "MX-1SKU-MULTI").await;
    let qty_acct = open_qty_account(&pool, &sku, "MAIN").await;
    let v_raw = open_value_account(&pool, &sku, "MAIN", "inv_value_raw", "USD").await;
    let v_fg = open_value_account(&pool, &sku, "MAIN", "inv_value_fg", "USD").await;

    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    // RAW activity: receive 100@5, deplete 20, receive 100@9.
    // Per-class final avg = (500+900)/(100+100) = 7. Variance = 20 × (7-5) = 40.
    adjust_with_class(&pool, &sku, "MAIN", 100, Some(5), "USD", "raw", "2026-04-05")
        .await.expect("raw r1");
    adjust_with_class(&pool, &sku, "MAIN", -20, None, "USD", "raw", "2026-04-10")
        .await.expect("raw dep");
    adjust_with_class(&pool, &sku, "MAIN", 100, Some(9), "USD", "raw", "2026-04-15")
        .await.expect("raw r2");

    // FG activity: receive 50@10, deplete 30. Pre-deplete pool: 50u/$500 → avg $10.
    // Per-class final avg = 500/50 = 10. Variance = 30 × (10-10) = 0.
    //
    // Cross-class qty contamination check: stock_available now contains
    // raw qty (100-20+100=180) plus fg qty (50). Pre-fix, the periodic
    // close hook would compute fg's qty_in as the cross-class total
    // (180+50=230), giving fg a corrupted denominator and corrupted
    // variance. Post-fix, fg's qty_in is its own per-class transfers.qty
    // (50), and the variance is correctly 0.
    adjust_with_class(&pool, &sku, "MAIN", 50, Some(10), "USD", "fg", "2026-04-05")
        .await.expect("fg r1");
    adjust_with_class(&pool, &sku, "MAIN", -30, None, "USD", "fg", "2026-04-10")
        .await.expect("fg dep");

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value =
        sqlx::query_scalar("SELECT close_period($1, $2::UUID, FALSE, FALSE)")
            .bind(pid)
            .bind(&actor)
            .fetch_one(&pool)
            .await
            .expect("close");

    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(2),
               "two depletions finalized — one per class");

    let raw_var: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.credit_account_id = $1",
    ).bind(v_raw).fetch_one(&pool).await.expect("raw variance");
    let fg_var: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.credit_account_id = $1",
    ).bind(v_fg).fetch_one(&pool).await.expect("fg variance");

    assert_eq!(raw_var, 40, "raw class: 20 × ($7 final - $5 provisional) = $40");
    assert_eq!(fg_var, 0, "fg class: 30 × ($10 final - $10 provisional) = $0");

    // Sanity: stock_available has cross-class qty.
    assert_eq!(balance(&pool, qty_acct).await, 100 - 20 + 100 + 50 - 30,
               "stock_available aggregates raw + fg qty");
}

#[tokio::test]
async fn multiple_skus_in_same_period_close_independently() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (a, _, _) = setup_wac_periodic_sku(&pool, "MX-A").await;
    let (b, _, _) = setup_wac_periodic_sku(&pool, "MX-B").await;

    // SKU-A: 100@5, dep 30, 100@7. Variance = 30 × (6-5) = 30.
    adjust(&pool, &a, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &a, "MAIN", -30, None, "2026-04-10").await.unwrap();
    adjust(&pool, &a, "MAIN", 100, Some(7), "2026-04-15").await.unwrap();

    // SKU-B: 100@10, dep 50. Variance = 50 × (10-10) = 0.
    adjust(&pool, &b, "MAIN", 100, Some(10), "2026-04-05").await.unwrap();
    adjust(&pool, &b, "MAIN", -50, None, "2026-04-10").await.unwrap();

    let _ = close_period(&pool, "2026-04", false).await.unwrap();

    let total_finalized: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional
          WHERE cost_method='wac_periodic' AND finalized_at IS NOT NULL",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(total_finalized, 2);
}

// ============================================================
// Section 3: Period boundaries
// ============================================================

#[tokio::test]
async fn cross_period_uses_only_in_period_receipts() {
    // Periodic WAC final avg per the implementation (and Oracle PAC /
    // SAP S/4 convention): Σ(in-period receipts value) /
    // Σ(in-period receipts qty). Carry-forward inventory carries its
    // prior-period cost; depletions in the new period get re-costed
    // against the new period's receipt avg.
    //
    // This test verifies that the May close uses ONLY May's receipts
    // for the period avg — even though the running pool avg at
    // depletion time mixes April carry-forward with May receipts.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-CROSSP").await;

    // April: receive 100@5. Pool ends April with 100u/$500 (avg 5).
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    let _ = close_period(&pool, "2026-04", false).await.expect("close 04");

    // May: receive 100@9. Pool now 200u/$1400. Running avg at depletion = 7.
    // Deplete 30 at provisional=7.
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-05-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -30, None, "2026-05-10").await.unwrap();

    // May close: final avg = May receipts only = 900/100 = 9.
    // Variance = 30 × (9 - 7) = 60.
    let _ = close_period(&pool, "2026-05", false).await.expect("close 05");

    let may_variance: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic'
            AND t.business_date >= '2026-05-01'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        may_variance, 60,
        "May depletion: provisional=7 (full-pool running avg), final=9 (May receipts only)"
    );
}

#[tokio::test]
async fn multi_period_chain_each_close_independent() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-CHAIN").await;

    // April: 100@5, dep 20, 100@7. Final = 6. Variance = 20×(6-5) = 20.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -20, None, "2026-04-10").await.unwrap();
    adjust(&pool, &sku, "MAIN", 100, Some(7), "2026-04-15").await.unwrap();
    let _ = close_period(&pool, "2026-04", false).await.unwrap();

    // May: 100@9, dep 30. After April carry-forward (180u, val 1080
    // post-April-variance), May receives 100@9 → 280u, val 1980.
    // Running avg at deplete = floor(1980/280) = 7. Provisional = 7.
    // Final period avg (May receipts only) = 900/100 = 9.
    // Variance = 30 × (9 - 7) = 60.
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-05-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -30, None, "2026-05-10").await.unwrap();
    let _ = close_period(&pool, "2026-05", false).await.unwrap();

    let april: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.business_date < '2026-05-01'",
    ).fetch_one(&pool).await.unwrap();
    let may: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method='wac_periodic' AND t.business_date >= '2026-05-01'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(april, 20);
    assert_eq!(may, 60, "May provisional 7, final 9 → variance 60");
}

// ============================================================
// Section 4: Mixed reasons
// ============================================================

#[tokio::test]
async fn mixed_receipt_reasons_aggregate_into_period_avg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, qty, val) = setup_wac_periodic_sku(&pool, "MX-MIXIN").await;

    // Receipt via inventory_adjustment.
    adjust(&pool, &sku, "MAIN", 50, Some(5), "2026-04-05").await.unwrap();
    // pool: 50, value: 250.

    // Receipt via cycle_count_adj (direct post_transfers).
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;
    let _ = call_post_transfers(
        &pool,
        serde_json::json!([
            make_event("cycle_count_adj", qty, void_qty, 50, "2026-04-08", &fresh_uuid(&pool).await),
            make_event_with_qty("cycle_count_adj", val, void_val, 350, 50, "2026-04-08", &fresh_uuid(&pool).await),
        ]),
        false,
    ).await.expect("cycle count");
    // pool: 100, value: 600.

    // Deplete 40 at running avg = 6.
    adjust(&pool, &sku, "MAIN", -40, None, "2026-04-10").await.unwrap();

    // Final period avg = (250 + 350) / (50 + 50) = 6. Variance = 40 × (6 - 6) = 0.
    let _ = close_period(&pool, "2026-04", false).await.unwrap();
    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(variance, 0);
}

#[tokio::test]
async fn mixed_depletion_reasons_both_flagged() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _qty, val) = setup_wac_periodic_sku(&pool, "MX-MIXOUT").await;

    // Receive 100@5.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();

    // Depletion via inventory_adjustment OUT (qty=20 at avg=5).
    adjust(&pool, &sku, "MAIN", -20, None, "2026-04-10").await.unwrap();

    // Depletion via so_ship (qty=10 at running avg).
    let cogs = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let event = serde_json::json!({
        "reason":            "so_ship",
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  cogs,
        "credit_account_id": val,
        "qty":               10,
        "business_date":     "2026-04-12",
        "idempotency_key":   fresh_uuid(&pool).await,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    let _ = call_post_transfers(&pool, serde_json::json!([event]), false)
        .await.expect("so_ship");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2, "both depletion paths flag");
}

// ============================================================
// Section 5: Force flag & edge cases
// ============================================================

#[tokio::test]
async fn period_with_only_receipts_close_succeeds_no_provisional() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-RECVONLY").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    let summary = close_period(&pool, "2026-04", false).await.expect("close");
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(0));

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn force_bypass_processes_some_skips_unprocessable() {
    // Two pools in same period: Pool A has receipts, Pool B doesn't.
    // Without force, P0020 fires on B. With force, A is processed and B
    // is skipped (left un-finalized).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (a, _, _) = setup_wac_periodic_sku(&pool, "MX-BPA").await;
    let (b, qty_b, val_b) = setup_wac_periodic_sku(&pool, "MX-BPB").await;

    // Pool A: receive + deplete (processable).
    adjust(&pool, &a, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &a, "MAIN", -30, None, "2026-04-10").await.unwrap();

    // Pool B: pre-seed via direct INSERT (no receipt in period), then deplete.
    // To create a depletion without a receipt, we need stock first. Insert
    // qty + value via post_transfers in a closed PRIOR period via business_date
    // backdate, then deplete in 2026-04. We disable the append-only trigger
    // briefly to backdate.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;
    // Seed in 2026-04 to get past period checks, then backdate.
    let _ = call_post_transfers(
        &pool,
        serde_json::json!([
            make_event("cycle_count_adj", qty_b, void_qty, 100, "2026-04-01", &fresh_uuid(&pool).await),
            make_event_with_qty("cycle_count_adj", val_b, void_val, 500, 100, "2026-04-01", &fresh_uuid(&pool).await),
        ]),
        false,
    ).await.expect("seed B");
    // Backdate the receipts out of period.
    sqlx::query("ALTER TABLE transfers DISABLE TRIGGER trg_transfers_append_only")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE transfers SET business_date = '2026-03-15'
          WHERE business_date = '2026-04-01' AND debit_account_id IN ($1, $2)",
    ).bind(qty_b).bind(val_b).execute(&pool).await.unwrap();
    sqlx::query("ALTER TABLE transfers ENABLE TRIGGER trg_transfers_append_only")
        .execute(&pool).await.unwrap();

    // Now deplete B in 2026-04 — no receipts in period for B.
    adjust(&pool, &b, "MAIN", -30, None, "2026-04-15").await.unwrap();

    // Force-close: A processes, B skipped.
    let summary = close_period(&pool, "2026-04", true).await.expect("force close");
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(1), "A processed");
    assert_eq!(summary["forced"]["provisional"].as_bool(), Some(true));

    let unfin: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional
          WHERE cost_method='wac_periodic' AND finalized_at IS NULL",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(unfin, 1, "B's row stays un-finalized");
}

#[tokio::test]
async fn close_fails_then_retries_after_posting_receipts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, qty, val) = setup_wac_periodic_sku(&pool, "MX-RETRY").await;

    // Seed via a 2026-03 backdate so we have stock without an in-period receipt.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;
    let _ = call_post_transfers(
        &pool,
        serde_json::json!([
            make_event("cycle_count_adj", qty, void_qty, 100, "2026-04-01", &fresh_uuid(&pool).await),
            make_event_with_qty("cycle_count_adj", val, void_val, 500, 100, "2026-04-01", &fresh_uuid(&pool).await),
        ]),
        false,
    ).await.unwrap();
    sqlx::query("ALTER TABLE transfers DISABLE TRIGGER trg_transfers_append_only")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE transfers SET business_date='2026-03-15'
                  WHERE business_date='2026-04-01' AND debit_account_id IN ($1, $2)")
        .bind(qty).bind(val).execute(&pool).await.unwrap();
    sqlx::query("ALTER TABLE transfers ENABLE TRIGGER trg_transfers_append_only")
        .execute(&pool).await.unwrap();

    // Deplete in 2026-04 with no in-period receipts.
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-15").await.unwrap();

    // First close attempt: P0020.
    expect_sqlstate("P0020", || async {
        close_period(&pool, "2026-04", false).await.map(|_| ())
    }).await;

    // Post a receipt.
    adjust(&pool, &sku, "MAIN", 100, Some(7), "2026-04-20").await.unwrap();

    // Retry close: succeeds. Final avg = 700/100 = 7. Provisional was 5 (avg from
    // March-backdated receipt at depletion time = 500/100). Variance = 30 × (7-5) = 60.
    let _ = close_period(&pool, "2026-04", false).await.expect("retry");
    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_periodic'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(variance, 60);
}

// ============================================================
// Section 6: Audit trail & variance routing
// ============================================================

#[tokio::test]
async fn variance_wac_period_account_nets_to_zero_per_close() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-NETZERO").await;

    // Trigger non-zero variance.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-10").await.unwrap();
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-04-15").await.unwrap();
    let _ = close_period(&pool, "2026-04", false).await.unwrap();

    let var_acct = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0, "variance_wac_period nets to zero");

    // But the variance DID flow through it (debits + credits both > 0).
    let (debits, credits): (i64, i64) = sqlx::query_as(
        "SELECT debits_total::BIGINT, credits_total::BIGINT FROM accounts WHERE id = $1",
    ).bind(var_acct).fetch_one(&pool).await.unwrap();
    assert!(debits > 0, "variance flowed through (debits > 0)");
    assert_eq!(debits, credits, "and netted symmetrically");
}

#[tokio::test]
async fn audit_row_records_full_close_state() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-AUDIT").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -25, None, "2026-04-10").await.unwrap();
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-04-15").await.unwrap();
    let _ = close_period(&pool, "2026-04", false).await.unwrap();

    let (qty, finalized_at, variance_amount, variance_xfer_id): (i64, Option<String>, i64, Option<i64>)
        = sqlx::query_as(
            "SELECT qty, finalized_at::text, variance_amount, variance_transfer_id
               FROM transfers_provisional WHERE cost_method='wac_periodic'",
        ).fetch_one(&pool).await.unwrap();

    assert_eq!(qty, 25);
    assert!(finalized_at.is_some());
    assert_eq!(variance_amount, 50);  // 25 × (7 - 5)
    assert!(variance_xfer_id.is_some());

    // The variance_transfer_id points at a real transfer with reason='cost_restate'.
    let reason: String = sqlx::query_scalar(
        "SELECT reason::text FROM transfers WHERE id = $1",
    ).bind(variance_xfer_id.unwrap()).fetch_one(&pool).await.unwrap();
    assert_eq!(reason, "cost_restate");
}

#[tokio::test]
async fn variance_transfers_have_correct_document_kind() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _q, _v) = setup_wac_periodic_sku(&pool, "MX-DOCKIND").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-10").await.unwrap();
    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-04-15").await.unwrap();
    let _ = close_period(&pool, "2026-04", false).await.unwrap();

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers
          WHERE document_kind = 'wac_periodic_close'
            AND reason = 'cost_restate'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 2, "two transfers per provisional row (routing through variance acct)");
}

// ============================================================
// Section 7: Concurrency & idempotency
// ============================================================

#[tokio::test]
async fn close_already_closed_period_raises_p0014() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (sku, _, _) = setup_wac_periodic_sku(&pool, "MX-IDEM").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    let _ = close_period(&pool, "2026-04", false).await.unwrap();

    expect_sqlstate("P0014", || async {
        close_period(&pool, "2026-04", false).await.map(|_| ())
    }).await;
}

// ============================================================
// Section 8: Regression on other cost methods
// ============================================================

#[tokio::test]
async fn standard_sku_unaffected_by_wac_periodic_changes() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku: String = sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ('MX-STD', 'EA', 'standard')
         RETURNING id::text",
    ).fetch_one(&pool).await.unwrap();
    let _ = open_qty_account(&pool, &sku, "MAIN").await;
    let val = open_value_account(&pool, &sku, "MAIN", "inv_value_fg", "USD").await;
    seed_standard_cost(&pool, "MX-STD", 100).await;

    adjust(&pool, &sku, "MAIN", 50, None, "2026-04-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -20, None, "2026-04-10").await.unwrap();
    assert_eq!(balance(&pool, val).await, 50 * 100 - 20 * 100);  // 3000

    let summary = close_period(&pool, "2026-04", false).await.expect("close");
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(0),
               "standard SKU produces no provisional rows");

    // No transfers_provisional rows for this SKU.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE t.business_date BETWEEN '2026-04-01' AND '2026-04-30'
            AND tp.cost_method='wac_periodic'",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn wac_perpetual_sku_unaffected_by_wac_periodic_changes() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_perpetual_sku(&pool, "MX-WACP").await;
    let _ = open_qty_account(&pool, &sku, "MAIN").await;
    let val = open_value_account(&pool, &sku, "MAIN", "inv_value_fg", "USD").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05").await.unwrap();
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-10").await.unwrap();
    // wac_perpetual posts at live pool avg, no flagging.
    let pre_close_balance = balance(&pool, val).await;
    assert_eq!(pre_close_balance, 100 * 5 - 30 * 5);

    let summary = close_period(&pool, "2026-04", false).await.expect("close");
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(0));

    // Balance unchanged after close (no variance posted).
    assert_eq!(balance(&pool, val).await, pre_close_balance);
}

// ============================================================
// Section 9: Hook dispatch order — transaction-scoped spy.
// ============================================================

#[tokio::test]
async fn hooks_called_in_documented_order_with_real_body_preserved() {
    // Replaces the deferred spy from period_close.rs. Uses a sqlx
    // transaction so the CREATE OR REPLACE FUNCTION calls roll back at
    // end, leaving the production wac_periodic real body intact for any
    // tests that run after this one.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    let mut tx = pool.begin().await.expect("begin tx");

    sqlx::raw_sql(
        "CREATE TABLE _hook_log (n BIGSERIAL PRIMARY KEY, hook_name TEXT NOT NULL);

         DROP FUNCTION wac_periodic_close_hook(BIGINT, BOOLEAN);
         CREATE OR REPLACE FUNCTION wac_periodic_close_hook(p_period_id BIGINT, p_force_provisional BOOLEAN DEFAULT FALSE)
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO _hook_log (hook_name) VALUES ('wac_periodic'); RETURN 0; END;
         $$;

         DROP FUNCTION wac_retroactive_close_hook(BIGINT, BOOLEAN);
         CREATE OR REPLACE FUNCTION wac_retroactive_close_hook(p_period_id BIGINT, p_force_provisional BOOLEAN DEFAULT FALSE)
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO _hook_log (hook_name) VALUES ('wac_retroactive'); RETURN 0; END;
         $$;

         DROP FUNCTION cost_adjust_retroactive_hook(BIGINT, BOOLEAN);
         CREATE OR REPLACE FUNCTION cost_adjust_retroactive_hook(p_period_id BIGINT, p_force_provisional BOOLEAN DEFAULT FALSE)
         RETURNS BIGINT LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO _hook_log (hook_name) VALUES ('cost_adjust_retroactive'); RETURN 0; END;
         $$;",
    )
    .execute(&mut *tx)
    .await
    .expect("install spies in tx");

    // Run close_period inside the tx.
    sqlx::query("SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid)
        .bind(&actor)
        .execute(&mut *tx)
        .await
        .expect("close in tx");

    let order: Vec<String> = sqlx::query_scalar("SELECT hook_name FROM _hook_log ORDER BY n")
        .fetch_all(&mut *tx)
        .await
        .expect("read log");

    assert_eq!(
        order,
        vec!["wac_periodic", "wac_retroactive", "cost_adjust_retroactive"],
        "hooks invoked in spec'd order"
    );

    // ROLLBACK undoes the function overrides + the table creation +
    // close_period's UPDATE on periods. Production state is preserved.
    tx.rollback().await.expect("rollback");

    // Verify the real wac_periodic body is still in place by exercising
    // the dispatcher's wac_periodic branch on a fresh SKU (which should
    // raise P0011 for empty-pool first-IN, NOT a stub-style RETURN-0).
    let (sku, _, _) = setup_wac_periodic_sku(&pool, "MX-AFTER-SPY").await;
    expect_sqlstate("P0011", || async {
        adjust(&pool, &sku, "MAIN", 10, None, "2026-04-15").await.map(|_| ())
    }).await;
}
