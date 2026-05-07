//! `acct-7mb` / `acct-9tw.1` — smoke test for wac_retroactive.
//!
//! The canonical late-arrival use case: receipt arrives mid-period with
//! a business_date earlier than the depletion's, but is posted (booked)
//! after the depletion. Mid-period perpetual missed it; close-time
//! retroactive replay puts it before the depletion in chronological
//! order, computes a different running avg, and posts the variance.
//!
//! Full replay matrix lives in 9tw.2 (acct-e04).

mod common;

use common::*;

async fn insert_wac_retroactive_sku(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'wac_retroactive')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert wac_retroactive sku {code}: {e}"))
}

async fn open_qty(pool: &sqlx::PgPool, sku_id: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
           FROM locations l WHERE l.code = 'MAIN'
         RETURNING id",
    )
    .bind(sku_id)
    .fetch_one(pool)
    .await
    .expect("open stock_available")
}

async fn open_value_fg(pool: &sqlx::PgPool, sku_id: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = 'MAIN'
         RETURNING id",
    )
    .bind(sku_id)
    .fetch_one(pool)
    .await
    .expect("open inv_value_fg")
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
async fn adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    business_date: &str,
) -> sqlx::Result<String> {
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD',
            'fg', $5::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(&loc_id)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

#[tokio::test]
async fn canonical_late_arrival_posts_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_retroactive_sku(&pool, "WACR-LATE").await;
    let _ = open_qty(&pool, &sku).await;
    let val = open_value_fg(&pool, &sku).await;

    // Step 1 (real time T0): receive 100@5, business_date 2026-04-01.
    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.expect("rcv1");

    // Step 2 (real time T1 > T0): deplete 30, business_date 2026-04-10.
    // At posting time, pool state is 100u/$500 → running avg = 5.
    // Provisional value posted: 30 × 5 = 150.
    adjust(&pool, &sku, -30, None, "2026-04-10").await.expect("dep");

    // Step 3 (real time T2 > T1): receive 100@9, business_date 2026-04-05
    // (BACKDATED — paperwork filed late).
    adjust(&pool, &sku, 100, Some(9), "2026-04-05").await.expect("rcv2 backdated");

    // Pool state at this point (current balances):
    // value = 500 + 900 - 150 = 1250
    // qty   = 100 + 100 - 30  = 170
    assert_eq!(balance(&pool, val).await, 1250);

    // Close the period. Hook walks events in (business_date, posted_at, id) order:
    //   2026-04-01: receipt 1 (100@5) → pool 100/$500
    //   2026-04-05: receipt 2 (100@9) → pool 200/$1400 [late-arrival placed correctly]
    //   2026-04-10: depletion. Recomputed avg = 1400/200 = 7.
    //               Recomputed amount = 30 × 7 = 210.
    //               Variance = 210 - 150 = 60 (write-up).
    // After variance posting (routes through variance_wac_retroactive):
    //   inv_value_fg net effect: extra -60 (the variance).
    //   Final inv_value_fg = 1250 - 60 = 1190.
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close");

    assert_eq!(
        summary["hook_results"]["wac_retroactive"].as_i64(),
        Some(1),
        "one provisional finalized"
    );

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional WHERE cost_method='wac_retroactive'",
    )
    .fetch_one(&pool)
    .await
    .expect("variance");
    assert_eq!(variance, 60, "late-arrival variance: 30 × (7 final - 5 provisional)");

    assert_eq!(balance(&pool, val).await, 1190, "fg post-close: 1250 - 60");

    // Sanity: variance_wac_retroactive nets to zero.
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0);
}

#[tokio::test]
async fn no_late_arrival_zero_variance() {
    // Receipts and depletions all in chronological order, no backdated
    // events. Close should produce zero variance.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_retroactive_sku(&pool, "WACR-CLEAN").await;
    let _ = open_qty(&pool, &sku).await;
    let val = open_value_fg(&pool, &sku).await;

    // Receive 100@5, deplete 30 (provisional = 5). No late arrivals.
    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.expect("rcv");
    adjust(&pool, &sku, -30, None, "2026-04-10").await.expect("dep");

    let val_pre = balance(&pool, val).await;

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close");

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional WHERE cost_method='wac_retroactive'",
    )
    .fetch_one(&pool)
    .await
    .expect("variance");
    assert_eq!(variance, 0);
    assert_eq!(balance(&pool, val).await, val_pre, "no variance, pool unchanged");
}

// ============================================================
// Multi-depletion chronological replay
// ============================================================

#[tokio::test]
async fn multiple_depletions_each_at_their_running_avg() {
    // No late arrivals. Receipts and depletions interleave; each
    // depletion's recomputed avg matches the running pool avg at
    // that moment in time. Variance should be 0 for all (since
    // mid-period perpetual already saw correct ordering).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-MULTI-DEP").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();
    adjust(&pool, &sku, -20, None, "2026-04-05").await.unwrap();   // avg=5
    adjust(&pool, &sku, 100, Some(7), "2026-04-10").await.unwrap();
    adjust(&pool, &sku, -30, None, "2026-04-15").await.unwrap();   // avg=floor((400+700)/180)=floor(6.11)=6

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let variances: Vec<i64> = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional
          WHERE cost_method='wac_retroactive' ORDER BY posting_line_id")
        .fetch_all(&pool).await.unwrap();
    assert_eq!(variances, vec![0, 0],
        "no late arrivals → both depletions match perpetual chain");
}

// ============================================================
// Tied business_date — posted_at tiebreak
// ============================================================

#[tokio::test]
async fn tied_business_date_replays_in_posted_at_order() {
    // Two events on 2026-04-05: a depletion is posted FIRST (real time
    // T0) at running avg=5; then a backdated receipt (still 2026-04-05)
    // is posted SECOND (real time T1) at $9. Mid-period depletion saw
    // pool 100/$500 → avg 5. At close, replay order is by posted_at —
    // depletion comes first (T0 < T1), so the replay matches the
    // perpetual chain. Variance = 0.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-TIE-PA").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    // Pre-period seed: one receipt to establish pool.
    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();
    // T0: depletion on 2026-04-05.
    adjust(&pool, &sku, -20, None, "2026-04-05").await.unwrap();
    // T1: receipt on 2026-04-05 (same business_date, posted later).
    adjust(&pool, &sku, 100, Some(9), "2026-04-05").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional
          WHERE cost_method='wac_retroactive'")
        .fetch_one(&pool).await.unwrap();
    // Replay order: receipt (2026-04-01) → depletion (2026-04-05 T0) →
    // receipt (2026-04-05 T1). Depletion sees pool 100/$500 → avg=5.
    // Same as mid-period perpetual. Variance=0.
    assert_eq!(variance, 0);
}

// ============================================================
// Pre-period state used when no in-period receipts before depletion
// ============================================================

#[tokio::test]
async fn depletion_uses_pre_period_carry_forward() {
    // Receive in March (pre-period), deplete in April (in-period).
    // No in-period receipts at all. Replay's pre-period state is the
    // March receipt; depletion gets re-costed against that → variance=0.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-CARRY").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    // Need to receive in a period that's open. 2026-03 is fixture-closed
    // so use override to post into it.
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool).await.unwrap();
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    // Post via post_inventory_adjustment first into 2026-04 (open), then
    // backdate to 2026-03 via the disable-trigger pattern.
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 100, 5::BIGINT, 'USD', 'fg',
            '2026-04-01'::DATE, $3::UUID, $4::UUID, NULL)::text")
        .bind(&sku).bind(&loc_id).bind(&posted_by).bind(&key)
        .fetch_one(&pool).await.unwrap();

    sqlx::query("ALTER TABLE posting_lines DISABLE TRIGGER trg_posting_lines_append_only")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE posting_lines SET business_date = '2026-03-15'
          WHERE business_date = '2026-04-01'")
        .execute(&pool).await.unwrap();
    sqlx::query("ALTER TABLE posting_lines ENABLE TRIGGER trg_posting_lines_append_only")
        .execute(&pool).await.unwrap();

    // In-period depletion (no in-period receipts).
    adjust(&pool, &sku, -30, None, "2026-04-10").await.expect("dep");

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional
          WHERE cost_method='wac_retroactive'")
        .fetch_one(&pool).await.unwrap();
    // Pre-period state: 100/$500. Depletion at avg=5. Recomputed=5. Variance=0.
    assert_eq!(variance, 0,
        "depletion uses pre-period carry-forward; no variance");
}

// ============================================================
// Multi-pool one SKU
// ============================================================

#[tokio::test]
async fn raw_and_fg_pools_replay_independently_one_sku() {
    // Same SKU has both raw and fg pools. Each replays independently
    // against its own per-class transfer history. Late arrival in one
    // class doesn't affect the other.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-MULTIPOOL").await;
    let _ = open_qty(&pool, &sku).await;
    let v_raw: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_raw', 'value', 'USD', 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = 'MAIN'
         RETURNING id")
        .bind(&sku).fetch_one(&pool).await.unwrap();
    let _v_fg = open_value_fg(&pool, &sku).await;

    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code='MAIN'")
        .fetch_one(&pool).await.unwrap();
    async fn adj_class(pool: &sqlx::PgPool, sku: &str, loc: &str, qty: i64,
                       cost: Option<i64>, class: &str, date: &str) -> sqlx::Result<String> {
        let posted_by = fresh_uuid(pool).await;
        let key = fresh_uuid(pool).await;
        sqlx::query_scalar(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD',
                $5, $6::DATE, $7::UUID, $8::UUID, NULL)::text")
            .bind(sku).bind(loc).bind(qty).bind(cost).bind(class).bind(date)
            .bind(&posted_by).bind(&key)
            .fetch_one(pool).await
    }

    // RAW: 100@5 on 2026-04-01, deplete 20 on 2026-04-10 (provisional=5),
    // late-book 100@9 on 2026-04-15 backdated to 2026-04-05.
    adj_class(&pool, &sku, &loc_id, 100, Some(5), "raw", "2026-04-01").await.unwrap();
    adj_class(&pool, &sku, &loc_id, -20, None,    "raw", "2026-04-10").await.unwrap();
    adj_class(&pool, &sku, &loc_id, 100, Some(9), "raw", "2026-04-05").await.unwrap();
    // Replay: avg = (500+900)/200 = 7. Variance = 20 × (7-5) = 40.

    // FG: 50@10 on 2026-04-01, deplete 30 on 2026-04-10. No late arrivals.
    adj_class(&pool, &sku, &loc_id, 50, Some(10), "fg", "2026-04-01").await.unwrap();
    adj_class(&pool, &sku, &loc_id, -30, None,    "fg", "2026-04-10").await.unwrap();
    // Replay: avg=10 throughout. Variance=0.

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let raw_var: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM posting_lines_provisional tp
           JOIN posting_lines t ON t.id = tp.posting_line_id
          WHERE tp.cost_method='wac_retroactive' AND t.credit_account_id = $1")
        .bind(v_raw).fetch_one(&pool).await.unwrap();
    assert_eq!(raw_var, 40, "raw class with late arrival → variance 40");

    let fg_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines_provisional tp
           JOIN posting_lines t ON t.id = tp.posting_line_id
          WHERE tp.cost_method='wac_retroactive' AND tp.variance_amount = 0")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(fg_count, 1, "fg class no late arrival → variance 0");
}

// ============================================================
// Difference vs wac_periodic
// ============================================================

#[tokio::test]
async fn retroactive_and_periodic_diverge_on_late_arrival() {
    // Same setup on a wac_periodic vs wac_retroactive SKU side by side.
    // Late-arrival receipt with business_date earlier than the depletion.
    //
    // wac_periodic: final period avg = Σ(in-period receipts)/Σ(in-period qty).
    //   Receipts: 100@5 + 100@9 = 1400/200 = 7. Variance = 30 × (7-5) = 60.
    // wac_retroactive: replay places late receipt before depletion.
    //   Pool at depletion = 200/$1400. Recomputed = 30 × 7 = 210.
    //   Variance = 60.
    //
    // In THIS specific case the two methods give the same number,
    // BECAUSE the depletion happens after both receipts in business_date
    // order. They diverge when depletions happen between receipts.
    // Test that case below.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let p_sku = sqlx::query_scalar::<_, String>(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ('WACR-VS-P', 'EA', 'wac_periodic') RETURNING id::text")
        .fetch_one(&pool).await.unwrap();
    let r_sku = insert_wac_retroactive_sku(&pool, "WACR-VS-R").await;

    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code='MAIN'")
        .fetch_one(&pool).await.unwrap();

    for sku in [&p_sku, &r_sku] {
        sqlx::query("INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
                     SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
                       FROM locations l WHERE l.code = 'MAIN'")
            .bind(sku).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
                     SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
                       FROM locations l WHERE l.code = 'MAIN'")
            .bind(sku).execute(&pool).await.unwrap();
    }

    // Depletion BETWEEN receipts: receive R1@5, deplete (between),
    // receive R2@9. Provisional avg at depletion = 5 (only R1 in pool).
    // - periodic final avg = (500+900)/(100+100) = 7. Variance = 30×(7-5)=60.
    // - retroactive replay: at depletion business_date 2026-04-05,
    //   only R1 (business_date 2026-04-01) is in chain.
    //   Recomputed = 30 × 5 = 150. Variance = 0.
    // Yes, they diverge here.
    for sku in [&p_sku, &r_sku] {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, 100, 5::BIGINT, 'USD', 'fg',
                '2026-04-01'::DATE, $3::UUID, $4::UUID, NULL)::text")
            .bind(sku).bind(&loc_id).bind(&posted_by).bind(&key)
            .fetch_one(&pool).await.unwrap();

        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, -30, NULL::BIGINT, 'USD', 'fg',
                '2026-04-05'::DATE, $3::UUID, $4::UUID, NULL)::text")
            .bind(sku).bind(&loc_id).bind(&posted_by).bind(&key)
            .fetch_one(&pool).await.unwrap();

        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, 100, 9::BIGINT, 'USD', 'fg',
                '2026-04-10'::DATE, $3::UUID, $4::UUID, NULL)::text")
            .bind(sku).bind(&loc_id).bind(&posted_by).bind(&key)
            .fetch_one(&pool).await.unwrap();
    }

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let p_variance: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM posting_lines_provisional tp
           JOIN posting_lines t ON t.id = tp.posting_line_id
           JOIN accounts a ON a.id = t.credit_account_id
          WHERE tp.cost_method='wac_periodic' AND a.sku_id = $1::UUID")
        .bind(&p_sku).fetch_one(&pool).await.unwrap();
    let r_variance: i64 = sqlx::query_scalar(
        "SELECT tp.variance_amount FROM posting_lines_provisional tp
           JOIN posting_lines t ON t.id = tp.posting_line_id
           JOIN accounts a ON a.id = t.credit_account_id
          WHERE tp.cost_method='wac_retroactive' AND a.sku_id = $1::UUID")
        .bind(&r_sku).fetch_one(&pool).await.unwrap();

    assert_eq!(p_variance, 60,
        "periodic uses single period avg = 7; variance = 30×(7-5)");
    assert_eq!(r_variance, 0,
        "retroactive replay puts depletion before R2; recomputed = 5; variance = 0");
}

// ============================================================
// Multi-period chain
// ============================================================

#[tokio::test]
async fn multi_period_chain_each_close_independent() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-CHAIN").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    // April: receive 100@5, deplete 20 (no late arrivals). Variance=0.
    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();
    adjust(&pool, &sku, -20, None, "2026-04-10").await.unwrap();
    let pid_april = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid_april).bind(&actor).fetch_one(&pool).await.expect("close april");

    // May: receive 100@9, deplete 30, late-book 100@7 backdated to 2026-05-05
    // (depletion business_date 2026-05-10).
    adjust(&pool, &sku, 100, Some(9), "2026-05-01").await.unwrap();
    adjust(&pool, &sku, -30, None, "2026-05-10").await.unwrap();
    adjust(&pool, &sku, 100, Some(7), "2026-05-05").await.unwrap();
    // Provisional at depletion: pool was carried-forward(80@5=400) + R(100@9=900) = 180/$1300, avg=floor(7.22)=7.
    // Replay: pre-period state from April end = 80/$400. In-period chronologically:
    //   R(100@9) on 2026-05-01 → 180/$1300
    //   R(100@7) on 2026-05-05 → 280/$2000
    //   D(30) on 2026-05-10 → recomputed avg = 2000/280 = floor(7.14)=7.
    // Variance = 30 × (7-7) = 0.
    // (Note: integer truncation makes provisional and recomputed equal here.)
    let pid_may = period_id(&pool, "2026-05").await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid_may).bind(&actor).fetch_one(&pool).await.expect("close may");

    let april_var: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional tp
           JOIN posting_lines t ON t.id = tp.posting_line_id
          WHERE tp.cost_method='wac_retroactive' AND t.business_date < '2026-05-01'")
        .fetch_one(&pool).await.unwrap();
    let may_var: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional tp
           JOIN posting_lines t ON t.id = tp.posting_line_id
          WHERE tp.cost_method='wac_retroactive' AND t.business_date >= '2026-05-01'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(april_var, 0);
    assert_eq!(may_var, 0, "May depletion sees both May receipts before it; running avg = 7");
}

// ============================================================
// Edge cases
// ============================================================

#[tokio::test]
async fn empty_period_no_provisional_rows() {
    // Period has receipts but no depletions. Hook returns 0; no
    // variance posted; no posting_lines_provisional rows created.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-NODEP").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");
    assert_eq!(summary["hook_results"]["wac_retroactive"].as_i64(), Some(0));

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines_provisional WHERE cost_method='wac_retroactive'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn no_p0020_when_only_pre_period_receipts() {
    // wac_periodic raises P0020 if a pool has provisional depletions
    // but ZERO in-period receipts (because periodic needs in-period
    // receipts to compute the avg). wac_retroactive uses pre-period
    // running state — so this case is fine; variance just reflects
    // pre-period state evolution.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-PRESEED").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    // Set up pre-period inventory via 2026-04 post + backdate.
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code='MAIN'")
        .fetch_one(&pool).await.unwrap();
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 100, 7::BIGINT, 'USD', 'fg',
            '2026-04-01'::DATE, $3::UUID, $4::UUID, NULL)::text")
        .bind(&sku).bind(&loc_id).bind(&posted_by).bind(&key)
        .fetch_one(&pool).await.unwrap();

    sqlx::query("ALTER TABLE posting_lines DISABLE TRIGGER trg_posting_lines_append_only")
        .execute(&pool).await.unwrap();
    sqlx::query("UPDATE posting_lines SET business_date = '2026-03-15' WHERE business_date = '2026-04-01'")
        .execute(&pool).await.unwrap();
    sqlx::query("ALTER TABLE posting_lines ENABLE TRIGGER trg_posting_lines_append_only")
        .execute(&pool).await.unwrap();

    // In-period depletion only.
    adjust(&pool, &sku, -25, None, "2026-04-15").await.unwrap();

    // Should close cleanly (no P0020, unlike wac_periodic).
    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional WHERE cost_method='wac_retroactive'")
        .fetch_one(&pool).await.unwrap();
    // Pre-period: 100/$700, avg=7. Depletion at avg=7. Recomputed=7. Variance=0.
    assert_eq!(variance, 0);
}

// ============================================================
// Audit & variance routing
// ============================================================

#[tokio::test]
async fn variance_wac_retroactive_nets_to_zero_per_close() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-NETZERO").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    // Trigger non-zero variance via late arrival.
    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();
    adjust(&pool, &sku, -30, None, "2026-04-10").await.unwrap();
    adjust(&pool, &sku, 100, Some(9), "2026-04-05").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let var_acct = account_id_by_kind_currency(&pool, "variance_wac_retroactive", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0);

    let (debits, credits): (i64, i64) = sqlx::query_as(
        "SELECT debits_total::BIGINT, credits_total::BIGINT FROM accounts WHERE id = $1")
        .bind(var_acct).fetch_one(&pool).await.unwrap();
    assert!(debits > 0, "variance flowed through");
    assert_eq!(debits, credits);
}

#[tokio::test]
async fn audit_row_records_finalized_state() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = insert_wac_retroactive_sku(&pool, "WACR-AUDIT").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();
    adjust(&pool, &sku, -25, None, "2026-04-10").await.unwrap();
    adjust(&pool, &sku, 100, Some(9), "2026-04-05").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");

    let (qty, finalized_at, variance, var_xfer): (i64, Option<String>, i64, Option<i64>) =
        sqlx::query_as(
            "SELECT qty, finalized_at::text, variance_amount, variance_posting_line_id
               FROM posting_lines_provisional WHERE cost_method='wac_retroactive'")
            .fetch_one(&pool).await.unwrap();
    assert_eq!(qty, 25);
    assert!(finalized_at.is_some());
    assert_eq!(variance, 50, "25 × (7-5)");
    assert!(var_xfer.is_some());

    // Variance transfer carries reason=cost_restate, document_kind=wac_retroactive_close.
    let (reason, doc_kind): (String, String) = sqlx::query_as(
        "SELECT reason::text, document_kind FROM posting_lines WHERE id = $1")
        .bind(var_xfer.unwrap()).fetch_one(&pool).await.unwrap();
    assert_eq!(reason, "cost_restate");
    assert_eq!(doc_kind, "wac_retroactive_close");
}

// ============================================================
// Regression — other cost methods unaffected
// ============================================================

#[tokio::test]
async fn standard_sku_unaffected_by_wac_retroactive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sqlx::query_scalar::<_, String>(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ('WACR-REG-STD', 'EA', 'standard') RETURNING id::text")
        .fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
                 SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
                   FROM locations l WHERE l.code = 'MAIN'")
        .bind(&sku).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
                 SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
                   FROM locations l WHERE l.code = 'MAIN'")
        .bind(&sku).execute(&pool).await.unwrap();
    seed_standard_cost(&pool, "WACR-REG-STD", 100).await;

    adjust(&pool, &sku, 50, None, "2026-04-01").await.unwrap();
    adjust(&pool, &sku, -20, None, "2026-04-10").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");
    assert_eq!(summary["hook_results"]["wac_retroactive"].as_i64(), Some(0));
}

#[tokio::test]
async fn wac_perpetual_sku_unaffected_by_wac_retroactive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sqlx::query_scalar::<_, String>(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ('WACR-REG-PERP', 'EA', 'wac_perpetual') RETURNING id::text")
        .fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
                 SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
                   FROM locations l WHERE l.code = 'MAIN'")
        .bind(&sku).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
                 SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
                   FROM locations l WHERE l.code = 'MAIN'")
        .bind(&sku).execute(&pool).await.unwrap();

    adjust(&pool, &sku, 100, Some(5), "2026-04-01").await.unwrap();
    adjust(&pool, &sku, -30, None, "2026-04-10").await.unwrap();

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)")
        .bind(pid).bind(&actor).fetch_one(&pool).await.expect("close");
    assert_eq!(summary["hook_results"]["wac_retroactive"].as_i64(), Some(0));
}

// ============================================================
// WIP class deferred (D3)
// ============================================================

#[tokio::test]
async fn wip_class_adjustment_raises_p0006_referencing_epic_j() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_retroactive_sku(&pool, "WACR-WIP").await;
    let _ = open_qty(&pool, &sku).await;
    let _ = open_value_fg(&pool, &sku).await;

    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code='MAIN'")
        .fetch_one(&pool).await.unwrap();
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let err = sqlx::query(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 5, 100::BIGINT, 'USD',
            'wip', '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL)")
        .bind(&sku).bind(&loc_id).bind(&posted_by).bind(&key)
        .execute(&pool).await
        .err().expect("expected P0006");
    let db_err = err.as_database_error().unwrap();
    assert_eq!(db_err.code().unwrap().as_ref(), "P0006");
    assert!(db_err.message().contains("acct-p7v"),
        "WIP block message must reference Epic J (acct-p7v)");
}
