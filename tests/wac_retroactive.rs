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
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_retroactive'",
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
        "SELECT variance_amount FROM transfers_provisional WHERE cost_method='wac_retroactive'",
    )
    .fetch_one(&pool)
    .await
    .expect("variance");
    assert_eq!(variance, 0);
    assert_eq!(balance(&pool, val).await, val_pre, "no variance, pool unchanged");
}
