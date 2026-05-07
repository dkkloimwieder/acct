//! `acct-tzh` / `acct-75z.2` — proves the multi-class fix on wac_perpetual.
//!
//! Pre-0030 `_post_posting_lines_compute_amount` for `wac_perpetual` divided
//! `inv_value_<pool>.balance / stock_available.balance`. For a SKU with
//! both `inv_value_raw` AND `inv_value_fg` open, `stock_available`
//! pooled the qty across classes, inflating the divisor — unit cost
//! came out wrong.
//!
//! Post-0030, the divisor is computed from `transfers.qty` summed over
//! events tagged on the specific value pool — class-isolated.
//!
//! These tests would fail on 0029. They pass on 0030 because the per-
//! class qty SUM correctly counts only receipts to that specific pool.

mod common;

use common::*;

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

async fn open_value(pool: &sqlx::PgPool, sku_id: &str, kind: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT '{kind}', 'value', 'USD', 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = 'MAIN'
         RETURNING id",
    ))
    .bind(sku_id)
    .fetch_one(pool)
    .await
    .expect("open value account")
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

/// Seed a class-specific pool by posting a paired qty + value cycle_count_adj.
/// The value-leg event includes qty so the per-class SUM populates correctly.
async fn seed_class(
    pool: &sqlx::PgPool,
    qty_acct: i64,
    void_qty: i64,
    val_acct: i64,
    void_val: i64,
    qty: i64,
    value_usd: i64,
    business_date: &str,
) {
    let _ = call_post_posting_lines(
        pool,
        serde_json::json!([
            make_event("cycle_count_adj", qty_acct, void_qty, qty, business_date, &fresh_uuid(pool).await),
            make_event_with_qty("cycle_count_adj", val_acct, void_val, value_usd, qty, business_date, &fresh_uuid(pool).await),
        ]),
        false,
    )
    .await
    .expect("seed pool");
}

#[tokio::test]
async fn perpetual_so_ship_uses_per_class_fg_avg_not_pooled() {
    // Same SKU has both raw AND fg pools open.
    //   raw: 100u / $500 (avg $5)
    //   fg:  100u / $1000 (avg $10)
    // so_ship qty=10 from inv_value_fg should compute unit at fg's avg ($10),
    // not at the cross-class blended avg (200u for $1500 = $7.50).
    //
    // Pre-0030 (stock_available divisor):
    //   stock_available = 200u (raw + fg pooled), inv_value_fg = $1000
    //   unit = $1000 / 200 = $5, amount = $50 (WRONG)
    //
    // Post-0030 (per-class SUM divisor):
    //   transfers.qty for inv_value_fg = +100, inv_value_fg balance = $1000
    //   unit = $1000 / 100 = $10, amount = $100 (CORRECT)
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_perpetual_sku(&pool, "MULTICLASS-PERP").await;
    let qty_acct = open_qty(&pool, &sku).await;
    let raw_acct = open_value(&pool, &sku, "inv_value_raw").await;
    let fg_acct = open_value(&pool, &sku, "inv_value_fg").await;

    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    seed_class(&pool, qty_acct, void_qty, raw_acct, void_val, 100, 500, "2026-04-15").await;
    seed_class(&pool, qty_acct, void_qty, fg_acct, void_val, 100, 1000, "2026-04-15").await;

    // Pool state confirmed:
    assert_eq!(balance(&pool, raw_acct).await, 500);
    assert_eq!(balance(&pool, fg_acct).await, 1000);
    assert_eq!(balance(&pool, qty_acct).await, 200, "stock_available pools across classes");

    // so_ship from fg.
    let cogs = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let event = serde_json::json!({
        "reason":            "so_ship",
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  cogs,
        "credit_account_id": fg_acct,
        "qty":               10,
        "business_date":     "2026-04-16",
        "idempotency_key":   fresh_uuid(&pool).await,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    let _ = call_post_posting_lines(&pool, serde_json::json!([event]), false)
        .await
        .expect("so_ship");

    // Post-fix expectation: fg unit cost = $10, amount = $100.
    assert_eq!(balance(&pool, cogs).await, 100, "cogs at per-class fg unit cost ($10)");
    assert_eq!(balance(&pool, fg_acct).await, 900, "fg pool: $1000 - $100");
    // Raw pool unaffected.
    assert_eq!(balance(&pool, raw_acct).await, 500);
}

#[tokio::test]
async fn perpetual_scrap_from_raw_uses_per_class_raw_avg() {
    // Symmetric to the fg test. scrap qty=5 from inv_value_raw should use
    // raw's per-class avg ($5), not the blended ($7.50).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_perpetual_sku(&pool, "MULTICLASS-RAW-SCRAP").await;
    let qty_acct = open_qty(&pool, &sku).await;
    let raw_acct = open_value(&pool, &sku, "inv_value_raw").await;
    let fg_acct = open_value(&pool, &sku, "inv_value_fg").await;

    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    seed_class(&pool, qty_acct, void_qty, raw_acct, void_val, 100, 500, "2026-04-15").await;
    seed_class(&pool, qty_acct, void_qty, fg_acct, void_val, 100, 1000, "2026-04-15").await;

    // Using cogs as the debit-side P&L offset for scrap (variance_scrap
    // account_kind isn't currently seeded in the fixture; semantics are
    // close enough for verifying the per-class divisor).
    let scrap_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let event = serde_json::json!({
        "reason":            "scrap",
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  scrap_acct,
        "credit_account_id": raw_acct,
        "qty":               5,
        "business_date":     "2026-04-16",
        "idempotency_key":   fresh_uuid(&pool).await,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    let _ = call_post_posting_lines(&pool, serde_json::json!([event]), false)
        .await
        .expect("scrap");

    // Pre-fix would have computed unit = (500+1000)/200 = $7.50, amount $37 (truncating).
    // Post-fix: per-class raw avg = $5, amount = $25.
    assert_eq!(balance(&pool, scrap_acct).await, 25, "scrap variance at per-class raw unit ($5)");
    assert_eq!(balance(&pool, raw_acct).await, 475, "raw: $500 - $25");
    assert_eq!(balance(&pool, fg_acct).await, 1000, "fg pool unaffected");
}

#[tokio::test]
async fn perpetual_raw_pool_avg_independent_of_fg_activity() {
    // After receiving more fg at a wildly different cost, the raw pool's
    // per-class avg shouldn't budge. Pre-fix it would change because
    // stock_available aggregates qty across classes.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_perpetual_sku(&pool, "MULTICLASS-INDEP").await;
    let qty_acct = open_qty(&pool, &sku).await;
    let raw_acct = open_value(&pool, &sku, "inv_value_raw").await;
    let fg_acct = open_value(&pool, &sku, "inv_value_fg").await;

    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    // Raw: 100u/$500 → $5/u.
    seed_class(&pool, qty_acct, void_qty, raw_acct, void_val, 100, 500, "2026-04-15").await;
    // FG: 50u/$5000 → $100/u (extreme).
    seed_class(&pool, qty_acct, void_qty, fg_acct, void_val, 50, 5000, "2026-04-15").await;

    // Now scrap 1u from raw. Should still cost $5 (raw's per-class avg),
    // not the blended ($5500/150 = $36.67).
    // Using cogs as the debit-side P&L offset for scrap (variance_scrap
    // account_kind isn't currently seeded in the fixture; semantics are
    // close enough for verifying the per-class divisor).
    let scrap_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let event = serde_json::json!({
        "reason":            "scrap",
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  scrap_acct,
        "credit_account_id": raw_acct,
        "qty":               1,
        "business_date":     "2026-04-16",
        "idempotency_key":   fresh_uuid(&pool).await,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    let _ = call_post_posting_lines(&pool, serde_json::json!([event]), false)
        .await
        .expect("scrap");

    assert_eq!(balance(&pool, scrap_acct).await, 5, "raw scrap at per-class raw avg ($5)");
    assert_eq!(balance(&pool, raw_acct).await, 495);
    assert_eq!(balance(&pool, fg_acct).await, 5000, "fg untouched");
}
