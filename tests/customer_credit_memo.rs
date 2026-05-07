//! `acct-tae` — post_customer_credit_memo / standalone AR credit memo.
//!
//! Covers:
//!   * pure financial credit (e.g. goodwill, dispute settlement)
//!   * standalone goods return (no ship_line ref) per disposition
//!   * mixed memo (financial + goods_return on same doc)
//!   * tax handling on financial line
//!   * idempotency replay
//!   * validation: P0048 paths

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

#[allow(dead_code)]
struct Scaffold {
    customer_id: String,
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_ar: i64,
    revenue_acct: i64,
    cogs_acct: i64,
    tax_acct: i64,
    var_scrap_acct: i64,
    stock_scrap_acct: i64,
    stock_quarantine_acct: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
}

async fn fresh_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("customer {code}: {e}"))
}

async fn fresh_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("sku {code}: {e}"))
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("loc {code}: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id,
                                counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction) RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn set_std(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .expect("std cost");
}

async fn scaffold(pool: &PgPool, suffix: &str) -> Scaffold {
    let customer_id = fresh_customer(pool, &format!("CUST-MEMO-{suffix}")).await;
    let sku_id = fresh_sku(pool, &format!("SKU-MEMO-{suffix}")).await;
    let loc_id = fresh_location(pool, &format!("MEMO-{suffix}")).await;
    set_std(pool, &sku_id, 60).await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let cust_qty = open_account(
        pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let stock_scrap_acct = open_account(
        pool, "stock_scrap", "qty", None, Some(&sku_id), None, None, "debit",
    )
    .await;
    let stock_quarantine_acct = open_account(
        pool, "stock_quarantine", "qty", None, Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;

    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let tax_acct = account_id_by_kind_currency(pool, "sales_tax_payable", Some("USD")).await;
    let var_scrap_acct = account_id_by_kind_currency(pool, "variance_scrap", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    Scaffold {
        customer_id, sku_id, loc_id, qty_acct, val_acct, cust_qty, cust_ar,
        revenue_acct, cogs_acct, tax_acct, var_scrap_acct,
        stock_scrap_acct, stock_quarantine_acct,
        creation_void_qty, creation_void_val,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id).fetch_one(pool).await.expect("balance")
}

async fn seed_ar_balance(pool: &PgPool, s: &Scaffold, amount: i64) {
    // Mimic an actual ar_invoice posting: ar DR / revenue CR. This both
    // builds ar's debit balance (so credit memo can reduce it) AND
    // builds revenue's credit balance (so the memo can debit revenue
    // without pushing the credit-normal account negative).
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([{
        "reason":"ar_invoice","document_kind":"seed","document_id":doc_id,
        "debit_account_id":s.cust_ar,"credit_account_id":s.revenue_acct,
        "amount":amount,
        "business_date":"2026-04-15",
        "idempotency_key":fresh_uuid(pool).await,
        "posted_by":posted_by
    }]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)").bind(mint)
        .execute(pool).await.expect("seed ar");
}

/// Pre-seed sales_tax_payable with credit balance so memos that debit
/// the tax account don't push it negative.
async fn seed_sales_tax(pool: &PgPool, s: &Scaffold, amount: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([{
        "reason":"ar_invoice","document_kind":"seed","document_id":doc_id,
        "debit_account_id":s.cust_ar,"credit_account_id":s.tax_acct,
        "amount":amount,
        "business_date":"2026-04-15",
        "idempotency_key":fresh_uuid(pool).await,
        "posted_by":posted_by
    }]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)").bind(mint)
        .execute(pool).await.expect("seed sales_tax");
}

/// Pre-seed the accounts a goods_return memo will drain — customer_pool
/// (qty), cogs (value). Mimics the side-effects of a prior so_ship so
/// standalone-return debit/credit legs land on accounts that have
/// non-zero opening balances. cycle_count_adj is not a cost-event
/// reason; the dispatcher passes through with caller-supplied amount.
async fn seed_customer_pool(pool: &PgPool, s: &Scaffold, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":s.cust_qty,"credit_account_id":s.creation_void_qty,
         "amount":qty,"qty":qty,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        // cogs DR mimics prior ship's cogs leg so the memo's cogs-credit
        // doesn't push debit-normal cogs negative. Use unit_cost=60 to
        // match the test's typical cost.
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":s.cogs_acct,"credit_account_id":s.creation_void_val,
         "amount":qty * 60,"qty":qty,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by}
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)").bind(mint)
        .execute(pool).await.expect("seed customer_pool + cogs");
}

async fn call_memo(
    pool: &PgPool, customer_id: &str, currency: &str, lines: serde_json::Value,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_customer_credit_memo($1::UUID, $2, $3::JSONB, '2026-04-25'::DATE,
                                            $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(customer_id).bind(currency).bind(lines)
    .bind(&posted_by).bind(&key)
    .fetch_one(pool).await
}

#[tokio::test]
async fn financial_goodwill_credit_drains_ar() {
    // Customer has ar=500 (some prior invoice). Issue a $50 goodwill credit.
    // ar should drain by 50; revenue debited by 50.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "GOODWILL").await;
    seed_ar_balance(&pool, &s, 500).await;

    assert_eq!(balance(&pool, s.cust_ar).await, 500);
    let revenue_before = balance(&pool, s.revenue_acct).await;

    let lines = json!([{
        "kind": "financial",
        "revenue_account_id": s.revenue_acct,
        "amount": 50
    }]);
    call_memo(&pool, &s.customer_id, "USD", lines).await.expect("memo");

    assert_eq!(balance(&pool, s.cust_ar).await, 450);
    assert_eq!(balance(&pool, s.revenue_acct).await - revenue_before, 50);

    assert_invariants_hold(&pool, "financial_goodwill_credit_drains_ar").await;
}

#[tokio::test]
async fn financial_credit_with_tax_drains_ar() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "TAX").await;
    seed_ar_balance(&pool, &s, 500).await;
    // Pre-seed tax payable with credit balance (mimics prior tax-bearing
    // invoice). The memo's tax-leg will debit it back down.
    seed_sales_tax(&pool, &s, 8).await;
    // ar now = 500 + 8 = 508 (the tax seed also debited ar).
    assert_eq!(balance(&pool, s.cust_ar).await, 508);

    let lines = json!([{
        "kind": "financial",
        "revenue_account_id": s.revenue_acct,
        "amount": 100,
        "tax_amount": 8
    }]);
    call_memo(&pool, &s.customer_id, "USD", lines).await.expect("memo");

    // Total ar credit = 100 + 8 = 108. ar = 508 - 108 = 400.
    assert_eq!(balance(&pool, s.cust_ar).await, 400);
}

#[tokio::test]
async fn standalone_goods_return_restock() {
    // Customer returns 5 units of an old SKU; we restock and credit ar.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "GOODS-RESTOCK").await;
    seed_ar_balance(&pool, &s, 1000).await;
    seed_customer_pool(&pool, &s, 5).await;

    let lines = json!([{
        "kind": "goods_return",
        "sku_id": s.sku_id,
        "location_id": s.loc_id,
        "qty": 5,
        "unit_cost": 60,
        "unit_price": 100,
        "disposition": "restock",
        "amount": 500
    }]);
    call_memo(&pool, &s.customer_id, "USD", lines).await.expect("memo");

    // qty=+5 to stock_available; value=+300 to inv_value_fg; ar drained 500.
    assert_eq!(balance(&pool, s.qty_acct).await, 5);
    assert_eq!(balance(&pool, s.val_acct).await, 300);
    assert_eq!(balance(&pool, s.cust_ar).await, 500);

    assert_invariants_hold(&pool, "standalone_goods_return_restock").await;
}

#[tokio::test]
async fn standalone_goods_return_scrap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "GOODS-SCRAP").await;
    seed_ar_balance(&pool, &s, 1000).await;
    seed_customer_pool(&pool, &s, 5).await;
    let var_scrap_before = balance(&pool, s.var_scrap_acct).await;

    let lines = json!([{
        "kind": "goods_return",
        "sku_id": s.sku_id, "location_id": s.loc_id,
        "qty": 5, "unit_cost": 60, "unit_price": 100,
        "disposition": "scrap", "amount": 500
    }]);
    call_memo(&pool, &s.customer_id, "USD", lines).await.expect("memo");

    assert_eq!(balance(&pool, s.stock_scrap_acct).await, 5);
    assert_eq!(balance(&pool, s.var_scrap_acct).await - var_scrap_before, 300);
    assert_eq!(balance(&pool, s.cust_ar).await, 500);
}

#[tokio::test]
async fn standalone_goods_return_repair() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "GOODS-REPAIR").await;
    seed_ar_balance(&pool, &s, 1000).await;
    seed_customer_pool(&pool, &s, 5).await;

    let lines = json!([{
        "kind": "goods_return",
        "sku_id": s.sku_id, "location_id": s.loc_id,
        "qty": 5, "unit_cost": 60, "unit_price": 100,
        "disposition": "repair", "amount": 500
    }]);
    call_memo(&pool, &s.customer_id, "USD", lines).await.expect("memo");

    assert_eq!(balance(&pool, s.stock_quarantine_acct).await, 5);
    assert_eq!(balance(&pool, s.val_acct).await, 300);
    assert_eq!(balance(&pool, s.cust_ar).await, 500);
}

#[tokio::test]
async fn mixed_memo_financial_plus_goods() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "MIXED").await;
    seed_ar_balance(&pool, &s, 1000).await;
    seed_customer_pool(&pool, &s, 3).await;

    let lines = json!([
        {"kind": "financial", "revenue_account_id": s.revenue_acct, "amount": 50},
        {"kind": "goods_return", "sku_id": s.sku_id, "location_id": s.loc_id,
         "qty": 3, "unit_cost": 60, "unit_price": 100, "disposition": "restock",
         "amount": 300}
    ]);
    call_memo(&pool, &s.customer_id, "USD", lines).await.expect("memo");

    // ar drained by 50 + 300 = 350.
    assert_eq!(balance(&pool, s.cust_ar).await, 1000 - 350);
    assert_eq!(balance(&pool, s.qty_acct).await, 3);
    assert_eq!(balance(&pool, s.val_acct).await, 180);
}

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "IDEMP").await;
    seed_ar_balance(&pool, &s, 500).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "kind": "financial", "revenue_account_id": s.revenue_acct, "amount": 50
    }]);

    let id1: String = sqlx::query_scalar(
        "SELECT post_customer_credit_memo($1::UUID, 'USD', $2::JSONB,
                                            '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id).bind(lines.clone()).bind(&posted_by).bind(&key)
    .fetch_one(&pool).await.expect("memo-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_customer_credit_memo($1::UUID, 'USD', $2::JSONB,
                                            '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id).bind(lines).bind(&posted_by).bind(&key)
    .fetch_one(&pool).await.expect("memo-2 replay");

    assert_eq!(id1, id2);
    assert_eq!(balance(&pool, s.cust_ar).await, 450);
}

#[tokio::test]
async fn unknown_customer_raises_p0048() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    let lines = json!([{
        "kind": "financial", "revenue_account_id": 999, "amount": 1
    }]);
    expect_sqlstate("P0048", || async {
        call_memo(&pool, &bogus, "USD", lines.clone()).await
    }).await;
}

#[tokio::test]
async fn empty_lines_raises_p0048() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "EMPTY").await;
    seed_ar_balance(&pool, &s, 100).await;
    expect_sqlstate("P0048", || async {
        call_memo(&pool, &s.customer_id, "USD", json!([])).await
    }).await;
}

#[tokio::test]
async fn unknown_kind_raises_p0048() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "BAD-KIND").await;
    seed_ar_balance(&pool, &s, 100).await;
    let lines = json!([{"kind": "weird", "amount": 50}]);
    expect_sqlstate("P0048", || async {
        call_memo(&pool, &s.customer_id, "USD", lines.clone()).await
    }).await;
}

#[tokio::test]
async fn financial_with_closed_account_raises_p0048() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "CLOSED").await;
    seed_ar_balance(&pool, &s, 100).await;
    sqlx::query("UPDATE accounts SET is_closed = TRUE WHERE id = $1")
        .bind(s.revenue_acct).execute(&pool).await.expect("close");
    let lines = json!([{
        "kind": "financial", "revenue_account_id": s.revenue_acct, "amount": 50
    }]);
    expect_sqlstate("P0048", || async {
        call_memo(&pool, &s.customer_id, "USD", lines.clone()).await
    }).await;
}

#[tokio::test]
async fn goods_return_amount_mismatch_raises_p0048() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "AMOUNT-MISMATCH").await;
    seed_ar_balance(&pool, &s, 1000).await;

    let lines = json!([{
        "kind": "goods_return", "sku_id": s.sku_id, "location_id": s.loc_id,
        "qty": 5, "unit_cost": 60, "unit_price": 100, "disposition": "restock",
        "amount": 999
    }]);
    expect_sqlstate("P0048", || async {
        call_memo(&pool, &s.customer_id, "USD", lines.clone()).await
    }).await;
}
