//! `acct-b6e` — post_vendor_debit_memo / standalone AP debit memo.
//!
//! Covers:
//!   * pure financial debit (e.g. disputed invoice, vendor allowance)
//!   * standalone goods return (no recv_line ref)
//!   * mixed memo
//!   * idempotency replay
//!   * validation: P0049 paths

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

#[allow(dead_code)]
struct Scaffold {
    vendor_id: String,
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_ap: i64,
    expense_acct: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
}

async fn fresh_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Ven {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("vendor {code}: {e}"))
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

async fn scaffold(pool: &PgPool, suffix: &str) -> Scaffold {
    let vendor_id = fresh_vendor(pool, &format!("VEN-MEMO-{suffix}")).await;
    let sku_id = fresh_sku(pool, &format!("SKU-VMEMO-{suffix}")).await;
    let loc_id = fresh_location(pool, &format!("VMEMO-{suffix}")).await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let ven_qty = open_account(
        pool, "vendor_pool", "qty", None, None, None, Some(&vendor_id), "credit",
    )
    .await;
    let ven_ap = open_account(
        pool, "ap", "value", Some("USD"), None, None, Some(&vendor_id), "credit",
    )
    .await;

    // Use an existing fixture-seeded debit-normal account as the "expense"
    // for financial memos; cogs USD has the right shape (debit-normal,
    // value, USD, NULL counterparty).
    let expense_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    Scaffold {
        vendor_id, sku_id, loc_id, qty_acct, val_acct,
        ven_qty, ven_ap, expense_acct, creation_void_qty, creation_void_val,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id).fetch_one(pool).await.expect("balance")
}

/// Mimic prior ap_bill: expense DR / ap CR. Builds both ap's credit
/// balance (so memo can debit it) and expense's debit balance (so
/// memo can credit expense without pushing it negative).
async fn seed_ap_balance(pool: &PgPool, s: &Scaffold, amount: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([{
        "reason":"ap_bill","document_kind":"seed","document_id":doc_id,
        "debit_account_id":s.expense_acct,"credit_account_id":s.ven_ap,
        "amount":amount,"business_date":"2026-04-15",
        "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by
    }]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)").bind(mint)
        .execute(pool).await.expect("seed ap");
}

/// Mimic prior po_receipt: vendor_pool CR / stock_available DR (qty)
/// + inv_value_raw DR / ap CR (value, additional). Adds qty to
/// stock_available and a credit to vendor_pool (debit-normal so the
/// memo's vendor_pool DR doesn't trip).
async fn seed_prior_receipt(pool: &PgPool, s: &Scaffold, qty: i64, unit_cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        // qty leg: stock_available DR / vendor_pool CR
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":doc_id,
         "debit_account_id":s.qty_acct,"credit_account_id":s.ven_qty,
         "amount":qty,"qty":qty,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        // value leg: inv_value_raw DR / ap CR (additional ap balance)
        {"reason":"ap_bill","document_kind":"seed","document_id":doc_id,
         "debit_account_id":s.val_acct,"credit_account_id":s.ven_ap,
         "amount":qty * unit_cost,"qty":qty,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by}
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)").bind(mint)
        .execute(pool).await.expect("seed prior receipt");
}

async fn call_memo(
    pool: &PgPool, vendor_id: &str, currency: &str, lines: serde_json::Value,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_vendor_debit_memo($1::UUID, $2, $3::JSONB, '2026-04-25'::DATE,
                                         $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(vendor_id).bind(currency).bind(lines)
    .bind(&posted_by).bind(&key)
    .fetch_one(pool).await
}

#[tokio::test]
async fn financial_disputed_invoice_drains_ap() {
    // Vendor billed us $200 we shouldn't have been billed; debit memo
    // reduces ap by $200. Net: ap drains by $200, expense_acct credits
    // by $200 (we're reversing the expense recognition).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "DISPUTE").await;
    seed_ap_balance(&pool, &s, 1000).await;

    assert_eq!(balance(&pool, s.ven_ap).await, -1000);
    let exp_before = balance(&pool, s.expense_acct).await;

    let lines = json!([{
        "kind": "financial",
        "expense_account_id": s.expense_acct,
        "amount": 200
    }]);
    call_memo(&pool, &s.vendor_id, "USD", lines).await.expect("memo");

    // ap drained by 200 (now -800); expense reduced by 200.
    assert_eq!(balance(&pool, s.ven_ap).await, -800);
    assert_eq!(balance(&pool, s.expense_acct).await - exp_before, -200);

    assert_invariants_hold(&pool, "financial_disputed_invoice_drains_ap").await;
}

#[tokio::test]
async fn standalone_goods_return_drains_inventory_and_ap() {
    // Standalone vendor return: 5 units back to vendor, value drops out
    // of inv_value_raw, ap reduced.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "GOODS").await;
    seed_prior_receipt(&pool, &s, 5, 100).await;

    // Pre-state: stock_available=5, inv_value_raw=500, ap=-500.
    assert_eq!(balance(&pool, s.qty_acct).await, 5);
    assert_eq!(balance(&pool, s.val_acct).await, 500);
    assert_eq!(balance(&pool, s.ven_ap).await, -500);

    let lines = json!([{
        "kind": "goods_return",
        "sku_id": s.sku_id,
        "location_id": s.loc_id,
        "qty": 5,
        "unit_cost": 100,
        "amount": 500
    }]);
    call_memo(&pool, &s.vendor_id, "USD", lines).await.expect("memo");

    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);

    assert_invariants_hold(&pool, "standalone_goods_return_drains_inventory_and_ap").await;
}

#[tokio::test]
async fn mixed_memo_financial_plus_goods() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "MIXED").await;
    seed_ap_balance(&pool, &s, 1000).await;
    seed_prior_receipt(&pool, &s, 5, 100).await;

    // Pre-state: ap = -1000 (expense seed) + -500 (receipt seed) = -1500.
    assert_eq!(balance(&pool, s.ven_ap).await, -1500);

    let lines = json!([
        {"kind": "financial", "expense_account_id": s.expense_acct, "amount": 200},
        {"kind": "goods_return", "sku_id": s.sku_id, "location_id": s.loc_id,
         "qty": 3, "unit_cost": 100, "amount": 300}
    ]);
    call_memo(&pool, &s.vendor_id, "USD", lines).await.expect("memo");

    // ap drained by 200 + 300 = 500. ap = -1500 + 500 = -1000.
    assert_eq!(balance(&pool, s.ven_ap).await, -1000);
    assert_eq!(balance(&pool, s.qty_acct).await, 2);
    assert_eq!(balance(&pool, s.val_acct).await, 200);
}

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "IDEMP").await;
    seed_ap_balance(&pool, &s, 500).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "kind": "financial", "expense_account_id": s.expense_acct, "amount": 100
    }]);

    let id1: String = sqlx::query_scalar(
        "SELECT post_vendor_debit_memo($1::UUID, 'USD', $2::JSONB,
                                         '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.vendor_id).bind(lines.clone()).bind(&posted_by).bind(&key)
    .fetch_one(&pool).await.expect("memo-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_vendor_debit_memo($1::UUID, 'USD', $2::JSONB,
                                         '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.vendor_id).bind(lines).bind(&posted_by).bind(&key)
    .fetch_one(&pool).await.expect("memo-2 replay");

    assert_eq!(id1, id2);
    // Only one memo drained.
    assert_eq!(balance(&pool, s.ven_ap).await, -400);
}

#[tokio::test]
async fn unknown_vendor_raises_p0049() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    let lines = json!([{"kind": "financial", "expense_account_id": 999, "amount": 1}]);
    expect_sqlstate("P0049", || async {
        call_memo(&pool, &bogus, "USD", lines.clone()).await
    }).await;
}

#[tokio::test]
async fn empty_lines_raises_p0049() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "EMPTY").await;
    seed_ap_balance(&pool, &s, 100).await;
    expect_sqlstate("P0049", || async {
        call_memo(&pool, &s.vendor_id, "USD", json!([])).await
    }).await;
}

#[tokio::test]
async fn unknown_kind_raises_p0049() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "BAD-KIND").await;
    seed_ap_balance(&pool, &s, 100).await;
    let lines = json!([{"kind": "weird", "amount": 50}]);
    expect_sqlstate("P0049", || async {
        call_memo(&pool, &s.vendor_id, "USD", lines.clone()).await
    }).await;
}

#[tokio::test]
async fn financial_with_closed_expense_raises_p0049() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "CLOSED").await;
    seed_ap_balance(&pool, &s, 100).await;
    sqlx::query("UPDATE accounts SET is_closed = TRUE WHERE id = $1")
        .bind(s.expense_acct).execute(&pool).await.expect("close");
    let lines = json!([{
        "kind": "financial", "expense_account_id": s.expense_acct, "amount": 50
    }]);
    expect_sqlstate("P0049", || async {
        call_memo(&pool, &s.vendor_id, "USD", lines.clone()).await
    }).await;
}

#[tokio::test]
async fn goods_return_amount_mismatch_raises_p0049() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold(&pool, "AMOUNT").await;
    seed_prior_receipt(&pool, &s, 5, 100).await;

    let lines = json!([{
        "kind": "goods_return", "sku_id": s.sku_id, "location_id": s.loc_id,
        "qty": 5, "unit_cost": 100, "amount": 999
    }]);
    expect_sqlstate("P0049", || async {
        call_memo(&pool, &s.vendor_id, "USD", lines.clone()).await
    }).await;
}
