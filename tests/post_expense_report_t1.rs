//! acct-wb75.4.3 — Phase F3: post_expense_report integration matrix.
//!
//! Coverage:
//!   * Happy path cash settlement: travel/meal expenses → cash.
//!   * Happy path deferred settlement: expenses → ap_employee (per-
//!     employee partitioned account, cleared later).
//!   * Multi-line report.
//!   * Idempotent replay returns same doc id; no duplicate posting_lines.
//!   * Reject empty array (P0046).
//!   * Reject zero / negative amount (P0046).
//!   * Reject SKU-bearing expense account (P0046).
//!   * Reject qty-ledger expense account (P0046).
//!   * Reject expense currency mismatch (P0046).
//!   * Reject expense_account == settlement_account (P0046).
//!   * Reject closed expense account (P0046).
//!   * Reject unknown expense account (P0046).
//!   * Reject employee not found (P0046).
//!   * Reject inactive employee (P0046).
//!   * Reject employee currency mismatch (P0046).
//!   * Reject settlement currency mismatch (P0046).
//!   * Reject SKU-bearing settlement account (P0046).
//!   * Reject qty-ledger settlement account (P0046).
//!   * Reject closed settlement account (P0046).
//!   * Reject unknown settlement account (P0046).
//!   * Reject bad p_currency (P0046).
//!   * Closed-period gating bubbles up from post_posting_lines (P0005).
//!   * No movement / inventory extension / provisional rows produced.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

const ACTOR: &str = "00000000-0000-0000-0000-000000000010";

// ============================================================
// Helpers
// ============================================================

async fn account_balance(pool: &PgPool, id: i64) -> (i64, i64) {
    sqlx::query_as("SELECT debits_total, credits_total FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("account balance")
}

/// Pre-fund a debit-normal value account (e.g. `cash`) so a subsequent
/// credit doesn't violate `accounts_check`. Uses a journal_entry from
/// inv_adj_expense (unrestricted) → target.
async fn prefund(pool: &PgPool, target_account: i64, amount: i64) {
    let inv_adj = account_id_by_kind_currency(pool, "inv_adj_expense", Some("USD")).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([
        {"debit_account_id": target_account, "credit_account_id": inv_adj, "amount": amount}
    ]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_journal_entry($1, '2026-04-01'::DATE, $2::UUID, $3::UUID, NULL)::TEXT",
    )
    .bind(lines)
    .bind(ACTOR)
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("prefund");
}

async fn fresh_employee(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO employees (code, name, currency) VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Employee {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_employee_inactive(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO employees (code, name, currency, is_active)
         VALUES ($1, $2, $3, FALSE) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Employee {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Open a per-employee `ap_employee` account (counterparty-partitioned).
async fn open_ap_employee(pool: &PgPool, employee_id: &str, currency: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_employee'::account_kind, 'value'::ledger_kind, $1, $2::UUID, 'credit'::balance_direction)
         RETURNING id",
    )
    .bind(currency)
    .bind(employee_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn call_er(
    pool: &PgPool,
    employee_id: &str,
    currency: &str,
    lines: serde_json::Value,
    settlement_account_id: i64,
    business_date: &str,
    idempotency_key: &str,
    report_number: Option<&str>,
    memo: Option<&str>,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_expense_report($1::UUID, $2::CHAR(3), $3, $4, $5::DATE, $6::UUID, $7::UUID, $8, $9)::TEXT",
    )
    .bind(employee_id)
    .bind(currency)
    .bind(lines)
    .bind(settlement_account_id)
    .bind(business_date)
    .bind(ACTOR)
    .bind(idempotency_key)
    .bind(report_number)
    .bind(memo)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn happy_path_cash_settlement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let emp = fresh_employee(&pool, "E-CASH", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let labor_exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    prefund(&pool, cash, 1_000_000).await;

    let (lab_d0, _) = account_balance(&pool, labor_exp).await;
    let (_, cash_c0) = account_balance(&pool, cash).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": labor_exp, "amount": 12_500, "description": "client lunch"}
    ]);
    let doc = call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, Some("ER-001"), Some("April travel"))
        .await
        .expect("happy path cash");
    assert!(!doc.is_empty());

    let (lab_d1, _) = account_balance(&pool, labor_exp).await;
    let (_, cash_c1) = account_balance(&pool, cash).await;
    assert_eq!(lab_d1 - lab_d0, 12_500);
    assert_eq!(cash_c1 - cash_c0, 12_500);

    let row: (Option<String>, Option<String>, i64) = sqlx::query_as(
        "SELECT report_number, memo, settlement_account_id FROM expense_reports WHERE id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0.as_deref(), Some("ER-001"));
    assert_eq!(row.1.as_deref(), Some("April travel"));
    assert_eq!(row.2, cash);

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM expense_report_lines WHERE expense_report_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 1);

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines
          WHERE document_id = $1::UUID AND reason = 'expense_report'",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 1);

    assert_invariants_hold(&pool, "happy_path_cash_settlement").await;
}

#[tokio::test]
async fn happy_path_deferred_ap_employee() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let emp = fresh_employee(&pool, "E-DEFER", "USD").await;
    let ap_emp = open_ap_employee(&pool, &emp, "USD").await;
    let labor_exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": labor_exp, "amount": 8_000}]);
    let _doc = call_er(&pool, &emp, "USD", lines, ap_emp, "2026-04-15", &key, None, None)
        .await
        .expect("deferred ok");

    let (_, ap_emp_c) = account_balance(&pool, ap_emp).await;
    assert_eq!(ap_emp_c, 8_000);

    assert_invariants_hold(&pool, "happy_path_deferred_ap_employee").await;
}

#[tokio::test]
async fn multi_line_cash_settlement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let emp = fresh_employee(&pool, "E-MULTI", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    prefund(&pool, cash, 1_000_000).await;
    let exp1 = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let exp2 = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let exp3 = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp1, "amount": 1_000, "description": "meals"},
        {"expense_account_id": exp2, "amount": 2_500, "description": "supplies"},
        {"expense_account_id": exp3, "amount":   500, "description": "shipping"},
    ]);
    let doc = call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
        .await
        .expect("multi-line ok");

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM expense_report_lines WHERE expense_report_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 3);

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 3);

    let total_to_cash: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(amount),0)::BIGINT FROM posting_lines
          WHERE document_id = $1::UUID AND credit_account_id = $2",
    )
    .bind(&doc)
    .bind(cash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total_to_cash, 4_000);

    assert_invariants_hold(&pool, "multi_line_cash_settlement").await;
}

#[tokio::test]
async fn idempotent_replay_returns_same_doc() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let emp = fresh_employee(&pool, "E-IDEM", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    prefund(&pool, cash, 1_000_000).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 5_000}]);
    let d1 = call_er(&pool, &emp, "USD", lines.clone(), cash, "2026-04-15", &key, None, None).await.unwrap();
    let d2 = call_er(&pool, &emp, "USD", lines,         cash, "2026-04-15", &key, None, None).await.unwrap();
    assert_eq!(d1, d2);

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM expense_report_lines WHERE expense_report_id = $1::UUID",
    )
    .bind(&d1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 1);

    let np: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
    )
    .bind(&d1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(np, 1);
}

#[tokio::test]
async fn reject_empty_array() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-EMPTY", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", json!([]), cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_null_lines() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-NULL", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", json!(null), cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_zero_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-ZERO", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 0}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_negative_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-NEG", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": -100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_sku_bearing_expense() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-SKU", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let sku_acct: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'inv_value_raw' AND sku_id IS NOT NULL LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": sku_acct, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_qty_ledger_expense() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-QTY", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let qty: i64 = sqlx::query_scalar("SELECT id FROM accounts WHERE ledger_kind='qty' LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": qty, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_expense_currency_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-CCY", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp_eur = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp_eur, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_expense_equals_settlement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-EQ", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": cash, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_unknown_expense() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-UNK", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": 9_999_999, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_employee_not_found() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    let phony = "00000000-0000-0000-0000-deadbeefdead";
    expect_sqlstate("P0046", || async {
        call_er(&pool, phony, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_employee_inactive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee_inactive(&pool, "E-INACTIVE", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_employee_currency_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Employee functional ccy is EUR but report is USD.
    let emp = fresh_employee(&pool, "E-EUREMP", "EUR").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_settlement_currency_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-SCCY", "USD").await;
    // EUR cash account paired with a USD report.
    let cash_eur = account_id_by_kind_currency(&pool, "cash", Some("EUR")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, cash_eur, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_sku_bearing_settlement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-SSKU", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let sku_acct: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'inv_value_raw' AND sku_id IS NOT NULL LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, sku_acct, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_qty_ledger_settlement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-SQTY", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let qty: i64 = sqlx::query_scalar("SELECT id FROM accounts WHERE ledger_kind='qty' LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, qty, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_unknown_settlement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-SUNK", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "USD", lines, 9_999_999, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_bad_currency() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-BADCCY", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    // Two-char currency.
    expect_sqlstate("P0046", || async {
        call_er(&pool, &emp, "US", lines, cash, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn closed_period_blocks() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let emp = fresh_employee(&pool, "E-CLOSED", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0005", || async {
        call_er(&pool, &emp, "USD", lines, cash, "2026-03-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn no_movement_or_inventory_extension_written() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let emp = fresh_employee(&pool, "E-CLEAN", "USD").await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    prefund(&pool, cash, 1_000_000).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 7_000}]);
    let doc = call_er(&pool, &emp, "USD", lines, cash, "2026-04-15", &key, None, None)
        .await
        .unwrap();

    let n_im: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_movements im
          JOIN posting_lines pl ON pl.id = im.posting_line_id
         WHERE pl.document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_im, 0);

    let n_pli: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_line_inventory pli
          JOIN posting_lines pl ON pl.id = pli.posting_line_id
         WHERE pl.document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_pli, 0);

    let n_prov: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines_provisional plp
          JOIN posting_lines pl ON pl.id = plp.posting_line_id
         WHERE pl.document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_prov, 0);

    let n_with_qty: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines
          WHERE document_id = $1::UUID AND qty IS NOT NULL",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_with_qty, 0);

    assert_invariants_hold(&pool, "no_movement_or_inventory_extension_written").await;
}
