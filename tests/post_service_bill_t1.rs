//! acct-wb75.4.2 — Phase F2: post_service_bill integration matrix.
//!
//! Coverage:
//!   * Happy path: single-line vendor service bill (utility expense → vendor ap).
//!   * Multi-line bill (no tax).
//!   * With-tax line: expense + tax both DR vendor ap.
//!   * Idempotent replay returns same doc id; no duplicate posting_lines.
//!   * Reject empty array (P0045).
//!   * Reject zero / negative amount (P0045).
//!   * Reject negative tax_amount (P0045).
//!   * Reject tax_amount > 0 without tax_account_id (P0045).
//!   * Reject tax_account_id without tax_amount > 0 (P0045).
//!   * Reject SKU-bearing expense account (P0045).
//!   * Reject qty-ledger expense account (P0045).
//!   * Reject expense currency mismatch (P0045).
//!   * Reject expense_account == vendor_ap (P0045).
//!   * Reject tax_account == expense_account (P0045).
//!   * Reject tax_account == vendor_ap (P0045).
//!   * Reject closed expense account (P0045).
//!   * Reject unknown expense account (P0045).
//!   * Reject vendor not found (P0045).
//!   * Reject no open ap account for vendor/currency (P0045).
//!   * Reject bad p_currency (P0045).
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

/// Insert a vendor + per-currency `ap` account (counterparty-partitioned).
async fn fresh_vendor_with_ap(pool: &PgPool, code: &str, currency: &str) -> (String, i64) {
    let vendor: String = sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap();

    let ap: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap'::account_kind, 'value'::ledger_kind, $1, $2::UUID, 'credit'::balance_direction)
         RETURNING id",
    )
    .bind(currency)
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    (vendor, ap)
}

async fn call_sb(
    pool: &PgPool,
    vendor_id: &str,
    currency: &str,
    lines: serde_json::Value,
    business_date: &str,
    idempotency_key: &str,
    invoice_number: Option<&str>,
    memo: Option<&str>,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_service_bill($1::UUID, $2::CHAR(3), $3, $4::DATE, $5::UUID, $6::UUID, $7, $8)::TEXT",
    )
    .bind(vendor_id)
    .bind(currency)
    .bind(lines)
    .bind(business_date)
    .bind(ACTOR)
    .bind(idempotency_key)
    .bind(invoice_number)
    .bind(memo)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn happy_path_single_line() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (vendor, ap) = fresh_vendor_with_ap(&pool, "V-SB-1", "USD").await;
    let labor_exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    let (lab_d0, lab_c0) = account_balance(&pool, labor_exp).await;
    let (ap_d0, ap_c0) = account_balance(&pool, ap).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": labor_exp, "amount": 50_000, "description": "monthly utility"}
    ]);
    let doc = call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, Some("INV-001"), Some("April utility"))
        .await
        .expect("happy path");
    assert!(!doc.is_empty());

    let (lab_d1, lab_c1) = account_balance(&pool, labor_exp).await;
    let (ap_d1, ap_c1) = account_balance(&pool, ap).await;
    assert_eq!(lab_d1 - lab_d0, 50_000);
    assert_eq!(lab_c1 - lab_c0, 0);
    assert_eq!(ap_c1 - ap_c0, 50_000);
    assert_eq!(ap_d1 - ap_d0, 0);

    // Header persisted with invoice_number + memo.
    let row: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT invoice_number, memo FROM service_bills WHERE id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0.as_deref(), Some("INV-001"));
    assert_eq!(row.1.as_deref(), Some("April utility"));

    // Line + posting_lines counts.
    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM service_bill_lines WHERE service_bill_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 1);

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines
          WHERE document_id = $1::UUID AND reason = 'service_bill'",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 1);

    assert_invariants_hold(&pool, "happy_path_single_line").await;
}

#[tokio::test]
async fn multi_line_no_tax() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (vendor, ap) = fresh_vendor_with_ap(&pool, "V-SB-2", "USD").await;
    let labor_exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let cogs = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let disp = account_id_by_kind_currency(&pool, "disposal_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": labor_exp, "amount": 1_000},
        {"expense_account_id": cogs,      "amount": 2_000},
        {"expense_account_id": disp,      "amount": 3_000},
    ]);
    let doc = call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
        .await
        .expect("multi-line ok");

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM service_bill_lines WHERE service_bill_id = $1::UUID",
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

    let total_to_ap: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(amount),0)::BIGINT FROM posting_lines
          WHERE document_id = $1::UUID AND credit_account_id = $2",
    )
    .bind(&doc)
    .bind(ap)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total_to_ap, 6_000);

    assert_invariants_hold(&pool, "multi_line_no_tax").await;
}

#[tokio::test]
async fn line_with_tax() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (vendor, ap) = fresh_vendor_with_ap(&pool, "V-SB-3", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    // Use cogs as the input-tax debit account (same shape: value-ledger,
    // non-SKU, USD, debit-normal). The semantic taxonomy is not enforced
    // by post_service_bill; only currency / ledger_kind / sku_id are.
    let tax_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;

    let (exp_d0, _) = account_balance(&pool, exp).await;
    let (tax_d0, _) = account_balance(&pool, tax_acct).await;
    let (_, ap_c0) = account_balance(&pool, ap).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {
            "expense_account_id": exp,
            "amount": 10_000,
            "tax_account_id": tax_acct,
            "tax_amount": 800,
            "description": "consulting fee + tax"
        }
    ]);
    let doc = call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
        .await
        .expect("with tax ok");

    // Two postings: expense DR / ap CR + tax DR / ap CR.
    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 2);

    let (exp_d1, _) = account_balance(&pool, exp).await;
    let (tax_d1, _) = account_balance(&pool, tax_acct).await;
    let (_, ap_c1) = account_balance(&pool, ap).await;
    assert_eq!(exp_d1 - exp_d0, 10_000);
    assert_eq!(tax_d1 - tax_d0, 800);
    assert_eq!(ap_c1 - ap_c0, 10_800);

    // service_bill_lines has 1 row carrying both amounts.
    let row: (i64, Option<i64>, i64) = sqlx::query_as(
        "SELECT amount, tax_account_id, tax_amount
           FROM service_bill_lines WHERE service_bill_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 10_000);
    assert_eq!(row.1, Some(tax_acct));
    assert_eq!(row.2, 800);

    assert_invariants_hold(&pool, "line_with_tax").await;
}

#[tokio::test]
async fn idempotent_replay_returns_same_doc() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-4", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 5_000}]);
    let d1 = call_sb(&pool, &vendor, "USD", lines.clone(), "2026-04-15", &key, None, None).await.unwrap();
    let d2 = call_sb(&pool, &vendor, "USD", lines,         "2026-04-15", &key, None, None).await.unwrap();
    assert_eq!(d1, d2);

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM service_bill_lines WHERE service_bill_id = $1::UUID",
    )
    .bind(&d1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 1, "no duplicate lines on replay");

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
    )
    .bind(&d1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 1, "no duplicate postings on replay");
}

#[tokio::test]
async fn reject_empty_array() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-EMPTY", "USD").await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", json!([]), "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_null_lines() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-NULL", "USD").await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", json!(null), "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_zero_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-ZERO", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 0}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_negative_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-NEG", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": -100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_negative_tax_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-NEGTAX", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let tax = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp, "amount": 100, "tax_account_id": tax, "tax_amount": -1}
    ]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_tax_amount_without_account() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-TAX1", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp, "amount": 100, "tax_amount": 50}
    ]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_tax_account_without_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-TAX2", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let tax = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp, "amount": 100, "tax_account_id": tax, "tax_amount": 0}
    ]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_sku_bearing_expense() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-SKU", "USD").await;
    let sku_acct: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'inv_value_raw' AND sku_id IS NOT NULL LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("seed has inv_value_raw");
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": sku_acct, "amount": 100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_qty_ledger_expense() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-QTY", "USD").await;
    let qty: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE ledger_kind = 'qty' LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": qty, "amount": 100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_currency_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-CCY", "USD").await;
    // Bill currency USD but expense in EUR.
    let exp_eur = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp_eur, "amount": 100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_expense_equals_vendor_ap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, ap) = fresh_vendor_with_ap(&pool, "V-SB-EQ", "USD").await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": ap, "amount": 100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_tax_equals_expense() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-TXEXP", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp, "amount": 100, "tax_account_id": exp, "tax_amount": 10}
    ]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_tax_equals_vendor_ap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, ap) = fresh_vendor_with_ap(&pool, "V-SB-TXAP", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp, "amount": 100, "tax_account_id": ap, "tax_amount": 10}
    ]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_unknown_expense_account() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-UNK", "USD").await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": 9_999_999, "amount": 100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_vendor_not_found() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    let phony = "00000000-0000-0000-0000-deadbeefdead";
    expect_sqlstate("P0045", || async {
        call_sb(&pool, phony, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_no_open_ap_account() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Vendor created without per-currency ap account.
    let vendor: String = sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ('V-SB-NOAP','Vendor V-SB-NOAP','USD') RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_bad_currency() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-BADCCY", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    // Two-char currency — fails the length(p_currency) <> 3 gate.
    expect_sqlstate("P0045", || async {
        call_sb(&pool, &vendor, "US", lines, "2026-04-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn closed_period_blocks() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-CLOSED", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"expense_account_id": exp, "amount": 100}]);
    // 2026-03 is closed in the fixture; post_posting_lines raises P0005.
    expect_sqlstate("P0005", || async {
        call_sb(&pool, &vendor, "USD", lines, "2026-03-15", &key, None, None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn no_movement_or_inventory_extension_written() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let (vendor, _) = fresh_vendor_with_ap(&pool, "V-SB-CLEAN", "USD").await;
    let exp = account_id_by_kind_currency(&pool, "labor_expense", Some("USD")).await;
    let tax = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"expense_account_id": exp, "amount": 7_000, "tax_account_id": tax, "tax_amount": 500}
    ]);
    let doc = call_sb(&pool, &vendor, "USD", lines, "2026-04-15", &key, None, None)
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

    // qty must be NULL on every service_bill posting_line (value-only).
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
