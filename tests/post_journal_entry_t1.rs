//! acct-wb75.4.1 — Phase F1: post_journal_entry integration matrix.
//!
//! Coverage:
//!   * Happy path: single-line cash → revenue.
//!   * Multi-line entry: balanced 2+ lines.
//!   * Header + line tables persisted, posting_lines emitted, account
//!     balances move correctly.
//!   * Idempotent replay returns same doc id, no duplicate posting_lines.
//!   * Reject empty array (P0042).
//!   * Reject zero / negative amount (P0042).
//!   * Reject same DR/CR within a line (P0042).
//!   * Reject SKU-bearing account (P0042).
//!   * Reject qty-ledger account (P0042).
//!   * Reject cross-currency leg (P0042).
//!   * Reject unknown account id (P0042).
//!   * Closed-period gating bubbles up from post_posting_lines (P0005).

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

async fn call_je(
    pool: &PgPool,
    lines: serde_json::Value,
    business_date: &str,
    idempotency_key: &str,
    memo: Option<&str>,
) -> sqlx::Result<String> {
    sqlx::query_scalar(
        "SELECT post_journal_entry($1, $2::DATE, $3::UUID, $4::UUID, $5)::TEXT",
    )
    .bind(lines)
    .bind(business_date)
    .bind(ACTOR)
    .bind(idempotency_key)
    .bind(memo)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn happy_path_single_line_cash_to_revenue() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;


    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let (cash_d0, cash_c0) = account_balance(&pool, cash).await;
    let (rev_d0, rev_c0) = account_balance(&pool, rev).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": 12345, "description": "misc revenue"},
    ]);
    let doc = call_je(&pool, lines, "2026-04-15", &key, Some("ad-hoc revenue"))
        .await
        .expect("happy path");
    assert!(!doc.is_empty());

    let (cash_d1, cash_c1) = account_balance(&pool, cash).await;
    let (rev_d1, rev_c1) = account_balance(&pool, rev).await;
    assert_eq!(cash_d1 - cash_d0, 12345);
    assert_eq!(cash_c1 - cash_c0, 0);
    assert_eq!(rev_c1 - rev_c0, 12345);
    assert_eq!(rev_d1 - rev_d0, 0);

    // Header + line persisted.
    let memo: Option<String> =
        sqlx::query_scalar("SELECT memo FROM journal_entries WHERE id = $1::UUID")
            .bind(&doc)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(memo.as_deref(), Some("ad-hoc revenue"));

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM journal_entry_lines WHERE journal_entry_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 1);

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID AND reason = 'journal_entry'",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 1);

    assert_invariants_hold(&pool, "happy_path_single_line").await;
}

#[tokio::test]
async fn multi_line_balanced_entry() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;


    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let cogs = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let labor_applied = account_id_by_kind_currency(&pool, "labor_applied", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": 10000},
        {"debit_account_id": cogs, "credit_account_id": labor_applied, "amount": 4000},
    ]);
    let doc = call_je(&pool, lines, "2026-04-15", &key, None)
        .await
        .expect("multi-line ok");

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM journal_entry_lines WHERE journal_entry_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 2);

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 2);

    assert_invariants_hold(&pool, "multi_line_balanced").await;
}

#[tokio::test]
async fn idempotent_replay_returns_same_doc() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;


    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": 5000}
    ]);
    let doc1 = call_je(&pool, lines.clone(), "2026-04-15", &key, None)
        .await
        .unwrap();
    let doc2 = call_je(&pool, lines, "2026-04-15", &key, None).await.unwrap();
    assert_eq!(doc1, doc2);

    let n_lines: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM journal_entry_lines WHERE journal_entry_id = $1::UUID",
    )
    .bind(&doc1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lines, 1, "no duplicate lines on replay");

    let n_postings: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines WHERE document_id = $1::UUID",
    )
    .bind(&doc1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_postings, 1, "no duplicate postings on replay");
}

#[tokio::test]
async fn reject_empty_array() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0042", || async {
        call_je(&pool, json!([]), "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_null_lines() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0042", || async {
        call_je(&pool, json!(null), "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_zero_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": 0}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_negative_amount() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": -100}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_same_dr_cr() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": cash, "amount": 100}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_qty_ledger_account() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": void_qty, "credit_account_id": cash, "amount": 100}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_sku_bearing_account() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;


    // Find any inv_value_raw account from the fixture.
    let sku_acct: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'inv_value_raw' AND sku_id IS NOT NULL LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("seed has inv_value_raw");
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": sku_acct, "credit_account_id": cash, "amount": 100}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_cross_currency_leg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev_eur = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash_usd, "credit_account_id": rev_eur, "amount": 100}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn reject_unknown_account() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": 9_999_999, "credit_account_id": cash, "amount": 100}
    ]);
    expect_sqlstate("P0042", || async {
        call_je(&pool, lines, "2026-04-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn closed_period_blocks() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;



    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": 100}
    ]);
    // post_posting_lines raises P0005 on closed-period (period_locked).
    expect_sqlstate("P0005", || async {
        call_je(&pool, lines, "2026-03-15", &key, None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn no_movement_or_inventory_extension_written() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let rev = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"debit_account_id": cash, "credit_account_id": rev, "amount": 7777}
    ]);
    let doc = call_je(&pool, lines, "2026-04-15", &key, None).await.unwrap();

    // No inventory_movements row should be written for journal_entry.
    let n_im: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_movements im
         JOIN posting_lines pl ON pl.id = im.posting_line_id
         WHERE pl.document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_im, 0, "journal_entry must not write inventory_movements");

    // No posting_line_inventory row.
    let n_pli: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_line_inventory pli
         JOIN posting_lines pl ON pl.id = pli.posting_line_id
         WHERE pl.document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_pli, 0, "journal_entry must not write posting_line_inventory");

    // No posting_lines_provisional row.
    let n_prov: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM posting_lines_provisional plp
         JOIN posting_lines pl ON pl.id = plp.posting_line_id
         WHERE pl.document_id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_prov, 0, "journal_entry must not flag posting_lines_provisional");
}
