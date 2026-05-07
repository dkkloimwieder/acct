//! `acct-th7` / Slice C — post_ar_payment matrix.
//!
//! Coverage:
//!   * Happy path: cash debited, ar credited.
//!   * Validation: P0039 (amount <= 0, customer missing).
//!   * P0010: customer + currency without ar account.
//!   * Partial payment leaves ar with remainder.
//!   * Multi-payment sequence drains ar to 0.
//!   * Idempotency: replay returns existing id.

mod common;

use common::*;
use sqlx::PgPool;

async fn fresh_customer(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert customer: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open_account {kind}: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Open per-customer ar (USD) and seed it with `seed_ar` debit (mirrors
/// what a customer_invoice would do). Returns (customer_id, cust_ar_id).
async fn scaffold_with_ar(pool: &PgPool, code: &str, seed_ar: i64) -> (String, i64) {
    let cust_id = fresh_customer(pool, code, "USD").await;
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"), Some(&cust_id), "debit",
    )
    .await;
    if seed_ar > 0 {
        let cust_unsettled = open_account(
            pool, "ar_unsettled", "value", Some("USD"), Some(&cust_id), "debit",
        )
        .await;
        let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
        // Seed ar_unsettled (debit) via cycle_count_adj + then post a
        // synthetic ar_invoice to migrate ar_unsettled → ar.
        let posted_by = fresh_uuid(pool).await;
        let mint = serde_json::json!([
            {"reason":"cycle_count_adj","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":cust_unsettled,"credit_account_id":void_val,
             "amount":seed_ar,"qty":seed_ar,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":cust_id},
            {"reason":"ar_invoice","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":cust_ar,"credit_account_id":cust_unsettled,
             "amount":seed_ar,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":cust_id},
        ]);
        sqlx::query("SELECT post_posting_lines($1, FALSE)")
            .bind(mint).execute(pool).await.expect("seed ar");
    }
    (cust_id, cust_ar)
}

async fn call_payment(
    pool: &PgPool,
    customer_id: &str,
    currency: &str,
    amount: i64,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_ar_payment($1::UUID, $2::CHAR(3), $3::BIGINT,
                                 '2026-04-25'::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(customer_id)
    .bind(currency)
    .bind(amount)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Happy paths
// ============================================================

#[tokio::test]
async fn happy_path_cash_debit_ar_credit() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (cust_id, cust_ar) = scaffold_with_ar(&pool, "PAY-A", 1000).await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;

    let cash_before = balance(&pool, cash).await;

    let _id = call_payment(&pool, &cust_id, "USD", 600).await.expect("pay");

    assert_eq!(balance(&pool, cust_ar).await, 1000 - 600);
    assert_eq!(balance(&pool, cash).await - cash_before, 600);

    assert_invariants_hold(&pool, "happy_path_cash_debit_ar_credit").await;
}

#[tokio::test]
async fn partial_then_full_drains_ar() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (cust_id, cust_ar) = scaffold_with_ar(&pool, "PAY-PART", 500).await;

    call_payment(&pool, &cust_id, "USD", 300).await.expect("pay-1");
    assert_eq!(balance(&pool, cust_ar).await, 200);

    call_payment(&pool, &cust_id, "USD", 200).await.expect("pay-2");
    assert_eq!(balance(&pool, cust_ar).await, 0);
}

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (cust_id, cust_ar) = scaffold_with_ar(&pool, "PAY-IDEMP", 1000).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let id1: String = sqlx::query_scalar(
        "SELECT post_ar_payment($1::UUID, 'USD', 400, '2026-04-25', $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(&cust_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("pay-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_ar_payment($1::UUID, 'USD', 400, '2026-04-25', $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(&cust_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("pay-2 replay");

    assert_eq!(id1, id2);
    // ar drained only once.
    assert_eq!(balance(&pool, cust_ar).await, 600);
}

// ============================================================
// Validation gates
// ============================================================

#[tokio::test]
async fn amount_zero_raises_p0039() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (cust_id, _) = scaffold_with_ar(&pool, "PAY-ZERO", 100).await;
    expect_sqlstate("P0039", || async {
        call_payment(&pool, &cust_id, "USD", 0).await
    })
    .await;
}

#[tokio::test]
async fn amount_negative_raises_p0039() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (cust_id, _) = scaffold_with_ar(&pool, "PAY-NEG", 100).await;
    expect_sqlstate("P0039", || async {
        call_payment(&pool, &cust_id, "USD", -10).await
    })
    .await;
}

#[tokio::test]
async fn unknown_customer_raises_p0039() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate("P0039", || async {
        call_payment(&pool, &bogus, "USD", 100).await
    })
    .await;
}

#[tokio::test]
async fn no_ar_account_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Customer with NO ar account opened (skip scaffold helper).
    let cust_id = fresh_customer(&pool, "PAY-NO-AR", "USD").await;
    expect_sqlstate("P0010", || async {
        call_payment(&pool, &cust_id, "USD", 100).await
    })
    .await;
}

#[tokio::test]
async fn no_cash_account_for_currency_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (cust_id, _) = scaffold_with_ar(&pool, "PAY-NO-CASH", 100).await;
    // Customer's ar is USD; payment in GBP — no cash(GBP) seeded.
    expect_sqlstate("P0010", || async {
        call_payment(&pool, &cust_id, "GBP", 100).await
    })
    .await;
}
