//! `acct-bvh` — post_ap_payment matrix.
//!
//! Symmetric to `tests/ar_payment.rs`. Coverage:
//!   * Happy path: ap debited, cash credited.
//!   * Validation: P0042 (amount <= 0, vendor missing).
//!   * P0010: vendor + currency without ap account; missing cash.
//!   * Partial payment leaves ap with remainder.
//!   * Multi-payment sequence drains ap to 0.
//!   * Idempotency: replay returns existing id.

mod common;

use common::*;
use sqlx::PgPool;

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vend {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert vendor: {e}"))
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

/// Open per-vendor ap (USD) and seed it with `seed_ap` credit (mirrors
/// what a po_receipt + ap_bill would do organically). Returns
/// `(vendor_id, vend_ap_id)`.
async fn scaffold_with_ap(pool: &PgPool, code: &str, seed_ap: i64) -> (String, i64) {
    let vend_id = fresh_vendor(pool, code, "USD").await;
    let vend_ap = open_account(
        pool, "ap", "value", Some("USD"), Some(&vend_id), "credit",
    )
    .await;
    if seed_ap > 0 {
        let vend_unsettled = open_account(
            pool, "ap_unsettled", "value", Some("USD"), Some(&vend_id), "credit",
        )
        .await;
        let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
        let cash = account_id_by_kind_currency(pool, "cash", Some("USD")).await;
        // Mirror of ar_payment's scaffold but reversed (ap is credit-
        // normal): seed ap_unsettled via creation_void → ap_unsettled
        // (cycle_count_adj), then clear ap_unsettled → ap (ap_bill).
        // Also mint cash so it can be drained by the payment without
        // tripping the debit-normal accounts_check constraint.
        let posted_by = fresh_uuid(pool).await;
        let mint = serde_json::json!([
            {"reason":"cycle_count_adj","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":void_val,"credit_account_id":vend_unsettled,
             "amount":seed_ap,"qty":seed_ap,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":vend_id},
            {"reason":"ap_bill","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":vend_unsettled,"credit_account_id":vend_ap,
             "amount":seed_ap,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by,
             "counterparty_id":vend_id},
            {"reason":"cycle_count_adj","document_kind":"seed",
             "document_id":fresh_uuid(pool).await,
             "debit_account_id":cash,"credit_account_id":void_val,
             "amount":seed_ap,
             "business_date":"2026-04-15",
             "idempotency_key":fresh_uuid(pool).await,
             "posted_by":posted_by},
        ]);
        sqlx::query("SELECT post_posting_lines($1, FALSE)")
            .bind(mint).execute(pool).await.expect("seed ap");
    }
    (vend_id, vend_ap)
}

async fn call_payment(
    pool: &PgPool,
    vendor_id: &str,
    currency: &str,
    amount: i64,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_ap_payment($1::UUID, $2::CHAR(3), $3::BIGINT,
                                 '2026-04-25'::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(vendor_id)
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
async fn happy_path_ap_debit_cash_credit() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vend_id, vend_ap) = scaffold_with_ap(&pool, "PAY-A", 1000).await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;

    let cash_before = balance(&pool, cash).await;

    let _id = call_payment(&pool, &vend_id, "USD", 600).await.expect("pay");

    // ap is credit-normal: positive balance is stored as (debits - credits) < 0.
    // After paying 600 the remaining liability is 400, so balance stays -400.
    assert_eq!(balance(&pool, vend_ap).await, -(1000 - 600));
    // cash is debit-normal: payment debits 600, but we are CREDITING here
    // (cash leaves), so the balance delta is -600.
    assert_eq!(balance(&pool, cash).await - cash_before, -600);

    assert_invariants_hold(&pool, "happy_path_ap_debit_cash_credit").await;
}

#[tokio::test]
async fn partial_then_full_drains_ap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vend_id, vend_ap) = scaffold_with_ap(&pool, "PAY-PART", 500).await;

    call_payment(&pool, &vend_id, "USD", 300).await.expect("pay-1");
    assert_eq!(balance(&pool, vend_ap).await, -200);

    call_payment(&pool, &vend_id, "USD", 200).await.expect("pay-2");
    assert_eq!(balance(&pool, vend_ap).await, 0);
}

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vend_id, vend_ap) = scaffold_with_ap(&pool, "PAY-IDEMP", 1000).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let id1: String = sqlx::query_scalar(
        "SELECT post_ap_payment($1::UUID, 'USD', 400, '2026-04-25', $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(&vend_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("pay-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_ap_payment($1::UUID, 'USD', 400, '2026-04-25', $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(&vend_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("pay-2 replay");

    assert_eq!(id1, id2);
    // ap drained only once.
    assert_eq!(balance(&pool, vend_ap).await, -600);
}

// ============================================================
// Validation gates
// ============================================================

#[tokio::test]
async fn amount_zero_raises_p0042() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vend_id, _) = scaffold_with_ap(&pool, "PAY-ZERO", 100).await;
    expect_sqlstate("P0042", || async {
        call_payment(&pool, &vend_id, "USD", 0).await
    })
    .await;
}

#[tokio::test]
async fn amount_negative_raises_p0042() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vend_id, _) = scaffold_with_ap(&pool, "PAY-NEG", 100).await;
    expect_sqlstate("P0042", || async {
        call_payment(&pool, &vend_id, "USD", -10).await
    })
    .await;
}

#[tokio::test]
async fn unknown_vendor_raises_p0042() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate("P0042", || async {
        call_payment(&pool, &bogus, "USD", 100).await
    })
    .await;
}

#[tokio::test]
async fn no_ap_account_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Vendor with NO ap account opened (skip scaffold helper).
    let vend_id = fresh_vendor(&pool, "PAY-NO-AP", "USD").await;
    expect_sqlstate("P0010", || async {
        call_payment(&pool, &vend_id, "USD", 100).await
    })
    .await;
}

#[tokio::test]
async fn no_cash_account_for_currency_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (vend_id, _) = scaffold_with_ap(&pool, "PAY-NO-CASH", 100).await;
    // Vendor's ap is USD; payment in GBP — no cash(GBP) seeded.
    expect_sqlstate("P0010", || async {
        call_payment(&pool, &vend_id, "GBP", 100).await
    })
    .await;
}
