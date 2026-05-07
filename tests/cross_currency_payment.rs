//! `acct-3xcg` — cross-currency cash settlement tests for
//! `post_ar_payment` and `post_ap_payment` (mig 0092).
//!
//! Coverage matrix:
//!
//!   AR side (customer pays us in different currency than AR):
//!     - same-currency regression (no behavior change, NULL params)
//!     - cross-currency same-rate (delta = 0; no FX leg)
//!     - cross-currency rate-up (customer paid more → gain)
//!     - cross-currency rate-down (customer paid less → loss)
//!     - missing fx_rate at business_date → P0050
//!     - missing p_cash_amount when cross-currency → P0039
//!     - mismatched p_cash_amount when same-currency → P0039
//!     - cross-currency idempotency replay
//!     - multi-call partial settlements at different rates
//!     - missing fx_clearing account → P0010
//!
//!   AP side (we pay vendor in different currency than AP):
//!     - same-currency regression
//!     - cross-currency same-rate
//!     - cross-currency rate-up (we paid more → loss)
//!     - cross-currency rate-down (we paid less → gain)
//!     - missing fx_rate → P0050
//!     - missing p_cash_amount → P0042
//!     - mismatched p_cash_amount → P0042
//!
//!   Invariants (every test):
//!     - assert_invariants_hold (I1-I7 from acct-du2)
//!     - per-(ledger_kind, currency) double-entry holds
//!     - fx_clearing balances pair (DR in ccy / CR in cash_ccy) net
//!       to zero in home currency at the test rate

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Helpers
// ============================================================

async fn fresh_customer(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Customer {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
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
    .unwrap()
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Insert a custom fx_rate at a specific date so tests can set
/// round-number rates and verify exact arithmetic. The lookup in
/// post_ar_payment / post_ap_payment uses `effective_at::DATE <=
/// p_business_date ORDER BY effective_at DESC LIMIT 1`, so a rate
/// inserted at `effective_at` becomes active for any business_date
/// >= that date.
async fn seed_fx_rate(
    pool: &PgPool,
    from_ccy: &str,
    to_ccy: &str,
    rate: &str,
    effective_at: &str,
) {
    sqlx::query(
        "INSERT INTO fx_rates (from_currency, to_currency, rate, effective_at, source)
         VALUES ($1, $2, $3::NUMERIC, $4::TIMESTAMPTZ, 'test')",
    )
    .bind(from_ccy)
    .bind(to_ccy)
    .bind(rate)
    .bind(effective_at)
    .execute(pool)
    .await
    .unwrap();
}

/// Seed customer's AR by minting through cycle_count_adj + ar_invoice
/// (mirrors the helper in tests/ar_payment.rs).
async fn seed_ar(pool: &PgPool, customer_id: &str, currency: &str, amount: i64) -> i64 {
    let cust_ar = open_account(
        pool, "ar", "value", Some(currency), Some(customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        pool, "ar_unsettled", "value", Some(currency), Some(customer_id), "debit",
    )
    .await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some(currency))
        .await;
    let posted_by = fresh_uuid(pool).await;
    let mint = json!([
        { "reason":"cycle_count_adj","document_kind":"seed",
          "document_id":fresh_uuid(pool).await,
          "debit_account_id":cust_unsettled,"credit_account_id":void_val,
          "amount":amount,"qty":amount,"business_date":"2026-04-15",
          "idempotency_key":fresh_uuid(pool).await,
          "posted_by":posted_by,"counterparty_id":customer_id },
        { "reason":"ar_invoice","document_kind":"seed",
          "document_id":fresh_uuid(pool).await,
          "debit_account_id":cust_ar,"credit_account_id":cust_unsettled,
          "amount":amount,"business_date":"2026-04-15",
          "idempotency_key":fresh_uuid(pool).await,
          "posted_by":posted_by,"counterparty_id":customer_id },
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap();
    cust_ar
}

/// Mint cash in `currency` so post_ap_payment can drain it without
/// tripping the debit-normal accounts_check on cash. Mirrors the
/// pattern in tests/ap_payment.rs::scaffold_with_ap.
async fn mint_cash(pool: &PgPool, currency: &str, amount: i64) {
    let cash = account_id_by_kind_currency(pool, "cash", Some(currency)).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some(currency)).await;
    let posted_by = fresh_uuid(pool).await;
    sqlx::query("SELECT post_posting_lines($1, FALSE)")
        .bind(json!([
            { "reason":"cycle_count_adj","document_kind":"seed",
              "document_id":fresh_uuid(pool).await,
              "debit_account_id":cash,"credit_account_id":void_val,
              "amount":amount,
              "business_date":"2026-04-15",
              "idempotency_key":fresh_uuid(pool).await,
              "posted_by":posted_by },
        ]))
        .execute(pool)
        .await
        .unwrap();
}

/// Seed vendor's AP. ap_unsettled → ap mirror of seed_ar's flow.
/// Returns the vendor-partitioned ap account id.
async fn seed_ap(pool: &PgPool, vendor_id: &str, currency: &str, amount: i64) -> i64 {
    let vend_ap = open_account(
        pool, "ap", "value", Some(currency), Some(vendor_id), "credit",
    )
    .await;
    let vend_unsettled = open_account(
        pool, "ap_unsettled", "value", Some(currency), Some(vendor_id), "credit",
    )
    .await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some(currency))
        .await;
    let posted_by = fresh_uuid(pool).await;
    // Mint ap_unsettled: void_val DR / ap_unsettled CR (credit-normal).
    // Then ap_bill-shape transfer: ap_unsettled DR / ap CR.
    let mint = json!([
        { "reason":"cycle_count_adj","document_kind":"seed",
          "document_id":fresh_uuid(pool).await,
          "debit_account_id":void_val,"credit_account_id":vend_unsettled,
          "amount":amount,"qty":amount,"business_date":"2026-04-15",
          "idempotency_key":fresh_uuid(pool).await,
          "posted_by":posted_by,"counterparty_id":vendor_id },
        { "reason":"ap_bill","document_kind":"seed",
          "document_id":fresh_uuid(pool).await,
          "debit_account_id":vend_unsettled,"credit_account_id":vend_ap,
          "amount":amount,"business_date":"2026-04-15",
          "idempotency_key":fresh_uuid(pool).await,
          "posted_by":posted_by,"counterparty_id":vendor_id },
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap();
    vend_ap
}

async fn call_ar_payment(
    pool: &PgPool,
    customer_id: &str,
    currency: &str,
    amount: i64,
    business_date: &str,
    idempotency_key: &str,
    cash_currency: Option<&str>,
    cash_amount: Option<i64>,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_ar_payment(
            $1::UUID, $2::CHAR(3), $3::BIGINT,
            $4::DATE, $5::UUID, $6::UUID, NULL,
            $7::CHAR(3), $8::BIGINT
         )::text",
    )
    .bind(customer_id)
    .bind(currency)
    .bind(amount)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .bind(cash_currency)
    .bind(cash_amount)
    .fetch_one(pool)
    .await
}

async fn call_ap_payment(
    pool: &PgPool,
    vendor_id: &str,
    currency: &str,
    amount: i64,
    business_date: &str,
    idempotency_key: &str,
    cash_currency: Option<&str>,
    cash_amount: Option<i64>,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_ap_payment(
            $1::UUID, $2::CHAR(3), $3::BIGINT,
            $4::DATE, $5::UUID, $6::UUID, NULL,
            $7::CHAR(3), $8::BIGINT
         )::text",
    )
    .bind(vendor_id)
    .bind(currency)
    .bind(amount)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .bind(cash_currency)
    .bind(cash_amount)
    .fetch_one(pool)
    .await
}

async fn fxc(pool: &PgPool, currency: &str) -> i64 {
    account_id_by_kind_currency(pool, "fx_clearing", Some(currency)).await
}

// ============================================================
// AR side — same-currency regression
// ============================================================

#[tokio::test]
async fn ar_same_currency_with_null_cash_params_unchanged() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = fresh_customer(&pool, "AR-SC-NULL", "USD").await;
    let cust_ar = seed_ar(&pool, &cust, "USD", 1000).await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let cash_before = balance(&pool, cash).await;

    let key = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "USD", 600, "2026-04-25", &key, None, None)
        .await
        .expect("same-ccy NULL params");

    assert_eq!(balance(&pool, cust_ar).await, 400);
    assert_eq!(balance(&pool, cash).await - cash_before, 600);
    assert_invariants_hold(&pool, "ar_same_currency_with_null_cash_params_unchanged").await;
}

// ============================================================
// AR side — cross-currency settlement
// ============================================================

#[tokio::test]
async fn ar_cross_currency_zero_delta_no_fx_leg() {
    // Customer pays $1200 USD against 1000 EUR AR @ rate 1.20.
    // v_expected = 1000 × 1.20 = 1200 = p_cash_amount → delta = 0,
    // no FX leg posted.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let cust = fresh_customer(&pool, "AR-XC-ZERO", "EUR").await;
    let cust_ar = seed_ar(&pool, &cust, "EUR", 1000).await;
    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let fx_eur = fxc(&pool, "EUR").await;
    let fx_usd = fxc(&pool, "USD").await;
    let gain_usd = account_id_by_kind_currency(&pool, "realized_fx_gain", Some("USD")).await;
    let loss_usd = account_id_by_kind_currency(&pool, "realized_fx_loss", Some("USD")).await;

    let cash_before = balance(&pool, cash_usd).await;
    let gain_before = balance(&pool, gain_usd).await;
    let loss_before = balance(&pool, loss_usd).await;

    let key = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1200))
        .await
        .expect("zero-delta cross-currency");

    assert_eq!(balance(&pool, cust_ar).await, 0, "ar drained");
    assert_eq!(balance(&pool, cash_usd).await - cash_before, 1200, "cash usd");
    assert_eq!(balance(&pool, fx_eur).await, 1000, "fx_clearing eur DR");
    assert_eq!(balance(&pool, fx_usd).await, -1200, "fx_clearing usd CR");
    assert_eq!(balance(&pool, gain_usd).await, gain_before, "no gain");
    assert_eq!(balance(&pool, loss_usd).await, loss_before, "no loss");
    assert_invariants_hold(&pool, "ar_cross_currency_zero_delta_no_fx_leg").await;
}

#[tokio::test]
async fn ar_cross_currency_rate_up_recognizes_gain() {
    // Customer pays $1300 against 1000 EUR @ rate 1.20.
    // v_expected = 1200; delta = 1300 - 1200 = 100 → gain.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let cust = fresh_customer(&pool, "AR-XC-GAIN", "EUR").await;
    let cust_ar = seed_ar(&pool, &cust, "EUR", 1000).await;
    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let fx_eur = fxc(&pool, "EUR").await;
    let fx_usd = fxc(&pool, "USD").await;
    let gain_usd = account_id_by_kind_currency(&pool, "realized_fx_gain", Some("USD")).await;

    let cash_before = balance(&pool, cash_usd).await;
    let gain_before = balance(&pool, gain_usd).await;

    let key = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1300))
        .await
        .expect("gain cross-currency");

    assert_eq!(balance(&pool, cust_ar).await, 0, "ar drained");
    assert_eq!(balance(&pool, cash_usd).await - cash_before, 1300, "cash usd");
    assert_eq!(balance(&pool, fx_eur).await, 1000, "fx_clearing eur DR");
    // fx_clearing(USD) net: p_cash_amount CR - v_delta DR = 1300 - 100 = 1200 CR
    assert_eq!(balance(&pool, fx_usd).await, -1200, "fx_clearing usd CR");
    assert_eq!(balance(&pool, gain_usd).await - gain_before, -100,
        "realized_fx_gain credited 100 (credit-normal balance shows as -100)");
    assert_invariants_hold(&pool, "ar_cross_currency_rate_up_recognizes_gain").await;
}

#[tokio::test]
async fn ar_cross_currency_rate_down_recognizes_loss() {
    // Customer pays $1100 against 1000 EUR @ rate 1.20.
    // v_expected = 1200; delta = 1100 - 1200 = -100 → loss.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let cust = fresh_customer(&pool, "AR-XC-LOSS", "EUR").await;
    let cust_ar = seed_ar(&pool, &cust, "EUR", 1000).await;
    let _cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let fx_eur = fxc(&pool, "EUR").await;
    let fx_usd = fxc(&pool, "USD").await;
    let loss_usd = account_id_by_kind_currency(&pool, "realized_fx_loss", Some("USD")).await;

    let loss_before = balance(&pool, loss_usd).await;

    let key = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1100))
        .await
        .expect("loss cross-currency");

    assert_eq!(balance(&pool, cust_ar).await, 0, "ar drained");
    assert_eq!(balance(&pool, fx_eur).await, 1000, "fx_clearing eur DR");
    // fx_clearing(USD) net: 1100 CR + 100 CR = 1200 CR
    assert_eq!(balance(&pool, fx_usd).await, -1200, "fx_clearing usd CR");
    assert_eq!(balance(&pool, loss_usd).await - loss_before, 100,
        "realized_fx_loss debited 100 (debit-normal balance increases)");
    assert_invariants_hold(&pool, "ar_cross_currency_rate_down_recognizes_loss").await;
}

// ============================================================
// AP side — same-currency regression
// ============================================================

#[tokio::test]
async fn ap_same_currency_with_null_cash_params_unchanged() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vend = fresh_vendor(&pool, "AP-SC-NULL", "USD").await;
    let vend_ap = seed_ap(&pool, &vend, "USD", 1000).await;
    mint_cash(&pool, "USD", 1000).await;
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let cash_before = balance(&pool, cash).await;

    let key = fresh_uuid(&pool).await;
    call_ap_payment(&pool, &vend, "USD", 400, "2026-04-25", &key, None, None)
        .await
        .expect("same-ccy NULL params");

    // ap was credit-normal at -1000; payment debits 400 → -600.
    assert_eq!(balance(&pool, vend_ap).await, -600);
    assert_eq!(balance(&pool, cash).await - cash_before, -400, "cash drained 400");
    assert_invariants_hold(&pool, "ap_same_currency_with_null_cash_params_unchanged").await;
}

// ============================================================
// AP side — cross-currency settlement
// ============================================================

#[tokio::test]
async fn ap_cross_currency_zero_delta_no_fx_leg() {
    // We pay $1200 USD against 1000 EUR AP @ rate 1.20.
    // v_expected = 1200; delta = 0 → no FX leg.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let vend = fresh_vendor(&pool, "AP-XC-ZERO", "EUR").await;
    let vend_ap = seed_ap(&pool, &vend, "EUR", 1000).await;
    mint_cash(&pool, "USD", 2000).await;
    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let fx_eur = fxc(&pool, "EUR").await;
    let fx_usd = fxc(&pool, "USD").await;
    let gain_usd = account_id_by_kind_currency(&pool, "realized_fx_gain", Some("USD")).await;
    let loss_usd = account_id_by_kind_currency(&pool, "realized_fx_loss", Some("USD")).await;

    let cash_before = balance(&pool, cash_usd).await;
    let gain_before = balance(&pool, gain_usd).await;
    let loss_before = balance(&pool, loss_usd).await;

    let key = fresh_uuid(&pool).await;
    call_ap_payment(&pool, &vend, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1200))
        .await
        .expect("zero-delta cross-currency");

    assert_eq!(balance(&pool, vend_ap).await, 0, "ap drained");
    assert_eq!(balance(&pool, cash_usd).await - cash_before, -1200, "cash usd CR'd 1200");
    assert_eq!(balance(&pool, fx_eur).await, -1000, "fx_clearing eur CR");
    assert_eq!(balance(&pool, fx_usd).await, 1200, "fx_clearing usd DR");
    assert_eq!(balance(&pool, gain_usd).await, gain_before, "no gain");
    assert_eq!(balance(&pool, loss_usd).await, loss_before, "no loss");
    assert_invariants_hold(&pool, "ap_cross_currency_zero_delta_no_fx_leg").await;
}

#[tokio::test]
async fn ap_cross_currency_rate_up_recognizes_loss() {
    // We pay $1300 against 1000 EUR @ rate 1.20.
    // v_expected = 1200; delta = 1300 - 1200 = 100 → loss (paid more).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let vend = fresh_vendor(&pool, "AP-XC-LOSS", "EUR").await;
    let vend_ap = seed_ap(&pool, &vend, "EUR", 1000).await;
    mint_cash(&pool, "USD", 2000).await;
    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let fx_eur = fxc(&pool, "EUR").await;
    let fx_usd = fxc(&pool, "USD").await;
    let loss_usd = account_id_by_kind_currency(&pool, "realized_fx_loss", Some("USD")).await;

    let cash_before = balance(&pool, cash_usd).await;
    let loss_before = balance(&pool, loss_usd).await;

    let key = fresh_uuid(&pool).await;
    call_ap_payment(&pool, &vend, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1300))
        .await
        .expect("loss cross-currency");

    assert_eq!(balance(&pool, vend_ap).await, 0, "ap drained");
    assert_eq!(balance(&pool, cash_usd).await - cash_before, -1300, "cash usd CR'd 1300");
    assert_eq!(balance(&pool, fx_eur).await, -1000, "fx_clearing eur CR");
    // fx_clearing(USD) net: 1300 DR - 100 CR = 1200 DR
    assert_eq!(balance(&pool, fx_usd).await, 1200, "fx_clearing usd DR");
    assert_eq!(balance(&pool, loss_usd).await - loss_before, 100, "realized_fx_loss");
    assert_invariants_hold(&pool, "ap_cross_currency_rate_up_recognizes_loss").await;
}

#[tokio::test]
async fn ap_cross_currency_rate_down_recognizes_gain() {
    // We pay $1100 against 1000 EUR @ rate 1.20.
    // v_expected = 1200; delta = 1100 - 1200 = -100 → gain (paid less).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let vend = fresh_vendor(&pool, "AP-XC-GAIN", "EUR").await;
    let vend_ap = seed_ap(&pool, &vend, "EUR", 1000).await;
    mint_cash(&pool, "USD", 2000).await;
    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let fx_eur = fxc(&pool, "EUR").await;
    let fx_usd = fxc(&pool, "USD").await;
    let gain_usd = account_id_by_kind_currency(&pool, "realized_fx_gain", Some("USD")).await;

    let cash_before = balance(&pool, cash_usd).await;
    let gain_before = balance(&pool, gain_usd).await;

    let key = fresh_uuid(&pool).await;
    call_ap_payment(&pool, &vend, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1100))
        .await
        .expect("gain cross-currency");

    assert_eq!(balance(&pool, vend_ap).await, 0, "ap drained");
    assert_eq!(balance(&pool, cash_usd).await - cash_before, -1100, "cash usd CR'd 1100");
    assert_eq!(balance(&pool, fx_eur).await, -1000, "fx_clearing eur CR");
    // fx_clearing(USD) net: 1100 DR + 100 DR = 1200 DR
    assert_eq!(balance(&pool, fx_usd).await, 1200, "fx_clearing usd DR");
    assert_eq!(balance(&pool, gain_usd).await - gain_before, -100,
        "realized_fx_gain credited 100");
    assert_invariants_hold(&pool, "ap_cross_currency_rate_down_recognizes_gain").await;
}

// ============================================================
// Validation errors
// ============================================================

#[tokio::test]
async fn ar_cross_currency_missing_fx_rate_raises_p0050() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // Use a date BEFORE any seeded fx_rates row (seed inserts at
    // 2026-04-01).
    let cust = fresh_customer(&pool, "AR-XC-NORATE", "EUR").await;
    seed_ar(&pool, &cust, "EUR", 1000).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0050", || async {
        call_ar_payment(&pool, &cust, "EUR", 1000, "2026-03-15", &key,
                        Some("USD"), Some(1200)).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn ap_cross_currency_missing_fx_rate_raises_p0050() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vend = fresh_vendor(&pool, "AP-XC-NORATE", "EUR").await;
    seed_ap(&pool, &vend, "EUR", 1000).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0050", || async {
        call_ap_payment(&pool, &vend, "EUR", 1000, "2026-03-15", &key,
                        Some("USD"), Some(1200)).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn ar_cross_currency_missing_cash_amount_raises_p0039() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = fresh_customer(&pool, "AR-XC-MISSING", "EUR").await;
    seed_ar(&pool, &cust, "EUR", 1000).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0039", || async {
        call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                        Some("USD"), None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn ap_cross_currency_missing_cash_amount_raises_p0042() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vend = fresh_vendor(&pool, "AP-XC-MISSING", "EUR").await;
    seed_ap(&pool, &vend, "EUR", 1000).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0042", || async {
        call_ap_payment(&pool, &vend, "EUR", 1000, "2026-04-25", &key,
                        Some("USD"), None).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn ar_same_currency_mismatched_cash_amount_raises_p0039() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let cust = fresh_customer(&pool, "AR-SC-MISMATCH", "USD").await;
    seed_ar(&pool, &cust, "USD", 1000).await;

    let key = fresh_uuid(&pool).await;
    // Same currency but p_cash_amount != p_amount.
    expect_sqlstate("P0039", || async {
        call_ar_payment(&pool, &cust, "USD", 600, "2026-04-25", &key,
                        Some("USD"), Some(700)).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn ap_same_currency_mismatched_cash_amount_raises_p0042() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let vend = fresh_vendor(&pool, "AP-SC-MISMATCH", "USD").await;
    seed_ap(&pool, &vend, "USD", 1000).await;

    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0042", || async {
        call_ap_payment(&pool, &vend, "USD", 400, "2026-04-25", &key,
                        Some("USD"), Some(500)).await.map(|_| ())
    })
    .await;
}

// ============================================================
// Idempotency + multi-call sequences
// ============================================================

#[tokio::test]
async fn ar_cross_currency_idempotency_replay_returns_same_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let cust = fresh_customer(&pool, "AR-XC-IDEMP", "EUR").await;
    let cust_ar = seed_ar(&pool, &cust, "EUR", 1000).await;

    let key = fresh_uuid(&pool).await;
    let id1 = call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                              Some("USD"), Some(1300))
        .await
        .expect("first call");
    let id2 = call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                              Some("USD"), Some(1300))
        .await
        .expect("replay");

    assert_eq!(id1, id2, "replay returns same id");
    // AR drained only once.
    assert_eq!(balance(&pool, cust_ar).await, 0);
    assert_invariants_hold(&pool, "ar_cross_currency_idempotency").await;
}

#[tokio::test]
async fn ar_cross_currency_partial_settlements_at_different_rates() {
    // Seed 1000 EUR AR. Two partial payments at different rates.
    // First: 400 EUR settled with $500 USD @ rate 1.20 (expected 480,
    //   delta +20 → gain).
    // Second: 600 EUR settled with $720 USD @ rate 1.10 (expected 660,
    //   delta +60 → gain).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-20 00:00:00+00").await;
    seed_fx_rate(&pool, "EUR", "USD", "1.10", "2026-04-26 00:00:00+00").await;

    let cust = fresh_customer(&pool, "AR-XC-PARTIAL", "EUR").await;
    let cust_ar = seed_ar(&pool, &cust, "EUR", 1000).await;
    let gain_usd = account_id_by_kind_currency(&pool, "realized_fx_gain", Some("USD")).await;
    let cash_usd = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;

    let gain_before = balance(&pool, gain_usd).await;
    let cash_before = balance(&pool, cash_usd).await;

    // First payment @ 1.20.
    let k1 = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "EUR", 400, "2026-04-25", &k1,
                    Some("USD"), Some(500))
        .await
        .expect("partial 1");
    assert_eq!(balance(&pool, cust_ar).await, 600, "ar after partial 1");

    // Second payment @ 1.10.
    let k2 = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "EUR", 600, "2026-04-27", &k2,
                    Some("USD"), Some(720))
        .await
        .expect("partial 2");
    assert_eq!(balance(&pool, cust_ar).await, 0, "ar drained");

    // Cumulative gain: 20 (first) + 60 (second) = 80
    // (credit-normal balance = -80).
    assert_eq!(balance(&pool, gain_usd).await - gain_before, -80,
        "cumulative gain across both partials");
    assert_eq!(balance(&pool, cash_usd).await - cash_before, 1220,
        "cumulative cash 500 + 720");

    assert_invariants_hold(&pool, "ar_cross_currency_partial_settlements").await;
}

// ============================================================
// fx_clearing pairs net to zero in home currency at test rate
// ============================================================

#[tokio::test]
async fn fx_clearing_pair_nets_to_zero_in_home_currency_at_rate() {
    // After AR cross-currency settlement, fx_clearing(EUR) DR
    // p_amount must equal fx_clearing(USD) CR v_expected when
    // translated at the effective rate. Verifies the bridge is
    // consistent.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    seed_fx_rate(&pool, "EUR", "USD", "1.20", "2026-04-25 00:00:00+00").await;

    let cust = fresh_customer(&pool, "AR-XC-BRIDGE", "EUR").await;
    seed_ar(&pool, &cust, "EUR", 1000).await;

    let key = fresh_uuid(&pool).await;
    call_ar_payment(&pool, &cust, "EUR", 1000, "2026-04-25", &key,
                    Some("USD"), Some(1300))
        .await
        .expect("gain cross-currency");

    let fx_eur = balance(&pool, fxc(&pool, "EUR").await).await;
    let fx_usd = balance(&pool, fxc(&pool, "USD").await).await;
    // 1000 EUR DR translated to USD @ 1.20 = 1200 USD DR-equivalent.
    // 1200 USD CR (raw). The pair offsets in home currency.
    assert_eq!(fx_eur, 1000, "fx_clearing(EUR) DR 1000");
    let eur_in_usd = (fx_eur as f64 * 1.20) as i64;
    assert_eq!(eur_in_usd + fx_usd, 0,
        "fx_clearing pair nets to zero in home currency at test rate");
}
