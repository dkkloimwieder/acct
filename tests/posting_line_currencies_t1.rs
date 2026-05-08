//! T1 probes for `posting_line_currencies` (mig 0022, acct-wb75.1.2).
//! Phase B2 of the convergence plan (acct-wb75). Pin the table-level
//! constraints — CHECK (fx_rate > 0), FK to posting_lines, PK
//! uniqueness — plus the dispatcher branches: skip on functional ==
//! transaction; write on functional != transaction with fx lookup;
//! P0050 when no fx_rate exists for the requested pair at
//! business_date.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

async fn one_usd_transfer(pool: &PgPool) -> i64 {
    let cash = account_id_by_kind_currency(pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(pool, json!([event]), false)
        .await
        .expect("seed transfer");
    sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn fx_rate_must_be_positive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    // fx_rate = 0 violates CHECK 23514.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_line_currencies
                (posting_line_id, amount_transaction,
                 currency_transaction, fx_rate_to_functional)
             VALUES ($1, 100, 'EUR', 0)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;

    // fx_rate < 0 also rejected.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO posting_line_currencies
                (posting_line_id, amount_transaction,
                 currency_transaction, fx_rate_to_functional)
             VALUES ($1, 100, 'EUR', -1)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unknown_posting_line_id_fk_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO posting_line_currencies
                (posting_line_id, amount_transaction,
                 currency_transaction, fx_rate_to_functional)
             VALUES (999999999, 100, 'EUR', 1.087)",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn duplicate_posting_line_id_violates_pk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let xfer = one_usd_transfer(&pool).await;

    sqlx::query(
        "INSERT INTO posting_line_currencies
            (posting_line_id, amount_transaction,
             currency_transaction, fx_rate_to_functional)
         VALUES ($1, 100, 'EUR', 1.087)",
    )
    .bind(xfer)
    .execute(&pool)
    .await
    .unwrap();

    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO posting_line_currencies
                (posting_line_id, amount_transaction,
                 currency_transaction, fx_rate_to_functional)
             VALUES ($1, 200, 'EUR', 1.0)",
        )
        .bind(xfer)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn dispatcher_skips_extension_when_transaction_eq_functional() {
    // USD legal_entity functional = USD; USD-account postings get no row.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_line_currencies WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 0, "no extension row for USD-functional USD-transaction");
}

#[tokio::test]
async fn dispatcher_writes_extension_when_transaction_ne_functional() {
    // USD legal_entity functional = USD; EUR-account postings DO get a row.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("EUR")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let row: (i64, String, String) = sqlx::query_as(
        "SELECT amount_transaction, currency_transaction,
                fx_rate_to_functional::TEXT
           FROM posting_line_currencies WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .expect("extension row exists");

    assert_eq!(row.0, 100, "amount_transaction = posting_lines.amount");
    assert_eq!(row.1, "EUR", "currency_transaction = transaction currency");
    // fx_rate looked up from fx_rates: EUR → USD at 2026-04-01 = 1.0869565000.
    assert_eq!(row.2, "1.0869565000", "fx_rate matches fx_rates lookup");
}

#[tokio::test]
async fn dispatcher_skips_extension_for_qty_legs() {
    // qty-only legs have NULL currency on both sides; no extension row.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // creation_void qty pair: both sides ledger_kind='qty', currency NULL.
    // Need a sku-bound qty account to pair against — use stock_available.
    let cv_qty: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE kind = 'creation_void' AND ledger_kind = 'qty'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", stock, cv_qty, 5, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post qty leg");

    let xfer: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&key)
            .fetch_one(&pool)
            .await
            .unwrap();

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_line_currencies WHERE posting_line_id = $1",
    )
    .bind(xfer)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 0, "qty-leg posting has no extension row");
}

#[tokio::test]
async fn dispatcher_raises_p0050_when_fx_rate_missing() {
    // Drop the EUR→USD rate, then attempt an EUR-account posting.
    // Extension write fails with P0050 (missing_fx_rate).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    sqlx::query("DELETE FROM fx_rates WHERE from_currency = 'EUR' AND to_currency = 'USD'")
        .execute(&pool)
        .await
        .unwrap();

    let cash = account_id_by_kind_currency(&pool, "cash", Some("EUR")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);

    expect_sqlstate("P0050", || async {
        call_post_posting_lines(&pool, json!([event]), false).await
    })
    .await;
}

#[tokio::test]
async fn extension_amount_eq_posting_line_amount_invariant() {
    // The B2 invariant: every extension row's amount_transaction equals
    // its paired posting_lines.amount. Probe across multiple postings.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("EUR")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;

    for amount in [50_i64, 1234, 999_999] {
        let key = fresh_uuid(&pool).await;
        let event = make_event("ar_payment", cash, revenue, amount, "2026-04-15", &key);
        call_post_posting_lines(&pool, json!([event]), false)
            .await
            .expect("post");
    }

    let mismatched: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM posting_line_currencies plc
           JOIN posting_lines pl ON pl.id = plc.posting_line_id
          WHERE plc.amount_transaction <> pl.amount",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(mismatched, 0, "amount_transaction = posting_lines.amount");
}
