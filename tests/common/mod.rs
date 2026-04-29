//! Shared test harness helpers. Loaded via `mod common;` in each
//! integration test file. Not itself a test binary.

#![allow(dead_code)]

use sqlx::{PgPool, postgres::PgPoolOptions};
use std::env;
use std::future::Future;

const FIXTURE_SQL: &str = include_str!("../../db/fixtures/small/seed.sql");

/// Connect to the test DB. Requires `TEST_DATABASE_URL` (set by
/// scripts/run-tests.sh). Panics if unset or unreachable.
pub async fn connect_test_db() -> PgPool {
    let url = env::var("TEST_DATABASE_URL").expect(
        "TEST_DATABASE_URL not set — run via ./scripts/run-tests.sh \
         (it provisions an ephemeral 'acct_test' DB and exports the URL)",
    );
    PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .expect("connect to test DB")
}

/// TRUNCATE all base tables, then re-seed from db/fixtures/small/seed.sql.
/// Use this between tests that mutate state. NOT safe to run concurrently
/// against the same DB — `cargo test -- --test-threads=1` is the simplest
/// safeguard.
pub async fn reset_to_fixture(pool: &PgPool) {
    sqlx::raw_sql(
        "TRUNCATE TABLE
            transfers,
            inventory_reservations,
            period_snapshots,
            commodity_receipts,
            accounts,
            periods,
            fx_rates,
            skus,
            locations,
            sales_orders,
            purchase_orders
         RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate fixture tables");

    sqlx::raw_sql(FIXTURE_SQL)
        .execute(pool)
        .await
        .expect("seed fixture");
}

/// Run an async operation that returns `sqlx::Result<T>` and assert that it
/// fails with the given Postgres SQLSTATE code. Panics if the operation
/// succeeds, or if the failure carries a different code (or no code).
pub async fn expect_sqlstate<T, F, Fut>(code: &str, op: F)
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = sqlx::Result<T>>,
{
    let err = op().await.err().unwrap_or_else(|| {
        panic!("expected SQLSTATE {code}, got Ok");
    });
    let db_err = err
        .as_database_error()
        .unwrap_or_else(|| panic!("expected database error, got: {err:?}"));
    let actual = db_err
        .code()
        .unwrap_or_else(|| panic!("error has no SQLSTATE: {err:?}"));
    assert_eq!(
        actual.as_ref(),
        code,
        "expected SQLSTATE {code}, got {actual} ({})",
        db_err.message()
    );
}

/// Thin wrapper around `SELECT post_transfers($1, $2)`. Returns the JSONB
/// result array on success.
pub async fn call_post_transfers(
    pool: &PgPool,
    events: serde_json::Value,
    override_closed: bool,
) -> sqlx::Result<serde_json::Value> {
    sqlx::query_scalar("SELECT post_transfers($1, $2)")
        .bind(events)
        .bind(override_closed)
        .fetch_one(pool)
        .await
}

/// Generate a UUID server-side (avoids needing the `uuid` crate as a
/// dev-dependency). Returned as the canonical hex string form.
pub async fn fresh_uuid(pool: &PgPool) -> String {
    sqlx::query_scalar("SELECT gen_random_uuid()::text")
        .fetch_one(pool)
        .await
        .expect("gen_random_uuid")
}

/// Look up a non-sku-scoped account by `(kind, currency)`. Pass
/// `currency = None` for qty accounts (e.g. `creation_void` on the qty
/// ledger). Panics if the account does not exist or is ambiguous.
pub async fn account_id_by_kind_currency(
    pool: &PgPool,
    kind: &str,
    currency: Option<&str>,
) -> i64 {
    sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE kind::text = $1
            AND sku_id IS NULL
            AND ((currency = $2) OR ($2 IS NULL AND currency IS NULL))",
    )
    .bind(kind)
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("lookup account kind={kind} currency={currency:?}: {e}"))
}

/// Look up the stock_available account for a given SKU + location pair.
pub async fn account_id_stock_available(pool: &PgPool, sku_code: &str, loc_code: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT a.id
           FROM accounts a
           JOIN skus      s ON s.id = a.sku_id
           JOIN locations l ON l.id = a.location_id
          WHERE a.kind = 'stock_available'
            AND s.code = $1
            AND l.code = $2",
    )
    .bind(sku_code)
    .bind(loc_code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("stock_available lookup {sku_code}/{loc_code}: {e}"))
}

/// Build a minimal `post_transfers` event JSON. Fixed `document_kind`,
/// `document_id`, and `posted_by` — those don't affect any invariant the
/// T2 suite exercises. Optional fields (document_line_id, routing_op,
/// counterparty_id) are omitted; `post_transfers` casts them as NULL.
pub fn make_event(
    reason: &str,
    debit_account_id: i64,
    credit_account_id: i64,
    amount: i64,
    business_date: &str,
    idempotency_key: &str,
) -> serde_json::Value {
    serde_json::json!({
        "reason":            reason,
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  debit_account_id,
        "credit_account_id": credit_account_id,
        "amount":            amount,
        "business_date":     business_date,
        "idempotency_key":   idempotency_key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    })
}
