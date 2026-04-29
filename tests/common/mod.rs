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
