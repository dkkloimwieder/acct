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
    connect_test_db_with(8).await
}

/// Like `connect_test_db` but with a caller-specified max_connections.
/// Used by the T4 load test so it can spawn many concurrent writers
/// without each one starving the pool.
pub async fn connect_test_db_with(max_connections: u32) -> PgPool {
    let url = env::var("TEST_DATABASE_URL").expect(
        "TEST_DATABASE_URL not set — run via ./scripts/run-tests.sh \
         (it provisions an ephemeral 'acct_test' DB and exports the URL)",
    );
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&url)
        .await
        .expect("connect to test DB")
}

/// Read `pg_stat_database.deadlocks` for the current database. Used by
/// the T4 load test to assert deadlock-freedom across a run.
pub async fn pg_deadlock_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .expect("pg_stat_database read")
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
            reconciliation_alerts,
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

/// Resolve an account by a flexible (kind, sku_code, location_code,
/// currency, routing_op) selector — used by the T5 conformance harness
/// to map JSON selectors to BIGSERIAL ids. Panics if zero or >1 rows
/// match (selector ambiguity is a case-authoring bug).
#[allow(clippy::too_many_arguments)]
pub async fn account_id_for_selector(
    pool: &PgPool,
    kind: &str,
    sku_code: Option<&str>,
    location_code: Option<&str>,
    currency: Option<&str>,
    routing_op: Option<i32>,
) -> i64 {
    let rows: Vec<i64> = sqlx::query_scalar(
        "SELECT a.id
           FROM accounts a
           LEFT JOIN skus      s ON s.id = a.sku_id
           LEFT JOIN locations l ON l.id = a.location_id
          WHERE a.kind::text = $1
            AND ($2::text IS NULL OR s.code = $2)
            AND ($3::text IS NULL OR l.code = $3)
            AND ($4::text IS NULL OR a.currency = $4)
            AND ($5::int  IS NULL OR a.routing_op = $5)
            AND ($2::text IS NOT NULL OR a.sku_id      IS NULL)
            AND ($3::text IS NOT NULL OR a.location_id IS NULL)
            AND ($4::text IS NOT NULL OR a.currency    IS NULL)
            AND ($5::int  IS NOT NULL OR a.routing_op  IS NULL)",
    )
    .bind(kind)
    .bind(sku_code)
    .bind(location_code)
    .bind(currency)
    .bind(routing_op)
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("selector query failed: {e}"));
    match rows.len() {
        0 => panic!(
            "selector matched no account: kind={kind} sku={sku_code:?} loc={location_code:?} currency={currency:?} routing_op={routing_op:?}"
        ),
        1 => rows[0],
        n => panic!(
            "selector matched {n} accounts (ambiguous): kind={kind} sku={sku_code:?} loc={location_code:?} currency={currency:?} routing_op={routing_op:?}; ids={rows:?}"
        ),
    }
}

/// Snapshot every account's `(debits_total, credits_total)` keyed by
/// id. The conformance harness uses this for "no unexpected change"
/// assertions — every account whose balance changed must appear in
/// the case's `deltas` list.
pub async fn snapshot_balances(pool: &PgPool) -> std::collections::HashMap<i64, (i64, i64)> {
    let rows: Vec<(i64, i64, i64)> =
        sqlx::query_as("SELECT id, debits_total, credits_total FROM accounts ORDER BY id")
            .fetch_all(pool)
            .await
            .expect("snapshot_balances");
    rows.into_iter().map(|(id, d, c)| (id, (d, c))).collect()
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

/// Insert a fresh sales_orders row, return its id as canonical UUID text.
pub async fn fresh_sales_order(pool: &PgPool) -> String {
    sqlx::query_scalar("INSERT INTO sales_orders (status) VALUES ('open') RETURNING id::text")
        .fetch_one(pool)
        .await
        .expect("insert sales_order")
}

/// Stock a (sku, location) by `qty` units via post_transfers — the
/// fixture seeds zero on-hand. Posts a `cycle_count_adj` event with
/// stock_available on the debit side and `creation_void` (qty) on the
/// credit side, which is the canonical "balance from nothing" pattern
/// for qty ledgers and keeps every CHECK happy.
pub async fn seed_stock(pool: &PgPool, sku_code: &str, loc_code: &str, qty: i64) {
    let stock = account_id_stock_available(pool, sku_code, loc_code).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, qty, "2026-04-15", &key);
    let result = call_post_transfers(pool, serde_json::json!([event]), false)
        .await
        .expect("seed_stock post_transfers");
    assert_eq!(result[0]["result"], "ok", "seed_stock: {result}");
}

/// Invoke the `reserve_inventory()` PL/pgSQL function (migration 0014).
/// Returns `Some(id)` on success, `None` if the function returned
/// NULL (qty_promisable < qty). Both `so_id` and `so_line_id` are UUID
/// strings; expiry is set 1 hour out. The single-statement CTE+INSERT
/// pattern shown in doc §3.3 is unsafe under concurrent reservers
/// (snapshot taken before FOR UPDATE wait stays in effect for the
/// SUM subquery) — see the migration's header comment.
pub async fn try_reserve(
    pool: &PgPool,
    sku_code: &str,
    loc_code: &str,
    qty: i64,
    so_id: &str,
    so_line_id: &str,
) -> Option<String> {
    let sku_id: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(sku_code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("sku {sku_code}: {e}"));
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(loc_code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("location {loc_code}: {e}"));
    let result: Option<String> = sqlx::query_scalar(
        "SELECT reserve_inventory(
            $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
            clock_timestamp() + INTERVAL '1 hour'
         )::text",
    )
    .bind(&sku_id)
    .bind(&loc_id)
    .bind(qty)
    .bind(so_id)
    .bind(so_line_id)
    .fetch_one(pool)
    .await
    .expect("reserve_inventory call");
    result
}
