//! Shared test harness helpers. Loaded via `mod common;` in each
//! integration test file. Not itself a test binary.

#![allow(dead_code)]

pub mod outbox_worker;

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
            ledger_outbox,
            wo_events,
            wo_routings,
            wo_outputs,
            work_orders,
            bom_headers,
            engineering_change_orders,
            absorption_classes,
            accounts,
            standard_costs,
            periods,
            fx_rates,
            skus,
            locations,
            ar_payments,
            customer_return_lines,
            customer_returns,
            customer_invoice_lines,
            customer_invoices,
            so_shipment_lines,
            so_shipments,
            so_allocations,
            sales_order_lines,
            sales_orders,
            customers,
            ap_payments,
            purchase_orders,
            vendors
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

/// Insert a standard_costs row directly (bypasses post_standard_cost_roll).
/// Use this in tests that create a SKU mid-run and need it usable
/// immediately without going through the OCC + WIP + revaluation gates
/// of the production roll function. The fixture already seeds
/// standard_costs for SKU-A through SKU-H at effective_at='1970-01-01';
/// this helper is for ad-hoc SKUs.
pub async fn seed_standard_cost(pool: &PgPool, sku_code: &str, cost: i64) {
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         SELECT id, $2, '1970-01-01'::DATE,
                '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
           FROM skus WHERE code = $1",
    )
    .bind(sku_code)
    .bind(cost)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("seed_standard_cost {sku_code}={cost}: {e}"));
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

/// Build a `post_transfers` event JSON with explicit qty. Use this for
/// value-leg receipts that go through wac_perpetual or wac_periodic
/// pools — the per-class qty divisor (migration 0030, transfers.qty
/// column) needs the qty to populate. For qty-leg events where qty
/// equals amount, the regular `make_event` is fine; the post_transfers
/// INSERT logic infers qty := amount when both sides are qty-class.
pub fn make_event_with_qty(
    reason: &str,
    debit_account_id: i64,
    credit_account_id: i64,
    amount: i64,
    qty: i64,
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
        "qty":               qty,
        "business_date":     business_date,
        "idempotency_key":   idempotency_key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    })
}

/// Build a minimal `post_transfers` event JSON. Fixed `document_kind`,
/// `document_id`, and `posted_by` — those don't affect any invariant the
/// T2 suite exercises. Optional fields (document_line_id, routing_op,
/// counterparty_id) are omitted; `post_transfers` casts them as NULL.
///
/// Note: as of acct-1vr (migration 0030), value-leg events posting to
/// wac_perpetual / wac_periodic pools must include qty for the per-class
/// divisor to compute correctly. Use `make_event_with_qty` for those.
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

// ============================================================
// BOM2 helpers (acct-jg2)
//
// Test-side scaffolding for the BOM2 model. These build up
// bom_headers + bom_lines + wo_outputs + absorption_classes rows.
// As of acct-jtt (migration 0062) the legacy boms /
// wo_routing_burdens path is gone — every WO must have a
// bom_header. post_wo_start auto-initializes a default wo_outputs
// row if none exists.
// ============================================================

/// Register an absorption_class. Returns the id. The seed already
/// inserts labor_std + oh_std; use this for ad-hoc per-test classes
/// (e.g. osp_plating, production_setup).
pub async fn register_absorption_class(
    pool: &PgPool,
    code: &str,
    applied_account_kind: &str,
    expense_account_kind: Option<&str>,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO absorption_classes
            (code, display_name, applied_account_kind, expense_account_kind)
         VALUES ($1, $1, $2::account_kind, $3::account_kind)
         RETURNING id",
    )
    .bind(code)
    .bind(applied_account_kind)
    .bind(expense_account_kind)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("register_absorption_class {code}: {e}"))
}

/// Create a bom_header row. Returns the id. Defaults: alternate_no=1,
/// revision_no='A', is_primary=true, status='active', no eco_id.
pub async fn create_bom_header(pool: &PgPool, parent_sku_code: &str) -> i64 {
    create_bom_header_full(pool, parent_sku_code, 1, "A", true, "active", None).await
}

/// Full-control bom_header creation. Use when alternate / revision /
/// is_primary / status / eco_id need to deviate from the default.
#[allow(clippy::too_many_arguments)]
pub async fn create_bom_header_full(
    pool: &PgPool,
    parent_sku_code: &str,
    alternate_no: i32,
    revision_no: &str,
    is_primary: bool,
    status: &str,
    eco_id: Option<i64>,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers
            (parent_sku_id, alternate_no, revision_no, is_primary, status, eco_id)
         SELECT s.id, $2, $3, $4, $5, $6
           FROM skus s WHERE s.code = $1
         RETURNING id",
    )
    .bind(parent_sku_code)
    .bind(alternate_no)
    .bind(revision_no)
    .bind(is_primary)
    .bind(status)
    .bind(eco_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom_header parent={parent_sku_code} alt={alternate_no} rev={revision_no}: {e}"))
}

/// Add an item line to a bom_header. component sku/location resolved
/// by code. Defaults: scrap_pct=0, fire_at='op_arrival' (item lines
/// can only be per_unit + op_arrival per the bom_lines CHECKs).
#[allow(clippy::too_many_arguments)]
pub async fn add_bom_item(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    applies_at_op: i32,
    component_sku_code: &str,
    component_loc_code: &str,
    qty_per_parent: i64,
    scrap_pct: f64,
) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, scrap_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         SELECT $1, $2, 'item', 'per_unit', $3, 'op_arrival', $4,
                s.id, l.id, $7
           FROM skus s, locations l WHERE s.code = $5 AND l.code = $6",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(scrap_pct)
    .bind(component_sku_code)
    .bind(component_loc_code)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| {
        panic!(
            "add_bom_item bom={bom_id} line={line_no} comp={component_sku_code}/{component_loc_code}: {e}"
        )
    });
}

/// Add a service line to a bom_header. class lookup by code in
/// absorption_classes. basis defaults to 'per_unit'; fire_at defaults
/// to 'op_arrival'. Set fire_at='wo_start' for charges that fire
/// upfront (only valid with basis='per_lot').
#[allow(clippy::too_many_arguments)]
pub async fn add_bom_service(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    applies_at_op: i32,
    class_code: &str,
    std_amount: i64,
    basis: &str,
    fire_at: &str,
) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at,
             absorption_class_id, std_amount)
         SELECT $1, $2, 'service', $5, $3, $6, ac.id, $7
           FROM absorption_classes ac WHERE ac.code = $4",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(class_code)
    .bind(basis)
    .bind(fire_at)
    .bind(std_amount)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_service bom={bom_id} line={line_no} class={class_code}: {e}"));
}

/// Add a charge line to a bom_header. Charges are always per_lot
/// (CHECK constraint). fire_at defaults to 'op_arrival' but
/// 'wo_start' is the common case for setup/freight charges.
#[allow(clippy::too_many_arguments)]
pub async fn add_bom_charge(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    applies_at_op: i32,
    class_code: &str,
    std_amount: i64,
    fire_at: &str,
) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at,
             absorption_class_id, std_amount)
         SELECT $1, $2, 'charge', 'per_lot', $3, $5, ac.id, $6
           FROM absorption_classes ac WHERE ac.code = $4",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(class_code)
    .bind(fire_at)
    .bind(std_amount)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_charge bom={bom_id} line={line_no} class={class_code}: {e}"));
}

/// Add a wo_outputs row. Used by tests that exercise multi-output WOs
/// (co-products, by-products). For single-output, post_wo_start
/// auto-initializes a default wo_outputs row when the new path is
/// active and none exists.
#[allow(clippy::too_many_arguments)]
pub async fn add_wo_output(
    pool: &PgPool,
    wo_id: &str,
    output_no: i32,
    output_sku_code: &str,
    fg_loc_code: &str,
    qty: i64,
    allocation_method: &str,
    allocation_pct: f64,
) {
    sqlx::query(
        "INSERT INTO wo_outputs
            (wo_id, output_no, output_sku_id, fg_location_id, qty,
             allocation_method, allocation_pct)
         SELECT $1::UUID, $2, s.id, l.id, $5, $6, $7
           FROM skus s, locations l WHERE s.code = $3 AND l.code = $4",
    )
    .bind(wo_id)
    .bind(output_no)
    .bind(output_sku_code)
    .bind(fg_loc_code)
    .bind(qty)
    .bind(allocation_method)
    .bind(allocation_pct)
    .execute(pool)
    .await
    .unwrap_or_else(|e| {
        panic!(
            "add_wo_output wo={wo_id} #{output_no} sku={output_sku_code}/{fg_loc_code}: {e}"
        )
    });
}

/// Set work_orders.bom_id to pin a specific BOM (alternate or future-
/// effective draft) on the WO. NULL means "use primary active at
/// business_date" (the default — post_wo_start picks via
/// _wo_resolve_bom_for).
pub async fn set_wo_bom_id(pool: &PgPool, wo_id: &str, bom_id: i64) {
    sqlx::query("UPDATE work_orders SET bom_id = $2 WHERE id = $1::UUID")
        .bind(wo_id)
        .bind(bom_id)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("set_wo_bom_id wo={wo_id} bom={bom_id}: {e}"));
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

// ============================================================
// acct-du2 Phase 3 — assert_invariants_hold.
//
// Seven invariants from the acct-du2 epic:
//
//   I1  No debit-normal account has negative balance.
//   I2  No credit-normal account has positive balance.
//   I3  Per-class signed qty SUM on inv_value_<class> via transfers.qty
//       is ≥ 0 for every account that has any qty-bearing transfer.
//   I4  stock_wip qty ≥ 0 for every parent_sku × routing_op.
//   I5  Class avg ≈ inv_value.balance / per-class qty SUM (within
//       BIGINT-truncation tolerance).
//   I6  Per-(ledger_kind, currency) double-entry: SUM(debits) =
//       SUM(credits).
//   I7  No transfers_provisional row violates the existing CHECK
//       (schema-enforced; we double-check post-write).
//
// I1 / I2 / I7 are also enforced by Postgres CHECK constraints — this
// helper detects schema-violation paths that bypass post_transfers.
// I3 / I5 / I6 are application-level and need queries.
//
// Use from property tests + targeted regression tests. Panics on
// violation with a descriptive message including `label`.
// ============================================================

pub async fn assert_invariants_hold(pool: &PgPool, label: &str) {
    // I1 + I2: balance signs vs normal_side.
    let bad_balance: Vec<(i64, String, String, i64, i64)> = sqlx::query_as(
        "SELECT id, kind::TEXT, normal_side::TEXT, debits_total, credits_total
           FROM accounts
          WHERE NOT is_closed
            AND CASE normal_side::TEXT
                  WHEN 'debit'  THEN credits_total > debits_total
                  WHEN 'credit' THEN debits_total  > credits_total
                  ELSE FALSE
                END",
    )
    .fetch_all(pool)
    .await
    .expect("invariants I1/I2 query");
    assert!(
        bad_balance.is_empty(),
        "[{label}] I1/I2 violation — debit/credit balance against normal_side: {bad_balance:?}",
    );

    // I3: per-class signed qty SUM on inv_value_<class> via transfers.qty.
    // For every wac_* SKU's value account that has at least one qty-
    // bearing transfer, SUM(signed qty) must be >= 0.
    //
    // Standard-cost SKUs are EXCLUDED: their inv_value_wip pools receive
    // wo_start / op_arrival charges (services + lot charges) that are
    // amount-only (no qty), and qty-bearing depletions (op_move_v /
    // scrap_v / wo_complete_v) reduce the signed qty SUM. This is by
    // design — per-class qty SUM is not load-bearing for standard cost
    // (pricing comes from std_cum). The invariant matters for wac_*
    // SKUs where the qty SUM IS the divisor for running-avg pricing.
    let bad_class_qty: Vec<(i64, String, String, i64)> = sqlx::query_as(
        "SELECT a.id, a.kind::TEXT, COALESCE(s.cost_method::TEXT, 'unknown'),
                COALESCE(SUM(CASE
                  WHEN t.debit_account_id  = a.id THEN  t.qty
                  WHEN t.credit_account_id = a.id THEN -t.qty
                END), 0)::BIGINT AS net_qty
           FROM accounts a
           JOIN skus s ON s.id = a.sku_id
           JOIN transfers t
             ON a.id IN (t.debit_account_id, t.credit_account_id)
            AND t.qty IS NOT NULL
          WHERE a.kind::TEXT IN ('inv_value_raw', 'inv_value_fg', 'inv_value_wip')
            AND NOT a.is_closed
            AND s.cost_method::TEXT IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive')
          GROUP BY a.id, a.kind, s.cost_method
         HAVING COALESCE(SUM(CASE
                  WHEN t.debit_account_id  = a.id THEN  t.qty
                  WHEN t.credit_account_id = a.id THEN -t.qty
                END), 0) < 0",
    )
    .fetch_all(pool)
    .await
    .expect("invariants I3 query");
    assert!(
        bad_class_qty.is_empty(),
        "[{label}] I3 violation — per-class signed qty SUM negative on inv_value_* (wac_* SKUs only): {bad_class_qty:?}",
    );

    // I4: stock_wip qty >= 0 (subset of I1; explicit check for clarity).
    let bad_wip_qty: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT id, debits_total, credits_total
           FROM accounts
          WHERE kind::TEXT = 'stock_wip' AND NOT is_closed
            AND debits_total < credits_total",
    )
    .fetch_all(pool)
    .await
    .expect("invariants I4 query");
    assert!(
        bad_wip_qty.is_empty(),
        "[{label}] I4 violation — stock_wip qty negative: {bad_wip_qty:?}",
    );

    // I5: class avg consistency. For each value account with both non-
    // zero balance AND non-zero per-class qty SUM, balance/qty must be
    // consistent across the period (i.e. value/qty ratio sane).
    // Tolerance: ±1 (BIGINT truncation); the check is value_balance >= 0
    // when qty SUM > 0, and value_balance ≈ qty_SUM × avg_unit_cost.
    // We can't recompute avg_unit_cost at this layer without replaying
    // the chain, so the cheap check here is: value pool with positive
    // qty SUM must have non-negative balance. (Negative balance would
    // already be caught by I1; this is a redundant assertion for
    // confidence.)
    let bad_avg: Vec<(i64, String, i64, i64)> = sqlx::query_as(
        "WITH per_acct AS (
            SELECT a.id, a.kind::TEXT AS k,
                   (a.debits_total - a.credits_total) AS balance,
                   COALESCE(SUM(CASE
                     WHEN t.debit_account_id  = a.id THEN  t.qty
                     WHEN t.credit_account_id = a.id THEN -t.qty
                   END), 0)::BIGINT AS net_qty
              FROM accounts a
              JOIN skus s ON s.id = a.sku_id
              LEFT JOIN transfers t
                     ON a.id IN (t.debit_account_id, t.credit_account_id)
                    AND t.qty IS NOT NULL
             WHERE a.kind::TEXT IN ('inv_value_raw','inv_value_fg','inv_value_wip')
               AND NOT a.is_closed
               AND s.cost_method::TEXT IN ('wac_perpetual', 'wac_periodic', 'wac_retroactive')
             GROUP BY a.id, a.kind, a.debits_total, a.credits_total
         )
         SELECT id, k, balance, net_qty
           FROM per_acct
          WHERE net_qty > 0 AND balance < 0",
    )
    .fetch_all(pool)
    .await
    .expect("invariants I5 query");
    assert!(
        bad_avg.is_empty(),
        "[{label}] I5 violation — value-pool balance < 0 with positive qty SUM (wac_* only): {bad_avg:?}",
    );

    // I6: per-(ledger_kind, currency) double-entry.
    // SUM(debits_total) - SUM(credits_total) must be 0 for every
    // (ledger_kind, currency) partition. NULL currency (qty pools)
    // grouped together.
    let bad_de: Vec<(String, Option<String>, i64, i64)> = sqlx::query_as(
        "SELECT ledger_kind, currency,
                SUM(debits_total)::BIGINT AS d,
                SUM(credits_total)::BIGINT AS c
           FROM accounts
          GROUP BY ledger_kind, currency
         HAVING SUM(debits_total) <> SUM(credits_total)",
    )
    .fetch_all(pool)
    .await
    .expect("invariants I6 query");
    assert!(
        bad_de.is_empty(),
        "[{label}] I6 violation — per-(ledger_kind, currency) double-entry: {bad_de:?}",
    );

    // I7: transfers_provisional CHECK consistency.
    // Schema-enforced; double-check post-write. The CHECK constraint
    // (mig 0029 + 0065 relaxation) enforces:
    //   - finalized_at NULL ↔ variance_amount NULL ↔ variance_transfer_id NULL
    //   - finalized_at NOT NULL with variance_amount != 0 may have
    //     variance_transfer_id NULL (internal-chain) OR NOT NULL (leaf).
    let bad_prov: Vec<(i64,)> = sqlx::query_as(
        "SELECT transfer_id
           FROM transfers_provisional
          WHERE (finalized_at IS NULL AND variance_amount IS NOT NULL)
             OR (finalized_at IS NOT NULL AND variance_amount IS NULL)",
    )
    .fetch_all(pool)
    .await
    .expect("invariants I7 query");
    assert!(
        bad_prov.is_empty(),
        "[{label}] I7 violation — transfers_provisional finalized/variance mismatch: {bad_prov:?}",
    );
}
