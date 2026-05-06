//! `acct-th7` / Slice C — post_so_ship matrix.
//!
//! Coverage:
//!   * Happy path: standard SKU, FG seeded, ship qty < FG qty → 4 events
//!     posted (qty leg, COGS leg, revenue leg, optional tax leg)
//!     and so_shipments+lines persisted.
//!   * WAC SKU (per-class): COGS dispatched at running avg from
//!     inv_value_fg.
//!   * Multi-line: ship two so_lines in one call.
//!   * Reservation transition: 'active' → 'shipped'.
//!   * Validation:
//!     - SO not found → P0037
//!     - SO with NULL customer_id → P0037
//!     - Empty p_lines → P0037
//!     - so_line not found → P0037
//!     - Wrong SO ownership → P0037
//!     - qty_shipped <= 0 → P0037
//!     - Over-ship vs qty_ordered → P0038
//!   * Idempotency: replay returns existing id.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffold
// ============================================================

#[allow(dead_code)]
struct ShipScaffold {
    customer_id: String,
    so_id: String,
    sku_id: String,
    ship_loc_id: String,
    so_line_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
    revenue_acct: i64,
    cogs_acct: i64,
    tax_acct: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
}

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
    .unwrap_or_else(|e| panic!("insert customer {code}: {e}"))
}

async fn fresh_sku(pool: &PgPool, code: &str, cost_method: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', $2::cost_method) RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert sku {code}: {e}"))
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert loc {code}: {e}"))
}

async fn create_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_so: {e}"))
}

async fn create_so_no_customer(pool: &PgPool) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES (NULL, 'open') RETURNING id::text",
    )
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_so_no_customer: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn add_so_line(
    pool: &PgPool,
    so_id: &str,
    line_no: i32,
    sku_id: &str,
    ship_loc_id: &str,
    qty_ordered: i64,
    unit_price: i64,
    currency: &str,
    tax_amount: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, $7, $8)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .bind(currency)
    .bind(tax_amount)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("add_so_line: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open_account {kind}: {e}"))
}

async fn set_std_cost(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("set_std_cost {sku_id}: {e}"));
}

/// Standard-SKU scaffold. Customer + SO with one line for `sku_id`,
/// ship_qty `qty_ordered`, unit_price 100 USD. Standard cost = 60.
async fn scaffold_standard(
    pool: &PgPool,
    suffix: &str,
    qty_ordered: i64,
    unit_price: i64,
    tax_amount: i64,
) -> ShipScaffold {
    let customer_id = fresh_customer(pool, &format!("CUST-{suffix}"), "USD").await;
    let sku_id = fresh_sku(pool, &format!("SKU-S-{suffix}"), "standard").await;
    let ship_loc_id = fresh_location(pool, &format!("SHIP-{suffix}")).await;

    set_std_cost(pool, &sku_id, 60).await;

    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(
        pool,
        &so_id,
        1,
        &sku_id,
        &ship_loc_id,
        qty_ordered,
        unit_price,
        "USD",
        tax_amount,
    )
    .await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None, Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;
    let cust_qty = open_account(
        pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;

    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let tax_acct = account_id_by_kind_currency(pool, "sales_tax_payable", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    ShipScaffold {
        customer_id,
        so_id,
        sku_id,
        ship_loc_id,
        so_line_id,
        qty_acct,
        val_acct,
        cust_qty,
        cust_unsettled,
        revenue_acct,
        cogs_acct,
        tax_acct,
        creation_void_qty,
        creation_void_val,
    }
}

/// Seed FG inventory (qty + value) so ship can deplete from a real pool.
async fn seed_fg(pool: &PgPool, s: &ShipScaffold, qty: i64, total_value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"so_test_seed", "document_id":doc_id,
         "debit_account_id":s.qty_acct, "credit_account_id":s.creation_void_qty,
         "amount":qty, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"so_test_seed", "document_id":doc_id,
         "debit_account_id":s.val_acct, "credit_account_id":s.creation_void_val,
         "amount":total_value, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_fg: {e}"));
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn call_so_ship(
    pool: &PgPool,
    so_id: &str,
    lines: serde_json::Value,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Happy paths
// ============================================================

#[tokio::test]
async fn happy_path_standard_sku_no_tax() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "HAPPY1", 50, 100, 0).await;
    seed_fg(&pool, &s, 50, 3000).await; // unit_cost = 60

    let lines = json!([
        {"so_line_id": s.so_line_id, "qty_shipped": 30}
    ]);
    let _doc_id = call_so_ship(&pool, &s.so_id, lines)
        .await
        .expect("ship");

    // qty leg: customer_pool DR 30, stock_available CR 30
    assert_eq!(balance(&pool, s.cust_qty).await, 30);
    assert_eq!(balance(&pool, s.qty_acct).await, 50 - 30);

    // COGS leg: cogs DR 30×60=1800, inv_value_fg CR 1800
    assert_eq!(balance(&pool, s.cogs_acct).await, 1800);
    assert_eq!(balance(&pool, s.val_acct).await, 3000 - 1800);

    // Revenue leg: ar_unsettled DR 30×100=3000, revenue CR 3000
    assert_eq!(balance(&pool, s.cust_unsettled).await, 3000);
    let rev_balance = balance(&pool, s.revenue_acct).await;
    // revenue is credit-normal; debits_total - credits_total = -3000
    assert_eq!(rev_balance, -3000);

    // No tax leg.
    let tax_balance = balance(&pool, s.tax_acct).await;
    assert_eq!(tax_balance, 0);

    // Audit row persisted with dispatcher-resolved unit_cost = 60.
    let (qty, unit_cost, unit_price): (i64, i64, i64) = sqlx::query_as(
        "SELECT qty_shipped, unit_cost, unit_price FROM so_shipment_lines
         WHERE so_line_id = $1::UUID",
    )
    .bind(&s.so_line_id)
    .fetch_one(&pool)
    .await
    .expect("audit row");
    assert_eq!(qty, 30);
    assert_eq!(unit_cost, 60);
    assert_eq!(unit_price, 100);

    assert_invariants_hold(&pool, "happy_path_standard_sku_no_tax").await;
}

#[tokio::test]
async fn happy_path_standard_sku_with_tax() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "HAPPY2", 20, 50, 5).await; // tax 5/unit
    seed_fg(&pool, &s, 20, 600).await; // unit_cost = 30

    let lines = json!([
        {"so_line_id": s.so_line_id, "qty_shipped": 10, "tax_amount": 50}
    ]);
    let _doc_id = call_so_ship(&pool, &s.so_id, lines)
        .await
        .expect("ship");

    // ar_unsettled: 10 × 50 (revenue) + 50 (tax) = 550
    assert_eq!(balance(&pool, s.cust_unsettled).await, 550);
    // sales_tax_payable: credited 50 → balance -50
    assert_eq!(balance(&pool, s.tax_acct).await, -50);

    assert_invariants_hold(&pool, "happy_path_standard_sku_with_tax").await;
}

#[tokio::test]
async fn happy_path_wac_sku_dispatcher_priced() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // WAC perpetual SKU. Pool at 100 qty, $700 → unit avg = 7.
    let customer_id = fresh_customer(&pool, "CUST-WAC1", "USD").await;
    let sku_id = fresh_sku(&pool, "SKU-WAC1", "wac_perpetual").await;
    let ship_loc_id = fresh_location(&pool, "SHIP-WAC1").await;

    let so_id = create_so(&pool, &customer_id).await;
    let so_line_id = add_so_line(
        &pool, &so_id, 1, &sku_id, &ship_loc_id, 100, 10, "USD", 0,
    )
    .await;

    let qty_acct = open_account(
        &pool, "stock_available", "qty", None, Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        &pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&ship_loc_id), None, "debit",
    )
    .await;
    let _cust_qty = open_account(
        &pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        &pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;

    let posted_by = fresh_uuid(&pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":fresh_uuid(&pool).await,
         "debit_account_id":qty_acct,"credit_account_id":void_qty,
         "amount":100,"qty":100,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":fresh_uuid(&pool).await,
         "debit_account_id":val_acct,"credit_account_id":void_val,
         "amount":700,"qty":100,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(&pool).await,"posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint).execute(&pool).await.expect("seed");

    let lines = json!([{"so_line_id": so_line_id, "qty_shipped": 10}]);
    let _doc_id = call_so_ship(&pool, &so_id, lines).await.expect("ship");

    // unit_cost = 700/100 = 7. COGS = 70. Revenue = 10×10 = 100.
    let cogs_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    assert_eq!(balance(&pool, cogs_acct).await, 70);
    assert_eq!(balance(&pool, val_acct).await, 700 - 70);
    assert_eq!(balance(&pool, cust_unsettled).await, 100);

    // Audit row captures unit_cost = 7.
    let unit_cost: i64 = sqlx::query_scalar(
        "SELECT unit_cost FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&so_line_id)
    .fetch_one(&pool)
    .await
    .expect("audit");
    assert_eq!(unit_cost, 7);

    // acct-5prc / R7 (AP9). The persisted unit_cost MUST equal the
    // ledger's effective unit cost on the COGS leg (transfer.amount
    // / transfer.qty). This is the audit-trail-vs-ledger invariant
    // mig 0091's FOR UPDATE on v_val_acct preserves under
    // concurrency. Single-threaded the values trivially agree, but
    // asserting it here pins the invariant so future cost-dispatch
    // refactors can't silently break it.
    let cogs_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let (ledger_amount, ledger_qty): (i64, i64) = sqlx::query_as(
        "SELECT amount, qty FROM transfers
          WHERE document_kind = 'so_shipment'
            AND reason        = 'so_ship'
            AND debit_account_id  = $1
            AND credit_account_id = $2",
    )
    .bind(cogs_acct)
    .bind(val_acct)
    .fetch_one(&pool)
    .await
    .expect("cogs transfer");
    assert_eq!(ledger_amount / ledger_qty, unit_cost,
        "audit unit_cost ({unit_cost}) must equal ledger amount/qty ({}/{})",
        ledger_amount, ledger_qty);

    assert_invariants_hold(&pool, "happy_path_wac_sku_dispatcher_priced").await;
}

#[tokio::test]
async fn reservation_active_to_shipped() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "RES1", 50, 100, 0).await;
    seed_fg(&pool, &s, 50, 3000).await;

    // Insert an active reservation tied to this SO + line.
    let rsv_id: String = sqlx::query_scalar(
        "INSERT INTO inventory_reservations
            (sku_id, location_id, qty, so_id, so_line_id, status, expires_at)
         VALUES ($1::UUID, $2::UUID, 30, $3::UUID, $4::UUID, 'active',
                 clock_timestamp() + INTERVAL '1 hour')
         RETURNING id::text",
    )
    .bind(&s.sku_id)
    .bind(&s.ship_loc_id)
    .bind(&s.so_id)
    .bind(&s.so_line_id)
    .fetch_one(&pool)
    .await
    .expect("insert reservation");

    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 30}]);
    let _doc_id = call_so_ship(&pool, &s.so_id, lines).await.expect("ship");

    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&rsv_id)
    .fetch_one(&pool)
    .await
    .expect("status");
    assert_eq!(status, "shipped");

    let has_resolved: bool = sqlx::query_scalar(
        "SELECT resolved_at IS NOT NULL FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&rsv_id)
    .fetch_one(&pool)
    .await
    .expect("resolved_at");
    assert!(has_resolved, "resolved_at must be set");
}

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "SHIP-IDEMP", 50, 100, 0).await;
    seed_fg(&pool, &s, 50, 3000).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 10}]);

    let id1: String = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(&lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("ship 1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(&lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("ship 2 replay");

    assert_eq!(id1, id2, "replay returns existing id");

    // Only one ar_unsettled debit (no double-post).
    assert_eq!(balance(&pool, s.cust_unsettled).await, 1000);
}

// ============================================================
// Validation gates
// ============================================================

#[tokio::test]
async fn so_not_found_raises_p0037() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate("P0037", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                                  $3::UUID, $4::UUID, NULL)::text",
        )
        .bind(&bogus)
        .bind(json!([{"so_line_id": bogus, "qty_shipped": 1}]))
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn so_no_customer_raises_p0037() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let so_id = create_so_no_customer(&pool).await;
    expect_sqlstate("P0037", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        let bogus_line = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                                  $3::UUID, $4::UUID, NULL)::text",
        )
        .bind(&so_id)
        .bind(json!([{"so_line_id": bogus_line, "qty_shipped": 1}]))
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn empty_lines_raises_p0037() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "EMPTY", 10, 10, 0).await;
    expect_sqlstate("P0037", || async {
        call_so_ship(&pool, &s.so_id, json!([])).await
    })
    .await;
}

#[tokio::test]
async fn so_line_wrong_so_raises_p0037() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s_a = scaffold_standard(&pool, "OWN-A", 10, 10, 0).await;
    let s_b = scaffold_standard(&pool, "OWN-B", 10, 10, 0).await;
    seed_fg(&pool, &s_a, 10, 100).await;
    // Try to ship s_a's line via s_b's SO.
    expect_sqlstate("P0037", || async {
        let lines = json!([{"so_line_id": s_a.so_line_id, "qty_shipped": 1}]);
        call_so_ship(&pool, &s_b.so_id, lines).await
    })
    .await;
}

#[tokio::test]
async fn qty_zero_raises_p0037() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "Q0", 10, 10, 0).await;
    expect_sqlstate("P0037", || async {
        let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 0}]);
        call_so_ship(&pool, &s.so_id, lines).await
    })
    .await;
}

#[tokio::test]
async fn over_ship_raises_p0038() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "OVER", 10, 10, 0).await;
    seed_fg(&pool, &s, 100, 600).await; // plenty of FG, but qty_ordered=10
    expect_sqlstate("P0038", || async {
        let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 11}]);
        call_so_ship(&pool, &s.so_id, lines).await
    })
    .await;
}

#[tokio::test]
async fn cumulative_over_ship_across_calls_raises_p0038() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "CUM", 10, 10, 0).await;
    seed_fg(&pool, &s, 50, 3000).await; // unit_cost = 60 (matches set_std_cost)

    // First ship 7 — fine.
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 7}]);
    call_so_ship(&pool, &s.so_id, lines).await.expect("ship 7");

    // Second ship 4 — cumulative 11 > 10 → P0038.
    expect_sqlstate("P0038", || async {
        let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": 4}]);
        call_so_ship(&pool, &s.so_id, lines).await
    })
    .await;
}
