//! `acct-th7` / Slice C — post_customer_invoice matrix.
//!
//! Three-way match clearance of ar_unsettled → ar.
//!
//! Coverage:
//!   * Happy path: invoice covers full shipment; ar_unsettled drains
//!     to 0; ar gains amount.
//!   * Partial invoice: shipped 10, invoice 6, ar_unsettled retains
//!     remainder; subsequent invoice for 4 clears it.
//!   * Three-way mismatch (P0040): unit_price diverges; amount
//!     diverges from qty × unit_price; over-invoice qty.
//!   * Service line: revenue_account → ar with optional tax leg
//!     posting to sales_tax_payable.
//!   * P0041: customer mismatch, currency mismatch, missing /
//!     closed / wrong-ledger / wrong-currency revenue_account,
//!     unknown line kind.
//!   * Idempotency: replay returns existing id.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffold
// ============================================================

#[allow(dead_code)]
struct InvScaffold {
    customer_id: String,
    so_id: String,
    sku_id: String,
    ship_loc_id: String,
    so_line_id: String,
    cust_unsettled: i64,
    cust_ar: i64,
    revenue_acct: i64,
    tax_acct: i64,
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

async fn fresh_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
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
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, $7)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .bind(currency)
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
    .unwrap_or_else(|e| panic!("set_std_cost: {e}"));
}

/// Scaffold a customer + SO with one line, run a ship to seed
/// ar_unsettled, then return the resolved IDs for invoice testing.
async fn scaffold_post_ship(
    pool: &PgPool,
    suffix: &str,
    qty_ordered: i64,
    unit_price: i64,
    qty_shipped: i64,
) -> InvScaffold {
    let customer_id = fresh_customer(pool, &format!("CUST-{suffix}"), "USD").await;
    let sku_id = fresh_sku(pool, &format!("SKU-{suffix}")).await;
    let ship_loc_id = fresh_location(pool, &format!("LOC-{suffix}")).await;

    set_std_cost(pool, &sku_id, 60).await;

    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(
        pool, &so_id, 1, &sku_id, &ship_loc_id, qty_ordered, unit_price, "USD",
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
    let _cust_qty = open_account(
        pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        pool, "ar_unsettled", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    )
    .await;

    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let tax_acct = account_id_by_kind_currency(pool, "sales_tax_payable", Some("USD")).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    // Seed FG.
    let posted_by = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":fresh_uuid(pool).await,
         "debit_account_id":qty_acct,"credit_account_id":void_qty,
         "amount":qty_ordered,"qty":qty_ordered,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
        {"reason":"cycle_count_adj","document_kind":"seed","document_id":fresh_uuid(pool).await,
         "debit_account_id":val_acct,"credit_account_id":void_val,
         "amount":qty_ordered * 60,"qty":qty_ordered,"business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,"posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint).execute(pool).await.expect("seed");

    // Ship (skip if zero — service-only invoice tests).
    if qty_shipped > 0 {
        let posted_by = fresh_uuid(pool).await;
        let key = fresh_uuid(pool).await;
        sqlx::query(
            "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                                  $3::UUID, $4::UUID, NULL)",
        )
        .bind(&so_id)
        .bind(json!([{"so_line_id": so_line_id, "qty_shipped": qty_shipped}]))
        .bind(&posted_by)
        .bind(&key)
        .execute(pool)
        .await
        .expect("ship");
    }

    InvScaffold {
        customer_id,
        so_id,
        sku_id,
        ship_loc_id,
        so_line_id,
        cust_unsettled,
        cust_ar,
        revenue_acct,
        tax_acct,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn call_invoice(
    pool: &PgPool,
    customer_id: &str,
    currency: &str,
    lines: serde_json::Value,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_customer_invoice($1::UUID, $2::CHAR(3), $3::JSONB,
                                       '2026-04-20'::DATE, $4::UUID, $5::UUID,
                                       NULL)::text",
    )
    .bind(customer_id)
    .bind(currency)
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
async fn full_invoice_clears_ar_unsettled() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "FULL", 50, 100, 30).await;

    // Pre-invoice: ar_unsettled = 30 × 100 = 3000.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 3000);
    assert_eq!(balance(&pool, s.cust_ar).await, 0);

    let lines = json!([
        {"kind": "so_match", "so_line_id": s.so_line_id,
         "qty": 30, "unit_price": 100, "amount": 3000}
    ]);
    let _doc_id = call_invoice(&pool, &s.customer_id, "USD", lines)
        .await
        .expect("invoice");

    // ar_unsettled drained, ar credited.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 3000);

    assert_invariants_hold(&pool, "full_invoice_clears_ar_unsettled").await;
}

#[tokio::test]
async fn partial_invoice_then_complete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "PART", 50, 100, 10).await;

    // Pre: ar_unsettled = 10 × 100 = 1000.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 1000);

    // First invoice: 6 of 10.
    let lines = json!([
        {"kind": "so_match", "so_line_id": s.so_line_id,
         "qty": 6, "unit_price": 100, "amount": 600}
    ]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await.expect("inv-1");

    assert_eq!(balance(&pool, s.cust_unsettled).await, 1000 - 600);
    assert_eq!(balance(&pool, s.cust_ar).await, 600);

    // Second invoice: remaining 4.
    let lines = json!([
        {"kind": "so_match", "so_line_id": s.so_line_id,
         "qty": 4, "unit_price": 100, "amount": 400}
    ]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await.expect("inv-2");

    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 1000);
}

#[tokio::test]
async fn service_line_posts_directly_to_ar() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "SVC", 10, 100, 0).await;
    // No shipment → ar_unsettled is 0.
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);

    let lines = json!([
        {"kind": "service", "revenue_account_id": s.revenue_acct, "amount": 250}
    ]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await.expect("inv");

    // ar gets 250; revenue credited 250.
    assert_eq!(balance(&pool, s.cust_ar).await, 250);
    assert_eq!(balance(&pool, s.revenue_acct).await, -250);

    assert_invariants_hold(&pool, "service_line_posts_directly_to_ar").await;
}

#[tokio::test]
async fn service_line_with_tax() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "SVCTAX", 10, 100, 0).await;

    let lines = json!([
        {"kind": "service", "revenue_account_id": s.revenue_acct,
         "amount": 200, "tax_amount": 16}
    ]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await.expect("inv");

    // ar = 200 + 16 = 216. revenue = 200. tax = 16.
    assert_eq!(balance(&pool, s.cust_ar).await, 216);
    assert_eq!(balance(&pool, s.revenue_acct).await, -200);
    assert_eq!(balance(&pool, s.tax_acct).await, -16);
}

#[tokio::test]
async fn mixed_so_match_plus_service() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "MIX", 20, 100, 5).await;
    // After ship: ar_unsettled = 500.

    let lines = json!([
        {"kind": "so_match", "so_line_id": s.so_line_id,
         "qty": 5, "unit_price": 100, "amount": 500},
        {"kind": "service", "revenue_account_id": s.revenue_acct, "amount": 75}
    ]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await.expect("inv");

    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 500 + 75);
}

#[tokio::test]
async fn idempotency_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "INV-IDEMP", 10, 100, 5).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"kind": "so_match", "so_line_id": s.so_line_id,
         "qty": 5, "unit_price": 100, "amount": 500}
    ]);

    let id1: String = sqlx::query_scalar(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB, '2026-04-20'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id)
    .bind(&lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("inv-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB, '2026-04-20'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id)
    .bind(&lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("inv-2 replay");

    assert_eq!(id1, id2);
    // No double-post.
    assert_eq!(balance(&pool, s.cust_ar).await, 500);
}

// ============================================================
// Validation gates
// ============================================================

#[tokio::test]
async fn unit_price_mismatch_raises_p0040() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "UPM", 10, 100, 5).await;
    expect_sqlstate("P0040", || async {
        let lines = json!([
            {"kind": "so_match", "so_line_id": s.so_line_id,
             "qty": 5, "unit_price": 99, "amount": 495}
        ]);
        call_invoice(&pool, &s.customer_id, "USD", lines).await
    })
    .await;
}

#[tokio::test]
async fn amount_mismatch_raises_p0040() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "AMTMM", 10, 100, 5).await;
    expect_sqlstate("P0040", || async {
        let lines = json!([
            {"kind": "so_match", "so_line_id": s.so_line_id,
             "qty": 5, "unit_price": 100, "amount": 499}
        ]);
        call_invoice(&pool, &s.customer_id, "USD", lines).await
    })
    .await;
}

#[tokio::test]
async fn over_invoice_raises_p0040() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "OVERINV", 10, 100, 5).await;
    expect_sqlstate("P0040", || async {
        // shipped 5; trying to invoice 7 → P0040.
        let lines = json!([
            {"kind": "so_match", "so_line_id": s.so_line_id,
             "qty": 7, "unit_price": 100, "amount": 700}
        ]);
        call_invoice(&pool, &s.customer_id, "USD", lines).await
    })
    .await;
}

#[tokio::test]
async fn unknown_customer_raises_p0041() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate("P0041", || async {
        let lines = json!([{"kind": "service", "revenue_account_id": 1, "amount": 100}]);
        call_invoice(&pool, &bogus, "USD", lines).await
    })
    .await;
}

#[tokio::test]
async fn empty_lines_raises_p0041() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "EMPT", 10, 100, 0).await;
    expect_sqlstate("P0041", || async {
        call_invoice(&pool, &s.customer_id, "USD", json!([])).await
    })
    .await;
}

#[tokio::test]
async fn unknown_line_kind_raises_p0041() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "BADKIND", 10, 100, 0).await;
    expect_sqlstate("P0041", || async {
        let lines = json!([{"kind": "wat", "amount": 100}]);
        call_invoice(&pool, &s.customer_id, "USD", lines).await
    })
    .await;
}

#[tokio::test]
async fn so_match_wrong_customer_raises_p0041() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s_a = scaffold_post_ship(&pool, "WRC-A", 10, 100, 5).await;
    let s_b = scaffold_post_ship(&pool, "WRC-B", 10, 100, 0).await;
    expect_sqlstate("P0041", || async {
        // Try to invoice s_a's so_line under s_b's customer.
        let lines = json!([
            {"kind": "so_match", "so_line_id": s_a.so_line_id,
             "qty": 5, "unit_price": 100, "amount": 500}
        ]);
        call_invoice(&pool, &s_b.customer_id, "USD", lines).await
    })
    .await;
}

#[tokio::test]
async fn service_line_revenue_account_wrong_currency_raises_p0041() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "WCY", 10, 100, 0).await;
    let eur_revenue = account_id_by_kind_currency(&pool, "revenue", Some("EUR")).await;
    expect_sqlstate("P0041", || async {
        // Invoice currency=USD but revenue account=EUR.
        let lines = json!([
            {"kind": "service", "revenue_account_id": eur_revenue, "amount": 100}
        ]);
        call_invoice(&pool, &s.customer_id, "USD", lines).await
    })
    .await;
}

// ============================================================
// Three-way match tolerance windows (acct-7mc, AR side)
// ============================================================

async fn set_customer_tolerance(pool: &PgPool, customer_id: &str, pct: &str) {
    sqlx::query(
        "UPDATE customers SET unit_price_tolerance_pct = $1::NUMERIC WHERE id = $2::UUID",
    )
    .bind(pct)
    .bind(customer_id)
    .execute(pool)
    .await
    .expect("set tolerance");
}

async fn ar_match_tol_acct(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT id FROM accounts
         WHERE kind='variance_match_tolerance' AND ledger_kind='value'
           AND currency='USD' AND counterparty_id IS NULL AND NOT is_closed",
    )
    .fetch_one(pool)
    .await
    .expect("match_tol")
}

#[tokio::test]
async fn ar_within_tolerance_unfavorable_absorbs_to_variance() {
    // SO unit_price=100, customer tolerance=2%. Invoice at 102 (2%
    // exactly). variance_match_tolerance credits 20 (gain on AR side
    // — customer pays more than expected); ar gains 1020 total.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "AR-TOL", 50, 100, 10).await;
    set_customer_tolerance(&pool, &s.customer_id, "2.0").await;
    let mt = ar_match_tol_acct(&pool).await;
    let mt_before = balance(&pool, mt).await;

    let lines = json!([{
        "kind": "so_match", "so_line_id": s.so_line_id,
        "qty": 10, "unit_price": 102, "amount": 1020
    }]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await
        .expect("within tolerance ar");

    // ar_unsettled drained by base 1000 (qty * so_line.unit_price).
    // ar gained 1020 (1000 base from cleared accrual + 20 absorption).
    // variance_match_tolerance credit = -20 (gain on credit-normal-eq
    // unrestricted P&L).
    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 1020);
    assert_eq!(balance(&pool, mt).await - mt_before, -20);

    assert_invariants_hold(&pool, "ar_within_tolerance_unfavorable_absorbs_to_variance").await;
}

#[tokio::test]
async fn ar_within_tolerance_favorable_absorbs_to_variance() {
    // Invoice at 98 (2% below). Less ar; variance debits 20 (loss).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "AR-TOL-FAV", 50, 100, 10).await;
    set_customer_tolerance(&pool, &s.customer_id, "2.0").await;
    let mt = ar_match_tol_acct(&pool).await;
    let mt_before = balance(&pool, mt).await;

    let lines = json!([{
        "kind": "so_match", "so_line_id": s.so_line_id,
        "qty": 10, "unit_price": 98, "amount": 980
    }]);
    call_invoice(&pool, &s.customer_id, "USD", lines).await
        .expect("within tolerance ar fav");

    assert_eq!(balance(&pool, s.cust_unsettled).await, 0);
    assert_eq!(balance(&pool, s.cust_ar).await, 980);
    assert_eq!(balance(&pool, mt).await - mt_before, 20);
}

#[tokio::test]
async fn ar_out_of_tolerance_still_raises_p0040() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "AR-OOT", 50, 100, 10).await;
    set_customer_tolerance(&pool, &s.customer_id, "2.0").await;

    let lines = json!([{
        "kind": "so_match", "so_line_id": s.so_line_id,
        "qty": 10, "unit_price": 105, "amount": 1050
    }]);
    expect_sqlstate("P0040", || async {
        call_invoice(&pool, &s.customer_id, "USD", lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn ar_zero_tolerance_default_is_strict() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_post_ship(&pool, "AR-STRICT", 50, 100, 10).await;
    let lines = json!([{
        "kind": "so_match", "so_line_id": s.so_line_id,
        "qty": 10, "unit_price": 101, "amount": 1010
    }]);
    expect_sqlstate("P0040", || async {
        call_invoice(&pool, &s.customer_id, "USD", lines.clone()).await
    })
    .await;
}
