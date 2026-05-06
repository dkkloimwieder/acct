//! `acct-7mg` / Slice A — `post_ap_bill` matrix.
//!
//! Two line modes:
//!   po_match — clears ap_unsettled → ap with strict three-way match
//!              (qty within received-not-billed remainder, unit_cost
//!              equals po_line.unit_cost, amount = qty × unit_cost).
//!   service  — caller-supplied expense_account → ap. No PO reference.
//! Mixed bills (po_match + service) supported.

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Setup helpers
// ============================================================

#[allow(dead_code)]
struct Setup {
    vendor_id: String,
    po_id: String,
    po_line_id: String,
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_unsettled: i64,
    ap: i64,
    var_ppv: i64,
}

async fn id_text(pool: &PgPool, q: &str, bind: &str) -> String {
    sqlx::query_scalar(q)
        .bind(bind)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("{q} for {bind}: {e}"))
}

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_po(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status) VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_po_line(
    pool: &PgPool,
    po_id: &str,
    line_no: i32,
    sku_id: &str,
    loc_id: &str,
    qty_ordered: i64,
    unit_cost: i64,
    currency: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, $7)
         RETURNING id::text",
    )
    .bind(po_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty_ordered)
    .bind(unit_cost)
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
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, counterparty_id, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID, $7::balance_direction)
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
    .unwrap_or_else(|e| panic!("open {kind}/{ledger_kind}: {e}"))
}

/// Build the vendor + PO + PO-line + accounts scaffold; standard SKU-A
/// at MAIN. po and std equal (no PPV) to keep arithmetic simple.
async fn build_setup(pool: &PgPool, vendor_code: &str) -> Setup {
    let sku = id_text(pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(pool, vendor_code, "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku, &loc, 100, 100, "USD").await;

    let qty_acct = account_id_stock_available(pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        pool, "inv_value_raw", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;

    let ven_qty =
        open_account(pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit")
            .await;
    let ven_unsettled = open_account(
        pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let ap = open_account(
        pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let var_ppv = open_account(
        pool, "variance_ppv", "value", Some("USD"), None, None, None, "unrestricted",
    )
    .await;

    Setup {
        vendor_id: vendor,
        po_id: po,
        po_line_id: po_line,
        sku_id: sku,
        loc_id: loc,
        qty_acct,
        val_acct,
        ven_qty,
        ven_unsettled,
        ap,
        var_ppv,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn call_receipt(
    pool: &PgPool,
    po_id: &str,
    lines: serde_json::Value,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(po_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_bill(
    pool: &PgPool,
    vendor_id: &str,
    currency: &str,
    lines: serde_json::Value,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_ap_bill($1::UUID, $2, $3, $4::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(vendor_id)
    .bind(currency)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

/// Receive 10 of the PO's first line, posting via post_po_receipt.
async fn receive_10(pool: &PgPool, sx: &Setup) {
    let lines = serde_json::json!([{ "po_line_id": sx.po_line_id, "qty_received": 10 }]);
    call_receipt(pool, &sx.po_id, lines, "2026-04-15")
        .await
        .expect("receipt");
}

// ============================================================
// po_match clearance flow
// ============================================================

#[tokio::test]
async fn po_match_full_clearance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    // After receipt: ap_unsettled CR 1000, ap untouched.
    assert_eq!(balance(&pool, sx.ven_unsettled).await, -1000);
    assert_eq!(balance(&pool, sx.ap).await, 0);

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 100, "amount": 1000
    }]);
    call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key)
        .await
        .expect("bill");

    // ap_unsettled cleared → ap.
    assert_eq!(balance(&pool, sx.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, sx.ap).await, -1000);
}

#[tokio::test]
async fn po_match_partial_then_remainder() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    // Bill 4 of 10.
    let k1 = fresh_uuid(&pool).await;
    call_bill(
        &pool, &sx.vendor_id, "USD",
        serde_json::json!([{
            "kind": "po_match", "po_line_id": sx.po_line_id,
            "qty": 4, "unit_cost": 100, "amount": 400
        }]),
        "2026-04-16", &k1,
    )
    .await
    .expect("bill 1");
    assert_eq!(balance(&pool, sx.ven_unsettled).await, -600);
    assert_eq!(balance(&pool, sx.ap).await, -400);

    // Bill remaining 6.
    let k2 = fresh_uuid(&pool).await;
    call_bill(
        &pool, &sx.vendor_id, "USD",
        serde_json::json!([{
            "kind": "po_match", "po_line_id": sx.po_line_id,
            "qty": 6, "unit_cost": 100, "amount": 600
        }]),
        "2026-04-17", &k2,
    )
    .await
    .expect("bill 2");
    assert_eq!(balance(&pool, sx.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, sx.ap).await, -1000);
}

#[tokio::test]
async fn po_match_over_received_not_billed_p0024() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    // Try to bill 11 against received 10.
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 11, "unit_cost": 100, "amount": 1100
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn po_match_unit_cost_mismatch_p0024() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    let key = fresh_uuid(&pool).await;
    // PO unit_cost = 100; bill 99.
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 99, "amount": 990
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn po_match_amount_mismatch_p0024() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    let key = fresh_uuid(&pool).await;
    // qty × unit_cost = 1000 but amount = 999.
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 100, "amount": 999
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn po_match_double_bill_same_line_p0024() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    // Bill all 10 first.
    let k1 = fresh_uuid(&pool).await;
    call_bill(
        &pool, &sx.vendor_id, "USD",
        serde_json::json!([{
            "kind": "po_match", "po_line_id": sx.po_line_id,
            "qty": 10, "unit_cost": 100, "amount": 1000
        }]),
        "2026-04-16", &k1,
    )
    .await
    .expect("bill 1");

    // Try to bill 1 more — over received-not-billed.
    let k2 = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 1, "unit_cost": 100, "amount": 100
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-17", &k2).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn po_match_wrong_vendor_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx_a = build_setup(&pool, "S-A").await;
    receive_10(&pool, &sx_a).await;
    // Vendor B exists too.
    let ven_b = fresh_vendor(&pool, "S-B", "USD").await;
    let _ = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&ven_b), "credit",
    )
    .await;
    let _ = open_account(
        &pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&ven_b), "credit",
    )
    .await;

    // Try to bill A's po_line under vendor B.
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx_a.po_line_id,
        "qty": 10, "unit_cost": 100, "amount": 1000
    }]);
    expect_sqlstate("P0025", || async {
        call_bill(&pool, &ven_b, "USD", lines, "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn po_match_currency_mismatch_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    // Open EUR ap for the vendor so we get past the ap-lookup.
    let _ = open_account(
        &pool, "ap", "value", Some("EUR"), None, None, Some(&sx.vendor_id), "credit",
    )
    .await;

    // Try to bill the USD po_line with currency=EUR.
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 100, "amount": 1000
    }]);
    expect_sqlstate("P0025", || async {
        call_bill(&pool, &sx.vendor_id, "EUR", lines, "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

// ============================================================
// service mode
// ============================================================

#[tokio::test]
async fn service_single_line_clears_to_ap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "SVC-1", "USD").await;
    let ap = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    // Service expense goes to a labor_expense USD account (existing
    // account_kind, simplest fit for "professional services" placeholder).
    let expense = open_account(
        &pool, "labor_expense", "value", Some("USD"), None, None, None, "debit",
    )
    .await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "service", "expense_account_id": expense, "amount": 750
    }]);
    call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key)
        .await
        .expect("bill");

    assert_eq!(balance(&pool, expense).await, 750);
    assert_eq!(balance(&pool, ap).await, -750);
}

#[tokio::test]
async fn service_multi_line_two_expense_accounts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "SVC-2", "USD").await;
    let ap = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    // Use two distinct expense-style accounts: labor_expense (debit-normal)
    // and inv_adj_expense (unrestricted). Both accept a debit at zero
    // balance; the fixture already seeds inv_adj_expense USD.
    let exp_labor = open_account(
        &pool, "labor_expense", "value", Some("USD"), None, None, None, "debit",
    )
    .await;
    let exp_adj =
        account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([
        { "kind": "service", "expense_account_id": exp_labor, "amount": 300 },
        { "kind": "service", "expense_account_id": exp_adj,   "amount": 200 },
    ]);
    call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key)
        .await
        .expect("bill");

    assert_eq!(balance(&pool, exp_labor).await, 300);
    assert_eq!(balance(&pool, exp_adj).await, 200);
    assert_eq!(balance(&pool, ap).await, -500);
}

#[tokio::test]
async fn service_closed_expense_account_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "SVC-3", "USD").await;
    let _ = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let expense = open_account(
        &pool, "labor_expense", "value", Some("USD"), None, None, None, "debit",
    )
    .await;
    sqlx::query("UPDATE accounts SET is_closed = TRUE, closed_at = clock_timestamp() WHERE id = $1")
        .bind(expense)
        .execute(&pool)
        .await
        .unwrap();

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "service", "expense_account_id": expense, "amount": 100
    }]);
    expect_sqlstate("P0025", || async {
        call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn service_currency_mismatch_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "SVC-4", "USD").await;
    let _ = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    // EUR expense account; bill is USD.
    let exp_eur = open_account(
        &pool, "labor_expense", "value", Some("EUR"), None, None, None, "debit",
    )
    .await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "service", "expense_account_id": exp_eur, "amount": 100
    }]);
    expect_sqlstate("P0025", || async {
        call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

// ============================================================
// Mixed bill: po_match + service in one document
// ============================================================

#[tokio::test]
async fn mixed_bill_po_match_plus_service() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "MIX").await;
    receive_10(&pool, &sx).await;
    let exp = open_account(
        &pool, "labor_expense", "value", Some("USD"), None, None, None, "debit",
    )
    .await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([
        { "kind": "po_match", "po_line_id": sx.po_line_id,
          "qty": 10, "unit_cost": 100, "amount": 1000 },
        { "kind": "service", "expense_account_id": exp, "amount": 50 },
    ]);
    call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key)
        .await
        .expect("bill");

    assert_eq!(balance(&pool, sx.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, exp).await, 50);
    assert_eq!(balance(&pool, sx.ap).await, -1050);
}

// ============================================================
// Validation paths
// ============================================================

#[tokio::test]
async fn idempotency_replay_returns_same_doc_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 100, "amount": 1000
    }]);
    let id1 = call_bill(&pool, &sx.vendor_id, "USD", lines.clone(), "2026-04-16", &key)
        .await
        .expect("first");
    let id2 = call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key)
        .await
        .expect("replay");
    assert_eq!(id1, id2);
    // Balances unchanged after replay (still cleared once).
    assert_eq!(balance(&pool, sx.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, sx.ap).await, -1000);
}

#[tokio::test]
async fn closed_period_raises_p0005() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sx = build_setup(&pool, "S1").await;
    receive_10(&pool, &sx).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 100, "amount": 1000
    }]);
    // 2026-03 is closed in the fixture.
    expect_sqlstate("P0005", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-03-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn unknown_vendor_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let bogus = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    // No accounts opened for bogus; we expect P0025 before the ap lookup.
    let lines = serde_json::json!([{
        "kind": "service", "expense_account_id": 1, "amount": 1
    }]);
    expect_sqlstate("P0025", || async {
        call_bill(&pool, &bogus, "USD", lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn no_ap_account_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "NOAP", "USD").await;
    // Don't open an ap account.
    let expense = open_account(
        &pool, "labor_expense", "value", Some("USD"), None, None, None, "debit",
    )
    .await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "service", "expense_account_id": expense, "amount": 100
    }]);
    expect_sqlstate("P0010", || async {
        call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn empty_lines_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "EMPTY", "USD").await;
    let _ = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([]);
    expect_sqlstate("P0025", || async {
        call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn unknown_line_kind_p0025() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let vendor = fresh_vendor(&pool, "BADKIND", "USD").await;
    let _ = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "unknown_thing", "amount": 10
    }]);
    // Table CHECK fires at INSERT time inside the function with code 23514.
    // Or function may pre-empt with P0025 — either is acceptable; test
    // accepts the function-layer code.
    let result = call_bill(&pool, &vendor, "USD", lines, "2026-04-15", &key).await;
    assert!(result.is_err(), "expected error for unknown kind");
    let err = result.unwrap_err();
    let db = err.as_database_error().expect("db error");
    let code = db.code().expect("code").to_string();
    assert!(
        code == "P0025" || code == "23514",
        "expected P0025 or 23514, got {code}: {}",
        db.message()
    );
}

// ============================================================
// Three-way match tolerance windows (acct-7mc)
// ============================================================

async fn set_vendor_tolerance(pool: &PgPool, vendor_id: &str, pct: &str) {
    sqlx::query(
        "UPDATE vendors SET unit_cost_tolerance_pct = $1::NUMERIC WHERE id = $2::UUID",
    )
    .bind(pct)
    .bind(vendor_id)
    .execute(pool)
    .await
    .expect("set tolerance");
}

async fn match_tol_acct(pool: &PgPool) -> i64 {
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
async fn within_tolerance_unfavorable_absorbs_to_variance() {
    // PO unit_cost=100, vendor tolerance=2%. Bill at unit_cost=102 (2%
    // exactly). variance_match_tolerance debits 20 (loss); ap credits
    // 1020 total (1000 base + 20 absorption).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sx = build_setup(&pool, "VEN-TOL").await;
    receive_10(&pool, &sx).await;
    set_vendor_tolerance(&pool, &sx.vendor_id, "2.0").await;
    let mt = match_tol_acct(&pool).await;
    let mt_before = balance(&pool, mt).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 102, "amount": 1020
    }]);
    call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key).await
        .expect("within tolerance");

    assert_eq!(balance(&pool, sx.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, sx.ap).await, -1020);
    assert_eq!(balance(&pool, mt).await - mt_before, 20);

    assert_invariants_hold(&pool, "within_tolerance_unfavorable_absorbs_to_variance").await;
}

#[tokio::test]
async fn within_tolerance_favorable_absorbs_to_variance() {
    // Bill at unit_cost=98 (2% below). Favorable: variance credits 20
    // (gain); ap credits 980 (1000 base, then drain 20).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sx = build_setup(&pool, "VEN-TOL-FAV").await;
    receive_10(&pool, &sx).await;
    set_vendor_tolerance(&pool, &sx.vendor_id, "2.0").await;
    let mt = match_tol_acct(&pool).await;
    let mt_before = balance(&pool, mt).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 98, "amount": 980
    }]);
    call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key).await
        .expect("within tolerance fav");

    assert_eq!(balance(&pool, sx.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, sx.ap).await, -980);
    assert_eq!(balance(&pool, mt).await - mt_before, -20);
}

#[tokio::test]
async fn out_of_tolerance_still_raises_p0024() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sx = build_setup(&pool, "VEN-OOT").await;
    receive_10(&pool, &sx).await;
    set_vendor_tolerance(&pool, &sx.vendor_id, "2.0").await;

    let key = fresh_uuid(&pool).await;
    // 5% over → 5 > 2 → reject.
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 105, "amount": 1050
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines.clone(), "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn zero_tolerance_default_is_strict() {
    // Default vendor tolerance = 0; any unit_cost mismatch raises.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sx = build_setup(&pool, "VEN-STRICT").await;
    receive_10(&pool, &sx).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 101, "amount": 1010
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &sx.vendor_id, "USD", lines.clone(), "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn within_tolerance_exact_at_boundary_succeeds() {
    // Tolerance = 5%; bill at exactly 5% over (not >).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sx = build_setup(&pool, "VEN-BOUND").await;
    receive_10(&pool, &sx).await;
    set_vendor_tolerance(&pool, &sx.vendor_id, "5.0").await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": sx.po_line_id,
        "qty": 10, "unit_cost": 105, "amount": 1050
    }]);
    call_bill(&pool, &sx.vendor_id, "USD", lines, "2026-04-16", &key).await
        .expect("at-boundary succeeds");

    assert_eq!(balance(&pool, sx.ap).await, -1050);
}

// ============================================================
// acct-nuw7 — zero-baseline tolerance check
// ============================================================

#[tokio::test]
async fn zero_cost_po_line_with_nonzero_bill_raises_p0024_not_22012() {
    // PO line at unit_cost=0 (free-good / placeholder); vendor
    // tolerance > 0; bill at unit_cost=10. Pre-fix the ABS divide
    // at mig 0090 line 181 trips SQLSTATE 22012 ("division by
    // zero"). Post-fix the zero-baseline arm routes through P0024
    // ("out of tolerance by definition").
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc = id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(&pool, "VEN-ZC", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let po_line = fresh_po_line(&pool, &po, 1, &sku, &loc, 100, 0, "USD").await;

    let _ven_unsettled = open_account(
        &pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let _ap = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;

    set_vendor_tolerance(&pool, &vendor, "5.0").await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{
        "kind": "po_match", "po_line_id": po_line,
        "qty": 10, "unit_cost": 10, "amount": 100
    }]);
    expect_sqlstate("P0024", || async {
        call_bill(&pool, &vendor, "USD", lines.clone(), "2026-04-16", &key).await.map(|_| ())
    })
    .await;
}
