//! `acct-7mg` / Slice A — `post_po_receipt` matrix.
//!
//! GRNI semantics (D1): receipts credit `ap_unsettled`, not `ap`.
//! `ap_bill` does the clearance later (`ap_bill.rs` matrix).
//!
//! Strict over-receipt (D3): cumulative qty_received per po_line cannot
//! exceed qty_ordered. Tolerance / over-receipt is Phase 2.
//!
//! Standard SKUs route (po_unit_cost − std)·qty through `variance_ppv`.
//! WAC SKUs post inventory at po_unit_cost; the pool re-averages
//! organically. fifo / lot raise P0006 (acct-8gg).

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Scaffold: open accounts that the inflow workflow needs.
// ============================================================

#[allow(dead_code)]
struct Scaffold {
    vendor_id: String,
    po_id: String,
    po_line_id: String,
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,    // vendor_pool (qty)
    ven_unsettled: i64, // ap_unsettled (value)
    ap: i64,         // ap (value, for bill tests)
    var_ppv: i64,    // variance_ppv (value)
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
    .unwrap_or_else(|e| panic!("insert vendor {code}: {e}"))
}

async fn fresh_po(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status) VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert PO: {e}"))
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
    .unwrap_or_else(|e| panic!("insert po_line: {e}"))
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

/// Build a scaffold for SKU-A/MAIN/USD with one PO line. SKU-A is standard,
/// std_cost=100 from the fixture. inv_value_raw + stock_available accounts
/// for SKU-A/MAIN exist in the fixture; we add the vendor-side accounts
/// + variance_ppv.
async fn scaffold_skua(
    pool: &PgPool,
    qty_ordered: i64,
    po_unit_cost: i64,
) -> Scaffold {
    let sku = id_text(pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(pool, "SUPP-1", "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku, &loc, qty_ordered, po_unit_cost, "USD").await;

    let qty_acct = account_id_stock_available(pool, "SKU-A", "MAIN").await;
    // inv_value_raw for SKU-A/MAIN/USD is already in the fixture; reuse.
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
    // variance_ppv USD is in the fixture; reuse to avoid creating a
    // duplicate that would make post_po_receipt's lookup non-deterministic.
    let var_ppv = account_id_by_kind_currency(pool, "variance_ppv", Some("USD")).await;

    Scaffold {
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
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(po_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Standard SKU paths
// ============================================================

#[tokio::test]
async fn standard_sku_full_receipt_no_ppv() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-A std = 100, po = 100 → no PPV.
    let sf = scaffold_skua(&pool, 10, 100).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 10 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.expect("receipt");

    // qty leg: stock_available DR 10, vendor_pool CR 10
    assert_eq!(balance(&pool, sf.qty_acct).await, 10);
    assert_eq!(balance(&pool, sf.ven_qty).await, -10);
    // value leg: inv_value_raw DR 1000, ap_unsettled CR 1000 (qty * std)
    assert_eq!(balance(&pool, sf.val_acct).await, 1000);
    assert_eq!(balance(&pool, sf.ven_unsettled).await, -1000);
    // No PPV.
    assert_eq!(balance(&pool, sf.var_ppv).await, 0);
    // ap untouched (GRNI: only ap_unsettled credited at receipt).
    assert_eq!(balance(&pool, sf.ap).await, 0);
}

#[tokio::test]
async fn standard_sku_unfavorable_ppv() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-A std = 100, po = 120 → PPV unfavorable, +20 per unit.
    let sf = scaffold_skua(&pool, 10, 120).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 10 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.expect("receipt");

    // inv_value_raw lands at std: 10 × 100 = 1000.
    assert_eq!(balance(&pool, sf.val_acct).await, 1000);
    // variance_ppv DR 200 (unfavorable: po > std).
    assert_eq!(balance(&pool, sf.var_ppv).await, 200);
    // ap_unsettled CR total = std×qty + ppv = 1000 + 200 = 1200 = po×qty.
    assert_eq!(balance(&pool, sf.ven_unsettled).await, -1200);
}

#[tokio::test]
async fn standard_sku_favorable_ppv() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-A std = 100, po = 80 → PPV favorable, −20 per unit.
    let sf = scaffold_skua(&pool, 10, 80).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 10 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.expect("receipt");

    // inv_value_raw lands at std: 10 × 100 = 1000.
    assert_eq!(balance(&pool, sf.val_acct).await, 1000);
    // variance_ppv CR 200 (favorable: po < std). credit balance = -200.
    assert_eq!(balance(&pool, sf.var_ppv).await, -200);
    // ap_unsettled CR total = std×qty − |ppv| = 1000 − 200 = 800 = po×qty.
    assert_eq!(balance(&pool, sf.ven_unsettled).await, -800);
}

// ============================================================
// WAC perpetual: pool re-averages
// ============================================================

#[tokio::test]
async fn wac_perpetual_receipt_pool_re_averages() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-WAC at MAIN; fixture has stock_available + inv_value_raw open
    // for SKU-WAC/MAIN. Need vendor-side accounts.
    let sku = id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-WAC").await;
    let loc = id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(&pool, "SUPP-WAC", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_raw", Some("SKU-WAC"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let ven_qty =
        open_account(&pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit")
            .await;
    let ven_unsettled = open_account(
        &pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;

    // First receipt: 10 units @ $50 → pool 10 / $500.
    let line1 = fresh_po_line(&pool, &po, 1, &sku, &loc, 10, 50, "USD").await;
    let k1 = fresh_uuid(&pool).await;
    call_receipt(
        &pool, &po,
        serde_json::json!([{ "po_line_id": line1, "qty_received": 10 }]),
        "2026-04-15", &k1,
    ).await.expect("receipt 1");

    assert_eq!(balance(&pool, qty_acct).await, 10);
    assert_eq!(balance(&pool, val_acct).await, 500);
    assert_eq!(balance(&pool, ven_qty).await, -10);
    assert_eq!(balance(&pool, ven_unsettled).await, -500);

    // Second receipt: 10 units @ $70 → pool 20 / $1200, avg $60.
    let line2 = fresh_po_line(&pool, &po, 2, &sku, &loc, 10, 70, "USD").await;
    let k2 = fresh_uuid(&pool).await;
    call_receipt(
        &pool, &po,
        serde_json::json!([{ "po_line_id": line2, "qty_received": 10 }]),
        "2026-04-16", &k2,
    ).await.expect("receipt 2");

    assert_eq!(balance(&pool, qty_acct).await, 20);
    assert_eq!(balance(&pool, val_acct).await, 1200);
    // pool average is 1200 / 20 = 60.
    let avg: i64 = sqlx::query_scalar(
        "SELECT (debits_total - credits_total) /
                NULLIF((SELECT debits_total - credits_total FROM accounts WHERE id = $1), 0)
         FROM accounts WHERE id = $2",
    )
    .bind(qty_acct)
    .bind(val_acct)
    .fetch_one(&pool)
    .await
    .expect("avg");
    assert_eq!(avg, 60);
}

// ============================================================
// Multi-line / partial receipts
// ============================================================

#[tokio::test]
async fn multi_line_po_partial_receipt() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua(&pool, 20, 100).await;
    // Add a second line for the same SKU-A/MAIN/USD on the same PO.
    let line2 = fresh_po_line(&pool, &sf.po_id, 2, &sf.sku_id, &sf.loc_id, 30, 100, "USD").await;

    let key = fresh_uuid(&pool).await;
    // Receive only 5 of the first line and 10 of the second.
    let lines = serde_json::json!([
        { "po_line_id": sf.po_line_id, "qty_received": 5 },
        { "po_line_id": line2,         "qty_received": 10 },
    ]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.expect("receipt");

    assert_eq!(balance(&pool, sf.qty_acct).await, 15);
    assert_eq!(balance(&pool, sf.val_acct).await, 1500);

    // A second receipt should be able to take more of either line.
    let key2 = fresh_uuid(&pool).await;
    let lines2 = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 15 }]);
    call_receipt(&pool, &sf.po_id, lines2, "2026-04-16", &key2)
        .await
        .expect("receipt 2");
    assert_eq!(balance(&pool, sf.qty_acct).await, 30);
}

// ============================================================
// Idempotency
// ============================================================

#[tokio::test]
async fn idempotency_replay_returns_same_doc_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua(&pool, 10, 100).await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 10 }]);

    let id1 = call_receipt(&pool, &sf.po_id, lines.clone(), "2026-04-15", &key)
        .await
        .expect("first call");
    let id2 = call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("replay");
    assert_eq!(id1, id2);

    // Second call must NOT have re-posted. Balances unchanged.
    assert_eq!(balance(&pool, sf.qty_acct).await, 10);
    assert_eq!(balance(&pool, sf.val_acct).await, 1000);
}

// ============================================================
// Validation paths (P0022, P0023, P0006, P0005)
// ============================================================

#[tokio::test]
async fn unknown_po_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Need at least one vendor so accounts exist.
    let _ = fresh_vendor(&pool, "S", "USD").await;
    let bogus_po = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let bogus_line = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": bogus_line, "qty_received": 1 }]);

    expect_sqlstate("P0022", || async {
        call_receipt(&pool, &bogus_po, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn unknown_po_line_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua(&pool, 10, 100).await;
    let bogus_line = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": bogus_line, "qty_received": 1 }]);

    expect_sqlstate("P0022", || async {
        call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn po_line_from_different_po_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf1 = scaffold_skua(&pool, 10, 100).await;
    // A second PO on the same vendor with its own line.
    let po2 = fresh_po(&pool, &sf1.vendor_id).await;
    let line_on_po2 =
        fresh_po_line(&pool, &po2, 1, &sf1.sku_id, &sf1.loc_id, 10, 100, "USD").await;

    // Try to receive po2's line against po1.
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": line_on_po2, "qty_received": 1 }]);

    expect_sqlstate("P0022", || async {
        call_receipt(&pool, &sf1.po_id, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn over_receipt_raises_p0023() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // qty_ordered = 10, try to receive 11 in one go.
    let sf = scaffold_skua(&pool, 10, 100).await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 11 }]);

    expect_sqlstate("P0023", || async {
        call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;

    // Also test cumulative over-receipt: receive 6 then try 5 (total 11).
    let sf2 = scaffold_skua_with_code(&pool, "SUPP-OVER", 10, 100).await;
    let k1 = fresh_uuid(&pool).await;
    call_receipt(
        &pool, &sf2.po_id,
        serde_json::json!([{ "po_line_id": sf2.po_line_id, "qty_received": 6 }]),
        "2026-04-15", &k1,
    ).await.expect("first receipt");

    let k2 = fresh_uuid(&pool).await;
    let lines2 = serde_json::json!([{ "po_line_id": sf2.po_line_id, "qty_received": 5 }]);
    expect_sqlstate("P0023", || async {
        call_receipt(&pool, &sf2.po_id, lines2, "2026-04-16", &k2).await.map(|_| ())
    })
    .await;
}

/// Variant of scaffold_skua with caller-supplied vendor code so we can
/// have multiple scaffolds in one test.
async fn scaffold_skua_with_code(
    pool: &PgPool,
    vendor_code: &str,
    qty_ordered: i64,
    po_unit_cost: i64,
) -> Scaffold {
    let sku = id_text(pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(pool, vendor_code, "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku, &loc, qty_ordered, po_unit_cost, "USD").await;

    let qty_acct = account_id_stock_available(pool, "SKU-A", "MAIN").await;
    let val_acct = match sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts WHERE kind='inv_value_raw' AND sku_id=$1::UUID
           AND location_id=$2::UUID AND currency='USD' AND NOT is_closed",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_optional(pool)
    .await
    .unwrap()
    {
        Some(id) => id,
        None => {
            open_account(
                pool, "inv_value_raw", "value", Some("USD"),
                Some(&sku), Some(&loc), None, "debit",
            )
            .await
        }
    };

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
    // variance_ppv USD is in the fixture; reuse to keep the function's
    // lookup deterministic.
    let var_ppv = account_id_by_kind_currency(pool, "variance_ppv", Some("USD")).await;

    Scaffold {
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

#[tokio::test]
async fn fifo_sku_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-FIF (fifo cost_method) is in the fixture but has no value account.
    // Make one + vendor scaffold so the function gets to the cost_method
    // dispatch, which raises P0006.
    let sku = id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-FIF").await;
    let loc = id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(&pool, "SUPP-FIF", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let line = fresh_po_line(&pool, &po, 1, &sku, &loc, 10, 100, "USD").await;
    let _ = open_account(
        &pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit",
    )
    .await;
    let _ = open_account(&pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit")
        .await;
    let _ = open_account(
        &pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": line, "qty_received": 5 }]);
    expect_sqlstate("P0006", || async {
        call_receipt(&pool, &po, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn closed_period_raises_p0005() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Fixture's 2026-03 period is closed.
    let sf = scaffold_skua(&pool, 10, 100).await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 5 }]);

    expect_sqlstate("P0005", || async {
        call_receipt(&pool, &sf.po_id, lines, "2026-03-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn negative_qty_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua(&pool, 10, 100).await;
    let key = fresh_uuid(&pool).await;
    // Function rejects qty_received <= 0 BEFORE the table CHECK fires.
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 0 }]);

    expect_sqlstate("P0022", || async {
        call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn empty_lines_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua(&pool, 10, 100).await;
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([]);

    expect_sqlstate("P0022", || async {
        call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key).await.map(|_| ())
    })
    .await;
}

// ============================================================
// Side effect: receipt rows persisted; transfers carry document_id
// ============================================================

#[tokio::test]
async fn receipt_persists_document_rows_and_links_transfers() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua(&pool, 10, 120).await; // PPV path
    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": sf.po_line_id, "qty_received": 10 }]);
    let doc_id = call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("receipt");

    let header_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM po_receipts WHERE id = $1::UUID")
            .bind(&doc_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(header_count, 1);

    let line_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM po_receipt_lines WHERE receipt_id = $1::UUID")
            .bind(&doc_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(line_count, 1);

    // 3 transfers expected (qty + value + ppv) with this doc_id.
    let xfer_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transfers WHERE document_id = $1::UUID AND document_kind = 'po_receipt'",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(xfer_count, 3, "expected qty + value + ppv events");
}
