//! `acct-quk` — post_po_return / vendor return matrix.
//!
//! Coverage:
//!   * Happy path WAC: qty back to vendor_pool, value out of inv_value_raw,
//!     ap reduced — no PPV.
//!   * Happy path standard with po > std: PPV reverse-routes the diff.
//!   * Happy path standard with po < std: PPV reverse-routes the negative
//!     diff (flipped legs).
//!   * Partial then remainder: cumulative-not-yet-returned drains to 0.
//!   * Over-return cumulative raises P0047.
//!   * Wrong vendor ownership raises P0046.
//!   * Unknown recv_line raises P0046.
//!   * Idempotency replay returns existing id.
//!   * Validation: P0046 for empty lines, qty<=0, unknown vendor.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffold (mirror of po_receipt's plus ap-side accounts)
// ============================================================

#[allow(dead_code)]
struct ReturnScaffold {
    vendor_id: String,
    po_id: String,
    po_line_id: String,
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_unsettled: i64,
    ven_ap: i64,
    var_ppv: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
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
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert PO: {e}"))
}

#[allow(clippy::too_many_arguments)]
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

/// SKU-A (standard, std=100) scaffold. po_unit_cost configurable so
/// PPV path can be exercised. Vendor + PO + PO line + ap-side accounts
/// scaffolded.
async fn scaffold_standard(
    pool: &PgPool,
    suffix: &str,
    qty_ordered: i64,
    po_unit_cost: i64,
) -> ReturnScaffold {
    let sku = id_text(pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(pool, &format!("VEN-RET-{suffix}"), "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku, &loc, qty_ordered, po_unit_cost, "USD").await;

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
    let ven_ap = open_account(
        pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let var_ppv = account_id_by_kind_currency(pool, "variance_ppv", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    ReturnScaffold {
        vendor_id: vendor,
        po_id: po,
        po_line_id: po_line,
        sku_id: sku,
        loc_id: loc,
        qty_acct,
        val_acct,
        ven_qty,
        ven_unsettled,
        ven_ap,
        var_ppv,
        creation_void_qty,
        creation_void_val,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Receive `qty` units against the SO line, then bill them so ap carries
/// a credit balance for the return debit memo to debit against. Returns
/// the po_receipt_lines.id of the receipt line.
async fn receive_and_bill(pool: &PgPool, s: &ReturnScaffold, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"po_line_id": s.po_line_id, "qty_received": qty}]);
    sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.po_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_po_receipt: {e}"));

    let recv_line_id: String = sqlx::query_scalar(
        "SELECT prl.id::text FROM po_receipt_lines prl
          WHERE prl.po_line_id = $1::UUID
          ORDER BY prl.id DESC LIMIT 1",
    )
    .bind(&s.po_line_id)
    .fetch_one(pool)
    .await
    .expect("recv_line lookup");

    // Bill it so ap_unsettled clears to ap.
    let unit_cost: i64 = sqlx::query_scalar(
        "SELECT unit_cost FROM purchase_order_lines WHERE id = $1::UUID",
    )
    .bind(&s.po_line_id)
    .fetch_one(pool)
    .await
    .expect("po_line unit_cost");

    let bill_lines = json!([{
        "kind": "po_match",
        "po_line_id": s.po_line_id,
        "qty": qty,
        "unit_cost": unit_cost,
        "amount": qty * unit_cost
    }]);
    let posted_by2 = fresh_uuid(pool).await;
    let key2 = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-16'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.vendor_id)
    .bind(bill_lines)
    .bind(&posted_by2)
    .bind(&key2)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_ap_bill: {e}"));

    recv_line_id
}

async fn call_return(
    pool: &PgPool,
    vendor_id: &str,
    lines: serde_json::Value,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(vendor_id)
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
async fn standard_sku_po_eq_std_no_ppv() {
    // SKU-A std=100, po=100 → no PPV on receipt or return.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "EQUAL", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    let qty_after_recv = balance(&pool, s.qty_acct).await;
    let val_after_recv = balance(&pool, s.val_acct).await;
    let ap_after_bill = balance(&pool, s.ven_ap).await;
    let var_ppv_before = balance(&pool, s.var_ppv).await;

    // Receipt+bill: qty +10, value +1000, ap_unsettled cleared,
    // ap=1000 (credit-normal: stored as -1000 via debits-credits).
    assert_eq!(qty_after_recv, 10);
    assert_eq!(val_after_recv, 1000);
    assert_eq!(ap_after_bill, -1000);

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 10}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return");

    // Qty back to vendor_pool; value out of inv_value_raw; ap drained.
    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
    // No PPV movement.
    assert_eq!(balance(&pool, s.var_ppv).await, var_ppv_before);

    assert_invariants_hold(&pool, "standard_sku_po_eq_std_no_ppv").await;
}

#[tokio::test]
async fn standard_sku_po_gt_std_ppv_reverses() {
    // SKU-A std=100, po=120 → favorable PPV on receipt
    // (inventory at 100, ap_unsettled at 120; 20*qty to variance_ppv DR).
    // On return PPV must reverse symmetrically: ap drains by 1200 total
    // (1000 inv + 200 PPV); inv_value_raw drops by 1000; variance_ppv
    // drains by 200 (CR → balance reduces).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "POGT", 10, 120).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    // After receipt+bill: inv_value_raw = 10*100 = 1000; ap = 1200;
    // variance_ppv += 200 (debit-side, so balance goes up by 200).
    assert_eq!(balance(&pool, s.val_acct).await, 1000);
    assert_eq!(balance(&pool, s.ven_ap).await, -1200);
    let var_ppv_post_receipt = balance(&pool, s.var_ppv).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 10}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return");

    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
    // PPV drains back to pre-receipt level (variance reverses).
    let var_ppv_after_return = balance(&pool, s.var_ppv).await;
    assert_eq!(var_ppv_after_return - var_ppv_post_receipt, -200);
}

#[tokio::test]
async fn standard_sku_po_lt_std_ppv_reverses() {
    // SKU-A std=100, po=80 → unfavorable PPV on receipt
    // (inventory at 100, ap_unsettled at 80; ap_unsettled DR 20*qty
    // / variance_ppv CR — variance balance drops). On return:
    // mirror — variance_ppv DR 200 / ap CR 200.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "POLT", 10, 80).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    assert_eq!(balance(&pool, s.val_acct).await, 1000); // 10*std
    assert_eq!(balance(&pool, s.ven_ap).await, -800);   // 10*po
    let var_ppv_post_receipt = balance(&pool, s.var_ppv).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 10}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return");

    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
    let var_ppv_after_return = balance(&pool, s.var_ppv).await;
    // Variance balance INCREASES on return by +200 (back to pre-receipt).
    assert_eq!(var_ppv_after_return - var_ppv_post_receipt, 200);
}

// ============================================================
// Partial returns
// ============================================================

#[tokio::test]
async fn partial_then_remainder_drains_to_zero() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "PARTIAL", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    // Return 4 of 10.
    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 4}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return-1");

    assert_eq!(balance(&pool, s.qty_acct).await, 6);
    assert_eq!(balance(&pool, s.val_acct).await, 600);
    assert_eq!(balance(&pool, s.ven_ap).await, -600);

    // Return remaining 6.
    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 6}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return-2");

    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
}

#[tokio::test]
async fn over_return_cumulative_raises_p0047() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "OVER", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 7}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return-1");

    let over_lines = json!([{"recv_line_id": recv_line, "qty_returned": 4}]);
    expect_sqlstate("P0047", || async {
        call_return(&pool, &s.vendor_id, over_lines.clone()).await
    })
    .await;
}

// ============================================================
// Idempotency
// ============================================================

#[tokio::test]
async fn idempotency_replay_returns_existing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "IDEMP", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 5}]);

    let id1: String = sqlx::query_scalar(
        "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.vendor_id)
    .bind(lines.clone())
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("return-1");

    let id2: String = sqlx::query_scalar(
        "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.vendor_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("return-2 replay");

    assert_eq!(id1, id2);
    // Only one return drained (qty=5 not qty=10).
    assert_eq!(balance(&pool, s.qty_acct).await, 5);
}

// ============================================================
// Validation
// ============================================================

#[tokio::test]
async fn unknown_vendor_raises_p0046() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    let bogus_recv = fresh_uuid(&pool).await;
    let lines = json!([{"recv_line_id": bogus_recv, "qty_returned": 1}]);
    expect_sqlstate("P0046", || async {
        call_return(&pool, &bogus, lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn empty_lines_raises_p0046() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "EMPTY", 10, 100).await;
    expect_sqlstate("P0046", || async {
        call_return(&pool, &s.vendor_id, json!([])).await
    })
    .await;
}

#[tokio::test]
async fn unknown_recv_line_raises_p0046() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "BAD-RECV", 10, 100).await;
    let bogus_recv = fresh_uuid(&pool).await;
    let lines = json!([{"recv_line_id": bogus_recv, "qty_returned": 1}]);
    expect_sqlstate("P0046", || async {
        call_return(&pool, &s.vendor_id, lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn wrong_vendor_ownership_raises_p0046() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "WRONG-OWN", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    let other_vendor = fresh_vendor(&pool, "VEN-OTHER", "USD").await;
    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 1}]);
    expect_sqlstate("P0046", || async {
        call_return(&pool, &other_vendor, lines.clone()).await
    })
    .await;
}

#[tokio::test]
async fn qty_returned_zero_raises_p0046() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "ZERO", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 0}]);
    expect_sqlstate("P0046", || async {
        call_return(&pool, &s.vendor_id, lines.clone()).await
    })
    .await;
}
