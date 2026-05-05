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

// ============================================================
// State-aware routing (acct-tk7)
// ============================================================

/// Receive `qty` units against the PO line WITHOUT issuing a bill.
/// Returns the po_receipt_lines.id of the receipt line.
async fn receive_no_bill(pool: &PgPool, s: &ReturnScaffold, qty: i64) -> String {
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

    sqlx::query_scalar(
        "SELECT prl.id::text FROM po_receipt_lines prl
          WHERE prl.po_line_id = $1::UUID
          ORDER BY prl.id DESC LIMIT 1",
    )
    .bind(&s.po_line_id)
    .fetch_one(pool)
    .await
    .expect("recv_line lookup")
}

async fn bill_qty(pool: &PgPool, s: &ReturnScaffold, qty: i64) {
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
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-16'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.vendor_id)
    .bind(bill_lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_ap_bill: {e}"));
}

async fn split_columns(pool: &PgPool, return_id: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT
           COALESCE(SUM(qty_to_ap_unsettled), 0)::BIGINT,
           COALESCE(SUM(qty_to_ap), 0)::BIGINT
         FROM po_return_lines WHERE return_id = $1::UUID",
    )
    .bind(return_id)
    .fetch_one(pool)
    .await
    .expect("split_columns")
}

#[tokio::test]
async fn pre_bill_return_routes_to_ap_unsettled() {
    // Receive 10 (no bill). Return 10. Should drain ap_unsettled, not ap.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "PRE-BILL", 10, 100).await;
    let recv_line = receive_no_bill(&pool, &s, 10).await;

    // After receipt: ap_unsettled = -1000 (credit balance via debits-credits).
    assert_eq!(balance(&pool, s.ven_unsettled).await, -1000);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 10}]);
    let return_id = call_return(&pool, &s.vendor_id, lines).await.expect("return");

    // ap_unsettled drained to 0; ap untouched; qty_to_ap_unsettled = 10.
    assert_eq!(balance(&pool, s.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
    assert_eq!(balance(&pool, s.qty_acct).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);

    let (to_us, to_ap) = split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 10);
    assert_eq!(to_ap, 0);

    assert_invariants_hold(&pool, "pre_bill_return_routes_to_ap_unsettled").await;
}

#[tokio::test]
async fn pre_bill_return_with_ppv_drains_unsettled_and_ppv() {
    // Receive 10 with po=120, std=100 → ap_unsettled credit = 1200, variance_ppv += 200.
    // Return 10 pre-bill → ap_unsettled drains 1200; variance_ppv reverses 200.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "PRE-BILL-PPV", 10, 120).await;
    let recv_line = receive_no_bill(&pool, &s, 10).await;

    assert_eq!(balance(&pool, s.ven_unsettled).await, -1200);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
    let var_post_recv = balance(&pool, s.var_ppv).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 10}]);
    let return_id = call_return(&pool, &s.vendor_id, lines).await.expect("return");

    assert_eq!(balance(&pool, s.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    // Variance reverses by -200.
    assert_eq!(balance(&pool, s.var_ppv).await - var_post_recv, -200);

    let (to_us, to_ap) = split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 10);
    assert_eq!(to_ap, 0);
}

#[tokio::test]
async fn partial_bill_then_return_splits_routing() {
    // Receive 10, bill 6. Return 7.
    // Routing: 4 to ap_unsettled (un-billed remainder), 3 to ap.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "SPLIT", 10, 100).await;
    let recv_line = receive_no_bill(&pool, &s, 10).await;
    bill_qty(&pool, &s, 6).await;

    // After receipt+partial-bill: ap_unsettled = -400 (10-6=4 unbilled);
    // ap = -600 (6 billed).
    assert_eq!(balance(&pool, s.ven_unsettled).await, -400);
    assert_eq!(balance(&pool, s.ven_ap).await, -600);

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 7}]);
    let return_id = call_return(&pool, &s.vendor_id, lines).await.expect("return");

    // ap_unsettled drains by 4*100=400 → 0;
    // ap drains by 3*100=300 → -300.
    assert_eq!(balance(&pool, s.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, -300);
    // Inventory drains by 7 (full qty regardless of route).
    assert_eq!(balance(&pool, s.qty_acct).await, 3);
    assert_eq!(balance(&pool, s.val_acct).await, 300);

    let (to_us, to_ap) = split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 4);
    assert_eq!(to_ap, 3);
}

#[tokio::test]
async fn cumulative_across_multiple_returns() {
    // Receive 10 (no bill). Return 4 (all to unsettled). Bill 6. Return 4 (all to ap).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "CUM", 10, 100).await;
    let recv_line = receive_no_bill(&pool, &s, 10).await;

    let lines1 = json!([{"recv_line_id": recv_line, "qty_returned": 4}]);
    let r1 = call_return(&pool, &s.vendor_id, lines1).await.expect("return-1");
    let (a1, b1) = split_columns(&pool, &r1).await;
    assert_eq!(a1, 4);
    assert_eq!(b1, 0);

    // After return 1: ap_unsettled = -600 (10-4 received-unbilled-unreturned).
    assert_eq!(balance(&pool, s.ven_unsettled).await, -600);

    // Bill 6 (allowed: avail = 10-0-4 = 6).
    bill_qty(&pool, &s, 6).await;
    assert_eq!(balance(&pool, s.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, s.ven_ap).await, -600);

    // Now return 4 more (fully on ap side; unsettled remainder = 10-6-4 = 0).
    let lines2 = json!([{"recv_line_id": recv_line, "qty_returned": 4}]);
    let r2 = call_return(&pool, &s.vendor_id, lines2).await.expect("return-2");
    let (a2, b2) = split_columns(&pool, &r2).await;
    assert_eq!(a2, 0);
    assert_eq!(b2, 4);

    // ap drains by 400 → -200.
    assert_eq!(balance(&pool, s.ven_ap).await, -200);
}

#[tokio::test]
async fn over_bill_after_return_to_unsettled_rejected() {
    // Receive 10, return 3 to ap_unsettled (no bill yet). Try billing 10 — rejected.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "OVER-BILL", 10, 100).await;
    let recv_line = receive_no_bill(&pool, &s, 10).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 3}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return");

    // Try to bill 10 — should fail because available = 10-0-3 = 7.
    let bill_lines = json!([{
        "kind": "po_match",
        "po_line_id": s.po_line_id,
        "qty": 10,
        "unit_cost": 100,
        "amount": 1000
    }]);
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0024", || async {
        sqlx::query(
            "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-16'::DATE,
                                  $3::UUID, $4::UUID, NULL)",
        )
        .bind(&s.vendor_id)
        .bind(bill_lines.clone())
        .bind(&posted_by)
        .bind(&key)
        .execute(&pool)
        .await
        .map(|_| String::new())
    })
    .await;
}

#[tokio::test]
async fn pre_bill_return_with_po_lt_std_drains_correctly() {
    // po=80, std=100. Receive 10 → ap_unsettled CR 800; variance_ppv += -200
    // (variance balance drops by 200 from baseline). Pre-bill return 10:
    // event order PPV(reversal) BEFORE value-leg matters because ap_unsettled
    // is credit-normal. Variance reverses, ap_unsettled drains.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "POLT-PRE", 10, 80).await;
    let recv_line = receive_no_bill(&pool, &s, 10).await;

    assert_eq!(balance(&pool, s.ven_unsettled).await, -800);
    assert_eq!(balance(&pool, s.val_acct).await, 1000);
    let var_post_recv = balance(&pool, s.var_ppv).await;

    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 10}]);
    call_return(&pool, &s.vendor_id, lines).await.expect("return");

    assert_eq!(balance(&pool, s.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, s.val_acct).await, 0);
    // Variance increases by 200 (back to pre-receipt baseline).
    assert_eq!(balance(&pool, s.var_ppv).await - var_post_recv, 200);
}

#[tokio::test]
async fn override_closed_period_allows_back_post() {
    // Lock a period in the past, then verify the return rejects without
    // override and accepts with override.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_standard(&pool, "OVERRIDE", 10, 100).await;
    let recv_line = receive_and_bill(&pool, &s, 10).await;

    // Close the period covering the return business_date 2026-04-25.
    sqlx::query(
        "UPDATE periods SET closed_at = clock_timestamp()
          WHERE opens_at <= '2026-04-25'::DATE AND closes_at >= '2026-04-25'::DATE",
    )
    .execute(&pool)
    .await
    .expect("close period");

    let posted_by = fresh_uuid(&pool).await;
    let key1 = fresh_uuid(&pool).await;
    let lines = json!([{"recv_line_id": recv_line, "qty_returned": 5}]);

    // Without override → P0005.
    expect_sqlstate("P0005", || async {
        sqlx::query_scalar::<_, String>(
            "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                    $3::UUID, $4::UUID, NULL, FALSE)::text",
        )
        .bind(&s.vendor_id)
        .bind(lines.clone())
        .bind(&posted_by)
        .bind(&key1)
        .fetch_one(&pool)
        .await
    })
    .await;

    // With override → succeeds.
    let key2 = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                $3::UUID, $4::UUID, NULL, TRUE)::text",
    )
    .bind(&s.vendor_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key2)
    .fetch_one(&pool)
    .await
    .expect("override return");

    assert_eq!(balance(&pool, s.qty_acct).await, 5);
}

// ============================================================
// WAC cost-method coverage (acct-2j4)
// ============================================================

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

#[allow(dead_code)]
struct WacScaffold {
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
}

async fn scaffold_wac(
    pool: &PgPool,
    suffix: &str,
    qty_ordered: i64,
    po_unit_cost: i64,
) -> WacScaffold {
    let sku_id = fresh_sku(pool, &format!("SKU-WAC-{suffix}"), "wac_perpetual").await;
    let loc_id = fresh_location(pool, &format!("WAC-{suffix}")).await;
    let vendor = fresh_vendor(pool, &format!("VEN-WAC-{suffix}"), "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku_id, &loc_id, qty_ordered, po_unit_cost, "USD").await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let ven_qty = open_account(
        pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit",
    )
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

    WacScaffold {
        vendor_id: vendor,
        po_id: po,
        po_line_id: po_line,
        sku_id,
        loc_id,
        qty_acct,
        val_acct,
        ven_qty,
        ven_unsettled,
        ven_ap,
        var_ppv,
    }
}

async fn receive_wac_no_bill(pool: &PgPool, w: &WacScaffold, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"po_line_id": w.po_line_id, "qty_received": qty}]);
    sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)",
    )
    .bind(&w.po_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_po_receipt(wac): {e}"));

    sqlx::query_scalar(
        "SELECT prl.id::text FROM po_receipt_lines prl
          WHERE prl.po_line_id = $1::UUID
          ORDER BY prl.id DESC LIMIT 1",
    )
    .bind(&w.po_line_id)
    .fetch_one(pool)
    .await
    .expect("recv_line lookup")
}

async fn bill_wac_qty(pool: &PgPool, w: &WacScaffold, qty: i64) {
    let unit_cost: i64 = sqlx::query_scalar(
        "SELECT unit_cost FROM purchase_order_lines WHERE id = $1::UUID",
    )
    .bind(&w.po_line_id)
    .fetch_one(pool)
    .await
    .expect("po_line unit_cost");

    let bill_lines = json!([{
        "kind": "po_match",
        "po_line_id": w.po_line_id,
        "qty": qty,
        "unit_cost": unit_cost,
        "amount": qty * unit_cost
    }]);
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-16'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&w.vendor_id)
    .bind(bill_lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_ap_bill(wac): {e}"));
}

async fn call_wac_return(
    pool: &PgPool,
    w: &WacScaffold,
    recv_line: &str,
    qty: i64,
) -> String {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"recv_line_id": recv_line, "qty_returned": qty}]);
    sqlx::query_scalar(
        "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&w.vendor_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("wac return")
}

#[tokio::test]
async fn wac_perpetual_post_bill_return_no_ppv() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let w = scaffold_wac(&pool, "POSTBILL", 10, 100).await;
    let recv_line = receive_wac_no_bill(&pool, &w, 10).await;
    bill_wac_qty(&pool, &w, 10).await;

    assert_eq!(balance(&pool, w.qty_acct).await, 10);
    assert_eq!(balance(&pool, w.val_acct).await, 1000);
    assert_eq!(balance(&pool, w.ven_ap).await, -1000);
    assert_eq!(balance(&pool, w.ven_unsettled).await, 0);
    let var_before = balance(&pool, w.var_ppv).await;

    call_wac_return(&pool, &w, &recv_line, 10).await;

    assert_eq!(balance(&pool, w.qty_acct).await, 0);
    assert_eq!(balance(&pool, w.val_acct).await, 0);
    assert_eq!(balance(&pool, w.ven_ap).await, 0);
    assert_eq!(balance(&pool, w.var_ppv).await, var_before);

    assert_invariants_hold(&pool, "wac_perpetual_post_bill_return_no_ppv").await;
}

#[tokio::test]
async fn wac_perpetual_pre_bill_return_routes_to_ap_unsettled() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let w = scaffold_wac(&pool, "PREBILL", 10, 100).await;
    let recv_line = receive_wac_no_bill(&pool, &w, 10).await;

    assert_eq!(balance(&pool, w.ven_unsettled).await, -1000);
    assert_eq!(balance(&pool, w.ven_ap).await, 0);
    let var_before = balance(&pool, w.var_ppv).await;

    let return_id = call_wac_return(&pool, &w, &recv_line, 10).await;

    assert_eq!(balance(&pool, w.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, w.ven_ap).await, 0);
    assert_eq!(balance(&pool, w.val_acct).await, 0);
    assert_eq!(balance(&pool, w.var_ppv).await, var_before);

    let (to_us, to_ap) = split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 10);
    assert_eq!(to_ap, 0);
}

#[tokio::test]
async fn wac_perpetual_partial_bill_split_no_ppv() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let w = scaffold_wac(&pool, "WACSPLIT", 10, 100).await;
    let recv_line = receive_wac_no_bill(&pool, &w, 10).await;
    bill_wac_qty(&pool, &w, 6).await;

    assert_eq!(balance(&pool, w.ven_unsettled).await, -400);
    assert_eq!(balance(&pool, w.ven_ap).await, -600);
    let var_before = balance(&pool, w.var_ppv).await;

    let return_id = call_wac_return(&pool, &w, &recv_line, 7).await;

    assert_eq!(balance(&pool, w.ven_unsettled).await, 0);
    assert_eq!(balance(&pool, w.ven_ap).await, -300);
    assert_eq!(balance(&pool, w.qty_acct).await, 3);
    assert_eq!(balance(&pool, w.val_acct).await, 300);
    assert_eq!(balance(&pool, w.var_ppv).await, var_before);

    let (to_us, to_ap) = split_columns(&pool, &return_id).await;
    assert_eq!(to_us, 4);
    assert_eq!(to_ap, 3);
}

// ============================================================
// Multi-line return doc spanning split states (acct-7nv)
// ============================================================

#[tokio::test]
async fn multi_line_return_routes_each_line_independently() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc = id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(&pool, "VEN-MULTI", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let line_a = fresh_po_line(&pool, &po, 1, &sku, &loc, 10, 100, "USD").await;
    let line_b = fresh_po_line(&pool, &po, 2, &sku, &loc, 10, 100, "USD").await;

    let qty_acct = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool, "inv_value_raw", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let _ven_qty = open_account(
        &pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit",
    )
    .await;
    let ven_unsettled = open_account(
        &pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let ven_ap = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"po_line_id": line_a, "qty_received": 10},
        {"po_line_id": line_b, "qty_received": 10}
    ]);
    sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)",
    )
    .bind(&po)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("receipt");

    let recv_a: String = sqlx::query_scalar(
        "SELECT id::text FROM po_receipt_lines WHERE po_line_id = $1::UUID",
    )
    .bind(&line_a)
    .fetch_one(&pool)
    .await
    .expect("recv_a");
    let recv_b: String = sqlx::query_scalar(
        "SELECT id::text FROM po_receipt_lines WHERE po_line_id = $1::UUID",
    )
    .bind(&line_b)
    .fetch_one(&pool)
    .await
    .expect("recv_b");

    let bill_lines = json!([{
        "kind": "po_match",
        "po_line_id": line_a,
        "qty": 10,
        "unit_cost": 100,
        "amount": 1000
    }]);
    let posted_by2 = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-16'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&vendor)
    .bind(bill_lines)
    .bind(&posted_by2)
    .bind(&key2)
    .execute(&pool)
    .await
    .expect("bill A");

    assert_eq!(balance(&pool, ven_ap).await, -1000);
    assert_eq!(balance(&pool, ven_unsettled).await, -1000);

    let return_lines = json!([
        {"recv_line_id": recv_a, "qty_returned": 10},
        {"recv_line_id": recv_b, "qty_returned": 10}
    ]);
    let return_id = call_return(&pool, &vendor, return_lines).await.expect("multi-line return");

    assert_eq!(balance(&pool, ven_ap).await, 0);
    assert_eq!(balance(&pool, ven_unsettled).await, 0);
    assert_eq!(balance(&pool, qty_acct).await, 0);
    assert_eq!(balance(&pool, val_acct).await, 0);

    let line_a_split: (i64, i64) = sqlx::query_as(
        "SELECT qty_to_ap_unsettled::BIGINT, qty_to_ap::BIGINT
         FROM po_return_lines WHERE return_id = $1::UUID AND recv_line_id = $2::UUID",
    )
    .bind(&return_id)
    .bind(&recv_a)
    .fetch_one(&pool)
    .await
    .expect("line_a split");
    assert_eq!(line_a_split, (0, 10));

    let line_b_split: (i64, i64) = sqlx::query_as(
        "SELECT qty_to_ap_unsettled::BIGINT, qty_to_ap::BIGINT
         FROM po_return_lines WHERE return_id = $1::UUID AND recv_line_id = $2::UUID",
    )
    .bind(&return_id)
    .bind(&recv_b)
    .fetch_one(&pool)
    .await
    .expect("line_b split");
    assert_eq!(line_b_split, (10, 0));

    assert_invariants_hold(&pool, "multi_line_return_routes_each_line_independently").await;
}

// ============================================================
// Multi-currency state-aware routing (acct-bh0)
// ============================================================

#[tokio::test]
async fn multi_currency_split_routes_per_currency_partition() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_usd = id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let sku_eur = id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-B").await;
    let loc = id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(&pool, "VEN-FX", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let line_usd = fresh_po_line(&pool, &po, 1, &sku_usd, &loc, 10, 100, "USD").await;
    let line_eur = fresh_po_line(&pool, &po, 2, &sku_eur, &loc, 10, 80, "EUR").await;

    let qty_usd = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let val_usd = account_id_for_selector(
        &pool, "inv_value_raw", Some("SKU-A"), Some("MAIN"), Some("USD"), None,
    )
    .await;
    let qty_eur = open_account(
        &pool, "stock_available", "qty", None, Some(&sku_eur), Some(&loc), None, "debit",
    )
    .await;
    let val_eur = open_account(
        &pool, "inv_value_raw", "value", Some("EUR"), Some(&sku_eur), Some(&loc), None, "debit",
    )
    .await;
    let _ven_qty = open_account(
        &pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit",
    )
    .await;
    let unsettled_usd = open_account(
        &pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let unsettled_eur = open_account(
        &pool, "ap_unsettled", "value", Some("EUR"), None, None, Some(&vendor), "credit",
    )
    .await;
    let ap_usd = open_account(
        &pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    )
    .await;
    let ap_eur = open_account(
        &pool, "ap", "value", Some("EUR"), None, None, Some(&vendor), "credit",
    )
    .await;
    // EUR variance_ppv (SKU-B is standard cost=200; po=80 → PPV credit). Per
    // fixture, variance_* accounts are unrestricted normal_side.
    let _var_ppv_eur = open_account(
        &pool, "variance_ppv", "value", Some("EUR"), None, None, None, "unrestricted",
    )
    .await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([
        {"po_line_id": line_usd, "qty_received": 10},
        {"po_line_id": line_eur, "qty_received": 10}
    ]);
    sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)",
    )
    .bind(&po)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("multi-ccy receipt");

    let recv_usd: String = sqlx::query_scalar(
        "SELECT id::text FROM po_receipt_lines WHERE po_line_id = $1::UUID",
    )
    .bind(&line_usd)
    .fetch_one(&pool)
    .await
    .expect("recv_usd");
    let recv_eur: String = sqlx::query_scalar(
        "SELECT id::text FROM po_receipt_lines WHERE po_line_id = $1::UUID",
    )
    .bind(&line_eur)
    .fetch_one(&pool)
    .await
    .expect("recv_eur");

    let bill_usd = json!([{
        "kind": "po_match", "po_line_id": line_usd,
        "qty": 10, "unit_cost": 100, "amount": 1000
    }]);
    let posted_by2 = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-16'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&vendor)
    .bind(bill_usd)
    .bind(&posted_by2)
    .bind(&key2)
    .execute(&pool)
    .await
    .expect("bill USD");

    assert_eq!(balance(&pool, ap_usd).await, -1000);
    assert_eq!(balance(&pool, unsettled_usd).await, 0);
    assert_eq!(balance(&pool, ap_eur).await, 0);
    assert_eq!(balance(&pool, unsettled_eur).await, -800);

    let return_lines = json!([
        {"recv_line_id": recv_usd, "qty_returned": 10},
        {"recv_line_id": recv_eur, "qty_returned": 10}
    ]);
    call_return(&pool, &vendor, return_lines).await.expect("multi-ccy return");

    assert_eq!(balance(&pool, ap_usd).await, 0);
    assert_eq!(balance(&pool, unsettled_usd).await, 0);
    assert_eq!(balance(&pool, ap_eur).await, 0);
    assert_eq!(balance(&pool, unsettled_eur).await, 0);

    assert_eq!(balance(&pool, qty_usd).await, 0);
    assert_eq!(balance(&pool, val_usd).await, 0);
    assert_eq!(balance(&pool, qty_eur).await, 0);
    assert_eq!(balance(&pool, val_eur).await, 0);

    assert_invariants_hold(&pool, "multi_currency_split_routes_per_currency_partition").await;
}
