//! T1 probes for the D2 standard-cost dispatcher integration
//! (mig 0026, acct-wb75.3.2). Phase D D2 of the convergence plan.
//!
//! Verifies that `_post_posting_lines_apply_event`'s D-block writes
//! one inventory_movements row per qualifying inv_value_* value-leg
//! posting on a STANDARD-cost SKU, with the right shape:
//!   - product_id resolves credit-first per R2.
//!   - quantity signed: negative when credit side is the inventory
//!     leg (value flowing OUT), positive otherwise.
//!   - standard_unit_cost = _resolve_standard_cost_at(sku, date).
//!   - actual_unit_cost = posting amount / abs(qty) (= std for
//!     standard SKU; subledger ↔ GL invariant).
//!   - ppv_amount = 0 (D2 default; PPV stays on variance_ppv
//!     posting_lines per the existing GL pattern).
//!   - event_type via the centralized helper.
//!   - posting_line_id linked back to the source posting.
//!
//! WAC SKUs (D3) and FIFO SKUs (Phase E) intentionally do NOT yet
//! produce movements; this file pins that boundary.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// PO receipt scaffolding — same shape as tests/po_receipt.rs but
// pared down. SKU-A is standard cost, std=100 from the fixture.
// ============================================================

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $1, $2)
         RETURNING id::text",
    )
    .bind(code)
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_po(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open')
         RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn fresh_po_line(
    pool: &PgPool,
    po_id: &str,
    line_no: i32,
    sku_id: &str,
    location_id: &str,
    qty_ordered: i64,
    unit_cost: i64,
    currency: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, $7)
         RETURNING id::text",
    )
    .bind(po_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(location_id)
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
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}/{ledger_kind}: {e}"))
}

struct Scaffold {
    po_id: String,
    po_line_id: String,
}

async fn scaffold_skua_receipt(pool: &PgPool, qty_ordered: i64, po_unit_cost: i64) -> Scaffold {
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-A'")
        .fetch_one(pool)
        .await
        .unwrap();
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool)
        .await
        .unwrap();
    let vendor = fresh_vendor(pool, "VEND-D2", "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku, &loc, qty_ordered, po_unit_cost, "USD").await;

    open_account(pool, "vendor_pool", "qty", None, Some(&vendor), "credit").await;
    open_account(pool, "ap_unsettled", "value", Some("USD"), Some(&vendor), "credit").await;
    open_account(pool, "ap", "value", Some("USD"), Some(&vendor), "credit").await;

    Scaffold {
        po_id: po,
        po_line_id: po_line,
    }
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
// Standard SKU receipt → one inventory_movements row
// ============================================================

#[tokio::test]
async fn po_receipt_writes_one_movement_for_standard_sku() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua_receipt(&pool, 10, 100).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{ "po_line_id": sf.po_line_id, "qty_received": 10 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("po_receipt");

    // One value-leg posting on inv_value_raw → exactly one movement.
    // The qty-leg posting (stock_available ↔ vendor_pool, both qty
    // ledger) does NOT trigger the D-block.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_movements
          WHERE event_type = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1, "expected one receipt movement, got {count}");

    let row: (i32, String, String, i16, String, i64) = sqlx::query_as(
        "SELECT event_type::INT, quantity::TEXT, standard_unit_cost::TEXT,
                cost_book_id, cost_currency, posting_line_id
           FROM inventory_movements
          WHERE event_type = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 1, "event_type=1 (receipt)");
    assert!(
        row.1.starts_with("10"),
        "quantity should be +10 (positive — receipt-like), got {:?}",
        row.1
    );
    assert!(
        row.2.starts_with("100"),
        "standard_unit_cost = SKU-A std (100), got {:?}",
        row.2
    );
    assert_eq!(row.3, 1, "default cost_book_id = 1");
    assert_eq!(row.4, "USD", "cost_currency = posting currency");

    // Linked posting_line is the inv_value_raw value-leg with amount=1000.
    let amount: i64 =
        sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
            .bind(row.5)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(amount, 1000, "linked posting_line.amount = qty × std = 1000");
}

#[tokio::test]
async fn po_receipt_subledger_matches_gl_value() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_skua_receipt(&pool, 7, 100).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{ "po_line_id": sf.po_line_id, "qty_received": 7 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("po_receipt");

    // D5 recon precondition: SUM(quantity × actual_unit_cost) on the
    // subledger ≈ SUM(amount) on inv_value_* posting_lines for the
    // same (sku, location, period). With actual_unit_cost = std for
    // standard cost (no IPV at issue), the two should match exactly.
    let subledger_total: String = sqlx::query_scalar(
        "SELECT SUM(quantity * actual_unit_cost)::TEXT
           FROM inventory_movements
          WHERE event_type = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let gl_total: i64 = sqlx::query_scalar(
        "SELECT SUM(pl.amount)::BIGINT
           FROM posting_lines pl
           JOIN accounts d ON d.id = pl.debit_account_id
          WHERE d.kind = 'inv_value_raw'
            AND pl.reason = 'po_receipt'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        subledger_total.starts_with("700"),
        "subledger total: 7 × 100 = 700, got {subledger_total:?}"
    );
    assert_eq!(gl_total, 700, "GL inv_value_raw debit = 700 (= 7 × std 100)");
}

#[tokio::test]
async fn po_receipt_with_ppv_keeps_subledger_at_standard() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // PO at 110, std at 100 — PPV of 10/unit. The inventory subledger
    // should still record actual_unit_cost = 100 (= what inventory
    // was capitalized at on inv_value_raw). The 10/unit difference
    // lives in a separate variance_ppv posting_line, NOT in
    // ppv_amount on the movement (D2 design call).
    let sf = scaffold_skua_receipt(&pool, 5, 110).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{ "po_line_id": sf.po_line_id, "qty_received": 5 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("po_receipt");

    let row: (String, String, String) = sqlx::query_as(
        "SELECT actual_unit_cost::TEXT, standard_unit_cost::TEXT, ppv_amount::TEXT
           FROM inventory_movements
          WHERE event_type = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        row.0.starts_with("100"),
        "actual_unit_cost stays at std (100) — PPV is on variance_ppv posting_line; got {:?}",
        row.0
    );
    assert!(row.1.starts_with("100"), "standard_unit_cost = 100");
    assert!(
        row.2 == "0" || row.2 == "0.0000",
        "ppv_amount = 0 (D2 default; PPV detail on variance_ppv posting_line); got {:?}",
        row.2
    );

    // Sanity: the variance_ppv posting_line DID post 50 (5 × 10).
    let ppv_total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(pl.amount), 0)::BIGINT
           FROM posting_lines pl
           JOIN accounts d ON d.id = pl.debit_account_id
          WHERE d.kind = 'variance_ppv'
            AND pl.reason = 'ppv'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ppv_total, 50, "variance_ppv = 5 × (110 - 100) = 50");
}

// FIFO via post_po_receipt is supported as of W1 (acct-t1sc, mig 0034).
// Positive cost-flow assertions live in tests/fifo_po_receipt_t1.rs.

// ============================================================
// Non-inventory postings — must not write
// ============================================================

#[tokio::test]
async fn cash_to_revenue_does_not_write_movement() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    let revenue = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("ar_payment", cash, revenue, 100, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("ar_payment");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM inventory_movements")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "value-leg post with no inv_value_* on either side must not write"
    );
}

// ============================================================
// Helper function in isolation
// ============================================================

#[tokio::test]
async fn event_type_helper_maps_known_reasons() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cases: Vec<(&str, f64, Option<i16>)> = vec![
        ("po_receipt", 1.0, Some(1)),
        ("so_ship", -1.0, Some(2)),
        ("rm_issue_to_wo", 1.0, Some(8)),
        ("wo_complete_v", 1.0, Some(9)),
        ("op_move_v", 1.0, Some(11)),
        ("scrap_v", -1.0, Some(7)),
        ("cycle_count_adj", 5.0, Some(5)),
        ("cycle_count_adj", -5.0, Some(6)),
        ("cost_adjustment", 0.0, Some(16)),
        ("standard_cost_roll", 0.0, Some(14)),
        ("po_return_to_vendor", -1.0, Some(13)),
        ("customer_return", 1.0, Some(12)),
        // Unmapped reasons return NULL (caller skips the INSERT).
        ("ar_payment", 1.0, None),
        ("labor_apply", 0.0, None),
        ("ppv", 0.0, None),
    ];

    for (reason, qty, expected) in cases {
        let actual: Option<i16> = sqlx::query_scalar(
            "SELECT _inventory_movement_event_type($1::posting_line_reason, $2::NUMERIC)",
        )
        .bind(reason)
        .bind(qty)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            actual, expected,
            "event_type for ({reason}, qty={qty}): expected {expected:?}, got {actual:?}"
        );
    }
}
