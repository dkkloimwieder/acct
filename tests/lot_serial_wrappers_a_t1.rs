//! T1 probes for sxl2.3 wrappers A: post_po_receipt and
//! post_inventory_adjustment with tracked_by='lot_and_serial'
//! (mig 0063, acct-sxl2.3).
//!
//! Drives each wrapper end-to-end and asserts:
//!   - serial arrays thread through to apply_event's E2.5 block
//!     (units + receipt events created),
//!   - audit columns (po_receipt_lines.unit_ids /
//!     inventory_adjustments.unit_ids) get stamped,
//!   - validation errors (length / cross-lot / non-serial-SKU) raise
//!     P0006 cleanly,
//!   - -qty path on lot_and_serial walks the lot, flips unit status
//!     to 'consumed', and emits type=2 events.
//!
//!   W1 — post_po_receipt with unit_serials creates N units + audit
//!   W2 — post_po_receipt unit_serials length mismatch raises P0006
//!   W3 — post_po_receipt serials on non-lot_and_serial SKU → P0006
//!   W4 — post_po_receipt no serials → auto-gen (lot_and_serial)
//!   W5 — post_inventory_adjustment +qty with unit_serials creates units
//!   W6 — post_inventory_adjustment -qty with unit_ids consumes
//!   W7 — post_inventory_adjustment -qty with unit_serials resolves
//!   W8 — post_inventory_adjustment XOR violation → P0006
//!   W9 — post_inventory_adjustment -qty missing both IDs/serials → P0006
//!   W10 — post_inventory_adjustment -qty units across two lots → P0006
//!   W11 — post_inventory_adjustment -qty count mismatch → P0006

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Scaffolding
// ============================================================

async fn fresh_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
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
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD') RETURNING id::text",
    )
    .bind(po_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty_ordered)
    .bind(unit_cost)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_sku(pool: &PgPool, code: &str, tracked: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, $2::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .bind(tracked)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(dead_code)]
struct Scaffold {
    vendor: String,
    po: String,
    sku: String,
    loc: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_unsettled: i64,
}

async fn scaffold(pool: &PgPool, label: &str, tracked: &str) -> Scaffold {
    let sku = fresh_sku(pool, &format!("SKU-{label}"), tracked).await;
    let loc: String =
        sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
            .fetch_one(pool)
            .await
            .unwrap();
    let vendor = fresh_vendor(pool, &format!("V-{label}")).await;
    let po = fresh_po(pool, &vendor).await;

    let qty_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available', 'qty', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku).bind(&loc).fetch_one(pool).await.unwrap();

    let val_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku).bind(&loc).fetch_one(pool).await.unwrap();

    let ven_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('vendor_pool', 'qty', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor).fetch_one(pool).await.unwrap();

    let ven_unsettled: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor).fetch_one(pool).await.unwrap();

    Scaffold { vendor, po, sku, loc, qty_acct, val_acct, ven_qty, ven_unsettled }
}

async fn run_post_po_receipt(
    pool: &PgPool,
    sc: &Scaffold,
    line: serde_json::Value,
) -> Result<String, sqlx::Error> {
    let key = fresh_uuid(pool).await;
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&sc.po)
    .bind(json!([line]))
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn run_inv_adj(
    pool: &PgPool,
    sc: &Scaffold,
    qty_delta: i64,
    unit_cost: Option<i64>,
    lot_metadata: Option<serde_json::Value>,
) -> Result<String, sqlx::Error> {
    let key = fresh_uuid(pool).await;
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, $4, 'USD', 'raw',
            '2026-04-15'::DATE, $5::UUID, $6::UUID, NULL, $7::JSONB
         )::text",
    )
    .bind(&sc.sku)
    .bind(&sc.loc)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(&posted_by)
    .bind(&key)
    .bind(lot_metadata)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests — post_po_receipt
// ============================================================

#[tokio::test]
async fn w1_po_receipt_with_unit_serials_creates_units_and_audit() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W1", "lot_and_serial").await;
    let po_line = fresh_po_line(&pool, &sc.po, 1, &sc.sku, &sc.loc, 3, 10_00).await;

    let line = json!({
        "po_line_id":   po_line,
        "qty_received": 3,
        "lot_code":     "W1-LOT-001",
        "unit_serials": ["W1-U-001", "W1-U-002", "W1-U-003"],
    });
    let _doc = run_post_po_receipt(&pool, &sc, line).await.expect("w1 receipt");

    // 3 units created against this receipt line's lot.
    let n_units: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_units WHERE product_id = $1::UUID",
    )
    .bind(&sc.sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_units, 3);

    // Audit column on po_receipt_lines populated with the unit ids.
    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_ids FROM po_receipt_lines
          WHERE po_line_id = $1::UUID",
    )
    .bind(&po_line)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unit_ids.len(), 3);
}

#[tokio::test]
async fn w2_po_receipt_unit_serials_length_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W2", "lot_and_serial").await;
    let po_line = fresh_po_line(&pool, &sc.po, 1, &sc.sku, &sc.loc, 3, 10_00).await;

    let line = json!({
        "po_line_id":   po_line,
        "qty_received": 3,
        "lot_code":     "W2-LOT-001",
        "unit_serials": ["W2-U-001", "W2-U-002"], // 2 != qty 3
    });
    expect_sqlstate("P0006", || async {
        run_post_po_receipt(&pool, &sc, line.clone()).await
    })
    .await;
}

#[tokio::test]
async fn w3_po_receipt_serials_on_non_lot_and_serial_sku_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    // tracked_by='lot' (not 'lot_and_serial') — serials are not valid.
    let sc = scaffold(&pool, "W3", "lot").await;
    let po_line = fresh_po_line(&pool, &sc.po, 1, &sc.sku, &sc.loc, 2, 10_00).await;

    let line = json!({
        "po_line_id":   po_line,
        "qty_received": 2,
        "lot_code":     "W3-LOT-001",
        "unit_serials": ["W3-U-001", "W3-U-002"],
    });
    expect_sqlstate("P0006", || async {
        run_post_po_receipt(&pool, &sc, line.clone()).await
    })
    .await;
}

#[tokio::test]
async fn w4_po_receipt_auto_generates_when_no_unit_serials() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W4", "lot_and_serial").await;
    let po_line = fresh_po_line(&pool, &sc.po, 1, &sc.sku, &sc.loc, 4, 10_00).await;

    let line = json!({
        "po_line_id":   po_line,
        "qty_received": 4,
        "lot_code":     "W4-LOT-A",
        // no unit_serials
    });
    let _doc = run_post_po_receipt(&pool, &sc, line).await.expect("w4 receipt");

    let serials: Vec<String> = sqlx::query_scalar(
        "SELECT serial_no FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sc.sku)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(serials.len(), 4);
    for (i, s) in serials.iter().enumerate() {
        assert_eq!(s, &format!("W4-LOT-A-U{:06}", i + 1));
    }
}

// ============================================================
// Tests — post_inventory_adjustment +qty
// ============================================================

#[tokio::test]
async fn w5_inv_adj_plus_qty_with_unit_serials_creates_units() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W5", "lot_and_serial").await;

    // +qty=3 with explicit lot_code + serials.
    let doc = run_inv_adj(
        &pool,
        &sc,
        3,
        Some(10_00),
        Some(json!({
            "lot_code":     "W5-LOT-A",
            "unit_serials": ["W5-U-001", "W5-U-002", "W5-U-003"],
            "external_serials": ["MFR-W5-X", "MFR-W5-Y", "MFR-W5-Z"],
        })),
    )
    .await
    .expect("w5 adj");

    let rows: Vec<(String, Option<String>, String)> = sqlx::query_as(
        "SELECT serial_no, external_serial_no, status::text
           FROM inventory_units WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sc.sku)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "W5-U-001");
    assert_eq!(rows[0].1.as_deref(), Some("MFR-W5-X"));
    assert_eq!(rows[0].2, "available");

    // Audit stamped.
    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_ids FROM inventory_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unit_ids.len(), 3);
}

// ============================================================
// Tests — post_inventory_adjustment -qty
// ============================================================

/// Helper: pre-seed N units for sc.sku via a +qty adjustment.
async fn seed_units(
    pool: &PgPool,
    sc: &Scaffold,
    lot_code: &str,
    serials: &[&str],
) {
    let n = serials.len() as i64;
    let payload = json!({
        "lot_code":     lot_code,
        "unit_serials": serials,
    });
    run_inv_adj(pool, sc, n, Some(10_00), Some(payload))
        .await
        .expect("seed +qty");
}

#[tokio::test]
async fn w6_inv_adj_minus_qty_with_unit_ids_consumes() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W6", "lot_and_serial").await;
    seed_units(&pool, &sc, "W6-LOT-A", &["W6-U-001", "W6-U-002", "W6-U-003"]).await;

    // Resolve the 2 unit_ids we want to consume.
    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID AND serial_no IN ('W6-U-001','W6-U-002')
          ORDER BY serial_no",
    )
    .bind(&sc.sku)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(unit_ids.len(), 2);

    let doc = run_inv_adj(
        &pool,
        &sc,
        -2,
        None,
        Some(json!({ "unit_ids": unit_ids })),
    )
    .await
    .expect("w6 consume");

    // Both consumed units flipped to 'consumed'.
    let consumed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_units
          WHERE unit_id = ANY($1) AND status = 'consumed'",
    )
    .bind(&unit_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(consumed, 2);

    // One unit (W6-U-003) remains available.
    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_units
          WHERE product_id = $1::UUID AND status = 'available'",
    )
    .bind(&sc.sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(remaining, 1);

    // Two type=2 (issue) events emitted.
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events
          WHERE unit_id = ANY($1) AND event_type = 2",
    )
    .bind(&unit_ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_events, 2);

    // Audit stamped.
    let audit: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_ids FROM inventory_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit.len(), 2);
    let mut sorted = audit.clone();
    sorted.sort();
    let mut expected = unit_ids.clone();
    expected.sort();
    assert_eq!(sorted, expected);
}

#[tokio::test]
async fn w7_inv_adj_minus_qty_with_unit_serials_resolves() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W7", "lot_and_serial").await;
    seed_units(&pool, &sc, "W7-LOT-A", &["W7-U-001", "W7-U-002"]).await;

    let _doc = run_inv_adj(
        &pool,
        &sc,
        -2,
        None,
        Some(json!({ "unit_serials": ["W7-U-001", "W7-U-002"] })),
    )
    .await
    .expect("w7 consume by serials");

    let consumed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_units
          WHERE product_id = $1::UUID AND status = 'consumed'",
    )
    .bind(&sc.sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(consumed, 2);
}

#[tokio::test]
async fn w8_inv_adj_xor_violation_unit_ids_and_serials_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W8", "lot_and_serial").await;
    seed_units(&pool, &sc, "W8-LOT-A", &["W8-U-001"]).await;
    let unit_id: i64 = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units WHERE product_id = $1::UUID",
    )
    .bind(&sc.sku)
    .fetch_one(&pool)
    .await
    .unwrap();

    expect_sqlstate("P0006", || async {
        run_inv_adj(
            &pool,
            &sc,
            -1,
            None,
            Some(json!({
                "unit_ids":     [unit_id],
                "unit_serials": ["W8-U-001"],
            })),
        )
        .await
    })
    .await;
}

#[tokio::test]
async fn w9_inv_adj_minus_qty_missing_unit_ids_and_serials_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W9", "lot_and_serial").await;
    seed_units(&pool, &sc, "W9-LOT-A", &["W9-U-001"]).await;
    let lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sc.sku)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Even passing lot_id (legacy lot_fifo path) doesn't satisfy the
    // lot_and_serial requirement that the caller identify which units.
    expect_sqlstate("P0006", || async {
        run_inv_adj(
            &pool,
            &sc,
            -1,
            None,
            Some(json!({ "lot_id": lot_id })),
        )
        .await
    })
    .await;
}

#[tokio::test]
async fn w10_inv_adj_minus_qty_units_across_two_lots_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W10", "lot_and_serial").await;
    seed_units(&pool, &sc, "W10-LOT-A", &["W10-U-001"]).await;
    seed_units(&pool, &sc, "W10-LOT-B", &["W10-U-002"]).await;

    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sc.sku)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(unit_ids.len(), 2);

    expect_sqlstate("P0006", || async {
        run_inv_adj(
            &pool,
            &sc,
            -2,
            None,
            Some(json!({ "unit_ids": unit_ids.clone() })),
        )
        .await
    })
    .await;
}

#[tokio::test]
async fn w11_inv_adj_minus_qty_count_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sc = scaffold(&pool, "W11", "lot_and_serial").await;
    seed_units(&pool, &sc, "W11-LOT-A", &["W11-U-001", "W11-U-002"]).await;
    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units WHERE product_id = $1::UUID",
    )
    .bind(&sc.sku)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(unit_ids.len(), 2);

    // qty_delta=-1 but 2 unit_ids — count mismatch.
    expect_sqlstate("P0006", || async {
        run_inv_adj(
            &pool,
            &sc,
            -1,
            None,
            Some(json!({ "unit_ids": unit_ids.clone() })),
        )
        .await
    })
    .await;
}
