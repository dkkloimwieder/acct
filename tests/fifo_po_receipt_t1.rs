//! T1 probes for FIFO via `post_po_receipt` (mig 0034, acct-t1sc).
//! Phase E1 follow-up W1.
//!
//! Drives the document entry-point wrapper end-to-end (not
//! `post_posting_lines` directly) to verify:
//!   - the FIFO gate is now permissive (was P0006, now succeeds),
//!   - the value-leg drives apply_event's E1 block to write a
//!     cost_layers row at the asserted PO unit_cost,
//!   - posting_line_inventory.cost_method_at_event = 'fifo',
//!   - inventory_movements row writes (D-block subledger),
//!   - per-class qty divisor and GRNI accrual still hold.
//!
//! `post_po_receipt` for a FIFO SKU does NOT route through PPV
//! (FIFO carries the receipt cost into a layer; variance emerges
//! later only at AP-bill three-way mismatch). v_ppv_amount is 0
//! by construction in the wrapper's ELSE branch.

mod common;

use common::*;
use sqlx::PgPool;

#[allow(dead_code)]
struct Scaffold {
    vendor_id: String,
    po_id: String,
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_unsettled: i64,
}

async fn id_text(pool: &PgPool, q: &str, bind: &str) -> String {
    sqlx::query_scalar(q).bind(bind).fetch_one(pool).await.unwrap()
}

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

async fn scaffold_fif(pool: &PgPool, label: &str) -> Scaffold {
    let sku = id_text(pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-FIF").await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(pool, label).await;
    let po = fresh_po(pool, &vendor).await;
    let qty_acct = account_id_stock_available(pool, "SKU-FIF", "MAIN").await;

    let val_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

    let ven_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('vendor_pool', 'qty', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    let ven_unsettled: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    Scaffold {
        vendor_id: vendor,
        po_id: po,
        sku_id: sku,
        loc_id: loc,
        qty_acct,
        val_acct,
        ven_qty,
        ven_unsettled,
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
// W1.1: single FIFO receipt creates exactly one cost_layer.
// ============================================================

#[tokio::test]
async fn fifo_receipt_creates_one_layer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fif(&pool, "VEND-FIF-W1A").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 10, 100).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": po_line, "qty_received": 10 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("FIFO po_receipt should succeed (W1)");

    // Ledger: per-class.
    assert_eq!(balance(&pool, sf.qty_acct).await, 10);
    assert_eq!(balance(&pool, sf.val_acct).await, 1000);
    assert_eq!(balance(&pool, sf.ven_qty).await, -10);
    assert_eq!(balance(&pool, sf.ven_unsettled).await, -1000);

    // Exactly one layer at the PO unit_cost.
    let layer_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layers
         WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layer_count, 1, "expected exactly 1 cost_layer for FIFO receipt");

    let (orig_qty, unit_cost): (String, String) = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT FROM cost_layers
         WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(orig_qty.starts_with("10"), "orig_qty got {orig_qty}");
    assert!(unit_cost.starts_with("100"), "unit_cost got {unit_cost}");
}

// ============================================================
// W1.2: cost_method_at_event snapshot on posting_line_inventory.
// ============================================================

#[tokio::test]
async fn fifo_receipt_stamps_cost_method_at_event() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fif(&pool, "VEND-FIF-W1B").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 5, 80).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": po_line, "qty_received": 5 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-16", &key)
        .await
        .expect("FIFO po_receipt");

    // Both qty-leg and value-leg should have pli rows; both should
    // snapshot cost_method_at_event = 'fifo' (R7 audit field).
    let methods: Vec<String> = sqlx::query_scalar(
        "SELECT pli.cost_method_at_event::TEXT FROM posting_line_inventory pli
         JOIN posting_lines pl ON pl.id = pli.posting_line_id
         WHERE pl.document_kind = 'po_receipt'
         ORDER BY pl.id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!methods.is_empty(), "expected pli rows");
    for m in &methods {
        assert_eq!(m, "fifo", "all pli rows should snapshot 'fifo'");
    }
}

// ============================================================
// W1.3: D-block writes inventory_movements rows for FIFO receipt.
// Per-leg attribution: DR side +qty on inv_value_raw; CR side
// (vendor_pool/ap_unsettled have no sku_id) skipped.
// ============================================================

#[tokio::test]
async fn fifo_receipt_writes_inventory_movements() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fif(&pool, "VEND-FIF-W1C").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 7, 120).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": po_line, "qty_received": 7 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-17", &key)
        .await
        .expect("FIFO po_receipt");

    // One movement row on the inv_value_raw DR leg.
    let (qty, actual_uc): (String, String) = sqlx::query_as(
        "SELECT im.quantity::TEXT, im.actual_unit_cost::TEXT
           FROM inventory_movements im
          WHERE im.product_id = $1::UUID
            AND im.location_id = $2::UUID
            AND im.event_type = 1",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(qty.starts_with("7"), "qty got {qty}");
    assert!(actual_uc.starts_with("120"), "actual_uc got {actual_uc}");
}

// ============================================================
// W1.4: two sequential FIFO receipts produce two layers in order.
// ============================================================

#[tokio::test]
async fn fifo_two_receipts_two_layers_in_order() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fif(&pool, "VEND-FIF-W1D").await;

    let pl1 = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 10, 50).await;
    let k1 = fresh_uuid(&pool).await;
    call_receipt(
        &pool,
        &sf.po_id,
        serde_json::json!([{ "po_line_id": pl1, "qty_received": 10 }]),
        "2026-04-15",
        &k1,
    )
    .await
    .expect("receipt 1");

    let pl2 = fresh_po_line(&pool, &sf.po_id, 2, &sf.sku_id, &sf.loc_id, 10, 70).await;
    let k2 = fresh_uuid(&pool).await;
    call_receipt(
        &pool,
        &sf.po_id,
        serde_json::json!([{ "po_line_id": pl2, "qty_received": 10 }]),
        "2026-04-16",
        &k2,
    )
    .await
    .expect("receipt 2");

    let layers: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT, receipt_date::TEXT
           FROM cost_layers
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY receipt_date, layer_id",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(layers.len(), 2);
    assert!(layers[0].0.starts_with("10"));
    assert!(layers[0].1.starts_with("50"));
    assert_eq!(layers[0].2, "2026-04-15");
    assert!(layers[1].0.starts_with("10"));
    assert!(layers[1].1.starts_with("70"));
    assert_eq!(layers[1].2, "2026-04-16");
}

// ============================================================
// W1.5: idempotency — replaying same idempotency_key returns same
// doc id and DOES NOT create a duplicate layer.
// ============================================================

#[tokio::test]
async fn fifo_receipt_idempotent_no_duplicate_layer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fif(&pool, "VEND-FIF-W1E").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 4, 200).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": po_line, "qty_received": 4 }]);
    let doc1 = call_receipt(&pool, &sf.po_id, lines.clone(), "2026-04-18", &key)
        .await
        .expect("first receipt");
    let doc2 = call_receipt(&pool, &sf.po_id, lines, "2026-04-18", &key)
        .await
        .expect("replay");
    assert_eq!(doc1, doc2, "idempotent replay should return same doc id");

    let layer_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layers
         WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layer_count, 1, "replay must not duplicate the layer");
}

// ============================================================
// W1.6: 'lot' cost_method still raises P0006 (acct-uze).
// ============================================================

#[tokio::test]
async fn lot_cost_method_still_blocked() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Promote SKU-FIF to 'lot' for this test (the fixture has no 'lot' SKU).
    sqlx::query("UPDATE skus SET cost_method = 'lot' WHERE code = 'SKU-FIF'")
        .execute(&pool)
        .await
        .unwrap();

    let sf = scaffold_fif(&pool, "VEND-FIF-W1F").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 1, 50).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": po_line, "qty_received": 1 }]);
    let err = call_receipt(&pool, &sf.po_id, lines, "2026-04-19", &key)
        .await
        .expect_err("'lot' should still raise P0006");
    let sqlstate = err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned());
    assert_eq!(sqlstate.as_deref(), Some("P0006"));
}

// ============================================================
// W1.7: recon check #8 stays clean after FIFO receipt.
// ============================================================

#[tokio::test]
async fn fifo_receipt_passes_recon() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fif(&pool, "VEND-FIF-W1G").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 12, 75).await;

    let key = fresh_uuid(&pool).await;
    let lines = serde_json::json!([{ "po_line_id": po_line, "qty_received": 12 }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-20", &key)
        .await
        .expect("receipt");

    let new_alerts: i64 = sqlx::query_scalar("SELECT run_daily_reconciliation()::BIGINT")
        .fetch_one(&pool)
        .await
        .unwrap();
    let fifo_alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts
         WHERE alert_kind = 'fifo_layer_residual_mismatch'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        fifo_alerts, 0,
        "no FIFO recon alerts expected after clean receipt; new={new_alerts}"
    );
}
