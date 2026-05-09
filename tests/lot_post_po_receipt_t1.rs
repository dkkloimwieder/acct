//! T1 probes for lot_fifo via `post_po_receipt` (mig 0047, acct-q9ef).
//! Phase E2 follow-up L1.
//!
//! Drives the wrapper end-to-end (not `post_posting_lines` directly)
//! to verify:
//!   - per-line lot_code + optional metadata flow into apply_event's
//!     E2 block via the value-leg event JSON,
//!   - inventory_lots row is created at the asserted unit_cost,
//!   - po_receipt_lines.lot_id stamp is set to the new lot,
//!   - posting_line_inventory.lot_id and inventory_movements.lot_id
//!     stamps are set (via apply_event's E2 block, mig 0046),
//!   - cost_method_at_receipt snapshot = 'lot_fifo',
//!   - missing lot_code on a lot_fifo line raises a wrapper-level
//!     P0022 error before post_posting_lines is invoked,
//!   - non-lot SKUs that happen to carry lot_code keys pass through
//!     harmlessly (lot_id stays NULL),
//!   - idempotent replay returns the same doc_id and does not
//!     create a duplicate lot.

mod common;

use common::*;
use serde_json::json;
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

async fn fresh_lot_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, 'lot'::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .expect("insert lot SKU")
}

async fn scaffold_lot(pool: &PgPool, label: &str) -> Scaffold {
    let sku_code = format!("SKU-LOT-{label}");
    let sku = fresh_lot_sku(pool, &sku_code).await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(pool, label).await;
    let po = fresh_po(pool, &vendor).await;

    let qty_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available', 'qty', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

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
// L1.1: lot_fifo receipt creates inventory_lots + stamps audit fields.
// ============================================================

#[tokio::test]
async fn lot_receipt_creates_lot_and_stamps_audit_fields() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1A").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 10, 100).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "po_line_id":   po_line,
        "qty_received": 10,
        "lot_code":     "LOT-A001"
    }]);
    let doc_id = call_receipt(&pool, &sf.po_id, lines, "2026-04-15", &key)
        .await
        .expect("lot_fifo po_receipt should succeed");

    // Per-class ledger.
    assert_eq!(balance(&pool, sf.qty_acct).await, 10);
    assert_eq!(balance(&pool, sf.val_acct).await, 1000);
    assert_eq!(balance(&pool, sf.ven_qty).await, -10);
    assert_eq!(balance(&pool, sf.ven_unsettled).await, -1000);

    // Exactly one inventory_lots row at the PO unit_cost.
    let (lot_id, orig_qty, unit_cost, lot_code, currency): (i64, String, String, String, String) =
        sqlx::query_as(
            "SELECT lot_id, original_quantity::TEXT, unit_cost::TEXT, lot_code, cost_currency
               FROM inventory_lots
              WHERE product_id = $1::UUID AND location_id = $2::UUID",
        )
        .bind(&sf.sku_id)
        .bind(&sf.loc_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(orig_qty.starts_with("10"), "orig_qty got {orig_qty}");
    assert!(unit_cost.starts_with("100"), "unit_cost got {unit_cost}");
    assert_eq!(lot_code, "LOT-A001");
    assert_eq!(currency, "USD");

    // po_receipt_lines.lot_id stamp.
    let prl_lot: Option<i64> = sqlx::query_scalar(
        "SELECT lot_id FROM po_receipt_lines WHERE receipt_id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prl_lot, Some(lot_id), "po_receipt_lines.lot_id should match created lot");

    // posting_line_inventory.lot_id stamped on value-leg only (qty-leg
    // touches stock_available which carries no lot identity).
    let pli_lots: Vec<Option<i64>> = sqlx::query_scalar(
        "SELECT pli.lot_id FROM posting_line_inventory pli
         JOIN posting_lines pl ON pl.id = pli.posting_line_id
         WHERE pl.document_id = $1::UUID AND pl.reason = 'po_receipt'
         ORDER BY pl.id",
    )
    .bind(&doc_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let stamped: Vec<i64> = pli_lots.into_iter().flatten().collect();
    assert_eq!(stamped, vec![lot_id], "exactly one pli row should carry lot_id");

    // inventory_movements.lot_id stamped on the DR-side movement.
    let im_lot: Option<i64> = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_movements
         WHERE product_id = $1::UUID AND location_id = $2::UUID
         ORDER BY movement_id",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(im_lot, Some(lot_id));

    // cost_method_at_receipt snapshot.
    let cm: String = sqlx::query_scalar(
        "SELECT cost_method_at_receipt::TEXT FROM po_receipt_lines
         WHERE receipt_id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cm, "lot_fifo");
}

// ============================================================
// L1.2: missing lot_code on lot_fifo line raises wrapper P0022.
// ============================================================

#[tokio::test]
async fn lot_receipt_missing_lot_code_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1B").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 5, 80).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{ "po_line_id": po_line, "qty_received": 5 }]);
    let err = call_receipt(&pool, &sf.po_id, lines, "2026-04-16", &key)
        .await
        .err()
        .expect("expected wrapper P0022 for missing lot_code");
    let dberr = err.as_database_error().expect("db error");
    assert_eq!(dberr.code().as_deref(), Some("P0022"), "got {dberr:?}");
    assert!(
        dberr.message().contains("lot_code"),
        "expected lot_code mention, got: {}",
        dberr.message()
    );
}

// ============================================================
// L1.3: empty lot_code (zero-length) also raises P0022.
// ============================================================

#[tokio::test]
async fn lot_receipt_empty_lot_code_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1C").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 5, 80).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{ "po_line_id": po_line, "qty_received": 5, "lot_code": "" }]);
    let err = call_receipt(&pool, &sf.po_id, lines, "2026-04-16", &key)
        .await
        .err()
        .expect("expected wrapper P0022 for empty lot_code");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0022"));
}

// ============================================================
// L1.4: two-line receipt creates two distinct lots in order.
// ============================================================

#[tokio::test]
async fn lot_two_line_receipt_creates_two_lots() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1D").await;
    let pl1 = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 10, 50).await;
    let pl2 = fresh_po_line(&pool, &sf.po_id, 2, &sf.sku_id, &sf.loc_id, 5, 70).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([
        { "po_line_id": pl1, "qty_received": 10, "lot_code": "LOT-D001" },
        { "po_line_id": pl2, "qty_received":  5, "lot_code": "LOT-D002" },
    ]);
    let doc_id = call_receipt(&pool, &sf.po_id, lines, "2026-04-17", &key)
        .await
        .expect("multi-line lot receipt");

    let lots: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT lot_id, lot_code, original_quantity::TEXT
           FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY lot_code",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(lots.len(), 2, "expected 2 lots, got {}", lots.len());
    assert_eq!(lots[0].1, "LOT-D001");
    assert!(lots[0].2.starts_with("10"));
    assert_eq!(lots[1].1, "LOT-D002");
    assert!(lots[1].2.starts_with("5"));

    // Both po_receipt_lines stamped distinctly.
    let stamped: Vec<i64> = sqlx::query_scalar(
        "SELECT prl.lot_id FROM po_receipt_lines prl
         WHERE prl.receipt_id = $1::UUID
           AND prl.lot_id IS NOT NULL
         ORDER BY prl.lot_id",
    )
    .bind(&doc_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stamped.len(), 2);
    assert_eq!(stamped[0], lots[0].0);
    assert_eq!(stamped[1], lots[1].0);
}

// ============================================================
// L1.5: optional metadata (expiration_date, supplier_lot_number,
// attributes JSON) persisted on the inventory_lots row.
// ============================================================

#[tokio::test]
async fn lot_receipt_persists_optional_metadata() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1E").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 8, 90).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "po_line_id":          po_line,
        "qty_received":        8,
        "lot_code":            "LOT-E001",
        "manufacture_date":    "2026-01-15",
        "expiration_date":     "2027-01-15",
        "supplier_lot_number": "VEND-LOT-XYZ-42",
        "quality_status":      "pending",
        "attributes":          { "country_of_origin": "DE", "potency_pct": 99.5 }
    }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-18", &key)
        .await
        .expect("metadata receipt");

    let (mfg, exp, sup, qs, attrs): (
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<serde_json::Value>,
    ) = sqlx::query_as(
        "SELECT manufacture_date::TEXT, expiration_date::TEXT,
                supplier_lot_number, quality_status, attributes
           FROM inventory_lots
          WHERE product_id = $1::UUID",
    )
    .bind(&sf.sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(mfg.as_deref(), Some("2026-01-15"));
    assert_eq!(exp.as_deref(), Some("2027-01-15"));
    assert_eq!(sup.as_deref(), Some("VEND-LOT-XYZ-42"));
    assert_eq!(qs, "pending");
    let attrs = attrs.expect("attributes JSON");
    assert_eq!(attrs["country_of_origin"], json!("DE"));
}

// ============================================================
// L1.6: non-lot SKU + harmless lot_code key passes through.
// po_receipt_lines.lot_id stays NULL; no inventory_lots row.
// ============================================================

#[tokio::test]
async fn non_lot_sku_ignores_lot_code_key() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Scaffold standard SKU-A with a vendor + PO. SKU-A is fixture
    // (cost_method='standard', tracked_by='none').
    let sku_a: String =
        id_text(&pool, "SELECT id::text FROM skus WHERE code = $1", "SKU-A").await;
    let loc: String =
        id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let vendor = fresh_vendor(&pool, "L1F").await;
    let po = fresh_po(&pool, &vendor).await;
    // SKU-A's stock_available and inv_value_raw at MAIN are seeded by
    // the fixture; only vendor-side accounts need creation here.
    let _ven_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('vendor_pool', 'qty', $1::UUID, 'credit') RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(&pool)
    .await
    .unwrap();
    let _ven_val: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit') RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Standard SKU has fixture standard cost; use it as PO unit_cost.
    let po_line = fresh_po_line(&pool, &po, 1, &sku_a, &loc, 5, 100).await;

    let key = fresh_uuid(&pool).await;
    // Carries lot_code anyway — should be harmlessly ignored.
    let lines = json!([{
        "po_line_id":   po_line,
        "qty_received": 5,
        "lot_code":     "STRAY-LOT-IGNORED"
    }]);
    let doc_id = call_receipt(&pool, &po, lines, "2026-04-19", &key)
        .await
        .expect("non-lot SKU should accept (and ignore) lot_code");

    // No inventory_lots row was created for SKU-A.
    let lot_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sku_a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_count, 0);

    // po_receipt_lines.lot_id stays NULL.
    let prl_lot: Option<i64> = sqlx::query_scalar(
        "SELECT lot_id FROM po_receipt_lines WHERE receipt_id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prl_lot, None);
}

// ============================================================
// L1.7: idempotent replay returns same doc_id, no duplicate lot.
// ============================================================

#[tokio::test]
async fn lot_receipt_idempotent_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1G").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 6, 75).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "po_line_id":   po_line,
        "qty_received": 6,
        "lot_code":     "LOT-G001"
    }]);

    let doc1 = call_receipt(&pool, &sf.po_id, lines.clone(), "2026-04-20", &key)
        .await
        .expect("first call");
    let doc2 = call_receipt(&pool, &sf.po_id, lines, "2026-04-20", &key)
        .await
        .expect("replay call");
    assert_eq!(doc1, doc2, "idempotent replay should return same doc_id");

    let lot_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sf.sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_count, 1, "replay must not create a duplicate lot");

    // Ledger unchanged on replay.
    assert_eq!(balance(&pool, sf.qty_acct).await, 6);
    assert_eq!(balance(&pool, sf.val_acct).await, 450);
}

// ============================================================
// L1.8: residual = original_quantity (no events emitted yet).
// ============================================================

#[tokio::test]
async fn lot_residual_equals_original_after_receipt() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L1H").await;
    let po_line = fresh_po_line(&pool, &sf.po_id, 1, &sf.sku_id, &sf.loc_id, 12, 60).await;

    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "po_line_id":   po_line,
        "qty_received": 12,
        "lot_code":     "LOT-H001"
    }]);
    call_receipt(&pool, &sf.po_id, lines, "2026-04-21", &key)
        .await
        .expect("receipt");

    let (lot_id, receipt_date): (i64, String) = sqlx::query_as(
        "SELECT lot_id, receipt_date::TEXT FROM inventory_lots
         WHERE product_id = $1::UUID",
    )
    .bind(&sf.sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let residual: String = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty($1, $2::DATE)::TEXT",
    )
    .bind(lot_id)
    .bind(&receipt_date)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(residual.starts_with("12"), "residual got {residual}");

    // No inventory_lot_events row yet (Pattern A — receipt is not an event).
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lot_events WHERE lot_id = $1",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 0);
}
