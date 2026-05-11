//! T1 probes for the sxl2.2 apply_event E2.5 block (mig 0062,
//! acct-sxl2.2). Drives direct post_posting_lines calls with
//! tracked_by='lot_and_serial' SKUs and asserts inventory_units +
//! inventory_unit_events writes alongside the existing lot creation.
//!
//!   A1 — caller-supplied unit_serials (length=qty): N inventory_units
//!        rows + N type=1 receipt events; serial_no = supplied values.
//!   A2 — unit_serials length mismatch → P0006
//!        ('lot_and_serial_serial_count_mismatch').
//!   A3 — no unit_serials → auto-generates <lot_code>-U<padded-seq>
//!        format, count matches qty.
//!   A4 — tracked_by='lot' (not 'lot_and_serial') → no inventory_units
//!        rows created (E2.5 gate negative).
//!   A5 — external_serials supplied (length=qty): external_serial_no
//!        populated on the unit rows.
//!   A6 — external_serials length mismatch → P0006
//!        ('lot_and_serial_external_count_mismatch').
//!   A7 — external_serials with NULL elements: nullable column accepts
//!        them; only non-NULL entries hold the partial UNIQUE slot.
//!   A8 — type=1 event payload: posting_line_id, new_status='available',
//!        location_id_to all set correctly.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

const USD_CV: &str = "creation_void";

async fn fresh_sku(
    pool: &PgPool,
    code: &str,
    tracked: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, $2::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .bind(tracked)
    .fetch_one(pool)
    .await
    .expect("insert SKU")
}

/// Returns (stock_available_id, inv_value_raw_id) at MAIN for a fresh
/// lot_and_serial SKU. lot_id partition stays NULL (E2.5 doesn't
/// require per-lot accounts).
async fn open_accounts(pool: &PgPool, sku_id: &str) -> (i64, i64) {
    let loc_id: String = sqlx::query_scalar(
        "SELECT id::text FROM locations WHERE code = 'MAIN'",
    )
    .fetch_one(pool)
    .await
    .unwrap();

    let stock: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available'::account_kind, 'qty'::ledger_kind,
                 $1::UUID, $2::UUID, 'debit'::balance_direction)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(&loc_id)
    .fetch_one(pool)
    .await
    .unwrap();

    let val: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw'::account_kind, 'value'::ledger_kind, 'USD',
                 $1::UUID, $2::UUID, 'debit'::balance_direction)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(&loc_id)
    .fetch_one(pool)
    .await
    .unwrap();

    (stock, val)
}

/// Build a receipt event with optional sxl2 keys.
fn receipt_event(
    debit_value: i64,
    credit_void_value: i64,
    qty: i64,
    amount: i64,
    business_date: &str,
    idempotency_key: &str,
    lot_code: &str,
    extras: serde_json::Value,
) -> serde_json::Value {
    let mut ev = json!({
        "reason":            "cycle_count_adj",
        "document_kind":     "lot_receipt",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  debit_value,
        "credit_account_id": credit_void_value,
        "amount":            amount,
        "qty":               qty,
        "business_date":     business_date,
        "idempotency_key":   idempotency_key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
        "lot_code":          lot_code,
    });
    if let serde_json::Value::Object(map) = extras {
        for (k, v) in map {
            ev[k] = v;
        }
    }
    ev
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn a1_caller_supplied_unit_serials_create_units_and_events() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A1", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        3,
        30_00,
        "2026-04-15",
        &key,
        "A1-LOT-001",
        json!({
            "unit_serials": ["A1-U-001", "A1-U-002", "A1-U-003"],
        }),
    );
    let res = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("a1 receipt");
    assert_eq!(res[0]["result"], "ok");

    let rows: Vec<(i64, String, Option<String>, String, i64)> = sqlx::query_as(
        "SELECT unit_id, serial_no, external_serial_no, status::text, lot_id
           FROM inventory_units
          WHERE product_id = $1::UUID
          ORDER BY serial_no",
    )
    .bind(&sku_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 3);
    let serials: Vec<&str> = rows.iter().map(|r| r.1.as_str()).collect();
    assert_eq!(serials, vec!["A1-U-001", "A1-U-002", "A1-U-003"]);
    assert!(rows.iter().all(|r| r.2.is_none()));
    assert!(rows.iter().all(|r| r.3 == "available"));
    let lot_id = rows[0].4;
    assert!(rows.iter().all(|r| r.4 == lot_id));

    // Each unit has a type=1 receipt event tied to the same posting_line_id.
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events e
           JOIN inventory_units u ON u.unit_id = e.unit_id
          WHERE u.product_id = $1::UUID AND e.event_type = 1",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_events, 3);
}

#[tokio::test]
async fn a2_unit_serials_length_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A2", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        3,
        30_00,
        "2026-04-15",
        &key,
        "A2-LOT-001",
        json!({
            "unit_serials": ["A2-U-001", "A2-U-002"], // only 2 — qty is 3
        }),
    );
    expect_sqlstate("P0006", || async {
        call_post_posting_lines(&pool, json!([ev.clone()]), false).await
    })
    .await;
}

#[tokio::test]
async fn a3_no_unit_serials_auto_generates_with_lot_code_prefix() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A3", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        4,
        40_00,
        "2026-04-15",
        &key,
        "A3-LOT-A",
        json!({}), // no unit_serials
    );
    let res = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("a3 receipt");
    assert_eq!(res[0]["result"], "ok");

    let serials: Vec<String> = sqlx::query_scalar(
        "SELECT serial_no FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sku_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(serials.len(), 4);
    for (i, s) in serials.iter().enumerate() {
        let expected = format!("A3-LOT-A-U{:06}", i + 1);
        assert_eq!(s, &expected, "auto-gen format mismatch at idx {i}");
    }
}

#[tokio::test]
async fn a4_lot_tracking_only_creates_no_units() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A4", "lot").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    // Even if caller passes unit_serials, tracked_by='lot' (not
    // 'lot_and_serial') means the E2.5 gate is negative — units are
    // ignored.
    let ev = receipt_event(
        val,
        void_value,
        2,
        20_00,
        "2026-04-15",
        &key,
        "A4-LOT-001",
        json!({
            "unit_serials": ["A4-U-001", "A4-U-002"],
        }),
    );
    let res = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("a4 receipt");
    assert_eq!(res[0]["result"], "ok");

    // Lot row created (lot tracking on), but no units.
    let n_lots: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_lots, 1);

    let n_units: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_units WHERE product_id = $1::UUID",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_units, 0);
}

#[tokio::test]
async fn a5_external_serials_populated_when_supplied() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A5", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        2,
        20_00,
        "2026-04-15",
        &key,
        "A5-LOT-001",
        json!({
            "unit_serials":     ["A5-U-001", "A5-U-002"],
            "external_serials": ["MFR-A5-AAA", "MFR-A5-BBB"],
        }),
    );
    let res = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("a5 receipt");
    assert_eq!(res[0]["result"], "ok");

    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT serial_no, external_serial_no FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sku_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "A5-U-001");
    assert_eq!(rows[0].1.as_deref(), Some("MFR-A5-AAA"));
    assert_eq!(rows[1].0, "A5-U-002");
    assert_eq!(rows[1].1.as_deref(), Some("MFR-A5-BBB"));
}

#[tokio::test]
async fn a6_external_serials_length_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A6", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        2,
        20_00,
        "2026-04-15",
        &key,
        "A6-LOT-001",
        json!({
            "unit_serials":     ["A6-U-001", "A6-U-002"],
            "external_serials": ["MFR-A6-AAA"], // only 1 — mismatch
        }),
    );
    expect_sqlstate("P0006", || async {
        call_post_posting_lines(&pool, json!([ev.clone()]), false).await
    })
    .await;
}

#[tokio::test]
async fn a7_external_serials_with_null_elements_allowed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A7", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        3,
        30_00,
        "2026-04-15",
        &key,
        "A7-LOT-001",
        json!({
            "unit_serials":     ["A7-U-001", "A7-U-002", "A7-U-003"],
            "external_serials": ["MFR-A7-AAA", null, "MFR-A7-CCC"],
        }),
    );
    let res = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("a7 receipt");
    assert_eq!(res[0]["result"], "ok");

    let exts: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT external_serial_no FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sku_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(exts.len(), 3);
    assert_eq!(exts[0].as_deref(), Some("MFR-A7-AAA"));
    assert!(exts[1].is_none(), "middle external should be NULL");
    assert_eq!(exts[2].as_deref(), Some("MFR-A7-CCC"));
}

#[tokio::test]
async fn a8_receipt_event_payload_populated_correctly() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "SKU-A8", "lot_and_serial").await;
    let (_stock, val) = open_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let loc_id: String =
        sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let key = fresh_uuid(&pool).await;

    let ev = receipt_event(
        val,
        void_value,
        2,
        20_00,
        "2026-04-15",
        &key,
        "A8-LOT-001",
        json!({
            "unit_serials": ["A8-U-001", "A8-U-002"],
        }),
    );
    let res = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("a8 receipt");
    assert_eq!(res[0]["result"], "ok");

    // Resolve the receipt posting_line via idempotency_key.
    let posting_line_id: i64 = sqlx::query_scalar(
        "SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Each receipt event row: type=1, posting_line_id matches,
    // new_status='available', location_id_to=MAIN, no from_location.
    let rows: Vec<(i32, i64, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT e.event_type::int, e.posting_line_id, e.new_status::text,
                e.location_id_to::text, e.location_id_from::text
           FROM inventory_unit_events e
           JOIN inventory_units u ON u.unit_id = e.unit_id
          WHERE u.product_id = $1::UUID
          ORDER BY u.serial_no",
    )
    .bind(&sku_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(r.0, 1);
        assert_eq!(r.1, posting_line_id);
        assert_eq!(r.2, "available");
        assert_eq!(r.3.as_deref(), Some(loc_id.as_str()));
        assert!(r.4.is_none());
    }
}
