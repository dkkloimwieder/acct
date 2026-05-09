//! T1 probes for lot_fifo dispatcher + apply_event extension
//! (mig 0045 + 0046, claimed against acct-uze E2.2). Pin the
//! cost-method dispatch + lot subledger writes via direct
//! post_posting_lines calls. Wrappers ship in E2.3.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

const USD_CV: &str = "creation_void";

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

/// Stand up the qty + value accounts (anonymous lot=NULL) for a fresh
/// lot SKU at MAIN. Returns (stock_available_id, inv_value_raw_id).
async fn open_lot_accounts(pool: &PgPool, sku_id: &str) -> (i64, i64) {
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

fn lot_receipt_event(
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

fn lot_issue_event(
    debit_cogs: i64,
    credit_value: i64,
    qty: i64,
    business_date: &str,
    idempotency_key: &str,
    specific_lot_id: Option<i64>,
) -> serde_json::Value {
    // Use 'so_ship' so the dispatcher routes through
    // _compute_amount_lot_fifo_outbound (cost-event reason).
    let mut ev = json!({
        "reason":            "so_ship",
        "document_kind":     "lot_issue",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  debit_cogs,
        "credit_account_id": credit_value,
        "qty":               qty,
        "business_date":     business_date,
        "idempotency_key":   idempotency_key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    if let Some(lot_id) = specific_lot_id {
        ev["lot_id"] = json!(lot_id);
    }
    ev
}

async fn cogs_usd(pool: &PgPool) -> i64 {
    account_id_by_kind_currency(pool, "cogs", Some("USD")).await
}

// ============================================================
// Receipt creates inventory_lots row
// ============================================================

#[tokio::test]
async fn lot_receipt_creates_lot_row_with_metadata() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku(&pool, "SKU-LOT").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    let key = fresh_uuid(&pool).await;
    let ev = lot_receipt_event(
        val,
        void_value,
        100,
        12_00,
        "2026-04-15",
        &key,
        "LOT-001",
        json!({
            "expiration_date": "2027-04-15",
            "manufacture_date": "2026-04-01",
            "supplier_lot_number": "VENDOR-LOT-A",
            "quality_status": "approved",
        }),
    );
    let result = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .expect("receipt");
    assert_eq!(result[0]["result"], "ok");

    let row: (String, String, String, String, String, String) = sqlx::query_as(
        "SELECT lot_code, original_quantity::text, unit_cost::text,
                expiration_date::text, supplier_lot_number, quality_status
           FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "LOT-001");
    assert_eq!(row.1, "100.000000");
    // amount=12_00 cents / qty=100 units = 12 cents/unit (NUMERIC(19,4) = "12.0000")
    assert_eq!(row.2, "12.0000");
    assert_eq!(row.3, "2027-04-15");
    assert_eq!(row.4, "VENDOR-LOT-A");
    assert_eq!(row.5, "approved");

    // posting_line_inventory.lot_id stamped
    let pli_lot: Option<i64> = sqlx::query_scalar(
        "SELECT pli.lot_id FROM posting_line_inventory pli
           JOIN posting_lines pl ON pl.id = pli.posting_line_id
          WHERE pl.idempotency_key = $1::UUID",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(pli_lot.is_some(), "pli.lot_id stamped on receipt");

    // inventory_movements.lot_id stamped
    let im_lot: Option<i64> = sqlx::query_scalar(
        "SELECT im.lot_id FROM inventory_movements im
           JOIN posting_lines pl ON pl.id = im.posting_line_id
          WHERE pl.idempotency_key = $1::UUID
            AND im.product_id = $2::UUID",
    )
    .bind(&key)
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(im_lot.is_some(), "inventory_movements.lot_id stamped on receipt");
}

#[tokio::test]
async fn lot_receipt_without_lot_code_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku(&pool, "SKU-LOT").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    let key = fresh_uuid(&pool).await;
    // omit lot_code
    let ev = json!({
        "reason":            "cycle_count_adj",
        "document_kind":     "lot_receipt",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  val,
        "credit_account_id": void_value,
        "amount":            12_00,
        "qty":               100,
        "business_date":     "2026-04-15",
        "idempotency_key":   key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    let err = call_post_posting_lines(&pool, json!([ev]), false)
        .await
        .unwrap_err();
    let code = err.as_database_error().unwrap().code().unwrap().to_string();
    assert_eq!(code, "P0006");
}

// ============================================================
// FIFO walk on outflow without specific lot
// ============================================================

#[tokio::test]
async fn fifo_walk_consumes_oldest_lot_first() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku(&pool, "SKU-LOT").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Lot A: 50 @ $10 received 2026-04-10
    let key_a = fresh_uuid(&pool).await;
    let ev_a = lot_receipt_event(
        val, void_value, 50, 5_00, "2026-04-10", &key_a, "LOT-A", json!({}),
    );
    call_post_posting_lines(&pool, json!([ev_a]), false).await.unwrap();

    // Lot B: 50 @ $20 received 2026-04-12
    let key_b = fresh_uuid(&pool).await;
    let ev_b = lot_receipt_event(
        val, void_value, 50, 10_00, "2026-04-12", &key_b, "LOT-B", json!({}),
    );
    call_post_posting_lines(&pool, json!([ev_b]), false).await.unwrap();

    // Issue 30 — should all come from LOT-A
    let key_i1 = fresh_uuid(&pool).await;
    let ev_i1 = lot_issue_event(void_value, val, 30, "2026-04-15", &key_i1, None);
    let r = call_post_posting_lines(&pool, json!([ev_i1]), false).await.unwrap();
    assert_eq!(r[0]["result"], "ok");

    // Lookup posting_line.amount — should be 30 * 0.10 = 3 (cents per unit / 50 qty / 5_00 cents)
    // Actually lot A was 50 qty for amount 5_00 cents -> unit_cost = 10 cents/unit (0.1000)
    // 30 units * 10 cents = 300 cents = 3_00
    let amt: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key_i1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(amt, 3_00, "30 from LOT-A @ 10c/unit = 3_00");

    // Issue 30 more — should split: 20 from LOT-A (residual), 10 from LOT-B
    let key_i2 = fresh_uuid(&pool).await;
    let ev_i2 = lot_issue_event(void_value, val, 30, "2026-04-16", &key_i2, None);
    let r2 = call_post_posting_lines(&pool, json!([ev_i2]), false).await.unwrap();
    assert_eq!(r2[0]["result"], "ok");

    let amt2: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key_i2)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 20 @ 10c (LOT-A) + 10 @ 20c (LOT-B) = 200 + 200 = 400 cents
    assert_eq!(amt2, 4_00, "split issue: 20 @ LOT-A + 10 @ LOT-B = 4_00");

    // Verify two issue events were written across the two lots.
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lot_events e
           JOIN posting_lines pl ON pl.id = e.posting_line_id
          WHERE pl.idempotency_key = $1::UUID
            AND e.event_type = 1",
    )
    .bind(&key_i2)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_events, 2, "split issue produces 2 lot_events");
}

// ============================================================
// Specific lot_id pin
// ============================================================

#[tokio::test]
async fn specific_lot_id_pins_depletion_to_that_lot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku(&pool, "SKU-LOT").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let cogs = cogs_usd(&pool).await;

    // Two lots: A older, B newer
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(val, void_value, 50, 5_00, "2026-04-10", &key_a, "LOT-A", json!({}))]),
        false,
    ).await.unwrap();
    let key_b = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(val, void_value, 50, 10_00, "2026-04-12", &key_b, "LOT-B", json!({}))]),
        false,
    ).await.unwrap();

    let lot_b_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE product_id = $1::UUID AND lot_code = 'LOT-B'",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Issue 30 pinned to LOT-B → must NOT touch LOT-A
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(cogs, val,30, "2026-04-15", &key_i, Some(lot_b_id));
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    let amt: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key_i)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 30 @ 0.20 = 6_00 cents
    assert_eq!(amt, 6_00, "specific lot pin uses LOT-B's unit_cost");

    let evs: Vec<i64> = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lot_events e
           JOIN posting_lines pl ON pl.id = e.posting_line_id
          WHERE pl.idempotency_key = $1::UUID",
    )
    .bind(&key_i)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0], lot_b_id);
}

#[tokio::test]
async fn specific_lot_short_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku(&pool, "SKU-LOT").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let cogs = cogs_usd(&pool).await;

    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(val, void_value, 50, 5_00, "2026-04-10", &key_a, "LOT-A", json!({}))]),
        false,
    ).await.unwrap();

    let lot_a_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Issue 75 from LOT-A which only has 50 → P0006 lot_residual_short
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(cogs, val,75, "2026-04-15", &key_i, Some(lot_a_id));
    let err = call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "P0006");
}

#[tokio::test]
async fn fifo_walk_no_lots_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku(&pool, "SKU-LOT").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let _void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let cogs = cogs_usd(&pool).await;

    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(cogs, val,10, "2026-04-15", &key_i, None);
    let err = call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "P0006");
}

// ============================================================
// Cost_method_strategies registration
// ============================================================

#[tokio::test]
async fn lot_fifo_strategy_is_registered() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let row: (String, String, bool) = sqlx::query_as(
        "SELECT compute_fn_name, event_kind::text, flag_provisional
           FROM cost_method_strategies
          WHERE cost_method = 'lot_fifo'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "_compute_amount_lot_fifo_outbound");
    assert_eq!(row.1, "outbound");
    assert!(!row.2, "lot_fifo not flagged for provisional");
}
