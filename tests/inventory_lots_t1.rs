//! T1 probes for `inventory_lots` + `inventory_lot_events` (mig 0044,
//! acct-ua3r). Phase E2 E2.1 of the convergence plan. Pin schema
//! invariants of the lot foundation BEFORE the dispatcher in E2.2
//! wires real writes. Drives direct INSERTs to verify FK constraints,
//! append-only triggers, partition routing, residual helper, and the
//! lot-related column extensions on accounts / inventory_reservations
//! / skus.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

async fn stage_posting_line(pool: &PgPool, sku_code: &str, qty: i64) -> i64 {
    let stock = account_id_stock_available(pool, sku_code, "MAIN").await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, qty, "2026-04-15", &key);
    let result = call_post_posting_lines(pool, json!([event]), false)
        .await
        .expect("stage post_posting_lines");
    assert_eq!(result[0]["result"], "ok", "stage: {result}");
    sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .expect("fetch staged posting_line.id")
}

async fn sku_id(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("sku lookup")
}

async fn loc_id(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("location lookup")
}

async fn stage_lot(
    pool: &PgPool,
    sku_code: &str,
    loc_code: &str,
    pl_id: i64,
    lot_code: &str,
    receipt_date: &str,
    qty: &str,
    unit_cost: &str,
) -> i64 {
    let s = sku_id(pool, sku_code).await;
    let l = loc_id(pool, loc_code).await;
    sqlx::query_scalar(
        "INSERT INTO inventory_lots
            (product_id, location_id, receipt_posting_line_id, lot_code,
             receipt_date, original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, $2::UUID, $3, $4, $5::DATE, $6::NUMERIC, $7::NUMERIC, 'USD')
         RETURNING lot_id",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl_id)
    .bind(lot_code)
    .bind(receipt_date)
    .bind(qty)
    .bind(unit_cost)
    .fetch_one(pool)
    .await
    .expect("stage inventory_lots row")
}

// ============================================================
// Partitioning: 24 monthly partitions per table
// ============================================================

#[tokio::test]
async fn baked_partitions_span_2026_to_2027_for_both_tables() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let lots_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM pg_inherits i
           JOIN pg_class c ON c.oid = i.inhparent
          WHERE c.relname = 'inventory_lots'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lots_count, 24, "inventory_lots: 24 monthly partitions baked");

    let events_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM pg_inherits i
           JOIN pg_class c ON c.oid = i.inhparent
          WHERE c.relname = 'inventory_lot_events'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(events_count, 24, "inventory_lot_events: 24 monthly partitions baked");
}

#[tokio::test]
async fn lot_inserts_route_to_correct_partition_by_receipt_date() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl_apr = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_apr = stage_lot(&pool, "SKU-A", "MAIN", pl_apr, "LOT-APR", "2026-04-10", "100", "10").await;

    let pl_may = stage_posting_line(&pool, "SKU-A", 50).await;
    let lot_may = stage_lot(&pool, "SKU-A", "MAIN", pl_may, "LOT-MAY", "2026-05-15", "50", "12").await;

    let p_apr: String = sqlx::query_scalar(
        "SELECT tableoid::regclass::text FROM inventory_lots WHERE lot_id = $1",
    )
    .bind(lot_apr)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(p_apr, "inventory_lots_2026_04");

    let p_may: String = sqlx::query_scalar(
        "SELECT tableoid::regclass::text FROM inventory_lots WHERE lot_id = $1",
    )
    .bind(lot_may)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(p_may, "inventory_lots_2026_05");
}

// ============================================================
// FK and CHECK constraints
// ============================================================

#[tokio::test]
async fn lot_event_fk_to_lots_on_composite_key() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-FK", "2026-04-10", "100", "10").await;

    // Issue event with correct (lot_id, lot_receipt_date): OK.
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 1, -10)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .expect("paired (lot_id, receipt_date) FK ok");

    // Mismatched receipt_date: FK violation (23503).
    let err = sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-11'::DATE, '2026-04-15'::DATE, 1, -10)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap_err();
    let sqlstate = err.as_database_error().unwrap().code().unwrap().to_string();
    assert_eq!(sqlstate, "23503", "got {sqlstate}, expected FK violation");
}

#[tokio::test]
async fn event_type_quantity_check_enforces_signs() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-CK", "2026-04-10", "100", "10").await;

    // Issue (event_type=1) must have negative qty.
    let err = sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 1, 10)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        err.as_database_error().unwrap().code().unwrap(),
        "23514",
        "issue with positive qty must be CHECK-rejected"
    );

    // adjust_in (event_type=7) must have positive qty.
    let err = sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 7, -5)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");

    // transfer (event_type=2) must have qty=0.
    let err = sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change,
             location_id_from, location_id_to)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 2, -5,
                 (SELECT id FROM locations WHERE code = 'MAIN'),
                 (SELECT id FROM locations WHERE code = 'ALT'))",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");
}

#[tokio::test]
async fn transfer_event_requires_distinct_from_to_locations() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-TF", "2026-04-10", "100", "10").await;
    let main = loc_id(&pool, "MAIN").await;

    // Same from and to → CHECK violation.
    let err = sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change,
             location_id_from, location_id_to)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 2, 0, $2::UUID, $2::UUID)",
    )
    .bind(lot_id)
    .bind(&main)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");
}

#[tokio::test]
async fn status_change_event_requires_new_quality_status() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-SC", "2026-04-10", "100", "10").await;

    // status_change without new_quality_status → reject.
    let err = sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 6, 0)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");

    // With new_quality_status: ok.
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change,
             new_quality_status)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 6, 0, 'on_hold')",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .expect("status_change with new_quality_status ok");
}

#[tokio::test]
async fn quality_status_check_rejects_unknown_value() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;

    let err = sqlx::query(
        "INSERT INTO inventory_lots
            (product_id, location_id, receipt_posting_line_id, lot_code,
             receipt_date, original_quantity, unit_cost, cost_currency, quality_status)
         VALUES ((SELECT id FROM skus WHERE code='SKU-A'),

                 (SELECT id FROM locations WHERE code='MAIN'),
                 $1, 'LOT-Q', '2026-04-10', 100, 10, 'USD', 'unknown_status')",
    )
    .bind(pl)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");
}

// ============================================================
// Append-only triggers
// ============================================================

#[tokio::test]
async fn inventory_lots_blocks_update_and_delete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-AO", "2026-04-10", "100", "10").await;

    let upd = sqlx::query("UPDATE inventory_lots SET unit_cost = 99 WHERE lot_id = $1")
        .bind(lot_id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(upd.as_database_error().unwrap().code().unwrap(), "P9999");

    let del = sqlx::query("DELETE FROM inventory_lots WHERE lot_id = $1")
        .bind(lot_id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(del.as_database_error().unwrap().code().unwrap(), "P9999");
}

#[tokio::test]
async fn inventory_lot_events_blocks_update_and_delete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-AE", "2026-04-10", "100", "10").await;

    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 1, -5)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap();

    let upd = sqlx::query(
        "UPDATE inventory_lot_events SET quantity_change = -1 WHERE lot_id = $1",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(upd.as_database_error().unwrap().code().unwrap(), "P9999");

    let del =
        sqlx::query("DELETE FROM inventory_lot_events WHERE lot_id = $1")
            .bind(lot_id)
            .execute(&pool)
            .await
            .unwrap_err();
    assert_eq!(del.as_database_error().unwrap().code().unwrap(), "P9999");
}

// ============================================================
// Residual helper
// ============================================================

#[tokio::test]
async fn remaining_qty_returns_null_when_lot_missing() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let q: Option<String> = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty(99999, '2026-04-10'::DATE)::TEXT",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(q.is_none(), "missing lot should return NULL");
}

#[tokio::test]
async fn remaining_qty_starts_at_original_with_no_events() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-R0", "2026-04-10", "100", "10").await;

    let q: String = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty($1, '2026-04-10'::DATE)::TEXT",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(q, "100.000000");
}

#[tokio::test]
async fn remaining_qty_decrements_with_issue_and_increments_with_adjust_in() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_id = stage_lot(&pool, "SKU-A", "MAIN", pl, "LOT-R1", "2026-04-10", "100", "10").await;

    // Issue 30
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-15'::DATE, 1, -30)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap();

    // Adjust in 5
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-20'::DATE, 7, 5)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap();

    // Expiration writeoff 10
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-25'::DATE, 5, -10)",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap();

    // Status change (qty=0, should not affect)
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change,
             new_quality_status)
         VALUES ($1, '2026-04-10'::DATE, '2026-04-26'::DATE, 6, 0, 'on_hold')",
    )
    .bind(lot_id)
    .execute(&pool)
    .await
    .unwrap();

    let q: String = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty($1, '2026-04-10'::DATE)::TEXT",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 100 - 30 + 5 - 10 = 65
    assert_eq!(q, "65.000000");
}

// ============================================================
// skus.tracked_by enum
// ============================================================

#[tokio::test]
async fn tracked_by_default_is_none_for_existing_skus() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let row: (String,) = sqlx::query_as(
        "SELECT tracked_by::text FROM skus WHERE code = 'SKU-A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "none");
}

#[tokio::test]
async fn tracked_by_accepts_lot_serial_and_lot_and_serial() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    for v in &["lot", "serial", "lot_and_serial", "none"] {
        sqlx::query("UPDATE skus SET tracked_by = $1::inventory_tracking WHERE code = 'SKU-A'")
            .bind(v)
            .execute(&pool)
            .await
            .expect(&format!("set tracked_by = {v}"));
    }
}

// ============================================================
// accounts.lot_id partition + UK extension
// ============================================================

#[tokio::test]
async fn accounts_lot_id_check_blocks_non_inventory_kinds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Try to set lot_id on a 'cash' account — must be rejected.
    let cash_id: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'cash' AND currency = 'USD' LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let err = sqlx::query("UPDATE accounts SET lot_id = 1 WHERE id = $1")
        .bind(cash_id)
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");
}

#[tokio::test]
async fn accounts_uk_allows_distinct_lot_ids_at_same_sku_location() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Stand up a real lot to satisfy the (logical) lot_id reference.
    let pl1 = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_a =
        stage_lot(&pool, "SKU-A", "MAIN", pl1, "LOT-A", "2026-04-10", "100", "10").await;
    let pl2 = stage_posting_line(&pool, "SKU-A", 100).await;
    let lot_b =
        stage_lot(&pool, "SKU-A", "MAIN", pl2, "LOT-B", "2026-04-11", "100", "12").await;

    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    // Insert two distinct stock_available accounts at same (sku, loc) with
    // different lot_ids — the COALESCE(lot_id, 0) UK must allow this.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side, lot_id)
         VALUES ('stock_available'::account_kind, 'qty'::ledger_kind,
                 $1::UUID, $2::UUID, 'debit'::balance_direction, $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(lot_a)
    .execute(&pool)
    .await
    .expect("first lot_id slot");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side, lot_id)
         VALUES ('stock_available'::account_kind, 'qty'::ledger_kind,
                 $1::UUID, $2::UUID, 'debit'::balance_direction, $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(lot_b)
    .execute(&pool)
    .await
    .expect("second lot_id slot");

    // Inserting a third row at the same (sku, loc, lot_a) duplicates →
    // unique violation 23505.
    let err = sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side, lot_id)
         VALUES ('stock_available'::account_kind, 'qty'::ledger_kind,
                 $1::UUID, $2::UUID, 'debit'::balance_direction, $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(lot_a)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23505");
}

// ============================================================
// inventory_reservations.lot_id + lot_specific
// ============================================================

#[tokio::test]
async fn lot_specific_true_requires_lot_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;
    let so_id: String = sqlx::query_scalar(
        "INSERT INTO sales_orders (status) VALUES ('open') RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let so_line_id = fresh_uuid(&pool).await;

    // lot_specific=TRUE with lot_id NULL → CHECK violation.
    let err = sqlx::query(
        "INSERT INTO inventory_reservations
            (sku_id, location_id, qty, so_id, so_line_id, expires_at,
             lot_specific)
         VALUES ($1::UUID, $2::UUID, 10, $3::UUID, $4::UUID,
                 now() + INTERVAL '1 hour', TRUE)",
    )
    .bind(&s)
    .bind(&l)
    .bind(&so_id)
    .bind(&so_line_id)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23514");

    // lot_specific=FALSE with lot_id NULL → ok.
    sqlx::query(
        "INSERT INTO inventory_reservations
            (sku_id, location_id, qty, so_id, so_line_id, expires_at,
             lot_specific)
         VALUES ($1::UUID, $2::UUID, 10, $3::UUID, $4::UUID,
                 now() + INTERVAL '1 hour', FALSE)",
    )
    .bind(&s)
    .bind(&l)
    .bind(&so_id)
    .bind(&so_line_id)
    .execute(&pool)
    .await
    .expect("lot_specific=false + lot_id=null ok");
}

// ============================================================
// Business-key UK
// ============================================================

#[tokio::test]
async fn lot_business_key_blocks_same_day_duplicate() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl1 = stage_posting_line(&pool, "SKU-A", 100).await;
    stage_lot(&pool, "SKU-A", "MAIN", pl1, "LOT-DUP", "2026-04-10", "100", "10").await;

    let pl2 = stage_posting_line(&pool, "SKU-A", 50).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;
    // Same lot_code, same product, same date → 23505.
    let err = sqlx::query(
        "INSERT INTO inventory_lots
            (product_id, location_id, receipt_posting_line_id, lot_code,
             receipt_date, original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, $2::UUID, $3, 'LOT-DUP', '2026-04-10', 50, 12, 'USD')",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl2)
    .execute(&pool)
    .await
    .unwrap_err();
    assert_eq!(err.as_database_error().unwrap().code().unwrap(), "23505");
}
