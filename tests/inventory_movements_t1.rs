//! T1 probes for `inventory_movements` (mig 0025, acct-wb75.3.1).
//! Phase D D1 of the convergence plan. Pin schema invariants of the
//! foundational subledger BEFORE the dispatcher integration in
//! D2/D3 wires real writes. The dispatcher does not yet write to
//! this table; these probes drive direct INSERTs to verify schema
//! constraints, partition routing, lookup seeds, and the rollover
//! helper.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

const POSTED_BY: &str = "00000000-0000-0000-0000-0000000000bb";

/// Stage a real posting_line so we can exercise the FK target.
/// Returns the posting_line.id. Uses the same cycle_count_adj
/// pattern as `seed_stock` so the row is realistic but the
/// inventory_movements row we then insert is hand-crafted.
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

// ============================================================
// Lookup tables
// ============================================================

#[tokio::test]
async fn event_types_seeded_with_16_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM inventory_movement_event_types")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 16, "expected 16 event_types rows");

    // Spot-check the keystone values that backfill (D4) will reference.
    let receipt: String =
        sqlx::query_scalar("SELECT name FROM inventory_movement_event_types WHERE event_type = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(receipt, "receipt");

    let cost_adj: String =
        sqlx::query_scalar("SELECT name FROM inventory_movement_event_types WHERE event_type = 16")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        cost_adj, "cost_adjustment",
        "event_type 16 = the append-only correction marker D6 will write"
    );
}

#[tokio::test]
async fn event_type_name_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    expect_sqlstate("23505", || async {
        sqlx::query("INSERT INTO inventory_movement_event_types (event_type, name) VALUES (99, 'receipt')")
            .execute(&pool)
            .await
    })
    .await;
}

#[tokio::test]
async fn cost_books_seeded_with_primary() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let row: (i16, String) = sqlx::query_as("SELECT id, name FROM cost_books WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row, (1, "primary".to_string()));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM cost_books")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "phase D1 ships a single book; multi-book deferred to acct-zf80");
}

// ============================================================
// FK constraints
// ============================================================

#[tokio::test]
async fn fk_posting_line_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO inventory_movements
                (product_id, legal_entity_id, location_id, event_type,
                 movement_date, quantity, actual_unit_cost, cost_currency,
                 posting_line_id)
             VALUES ($1::UUID, 1, $2::UUID, 1, '2026-04-15',
                     1.0, 100.0, 'USD', 99999999)",
        )
        .bind(&s)
        .bind(&l)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_product_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO inventory_movements
                (product_id, legal_entity_id, location_id, event_type,
                 movement_date, quantity, actual_unit_cost, cost_currency,
                 posting_line_id)
             VALUES ('00000000-0000-0000-0000-deadbeef0000'::UUID,
                     1, $1::UUID, 1, '2026-04-15',
                     1.0, 100.0, 'USD', $2)",
        )
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_event_type_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO inventory_movements
                (product_id, legal_entity_id, location_id, event_type,
                 movement_date, quantity, actual_unit_cost, cost_currency,
                 posting_line_id)
             VALUES ($1::UUID, 1, $2::UUID, 99, '2026-04-15',
                     1.0, 100.0, 'USD', $3)",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_cost_book_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO inventory_movements
                (product_id, legal_entity_id, cost_book_id, location_id,
                 event_type, movement_date, quantity, actual_unit_cost,
                 cost_currency, posting_line_id)
             VALUES ($1::UUID, 1, 99, $2::UUID, 1, '2026-04-15',
                     1.0, 100.0, 'USD', $3)",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// NOT NULL invariants on the keystone columns
// ============================================================

#[tokio::test]
async fn actual_unit_cost_not_null_required() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23502", || async {
        sqlx::query(
            "INSERT INTO inventory_movements
                (product_id, legal_entity_id, location_id, event_type,
                 movement_date, quantity, cost_currency, posting_line_id)
             VALUES ($1::UUID, 1, $2::UUID, 1, '2026-04-15',
                     1.0, 'USD', $3)",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn cost_book_default_is_one() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    sqlx::query(
        "INSERT INTO inventory_movements
            (product_id, legal_entity_id, location_id, event_type,
             movement_date, quantity, actual_unit_cost, cost_currency,
             posting_line_id)
         VALUES ($1::UUID, 1, $2::UUID, 1, '2026-04-15',
                 1.0, 100.0, 'USD', $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl)
    .execute(&pool)
    .await
    .expect("insert with default cost_book_id");

    let book_id: i16 =
        sqlx::query_scalar("SELECT cost_book_id FROM inventory_movements WHERE posting_line_id = $1")
            .bind(pl)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(book_id, 1, "default cost_book_id = 1 ('primary')");
}

#[tokio::test]
async fn ppv_amount_default_zero() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    sqlx::query(
        "INSERT INTO inventory_movements
            (product_id, legal_entity_id, location_id, event_type,
             movement_date, quantity, actual_unit_cost, cost_currency,
             posting_line_id)
         VALUES ($1::UUID, 1, $2::UUID, 1, '2026-04-15',
                 1.0, 100.0, 'USD', $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl)
    .execute(&pool)
    .await
    .expect("insert without ppv_amount");

    // Default 0 — verified via numeric scan; sqlx maps NUMERIC to Decimal,
    // but for a default-driven probe a scalar text round-trip is enough.
    let ppv_text: String =
        sqlx::query_scalar("SELECT ppv_amount::TEXT FROM inventory_movements WHERE posting_line_id = $1")
            .bind(pl)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        ppv_text == "0" || ppv_text == "0.0000",
        "default ppv_amount: got {ppv_text:?}"
    );
}

// ============================================================
// Quantity sign convention
// ============================================================

#[tokio::test]
async fn quantity_accepts_signed_values() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl_pos = stage_posting_line(&pool, "SKU-A", 1).await;
    let pl_neg = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    // Positive: receipt (event_type=1).
    sqlx::query(
        "INSERT INTO inventory_movements
            (product_id, legal_entity_id, location_id, event_type,
             movement_date, quantity, actual_unit_cost, cost_currency,
             posting_line_id)
         VALUES ($1::UUID, 1, $2::UUID, 1, '2026-04-15',
                 5.0, 100.0, 'USD', $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl_pos)
    .execute(&pool)
    .await
    .expect("positive quantity insert");

    // Negative: issue (event_type=2). Quantity is SIGNED on this
    // table — sign distinguishes receipt-like from issue-like
    // movements. Contrast with posting_line_inventory.quantity which
    // is unsigned (sign there lives in debit/credit direction).
    sqlx::query(
        "INSERT INTO inventory_movements
            (product_id, legal_entity_id, location_id, event_type,
             movement_date, quantity, actual_unit_cost, cost_currency,
             posting_line_id)
         VALUES ($1::UUID, 1, $2::UUID, 2, '2026-04-15',
                 -3.0, 100.0, 'USD', $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl_neg)
    .execute(&pool)
    .await
    .expect("negative quantity insert");

    let net_qty_text: String = sqlx::query_scalar(
        "SELECT SUM(quantity)::TEXT FROM inventory_movements
         WHERE product_id = $1::UUID",
    )
    .bind(&s)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 5.0 + (-3.0) = 2.0 — sign convention preserved.
    assert!(
        net_qty_text.starts_with('2'),
        "net qty 5 + (-3) should sum to 2, got {net_qty_text:?}"
    );
}

// ============================================================
// Partitioning
// ============================================================

#[tokio::test]
async fn baked_partitions_span_2026_to_2027() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM pg_inherits i
           JOIN pg_class c    ON c.oid = i.inhparent
          WHERE c.relname = 'inventory_movements'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 24, "expected 24 monthly partitions baked (2026-01 through 2027-12)");

    // Spot-check a fixture-relevant partition exists.
    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM pg_class
          WHERE relname = 'inventory_movements_2026_04'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(exists, 1, "2026-04 partition (fixture period) must exist");
}

#[tokio::test]
async fn insert_outside_partition_window_fails() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    // 2030 is well outside the 24-month bake window. PG raises 23514
    // ('no partition of relation "inventory_movements" found for row').
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO inventory_movements
                (product_id, legal_entity_id, location_id, event_type,
                 movement_date, quantity, actual_unit_cost, cost_currency,
                 posting_line_id)
             VALUES ($1::UUID, 1, $2::UUID, 1, '2030-04-15',
                     1.0, 100.0, 'USD', $3)",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn rollover_helper_is_idempotent() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // 2026-04 partition was baked at migration time; calling the helper
    // again must not error (CREATE TABLE IF NOT EXISTS).
    sqlx::query("SELECT _create_inventory_movements_partition('2026-04-01'::DATE)")
        .execute(&pool)
        .await
        .expect("idempotent re-create");

    // Create a brand-new partition outside the bake window, then verify
    // we can now insert a row dated to it.
    sqlx::query("SELECT _create_inventory_movements_partition('2028-03-01'::DATE)")
        .execute(&pool)
        .await
        .expect("create new partition");

    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pg_class WHERE relname = 'inventory_movements_2028_03'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(exists, 1, "rollover helper created 2028-03 partition");

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;
    sqlx::query(
        "INSERT INTO inventory_movements
            (product_id, legal_entity_id, location_id, event_type,
             movement_date, quantity, actual_unit_cost, cost_currency,
             posting_line_id)
         VALUES ($1::UUID, 1, $2::UUID, 1, '2028-03-15',
                 1.0, 100.0, 'USD', $3)",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl)
    .execute(&pool)
    .await
    .expect("insert into newly-created partition");
}

// ============================================================
// D2 / D3 / D4 / D5 are not yet wired — confirming the table is
// EMPTY after a normal seed + posting flow. This is the regression
// gate that catches accidental dispatcher writes before they're
// sanctioned.
// ============================================================

#[tokio::test]
async fn dispatcher_does_not_write_yet() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Run a normal posting that DOES produce a posting_line_inventory
    // row (Phase C is wired). After D1 ships, inventory_movements
    // should still be empty — D2 wires the dispatcher. This test
    // intentionally fails when D2 lands and is rewritten there.
    let _ = stage_posting_line(&pool, "SKU-A", 5).await;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM inventory_movements")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "D1 ships schema only; dispatcher integration is D2/D3 — \
         this test will need to be rewritten when D2 lands"
    );

    // Sanity: the posting_line_inventory row from C1 IS present.
    let pli_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM posting_line_inventory")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        pli_count >= 1,
        "Phase C extension must still write; got {pli_count} rows"
    );

    // Suppress unused-constant warning when the helper is only referenced
    // here through the fixture's bootstrap actor UUID.
    let _ = POSTED_BY;
}
