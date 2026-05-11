//! T1 probes for inventory_units + inventory_unit_events schema
//! (mig 0061, acct-sxl2.1).
//!
//! Schema-only probes — no dispatcher hooks or wrapper wiring yet.
//! Validates the structural invariants the sxl2.2+ work will build on.
//!
//!   S1 — row insertable with full happy-path payload (system serial +
//!        external serial + lot FK).
//!   S2 — partial UNIQUE (product_id, serial_no) blocks a second
//!        active row with the same serial.
//!   S3 — releasing the serial via status flip to a terminal state
//!        ('shipped' / 'consumed' / 'scrapped') frees the slot for
//!        serial reuse on a new active unit.
//!   S4 — partial UNIQUE (product_id, external_serial_no) blocks a
//!        second active row with the same external serial.
//!   S5 — inventory_unit_events append-only trigger blocks UPDATE
//!        and DELETE (P9999 from block_inventory_lot_modifications).
//!   S6 — inventory_units MUTABLE per Q6: UPDATE status,
//!        current_location_id, lot_id succeeds (no trigger on units).
//!   S7 — inventory_reservations.unit_ids column accessible + GIN
//!        index in place.
//!   S8 — 24-month partition coverage on inventory_unit_events
//!        (2026-01 through 2027-12; 24 partitions exist).
//!   S9 — composite FK (lot_id, lot_receipt_date) rejects orphan
//!        unit (no matching inventory_lots row).
//!   S10 — inventory_units FK from inventory_unit_events.unit_id
//!         rejects orphan event.

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Local scaffolding
// ============================================================

async fn fresh_sku_lot_and_serial(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method,
                 'lot_and_serial'::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Create a synthetic lot for unit FK targeting. We bypass the normal
/// post_po_receipt wrapper here because sxl2.1 is schema-only — the
/// dispatcher hooks land in sxl2.2. We need a real inventory_lots row
/// (composite PK + receipt_posting_line_id FK), so we synthesize one
/// by posting a minimal posting_lines row and inserting directly.
async fn synth_lot(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    lot_code: &str,
    qty: f64,
) -> (i64, String) {
    // Synthesize a posting_lines row so the lot's
    // receipt_posting_line_id FK has a target. We pick a non-inventory
    // reason/leg pair to avoid touching balances or apply_event's
    // class-dispatch — just a value-only ap/cash leg.
    let ap_acct: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind='ap' AND currency='USD' LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let cash_acct: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind='cash' AND currency='USD' LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let idem = fresh_uuid(pool).await;
    let posted_by = fresh_uuid(pool).await;
    let pl_id: i64 = sqlx::query_scalar(
        "INSERT INTO posting_lines (
            reason, document_kind, document_id, document_line_id,
            debit_account_id, credit_account_id, amount, qty,
            period_id, business_date, idempotency_key, posted_by
         ) VALUES (
            'ap_payment'::posting_line_reason, 'manual',
            $1::UUID, NULL, $2, $3, 1, NULL,
            (SELECT id FROM periods WHERE '2026-05-10' BETWEEN opens_at AND closes_at LIMIT 1),
            '2026-05-10'::DATE, $4::UUID, $5::UUID
         ) RETURNING id",
    )
    .bind(fresh_uuid(pool).await)
    .bind(cash_acct)
    .bind(ap_acct)
    .bind(&idem)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap();

    let lot_id: i64 = sqlx::query_scalar(
        "INSERT INTO inventory_lots (
            product_id, location_id, lot_code, receipt_posting_line_id,
            receipt_date, original_quantity, unit_cost, cost_currency
         ) VALUES ($1::UUID, $2::UUID, $3, $4, '2026-05-10'::DATE,
                   $5, 100, 'USD')
         RETURNING lot_id",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(lot_code)
    .bind(pl_id)
    .bind(qty)
    .fetch_one(pool)
    .await
    .unwrap();

    // Return the posting_line_id alongside for unit FK targeting.
    (lot_id, pl_id.to_string())
}

/// Insert a unit directly (sxl2.2 will route this through dispatcher
/// hooks; sxl2.1 only validates the schema accepts the shape).
async fn insert_unit(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    lot_id: i64,
    serial: &str,
    external: Option<&str>,
    posting_line_id: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO inventory_units (
            product_id, lot_id, lot_receipt_date, serial_no,
            external_serial_no, current_location_id,
            receipt_posting_line_id
         ) VALUES ($1::UUID, $2, '2026-05-10'::DATE, $3, $4, $5::UUID, $6)
         RETURNING unit_id",
    )
    .bind(sku_id)
    .bind(lot_id)
    .bind(serial)
    .bind(external)
    .bind(loc_id)
    .bind(posting_line_id)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn s1_unit_insertable_with_full_payload() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S1-SKU").await;
    let loc = fresh_location(&pool, "S1-LOC").await;
    let (lot_id, pl_id) = synth_lot(&pool, &sku, &loc, "S1-LOT-A", 5.0).await;

    let unit_id = insert_unit(
        &pool,
        &sku,
        &loc,
        lot_id,
        "S1-U001",
        Some("VENDOR-ABC-001"),
        pl_id.parse().unwrap(),
    )
    .await
    .unwrap();
    assert!(unit_id > 0);

    let (status, serial, ext): (String, String, Option<String>) = sqlx::query_as(
        "SELECT status::text, serial_no, external_serial_no
           FROM inventory_units WHERE unit_id = $1",
    )
    .bind(unit_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "available");
    assert_eq!(serial, "S1-U001");
    assert_eq!(ext.as_deref(), Some("VENDOR-ABC-001"));
}

#[tokio::test]
async fn s2_partial_unique_blocks_dup_active_serial() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S2-SKU").await;
    let loc = fresh_location(&pool, "S2-LOC").await;
    let (lot_id, pl_id) = synth_lot(&pool, &sku, &loc, "S2-LOT-A", 5.0).await;
    let pl: i64 = pl_id.parse().unwrap();

    let _ = insert_unit(&pool, &sku, &loc, lot_id, "S2-U001", None, pl)
        .await
        .unwrap();

    let err = insert_unit(&pool, &sku, &loc, lot_id, "S2-U001", None, pl)
        .await
        .unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("inventory_units_serial_unique")
            || s.contains("duplicate key"),
        "expected partial UNIQUE rejection, got: {s}"
    );
}

#[tokio::test]
async fn s3_terminal_status_releases_serial_slot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S3-SKU").await;
    let loc = fresh_location(&pool, "S3-LOC").await;
    let (lot_id, pl_id) = synth_lot(&pool, &sku, &loc, "S3-LOT-A", 5.0).await;
    let pl: i64 = pl_id.parse().unwrap();

    let first = insert_unit(&pool, &sku, &loc, lot_id, "S3-U001", None, pl)
        .await
        .unwrap();
    // Flip first unit to 'shipped' (terminal — releases the serial slot).
    sqlx::query("UPDATE inventory_units SET status='shipped' WHERE unit_id=$1")
        .bind(first)
        .execute(&pool)
        .await
        .unwrap();

    // Same serial on a fresh active unit must now succeed.
    let second = insert_unit(&pool, &sku, &loc, lot_id, "S3-U001", None, pl)
        .await
        .unwrap();
    assert_ne!(first, second);
}

#[tokio::test]
async fn s4_partial_unique_blocks_dup_active_external_serial() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S4-SKU").await;
    let loc = fresh_location(&pool, "S4-LOC").await;
    let (lot_id, pl_id) = synth_lot(&pool, &sku, &loc, "S4-LOT-A", 5.0).await;
    let pl: i64 = pl_id.parse().unwrap();

    let _ = insert_unit(
        &pool,
        &sku,
        &loc,
        lot_id,
        "S4-U001",
        Some("MFR-XYZ-001"),
        pl,
    )
    .await
    .unwrap();

    // Different system serial, same external — partial UNIQUE on external
    // fires.
    let err = insert_unit(
        &pool,
        &sku,
        &loc,
        lot_id,
        "S4-U002",
        Some("MFR-XYZ-001"),
        pl,
    )
    .await
    .unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("inventory_units_external_serial_unique")
            || s.contains("duplicate key"),
        "expected external_serial partial UNIQUE rejection, got: {s}"
    );

    // Inserting with external NULL should still succeed (partial filter
    // excludes NULL).
    let _null_ext = insert_unit(&pool, &sku, &loc, lot_id, "S4-U003", None, pl)
        .await
        .unwrap();
}

#[tokio::test]
async fn s5_unit_events_append_only_blocks_update_and_delete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S5-SKU").await;
    let loc = fresh_location(&pool, "S5-LOC").await;
    let (lot_id, pl_id) = synth_lot(&pool, &sku, &loc, "S5-LOT-A", 1.0).await;
    let pl: i64 = pl_id.parse().unwrap();
    let unit_id = insert_unit(&pool, &sku, &loc, lot_id, "S5-U001", None, pl)
        .await
        .unwrap();

    // Append an event (receipt).
    sqlx::query(
        "INSERT INTO inventory_unit_events (
            unit_id, event_date, event_type, posting_line_id, new_status
         ) VALUES ($1, '2026-05-10'::DATE, 1, $2, 'available')",
    )
    .bind(unit_id)
    .bind(pl)
    .execute(&pool)
    .await
    .unwrap();

    // UPDATE must be blocked by the append-only trigger.
    expect_sqlstate("P9999", || async {
        sqlx::query("UPDATE inventory_unit_events SET notes='tampered'")
            .execute(&pool)
            .await
    })
    .await;

    // DELETE must be blocked too.
    expect_sqlstate("P9999", || async {
        sqlx::query("DELETE FROM inventory_unit_events WHERE unit_id=$1")
            .bind(unit_id)
            .execute(&pool)
            .await
    })
    .await;
}

#[tokio::test]
async fn s6_units_mutable_status_location_lot_updateable() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S6-SKU").await;
    let loc_a = fresh_location(&pool, "S6-LOC-A").await;
    let loc_b = fresh_location(&pool, "S6-LOC-B").await;
    let (lot_a, pl_id_a) = synth_lot(&pool, &sku, &loc_a, "S6-LOT-A", 1.0).await;
    let (lot_b, _) = synth_lot(&pool, &sku, &loc_b, "S6-LOT-B", 1.0).await;
    let pl: i64 = pl_id_a.parse().unwrap();

    let unit_id = insert_unit(&pool, &sku, &loc_a, lot_a, "S6-U001", None, pl)
        .await
        .unwrap();

    // Q6: identity is mutable. Update status, current_location_id, lot_id,
    // lot_receipt_date in one go (mimics what post_lot_transfer will do).
    sqlx::query(
        "UPDATE inventory_units
            SET status = 'reserved',
                current_location_id = $1::UUID,
                lot_id = $2,
                lot_receipt_date = '2026-05-10'::DATE,
                updated_at = clock_timestamp()
          WHERE unit_id = $3",
    )
    .bind(&loc_b)
    .bind(lot_b)
    .bind(unit_id)
    .execute(&pool)
    .await
    .unwrap();

    let (status, loc, lot): (String, String, i64) = sqlx::query_as(
        "SELECT status::text, current_location_id::text, lot_id
           FROM inventory_units WHERE unit_id = $1",
    )
    .bind(unit_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "reserved");
    assert_eq!(loc, loc_b);
    assert_eq!(lot, lot_b);
}

#[tokio::test]
async fn s7_reservations_unit_ids_column_accessible() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Probe metadata to confirm column exists with the expected type.
    let (data_type, udt_name): (String, String) = sqlx::query_as(
        "SELECT data_type, udt_name
           FROM information_schema.columns
          WHERE table_name = 'inventory_reservations'
            AND column_name = 'unit_ids'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(data_type, "ARRAY");
    assert_eq!(udt_name, "_int8");

    // GIN partial index exists.
    let idx_def: Option<String> = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes
          WHERE indexname = 'inventory_reservations_unit_ids'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    let idx = idx_def.expect("inventory_reservations_unit_ids index missing");
    assert!(idx.contains("USING gin"), "expected GIN index, got: {idx}");
    assert!(idx.contains("unit_ids IS NOT NULL"));
}

#[tokio::test]
async fn s8_inventory_unit_events_24_month_partition_coverage() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
           FROM pg_inherits i
           JOIN pg_class c  ON c.oid  = i.inhrelid
           JOIN pg_class pc ON pc.oid = i.inhparent
          WHERE pc.relname = 'inventory_unit_events'
            AND c.relname  LIKE 'inventory_unit_events_____\\___%' ESCAPE '\\'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        n >= 24,
        "expected ≥24 monthly partitions on inventory_unit_events, got {n}"
    );

    // Confirm boundaries cover Jan 2026 and Dec 2027.
    let jan_2026: Option<String> = sqlx::query_scalar(
        "SELECT relname FROM pg_class WHERE relname = 'inventory_unit_events_2026_01'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(jan_2026.is_some(), "Jan 2026 partition missing");

    let dec_2027: Option<String> = sqlx::query_scalar(
        "SELECT relname FROM pg_class WHERE relname = 'inventory_unit_events_2027_12'",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert!(dec_2027.is_some(), "Dec 2027 partition missing");
}

#[tokio::test]
async fn s9_orphan_lot_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku_lot_and_serial(&pool, "S9-SKU").await;
    let loc = fresh_location(&pool, "S9-LOC").await;
    // Resolve a valid posting_line_id by synthesizing a lot we won't use.
    let (_, pl_id) = synth_lot(&pool, &sku, &loc, "S9-LOT-A", 1.0).await;
    let pl: i64 = pl_id.parse().unwrap();

    // Reference a non-existent lot_id.
    let err = sqlx::query(
        "INSERT INTO inventory_units (
            product_id, lot_id, lot_receipt_date, serial_no,
            current_location_id, receipt_posting_line_id
         ) VALUES ($1::UUID, 999999, '2026-05-10'::DATE, 'S9-U001',
                   $2::UUID, $3)",
    )
    .bind(&sku)
    .bind(&loc)
    .bind(pl)
    .execute(&pool)
    .await
    .unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("inventory_units_lot_id_lot_receipt_date_fkey")
            || s.contains("foreign key")
            || s.contains("violates foreign key constraint"),
        "expected composite FK rejection, got: {s}"
    );
}

#[tokio::test]
async fn s10_orphan_unit_rejected_from_events() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let err = sqlx::query(
        "INSERT INTO inventory_unit_events (
            unit_id, event_date, event_type, new_status
         ) VALUES (999999, '2026-05-10'::DATE, 1, 'available')",
    )
    .execute(&pool)
    .await
    .unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("inventory_unit_events_unit_id_fkey")
            || s.contains("foreign key")
            || s.contains("violates foreign key constraint"),
        "expected unit_id FK rejection, got: {s}"
    );
}
