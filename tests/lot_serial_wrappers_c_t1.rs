//! T1 probes for sxl2.5 wrappers C: post_lot_transfer +
//! _wo_emit_bom_lines (rm_issue_to_wo path) extended for
//! tracked_by='lot_and_serial' (mig 0065, acct-sxl2.5).
//!
//!   Y1 — lot_transfer with unit_ids moves units to dest lot+loc;
//!        type=3 unit events emitted; status stays 'available'.
//!   Y2 — lot_transfer lot_and_serial without unit_ids → P0006.
//!   Y3 — lot_transfer unit_ids length mismatch → P0006.
//!   Y4 — lot_transfer units span two source lots → P0006.
//!   Y5 — lot_transfer round-trip A→B→A leaves units active and
//!        partial UNIQUE intact (same serial transferable through
//!        the chain).
//!   Y6 — rm_issue_to_wo (post_wo_start at first-op fire) with
//!        lot_and_serial component creates wo_unit_consumption rows.
//!   Y7 — rm_issue_to_wo flips picked units to 'consumed' and
//!        stamps work_order_id; emits type=2 unit events.
//!   Y8 — wo_unit_consumption append-only trigger blocks
//!        UPDATE / DELETE (P9999).
//!   Y9 — idempotent replay of post_lot_transfer does not
//!        duplicate units / events / dest lots.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Scaffolding
// ============================================================

async fn fresh_loc(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_sku(
    pool: &PgPool,
    code: &str,
    cost_method: &str,
    tracked_by: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', $2::cost_method, $3::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .bind(tracked_by)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    location_id: Option<&str>,
    routing_op: Option<i32>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (
            kind, ledger_kind, currency, sku_id, location_id,
            routing_op, counterparty_id, normal_side
         ) VALUES ($1::account_kind, $2::ledger_kind, $3,
                   $4::UUID, $5::UUID, $6, $7::UUID,
                   $8::balance_direction)
         RETURNING id",
    )
    .bind(kind).bind(ledger_kind).bind(currency)
    .bind(sku_id).bind(location_id).bind(routing_op)
    .bind(counterparty_id).bind(normal_side)
    .fetch_one(pool).await.unwrap()
}

#[allow(dead_code)]
struct Sf {
    sku: String,
    loc_a: String,
    loc_b: String,
}

/// Build a lot_fifo + lot_and_serial SKU with stock at two
/// locations so we can transfer between them.
async fn scaffold_transfer(pool: &PgPool, suffix: &str) -> Sf {
    let sku = fresh_sku(pool, &format!("LS-{suffix}"), "lot_fifo", "lot_and_serial").await;
    let loc_a = fresh_loc(pool, &format!("LS-{suffix}-A")).await;
    let loc_b = fresh_loc(pool, &format!("LS-{suffix}-B")).await;

    for loc in [&loc_a, &loc_b] {
        open_account(pool, "stock_available", "qty", None, Some(&sku), Some(loc), None, None, "debit").await;
        open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(loc), None, None, "debit").await;
    }

    Sf { sku, loc_a, loc_b }
}

/// Seed N units at loc via post_inventory_adjustment +qty.
async fn seed_units(
    pool: &PgPool,
    sf: &Sf,
    loc: &str,
    qty: i64,
    unit_cost: i64,
    lot_code: &str,
    serials: &[&str],
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let meta = json!({
        "lot_code":     lot_code,
        "unit_serials": serials,
    });
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, $4, 'USD', 'raw',
            '2026-04-10'::DATE, $5::UUID, $6::UUID, NULL, $7::JSONB
         )::text",
    )
    .bind(&sf.sku).bind(loc).bind(qty).bind(unit_cost)
    .bind(&posted_by).bind(&key).bind(meta)
    .fetch_one(pool).await.unwrap();
}

async fn call_lot_transfer(
    pool: &PgPool,
    from: &str,
    to: &str,
    lines: serde_json::Value,
    idem: Option<String>,
) -> Result<String, sqlx::Error> {
    call_lot_transfer_at(pool, from, to, lines, idem, "2026-04-15").await
}

async fn call_lot_transfer_at(
    pool: &PgPool,
    from: &str,
    to: &str,
    lines: serde_json::Value,
    idem: Option<String>,
    business_date: &str,
) -> Result<String, sqlx::Error> {
    let posted_by = fresh_uuid(pool).await;
    let key = match idem {
        Some(k) => k,
        None => fresh_uuid(pool).await,
    };
    sqlx::query_scalar(
        "SELECT post_lot_transfer(
            $1::UUID, $2::UUID, $3::JSONB,
            $6::DATE, $4::UUID, $5::UUID, NULL
         )::text",
    )
    .bind(from).bind(to).bind(lines)
    .bind(&posted_by).bind(&key).bind(business_date)
    .fetch_one(pool).await
}

// ============================================================
// Tests — post_lot_transfer
// ============================================================

#[tokio::test]
async fn y1_lot_transfer_with_unit_ids_moves_units() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_transfer(&pool, "Y1").await;
    seed_units(&pool, &sf, &sf.loc_a, 3, 10_00, "Y1-LOT-A",
               &["Y1-U-001", "Y1-U-002", "Y1-U-003"]).await;

    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID
            AND current_location_id = $2::UUID
          ORDER BY serial_no",
    )
    .bind(&sf.sku).bind(&sf.loc_a)
    .fetch_all(&pool).await.unwrap();
    assert_eq!(unit_ids.len(), 3);

    let lines = json!([{
        "sku_id":   sf.sku,
        "qty":      3,
        "unit_ids": unit_ids,
    }]);
    let _doc = call_lot_transfer(&pool, &sf.loc_a, &sf.loc_b, lines, None)
        .await
        .expect("y1 transfer");

    // Units now at loc_b, status still 'available'.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT serial_no, status::text FROM inventory_units
          WHERE product_id = $1::UUID
            AND current_location_id = $2::UUID
          ORDER BY serial_no",
    )
    .bind(&sf.sku).bind(&sf.loc_b)
    .fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);
    for r in &rows {
        assert_eq!(r.1, "available");
    }

    // One type=3 transfer event per unit.
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events
          WHERE unit_id = ANY($1) AND event_type = 3",
    )
    .bind(&unit_ids)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(n_events, 3);
}

#[tokio::test]
async fn y2_lot_transfer_lot_and_serial_without_unit_ids_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_transfer(&pool, "Y2").await;
    seed_units(&pool, &sf, &sf.loc_a, 2, 10_00, "Y2-LOT-A",
               &["Y2-U-001", "Y2-U-002"]).await;

    // Line omits unit_ids and unit_serials — lot_and_serial requires one.
    let lines = json!([{ "sku_id": sf.sku, "qty": 2 }]);
    expect_sqlstate("P0006", || async {
        call_lot_transfer(&pool, &sf.loc_a, &sf.loc_b, lines.clone(), None).await
    }).await;
}

#[tokio::test]
async fn y3_lot_transfer_unit_ids_length_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_transfer(&pool, "Y3").await;
    seed_units(&pool, &sf, &sf.loc_a, 3, 10_00, "Y3-LOT-A",
               &["Y3-U-001", "Y3-U-002", "Y3-U-003"]).await;

    let some_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID AND serial_no IN ('Y3-U-001','Y3-U-002')
          ORDER BY serial_no",
    )
    .bind(&sf.sku).fetch_all(&pool).await.unwrap();
    assert_eq!(some_ids.len(), 2);

    // 2 ids but qty=3 → length mismatch.
    let lines = json!([{ "sku_id": sf.sku, "qty": 3, "unit_ids": some_ids }]);
    expect_sqlstate("P0006", || async {
        call_lot_transfer(&pool, &sf.loc_a, &sf.loc_b, lines.clone(), None).await
    }).await;
}

#[tokio::test]
async fn y4_lot_transfer_units_across_two_lots_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_transfer(&pool, "Y4").await;
    seed_units(&pool, &sf, &sf.loc_a, 2, 10_00, "Y4-LOT-A",
               &["Y4-A-001", "Y4-A-002"]).await;
    seed_units(&pool, &sf, &sf.loc_a, 2, 10_00, "Y4-LOT-B",
               &["Y4-B-001", "Y4-B-002"]).await;

    let mixed_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID
            AND serial_no IN ('Y4-A-001','Y4-B-001')
          ORDER BY serial_no",
    )
    .bind(&sf.sku).fetch_all(&pool).await.unwrap();
    assert_eq!(mixed_ids.len(), 2);

    let lines = json!([{ "sku_id": sf.sku, "qty": 2, "unit_ids": mixed_ids }]);
    expect_sqlstate("P0006", || async {
        call_lot_transfer(&pool, &sf.loc_a, &sf.loc_b, lines.clone(), None).await
    }).await;
}

#[tokio::test]
async fn y5_lot_transfer_round_trip_preserves_partial_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_transfer(&pool, "Y5").await;
    seed_units(&pool, &sf, &sf.loc_a, 2, 10_00, "Y5-LOT-A",
               &["Y5-U-001", "Y5-U-002"]).await;

    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sf.sku).fetch_all(&pool).await.unwrap();

    // A → B
    let lines = json!([{ "sku_id": sf.sku, "qty": 2, "unit_ids": unit_ids }]);
    call_lot_transfer_at(&pool, &sf.loc_a, &sf.loc_b, lines, None, "2026-04-15").await
        .expect("y5 a->b");

    // B → A using same units (resolution by unit_serials this time).
    // Different business_date so the dest lot row doesn't collide on
    // the existing (product_id, lot_code, receipt_date) UNIQUE.
    let lines = json!([{
        "sku_id":        sf.sku,
        "qty":           2,
        "unit_serials": ["Y5-U-001", "Y5-U-002"],
    }]);
    call_lot_transfer_at(&pool, &sf.loc_b, &sf.loc_a, lines, None, "2026-04-16").await
        .expect("y5 b->a");

    // Final state: units back at loc_a, still 'available'.
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT serial_no, status::text,
                (current_location_id = $2::UUID)::text
           FROM inventory_units
          WHERE product_id = $1::UUID
          ORDER BY serial_no",
    )
    .bind(&sf.sku).bind(&sf.loc_a)
    .fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert_eq!(r.1, "available");
        assert_eq!(r.2.as_deref(), Some("true"));
    }

    // Each unit has 2 type=3 events (one per transfer).
    let total_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events
          WHERE unit_id = ANY($1) AND event_type = 3",
    )
    .bind(&unit_ids)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(total_events, 4);
}

#[tokio::test]
async fn y9_lot_transfer_idempotent_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_transfer(&pool, "Y9").await;
    seed_units(&pool, &sf, &sf.loc_a, 2, 10_00, "Y9-LOT-A",
               &["Y9-U-001", "Y9-U-002"]).await;

    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sf.sku).fetch_all(&pool).await.unwrap();

    let lines = json!([{ "sku_id": sf.sku, "qty": 2, "unit_ids": unit_ids }]);
    let key = fresh_uuid(&pool).await;
    let doc1 = call_lot_transfer(&pool, &sf.loc_a, &sf.loc_b, lines.clone(),
                                  Some(key.clone())).await
        .expect("y9 first");
    let doc2 = call_lot_transfer(&pool, &sf.loc_a, &sf.loc_b, lines,
                                  Some(key)).await
        .expect("y9 replay");
    assert_eq!(doc1, doc2);

    // Still exactly 2 type=3 events (replay short-circuits at top).
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events
          WHERE unit_id = ANY($1) AND event_type = 3",
    )
    .bind(&unit_ids)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(n_events, 2);
}

// ============================================================
// Scaffolding — rm_issue_to_wo path (Y6 / Y7 / Y8)
// ============================================================

#[allow(dead_code)]
struct WoSf {
    parent: String,
    comp: String,
    raw_loc: String,
    fg_loc: String,
}

/// Build a standard parent + lot_fifo+lot_and_serial component
/// with a 1:1 BOM at op_arrival.
async fn scaffold_wo(pool: &PgPool, suffix: &str) -> WoSf {
    let parent = fresh_sku(pool, &format!("PAR-{suffix}"), "standard", "none").await;
    let comp = fresh_sku(pool, &format!("CMP-LS-{suffix}"), "lot_fifo", "lot_and_serial").await;

    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, 100, '2026-01-01', $2::UUID, $3::UUID)",
    )
    .bind(&parent).bind(&posted_by).bind(&key).execute(pool).await.unwrap();

    let raw_loc = fresh_loc(pool, &format!("WO-{suffix}-RAW")).await;
    let fg_loc = fresh_loc(pool, &format!("WO-{suffix}-FG")).await;

    open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, None, "debit").await;

    open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    // BOM: 1 component per parent at op 10, applies_at_op=10 with fire_at='op_arrival'.
    let bom_id: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(&parent).fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', 100,
                 $2::UUID, $3::UUID, 1)",
    )
    .bind(bom_id).bind(&comp).bind(&raw_loc).execute(pool).await.unwrap();

    WoSf { parent, comp, raw_loc, fg_loc }
}

async fn seed_comp_units(
    pool: &PgPool,
    sf: &WoSf,
    qty: i64,
    lot_code: &str,
    serials: &[&str],
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let meta = json!({
        "lot_code":     lot_code,
        "unit_serials": serials,
    });
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, 50, 'USD', 'raw',
            '2026-04-10'::DATE, $4::UUID, $5::UUID, NULL, $6::JSONB
         )::text",
    )
    .bind(&sf.comp).bind(&sf.raw_loc).bind(qty)
    .bind(&posted_by).bind(&key).bind(meta)
    .fetch_one(pool).await.unwrap();
}

async fn create_wo(pool: &PgPool, sf: &WoSf, suffix: &str, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    let wo_id = sqlx::query_scalar::<_, String>(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id,
                                  qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID)
         RETURNING id::text",
    )
    .bind(format!("WO-{suffix}"))
    .bind(&sf.parent).bind(&sf.fg_loc).bind(qty).bind(&posted_by)
    .fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO wo_routings (wo_id, routing_op, op_name)
         VALUES ($1::UUID, 10, 'ASSEMBLE')",
    )
    .bind(&wo_id).execute(pool).await.unwrap();
    wo_id
}

async fn call_wo_start(pool: &PgPool, wo_id: &str) -> Result<(), sqlx::Error> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL, NULL)::text",
    )
    .bind(wo_id).bind(&posted_by).bind(&key)
    .fetch_one(pool).await.map(|_| ())
}

// ============================================================
// Tests — rm_issue_to_wo path
// ============================================================

#[tokio::test]
async fn y6_rm_issue_creates_wo_unit_consumption_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_wo(&pool, "Y6").await;
    seed_comp_units(&pool, &sf, 3, "Y6-LOT-A",
                    &["Y6-U-001", "Y6-U-002", "Y6-U-003"]).await;

    let wo_id = create_wo(&pool, &sf, "Y6", 3).await;
    call_wo_start(&pool, &wo_id).await.expect("y6 wo_start");

    // 3 rows in wo_unit_consumption (one per consumed unit).
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM wo_unit_consumption WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id).fetch_one(&pool).await.unwrap();
    assert_eq!(n, 3);

    // Component sku stamped correctly.
    let comp_sku: String = sqlx::query_scalar(
        "SELECT DISTINCT component_sku_id::text FROM wo_unit_consumption
          WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id).fetch_one(&pool).await.unwrap();
    assert_eq!(comp_sku, sf.comp);
}

#[tokio::test]
async fn y7_rm_issue_flips_units_to_consumed_and_stamps_wo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_wo(&pool, "Y7").await;
    seed_comp_units(&pool, &sf, 2, "Y7-LOT-A",
                    &["Y7-U-001", "Y7-U-002"]).await;

    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sf.comp).fetch_all(&pool).await.unwrap();
    assert_eq!(unit_ids.len(), 2);

    let wo_id = create_wo(&pool, &sf, "Y7", 2).await;
    call_wo_start(&pool, &wo_id).await.expect("y7 wo_start");

    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT status::text, work_order_id::text
           FROM inventory_units
          WHERE unit_id = ANY($1) ORDER BY unit_id",
    )
    .bind(&unit_ids)
    .fetch_all(&pool).await.unwrap();
    for r in &rows {
        assert_eq!(r.0, "consumed");
        assert_eq!(r.1.as_deref(), Some(wo_id.as_str()));
    }

    // type=2 events per unit.
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events
          WHERE unit_id = ANY($1) AND event_type = 2",
    )
    .bind(&unit_ids)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(n_events, 2);
}

#[tokio::test]
async fn y8_wo_unit_consumption_append_only_blocks_update_and_delete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold_wo(&pool, "Y8").await;
    seed_comp_units(&pool, &sf, 1, "Y8-LOT-A", &["Y8-U-001"]).await;

    let wo_id = create_wo(&pool, &sf, "Y8", 1).await;
    call_wo_start(&pool, &wo_id).await.expect("y8 wo_start");

    expect_sqlstate("P9999", || async {
        sqlx::query("UPDATE wo_unit_consumption SET routing_op = 99 WHERE wo_id = $1::UUID")
            .bind(&wo_id).execute(&pool).await
    }).await;

    expect_sqlstate("P9999", || async {
        sqlx::query("DELETE FROM wo_unit_consumption WHERE wo_id = $1::UUID")
            .bind(&wo_id).execute(&pool).await
    }).await;
}
