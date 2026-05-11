//! T1 probes for sxl2.6 recon check #14: lot_unit_count_mismatch
//! (mig 0066, acct-sxl2.6).
//!
//!   R1 — clean lot_and_serial receipt: COUNT(active units) == residual
//!        → 0 alerts of kind 'lot_unit_count_mismatch'.
//!   R2 — phantom unit (synthesize a stray inventory_lot_events drain
//!        without flipping unit status) → check #14 fires.
//!   R3 — phantom residual (synthesize a unit status flip to 'consumed'
//!        without emitting the lot drain event) → check #14 fires.
//!   R4 — tracked_by='lot' SKU (no units) → check #14 does not fire.
//!   R5 — after rm_issue (lot drained + units flipped) → 0 alerts.
//!   R6 — after lot_transfer (units migrate to dest lot) → 0 alerts.

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
    tracked_by: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, $2::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
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
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (
            kind, ledger_kind, currency, sku_id, location_id, normal_side
         ) VALUES ($1::account_kind, $2::ledger_kind, $3,
                   $4::UUID, $5::UUID, $6::balance_direction)
         RETURNING id",
    )
    .bind(kind).bind(ledger_kind).bind(currency)
    .bind(sku_id).bind(location_id).bind(normal_side)
    .fetch_one(pool).await.unwrap()
}

#[allow(dead_code)]
struct Sf {
    sku: String,
    loc: String,
}

async fn scaffold(pool: &PgPool, suffix: &str, tracked_by: &str) -> Sf {
    let sku = fresh_sku(pool, &format!("LU-{suffix}"), tracked_by).await;
    let loc = fresh_loc(pool, &format!("LU-{suffix}-MAIN")).await;
    open_account(pool, "stock_available", "qty", None, Some(&sku), Some(&loc), "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), "debit").await;
    Sf { sku, loc }
}

async fn seed_units(
    pool: &PgPool,
    sf: &Sf,
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
    .bind(&sf.sku).bind(&sf.loc).bind(qty)
    .bind(&posted_by).bind(&key).bind(meta)
    .fetch_one(pool).await.unwrap();
}

async fn seed_lot_only(
    pool: &PgPool,
    sf: &Sf,
    qty: i64,
    lot_code: &str,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let meta = json!({ "lot_code": lot_code });
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, 50, 'USD', 'raw',
            '2026-04-10'::DATE, $4::UUID, $5::UUID, NULL, $6::JSONB
         )::text",
    )
    .bind(&sf.sku).bind(&sf.loc).bind(qty)
    .bind(&posted_by).bind(&key).bind(meta)
    .fetch_one(pool).await.unwrap();
}

/// Count alerts of a given kind raised in this recon run.
async fn count_alerts_of_kind(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_alerts WHERE alert_kind = $1",
    )
    .bind(kind)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn run_recon(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(pool).await.unwrap()
}

// ============================================================
// Tests
// ============================================================

#[tokio::test]
async fn r1_clean_lot_and_serial_no_unit_count_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "R1", "lot_and_serial").await;
    seed_units(&pool, &sf, 3, "R1-LOT-A", &["R1-U-001", "R1-U-002", "R1-U-003"]).await;

    let _n = run_recon(&pool).await;
    let mismatch = count_alerts_of_kind(&pool, "lot_unit_count_mismatch").await;
    assert_eq!(mismatch, 0, "clean lot_and_serial should not raise check #14");
}

#[tokio::test]
async fn r2_phantom_unit_fires_check_14() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "R2", "lot_and_serial").await;
    seed_units(&pool, &sf, 2, "R2-LOT-A", &["R2-U-001", "R2-U-002"]).await;

    // Synthesize: emit an extra inventory_lot_events drain WITHOUT
    // flipping a unit's status. Lot residual will go to 0 while
    // 2 units remain at status='available' → mismatch.
    let lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID ORDER BY lot_id LIMIT 1",
    )
    .bind(&sf.sku).fetch_one(&pool).await.unwrap();
    let pl_id: i64 = sqlx::query_scalar(
        "SELECT id FROM posting_lines
          WHERE reason = 'inventory_adjustment' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type,
             quantity_change, posting_line_id, location_id_from, notes)
         SELECT $1, receipt_date, '2026-04-12', 8, -2, $2, $3::UUID, 'r2_phantom'
           FROM inventory_lots WHERE lot_id = $1",
    )
    .bind(lot_id).bind(pl_id).bind(&sf.loc)
    .execute(&pool).await.unwrap();

    let _n = run_recon(&pool).await;
    let mismatch = count_alerts_of_kind(&pool, "lot_unit_count_mismatch").await;
    assert!(mismatch >= 1, "phantom drain should raise check #14");
}

#[tokio::test]
async fn r3_phantom_residual_fires_check_14() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "R3", "lot_and_serial").await;
    seed_units(&pool, &sf, 2, "R3-LOT-A", &["R3-U-001", "R3-U-002"]).await;

    // Synthesize: flip a unit to 'consumed' WITHOUT emitting the
    // lot drain event. Lot residual stays at 2, but only 1 active
    // unit remains → mismatch.
    sqlx::query(
        "UPDATE inventory_units SET status = 'consumed'
          WHERE product_id = $1::UUID AND serial_no = 'R3-U-002'",
    )
    .bind(&sf.sku).execute(&pool).await.unwrap();

    let _n = run_recon(&pool).await;
    let mismatch = count_alerts_of_kind(&pool, "lot_unit_count_mismatch").await;
    assert!(mismatch >= 1, "phantom unit-status flip should raise check #14");
}

#[tokio::test]
async fn r4_tracked_by_lot_only_does_not_fire_check_14() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "R4", "lot").await;
    // lot-only (no serial) — seed via post_inventory_adjustment without serials.
    seed_lot_only(&pool, &sf, 3, "R4-LOT-A").await;

    let _n = run_recon(&pool).await;
    let mismatch = count_alerts_of_kind(&pool, "lot_unit_count_mismatch").await;
    assert_eq!(mismatch, 0, "tracked_by='lot' is out of scope for check #14");
}

#[tokio::test]
async fn r5_post_rm_issue_recon_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Build a WO scaffold where the component is lot_fifo+lot_and_serial,
    // run wo_start → rm_issue consumes lot + units.
    let parent = fresh_sku(&pool, "R5-PAR", "none").await;
    let comp = fresh_sku(&pool, "R5-CMP", "lot_and_serial").await;
    sqlx::query(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'standard', 'none')
         ON CONFLICT DO NOTHING",
    )
    .bind("R5-IGNORE").execute(&pool).await.ok();
    // Re-fetch parent with cost_method='standard'.
    sqlx::query("UPDATE skus SET cost_method = 'standard' WHERE id = $1::UUID")
        .bind(&parent).execute(&pool).await.unwrap();
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, 100, '2026-01-01', $2::UUID, $3::UUID)",
    )
    .bind(&parent).bind(&posted_by).bind(&key).execute(&pool).await.unwrap();

    let raw_loc = fresh_loc(&pool, "R5-RAW").await;
    let fg_loc = fresh_loc(&pool, "R5-FG").await;

    open_account(&pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), "debit").await;
    open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), "debit").await;
    open_account(&pool, "stock_consumed", "qty", None, Some(&comp), None, "debit").await;

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
         VALUES ('stock_wip', 'qty', $1::UUID, 10, 'debit')",
    )
    .bind(&parent).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, routing_op, normal_side)
         VALUES ('inv_value_wip', 'value', 'USD', $1::UUID, 10, 'debit')",
    )
    .bind(&parent).execute(&pool).await.unwrap();
    open_account(&pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), "debit").await;
    open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), "debit").await;

    let bom_id: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(&parent).fetch_one(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', 100,
                 $2::UUID, $3::UUID, 1)",
    )
    .bind(bom_id).bind(&comp).bind(&raw_loc).execute(&pool).await.unwrap();

    // Seed 2 lot_and_serial component units.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 2, 50, 'USD', 'raw',
            '2026-04-10'::DATE, $3::UUID, $4::UUID, NULL,
            $5::JSONB
         )::text",
    )
    .bind(&comp).bind(&raw_loc).bind(&posted_by).bind(&key)
    .bind(json!({ "lot_code": "R5-LOT-A",
                  "unit_serials": ["R5-U-001","R5-U-002"] }))
    .fetch_one(&pool).await.unwrap();

    let posted_by = fresh_uuid(&pool).await;
    let wo_id = sqlx::query_scalar::<_, String>(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id,
                                  qty_target, currency, posted_by)
         VALUES ('R5-WO', $1::UUID, $2::UUID, 2, 'USD', $3::UUID)
         RETURNING id::text",
    )
    .bind(&parent).bind(&fg_loc).bind(&posted_by)
    .fetch_one(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO wo_routings (wo_id, routing_op, op_name)
         VALUES ($1::UUID, 10, 'ASM')",
    )
    .bind(&wo_id).execute(&pool).await.unwrap();

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL, NULL)::text",
    )
    .bind(&wo_id).bind(&posted_by).bind(&key)
    .fetch_one(&pool).await.unwrap();

    // After wo_start: lot residual = 0, units status='consumed'.
    let _n = run_recon(&pool).await;
    let mismatch = count_alerts_of_kind(&pool, "lot_unit_count_mismatch").await;
    assert_eq!(mismatch, 0, "post-rm_issue lot+units stay in sync");
}

#[tokio::test]
async fn r6_post_lot_transfer_recon_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = fresh_sku(&pool, "R6", "lot_and_serial").await;
    let loc_a = fresh_loc(&pool, "R6-A").await;
    let loc_b = fresh_loc(&pool, "R6-B").await;
    for loc in [&loc_a, &loc_b] {
        open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(loc), "debit").await;
        open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(loc), "debit").await;
    }

    // Seed 2 units at loc_a.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 2, 50, 'USD', 'raw',
            '2026-04-10'::DATE, $3::UUID, $4::UUID, NULL, $5::JSONB
         )::text",
    )
    .bind(&sku).bind(&loc_a).bind(&posted_by).bind(&key)
    .bind(json!({ "lot_code": "R6-LOT-A",
                  "unit_serials": ["R6-U-001","R6-U-002"] }))
    .fetch_one(&pool).await.unwrap();

    let unit_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sku).fetch_all(&pool).await.unwrap();

    // Transfer A→B.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{ "sku_id": sku, "qty": 2, "unit_ids": unit_ids }]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_lot_transfer($1::UUID, $2::UUID, $3::JSONB,
                                   '2026-04-15'::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&loc_a).bind(&loc_b).bind(&lines)
    .bind(&posted_by).bind(&key)
    .fetch_one(&pool).await.unwrap();

    // After transfer: source lot residual 0 + 0 units; dest lot residual 2 + 2 units.
    let _n = run_recon(&pool).await;
    let mismatch = count_alerts_of_kind(&pool, "lot_unit_count_mismatch").await;
    assert_eq!(mismatch, 0, "post-lot_transfer source+dest stay in sync");
}
