//! T1 probes for the lot subledger residual recon checks added in
//! mig 0051 (acct-ujqw, E2.4) — `lot_residual_mismatch` (check #9)
//! and `lot_negative_residual` (check #10) appended to
//! `run_daily_reconciliation`.
//!
//! Coverage:
//!   E2.4.1 clean lot_fifo full lifecycle → 0 lot alerts
//!   E2.4.2 phantom inventory_lots row → lot_residual_mismatch fires
//!   E2.4.3 phantom inventory_lot_events reducing residual →
//!          lot_residual_mismatch fires
//!   E2.4.4 phantom event taking a single lot residual negative →
//!          BOTH lot_negative_residual AND lot_residual_mismatch fire
//!   E2.4.5 lot_fifo SKU with no activity → 0 lot alerts
//!   E2.4.6 mixed FIFO + lot_fifo + standard SKUs → only lot_fifo probed
//!   E2.4.7 multi-lot partial depletion → 0 lot alerts
//!   E2.4.8 idempotent recon — running twice yields same alert count

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ----- helpers --------------------------------------------------

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

async fn fresh_fifo_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'fifo'::cost_method)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .expect("insert fifo SKU")
}

async fn fresh_standard_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard'::cost_method)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .expect("insert standard SKU")
}

async fn id_text(pool: &PgPool, q: &str, bind: &str) -> String {
    sqlx::query_scalar(q).bind(bind).fetch_one(pool).await.unwrap()
}

#[allow(dead_code)]
struct LotScaffold {
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
}

async fn scaffold_lot(pool: &PgPool, label: &str) -> LotScaffold {
    let sku_code = format!("SKU-LOT-{label}");
    let sku = fresh_lot_sku(pool, &sku_code).await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;

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

    LotScaffold { sku_id: sku, loc_id: loc, qty_acct, val_acct }
}

#[allow(clippy::too_many_arguments)]
async fn seed_lot(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
    lot_code: &str,
    idem_key: &str,
) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4, 'USD', 'raw',
            $5::DATE, $6::UUID, $7::UUID, NULL, $8
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idem_key)
    .bind(json!({ "lot_code": lot_code }))
    .fetch_one(pool)
    .await
    .expect("seed_lot succeeds")
}

#[allow(clippy::too_many_arguments)]
async fn deplete_lot(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    business_date: &str,
    lot_id: i64,
    idem_key: &str,
) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, NULL, 'USD', 'raw',
            $4::DATE, $5::UUID, $6::UUID, NULL, $7
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(-qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idem_key)
    .bind(json!({ "lot_id": lot_id }))
    .fetch_one(pool)
    .await
    .expect("deplete_lot succeeds")
}

async fn lot_alert_count(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts WHERE alert_kind = $1",
    )
    .bind(kind)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn run_recon(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(pool)
        .await
        .unwrap()
}

// ----------------------------------------------------------------
// E2.4.1 — clean full-lifecycle on lot_fifo SKU → 0 alerts.
// ----------------------------------------------------------------

#[tokio::test]
async fn clean_lifecycle_no_lot_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "R1").await;

    // Seed two lots, deplete some.
    let k1 = fresh_uuid(&pool).await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-R1A", &k1).await;
    let k2 = fresh_uuid(&pool).await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 8, 120, "2026-04-16", "LOT-R1B", &k2).await;

    let lot_a: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = 'LOT-R1A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let k3 = fresh_uuid(&pool).await;
    deplete_lot(&pool, &sf.sku_id, &sf.loc_id, 4, "2026-04-17", lot_a, &k3).await;

    // Pool: 10 + 8 - 4 = 14. Lot side: 10 + 8 - 4 = 14. Match.
    let bal: i64 = sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(sf.qty_acct)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bal, 14);

    run_recon(&pool).await;
    assert_eq!(lot_alert_count(&pool, "lot_residual_mismatch").await, 0);
    assert_eq!(lot_alert_count(&pool, "lot_negative_residual").await, 0);
}

// ----------------------------------------------------------------
// E2.4.2 — phantom inventory_lots row → lot_residual_mismatch.
//
// INSERT a second lot row reusing a real receipt_posting_line_id
// (FK satisfied) but without a corresponding stock_available
// debit. lot_total = real + phantom; on_hand = real only.
// ----------------------------------------------------------------

#[tokio::test]
async fn phantom_lot_row_fires_residual_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "R2").await;
    let k = fresh_uuid(&pool).await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 5, 100, "2026-04-15", "LOT-R2A", &k).await;

    // Find a posting_lines.id we can reuse for FK.
    let recv_pl: i64 = sqlx::query_scalar(
        "SELECT receipt_posting_line_id FROM inventory_lots WHERE lot_code = 'LOT-R2A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Phantom INSERT: same product/location, fresh lot_code, qty 7
    // (no stock_available debit to match it).
    sqlx::query(
        "INSERT INTO inventory_lots
            (product_id, legal_entity_id, cost_book_id, location_id, lot_code,
             receipt_posting_line_id, receipt_date, original_quantity, unit_cost,
             cost_currency)
         VALUES ($1::UUID, 1, 1, $2::UUID, 'LOT-PHANTOM',
                 $3, '2026-04-15'::DATE, 7, 100, 'USD')",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .bind(recv_pl)
    .execute(&pool)
    .await
    .expect("phantom INSERT");

    run_recon(&pool).await;
    assert_eq!(
        lot_alert_count(&pool, "lot_residual_mismatch").await,
        1,
        "expected one residual mismatch alert for phantom lot"
    );
    // Phantom's residual is +7 (no events), so negative-residual stays clean.
    assert_eq!(lot_alert_count(&pool, "lot_negative_residual").await, 0);
}

// ----------------------------------------------------------------
// E2.4.3 — phantom inventory_lot_events row reduces residual without
// touching on_hand → lot_residual_mismatch fires (residual still ≥ 0).
// ----------------------------------------------------------------

#[tokio::test]
async fn phantom_event_reduces_residual_fires_mismatch() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "R3").await;
    let k = fresh_uuid(&pool).await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-R3A", &k).await;

    let (lot_id, recv_date): (i64, String) = sqlx::query_as(
        "SELECT lot_id, receipt_date::TEXT FROM inventory_lots WHERE lot_code = 'LOT-R3A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Phantom adjust_out event reducing residual by 3 (no posting_line).
    // residual_per_lot = 10 + (-3) = 7 ≥ 0 (so #10 stays clean).
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, $2::DATE, '2026-04-16'::DATE, 8, -3)",
    )
    .bind(lot_id)
    .bind(&recv_date)
    .execute(&pool)
    .await
    .expect("phantom event INSERT");

    run_recon(&pool).await;
    assert_eq!(
        lot_alert_count(&pool, "lot_residual_mismatch").await,
        1,
        "expected residual mismatch from phantom event"
    );
    assert_eq!(lot_alert_count(&pool, "lot_negative_residual").await, 0);
}

// ----------------------------------------------------------------
// E2.4.4 — phantom event taking a lot residual negative fires both
// lot_negative_residual AND lot_residual_mismatch.
// ----------------------------------------------------------------

#[tokio::test]
async fn phantom_event_negative_residual_fires_both() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "R4").await;
    let k = fresh_uuid(&pool).await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 5, 80, "2026-04-15", "LOT-R4A", &k).await;

    let (lot_id, recv_date): (i64, String) = sqlx::query_as(
        "SELECT lot_id, receipt_date::TEXT FROM inventory_lots WHERE lot_code = 'LOT-R4A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Phantom event of -10: residual = 5 + (-10) = -5.
    sqlx::query(
        "INSERT INTO inventory_lot_events
            (lot_id, lot_receipt_date, event_date, event_type, quantity_change)
         VALUES ($1, $2::DATE, '2026-04-16'::DATE, 8, -10)",
    )
    .bind(lot_id)
    .bind(&recv_date)
    .execute(&pool)
    .await
    .expect("phantom negative-residual event");

    run_recon(&pool).await;
    assert_eq!(
        lot_alert_count(&pool, "lot_negative_residual").await,
        1,
        "expected negative-residual alert"
    );
    assert_eq!(
        lot_alert_count(&pool, "lot_residual_mismatch").await,
        1,
        "expected residual mismatch alongside negative-residual"
    );
}

// ----------------------------------------------------------------
// E2.4.5 — lot_fifo SKU with no activity (no lots, no pool) → 0 alerts.
// ----------------------------------------------------------------

#[tokio::test]
async fn no_activity_no_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Just create the SKU and accounts — no postings at all.
    let _sf = scaffold_lot(&pool, "R5").await;

    run_recon(&pool).await;
    assert_eq!(lot_alert_count(&pool, "lot_residual_mismatch").await, 0);
    assert_eq!(lot_alert_count(&pool, "lot_negative_residual").await, 0);
}

// ----------------------------------------------------------------
// E2.4.6 — mixed FIFO + lot_fifo + standard SKUs coexist; only
// lot_fifo SKUs are probed by checks #9/#10. Inject drift on a
// FIFO SKU's cost_layer → only the FIFO check (#8) fires, not lot.
// ----------------------------------------------------------------

#[tokio::test]
async fn mixed_methods_only_lot_probed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Lot SKU — clean lifecycle.
    let lot_sf = scaffold_lot(&pool, "R6L").await;
    let k1 = fresh_uuid(&pool).await;
    seed_lot(&pool, &lot_sf.sku_id, &lot_sf.loc_id, 5, 100, "2026-04-15", "LOT-R6A", &k1).await;

    // FIFO SKU — clean stock_available + cost_layer would match. We
    // inject a phantom cost_layer to force fifo_layer_residual_mismatch
    // (proves the lot check ignores it).
    let fifo_sku = fresh_fifo_sku(&pool, "SKU-FIFO-R6").await;
    let loc = id_text(&pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let _fifo_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available', 'qty', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&fifo_sku)
    .bind(&loc)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Use a real posting_line as FK source — reuse the lot seed's posting_line_id
    // since cost_layers.receipt_posting_line_id has the same FK target.
    let any_pl: i64 = sqlx::query_scalar("SELECT id FROM posting_lines ORDER BY id DESC LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO cost_layers
            (product_id, legal_entity_id, cost_book_id, location_id,
             receipt_posting_line_id, receipt_date,
             original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, 1, 1, $2::UUID, $3, '2026-04-15'::DATE,
                 4, 100, 'USD')",
    )
    .bind(&fifo_sku)
    .bind(&loc)
    .bind(any_pl)
    .execute(&pool)
    .await
    .expect("phantom fifo layer");

    // Standard SKU — also exists; should never appear in lot probes.
    let _std_sku = fresh_standard_sku(&pool, "SKU-STD-R6").await;

    run_recon(&pool).await;
    // Lot side: clean.
    assert_eq!(lot_alert_count(&pool, "lot_residual_mismatch").await, 0);
    assert_eq!(lot_alert_count(&pool, "lot_negative_residual").await, 0);
    // FIFO side: phantom layer triggers fifo_layer_residual_mismatch.
    assert_eq!(lot_alert_count(&pool, "fifo_layer_residual_mismatch").await, 1);
}

// ----------------------------------------------------------------
// E2.4.7 — multi-lot with partial depletions across several lots →
// 0 alerts (subledger and pool stay aligned).
// ----------------------------------------------------------------

#[tokio::test]
async fn multi_lot_partial_depletion_no_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "R7").await;

    // Three lots staggered across April.
    for (i, (qty, uc, day, code)) in [
        (10, 100, "2026-04-15", "LOT-R7A"),
        (15, 110, "2026-04-16", "LOT-R7B"),
        (8, 130, "2026-04-17", "LOT-R7C"),
    ]
    .iter()
    .enumerate()
    {
        let k = fresh_uuid(&pool).await;
        let _ = i;
        seed_lot(&pool, &sf.sku_id, &sf.loc_id, *qty, *uc, day, code, &k).await;
    }

    // Deplete partial quantities from two of them.
    let lot_a: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = 'LOT-R7A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let lot_b: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = 'LOT-R7B'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let k = fresh_uuid(&pool).await;
    deplete_lot(&pool, &sf.sku_id, &sf.loc_id, 3, "2026-04-18", lot_a, &k).await;
    let k = fresh_uuid(&pool).await;
    deplete_lot(&pool, &sf.sku_id, &sf.loc_id, 5, "2026-04-19", lot_b, &k).await;

    // Pool: 10+15+8 - 3-5 = 25. Lot side: same.
    let bal: i64 = sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(sf.qty_acct)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bal, 25);

    run_recon(&pool).await;
    assert_eq!(lot_alert_count(&pool, "lot_residual_mismatch").await, 0);
    assert_eq!(lot_alert_count(&pool, "lot_negative_residual").await, 0);
}

// ----------------------------------------------------------------
// E2.4.8 — running recon twice doubles the alert count when drift
// persists (recon is INSERT-only, not idempotent on alerts table).
// Verifies each invocation fires the same #checks against the same
// drift, so callers run it once per business_date.
// ----------------------------------------------------------------

#[tokio::test]
async fn recon_alert_inserts_per_invocation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "R8").await;
    let k = fresh_uuid(&pool).await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 6, 90, "2026-04-15", "LOT-R8A", &k).await;

    let recv_pl: i64 = sqlx::query_scalar(
        "SELECT receipt_posting_line_id FROM inventory_lots WHERE lot_code = 'LOT-R8A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // One phantom lot → one drift cell.
    sqlx::query(
        "INSERT INTO inventory_lots
            (product_id, legal_entity_id, cost_book_id, location_id, lot_code,
             receipt_posting_line_id, receipt_date, original_quantity, unit_cost,
             cost_currency)
         VALUES ($1::UUID, 1, 1, $2::UUID, 'LOT-R8-PH',
                 $3, '2026-04-15'::DATE, 4, 90, 'USD')",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .bind(recv_pl)
    .execute(&pool)
    .await
    .expect("phantom INSERT");

    run_recon(&pool).await;
    let after_first = lot_alert_count(&pool, "lot_residual_mismatch").await;

    run_recon(&pool).await;
    let after_second = lot_alert_count(&pool, "lot_residual_mismatch").await;

    assert_eq!(after_first, 1);
    assert_eq!(after_second, 2, "second invocation re-inserts the same alert");
}
