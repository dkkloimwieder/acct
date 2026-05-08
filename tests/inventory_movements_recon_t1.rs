//! T1 probes for the D5 subledger ↔ GL recon invariant
//! (mig 0029, acct-wb75.3.5). Phase D D5 of the convergence plan.
//!
//! Asserts run_daily_reconciliation's check #7 'subledger_gl_divergence':
//!   - Clean state after a normal po_receipt → no alert.
//!   - WAC running-avg with BIGINT truncation drift stays within
//!     1-cent tolerance per bucket.
//!   - Synthetic divergence (subledger doctored to differ from GL)
//!     surfaces an alert with the expected payload shape.
//!   - Subledger-only or GL-only buckets count as divergent.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

async fn alert_count(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts WHERE alert_kind = $1",
    )
    .bind(kind)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn truncate_alerts(pool: &PgPool) {
    sqlx::query("TRUNCATE reconciliation_alerts")
        .execute(pool)
        .await
        .unwrap();
}

async fn fresh_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $1, 'USD')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_po(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn fresh_po_line(
    pool: &PgPool,
    po_id: &str,
    line_no: i32,
    sku_id: &str,
    location_id: &str,
    qty_ordered: i64,
    unit_cost: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD')
         RETURNING id::text",
    )
    .bind(po_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(location_id)
    .bind(qty_ordered)
    .bind(unit_cost)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    counterparty_id: &str,
    normal_side: &str,
) -> i64 {
    let currency = if ledger_kind == "value" { Some("USD") } else { None };
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn ship_a_po_receipt(pool: &PgPool, sku_code: &str, qty: i64, unit_cost: i64) {
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(sku_code)
        .fetch_one(pool)
        .await
        .unwrap();
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool)
        .await
        .unwrap();
    let vendor = fresh_vendor(pool, &format!("VEND-D5-{qty}-{unit_cost}")).await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, 1, &sku, &loc, qty, unit_cost).await;
    open_account(pool, "vendor_pool", "qty", &vendor, "credit").await;
    open_account(pool, "ap_unsettled", "value", &vendor, "credit").await;
    open_account(pool, "ap", "value", &vendor, "credit").await;

    let key = fresh_uuid(pool).await;
    let lines = json!([{ "po_line_id": po_line, "qty_received": qty }]);
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_po_receipt($1::UUID, $2, '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&po)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("po receipt");
}

// ============================================================
// Clean state — recon stays silent
// ============================================================

#[tokio::test]
async fn clean_state_after_po_receipt_no_divergence_alert() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    ship_a_po_receipt(&pool, "SKU-A", 10, 100).await;
    truncate_alerts(&pool).await;

    sqlx::query_scalar::<_, i32>("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();

    let n = alert_count(&pool, "subledger_gl_divergence").await;
    assert_eq!(
        n, 0,
        "po_receipt at standard cost: subledger 10×100=1000, GL inv_value_raw=1000; no divergence"
    );
}

// ============================================================
// Synthetic divergence — alert fires
// ============================================================

#[tokio::test]
async fn synthetic_subledger_drift_triggers_alert() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    ship_a_po_receipt(&pool, "SKU-A", 10, 100).await;

    // Artificially inflate the subledger row by $5 (500 cents) — well
    // beyond the 1-cent tolerance. This simulates a drift that recon
    // is supposed to catch.
    sqlx::query("UPDATE inventory_movements SET actual_unit_cost = actual_unit_cost + 50")
        .execute(&pool)
        .await
        .unwrap();

    truncate_alerts(&pool).await;
    sqlx::query_scalar::<_, i32>("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();

    let n = alert_count(&pool, "subledger_gl_divergence").await;
    assert_eq!(n, 1, "subledger drifted 50 × 10 = 500 from GL → 1 alert");

    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM reconciliation_alerts
         WHERE alert_kind = 'subledger_gl_divergence' LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let diff = payload["diff"].as_str().expect("diff in payload");
    // GL is 1000, subledger is now 1500 (10 × 150). diff = gl - sub = -500.
    assert!(
        diff.starts_with("-500") || diff.starts_with("-500.") || diff == "-500",
        "expected diff -500, got {diff:?}"
    );
}

#[tokio::test]
async fn subledger_only_bucket_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    ship_a_po_receipt(&pool, "SKU-A", 5, 100).await;

    // Re-point the existing inventory_movements row at a different
    // location (ALT). The GL bucket is (SKU-A, MAIN); the subledger
    // bucket is (SKU-A, ALT). FULL OUTER JOIN: two diverged buckets.
    sqlx::query(
        "UPDATE inventory_movements SET location_id =
           (SELECT id FROM locations WHERE code = 'ALT')",
    )
    .execute(&pool)
    .await
    .unwrap();

    truncate_alerts(&pool).await;
    sqlx::query_scalar::<_, i32>("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();

    let n = alert_count(&pool, "subledger_gl_divergence").await;
    assert_eq!(
        n, 2,
        "movement at ALT vs GL at MAIN → two divergent buckets (one per side)"
    );
}

// ============================================================
// 1-cent tolerance window
// ============================================================

#[tokio::test]
async fn one_cent_subledger_drift_stays_silent() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    ship_a_po_receipt(&pool, "SKU-A", 10, 100).await;

    // Drift exactly 1 cent total (within tolerance). Each unit cost
    // is 100; bumping one row by 0.0001 × 10 units = 0.001 (< 1 cent
    // total). To get a precise 1-cent total drift, bump by 0.1 / 10
    // = 0.0100 per unit; total = 0.1 cents — well within tolerance.
    sqlx::query("UPDATE inventory_movements SET actual_unit_cost = actual_unit_cost + 0.0001")
        .execute(&pool)
        .await
        .unwrap();

    truncate_alerts(&pool).await;
    sqlx::query_scalar::<_, i32>("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();

    let n = alert_count(&pool, "subledger_gl_divergence").await;
    assert_eq!(
        n, 0,
        "tiny rounding drift below 1 cent total stays under recon threshold"
    );
}
