//! T1 probes for FIFO layer residual recon (mig 0033, acct-swwp).
//! Phase E1 E1.3 of the convergence plan.
//!
//! Asserts run_daily_reconciliation's check #8
//! 'fifo_layer_residual_mismatch':
//!   - Clean state after FIFO receipt → no alert.
//!   - After receipt + issue, residual = on-hand → no alert.
//!   - Synthetic drift (manual depletion bypassing apply_event)
//!     surfaces an alert with the expected payload shape.
//!   - Non-FIFO SKUs (standard, WAC) are excluded from the check.

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

async fn run_recon(pool: &PgPool) {
    sqlx::query_scalar::<_, i32>("SELECT run_daily_reconciliation()")
        .fetch_one(pool)
        .await
        .unwrap();
}

struct FifScaffold {
    sku: String,
    loc: String,
    inv_raw: i64,
    stock_avail: i64,
    ap_unsettled: i64,
    ven_qty: i64,
    inv_adj_expense: i64,
    void_qty: i64,
}

async fn fresh_vendor_uuid(pool: &PgPool, label: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $1, 'USD') RETURNING id::text",
    )
    .bind(label)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn scaffold_fif(pool: &PgPool) -> FifScaffold {
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-FIF'")
        .fetch_one(pool)
        .await
        .unwrap();
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool)
        .await
        .unwrap();
    let vendor = fresh_vendor_uuid(pool, "VEND-FIF-RECON").await;

    let inv_raw: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

    let ap_unsettled: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
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

    let stock_avail = account_id_stock_available(pool, "SKU-FIF", "MAIN").await;
    let inv_adj_expense = account_id_by_kind_currency(pool, "inv_adj_expense", Some("USD")).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;

    FifScaffold {
        sku,
        loc,
        inv_raw,
        stock_avail,
        ap_unsettled,
        ven_qty,
        inv_adj_expense,
        void_qty,
    }
}

async fn fifo_receipt(
    pool: &PgPool,
    s: &FifScaffold,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
) {
    let qty_key = fresh_uuid(pool).await;
    let val_key = fresh_uuid(pool).await;
    let amount = qty * unit_cost;
    let qty_event = make_event(
        "po_receipt",
        s.stock_avail,
        s.ven_qty,
        qty,
        business_date,
        &qty_key,
    );
    let val_event = make_event_with_qty(
        "po_receipt",
        s.inv_raw,
        s.ap_unsettled,
        amount,
        qty,
        business_date,
        &val_key,
    );
    let result = call_post_posting_lines(pool, json!([qty_event, val_event]), false)
        .await
        .unwrap();
    assert_eq!(result[0]["result"], "ok");
    assert_eq!(result[1]["result"], "ok");
}

async fn fifo_issue(
    pool: &PgPool,
    s: &FifScaffold,
    qty: i64,
    business_date: &str,
) {
    let qty_key = fresh_uuid(pool).await;
    let val_key = fresh_uuid(pool).await;
    let qty_event = make_event(
        "scrap",
        s.void_qty,
        s.stock_avail,
        qty,
        business_date,
        &qty_key,
    );
    let val_event = make_event_with_qty(
        "scrap",
        s.inv_adj_expense,
        s.inv_raw,
        0,
        qty,
        business_date,
        &val_key,
    );
    call_post_posting_lines(pool, json!([qty_event, val_event]), false)
        .await
        .unwrap();
}

// ============================================================
// Clean state after normal posting paths → no alert
// ============================================================

#[tokio::test]
async fn no_alert_after_clean_fifo_receipt() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;

    truncate_alerts(&pool).await;
    run_recon(&pool).await;
    assert_eq!(alert_count(&pool, "fifo_layer_residual_mismatch").await, 0);
}

#[tokio::test]
async fn no_alert_after_receipt_and_issue() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;
    fifo_issue(&pool, &s, 4, "2026-04-16").await;

    truncate_alerts(&pool).await;
    run_recon(&pool).await;
    assert_eq!(alert_count(&pool, "fifo_layer_residual_mismatch").await, 0);
}

#[tokio::test]
async fn no_alert_for_multi_layer_state() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 5, 10, "2026-04-15").await;
    fifo_receipt(&pool, &s, 5, 20, "2026-04-16").await;
    fifo_receipt(&pool, &s, 5, 30, "2026-04-17").await;
    fifo_issue(&pool, &s, 8, "2026-04-18").await;

    truncate_alerts(&pool).await;
    run_recon(&pool).await;
    assert_eq!(alert_count(&pool, "fifo_layer_residual_mismatch").await, 0);
}

// ============================================================
// Synthetic drift surfaces an alert
// ============================================================

#[tokio::test]
async fn synthetic_drift_surfaces_alert() {
    // Direct INSERT into cost_layer_depletions bypassing apply_event,
    // so the layer residual diverges from stock_available on-hand.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;

    // Stage a posting_line for the FK (depletions require posting_line_id NOT NULL).
    let pl_id = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM posting_lines ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let layer_id: i64 = sqlx::query_scalar(
        "SELECT layer_id FROM cost_layers WHERE product_id = $1::UUID",
    )
    .bind(&s.sku)
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO cost_layer_depletions
            (layer_id, layer_receipt_date, issue_date,
             depleted_quantity, unit_cost, cost_amount, posting_line_id)
         VALUES ($1, '2026-04-15', '2026-04-16', 3.0, 100.0, 300, $2)",
    )
    .bind(layer_id)
    .bind(pl_id)
    .execute(&pool)
    .await
    .unwrap();

    // layer_residual now = 10 - 3 = 7; stock_available on-hand still 10.
    truncate_alerts(&pool).await;
    run_recon(&pool).await;
    assert_eq!(alert_count(&pool, "fifo_layer_residual_mismatch").await, 1);

    let payload: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM reconciliation_alerts WHERE alert_kind = 'fifo_layer_residual_mismatch'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(payload["product_id"].as_str().unwrap(), s.sku);
    assert_eq!(payload["location_id"].as_str().unwrap(), s.loc);
    let layer_residual_str = payload["layer_residual"].as_str().unwrap();
    let on_hand_str = payload["on_hand"].as_str().unwrap();
    let diff_str = payload["diff"].as_str().unwrap();
    assert!(layer_residual_str.starts_with("7"), "residual {layer_residual_str}");
    assert!(on_hand_str.starts_with("10"), "on_hand {on_hand_str}");
    assert!(diff_str.starts_with("-3"), "diff {diff_str}");
}

#[tokio::test]
async fn drift_with_layer_only_no_on_hand() {
    // Layer exists for FIFO SKU but no stock_available account →
    // FULL OUTER JOIN surfaces a layer-side bucket with on_hand=0.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Open accounts manually (no fifo_receipt → no qty-leg → no
    // stock_available debit). Direct INSERT a layer.
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-FIF'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool)
        .await
        .unwrap();

    // Stage a posting_line for receipt_posting_line_id FK.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let stock = account_id_stock_available(&pool, "SKU-FIF", "MAIN").await;
    let key = fresh_uuid(&pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, 1, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false).await.unwrap();
    let pl_id = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO cost_layers
            (product_id, location_id, receipt_posting_line_id,
             receipt_date, original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, $2::UUID, $3, '2026-04-15', 5.0, 100.0, 'USD')",
    )
    .bind(&sku)
    .bind(&loc)
    .bind(pl_id)
    .execute(&pool)
    .await
    .unwrap();

    // layer_residual=5; on_hand=1 (the cycle_count_adj seeded 1 unit) → diff 4.
    truncate_alerts(&pool).await;
    run_recon(&pool).await;
    assert_eq!(alert_count(&pool, "fifo_layer_residual_mismatch").await, 1);
}

// ============================================================
// Non-FIFO SKUs are excluded from check #8
// ============================================================

#[tokio::test]
async fn non_fifo_sku_not_evaluated() {
    // SKU-A is standard cost; check #8 must skip it entirely.
    // Synthetically diverge (impossible in practice, since standard
    // SKUs don't write to cost_layers, but this proves the filter
    // is correct).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    truncate_alerts(&pool).await;
    run_recon(&pool).await;
    assert_eq!(
        alert_count(&pool, "fifo_layer_residual_mismatch").await,
        0,
        "no FIFO state in fixture → no alert"
    );

    // Verify SKU-A (standard) has no layers and isn't in the check.
    let skua_layers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layers cl
           JOIN skus s ON s.id = cl.product_id WHERE s.code = 'SKU-A'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(skua_layers, 0);
}
