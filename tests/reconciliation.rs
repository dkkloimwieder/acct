//! O2 — Daily reconciliation function (`run_daily_reconciliation`).
//!
//! Three cases:
//!   - Clean fixture → 0 alerts.
//!   - Per-ledger double-entry imbalance (induced by direct UPDATE
//!     bypassing post_transfers) → 1 `double_entry_imbalance` alert
//!     with the expected payload.
//!   - Reservation over-promise (induced by direct INSERT bypassing
//!     reserve_inventory) → 1 `reservation_over_promise` alert.

mod common;

use common::*;

#[tokio::test]
async fn clean_fixture_produces_no_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let inserted: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .expect("run_daily_reconciliation");
    assert_eq!(inserted, 0, "clean fixture should produce 0 alerts");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_alerts")
        .fetch_one(&pool)
        .await
        .expect("count alerts");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn double_entry_imbalance_produces_alert() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Inject an imbalance into the value/USD ledger by directly
    // updating cash USD's debits_total. This bypasses post_transfers
    // — exactly the kind of drift the recon job exists to detect.
    let cash = account_id_by_kind_currency(&pool, "cash", Some("USD")).await;
    sqlx::query("UPDATE accounts SET debits_total = debits_total + 100 WHERE id = $1")
        .bind(cash)
        .execute(&pool)
        .await
        .expect("inject imbalance");

    let inserted: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .expect("run_daily_reconciliation");
    assert_eq!(inserted, 1);

    let row: (String, serde_json::Value) = sqlx::query_as(
        "SELECT alert_type, payload FROM reconciliation_alerts ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("fetch alert");
    assert_eq!(row.0, "double_entry_imbalance");
    let p = &row.1;
    assert_eq!(p["ledger_kind"], "value");
    assert_eq!(p["currency"], "USD");
    assert_eq!(p["imbalance"], 100);
}

#[tokio::test]
async fn reservation_over_promise_produces_alert() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Insert an active reservation against (SKU-A, MAIN) with on-hand=0.
    // Direct INSERT bypasses reserve_inventory's qty_promisable gate
    // — same kind of drift the recon detects.
    let so_id = fresh_sales_order(&pool).await;
    sqlx::query(
        "INSERT INTO inventory_reservations
           (sku_id, location_id, qty, so_id, so_line_id, expires_at, status)
         SELECT (SELECT id FROM skus      WHERE code = 'SKU-A'),
                (SELECT id FROM locations WHERE code = 'MAIN'),
                3, $1::UUID, gen_random_uuid(),
                clock_timestamp() + INTERVAL '1 hour',
                'active'",
    )
    .bind(&so_id)
    .execute(&pool)
    .await
    .expect("inject over-promise");

    let inserted: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .expect("run_daily_reconciliation");
    assert_eq!(inserted, 1);

    let row: (String, serde_json::Value) = sqlx::query_as(
        "SELECT alert_type, payload FROM reconciliation_alerts ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("fetch alert");
    assert_eq!(row.0, "reservation_over_promise");
    let p = &row.1;
    assert_eq!(p["on_hand"], 0);
    assert_eq!(p["reserved"], 3);
    assert_eq!(p["deficit"], -3);
}
