//! T1 probes for reserve_inventory lot pin extension (mig 0052,
//! acct-knuu, E2.5). Two new trailing parameters:
//!   p_lot_id        BIGINT  DEFAULT NULL
//!   p_lot_specific  BOOLEAN DEFAULT FALSE
//!
//! Coverage:
//!   E2.5.1 backward-compat — 7-arg call still works
//!   E2.5.2 non-pinned reservation on lot SKU works
//!   E2.5.3 pinned reservation on a specific lot succeeds
//!   E2.5.4 pinned reservation respects lot_residual cap
//!   E2.5.5 pinned reservation conservative against non-pinned
//!   E2.5.6 lot_specific=TRUE without lot_id raises P0052
//!   E2.5.7 pinned reservation with cross-SKU lot_id raises P0010
//!   E2.5.8 two pinned to different lots both succeed
//!   E2.5.9 reservations are observable via inventory_reservations

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ---------- helpers ----------

async fn fresh_lot_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, 'lot'::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn create_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn id_text(pool: &PgPool, q: &str, bind: &str) -> String {
    sqlx::query_scalar(q).bind(bind).fetch_one(pool).await.unwrap()
}

#[allow(dead_code)]
struct Scaffold {
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    customer_id: String,
    so_id: String,
}

async fn scaffold(pool: &PgPool, label: &str) -> Scaffold {
    let sku = fresh_lot_sku(pool, &format!("SKU-LOT-RSV-{label}")).await;
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

    let cust = fresh_customer(pool, &format!("RSV-CUST-{label}")).await;
    let so = create_so(pool, &cust).await;

    Scaffold { sku_id: sku, loc_id: loc, qty_acct, val_acct, customer_id: cust, so_id: so }
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
) -> i64 {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
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
    .bind(&key)
    .bind(json!({ "lot_code": lot_code }))
    .fetch_one(pool)
    .await
    .unwrap();

    sqlx::query_scalar::<_, i64>(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = $1",
    )
    .bind(lot_code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_so_line_id(pool: &PgPool) -> String {
    fresh_uuid(pool).await
}

async fn call_reserve_pinned(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    so_id: &str,
    lot_id: i64,
) -> sqlx::Result<Option<String>> {
    let line = fresh_so_line_id(pool).await;
    sqlx::query_scalar(
        "SELECT reserve_inventory(
            $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
            '2099-01-01'::TIMESTAMPTZ, NULL, $6::BIGINT, TRUE
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty)
    .bind(so_id)
    .bind(&line)
    .bind(lot_id)
    .fetch_one(pool)
    .await
}

async fn call_reserve_unpinned(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    so_id: &str,
) -> sqlx::Result<Option<String>> {
    let line = fresh_so_line_id(pool).await;
    sqlx::query_scalar(
        "SELECT reserve_inventory(
            $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
            '2099-01-01'::TIMESTAMPTZ, NULL
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty)
    .bind(so_id)
    .bind(&line)
    .fetch_one(pool)
    .await
}

// ----------------------------------------------------------------
// E2.5.1 — backward-compat: 7-arg call still works on lot SKU.
// ----------------------------------------------------------------

#[tokio::test]
async fn backward_compat_seven_arg_call_works() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "B").await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-B").await;

    let id = call_reserve_unpinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id)
        .await
        .expect("query")
        .expect("reservation succeeded");

    let (lot_id, lot_specific): (Option<i64>, bool) = sqlx::query_as(
        "SELECT lot_id, lot_specific FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_id, None, "lot_id should default NULL on 7-arg call");
    assert!(!lot_specific, "lot_specific should default FALSE");
}

// ----------------------------------------------------------------
// E2.5.2 — non-pinned (lot_specific=FALSE) reserves any-lot qty.
// ----------------------------------------------------------------

#[tokio::test]
async fn unpinned_reservation_uses_total_on_hand() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "U").await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-U-A").await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 8, 110, "2026-04-16", "LOT-U-B").await;

    // Total on_hand = 18; reserve 15 (any lot).
    let id = call_reserve_unpinned(&pool, &sf.sku_id, &sf.loc_id, 15, &sf.so_id)
        .await
        .expect("query")
        .expect("15-unit non-pinned reservation succeeded");

    let qty: i64 = sqlx::query_scalar(
        "SELECT qty FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(qty, 15);
}

// ----------------------------------------------------------------
// E2.5.3 — pinned to a specific lot succeeds when lot has qty.
// ----------------------------------------------------------------

#[tokio::test]
async fn pinned_reservation_on_lot_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "P").await;
    let lot_a = seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-P-A").await;

    let id = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 7, &sf.so_id, lot_a)
        .await
        .expect("query")
        .expect("pinned reservation succeeded");

    let (lot_id, lot_specific): (Option<i64>, bool) = sqlx::query_as(
        "SELECT lot_id, lot_specific FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_id, Some(lot_a));
    assert!(lot_specific);
}

// ----------------------------------------------------------------
// E2.5.4 — pinned reservation respects lot residual cap.
// Lot has 10. Try to pin 12 → returns NULL.
// ----------------------------------------------------------------

#[tokio::test]
async fn pinned_reservation_capped_by_lot_residual() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "C").await;
    let lot_a = seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-C-A").await;
    // Seed a second lot to ensure SKU on_hand is high (18) — pinned should
    // still be capped by lot A's residual (10), not the SKU total.
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 8, 100, "2026-04-16", "LOT-C-B").await;

    let result = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 12, &sf.so_id, lot_a)
        .await
        .expect("query");
    assert!(result.is_none(), "pinned 12-unit on 10-unit lot should return NULL");

    // Reserve exactly 10 → succeeds.
    let ok = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 10, &sf.so_id, lot_a)
        .await
        .expect("query")
        .expect("10-unit pin should succeed");
    let qty: i64 = sqlx::query_scalar(
        "SELECT qty FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&ok)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(qty, 10);
}

// ----------------------------------------------------------------
// E2.5.5 — pinned reservation is conservative against non-pinned.
// Lot A=10. Non-pinned reserved 5 (could draw from A or B). Lot B=8.
// Try to pin 7 to A → conservative: A_residual(10) - non_pinned(5) = 5 ≤ 7 → NULL.
// ----------------------------------------------------------------

#[tokio::test]
async fn pinned_promisable_subtracts_nonpinned() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "X").await;
    let lot_a = seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-X-A").await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 8, 100, "2026-04-16", "LOT-X-B").await;

    // Non-pinned reservation of 5.
    call_reserve_unpinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id)
        .await
        .expect("query")
        .expect("non-pinned reservation");

    // Pinned 7 to A — conservative says 10 - 5 = 5, so 7 fails.
    let result = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 7, &sf.so_id, lot_a)
        .await
        .expect("query");
    assert!(result.is_none(),
            "pinned 7 should fail with non-pinned 5 outstanding (conservative)");

    // Pinned 5 succeeds.
    let ok = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_a)
        .await
        .expect("query")
        .expect("pinned 5 should succeed");
    assert!(!ok.is_empty());
}

// ----------------------------------------------------------------
// E2.5.6 — lot_specific=TRUE with NULL lot_id → P0052.
// ----------------------------------------------------------------

#[tokio::test]
async fn lot_specific_without_lot_id_raises_p0052() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "V").await;
    seed_lot(&pool, &sf.sku_id, &sf.loc_id, 5, 50, "2026-04-15", "LOT-V").await;

    let line = fresh_uuid(&pool).await;
    let err = sqlx::query_scalar::<_, String>(
        "SELECT reserve_inventory(
            $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
            '2099-01-01'::TIMESTAMPTZ, NULL, NULL, TRUE
         )::text",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .bind(2_i64)
    .bind(&sf.so_id)
    .bind(&line)
    .fetch_one(&pool)
    .await
    .err()
    .expect("expected P0052");

    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0052"));
}

// ----------------------------------------------------------------
// E2.5.7 — pinning a lot that doesn't belong to (sku, location)
// raises P0010.
// ----------------------------------------------------------------

#[tokio::test]
async fn cross_sku_lot_pin_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf_a = scaffold(&pool, "A1").await;
    let sf_b = scaffold(&pool, "B1").await;

    // Seed a lot under SKU-B; try to pin under SKU-A.
    let lot_b = seed_lot(&pool, &sf_b.sku_id, &sf_b.loc_id, 5, 50, "2026-04-15", "LOT-A1-CROSS").await;

    let err = call_reserve_pinned(&pool, &sf_a.sku_id, &sf_a.loc_id, 1, &sf_a.so_id, lot_b)
        .await
        .err()
        .expect("expected P0010");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0010"));
}

// ----------------------------------------------------------------
// E2.5.8 — two pinned reservations to different lots both succeed.
// ----------------------------------------------------------------

#[tokio::test]
async fn two_pins_to_different_lots_succeed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T").await;
    let lot_a = seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-T-A").await;
    let lot_b = seed_lot(&pool, &sf.sku_id, &sf.loc_id, 8, 100, "2026-04-16", "LOT-T-B").await;

    let r_a = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 6, &sf.so_id, lot_a)
        .await
        .expect("query")
        .expect("pin to lot A succeeded");
    let r_b = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_b)
        .await
        .expect("query")
        .expect("pin to lot B succeeded");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_reservations
         WHERE id = $1::UUID OR id = $2::UUID",
    )
    .bind(&r_a)
    .bind(&r_b)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
}

// ----------------------------------------------------------------
// E2.5.9 — reservations are persisted with the lot pin metadata.
// ----------------------------------------------------------------

#[tokio::test]
async fn reservation_row_persists_lot_pin_metadata() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "M").await;
    let lot_a = seed_lot(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-15", "LOT-M-A").await;

    let id = call_reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 4, &sf.so_id, lot_a)
        .await
        .expect("query")
        .expect("pinned reservation succeeded");

    let (lot_id, lot_specific, status, qty): (Option<i64>, bool, String, i64) = sqlx::query_as(
        "SELECT lot_id, lot_specific, status::TEXT, qty
           FROM inventory_reservations WHERE id = $1::UUID",
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(lot_id, Some(lot_a));
    assert!(lot_specific);
    assert_eq!(status, "active");
    assert_eq!(qty, 4);
}
