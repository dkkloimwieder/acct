//! T1 probes for post_so_allocate pinned-reservation validation
//! (mig 0053, acct-5vh9, E2.5-followup W1).
//!
//! At allocate time, the function MUST verify each pinned active
//! reservation's lot still has enough residual to honor the pin
//! (inventory_lots residual >= reservation.qty). Concurrent ships
//! / adjustments / non-pinned reservations resolving to this lot
//! between reserve and allocate may have depleted it; catching at
//! allocate prevents a later ship-time failure from leaving the SO
//! line in a half-state.
//!
//! Coverage:
//!   E2.5f.A1 happy path — pinned residual ok, allocate succeeds
//!   E2.5f.A2 pinned lot underfulfilled (post-reserve adjust out) → P0053
//!   E2.5f.A3 only unpinned reservations → no validation, succeeds
//!   E2.5f.A4 mixed pinned + unpinned, residual ok → succeeds
//!   E2.5f.A5 SO with no reservations → succeeds (no-op validation)
//!   E2.5f.A6 idempotent replay — second call returns same doc_id
//!   E2.5f.A7 lot_id pointing at nonexistent lot → P0053
//!   E2.5f.A8 already-allocated pins not re-validated
//!   E2.5f.A9 status flips to 'allocated' after successful validation

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Scaffolding (mirrors lot_reserve_inventory_t1)
// ============================================================

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
    let sku = fresh_lot_sku(pool, &format!("SKU-LOT-AL-{label}")).await;
    let loc: String =
        sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
            .fetch_one(pool)
            .await
            .unwrap();

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

    let cust = fresh_customer(pool, &format!("AL-CUST-{label}")).await;
    let so = create_so(pool, &cust).await;

    Scaffold { sku_id: sku, loc_id: loc, qty_acct, val_acct, customer_id: cust, so_id: so }
}

#[allow(clippy::too_many_arguments)]
async fn seed_lot_in(
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

    sqlx::query_scalar::<_, i64>("SELECT lot_id FROM inventory_lots WHERE lot_code = $1")
        .bind(lot_code)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn deplete_lot(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    business_date: &str,
    lot_id: i64,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
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
    .bind(&key)
    .bind(json!({ "lot_id": lot_id }))
    .fetch_one(pool)
    .await
    .unwrap();
}

async fn fresh_so_line_id(pool: &PgPool) -> String {
    fresh_uuid(pool).await
}

async fn reserve_pinned(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    so_id: &str,
    lot_id: i64,
) -> String {
    let line = fresh_so_line_id(pool).await;
    sqlx::query_scalar::<_, Option<String>>(
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
    .unwrap()
    .expect("reserve_pinned returned NULL")
}

async fn reserve_unpinned(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    so_id: &str,
) -> String {
    let line = fresh_so_line_id(pool).await;
    sqlx::query_scalar::<_, Option<String>>(
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
    .unwrap()
    .expect("reserve_unpinned returned NULL")
}

async fn call_allocate(pool: &PgPool, so_id: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                  $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(so_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_allocate_with_key(
    pool: &PgPool,
    so_id: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_allocate($1::UUID, '2026-04-19'::DATE,
                                  $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(so_id)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn reservation_status(pool: &PgPool, reservation_id: &str) -> String {
    sqlx::query_scalar("SELECT status::text FROM inventory_reservations WHERE id = $1::UUID")
        .bind(reservation_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

// ============================================================
// E2.5f.A1 — happy path: pinned residual >= qty, allocate ok
// ============================================================

#[tokio::test]
async fn allocate_pinned_residual_ok_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A1").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A1").await;
    let r = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_a).await;

    call_allocate(&pool, &sf.so_id).await.expect("allocate");

    assert_eq!(reservation_status(&pool, &r).await, "allocated");
}

// ============================================================
// E2.5f.A2 — pinned lot underfulfilled by post-reserve adjustment
// ============================================================

#[tokio::test]
async fn allocate_pinned_lot_short_raises_p0053() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A2").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A2").await;
    let _r = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_a).await;

    // Concurrent path: a different process draws 8 from the lot
    // (e.g. via inventory_adjustment) before allocate runs. Lot
    // residual now 2; the 5-qty pin is no longer honorable.
    deplete_lot(&pool, &sf.sku_id, &sf.loc_id, 8, "2026-04-11", lot_a).await;

    let err = call_allocate(&pool, &sf.so_id).await.unwrap_err();
    let code = err.as_database_error().and_then(|e| e.code()).map(|s| s.to_string());
    assert_eq!(code.as_deref(), Some("P0053"), "got {err:?}");
}

// ============================================================
// E2.5f.A3 — only unpinned reservations: no validation, succeeds
// ============================================================

#[tokio::test]
async fn allocate_unpinned_only_skips_validation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A3").await;

    let _lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A3").await;
    let r = reserve_unpinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id).await;

    call_allocate(&pool, &sf.so_id).await.expect("allocate");
    assert_eq!(reservation_status(&pool, &r).await, "allocated");
}

// ============================================================
// E2.5f.A4 — mixed pinned + unpinned, residual ok
// ============================================================

#[tokio::test]
async fn allocate_mixed_pinned_unpinned_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A4").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 20, 100, "2026-04-10", "LOT-A4").await;
    let r_pin = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 6, &sf.so_id, lot_a).await;
    let r_un = reserve_unpinned(&pool, &sf.sku_id, &sf.loc_id, 4, &sf.so_id).await;

    call_allocate(&pool, &sf.so_id).await.expect("allocate");

    assert_eq!(reservation_status(&pool, &r_pin).await, "allocated");
    assert_eq!(reservation_status(&pool, &r_un).await, "allocated");
}

// ============================================================
// E2.5f.A5 — SO with no reservations: empty validation loop
// ============================================================

#[tokio::test]
async fn allocate_no_reservations_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A5").await;

    let _lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A5").await;
    // No reservations created.

    call_allocate(&pool, &sf.so_id).await.expect("allocate");
}

// ============================================================
// E2.5f.A6 — idempotent replay returns same doc_id
// ============================================================

#[tokio::test]
async fn allocate_idempotent_replay_returns_same_doc() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A6").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A6").await;
    let _r = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_a).await;

    let key = fresh_uuid(&pool).await;
    let doc1 = call_allocate_with_key(&pool, &sf.so_id, &key).await.expect("first");
    let doc2 = call_allocate_with_key(&pool, &sf.so_id, &key).await.expect("replay");
    assert_eq!(doc1, doc2);
}

// ============================================================
// E2.5f.A7 — lot_id pointing at a nonexistent lot raises P0053
// (defensive; reservation row would be FK-orphaned if this ever
// happened, but the validation should not silently skip)
// ============================================================

#[tokio::test]
async fn allocate_pinned_lot_missing_raises_p0053() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A7").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A7").await;
    let r = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_a).await;

    // Forge: directly UPDATE the reservation to point at a phantom
    // lot_id that doesn't exist in inventory_lots. Bypasses the
    // CHECK because lot_specific stays TRUE and lot_id stays NOT
    // NULL; the FK would normally protect us if there were one,
    // but inventory_reservations.lot_id is a soft pointer (no FK).
    sqlx::query("UPDATE inventory_reservations SET lot_id = $1 WHERE id = $2::UUID")
        .bind(999_999_999i64)
        .bind(&r)
        .execute(&pool)
        .await
        .unwrap();

    let err = call_allocate(&pool, &sf.so_id).await.unwrap_err();
    let code = err.as_database_error().and_then(|e| e.code()).map(|s| s.to_string());
    assert_eq!(code.as_deref(), Some("P0053"), "got {err:?}");
}

// ============================================================
// E2.5f.A8 — already-'allocated' pins are not re-validated.
// (The status='active' filter excludes them.)
// ============================================================

#[tokio::test]
async fn allocate_already_allocated_pins_not_revalidated() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A8").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 10, 100, "2026-04-10", "LOT-A8").await;
    let r = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 5, &sf.so_id, lot_a).await;

    // First allocate: validates + flips to 'allocated'.
    call_allocate(&pool, &sf.so_id).await.expect("first allocate");
    assert_eq!(reservation_status(&pool, &r).await, "allocated");

    // Concurrent depletion drops residual below the pin's qty.
    deplete_lot(&pool, &sf.sku_id, &sf.loc_id, 8, "2026-04-11", lot_a).await;

    // Second allocate (different idempotency_key): validation
    // skips the now-'allocated' pin, succeeds. (The original pin
    // is still drift-bound to the lot for ship-time use; that's
    // not this validation's problem.)
    call_allocate(&pool, &sf.so_id).await.expect("second allocate");
}

// ============================================================
// E2.5f.A9 — successful allocate flips active reservations to
// 'allocated' (incl. pinned and unpinned)
// ============================================================

#[tokio::test]
async fn allocate_flips_status_for_pinned_and_unpinned() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "A9").await;

    let lot_a = seed_lot_in(&pool, &sf.sku_id, &sf.loc_id, 30, 100, "2026-04-10", "LOT-A9").await;
    let r_pin = reserve_pinned(&pool, &sf.sku_id, &sf.loc_id, 4, &sf.so_id, lot_a).await;
    let r_un = reserve_unpinned(&pool, &sf.sku_id, &sf.loc_id, 7, &sf.so_id).await;

    call_allocate(&pool, &sf.so_id).await.expect("allocate");

    assert_eq!(reservation_status(&pool, &r_pin).await, "allocated");
    assert_eq!(reservation_status(&pool, &r_un).await, "allocated");
}
