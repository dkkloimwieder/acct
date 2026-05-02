//! `acct-wl1` / Slice B.2 — T1 invariant probes for the conversion-
//! cycle tables (work_orders, wo_routings, wo_routing_burdens, boms,
//! wo_events).
//!
//! Pins the table-level CHECK / FK / UNIQUE constraints so a schema
//! regression surfaces here rather than as confused matrix-test
//! failures.

mod common;

use common::*;
use sqlx::PgPool;

async fn one_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn one_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn one_wo(pool: &PgPool) -> String {
    let parent = one_sku(pool, "T1-P").await;
    let loc = one_location(pool, "T1-FG").await;
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ('T1-WO', $1::UUID, $2::UUID, 10, 'USD', $3::UUID) RETURNING id::text",
    )
    .bind(&parent)
    .bind(&loc)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ============================================================
// work_orders
// ============================================================

#[tokio::test]
async fn work_orders_qty_target_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-A").await;
    let loc = one_location(&pool, "T1-L-A").await;
    let posted_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
             VALUES ('T1-WO-A', $1::UUID, $2::UUID, 0, 'USD', $3::UUID)",
        )
        .bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn work_orders_completed_plus_scrapped_over_target_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-B").await;
    let loc = one_location(&pool, "T1-L-B").await;
    let posted_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target,
                                       qty_completed, qty_scrapped, currency, posted_by)
             VALUES ('T1-WO-B', $1::UUID, $2::UUID, 10, 6, 5, 'USD', $3::UUID)",
        )
        .bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn work_orders_bad_status_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-C").await;
    let loc = one_location(&pool, "T1-L-C").await;
    let posted_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, status, currency, posted_by)
             VALUES ('T1-WO-C', $1::UUID, $2::UUID, 10, 'bogus', 'USD', $3::UUID)",
        )
        .bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn work_orders_duplicate_wo_no_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-D").await;
    let loc = one_location(&pool, "T1-L-D").await;
    let posted_by = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ('T1-WO-DUP', $1::UUID, $2::UUID, 10, 'USD', $3::UUID)",
    ).bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
             VALUES ('T1-WO-DUP', $1::UUID, $2::UUID, 10, 'USD', $3::UUID)",
        ).bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

// ============================================================
// wo_routings
// ============================================================

#[tokio::test]
async fn wo_routings_routing_op_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 0, 'OP0')")
            .bind(&wo).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_routings_duplicate_op_violates_pk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'OP1')")
        .bind(&wo).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'OP1B')")
            .bind(&wo).execute(&pool).await
    }).await;
}

// ============================================================
// wo_routing_burdens
// ============================================================

#[tokio::test]
async fn wo_routing_burdens_negative_amount_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'OP1')")
        .bind(&wo).execute(&pool).await.unwrap();
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_routing_burdens (wo_id, routing_op, applied_account_kind, std_amount)
             VALUES ($1::UUID, 10, 'labor_applied', -1)",
        ).bind(&wo).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_routing_burdens_unknown_op_violates_fk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO wo_routing_burdens (wo_id, routing_op, applied_account_kind, std_amount)
             VALUES ($1::UUID, 99, 'labor_applied', 5)",
        ).bind(&wo).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_routing_burdens_duplicate_pk_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'OP1')")
        .bind(&wo).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO wo_routing_burdens (wo_id, routing_op, applied_account_kind, std_amount)
         VALUES ($1::UUID, 10, 'labor_applied', 5)",
    ).bind(&wo).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO wo_routing_burdens (wo_id, routing_op, applied_account_kind, std_amount)
             VALUES ($1::UUID, 10, 'labor_applied', 7)",
        ).bind(&wo).execute(&pool).await
    }).await;
}

// ============================================================
// boms
// ============================================================

#[tokio::test]
async fn boms_qty_per_parent_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-BOM-P").await;
    let comp = one_sku(&pool, "T1-BOM-C").await;
    let loc = one_location(&pool, "T1-BOM-L").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO boms (parent_sku_id, component_sku_id, component_loc_id, qty_per_parent)
             VALUES ($1::UUID, $2::UUID, $3::UUID, 0)",
        ).bind(&parent).bind(&comp).bind(&loc).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn boms_self_reference_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = one_sku(&pool, "T1-BOM-SELF").await;
    let loc = one_location(&pool, "T1-BOM-LSELF").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO boms (parent_sku_id, component_sku_id, component_loc_id, qty_per_parent)
             VALUES ($1::UUID, $1::UUID, $2::UUID, 1)",
        ).bind(&sku).bind(&loc).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn boms_duplicate_pk_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-BOM-DP").await;
    let comp = one_sku(&pool, "T1-BOM-DC").await;
    let loc = one_location(&pool, "T1-BOM-DL").await;
    sqlx::query(
        "INSERT INTO boms (parent_sku_id, component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1::UUID, $2::UUID, $3::UUID, 1)",
    ).bind(&parent).bind(&comp).bind(&loc).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO boms (parent_sku_id, component_sku_id, component_loc_id, qty_per_parent)
             VALUES ($1::UUID, $2::UUID, $3::UUID, 2)",
        ).bind(&parent).bind(&comp).bind(&loc).execute(&pool).await
    }).await;
}

// ============================================================
// wo_events
// ============================================================

#[tokio::test]
async fn wo_events_bad_event_kind_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'bogus', '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_start_with_routing_op_from_violates_composite_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    // 'start' must have all routing_op_* and qty NULL.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, routing_op_from, business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'start', 10, '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_op_move_missing_to_op_violates_composite_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, routing_op_from, qty,
                                    business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'op_move', 10, 5, '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_qty_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    // qty IS NULL OR qty > 0.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, routing_op_from, qty,
                                    business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'scrap', 10, 0, '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_duplicate_idempotency_key_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO wo_events (wo_id, event_kind, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, 'start', '2026-04-15', $2::UUID, $3::UUID)",
    ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'start', '2026-04-16', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}
