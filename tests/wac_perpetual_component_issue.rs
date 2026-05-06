//! Tier 1 of acct-rgb (acct-24b). Tests that rm_issue_to_wo dispatches
//! on the COMPONENT's cost_method, fixing the prior bug where wac_perpetual
//! components were silently issued at standard cost.
//!
//! Coverage:
//!   * Running-avg consumption: pool with composed costs drains at avg.
//!   * cost_adjust on the raw pool propagates into the next WO consumption.
//!   * Mixed components in same BOM: standard + wac_perpetual issued
//!     correctly per their cost_method.
//!   * Pool stays consistent (no drift) across multiple consumptions.
//!   * Empty pool raises P0010 (cannot deplete what doesn't exist).
//!   * wac_periodic / wac_retroactive components raise P0026 (deferred to
//!     acct-7py / acct-rso).

mod common;

use common::*;
use sqlx::PgPool;
use serde_json::json;

// ============================================================
// Local scaffolding
// ============================================================

async fn fresh_sku(pool: &PgPool, code: &str, cost_method: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', $2::cost_method) RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert sku {code}: {e}"))
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert location {code}: {e}"))
}

async fn set_std_cost(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("set_std_cost {sku_id}={cost}: {e}"));
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    routing_op: Option<i32>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, routing_op, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(routing_op)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Seed a wac_perpetual component pool via post_inventory_adjustment with
/// asserted unit_cost. Composes correctly across multiple seed calls.
async fn seed_component(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, $4, 'USD', 'raw',
            $5::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("seed_component sku={sku} qty={qty}@{unit_cost}: {e}"));
}

async fn cost_adjust(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    target_unit_cost: i64,
    business_date: &str,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_cost_adjustment(
            $1::UUID, $2::UUID, 'USD', 'raw', $3,
            $4::DATE, $5::UUID, $6::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(target_unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("cost_adjust sku={sku} target={target_unit_cost}: {e}"));
}

async fn create_wo(pool: &PgPool, wo_no: &str, parent: &str, fg_loc: &str, qty: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(parent)
    .bind(fg_loc)
    .bind(qty)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_wo: {e}"))
}

async fn add_routing(pool: &PgPool, wo_id: &str, op: i32, name: &str) {
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)")
        .bind(wo_id)
        .bind(op)
        .bind(name)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("add_routing: {e}"));
}

async fn create_bom(pool: &PgPool, parent: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom: {e}"))
}

async fn add_bom_item(pool: &PgPool, bom_id: i64, line_no: i32, op: i32, comp: &str, comp_loc: &str, qty_per_parent: i64) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 100,
                 $4::UUID, $5::UUID, $6)",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(op)
    .bind(comp)
    .bind(comp_loc)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_item: {e}"));
}

async fn call_wo_start(pool: &PgPool, wo_id: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

/// Open all the parent's WIP/FG accounts plus the variance_wo_close USD
/// account (already in seed but ensure presence).
async fn open_parent_wip_fg(pool: &PgPool, parent: &str, fg_loc: &str) -> (i64, i64) {
    let _wip_qty = open_account(pool, "stock_wip", "qty", None, Some(parent), None, Some(10), "debit").await;
    let wip_val = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(parent), None, Some(10), "debit").await;
    let _fg_qty = open_account(pool, "stock_available", "qty", None, Some(parent), Some(fg_loc), None, "debit").await;
    let fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(parent), Some(fg_loc), None, "debit").await;
    (wip_val, fg_val)
}

/// Open all the component's raw + consumed accounts.
async fn open_component_raw(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let raw_qty = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let raw_val = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _consumed = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
    (raw_qty, raw_val)
}

// ============================================================
// Tests
// ============================================================

/// Composed-cost path: PO-receive-style at $10 then at $14 → pool weighted
/// avg = $12. WO consumes via rm_issue_to_wo. The value-leg drains
/// 40 × $12 = $480 (NOT 40 × $std). Pool balance and qty decrement
/// correctly; running avg stays at $12 (no drift).
#[tokio::test]
async fn wac_perpetual_component_drains_at_running_avg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P1", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "P1-C", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "P1-RAW").await;
    let fg_loc = fresh_location(&pool, "P1-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    seed_component(&pool, &comp, &raw_loc, 100, 10, "2026-04-10").await;
    seed_component(&pool, &comp, &raw_loc, 100, 14, "2026-04-11").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 2400, "pool composed: $1000 + $1400");

    // qty/p=2, WO qty=20 → adj_qty=40. running avg = $2400/200 = $12.
    // Expected WIP fill = 40 × $12 = $480.
    let wo_id = create_wo(&pool, "WO1", &parent, &fg_loc, 20).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

    assert_eq!(balance(&pool, wip_val).await, 480, "WIP fills at qty × pool_avg");
    assert_eq!(balance(&pool, comp_raw_val).await, 2400 - 480,
               "pool drops by exactly the issued value (no drift)");

    // Verify running avg stayed at $12. Per-class qty = 200 - 40 = 160.
    // Pool value = $1920. avg = $12. ✓
    let post_pool_qty: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CASE WHEN t.debit_account_id = $1 THEN t.qty
                                  WHEN t.credit_account_id = $1 THEN -t.qty END), 0)::BIGINT
           FROM transfers t
          WHERE $1 IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL",
    )
    .bind(comp_raw_val)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(post_pool_qty, 160);
    assert_eq!(balance(&pool, comp_raw_val).await / post_pool_qty, 12,
               "running avg unchanged after consumption");
}

/// cost_adjust on a wac_perpetual component pool propagates into the
/// next WO consumption. This is the regression test for the bug that
/// motivated acct-rgb: cost_adjust was observable on the raw pool but
/// invisible to downstream WO consumption.
#[tokio::test]
async fn wac_perpetual_component_cost_adjust_propagates_into_next_wo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P2", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "P2-C", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "P2-RAW").await;
    let fg_loc = fresh_location(&pool, "P2-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;
    let var_cost_adj = account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    seed_component(&pool, &comp, &raw_loc, 100, 10, "2026-04-10").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 1000);

    // Revalue raw to $12. Pool = $1200; var = -200.
    cost_adjust(&pool, &comp, &raw_loc, 12, "2026-04-11").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 1200);
    assert_eq!(balance(&pool, var_cost_adj).await, -200);

    // WO qty=20, qty/p=2 → adj_qty=40. Expected WIP = 40 × $12 = $480
    // (NOT 40 × $std-from-stale-snapshot).
    let wo_id = create_wo(&pool, "WO2", &parent, &fg_loc, 20).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

    assert_eq!(balance(&pool, wip_val).await, 480,
               "WIP picks up the post-adjust running avg");
    assert_eq!(balance(&pool, comp_raw_val).await, 720,
               "raw pool: $1200 − $480 = $720");
    // var_cost_adj unchanged by the WO (only cost_adjust touches it).
    assert_eq!(balance(&pool, var_cost_adj).await, -200);
}

/// Mixed-method components in same BOM: one standard, one wac_perpetual.
/// Each issues per its own cost_method.
#[tokio::test]
async fn wac_perpetual_mixed_with_standard_components_in_same_bom() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P3", "standard").await;
    set_std_cost(&pool, &parent, 100).await;

    // Standard component: std=$8.
    let comp_std = fresh_sku(&pool, "P3-CSTD", "standard").await;
    set_std_cost(&pool, &comp_std, 8).await;
    // wac_perpetual component.
    let comp_wac = fresh_sku(&pool, "P3-CWAC", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "P3-RAW").await;
    let fg_loc = fresh_location(&pool, "P3-FG").await;

    // Std component raw: 100 @ NULL (standard takes std cost).
    let (_, std_raw_val) = open_component_raw(&pool, &comp_std, &raw_loc).await;
    {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query_scalar::<_, String>(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, 100, NULL, 'USD', 'raw',
                '2026-04-10'::DATE, $3::UUID, $4::UUID, NULL
             )::text",
        )
        .bind(&comp_std)
        .bind(&raw_loc)
        .bind(&posted_by)
        .bind(&key)
        .fetch_one(&pool)
        .await
        .unwrap();
    }
    // wac component pool: 100 @ $10.
    let (_, wac_raw_val) = open_component_raw(&pool, &comp_wac, &raw_loc).await;
    seed_component(&pool, &comp_wac, &raw_loc, 100, 10, "2026-04-10").await;

    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // BOM: comp_std qty/p=2, comp_wac qty/p=2.
    let wo_id = create_wo(&pool, "WO3", &parent, &fg_loc, 20).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp_std, &raw_loc, 2).await;
    add_bom_item(&pool, bom_id, 2, 10, &comp_wac, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

    // Std component: 40 × $8 = $320.
    // wac component: 40 × $10 (running avg) = $400.
    // WIP = $720.
    assert_eq!(balance(&pool, wip_val).await, 720);
    assert_eq!(balance(&pool, std_raw_val).await, 100 * 8 - 40 * 8);  // 480
    assert_eq!(balance(&pool, wac_raw_val).await, 1000 - 400);         // 600
}

/// Multiple WOs consuming same wac_perpetual component pool: each takes
/// the avg at its issue time. Pool stays consistent across consumptions.
#[tokio::test]
async fn wac_perpetual_running_avg_consistent_across_multiple_consumptions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P4", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "P4-C", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "P4-RAW").await;
    let fg_loc = fresh_location(&pool, "P4-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Pool: 200 @ $12 avg from composed receipts.
    seed_component(&pool, &comp, &raw_loc, 100, 10, "2026-04-10").await;
    seed_component(&pool, &comp, &raw_loc, 100, 14, "2026-04-11").await;

    // WO1: qty=10, qty/p=2 → adj=20. drain 20 × $12 = $240. Pool: 180@$12=$2160.
    let wo1 = create_wo(&pool, "WO-A", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;
    call_wo_start(&pool, &wo1).await.expect("wo_start1");
    assert_eq!(balance(&pool, comp_raw_val).await, 2160);

    // WO2: same parent, same BOM (re-use). qty=15, qty/p=2 → adj=30. drain 30 × $12 = $360.
    // Pool: 150 @ $12 = $1800.
    let wo2 = create_wo(&pool, "WO-B", &parent, &fg_loc, 15).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    call_wo_start(&pool, &wo2).await.expect("wo_start2");
    assert_eq!(balance(&pool, comp_raw_val).await, 1800,
               "pool consistent: avg unchanged across consumptions");
    assert_eq!(balance(&pool, wip_val).await, 240 + 360,
               "WIP accumulated from both WOs");
}

/// Empty wac_perpetual component pool → P0010 on wo_start.
#[tokio::test]
async fn wac_perpetual_empty_component_pool_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P5", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "P5-C", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "P5-RAW").await;
    let fg_loc = fresh_location(&pool, "P5-FG").await;

    open_component_raw(&pool, &comp, &raw_loc).await;
    open_parent_wip_fg(&pool, &parent, &fg_loc).await;
    // Pool intentionally empty — no seed call.

    let wo_id = create_wo(&pool, "WO-E", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    expect_sqlstate(
        "P0010",
        || async { call_wo_start(&pool, &wo_id).await.map(|_| ()) },
    )
    .await;
}

/// wac_periodic component on a standard parent (acct-7eo, mig 0077).
/// Smoke test that the formerly-deferred mixed case now succeeds at
/// rm_issue time. Full mixed-case variance routing is verified by
/// tests/wac_periodic_component_issue.rs::standard_parent_with_wac_periodic_component_routes_mixed_variance.
#[tokio::test]
async fn wac_periodic_component_on_standard_parent_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "P6", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "P6-C", "wac_periodic").await;
    let raw_loc = fresh_location(&pool, "P6-RAW").await;
    let fg_loc = fresh_location(&pool, "P6-FG").await;

    let (_, _comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let _ = sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 100, 10, 'USD', 'raw',
            '2026-04-10'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    )
    .bind(&comp)
    .bind(&raw_loc)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .unwrap();

    let wo_id = create_wo(&pool, "WO-WP", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id).await
        .expect("acct-7eo: mixed wac_periodic component on standard parent now succeeds");
}

// Suppress unused warning on json import (helper for future extension).
#[allow(dead_code)]
fn _unused() -> serde_json::Value { json!({}) }
