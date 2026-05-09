//! T1 probes for lot_fifo via rm_issue_to_wo at WO start
//! (mig 0049, acct-7jkk). Phase E2 follow-up L3.
//!
//! Drives a multi-component WO where one or more components are
//! lot_fifo, then verifies:
//!   - the wrapper walks _lot_walk_layers under FOR UPDATE for v_value,
//!   - apply_event re-walks under the same locks for inventory_lot_events
//!     writeback,
//!   - SUM(inventory_lot_events.quantity_change) per lot = -depleted qty
//!     and pool ledger balance matches by per-row construction,
//!   - FIFO walk default (no pin) consumes the oldest lot first,
//!   - explicit pin via p_component_lot_pins consumes the named lot,
//!   - pool empty raises (qty CHECK or P0006 — either is acceptable),
//!   - mixed component cost methods (lot_fifo + standard) coexist.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffolding (mirrors fifo_rm_issue_to_wo_t1).
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
    .unwrap()
}

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

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
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
    .unwrap();
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
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
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
    .unwrap()
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Seed a lot via post_inventory_adjustment (L2 contract):
/// +qty with lot_code creates an inventory_lots row.
async fn seed_lot(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    unit_cost: i64,
    lot_code: &str,
    business_date: &str,
) -> i64 {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, $4, 'USD', 'raw',
            $5::DATE, $6::UUID, $7::UUID, NULL, $8
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .bind(json!({ "lot_code": lot_code }))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("seed_lot sku={sku} qty={qty}@{unit_cost}: {e}"));

    sqlx::query_scalar("SELECT lot_id FROM inventory_lots WHERE product_id = $1::UUID AND lot_code = $2")
        .bind(sku)
        .bind(lot_code)
        .fetch_one(pool)
        .await
        .unwrap()
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
    .unwrap()
}

async fn add_routing(pool: &PgPool, wo_id: &str, op: i32, name: &str) {
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)")
        .bind(wo_id)
        .bind(op)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

async fn create_bom(pool: &PgPool, parent: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn add_bom_item(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    op: i32,
    comp: &str,
    comp_loc: &str,
    qty_per_parent: i64,
) {
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
    .unwrap();
}

async fn call_wo_start(
    pool: &PgPool,
    wo_id: &str,
    pins: Option<serde_json::Value>,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL, $4)::text",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(&key)
    .bind(pins)
    .fetch_one(pool)
    .await
}

async fn open_parent_wip_fg(pool: &PgPool, parent: &str, fg_loc: &str) -> (i64, i64) {
    let _wip_qty = open_account(pool, "stock_wip", "qty", None, Some(parent), None, Some(10), "debit").await;
    let wip_val = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(parent), None, Some(10), "debit").await;
    let _fg_qty = open_account(pool, "stock_available", "qty", None, Some(parent), Some(fg_loc), None, "debit").await;
    let fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(parent), Some(fg_loc), None, "debit").await;
    (wip_val, fg_val)
}

async fn open_component_raw(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let raw_qty = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let raw_val = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _consumed = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
    (raw_qty, raw_val)
}

// ============================================================
// L3.1: lot_fifo component drains via FIFO walk default (no pin).
// Two lots seeded; oldest receipt_date consumed first.
// ============================================================

#[tokio::test]
async fn lot_component_drains_in_fifo_order_default() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "L3-P1", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_lot_sku(&pool, "L3-P1-C").await;
    let raw_loc = fresh_location(&pool, "L3-P1-RAW").await;
    let fg_loc = fresh_location(&pool, "L3-P1-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Lot A: 30 @ 10 ($300) on 2026-04-10. Lot B: 50 @ 14 ($700) on 2026-04-12.
    let lot_a = seed_lot(&pool, &comp, &raw_loc, 30, 10, "LOT-A001", "2026-04-10").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 50, 14, "LOT-A002", "2026-04-12").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 1000);

    // qty/p=2, WO qty=20 → adj_qty=40. FIFO span:
    //   30 from Lot A ($300) + 10 from Lot B ($140) = $440.
    let wo_id = create_wo(&pool, "L3-WO1", &parent, &fg_loc, 20).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id, None).await.expect("wo_start");

    assert_eq!(balance(&pool, wip_val).await, 440);
    assert_eq!(balance(&pool, comp_raw_val).await, 1000 - 440);

    // inventory_lot_events: -30 against Lot A then -10 against Lot B.
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT lot_id, quantity_change::TEXT
           FROM inventory_lot_events
          ORDER BY event_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events.len(), 2, "2 events expected (FIFO span)");
    assert_eq!(events[0].0, lot_a);
    assert!(events[0].1.starts_with("-30"), "got {}", events[0].1);
    assert_eq!(events[1].0, lot_b);
    assert!(events[1].1.starts_with("-10"), "got {}", events[1].1);
}

// ============================================================
// L3.2: explicit pin via p_component_lot_pins consumes the named
// lot (NEWER lot, despite an older lot existing).
// ============================================================

#[tokio::test]
async fn lot_component_pin_consumes_named_lot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "L3-P2", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_lot_sku(&pool, "L3-P2-C").await;
    let raw_loc = fresh_location(&pool, "L3-P2-RAW").await;
    let fg_loc = fresh_location(&pool, "L3-P2-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 30, 10, "LOT-B001", "2026-04-10").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 50, 14, "LOT-B002", "2026-04-12").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 1000);

    // adj_qty=20 (qty/p=2 × WO qty=10). Pin to Lot B explicitly.
    // Cost = 20 × $14 = $280.
    let wo_id = create_wo(&pool, "L3-WO2", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.clone(): lot_b });
    call_wo_start(&pool, &wo_id, Some(pins)).await.expect("wo_start with pin");

    assert_eq!(balance(&pool, wip_val).await, 280);
    assert_eq!(balance(&pool, comp_raw_val).await, 1000 - 280);

    // Single event against Lot B (Lot A untouched).
    let events: Vec<(i64, String)> = sqlx::query_as(
        "SELECT lot_id, quantity_change::TEXT
           FROM inventory_lot_events
          ORDER BY event_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events.len(), 1, "1 event expected (pinned to Lot B)");
    assert_eq!(events[0].0, lot_b);
    assert!(events[0].1.starts_with("-20"));

    // Lot A residual unchanged at 30.
    let (a_orig, a_events): (String, Option<i64>) = sqlx::query_as(
        "SELECT il.original_quantity::TEXT, ile.quantity_change::BIGINT
           FROM inventory_lots il
           LEFT JOIN inventory_lot_events ile ON ile.lot_id = il.lot_id
          WHERE il.lot_id = $1",
    )
    .bind(lot_a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(a_orig.starts_with("30"));
    assert_eq!(a_events, None, "Lot A should have no events");
}

// ============================================================
// L3.3: empty lot_fifo pool rejects WO start.
// ============================================================

#[tokio::test]
async fn lot_component_empty_pool_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "L3-P3", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_lot_sku(&pool, "L3-P3-C").await;
    let raw_loc = fresh_location(&pool, "L3-P3-RAW").await;
    let fg_loc = fresh_location(&pool, "L3-P3-FG").await;

    let _ = open_component_raw(&pool, &comp, &raw_loc).await;
    let _ = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // No lots seeded.
    let wo_id = create_wo(&pool, "L3-WO3", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let err = call_wo_start(&pool, &wo_id, None).await.err().expect("empty pool reject");
    let code = err
        .as_database_error()
        .and_then(|e| e.code().map(|c| c.into_owned()))
        .unwrap_or_default();
    assert!(
        code == "P0006" || code == "23514",
        "expected P0006 or 23514, got {code}"
    );
}

// ============================================================
// L3.4: mixed components — one lot_fifo + one standard — coexist.
// Both consume their respective costs into parent WIP.
// ============================================================

#[tokio::test]
async fn lot_and_standard_components_coexist() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "L3-P4", "standard").await;
    set_std_cost(&pool, &parent, 500).await;
    let lot_comp = fresh_lot_sku(&pool, "L3-P4-CL").await;
    let std_comp = fresh_sku(&pool, "L3-P4-CS", "standard").await;
    set_std_cost(&pool, &std_comp, 25).await;
    let raw_loc = fresh_location(&pool, "L3-P4-RAW").await;
    let fg_loc = fresh_location(&pool, "L3-P4-FG").await;

    let (_, lot_raw_val) = open_component_raw(&pool, &lot_comp, &raw_loc).await;
    let (_, std_raw_val) = open_component_raw(&pool, &std_comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Lot component: 50 @ 12 = $600.
    seed_lot(&pool, &lot_comp, &raw_loc, 50, 12, "LOT-C001", "2026-04-10").await;
    // Standard component: seed via standard branch of post_inventory_adjustment.
    let posted_by = fresh_uuid(&pool).await;
    let std_key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, NULL, 'USD', 'raw',
            '2026-04-10'::DATE, $4::UUID, $5::UUID, NULL, NULL
         )::text",
    )
    .bind(&std_comp)
    .bind(&raw_loc)
    .bind(40_i64)
    .bind(&posted_by)
    .bind(&std_key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(balance(&pool, lot_raw_val).await, 600);
    assert_eq!(balance(&pool, std_raw_val).await, 1000); // 40 × $25 std

    // adj_qty for each component = 1 × WO qty=10 = 10.
    // Lot draws: 10 × $12 = $120. Std draws: 10 × $25 = $250. WIP = 370.
    let wo_id = create_wo(&pool, "L3-WO4", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "ASM").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &lot_comp, &raw_loc, 1).await;
    add_bom_item(&pool, bom_id, 2, 10, &std_comp, &raw_loc, 1).await;

    call_wo_start(&pool, &wo_id, None).await.expect("wo_start mixed");

    assert_eq!(balance(&pool, wip_val).await, 370);
    assert_eq!(balance(&pool, lot_raw_val).await, 600 - 120);
    assert_eq!(balance(&pool, std_raw_val).await, 1000 - 250);

    // Lot got an event; standard SKU has no inventory_lots row.
    let lot_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lot_events ile
         JOIN inventory_lots il ON il.lot_id = ile.lot_id
         WHERE il.product_id = $1::UUID",
    )
    .bind(&lot_comp)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_events, 1);
    let std_lots: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&std_comp)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(std_lots, 0);
}

// ============================================================
// L3.5: pinned lot residual short rejects (P0006).
// ============================================================

#[tokio::test]
async fn lot_component_pin_residual_short_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "L3-P5", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_lot_sku(&pool, "L3-P5-C").await;
    let raw_loc = fresh_location(&pool, "L3-P5-RAW").await;
    let fg_loc = fresh_location(&pool, "L3-P5-FG").await;

    let _ = open_component_raw(&pool, &comp, &raw_loc).await;
    let _ = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Lot A only has 5 units; demand is 20.
    let lot_a = seed_lot(&pool, &comp, &raw_loc, 5, 10, "LOT-D001", "2026-04-10").await;
    // Seed Lot B with plenty so the pool overall has enough — only the
    // PINNED lot is short.
    let _lot_b = seed_lot(&pool, &comp, &raw_loc, 100, 12, "LOT-D002", "2026-04-12").await;

    let wo_id = create_wo(&pool, "L3-WO5", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await; // adj_qty = 20

    let pins = json!({ comp.clone(): lot_a });
    let err = call_wo_start(&pool, &wo_id, Some(pins))
        .await
        .err()
        .expect("expected residual-short rejection");
    let code = err
        .as_database_error()
        .and_then(|e| e.code().map(|c| c.into_owned()))
        .unwrap_or_default();
    assert_eq!(code, "P0006", "expected P0006 lot_residual_short, got {code}");
}

// ============================================================
// L3.6: idempotent replay of post_wo_start with pins.
// ============================================================

#[tokio::test]
async fn lot_component_idempotent_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "L3-P6", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_lot_sku(&pool, "L3-P6-C").await;
    let raw_loc = fresh_location(&pool, "L3-P6-RAW").await;
    let fg_loc = fresh_location(&pool, "L3-P6-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    let lot = seed_lot(&pool, &comp, &raw_loc, 50, 8, "LOT-E001", "2026-04-10").await;

    let wo_id = create_wo(&pool, "L3-WO6", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await; // adj_qty = 10

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let pins = json!({ comp.clone(): lot });

    let r1: String = sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL, $4)::text",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .bind(&pins)
    .fetch_one(&pool)
    .await
    .expect("first wo_start");

    let r2: String = sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL, $4)::text",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .bind(&pins)
    .fetch_one(&pool)
    .await
    .expect("replay wo_start");
    assert_eq!(r1, r2);

    // Ledger unchanged on replay: 10 × $8 = $80.
    assert_eq!(balance(&pool, wip_val).await, 80);
    assert_eq!(balance(&pool, comp_raw_val).await, 50 * 8 - 80);

    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lot_events WHERE lot_id = $1",
    )
    .bind(lot)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_count, 1, "no duplicate event on replay");
}
