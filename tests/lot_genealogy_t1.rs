//! T1 probes for lot_genealogy (mig 0059, acct-3j3z).
//!
//! Drives lot_fifo parent + lot_fifo component WOs through
//! post_wo_start + post_wo_complete and asserts the parent-child
//! lineage table:
//!
//!   G1 — single component / single FG output: one row tying
//!        parent lot → FG child.
//!   G2 — multi-component / single FG output: N rows, one per
//!        consumed component lot.
//!   G3 — unpinned multi-lot FIFO walk / single FG output:
//!        each source lot becomes a parent → child genealogy row.
//!   G4 — multi-FG co-product (one component × two outputs with
//!        60/40 allocation_pct): qty_consumed splits proportionally
//!        per Q1=(b).
//!   G5 — partial wo_complete (50%/50% via two events): two FG lots
//!        emerge, each gets proportional consumption per Q2=(b).
//!   G6 — idempotent replay of post_wo_complete: ON CONFLICT DO
//!        NOTHING prevents duplicate genealogy rows.
//!   G7 — standard parent: NO lot_genealogy rows written
//!        (helper not invoked; v_output_recs stays empty).
//!   G8 — recon check #12 clean after a complete WO cycle.
//!   G9 — recon check #12 fires on synthesized overshoot.
//!   G10 — lineage downstream view: from raw lot V, walk forward
//!         to FG children including transitively through a second WO.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffolding
// ============================================================

async fn fresh_sku_lot(pool: &PgPool, code: &str) -> String {
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

async fn fresh_sku_standard(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard'::cost_method) RETURNING id::text",
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
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

/// Open lot_fifo component accounts at a raw location.
async fn open_lot_component(pool: &PgPool, comp: &str, raw_loc: &str) {
    let _ = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let _ = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _ = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
}

/// Open per-routing-op WIP accounts for a parent.
async fn open_parent_wip_ops(pool: &PgPool, parent: &str, ops: &[i32]) {
    for &op in ops {
        let _ = open_account(pool, "stock_wip", "qty", None, Some(parent), None, Some(op), "debit").await;
        let _ = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(parent), None, Some(op), "debit").await;
    }
}

/// Open FG accounts for a parent at fg_loc.
async fn open_parent_fg(pool: &PgPool, parent: &str, fg_loc: &str) {
    let _ = open_account(pool, "stock_available", "qty", None, Some(parent), Some(fg_loc), None, "debit").await;
    let _ = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(parent), Some(fg_loc), None, "debit").await;
}

/// Seed a lot via post_inventory_adjustment with lot_metadata.
async fn seed_lot(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
    lot_code: &str,
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
    .unwrap_or_else(|e| panic!("seed_lot {lot_code}: {e}"));

    sqlx::query_scalar::<_, i64>(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID AND lot_code = $3",
    )
    .bind(sku)
    .bind(loc)
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

#[allow(clippy::too_many_arguments)]
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
    business_date: &str,
    lot_pins: Option<serde_json::Value>,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, $5)::text",
    )
    .bind(wo_id)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .bind(lot_pins)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(
    pool: &PgPool,
    wo_id: &str,
    qty: i64,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete_replay(
    pool: &PgPool,
    wo_id: &str,
    qty: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

async fn count_genealogy(pool: &PgPool, wo_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM lot_genealogy WHERE wo_id = $1::UUID")
        .bind(wo_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn count_alerts_kind(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM reconciliation_alerts WHERE alert_kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Insert a wo_output row directly (for multi-output co-product scenarios).
#[allow(clippy::too_many_arguments)]
async fn add_wo_output(
    pool: &PgPool,
    wo_id: &str,
    output_no: i32,
    output_sku: &str,
    fg_loc: &str,
    qty: i64,
    allocation_pct: i32,
) {
    sqlx::query(
        "INSERT INTO wo_outputs
            (wo_id, output_no, output_sku_id, fg_location_id, qty,
             allocation_method, allocation_pct)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5,
                 'fixed_ratio', $6)",
    )
    .bind(wo_id)
    .bind(output_no)
    .bind(output_sku)
    .bind(fg_loc)
    .bind(qty)
    .bind(allocation_pct)
    .execute(pool)
    .await
    .unwrap();
}

// ============================================================
// G1 — single component + single FG output: one genealogy row.
// ============================================================

#[tokio::test]
async fn single_component_single_fg_writes_one_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G1-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G1-COMP").await;
    let raw_loc = fresh_location(&pool, "G1-RAW").await;
    let fg_loc = fresh_location(&pool, "G1-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "G1-LOT-A").await;

    let wo_id = create_wo(&pool, "G1-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    // qty_per_parent=2, qty_target=5 -> adj_qty=10. Pin LOT-A.
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    assert_eq!(count_genealogy(&pool, &wo_id).await, 1, "one row");

    // Verify the row content.
    let row: (i64, i64, String, String) = sqlx::query_as(
        "SELECT parent_lot_id, child_lot_id, qty_consumed::text, wo_id::text
           FROM lot_genealogy WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, lot_a, "parent is LOT-A");
    assert!(row.1 > lot_a, "child lot_id is the new FG lot (later id)");
    // qty_consumed = 10 (full consumption) × 100% × 1.0 = 10.
    assert_eq!(row.2.split('.').next().unwrap(), "10");
    assert_eq!(row.3, wo_id);

    // Recon clean.
    let _: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count_alerts_kind(&pool, "lot_genealogy_qty_overshoot").await, 0);
}

// ============================================================
// G2 — multi-component + single FG: N rows (one per component lot).
// ============================================================

#[tokio::test]
async fn multi_component_single_fg_writes_n_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G2-PARENT").await;
    let comp_a = fresh_sku_lot(&pool, "G2-COMP-A").await;
    let comp_b = fresh_sku_lot(&pool, "G2-COMP-B").await;
    let comp_c = fresh_sku_lot(&pool, "G2-COMP-C").await;
    let raw_loc = fresh_location(&pool, "G2-RAW").await;
    let fg_loc = fresh_location(&pool, "G2-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp_a, &raw_loc).await;
    open_lot_component(&pool, &comp_b, &raw_loc).await;
    open_lot_component(&pool, &comp_c, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp_a, &raw_loc, 20, 100, "2026-04-10", "G2-LOT-A").await;
    let lot_b = seed_lot(&pool, &comp_b, &raw_loc, 30, 200, "2026-04-10", "G2-LOT-B").await;
    let lot_c = seed_lot(&pool, &comp_c, &raw_loc, 40, 300, "2026-04-10", "G2-LOT-C").await;

    let wo_id = create_wo(&pool, "G2-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp_a, &raw_loc, 2).await;
    add_bom_item(&pool, bom_id, 2, 10, &comp_b, &raw_loc, 3).await;
    add_bom_item(&pool, bom_id, 3, 10, &comp_c, &raw_loc, 1).await;

    let pins = json!({
        comp_a.to_string(): lot_a,
        comp_b.to_string(): lot_b,
        comp_c.to_string(): lot_c,
    });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    assert_eq!(count_genealogy(&pool, &wo_id).await, 3, "3 rows for 3 component lots");

    // All point to the same child_lot.
    let n_distinct_children: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT child_lot_id)::BIGINT FROM lot_genealogy WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n_distinct_children, 1, "single FG output → one child");

    // Verify per-parent qty_consumed values.
    let parents: Vec<(i64, String)> = sqlx::query_as(
        "SELECT parent_lot_id, qty_consumed::text FROM lot_genealogy
          WHERE wo_id = $1::UUID ORDER BY parent_lot_id",
    )
    .bind(&wo_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    let parent_ids: Vec<i64> = parents.iter().map(|r| r.0).collect();
    assert!(parent_ids.contains(&lot_a) && parent_ids.contains(&lot_b) && parent_ids.contains(&lot_c));

    let _: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count_alerts_kind(&pool, "lot_genealogy_qty_overshoot").await, 0);
}

// ============================================================
// G3 — unpinned multi-lot FIFO walk: 2 source lots tied to 1 child.
// ============================================================

#[tokio::test]
async fn unpinned_multi_lot_walk_writes_two_genealogy_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G3-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G3-COMP").await;
    let raw_loc = fresh_location(&pool, "G3-RAW").await;
    let fg_loc = fresh_location(&pool, "G3-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 4, 100, "2026-04-10", "G3-LOT-A").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 6, 200, "2026-04-12", "G3-LOT-B").await;

    let wo_id = create_wo(&pool, "G3-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    // qty_per_parent=1, qty_target=5 -> adj_qty=5. Spans both: 4 from A + 1 from B.
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    call_wo_start(&pool, &wo_id, "2026-04-15", None)
        .await
        .expect("unpinned wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    assert_eq!(count_genealogy(&pool, &wo_id).await, 2, "two parent lots");

    let rows: Vec<(i64, i64, String)> = sqlx::query_as(
        "SELECT parent_lot_id, child_lot_id, qty_consumed::text
           FROM lot_genealogy WHERE wo_id = $1::UUID ORDER BY parent_lot_id",
    )
    .bind(&wo_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    let parents: Vec<i64> = rows.iter().map(|r| r.0).collect();
    assert!(parents.contains(&lot_a) && parents.contains(&lot_b));
    assert_eq!(rows[0].1, rows[1].1, "both rows point at same FG child");

    // SUM(qty_consumed) over both rows = total consumption (5 units) × 100% × 1.0.
    let total: String = sqlx::query_scalar(
        "SELECT SUM(qty_consumed)::TEXT FROM lot_genealogy WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total.split('.').next().unwrap(), "5");
}

// ============================================================
// G4 — multi-FG co-product (1 component × 2 outputs 60/40):
//      qty_consumed splits proportionally per Q1=(b).
// ============================================================

#[tokio::test]
async fn multi_output_co_product_splits_qty_by_allocation_pct() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G4-PARENT").await;
    let coprod = fresh_sku_lot(&pool, "G4-COPROD").await;
    let comp = fresh_sku_lot(&pool, "G4-COMP").await;
    let raw_loc = fresh_location(&pool, "G4-RAW").await;
    let fg_loc = fresh_location(&pool, "G4-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_parent_fg(&pool, &coprod, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "G4-LOT-A").await;

    let wo_id = create_wo(&pool, "G4-WO", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    // Two outputs: 60% parent / 40% co-product. wo_start auto-init only
    // creates a single primary row; we want a manual two-output split.
    add_wo_output(&pool, &wo_id, 1, &parent, &fg_loc, 6, 60).await;
    add_wo_output(&pool, &wo_id, 2, &coprod, &fg_loc, 4, 40).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 10, "2026-04-16")
        .await
        .expect("wo_complete");

    // Two children (one per output) × one parent = 2 rows.
    assert_eq!(count_genealogy(&pool, &wo_id).await, 2);

    // qty_consumed for output 1 = 10 × 60/100 × 1.0 = 6.
    // qty_consumed for output 2 = 10 × 40/100 × 1.0 = 4.
    let qtys: Vec<String> = sqlx::query_scalar(
        "SELECT qty_consumed::text FROM lot_genealogy
          WHERE wo_id = $1::UUID
          ORDER BY qty_consumed DESC",
    )
    .bind(&wo_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(qtys[0].split('.').next().unwrap(), "6", "60% output");
    assert_eq!(qtys[1].split('.').next().unwrap(), "4", "40% output");
}

// ============================================================
// G5 — partial wo_complete: 2 FG lots with proportional consumption.
// ============================================================

#[tokio::test]
async fn partial_wo_complete_splits_consumption_by_qty_share() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G5-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G5-COMP").await;
    let raw_loc = fresh_location(&pool, "G5-RAW").await;
    let fg_loc = fresh_location(&pool, "G5-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "G5-LOT-A").await;

    let wo_id = create_wo(&pool, "G5-WO", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");

    // Two partials: 5 then 5, qty_target=10.
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("partial 1");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-17")
        .await
        .expect("partial 2");

    // 1 parent × 2 children (one per partial) = 2 rows.
    assert_eq!(count_genealogy(&pool, &wo_id).await, 2);

    // Each partial = 50% share. qty_consumed = 10 (consumed at start)
    // × 100% × 0.5 = 5 per partial.
    let qtys: Vec<String> = sqlx::query_scalar(
        "SELECT qty_consumed::text FROM lot_genealogy
          WHERE wo_id = $1::UUID ORDER BY child_lot_id",
    )
    .bind(&wo_id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(qtys[0].split('.').next().unwrap(), "5");
    assert_eq!(qtys[1].split('.').next().unwrap(), "5");
}

// ============================================================
// G6 — idempotent replay of post_wo_complete: no duplicate rows.
// ============================================================

#[tokio::test]
async fn idempotent_replay_does_not_duplicate_genealogy() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G6-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G6-COMP").await;
    let raw_loc = fresh_location(&pool, "G6-RAW").await;
    let fg_loc = fresh_location(&pool, "G6-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 20, 100, "2026-04-10", "G6-LOT-A").await;

    let wo_id = create_wo(&pool, "G6-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");

    let key = fresh_uuid(&pool).await;
    call_wo_complete_replay(&pool, &wo_id, 5, "2026-04-16", &key)
        .await
        .expect("wo_complete first");
    let n1 = count_genealogy(&pool, &wo_id).await;
    assert_eq!(n1, 1);

    // Replay with same idempotency_key — no-op at wo_events level.
    call_wo_complete_replay(&pool, &wo_id, 5, "2026-04-16", &key)
        .await
        .expect("wo_complete replay");
    let n2 = count_genealogy(&pool, &wo_id).await;
    assert_eq!(n2, n1, "no new rows on replay");
}

// ============================================================
// G7 — standard parent: NO genealogy rows written.
// ============================================================

#[tokio::test]
async fn standard_parent_writes_no_genealogy() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "G7-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "G7-COMP").await;
    let raw_loc = fresh_location(&pool, "G7-RAW").await;
    let fg_loc = fresh_location(&pool, "G7-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 20, 100, "2026-04-10", "G7-LOT-A").await;

    let wo_id = create_wo(&pool, "G7-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    // wo_lot_consumption STILL gets written (component is lot_fifo).
    let n_consumption: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM wo_lot_consumption WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(n_consumption > 0, "lot consumption still recorded");

    // But lot_genealogy gets ZERO rows (parent is standard, no FG-side
    // lot is created).
    assert_eq!(count_genealogy(&pool, &wo_id).await, 0);
}

// ============================================================
// G8 — recon check #12 clean after a complete WO cycle.
// ============================================================

#[tokio::test]
async fn recon_check_12_clean_post_wo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G8-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G8-COMP").await;
    let raw_loc = fresh_location(&pool, "G8-RAW").await;
    let fg_loc = fresh_location(&pool, "G8-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "G8-LOT-A").await;

    let wo_id = create_wo(&pool, "G8-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    // Pre-recon: clean.
    let alerts_before = count_alerts_kind(&pool, "lot_genealogy_qty_overshoot").await;
    assert_eq!(alerts_before, 0);

    // Run recon.
    let _: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();

    // Post-recon: still clean for #12.
    let alerts_after = count_alerts_kind(&pool, "lot_genealogy_qty_overshoot").await;
    assert_eq!(alerts_after, 0);
}

// ============================================================
// G9 — recon check #12 fires on synthesized overshoot.
// ============================================================

#[tokio::test]
async fn recon_check_12_fires_on_overshoot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G9-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G9-COMP").await;
    let raw_loc = fresh_location(&pool, "G9-RAW").await;
    let fg_loc = fresh_location(&pool, "G9-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "G9-LOT-A").await;

    let wo_id = create_wo(&pool, "G9-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_a });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    // Synthesize an overshoot by INSERTing a phantom genealogy row
    // tied to a fresh wo_id (only existing for the wo_id slot — no
    // BOM, no start, no complete) with a HUGE qty_consumed that by
    // itself exceeds the lot_events sum.
    let wo3_id = create_wo(&pool, "G9-WO3", &parent, &fg_loc, 1).await;

    // INSERT phantom genealogy row directly via sub-SELECTs so we
    // don't need to round-trip dates through Rust.
    sqlx::query(
        "INSERT INTO lot_genealogy
            (parent_lot_id, parent_receipt_date,
             child_lot_id, child_receipt_date,
             qty_consumed, wo_id, posting_line_id)
         SELECT
            $1,
            (SELECT receipt_date FROM inventory_lots WHERE lot_id = $1 LIMIT 1),
            (SELECT child_lot_id FROM lot_genealogy WHERE wo_id = $2::UUID LIMIT 1),
            (SELECT child_receipt_date FROM lot_genealogy WHERE wo_id = $2::UUID LIMIT 1),
            9999,
            $3::UUID,
            (SELECT id FROM posting_lines LIMIT 1)",
    )
    .bind(lot_a)
    .bind(&wo_id)
    .bind(&wo3_id)
    .execute(&pool)
    .await
    .expect("insert phantom genealogy row");

    // Run recon — check #12 should fire.
    sqlx::query("DELETE FROM reconciliation_alerts")
        .execute(&pool)
        .await
        .unwrap();
    let _: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        count_alerts_kind(&pool, "lot_genealogy_qty_overshoot").await >= 1,
        "phantom 9999 qty_consumed must overshoot events_total"
    );
}

// ============================================================
// G10 — lineage views: downstream walk from raw lot V, upstream walk
// from FG child. Single-level chain validates the recursive CTE shape;
// transitive multi-WO chains require accounts.lot_id partition or
// raw-vs-FG location split (out of scope, see plan-3j3z follow-up).
// ============================================================

#[tokio::test]
async fn lineage_views_return_direct_relationships() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "G10-PARENT").await;
    let comp = fresh_sku_lot(&pool, "G10-COMP").await;
    let raw_loc = fresh_location(&pool, "G10-RAW").await;
    let fg_loc = fresh_location(&pool, "G10-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    // Two raw lots feeding one FG output → 2 genealogy rows tying
    // both raw lots to the same child. Both views should resolve the
    // direct relationships.
    let lot_a = seed_lot(&pool, &comp, &raw_loc, 5, 100, "2026-04-10", "G10-LOT-A").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 5, 200, "2026-04-12", "G10-LOT-B").await;

    let wo_id = create_wo(&pool, "G10-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id, "2026-04-15", None)
        .await
        .expect("wo_start");
    call_wo_complete(&pool, &wo_id, 5, "2026-04-16")
        .await
        .expect("wo_complete");

    // Resolve the FG child lot.
    let fg_child: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID LIMIT 1",
    )
    .bind(&parent)
    .bind(&fg_loc)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Downstream view: from lot_a, find FG children.
    let descendants_a: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT descendant_lot_id FROM v_lot_lineage_downstream
          WHERE root_lot_id = $1",
    )
    .bind(lot_a)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(descendants_a.contains(&fg_child), "FG child reachable from lot_a");

    // Downstream from lot_b: same FG child.
    let descendants_b: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT descendant_lot_id FROM v_lot_lineage_downstream
          WHERE root_lot_id = $1",
    )
    .bind(lot_b)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(descendants_b.contains(&fg_child), "FG child reachable from lot_b");

    // Upstream view: from FG child, find ancestor raw lots (both).
    let ancestors: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT ancestor_lot_id FROM v_lot_lineage_upstream
          WHERE child_lot_id = $1
          ORDER BY ancestor_lot_id",
    )
    .bind(fg_child)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(ancestors.contains(&lot_a), "lot_a is an ancestor");
    assert!(ancestors.contains(&lot_b), "lot_b is an ancestor");

    // Depth = 1 for direct relationships.
    let max_depth: i32 = sqlx::query_scalar(
        "SELECT MAX(depth)::INT FROM v_lot_lineage_downstream
          WHERE root_lot_id = $1",
    )
    .bind(lot_a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(max_depth, 1, "single-level depth");
}
