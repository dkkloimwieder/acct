//! T1 probes for wo_lot_consumption (mig 0058, acct-vohc).
//!
//! Drives lot_fifo components through post_wo_start + post_op_move
//! and asserts the per-routing-op consumption persistence layer:
//!
//!   V1 — pinned single-lot consumption at wo_start writes one row.
//!   V2 — unpinned multi-lot FIFO walk writes N rows tied to ONE
//!        rm_issue_to_wo value-leg posting_line.
//!   V3 — mixed lot_fifo + standard components: only lot_fifo
//!        writes wo_lot_consumption.
//!   V4 — op_arrival on routing_op > first_op (post_op_move) stamps
//!        routing_op = the destination op, not first_op.
//!   V5 — idempotent replay of post_wo_start does not create
//!        duplicate wo_lot_consumption rows.
//!   V6 — recon check #11 forward + inverse: clean post-WO yields
//!        zero alerts.
//!   V7 — lineage query: SELECT DISTINCT lot_id FROM wo_lot_consumption
//!        WHERE wo_id=$1 returns the expected lots.
//!   V8 — multi-WO interleave: each WO's consumption isolated.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffolding
// ============================================================

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

async fn fresh_sku_standard_component(pool: &PgPool, code: &str) -> String {
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

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Seed a lot via post_inventory_adjustment (+qty, lot_metadata.lot_code).
/// Returns the new lot_id.
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

async fn call_wo_start_replay(
    pool: &PgPool,
    wo_id: &str,
    business_date: &str,
    lot_pins: Option<serde_json::Value>,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, $5)::text",
    )
    .bind(wo_id)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .bind(lot_pins)
    .fetch_one(pool)
    .await
}

async fn call_op_move(
    pool: &PgPool,
    wo_id: &str,
    from_op: i32,
    to_op: i32,
    qty: i64,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_op_move($1::UUID, $2, $3, $4, $5::DATE, $6::UUID, $7::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(from_op)
    .bind(to_op)
    .bind(qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

/// Open per-routing-op stock_wip + inv_value_wip accounts for a parent.
async fn open_parent_wip_ops(pool: &PgPool, parent: &str, ops: &[i32]) {
    for &op in ops {
        let _ = open_account(pool, "stock_wip", "qty", None, Some(parent), None, Some(op), "debit").await;
        let _ = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(parent), None, Some(op), "debit").await;
    }
}

async fn open_parent_fg(pool: &PgPool, parent: &str, fg_loc: &str) {
    let _ = open_account(pool, "stock_available", "qty", None, Some(parent), Some(fg_loc), None, "debit").await;
    let _ = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(parent), Some(fg_loc), None, "debit").await;
}

/// Open lot_fifo component accounts (stock_available + inv_value_raw +
/// stock_consumed) at the given location.
async fn open_lot_component(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let qty_acct = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let val_acct = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _ = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
    (qty_acct, val_acct)
}

async fn open_standard_component(pool: &PgPool, comp: &str, raw_loc: &str) -> (i64, i64) {
    let qty_acct = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let val_acct = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _ = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
    (qty_acct, val_acct)
}

async fn seed_standard_qty(pool: &PgPool, sku: &str, loc: &str, qty: i64, business_date: &str) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, NULL, 'USD', 'raw',
            $4::DATE, $5::UUID, $6::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap();
}

async fn count_consumption(pool: &PgPool, wo_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM wo_lot_consumption WHERE wo_id = $1::UUID")
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

// ============================================================
// V1 — pinned single-lot consumption at wo_start.
// ============================================================

#[tokio::test]
async fn pinned_single_lot_consumption_writes_one_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V1-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V1-COMP").await;
    let raw_loc = fresh_location(&pool, "V1-RAW").await;
    let fg_loc = fresh_location(&pool, "V1-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let (_, comp_val) = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_id = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "LOT-V1").await;
    assert_eq!(balance(&pool, comp_val).await, 5000);

    let wo_id = create_wo(&pool, "V1-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    // qty_per_parent=2, qty_target=5 -> adj_qty=10. Pin LOT-V1.
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_id });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("pinned wo_start");

    // Exactly ONE wo_lot_consumption row.
    assert_eq!(count_consumption(&pool, &wo_id).await, 1);

    let row: (String, i32, String, i64, String, i64) = sqlx::query_as(
        "SELECT wo_id::text, routing_op, component_sku_id::text, lot_id, qty::text, posting_line_id
           FROM wo_lot_consumption WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, wo_id);
    assert_eq!(row.1, 10, "routing_op stamps the fire op (first_op)");
    assert_eq!(row.2, comp);
    assert_eq!(row.3, lot_id);
    assert_eq!(row.4.split('.').next().unwrap(), "10");
    assert!(row.5 > 0, "posting_line_id resolved");

    // posting_line_id matches the rm_issue_to_wo VALUE-leg posting.
    let pl_amount: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE id = $1",
    )
    .bind(row.5)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pl_amount, 1000, "value-leg amount: 10 * 100");
}

// ============================================================
// V2 — unpinned multi-lot FIFO walk writes N rows for ONE posting.
// ============================================================

#[tokio::test]
async fn unpinned_multi_lot_walk_writes_n_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V2-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V2-COMP").await;
    let raw_loc = fresh_location(&pool, "V2-RAW").await;
    let fg_loc = fresh_location(&pool, "V2-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let (_, comp_val) = open_lot_component(&pool, &comp, &raw_loc).await;

    // Two lots; receipt_date dictates FIFO order.
    let lot_a = seed_lot(&pool, &comp, &raw_loc, 4, 100, "2026-04-10", "LOT-A").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 6, 200, "2026-04-12", "LOT-B").await;
    assert_eq!(balance(&pool, comp_val).await, 4 * 100 + 6 * 200); // 1600

    let wo_id = create_wo(&pool, "V2-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    // qty_per_parent=1, qty_target=5 -> adj_qty=5. Spans both: 4 from A + 1 from B.
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    // No pin -> FIFO walk.
    call_wo_start(&pool, &wo_id, "2026-04-15", None)
        .await
        .expect("unpinned wo_start");

    // Expect TWO rows in wo_lot_consumption.
    assert_eq!(count_consumption(&pool, &wo_id).await, 2);

    let rows: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT lot_id, qty::text, posting_line_id
           FROM wo_lot_consumption
          WHERE wo_id = $1::UUID
          ORDER BY lot_id",
    )
    .bind(&wo_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(rows[0].0, lot_a, "lot_a (FIFO first) recorded");
    assert_eq!(rows[0].1.split('.').next().unwrap(), "4", "lot_a fully drained: 4");
    assert_eq!(rows[1].0, lot_b);
    assert_eq!(rows[1].1.split('.').next().unwrap(), "1", "lot_b partial: 1");

    // Both rows tied to the SAME posting_line_id (one rm_issue value-leg).
    assert_eq!(rows[0].2, rows[1].2, "same posting_line_id for multi-lot walk");

    // SUM(qty) = adj_qty (5).
    let sum_qty: String = sqlx::query_scalar(
        "SELECT SUM(qty)::TEXT FROM wo_lot_consumption WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sum_qty.split('.').next().unwrap(), "5");

    // posting_line.amount = 4*100 + 1*200 = 600.
    let pl_amount: i64 = sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
        .bind(rows[0].2)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(pl_amount, 600);
}

// ============================================================
// V3 — mixed lot_fifo + standard components: only lot_fifo writes.
// ============================================================

#[tokio::test]
async fn mixed_components_only_lot_fifo_writes() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V3-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp_lot = fresh_sku_lot(&pool, "V3-LOT").await;
    let comp_std = fresh_sku_standard_component(&pool, "V3-STD").await;
    set_std_cost(&pool, &comp_std, 50).await;
    let raw_loc = fresh_location(&pool, "V3-RAW").await;
    let fg_loc = fresh_location(&pool, "V3-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let _ = open_lot_component(&pool, &comp_lot, &raw_loc).await;
    let _ = open_standard_component(&pool, &comp_std, &raw_loc).await;

    seed_lot(&pool, &comp_lot, &raw_loc, 10, 100, "2026-04-10", "LOT-V3").await;
    seed_standard_qty(&pool, &comp_std, &raw_loc, 20, "2026-04-10").await;

    let wo_id = create_wo(&pool, "V3-WO", &parent, &fg_loc, 3).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp_lot, &raw_loc, 1).await;
    add_bom_item(&pool, bom_id, 2, 10, &comp_std, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id, "2026-04-15", None)
        .await
        .expect("mixed wo_start");

    // Only lot_fifo component writes wo_lot_consumption.
    assert_eq!(count_consumption(&pool, &wo_id).await, 1);

    let comp_id: String = sqlx::query_scalar(
        "SELECT component_sku_id::text FROM wo_lot_consumption WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(comp_id, comp_lot, "only the lot_fifo component is recorded");
}

// ============================================================
// V4 — op_arrival on routing_op > first_op (post_op_move) stamps
// routing_op = the destination op.
// ============================================================

#[tokio::test]
async fn op_arrival_post_op_move_stamps_destination_op() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V4-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V4-COMP").await;
    let raw_loc = fresh_location(&pool, "V4-RAW").await;
    let fg_loc = fresh_location(&pool, "V4-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10, 20]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let _ = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_id = seed_lot(&pool, &comp, &raw_loc, 30, 100, "2026-04-10", "LOT-V4").await;

    let wo_id = create_wo(&pool, "V4-WO", &parent, &fg_loc, 4).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    add_routing(&pool, &wo_id, 20, "ASSEMBLE").await;

    let bom_id = create_bom(&pool, &parent).await;
    // Component fires at op 20 (op_arrival), not op 10.
    add_bom_item(&pool, bom_id, 1, 20, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_id });

    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins.clone()))
        .await
        .expect("wo_start");

    // wo_start fires only first-op items (none in this BOM); no consumption yet.
    assert_eq!(
        count_consumption(&pool, &wo_id).await,
        0,
        "no consumption at wo_start (BOM line applies_at_op=20)"
    );

    // op_move 10 -> 20 fires op_arrival items at op 20.
    call_op_move(&pool, &wo_id, 10, 20, 4, "2026-04-16")
        .await
        .expect("op_move 10->20");

    assert_eq!(count_consumption(&pool, &wo_id).await, 1);

    let routing_op: i32 = sqlx::query_scalar(
        "SELECT routing_op FROM wo_lot_consumption WHERE wo_id = $1::UUID",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(routing_op, 20, "stamps DEST op_arrival, not first_op");
}

// ============================================================
// V5 — idempotent replay of post_wo_start.
// ============================================================

#[tokio::test]
async fn idempotent_replay_does_not_duplicate_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V5-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V5-COMP").await;
    let raw_loc = fresh_location(&pool, "V5-RAW").await;
    let fg_loc = fresh_location(&pool, "V5-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let _ = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_id = seed_lot(&pool, &comp, &raw_loc, 20, 100, "2026-04-10", "LOT-V5").await;

    let wo_id = create_wo(&pool, "V5-WO", &parent, &fg_loc, 3).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_id });
    let idem = fresh_uuid(&pool).await;

    call_wo_start_replay(&pool, &wo_id, "2026-04-15", Some(pins.clone()), &idem)
        .await
        .expect("first wo_start");
    let first_count = count_consumption(&pool, &wo_id).await;
    assert_eq!(first_count, 1);

    // Replay with the SAME idempotency_key: short-circuits before batching.
    call_wo_start_replay(&pool, &wo_id, "2026-04-15", Some(pins), &idem)
        .await
        .expect("replay wo_start");
    let after_count = count_consumption(&pool, &wo_id).await;
    assert_eq!(after_count, 1, "replay does not add rows");
}

// ============================================================
// V6 — recon check #11 forward + inverse: clean post-WO yields
// zero alerts.
// ============================================================

#[tokio::test]
async fn recon_check_11_clean_after_lot_consumption() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V6-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V6-COMP").await;
    let raw_loc = fresh_location(&pool, "V6-RAW").await;
    let fg_loc = fresh_location(&pool, "V6-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let _ = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_id = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "LOT-V6").await;

    let wo_id = create_wo(&pool, "V6-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_id });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");

    // Run recon and assert both check-11 flavors are zero.
    sqlx::query("SELECT run_daily_reconciliation()")
        .execute(&pool)
        .await
        .unwrap();

    let forward = count_alerts_kind(&pool, "wo_lot_consumption_orphan_forward").await;
    let inverse = count_alerts_kind(&pool, "wo_lot_consumption_orphan_inverse").await;

    assert_eq!(forward, 0, "no forward orphans after clean WO");
    assert_eq!(inverse, 0, "no inverse orphans after clean WO");
}

// ============================================================
// V6b — recon check #11 inverse fires when wo_lot_consumption qty
// disagrees with inventory_lot_events.quantity_change.
// (Defensive net for future code drift; doesn't normally trigger.)
// ============================================================

#[tokio::test]
async fn recon_check_11_inverse_fires_on_qty_drift() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V6b-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V6b-COMP").await;
    let raw_loc = fresh_location(&pool, "V6b-RAW").await;
    let fg_loc = fresh_location(&pool, "V6b-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let _ = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_id = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "LOT-V6b").await;

    let wo_id = create_wo(&pool, "V6b-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let pins = json!({ comp.to_string(): lot_id });
    call_wo_start(&pool, &wo_id, "2026-04-15", Some(pins))
        .await
        .expect("wo_start");

    // Synthesize an orphan: insert a wo_lot_consumption row with a qty
    // that disagrees with any inventory_lot_event. Use a manual INSERT
    // on a fresh wo_event-style placeholder.
    // Here we exercise the check by adding a doctored row referencing
    // an existing event/pl but with a different qty.
    let pl_id: i64 = sqlx::query_scalar(
        "SELECT pl.id FROM posting_lines pl
          JOIN accounts a ON a.id = pl.credit_account_id
         WHERE pl.reason = 'rm_issue_to_wo'
           AND a.kind = 'inv_value_raw'
           AND pl.qty IS NOT NULL
         ORDER BY pl.id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let event_id: String = sqlx::query_scalar(
        "SELECT id::text FROM wo_events WHERE wo_id = $1::UUID ORDER BY id DESC LIMIT 1",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Doctor a row with qty=999 (mismatches the lot event's -10).
    sqlx::query(
        "INSERT INTO wo_lot_consumption
            (wo_id, wo_event_id, routing_op, component_sku_id,
             lot_id, lot_receipt_date, qty, posting_line_id)
         VALUES ($1::UUID, $2::UUID, 10, $3::UUID,
                 $4, '2026-04-10'::DATE, 999, $5)",
    )
    .bind(&wo_id)
    .bind(&event_id)
    .bind(&comp)
    .bind(lot_id)
    .bind(pl_id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query("SELECT run_daily_reconciliation()")
        .execute(&pool)
        .await
        .unwrap();

    let inverse = count_alerts_kind(&pool, "wo_lot_consumption_orphan_inverse").await;
    assert!(inverse >= 1, "doctored qty mismatch raises inverse alert");
}

// ============================================================
// V7 — lineage query: SELECT DISTINCT lot_id FROM wo_lot_consumption.
// ============================================================

#[tokio::test]
async fn lineage_query_returns_consumed_lots() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_standard(&pool, "V7-PARENT").await;
    set_std_cost(&pool, &parent, 1000).await;
    let comp = fresh_sku_lot(&pool, "V7-COMP").await;
    let raw_loc = fresh_location(&pool, "V7-RAW").await;
    let fg_loc = fresh_location(&pool, "V7-FG").await;

    open_parent_wip_ops(&pool, &parent, &[10]).await;
    open_parent_fg(&pool, &parent, &fg_loc).await;
    let _ = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 4, 100, "2026-04-10", "LOT-V7A").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 6, 200, "2026-04-12", "LOT-V7B").await;

    let wo_id = create_wo(&pool, "V7-WO", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    call_wo_start(&pool, &wo_id, "2026-04-15", None)
        .await
        .expect("wo_start");

    let lots: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT lot_id FROM wo_lot_consumption WHERE wo_id = $1::UUID ORDER BY lot_id",
    )
    .bind(&wo_id)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(lots, vec![lot_a, lot_b], "lineage query returns both lots");
}

// ============================================================
// V8 — multi-WO interleave: each WO's consumption isolated.
// ============================================================

#[tokio::test]
async fn multi_wo_interleave_isolates_per_wo_consumption() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent_a = fresh_sku_standard(&pool, "V8-PARENT-A").await;
    set_std_cost(&pool, &parent_a, 1000).await;
    let parent_b = fresh_sku_standard(&pool, "V8-PARENT-B").await;
    set_std_cost(&pool, &parent_b, 1000).await;
    let comp = fresh_sku_lot(&pool, "V8-COMP").await;
    let raw_loc = fresh_location(&pool, "V8-RAW").await;
    let fg_loc_a = fresh_location(&pool, "V8-FGA").await;
    let fg_loc_b = fresh_location(&pool, "V8-FGB").await;

    open_parent_wip_ops(&pool, &parent_a, &[10]).await;
    open_parent_wip_ops(&pool, &parent_b, &[10]).await;
    open_parent_fg(&pool, &parent_a, &fg_loc_a).await;
    open_parent_fg(&pool, &parent_b, &fg_loc_b).await;
    let _ = open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 10, 100, "2026-04-10", "LOT-V8A").await;
    let lot_b = seed_lot(&pool, &comp, &raw_loc, 10, 150, "2026-04-12", "LOT-V8B").await;

    // WO-A pins lot_a; WO-B pins lot_b.
    let wo_a = create_wo(&pool, "V8-WOA", &parent_a, &fg_loc_a, 3).await;
    add_routing(&pool, &wo_a, 10, "MILL").await;
    let bom_a = create_bom(&pool, &parent_a).await;
    add_bom_item(&pool, bom_a, 1, 10, &comp, &raw_loc, 2).await;

    let wo_b = create_wo(&pool, "V8-WOB", &parent_b, &fg_loc_b, 2).await;
    add_routing(&pool, &wo_b, 10, "MILL").await;
    let bom_b = create_bom(&pool, &parent_b).await;
    add_bom_item(&pool, bom_b, 1, 10, &comp, &raw_loc, 3).await;

    let pins_a = json!({ comp.to_string(): lot_a });
    let pins_b = json!({ comp.to_string(): lot_b });

    call_wo_start(&pool, &wo_a, "2026-04-15", Some(pins_a))
        .await
        .expect("wo_a");
    call_wo_start(&pool, &wo_b, "2026-04-16", Some(pins_b))
        .await
        .expect("wo_b");

    // WO-A sees only lot_a; WO-B sees only lot_b.
    let lots_a: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT lot_id FROM wo_lot_consumption WHERE wo_id = $1::UUID ORDER BY lot_id",
    )
    .bind(&wo_a)
    .fetch_all(&pool)
    .await
    .unwrap();
    let lots_b: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT lot_id FROM wo_lot_consumption WHERE wo_id = $1::UUID ORDER BY lot_id",
    )
    .bind(&wo_b)
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(lots_a, vec![lot_a]);
    assert_eq!(lots_b, vec![lot_b]);

    // Total rows: one row per WO.
    assert_eq!(count_consumption(&pool, &wo_a).await, 1);
    assert_eq!(count_consumption(&pool, &wo_b).await, 1);
}
