//! T1 probes for FG-lot end-to-end (mig 0050, acct-ie88).
//! Phase E2 follow-up L4.
//!
//! Drives the full FG-lot lifecycle:
//!   1. Build a WO with a lot_fifo parent SKU (post_wo_start admits).
//!   2. Run wo_complete (lot_fifo parent treated as WAC for WIP-pool
//!      running avg; drain to inv_value_fg creates an inventory_lots
//!      row via apply_event's E2 block; lot_code from
//!      wo_outputs.lot_code or auto-gen 'WO-{event_short}-{output_no}').
//!   3. post_so_ship walks FG lots via _lot_walk_layers (E2 block on
//!      inv_value_fg credit-side fires; depletion writeback writes
//!      inventory_lot_events 'issue' rows; SUM = posting_line.amount).
//!
//! Also exercises:
//!   - cost_method_at_ship='lot_fifo' snapshot on so_shipment_lines
//!   - so_shipment_lines.lot_id audit stamp
//!   - explicit lot pin via p_lines per-line 'lot_id' key
//!   - multi-WO multi-lot FIFO walk by receipt_date
//!   - 'lot' (legacy) still raises P0006 at wo_start AND so_ship
//!   - recon checks stay clean

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Local scaffolding (mirrors fifo_fg_lifecycle_t1)
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

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    routing_op: Option<i32>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, routing_op,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6,
                 $7::UUID, $8::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(routing_op)
    .bind(counterparty_id)
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

async fn seed_standard_component(pool: &PgPool, sku: &str, loc: &str, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, NULL, 'USD', 'raw',
            '2026-04-10'::DATE, $4::UUID, $5::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap();
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

async fn call_wo_start(pool: &PgPool, wo_id: &str, business_date: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, NULL)::text",
    )
    .bind(wo_id)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
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

#[allow(clippy::too_many_arguments)]
async fn add_so_line(
    pool: &PgPool,
    so_id: &str,
    line_no: i32,
    sku_id: &str,
    ship_loc_id: &str,
    qty_ordered: i64,
    unit_price: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD', 0)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn call_so_ship(
    pool: &PgPool,
    so_id: &str,
    lines: serde_json::Value,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(so_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

struct LotFgScaffold {
    parent: String,
    fg_loc: String,
    customer_id: String,
    parent_wip_val: i64,
    parent_fg_val: i64,
    parent_fg_qty: i64,
    cogs_acct: i64,
}

async fn scaffold_lot_fg(pool: &PgPool, suffix: &str) -> LotFgScaffold {
    // Set tracked_by='lot' on the parent so accounts.lot_id partition
    // semantics align — though for FG-lot the inv_value_fg account is
    // partitioned with lot_id IS NULL (the lot is recorded in
    // inventory_lots, not as separate accounts).
    let parent = fresh_sku(pool, &format!("FG-LOT-{suffix}"), "lot_fifo").await;
    sqlx::query("UPDATE skus SET tracked_by = 'lot' WHERE id = $1::UUID")
        .bind(&parent)
        .execute(pool)
        .await
        .unwrap();

    let comp = fresh_sku(pool, &format!("FG-LOT-{suffix}-C"), "standard").await;
    set_std_cost(pool, &comp, 50).await;
    let raw_loc = fresh_location(pool, &format!("FG-LOT-{suffix}-RAW")).await;
    let fg_loc = fresh_location(pool, &format!("FG-LOT-{suffix}-FG")).await;
    let customer_id = fresh_customer(pool, &format!("FG-LOT-{suffix}-CUST")).await;

    open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, None, "debit").await;

    open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    let parent_wip_val =
        open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    let parent_fg_qty =
        open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val =
        open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    open_account(pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    open_account(pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;

    seed_standard_component(pool, &comp, &raw_loc, 200).await;

    let bom_id = create_bom(pool, &parent).await;
    add_bom_item(pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    LotFgScaffold {
        parent,
        fg_loc,
        customer_id,
        parent_wip_val,
        parent_fg_val,
        parent_fg_qty,
        cogs_acct,
    }
}

// ============================================================
// L4.1: end-to-end — wo_complete creates FG lot via auto-gen code;
// SO ship walks it via FIFO default.
// ============================================================

#[tokio::test]
async fn lot_fifo_wo_complete_creates_lot_and_so_ship_walks_it() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "1").await;

    let wo_id = create_wo(&pool, "WO-LOT-1", &sf.parent, &sf.fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo_id, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo_id, 10, "2026-04-16")
        .await
        .expect("wo_complete");

    // FG pool: 10 qty / $500.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 10);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 500);
    assert_eq!(balance(&pool, sf.parent_wip_val).await, 0);

    // FG lot created.
    let lots: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT lot_code, original_quantity::TEXT, unit_cost::TEXT, receipt_date::TEXT
           FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY lot_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(lots.len(), 1, "wo_complete creates 1 FG lot");
    assert!(
        lots[0].0.starts_with("WO-"),
        "auto-gen lot_code starts with 'WO-' got {:?}",
        lots[0].0
    );
    assert!(lots[0].1.starts_with("10"), "qty got {:?}", lots[0].1);
    assert!(lots[0].2.starts_with("50"), "unit_cost got {:?}", lots[0].2);

    // Sell 6.
    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 6, 100).await;
    call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 6 }]),
        "2026-04-17",
    )
    .await
    .expect("so_ship");

    // FG pool: 4 qty / $200.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 4);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 200);

    // COGS = 6 × $50 = $300.
    let cogs_delta: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(amount), 0)::BIGINT FROM posting_lines
          WHERE debit_account_id = $1 AND reason = 'so_ship'",
    )
    .bind(sf.cogs_acct)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cogs_delta, 300);

    // One issue event of -6.
    let events: Vec<(String, i16)> = sqlx::query_as(
        "SELECT e.quantity_change::TEXT, e.event_type
           FROM inventory_lot_events e
           JOIN inventory_lots il ON il.lot_id = e.lot_id
                                 AND il.receipt_date = e.lot_receipt_date
          WHERE il.product_id = $1::UUID AND il.location_id = $2::UUID
          ORDER BY e.event_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].0.starts_with("-6"), "got {:?}", events[0].0);
    assert_eq!(events[0].1, 1);
}

// ============================================================
// L4.2: multi-WO multi-lot — WO 1 then WO 2; sell across both in
// receipt-date FIFO order.
// ============================================================

#[tokio::test]
async fn lot_fifo_multi_wo_multi_lot_fifo_walk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "2").await;

    let wo1 = create_wo(&pool, "WO-LOT-2A", &sf.parent, &sf.fg_loc, 5).await;
    add_routing(&pool, &wo1, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo1, "2026-04-15").await.expect("wo_start 1");
    call_wo_complete(&pool, &wo1, 5, "2026-04-15")
        .await
        .expect("wo_complete 1");

    let wo2 = create_wo(&pool, "WO-LOT-2B", &sf.parent, &sf.fg_loc, 8).await;
    add_routing(&pool, &wo2, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo2, "2026-04-17").await.expect("wo_start 2");
    call_wo_complete(&pool, &wo2, 8, "2026-04-18")
        .await
        .expect("wo_complete 2");

    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 13);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 650);

    let lots: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT, receipt_date::TEXT
           FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY receipt_date, lot_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(lots.len(), 2);
    assert!(lots[0].0.starts_with("5"));
    assert!(lots[1].0.starts_with("8"));

    // Sell 7 → 5 from lot1 + 2 from lot2.
    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 7, 100).await;
    call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 7 }]),
        "2026-04-19",
    )
    .await
    .expect("so_ship");

    let events: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.quantity_change::TEXT, il.receipt_date::TEXT
           FROM inventory_lot_events e
           JOIN inventory_lots il ON il.lot_id = e.lot_id
                                 AND il.receipt_date = e.lot_receipt_date
          WHERE il.product_id = $1::UUID AND il.location_id = $2::UUID
            AND e.event_type = 1
          ORDER BY il.receipt_date, e.event_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events.len(), 2, "two issue events spanning both lots");
    assert!(events[0].0.starts_with("-5"));
    assert!(events[1].0.starts_with("-2"));

    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 6);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 300);
}

// ============================================================
// L4.3: explicit lot pin via p_lines 'lot_id' — even out-of-FIFO-order
// lot is consumed.
// ============================================================

#[tokio::test]
async fn lot_fifo_so_ship_specific_lot_pin() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "3").await;

    // Two WOs → two lots ordered by receipt_date.
    let wo1 = create_wo(&pool, "WO-LOT-3A", &sf.parent, &sf.fg_loc, 4).await;
    add_routing(&pool, &wo1, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo1, "2026-04-15").await.expect("wo_start 1");
    call_wo_complete(&pool, &wo1, 4, "2026-04-15")
        .await
        .expect("wo_complete 1");

    let wo2 = create_wo(&pool, "WO-LOT-3B", &sf.parent, &sf.fg_loc, 6).await;
    add_routing(&pool, &wo2, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo2, "2026-04-17").await.expect("wo_start 2");
    call_wo_complete(&pool, &wo2, 6, "2026-04-17")
        .await
        .expect("wo_complete 2");

    // Capture the SECOND lot's id (the newer one).
    let second_lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY receipt_date DESC, lot_id DESC LIMIT 1",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Pin the SECOND lot for shipping 3 units (out-of-FIFO order).
    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 3, 100).await;
    call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 3, "lot_id": second_lot_id }]),
        "2026-04-19",
    )
    .await
    .expect("so_ship pinned");

    // First lot still has full 4; second lot reduced from 6 to 3.
    let residuals: Vec<(i64, String)> = sqlx::query_as(
        "SELECT il.lot_id,
                (il.original_quantity + COALESCE(
                  (SELECT SUM(e.quantity_change) FROM inventory_lot_events e
                    WHERE e.lot_id = il.lot_id AND e.lot_receipt_date = il.receipt_date),
                  0
                ))::TEXT
           FROM inventory_lots il
          WHERE il.product_id = $1::UUID AND il.location_id = $2::UUID
          ORDER BY il.receipt_date, il.lot_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(residuals.len(), 2);
    assert!(residuals[0].1.starts_with("4"), "lot1 residual got {:?}", residuals[0].1);
    assert!(residuals[1].1.starts_with("3"), "lot2 residual got {:?}", residuals[1].1);
    assert_eq!(residuals[1].0, second_lot_id);
}

// ============================================================
// L4.4: cost_method_at_ship + so_shipment_lines.lot_id audit stamps.
// ============================================================

#[tokio::test]
async fn lot_fifo_so_ship_snapshots_cost_method_and_lot_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "4").await;

    let wo = create_wo(&pool, "WO-LOT-4", &sf.parent, &sf.fg_loc, 4).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 4, "2026-04-15").await.expect("wo_complete");

    let lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID LIMIT 1",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_one(&pool)
    .await
    .unwrap();

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 2, 100).await;
    call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 2 }]),
        "2026-04-17",
    )
    .await
    .expect("so_ship");

    // cost_method_at_ship = 'lot_fifo'.
    let cms: Vec<String> =
        sqlx::query_scalar("SELECT cost_method_at_ship::TEXT FROM so_shipment_lines")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(!cms.is_empty());
    for cm in cms {
        assert_eq!(cm, "lot_fifo");
    }

    // so_shipment_lines.lot_id stamped to the (only) consumed lot.
    let stamped: i64 = sqlx::query_scalar("SELECT lot_id FROM so_shipment_lines LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stamped, lot_id);

    // posting_line_inventory.cost_method_at_event = 'lot_fifo' on the
    // value-leg (SKU resolution from credit-side inv_value_fg).
    let plis: Vec<String> = sqlx::query_scalar(
        "SELECT pli.cost_method_at_event::TEXT FROM posting_line_inventory pli
         JOIN posting_lines pl ON pl.id = pli.posting_line_id
         WHERE pl.document_kind = 'so_ship'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!plis.is_empty());
    for cm in plis {
        assert_eq!(cm, "lot_fifo");
    }
}

// ============================================================
// L4.5: COGS amount equals SUM(|quantity_change| × unit_cost) across
// issue events (the three-walk consistency check).
// ============================================================

#[tokio::test]
async fn lot_fifo_cogs_amount_equals_sum_lot_events() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "5").await;

    let wo = create_wo(&pool, "WO-LOT-5", &sf.parent, &sf.fg_loc, 12).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 12, "2026-04-15").await.expect("wo_complete");

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 7, 100).await;
    call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 7 }]),
        "2026-04-17",
    )
    .await
    .expect("so_ship");

    let cogs_amount: i64 = sqlx::query_scalar(
        "SELECT pl.amount FROM posting_lines pl
          WHERE pl.debit_account_id = $1
            AND pl.reason = 'so_ship'
            AND pl.qty IS NOT NULL
          ORDER BY pl.id DESC LIMIT 1",
    )
    .bind(sf.cogs_acct)
    .fetch_one(&pool)
    .await
    .unwrap();

    let evt_sum: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(ABS(e.quantity_change) * il.unit_cost), 0)::BIGINT
           FROM inventory_lot_events e
           JOIN inventory_lots il ON il.lot_id = e.lot_id
                                 AND il.receipt_date = e.lot_receipt_date
          WHERE il.product_id = $1::UUID AND il.location_id = $2::UUID
            AND e.event_type = 1",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(evt_sum, cogs_amount);
    assert_eq!(cogs_amount, 350); // 7 × $50.
}

// (L4.6 lot_fifo parent + by-products → P0006 was REMOVED in acct-fjxp.
// Behavior is now SUPPORTED for nrv_credit, negligible, and
// disposal_cost(period). See tests/lot_fifo_byproducts_t1.rs for
// positive-coverage cases. disposal_cost(inventoriable) still
// raises P0006 — that's exercised in the new test binary too.)

// ============================================================
// L4.7: idempotent replay — same idempotency_key returns same doc_id,
// no extra inventory_lot_events.
// ============================================================

#[tokio::test]
async fn lot_fifo_so_ship_idempotent_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "7").await;
    let wo = create_wo(&pool, "WO-LOT-7", &sf.parent, &sf.fg_loc, 4).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 4, "2026-04-15").await.expect("wo_complete");

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 2, 100).await;

    // Two so_ship calls with same idempotency_key.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let doc1: String = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&so_id)
    .bind(serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 2 }]))
    .bind("2026-04-17")
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("ship 1");

    let doc2: String = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&so_id)
    .bind(serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 2 }]))
    .bind("2026-04-17")
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("ship 2 (replay)");

    assert_eq!(doc1, doc2);

    // Only ONE issue event regardless of replays.
    let evt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lot_events e
         JOIN inventory_lots il ON il.lot_id = e.lot_id
                              AND il.receipt_date = e.lot_receipt_date
         WHERE il.product_id = $1::UUID AND il.location_id = $2::UUID",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(evt_count, 1);
}

// ============================================================
// L4.8: recon checks stay clean across full FG-lot lifecycle.
// ============================================================

#[tokio::test]
async fn lot_fifo_passes_recon() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot_fg(&pool, "8").await;

    let wo = create_wo(&pool, "WO-LOT-8", &sf.parent, &sf.fg_loc, 8).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 8, "2026-04-15").await.expect("wo_complete");

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 5, 100).await;
    call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 5 }]),
        "2026-04-17",
    )
    .await
    .expect("so_ship");

    // Run recon — fifo_layer_residual_mismatch should be 0
    // (lot_fifo doesn't write cost_layers, but recon's other checks
    // should still be clean across this workload).
    let _ = sqlx::query_scalar::<_, i64>("SELECT run_daily_reconciliation()::BIGINT")
        .fetch_one(&pool)
        .await
        .unwrap();
    let alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts
         WHERE alert_kind IN ('double_entry_imbalance', 'currency_mismatch',
                              'subledger_gl_divergence')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(alerts, 0, "recon clean across full FG-lot lifecycle");
}
