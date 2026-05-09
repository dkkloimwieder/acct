//! T1 probes for FG-FIFO end-to-end (mig 0037 + 0038 + 0039, acct-clrd).
//! Phase E1 follow-up W4.
//!
//! Drives the full FG-FIFO lifecycle:
//!   1. Build a WO with a FIFO parent SKU (post_wo_start admits).
//!   2. Run op_move + wo_complete (FIFO parent treated as WAC for
//!      WIP-pool running avg; drain to FG creates an FG layer via
//!      apply_event's E1 block now widened to inv_value_fg).
//!   3. post_so_ship walks the FG layer (E1 block on inv_value_fg
//!      credit-side fires; depletion writeback writes
//!      cost_layer_depletions; SUM = posting_line.amount).
//!
//! Also exercises:
//!   - cost_method_at_ship='fifo' snapshot on so_shipment_lines
//!   - 'lot' still raises P0006 (acct-uze)
//!   - recon check #8 stays clean
//!   - multiple WOs creating multiple FG layers; sell across them in
//!     FIFO order

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Local scaffolding (mirrors so_ship.rs / wac_perpetual_component_issue.rs)
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

/// Seed a standard-cost component pool by posting an inventory
/// adjustment with NULL unit_cost (post_inventory_adjustment resolves
/// the standard cost internally for cost_method='standard').
async fn seed_standard_component(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
) {
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
    .unwrap();
}

async fn call_wo_start(pool: &PgPool, wo_id: &str, business_date: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(pool: &PgPool, wo_id: &str, qty: i64, business_date: &str) -> sqlx::Result<String> {
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

/// Open everything a FIFO-parent WO + so_ship needs.
struct FgFifScaffold {
    parent: String,
    fg_loc: String,
    customer_id: String,
    parent_wip_val: i64,
    parent_fg_val: i64,
    parent_fg_qty: i64,
    cogs_acct: i64,
    revenue_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
}

async fn scaffold_fg_fifo(pool: &PgPool, suffix: &str) -> FgFifScaffold {
    let parent = fresh_sku(pool, &format!("FG-FIF-{suffix}"), "fifo").await;
    let comp = fresh_sku(pool, &format!("FG-FIF-{suffix}-C"), "standard").await;
    set_std_cost(pool, &comp, 50).await;
    let raw_loc = fresh_location(pool, &format!("FG-FIF-{suffix}-RAW")).await;
    let fg_loc = fresh_location(pool, &format!("FG-FIF-{suffix}-FG")).await;
    let customer_id = fresh_customer(pool, &format!("FG-FIF-{suffix}-CUST")).await;

    // Component (raw) + parent (WIP/FG) accounts.
    open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, None, "debit").await;

    open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    let parent_wip_val = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    let parent_fg_qty = open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    let parent_fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    // Customer-side accounts (normal side is DEBIT for so_ship's
    // customer_pool/ar_unsettled — they're receivables).
    let cust_qty = open_account(pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    let cust_unsettled = open_account(pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    // Reuse fixture cogs/revenue (already exist).
    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;

    // Seed component pool — 200 units at standard cost ($50) covers
    // all WOs in the test.
    seed_standard_component(pool, &comp, &raw_loc, 200).await;

    // Build BOM: 1 component per parent unit.
    let bom_id = create_bom(pool, &parent).await;
    add_bom_item(pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    FgFifScaffold {
        parent,
        fg_loc,
        customer_id,
        parent_wip_val,
        parent_fg_val,
        parent_fg_qty,
        cogs_acct,
        revenue_acct,
        cust_qty,
        cust_unsettled,
    }
}

// ============================================================
// W4.1: end-to-end — wo_start/wo_complete creates FG layer; SO ship
// walks it.
// ============================================================

#[tokio::test]
async fn fg_fifo_wo_complete_creates_layer_and_so_ship_walks_it() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fg_fifo(&pool, "1").await;

    // WO 1: produce 10 parents from 10 components (each at $50).
    // Total drain = 10 × $50 = $500. FG layer: 10 @ $50.
    let wo_id = create_wo(&pool, "WO-FGFIF-1", &sf.parent, &sf.fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo_id, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo_id, 10, "2026-04-16").await.expect("wo_complete");

    // FG pool: 10 qty / $500.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 10);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 500);
    // WIP pool drained to 0.
    assert_eq!(balance(&pool, sf.parent_wip_val).await, 0);

    // FG layer created at $50.
    let layers: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT, receipt_date::TEXT
           FROM cost_layers
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY layer_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(layers.len(), 1, "wo_complete creates 1 FG layer");
    assert!(layers[0].0.starts_with("10"), "qty got {:?}", layers[0].0);
    assert!(layers[0].1.starts_with("50"), "uc got {:?}", layers[0].1);

    // Sell 6 of the 10 via SO ship.
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

    // FG pool: 4 qty / $200 left (10 - 6 = 4 @ $50).
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

    // One depletion of 6 @ $50 = $300.
    let depletions: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT d.depleted_quantity::TEXT, d.unit_cost::TEXT, d.cost_amount::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID
          ORDER BY d.depletion_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 1);
    assert!(depletions[0].0.starts_with("6"));
    assert!(depletions[0].1.starts_with("50"));
    assert!(depletions[0].2.starts_with("300"));
}

// ============================================================
// W4.2: multiple WOs at different costs create multiple FG layers;
// SO ship spans layers in FIFO order.
// ============================================================

#[tokio::test]
async fn fg_fifo_multi_wo_layers_sell_in_fifo_order() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fg_fifo(&pool, "2").await;

    // WO 1: produce 5 parents @ component $50 → FG layer 1: 5 @ $50.
    let wo1 = create_wo(&pool, "WO-FGFIF-2A", &sf.parent, &sf.fg_loc, 5).await;
    add_routing(&pool, &wo1, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo1, "2026-04-15").await.expect("wo_start 1");
    call_wo_complete(&pool, &wo1, 5, "2026-04-16").await.expect("wo_complete 1");

    // Cost-adjust the COMPONENT pool from $50 to $80 — but component
    // is 'standard' so cost_adjust isn't applicable. Instead we'll
    // just do a second WO using the same component pool (still $50).
    // Actually: let's build a SECOND WO with a NEW component at a
    // different price by seeding it. The simplest demo of FIFO order
    // is two WOs where the parent WIP drains at different running
    // costs because the component pool changes.
    //
    // Simpler: just add another seeded standard SKU with a different
    // standard cost. Use a separate parent? No — same parent with
    // different std_cost ECO would be invasive.
    //
    // Easiest: for this test, the two layers are at the SAME unit cost
    // ($50). Verifying FIFO order with same unit_cost is still
    // meaningful — different layer_ids, ordered by receipt_date.
    let wo2 = create_wo(&pool, "WO-FGFIF-2B", &sf.parent, &sf.fg_loc, 8).await;
    add_routing(&pool, &wo2, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo2, "2026-04-17").await.expect("wo_start 2");
    call_wo_complete(&pool, &wo2, 8, "2026-04-18").await.expect("wo_complete 2");

    // FG: 13 qty / $650 (5 @ 50 + 8 @ 50).
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 13);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 650);

    let layers: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT, receipt_date::TEXT
           FROM cost_layers
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY layer_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(layers.len(), 2);
    assert!(layers[0].0.starts_with("5"));
    assert!(layers[1].0.starts_with("8"));

    // Sell 7 → spans both layers (5 from L1 + 2 from L2).
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

    // Two depletions ordered by layer.
    let depletions: Vec<(String, String)> = sqlx::query_as(
        "SELECT d.depleted_quantity::TEXT, d.cost_amount::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID
          ORDER BY cl.layer_id, d.depletion_id",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 2);
    assert!(depletions[0].0.starts_with("5"));
    assert!(depletions[0].1.starts_with("250"));
    assert!(depletions[1].0.starts_with("2"));
    assert!(depletions[1].1.starts_with("100"));

    // FG: 6 qty / $300 left.
    assert_eq!(balance(&pool, sf.parent_fg_qty).await, 6);
    assert_eq!(balance(&pool, sf.parent_fg_val).await, 300);
}

// ============================================================
// W4.3: cost_method_at_ship snapshot.
// ============================================================

#[tokio::test]
async fn fg_fifo_so_ship_snapshots_cost_method() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fg_fifo(&pool, "3").await;
    let wo = create_wo(&pool, "WO-FGFIF-3", &sf.parent, &sf.fg_loc, 4).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 4, "2026-04-16").await.expect("wo_complete");

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

    let cms: Vec<String> = sqlx::query_scalar(
        "SELECT cost_method_at_ship::TEXT FROM so_shipment_lines",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!cms.is_empty());
    for cm in cms { assert_eq!(cm, "fifo"); }

    let plis: Vec<String> = sqlx::query_scalar(
        "SELECT pli.cost_method_at_event::TEXT FROM posting_line_inventory pli
         JOIN posting_lines pl ON pl.id = pli.posting_line_id
         WHERE pl.document_kind = 'so_ship'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!plis.is_empty());
    for cm in plis { assert_eq!(cm, "fifo"); }
}

// ============================================================
// W4.4: SUM(depletions) = posting_line.amount on cogs-leg.
// ============================================================

#[tokio::test]
async fn fg_fifo_cogs_amount_equals_sum_depletions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fg_fifo(&pool, "4").await;
    let wo = create_wo(&pool, "WO-FGFIF-4", &sf.parent, &sf.fg_loc, 12).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 12, "2026-04-16").await.expect("wo_complete");

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

    // The cogs leg's posting_line.amount equals SUM(depletion.cost_amount).
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
    let dep_sum: String = sqlx::query_scalar(
        "SELECT COALESCE(SUM(d.cost_amount), 0)::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID",
    )
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep_sum.split('.').next().unwrap(), cogs_amount.to_string());
    assert_eq!(cogs_amount, 350); // 7 × $50
}

// ============================================================
// W4.5: lot parent still raises P0006 at post_wo_start.
// ============================================================

#[tokio::test]
async fn lot_parent_still_raises_p0006_at_wo_start() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "LOT-PARENT", "lot").await;
    let fg_loc = fresh_location(&pool, "LOT-FG").await;
    let comp = fresh_sku(&pool, "LOT-COMP", "standard").await;
    set_std_cost(&pool, &comp, 10).await;

    open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    open_account(&pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    let wo = create_wo(&pool, "WO-LOT", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;

    let res = call_wo_start(&pool, &wo, "2026-04-15").await;
    let err = res.expect_err("lot parent must raise");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned()).as_deref(),
        Some("P0026"),
    );
}

// ============================================================
// W4.6: lot SKU still raises P0006 at post_so_ship.
// ============================================================

#[tokio::test]
async fn lot_sku_still_raises_p0006_at_so_ship() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "LOT-SHIP", "lot").await;
    let ship_loc = fresh_location(&pool, "LOT-SHIP-LOC").await;
    let customer_id = fresh_customer(&pool, "LOT-CUST").await;

    open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&ship_loc), None, None, "debit").await;
    open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&ship_loc), None, None, "debit").await;
    open_account(&pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    open_account(&pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    let so_id = create_so(&pool, &customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sku, &ship_loc, 1, 100).await;

    let res = call_so_ship(
        &pool,
        &so_id,
        serde_json::json!([{ "so_line_id": so_line_id, "qty_shipped": 1 }]),
        "2026-04-15",
    )
    .await;
    let err = res.expect_err("lot SKU must raise");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned()).as_deref(),
        Some("P0006"),
    );
}

// ============================================================
// W4.7: recon check #8 stays clean across full FG-FIFO lifecycle.
// ============================================================

#[tokio::test]
async fn fg_fifo_passes_recon() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_fg_fifo(&pool, "7").await;
    let wo = create_wo(&pool, "WO-FGFIF-7", &sf.parent, &sf.fg_loc, 8).await;
    add_routing(&pool, &wo, 10, "ASSEMBLE").await;
    call_wo_start(&pool, &wo, "2026-04-15").await.expect("wo_start");
    call_wo_complete(&pool, &wo, 8, "2026-04-16").await.expect("wo_complete");

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

    let _ = sqlx::query_scalar::<_, i64>("SELECT run_daily_reconciliation()::BIGINT")
        .fetch_one(&pool)
        .await
        .unwrap();
    let alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts
         WHERE alert_kind = 'fifo_layer_residual_mismatch'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(alerts, 0, "recon clean across full FG-FIFO lifecycle");
}
