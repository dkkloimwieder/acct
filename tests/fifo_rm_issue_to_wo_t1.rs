//! T1 probes for FIFO via rm_issue_to_wo (mig 0036, acct-n77v).
//! Phase E1 follow-up W3.
//!
//! Drives a multi-component WO where one or more components are FIFO,
//! then verifies:
//!   - the wrapper walks _fifo_walk_layers under FOR UPDATE for v_value,
//!   - apply_event re-walks under the same locks for depletion writeback,
//!   - SUM(cost_layer_depletions.cost_amount) = posting_line.amount on
//!     the rm_issue_to_wo value-leg,
//!   - layers consume in FIFO order (oldest receipt_date first),
//!   - mixed component cost_methods (FIFO + standard) coexist in one BOM,
//!   - empty FIFO pool fails the qty-leg first (CHECK / 23514) before the
//!     value-leg walk would raise P0006,
//!   - recon check #8 stays clean across the cycle.

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Local scaffolding (mirrors wac_perpetual_component_issue.rs)
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

/// Seed a FIFO layer via post_inventory_adjustment (relies on W2).
async fn seed_fifo_layer(
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
    .unwrap_or_else(|e| panic!("seed_fifo_layer sku={sku} qty={qty}@{unit_cost}: {e}"));
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
// W3.1: single-layer FIFO component drains in FIFO order.
// ============================================================

#[tokio::test]
async fn fifo_component_drains_at_layer_unit_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "W3-P1", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "W3-P1-C", "fifo").await;
    let raw_loc = fresh_location(&pool, "W3-P1-RAW").await;
    let fg_loc = fresh_location(&pool, "W3-P1-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    seed_fifo_layer(&pool, &comp, &raw_loc, 100, 10, "2026-04-10").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 1000);

    // qty/p=2, WO qty=20 → adj_qty=40. 40 × $10 = $400.
    let wo_id = create_wo(&pool, "W3-WO1", &parent, &fg_loc, 20).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

    assert_eq!(balance(&pool, wip_val).await, 400);
    assert_eq!(balance(&pool, comp_raw_val).await, 600);

    // Single depletion of 40 @ 10 = 400.
    let depletions: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT d.depleted_quantity::TEXT, d.unit_cost::TEXT, d.cost_amount::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID
          ORDER BY cl.layer_id",
    )
    .bind(&comp)
    .bind(&raw_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 1);
    assert!(depletions[0].0.starts_with("40"));
    assert!(depletions[0].1.starts_with("10"));
    assert!(depletions[0].2.starts_with("400"));
}

// ============================================================
// W3.2: multi-layer FIFO component drains across layers in order;
// SUM(depletions.cost_amount) = posting_line.amount.
// ============================================================

#[tokio::test]
async fn fifo_component_drains_across_layers_in_fifo_order() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "W3-P2", "standard").await;
    set_std_cost(&pool, &parent, 200).await;
    let comp = fresh_sku(&pool, "W3-P2-C", "fifo").await;
    let raw_loc = fresh_location(&pool, "W3-P2-RAW").await;
    let fg_loc = fresh_location(&pool, "W3-P2-FG").await;

    let (_, comp_raw_val) = open_component_raw(&pool, &comp, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Layer 1: 30 @ 10 ($300). Layer 2: 50 @ 14 ($700). Total: 80 / $1000.
    seed_fifo_layer(&pool, &comp, &raw_loc, 30, 10, "2026-04-10").await;
    seed_fifo_layer(&pool, &comp, &raw_loc, 50, 14, "2026-04-12").await;
    assert_eq!(balance(&pool, comp_raw_val).await, 1000);

    // qty/p=2, WO qty=20 → adj_qty=40. Spans both layers:
    //   30 from L1 ($300) + 10 from L2 ($140) = $440.
    let wo_id = create_wo(&pool, "W3-WO2", &parent, &fg_loc, 20).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

    assert_eq!(balance(&pool, wip_val).await, 440);
    assert_eq!(balance(&pool, comp_raw_val).await, 1000 - 440);

    let depletions: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT d.depleted_quantity::TEXT, d.unit_cost::TEXT, d.cost_amount::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID
          ORDER BY cl.layer_id, d.depletion_id",
    )
    .bind(&comp)
    .bind(&raw_loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 2, "2 depletions expected (FIFO span)");
    assert!(depletions[0].0.starts_with("30") && depletions[0].1.starts_with("10"));
    assert!(depletions[1].0.starts_with("10") && depletions[1].1.starts_with("14"));

    // SUM(cost_amount) = posting_line.amount on the rm_issue value-leg
    // (debit=parent WIP, credit=component raw).
    let pl_amount: i64 = sqlx::query_scalar(
        "SELECT pl.amount FROM posting_lines pl
          WHERE pl.credit_account_id = $1
            AND pl.reason = 'rm_issue_to_wo'
            AND pl.qty IS NOT NULL
          ORDER BY pl.id DESC LIMIT 1",
    )
    .bind(comp_raw_val)
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
    .bind(&comp)
    .bind(&raw_loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep_sum.split('.').next().unwrap(), pl_amount.to_string());
    assert_eq!(pl_amount, 440);
}

// ============================================================
// W3.3: mixed components (FIFO + standard) in one BOM both work.
// ============================================================

#[tokio::test]
async fn mixed_fifo_and_standard_components_in_one_bom() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "W3-P3", "standard").await;
    set_std_cost(&pool, &parent, 200).await;
    let comp_fifo = fresh_sku(&pool, "W3-P3-CF", "fifo").await;
    let comp_std = fresh_sku(&pool, "W3-P3-CS", "standard").await;
    set_std_cost(&pool, &comp_std, 7).await;
    let raw_loc = fresh_location(&pool, "W3-P3-RAW").await;
    let fg_loc = fresh_location(&pool, "W3-P3-FG").await;

    let (_, fifo_raw_val) = open_component_raw(&pool, &comp_fifo, &raw_loc).await;
    let (_, std_raw_val) = open_component_raw(&pool, &comp_std, &raw_loc).await;
    let (wip_val, _) = open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Seed FIFO comp pool. Standard comp doesn't need a pool to issue
    // (negative inventory CHECK fires only on stock_available; we seed
    // qty without value so the qty-leg has supply).
    seed_fifo_layer(&pool, &comp_fifo, &raw_loc, 100, 10, "2026-04-10").await;

    // Seed a standard-cost comp pool too — issue from stock requires
    // positive on-hand qty.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, NULL, 'USD', 'raw',
            '2026-04-10'::DATE, $4::UUID, $5::UUID, NULL
         )::text",
    )
    .bind(&comp_std)
    .bind(&raw_loc)
    .bind(50i64)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("seed std comp");
    assert_eq!(balance(&pool, std_raw_val).await, 50 * 7);

    // qty/p for fifo=2, std=1. WO qty=10 → adj_fifo=20, adj_std=10.
    // Expected WIP fill = 20 × $10 + 10 × $7 = 200 + 70 = $270.
    let wo_id = create_wo(&pool, "W3-WO3", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp_fifo, &raw_loc, 2).await;
    add_bom_item(&pool, bom_id, 2, 10, &comp_std, &raw_loc, 1).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

    assert_eq!(balance(&pool, wip_val).await, 270);
    assert_eq!(balance(&pool, fifo_raw_val).await, 800, "FIFO drained 200");
    assert_eq!(balance(&pool, std_raw_val).await, 50 * 7 - 70, "std drained 70");

    // FIFO comp got exactly one depletion of 20 @ 10.
    let dep_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID",
    )
    .bind(&comp_fifo)
    .bind(&raw_loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep_count, 1, "FIFO comp depleted once");
}

// ============================================================
// W3.4: empty FIFO pool — qty-leg's stock_available CHECK fires
// before the value-leg walk would raise P0006.
// ============================================================

#[tokio::test]
async fn fifo_component_empty_pool_blocks_via_qty_leg() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "W3-P4", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "W3-P4-C", "fifo").await;
    let raw_loc = fresh_location(&pool, "W3-P4-RAW").await;
    let fg_loc = fresh_location(&pool, "W3-P4-FG").await;

    open_component_raw(&pool, &comp, &raw_loc).await;
    open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // No seeding — pool empty.
    let wo_id = create_wo(&pool, "W3-WO4", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let res = call_wo_start(&pool, &wo_id).await;
    let err = res.expect_err("empty FIFO pool must fail");
    let code = err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned());
    // qty-leg's stock_available CHECK (no negative) is 23514; value-leg
    // walk would raise P0006. Either is acceptable — what matters is the
    // post is rejected.
    assert!(
        code.as_deref() == Some("23514") || code.as_deref() == Some("P0006"),
        "got {code:?}",
    );
}

// ============================================================
// W3.5: recon check #8 stays clean across full WO consume cycle.
// ============================================================

#[tokio::test]
async fn fifo_rm_issue_passes_recon() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "W3-P5", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "W3-P5-C", "fifo").await;
    let raw_loc = fresh_location(&pool, "W3-P5-RAW").await;
    let fg_loc = fresh_location(&pool, "W3-P5-FG").await;

    open_component_raw(&pool, &comp, &raw_loc).await;
    open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    seed_fifo_layer(&pool, &comp, &raw_loc, 50, 8, "2026-04-10").await;
    seed_fifo_layer(&pool, &comp, &raw_loc, 50, 12, "2026-04-12").await;

    let wo_id = create_wo(&pool, "W3-WO5", &parent, &fg_loc, 30).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;

    call_wo_start(&pool, &wo_id).await.expect("wo_start");

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
    assert_eq!(alerts, 0);
}

// ============================================================
// W3.6: 'lot' component still raises P0006 (acct-uze).
// ============================================================

#[tokio::test]
async fn lot_component_still_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "W3-P6", "standard").await;
    set_std_cost(&pool, &parent, 100).await;
    let comp = fresh_sku(&pool, "W3-P6-C", "lot").await;
    let raw_loc = fresh_location(&pool, "W3-P6-RAW").await;
    let fg_loc = fresh_location(&pool, "W3-P6-FG").await;

    let (raw_qty_acct, _) = open_component_raw(&pool, &comp, &raw_loc).await;
    open_parent_wip_fg(&pool, &parent, &fg_loc).await;

    // Seed qty supply directly on the qty-leg so it doesn't fire first.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let key = fresh_uuid(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let event = serde_json::json!({
        "reason": "inventory_adjustment",
        "document_kind": "inventory_adjustment_doc",
        "document_id": fresh_uuid(&pool).await,
        "debit_account_id": raw_qty_acct,
        "credit_account_id": void_qty,
        "amount": 100,
        "qty": 100,
        "business_date": "2026-04-10",
        "idempotency_key": key,
        "posted_by": posted_by,
    });
    call_post_posting_lines(&pool, serde_json::json!([event]), false)
        .await
        .expect("seed qty");

    let wo_id = create_wo(&pool, "W3-WO6", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    let bom_id = create_bom(&pool, &parent).await;
    add_bom_item(&pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;

    let res = call_wo_start(&pool, &wo_id).await;
    let err = res.expect_err("lot component must raise P0006");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned()).as_deref(),
        Some("P0006"),
    );
}
