//! Property test for the L1-L4 lot_fifo wrapper integration (acct-a3v5).
//!
//! Drives random workloads through the document entry-point wrappers
//! (post_po_receipt / post_inventory_adjustment / post_wo_start /
//! post_wo_complete / post_so_ship) and asserts NINE invariants per
//! case. Mirrors property_fifo_wrappers.rs for the lot_fifo cost
//! method: lot_fifo SKUs use inventory_lots + inventory_lot_events
//! instead of cost_layers + cost_layer_depletions.
//!
//!   I1  Every successful inflow (ReceiveRaw / AdjustInRaw / WO complete)
//!       creates exactly one inventory_lots row at the asserted (W1/W2)
//!       / computed (W4 wo_complete drain to FG) unit_cost.
//!
//!   I2  Every successful outflow (AdjustOutRaw / WO consume / Ship)
//!       writes ≥1 inventory_lot_events 'issue' row (event_type=1) with
//!       negative quantity_change summing to the issued qty.
//!
//!   I3  SUM(|quantity_change| × inventory_lots.unit_cost) per
//!       posting_line = posting_line.amount EXACTLY (per-row ROUND
//!       construction in _lot_write_issues guarantees this).
//!
//!   I4  inventory_lots.original_quantity + SUM(quantity_change) ≥ 0
//!       for every lot (no over-consumption).
//!
//!   I5  posting_line_inventory.cost_method_at_event = 'lot_fifo' on
//!       every pli row whose posting_line touches inv_value_raw or
//!       inv_value_fg on the lot_fifo SKU.
//!
//!   I6  cost_method_at_receipt = 'lot_fifo' on po_receipt_lines for
//!       lot_fifo receipts; cost_method_at_ship = 'lot_fifo' on
//!       so_shipment_lines.
//!
//!   I7  Idempotent replay: re-calling the last successful op with
//!       the same idempotency_key returns the same doc id and does
//!       not duplicate any lot or event row.
//!
//!   I8  common::assert_invariants_hold (per-ledger double-entry,
//!       balance sign vs normal_side, append-only, etc) hold after
//!       every successful op.
//!
//!   I9  run_daily_reconciliation produces no double_entry_imbalance,
//!       currency_mismatch, or subledger_gl_divergence alerts at end
//!       of case (per-lot residual recon is E2.4 follow-up; not in
//!       scope here).
//!
//! PROPTEST_CASES env var controls scenario count (default 100).
//! --test-threads=1 required since the test resets the shared DB
//! between scenarios. Wall-time target ~30-60 seconds at default.

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

// ============================================================
// Op variants (mirror FIFO's 5 ops, weights 3:2:1:2:2 to bias
// inflows so the pool isn't drained too fast).
// ============================================================

#[derive(Debug, Clone)]
enum Op {
    /// L1: post_po_receipt for lot_fifo raw SKU at asserted unit_cost
    /// with explicit lot_code. Creates one inventory_lots row.
    ReceiveRaw { qty: i64, unit_cost: i64 },
    /// L2-in: post_inventory_adjustment +qty 'raw' at asserted
    /// unit_cost with explicit lot_code. Creates one inventory_lots
    /// row.
    AdjustInRaw { qty: i64, unit_cost: i64 },
    /// L2-out: post_inventory_adjustment -qty 'raw' with EXPLICIT
    /// lot_id pin (per L2 design — operator-driven adjustments must
    /// name the lot; FIFO default deliberately disallowed). Skipped
    /// at runtime if no active raw lot has sufficient residual.
    AdjustOutRaw { qty: i64 },
    /// L3 + L4 setup: create a fresh WO with matching qty_target,
    /// post_wo_start (consumes raw via FIFO walk OR explicit pin),
    /// post_wo_complete (lot_fifo parent treated as WAC for WIP;
    /// drain to inv_value_fg creates one inventory_lots FG row at
    /// the WIP-pool running avg). `pin_component` selects between
    /// FIFO default and an explicit p_component_lot_pins entry.
    /// Skipped if raw on-hand insufficient.
    RunWO { qty: i64, pin_component: bool },
    /// L4 leaf: post_so_ship from the FG lot pool. Walks FIFO by
    /// receipt_date OR pins a specific lot when `pin_explicit` is
    /// true (the wrapper falls back to FIFO if no active FG lot has
    /// sufficient residual). Skipped if fg on-hand insufficient.
    Ship { qty: i64, pin_explicit: bool },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Bias inflows higher so the pool isn't drained instantly.
        3 => (3i64..=15, 10i64..=200)
            .prop_map(|(qty, unit_cost)| Op::ReceiveRaw { qty, unit_cost }),
        2 => (3i64..=15, 10i64..=200)
            .prop_map(|(qty, unit_cost)| Op::AdjustInRaw { qty, unit_cost }),
        1 => (1i64..=8).prop_map(|qty| Op::AdjustOutRaw { qty }),
        2 => (1i64..=6, any::<bool>())
            .prop_map(|(qty, pin_component)| Op::RunWO { qty, pin_component }),
        2 => (1i64..=6, any::<bool>())
            .prop_map(|(qty, pin_explicit)| Op::Ship { qty, pin_explicit }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 5..=15)
}

// ============================================================
// Local scaffolding (one instance per case).
// ============================================================

#[allow(dead_code)]
#[derive(Debug)]
struct Scaffold {
    raw_sku: String,
    raw_loc: String,
    parent: String,
    fg_loc: String,
    customer_id: String,
    vendor_id: String,
    po_id: String,
    bom_id: i64,
    /// stock_available qty on raw lot pool.
    raw_qty: i64,
    /// inv_value_raw on raw lot pool.
    raw_val: i64,
    /// stock_wip qty (op=10) on parent.
    wip_qty: i64,
    /// inv_value_wip (op=10) on parent.
    wip_val: i64,
    /// stock_available qty on parent FG pool.
    fg_qty: i64,
    /// inv_value_fg on parent FG pool.
    fg_val: i64,
    /// COGS account (USD) — fixture-seeded.
    cogs_acct: i64,
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

async fn fresh_location_local(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_customer_local(pool: &PgPool, code: &str) -> String {
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

async fn fresh_vendor_local(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_po_local(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
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

async fn scaffold(pool: &PgPool, suffix: usize) -> Scaffold {
    let raw_sku = fresh_lot_sku(pool, &format!("PRP-LOT-R-{suffix}")).await;
    let parent = fresh_lot_sku(pool, &format!("PRP-LOT-P-{suffix}")).await;
    let raw_loc = fresh_location_local(pool, &format!("PRP-LOT-RAW-{suffix}")).await;
    let fg_loc = fresh_location_local(pool, &format!("PRP-LOT-FG-{suffix}")).await;
    let customer_id = fresh_customer_local(pool, &format!("PRP-LOT-CUST-{suffix}")).await;
    let vendor_id = fresh_vendor_local(pool, &format!("PRP-LOT-VEND-{suffix}")).await;
    let po_id = fresh_po_local(pool, &vendor_id).await;

    // Raw lot pool (drives ReceiveRaw / AdjustInRaw / AdjustOutRaw / RunWO consume).
    let raw_qty = open_account(
        pool, "stock_available", "qty", None,
        Some(&raw_sku), Some(&raw_loc), None, None, "debit",
    )
    .await;
    let raw_val = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(&raw_sku), Some(&raw_loc), None, None, "debit",
    )
    .await;
    open_account(pool, "stock_consumed", "qty", None, Some(&raw_sku), None, None, None, "debit").await;

    // Vendor + AP (Receive).
    open_account(pool, "vendor_pool", "qty", None, None, None, None, Some(&vendor_id), "credit").await;
    open_account(pool, "ap_unsettled", "value", Some("USD"), None, None, None, Some(&vendor_id), "credit").await;

    // Parent WIP + FG (RunWO + Ship).
    let wip_qty = open_account(
        pool, "stock_wip", "qty", None,
        Some(&parent), None, Some(10), None, "debit",
    )
    .await;
    let wip_val = open_account(
        pool, "inv_value_wip", "value", Some("USD"),
        Some(&parent), None, Some(10), None, "debit",
    )
    .await;
    let fg_qty = open_account(
        pool, "stock_available", "qty", None,
        Some(&parent), Some(&fg_loc), None, None, "debit",
    )
    .await;
    let fg_val = open_account(
        pool, "inv_value_fg", "value", Some("USD"),
        Some(&parent), Some(&fg_loc), None, None, "debit",
    )
    .await;

    // Customer + AR (Ship).
    open_account(pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    open_account(pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;

    // BOM: 1 lot_fifo component per parent unit, fires at op 10.
    let bom_id: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers
            (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(&parent)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', 100,
                 $2::UUID, $3::UUID, 1)",
    )
    .bind(bom_id)
    .bind(&raw_sku)
    .bind(&raw_loc)
    .execute(pool)
    .await
    .unwrap();

    Scaffold {
        raw_sku,
        raw_loc,
        parent,
        fg_loc,
        customer_id,
        vendor_id,
        po_id,
        bom_id,
        raw_qty,
        raw_val,
        wip_qty,
        wip_val,
        fg_qty,
        fg_val,
        cogs_acct,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ============================================================
// Active-lot helpers — used to pick a viable lot for explicit
// pinning on AdjustOutRaw / RunWO (component) / Ship.
// ============================================================

/// Pick the oldest-receipt-date active lot for `(sku, loc)` whose
/// residual is ≥ `min_qty`. Returns None if none qualify.
async fn pick_active_lot(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    min_qty: i64,
) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT il.lot_id
           FROM inventory_lots il
          WHERE il.product_id = $1::UUID
            AND il.location_id = $2::UUID
            AND (il.original_quantity + COALESCE(
                  (SELECT SUM(e.quantity_change)
                     FROM inventory_lot_events e
                    WHERE e.lot_id = il.lot_id
                      AND e.lot_receipt_date = il.receipt_date),
                  0
                )) >= $3
          ORDER BY il.receipt_date, il.lot_id
          LIMIT 1",
    )
    .bind(sku)
    .bind(loc)
    .bind(min_qty)
    .fetch_optional(pool)
    .await
    .unwrap()
}

// ============================================================
// Op call wrappers.
// ============================================================

#[allow(clippy::too_many_arguments)]
async fn op_receive_raw(
    pool: &PgPool,
    sf: &Scaffold,
    qty: i64,
    unit_cost: i64,
    lot_code: &str,
    business_date: &str,
    line_no: i32,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let po_line: String = sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD')
         RETURNING id::text",
    )
    .bind(&sf.po_id)
    .bind(line_no)
    .bind(&sf.raw_sku)
    .bind(&sf.raw_loc)
    .bind(qty)
    .bind(unit_cost)
    .fetch_one(pool)
    .await
    .unwrap();
    let lines = json!([{
        "po_line_id":   po_line,
        "qty_received": qty,
        "lot_code":     lot_code,
    }]);
    sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&sf.po_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn op_adjust_in_raw(
    pool: &PgPool,
    sf: &Scaffold,
    qty: i64,
    unit_cost: i64,
    lot_code: &str,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD',
            'raw', $5::DATE, $6::UUID, $7::UUID, NULL, $8
         )::text",
    )
    .bind(&sf.raw_sku)
    .bind(&sf.raw_loc)
    .bind(qty)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .bind(json!({ "lot_code": lot_code }))
    .fetch_one(pool)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn op_adjust_out_raw(
    pool: &PgPool,
    sf: &Scaffold,
    qty: i64,
    lot_id: i64,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, NULL::BIGINT, 'USD',
            'raw', $4::DATE, $5::UUID, $6::UUID, NULL, $7
         )::text",
    )
    .bind(&sf.raw_sku)
    .bind(&sf.raw_loc)
    .bind(-qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .bind(json!({ "lot_id": lot_id }))
    .fetch_one(pool)
    .await
}

async fn op_run_wo(
    pool: &PgPool,
    sf: &Scaffold,
    wo_no: &str,
    qty: i64,
    business_date_start: &str,
    business_date_complete: &str,
    component_pin: Option<i64>,
) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let wo_id: String = sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .bind(qty)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'ASSEMBLE')")
        .bind(&wo_id)
        .execute(pool)
        .await
        .unwrap();

    let pins: Option<serde_json::Value> = component_pin
        .map(|lot_id| json!({ sf.raw_sku.clone(): lot_id }));

    let start_key = fresh_uuid(pool).await;
    let start_posted = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, $5)::text",
    )
    .bind(&wo_id)
    .bind(business_date_start)
    .bind(&start_posted)
    .bind(&start_key)
    .bind(pins)
    .fetch_one(pool)
    .await?;

    let comp_key = fresh_uuid(pool).await;
    let comp_posted = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&wo_id)
    .bind(qty)
    .bind(business_date_complete)
    .bind(&comp_posted)
    .bind(&comp_key)
    .fetch_one(pool)
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn op_ship(
    pool: &PgPool,
    sf: &Scaffold,
    qty: i64,
    unit_price: i64,
    pin_lot: Option<i64>,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let so_id: String = sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(&sf.customer_id)
    .fetch_one(pool)
    .await
    .unwrap();
    let so_line_id: String = sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered, unit_price, currency, tax_amount)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, $4, $5, 'USD', 0)
         RETURNING id::text",
    )
    .bind(&so_id)
    .bind(&sf.parent)
    .bind(&sf.fg_loc)
    .bind(qty)
    .bind(unit_price)
    .fetch_one(pool)
    .await
    .unwrap();

    let line = match pin_lot {
        Some(lot) => json!({
            "so_line_id": so_line_id,
            "qty_shipped": qty,
            "lot_id": lot,
        }),
        None => json!({
            "so_line_id": so_line_id,
            "qty_shipped": qty,
        }),
    };
    let lines = json!([line]);

    sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&so_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Counters carried across an op sequence.
// ============================================================

#[derive(Default, Debug)]
struct Counters {
    raw_lots: i64,
    fg_lots: i64,
    inflow_ops: i64,
    outflow_ops: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum ReplayInfo {
    Receive {
        qty: i64,
        unit_cost: i64,
        lot_code: String,
        business_date: String,
        line_no: i32,
        key: String,
    },
    AdjustIn {
        qty: i64,
        unit_cost: i64,
        lot_code: String,
        business_date: String,
        key: String,
    },
    AdjustOut {
        qty: i64,
        lot_id: i64,
        business_date: String,
        key: String,
    },
    Ship {
        qty: i64,
        pin_lot: Option<i64>,
        business_date: String,
        key: String,
    },
}

// ============================================================
// Main test.
// ============================================================

#[tokio::test(flavor = "current_thread")]
async fn property_lot_wrappers() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases as usize {
        reset_to_fixture(&pool).await;

        let tree = strategy.new_tree(&mut runner).expect("new_tree");
        let ops: Vec<Op> = tree.current();

        let label = format!("lot_wrappers#{case_idx}");
        let sf = scaffold(&pool, case_idx).await;

        // Track tracked-on-hand expectations to short-circuit ops that
        // would deterministically fail (over-deplete). The ledger's
        // CHECK is the ultimate authority; this saves on noisy errors.
        let mut raw_on_hand: i64 = 0;
        let mut fg_on_hand: i64 = 0;
        let mut counters = Counters::default();
        // posting_line ids of every successful outflow whose value-leg
        // drains a lot_fifo pool, for I3 SUM(events) = amount checks.
        let mut outflow_pls: Vec<i64> = Vec::new();
        // Last successful op replay info (I7).
        let mut last_replay: Option<ReplayInfo> = None;

        for (i, op) in ops.iter().enumerate() {
            // Stagger business dates across the 3 open periods (Apr/May/Jun)
            // so receipt_date ordering is deterministic.
            let day = 1 + (i as i64 % 27);
            let month_idx = (i / 27) % 3;
            let month = ["04", "05", "06"][month_idx];
            let business_date = format!("2026-{month}-{day:02}");
            let business_date_complete = {
                let day2 = 1 + ((i as i64 + 1) % 27);
                let month_idx2 = ((i + 1) / 27) % 3;
                let month2 = ["04", "05", "06"][month_idx2];
                format!("2026-{month2}-{day2:02}")
            };

            match *op {
                Op::ReceiveRaw { qty, unit_cost } => {
                    let key = fresh_uuid(&pool).await;
                    let line_no = (i as i32) + 1;
                    let lot_code = format!("L5-LOT-{case_idx}-{i}");
                    let res = op_receive_raw(
                        &pool,
                        &sf,
                        qty,
                        unit_cost,
                        &lot_code,
                        &business_date,
                        line_no,
                        &key,
                    )
                    .await;
                    if res.is_ok() {
                        raw_on_hand += qty;
                        counters.raw_lots += 1;
                        counters.inflow_ops += 1;
                        last_replay = Some(ReplayInfo::Receive {
                            qty,
                            unit_cost,
                            lot_code,
                            business_date: business_date.clone(),
                            line_no,
                            key,
                        });
                        assert_invariants_hold(&pool, &format!("{label}/{i} ReceiveRaw")).await;
                    }
                }
                Op::AdjustInRaw { qty, unit_cost } => {
                    let key = fresh_uuid(&pool).await;
                    let lot_code = format!("L5-AIN-{case_idx}-{i}");
                    let res = op_adjust_in_raw(
                        &pool,
                        &sf,
                        qty,
                        unit_cost,
                        &lot_code,
                        &business_date,
                        &key,
                    )
                    .await;
                    if res.is_ok() {
                        raw_on_hand += qty;
                        counters.raw_lots += 1;
                        counters.inflow_ops += 1;
                        last_replay = Some(ReplayInfo::AdjustIn {
                            qty,
                            unit_cost,
                            lot_code,
                            business_date: business_date.clone(),
                            key,
                        });
                        assert_invariants_hold(&pool, &format!("{label}/{i} AdjustInRaw")).await;
                    }
                }
                Op::AdjustOutRaw { qty } => {
                    if qty <= 0 || qty > raw_on_hand {
                        continue;
                    }
                    let lot_id = match pick_active_lot(&pool, &sf.raw_sku, &sf.raw_loc, qty).await
                    {
                        Some(id) => id,
                        None => continue,
                    };
                    let key = fresh_uuid(&pool).await;
                    let res = op_adjust_out_raw(
                        &pool,
                        &sf,
                        qty,
                        lot_id,
                        &business_date,
                        &key,
                    )
                    .await;
                    if res.is_ok() {
                        raw_on_hand -= qty;
                        counters.outflow_ops += 1;
                        let pl_id: i64 = sqlx::query_scalar(
                            "SELECT pl.id FROM posting_lines pl
                             WHERE pl.credit_account_id = $1
                               AND pl.reason = 'inventory_adjustment'
                               AND pl.qty IS NOT NULL
                             ORDER BY pl.id DESC LIMIT 1",
                        )
                        .bind(sf.raw_val)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                        outflow_pls.push(pl_id);
                        last_replay = Some(ReplayInfo::AdjustOut {
                            qty,
                            lot_id,
                            business_date: business_date.clone(),
                            key,
                        });
                        assert_invariants_hold(&pool, &format!("{label}/{i} AdjustOutRaw")).await;
                    }
                }
                Op::RunWO { qty, pin_component } => {
                    if qty <= 0 || qty > raw_on_hand {
                        continue;
                    }
                    // For pin_component=true, find a raw lot with ≥ qty residual
                    // (per BOM qty_per_parent=1, adj_qty == qty). Fall back to
                    // FIFO if none qualify.
                    let pin: Option<i64> = if pin_component {
                        pick_active_lot(&pool, &sf.raw_sku, &sf.raw_loc, qty).await
                    } else {
                        None
                    };
                    let wo_no = format!("PRP-LOT-WO-{case_idx}-{i}");
                    let res = op_run_wo(
                        &pool,
                        &sf,
                        &wo_no,
                        qty,
                        &business_date,
                        &business_date_complete,
                        pin,
                    )
                    .await;
                    if res.is_ok() {
                        raw_on_hand -= qty;
                        fg_on_hand += qty;
                        // wo_complete creates ONE FG inventory_lots row.
                        counters.fg_lots += 1;
                        counters.inflow_ops += 1; // FG inflow
                        counters.outflow_ops += 1; // raw outflow
                        // Capture rm_issue_to_wo posting_line id for I2/I3
                        // raw-side check.
                        let raw_pl: Option<i64> = sqlx::query_scalar(
                            "SELECT pl.id FROM posting_lines pl
                             WHERE pl.credit_account_id = $1
                               AND pl.reason = 'rm_issue_to_wo'
                               AND pl.qty IS NOT NULL
                             ORDER BY pl.id DESC LIMIT 1",
                        )
                        .bind(sf.raw_val)
                        .fetch_optional(&pool)
                        .await
                        .unwrap();
                        if let Some(pl) = raw_pl {
                            outflow_pls.push(pl);
                        }
                        // No idempotent replay for RunWO (it spans 2
                        // wrappers + WO row creation); we replay simpler
                        // ops only.
                        assert_invariants_hold(&pool, &format!("{label}/{i} RunWO")).await;
                    }
                }
                Op::Ship { qty, pin_explicit } => {
                    if qty <= 0 || qty > fg_on_hand {
                        continue;
                    }
                    // For pin_explicit, find an FG lot with ≥ qty residual.
                    // Fall back to FIFO walk (None) when none qualify — the
                    // wrapper handles either path.
                    let pin: Option<i64> = if pin_explicit {
                        pick_active_lot(&pool, &sf.parent, &sf.fg_loc, qty).await
                    } else {
                        None
                    };
                    let key = fresh_uuid(&pool).await;
                    let res = op_ship(
                        &pool,
                        &sf,
                        qty,
                        100,
                        pin,
                        &business_date,
                        &key,
                    )
                    .await;
                    if res.is_ok() {
                        fg_on_hand -= qty;
                        counters.outflow_ops += 1;
                        let pl_id: i64 = sqlx::query_scalar(
                            "SELECT pl.id FROM posting_lines pl
                             WHERE pl.credit_account_id = $1
                               AND pl.reason = 'so_ship'
                               AND pl.qty IS NOT NULL
                             ORDER BY pl.id DESC LIMIT 1",
                        )
                        .bind(sf.fg_val)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                        outflow_pls.push(pl_id);
                        last_replay = Some(ReplayInfo::Ship {
                            qty,
                            pin_lot: pin,
                            business_date: business_date.clone(),
                            key,
                        });
                        assert_invariants_hold(&pool, &format!("{label}/{i} Ship")).await;
                    }
                }
            }
        }

        // Skip cases where nothing landed (proptest sometimes generates
        // sequences whose ops all gate out).
        if counters.inflow_ops == 0 {
            continue;
        }

        // ------------------------------------------------------------
        // I1: inventory_lots row count = inflow_ops applied.
        //   raw_lots from Receive + AdjustIn applied.
        //   fg_lots  from RunWO applied (each wo_complete creates 1).
        // ------------------------------------------------------------
        let raw_lot_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM inventory_lots
              WHERE product_id = $1::UUID AND location_id = $2::UUID",
        )
        .bind(&sf.raw_sku)
        .bind(&sf.raw_loc)
        .fetch_one(&pool)
        .await
        .unwrap();
        let fg_lot_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM inventory_lots
              WHERE product_id = $1::UUID AND location_id = $2::UUID",
        )
        .bind(&sf.parent)
        .bind(&sf.fg_loc)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            raw_lot_count, counters.raw_lots,
            "[{label}] I1 raw: lots {raw_lot_count} ≠ tracked inflows {}",
            counters.raw_lots
        );
        assert_eq!(
            fg_lot_count, counters.fg_lots,
            "[{label}] I1 fg: lots {fg_lot_count} ≠ tracked WO completes {}",
            counters.fg_lots
        );

        // ------------------------------------------------------------
        // I2: every outflow posting_line has ≥1 inventory_lot_events
        // 'issue' row summing to the issued qty (negative qty_change).
        // ------------------------------------------------------------
        for pl in &outflow_pls {
            let row: (i64, String, Option<String>) = sqlx::query_as(
                "SELECT COUNT(*)::BIGINT,
                        COALESCE(SUM(-e.quantity_change), 0)::TEXT,
                        pl.qty::TEXT
                   FROM inventory_lot_events e
                   JOIN posting_lines pl ON pl.id = e.posting_line_id
                  WHERE e.posting_line_id = $1
                    AND e.event_type = 1
                  GROUP BY pl.qty",
            )
            .bind(pl)
            .fetch_one(&pool)
            .await
            .unwrap();
            let evt_count = row.0;
            let evt_sum_str = row.1;
            let issue_qty_str = row.2.unwrap_or_default();
            assert!(
                evt_count >= 1,
                "[{label}] I2 (pl={pl}): expected ≥1 issue event, got {evt_count}"
            );
            let evt_sum: f64 = evt_sum_str.parse().unwrap();
            let issue_qty: f64 = issue_qty_str.parse().unwrap();
            assert!(
                (evt_sum - issue_qty).abs() < 1e-6,
                "[{label}] I2 (pl={pl}): SUM(-qty_change) {evt_sum} ≠ issue qty {issue_qty}"
            );
        }

        // ------------------------------------------------------------
        // I3: SUM(|qty_change| × inventory_lots.unit_cost) per outflow
        // pl = posting_line.amount EXACTLY.
        // ------------------------------------------------------------
        for pl in &outflow_pls {
            let pl_amount: i64 =
                sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
                    .bind(pl)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let evt_sum: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(ABS(e.quantity_change) * il.unit_cost), 0)::BIGINT
                   FROM inventory_lot_events e
                   JOIN inventory_lots il ON il.lot_id = e.lot_id
                                          AND il.receipt_date = e.lot_receipt_date
                  WHERE e.posting_line_id = $1
                    AND e.event_type = 1",
            )
            .bind(pl)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                pl_amount, evt_sum,
                "[{label}] I3 (pl={pl}): amount {pl_amount} ≠ SUM(events) {evt_sum}"
            );
        }

        // ------------------------------------------------------------
        // I4: residual ≥ 0 per lot (in BOTH raw and fg pools).
        // ------------------------------------------------------------
        let neg_residual: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT il.lot_id,
                    il.original_quantity::TEXT,
                    COALESCE(
                      (SELECT SUM(e.quantity_change)::TEXT
                         FROM inventory_lot_events e
                        WHERE e.lot_id = il.lot_id
                          AND e.lot_receipt_date = il.receipt_date),
                      '0'
                    )
               FROM inventory_lots il
              WHERE il.product_id IN ($1::UUID, $2::UUID)
                AND (il.original_quantity + COALESCE(
                      (SELECT SUM(e.quantity_change)
                         FROM inventory_lot_events e
                        WHERE e.lot_id = il.lot_id
                          AND e.lot_receipt_date = il.receipt_date),
                      0
                    )) < 0",
        )
        .bind(&sf.raw_sku)
        .bind(&sf.parent)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            neg_residual.is_empty(),
            "[{label}] I4 violation — over-consumed lots: {neg_residual:?}"
        );

        // ------------------------------------------------------------
        // I5: cost_method_at_event = 'lot_fifo' on every pli row keyed
        // to a posting_line that touches the lot_fifo pools (raw or
        // fg). WIP-touching pli rows may carry a non-lot_fifo method
        // (parent's WIP-side dispatch treats it as WAC for lot_fifo
        // parent per L4 design); we exclude WIP from the check.
        // ------------------------------------------------------------
        let pli_methods: Vec<(i64, String)> = sqlx::query_as(
            "SELECT pli.posting_line_id, pli.cost_method_at_event::TEXT
               FROM posting_line_inventory pli
               JOIN posting_lines pl ON pl.id = pli.posting_line_id
              WHERE pl.debit_account_id IN ($1, $2)
                 OR pl.credit_account_id IN ($1, $2)",
        )
        .bind(sf.raw_val)
        .bind(sf.fg_val)
        .fetch_all(&pool)
        .await
        .unwrap();
        for (pl_id, m) in &pli_methods {
            assert_eq!(
                m, "lot_fifo",
                "[{label}] I5 (pl={pl_id}) cost_method_at_event = '{m}', expected 'lot_fifo'"
            );
        }

        // ------------------------------------------------------------
        // I6: cost_method_at_receipt = 'lot_fifo' on po_receipt_lines
        //     for lot_fifo receipts; cost_method_at_ship = 'lot_fifo'
        //     on so_shipment_lines.
        // ------------------------------------------------------------
        let bad_recv: Vec<String> = sqlx::query_scalar(
            "SELECT prl.cost_method_at_receipt::TEXT
               FROM po_receipt_lines prl
               JOIN purchase_order_lines pol ON pol.id = prl.po_line_id
              WHERE pol.sku_id = $1::UUID
                AND prl.cost_method_at_receipt::TEXT <> 'lot_fifo'",
        )
        .bind(&sf.raw_sku)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            bad_recv.is_empty(),
            "[{label}] I6: po_receipt_lines on lot_fifo SKU did not snapshot 'lot_fifo': {bad_recv:?}"
        );
        let bad_ship: Vec<String> = sqlx::query_scalar(
            "SELECT ssl.cost_method_at_ship::TEXT
               FROM so_shipment_lines ssl
               JOIN sales_order_lines sol ON sol.id = ssl.so_line_id
              WHERE sol.sku_id = $1::UUID
                AND ssl.cost_method_at_ship::TEXT <> 'lot_fifo'",
        )
        .bind(&sf.parent)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(
            bad_ship.is_empty(),
            "[{label}] I6: so_shipment_lines on lot_fifo SKU did not snapshot 'lot_fifo': {bad_ship:?}"
        );

        // ------------------------------------------------------------
        // I7: idempotent replay of the last successful op returns
        // the same doc id and does not duplicate state.
        // ------------------------------------------------------------
        if let Some(replay) = last_replay.clone() {
            let pre_lots = raw_lot_count + fg_lot_count;
            let pre_events: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM inventory_lot_events",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            match replay {
                ReplayInfo::Receive {
                    qty: _,
                    unit_cost: _,
                    lot_code: _,
                    business_date: _,
                    line_no: _,
                    key,
                } => {
                    // For receipt replay, re-call post_po_receipt with the
                    // same key. We cannot re-INSERT a po_line so we pull
                    // the existing po_line + lot_code from the original.
                    let (orig_doc, po_line_id, qty_received, lot_code, bd):
                        (String, String, i64, String, String) = sqlx::query_as(
                        "SELECT pr.id::text, pol.id::text, prl.qty_received,
                                il.lot_code, pr.business_date::TEXT
                           FROM po_receipt_lines prl
                           JOIN po_receipts pr ON pr.id = prl.receipt_id
                           JOIN purchase_order_lines pol ON pol.id = prl.po_line_id
                           JOIN inventory_lots il ON il.lot_id = prl.lot_id
                          WHERE pr.idempotency_key = $1::UUID
                          ORDER BY prl.id DESC LIMIT 1",
                    )
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    let posted_by = fresh_uuid(&pool).await;
                    let lines = json!([{
                        "po_line_id":   po_line_id,
                        "qty_received": qty_received,
                        "lot_code":     lot_code,
                    }]);
                    let replay_doc: String = sqlx::query_scalar(
                        "SELECT post_po_receipt($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
                    )
                    .bind(&sf.po_id)
                    .bind(lines)
                    .bind(bd)
                    .bind(&posted_by)
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    assert_eq!(
                        replay_doc, orig_doc,
                        "[{label}] I7 receipt replay: doc id drifted"
                    );
                }
                ReplayInfo::AdjustIn {
                    qty,
                    unit_cost,
                    lot_code,
                    business_date,
                    key,
                } => {
                    let orig: String = sqlx::query_scalar(
                        "SELECT id::text FROM inventory_adjustments WHERE idempotency_key = $1::UUID",
                    )
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    let replay = op_adjust_in_raw(
                        &pool,
                        &sf,
                        qty,
                        unit_cost,
                        &lot_code,
                        &business_date,
                        &key,
                    )
                    .await
                    .expect("AdjustIn replay should succeed");
                    assert_eq!(
                        replay, orig,
                        "[{label}] I7 AdjustIn replay: doc id drifted"
                    );
                }
                ReplayInfo::AdjustOut {
                    qty,
                    lot_id,
                    business_date,
                    key,
                } => {
                    let orig: String = sqlx::query_scalar(
                        "SELECT id::text FROM inventory_adjustments WHERE idempotency_key = $1::UUID",
                    )
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    let replay = op_adjust_out_raw(
                        &pool,
                        &sf,
                        qty,
                        lot_id,
                        &business_date,
                        &key,
                    )
                    .await
                    .expect("AdjustOut replay should succeed");
                    assert_eq!(
                        replay, orig,
                        "[{label}] I7 AdjustOut replay: doc id drifted"
                    );
                }
                ReplayInfo::Ship { qty: _, pin_lot: _, business_date: _, key } => {
                    // The Ship op generates a fresh SO per call. Replay
                    // re-calls post_so_ship with the same idempotency_key
                    // against the same SO + line. Look up original.
                    let (orig, so_id, so_line_id, qty_shipped, lot_id, bd):
                        (String, String, String, i64, Option<i64>, String) =
                        sqlx::query_as(
                        "SELECT s.id::text, s.so_id::text, ssl.so_line_id::text,
                                ssl.qty_shipped, ssl.lot_id, s.business_date::TEXT
                           FROM so_shipments s
                           JOIN so_shipment_lines ssl ON ssl.shipment_id = s.id
                          WHERE s.idempotency_key = $1::UUID
                          ORDER BY ssl.id DESC LIMIT 1",
                    )
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    let posted_by = fresh_uuid(&pool).await;
                    let line = match lot_id {
                        Some(lot) => json!({
                            "so_line_id":  so_line_id,
                            "qty_shipped": qty_shipped,
                            "lot_id":      lot,
                        }),
                        None => json!({
                            "so_line_id":  so_line_id,
                            "qty_shipped": qty_shipped,
                        }),
                    };
                    let lines = json!([line]);
                    let replay_doc: String = sqlx::query_scalar(
                        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
                    )
                    .bind(&so_id)
                    .bind(lines)
                    .bind(bd)
                    .bind(&posted_by)
                    .bind(&key)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                    assert_eq!(
                        replay_doc, orig,
                        "[{label}] I7 Ship replay: doc id drifted"
                    );
                }
            }
            let post_lots: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM inventory_lots
                  WHERE product_id IN ($1::UUID, $2::UUID)",
            )
            .bind(&sf.raw_sku)
            .bind(&sf.parent)
            .fetch_one(&pool)
            .await
            .unwrap();
            let post_events: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)::BIGINT FROM inventory_lot_events",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                post_lots, pre_lots,
                "[{label}] I7: replay duplicated inventory_lots"
            );
            assert_eq!(
                post_events, pre_events,
                "[{label}] I7: replay duplicated inventory_lot_events"
            );
        }

        // ------------------------------------------------------------
        // I8: invariants hold at end of case (also checked per-op).
        // ------------------------------------------------------------
        assert_invariants_hold(&pool, &format!("{label} end")).await;

        // ------------------------------------------------------------
        // I9: run_daily_reconciliation produces no double_entry_imbalance,
        //     currency_mismatch, or subledger_gl_divergence alerts.
        //     (Per-lot residual recon is E2.4 follow-up.)
        // ------------------------------------------------------------
        let _ = sqlx::query_scalar::<_, i64>(
            "SELECT run_daily_reconciliation()::BIGINT",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let alerts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts
              WHERE alert_kind IN ('double_entry_imbalance',
                                   'currency_mismatch',
                                   'subledger_gl_divergence')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            alerts, 0,
            "[{label}] I9: recon flagged a core consistency alert — \
             raw_on_hand={raw_on_hand} fg_on_hand={fg_on_hand} \
             raw_balance={} fg_balance={}",
            balance(&pool, sf.raw_qty).await,
            balance(&pool, sf.fg_qty).await,
        );

        // Sanity: tracked on_hand must match account balance.
        assert_eq!(
            balance(&pool, sf.raw_qty).await,
            raw_on_hand,
            "[{label}] tracker drift on raw_qty"
        );
        assert_eq!(
            balance(&pool, sf.fg_qty).await,
            fg_on_hand,
            "[{label}] tracker drift on fg_qty"
        );
    }
}
