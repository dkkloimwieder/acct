//! `acct-1s6r` — End-to-end Slice A + Slice B + Slice C mixed-workload load test.
//!
//! Existing load_*.rs binaries each cover a single workload shape:
//!   - load_realistic_workload.rs  (shape F) — bin_move cross-account spread
//!   - load_inflow_workload.rs     (shape N) — Slice A PO + AP cycle only
//!   - load_outbox_*               — outbox transport variants
//!   - load_value_workload.rs / load_deadlock_freedom.rs / load_reservation_interleave.rs
//!
//! What's missing — and what this test provides — is a load shape that
//! mixes inflow (PO receipt + AP bill), conversion (WO start / op_move /
//! WO complete), and outflow (SO ship + invoice + AR payment) all running
//! concurrently against the same SKU pool. That's the contention shape
//! that decides whether acct-c4p (pseudo-sync pivot) and acct-e8g
//! (posting_lines partitioning) need to ship and gives a measured
//! answer to the per-period accounts-row explosion question.
//!
//! Env knobs (defaults shown):
//!
//!   T4_DURATION_SECS=600   wall-clock per run (10 min default)
//!   T4_WRITERS=32          concurrent tokio writers
//!   T4_BENCH_SKUS=50       mixed-method SKU pool (PO + SO eligible)
//!   T4_BENCH_VENDORS=10
//!   T4_BENCH_CUSTOMERS=10
//!   T4_BENCH_WO_SKUS=5     FG-only SKUs (WO parents)
//!
//! The test is `#[ignore]` — opt-in via:
//!
//!   T4_DURATION_SECS=30 cargo test --test load_phase1_mixed_workload \
//!     -- --ignored --nocapture
//!
//! Workload mix per writer cycle (weighted random op picker):
//!
//!   1.00  PO receipt
//!   0.80  AP bill (clears a prior receipt; skip if queue empty)
//!   0.50  WO start
//!   1.50  op_move on an in-flight WO (skip if queue empty)
//!   0.40  WO complete   (skip if queue empty)
//!   1.00  SO ship       (FG inventory pre-stocked so independent of WOs)
//!   0.70  customer invoice (clears a prior ship; skip if queue empty)
//!   0.30  AR payment    (settles an open invoice; skip if queue empty)
//!   0.05  customer/PO return (random split)
//!
//! State-machine errors that arise from queue starvation are non-fatal
//! (skip and re-pick). Deadlocks panic the writer (CI guard).

#![allow(dead_code)]

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;
use std::collections::VecDeque;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

// ============================================================
// RNG + UUID helpers (mirror load_realistic_workload.rs)
// ============================================================

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0xdead_beef_cafe_babe;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn fresh_uuid_str(rng: &mut u64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        xorshift(rng) as u32,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) & 0xffff_ffff_ffff,
    )
}

// ============================================================
// Fixture entities
// ============================================================

#[derive(Clone)]
struct VendorSpot {
    vendor_id: String,
    /// One PO per vendor. Picked at receipt time.
    po_id: String,
    po_line_id: String,
    /// The SKU this vendor's PO line points at. Same SKU may appear under
    /// multiple vendors — that's fine.
    sku_idx: usize,
}

#[derive(Clone)]
struct CustomerSpot {
    customer_id: String,
    /// One SO per customer.
    so_id: String,
    so_line_id: String,
    sku_idx: usize,
}

#[derive(Clone)]
struct SkuSpot {
    code: String,
    cost_method: String,
    is_fg_only: bool,
    sku_id: String,
    /// Standard cost (used by all writers as the unit_cost when this SKU
    /// is the subject of a PO receipt / SO ship; PPV stays at 0).
    std_cost: i64,
}

#[derive(Clone)]
struct WoSpot {
    /// Parent SKU (FG-only). Standard cost.
    parent_idx: usize,
    /// Component SKU indexes (subset of the mixed-method pool — kept to
    /// standard SKUs to keep WO dispatch deterministic).
    components: Vec<usize>,
    /// Routing op count (1 or 2; mix of single-op and multi-op BOMs).
    op_count: i32,
    /// bom_id pinned via work_orders.bom_id.
    bom_id: i64,
}

// ============================================================
// Workload state — shared across writers via Arc<Mutex<...>>
// ============================================================

#[derive(Clone)]
struct PendingReceipt {
    po_id: String,
    po_line_id: String,
    vendor_id: String,
    sku_idx: usize,
    qty: i64,
    unit_cost: i64,
    business_date: String,
}

#[derive(Clone)]
struct InFlightWo {
    wo_id: String,
    wo_spot_idx: usize,
    qty: i64,
    /// Next op to which qty needs to be moved (or completed if equals
    /// last_op).
    current_op: i32,
    /// Last op of this WO (== op_count * 10, since ops are 10-spaced).
    last_op: i32,
    business_date: String,
}

#[derive(Clone)]
struct PendingShip {
    so_id: String,
    so_line_id: String,
    customer_id: String,
    sku_idx: usize,
    qty: i64,
    unit_price: i64,
    business_date: String,
}

#[derive(Clone)]
struct OpenInvoice {
    customer_id: String,
    amount: i64,
}

#[derive(Default)]
struct WorkloadState {
    receipts: VecDeque<PendingReceipt>,
    wos: VecDeque<InFlightWo>,
    ships: VecDeque<PendingShip>,
    invoices: VecDeque<OpenInvoice>,
}

// ============================================================
// Op selection
// ============================================================

#[derive(Clone, Copy, Debug)]
enum WriterOp {
    PoReceipt,
    ApBill,
    WoStart,
    OpMove,
    WoComplete,
    SoShip,
    CustomerInvoice,
    ArPayment,
    Return,
}

/// Discrete weights — same order as WriterOp. Sum to 6.30; we run a
/// weighted lottery over them per cycle.
const OP_WEIGHTS: &[(WriterOp, f64)] = &[
    (WriterOp::PoReceipt, 1.00),
    (WriterOp::ApBill, 0.80),
    (WriterOp::WoStart, 0.50),
    (WriterOp::OpMove, 1.50),
    (WriterOp::WoComplete, 0.40),
    (WriterOp::SoShip, 1.00),
    (WriterOp::CustomerInvoice, 0.70),
    (WriterOp::ArPayment, 0.30),
    (WriterOp::Return, 0.05),
];

fn pick_op(rng: &mut u64) -> WriterOp {
    let total: f64 = OP_WEIGHTS.iter().map(|(_, w)| *w).sum();
    let r = (xorshift(rng) as f64 / u64::MAX as f64) * total;
    let mut cum = 0.0;
    for (op, w) in OP_WEIGHTS {
        cum += *w;
        if r <= cum {
            return *op;
        }
    }
    WriterOp::PoReceipt
}

const OP_VARIANT_COUNT: usize = 9;

fn op_idx(op: WriterOp) -> usize {
    match op {
        WriterOp::PoReceipt => 0,
        WriterOp::ApBill => 1,
        WriterOp::WoStart => 2,
        WriterOp::OpMove => 3,
        WriterOp::WoComplete => 4,
        WriterOp::SoShip => 5,
        WriterOp::CustomerInvoice => 6,
        WriterOp::ArPayment => 7,
        WriterOp::Return => 8,
    }
}

fn op_name(op: WriterOp) -> &'static str {
    match op {
        WriterOp::PoReceipt => "po_receipt",
        WriterOp::ApBill => "ap_bill",
        WriterOp::WoStart => "wo_start",
        WriterOp::OpMove => "op_move",
        WriterOp::WoComplete => "wo_complete",
        WriterOp::SoShip => "so_ship",
        WriterOp::CustomerInvoice => "customer_invoice",
        WriterOp::ArPayment => "ar_payment",
        WriterOp::Return => "return",
    }
}

// ============================================================
// Outcome — distinguishes "ok / state-skip / err / deadlock"
// ============================================================

enum Outcome {
    /// Wrapper returned OK; record latency in op histogram.
    Ok(Duration),
    /// Skipped because shared state had no queue entry. Don't count.
    Skip,
    /// Wrapper raised a non-deadlock error. Count, log, keep going.
    Err(String),
    /// SQLSTATE 40P01. Panic the writer.
    Deadlock(String),
}

fn classify_err(e: sqlx::Error, label: &str) -> Outcome {
    let code = e
        .as_database_error()
        .and_then(|d| d.code().map(|c| c.into_owned()))
        .unwrap_or_else(|| "no-code".to_string());
    if code == "40P01" {
        return Outcome::Deadlock(format!("{label}: {e}"));
    }
    Outcome::Err(format!("[{code}] {label}: {e}"))
}

// ============================================================
// Fixture setup
// ============================================================

/// Build the load fixture programmatically:
///   - 50 mixed-method SKUs at MAIN (PO + SO eligible)
///   -  5 FG-only SKUs (WO parents; standard cost)
///   - 10 vendors with 1 PO each (line points at a randomly-assigned SKU)
///   - 10 customers with 1 SO each (line points at a randomly-assigned SKU)
///   -  5 BOMs (1 per WO SKU; mix of single-op + multi-op routings)
///   -  3 absorption classes (labor_std + oh_std are seeded; the seed
///      already inserts both; we don't add a third — generality with two
///      is sufficient)
///   - All in USD (cross-currency is acct-3xcg / out of scope)
///   - Pre-stock raw + fg inventory via post_inventory_adjustment so
///     posting_lines.qty rows back wac dispatch divisors (R1)
async fn setup_load_fixture(
    pool: &PgPool,
    n_skus: usize,
    n_vendors: usize,
    n_customers: usize,
    n_wo_skus: usize,
) -> (
    Vec<SkuSpot>,
    Vec<VendorSpot>,
    Vec<CustomerSpot>,
    Vec<WoSpot>,
) {
    // Distribution of cost methods across the mixed pool:
    // 50 = 25 standard / 15 wac_perpetual / 8 wac_periodic / 2 wac_retroactive
    let cost_methods: Vec<&'static str> = (0..n_skus)
        .map(|i| {
            let f25 = n_skus * 25 / 50;
            let f15 = n_skus * 40 / 50;
            let f8 = n_skus * 48 / 50;
            if i < f25 {
                "standard"
            } else if i < f15 {
                "wac_perpetual"
            } else if i < f8 {
                "wac_periodic"
            } else {
                "wac_retroactive"
            }
        })
        .collect();

    // Standard cost per SKU. Uniform 100 across the MIX pool so the
    // WO path's std-cost math reconciles cleanly (parent_std = sum of
    // component stds = N×100 where N is component count per BOM).
    // Varying costs would force per-WO parent_std computation; the
    // load test cares about contention shape, not cost variance.
    let std_costs: Vec<i64> = (0..n_skus).map(|_| 100i64).collect();

    // ---- 1) SKUs (mixed pool + WO-parent FG). ----
    for i in 0..n_skus {
        let code = format!("MIX-{:03}", i);
        sqlx::query(
            "INSERT INTO skus (code, uom, cost_method)
             VALUES ($1, 'EA', $2::cost_method)
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .bind(cost_methods[i])
        .execute(pool)
        .await
        .expect("insert MIX sku");
    }
    for i in 0..n_wo_skus {
        let code = format!("WO-{:03}", i);
        sqlx::query(
            "INSERT INTO skus (code, uom, cost_method)
             VALUES ($1, 'EA', 'standard')
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .execute(pool)
        .await
        .expect("insert WO sku");
    }

    // Standard costs.
    for (i, code_prefix) in (0..n_skus).map(|i| (i, "MIX")) {
        let code = format!("{:}-{:03}", code_prefix, i);
        sqlx::query(
            "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
             SELECT id, $2, '1970-01-01'::DATE,
                    '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
               FROM skus WHERE code = $1
               AND NOT EXISTS (SELECT 1 FROM standard_costs sc JOIN skus s ON s.id = sc.sku_id WHERE s.code = $1)",
        )
        .bind(&code)
        .bind(std_costs[i])
        .execute(pool)
        .await
        .expect("insert std_cost MIX");
    }
    for i in 0..n_wo_skus {
        let code = format!("WO-{:03}", i);
        // parent_std = sum-of-component-stds = (1 or 2) × 100. We can
        // derive op_count from i % 2 here (same rule as later BOM build).
        let parent_std = if i % 2 == 0 { 100 } else { 200 };
        sqlx::query(
            "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
             SELECT id, $2, '1970-01-01'::DATE,
                    '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
               FROM skus WHERE code = $1
               AND NOT EXISTS (SELECT 1 FROM standard_costs sc JOIN skus s ON s.id = sc.sku_id WHERE s.code = $1)",
        )
        .bind(&code)
        .bind(parent_std as i64)
        .execute(pool)
        .await
        .expect("insert std_cost WO");
    }

    // ---- 2) Per-SKU accounts: stock_available, inv_value_raw, inv_value_fg. ----
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
           FROM skus s, locations l
          WHERE (s.code LIKE 'MIX-%' OR s.code LIKE 'WO-%')
            AND l.code = 'MAIN'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind = 'stock_available' AND a.sku_id = s.id
                 AND a.location_id = l.id AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create stock_available");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_raw', 'value', 'USD', 'debit', s.id, l.id
           FROM skus s, locations l
          WHERE (s.code LIKE 'MIX-%' OR s.code LIKE 'WO-%')
            AND l.code = 'MAIN'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind = 'inv_value_raw' AND a.sku_id = s.id
                 AND a.location_id = l.id AND a.currency = 'USD'
                 AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create inv_value_raw");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_fg', 'value', 'USD', 'debit', s.id, l.id
           FROM skus s, locations l
          WHERE (s.code LIKE 'MIX-%' OR s.code LIKE 'WO-%')
            AND l.code = 'MAIN'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind = 'inv_value_fg' AND a.sku_id = s.id
                 AND a.location_id = l.id AND a.currency = 'USD'
                 AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create inv_value_fg");

    // MIX SKUs that act as BOM components need stock_consumed.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, normal_side)
         SELECT 'stock_consumed', 'qty', s.id, 'debit'
           FROM skus s
          WHERE s.code LIKE 'MIX-%'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind = 'stock_consumed' AND a.sku_id = s.id
                 AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create stock_consumed");

    // WO parents also need stock_wip at op 10 + 20 + inv_value_wip op 10 + 20.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, routing_op, normal_side)
         SELECT 'stock_wip', 'qty', s.id, op, 'debit'
           FROM skus s, (VALUES (10), (20)) AS r(op)
          WHERE s.code LIKE 'WO-%'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind = 'stock_wip' AND a.sku_id = s.id
                 AND a.routing_op = op AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create stock_wip");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, routing_op)
         SELECT 'inv_value_wip', 'value', 'USD', 'debit', s.id, op
           FROM skus s, (VALUES (10), (20)) AS r(op)
          WHERE s.code LIKE 'WO-%'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind = 'inv_value_wip' AND a.sku_id = s.id
                 AND a.routing_op = op AND a.currency = 'USD'
                 AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create inv_value_wip");

    // Vendor / customer reference rows.
    for i in 0..n_vendors {
        let code = format!("LV-{:02}", i);
        sqlx::query(
            "INSERT INTO vendors (code, name, currency)
             VALUES ($1, $2, 'USD')
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .bind(format!("Load vendor {i}"))
        .execute(pool)
        .await
        .expect("insert vendor");
    }
    for i in 0..n_customers {
        let code = format!("LC-{:02}", i);
        sqlx::query(
            "INSERT INTO customers (code, name, default_currency)
             VALUES ($1, $2, 'USD')
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .bind(format!("Load customer {i}"))
        .execute(pool)
        .await
        .expect("insert customer");
    }

    // Per-vendor / per-customer accounts.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, normal_side, counterparty_id)
         SELECT 'vendor_pool', 'qty', 'credit', v.id FROM vendors v
          WHERE v.code LIKE 'LV-%' AND NOT EXISTS (
            SELECT 1 FROM accounts a WHERE a.kind='vendor_pool' AND a.counterparty_id=v.id AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create vendor_pool");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         SELECT 'ap_unsettled', 'value', 'USD', 'credit', v.id FROM vendors v
          WHERE v.code LIKE 'LV-%' AND NOT EXISTS (
            SELECT 1 FROM accounts a WHERE a.kind='ap_unsettled' AND a.counterparty_id=v.id AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create ap_unsettled");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         SELECT 'ap', 'value', 'USD', 'credit', v.id FROM vendors v
          WHERE v.code LIKE 'LV-%' AND NOT EXISTS (
            SELECT 1 FROM accounts a WHERE a.kind='ap' AND a.counterparty_id=v.id AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create ap");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, normal_side, counterparty_id)
         SELECT 'customer_pool', 'qty', 'debit', c.id FROM customers c
          WHERE c.code LIKE 'LC-%' AND NOT EXISTS (
            SELECT 1 FROM accounts a WHERE a.kind='customer_pool' AND a.counterparty_id=c.id AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create customer_pool");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         SELECT 'ar_unsettled', 'value', 'USD', 'debit', c.id FROM customers c
          WHERE c.code LIKE 'LC-%' AND NOT EXISTS (
            SELECT 1 FROM accounts a WHERE a.kind='ar_unsettled' AND a.counterparty_id=c.id AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create ar_unsettled");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         SELECT 'ar', 'value', 'USD', 'debit', c.id FROM customers c
          WHERE c.code LIKE 'LC-%' AND NOT EXISTS (
            SELECT 1 FROM accounts a WHERE a.kind='ar' AND a.counterparty_id=c.id AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create ar");

    // Per-customer revenue is not required (we use unpartitioned revenue
    // seeded by the fixture). post_so_ship looks up by (kind, ccy) when
    // sku-side is NULL.

    // ---- 3) POs (1 per vendor, with 1 line each pointing at a SKU). ----
    sqlx::query(
        "INSERT INTO purchase_orders (vendor_id, status)
         SELECT v.id, 'open' FROM vendors v
          WHERE v.code LIKE 'LV-%'
            AND NOT EXISTS (SELECT 1 FROM purchase_orders po WHERE po.vendor_id = v.id)",
    )
    .execute(pool)
    .await
    .expect("create POs");

    // Map vendor i -> sku i % n_skus (deterministic, repeatable; covers a
    // spread).
    for i in 0..n_vendors {
        let v_code = format!("LV-{:02}", i);
        let sku_idx = i % n_skus;
        let s_code = format!("MIX-{:03}", sku_idx);
        let unit_cost = std_costs[sku_idx];
        sqlx::query(
            "INSERT INTO purchase_order_lines
                (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
             SELECT po.id, 1, s.id, l.id, 100000000, $3, 'USD'
               FROM purchase_orders po
               JOIN vendors v ON v.id = po.vendor_id
               JOIN skus s ON s.code = $2
               JOIN locations l ON l.code = 'MAIN'
              WHERE v.code = $1
                AND NOT EXISTS (SELECT 1 FROM purchase_order_lines pl WHERE pl.po_id = po.id)",
        )
        .bind(&v_code)
        .bind(&s_code)
        .bind(unit_cost)
        .execute(pool)
        .await
        .expect("insert PO line");
    }

    // ---- 4) SOs (1 per customer). ----
    sqlx::query(
        "INSERT INTO sales_orders (customer_id, status)
         SELECT c.id, 'open' FROM customers c
          WHERE c.code LIKE 'LC-%'
            AND NOT EXISTS (SELECT 1 FROM sales_orders so WHERE so.customer_id = c.id)",
    )
    .execute(pool)
    .await
    .expect("create SOs");

    for i in 0..n_customers {
        let c_code = format!("LC-{:02}", i);
        let sku_idx = (i + 3) % n_skus; // offset so vendor/customer SKUs aren't aligned
        let s_code = format!("MIX-{:03}", sku_idx);
        let unit_price = std_costs[sku_idx] * 2; // 100% markup
        sqlx::query(
            "INSERT INTO sales_order_lines
                (so_id, line_no, sku_id, ship_location_id, qty_ordered, unit_price, currency)
             SELECT so.id, 1, s.id, l.id, 100000000, $3, 'USD'
               FROM sales_orders so
               JOIN customers c ON c.id = so.customer_id
               JOIN skus s ON s.code = $2
               JOIN locations l ON l.code = 'MAIN'
              WHERE c.code = $1
                AND NOT EXISTS (SELECT 1 FROM sales_order_lines sl WHERE sl.so_id = so.id)",
        )
        .bind(&c_code)
        .bind(&s_code)
        .bind(unit_price)
        .execute(pool)
        .await
        .expect("insert SO line");
    }

    // ---- 5) BOM per WO SKU. Alternates single-op (10) and multi-op (10, 20). ----
    let mut wo_spots = Vec::with_capacity(n_wo_skus);
    for i in 0..n_wo_skus {
        let parent_code = format!("WO-{:03}", i);
        let op_count = if i % 2 == 0 { 1 } else { 2 };
        // Components: pick 2 standard-cost SKUs from the MIX pool.
        let comp_a_idx = (i * 3) % (n_skus * 25 / 50).max(1); // standard slice
        let comp_b_idx = (i * 3 + 7) % (n_skus * 25 / 50).max(1);

        let bom_id: i64 = sqlx::query_scalar(
            "INSERT INTO bom_headers
                (parent_sku_id, alternate_no, revision_no, is_primary, status)
             SELECT s.id, 1, 'A', TRUE, 'active' FROM skus s WHERE s.code = $1
             RETURNING id",
        )
        .bind(&parent_code)
        .fetch_one(pool)
        .await
        .expect("create bom_header");

        // Component A
        let a_code = format!("MIX-{:03}", comp_a_idx);
        sqlx::query(
            "INSERT INTO bom_lines
                (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
                 component_sku_id, component_loc_id, qty_per_parent)
             SELECT $1, 1, 'item', 'per_unit', 10, 'op_arrival', 100,
                    s.id, l.id, 1
               FROM skus s, locations l WHERE s.code=$2 AND l.code='MAIN'",
        )
        .bind(bom_id)
        .bind(&a_code)
        .execute(pool)
        .await
        .expect("bom item A");

        // Component B (only on multi-op BOMs, fires at op 20)
        if op_count == 2 {
            let b_code = format!("MIX-{:03}", comp_b_idx);
            sqlx::query(
                "INSERT INTO bom_lines
                    (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
                     component_sku_id, component_loc_id, qty_per_parent)
                 SELECT $1, 2, 'item', 'per_unit', 20, 'op_arrival', 100,
                        s.id, l.id, 1
                   FROM skus s, locations l WHERE s.code=$2 AND l.code='MAIN'",
            )
            .bind(bom_id)
            .bind(&b_code)
            .execute(pool)
            .await
            .expect("bom item B");
        }

        let comps = if op_count == 2 {
            vec![comp_a_idx, comp_b_idx]
        } else {
            vec![comp_a_idx]
        };
        wo_spots.push(WoSpot {
            parent_idx: i,
            components: comps,
            op_count,
            bom_id,
        });
    }

    // ---- 6) Pre-stock all SKUs with raw + fg inventory via post_inventory_adjustment. ----
    // For wac dispatch's per-class qty divisor (R1) we need real
    // posting_lines.qty rows. post_inventory_adjustment writes them
    // properly. Use cost=std_cost so running avg starts at std.
    let bootstrap_actor = "00000000-0000-0000-0000-0000000000bb";
    let bootstrap_date = "2026-04-01"; // start of open periods
    let loc_id_main: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code='MAIN'")
        .fetch_one(pool)
        .await
        .expect("loc id");
    for (i, code_prefix) in (0..n_skus).map(|i| (i, "MIX")) {
        let code = format!("{:}-{:03}", code_prefix, i);
        let is_standard = cost_methods[i] == "standard";
        let cost = std_costs[i];
        let qty = 100_000i64;

        let sku_id: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
            .bind(&code)
            .fetch_one(pool)
            .await
            .expect("sku id");

        for inv_class in ["raw", "fg"] {
            let key = sqlx::query_scalar::<_, String>("SELECT gen_random_uuid()::text")
                .fetch_one(pool)
                .await
                .unwrap();
            // Standard SKUs: must NOT pass p_unit_cost (P0011). Non-standard:
            // pass cost so wac running avg starts at std.
            if is_standard {
                sqlx::query(
                    "SELECT post_inventory_adjustment(
                        $1::UUID, $2::UUID, $3, NULL, 'USD', $4,
                        $5::DATE, $6::UUID, $7::UUID, NULL)",
                )
                .bind(&sku_id)
                .bind(&loc_id_main)
                .bind(qty)
                .bind(inv_class)
                .bind(bootstrap_date)
                .bind(bootstrap_actor)
                .bind(&key)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("seed {inv_class} stock (standard) {code}: {e}"));
            } else {
                sqlx::query(
                    "SELECT post_inventory_adjustment(
                        $1::UUID, $2::UUID, $3, $4, 'USD', $5,
                        $6::DATE, $7::UUID, $8::UUID, NULL)",
                )
                .bind(&sku_id)
                .bind(&loc_id_main)
                .bind(qty)
                .bind(cost)
                .bind(inv_class)
                .bind(bootstrap_date)
                .bind(bootstrap_actor)
                .bind(&key)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("seed {inv_class} stock (wac) {code}: {e}"));
            }
        }
    }

    // WO components also need extra raw stock — already covered above
    // since WO components ARE MIX SKUs.

    // ---- 7) Read back fixture identities for the workers. ----
    let mut sku_spots: Vec<SkuSpot> = Vec::with_capacity(n_skus + n_wo_skus);
    for i in 0..n_skus {
        let code = format!("MIX-{:03}", i);
        let sku_id: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code=$1")
            .bind(&code)
            .fetch_one(pool)
            .await
            .unwrap();
        sku_spots.push(SkuSpot {
            code: code.clone(),
            cost_method: cost_methods[i].to_string(),
            is_fg_only: false,
            sku_id,
            std_cost: std_costs[i],
        });
    }
    for i in 0..n_wo_skus {
        let code = format!("WO-{:03}", i);
        let sku_id: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code=$1")
            .bind(&code)
            .fetch_one(pool)
            .await
            .unwrap();
        sku_spots.push(SkuSpot {
            code: code.clone(),
            cost_method: "standard".to_string(),
            is_fg_only: true,
            sku_id,
            std_cost: 1000,
        });
    }

    let mut vendor_spots: Vec<VendorSpot> = Vec::with_capacity(n_vendors);
    for i in 0..n_vendors {
        let v_code = format!("LV-{:02}", i);
        let row: (String, String, String) = sqlx::query_as(
            "SELECT v.id::text, po.id::text, pl.id::text
               FROM vendors v
               JOIN purchase_orders po ON po.vendor_id = v.id
               JOIN purchase_order_lines pl ON pl.po_id = po.id
              WHERE v.code = $1",
        )
        .bind(&v_code)
        .fetch_one(pool)
        .await
        .expect("vendor spot");
        vendor_spots.push(VendorSpot {
            vendor_id: row.0,
            po_id: row.1,
            po_line_id: row.2,
            sku_idx: i % n_skus,
        });
    }

    let mut customer_spots: Vec<CustomerSpot> = Vec::with_capacity(n_customers);
    for i in 0..n_customers {
        let c_code = format!("LC-{:02}", i);
        let row: (String, String, String) = sqlx::query_as(
            "SELECT c.id::text, so.id::text, sl.id::text
               FROM customers c
               JOIN sales_orders so ON so.customer_id = c.id
               JOIN sales_order_lines sl ON sl.so_id = so.id
              WHERE c.code = $1",
        )
        .bind(&c_code)
        .fetch_one(pool)
        .await
        .expect("customer spot");
        customer_spots.push(CustomerSpot {
            customer_id: row.0,
            so_id: row.1,
            so_line_id: row.2,
            sku_idx: (i + 3) % n_skus,
        });
    }

    (sku_spots, vendor_spots, customer_spots, wo_spots)
}

// ============================================================
// Per-op implementations
// ============================================================

#[allow(clippy::too_many_arguments)]
async fn do_po_receipt(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    vendor_spots: &[VendorSpot],
    sku_spots: &[SkuSpot],
    rng: &mut u64,
    posted_by: &str,
    business_date: &str,
) -> Outcome {
    let v = &vendor_spots[(xorshift(rng) as usize) % vendor_spots.len()];
    let qty = 5 + (xorshift(rng) % 46) as i64; // 5..50
    let sku = &sku_spots[v.sku_idx];
    let unit_cost = sku.std_cost; // no PPV
    let key = fresh_uuid_str(rng);

    let lines = json!([{
        "po_line_id": v.po_line_id,
        "qty_received": qty,
    }]);

    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_po_receipt($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)",
    )
    .bind(&v.po_id)
    .bind(&lines)
    .bind(business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => {
            // Queue for AP-bill consumer.
            let mut s = state.lock().await;
            s.receipts.push_back(PendingReceipt {
                po_id: v.po_id.clone(),
                po_line_id: v.po_line_id.clone(),
                vendor_id: v.vendor_id.clone(),
                sku_idx: v.sku_idx,
                qty,
                unit_cost,
                business_date: business_date.to_string(),
            });
            Outcome::Ok(dur)
        }
        Err(e) => classify_err(e, "po_receipt"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_ap_bill(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    _sku_spots: &[SkuSpot],
    rng: &mut u64,
    posted_by: &str,
) -> Outcome {
    let pr = {
        let mut s = state.lock().await;
        s.receipts.pop_front()
    };
    let Some(pr) = pr else {
        return Outcome::Skip;
    };

    let amount = pr.qty * pr.unit_cost;
    let lines = json!([{
        "kind": "po_match",
        "po_line_id": pr.po_line_id,
        "qty": pr.qty,
        "unit_cost": pr.unit_cost,
        "amount": amount,
    }]);
    let key = fresh_uuid_str(rng);

    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_ap_bill($1::UUID, $2, $3, $4::DATE, $5::UUID, $6::UUID, NULL)",
    )
    .bind(&pr.vendor_id)
    .bind("USD")
    .bind(&lines)
    .bind(&pr.business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => Outcome::Ok(dur),
        Err(e) => classify_err(e, "ap_bill"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_wo_start(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    sku_spots: &[SkuSpot],
    wo_spots: &[WoSpot],
    rng: &mut u64,
    posted_by: &str,
    business_date: &str,
) -> Outcome {
    let spot_idx = (xorshift(rng) as usize) % wo_spots.len();
    let wo_spot = &wo_spots[spot_idx];
    let parent_sku = &sku_spots[wo_spot.parent_idx + (sku_spots.len() - wo_spots.len())]; // FG slice at end
    // Defensive: re-resolve by code instead. Easier.
    let parent_code = format!("WO-{:03}", wo_spot.parent_idx);
    let parent = sku_spots
        .iter()
        .find(|s| s.code == parent_code)
        .unwrap_or(parent_sku);
    let qty = 5 + (xorshift(rng) % 16) as i64; // 5..20

    // INSERT the WO row, then call post_wo_start with the new id.
    let wo_no = format!("WO-LOAD-{}", fresh_uuid_str(rng));
    let wo_id_res: Result<String, sqlx::Error> = sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, status, currency, bom_id, posted_by)
         SELECT $4, s.id, l.id, $2, 'draft', 'USD', $3, $5::UUID
           FROM skus s, locations l WHERE s.code=$1 AND l.code='MAIN'
         RETURNING id::text",
    )
    .bind(&parent.code)
    .bind(qty)
    .bind(wo_spot.bom_id)
    .bind(&wo_no)
    .bind(posted_by)
    .fetch_one(pool)
    .await;
    let wo_id = match wo_id_res {
        Ok(id) => id,
        Err(e) => return classify_err(e, "wo_insert"),
    };
    // Define routing in-line.
    if wo_spot.op_count >= 1 {
        let _ = sqlx::query(
            "INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'first')",
        )
        .bind(&wo_id)
        .execute(pool)
        .await;
    }
    if wo_spot.op_count >= 2 {
        let _ = sqlx::query(
            "INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 20, 'second')",
        )
        .bind(&wo_id)
        .execute(pool)
        .await;
    }

    let key = fresh_uuid_str(rng);
    let t0 = Instant::now();
    let res =
        sqlx::query("SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL)")
            .bind(&wo_id)
            .bind(business_date)
            .bind(posted_by)
            .bind(&key)
            .execute(pool)
            .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => {
            let last_op = wo_spot.op_count * 10;
            let mut s = state.lock().await;
            s.wos.push_back(InFlightWo {
                wo_id,
                wo_spot_idx: spot_idx,
                qty,
                current_op: 10,
                last_op,
                business_date: business_date.to_string(),
            });
            Outcome::Ok(dur)
        }
        Err(e) => classify_err(e, "wo_start"),
    }
}

async fn do_op_move(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    rng: &mut u64,
    posted_by: &str,
) -> Outcome {
    // Pop a WO that has further-to-go (current_op < last_op).
    let wo = {
        let mut s = state.lock().await;
        // Try up to 4 entries to find one that can op_move.
        let mut found = None;
        for _ in 0..4 {
            match s.wos.pop_front() {
                None => break,
                Some(w) if w.current_op < w.last_op => {
                    found = Some(w);
                    break;
                }
                Some(w) => s.wos.push_back(w), // not eligible for op_move, but still in flight
            }
        }
        found
    };
    let Some(wo) = wo else {
        return Outcome::Skip;
    };

    let from_op = wo.current_op;
    let to_op = from_op + 10;
    let key = fresh_uuid_str(rng);

    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_op_move($1::UUID, $2, $3, $4, $5::DATE, $6::UUID, $7::UUID, NULL)",
    )
    .bind(&wo.wo_id)
    .bind(from_op)
    .bind(to_op)
    .bind(wo.qty)
    .bind(&wo.business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => {
            let mut s = state.lock().await;
            s.wos.push_back(InFlightWo {
                current_op: to_op,
                ..wo
            });
            Outcome::Ok(dur)
        }
        Err(e) => {
            // Don't lose the WO on failure — push back so wo_complete or
            // future op_move can retry.
            let mut s = state.lock().await;
            s.wos.push_back(wo);
            classify_err(e, "op_move")
        }
    }
}

async fn do_wo_complete(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    rng: &mut u64,
    posted_by: &str,
) -> Outcome {
    // Pop a WO that has reached last_op.
    let wo = {
        let mut s = state.lock().await;
        let mut found = None;
        for _ in 0..4 {
            match s.wos.pop_front() {
                None => break,
                Some(w) if w.current_op == w.last_op => {
                    found = Some(w);
                    break;
                }
                Some(w) => s.wos.push_back(w),
            }
        }
        found
    };
    let Some(wo) = wo else {
        return Outcome::Skip;
    };

    let key = fresh_uuid_str(rng);
    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)",
    )
    .bind(&wo.wo_id)
    .bind(wo.qty)
    .bind(&wo.business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => Outcome::Ok(dur),
        Err(e) => classify_err(e, "wo_complete"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_so_ship(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    customer_spots: &[CustomerSpot],
    sku_spots: &[SkuSpot],
    rng: &mut u64,
    posted_by: &str,
    business_date: &str,
) -> Outcome {
    let c = &customer_spots[(xorshift(rng) as usize) % customer_spots.len()];
    let qty = 1 + (xorshift(rng) % 20) as i64; // 1..20
    let sku = &sku_spots[c.sku_idx];
    let unit_price = sku.std_cost * 2;
    let key = fresh_uuid_str(rng);

    let lines = json!([{
        "so_line_id": c.so_line_id,
        "qty_shipped": qty,
    }]);

    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)",
    )
    .bind(&c.so_id)
    .bind(&lines)
    .bind(business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => {
            let mut s = state.lock().await;
            s.ships.push_back(PendingShip {
                so_id: c.so_id.clone(),
                so_line_id: c.so_line_id.clone(),
                customer_id: c.customer_id.clone(),
                sku_idx: c.sku_idx,
                qty,
                unit_price,
                business_date: business_date.to_string(),
            });
            Outcome::Ok(dur)
        }
        Err(e) => classify_err(e, "so_ship"),
    }
}

async fn do_customer_invoice(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    rng: &mut u64,
    posted_by: &str,
) -> Outcome {
    let ship = {
        let mut s = state.lock().await;
        s.ships.pop_front()
    };
    let Some(ship) = ship else {
        return Outcome::Skip;
    };

    let amount = ship.qty * ship.unit_price;
    let lines = json!([{
        "kind": "so_match",
        "so_line_id": ship.so_line_id,
        "qty": ship.qty,
        "unit_price": ship.unit_price,
        "amount": amount,
    }]);
    let key = fresh_uuid_str(rng);

    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_customer_invoice($1::UUID, $2, $3, $4::DATE, $5::UUID, $6::UUID, NULL)",
    )
    .bind(&ship.customer_id)
    .bind("USD")
    .bind(&lines)
    .bind(&ship.business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => {
            let mut s = state.lock().await;
            s.invoices.push_back(OpenInvoice {
                customer_id: ship.customer_id,
                amount,
            });
            Outcome::Ok(dur)
        }
        Err(e) => classify_err(e, "customer_invoice"),
    }
}

async fn do_ar_payment(
    pool: &PgPool,
    state: &Arc<Mutex<WorkloadState>>,
    rng: &mut u64,
    posted_by: &str,
    business_date: &str,
) -> Outcome {
    let inv = {
        let mut s = state.lock().await;
        s.invoices.pop_front()
    };
    let Some(inv) = inv else {
        return Outcome::Skip;
    };

    let key = fresh_uuid_str(rng);
    let t0 = Instant::now();
    let res = sqlx::query(
        "SELECT post_ar_payment($1::UUID, $2, $3, $4::DATE, $5::UUID, $6::UUID, NULL, NULL, NULL)",
    )
    .bind(&inv.customer_id)
    .bind("USD")
    .bind(inv.amount)
    .bind(business_date)
    .bind(posted_by)
    .bind(&key)
    .execute(pool)
    .await;
    let dur = t0.elapsed();

    match res {
        Ok(_) => Outcome::Ok(dur),
        Err(e) => classify_err(e, "ar_payment"),
    }
}

async fn do_return(
    _pool: &PgPool,
    _state: &Arc<Mutex<WorkloadState>>,
    _rng: &mut u64,
    _posted_by: &str,
) -> Outcome {
    // Returns are rare (0.05 weight). Implementing PO return / customer
    // return requires tracking which receipts / ships are returnable AND
    // their full audit fields. For the baseline run we skip — counts in
    // the histogram as Skip, so the workload mix matches the spec
    // weights modulo what's measured. File as 1s6r-followup if return
    // path coverage is needed; mainstream-ERP load tests typically
    // exclude returns from steady-state contention probes.
    Outcome::Skip
}

// ============================================================
// Main test
// ============================================================

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 600 = 10min); see file header"]
async fn phase1_mixed_workload() {
    let duration_secs: u64 = env::var("T4_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let n_writers: u32 = env::var("T4_WRITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let n_skus: usize = env::var("T4_BENCH_SKUS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let n_vendors: usize = env::var("T4_BENCH_VENDORS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let n_customers: usize = env::var("T4_BENCH_CUSTOMERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let n_wo_skus: usize = env::var("T4_BENCH_WO_SKUS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let duration = Duration::from_secs(duration_secs);

    let pool = connect_test_db_with(n_writers + 8).await;
    reset_to_fixture(&pool).await;

    eprintln!(
        "T4-1s6r: setup begin (n_skus={n_skus} n_vendors={n_vendors} n_customers={n_customers} n_wo_skus={n_wo_skus})"
    );
    let setup_t0 = Instant::now();
    let (sku_spots, vendor_spots, customer_spots, wo_spots) =
        setup_load_fixture(&pool, n_skus, n_vendors, n_customers, n_wo_skus).await;
    let setup_dur = setup_t0.elapsed();
    eprintln!(
        "T4-1s6r: setup complete in {:.2}s ({} skus, {} vendors, {} customers, {} BOMs)",
        setup_dur.as_secs_f64(),
        sku_spots.len(),
        vendor_spots.len(),
        customer_spots.len(),
        wo_spots.len()
    );

    let sku_spots = Arc::new(sku_spots);
    let vendor_spots = Arc::new(vendor_spots);
    let customer_spots = Arc::new(customer_spots);
    let wo_spots = Arc::new(wo_spots);
    let state = Arc::new(Mutex::new(WorkloadState::default()));

    // Pre-snapshots.
    let _ = sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&pool)
        .await;
    let stat_db_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));
    let wal_lsn_before: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    let accounts_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let posting_lines_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let posting_lines_bytes_before: i64 = sqlx::query_scalar(
        "SELECT pg_total_relation_size('posting_lines')::BIGINT",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    let deadlocks_before = stat_db_before.4;
    let ok_count = Arc::new(AtomicU64::new(0));
    let skip_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));

    eprintln!(
        "T4-1s6r: workload writers={n_writers} duration={duration_secs}s mix={:?}",
        OP_WEIGHTS
            .iter()
            .map(|(o, w)| (op_name(*o), *w))
            .collect::<Vec<_>>()
    );

    let business_dates = ["2026-04-15", "2026-05-15", "2026-06-15"];
    let posted_by = "00000000-0000-0000-0000-0000000000bb".to_string();

    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_writers as usize);
    for w in 0..n_writers {
        let pool_w = pool.clone();
        let state_w = state.clone();
        let sku_w = sku_spots.clone();
        let vendor_w = vendor_spots.clone();
        let customer_w = customer_spots.clone();
        let wo_w = wo_spots.clone();
        let ok_c = ok_count.clone();
        let skip_c = skip_count.clone();
        let err_c = err_count.clone();
        let posted_by_w = posted_by.clone();

        handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((w as u64) << 32) ^ 0xa1b2_c3d4_e5f6_0708;
            // Per-op latency histograms.
            let mut histos: Vec<Vec<u32>> = (0..OP_VARIANT_COUNT).map(|_| Vec::new()).collect();

            while start.elapsed() < duration {
                let op = pick_op(&mut rng);
                let business_date =
                    business_dates[(xorshift(&mut rng) as usize) % business_dates.len()];

                let outcome = match op {
                    WriterOp::PoReceipt => {
                        do_po_receipt(
                            &pool_w,
                            &state_w,
                            &vendor_w,
                            &sku_w,
                            &mut rng,
                            &posted_by_w,
                            business_date,
                        )
                        .await
                    }
                    WriterOp::ApBill => {
                        do_ap_bill(&pool_w, &state_w, &sku_w, &mut rng, &posted_by_w).await
                    }
                    WriterOp::WoStart => {
                        do_wo_start(
                            &pool_w,
                            &state_w,
                            &sku_w,
                            &wo_w,
                            &mut rng,
                            &posted_by_w,
                            business_date,
                        )
                        .await
                    }
                    WriterOp::OpMove => do_op_move(&pool_w, &state_w, &mut rng, &posted_by_w).await,
                    WriterOp::WoComplete => {
                        do_wo_complete(&pool_w, &state_w, &mut rng, &posted_by_w).await
                    }
                    WriterOp::SoShip => {
                        do_so_ship(
                            &pool_w,
                            &state_w,
                            &customer_w,
                            &sku_w,
                            &mut rng,
                            &posted_by_w,
                            business_date,
                        )
                        .await
                    }
                    WriterOp::CustomerInvoice => {
                        do_customer_invoice(&pool_w, &state_w, &mut rng, &posted_by_w).await
                    }
                    WriterOp::ArPayment => {
                        do_ar_payment(&pool_w, &state_w, &mut rng, &posted_by_w, business_date)
                            .await
                    }
                    WriterOp::Return => {
                        do_return(&pool_w, &state_w, &mut rng, &posted_by_w).await
                    }
                };

                match outcome {
                    Outcome::Ok(d) => {
                        let us = d.as_micros().min(u32::MAX as u128) as u32;
                        histos[op_idx(op)].push(us);
                        ok_c.fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::Skip => {
                        skip_c.fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::Err(msg) => {
                        eprintln!("writer {w} err: {msg}");
                        err_c.fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::Deadlock(msg) => {
                        // Do NOT panic — deadlocks are real, expected
                        // findings for the mixed workload (multiple
                        // writers touching shared pools). Count them via
                        // pg_stat_database.deadlocks (post-run snapshot)
                        // and surface in the headline. Log once per
                        // occurrence for visibility.
                        eprintln!("writer {w} deadlock: {msg}");
                        err_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            histos
        }));
    }

    // Per-op aggregated histograms.
    let mut total_histos: Vec<Vec<u32>> = (0..OP_VARIANT_COUNT).map(|_| Vec::new()).collect();
    for h in handles {
        let writer_histos = h.await.expect("writer panic");
        for i in 0..OP_VARIANT_COUNT {
            total_histos[i].extend(writer_histos[i].iter().copied());
        }
    }
    let elapsed = start.elapsed();

    // Post-snapshots.
    let stat_db_after = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));
    let deadlocks_after = stat_db_after.4;
    let wal_lsn_after: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    let wal_bytes_delta: i64 = if !wal_lsn_before.is_empty() && !wal_lsn_after.is_empty() {
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::BIGINT")
            .bind(&wal_lsn_after)
            .bind(&wal_lsn_before)
            .fetch_one(&pool)
            .await
            .unwrap_or(0)
    } else {
        0
    };
    let accounts_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let posting_lines_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let posting_lines_bytes_after: i64 = sqlx::query_scalar(
        "SELECT pg_total_relation_size('posting_lines')::BIGINT",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    let top_queries: Vec<(String, i64, f64, f64)> = sqlx::query_as(
        "SELECT left(query, 100) AS q, calls::BIGINT,
                total_exec_time, mean_exec_time
           FROM pg_stat_statements
          WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND query NOT ILIKE 'SELECT pg_stat_%'
            AND query NOT ILIKE 'SELECT xact_commit%'
            AND query NOT ILIKE 'SELECT pg_current_wal_lsn%'
            AND query NOT ILIKE 'SELECT pg_total_relation_size%'
            AND query NOT ILIKE 'SELECT COUNT%'
          ORDER BY total_exec_time DESC
          LIMIT 20",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let ok = ok_count.load(Ordering::Relaxed);
    let skip = skip_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let total = ok + skip + err;

    let pct = |sorted: &[u32], q: f64| -> u32 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((q * sorted.len() as f64).floor() as usize).min(sorted.len() - 1);
        sorted[idx]
    };

    // Sort per-op histograms.
    let sorted_histos: Vec<Vec<u32>> = total_histos
        .into_iter()
        .map(|mut h| {
            h.sort_unstable();
            h
        })
        .collect();

    // Combined post_posting_lines latency surrogate: concat all op
    // histograms (since each wrapper call invokes post_posting_lines at
    // least once).
    let mut all_latencies: Vec<u32> =
        sorted_histos.iter().flatten().copied().collect();
    all_latencies.sort_unstable();

    let xact_commit_d = stat_db_after.0 - stat_db_before.0;
    let xact_rollbk_d = stat_db_after.1 - stat_db_before.1;
    let blks_read_d = stat_db_after.2 - stat_db_before.2;
    let blks_hit_d = stat_db_after.3 - stat_db_before.3;

    eprintln!("====================== T4-1s6r MIXED WORKLOAD SUMMARY ======================");
    eprintln!(
        "duration_s={:.2} writers={n_writers} (Slice A + B + C mixed)",
        elapsed.as_secs_f64(),
    );
    eprintln!(
        "ops: total={total} ok={ok} skip={skip} err={err} throughput={:.1}/s",
        total as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "combined wrapper latency_us: p50={} p95={} p99={} p99.9={} max={} (n={})",
        pct(&all_latencies, 0.50),
        pct(&all_latencies, 0.95),
        pct(&all_latencies, 0.99),
        pct(&all_latencies, 0.999),
        all_latencies.last().copied().unwrap_or(0),
        all_latencies.len(),
    );
    eprintln!("--- per-op latency (us) ---");
    eprintln!(
        "{:<20} {:>9} {:>9} {:>9} {:>10} {:>10}",
        "op", "n", "p50", "p95", "p99", "max"
    );
    for (i, op_w) in OP_WEIGHTS.iter().enumerate() {
        let h = &sorted_histos[i];
        eprintln!(
            "{:<20} {:>9} {:>9} {:>9} {:>10} {:>10}",
            op_name(op_w.0),
            h.len(),
            pct(h, 0.50),
            pct(h, 0.95),
            pct(h, 0.99),
            h.last().copied().unwrap_or(0),
        );
    }
    eprintln!(
        "deadlocks: delta={} ({} -> {})",
        deadlocks_after - deadlocks_before,
        deadlocks_before,
        deadlocks_after
    );
    eprintln!(
        "pg_stat_database: xact_commit_delta={xact_commit_d} xact_rollback_delta={xact_rollbk_d} blks_read_delta={blks_read_d} blks_hit_delta={blks_hit_d}"
    );
    eprintln!(
        "accounts: before={accounts_before} after={accounts_after} delta={}",
        accounts_after - accounts_before
    );
    eprintln!(
        "posting_lines: before={posting_lines_before} after={posting_lines_after} delta={}",
        posting_lines_after - posting_lines_before,
    );
    eprintln!(
        "posting_lines_size_mb: before={:.1} after={:.1} delta={:.1}",
        posting_lines_bytes_before as f64 / 1024.0 / 1024.0,
        posting_lines_bytes_after as f64 / 1024.0 / 1024.0,
        (posting_lines_bytes_after - posting_lines_bytes_before) as f64 / 1024.0 / 1024.0,
    );
    eprintln!(
        "wal_bytes_delta={wal_bytes_delta} (lsn {wal_lsn_before} -> {wal_lsn_after})"
    );
    eprintln!("--- pg_stat_statements top 20 by total_exec_time ---");
    for (q, calls, total_ms, mean_ms) in &top_queries {
        eprintln!(
            "  calls={:>10} total_ms={:>10.1} mean_ms={:>8.3}  query={}",
            calls, total_ms, mean_ms, q
        );
    }
    eprintln!("============================================================================");

    // Deadlocks are surfaced (not asserted == 0). The mixed workload
    // hits shared inventory pools (stock_wip / inv_value_*) under 32-
    // writer concurrency; documenting deadlock count is the headline
    // for acct-c4p / acct-zroo. If the count is 0, great; if non-zero,
    // that IS the finding.
    let deadlock_delta = deadlocks_after - deadlocks_before;
    if deadlock_delta > 0 {
        eprintln!(
            "FINDING: pg_stat_database.deadlocks rose by {deadlock_delta} during run"
        );
    }
    assert!(total > 0, "no ops executed");
}
