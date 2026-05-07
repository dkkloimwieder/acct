//! acct-ct0p (sub of acct-1cer) — property test for the AP9 / R7
//! audit-trail-vs-ledger invariant on `post_so_ship`.
//!
//! Mig 0091 (acct-5prc) closed the lock-gap that allowed
//! `so_shipment_lines.unit_cost` to drift from the COGS-leg
//! `transfer.amount / transfer.qty` under concurrent receipt + ship on
//! `wac_perpetual` SKUs. The persisted snapshot must always equal the
//! dispatcher-resolved cost on the ledger.
//!
//! Strategy: per scenario, bootstrap a wac_perpetual SKU + customer +
//! vendor + SO + so_line, then run a random sequence of `Receive` /
//! `Ship` ops. After each ship, assert the per-line audit field matches
//! the COGS transfer; after the full sequence, assert global I1-I7.
//!
//! `proptest`'s default sequencing is sequential; this test does not
//! attempt to *induce* concurrency — single-threaded the values are
//! computed by the same code path, so an audit/ledger mismatch implies
//! a logic bug in the function body itself. Real concurrency stress
//! belongs in a `load_*` binary.
//!
//! Use `PROPTEST_CASES=N` to override the 100-scenario default.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone)]
enum Op {
    Receive { qty: i64, unit_cost: i64 },
    Ship { qty: i64 },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Receive: qty 1..=200, unit_cost 1..=50.
        2 => (1i64..=200, 1i64..=50)
            .prop_map(|(qty, unit_cost)| Op::Receive { qty, unit_cost }),
        // Ship: qty 1..=80. Skipped at runtime if pool would go empty.
        1 => (1i64..=80).prop_map(|qty| Op::Ship { qty }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 2..=12)
}

#[tokio::test(flavor = "current_thread")]
async fn property_so_ship_audit_matches_ledger_invariant() {
    let pool = common::connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_op_seq();

    for case_idx in 0..cases {
        common::reset_to_fixture(&pool).await;

        let tree = strategy
            .new_tree(&mut runner)
            .expect("strategy.new_tree");
        let ops: Vec<Op> = tree.current();

        let label = format!("ap9_so_ship#{case_idx}");
        let s = scaffold_wac(&pool, &label).await;

        // Track running pool so Ship ops respect availability.
        // pool_qty: signed qty SUM on inv_value_fg
        // pool_value: value balance on inv_value_fg
        let mut pool_qty: i64 = 0;
        let mut pool_value: i64 = 0;
        let mut cumulative_shipped: i64 = 0;
        let qty_ordered: i64 = 100_000; // headroom for over-ship gate

        let mut step = 0usize;
        let mut performed_at_least_one_ship = false;

        for op in &ops {
            match *op {
                Op::Receive { qty, unit_cost } => {
                    do_receipt(&pool, &s, qty, unit_cost, step, &label).await;
                    pool_qty += qty;
                    pool_value += qty * unit_cost;
                }
                Op::Ship { qty } => {
                    if pool_qty < qty || cumulative_shipped + qty > qty_ordered {
                        // Skip — would deplete pool below 0 or overshoot
                        // so_line.qty_ordered (P0038).
                        step += 1;
                        continue;
                    }
                    let expected_unit_cost = pool_value / pool_qty;
                    let cogs_amount = qty * expected_unit_cost;

                    do_ship(&pool, &s, qty, step, &label).await;

                    assert_audit_matches_cogs_for_latest_ship(&pool, &s, &label).await;

                    pool_qty -= qty;
                    pool_value -= cogs_amount;
                    cumulative_shipped += qty;
                    performed_at_least_one_ship = true;
                }
            }
            step += 1;
        }

        if !performed_at_least_one_ship {
            // Pure-receipt scenario; still verify global invariants.
            common::assert_invariants_hold(&pool, &format!("{label}/recv-only")).await;
            continue;
        }

        common::assert_invariants_hold(&pool, &label).await;
    }
}

// ============================================================
// Audit-trail-matches-ledger invariant
// ============================================================

/// Walks every `so_shipment_lines` row for `s.so_line_id` and asserts
/// the persisted `unit_cost` equals the COGS leg's `amount / qty` on
/// the matching `transfers` row. AP9 / R7 invariant.
async fn assert_audit_matches_cogs_for_latest_ship(
    pool: &PgPool,
    s: &Scaffold,
    label: &str,
) {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT id::text, qty_shipped, unit_cost
           FROM so_shipment_lines
          WHERE so_line_id = $1::UUID
          ORDER BY id",
    )
    .bind(&s.so_line_id)
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("[{label}] query so_shipment_lines: {e}"));

    for (line_id, qty_shipped, audit_unit_cost) in rows {
        let (ledger_amount, ledger_qty): (i64, i64) = sqlx::query_as(
            "SELECT amount, qty
               FROM posting_lines
              WHERE document_kind  = 'so_ship'
                AND reason         = 'so_ship'
                AND document_line_id = $1::UUID
                AND debit_account_id  = $2
                AND credit_account_id = $3",
        )
        .bind(&line_id)
        .bind(s.cogs_acct)
        .bind(s.val_acct)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| {
            panic!("[{label}] query cogs transfer for line {line_id}: {e}")
        });

        assert_eq!(
            ledger_qty, qty_shipped,
            "[{label}/line {line_id}] ledger qty ({ledger_qty}) != audit qty_shipped ({qty_shipped})"
        );
        assert_eq!(
            ledger_amount,
            qty_shipped * audit_unit_cost,
            "[{label}/line {line_id}] ledger amount ({ledger_amount}) != qty_shipped × audit unit_cost ({qty_shipped} × {audit_unit_cost})"
        );
        assert_eq!(
            ledger_amount / ledger_qty,
            audit_unit_cost,
            "[{label}/line {line_id}] ledger amount/qty ({ledger_amount}/{ledger_qty} = {}) != audit unit_cost ({audit_unit_cost})",
            ledger_amount / ledger_qty
        );
    }
}

// ============================================================
// Scaffold + ops
// ============================================================

#[allow(dead_code)]
struct Scaffold {
    vendor_id: String,
    customer_id: String,
    sku_id: String,
    loc_id: String,
    so_id: String,
    so_line_id: String,
    po_id: String,

    // accounts
    qty_acct: i64,        // stock_available(sku, loc)
    val_acct: i64,        // inv_value_fg(sku, loc, USD)
    inv_raw_acct: i64,    // inv_value_raw(sku, loc, USD) — receipt lands here
    stock_raw_acct: i64,  // same stock_available used for raw + fg classes
    cogs_acct: i64,       // cogs(USD) — from fixture
    cust_qty: i64,        // customer_pool(customer)
    cust_unsettled: i64,  // ar_unsettled(customer, USD)
    revenue_acct: i64,    // revenue(USD) — from fixture
    ven_qty: i64,         // vendor_pool(vendor)
    ven_unsettled: i64,   // ap_unsettled(vendor, USD)
    ven_ap: i64,          // ap(vendor, USD)
}

async fn scaffold_wac(pool: &PgPool, label: &str) -> Scaffold {
    // wac_perpetual SKU. po_receipt deposits at po_unit_cost (no PPV);
    // so_ship reads inv_value_fg running avg.
    //
    // CRITICAL: for `wac_perpetual`, Slice A's `post_po_receipt` deposits
    // into `inv_value_raw`, not `inv_value_fg`. The property test ships
    // FG, so we need a step that *moves* qty + value from raw → fg.
    // Phase 0 has no transfer-order workflow, so we land directly on
    // inv_value_fg via cycle_count_adj seedings. That keeps the test
    // narrow on so_ship's dispatcher behavior.

    let suffix = sanitize(label);
    let vendor_id = fresh_vendor(pool, &format!("V-{suffix}"), "USD").await;
    let customer_id = fresh_customer(pool, &format!("C-{suffix}"), "USD").await;
    let sku_id = fresh_sku(pool, &format!("S-{suffix}"), "wac_perpetual").await;
    let loc_id = fresh_location(pool, &format!("L-{suffix}")).await;

    let po_id = create_po(pool, &vendor_id).await;
    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(
        pool,
        &so_id,
        1,
        &sku_id,
        &loc_id,
        100_000,
        10,
        "USD",
    )
    .await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None,
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    // For this test inv_value_fg + inv_value_raw share the same
    // stock_available qty pool (per-class signed qty divisor lives on
    // the value account's transfers, not on stock_available).
    let val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"),
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let inv_raw_acct = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let stock_raw_acct = qty_acct;

    let cust_qty = open_account(
        pool, "customer_pool", "qty", None,
        None, None, Some(&customer_id), "debit",
    )
    .await;
    let cust_unsettled = open_account(
        pool, "ar_unsettled", "value", Some("USD"),
        None, None, Some(&customer_id), "debit",
    )
    .await;

    let ven_qty = open_account(
        pool, "vendor_pool", "qty", None,
        None, None, Some(&vendor_id), "credit",
    )
    .await;
    let ven_unsettled = open_account(
        pool, "ap_unsettled", "value", Some("USD"),
        None, None, Some(&vendor_id), "credit",
    )
    .await;
    let ven_ap = open_account(
        pool, "ap", "value", Some("USD"),
        None, None, Some(&vendor_id), "credit",
    )
    .await;

    let cogs_acct = common::account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let revenue_acct = common::account_id_by_kind_currency(pool, "revenue", Some("USD")).await;

    Scaffold {
        vendor_id,
        customer_id,
        sku_id,
        loc_id,
        so_id,
        so_line_id,
        po_id,
        qty_acct,
        val_acct,
        inv_raw_acct,
        stock_raw_acct,
        cogs_acct,
        cust_qty,
        cust_unsettled,
        revenue_acct,
        ven_qty,
        ven_unsettled,
        ven_ap,
    }
}

/// Lands `qty` units at `unit_cost` directly into `inv_value_fg` via a
/// `cycle_count_adj` mint (the simplest op that exercises the same
/// `transfers.qty` per-class signed SUM that `post_so_ship` reads
/// post-mig-0091). This stands in for an inflow path because Phase 0
/// has no raw→FG transfer workflow.
///
/// The op-name `Receive` in the strategy is descriptive only — what
/// matters for the AP9/R7 invariant is that the FG pool's qty + value
/// move correctly so the next ship's dispatcher reads the post-update
/// average.
async fn do_receipt(
    pool: &PgPool,
    s: &Scaffold,
    qty: i64,
    unit_cost: i64,
    step: usize,
    label: &str,
) {
    let void_qty = common::account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = common::account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let posted_by = common::fresh_uuid(pool).await;
    let amount = qty * unit_cost;

    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"prop_seed",
         "document_id": common::fresh_uuid(pool).await,
         "debit_account_id": s.qty_acct,
         "credit_account_id": void_qty,
         "amount": qty, "qty": qty,
         "business_date": "2026-04-15",
         "idempotency_key": uuid_for(label, step, "rcv_qty"),
         "posted_by": posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"prop_seed",
         "document_id": common::fresh_uuid(pool).await,
         "debit_account_id": s.val_acct,
         "credit_account_id": void_val,
         "amount": amount, "qty": qty,
         "business_date": "2026-04-15",
         "idempotency_key": uuid_for(label, step, "rcv_val"),
         "posted_by": posted_by},
    ]);
    let r = common::call_post_posting_lines(pool, mint, false).await;
    r.unwrap_or_else(|e| panic!("[{label}/step {step}] receipt mint: {e}"));
}

async fn do_ship(
    pool: &PgPool,
    s: &Scaffold,
    qty: i64,
    step: usize,
    label: &str,
) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "ship");

    let lines = json!([
        {"so_line_id": s.so_line_id, "qty_shipped": qty}
    ]);

    let r: sqlx::Result<String> = sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await;
    r.unwrap_or_else(|e| panic!("[{label}/step {step}] post_so_ship qty={qty}: {e}"));
}

// ============================================================
// Local SQL helpers
// ============================================================

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vend {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert vendor {code}: {e}"))
}

async fn fresh_customer(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert customer {code}: {e}"))
}

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
    .unwrap_or_else(|e| panic!("insert loc {code}: {e}"))
}

async fn create_po(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_po: {e}"))
}

async fn create_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_so: {e}"))
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
    currency: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, $7, 0)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("add_so_line: {e}"))
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id,
             counterparty_id, normal_side)
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6::UUID,
                 $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open_account {kind}: {e}"))
}

// ============================================================
// Misc utilities
// ============================================================

fn sanitize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect()
}

fn uuid_for(label: &str, step: usize, kind: &str) -> String {
    let raw = format!("{label}/{step}/{kind}");
    let h = hash(raw.as_bytes());
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        h.0,
        (h.1 >> 16) as u16,
        (h.1 & 0xfff) as u16,
        (0x8000 | ((h.2 >> 16) & 0x3fff)) as u16,
        h.3 & 0xffff_ffff_ffff,
    )
}

fn hash(b: &[u8]) -> (u32, u32, u64, u64) {
    let mut h0: u64 = 0xcbf29ce484222325;
    for &x in b {
        h0 ^= x as u64;
        h0 = h0.wrapping_mul(0x100000001b3);
    }
    let h1 = h0.wrapping_mul(0x9e3779b97f4a7c15);
    let h2 = h0.rotate_left(17).wrapping_mul(0x94d049bb133111eb);
    let h3 = h0.rotate_right(13).wrapping_mul(0xc6a4a7935bd1e995);
    (h0 as u32, h1 as u32, h2, h3)
}
