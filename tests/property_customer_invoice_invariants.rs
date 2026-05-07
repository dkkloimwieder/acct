//! acct-eycl (sub of acct-1cer) — property test for
//! `post_customer_invoice` (mirror of acct-x8so on the AR side).
//!
//! Pins the three-way-match + tolerance-absorption invariants from
//! mig 0090 (acct-7mc) and mig 0091 (acct-nuw7 zero-baseline arm) for
//! `kind='so_match'` invoice lines:
//!
//! 1. Base leg: `ar DR / ar_unsettled CR`, amount = qty ×
//!    `so_line.unit_price` (NOT invoice price — preserves accrual
//!    integrity).
//! 2. Tolerance absorption leg (if `invoice_unit_price ≠
//!    so_unit_price`):
//!    - Invoice > SO: `ar DR / variance_match_tolerance CR`, amount =
//!      qty × (invoice − so) — favorable variance.
//!    - Invoice < SO: `variance_match_tolerance DR / ar CR`, amount =
//!      qty × (so − invoice) — unfavorable variance.
//! 3. Net `ar` debit Δ per line = qty × invoice_unit_price (base +
//!    tolerance composes to invoice price).
//! 4. `ar_unsettled` debit-Δ per line = −qty × so_line.unit_price
//!    (drains at original price).
//! 5. `variance_match_tolerance` Δ in (D − C) = −qty × (invoice − so)
//!    — sign-direction is **opposite** to AP-side because favorable
//!    revenue variance credits the account.
//! 6. Cumulative qty invoiced per so_line stays ≤ qty_shipped (P0040
//!    cumulative-remainder enforcement).
//! 7. I1–I7 invariants throughout.
//!
//! Random `tolerance_pct ∈ {0, 5, 10}` and `invoice_unit_price` drawn
//! from a within-window range; out-of-tolerance rejection is covered
//! by integration tests.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const SO_UNIT_PRICE: i64 = 100;
const QTY_PRE_SHIPPED: i64 = 1_000;
const FG_UNIT_COST: i64 = 60;

#[derive(Debug, Clone, Copy)]
struct InvoiceOp {
    qty: i64,
    invoice_unit_price: i64,
}

#[derive(Debug, Clone)]
struct ScenarioParams {
    tolerance_pct: u8, // 0, 5, or 10
    invoices: Vec<InvoiceOp>,
}

fn arb_scenario() -> impl Strategy<Value = ScenarioParams> {
    let tolerance = prop::sample::select(vec![0u8, 5, 10]);
    tolerance.prop_flat_map(|tol| {
        let low: i64 = (SO_UNIT_PRICE * (100 - tol as i64)) / 100;
        let high: i64 = (SO_UNIT_PRICE * (100 + tol as i64)) / 100;
        let invoices = prop::collection::vec(
            (1i64..=40, low..=high)
                .prop_map(|(qty, invoice_unit_price)| InvoiceOp {
                    qty,
                    invoice_unit_price,
                }),
            1..=8,
        );
        invoices.prop_map(move |b| ScenarioParams {
            tolerance_pct: tol,
            invoices: b,
        })
    })
}

#[tokio::test(flavor = "current_thread")]
async fn property_customer_invoice_invariants_hold() {
    let pool = common::connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_scenario();

    for case_idx in 0..cases {
        common::reset_to_fixture(&pool).await;

        let tree = strategy
            .new_tree(&mut runner)
            .expect("strategy.new_tree");
        let scn: ScenarioParams = tree.current();

        let label = format!("cust_inv#{case_idx}");
        let s = scaffold(&pool, &label, &scn).await;

        // Pre-ship QTY_PRE_SHIPPED units in a single shipment so all
        // invoice lines have an available remainder.
        do_ship(&pool, &s, QTY_PRE_SHIPPED, 0, &label).await;

        let mut cumulative_invoiced: i64 = 0;
        let mut step = 0usize;

        for op in &scn.invoices {
            if cumulative_invoiced + op.qty > QTY_PRE_SHIPPED {
                step += 1;
                continue;
            }
            let inv_uc = if scn.tolerance_pct == 0 {
                SO_UNIT_PRICE
            } else {
                op.invoice_unit_price
            };

            let bal_before = snapshot(&pool, &s).await;
            do_invoice(&pool, &s, op.qty, inv_uc, step + 1, &label).await;
            let bal_after = snapshot(&pool, &s).await;

            assert_per_line_balances(
                &bal_before,
                &bal_after,
                op.qty,
                inv_uc,
                SO_UNIT_PRICE,
                &label,
                step,
            );
            assert_cumulative_invoiced_within_shipped(
                &pool,
                &s.so_line_id,
                QTY_PRE_SHIPPED,
                &label,
                step,
            )
            .await;

            cumulative_invoiced += op.qty;
            common::assert_invariants_hold(&pool, &format!("{label}/step{step}")).await;
            step += 1;
        }

        let db_invoiced = cumulative_invoiced_total(&pool, &s.so_line_id).await;
        assert_eq!(
            db_invoiced, cumulative_invoiced,
            "[{label}] db cumulative invoiced ({db_invoiced}) != tracked ({cumulative_invoiced})"
        );
        assert!(db_invoiced <= QTY_PRE_SHIPPED);
    }
}

// ============================================================
// Per-line balance assertions
// ============================================================

#[derive(Debug, Clone, Copy)]
struct Snapshot {
    ar_unsettled: i64,
    ar: i64,
    var_tol: i64,
}

async fn snapshot(pool: &PgPool, s: &Scaffold) -> Snapshot {
    Snapshot {
        ar_unsettled: balance(pool, s.cust_unsettled).await,
        ar: balance(pool, s.cust_ar).await,
        var_tol: balance(pool, s.var_tol).await,
    }
}

#[allow(clippy::too_many_arguments)]
fn assert_per_line_balances(
    before: &Snapshot,
    after: &Snapshot,
    qty: i64,
    invoice_uc: i64,
    so_uc: i64,
    label: &str,
    step: usize,
) {
    // ar_unsettled is debit-normal. Invoice drains it: credits_total
    // grows by qty × so_uc → (D - C) Δ = -(qty × so_uc).
    let d_unsettled = after.ar_unsettled - before.ar_unsettled;
    let expected_unsettled = -(qty * so_uc);
    assert_eq!(
        d_unsettled, expected_unsettled,
        "[{label}/step{step}] ar_unsettled Δ ({d_unsettled}) != -qty×so_uc ({qty}×{so_uc} = {expected_unsettled})"
    );

    // ar is debit-normal. Net debit Δ = base (qty × so_uc) +
    // tolerance signed Δ (qty × (invoice_uc - so_uc)) = qty × invoice_uc.
    let d_ar = after.ar - before.ar;
    let expected_ar = qty * invoice_uc;
    assert_eq!(
        d_ar, expected_ar,
        "[{label}/step{step}] ar Δ ({d_ar}) != qty×invoice_uc ({qty}×{invoice_uc} = {expected_ar})"
    );

    // variance_match_tolerance Δ in (D - C):
    //   invoice > so → variance is CR'd → Δ = -qty × (invoice - so)
    //   invoice < so → variance is DR'd → Δ = +qty × (so - invoice)
    //                                       = -qty × (invoice - so)
    // Both cases collapse to: Δ = qty × (so_uc - invoice_uc).
    let d_var = after.var_tol - before.var_tol;
    let expected_var = qty * (so_uc - invoice_uc);
    assert_eq!(
        d_var, expected_var,
        "[{label}/step{step}] variance_match_tolerance Δ ({d_var}) != qty×(so-invoice) ({qty}×({so_uc}-{invoice_uc}) = {expected_var})"
    );
}

async fn assert_cumulative_invoiced_within_shipped(
    pool: &PgPool,
    so_line_id: &str,
    qty_shipped: i64,
    label: &str,
    step: usize,
) {
    let cum = cumulative_invoiced_total(pool, so_line_id).await;
    assert!(
        cum <= qty_shipped,
        "[{label}/step{step}] cumulative qty invoiced ({cum}) > qty_shipped ({qty_shipped})"
    );
}

async fn cumulative_invoiced_total(pool: &PgPool, so_line_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty), 0)::BIGINT
           FROM customer_invoice_lines
          WHERE so_line_id = $1::UUID AND kind = 'so_match'",
    )
    .bind(so_line_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("cumulative_invoiced: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

// ============================================================
// Scaffold + ops
// ============================================================

#[allow(dead_code)]
struct Scaffold {
    customer_id: String,
    sku_id: String,
    loc_id: String,
    so_id: String,
    so_line_id: String,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
    cust_ar: i64,
    var_tol: i64,
    cogs_acct: i64,
    revenue_acct: i64,
    creation_void_qty: i64,
    creation_void_val: i64,
}

async fn scaffold(pool: &PgPool, label: &str, scn: &ScenarioParams) -> Scaffold {
    let suffix = sanitize(label);
    let customer_id =
        fresh_customer(pool, &format!("C-{suffix}"), "USD", scn.tolerance_pct).await;
    // standard SKU keeps the seed-fg path simple; cost dispatch is
    // not the focus here.
    let sku_id = fresh_sku(pool, &format!("S-{suffix}"), "standard").await;
    set_std_cost(pool, &sku_id, FG_UNIT_COST).await;
    let loc_id = fresh_location(pool, &format!("L-{suffix}")).await;

    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(
        pool,
        &so_id,
        1,
        &sku_id,
        &loc_id,
        QTY_PRE_SHIPPED * 2,
        SO_UNIT_PRICE,
        "USD",
    )
    .await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None,
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"),
        Some(&sku_id), Some(&loc_id), None, "debit",
    )
    .await;
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
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"),
        None, None, Some(&customer_id), "debit",
    )
    .await;

    let cogs_acct = common::account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let revenue_acct =
        common::account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let var_tol = common::account_id_by_kind_currency(
        pool, "variance_match_tolerance", Some("USD"),
    )
    .await;
    let creation_void_qty =
        common::account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val =
        common::account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    // Seed FG inventory so post_so_ship can deplete from a real pool.
    seed_fg(pool, qty_acct, val_acct, creation_void_qty, creation_void_val,
            QTY_PRE_SHIPPED, QTY_PRE_SHIPPED * FG_UNIT_COST, label).await;

    Scaffold {
        customer_id,
        sku_id,
        loc_id,
        so_id,
        so_line_id,
        qty_acct,
        val_acct,
        cust_qty,
        cust_unsettled,
        cust_ar,
        var_tol,
        cogs_acct,
        revenue_acct,
        creation_void_qty,
        creation_void_val,
    }
}

async fn seed_fg(
    pool: &PgPool,
    qty_acct: i64,
    val_acct: i64,
    void_qty: i64,
    void_val: i64,
    qty: i64,
    total_value: i64,
    label: &str,
) {
    let posted_by = common::fresh_uuid(pool).await;
    let doc_id = common::fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"prop_seed",
         "document_id": doc_id,
         "debit_account_id": qty_acct,
         "credit_account_id": void_qty,
         "amount": qty, "qty": qty,
         "business_date": "2026-04-15",
         "idempotency_key": uuid_for(label, 0, "seed_qty"),
         "posted_by": posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"prop_seed",
         "document_id": doc_id,
         "debit_account_id": val_acct,
         "credit_account_id": void_val,
         "amount": total_value, "qty": qty,
         "business_date": "2026-04-15",
         "idempotency_key": uuid_for(label, 0, "seed_val"),
         "posted_by": posted_by},
    ]);
    common::call_post_posting_lines(pool, mint, false)
        .await
        .unwrap_or_else(|e| panic!("[{label}] seed_fg: {e}"));
}

async fn do_ship(pool: &PgPool, s: &Scaffold, qty: i64, step: usize, label: &str) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "ship");
    let lines = json!([{ "so_line_id": s.so_line_id, "qty_shipped": qty }]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("[{label}/step{step}] post_so_ship: {e}"));
}

async fn do_invoice(
    pool: &PgPool,
    s: &Scaffold,
    qty: i64,
    invoice_uc: i64,
    step: usize,
    label: &str,
) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "inv");
    let amount = qty * invoice_uc;
    let lines = json!([{
        "kind": "so_match",
        "so_line_id": s.so_line_id,
        "qty": qty,
        "unit_price": invoice_uc,
        "amount": amount,
    }]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_customer_invoice($1::UUID, 'USD'::CHAR(3), $2::JSONB,
                                       '2026-04-25'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.customer_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("[{label}/step{step}] post_customer_invoice: {e}"));
}

// ============================================================
// Local SQL helpers
// ============================================================

async fn fresh_customer(
    pool: &PgPool,
    code: &str,
    currency: &str,
    tolerance_pct: u8,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency, unit_price_tolerance_pct)
         VALUES ($1, $2, $3, $4) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .bind(currency)
    .bind::<f64>(tolerance_pct as f64)
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

async fn set_std_cost(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = common::fresh_uuid(pool).await;
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
    .unwrap_or_else(|e| panic!("set_std_cost {sku_id}: {e}"));
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
