//! acct-x8so (sub of acct-1cer) — property test for `post_ap_bill`.
//!
//! Pins the three-way-match + tolerance-absorption invariants from
//! mig 0090 (acct-7mc) and mig 0091 (acct-nuw7 zero-baseline arm).
//! For every successful `kind='po_match'` bill line:
//!
//! 1. Base leg: `ap_unsettled DR / ap CR`, amount = qty ×
//!    `po_line.unit_cost` (NOT bill `unit_cost` — preserves accrual
//!    integrity).
//! 2. Tolerance absorption leg (if `bill_unit_cost ≠ po_unit_cost`):
//!    - Bill > PO: `variance_match_tolerance DR / ap CR`, amount =
//!      qty × (bill − po).
//!    - Bill < PO: `ap DR / variance_match_tolerance CR`, amount =
//!      qty × (po − bill).
//! 3. Cumulative qty billed per po_line stays ≤ qty_received (P0024
//!    cumulative-remainder enforcement).
//! 4. I1–I7 invariants throughout.
//!
//! Random `tolerance_pct ∈ {0, 5, 10}` and `bill_unit_cost` drawn
//! from a window guaranteed to be within tolerance for the chosen
//! vendor. The "happy-path" property test does NOT exercise out-of-
//! tolerance rejection — that's covered by integration tests in
//! `tests/ap_bill.rs`.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const PO_UNIT_COST: i64 = 100;
const QTY_PRE_RECEIVED: i64 = 1_000;

#[derive(Debug, Clone, Copy)]
struct BillOp {
    qty: i64,
    bill_unit_cost: i64,
}

#[derive(Debug, Clone)]
struct ScenarioParams {
    tolerance_pct: u8, // 0, 5, or 10
    bills: Vec<BillOp>,
}

fn arb_scenario() -> impl Strategy<Value = ScenarioParams> {
    let tolerance = prop::sample::select(vec![0u8, 5, 10]);
    tolerance.prop_flat_map(|tol| {
        // For tolerance pct, allow bill_unit_cost in [po*(100-tol)/100,
        // po*(100+tol)/100]. With po=100 that's [100-tol, 100+tol].
        let low: i64 = (PO_UNIT_COST * (100 - tol as i64)) / 100;
        let high: i64 = (PO_UNIT_COST * (100 + tol as i64)) / 100;
        let bills = prop::collection::vec(
            (1i64..=40, low..=high)
                .prop_map(|(qty, bill_unit_cost)| BillOp { qty, bill_unit_cost }),
            1..=8,
        );
        bills.prop_map(move |b| ScenarioParams {
            tolerance_pct: tol,
            bills: b,
        })
    })
}

#[tokio::test(flavor = "current_thread")]
async fn property_ap_bill_invariants_hold() {
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

        let label = format!("ap_bill#{case_idx}");
        let s = scaffold(&pool, &label, &scn).await;

        // Pre-receive QTY_PRE_RECEIVED units so all bill qtys have
        // a remainder to draw from. Single receipt for simplicity.
        do_receipt(&pool, &s, QTY_PRE_RECEIVED, 0, &label).await;

        let mut cumulative_billed: i64 = 0;
        let mut step = 0usize;

        for op in &scn.bills {
            // Skip if this bill would over-bill the po_line.
            if cumulative_billed + op.qty > QTY_PRE_RECEIVED {
                step += 1;
                continue;
            }
            // tolerance_pct=0 must use exact po_unit_cost.
            // The strategy already constrains the window to single
            // value when tol=0, but be defensive.
            let bill_uc = if scn.tolerance_pct == 0 {
                PO_UNIT_COST
            } else {
                op.bill_unit_cost
            };

            let bal_before = snapshot(&pool, &s).await;
            do_bill(&pool, &s, op.qty, bill_uc, step + 1, &label).await;
            let bal_after = snapshot(&pool, &s).await;

            assert_per_line_balances(
                &bal_before,
                &bal_after,
                op.qty,
                bill_uc,
                PO_UNIT_COST,
                &label,
                step,
            );
            assert_cumulative_billed_within_received(
                &pool,
                &s.po_line_id,
                QTY_PRE_RECEIVED,
                &label,
                step,
            )
            .await;

            cumulative_billed += op.qty;
            common::assert_invariants_hold(&pool, &format!("{label}/step{step}")).await;
            step += 1;
        }

        // End-of-scenario cumulative check.
        let db_billed = cumulative_billed_total(&pool, &s.po_line_id).await;
        assert_eq!(
            db_billed, cumulative_billed,
            "[{label}] db cumulative billed ({db_billed}) != tracked ({cumulative_billed})"
        );
        assert!(db_billed <= QTY_PRE_RECEIVED);
    }
}

// ============================================================
// Per-line balance assertions
// ============================================================

#[derive(Debug, Clone, Copy)]
struct Snapshot {
    ap_unsettled: i64,
    ap: i64,
    var_tol: i64,
}

async fn snapshot(pool: &PgPool, s: &Scaffold) -> Snapshot {
    Snapshot {
        ap_unsettled: balance(pool, s.ven_unsettled).await,
        ap: balance(pool, s.ven_ap).await,
        var_tol: balance(pool, s.var_tol).await,
    }
}

#[allow(clippy::too_many_arguments)]
fn assert_per_line_balances(
    before: &Snapshot,
    after: &Snapshot,
    qty: i64,
    bill_uc: i64,
    po_uc: i64,
    label: &str,
    step: usize,
) {
    // ap_unsettled is credit-normal. Bill drains it: debits_total grows
    // by qty × po_uc. So (D - C)_after - (D - C)_before == +qty × po_uc.
    let d_unsettled = after.ap_unsettled - before.ap_unsettled;
    let expected_unsettled = qty * po_uc;
    assert_eq!(
        d_unsettled, expected_unsettled,
        "[{label}/step{step}] ap_unsettled Δ ({d_unsettled}) != qty×po_uc ({qty}×{po_uc}={expected_unsettled})"
    );

    // ap is credit-normal. Bill credits it by qty × bill_uc (base leg
    // qty × po_uc + tolerance leg signed delta = qty × bill_uc).
    // Credit-normal credit increases → (D - C) becomes more negative.
    let d_ap = after.ap - before.ap;
    let expected_ap = -(qty * bill_uc);
    assert_eq!(
        d_ap, expected_ap,
        "[{label}/step{step}] ap Δ ({d_ap}) != -qty×bill_uc ({expected_ap})"
    );

    // variance_match_tolerance Δ = qty × (bill_uc - po_uc).
    // Sign: bill > po → debit increases (Δ > 0); bill < po → credit
    // increases (Δ < 0). variance_match_tolerance is unrestricted in
    // fixture; (D - C) Δ tracks bill - po directly.
    let d_var = after.var_tol - before.var_tol;
    let expected_var = qty * (bill_uc - po_uc);
    assert_eq!(
        d_var, expected_var,
        "[{label}/step{step}] variance_match_tolerance Δ ({d_var}) != qty×(bill-po) ({qty}×({bill_uc}-{po_uc})={expected_var})"
    );
}

async fn assert_cumulative_billed_within_received(
    pool: &PgPool,
    po_line_id: &str,
    qty_received: i64,
    label: &str,
    step: usize,
) {
    let cum = cumulative_billed_total(pool, po_line_id).await;
    assert!(
        cum <= qty_received,
        "[{label}/step{step}] cumulative qty_billed ({cum}) > qty_received ({qty_received})"
    );
}

async fn cumulative_billed_total(pool: &PgPool, po_line_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty), 0)::BIGINT
           FROM vendor_bill_lines
          WHERE po_line_id = $1::UUID AND kind = 'po_match'",
    )
    .bind(po_line_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("cumulative_billed: {e}"))
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
    vendor_id: String,
    sku_id: String,
    loc_id: String,
    po_id: String,
    po_line_id: String,
    qty_acct: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_unsettled: i64,
    ven_ap: i64,
    var_tol: i64,
    var_ppv: i64,
}

async fn scaffold(pool: &PgPool, label: &str, scn: &ScenarioParams) -> Scaffold {
    let suffix = sanitize(label);
    let vendor_id = fresh_vendor(
        pool,
        &format!("V-{suffix}"),
        "USD",
        scn.tolerance_pct,
    )
    .await;
    // wac_perpetual SKU keeps the PPV path out of the property — it
    // would muddy the per-event balance assertions on ap. The bill
    // function dispatches off `po_line.unit_cost` regardless of cost
    // method, so this choice is incidental to what we're testing.
    let sku_id = fresh_sku(pool, &format!("S-{suffix}"), "wac_perpetual").await;
    let loc_id = fresh_location(pool, &format!("L-{suffix}")).await;

    let po_id = create_po(pool, &vendor_id).await;
    let po_line_id = add_po_line(
        pool,
        &po_id,
        1,
        &sku_id,
        &loc_id,
        QTY_PRE_RECEIVED * 2,
        PO_UNIT_COST,
        "USD",
    )
    .await;

    let qty_acct = open_account(
        pool,
        "stock_available",
        "qty",
        None,
        Some(&sku_id),
        Some(&loc_id),
        None,
        "debit",
    )
    .await;
    let val_acct = open_account(
        pool,
        "inv_value_raw",
        "value",
        Some("USD"),
        Some(&sku_id),
        Some(&loc_id),
        None,
        "debit",
    )
    .await;
    let ven_qty = open_account(
        pool,
        "vendor_pool",
        "qty",
        None,
        None,
        None,
        Some(&vendor_id),
        "credit",
    )
    .await;
    let ven_unsettled = open_account(
        pool,
        "ap_unsettled",
        "value",
        Some("USD"),
        None,
        None,
        Some(&vendor_id),
        "credit",
    )
    .await;
    let ven_ap = open_account(
        pool,
        "ap",
        "value",
        Some("USD"),
        None,
        None,
        Some(&vendor_id),
        "credit",
    )
    .await;
    // Reuse seeded variance_* accounts to avoid duplicate rows.
    let var_tol =
        common::account_id_by_kind_currency(pool, "variance_match_tolerance", Some("USD"))
            .await;
    let var_ppv = common::account_id_by_kind_currency(pool, "variance_ppv", Some("USD")).await;

    Scaffold {
        vendor_id,
        sku_id,
        loc_id,
        po_id,
        po_line_id,
        qty_acct,
        val_acct,
        ven_qty,
        ven_unsettled,
        ven_ap,
        var_tol,
        var_ppv,
    }
}

async fn do_receipt(pool: &PgPool, s: &Scaffold, qty: i64, step: usize, label: &str) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "rcv");
    let lines = json!([{ "po_line_id": s.po_line_id, "qty_received": qty }]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.po_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("[{label}/step{step}] post_po_receipt: {e}"));
}

async fn do_bill(
    pool: &PgPool,
    s: &Scaffold,
    qty: i64,
    bill_uc: i64,
    step: usize,
    label: &str,
) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "bill");
    let amount = qty * bill_uc;
    let lines = json!([{
        "kind": "po_match",
        "po_line_id": s.po_line_id,
        "qty": qty,
        "unit_cost": bill_uc,
        "amount": amount,
    }]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_ap_bill($1::UUID, 'USD'::CHAR(3), $2::JSONB,
                              '2026-04-20'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.vendor_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("[{label}/step{step}] post_ap_bill: {e}"));
}

// ============================================================
// Local SQL helpers
// ============================================================

async fn fresh_vendor(
    pool: &PgPool,
    code: &str,
    currency: &str,
    tolerance_pct: u8,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency, unit_cost_tolerance_pct)
         VALUES ($1, $2, $3, $4) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vend {code}"))
    .bind(currency)
    .bind::<f64>(tolerance_pct as f64)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert vendor {code}: {e}"))
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

#[allow(clippy::too_many_arguments)]
async fn add_po_line(
    pool: &PgPool,
    po_id: &str,
    line_no: i32,
    sku_id: &str,
    loc_id: &str,
    qty_ordered: i64,
    unit_cost: i64,
    currency: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, $7)
         RETURNING id::text",
    )
    .bind(po_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty_ordered)
    .bind(unit_cost)
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("add_po_line: {e}"))
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
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID,
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
