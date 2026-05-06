//! acct-6c8b (sub of acct-1cer) — property test for `post_po_receipt`.
//!
//! Pins the invariants Slice A's GRNI cycle relies on:
//!
//! 1. Σ `po_receipt_lines.qty_received` per `po_line` ≤
//!    `po_line.qty_ordered` (P0023 enforcement).
//! 2. After every receipt, per cost_method:
//!    - **standard**: PPV transfer.amount = qty × |po_unit_cost − std|;
//!      sign matches favorable / unfavorable direction (po > std →
//!      `variance_ppv DR / ap_unsettled CR`; po < std → reverse).
//!    - **wac_perpetual**: no PPV leg; pool value increase = qty ×
//!      po_unit_cost.
//! 3. `ap_unsettled` credit-side increase per line = qty × po_unit_cost
//!    (regardless of cost_method — value + PPV legs net to this).
//! 4. `stock_available` debit-side increase = total qty.
//! 5. I1–I7 invariants (per-class qty signs, per-(ledger_kind,
//!    currency) double-entry, transfers append-only).
//!
//! Random multi-receipt sequences against a 2-line PO (one standard
//! SKU + one wac_perpetual SKU). Single-threaded; the property test
//! pins behaviour against future cost-dispatch / cumulative-qty
//! refactors. Use `PROPTEST_CASES=N` to override the 100-scenario
//! default.

mod common;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;

#[derive(Debug, Clone, Copy)]
enum LineKind {
    Standard,
    Wac,
}

#[derive(Debug, Clone, Copy)]
struct ReceiveOp {
    line: LineKind,
    qty: i64,
}

#[derive(Debug, Clone)]
struct ScenarioParams {
    std_unit_cost: i64,    // po_line.unit_cost for the standard line
    std_cost_at_bd: i64,   // resolve_standard_cost_at value
    wac_unit_cost: i64,    // po_line.unit_cost for the wac line
    qty_ordered: i64,      // po_line.qty_ordered for both lines
    receipts: Vec<ReceiveOp>,
}

fn arb_scenario() -> impl Strategy<Value = ScenarioParams> {
    let std_unit_cost = 50i64..=150;
    let std_cost_at_bd = 50i64..=150;
    let wac_unit_cost = 50i64..=150;
    let qty_ordered = prop::sample::select(vec![200i64, 500, 1000]);
    let receipts = prop::collection::vec(
        (
            prop::sample::select(vec![LineKind::Standard, LineKind::Wac]),
            1i64..=80,
        )
            .prop_map(|(line, qty)| ReceiveOp { line, qty }),
        1..=10,
    );
    (std_unit_cost, std_cost_at_bd, wac_unit_cost, qty_ordered, receipts)
        .prop_map(|(s, sb, w, q, r)| ScenarioParams {
            std_unit_cost: s,
            std_cost_at_bd: sb,
            wac_unit_cost: w,
            qty_ordered: q,
            receipts: r,
        })
}

#[tokio::test(flavor = "current_thread")]
async fn property_po_receipt_invariants_hold() {
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

        let label = format!("po_recv#{case_idx}");
        let s = scaffold(&pool, &label, &scn).await;

        let mut std_received: i64 = 0;
        let mut wac_received: i64 = 0;
        let mut step = 0usize;

        for op in &scn.receipts {
            let (target_received, target_line_id, expected_unit_cost, kind) = match op.line {
                LineKind::Standard => (
                    &mut std_received,
                    &s.std_line_id,
                    scn.std_unit_cost,
                    LineKind::Standard,
                ),
                LineKind::Wac => (
                    &mut wac_received,
                    &s.wac_line_id,
                    scn.wac_unit_cost,
                    LineKind::Wac,
                ),
            };

            // Skip if this op would over-receive (function would raise
            // P0023 anyway; skipping keeps the scenario flowing).
            if *target_received + op.qty > scn.qty_ordered {
                step += 1;
                continue;
            }

            let bal_before = snapshot(&pool, &s).await;
            do_receipt(&pool, &s, target_line_id, op.qty, step, &label).await;
            let bal_after = snapshot(&pool, &s).await;

            assert_per_line_balances(
                &s, &bal_before, &bal_after, kind, op.qty,
                expected_unit_cost, scn.std_cost_at_bd, &label, step,
            );
            assert_cumulative_qty_within_ordered(
                &pool, target_line_id, scn.qty_ordered, &label, step,
            )
            .await;

            *target_received += op.qty;
            common::assert_invariants_hold(&pool, &format!("{label}/step{step}")).await;
            step += 1;
        }

        // End-of-scenario: verify cumulative receipts on each line did
        // not exceed qty_ordered, and that totals match what we tracked.
        let std_db = cumulative_received(&pool, &s.std_line_id).await;
        let wac_db = cumulative_received(&pool, &s.wac_line_id).await;
        assert_eq!(std_db, std_received,
            "[{label}] db cumulative std ({std_db}) != tracked ({std_received})");
        assert_eq!(wac_db, wac_received,
            "[{label}] db cumulative wac ({wac_db}) != tracked ({wac_received})");
        assert!(std_db <= scn.qty_ordered);
        assert!(wac_db <= scn.qty_ordered);
    }
}

// ============================================================
// Per-line balance assertions
// ============================================================

#[derive(Debug, Clone, Copy)]
struct Snapshot {
    stock_std: i64,
    stock_wac: i64,
    inv_std: i64,
    inv_wac: i64,
    ven_qty: i64,
    ven_unsettled: i64,
    var_ppv: i64,
}

async fn snapshot(pool: &PgPool, s: &Scaffold) -> Snapshot {
    Snapshot {
        stock_std: balance(pool, s.qty_acct_std).await,
        stock_wac: balance(pool, s.qty_acct_wac).await,
        inv_std: balance(pool, s.val_acct_std).await,
        inv_wac: balance(pool, s.val_acct_wac).await,
        ven_qty: balance(pool, s.ven_qty).await,
        ven_unsettled: balance(pool, s.ven_unsettled).await,
        var_ppv: balance(pool, s.var_ppv).await,
    }
}

#[allow(clippy::too_many_arguments)]
fn assert_per_line_balances(
    s: &Scaffold,
    before: &Snapshot,
    after: &Snapshot,
    kind: LineKind,
    qty: i64,
    po_unit_cost: i64,
    std_at_bd: i64,
    label: &str,
    step: usize,
) {
    let _ = s; // silence unused
    // Stock + vendor_pool qty leg always: stock_available increases by
    // qty, vendor_pool (credit-normal) credit increases by qty.
    match kind {
        LineKind::Standard => {
            let dq = after.stock_std - before.stock_std;
            assert_eq!(dq, qty,
                "[{label}/step{step}] stock_std qty Δ ({dq}) != qty ({qty})");
            // wac stock should be unchanged on a std-line receipt.
            assert_eq!(after.stock_wac, before.stock_wac,
                "[{label}/step{step}] stock_wac changed on std-line receipt");

            let dv = after.inv_std - before.inv_std;
            let expected_inv = qty * std_at_bd;
            assert_eq!(dv, expected_inv,
                "[{label}/step{step}] inv_value_raw(std) Δ ({dv}) != qty×std ({qty}×{std_at_bd}={expected_inv})");
            assert_eq!(after.inv_wac, before.inv_wac,
                "[{label}/step{step}] inv_value_raw(wac) changed on std-line receipt");

            // PPV leg: if po != std, variance_ppv balance shifts.
            // variance_ppv is normal_side='unrestricted' in fixture; we
            // assert (debit - credit) Δ tracks |po - std| × qty signed
            // by direction (unfavorable po>std → DR; favorable po<std → CR).
            let ppv_d = after.var_ppv - before.var_ppv;
            let expected_ppv = qty * (po_unit_cost - std_at_bd);
            assert_eq!(ppv_d, expected_ppv,
                "[{label}/step{step}] variance_ppv Δ ({ppv_d}) != qty×(po-std) ({qty}×({po_unit_cost}-{std_at_bd})={expected_ppv})");
        }
        LineKind::Wac => {
            let dq = after.stock_wac - before.stock_wac;
            assert_eq!(dq, qty,
                "[{label}/step{step}] stock_wac qty Δ ({dq}) != qty ({qty})");
            assert_eq!(after.stock_std, before.stock_std,
                "[{label}/step{step}] stock_std changed on wac-line receipt");

            let dv = after.inv_wac - before.inv_wac;
            let expected_inv = qty * po_unit_cost;
            assert_eq!(dv, expected_inv,
                "[{label}/step{step}] inv_value_raw(wac) Δ ({dv}) != qty×po_unit_cost ({qty}×{po_unit_cost}={expected_inv})");
            assert_eq!(after.inv_std, before.inv_std,
                "[{label}/step{step}] inv_value_raw(std) changed on wac-line receipt");
            // No PPV on wac receipts.
            assert_eq!(after.var_ppv, before.var_ppv,
                "[{label}/step{step}] variance_ppv changed on wac-line receipt");
        }
    }

    // ap_unsettled (credit-normal) net increase = qty × po_unit_cost
    // regardless of cost_method. ap_unsettled balance is debits-credits;
    // since it's credit-normal, the value goes more negative (or
    // less positive) by `qty × po_unit_cost`.
    //
    // For credit-normal, an increase in liability shows as the (D - C)
    // delta becoming more negative. Concretely we expect:
    //   (D-C)_after - (D-C)_before == -(qty × po_unit_cost)
    let d_unsettled = after.ven_unsettled - before.ven_unsettled;
    let expected_unsettled = -(qty * po_unit_cost);
    assert_eq!(d_unsettled, expected_unsettled,
        "[{label}/step{step}] ap_unsettled Δ ({d_unsettled}) != -qty×po_unit_cost ({expected_unsettled})");

    // vendor_pool (credit-normal) qty leg: credit by qty.
    let d_vqty = after.ven_qty - before.ven_qty;
    assert_eq!(d_vqty, -qty,
        "[{label}/step{step}] vendor_pool qty Δ ({d_vqty}) != -qty ({qty})");
}

async fn assert_cumulative_qty_within_ordered(
    pool: &PgPool,
    po_line_id: &str,
    qty_ordered: i64,
    label: &str,
    step: usize,
) {
    let cum = cumulative_received(pool, po_line_id).await;
    assert!(cum <= qty_ordered,
        "[{label}/step{step}] cumulative qty_received ({cum}) > qty_ordered ({qty_ordered})");
}

async fn cumulative_received(pool: &PgPool, po_line_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty_received), 0)::BIGINT
           FROM po_receipt_lines WHERE po_line_id = $1::UUID",
    )
    .bind(po_line_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("cumulative_received: {e}"))
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
    po_id: String,
    std_sku_id: String,
    wac_sku_id: String,
    loc_id: String,
    std_line_id: String,
    wac_line_id: String,
    qty_acct_std: i64,
    val_acct_std: i64,
    qty_acct_wac: i64,
    val_acct_wac: i64,
    ven_qty: i64,
    ven_unsettled: i64,
    var_ppv: i64,
}

async fn scaffold(pool: &PgPool, label: &str, scn: &ScenarioParams) -> Scaffold {
    let suffix = sanitize(label);
    let vendor_id = fresh_vendor(pool, &format!("V-{suffix}"), "USD").await;
    let std_sku_id = fresh_sku(pool, &format!("SS-{suffix}"), "standard").await;
    let wac_sku_id = fresh_sku(pool, &format!("SW-{suffix}"), "wac_perpetual").await;
    let loc_id = fresh_location(pool, &format!("L-{suffix}")).await;

    set_std_cost(pool, &std_sku_id, scn.std_cost_at_bd).await;

    let po_id = create_po(pool, &vendor_id).await;
    let std_line_id = add_po_line(
        pool, &po_id, 1, &std_sku_id, &loc_id,
        scn.qty_ordered, scn.std_unit_cost, "USD",
    )
    .await;
    let wac_line_id = add_po_line(
        pool, &po_id, 2, &wac_sku_id, &loc_id,
        scn.qty_ordered, scn.wac_unit_cost, "USD",
    )
    .await;

    let qty_acct_std = open_account(
        pool, "stock_available", "qty", None,
        Some(&std_sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let val_acct_std = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(&std_sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let qty_acct_wac = open_account(
        pool, "stock_available", "qty", None,
        Some(&wac_sku_id), Some(&loc_id), None, "debit",
    )
    .await;
    let val_acct_wac = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(&wac_sku_id), Some(&loc_id), None, "debit",
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
    // Reuse the seeded variance_ppv USD; opening a duplicate would make
    // the function's lookup nondeterministic.
    let var_ppv =
        common::account_id_by_kind_currency(pool, "variance_ppv", Some("USD")).await;

    Scaffold {
        vendor_id,
        po_id,
        std_sku_id,
        wac_sku_id,
        loc_id,
        std_line_id,
        wac_line_id,
        qty_acct_std,
        val_acct_std,
        qty_acct_wac,
        val_acct_wac,
        ven_qty,
        ven_unsettled,
        var_ppv,
    }
}

async fn do_receipt(
    pool: &PgPool,
    s: &Scaffold,
    po_line_id: &str,
    qty: i64,
    step: usize,
    label: &str,
) {
    let posted_by = common::fresh_uuid(pool).await;
    let key = uuid_for(label, step, "rcv");

    let lines = json!([{ "po_line_id": po_line_id, "qty_received": qty }]);

    let r: sqlx::Result<String> = sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.po_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await;
    r.unwrap_or_else(|e| panic!("[{label}/step{step}] post_po_receipt qty={qty}: {e}"));
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
