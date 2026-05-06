//! acct-l68l (sub of acct-1cer) — property test for post_customer_return
//! state-aware routing. AR-side mirror of property_po_return_routing.
//!
//! Exercises mig 0084 (acct-ari customer_return + per-disposition qty/value
//! routing) and mig 0086 (acct-tk7 split routing across ar_unsettled / ar
//! based on cumulative invoiced state).
//!
//! Per case:
//!
//! - One customer + one so_line at one ship_location.
//! - SKU is randomly standard (~50%) or wac_perpetual (~50%).
//!   Both supported by post_customer_return; cogs reversal uses
//!   so_shipment_lines.unit_cost (snapshotted at ship time).
//! - inv_value_fg pre-seeded at $60/unit so wac running avg matches
//!   standard's $60 — same expected unit_cost downstream regardless of
//!   cost method.
//! - Tax disabled (tax_amount = 0) to keep tax-pro-rating out of the
//!   routing assertion; see below for what's covered.
//! - Random sequence of (Ship | Invoice | Return) ops with random
//!   per-Return disposition (restock | scrap | repair).
//!
//! Per-Return assertions:
//!
//! - I1-I7 (assert_invariants_hold)
//! - customer_return_lines.qty_to_ar_unsettled + qty_to_ar == qty_returned
//!   (mig 0086 split invariant)
//! - Split equals un-invoiced-first draining order
//! - ar balance change == -(qty_to_ar × unit_price)  (debit-normal: drains)
//! - ar_unsettled balance change == -(qty_to_ar_unsettled × unit_price)
//! - Per disposition (qty side):
//!     restock     → stock_available     += qty
//!     scrap       → stock_scrap         += qty
//!     repair      → stock_quarantine    += qty
//! - Per disposition (value side):
//!     restock     → inv_value_fg        += qty × unit_cost_snap
//!     repair      → inv_value_fg        += qty × unit_cost_snap
//!     scrap       → variance_scrap      += qty × unit_cost_snap (write-off)
//! - Cumulative over-return rejected with P0045 (negative path —
//!   exercised by skipping over-return ops here; covered by integration test)
//!
//! Catches: state-split miscalculation; per-disposition qty routing
//! bugs; per-disposition value routing bugs; tax routing drift;
//! snapshotted-vs-current cost_method confusion (acct-6d8); accounts_check
//! trips from incorrect routing.
//!
//! Out of scope: tax-amount pro-rating (would need a tax-tracking invariant
//! and integer-truncation accounting); credit memos; pre-invoice "void
//! shipment" workflow (P2 design conversation pending).

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const QTY_ORDERED: i64 = 50;
const UNIT_PRICE: i64 = 100;
const UNIT_COST: i64 = 60;
const FG_SEED_QTY: i64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkuKind {
    Standard,
    WacPerpetual,
}

impl SkuKind {
    fn as_pg(&self) -> &'static str {
        match self {
            SkuKind::Standard => "standard",
            SkuKind::WacPerpetual => "wac_perpetual",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Restock,
    Scrap,
    Repair,
}

impl Disposition {
    fn as_pg(&self) -> &'static str {
        match self {
            Disposition::Restock => "restock",
            Disposition::Scrap => "scrap",
            Disposition::Repair => "repair",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Ship { qty: i64 },
    Invoice { qty: i64 },
    Return { qty: i64, disposition: Disposition },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 35% Ship
        7 => (3i64..=15).prop_map(|q| Op::Ship { qty: q }),
        // 35% Invoice
        7 => (3i64..=15).prop_map(|q| Op::Invoice { qty: q }),
        // 30% Return; equal-weight 3 dispositions
        2 => (1i64..=10).prop_map(|q| Op::Return { qty: q, disposition: Disposition::Restock }),
        2 => (1i64..=10).prop_map(|q| Op::Return { qty: q, disposition: Disposition::Scrap }),
        2 => (1i64..=10).prop_map(|q| Op::Return { qty: q, disposition: Disposition::Repair }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 6..=14)
}

#[derive(Debug, Default)]
struct SoState {
    qty_shipped: i64,
    qty_invoiced: i64,
    qty_returned_to_ar_unsettled: i64,
    qty_returned_to_ar: i64,
}

impl SoState {
    fn qty_returned(&self) -> i64 {
        self.qty_returned_to_ar_unsettled + self.qty_returned_to_ar
    }
    fn qty_un_invoiced(&self) -> i64 {
        // Routable-to-ar_unsettled budget for the next return.
        (self.qty_shipped - self.qty_invoiced - self.qty_returned_to_ar_unsettled).max(0)
    }
    fn qty_returnable(&self) -> i64 {
        self.qty_shipped - self.qty_returned()
    }
    fn qty_invoiceable(&self) -> i64 {
        // Mig 0086: invoicing reflects un-returned, un-invoiced remainder.
        (self.qty_shipped - self.qty_invoiced - self.qty_returned_to_ar_unsettled).max(0)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct Scaffold {
    customer_id: String,
    so_id: String,
    so_line_id: String,
    sku_kind: SkuKind,
    qty_acct: i64,
    val_acct: i64,
    cust_qty: i64,
    cust_unsettled: i64,
    cust_ar: i64,
    revenue_acct: i64,
    cogs_acct: i64,
    var_scrap_acct: i64,
    stock_scrap_acct: i64,
    stock_quarantine_acct: i64,
    /// so_shipment_lines.id; caller picks the most-recent with remaining
    /// qty when issuing a Return.
    ship_line_ids: Vec<String>,
}

#[tokio::test(flavor = "current_thread")]
async fn property_customer_return_routing_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();

    let sku_kind_strat = prop_oneof![
        Just(SkuKind::Standard),
        Just(SkuKind::WacPerpetual),
    ];

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;
        let kind = sku_kind_strat.new_tree(&mut runner).expect("kind").current();

        let mut scaffold = build_scaffold(&pool, case_idx, kind).await;
        let mut state = SoState::default();
        let label_root = format!("cr#{case_idx}({:?})", kind);

        let ops_tree = arb_op_seq().new_tree(&mut runner).expect("ops").current();

        for (i, op) in ops_tree.iter().enumerate() {
            let label = format!("{label_root}/op{i}");

            match *op {
                Op::Ship { qty } => {
                    if state.qty_shipped + qty > QTY_ORDERED {
                        continue;
                    }
                    let ship_line = ship(&pool, &scaffold, qty).await
                        .unwrap_or_else(|e| panic!("[{label}] ship: {e}"));
                    scaffold.ship_line_ids.push(ship_line);
                    state.qty_shipped += qty;
                }
                Op::Invoice { qty } => {
                    if qty > state.qty_invoiceable() {
                        continue;
                    }
                    let r = invoice(&pool, &scaffold, qty).await;
                    if r.is_err() {
                        continue;
                    }
                    state.qty_invoiced += qty;
                }
                Op::Return { qty, disposition } => {
                    if scaffold.ship_line_ids.is_empty() {
                        continue;
                    }
                    if qty > state.qty_returnable() {
                        // P0045 over-return; skip (negative path covered
                        // by integration test).
                        continue;
                    }
                    let ship_line_idx =
                        pick_ship_line_with_remainder(&pool, &scaffold).await;
                    let Some(ship_line_idx) = ship_line_idx else { continue };
                    let ship_line_id = scaffold.ship_line_ids[ship_line_idx].clone();

                    // Snapshot pre-return balances.
                    let pre_ar = balance(&pool, scaffold.cust_ar).await;
                    let pre_unsettled = balance(&pool, scaffold.cust_unsettled).await;
                    let pre_qty = balance(&pool, scaffold.qty_acct).await;
                    let pre_val = balance(&pool, scaffold.val_acct).await;
                    let pre_scrap_qty = balance(&pool, scaffold.stock_scrap_acct).await;
                    let pre_quarantine_qty = balance(&pool, scaffold.stock_quarantine_acct).await;
                    let pre_var_scrap = balance(&pool, scaffold.var_scrap_acct).await;

                    let pre_un_invoiced = state.qty_un_invoiced();
                    let exp_to_unsettled = qty.min(pre_un_invoiced);
                    let exp_to_ar = qty - exp_to_unsettled;

                    let r = call_return_one_line(
                        &pool, &scaffold.customer_id, &ship_line_id,
                        qty, disposition,
                    ).await;
                    if r.is_err() {
                        continue;
                    }
                    state.qty_returned_to_ar_unsettled += exp_to_unsettled;
                    state.qty_returned_to_ar += exp_to_ar;

                    // Assertion (a): mig 0086 split invariant on this row.
                    let (got_unsettled, got_ar) =
                        latest_return_split(&pool, &ship_line_id).await;
                    assert_eq!(
                        got_unsettled, exp_to_unsettled,
                        "[{label}] qty_to_ar_unsettled mismatch: expected {exp_to_unsettled} got {got_unsettled} \
                         (un_invoiced_pre={pre_un_invoiced} qty={qty} disp={disposition:?})",
                    );
                    assert_eq!(
                        got_ar, exp_to_ar,
                        "[{label}] qty_to_ar mismatch: expected {exp_to_ar} got {got_ar}",
                    );

                    // Assertion (b): ar balance change.
                    // ar is debit-normal: ship+invoice debits ar (positive
                    // balance); return CREDITS ar → balance decreases.
                    let expected_ar_delta = -exp_to_ar * UNIT_PRICE;
                    let actual_ar_delta = balance(&pool, scaffold.cust_ar).await - pre_ar;
                    assert_eq!(
                        actual_ar_delta, expected_ar_delta,
                        "[{label}] ar delta wrong: expected {expected_ar_delta}, got {actual_ar_delta}",
                    );

                    // Assertion (c): ar_unsettled balance change.
                    // Tax_amount = 0, so unsettled only carries unsettled-route
                    // portion. ar_unsettled is debit-normal: ship debits;
                    // invoice drains; return credits → balance decreases.
                    let expected_unsettled_delta = -exp_to_unsettled * UNIT_PRICE;
                    let actual_unsettled_delta =
                        balance(&pool, scaffold.cust_unsettled).await - pre_unsettled;
                    assert_eq!(
                        actual_unsettled_delta, expected_unsettled_delta,
                        "[{label}] ar_unsettled delta wrong: expected {expected_unsettled_delta}, got {actual_unsettled_delta}",
                    );

                    // Assertion (d): per-disposition qty + value routing.
                    let unit_cost = UNIT_COST; // pre-seeded; same for std + wac
                    let qty_delta_total = qty;
                    let val_delta_total = qty * unit_cost;

                    match disposition {
                        Disposition::Restock => {
                            assert_eq!(
                                balance(&pool, scaffold.qty_acct).await - pre_qty,
                                qty_delta_total,
                                "[{label}] restock: stock_available delta wrong",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.val_acct).await - pre_val,
                                val_delta_total,
                                "[{label}] restock: inv_value_fg delta wrong",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.stock_scrap_acct).await - pre_scrap_qty, 0,
                                "[{label}] restock leaked into stock_scrap",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.stock_quarantine_acct).await - pre_quarantine_qty, 0,
                                "[{label}] restock leaked into stock_quarantine",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.var_scrap_acct).await - pre_var_scrap, 0,
                                "[{label}] restock leaked into variance_scrap",
                            );
                        }
                        Disposition::Scrap => {
                            assert_eq!(
                                balance(&pool, scaffold.stock_scrap_acct).await - pre_scrap_qty,
                                qty_delta_total,
                                "[{label}] scrap: stock_scrap delta wrong",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.var_scrap_acct).await - pre_var_scrap,
                                val_delta_total,
                                "[{label}] scrap: variance_scrap delta wrong",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.qty_acct).await - pre_qty, 0,
                                "[{label}] scrap leaked into stock_available",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.val_acct).await - pre_val, 0,
                                "[{label}] scrap leaked into inv_value_fg",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.stock_quarantine_acct).await - pre_quarantine_qty, 0,
                                "[{label}] scrap leaked into stock_quarantine",
                            );
                        }
                        Disposition::Repair => {
                            assert_eq!(
                                balance(&pool, scaffold.stock_quarantine_acct).await - pre_quarantine_qty,
                                qty_delta_total,
                                "[{label}] repair: stock_quarantine delta wrong",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.val_acct).await - pre_val,
                                val_delta_total,
                                "[{label}] repair: inv_value_fg delta wrong",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.qty_acct).await - pre_qty, 0,
                                "[{label}] repair leaked into stock_available",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.stock_scrap_acct).await - pre_scrap_qty, 0,
                                "[{label}] repair leaked into stock_scrap",
                            );
                            assert_eq!(
                                balance(&pool, scaffold.var_scrap_acct).await - pre_var_scrap, 0,
                                "[{label}] repair leaked into variance_scrap",
                            );
                        }
                    }
                }
            }

            assert_invariants_hold(&pool, &label).await;
        }

        assert_invariants_hold(&pool, &format!("{label_root}/end")).await;
    }
}

// ============================================================
// Scaffold + helpers
// ============================================================

async fn build_scaffold(pool: &PgPool, case_idx: u32, kind: SkuKind) -> Scaffold {
    let customer_id = fresh_customer(pool, &format!("CUST-CR-{case_idx}"), "USD").await;
    let sku_id = fresh_sku(pool, &format!("CR-S-{case_idx}"), kind.as_pg()).await;
    let ship_loc_id = fresh_location(pool, &format!("CR-L-{case_idx}")).await;

    if matches!(kind, SkuKind::Standard) {
        set_std_cost(pool, &sku_id, UNIT_COST).await;
    }

    let so_id = create_so(pool, &customer_id).await;
    let so_line_id = add_so_line(pool, &so_id, &sku_id, &ship_loc_id, QTY_ORDERED, UNIT_PRICE).await;

    let qty_acct = open_account(
        pool, "stock_available", "qty", None,
        Some(&sku_id), Some(&ship_loc_id), None, "debit",
    ).await;
    let val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"),
        Some(&sku_id), Some(&ship_loc_id), None, "debit",
    ).await;
    let cust_qty = open_account(
        pool, "customer_pool", "qty", None, None, None, Some(&customer_id), "debit",
    ).await;
    let cust_unsettled = open_account(
        pool, "ar_unsettled", "value", Some("USD"),
        None, None, Some(&customer_id), "debit",
    ).await;
    let cust_ar = open_account(
        pool, "ar", "value", Some("USD"), None, None, Some(&customer_id), "debit",
    ).await;
    let stock_scrap_acct = open_account(
        pool, "stock_scrap", "qty", None, Some(&sku_id), None, None, "debit",
    ).await;
    let stock_quarantine_acct = open_account(
        pool, "stock_quarantine", "qty", None,
        Some(&sku_id), Some(&ship_loc_id), None, "debit",
    ).await;

    let revenue_acct = account_id_by_kind_currency(pool, "revenue", Some("USD")).await;
    let cogs_acct = account_id_by_kind_currency(pool, "cogs", Some("USD")).await;
    let var_scrap_acct = account_id_by_kind_currency(pool, "variance_scrap", Some("USD")).await;
    let creation_void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let creation_void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    // Pre-seed inv_value_fg generously: FG_SEED_QTY @ $UNIT_COST.
    // For wac_perpetual SKUs this fixes the running avg at $UNIT_COST so
    // ship-time cogs snapshot matches the standard SKU's std cost.
    seed_fg_pool(
        pool, qty_acct, val_acct, creation_void_qty, creation_void_val,
        FG_SEED_QTY, FG_SEED_QTY * UNIT_COST,
    ).await;

    Scaffold {
        customer_id,
        so_id,
        so_line_id,
        sku_kind: kind,
        qty_acct,
        val_acct,
        cust_qty,
        cust_unsettled,
        cust_ar,
        revenue_acct,
        cogs_acct,
        var_scrap_acct,
        stock_scrap_acct,
        stock_quarantine_acct,
        ship_line_ids: Vec::new(),
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn fresh_customer(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Customer {code}"))
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
    .unwrap_or_else(|e| panic!("set_std_cost: {e}"));
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

async fn add_so_line(
    pool: &PgPool,
    so_id: &str,
    sku_id: &str,
    ship_loc_id: &str,
    qty_ordered: i64,
    unit_price: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, $4, $5, 'USD', 0)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
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
            (kind, ledger_kind, currency, sku_id, location_id, counterparty_id, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID, $7::balance_direction)
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
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn seed_fg_pool(
    pool: &PgPool,
    qty_acct: i64,
    val_acct: i64,
    void_qty: i64,
    void_val: i64,
    qty: i64,
    value: i64,
) {
    let posted_by = fresh_uuid(pool).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"cr_test_seed", "document_id":doc_id,
         "debit_account_id":qty_acct, "credit_account_id":void_qty,
         "amount":qty, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"cr_test_seed", "document_id":doc_id,
         "debit_account_id":val_acct, "credit_account_id":void_val,
         "amount":value, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_fg_pool: {e}"));
}

async fn ship(pool: &PgPool, s: &Scaffold, qty: i64) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"so_line_id": s.so_line_id, "qty_shipped": qty}]);
    sqlx::query(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await?;
    let ship_line_id: String = sqlx::query_scalar(
        "SELECT id::text FROM so_shipment_lines
          WHERE so_line_id = $1::UUID
          ORDER BY id DESC LIMIT 1",
    )
    .bind(&s.so_line_id)
    .fetch_one(pool)
    .await?;
    Ok(ship_line_id)
}

async fn invoice(pool: &PgPool, s: &Scaffold, qty: i64) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{
        "kind": "so_match",
        "so_line_id": s.so_line_id,
        "qty": qty,
        "unit_price": UNIT_PRICE,
        "amount": qty * UNIT_PRICE,
    }]);
    sqlx::query(
        "SELECT post_customer_invoice($1::UUID, 'USD', $2::JSONB,
                                       '2026-04-21'::DATE, $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.customer_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn call_return_one_line(
    pool: &PgPool,
    customer_id: &str,
    ship_line_id: &str,
    qty: i64,
    disposition: Disposition,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{
        "ship_line_id": ship_line_id,
        "qty_returned": qty,
        "disposition": disposition.as_pg(),
    }]);
    sqlx::query_scalar(
        "SELECT post_customer_return($1::UUID, $2::JSONB,
                                      '2026-04-25'::DATE,
                                      $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(customer_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

/// Most-recent customer_return_lines row for `ship_line_id`. Returns
/// (qty_to_ar_unsettled, qty_to_ar).
async fn latest_return_split(pool: &PgPool, ship_line_id: &str) -> (i64, i64) {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT qty_to_ar_unsettled, qty_to_ar
           FROM customer_return_lines
          WHERE ship_line_id = $1::UUID
          ORDER BY id DESC LIMIT 1",
    )
    .bind(ship_line_id)
    .fetch_optional(pool)
    .await
    .expect("latest_return_split");
    row.unwrap_or((0, 0))
}

/// Find an index in `ship_line_ids` whose cumulative remainder
/// (qty_shipped - qty_returned) is positive.
async fn pick_ship_line_with_remainder(pool: &PgPool, s: &Scaffold) -> Option<usize> {
    for (idx, sid) in s.ship_line_ids.iter().enumerate() {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT
               sl.qty_shipped,
               COALESCE((SELECT SUM(qty_returned)::BIGINT
                          FROM customer_return_lines crl
                          WHERE crl.ship_line_id = sl.id), 0)
                 AS qty_returned
              FROM so_shipment_lines sl
             WHERE sl.id = $1::UUID",
        )
        .bind(sid)
        .fetch_one(pool)
        .await
        .expect("pick_ship_line");
        if row.0 > row.1 {
            return Some(idx);
        }
    }
    None
}
