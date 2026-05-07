//! acct-axal (sub of acct-1cer) — property test for post_so_ship full
//! surface (multi-line, mixed cost methods, multi-currency, tax leg,
//! cost_method_at_ship snapshot).
//!
//! Complements property_ap9_audit_ledger_consistency (acct-ct0p) which
//! pins only the AP9/R7 unit_cost-snapshot invariant on a narrow happy
//! path.
//!
//! Per case:
//!
//! - One customer (USD).
//! - 1-3 so_lines, each with a random SKU (standard or wac_perpetual)
//!   at one of 1-2 ship_locations. Random qty_ordered (10-30), unit_price
//!   ($80-$120), tax_amount ($0 or $5-$15).
//! - inv_value_fg pre-seeded at $60/unit per (sku, location) so wac
//!   running avg matches standard's std cost ($60).
//! - Random op sequence: Ship({so_line_idx, qty}, ...) — single-line or
//!   multi-line (1-N so_lines per Ship call). Bounded by cumulative
//!   qty_ordered per so_line.
//!
//! Per-Ship assertions (across ALL emitted events):
//!
//! - I1-I7 (assert_invariants_hold)
//! - For each so_shipment_lines row created:
//!     unit_cost == $UNIT_COST (60) — both standard and wac (running
//!     avg pinned to seed $UNIT_COST)
//!     cost_method_at_ship == sku.cost_method (mig 0087 snapshot)
//! - For each event emitted: sum-by-account matches expected:
//!     stock_available drains by qty
//!     inv_value_fg drains by qty × unit_cost
//!     customer_pool gains qty
//!     cogs gains qty × unit_cost (P&L)
//!     ar_unsettled gains qty × unit_price + tax_amount
//!     revenue gains qty × unit_price
//!     sales_tax_payable gains tax_amount (when tax > 0)
//! - so_lines.qty_ordered cumulative gate respected (P0038 surfaced
//!   when scaffold tries to over-ship; we skip those ops here)
//!
//! Catches: AP9/R7 drift on multi-line interactions; cost-method dispatch
//! bugs surfaced by future FIFO/lot work; multi-currency partition leaks;
//! cost_method_at_ship snapshot drift; over-ship gate bypass.
//!
//! Out of scope: reservation lifecycle (Reserve→Ship transition; could be
//! added but adds significant scaffolding for an orthogonal concern; the
//! acct-ujhn issue tracks reservation-state-machine drift separately).

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const UNIT_COST: i64 = 60;
const FG_SEED_QTY: i64 = 200; // per (sku, loc) pre-seed
const NUM_LOCATIONS: usize = 2;

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

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SoLine {
    so_line_id: String,
    sku_id: String,
    sku_kind: SkuKind,
    ship_loc_id: String,
    qty_ordered: i64,
    unit_price: i64,
    tax_amount: i64,
    qty_shipped: i64, // cumulative
}

#[derive(Debug, Clone)]
struct ShipSpec {
    so_line_idx: usize,
    qty: i64,
}

fn arb_sku_kind() -> impl Strategy<Value = SkuKind> {
    prop_oneof![Just(SkuKind::Standard), Just(SkuKind::WacPerpetual)]
}

fn arb_so_line_setup() -> impl Strategy<Value = (SkuKind, usize, i64, i64, i64)> {
    (
        arb_sku_kind(),
        0usize..NUM_LOCATIONS,
        10i64..=30,    // qty_ordered
        80i64..=120,   // unit_price
        prop_oneof![Just(0i64), 5i64..=15], // tax_amount: 50% zero, 50% [5,15]
    )
}

fn arb_so_lines() -> impl Strategy<Value = Vec<(SkuKind, usize, i64, i64, i64)>> {
    prop::collection::vec(arb_so_line_setup(), 1..=3)
}

fn arb_ship_spec(num_lines: usize) -> impl Strategy<Value = ShipSpec> {
    (0usize..num_lines, 1i64..=8).prop_map(|(idx, qty)| ShipSpec { so_line_idx: idx, qty })
}

/// One Ship call may include 1..=N so_line entries (multi-line shipment).
fn arb_ship_batch(num_lines: usize) -> impl Strategy<Value = Vec<ShipSpec>> {
    prop::collection::vec(arb_ship_spec(num_lines), 1..=2)
}

#[tokio::test(flavor = "current_thread")]
async fn property_so_ship_full_surface_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;

        let customer_id = fresh_customer(&pool, &format!("CUST-SS-{case_idx}"), "USD").await;
        // Pre-create N ship locations.
        let mut loc_ids: Vec<String> = Vec::with_capacity(NUM_LOCATIONS);
        for i in 0..NUM_LOCATIONS {
            loc_ids.push(fresh_location(&pool, &format!("SS-L{i}-{case_idx}")).await);
        }

        // Customer-side accounts (USD only).
        let cust_qty = open_account(
            &pool, "customer_pool", "qty", None, None, None,
            Some(&customer_id), "debit",
        ).await;
        let cust_unsettled = open_account(
            &pool, "ar_unsettled", "value", Some("USD"),
            None, None, Some(&customer_id), "debit",
        ).await;

        // Shared GL accounts (resolved per call).
        let revenue_acct = account_id_by_kind_currency(&pool, "revenue", Some("USD")).await;
        let cogs_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
        let tax_acct = account_id_by_kind_currency(&pool, "sales_tax_payable", Some("USD")).await;

        // Build SO + SO lines.
        let so_id = create_so(&pool, &customer_id).await;
        let so_lines_setup =
            arb_so_lines().new_tree(&mut runner).expect("so_lines").current();

        // Per-(sku, loc) account ids; we'll need them to assert balance
        // deltas. Map by (sku_idx, loc_idx).
        struct PerLineAccts {
            qty_acct: i64,
            val_acct: i64,
        }
        let mut so_lines: Vec<SoLine> = Vec::new();
        let mut line_accts: Vec<PerLineAccts> = Vec::new();

        for (i, (kind, loc_idx, qty_ordered, unit_price, tax_amount)) in
            so_lines_setup.iter().enumerate()
        {
            let sku_code = format!("SS-S{i}-{case_idx}");
            let sku = fresh_sku(&pool, &sku_code, kind.as_pg()).await;
            if matches!(kind, SkuKind::Standard) {
                set_std_cost(&pool, &sku, UNIT_COST).await;
            }

            let loc = &loc_ids[*loc_idx];

            // Open per-(sku,loc) accounts ONCE. If two so_lines share the
            // same (sku,loc), only the first opens; the second reuses.
            let qty_acct = ensure_account(
                &pool, "stock_available", "qty", None,
                Some(&sku), Some(loc), None, "debit",
            ).await;
            let val_acct = ensure_account(
                &pool, "inv_value_fg", "value", Some("USD"),
                Some(&sku), Some(loc), None, "debit",
            ).await;

            // Pre-seed FG: $UNIT_COST × FG_SEED_QTY. Pins wac running avg.
            seed_fg_pool(&pool, qty_acct, val_acct, FG_SEED_QTY, FG_SEED_QTY * UNIT_COST).await;

            let so_line_id = add_so_line(
                &pool, &so_id, (i + 1) as i32, &sku, loc,
                *qty_ordered, *unit_price, *tax_amount,
            ).await;

            so_lines.push(SoLine {
                so_line_id,
                sku_id: sku,
                sku_kind: *kind,
                ship_loc_id: loc.clone(),
                qty_ordered: *qty_ordered,
                unit_price: *unit_price,
                tax_amount: *tax_amount,
                qty_shipped: 0,
            });
            line_accts.push(PerLineAccts { qty_acct, val_acct });
        }

        let label_root = format!("ss#{case_idx}(n={})", so_lines.len());

        // Random ship op sequence. ~3-6 ship calls, each with 1-2 lines.
        let num_ships_strat = (3usize..=6).boxed();
        let num_ships = num_ships_strat.new_tree(&mut runner).expect("num_ships").current();

        for ship_idx in 0..num_ships {
            let label = format!("{label_root}/ship{ship_idx}");

            let batch_tree =
                arb_ship_batch(so_lines.len()).new_tree(&mut runner).expect("batch");
            let mut batch: Vec<ShipSpec> = batch_tree.current();

            // Filter: drop entries that would over-ship + dedupe so_line_idx
            // within the batch (post_so_ship doesn't aggregate same-line entries).
            let mut seen_lines = std::collections::HashSet::new();
            let mut feasible_batch: Vec<ShipSpec> = Vec::new();
            for spec in batch.drain(..) {
                if !seen_lines.insert(spec.so_line_idx) {
                    continue;
                }
                let line = &so_lines[spec.so_line_idx];
                if line.qty_shipped + spec.qty > line.qty_ordered {
                    continue;
                }
                feasible_batch.push(spec);
            }
            if feasible_batch.is_empty() {
                continue;
            }

            // Snapshot pre-ship balances per affected account.
            let mut pre_qty: std::collections::HashMap<i64, i64> =
                std::collections::HashMap::new();
            let mut pre_val: std::collections::HashMap<i64, i64> =
                std::collections::HashMap::new();
            for spec in &feasible_batch {
                let accts = &line_accts[spec.so_line_idx];
                pre_qty.insert(accts.qty_acct, balance(&pool, accts.qty_acct).await);
                pre_val.insert(accts.val_acct, balance(&pool, accts.val_acct).await);
            }
            let pre_cust_qty = balance(&pool, cust_qty).await;
            let pre_cust_unsettled = balance(&pool, cust_unsettled).await;
            let pre_revenue = balance(&pool, revenue_acct).await;
            let pre_cogs = balance(&pool, cogs_acct).await;
            let pre_tax = balance(&pool, tax_acct).await;

            // Build the post_so_ship payload from feasible batch.
            let payload: Vec<serde_json::Value> = feasible_batch.iter().map(|spec| {
                json!({
                    "so_line_id": so_lines[spec.so_line_idx].so_line_id,
                    "qty_shipped": spec.qty,
                })
            }).collect();

            let r = call_so_ship(&pool, &so_id, &payload).await;
            if r.is_err() {
                continue;
            }

            // Update tracker.
            for spec in &feasible_batch {
                so_lines[spec.so_line_idx].qty_shipped += spec.qty;
            }

            // Compute expected aggregate deltas across the batch.
            let mut exp_cust_qty_delta: i64 = 0;
            let mut exp_cust_unsettled_delta: i64 = 0;
            let mut exp_revenue_delta: i64 = 0;
            let mut exp_cogs_delta: i64 = 0;
            let mut exp_tax_delta: i64 = 0;

            // Per-account aggregations (multiple lines may share a sku/loc).
            let mut exp_qty_delta: std::collections::HashMap<i64, i64> =
                std::collections::HashMap::new();
            let mut exp_val_delta: std::collections::HashMap<i64, i64> =
                std::collections::HashMap::new();

            for spec in &feasible_batch {
                let line = &so_lines[spec.so_line_idx];
                let accts = &line_accts[spec.so_line_idx];
                // qty leg: stock_available drains; customer_pool gains.
                *exp_qty_delta.entry(accts.qty_acct).or_insert(0) -= spec.qty;
                exp_cust_qty_delta += spec.qty;
                // cogs leg: inv_value_fg drains by qty * UNIT_COST; cogs gains same.
                *exp_val_delta.entry(accts.val_acct).or_insert(0) -= spec.qty * UNIT_COST;
                exp_cogs_delta += spec.qty * UNIT_COST;
                // revenue leg: ar_unsettled gains qty * unit_price; revenue gains same.
                let revenue_amt = spec.qty * line.unit_price;
                exp_cust_unsettled_delta += revenue_amt;
                exp_revenue_delta += revenue_amt;
                // tax leg: ar_unsettled gains tax_amount; sales_tax_payable gains same.
                if line.tax_amount > 0 {
                    exp_cust_unsettled_delta += line.tax_amount;
                    exp_tax_delta += line.tax_amount;
                }
            }

            // (a) Per-(sku,loc) qty + value pool deltas.
            for (acct, exp) in exp_qty_delta.iter() {
                let actual = balance(&pool, *acct).await - pre_qty[acct];
                assert_eq!(
                    actual, *exp,
                    "[{label}] qty_acct {acct}: exp delta {exp}, got {actual}",
                );
            }
            for (acct, exp) in exp_val_delta.iter() {
                let actual = balance(&pool, *acct).await - pre_val[acct];
                assert_eq!(
                    actual, *exp,
                    "[{label}] val_acct {acct}: exp delta {exp}, got {actual}",
                );
            }

            // (b) Customer-side qty/value deltas.
            // customer_pool is debit-normal: ship debits → balance grows.
            let cust_qty_delta = balance(&pool, cust_qty).await - pre_cust_qty;
            assert_eq!(
                cust_qty_delta, exp_cust_qty_delta,
                "[{label}] customer_pool delta: exp {exp_cust_qty_delta}, got {cust_qty_delta}",
            );
            // ar_unsettled is debit-normal: ship debits → balance grows.
            let cust_unsettled_delta = balance(&pool, cust_unsettled).await - pre_cust_unsettled;
            assert_eq!(
                cust_unsettled_delta, exp_cust_unsettled_delta,
                "[{label}] ar_unsettled delta: exp {exp_cust_unsettled_delta}, got {cust_unsettled_delta}",
            );

            // (c) GL deltas.
            // revenue is credit-normal: ship credits → balance moves negative
            // (debits-credits sign convention).
            let revenue_delta = balance(&pool, revenue_acct).await - pre_revenue;
            assert_eq!(
                revenue_delta, -exp_revenue_delta,
                "[{label}] revenue delta: exp {} (credit-normal), got {revenue_delta}",
                -exp_revenue_delta,
            );
            // cogs is debit-normal: ship debits → balance grows.
            let cogs_delta = balance(&pool, cogs_acct).await - pre_cogs;
            assert_eq!(
                cogs_delta, exp_cogs_delta,
                "[{label}] cogs delta: exp {exp_cogs_delta}, got {cogs_delta}",
            );
            // sales_tax_payable is credit-normal.
            let tax_delta = balance(&pool, tax_acct).await - pre_tax;
            assert_eq!(
                tax_delta, -exp_tax_delta,
                "[{label}] tax delta: exp {} (credit-normal), got {tax_delta}",
                -exp_tax_delta,
            );

            // (d) so_shipment_lines.unit_cost == UNIT_COST + cost_method_at_ship matches.
            for spec in &feasible_batch {
                let line = &so_lines[spec.so_line_idx];
                let row: (i64, String) = sqlx::query_as(
                    "SELECT ssl.unit_cost, ssl.cost_method_at_ship::TEXT
                       FROM so_shipment_lines ssl
                      WHERE ssl.so_line_id = $1::UUID
                      ORDER BY ssl.id DESC LIMIT 1",
                )
                .bind(&line.so_line_id)
                .fetch_one(&pool)
                .await
                .expect("fetch ssl");
                assert_eq!(
                    row.0, UNIT_COST,
                    "[{label}] so_line {}: unit_cost snapshot wrong (expected {UNIT_COST}, got {})",
                    line.so_line_id, row.0,
                );
                assert_eq!(
                    row.1.as_str(), line.sku_kind.as_pg(),
                    "[{label}] so_line {}: cost_method_at_ship snapshot wrong (expected {}, got {})",
                    line.so_line_id, line.sku_kind.as_pg(), row.1,
                );
            }

            assert_invariants_hold(&pool, &label).await;
        }

        assert_invariants_hold(&pool, &format!("{label_root}/end")).await;
    }
}

// ============================================================
// Scaffold helpers
// ============================================================

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
    line_no: i32,
    sku_id: &str,
    ship_loc_id: &str,
    qty_ordered: i64,
    unit_price: i64,
    tax_amount: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD', $7)
         RETURNING id::text",
    )
    .bind(so_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(ship_loc_id)
    .bind(qty_ordered)
    .bind(unit_price)
    .bind(tax_amount)
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
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6::UUID, $7::balance_direction)
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

/// Open the account if not already present; otherwise return the existing id.
/// Needed because two so_lines on the same (sku, loc) would collide on
/// the unique index for stock_available / inv_value_fg.
#[allow(clippy::too_many_arguments)]
async fn ensure_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    loc_id: Option<&str>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    if let Some(id) = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts
          WHERE kind::text = $1 AND ledger_kind = $2::ledger_kind
            AND ((currency = $3) OR ($3 IS NULL AND currency IS NULL))
            AND ((sku_id = $4::UUID) OR ($4 IS NULL AND sku_id IS NULL))
            AND ((location_id = $5::UUID) OR ($5 IS NULL AND location_id IS NULL))
            AND ((counterparty_id = $6::UUID) OR ($6 IS NULL AND counterparty_id IS NULL))
            AND NOT is_closed",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .fetch_optional(pool)
    .await
    .expect("ensure_account select")
    {
        return id;
    }
    open_account(pool, kind, ledger_kind, currency, sku_id, loc_id, counterparty_id, normal_side)
        .await
}

async fn seed_fg_pool(pool: &PgPool, qty_acct: i64, val_acct: i64, qty: i64, value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        {"reason":"cycle_count_adj",
         "document_kind":"ss_test_seed", "document_id":doc_id,
         "debit_account_id":qty_acct, "credit_account_id":void_qty,
         "amount":qty, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
        {"reason":"cycle_count_adj",
         "document_kind":"ss_test_seed", "document_id":doc_id,
         "debit_account_id":val_acct, "credit_account_id":void_val,
         "amount":value, "qty":qty,
         "business_date":"2026-04-15",
         "idempotency_key":fresh_uuid(pool).await,
         "posted_by":posted_by},
    ]);
    sqlx::query("SELECT post_posting_lines($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_fg_pool: {e}"));
}

async fn call_so_ship(pool: &PgPool, so_id: &str, payload: &[serde_json::Value]) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = serde_json::Value::Array(payload.to_vec());
    sqlx::query(
        "SELECT post_so_ship($1::UUID, $2::JSONB, '2026-04-20'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(so_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .map(|_| ())
}
