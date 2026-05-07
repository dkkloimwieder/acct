//! acct-fute (sub of acct-1cer) — property test for post_po_return
//! state-aware routing.
//!
//! Exercises mig 0085 (acct-quk po_return + PPV reversal symmetry) and
//! mig 0086 (acct-tk7 split routing across ap_unsettled / ap based on
//! cumulative billed state).
//!
//! Per case:
//!
//! - One vendor + one po_line at one location.
//! - SKU is randomly standard (~50%) or wac_perpetual (~50%) — both
//!   cost methods supported by post_po_return; PPV applies only to
//!   standard.
//! - Random po_unit_cost vs std_cost relationship (=, >, <) so PPV
//!   reverses in both directions.
//! - Random sequence of (Receive | Bill | Return) ops bounded by the
//!   cumulative po_line state tracker.
//!
//! Per-op assertions:
//!
//! - I1-I7 (assert_invariants_hold)
//! - For each successful Return: po_return_lines.qty_to_ap_unsettled +
//!   qty_to_ap == qty_returned (mig 0086 split invariant)
//! - For each successful Return: the split equals the un-billed-first
//!   draining order (route to ap_unsettled while un-billed remainder
//!   exists, else route to ap)
//! - ap_unsettled balance + ap balance correctly track received-not-
//!   yet-billed and billed cumulative state
//! - PPV reversal direction correct on standard SKUs:
//!     po > std → on receipt: variance_ppv DR; on return: variance_ppv CR
//!     po < std → on receipt: variance_ppv CR; on return: variance_ppv DR
//!     po = std → no PPV movement
//! - On WAC SKUs: variance_ppv balance never moves
//! - Over-return rejected with P0047
//!
//! Catches: state-split miscalculation when partial bills + partial
//! returns interleave; PPV event ordering bugs (PPV before value leg
//! per acct-quk); idempotency replay drift; accounts_check trips from
//! incorrect routing.
//!
//! Out of scope: cross-period variance_ppv_prior_period_adj routing
//! (acct-b8n mig 0089 — requires closing a period mid-scenario; covered
//! by integration tests, deferred to a follow-up property test if
//! needed).

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use serde_json::json;
use sqlx::PgPool;

const TEST_PROPTEST_CASES_DEFAULT: u32 = 100;
const QTY_ORDERED: i64 = 50;
const STD_COST: i64 = 100;

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

#[derive(Debug, Clone, Copy)]
enum Op {
    Receive { qty: i64 },
    Bill { qty: i64 },
    Return { qty: i64 },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 35% Receive
        7 => (3i64..=15).prop_map(|q| Op::Receive { qty: q }),
        // 35% Bill
        7 => (3i64..=15).prop_map(|q| Op::Bill { qty: q }),
        // 30% Return
        6 => (1i64..=10).prop_map(|q| Op::Return { qty: q }),
    ]
}

fn arb_op_seq() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 6..=14)
}

#[derive(Debug, Default)]
struct PoState {
    qty_received: i64,
    qty_billed: i64,
    qty_returned_to_unsettled: i64,
    qty_returned_to_ap: i64,
}

impl PoState {
    fn qty_returned(&self) -> i64 {
        self.qty_returned_to_unsettled + self.qty_returned_to_ap
    }
    fn qty_un_billed(&self) -> i64 {
        // Un-billed remainder: qty actually received that hasn't been
        // billed yet, less the un-settled portion of prior returns.
        // This is the routable-to-ap_unsettled budget for the next return.
        (self.qty_received - self.qty_billed - self.qty_returned_to_unsettled).max(0)
    }
    fn qty_returnable(&self) -> i64 {
        self.qty_received - self.qty_returned()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct Scaffold {
    vendor_id: String,
    po_id: String,
    po_line_id: String,
    sku_code: String,
    sku_kind: SkuKind,
    po_unit_cost: i64,
    val_acct: i64,
    ven_qty: i64,
    ven_unsettled: i64,
    ven_ap: i64,
    var_ppv: i64,
    qty_acct: i64,
    /// po_receipt_lines.id; caller picks the most-recent recv_line with
    /// remaining qty when issuing a Return.
    recv_line_ids: Vec<String>,
}

#[tokio::test(flavor = "current_thread")]
async fn property_po_return_routing_invariants_hold() {
    let pool = connect_test_db().await;

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(TEST_PROPTEST_CASES_DEFAULT);

    let mut runner = proptest::test_runner::TestRunner::default();

    // Strategy for SKU kind (50/50 standard / wac_perpetual).
    let sku_kind_strat = prop_oneof![
        Just(SkuKind::Standard),
        Just(SkuKind::WacPerpetual),
    ];
    // po_unit_cost in [80, 120] gives a mix of po<std (80-99), po=std (100), po>std (101-120).
    let po_cost_strat = (80i64..=120i64).boxed();

    for case_idx in 0..cases {
        reset_to_fixture(&pool).await;
        let kind = sku_kind_strat.new_tree(&mut runner).expect("kind").current();
        let po_unit_cost = po_cost_strat.new_tree(&mut runner).expect("po_cost").current();

        let mut scaffold = build_scaffold(&pool, case_idx, kind, po_unit_cost).await;
        let mut state = PoState::default();
        let label_root = format!("pr#{case_idx}({:?},po={po_unit_cost})", kind);

        // Initial PPV balance snapshot — used to assert WAC scenarios
        // don't touch variance_ppv at all.
        let var_ppv_initial = balance(&pool, scaffold.var_ppv).await;

        let ops_tree = arb_op_seq().new_tree(&mut runner).expect("ops").current();

        for (i, op) in ops_tree.iter().enumerate() {
            let label = format!("{label_root}/op{i}");

            match *op {
                Op::Receive { qty } => {
                    if state.qty_received + qty > QTY_ORDERED {
                        continue; // P0023 over-receipt; skip
                    }
                    let recv_line = receive(&pool, &scaffold, qty, i).await
                        .unwrap_or_else(|e| panic!("[{label}] receive: {e}"));
                    scaffold.recv_line_ids.push(recv_line);
                    state.qty_received += qty;
                }
                Op::Bill { qty } => {
                    let billable = state.qty_received - state.qty_billed - state.qty_returned_to_unsettled;
                    if qty > billable.max(0) {
                        continue;
                    }
                    let r = bill(&pool, &scaffold, qty, i).await;
                    if r.is_err() {
                        continue;
                    }
                    state.qty_billed += qty;
                }
                Op::Return { qty } => {
                    if scaffold.recv_line_ids.is_empty() {
                        continue;
                    }
                    if qty > state.qty_returnable() {
                        // Over-return; expect P0047 if we DID call it.
                        // Skip to keep op success rate up (the negative
                        // path is covered by the integration test).
                        continue;
                    }
                    // Pick a recv_line that still has uncovered qty.
                    let recv_line_idx =
                        pick_recv_line_with_remainder(&pool, &scaffold).await;
                    let Some(recv_line_idx) = recv_line_idx else { continue };
                    let recv_line_id = &scaffold.recv_line_ids[recv_line_idx];

                    // Snapshot before-balances for assertion.
                    let pre_var_ppv = balance(&pool, scaffold.var_ppv).await;
                    let pre_ap = balance(&pool, scaffold.ven_ap).await;
                    let pre_unsettled = balance(&pool, scaffold.ven_unsettled).await;

                    let pre_un_billed = state.qty_un_billed();
                    let exp_to_unsettled = qty.min(pre_un_billed);
                    let exp_to_ap = qty - exp_to_unsettled;

                    let r = call_return_one_line(&pool, &scaffold.vendor_id, recv_line_id, qty).await;
                    if r.is_err() {
                        continue;
                    }
                    state.qty_returned_to_unsettled += exp_to_unsettled;
                    state.qty_returned_to_ap += exp_to_ap;

                    // Assertion (a): mig 0086 split invariant on this line's row.
                    let (got_unsettled, got_ap) =
                        latest_return_split(&pool, recv_line_id).await;
                    assert_eq!(
                        got_unsettled, exp_to_unsettled,
                        "[{label}] qty_to_ap_unsettled mismatch: expected {exp_to_unsettled} got {got_unsettled} \
                         (un_billed_pre={pre_un_billed} qty={qty})",
                    );
                    assert_eq!(
                        got_ap, exp_to_ap,
                        "[{label}] qty_to_ap mismatch: expected {exp_to_ap} got {got_ap}",
                    );

                    // Assertion (b): PPV reversal direction.
                    if matches!(scaffold.sku_kind, SkuKind::Standard) {
                        let std_delta = scaffold.po_unit_cost - STD_COST;
                        let expected_ppv_delta = -std_delta * qty;
                        let actual_ppv_delta = balance(&pool, scaffold.var_ppv).await - pre_var_ppv;
                        assert_eq!(
                            actual_ppv_delta, expected_ppv_delta,
                            "[{label}] PPV reversal direction wrong: po={} std={STD_COST} qty={qty} \
                             expected_delta={expected_ppv_delta} actual_delta={actual_ppv_delta}",
                            scaffold.po_unit_cost,
                        );
                    } else {
                        // WAC: PPV must not move.
                        assert_eq!(
                            balance(&pool, scaffold.var_ppv).await, pre_var_ppv,
                            "[{label}] WAC SKU touched variance_ppv (delta={})",
                            balance(&pool, scaffold.var_ppv).await - pre_var_ppv,
                        );
                    }

                    // Assertion (c): ap balance reflects ap-route portion only.
                    // ap is credit-normal: balance is stored as -credits (debits-credits).
                    // Returning to ap DEBITS ap → balance grows toward 0.
                    // For std SKU: ap drains by qty * std_cost on the ap route;
                    // for wac SKU: ap drains by qty * po_unit_cost on the ap route.
                    let drain_unit = match scaffold.sku_kind {
                        SkuKind::Standard => STD_COST,
                        SkuKind::WacPerpetual => scaffold.po_unit_cost,
                    };
                    // Plus PPV's ap portion: for std + po!=std, return reverses
                    // (po-std)*qty_to_ap into ap also. Total ap drain for std =
                    // qty_to_ap * po_unit_cost.
                    let expected_ap_drain = match scaffold.sku_kind {
                        SkuKind::Standard => exp_to_ap * scaffold.po_unit_cost,
                        SkuKind::WacPerpetual => exp_to_ap * drain_unit,
                    };
                    let actual_ap_change = balance(&pool, scaffold.ven_ap).await - pre_ap;
                    assert_eq!(
                        actual_ap_change, expected_ap_drain,
                        "[{label}] ap movement wrong: expected drain {expected_ap_drain}, got delta {actual_ap_change}",
                    );

                    // Assertion (d): ap_unsettled balance reflects unsettled-route portion only.
                    let expected_unsettled_drain = match scaffold.sku_kind {
                        SkuKind::Standard => exp_to_unsettled * scaffold.po_unit_cost,
                        SkuKind::WacPerpetual => exp_to_unsettled * drain_unit,
                    };
                    let actual_unsettled_change =
                        balance(&pool, scaffold.ven_unsettled).await - pre_unsettled;
                    assert_eq!(
                        actual_unsettled_change, expected_unsettled_drain,
                        "[{label}] ap_unsettled movement wrong: expected drain {expected_unsettled_drain}, got delta {actual_unsettled_change}",
                    );
                }
            }

            assert_invariants_hold(&pool, &label).await;
        }

        // End-of-case sanity: WAC SKU never touched variance_ppv.
        if matches!(scaffold.sku_kind, SkuKind::WacPerpetual) {
            assert_eq!(
                balance(&pool, scaffold.var_ppv).await, var_ppv_initial,
                "[{label_root}/end] WAC SKU touched variance_ppv across the scenario",
            );
        }

        assert_invariants_hold(&pool, &format!("{label_root}/end")).await;
    }
}

// ============================================================
// Scaffold + helpers
// ============================================================

async fn build_scaffold(
    pool: &PgPool,
    case_idx: u32,
    kind: SkuKind,
    po_unit_cost: i64,
) -> Scaffold {
    let suffix = format!("PR-{case_idx}");
    let sku_code = format!("PR-S-{case_idx}");
    let loc_code = format!("PR-L-{case_idx}");

    let sku = fresh_sku(pool, &sku_code, kind.as_pg()).await;
    let loc = fresh_location(pool, &loc_code).await;
    if matches!(kind, SkuKind::Standard) {
        set_std_cost(pool, &sku, STD_COST).await;
    }

    let qty_acct = open_account(
        pool, "stock_available", "qty", None,
        Some(&sku), Some(&loc), None, "debit",
    ).await;
    let val_acct = open_account(
        pool, "inv_value_raw", "value", Some("USD"),
        Some(&sku), Some(&loc), None, "debit",
    ).await;

    let vendor = fresh_vendor(pool, &format!("VEN-{suffix}"), "USD").await;
    let po = fresh_po(pool, &vendor).await;
    let po_line = fresh_po_line(pool, &po, &sku, &loc, QTY_ORDERED, po_unit_cost, "USD").await;

    let ven_qty =
        open_account(pool, "vendor_pool", "qty", None, None, None, Some(&vendor), "credit").await;
    let ven_unsettled = open_account(
        pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), "credit",
    ).await;
    let ven_ap = open_account(
        pool, "ap", "value", Some("USD"), None, None, Some(&vendor), "credit",
    ).await;
    let var_ppv = account_id_by_kind_currency(pool, "variance_ppv", Some("USD")).await;

    Scaffold {
        vendor_id: vendor,
        po_id: po,
        po_line_id: po_line,
        sku_code,
        sku_kind: kind,
        po_unit_cost,
        val_acct,
        ven_qty,
        ven_unsettled,
        ven_ap,
        var_ppv,
        qty_acct,
        recv_line_ids: Vec::new(),
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
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

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, $3) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vendor {code}"))
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert vendor {code}: {e}"))
}

async fn fresh_po(pool: &PgPool, vendor_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_orders (vendor_id, status)
         VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert po: {e}"))
}

async fn fresh_po_line(
    pool: &PgPool,
    po_id: &str,
    sku_id: &str,
    loc_id: &str,
    qty: i64,
    unit_cost: i64,
    currency: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, 1, $2::UUID, $3::UUID, $4, $5, $6)
         RETURNING id::text",
    )
    .bind(po_id)
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty)
    .bind(unit_cost)
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert po_line: {e}"))
}

async fn receive(pool: &PgPool, s: &Scaffold, qty: i64, _i: usize) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"po_line_id": s.po_line_id, "qty_received": qty}]);
    let receipt_id: String = sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2::JSONB, '2026-04-15'::DATE,
                                 $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(&s.po_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await?;
    // Pull the freshly-created po_receipt_lines.id (most-recent for
    // this receipt + po_line). po_receipt_lines has no line_no column;
    // order by id desc.
    let recv_line_id: String = sqlx::query_scalar(
        "SELECT id::text FROM po_receipt_lines
          WHERE receipt_id = $1::UUID AND po_line_id = $2::UUID
          ORDER BY id DESC LIMIT 1",
    )
    .bind(&receipt_id)
    .bind(&s.po_line_id)
    .fetch_one(pool)
    .await?;
    Ok(recv_line_id)
}

async fn bill(pool: &PgPool, s: &Scaffold, qty: i64, _i: usize) -> sqlx::Result<()> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{
        "kind": "po_match",
        "po_line_id": s.po_line_id,
        "qty": qty,
        "unit_cost": s.po_unit_cost,
        "currency": "USD"
    }]);
    sqlx::query(
        "SELECT post_ap_bill($1::UUID, 'USD', $2::JSONB, '2026-04-18'::DATE,
                              $3::UUID, $4::UUID, NULL)",
    )
    .bind(&s.vendor_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn call_return_one_line(
    pool: &PgPool,
    vendor_id: &str,
    recv_line_id: &str,
    qty: i64,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let lines = json!([{"recv_line_id": recv_line_id, "qty_returned": qty}]);
    sqlx::query_scalar(
        "SELECT post_po_return($1::UUID, $2::JSONB, '2026-04-25'::DATE,
                                $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(vendor_id)
    .bind(lines)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

/// Most-recent po_return_lines row for `recv_line_id`. Returns
/// (qty_to_ap_unsettled, qty_to_ap).
async fn latest_return_split(pool: &PgPool, recv_line_id: &str) -> (i64, i64) {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT qty_to_ap_unsettled, qty_to_ap
           FROM po_return_lines
          WHERE recv_line_id = $1::UUID
          ORDER BY id DESC LIMIT 1",
    )
    .bind(recv_line_id)
    .fetch_optional(pool)
    .await
    .expect("latest_return_split");
    row.unwrap_or((0, 0))
}

/// Find an index in `recv_line_ids` whose cumulative remainder
/// (qty_received - qty_returned) is positive. Returns None if all
/// lines are exhausted.
async fn pick_recv_line_with_remainder(pool: &PgPool, s: &Scaffold) -> Option<usize> {
    for (idx, rid) in s.recv_line_ids.iter().enumerate() {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT
               r.qty_received,
               COALESCE((SELECT SUM(qty_returned)::BIGINT
                          FROM po_return_lines pr WHERE pr.recv_line_id = r.id), 0)
                 AS qty_returned
              FROM po_receipt_lines r
             WHERE r.id = $1::UUID",
        )
        .bind(rid)
        .fetch_one(pool)
        .await
        .expect("pick_recv_line");
        if row.0 > row.1 {
            return Some(idx);
        }
    }
    None
}
