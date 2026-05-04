//! Cross-path coverage: `post_cost_adjustment` (live wac_perpetual
//! revaluation) and `post_cost_adjustment_retroactive` (queued wac_periodic
//! / wac_retroactive re-cost) interleaved with the other Phase 0/1
//! doc-layer wrappers (`post_inventory_adjustment`, `post_po_receipt`,
//! `post_ap_bill`).
//!
//! Each test walks one transactional "path" — a specific sequence of
//! mid-period operations — and verifies that:
//!   * Pool revaluations from cost_adjust update inv_value_{class} and
//!     route the delta through `variance_cost_adjustment`.
//!   * The pool value carries forward correctly across subsequent
//!     receipts that compose with the revalued state.
//!   * Class isolation holds (raw revalue doesn't touch fg, etc.).
//!   * Cross-document side effects don't bleed (cost_adjust doesn't
//!     touch AP / ap_unsettled).
//!   * Forward-only semantics: a prior depletion's variance entries are
//!     not retroactively adjusted by a later cost_adjust.
//!   * Idempotency is preserved across an interleaved replay.
//!   * wac_periodic retroactive queues compose with mid-period depletions
//!     and the close-hook posts variances correctly (with the documented
//!     wac×retro double-correction stacking).
//!
//! Running-avg depletions on raw/fg pools are exercised via
//! `post_inventory_adjustment` with NULL unit_cost on a wac_perpetual /
//! wac_periodic / wac_retroactive SKU — that path reads pool running avg
//! to compute the depletion value. so_ship (Slice C, not yet shipped)
//! and the WO-context cost-event reasons (op_move / scrap / wo_complete)
//! are out of scope here. `rm_issue_to_wo` always uses component STANDARD
//! cost (BOM2 design, acct-6jq plan_only) so cost_adjust on a raw pool
//! has no observable effect on a downstream WO's rm_issue.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffolding
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
    .unwrap_or_else(|e| panic!("insert location {code}: {e}"))
}

async fn fresh_vendor(pool: &PgPool, code: &str, currency: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency) VALUES ($1, $2, $3) RETURNING id::text",
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
        "INSERT INTO purchase_orders (vendor_id, status) VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(vendor_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert PO: {e}"))
}

async fn fresh_po_line(
    pool: &PgPool,
    po_id: &str,
    line_no: i32,
    sku_id: &str,
    loc_id: &str,
    qty_ordered: i64,
    unit_cost: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO purchase_order_lines (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD')
         RETURNING id::text",
    )
    .bind(po_id)
    .bind(line_no)
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty_ordered)
    .bind(unit_cost)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert po_line: {e}"))
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
    routing_op: Option<i32>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, counterparty_id, routing_op, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6::UUID, $7, $8::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(counterparty_id)
    .bind(routing_op)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn credit_balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (credits_total - debits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("credit_balance")
}

async fn period_id(pool: &PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

async fn call_cost_adjust(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    inventory_class: &str,
    target_unit_cost: i64,
    business_date: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment(
            $1::UUID, $2::UUID, 'USD', $3, $4,
            $5::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(inventory_class)
    .bind(target_unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_cost_adjust_retro(
    pool: &PgPool,
    target_period_id: i64,
    sku_id: &str,
    loc_id: &str,
    inventory_class: &str,
    target_avg: i64,
    business_date: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment_retroactive(
            $1, $2::UUID, $3::UUID, 'USD', $4, $5,
            $6::DATE, $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(target_period_id)
    .bind(sku_id)
    .bind(loc_id)
    .bind(inventory_class)
    .bind(target_avg)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

/// Pass `unit_cost = None` for wac_perpetual depletions (and receipts on a
/// non-empty pool when the caller is OK with running-avg pricing). Pass
/// `Some(...)` to seed an empty pool or post a receipt at an asserted cost.
async fn call_inv_adjust(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    inventory_class: &str,
    business_date: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, $4, 'USD', $5,
            $6::DATE, $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(inventory_class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_po_receipt(
    pool: &PgPool,
    po_id: &str,
    po_line_id: &str,
    qty_received: i64,
    business_date: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let lines = json!([{ "po_line_id": po_line_id, "qty_received": qty_received }]);
    sqlx::query_scalar(
        "SELECT post_po_receipt($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(po_id)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_ap_bill_match(
    pool: &PgPool,
    vendor_id: &str,
    po_line_id: &str,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let lines = json!([{
        "kind": "po_match",
        "po_line_id": po_line_id,
        "qty": qty,
        "unit_cost": unit_cost,
        "amount": qty * unit_cost,
    }]);
    sqlx::query_scalar(
        "SELECT post_ap_bill($1::UUID, $2, $3, $4::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(vendor_id)
    .bind("USD")
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

/// wac_perpetual SKU + raw + fg value pools sharing one stock_available
/// (qty) account at MAIN. The qty account is shared across raw/fg classes
/// (per project convention: stock_available is per-(sku, location), value
/// pools split by class).
struct WacScaffold {
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    raw_val: i64,
    #[allow(dead_code)]
    fg_val: i64,
    var_cost_adj: i64,
}

async fn build_wac_scaffold(pool: &PgPool, sku_code: &str) -> WacScaffold {
    let sku_id = fresh_sku(pool, sku_code, "wac_perpetual").await;
    let loc_id = fresh_location(pool, &format!("{sku_code}-LOC")).await;
    let qty_acct = open_account(pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let raw_val = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let fg_val = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let var_cost_adj = account_id_by_kind_currency(pool, "variance_cost_adjustment", Some("USD")).await;
    WacScaffold { sku_id, loc_id, qty_acct, raw_val, fg_val, var_cost_adj }
}

// ============================================================
// Tests — wac_perpetual paths
// ============================================================

/// PO receipt → cost_adjust → PO receipt: pool composes correctly through
/// the revaluation. PO receipts post at po_unit_price (not running avg);
/// the cost_adjust delta accumulates in variance_cost_adjustment.
///
/// Path:
///   1. PO line @ $10. Receive 100 → raw pool = $1000, qty = 100.
///   2. cost_adjust raw to $12 → pool = $1200; var = -200 (write-up gain).
///   3. PO line @ $14. Receive 50 → pool += $700 → $1900, qty = 150.
///   4. cost_adjust raw to $12 → pool = 150 × $12 = $1800; delta = -100.
///      Net var = -300 (sum of two write-ups since the second was actually
///      a write-down: pool $1900 → $1800 means delta = $1800 - $1900 = -$100,
///      which is a debit to var_cost_adj — i.e. positive on a debit-normal
///      reading. Net var balance = -200 (R1 write-up gain) + 100 (R2
///      write-down loss) = -100.
#[tokio::test]
async fn wac_perpetual_po_then_cost_adjust_then_po_pool_composes() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = build_wac_scaffold(&pool, "WAC-POADJ").await;
    let vendor = fresh_vendor(&pool, "VEN-POADJ", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let _ven_qty = open_account(&pool, "vendor_pool", "qty", None, None, None, Some(&vendor), None, "credit").await;
    let _ap_unsettled = open_account(&pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), None, "credit").await;
    let _ap = open_account(&pool, "ap", "value", Some("USD"), None, None, Some(&vendor), None, "credit").await;

    let line_a = fresh_po_line(&pool, &po, 1, &s.sku_id, &s.loc_id, 100, 10).await;
    call_po_receipt(&pool, &po, &line_a, 100, "2026-04-10", &fresh_uuid(&pool).await)
        .await
        .expect("receipt 1");
    assert_eq!(balance(&pool, s.raw_val).await, 1000);
    assert_eq!(balance(&pool, s.qty_acct).await, 100);

    call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 12, "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("cost_adjust to $12");
    assert_eq!(balance(&pool, s.raw_val).await, 1200, "pool revalued");
    assert_eq!(balance(&pool, s.var_cost_adj).await, -200, "write-up gain credited");

    let line_b = fresh_po_line(&pool, &po, 2, &s.sku_id, &s.loc_id, 50, 14).await;
    call_po_receipt(&pool, &po, &line_b, 50, "2026-04-14", &fresh_uuid(&pool).await)
        .await
        .expect("receipt 2");
    assert_eq!(balance(&pool, s.raw_val).await, 1900,
               "pool composed: $1200 (post-adjust) + $700 (R2)");
    assert_eq!(balance(&pool, s.qty_acct).await, 150);

    // Adjust to $12 → 150 × $12 = $1800; delta = -$100 (write-down).
    call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 12, "2026-04-15",
        &fresh_uuid(&pool).await)
        .await
        .expect("cost_adjust to $12 again");
    assert_eq!(balance(&pool, s.raw_val).await, 1800);
    assert_eq!(balance(&pool, s.var_cost_adj).await, -100,
               "net: -200 write-up + 100 write-down = -100");
}

/// Class isolation under interleaved load. Uses separate locations for
/// raw vs fg because `post_cost_adjustment` reads pool_qty from
/// `stock_available` per (sku, location) — that account isn't class-
/// segmented, so co-locating raw + fg pools at the same location would
/// double-count qty into the cost_adjust math. The realistic ERP layout
/// puts raw and fg at distinct physical locations anyway.
///
/// Path:
///   1. inv_adj seed raw 100 @ $5 at LOC-RAW → raw pool=$500.
///   2. inv_adj seed fg 50 @ $20 at LOC-FG → fg pool=$1000.
///   3. cost_adjust raw to $7 → raw pool=$700, var=-200. fg untouched.
///   4. cost_adjust fg to $18 → fg pool=$900, var=-100 (net). raw untouched.
#[tokio::test]
async fn wac_perpetual_class_isolation_under_interleaved_adjust() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "WAC-CLS", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "WAC-CLS-RAW").await;
    let fg_loc = fresh_location(&pool, "WAC-CLS-FG").await;

    let _raw_qty = open_account(&pool, "stock_available", "qty", None, Some(&sku_id), Some(&raw_loc), None, None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku_id), Some(&raw_loc), None, None, "debit").await;
    let _fg_qty = open_account(&pool, "stock_available", "qty", None, Some(&sku_id), Some(&fg_loc), None, None, "debit").await;
    let fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&fg_loc), None, None, "debit").await;
    let var_cost_adj = account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    call_inv_adjust(&pool, &sku_id, &raw_loc, 100, Some(5), "raw", "2026-04-10",
        &fresh_uuid(&pool).await)
        .await
        .expect("seed raw");
    call_inv_adjust(&pool, &sku_id, &fg_loc, 50, Some(20), "fg", "2026-04-10",
        &fresh_uuid(&pool).await)
        .await
        .expect("seed fg");
    assert_eq!(balance(&pool, raw_val).await, 500);
    assert_eq!(balance(&pool, fg_val).await, 1000);

    call_cost_adjust(&pool, &sku_id, &raw_loc, "raw", 7, "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("adj raw");
    assert_eq!(balance(&pool, raw_val).await, 700);
    assert_eq!(balance(&pool, fg_val).await, 1000, "fg untouched");
    assert_eq!(balance(&pool, var_cost_adj).await, -200);

    call_cost_adjust(&pool, &sku_id, &fg_loc, "fg", 18, "2026-04-13",
        &fresh_uuid(&pool).await)
        .await
        .expect("adj fg");
    assert_eq!(balance(&pool, fg_val).await, 900);
    assert_eq!(balance(&pool, raw_val).await, 700, "raw untouched");
    assert_eq!(balance(&pool, var_cost_adj).await, -100,
               "net: -200 raw write-up + 100 fg write-down");
}

/// PO + AP bill three-way match + cost_adjust: AP and ap_unsettled are
/// not touched by cost_adjust. cost_adjust posts ONLY to value pool +
/// variance_cost_adjustment.
#[tokio::test]
async fn wac_perpetual_ap_unaffected_by_cost_adjust() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = build_wac_scaffold(&pool, "WAC-AP").await;
    let vendor = fresh_vendor(&pool, "VEN-AP", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let _ven_qty = open_account(&pool, "vendor_pool", "qty", None, None, None, Some(&vendor), None, "credit").await;
    let ap_unsettled = open_account(&pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), None, "credit").await;
    let ap = open_account(&pool, "ap", "value", Some("USD"), None, None, Some(&vendor), None, "credit").await;

    let line = fresh_po_line(&pool, &po, 1, &s.sku_id, &s.loc_id, 100, 10).await;
    call_po_receipt(&pool, &po, &line, 100, "2026-04-10", &fresh_uuid(&pool).await)
        .await
        .expect("receipt");
    assert_eq!(credit_balance(&pool, ap_unsettled).await, 1000,
               "AP unsettled accrued at receipt");
    assert_eq!(credit_balance(&pool, ap).await, 0);

    call_ap_bill_match(&pool, &vendor, &line, 100, 10, "2026-04-11",
        &fresh_uuid(&pool).await)
        .await
        .expect("ap bill");
    assert_eq!(credit_balance(&pool, ap_unsettled).await, 0,
               "AP unsettled cleared by bill");
    assert_eq!(credit_balance(&pool, ap).await, 1000);

    call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 13, "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("cost_adjust");
    assert_eq!(balance(&pool, s.raw_val).await, 1300);
    assert_eq!(credit_balance(&pool, ap_unsettled).await, 0,
               "AP unsettled untouched by cost_adjust");
    assert_eq!(credit_balance(&pool, ap).await, 1000,
               "AP untouched by cost_adjust");
    assert_eq!(balance(&pool, s.var_cost_adj).await, -300);
}

/// Forward-only: a cost_adjust does not retroactively alter prior
/// depletion transfer rows or their value/variance totals. Path:
///   1. inv_adj seed raw 100 @ $5 → pool=$500.
///   2. inv_adj depletion -20 @ $5 → pool=$400, qty=80.
///   3. cost_adjust raw to $8 → pool=$640. var=-240.
///   4. inv_adj depletion -10 @ $5 (caller still using $5 unit_cost) →
///      pool=$590, qty=70.
///
/// The first depletion's transfer at $5/unit is unchanged; cost_adjust
/// did NOT post any retroactive correction against it. Each depletion
/// stands at the unit_cost the caller asserted at the time.
#[tokio::test]
async fn wac_perpetual_cost_adjust_is_forward_only() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = build_wac_scaffold(&pool, "WAC-FWD").await;

    call_inv_adjust(&pool, &s.sku_id, &s.loc_id, 100, Some(5), "raw", "2026-04-10",
        &fresh_uuid(&pool).await)
        .await
        .expect("seed");

    // Capture the depletion's transfer value-leg amount before adjust.
    let dep1_id = call_inv_adjust(&pool, &s.sku_id, &s.loc_id, -20, None, "raw", "2026-04-11",
        &fresh_uuid(&pool).await)
        .await
        .expect("D1");
    assert_eq!(balance(&pool, s.raw_val).await, 400);

    let dep1_value_amount: i64 = sqlx::query_scalar(
        "SELECT amount FROM transfers
          WHERE document_id = $1::UUID AND credit_account_id = $2",
    )
    .bind(&dep1_id)
    .bind(s.raw_val)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep1_value_amount, 100, "D1 value-leg = 20 × $5");

    call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 8, "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("adjust to $8");
    assert_eq!(balance(&pool, s.raw_val).await, 640, "80 × $8");
    assert_eq!(balance(&pool, s.var_cost_adj).await, -240);

    // D1's transfer record stands.
    let dep1_value_amount_after: i64 = sqlx::query_scalar(
        "SELECT amount FROM transfers
          WHERE document_id = $1::UUID AND credit_account_id = $2",
    )
    .bind(&dep1_id)
    .bind(s.raw_val)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep1_value_amount_after, 100, "D1 value-leg unchanged");

    // No revaluation transfer was posted referencing D1's document.
    let cost_restate_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers
          WHERE document_id = $1::UUID AND reason = 'cost_restate'",
    )
    .bind(&dep1_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cost_restate_count, 0,
               "no retroactive cost_restate transfer against the prior depletion");

    // Subsequent depletion drains at the post-adjust running avg ($8/unit):
    // 10 × $8 = $80. Pool: $640 - $80 = $560.
    call_inv_adjust(&pool, &s.sku_id, &s.loc_id, -10, None, "raw", "2026-04-13",
        &fresh_uuid(&pool).await)
        .await
        .expect("D2");
    assert_eq!(balance(&pool, s.raw_val).await, 560,
               "D2 drains 10 × $8 (post-adjust avg)");
}

/// Idempotency under interleaved load. Path: receipt → cost_adjust →
/// unrelated inv_adj on FG → replay cost_adjust → depletion. Replay
/// returns the same audit row id and posts no second transfer.
#[tokio::test]
async fn wac_perpetual_cost_adjust_idempotent_under_interleaved_load() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = build_wac_scaffold(&pool, "WAC-IDEM").await;
    let vendor = fresh_vendor(&pool, "VEN-IDEM", "USD").await;
    let po = fresh_po(&pool, &vendor).await;
    let _ven_qty = open_account(&pool, "vendor_pool", "qty", None, None, None, Some(&vendor), None, "credit").await;
    let _ap_unsettled = open_account(&pool, "ap_unsettled", "value", Some("USD"), None, None, Some(&vendor), None, "credit").await;
    let _ap = open_account(&pool, "ap", "value", Some("USD"), None, None, Some(&vendor), None, "credit").await;

    let line = fresh_po_line(&pool, &po, 1, &s.sku_id, &s.loc_id, 100, 5).await;
    call_po_receipt(&pool, &po, &line, 100, "2026-04-10", &fresh_uuid(&pool).await)
        .await
        .expect("receipt");

    let key_adj = fresh_uuid(&pool).await;
    let id1 = call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 7, "2026-04-11", &key_adj)
        .await
        .expect("adj");
    assert_eq!(balance(&pool, s.raw_val).await, 700);
    assert_eq!(balance(&pool, s.var_cost_adj).await, -200);

    call_inv_adjust(&pool, &s.sku_id, &s.loc_id, 10, Some(100), "fg", "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("inv_adj fg");

    let id2 = call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 7, "2026-04-11", &key_adj)
        .await
        .expect("adj replay");
    assert_eq!(id1, id2, "replay returns same audit row id");
    assert_eq!(balance(&pool, s.raw_val).await, 700, "no double-apply on raw");
    assert_eq!(balance(&pool, s.var_cost_adj).await, -200, "variance unchanged");

    // Count cost_adjustment transfers against the audit row: 1 (single posting).
    let adj_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers
          WHERE document_id = $1::UUID AND reason = 'cost_adjustment'",
    )
    .bind(&id1)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(adj_count, 1, "exactly one cost_adjustment transfer for the adjust");
}

/// Sequential write-up then write-down nets correctly with intervening
/// PO + inv_adj activity.
#[tokio::test]
async fn wac_perpetual_writeup_then_writedown_with_intervening_activity() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let s = build_wac_scaffold(&pool, "WAC-NET").await;

    // Seed.
    call_inv_adjust(&pool, &s.sku_id, &s.loc_id, 100, Some(10), "raw", "2026-04-10",
        &fresh_uuid(&pool).await)
        .await
        .expect("seed");
    assert_eq!(balance(&pool, s.raw_val).await, 1000);

    // Write-up to $14 → +400.
    call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 14, "2026-04-11",
        &fresh_uuid(&pool).await)
        .await
        .expect("adj +14");
    assert_eq!(balance(&pool, s.raw_val).await, 1400);
    assert_eq!(balance(&pool, s.var_cost_adj).await, -400);

    // Intervening: more inv_adj activity (+50 @ caller-asserted $14).
    call_inv_adjust(&pool, &s.sku_id, &s.loc_id, 50, Some(14), "raw", "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("intervening inv_adj");
    assert_eq!(balance(&pool, s.raw_val).await, 2100);
    assert_eq!(balance(&pool, s.qty_acct).await, 150);

    // Intervening: depletion at $14.
    call_inv_adjust(&pool, &s.sku_id, &s.loc_id, -50, None, "raw", "2026-04-13",
        &fresh_uuid(&pool).await)
        .await
        .expect("intervening dep");
    assert_eq!(balance(&pool, s.raw_val).await, 1400);
    assert_eq!(balance(&pool, s.qty_acct).await, 100);

    // Write-down to $9 → 100 × 9 = 900; delta = -500.
    call_cost_adjust(&pool, &s.sku_id, &s.loc_id, "raw", 9, "2026-04-14",
        &fresh_uuid(&pool).await)
        .await
        .expect("adj -9");
    assert_eq!(balance(&pool, s.raw_val).await, 900);
    assert_eq!(balance(&pool, s.var_cost_adj).await, -400 + 500,
               "net var: -400 (write-up) + 500 (write-down) = +100");
}

// ============================================================
// Tests — wac_periodic / retroactive paths
// ============================================================

/// wac_periodic: cost_adjust_retroactive interleaved with mid-period
/// depletions. Path:
///   1. inv_adj seed FG 100 @ $10 → pool=$1000.
///   2. inv_adj depletion -30 @ $10 (provisional) → pool=$700.
///   3. inv_adj receipt 50 @ $14 → pool=$1400, qty=120.
///   4. queue cost_adjust_retroactive target avg $13 (vs current pool avg).
///   5. inv_adj depletion -20 @ $11 (provisional, at running avg
///      $1400/120 = $11) → pool=$1180.
///   6. close period.
///
/// At close, both hooks compose (documented "double-correction"):
///   * wac_periodic_close_hook: final_avg = $1700/$150 = $11 (truncated
///     from 11.33). D1 (provisional $10) → variance = ($11-$10)*30 = $30.
///     D2 (provisional $11) → variance = 0.
///   * cost_adjust_retroactive_hook (retro target $13):
///       D1 orig amount $300, qty 30 → orig per-unit $10 → delta = ($13-$10)*30 = $90.
///       D2 orig amount $220, qty 20 → orig per-unit $11 → delta = ($13-$11)*20 = $40.
///     Total retro variance magnitude = $130.
#[tokio::test]
async fn wac_periodic_cost_adjust_retro_interleaved_with_depletions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "WACPER-RETRO", "wac_periodic").await;
    let loc_id = fresh_location(&pool, "WACPER-RETRO-LOC").await;
    let _qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let var_wac = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;
    let var_retro = account_id_by_kind_currency(&pool, "variance_cost_adjust_retro", Some("USD")).await;

    let pid = period_id(&pool, "2026-04").await;

    call_inv_adjust(&pool, &sku_id, &loc_id, 100, Some(10), "fg", "2026-04-05",
        &fresh_uuid(&pool).await)
        .await
        .expect("R1");
    call_inv_adjust(&pool, &sku_id, &loc_id, -30, None, "fg", "2026-04-10",
        &fresh_uuid(&pool).await)
        .await
        .expect("D1");
    assert_eq!(balance(&pool, fg_val).await, 700);

    call_inv_adjust(&pool, &sku_id, &loc_id, 50, Some(14), "fg", "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("R2");
    assert_eq!(balance(&pool, fg_val).await, 1400);

    call_cost_adjust_retro(&pool, pid, &sku_id, &loc_id, "fg", 13, "2026-04-15",
        &fresh_uuid(&pool).await)
        .await
        .expect("retro queued");

    call_inv_adjust(&pool, &sku_id, &loc_id, -20, None, "fg", "2026-04-18",
        &fresh_uuid(&pool).await)
        .await
        .expect("D2");
    assert_eq!(balance(&pool, fg_val).await, 1180);

    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close");

    // wac_periodic processed at least D1 (D2's variance was 0 since prov
    // unit matched final_avg). cost_adjust_retroactive processed both D1
    // and D2 (queue-row dep_count = 2). Both hooks ran.
    assert!(summary["hook_results"]["wac_periodic"].as_i64().unwrap() >= 1);
    assert!(summary["hook_results"]["cost_adjust_retroactive"].as_i64().unwrap() >= 1);
    let _ = (var_wac, var_retro);  // accounts net to zero (2-leg wash); audit
                                   // rows carry the magnitudes.

    // Read the retro queue audit row's total_variance:
    //   D1 (qty 30, orig $300, prov_unit $10) → ($13-$10)*30 = $90.
    //   D2 (qty 20, orig $220, prov_unit $11) → ($13-$11)*20 = $40.
    //   Total = $130.
    let retro_total: i64 = sqlx::query_scalar(
        "SELECT total_variance FROM inventory_cost_adjustments_retroactive
          WHERE target_period_id = $1",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retro_total, 130, "retro total_variance = $90 + $40");

    // wac_periodic provisional row finalized state: D1 variance $30, D2 $0.
    let wac_variances: Vec<i64> = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.period_id = $1 AND tp.cost_method = 'wac_periodic'
          ORDER BY tp.transfer_id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(wac_variances, vec![30, 0], "D1 +$30 drift, D2 zero");

    let unfinalized: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers_provisional
          WHERE period_id = $1 AND finalized_at IS NULL",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(unfinalized, 0, "all provisionals finalized");
}

/// Multi-class wac_periodic: a retro queued on FG only doesn't touch raw
/// even though both classes had in-period activity.
#[tokio::test]
async fn wac_periodic_retro_multi_class_independent_close() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_sku(&pool, "WACPER-MC", "wac_periodic").await;
    let loc_id = fresh_location(&pool, "WACPER-MC-LOC").await;
    let _qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let raw_val = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, None, "debit").await;
    let fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku_id), Some(&loc_id), None, None, "debit").await;

    let pid = period_id(&pool, "2026-04").await;

    call_inv_adjust(&pool, &sku_id, &loc_id, 100, Some(5), "raw", "2026-04-05",
        &fresh_uuid(&pool).await)
        .await
        .expect("raw seed");
    call_inv_adjust(&pool, &sku_id, &loc_id, -20, None, "raw", "2026-04-10",
        &fresh_uuid(&pool).await)
        .await
        .expect("raw depletion");

    call_inv_adjust(&pool, &sku_id, &loc_id, 50, Some(20), "fg", "2026-04-05",
        &fresh_uuid(&pool).await)
        .await
        .expect("fg seed");
    call_inv_adjust(&pool, &sku_id, &loc_id, -10, None, "fg", "2026-04-12",
        &fresh_uuid(&pool).await)
        .await
        .expect("fg depletion");

    call_cost_adjust_retro(&pool, pid, &sku_id, &loc_id, "fg", 25, "2026-04-15",
        &fresh_uuid(&pool).await)
        .await
        .expect("retro fg only");

    assert_eq!(balance(&pool, raw_val).await, 400);
    assert_eq!(balance(&pool, fg_val).await, 800);

    let actor = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close");

    let raw_var_total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(variance_amount),0)::BIGINT
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.period_id = $1 AND tp.cost_method = 'wac_periodic'
            AND t.credit_account_id = $2",
    )
    .bind(pid)
    .bind(raw_val)
    .fetch_one(&pool)
    .await
    .unwrap();
    let fg_var_total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(variance_amount),0)::BIGINT
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.period_id = $1 AND tp.cost_method = 'wac_periodic'
            AND t.credit_account_id = $2",
    )
    .bind(pid)
    .bind(fg_val)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(raw_var_total, 0, "raw class no-drift on wac side");
    assert_eq!(fg_var_total, 0, "fg class also no-drift on wac side");

    // Retro audit row's total_variance: target $25 vs orig $20 on qty 10 = $50.
    // Raw class has no retro queued → no FG-cross-contamination.
    let retro_total: i64 = sqlx::query_scalar(
        "SELECT total_variance FROM inventory_cost_adjustments_retroactive
          WHERE target_period_id = $1 AND inventory_class = 'fg'",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retro_total, 50, "retro total = (25-20) * 10");

    let raw_retro_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_cost_adjustments_retroactive
          WHERE target_period_id = $1 AND inventory_class = 'raw'",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(raw_retro_count, 0, "no retro queued for raw class");
}
