//! Tier 1 of acct-8in (acct-bol). Tests wac_periodic + wac_retroactive
//! parent SKUs on single-routing-op WOs end-to-end through the BOM2
//! entry points + close hook.
//!
//! Scope:
//!   * wac_periodic single-op WO: depletion flagged via wo_complete_v;
//!     close hook recomputes from in-period receipts; variance posts.
//!   * Same shape for wac_retroactive (its close hook does chronological
//!     replay; for single-op WO with one wo_complete the math collapses
//!     to the same final-avg case).
//!   * Multi-op routing on wac_periodic / wac_retroactive raises P0026
//!     (deferred to acct-smn / acct-rso).

mod common;

use common::*;
use sqlx::PgPool;
use serde_json::json;

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
    .unwrap_or_else(|e| panic!("set_std_cost {sku_id}={cost}: {e}"));
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
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, currency, sku_id, location_id, routing_op, normal_side)
         VALUES ($1::account_kind, $2, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
         RETURNING id",
    )
    .bind(kind)
    .bind(ledger_kind)
    .bind(currency)
    .bind(sku_id)
    .bind(loc_id)
    .bind(routing_op)
    .bind(normal_side)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn create_wo(pool: &PgPool, wo_no: &str, parent_id: &str, fg_loc: &str, qty_target: i64) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(parent_id)
    .bind(fg_loc)
    .bind(qty_target)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_wo: {e}"))
}

async fn add_routing(pool: &PgPool, wo_id: &str, op: i32, name: &str) {
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)")
        .bind(wo_id)
        .bind(op)
        .bind(name)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("add_routing: {e}"));
}

async fn create_bom_header(pool: &PgPool, parent: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(parent)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom_header: {e}"))
}

async fn add_bom_item(pool: &PgPool, bom_id: i64, line_no: i32, applies_at_op: i32, comp: &str, comp_loc: &str, qty_per_parent: i64) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, scrap_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 0,
                 $4::UUID, $5::UUID, $6)",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(comp)
    .bind(comp_loc)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_item: {e}"));
}

async fn add_bom_service(pool: &PgPool, bom_id: i64, line_no: i32, applies_at_op: i32, class_code: &str, std_amount: i64) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, absorption_class_id, std_amount)
         SELECT $1, $2, 'service', 'per_unit', $3, 'op_arrival', ac.id, $5
           FROM absorption_classes ac WHERE ac.code = $4",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(class_code)
    .bind(std_amount)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_service: {e}"));
}

async fn pre_load_raw(pool: &PgPool, raw_qty: i64, raw_val: i64, qty: i64, value: i64) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_qty, "credit_account_id": void_qty,
          "amount": qty, "qty": qty, "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": raw_val, "credit_account_id": void_val,
          "amount": value, "qty": qty, "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("pre_load_raw: {e}"));
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

async fn period_id(pool: &PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

async fn call_wo_start(pool: &PgPool, wo_id: &str, key: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(pool: &PgPool, wo_id: &str, qty: i64, key: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, '2026-04-20'::DATE, $3::UUID, $4::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(qty)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

/// Single-op WAC WO: parent (cost_method) + 1 standard component (std=10),
/// op10 only, BOM has item (qty/p=2) + labor (5/unit).
struct SingleOpWo {
    wo_id: String,
    wip_qty_op10: i64,
    wip_val_op10: i64,
    fg_val_acct: i64,
    parent_id: String,
}

async fn scaffold_single_op(pool: &PgPool, parent_code: &str, cost_method: &str) -> SingleOpWo {
    let parent = fresh_sku(pool, parent_code, cost_method).await;
    let comp = fresh_sku(pool, &format!("{parent_code}-C"), "standard").await;
    let raw_loc = fresh_location(pool, &format!("{parent_code}-RAW")).await;
    let fg_loc = fresh_location(pool, &format!("{parent_code}-FG")).await;

    set_std_cost(pool, &comp, 10).await;

    let wip_qty_op10 = open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let wip_val_op10 = open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let _fg_qty = open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let fg_val_acct = open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;
    let _consumed = open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit").await;
    let _scrap = open_account(pool, "stock_scrap", "qty", None, Some(&parent), None, None, "debit").await;
    let raw_qty = open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, "debit").await;
    let raw_val = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, "debit").await;

    pre_load_raw(pool, raw_qty, raw_val, 200, 2000).await;

    let wo_id = create_wo(pool, &format!("{parent_code}-WO"), &parent, &fg_loc, 10).await;
    add_routing(pool, &wo_id, 10, "MILL").await;

    let bom_id = create_bom_header(pool, &parent).await;
    add_bom_item(pool, bom_id, 1, 10, &comp, &raw_loc, 2).await;
    add_bom_service(pool, bom_id, 2, 10, "labor_std", 5).await;

    SingleOpWo { wo_id, wip_qty_op10, wip_val_op10, fg_val_acct, parent_id: parent }
}

async fn close_period(pool: &PgPool, pid: i64) -> serde_json::Value {
    let actor = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(pool)
    .await
    .expect("close_period")
}

// ============================================================
// Tests
// ============================================================

/// Single-op WO on wac_periodic parent. Lifecycle:
///   wo_start: rm_issue 20 × $10 = $200 + labor 10 × $5 = $50 → pool@op10 = $250.
///   wo_complete(10): provisional unit = 250/10 = 25. drain $250.
///   wo_complete_v gets flagged into transfers_provisional with cost_method='wac_periodic'.
///   close period: pool_value_in = $250; pool_qty_in = 10. final_avg = 25. Variance = 0.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_single_op_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let wo = scaffold_single_op(&pool, "WACPER", "wac_periodic").await;
    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 250, "pool@op10 after wo_start");
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 10);

    call_wo_complete(&pool, &wo.wo_id, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 250, "FG drained at provisional avg 25");
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 0);

    // Verify the wo_complete_v depletion was flagged.
    let provisional_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic'
           AND tp.period_id = $1
           AND t.reason = 'wo_complete_v'",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .expect("provisional count");
    assert_eq!(provisional_count, 1, "wo_complete_v depletion flagged");

    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(1));

    let (finalized_at, variance_amount): (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT finalized_at::text, variance_amount
           FROM transfers_provisional tp
           JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method = 'wac_periodic' AND t.reason = 'wo_complete_v'
            AND tp.period_id = $1",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .expect("finalized row");

    assert!(finalized_at.is_some(), "row finalized");
    assert_eq!(variance_amount, Some(0), "no drift: provisional == final");
}

/// Drift case: TWO WOs on the same wac_periodic parent in the same period
/// at different unit costs force the close hook to post variance.
///
/// Setup (single op10 routing):
///   WO1: comp std=10, qty/p=2, labor=5. wo_start: pool +250 (200 rm + 50 lab),
///        qty=10. wo_complete(10): provisional avg = 250/10 = 25. drain 250.
///   WO2: SAME parent, different comp (std=20, qty/p=2), no labor.
///        wo_start: pool +200 (200 rm), qty=5. wo_complete(5):
///        provisional avg = 200/5 = 40. drain 200.
///
/// Close: pool_value_in = 250+200 = 450. pool_qty_in (parent qty inflows on
/// stock_wip(parent, op10)) = 10+5 = 15. final_avg = 450/15 = 30.
///   WO1 wo_complete_v variance = (30-25)*10 = +50
///   WO2 wo_complete_v variance = (30-40)*5  = -50
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_drift_posts_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "WACDRIFT", "wac_periodic").await;
    let comp_a = fresh_sku(&pool, "WACDRIFT-A", "standard").await;
    let comp_b = fresh_sku(&pool, "WACDRIFT-B", "standard").await;
    let raw_loc_a = fresh_location(&pool, "WACDRIFT-RAW-A").await;
    let raw_loc_b = fresh_location(&pool, "WACDRIFT-RAW-B").await;
    let fg_loc = fresh_location(&pool, "WACDRIFT-FG").await;

    set_std_cost(&pool, &comp_a, 10).await;
    set_std_cost(&pool, &comp_b, 20).await;

    let wip_qty = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let wip_val = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let _fg_qty = open_account(&pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let fg_val = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;
    for c in [&comp_a, &comp_b] {
        let _ = open_account(&pool, "stock_consumed", "qty", None, Some(c), None, None, "debit").await;
    }
    let _scrap = open_account(&pool, "stock_scrap", "qty", None, Some(&parent), None, None, "debit").await;
    let raw_qty_a = open_account(&pool, "stock_available", "qty", None, Some(&comp_a), Some(&raw_loc_a), None, "debit").await;
    let raw_val_a = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp_a), Some(&raw_loc_a), None, "debit").await;
    let raw_qty_b = open_account(&pool, "stock_available", "qty", None, Some(&comp_b), Some(&raw_loc_b), None, "debit").await;
    let raw_val_b = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&comp_b), Some(&raw_loc_b), None, "debit").await;
    let var_period = account_id_by_kind_currency(&pool, "variance_wac_period", Some("USD")).await;

    pre_load_raw(&pool, raw_qty_a, raw_val_a, 100, 1000).await;
    pre_load_raw(&pool, raw_qty_b, raw_val_b, 100, 2000).await;

    // WO1: qty 10, comp_a + labor.
    let wo1 = create_wo(&pool, "WACDRIFT-WO1", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo1, 10, "MILL").await;
    let bom1 = create_bom_header(&pool, &parent).await;
    add_bom_item(&pool, bom1, 1, 10, &comp_a, &raw_loc_a, 2).await;
    add_bom_service(&pool, bom1, 2, 10, "labor_std", 5).await;

    call_wo_start(&pool, &wo1, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip_val).await, 250, "WO1 wo_start: pool=200+50");
    call_wo_complete(&pool, &wo1, 10, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip_val).await, 0);

    // WO2: SAME parent (so SAME pool gets new receipts), different comp, no labor.
    // Different bom_header alternate_no so the UNIQUE (parent, alternate_no,
    // revision_no) constraint allows it to coexist with bom1. Pin via wo2.bom_id.
    let wo2 = create_wo(&pool, "WACDRIFT-WO2", &parent, &fg_loc, 5).await;
    add_routing(&pool, &wo2, 10, "MILL").await;
    let bom2: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 2, 'A', FALSE, 'active') RETURNING id",
    )
    .bind(&parent)
    .fetch_one(&pool)
    .await
    .expect("bom2");
    add_bom_item(&pool, bom2, 1, 10, &comp_b, &raw_loc_b, 2).await;
    // No labor on WO2 — different unit cost mix to force drift.
    sqlx::query("UPDATE work_orders SET bom_id = $1 WHERE id = $2::UUID")
        .bind(bom2).bind(&wo2).execute(&pool).await.unwrap();

    call_wo_start(&pool, &wo2, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip_val).await, 200, "WO2 wo_start: pool=2*5*20");
    call_wo_complete(&pool, &wo2, 5, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wip_val).await, 0);

    let pid = period_id(&pool, "2026-04").await;
    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(2), "2 depletions finalized");

    // Read variance per row.
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT t.amount, tp.variance_amount
           FROM transfers_provisional tp JOIN transfers t ON t.id = tp.transfer_id
          WHERE tp.cost_method = 'wac_periodic' AND t.reason = 'wo_complete_v'
            AND tp.period_id = $1
          ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .expect("finalized rows");

    assert_eq!(rows.len(), 2, "two depletions");
    assert_eq!(rows[0], (250, 50), "WO1: drain 250 + variance +50");
    assert_eq!(rows[1], (200, -50), "WO2: drain 200 + variance -50");

    // For inv_value_wip source pools, variance posts a SINGLE-leg event (not
    // the 2-leg wash used for raw/fg) so the WIP pool isn't credited into
    // negative territory by intermediate variance postings. variance_wac_period
    // accumulates per-row variances:
    //   WO1 variance +50: dr fg / cr variance — adds $50 credit to variance.
    //   WO2 variance -50: dr variance / cr fg — adds $50 debit to variance.
    let (var_dr, var_cr): (i64, i64) = sqlx::query_as(
        "SELECT debits_total::BIGINT, credits_total::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(var_period)
    .fetch_one(&pool)
    .await
    .expect("variance acct");
    assert_eq!(var_dr, 50, "variance debits = WO2's 50");
    assert_eq!(var_cr, 50, "variance credits = WO1's 50");

    // Pool drained net-zero across the variance posts.
    assert_eq!(balance(&pool, wip_val).await, 0);
    let _ = fg_val;
}

/// scrap_v on a wac_periodic parent gets flagged into transfers_provisional
/// and finalized at close. With single-op single-WO no drift, variance=0.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_scrap_finalized() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let wo = scaffold_single_op(&pool, "WACSCRAP", "wac_periodic").await;
    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    // pool=250, qty=10, unit=25.

    // scrap 2 at op10. accumulated_unit_cost = 250/10 = 25. variance_scrap += 50.
    let posted_by = fresh_uuid(&pool).await;
    let scrap_key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_scrap($1::UUID, $2, $3, '2026-04-20'::DATE, $4::UUID, $5::UUID, NULL)",
    )
    .bind(&wo.wo_id).bind(10i32).bind(2i64)
    .bind(&posted_by).bind(&scrap_key)
    .execute(&pool).await.expect("post_scrap");

    // pool=250-50=200, qty=8.
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 200);
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 8);

    // Finish the WO: qty_completed=8 + qty_scrapped=2 = 10 = target.
    call_wo_complete(&pool, &wo.wo_id, 8, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 200);
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 0);

    // Both scrap_v and wo_complete_v should be flagged.
    let reasons: Vec<String> = sqlx::query_scalar(
        "SELECT t.reason::text FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
         ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .expect("flagged reasons");
    assert_eq!(reasons, vec!["scrap_v".to_string(), "wo_complete_v".to_string()]);

    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(2));

    // Both finalized. No drift since pool only had wo_start receipts.
    let variances: Vec<i64> = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
         ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .expect("variances");
    assert_eq!(variances, vec![0, 0], "no drift");
}

/// Multi-op routing on wac_periodic parent must raise P0026 with hint to acct-smn.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_multi_op_routing_raises_p0026() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "WACPER-MO", "wac_periodic").await;
    let fg_loc = fresh_location(&pool, "WACPER-MO-FG").await;
    let _w1 = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let _w2 = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(20), "debit").await;
    let _v1 = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let _v2 = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(20), "debit").await;
    let wo_id = create_wo(&pool, "WACPER-MO-WO", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;
    add_routing(&pool, &wo_id, 20, "FINISH").await;

    expect_sqlstate(
        "P0026",
        || async { call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

/// Partial wo_complete on wac_periodic: two wo_complete_v events flagged,
/// both finalized at close. With single-WO single-period no drift, both
/// variances are 0.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_partial_wo_complete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let wo = scaffold_single_op(&pool, "WACPART", "wac_periodic").await;
    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    // pool=250, qty=10.

    call_wo_complete(&pool, &wo.wo_id, 4, &fresh_uuid(&pool).await).await.unwrap();
    // partial: drain 4 × 25 = 100. pool=150, qty=6.
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 100);
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 150);

    call_wo_complete(&pool, &wo.wo_id, 6, &fresh_uuid(&pool).await).await.unwrap();
    // final: drain 6 × 25 = 150. pool=0.
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 250);
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 0);

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
           AND t.reason = 'wo_complete_v'",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2, "two wo_complete_v depletions flagged");

    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(2));

    let variances: Vec<i64> = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
         ORDER BY t.id",
    )
    .bind(pid)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(variances, vec![0, 0]);
}

/// Fully-scrapped wac_periodic WO closed via post_wo_close_unproduced.
/// All 10 units scrapped at op10; post_wo_close_unproduced cleans up
/// residual (none in this case since scrap drains exactly). scrap_v
/// flagged + finalized at close with variance=0.
#[tokio::test(flavor = "multi_thread")]
async fn wac_periodic_fully_scrapped_close_unproduced() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let wo = scaffold_single_op(&pool, "WACSCRAPALL", "wac_periodic").await;
    let pid = period_id(&pool, "2026-04").await;

    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();

    // scrap all 10. unit=25. scrap_v=250.
    let posted_by = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_scrap($1::UUID, $2, $3, '2026-04-20'::DATE, $4::UUID, $5::UUID, NULL)",
    )
    .bind(&wo.wo_id).bind(10i32).bind(10i64)
    .bind(&posted_by).bind(&fresh_uuid(&pool).await)
    .execute(&pool).await.expect("post_scrap 10");

    assert_eq!(balance(&pool, wo.wip_val_op10).await, 0);
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 0);

    // Close unproduced.
    let key = fresh_uuid(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_close_unproduced($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo.wo_id).bind(&posted_by).bind(&key)
    .execute(&pool).await.expect("post_wo_close_unproduced");

    // WO is closed.
    let status: String = sqlx::query_scalar(
        "SELECT status::text FROM work_orders WHERE id = $1::UUID",
    )
    .bind(&wo.wo_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(status, "closed");

    // Close period — scrap_v depletion finalizes with variance=0.
    let summary = close_period(&pool, pid).await;
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(1));

    let variance: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM transfers_provisional tp
         JOIN transfers t ON t.id = tp.transfer_id
         WHERE tp.cost_method = 'wac_periodic' AND tp.period_id = $1
           AND t.reason = 'scrap_v'",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(variance, 0);
}

/// wac_retroactive on WIP raises P0026 in tier 1: its per-event chronological
/// replay reads transfers.qty from value-pool events (rm_issue_to_wo carries
/// component qty), giving wrong final_avg for inv_value_wip. The fix needs
/// merging qty-side events into the replay walk — tier 3 (acct-rso).
#[tokio::test(flavor = "multi_thread")]
async fn wac_retroactive_raises_p0026_pending_tier3() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "WACRETRO", "wac_retroactive").await;
    let fg_loc = fresh_location(&pool, "WACRETRO-FG").await;
    let _w = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let _v = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let wo_id = create_wo(&pool, "WACRETRO-WO", &parent, &fg_loc, 10).await;
    add_routing(&pool, &wo_id, 10, "MILL").await;

    expect_sqlstate(
        "P0026",
        || async { call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}
