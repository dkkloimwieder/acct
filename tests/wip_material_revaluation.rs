//! `acct-bru` — Phase 2 Epic G: WIP material revaluation companion to
//! post_standard_cost_roll.
//!
//! Coverage:
//!   * Gate trips with the default (p_revalue_wip = FALSE) when WIP
//!     exists — preserves mig 0028/0071 behavior.
//!   * Single-op WIP revaluation: WIP value grows by parent_qty × Δstd;
//!     variance routes through variance_wip_revaluation.
//!   * Multi-op WIP with absorbed labor / overhead: each WIP pool
//!     revalues by its own pool_qty × Δstd (read from paired
//!     stock_wip); labor_applied / oh_applied balances UNCHANGED.
//!   * Sequence of rolls with WIP carrying across — second roll's
//!     prior is the first roll's target; no double-counting.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffold (mirrors wo_lifecycle.rs but trimmed to what
// these tests need).
// ============================================================

#[allow(dead_code)]
struct Wo {
    parent_id: String,
    raw_loc_id: String,
    fg_loc_id: String,
    comp_a_id: String,
    comp_b_id: Option<String>,
    wo_id: String,
    bom_id: i64,
    wip_qty_op10: i64,
    wip_qty_op20: i64,
    wip_val_op10: i64,
    wip_val_op20: i64,
    raw_qty_a: i64,
    raw_qty_b: i64,
    raw_val_a: i64,
    raw_val_b: i64,
    labor_applied: i64,
    oh_applied: i64,
    variance_wip_reval: i64,
    creation_void: i64,
}

async fn fresh_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
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

async fn set_std_cost_at(pool: &PgPool, sku_id: &str, cost: i64, eff_at: &str) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(eff_at)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("set_std_cost_at {sku_id}={cost} eff={eff_at}: {e}"));
}

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
    .unwrap_or_else(|e| panic!("open {kind}/{ledger_kind} routing_op={routing_op:?}: {e}"))
}

async fn create_wo(
    pool: &PgPool,
    wo_no: &str,
    parent_id: &str,
    fg_loc_id: &str,
    qty_target: i64,
) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders
            (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID)
         RETURNING id::text",
    )
    .bind(wo_no)
    .bind(parent_id)
    .bind(fg_loc_id)
    .bind(qty_target)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_wo {wo_no}: {e}"))
}

async fn add_routing(pool: &PgPool, wo_id: &str, routing_op: i32, op_name: &str) {
    sqlx::query(
        "INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)",
    )
    .bind(wo_id)
    .bind(routing_op)
    .bind(op_name)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_routing {routing_op}: {e}"));
}

async fn create_bom_header_for(pool: &PgPool, parent_id: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers
            (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active')
         RETURNING id",
    )
    .bind(parent_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_bom_header parent={parent_id}: {e}"))
}

async fn add_bom_item_by_id(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    applies_at_op: i32,
    component_id: &str,
    component_loc_id: &str,
    qty_per_parent: i64,
) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, $2, 'item', 'per_unit', $3, 'op_arrival', 100,
                 $4::UUID, $5::UUID, $6)",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(component_id)
    .bind(component_loc_id)
    .bind(qty_per_parent)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_item line={line_no}: {e}"));
}

async fn add_bom_service_per_unit(
    pool: &PgPool,
    bom_id: i64,
    line_no: i32,
    applies_at_op: i32,
    class_code: &str,
    std_amount: i64,
) {
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at,
             absorption_class_id, std_amount)
         SELECT $1, $2, 'service', 'per_unit', $3, 'op_arrival',
                ac.id, $5
           FROM absorption_classes ac WHERE ac.code = $4",
    )
    .bind(bom_id)
    .bind(line_no)
    .bind(applies_at_op)
    .bind(class_code)
    .bind(std_amount)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_bom_service line={line_no} class={class_code}: {e}"));
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Single-component WO scaffold. parent_std rolled up to comp_a×2@10 = 20.
/// 1 routing op (op10). No services unless caller opts in.
async fn scaffold_wo_single_op(pool: &PgPool, wo_no: &str, qty_target: i64) -> Wo {
    let parent = fresh_sku(pool, &format!("BRU-P-{wo_no}")).await;
    let comp_a = fresh_sku(pool, &format!("BRU-CA-{wo_no}")).await;
    let raw_loc = fresh_location(pool, &format!("BRU-RAW-{wo_no}")).await;
    let fg_loc = fresh_location(pool, &format!("BRU-FG-{wo_no}")).await;

    set_std_cost_at(pool, &parent, 20, "2026-01-01").await;
    set_std_cost_at(pool, &comp_a, 10, "2026-01-01").await;

    let wip_qty_op10 = open_account(
        pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit",
    )
    .await;
    let wip_val_op10 = open_account(
        pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit",
    )
    .await;
    let raw_qty_a = open_account(
        pool, "stock_available", "qty", None, Some(&comp_a), Some(&raw_loc), None, "debit",
    )
    .await;
    let raw_val_a = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&comp_a), Some(&raw_loc), None, "debit",
    )
    .await;
    let _consumed_a = open_account(
        pool, "stock_consumed", "qty", None, Some(&comp_a), None, None, "debit",
    )
    .await;

    let labor_applied = account_id_by_kind_currency(pool, "labor_applied", Some("USD")).await;
    let oh_applied = account_id_by_kind_currency(pool, "oh_applied", Some("USD")).await;
    let variance_wip_reval =
        account_id_by_kind_currency(pool, "variance_wip_revaluation", Some("USD")).await;
    let creation_void = account_id_by_kind_currency(pool, "creation_void", None).await;

    let wo_id = create_wo(pool, wo_no, &parent, &fg_loc, qty_target).await;
    add_routing(pool, &wo_id, 10, "MILL").await;

    let bom_id = create_bom_header_for(pool, &parent).await;
    add_bom_item_by_id(pool, bom_id, 1, 10, &comp_a, &raw_loc, 2).await;

    Wo {
        parent_id: parent,
        raw_loc_id: raw_loc,
        fg_loc_id: fg_loc,
        comp_a_id: comp_a,
        comp_b_id: None,
        wo_id,
        bom_id,
        wip_qty_op10,
        wip_qty_op20: 0,
        wip_val_op10,
        wip_val_op20: 0,
        raw_qty_a,
        raw_qty_b: 0,
        raw_val_a,
        raw_val_b: 0,
        labor_applied,
        oh_applied,
        variance_wip_reval,
        creation_void,
    }
}

/// Two-component, two-op WO scaffold. parent_std rolled up = 67 from
/// comp_a×2@10 + comp_b×1@20 + op10 labor 5 + op10 oh 3 + op20 labor 7
/// + op20 oh 12.
async fn scaffold_wo_multi_op(pool: &PgPool, wo_no: &str, qty_target: i64) -> Wo {
    let parent = fresh_sku(pool, &format!("BRU-P-{wo_no}")).await;
    let comp_a = fresh_sku(pool, &format!("BRU-CA-{wo_no}")).await;
    let comp_b = fresh_sku(pool, &format!("BRU-CB-{wo_no}")).await;
    let raw_loc = fresh_location(pool, &format!("BRU-RAW-{wo_no}")).await;
    let fg_loc = fresh_location(pool, &format!("BRU-FG-{wo_no}")).await;

    set_std_cost_at(pool, &parent, 67, "2026-01-01").await;
    set_std_cost_at(pool, &comp_a, 10, "2026-01-01").await;
    set_std_cost_at(pool, &comp_b, 20, "2026-01-01").await;

    let wip_qty_op10 = open_account(
        pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit",
    )
    .await;
    let wip_qty_op20 = open_account(
        pool, "stock_wip", "qty", None, Some(&parent), None, Some(20), "debit",
    )
    .await;
    let wip_val_op10 = open_account(
        pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit",
    )
    .await;
    let wip_val_op20 = open_account(
        pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(20), "debit",
    )
    .await;
    let raw_qty_a = open_account(
        pool, "stock_available", "qty", None, Some(&comp_a), Some(&raw_loc), None, "debit",
    )
    .await;
    let raw_qty_b = open_account(
        pool, "stock_available", "qty", None, Some(&comp_b), Some(&raw_loc), None, "debit",
    )
    .await;
    let raw_val_a = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&comp_a), Some(&raw_loc), None, "debit",
    )
    .await;
    let raw_val_b = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&comp_b), Some(&raw_loc), None, "debit",
    )
    .await;
    let _consumed_a = open_account(
        pool, "stock_consumed", "qty", None, Some(&comp_a), None, None, "debit",
    )
    .await;
    let _consumed_b = open_account(
        pool, "stock_consumed", "qty", None, Some(&comp_b), None, None, "debit",
    )
    .await;
    // FG accounts to ensure raw/fg revaluation loop has nothing to do
    // for these tests (no fg balance pre-roll).
    let _fg_qty = open_account(
        pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit",
    )
    .await;
    let _fg_val = open_account(
        pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit",
    )
    .await;

    let labor_applied = account_id_by_kind_currency(pool, "labor_applied", Some("USD")).await;
    let oh_applied = account_id_by_kind_currency(pool, "oh_applied", Some("USD")).await;
    let variance_wip_reval =
        account_id_by_kind_currency(pool, "variance_wip_revaluation", Some("USD")).await;
    let creation_void = account_id_by_kind_currency(pool, "creation_void", None).await;

    let wo_id = create_wo(pool, wo_no, &parent, &fg_loc, qty_target).await;
    add_routing(pool, &wo_id, 10, "MILL").await;
    add_routing(pool, &wo_id, 20, "FINISH").await;

    let bom_id = create_bom_header_for(pool, &parent).await;
    add_bom_item_by_id(pool, bom_id, 1, 10, &comp_a, &raw_loc, 2).await;
    add_bom_item_by_id(pool, bom_id, 2, 10, &comp_b, &raw_loc, 1).await;
    add_bom_service_per_unit(pool, bom_id, 3, 10, "labor_std", 5).await;
    add_bom_service_per_unit(pool, bom_id, 4, 10, "oh_std", 3).await;
    add_bom_service_per_unit(pool, bom_id, 5, 20, "labor_std", 7).await;
    add_bom_service_per_unit(pool, bom_id, 6, 20, "oh_std", 12).await;

    Wo {
        parent_id: parent,
        raw_loc_id: raw_loc,
        fg_loc_id: fg_loc,
        comp_a_id: comp_a,
        comp_b_id: Some(comp_b),
        wo_id,
        bom_id,
        wip_qty_op10,
        wip_qty_op20,
        wip_val_op10,
        wip_val_op20,
        raw_qty_a,
        raw_qty_b,
        raw_val_a,
        raw_val_b,
        labor_applied,
        oh_applied,
        variance_wip_reval,
        creation_void,
    }
}

async fn pre_load_raw(
    pool: &PgPool,
    wo: &Wo,
    qty_a: i64,
    qty_b: i64,
    value_a: i64,
    value_b: i64,
) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = wo.creation_void;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let mut events = vec![
        json!({
            "reason": "cycle_count_adj",
            "document_kind": "wo_test_seed", "document_id": doc_id,
            "debit_account_id": wo.raw_qty_a, "credit_account_id": void_qty,
            "amount": qty_a, "qty": qty_a,
            "business_date": "2026-04-15",
            "idempotency_key": fresh_uuid(pool).await,
            "posted_by": posted_by
        }),
        json!({
            "reason": "cycle_count_adj",
            "document_kind": "wo_test_seed", "document_id": doc_id,
            "debit_account_id": wo.raw_val_a, "credit_account_id": void_val,
            "amount": value_a, "qty": qty_a,
            "business_date": "2026-04-15",
            "idempotency_key": fresh_uuid(pool).await,
            "posted_by": posted_by
        }),
    ];
    if wo.comp_b_id.is_some() {
        events.push(json!({
            "reason": "cycle_count_adj",
            "document_kind": "wo_test_seed", "document_id": doc_id,
            "debit_account_id": wo.raw_qty_b, "credit_account_id": void_qty,
            "amount": qty_b, "qty": qty_b,
            "business_date": "2026-04-15",
            "idempotency_key": fresh_uuid(pool).await,
            "posted_by": posted_by
        }));
        events.push(json!({
            "reason": "cycle_count_adj",
            "document_kind": "wo_test_seed", "document_id": doc_id,
            "debit_account_id": wo.raw_val_b, "credit_account_id": void_val,
            "amount": value_b, "qty": qty_b,
            "business_date": "2026-04-15",
            "idempotency_key": fresh_uuid(pool).await,
            "posted_by": posted_by
        }));
    }
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(serde_json::Value::Array(events))
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("pre_load_raw: {e}"));
}

async fn call_wo_start(pool: &PgPool, wo_id: &str) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_start($1::UUID, '2026-04-20'::DATE, $2::UUID, $3::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn call_op_move(pool: &PgPool, wo_id: &str, from: i32, to: i32, qty: i64) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_op_move($1::UUID, $2, $3, $4, '2026-04-20'::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(from)
    .bind(to)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn call_roll(
    pool: &PgPool,
    sku: &str,
    new_cost: i64,
    effective_at: &str,
    business_date: &str,
    expected_old: Option<i64>,
    revalue_wip: bool,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let idem = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_standard_cost_roll(
            $1::UUID, $2::BIGINT, $3::DATE, $4::DATE,
            $5::UUID, $6::UUID, NULL, $7::BIGINT, $8::BOOLEAN
         )::text",
    )
    .bind(sku)
    .bind(new_cost)
    .bind(effective_at)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&idem)
    .bind(expected_old)
    .bind(revalue_wip)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests
// ============================================================

/// Default (p_revalue_wip = FALSE) preserves the mig 0028/0071 gate:
/// rolling a SKU with open WIP raises P0006.
#[tokio::test]
async fn gate_still_trips_when_p_revalue_wip_false() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_single_op(&pool, "GATE-A", 50).await;
    pre_load_raw(&pool, &wo, 200, 0, 2000, 0).await;
    call_wo_start(&pool, &wo.wo_id).await.expect("wo_start");

    // WIP@op10 is non-zero (qty=50, value=1000). Default flag → P0006.
    expect_sqlstate("P0006", || async {
        call_roll(&pool, &wo.parent_id, 30, "2026-04-25", "2026-04-25", Some(20), false).await
    })
    .await;
}

/// Single-op WIP at qty=50, parent_std=20 → WIP value=1000. Roll
/// 20→30 with p_revalue_wip=TRUE: WIP value grows by 50×10=500;
/// variance_wip_revaluation receives a 500 credit (delta is positive,
/// debit-side is the WIP pool).
#[tokio::test]
async fn single_op_wip_revaluation_positive_delta() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_single_op(&pool, "SINGLE-A", 50).await;
    pre_load_raw(&pool, &wo, 200, 0, 2000, 0).await;
    call_wo_start(&pool, &wo.wo_id).await.expect("wo_start");

    // Sanity: WIP@op10 at qty=50, value=1000 (= 50 parents × 20 std).
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 50);
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 1000);

    let wip_var_before = balance(&pool, wo.variance_wip_reval).await;

    let _ = call_roll(&pool, &wo.parent_id, 30, "2026-04-25", "2026-04-25", Some(20), true)
        .await
        .expect("roll with revalue_wip");

    // WIP@op10 grows by 50 × (30 − 20) = 500.
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 1500);
    // qty pool untouched.
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 50);
    // variance_wip_revaluation took the credit-side: balance dropped
    // by 500 (debit-normal probe; credit-side movement is negative on
    // debits−credits).
    let wip_var_after = balance(&pool, wo.variance_wip_reval).await;
    assert_eq!(wip_var_before - wip_var_after, 500);

    assert_invariants_hold(&pool, "single_op_wip_revaluation_positive_delta").await;
}

/// Multi-op WIP with absorbed labor / overhead. After op_move 20 to
/// op20, WIP@op10 has 30 parents and WIP@op20 has 20. Roll 67→80
/// (Δ=13) with p_revalue_wip=TRUE: WIP@op10 grows by 30×13=390;
/// WIP@op20 grows by 20×13=260; total variance = 650. The
/// labor_applied / oh_applied account balances are UNCHANGED — the
/// absorbed-class portion of inv_value_wip is not scaled by the roll.
#[tokio::test]
async fn multi_op_wip_revaluation_labor_oh_untouched() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_multi_op(&pool, "MULTI-A", 50).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id).await.expect("wo_start");
    call_op_move(&pool, &wo.wo_id, 10, 20, 20).await.expect("op_move");

    // After wo_start (qty=50) + op_move(20 from op10→op20):
    //   WIP@op10 qty = 30
    //   WIP@op20 qty = 20
    let wip10_qty_before = balance(&pool, wo.wip_qty_op10).await;
    let wip20_qty_before = balance(&pool, wo.wip_qty_op20).await;
    assert_eq!(wip10_qty_before, 30);
    assert_eq!(wip20_qty_before, 20);

    let wip10_val_before = balance(&pool, wo.wip_val_op10).await;
    let wip20_val_before = balance(&pool, wo.wip_val_op20).await;
    let labor_before = balance(&pool, wo.labor_applied).await;
    let oh_before = balance(&pool, wo.oh_applied).await;
    let wip_var_before = balance(&pool, wo.variance_wip_reval).await;

    let _ = call_roll(&pool, &wo.parent_id, 80, "2026-04-25", "2026-04-25", Some(67), true)
        .await
        .expect("roll with revalue_wip");

    // Each WIP pool grows by pool_qty × Δstd.
    let delta = 80 - 67;
    let exp10 = wip10_val_before + 30 * delta;
    let exp20 = wip20_val_before + 20 * delta;
    assert_eq!(balance(&pool, wo.wip_val_op10).await, exp10);
    assert_eq!(balance(&pool, wo.wip_val_op20).await, exp20);

    // Labor / OH accounts unchanged — roll only scales the material
    // slice; absorbed-class P&L stays put.
    assert_eq!(balance(&pool, wo.labor_applied).await, labor_before);
    assert_eq!(balance(&pool, wo.oh_applied).await, oh_before);

    // variance_wip_revaluation took the credit-side total = 30×13 + 20×13 = 650.
    let wip_var_after = balance(&pool, wo.variance_wip_reval).await;
    assert_eq!(wip_var_before - wip_var_after, 30 * delta + 20 * delta);

    assert_invariants_hold(&pool, "multi_op_wip_revaluation_labor_oh_untouched").await;
}

/// Sequence of two rolls with WIP carrying across. Each roll's prior
/// is the PREVIOUS roll's target — pool_qty × Δstd is correctly
/// applied each time, no double-count.
#[tokio::test]
async fn sequence_of_rolls_with_wip_carryover() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_multi_op(&pool, "SEQ-A", 40).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id).await.expect("wo_start");
    call_op_move(&pool, &wo.wo_id, 10, 20, 15).await.expect("op_move");

    let wip10_qty = balance(&pool, wo.wip_qty_op10).await;
    let wip20_qty = balance(&pool, wo.wip_qty_op20).await;
    assert_eq!(wip10_qty, 25);
    assert_eq!(wip20_qty, 15);

    let wip10_val_t0 = balance(&pool, wo.wip_val_op10).await;
    let wip20_val_t0 = balance(&pool, wo.wip_val_op20).await;
    let wip_var_t0 = balance(&pool, wo.variance_wip_reval).await;

    // Roll #1: 67 → 80 (Δ=13).
    let _ = call_roll(&pool, &wo.parent_id, 80, "2026-04-25", "2026-04-25", Some(67), true)
        .await
        .expect("roll #1");

    let exp10_t1 = wip10_val_t0 + 25 * 13;
    let exp20_t1 = wip20_val_t0 + 15 * 13;
    assert_eq!(balance(&pool, wo.wip_val_op10).await, exp10_t1);
    assert_eq!(balance(&pool, wo.wip_val_op20).await, exp20_t1);
    let wip_var_t1 = balance(&pool, wo.variance_wip_reval).await;
    assert_eq!(wip_var_t0 - wip_var_t1, 25 * 13 + 15 * 13);

    // Roll #2: 80 → 100 (Δ=20). Prior = 80 (roll #1's target).
    let _ = call_roll(&pool, &wo.parent_id, 100, "2026-05-25", "2026-05-25", Some(80), true)
        .await
        .expect("roll #2");

    let exp10_t2 = exp10_t1 + 25 * 20;
    let exp20_t2 = exp20_t1 + 15 * 20;
    assert_eq!(balance(&pool, wo.wip_val_op10).await, exp10_t2);
    assert_eq!(balance(&pool, wo.wip_val_op20).await, exp20_t2);

    let wip_var_t2 = balance(&pool, wo.variance_wip_reval).await;
    assert_eq!(wip_var_t1 - wip_var_t2, 25 * 20 + 15 * 20);

    assert_invariants_hold(&pool, "sequence_of_rolls_with_wip_carryover").await;
}

/// Negative delta (cost roll DOWN). WIP pool drains by pool_qty ×
/// |Δstd| and the variance flows the OTHER direction
/// (variance_wip_revaluation gets the debit side).
#[tokio::test]
async fn single_op_wip_revaluation_negative_delta() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo_single_op(&pool, "NEG-A", 50).await;
    pre_load_raw(&pool, &wo, 200, 0, 2000, 0).await;
    call_wo_start(&pool, &wo.wo_id).await.expect("wo_start");

    let wip_var_before = balance(&pool, wo.variance_wip_reval).await;

    // Roll DOWN: 20 → 12 (Δ = -8). WIP value drops by 50 × 8 = 400.
    let _ = call_roll(&pool, &wo.parent_id, 12, "2026-04-25", "2026-04-25", Some(20), true)
        .await
        .expect("downward roll");

    assert_eq!(balance(&pool, wo.wip_val_op10).await, 600);
    let wip_var_after = balance(&pool, wo.variance_wip_reval).await;
    // variance debited 400 → balance increased by 400.
    assert_eq!(wip_var_after - wip_var_before, 400);

    assert_invariants_hold(&pool, "single_op_wip_revaluation_negative_delta").await;
}
