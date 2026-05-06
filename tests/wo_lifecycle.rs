//! `acct-wl1` / Slice B.2 — work-order lifecycle matrix.
//!
//! Covers `post_wo_start`, `post_op_move`, `post_wo_complete`,
//! `post_scrap` against the **BOM2 model** (bom_headers + bom_lines +
//! absorption_classes; acct-jg2). The pre-BOM2 `boms` /
//! `wo_routing_burdens` setup was retired in C3 (acct-jtt) — this file
//! now scaffolds via `create_bom_header` + `add_bom_item` +
//! `add_bom_service`.
//!
//! Review memo (acct-69p) drives the priority edge cases:
//!   - idempotency replay across all 4 functions (Item 9 / 0039 fix)
//!   - rework op_move (to_op < from_op) re-applies destination services
//!   - multi-batch op_move into the same op
//!   - empty per-op service lines
//!   - wo_close_v residual sign (unfavorable + favorable; pre-balance
//!     step from migration 0061 makes the favorable case reachable)
//!   - component without std cost → P0018
//!   - cross-currency component → P0010
//!   - applied_account_kind with no reason mapping → P0026
//!
//! Plus the validation errors: WO not found / not draft / not standard,
//! empty routing, missing BOM (P0033 in BOM2), from_op = to_op, op not
//! in routing, qty overflow, scrap qty > pool balance.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Per-test scaffold
// ============================================================

#[allow(dead_code)]
struct Wo {
    parent_id: String,
    raw_loc_id: String,
    fg_loc_id: String,
    comp_a_id: String,
    comp_b_id: String,
    wo_id: String,
    bom_id: i64,
    fg_qty_acct: i64,
    wip_qty_op10: i64,
    wip_qty_op20: i64,
    wip_val_op10: i64,
    wip_val_op20: i64,
    fg_val_acct: i64,
    scrap_qty_acct: i64,
    consumed_a: i64,
    consumed_b: i64,
    raw_qty_a: i64,
    raw_qty_b: i64,
    raw_val_a: i64,
    raw_val_b: i64,
    labor_applied: i64,
    oh_applied: i64,
    variance_wo_close: i64,
    variance_scrap: i64,
    creation_void: i64,
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
    currency: &str,
) -> String {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5, $6::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(parent_id)
    .bind(fg_loc_id)
    .bind(qty_target)
    .bind(currency)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_wo: {e}"))
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

/// Create a bom_header for the parent SKU. Returns the bom_id. The
/// header lands as primary, alternate=1, revision='A', status=active,
/// effective from -infinity. Tests that need alternates/revisions go
/// through the lower-level `create_bom_header_full` helper directly.
async fn create_bom_header_for_sku(pool: &PgPool, parent_id: &str) -> i64 {
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

/// Add an item line to a bom_header by component IDs (not codes — the
/// inline error tests create ad-hoc SKUs whose codes may collide across
/// tests). Uses fire_at='op_arrival', basis='per_unit', yield_pct=100.
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
    .unwrap_or_else(|e| panic!("add_bom_item_by_id bom={bom_id} line={line_no}: {e}"));
}

/// Add a per-unit service line referencing an absorption_class by code.
/// fire_at='op_arrival' (the only valid combo for per_unit per
/// bom_lines CHECKs).
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

/// Set up a 2-component, 2-op WO at qty_target with parent_std_cost
/// rolled up correctly. comp_a × 2 (std=10) + comp_b × 1 (std=20)
/// + op10 (labor 5, oh 3) + op20 (labor 7, oh 12) = 67 per unit.
/// parent_std_cost = 67 (so wo_complete drains exactly, residual = 0).
///
/// `with_services=true` adds the standard 4 service lines (labor+oh at
/// each op); tests that need the empty-service case pass `false` and
/// add their own selectively.
async fn scaffold_wo(pool: &PgPool, qty_target: i64, with_services: bool) -> Wo {
    let parent = fresh_sku(pool, "WO-P", "standard").await;
    let comp_a = fresh_sku(pool, "WO-CA", "standard").await;
    let comp_b = fresh_sku(pool, "WO-CB", "standard").await;
    let raw_loc = fresh_location(pool, "WO-RAW").await;
    let fg_loc = fresh_location(pool, "WO-FG").await;

    set_std_cost(pool, &parent, 67).await;
    set_std_cost(pool, &comp_a, 10).await;
    set_std_cost(pool, &comp_b, 20).await;

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
    let fg_qty_acct = open_account(
        pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit",
    )
    .await;
    let fg_val_acct = open_account(
        pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit",
    )
    .await;
    let scrap_qty_acct = open_account(
        pool, "stock_scrap", "qty", None, Some(&parent), None, None, "debit",
    )
    .await;

    let consumed_a = open_account(
        pool, "stock_consumed", "qty", None, Some(&comp_a), None, None, "debit",
    )
    .await;
    let consumed_b = open_account(
        pool, "stock_consumed", "qty", None, Some(&comp_b), None, None, "debit",
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

    let labor_applied = account_id_by_kind_currency(pool, "labor_applied", Some("USD")).await;
    let oh_applied = account_id_by_kind_currency(pool, "oh_applied", Some("USD")).await;
    let variance_wo_close =
        account_id_by_kind_currency(pool, "variance_wo_close", Some("USD")).await;
    let variance_scrap = account_id_by_kind_currency(pool, "variance_scrap", Some("USD")).await;
    let creation_void = account_id_by_kind_currency(pool, "creation_void", None).await;

    let wo_id = create_wo(pool, "WO-001", &parent, &fg_loc, qty_target, "USD").await;
    add_routing(pool, &wo_id, 10, "MILL").await;
    add_routing(pool, &wo_id, 20, "FINISH").await;

    let bom_id = create_bom_header_for_sku(pool, &parent).await;
    add_bom_item_by_id(pool, bom_id, 1, 10, &comp_a, &raw_loc, 2).await;
    add_bom_item_by_id(pool, bom_id, 2, 10, &comp_b, &raw_loc, 1).await;

    if with_services {
        add_bom_service_per_unit(pool, bom_id, 3, 10, "labor_std", 5).await;
        add_bom_service_per_unit(pool, bom_id, 4, 10, "oh_std", 3).await;
        add_bom_service_per_unit(pool, bom_id, 5, 20, "labor_std", 7).await;
        add_bom_service_per_unit(pool, bom_id, 6, 20, "oh_std", 12).await;
    }

    Wo {
        parent_id: parent,
        raw_loc_id: raw_loc,
        fg_loc_id: fg_loc,
        comp_a_id: comp_a,
        comp_b_id: comp_b,
        wo_id,
        bom_id,
        fg_qty_acct,
        wip_qty_op10,
        wip_qty_op20,
        wip_val_op10,
        wip_val_op20,
        fg_val_acct,
        scrap_qty_acct,
        consumed_a,
        consumed_b,
        raw_qty_a,
        raw_qty_b,
        raw_val_a,
        raw_val_b,
        labor_applied,
        oh_applied,
        variance_wo_close,
        variance_scrap,
        creation_void,
    }
}

async fn pre_load_raw(pool: &PgPool, wo: &Wo, qty_a: i64, qty_b: i64, value_a: i64, value_b: i64) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = wo.creation_void;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let doc_id = fresh_uuid(pool).await;
    let mint = json!([
        { "reason": "cycle_count_adj",
          "document_kind": "wo_test_seed", "document_id": doc_id,
          "debit_account_id": wo.raw_qty_a, "credit_account_id": void_qty,
          "amount": qty_a, "qty": qty_a,
          "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await,
          "posted_by": posted_by },
        { "reason": "cycle_count_adj",
          "document_kind": "wo_test_seed", "document_id": doc_id,
          "debit_account_id": wo.raw_val_a, "credit_account_id": void_val,
          "amount": value_a, "qty": qty_a,
          "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await,
          "posted_by": posted_by },
        { "reason": "cycle_count_adj",
          "document_kind": "wo_test_seed", "document_id": doc_id,
          "debit_account_id": wo.raw_qty_b, "credit_account_id": void_qty,
          "amount": qty_b, "qty": qty_b,
          "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await,
          "posted_by": posted_by },
        { "reason": "cycle_count_adj",
          "document_kind": "wo_test_seed", "document_id": doc_id,
          "debit_account_id": wo.raw_val_b, "credit_account_id": void_val,
          "amount": value_b, "qty": qty_b,
          "business_date": "2026-04-15",
          "idempotency_key": fresh_uuid(pool).await,
          "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(mint)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("pre_load_raw: {e}"));
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

async fn call_op_move(
    pool: &PgPool,
    wo_id: &str,
    from_op: i32,
    to_op: i32,
    qty: i64,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_op_move($1::UUID, $2, $3, $4, '2026-04-20'::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(from_op)
    .bind(to_op)
    .bind(qty)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn call_wo_complete(
    pool: &PgPool,
    wo_id: &str,
    qty: i64,
    key: &str,
) -> sqlx::Result<String> {
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

async fn call_scrap(
    pool: &PgPool,
    wo_id: &str,
    routing_op: i32,
    qty: i64,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_scrap($1::UUID, $2, $3, '2026-04-20'::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(wo_id)
    .bind(routing_op)
    .bind(qty)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

async fn wo_status(pool: &PgPool, wo_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM work_orders WHERE id = $1::UUID")
        .bind(wo_id)
        .fetch_one(pool)
        .await
        .expect("wo_status")
}

// ============================================================
// Happy path
// ============================================================

#[tokio::test]
async fn happy_path_start_then_op_move_then_complete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;

    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await)
        .await
        .expect("wo_start");
    assert_eq!(wo_status(&pool, &wo.wo_id).await, "released");

    // After WO_start at qty_target=100 (BOM2 path):
    //   WIP@op10 value = 100 × (40 RM + 5 labor + 3 oh) = 100 × 48 = 4800
    //   WIP@op10 qty   = 100
    //   raw_a value drained = 200 × 10 = 2000  (consumed)
    //   raw_b value drained = 100 × 20 = 2000
    //   labor_applied += 100 × 5 = 500  (credit-normal, balance -500)
    //   oh_applied    += 100 × 3 = 300  (credit-normal, balance -300)
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 4800);
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 100);
    assert_eq!(balance(&pool, wo.raw_val_a).await, 0);
    assert_eq!(balance(&pool, wo.raw_val_b).await, 0);
    assert_eq!(balance(&pool, wo.labor_applied).await, -500);
    assert_eq!(balance(&pool, wo.oh_applied).await, -300);

    // op_move 10→20 of all 100 units.
    //   WIP@op20 += 100 × 48 (per_unit_cum at applies_at_op<=10)
    //            + 100 × 7 labor + 100 × 12 oh   (op20 services, first arrival)
    //            = 4800 + 700 + 1200 = 6700 = 100 × parent_std (67).
    //   WIP@op10 drains.
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await)
        .await
        .expect("op_move");
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 0);
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 6700);
    assert_eq!(balance(&pool, wo.labor_applied).await, -500 - 700);
    assert_eq!(balance(&pool, wo.oh_applied).await, -300 - 1200);

    // wo_complete 100 from op20: drains WIP@op20 at qty × parent_std = 6700.
    //   FG inventory += 100 qty + 6700 value.
    //   WIP@op20 → 0 → no residual → no wo_close_v.
    call_wo_complete(&pool, &wo.wo_id, 100, &fresh_uuid(&pool).await)
        .await
        .expect("wo_complete");
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 0);
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 6700);
    assert_eq!(balance(&pool, wo.fg_qty_acct).await, 100);
    assert_eq!(balance(&pool, wo.variance_wo_close).await, 0);
    assert_eq!(wo_status(&pool, &wo.wo_id).await, "closed");
}

#[tokio::test]
async fn multi_batch_op_move_into_same_op() {
    // Item 11: two op_moves arriving at op20. In BOM2 the per_unit
    // service lines re-fire on every arrival; per_lot lines (none here)
    // would only fire on first arrival.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();

    call_op_move(&pool, &wo.wo_id, 10, 20, 60, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 40, &fresh_uuid(&pool).await).await.unwrap();

    // After both moves: WIP@op20 = (60 + 40) × 67 = 6700. WIP@op10 = 0.
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 6700);
    assert_eq!(balance(&pool, wo.wip_qty_op20).await, 100);
    assert_eq!(balance(&pool, wo.wip_val_op10).await, 0);
    assert_eq!(balance(&pool, wo.wip_qty_op10).await, 0);
}

#[tokio::test]
async fn rework_op_move_reapplies_destination_services() {
    // Item 2a: rework sends qty backwards (op20 → op10). In BOM2 the
    // subsequent-arrival emit at op10 re-fires per_unit *services* only
    // — the bom_lines item lines DO NOT re-fire (they fired once at
    // wo_start; re-issuing components on rework would double-consume
    // raw inventory). Subsequent-arrival service re-fire is the
    // labor/OH-per-pass model from the Slice B spec.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();

    let labor_before = balance(&pool, wo.labor_applied).await;
    let oh_before = balance(&pool, wo.oh_applied).await;
    let wip10_before = balance(&pool, wo.wip_val_op10).await;
    let wip20_before = balance(&pool, wo.wip_val_op20).await;
    let consumed_a_before = balance(&pool, wo.consumed_a).await;
    let consumed_b_before = balance(&pool, wo.consumed_b).await;

    // Rework: 10 units back to op10.
    call_op_move(&pool, &wo.wo_id, 20, 10, 10, &fresh_uuid(&pool).await).await.unwrap();

    // op10 services re-applied: 10 × (5 labor + 3 oh) = 50 + 30 = 80.
    assert_eq!(balance(&pool, wo.labor_applied).await, labor_before - 50);
    assert_eq!(balance(&pool, wo.oh_applied).await, oh_before - 30);
    // op_move_v value-leg at std_cum_at_op20 = 67: 10 × 67 = 670.
    // Plus op10 service re-fire: 80. WIP@op10 grew by 670 + 80 = 750.
    assert_eq!(balance(&pool, wo.wip_val_op10).await, wip10_before + 670 + 80);
    assert_eq!(balance(&pool, wo.wip_val_op20).await, wip20_before - 670);
    // BOM items did NOT re-issue: stock_consumed unchanged.
    assert_eq!(balance(&pool, wo.consumed_a).await, consumed_a_before);
    assert_eq!(balance(&pool, wo.consumed_b).await, consumed_b_before);
}

#[tokio::test]
async fn empty_services_op_succeeds() {
    // Item 12: an op with no per-unit service lines just moves value
    // forward (no apply events). Use scaffold(false) and add only op10
    // services. parent_std mismatch will pre-balance at wo_complete —
    // see `wo_complete_unfavorable_residual_routes_through_variance`.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, false).await;
    add_bom_service_per_unit(&pool, wo.bom_id, 3, 10, "labor_std", 5).await;
    add_bom_service_per_unit(&pool, wo.bom_id, 4, 10, "oh_std", 3).await;
    set_std_cost_override(&pool, &wo.parent_id, 48).await; // RM 40 + op10 8

    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();
    // WIP@op20 = 100 × 48 (per_unit_cum at op10, no op20 services).
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 4800);
}

async fn set_std_cost_override(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-02', $3::UUID, $4::UUID)",
    )
    .bind(sku_id)
    .bind(cost)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("set_std_cost_override: {e}"));
}

// ============================================================
// Variance + scrap
// ============================================================

#[tokio::test]
async fn wo_complete_unfavorable_residual_routes_through_variance() {
    // Item 5: WO_total > parent_std → residual > 0 → DR variance / CR WIP.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    // Parent std = 67 by scaffold; reset to 60 (7 less than per-unit accumulation).
    sqlx::query("DELETE FROM standard_costs WHERE sku_id = $1::UUID")
        .bind(&wo.parent_id)
        .execute(&pool)
        .await
        .unwrap();
    set_std_cost(&pool, &wo.parent_id, 60).await;

    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo.wo_id, 100, &fresh_uuid(&pool).await).await.unwrap();

    // WIP@op20 held 100 × 67 = 6700. wo_complete drains 100 × 60 = 6000.
    // BOM2 pre-balance step (mig 0061) emits an unfavorable wo_close_v
    // for the 700 residual (DR variance / CR pool) BEFORE the per-output
    // drain so the debit-normal CHECK on inv_value_wip stays satisfied.
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 0);
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 6000);
    assert_eq!(balance(&pool, wo.variance_wo_close).await, 700);
    assert_eq!(wo_status(&pool, &wo.wo_id).await, "closed");
}

#[tokio::test]
async fn wo_complete_favorable_residual_routes_through_variance() {
    // Item 5 reverse: parent_std > per-unit accumulation. The dispatcher
    // wants to drain qty × parent_std (7500) from a pool holding only
    // qty × per-unit accumulation (6700). Pre-Slice-B this would have
    // hit the debit-normal CHECK; under BOM2 (mig 0061) the
    // post_wo_complete pre-balance step emits a FAVORABLE wo_close_v
    // (DR pool / CR variance) BEFORE the per-output drain so the pool
    // is topped up to the drain target. Variance lands negative
    // (credit-side), reflecting the favorable position.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    sqlx::query("DELETE FROM standard_costs WHERE sku_id = $1::UUID")
        .bind(&wo.parent_id)
        .execute(&pool)
        .await
        .unwrap();
    set_std_cost(&pool, &wo.parent_id, 75).await;

    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo.wo_id, 100, &fresh_uuid(&pool).await).await.unwrap();

    // Pool@op20 was 6700 pre-complete. Drain target = 100 × 75 = 7500.
    // Pre-balance: DR pool / CR variance 800 → pool = 7500, variance = -800.
    // Per-output drain: 7500 → pool = 0, fg = 7500.
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 0);
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 7500);
    assert_eq!(balance(&pool, wo.variance_wo_close).await, -800);
    assert_eq!(wo_status(&pool, &wo.wo_id).await, "closed");
}

#[tokio::test]
async fn scrap_at_intermediate_op_routes_through_variance_scrap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    // Move all 100 to op20.
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();

    // Scrap 5 at op20. Pool unit cost = 6700/100 = 67. variance_scrap
    // gets 5 × 67 = 335. WIP@op20 drops by 335 value + 5 qty.
    call_scrap(&pool, &wo.wo_id, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 6700 - 335);
    assert_eq!(balance(&pool, wo.wip_qty_op20).await, 95);
    assert_eq!(balance(&pool, wo.scrap_qty_acct).await, 5);
    assert_eq!(balance(&pool, wo.variance_scrap).await, 335);
}

#[tokio::test]
async fn scrap_then_complete_remaining_balances_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();

    call_scrap(&pool, &wo.wo_id, 20, 5, &fresh_uuid(&pool).await).await.unwrap();
    call_wo_complete(&pool, &wo.wo_id, 95, &fresh_uuid(&pool).await).await.unwrap();
    assert_eq!(balance(&pool, wo.wip_val_op20).await, 0);
    assert_eq!(balance(&pool, wo.fg_qty_acct).await, 95);
    // FG value = 95 × parent_std (67) = 6365.
    assert_eq!(balance(&pool, wo.fg_val_acct).await, 6365);
    // Residual was 0 at close (we scrapped 5 × 67 + completed 95 × 67 = 6700 = WIP).
    assert_eq!(balance(&pool, wo.variance_wo_close).await, 0);
    assert_eq!(wo_status(&pool, &wo.wo_id).await, "closed");
}

#[tokio::test]
async fn scrap_qty_over_pool_balance_raises_p0026() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    // op20 has 0 qty; scrap should raise P0026.
    expect_sqlstate(
        "P0026",
        || async { call_scrap(&pool, &wo.wo_id, 20, 1, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

// ============================================================
// Idempotency replay (review item 9)
// ============================================================

#[tokio::test]
async fn wo_start_idempotency_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    let key = fresh_uuid(&pool).await;
    call_wo_start(&pool, &wo.wo_id, &key).await.unwrap();
    let val_before = balance(&pool, wo.wip_val_op10).await;
    // Re-fire with same key: must no-op (no extra events).
    call_wo_start(&pool, &wo.wo_id, &key).await.unwrap();
    assert_eq!(balance(&pool, wo.wip_val_op10).await, val_before);
}

#[tokio::test]
async fn op_move_idempotency_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    let key = fresh_uuid(&pool).await;
    call_op_move(&pool, &wo.wo_id, 10, 20, 50, &key).await.unwrap();
    let v10 = balance(&pool, wo.wip_val_op10).await;
    let v20 = balance(&pool, wo.wip_val_op20).await;
    call_op_move(&pool, &wo.wo_id, 10, 20, 50, &key).await.unwrap();
    assert_eq!(balance(&pool, wo.wip_val_op10).await, v10);
    assert_eq!(balance(&pool, wo.wip_val_op20).await, v20);
}

#[tokio::test]
async fn wo_complete_idempotency_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();
    let key = fresh_uuid(&pool).await;
    call_wo_complete(&pool, &wo.wo_id, 100, &key).await.unwrap();
    let fg = balance(&pool, wo.fg_val_acct).await;
    // Status is now 'closed' — second call with same key must replay
    // idempotently (the 0039 fix), not raise P0026.
    call_wo_complete(&pool, &wo.wo_id, 100, &key).await.unwrap();
    assert_eq!(balance(&pool, wo.fg_val_acct).await, fg);
}

#[tokio::test]
async fn scrap_idempotency_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();
    let key = fresh_uuid(&pool).await;
    call_scrap(&pool, &wo.wo_id, 20, 5, &key).await.unwrap();
    let var = balance(&pool, wo.variance_scrap).await;
    call_scrap(&pool, &wo.wo_id, 20, 5, &key).await.unwrap();
    assert_eq!(balance(&pool, wo.variance_scrap).await, var);
}

// ============================================================
// Validation errors
// ============================================================

#[tokio::test]
async fn wo_not_found_raises_p0026() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let bogus = fresh_uuid(&pool).await;
    expect_sqlstate(
        "P0026",
        || async { call_wo_start(&pool, &bogus, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn wo_already_started_raises_p0026() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    expect_sqlstate(
        "P0026",
        || async {
            call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn missing_routing_raises_p0026() {
    // routing-empty check fires before BOM resolution; no bom_header
    // needed.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = fresh_sku(&pool, "WO-NO-ROUTE", "standard").await;
    set_std_cost(&pool, &parent, 50).await;
    let fg_loc = fresh_location(&pool, "WO-FG").await;
    let wo_id = create_wo(&pool, "WO-X", &parent, &fg_loc, 10, "USD").await;
    expect_sqlstate(
        "P0026",
        || async { call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn missing_bom_raises_p0033() {
    // BOM2: no bom_header for parent → _wo_resolve_bom_for falls
    // through bom_header_at, which raises P0033
    // (bom_header_resolution_invalid). The pre-BOM2 P0029 path is
    // gone; per-spec migration has no boms / wo_routing_burdens.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = fresh_sku(&pool, "WO-NO-BOM", "standard").await;
    set_std_cost(&pool, &parent, 50).await;
    let fg_loc = fresh_location(&pool, "WO-FG").await;
    open_account(
        &pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit",
    )
    .await;
    open_account(
        &pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit",
    )
    .await;
    let wo_id = create_wo(&pool, "WO-Y", &parent, &fg_loc, 10, "USD").await;
    add_routing(&pool, &wo_id, 10, "OP1").await;
    expect_sqlstate(
        "P0033",
        || async { call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn op_move_from_eq_to_raises_p0028() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    expect_sqlstate(
        "P0028",
        || async {
            call_op_move(&pool, &wo.wo_id, 10, 10, 50, &fresh_uuid(&pool).await).await.map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn op_move_unknown_op_raises_p0028() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    expect_sqlstate(
        "P0028",
        || async {
            call_op_move(&pool, &wo.wo_id, 10, 30, 50, &fresh_uuid(&pool).await).await.map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn wo_complete_qty_overflow_raises_p0027() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 100, true).await;
    pre_load_raw(&pool, &wo, 200, 100, 2000, 2000).await;
    call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.unwrap();
    call_op_move(&pool, &wo.wo_id, 10, 20, 100, &fresh_uuid(&pool).await).await.unwrap();
    expect_sqlstate(
        "P0027",
        || async {
            call_wo_complete(&pool, &wo.wo_id, 101, &fresh_uuid(&pool).await).await.map(|_| ())
        },
    )
    .await;
}

#[tokio::test]
async fn applied_account_kind_no_mapping_raises_p0026() {
    // BOM2: register an absorption_class whose applied_account_kind is
    // not in the {labor_applied, oh_applied, absorption_pool} mapping
    // set. The new _wo_apply_reason_for(class_id, basis) overload's
    // ELSE branch raises P0026 when the bom_line fires.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = scaffold_wo(&pool, 10, false).await;
    pre_load_raw(&pool, &wo, 20, 10, 200, 200).await;
    register_absorption_class(&pool, "cogs_test", "cogs", None).await;
    add_bom_service_per_unit(&pool, wo.bom_id, 3, 10, "cogs_test", 5).await;
    expect_sqlstate(
        "P0026",
        || async { call_wo_start(&pool, &wo.wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn standard_component_without_std_cost_raises_p0018() {
    // Standard-cost component without a standard_costs row → P0018 from
    // resolve_standard_cost_at. wac_perpetual components no longer go
    // through resolve_standard_cost_at (acct-24b, mig 0066) — they hit
    // P0010 'empty pool' instead, covered by a separate test below.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = fresh_sku(&pool, "WO-P-NSC", "standard").await;
    set_std_cost(&pool, &parent, 50).await;
    let comp = fresh_sku(&pool, "WO-C-NSC", "standard").await;
    // No standard_costs row for comp — that's the P0018 trigger.
    let raw_loc = fresh_location(&pool, "WO-RAW-NSC").await;
    let fg_loc = fresh_location(&pool, "WO-FG-NSC").await;
    open_account(
        &pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit",
    )
    .await;
    open_account(
        &pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit",
    )
    .await;
    open_account(
        &pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit",
    )
    .await;
    open_account(
        &pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, "debit",
    )
    .await;
    open_account(
        &pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, "debit",
    )
    .await;
    let wo_id = create_wo(&pool, "WO-NSC", &parent, &fg_loc, 5, "USD").await;
    add_routing(&pool, &wo_id, 10, "OP1").await;
    let bom_id = create_bom_header_for_sku(&pool, &parent).await;
    add_bom_item_by_id(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;
    expect_sqlstate(
        "P0018",
        || async { call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test]
async fn cross_currency_component_raises_p0010() {
    // Item 4: WO is USD, component lives in EUR → no inv_value_raw match.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = fresh_sku(&pool, "WO-P-CC", "standard").await;
    set_std_cost(&pool, &parent, 50).await;
    let comp = fresh_sku(&pool, "WO-C-CC", "standard").await;
    set_std_cost(&pool, &comp, 10).await;
    let raw_loc = fresh_location(&pool, "WO-RAW-CC").await;
    let fg_loc = fresh_location(&pool, "WO-FG-CC").await;
    open_account(
        &pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit",
    )
    .await;
    open_account(
        &pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit",
    )
    .await;
    open_account(
        &pool, "stock_consumed", "qty", None, Some(&comp), None, None, "debit",
    )
    .await;
    open_account(
        &pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, "debit",
    )
    .await;
    // EUR raw value for comp; WO is USD so the lookup fails.
    open_account(
        &pool, "inv_value_raw", "value", Some("EUR"), Some(&comp), Some(&raw_loc), None, "debit",
    )
    .await;
    let wo_id = create_wo(&pool, "WO-CC", &parent, &fg_loc, 5, "USD").await;
    add_routing(&pool, &wo_id, 10, "OP1").await;
    let bom_id = create_bom_header_for_sku(&pool, &parent).await;
    add_bom_item_by_id(&pool, bom_id, 1, 10, &comp, &raw_loc, 1).await;
    expect_sqlstate(
        "P0010",
        || async { call_wo_start(&pool, &wo_id, &fresh_uuid(&pool).await).await.map(|_| ()) },
    )
    .await;
}
