//! BOM2 advanced tests (acct-jg2). Exercises the new BOM model:
//! header/lines split, alternates and revisions, fire_at scrap-aware
//! timing, phantom expansion, co-products, OSP custody (deferred),
//! ECO workflow.
//!
//! Each test file section is one C5* sub-issue. Tests use the BOM2
//! helpers from `common::*` (register_absorption_class,
//! create_bom_header, add_bom_item / add_bom_service / add_bom_charge,
//! add_wo_output, set_wo_bom_id) plus local SKU / location scaffolding.

mod common;

use common::*;
use sqlx::PgPool;

// ============================================================
// Local scaffolding helpers (mirror wo_lifecycle.rs patterns —
// duplicated rather than shared so the two test binaries stay
// independent).
// ============================================================

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

async fn fresh_phantom_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, is_phantom)
         VALUES ($1, 'EA', 'standard', TRUE) RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert phantom sku {code}: {e}"))
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

/// One row in the _wo_explode_bom output. Mirror of the function's
/// RETURNS TABLE columns we care about for assertions.
#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
struct ExplodedLine {
    kind: String,
    basis: String,
    applies_at_op: i32,
    fire_at: String,
    qty_per_parent: Option<i64>,
    std_amount: Option<i64>,
    depth: i32,
}

async fn explode(pool: &PgPool, bom_id: i64) -> Vec<ExplodedLine> {
    sqlx::query_as::<_, ExplodedLine>(
        "SELECT kind, basis, applies_at_op, fire_at,
                qty_per_parent, std_amount, depth
           FROM _wo_explode_bom($1, '2026-04-15'::DATE)
          ORDER BY source_bom_id, source_line_no, depth",
    )
    .bind(bom_id)
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| panic!("_wo_explode_bom({bom_id}): {e}"))
}

/// Open an account row directly. Mirrors wo_lifecycle.rs `open_account`
/// but exposed here so each test in this file can scaffold what it needs
/// without depending on the other binary's internals.
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
    .unwrap_or_else(|e| panic!("open {kind}/{ledger_kind} routing_op={routing_op:?}: {e}"))
}

async fn set_std_cost_for(pool: &PgPool, sku_id: &str, cost: i64) {
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

/// Lookup sku id by code.
async fn sku_id(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("sku_id {code}: {e}"))
}

/// Lookup location id by code.
async fn loc_id(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("loc_id {code}: {e}"))
}

/// Create a WO row. Returns the wo_id text. SKU-A scaffold from the
/// fixture has all required parent accounts (stock_wip op10/op20,
/// inv_value_wip op10/op20, stock_available MAIN, inv_value_fg MAIN,
/// stock_consumed, stock_scrap).
async fn create_test_wo(
    pool: &PgPool,
    wo_no: &str,
    parent_code: &str,
    fg_loc_code: &str,
    qty_target: i64,
) -> String {
    let parent_id = sku_id(pool, parent_code).await;
    let fg = loc_id(pool, fg_loc_code).await;
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no)
    .bind(&parent_id)
    .bind(&fg)
    .bind(qty_target)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("create_test_wo: {e}"))
}

async fn add_test_routing(pool: &PgPool, wo_id: &str, routing_op: i32, op_name: &str) {
    sqlx::query(
        "INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, $2, $3)",
    )
    .bind(wo_id)
    .bind(routing_op)
    .bind(op_name)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("add_test_routing {routing_op}: {e}"));
}

/// Scaffold a fresh component SKU + raw location + std_cost + the 3
/// component-side accounts needed by post_wo_start: stock_consumed,
/// stock_available(comp, raw_loc), inv_value_raw(comp, raw_loc, USD).
/// Also pre-stocks `seed_qty` units at standard cost so rm_issue has
/// something to drain.
async fn scaffold_component(
    pool: &PgPool,
    code: &str,
    raw_loc_code: &str,
    std_cost: i64,
    seed_qty: i64,
) {
    let comp_id = fresh_sku(pool, code).await;
    set_std_cost_for(pool, &comp_id, std_cost).await;
    let raw = loc_id(pool, raw_loc_code).await;
    let _consumed = open_account(
        pool, "stock_consumed", "qty", None, Some(&comp_id), None, None, "debit",
    )
    .await;
    let raw_qty = open_account(
        pool, "stock_available", "qty", None, Some(&comp_id), Some(&raw), None, "debit",
    )
    .await;
    let raw_val = open_account(
        pool, "inv_value_raw", "value", Some("USD"), Some(&comp_id), Some(&raw), None, "debit",
    )
    .await;
    // Pre-stock: post a cycle_count_adj on the qty side and a value-side
    // creation_void event to mint the standard-cost value.
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;
    let posted_by = fresh_uuid(pool).await;
    let dk1 = fresh_uuid(pool).await;
    let dk2 = fresh_uuid(pool).await;
    let events = serde_json::json!([
        {
            "reason": "cycle_count_adj",
            "document_kind": "test_doc",
            "document_id": "00000000-0000-0000-0000-0000000000aa",
            "debit_account_id": raw_qty,
            "credit_account_id": void_qty,
            "amount": seed_qty,
            "qty": seed_qty,
            "business_date": "2026-04-15",
            "idempotency_key": dk1,
            "posted_by": posted_by,
        },
        {
            "reason": "cycle_count_adj",
            "document_kind": "test_doc",
            "document_id": "00000000-0000-0000-0000-0000000000aa",
            "debit_account_id": raw_val,
            "credit_account_id": void_val,
            "amount": seed_qty * std_cost,
            "qty": seed_qty,
            "business_date": "2026-04-15",
            "idempotency_key": dk2,
            "posted_by": posted_by,
        }
    ]);
    call_post_transfers(pool, events, false)
        .await
        .expect("scaffold_component pre-stock");
}

async fn balance(pool: &PgPool, account_id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Sum the qty across all transfers for a given (debit_account_id) -
/// useful for "how many units were rm_issued for component X" assertions.
async fn rm_issued_qty(pool: &PgPool, consumed_acct_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty), 0)::BIGINT FROM transfers
          WHERE debit_account_id = $1 AND qty IS NOT NULL",
    )
    .bind(consumed_acct_id)
    .fetch_one(pool)
    .await
    .expect("rm_issued_qty")
}

// ============================================================
// C5a.1 — flat BOM (no phantoms): explode returns the lines unchanged
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn explode_flat_bom_returns_lines_unchanged() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku(&pool, "PHX-FLAT-P").await;
    let comp_a = fresh_sku(&pool, "PHX-FLAT-CA").await;
    let raw_loc = fresh_location(&pool, "PHX-RAW").await;

    let bom_id = create_bom_header(&pool, "PHX-FLAT-P").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-FLAT-CA", "PHX-RAW", 2, 0.0).await;
    add_bom_service(&pool, bom_id, 2, 10, "labor_std", 5, "per_unit", "op_arrival").await;
    add_bom_charge(&pool, bom_id, 3, 10, "oh_std", 100, "wo_start").await;

    let _ = (parent, comp_a, raw_loc); // referenced only via codes above

    let rows = explode(&pool, bom_id).await;
    assert_eq!(rows.len(), 3, "flat BOM: 3 lines in, 3 lines out");

    // Every row depth=1 (no phantom recursion), applies_at_op=10.
    for r in &rows {
        assert_eq!(r.depth, 1, "row {r:?} should be depth=1");
        assert_eq!(r.applies_at_op, 10, "row {r:?} should be applies_at_op=10");
    }

    // Item line came through with qty_per_parent=2.
    let item = rows.iter().find(|r| r.kind == "item").expect("item line");
    assert_eq!(item.basis, "per_unit");
    assert_eq!(item.qty_per_parent, Some(2));
    assert_eq!(item.fire_at, "op_arrival");

    // Service per_unit at std_amount=5.
    let svc = rows.iter().find(|r| r.kind == "service").expect("service line");
    assert_eq!(svc.basis, "per_unit");
    assert_eq!(svc.std_amount, Some(5));

    // Per-lot charge fire_at=wo_start at std_amount=100.
    let chg = rows.iter().find(|r| r.kind == "charge").expect("charge line");
    assert_eq!(chg.basis, "per_lot");
    assert_eq!(chg.fire_at, "wo_start");
    assert_eq!(chg.std_amount, Some(100));
}

// ============================================================
// C5a.2 — 1-level phantom: phantom child flattens at parent's op
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn explode_one_level_phantom_flattens_into_parent() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let _parent = fresh_sku(&pool, "PHX-1P-P").await;
    let _phantom = fresh_phantom_sku(&pool, "PHX-1P-PH").await;
    let _bolt = fresh_sku(&pool, "PHX-1P-BOLT").await;
    let _steel = fresh_sku(&pool, "PHX-1P-STEEL").await;
    let _raw_loc = fresh_location(&pool, "PHX-1P-RAW").await;

    // Parent BOM points at phantom (qty=3) at op20 plus a regular item.
    let parent_bom = create_bom_header(&pool, "PHX-1P-P").await;
    add_bom_item(&pool, parent_bom, 1, 20, "PHX-1P-PH", "PHX-1P-RAW", 3, 0.0).await;
    add_bom_item(&pool, parent_bom, 2, 10, "PHX-1P-STEEL", "PHX-1P-RAW", 1, 0.0).await;

    // Phantom's own primary BOM: 4 bolts per phantom + per_unit labor std=5.
    let phantom_bom = create_bom_header(&pool, "PHX-1P-PH").await;
    add_bom_item(&pool, phantom_bom, 1, 30, "PHX-1P-BOLT", "PHX-1P-RAW", 4, 0.0).await;
    add_bom_service(&pool, phantom_bom, 2, 30, "labor_std", 5, "per_unit", "op_arrival").await;

    let rows = explode(&pool, parent_bom).await;

    // Output: 1 STEEL item (depth=1) + 1 BOLT item (depth=2, scaled
    // qty=4×3=12) + 1 labor_std service (depth=2, scaled std=5×3=15).
    // The phantom-pointing item line itself is filtered out.
    assert_eq!(rows.len(), 3, "phantom flattens into parent: {rows:?}");

    let steel = rows
        .iter()
        .find(|r| r.kind == "item" && r.depth == 1)
        .expect("STEEL item at depth=1");
    assert_eq!(steel.applies_at_op, 10);
    assert_eq!(steel.qty_per_parent, Some(1));

    let bolt = rows
        .iter()
        .find(|r| r.kind == "item" && r.depth == 2)
        .expect("BOLT item at depth=2");
    // Phantom contents inherit parent's applies_at_op (op20), NOT the
    // phantom-line's own (op30 in phantom_bom).
    assert_eq!(bolt.applies_at_op, 20, "phantom contents inherit parent's applies_at_op");
    assert_eq!(bolt.qty_per_parent, Some(12), "qty_per_parent multiplied: 4 × 3 = 12");

    let svc = rows
        .iter()
        .find(|r| r.kind == "service")
        .expect("labor service at depth=2");
    assert_eq!(svc.depth, 2);
    assert_eq!(svc.applies_at_op, 20);
    assert_eq!(
        svc.std_amount,
        Some(15),
        "per_unit std_amount scaled by parent.qty_per_parent: 5 × 3"
    );
}

// ============================================================
// C5b — yield: scrap_pct is planning metadata, not build cost
// ============================================================
//
// bom_lines.scrap_pct records *planned* material loss for MRP/scheduling
// (e.g. "this component routinely loses 5% during assembly"). Build cost
// in WIP comes from ACTUAL usage — emission and per-op flow are LITERAL
// (qty_per × p_qty × comp_std). If the floor consumes more than the
// BOM's literal qty, the excess is a separate post_transfers call
// (an inventory adjustment or follow-up rm_issue_to_wo).
//
// skus.yield_mode picks how the parent's standard cost rollup treats
// scrap_pct (acct-6jq):
//   plan_only  (default): rollup = literal sum. variance_wo_close shows
//                          the full scrap-driven gap as unfavorable.
//   absorbed:             rollup factors in scrap (caller-set today;
//                          future tooling reads yield_mode and inflates).
//                          variance_wo_close captures the actual-vs-
//                          planned gap (favorable if no actual scrap).
//
// In BOTH modes pool flow is literal; only the parent_std × qty drain
// at wo_complete differs because the caller-set parent_std embodies
// the rollup decision.

#[tokio::test(flavor = "multi_thread")]
async fn yield_plan_only_emits_literal_qty_no_yield_variance() {
    // Mode B (plan_only, default): parent_std = literal qty_per × comp_std.
    // scrap_pct present but ignored at emission → drain matches pool;
    // variance_wo_close = 0 from yield (zero actual scrap).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-YPO-RAW").await;
    scaffold_component(&pool, "PHX-YPO-C", "PHX-YPO-RAW", 10, 1000).await;

    // BOM with scrap_pct=5% — recorded as planning hint.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-YPO-C", "PHX-YPO-RAW", 2, 5.0).await;

    // SKU-A's fixture-seeded std_cost=100. Parent_std for plan_only mode
    // would be `2 × 10 = 20` (literal) — fixture's 100 doesn't match,
    // so override for this test.
    let posted_by = fresh_uuid(&pool).await;
    let std_key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         SELECT id, 20, '2026-04-01'::DATE, $1::UUID, $2::UUID FROM skus WHERE code='SKU-A'",
    )
    .bind(&posted_by)
    .bind(&std_key)
    .execute(&pool)
    .await
    .expect("override SKU-A std cost to 20 (plan_only literal)");

    let wo_id = create_test_wo(&pool, "PHX-YPO-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    wo_start(&pool, &wo_id).await;

    // Emission is LITERAL: qty_per × p_qty = 2 × 100 = 200 (NOT 211).
    let consumed = stock_consumed_for(&pool, "PHX-YPO-C").await;
    assert_eq!(
        rm_issued_qty(&pool, consumed).await,
        200,
        "scrap_pct=5 does NOT gross up emission: literal 2×100=200"
    );

    // Pool@op10 = 200 × 10 = 2000.
    let wip_op10 = account_id_for_selector(
        &pool, "inv_value_wip", Some("SKU-A"), None, Some("USD"), Some(10),
    )
    .await;
    assert_eq!(balance(&pool, wip_op10).await, 2000);

    // Single-op WO: complete at qty=100, parent_std=20, drain = 2000.
    // Pool drains cleanly to 0; no variance from yield.
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, 100, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("post_wo_complete");

    let var_close = account_id_by_kind_currency(&pool, "variance_wo_close", Some("USD")).await;
    assert_eq!(
        balance(&pool, var_close).await,
        0,
        "plan_only with literal parent_std: no yield variance"
    );
    assert_eq!(balance(&pool, wip_op10).await, 0, "WIP fully drained");
}

#[tokio::test(flavor = "multi_thread")]
async fn yield_absorbed_via_scrap_aware_parent_std_surfaces_favorable_variance() {
    // Mode A (absorbed): caller marks parent yield_mode='absorbed' and
    // pre-sets parent_std to the rolled-up scrap-aware value. Pool flow
    // is still LITERAL (acct-6jq). At wo_complete, the larger drain
    // (parent_std × qty) over-drains the actually-literal pool, and the
    // residual sweep captures the gap as a favorable variance — meaning
    // we ran the WO without consuming the planned scrap allowance.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    sqlx::query("UPDATE skus SET yield_mode='absorbed' WHERE code='SKU-A'")
        .execute(&pool)
        .await
        .expect("set absorbed mode");

    fresh_location(&pool, "PHX-YAB-RAW").await;
    scaffold_component(&pool, "PHX-YAB-C", "PHX-YAB-RAW", 10, 1000).await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-YAB-C", "PHX-YAB-RAW", 2, 5.0).await;

    // Scrap-aware rollup: 2 / (1 - 0.05) × 10 = 21.05 → caller stores 21.
    let posted_by = fresh_uuid(&pool).await;
    let std_key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         SELECT id, 21, '2026-04-01'::DATE, $1::UUID, $2::UUID FROM skus WHERE code='SKU-A'",
    )
    .bind(&posted_by)
    .bind(&std_key)
    .execute(&pool)
    .await
    .expect("override SKU-A std cost to 21 (absorbed scrap-aware)");

    let wo_id = create_test_wo(&pool, "PHX-YAB-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    wo_start(&pool, &wo_id).await;

    // Emission LITERAL (same as plan_only): 200 components, pool=2000.
    let consumed = stock_consumed_for(&pool, "PHX-YAB-C").await;
    assert_eq!(rm_issued_qty(&pool, consumed).await, 200,
        "absorbed mode also emits literal at WO_start");

    let wip_op10 = account_id_for_selector(
        &pool, "inv_value_wip", Some("SKU-A"), None, Some("USD"), Some(10),
    )
    .await;
    assert_eq!(balance(&pool, wip_op10).await, 2000);

    // Complete: parent_std=21 × qty=100 = 2100 drain. Pool has 2000.
    // Walk-all sweep absorbs the −100 residual into variance_wo_close.
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, 100, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("post_wo_complete");

    // variance_wo_close: residual = 2000 (pool) − 2100 (drain) = −100.
    // The residual sweep credits variance_wo_close 100 (favorable),
    // surfacing as balance = −100 on the debit-normal account.
    let var_close = account_id_by_kind_currency(&pool, "variance_wo_close", Some("USD")).await;
    assert_eq!(
        balance(&pool, var_close).await,
        -100,
        "absorbed mode: 5% scrap allowance unspent → favorable variance −100"
    );
    assert_eq!(balance(&pool, wip_op10).await, 0, "WIP fully resolved");
}

// ============================================================
// C5c — fire_at semantics
// ============================================================
//
// fire_at controls *when* a bom_line fires:
//   wo_start    — fires at post_wo_start regardless of applies_at_op
//   op_arrival  — fires when qty first arrives at applies_at_op
// per_unit lines must be op_arrival (CHECK constraint).
// per_lot lines can be either; the wo_start variant pays setup-style
// charges upfront (e.g. production_setup), the op_arrival variant
// gates the charge on whether the op is actually reached
// (scrap-aware safety — see C5d).

/// Helper: post a single-batch op_move from from_op to to_op.
async fn op_move(pool: &PgPool, wo_id: &str, from_op: i32, to_op: i32, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_op_move($1::UUID, $2, $3, $4, '2026-04-15'::DATE, $5::UUID, $6::UUID, NULL)",
    )
    .bind(wo_id)
    .bind(from_op)
    .bind(to_op)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_op_move {from_op}->{to_op} qty={qty}: {e}"));
}

async fn wo_start(pool: &PgPool, wo_id: &str) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(wo_id)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_wo_start: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn fire_at_wo_start_per_lot_fires_immediately() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // SKU-A parent. Charge applies_at_op=20 but fire_at='wo_start'
    // (e.g. production setup that's paid upfront regardless of op).
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_charge(&pool, bom_id, 1, 20, "oh_std", 150, "wo_start").await;

    let wo_id = create_test_wo(&pool, "PHX-FA1-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    // Before WO_start: oh_applied has zero balance.
    let oh = account_id_by_kind_currency(&pool, "oh_applied", Some("USD")).await;
    assert_eq!(balance(&pool, oh).await, 0, "oh_applied starts at 0");

    wo_start(&pool, &wo_id).await;

    // After WO_start: the per_lot charge already fired even though
    // applies_at_op=20 hasn't been reached yet. Balance is -150
    // (credit-side increment).
    let bal = balance(&pool, oh).await;
    assert_eq!(bal, -150, "fire_at=wo_start charge fires upfront: oh_applied bal={bal}");
}

#[tokio::test(flavor = "multi_thread")]
async fn fire_at_op_arrival_per_lot_fires_only_when_op_reached() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    // Same charge but fire_at='op_arrival'.
    add_bom_charge(&pool, bom_id, 1, 20, "oh_std", 150, "op_arrival").await;

    let wo_id = create_test_wo(&pool, "PHX-FA2-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    let oh = account_id_by_kind_currency(&pool, "oh_applied", Some("USD")).await;

    wo_start(&pool, &wo_id).await;
    assert_eq!(
        balance(&pool, oh).await,
        0,
        "fire_at=op_arrival applies_at_op=20: did NOT fire at WO_start"
    );

    op_move(&pool, &wo_id, 10, 20, 100).await;
    assert_eq!(
        balance(&pool, oh).await,
        -150,
        "fire_at=op_arrival fires on first arrival at op=20"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fire_at_op_arrival_per_lot_fires_only_on_first_arrival() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    // Per-lot charge at op20.
    add_bom_charge(&pool, bom_id, 1, 20, "oh_std", 200, "op_arrival").await;

    let wo_id = create_test_wo(&pool, "PHX-FA3-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    let oh = account_id_by_kind_currency(&pool, "oh_applied", Some("USD")).await;

    wo_start(&pool, &wo_id).await;
    assert_eq!(balance(&pool, oh).await, 0);

    // First batch arrives at op20 → per_lot charge fires.
    op_move(&pool, &wo_id, 10, 20, 60).await;
    assert_eq!(
        balance(&pool, oh).await,
        -200,
        "first arrival at op20 fires the per_lot charge"
    );

    // Second batch arrives at op20 → per_lot must NOT re-fire.
    op_move(&pool, &wo_id, 10, 20, 40).await;
    assert_eq!(
        balance(&pool, oh).await,
        -200,
        "subsequent arrival at op20 does NOT re-fire per_lot (still -200, not -400)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fire_at_per_unit_service_scales_per_arrival() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    // Per-unit labor at op20: std_amount=5 per unit → fires every arrival
    // proportional to incoming qty.
    add_bom_service(&pool, bom_id, 1, 20, "labor_std", 5, "per_unit", "op_arrival").await;
    // Per-lot charge at op20: fires once.
    add_bom_charge(&pool, bom_id, 2, 20, "oh_std", 100, "op_arrival").await;

    let wo_id = create_test_wo(&pool, "PHX-FA4-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    let labor = account_id_by_kind_currency(&pool, "labor_applied", Some("USD")).await;
    let oh = account_id_by_kind_currency(&pool, "oh_applied", Some("USD")).await;

    wo_start(&pool, &wo_id).await;

    // First batch: 60 units arrive. Per_unit labor fires for 60 × 5 = 300.
    // Per-lot charge fires once at 100.
    op_move(&pool, &wo_id, 10, 20, 60).await;
    assert_eq!(balance(&pool, labor).await, -300, "per_unit fires 60 × 5 = 300");
    assert_eq!(balance(&pool, oh).await, -100, "per_lot fires once at 100");

    // Second batch: 40 units arrive. Per_unit fires for 40 × 5 = 200 more
    // (cumulative -500). Per-lot does NOT re-fire.
    op_move(&pool, &wo_id, 10, 20, 40).await;
    assert_eq!(
        balance(&pool, labor).await,
        -500,
        "per_unit cumulative: 300 + 200 = 500"
    );
    assert_eq!(
        balance(&pool, oh).await,
        -100,
        "per_lot stays at -100 (no re-fire)"
    );
}

// ============================================================
// C5d — scrap-aware safety + partial-scrap with per-lot charges
// ============================================================
//
// Plan §2 worked example. Three sub-cases:
//   (a) Per-lot charge already fired before partial scrap → charge
//       stays in WIP pool; scrapped portion absorbs its share at the
//       accumulated unit cost; no refund or pro-ration.
//   (b) Per-lot charge with fire_at='op_arrival' for an op the WO
//       never reaches → charge NEVER fires. Real-world correct: don't
//       pay for OSP freight if you never shipped to OSP.
//   (c) WO fully scrapped → post_wo_close_unproduced closes the WO.

async fn scrap(pool: &PgPool, wo_id: &str, routing_op: i32, qty: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "SELECT post_scrap($1::UUID, $2, $3, '2026-04-15'::DATE, $4::UUID, $5::UUID, NULL)",
    )
    .bind(wo_id)
    .bind(routing_op)
    .bind(qty)
    .bind(&posted_by)
    .bind(&key)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("post_scrap op={routing_op} qty={qty}: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn scrap_aware_safety_unfired_op_arrival_charge_never_incurred() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // BOM with charge at op20 fire_at='op_arrival'.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_charge(&pool, bom_id, 1, 20, "oh_std", 200, "op_arrival").await;

    let wo_id = create_test_wo(&pool, "PHX-SD1-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    let oh = account_id_by_kind_currency(&pool, "oh_applied", Some("USD")).await;

    wo_start(&pool, &wo_id).await;
    assert_eq!(balance(&pool, oh).await, 0, "charge has not fired (op20 not reached)");

    // Scrap all 100 units at op10 — never reach op20.
    scrap(&pool, &wo_id, 10, 100).await;

    // The op20 op_arrival charge NEVER fires: scrap-aware safety.
    assert_eq!(
        balance(&pool, oh).await,
        0,
        "charge with fire_at=op_arrival at op20 never fires when WO scrapped at op10"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scrap_aware_safety_wo_start_charge_is_sunk_even_on_full_scrap() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Charge fires at WO_start.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_charge(&pool, bom_id, 1, 10, "oh_std", 150, "wo_start").await;

    let wo_id = create_test_wo(&pool, "PHX-SD2-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    let oh = account_id_by_kind_currency(&pool, "oh_applied", Some("USD")).await;

    wo_start(&pool, &wo_id).await;
    assert_eq!(balance(&pool, oh).await, -150, "wo_start charge fires upfront");

    // Scrap everything at op10. The wo_start charge is in the WIP pool;
    // it gets absorbed by variance_scrap (since the entire pool drains).
    scrap(&pool, &wo_id, 10, 100).await;

    // oh_applied still holds the -150: that fee is incurred. The
    // scrap_v leg moved its share into variance_scrap on the value side
    // but did NOT reverse the absorption.
    assert_eq!(
        balance(&pool, oh).await,
        -150,
        "wo_start charge is sunk regardless of scrap"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_scrap_with_fired_per_lot_charge_absorbs_proportional_share() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // BOM has a per_lot charge that fires at op_arrival of op20.
    // Plus a per_unit service so the pool isn't dominated by the lot
    // charge alone — makes the absorbed share assertion meaningful.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_service(&pool, bom_id, 1, 20, "labor_std", 5, "per_unit", "op_arrival").await;
    add_bom_charge(&pool, bom_id, 2, 20, "oh_std", 200, "op_arrival").await;

    let wo_id = create_test_wo(&pool, "PHX-SD3-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    wo_start(&pool, &wo_id).await;
    op_move(&pool, &wo_id, 10, 20, 100).await;

    // After op_move 100 to op20:
    //   inv_value_wip(SKU-A, op20) holds:
    //     op_move_v amount = 100 × std_cum_at_op10
    //     std_cum_at_op10 = parent SKU-A's std=100 BUT bom-driven cum
    //     is 0 (no per_unit at op10, no fired per_lot). Actually the
    //     resolve_standard_cost_at(SKU-A) returns 100 (fixture seed)
    //     but our bom_lines compute has only op20 lines. std_cum_at_op10
    //     under new path = sum of per_unit at op<=10 = 0; sum of fired
    //     per_lot at op<=10 = 0 (none with fire_at=wo_start in this BOM).
    //     So std_cum_at_op10 = 0, op_move_v amount = 0.
    //   On first arrival at op20: per_unit labor fires 100×5 = 500;
    //   per_lot oh fires 200. WIP@op20 = 0 + 500 + 200 = 700, qty=100.
    //   accumulated_unit_cost = 700 / 100 = 7.

    let var_scrap = account_id_by_kind_currency(&pool, "variance_scrap", Some("USD")).await;
    let pre_var = balance(&pool, var_scrap).await;
    assert_eq!(pre_var, 0, "variance_scrap=0 before scrap");

    // Scrap 5 of 100 at op20 → unit_cost=7, value taken to scrap = 5 × 7 = 35.
    scrap(&pool, &wo_id, 20, 5).await;

    let post_var = balance(&pool, var_scrap).await;
    assert_eq!(
        post_var, 35,
        "scrap 5 at unit_cost=7 → variance_scrap=35 (proportional share of fired per-lot charge included)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn full_scrap_then_post_wo_close_unproduced() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Minimal BOM: just a wo_start charge so the pool isn't empty.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_charge(&pool, bom_id, 1, 10, "oh_std", 150, "wo_start").await;

    let wo_id = create_test_wo(&pool, "PHX-SD4-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    wo_start(&pool, &wo_id).await;
    scrap(&pool, &wo_id, 10, 100).await;

    // qty_completed=0, qty_scrapped=100=qty_target. Status still 'released'.
    let status: String = sqlx::query_scalar("SELECT status FROM work_orders WHERE id = $1::UUID")
        .bind(&wo_id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "released", "WO still released after full scrap");

    // post_wo_close_unproduced should close it cleanly.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_close_unproduced($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("post_wo_close_unproduced");

    let status: String = sqlx::query_scalar("SELECT status FROM work_orders WHERE id = $1::UUID")
        .bind(&wo_id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "closed", "WO closed after post_wo_close_unproduced");
}

// ============================================================
// C5e — co-products
// ============================================================
//
// Co-products are joint-production outputs where each output takes a
// share of total WIP value via wo_outputs.allocation_pct (Σ = 100).
// Each output also has its own qty (output.qty), proportionally drained
// per call as output.qty × p_qty / qty_target.
//
// By-products (NRV credit-back / negligible / disposal-cost) are
// deferred — see acct-7t4. The wo_outputs table here represents
// co-products only.

/// Scaffold an additional output SKU's FG-side accounts (stock_available
/// + inv_value_fg) at a location. Used when a WO has co-products where
/// the secondary output_sku ≠ parent_sku.
async fn scaffold_output_sku(pool: &PgPool, code: &str, fg_loc_code: &str) {
    let sku_id = fresh_sku(pool, code).await;
    let fg_loc = loc_id(pool, fg_loc_code).await;
    open_account(
        pool,
        "stock_available",
        "qty",
        None,
        Some(&sku_id),
        Some(&fg_loc),
        None,
        "debit",
    )
    .await;
    open_account(
        pool,
        "inv_value_fg",
        "value",
        Some("USD"),
        Some(&sku_id),
        Some(&fg_loc),
        None,
        "debit",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn coproduct_two_outputs_60_40_sales_value_split() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Component setup. Parent's std cost is fixture-seeded SKU-A=100,
    // so total_drain at qty_target=100 = 10,000 — clean splits below.
    fresh_location(&pool, "PHX-CP1-RAW").await;
    scaffold_component(&pool, "PHX-CP1-CMP", "PHX-CP1-RAW", 50, 1000).await;

    // Co-product B (the secondary co-product). Parent SKU-A doubles as
    // co-product A; its FG-side accounts are already in fixture.
    scaffold_output_sku(&pool, "PHX-CP1-B", "MAIN").await;

    // BOM: 2 units of component per parent at op10. value at op10 = 2×50 = 100/unit.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-CP1-CMP", "PHX-CP1-RAW", 2, 0.0).await;

    // WO + 2 routings.
    let wo_id = create_test_wo(&pool, "PHX-CP1-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    // Pre-populate wo_outputs (so post_wo_start doesn't auto-init the
    // single-output default). Co-product A=SKU-A 60 units / 60% value;
    // Co-product B=PHX-CP1-B 40 units / 40% value.
    add_wo_output(&pool, &wo_id, 1, "SKU-A",     "MAIN", 60, "sales_value", 60.0).await;
    add_wo_output(&pool, &wo_id, 2, "PHX-CP1-B", "MAIN", 40, "sales_value", 40.0).await;

    wo_start(&pool, &wo_id).await;
    op_move(&pool, &wo_id, 10, 20, 100).await;

    // Final completion.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, 100, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("post_wo_complete");

    // Assert each output got its qty + value share.
    let a_qty = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let b_qty = account_id_stock_available(&pool, "PHX-CP1-B", "MAIN").await;
    let a_val = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts WHERE kind='inv_value_fg' AND sku_id=(SELECT id FROM skus WHERE code='SKU-A') AND location_id=(SELECT id FROM locations WHERE code='MAIN')",
    )
    .fetch_one(&pool)
    .await
    .expect("a_val lookup");
    let b_val = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts WHERE kind='inv_value_fg' AND sku_id=(SELECT id FROM skus WHERE code='PHX-CP1-B') AND location_id=(SELECT id FROM locations WHERE code='MAIN')",
    )
    .fetch_one(&pool)
    .await
    .expect("b_val lookup");

    assert_eq!(balance(&pool, a_qty).await, 60, "co-product A: 60 units");
    assert_eq!(balance(&pool, b_qty).await, 40, "co-product B: 40 units");
    assert_eq!(balance(&pool, a_val).await, 6000, "co-product A: 60% of 10000");
    assert_eq!(balance(&pool, b_val).await, 4000, "co-product B: 40% of 10000");

    // WIP@op20 should drain to ~0; any tiny residue lands in
    // variance_wo_close via wo_close_v at final close.
    let wip_val_op20 = account_id_for_selector(
        &pool,
        "inv_value_wip",
        Some("SKU-A"),
        None,
        Some("USD"),
        Some(20),
    )
    .await;
    assert_eq!(balance(&pool, wip_val_op20).await, 0, "WIP@op20 fully drained");

    // Status = closed (final close was triggered).
    let status: String = sqlx::query_scalar("SELECT status FROM work_orders WHERE id = $1::UUID")
        .bind(&wo_id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn coproduct_two_outputs_70_30_fixed_ratio_partial_completion() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-CP2-RAW").await;
    scaffold_component(&pool, "PHX-CP2-CMP", "PHX-CP2-RAW", 50, 1000).await;
    scaffold_output_sku(&pool, "PHX-CP2-B", "MAIN").await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-CP2-CMP", "PHX-CP2-RAW", 2, 0.0).await;

    let wo_id = create_test_wo(&pool, "PHX-CP2-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    add_wo_output(&pool, &wo_id, 1, "SKU-A",     "MAIN", 70, "fixed_ratio", 70.0).await;
    add_wo_output(&pool, &wo_id, 2, "PHX-CP2-B", "MAIN", 30, "fixed_ratio", 30.0).await;

    wo_start(&pool, &wo_id).await;
    op_move(&pool, &wo_id, 10, 20, 100).await;

    // First partial completion: p_qty=50.
    let posted_by = fresh_uuid(&pool).await;
    let key1 = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, 50, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key1)
    .execute(&pool)
    .await
    .expect("partial wo_complete");

    let a_qty = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let b_qty = account_id_stock_available(&pool, "PHX-CP2-B", "MAIN").await;

    // First call: A.q = floor(70 * 50 / 100) = 35; B.q = 50 - 35 = 15 (last absorbs).
    assert_eq!(balance(&pool, a_qty).await, 35, "first partial: A=35");
    assert_eq!(balance(&pool, b_qty).await, 15, "first partial: B=15 (last absorbs residual)");

    // Status still 'released' — not at qty_target yet.
    let status: String = sqlx::query_scalar("SELECT status FROM work_orders WHERE id = $1::UUID")
        .bind(&wo_id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "released");

    // Second completion finishes the WO.
    let key2 = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, 50, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key2)
    .execute(&pool)
    .await
    .expect("final wo_complete");

    // Cumulative: A=70, B=30. Sum=100=qty_target.
    assert_eq!(balance(&pool, a_qty).await, 70, "cumulative A=70");
    assert_eq!(balance(&pool, b_qty).await, 30, "cumulative B=30");

    let status: String = sqlx::query_scalar("SELECT status FROM work_orders WHERE id = $1::UUID")
        .bind(&wo_id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn coproduct_allocation_pct_mismatch_raises_p0033() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-CP3-RAW").await;
    scaffold_component(&pool, "PHX-CP3-CMP", "PHX-CP3-RAW", 50, 1000).await;
    scaffold_output_sku(&pool, "PHX-CP3-B", "MAIN").await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-CP3-CMP", "PHX-CP3-RAW", 1, 0.0).await;

    let wo_id = create_test_wo(&pool, "PHX-CP3-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    // Mis-configured: 60 + 50 = 110%, not 100.
    add_wo_output(&pool, &wo_id, 1, "SKU-A",     "MAIN", 60, "fixed_ratio", 60.0).await;
    add_wo_output(&pool, &wo_id, 2, "PHX-CP3-B", "MAIN", 40, "fixed_ratio", 50.0).await;

    expect_sqlstate("P0033", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query(
            "SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
        )
        .bind(&wo_id)
        .bind(&posted_by)
        .bind(&key)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

// ============================================================
// C5a.3 — recursion limit: 17-level chain raises P0032
// ============================================================
//
// Build a chain PH-0 → PH-1 → PH-2 → ... → PH-17 where each phantom
// references the next via a single item line. After 16 levels of
// recursion the WHERE clause cuts off; if any depth=16 row is still
// a phantom-item-pointing line, P0032 fires.

#[tokio::test(flavor = "multi_thread")]
async fn explode_phantom_recursion_limit_raises_p0032() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let _raw_loc = fresh_location(&pool, "PHX-DEEP-RAW").await;

    // Top-level (non-phantom) parent.
    let _ = fresh_sku(&pool, "PHX-DEEP-0").await;
    // 17 levels of phantoms. Each level's BOM points at the next.
    for i in 1..=17 {
        let code = format!("PHX-DEEP-{i}");
        if i < 17 {
            fresh_phantom_sku(&pool, &code).await;
        } else {
            // Innermost has a real component to ground the recursion
            // (so without the cap, it would terminate). We're using a
            // depth-cap test, so this row never gets reached.
            fresh_sku(&pool, &code).await;
        }
    }

    // Build BOMs: PHX-DEEP-0 has line item → PHX-DEEP-1 (phantom).
    // Each phantom-i has line item → PHX-DEEP-(i+1).
    let top_bom = create_bom_header(&pool, "PHX-DEEP-0").await;
    add_bom_item(
        &pool,
        top_bom,
        1,
        10,
        "PHX-DEEP-1",
        "PHX-DEEP-RAW",
        1,
        0.0,
    )
    .await;
    for i in 1..=16 {
        let bom = create_bom_header(&pool, &format!("PHX-DEEP-{i}")).await;
        let next = format!("PHX-DEEP-{}", i + 1);
        add_bom_item(&pool, bom, 1, 10, &next, "PHX-DEEP-RAW", 1, 0.0).await;
    }

    // PHX-DEEP-0 → 1 → 2 → ... → 17 = 18 levels including the top.
    // Depth in the CTE goes 1..=17 (top is depth=1, PH-1 is depth=2,
    // ... PH-16 is depth=17). Cap is depth < 16, so depth=16 represents
    // PH-15, which still points at phantom PH-16 → P0032 fires.

    expect_sqlstate("P0032", || async {
        sqlx::query("SELECT _wo_explode_bom($1, '2026-04-15'::DATE)")
            .bind(top_bom)
            .execute(&pool)
            .await
            .map(|_| ())
    })
    .await;
}

// ============================================================
// C5f — alternate BOMs + revisions
// ============================================================
//
// _wo_resolve_bom_for picks:
//   - work_orders.bom_id if set (caller pin — bypasses effectivity/primary)
//   - else bom_header_at(parent, alt=1, business_date) — the primary path,
//     filtered by status='active' AND effective_at < business_date < obsolete_at
//
// post_wo_start's dispatch-by-existence check additionally requires a
// primary active BOM to exist for the parent at business_date when bom_id
// is NULL — so revision tests that flip primary across business_dates need
// to model that lifecycle (UPDATE is_primary as ECOs supersede each other).

async fn stock_consumed_for(pool: &PgPool, sku_code: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE kind='stock_consumed' AND sku_id=(SELECT id FROM skus WHERE code=$1)",
    )
    .bind(sku_code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("stock_consumed lookup {sku_code}: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn alternates_bom_id_null_picks_primary_set_picks_alternate() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-ALT-RAW").await;
    scaffold_component(&pool, "PHX-ALT-CA1", "PHX-ALT-RAW", 50, 1000).await;
    scaffold_component(&pool, "PHX-ALT-CA2", "PHX-ALT-RAW", 50, 1000).await;

    // alt 1 (primary, default helper): consumes CA1.
    let bom_alt1 = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_alt1, 1, 10, "PHX-ALT-CA1", "PHX-ALT-RAW", 1, 0.0).await;

    // alt 2 (substitute, is_primary=false): consumes CA2. Different
    // alternate_no → no conflict with the bom_headers_primary partial
    // unique index even though it covers alt 1.
    let bom_alt2 =
        create_bom_header_full(&pool, "SKU-A", 2, "A", false, "active", None).await;
    add_bom_item(&pool, bom_alt2, 1, 10, "PHX-ALT-CA2", "PHX-ALT-RAW", 1, 0.0).await;

    // WO 1: bom_id NULL → resolver falls back to primary alt 1.
    let wo1 = create_test_wo(&pool, "PHX-ALT-001", "SKU-A", "MAIN", 50).await;
    add_test_routing(&pool, &wo1, 10, "MILL").await;
    wo_start(&pool, &wo1).await;

    let ca1_consumed = stock_consumed_for(&pool, "PHX-ALT-CA1").await;
    let ca2_consumed = stock_consumed_for(&pool, "PHX-ALT-CA2").await;

    assert_eq!(
        rm_issued_qty(&pool, ca1_consumed).await,
        50,
        "WO1 (bom_id NULL) drained alt 1 component (CA1)"
    );
    assert_eq!(
        rm_issued_qty(&pool, ca2_consumed).await,
        0,
        "WO1 did not drain alt 2 component (CA2)"
    );

    // WO 2: bom_id pinned to alt 2.
    let wo2 = create_test_wo(&pool, "PHX-ALT-002", "SKU-A", "MAIN", 50).await;
    add_test_routing(&pool, &wo2, 10, "MILL").await;
    set_wo_bom_id(&pool, &wo2, bom_alt2).await;
    wo_start(&pool, &wo2).await;

    assert_eq!(
        rm_issued_qty(&pool, ca2_consumed).await,
        50,
        "WO2 (bom_id=alt 2) drained alt 2 component (CA2)"
    );
    assert_eq!(
        rm_issued_qty(&pool, ca1_consumed).await,
        50,
        "WO2 did not double-drain alt 1 (cumulative still 50 from WO1)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn primary_bom_resolution_via_resolver_function() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-RES-RAW").await;
    scaffold_component(&pool, "PHX-RES-CMP", "PHX-RES-RAW", 50, 500).await;

    // Single primary active BOM (default helper).
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-RES-CMP", "PHX-RES-RAW", 1, 0.0).await;

    let wo_id = create_test_wo(&pool, "PHX-RES-001", "SKU-A", "MAIN", 25).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    // Direct resolver-call assertion: _wo_resolve_bom_for returns our bom_id.
    let resolved: i64 = sqlx::query_scalar(
        "SELECT (_wo_resolve_bom_for($1::UUID, '2026-04-15'::DATE)).id",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .expect("resolver");
    assert_eq!(resolved, bom_id, "resolver picks the only active primary BOM");

    // Round-trip: post_wo_start emits the line.
    wo_start(&pool, &wo_id).await;
    let consumed = stock_consumed_for(&pool, "PHX-RES-CMP").await;
    assert_eq!(
        rm_issued_qty(&pool, consumed).await,
        25,
        "primary BOM emitted its rm_issue line"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn revisions_effective_date_picks_active_revision() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-REV-RAW").await;
    scaffold_component(&pool, "PHX-REV-CA", "PHX-REV-RAW", 50, 1000).await;
    scaffold_component(&pool, "PHX-REV-CB", "PHX-REV-RAW", 50, 1000).await;

    // rev A (alt=1, is_primary=true initially): window [2026-01-01, 2026-06-01).
    let bom_a = create_bom_header(&pool, "SKU-A").await;
    sqlx::query(
        "UPDATE bom_headers
            SET effective_at='2026-01-01'::TIMESTAMPTZ,
                obsolete_at='2026-06-01'::TIMESTAMPTZ
          WHERE id=$1",
    )
    .bind(bom_a)
    .execute(&pool)
    .await
    .expect("set rev A window");
    add_bom_item(&pool, bom_a, 1, 10, "PHX-REV-CA", "PHX-REV-RAW", 1, 0.0).await;

    // rev B (alt=1 rev='B', is_primary=false initially): window [2026-06-01, infinity).
    // is_primary stays false until the ECO transition below — the
    // bom_headers_primary partial unique index forbids two rows being
    // is_primary=true AND status='active' for the same (parent, alt).
    let bom_b =
        create_bom_header_full(&pool, "SKU-A", 1, "B", false, "active", None).await;
    sqlx::query(
        "UPDATE bom_headers SET effective_at='2026-06-01'::TIMESTAMPTZ WHERE id=$1",
    )
    .bind(bom_b)
    .execute(&pool)
    .await
    .expect("set rev B effective_at");
    add_bom_item(&pool, bom_b, 1, 10, "PHX-REV-CB", "PHX-REV-RAW", 1, 0.0).await;

    // Direct resolver assertions: bom_header_at filters status='active'
    // and the effectivity window — does NOT filter is_primary, so both
    // revs are eligible per their own window.
    let parent_sku_a: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code='SKU-A'")
        .fetch_one(&pool)
        .await
        .expect("SKU-A id");
    let resolved_a: i64 =
        sqlx::query_scalar("SELECT (bom_header_at($1::UUID, 1, '2026-04-15'::DATE)).id")
            .bind(&parent_sku_a)
            .fetch_one(&pool)
            .await
            .expect("resolver A");
    assert_eq!(resolved_a, bom_a, "2026-04-15 picks rev A by effective_at filter");

    let resolved_b: i64 =
        sqlx::query_scalar("SELECT (bom_header_at($1::UUID, 1, '2026-07-15'::DATE)).id")
            .bind(&parent_sku_a)
            .fetch_one(&pool)
            .await
            .expect("resolver B");
    assert_eq!(resolved_b, bom_b, "2026-07-15 picks rev B by effective_at filter");

    // April WO: rev A is primary in April → dispatch activates new path,
    // resolver returns rev A, post_wo_start drains CA.
    let wo_april = create_test_wo(&pool, "PHX-REV-APR", "SKU-A", "MAIN", 10).await;
    add_test_routing(&pool, &wo_april, 10, "MILL").await;
    let posted_by = fresh_uuid(&pool).await;
    let key1 = fresh_uuid(&pool).await;
    sqlx::query("SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)")
        .bind(&wo_april)
        .bind(&posted_by)
        .bind(&key1)
        .execute(&pool)
        .await
        .expect("post_wo_start april");

    // Simulate ECO transition: rev B becomes primary, rev A loses
    // primary status. (Real lifecycle is acct-yqt post_eco_approve;
    // we model it inline here.) Order matters: drop A's primary first,
    // then set B's, otherwise both rows are primary mid-transaction
    // and the partial unique index conflicts.
    sqlx::query("UPDATE bom_headers SET is_primary=FALSE WHERE id=$1")
        .bind(bom_a)
        .execute(&pool)
        .await
        .expect("rev A drop primary");
    sqlx::query("UPDATE bom_headers SET is_primary=TRUE WHERE id=$1")
        .bind(bom_b)
        .execute(&pool)
        .await
        .expect("rev B set primary");

    // June WO: rev A's window ends 2026-06-01, so a 2026-06-15 business
    // date falls into rev B's window. (Fixture only seeds periods through
    // 2026-06; using 2026-06-15 keeps us inside an open period.) Rev B
    // is primary now → dispatch activates new path, resolver returns
    // rev B, post_wo_start drains CB.
    let wo_july = create_test_wo(&pool, "PHX-REV-JUN", "SKU-A", "MAIN", 10).await;
    add_test_routing(&pool, &wo_july, 10, "MILL").await;
    let key2 = fresh_uuid(&pool).await;
    sqlx::query("SELECT post_wo_start($1::UUID, '2026-06-15'::DATE, $2::UUID, $3::UUID, NULL)")
        .bind(&wo_july)
        .bind(&posted_by)
        .bind(&key2)
        .execute(&pool)
        .await
        .expect("post_wo_start june");

    let ca_consumed = stock_consumed_for(&pool, "PHX-REV-CA").await;
    let cb_consumed = stock_consumed_for(&pool, "PHX-REV-CB").await;
    assert_eq!(
        rm_issued_qty(&pool, ca_consumed).await,
        10,
        "April WO drained rev A's component (CA)"
    );
    assert_eq!(
        rm_issued_qty(&pool, cb_consumed).await,
        10,
        "July WO drained rev B's component (CB)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bom_id_pin_overrides_default_picks_draft_bom() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-PIN-RAW").await;
    scaffold_component(&pool, "PHX-PIN-CDFL", "PHX-PIN-RAW", 50, 500).await;
    scaffold_component(&pool, "PHX-PIN-CDRF", "PHX-PIN-RAW", 50, 500).await;

    // Default active primary BOM (alt=1, rev='A', is_primary=true, status='active').
    let bom_default = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(
        &pool,
        bom_default,
        1,
        10,
        "PHX-PIN-CDFL",
        "PHX-PIN-RAW",
        1,
        0.0,
    )
    .await;

    // Draft BOM (status='draft', is_primary=false). bom_header_at would
    // skip this since it filters status='active'; but caller pins it
    // explicitly via work_orders.bom_id. _wo_resolve_bom_for honors
    // the pin without re-checking status — caller may legitimately
    // want to test a future-effective draft.
    let bom_draft =
        create_bom_header_full(&pool, "SKU-A", 1, "DRAFT", false, "draft", None).await;
    add_bom_item(
        &pool,
        bom_draft,
        1,
        10,
        "PHX-PIN-CDRF",
        "PHX-PIN-RAW",
        1,
        0.0,
    )
    .await;

    let wo_id = create_test_wo(&pool, "PHX-PIN-001", "SKU-A", "MAIN", 20).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    set_wo_bom_id(&pool, &wo_id, bom_draft).await;

    // _wo_resolve_bom_for honors the pin even for status='draft'.
    let resolved: i64 = sqlx::query_scalar(
        "SELECT (_wo_resolve_bom_for($1::UUID, '2026-04-15'::DATE)).id",
    )
    .bind(&wo_id)
    .fetch_one(&pool)
    .await
    .expect("resolver");
    assert_eq!(
        resolved, bom_draft,
        "bom_id pin bypasses effectivity/status filter"
    );

    wo_start(&pool, &wo_id).await;

    let dfl_consumed = stock_consumed_for(&pool, "PHX-PIN-CDFL").await;
    let drf_consumed = stock_consumed_for(&pool, "PHX-PIN-CDRF").await;
    assert_eq!(
        rm_issued_qty(&pool, dfl_consumed).await,
        0,
        "default BOM was NOT used (pin overrides)"
    );
    assert_eq!(
        rm_issued_qty(&pool, drf_consumed).await,
        20,
        "pinned draft BOM emitted its lines"
    );
}

// ============================================================
// C5g — ECO approval workflow
// ============================================================
//
// post_eco_approve(eco_id, effective_at, approved_by) transitions a
// draft ECO to 'approved' and walks bom_headers WHERE eco_id = ?:
//   - new draft rows: status='active', effective_at = ECO.effective_at
//   - prior active rows for the same (parent, alternate_no): status=
//     'obsolete', obsolete_at = ECO.effective_at
//
// The obsolete-old-then-activate-new ordering inside the function
// avoids a transient bom_headers_primary partial UNIQUE conflict when
// both rows are is_primary=true.

#[tokio::test(flavor = "multi_thread")]
async fn eco_approval_happy_path_no_predecessor() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-ECO1-RAW").await;
    scaffold_component(&pool, "PHX-ECO1-CMP", "PHX-ECO1-RAW", 50, 500).await;

    // Draft ECO with one attached draft bom_header. No prior active rev
    // exists (so the "obsolete predecessor" branch is a no-op here —
    // covered in the next test).
    let requested_by = fresh_uuid(&pool).await;
    let eco_id: i64 = sqlx::query_scalar(
        "INSERT INTO engineering_change_orders (code, description, requested_by)
         VALUES ('ECO-001', 'Initial BOM for SKU-A', $1::UUID) RETURNING id",
    )
    .bind(&requested_by)
    .fetch_one(&pool)
    .await
    .expect("create ECO");

    let bom_id =
        create_bom_header_full(&pool, "SKU-A", 1, "A", true, "draft", Some(eco_id)).await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-ECO1-CMP", "PHX-ECO1-RAW", 1, 0.0).await;

    // Approve at effective_at='2026-04-01' (covered by the open
    // 2026-04 period).
    let approved_by = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_eco_approve($1, '2026-04-01 00:00:00+00'::TIMESTAMPTZ, $2::UUID)",
    )
    .bind(eco_id)
    .bind(&approved_by)
    .execute(&pool)
    .await
    .expect("post_eco_approve");

    // Compare timestamps server-side to avoid pulling chrono into deps.
    let (eco_status, eco_eff_match, eco_approved_match, eco_approved_set): (
        String,
        bool,
        bool,
        bool,
    ) = sqlx::query_as(
        "SELECT status,
                effective_at = '2026-04-01 00:00:00+00'::TIMESTAMPTZ,
                approved_by  = $2::UUID,
                approved_at  IS NOT NULL
           FROM engineering_change_orders WHERE id = $1",
    )
    .bind(eco_id)
    .bind(&approved_by)
    .fetch_one(&pool)
    .await
    .expect("ECO row");
    assert_eq!(eco_status, "approved");
    assert!(eco_eff_match, "ECO.effective_at = 2026-04-01");
    assert!(eco_approved_match, "ECO.approved_by = approver");
    assert!(eco_approved_set, "ECO.approved_at populated");

    // bom_header now active with effective_at = ECO.effective_at.
    let (bom_status, bom_eff_match): (String, bool) = sqlx::query_as(
        "SELECT status, effective_at = '2026-04-01 00:00:00+00'::TIMESTAMPTZ
           FROM bom_headers WHERE id = $1",
    )
    .bind(bom_id)
    .fetch_one(&pool)
    .await
    .expect("bom_header row");
    assert_eq!(bom_status, "active", "bom_header activated");
    assert!(bom_eff_match, "bom_header.effective_at = ECO.effective_at");

    // Round-trip: WO at 2026-04-15 picks the now-active rev A and drains
    // its component (sanity check that the lifecycle ended in a usable
    // state).
    let wo_id = create_test_wo(&pool, "PHX-ECO1-001", "SKU-A", "MAIN", 5).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    wo_start(&pool, &wo_id).await;
    let cmp_consumed = stock_consumed_for(&pool, "PHX-ECO1-CMP").await;
    assert_eq!(rm_issued_qty(&pool, cmp_consumed).await, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn eco_obsoletes_prior_revision_then_new_rev_takes_over() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-ECO2-RAW").await;
    scaffold_component(&pool, "PHX-ECO2-CA", "PHX-ECO2-RAW", 50, 1000).await;
    scaffold_component(&pool, "PHX-ECO2-CB", "PHX-ECO2-RAW", 50, 1000).await;

    // Existing primary active rev A for SKU-A alt 1.
    let bom_a = create_bom_header(&pool, "SKU-A").await;
    sqlx::query(
        "UPDATE bom_headers SET effective_at='2026-01-01'::TIMESTAMPTZ WHERE id=$1",
    )
    .bind(bom_a)
    .execute(&pool)
    .await
    .expect("set rev A effective_at");
    add_bom_item(&pool, bom_a, 1, 10, "PHX-ECO2-CA", "PHX-ECO2-RAW", 1, 0.0).await;

    // Pre-ECO WO at 2026-04-15 picks rev A.
    let wo_pre = create_test_wo(&pool, "PHX-ECO2-PRE", "SKU-A", "MAIN", 7).await;
    add_test_routing(&pool, &wo_pre, 10, "MILL").await;
    wo_start(&pool, &wo_pre).await;
    let ca_consumed = stock_consumed_for(&pool, "PHX-ECO2-CA").await;
    assert_eq!(
        rm_issued_qty(&pool, ca_consumed).await,
        7,
        "pre-ECO WO drained rev A's component"
    );

    // Draft ECO + draft rev B (alternate=1, revision='B', is_primary=true,
    // status='draft'). is_primary=true is allowed at draft because the
    // bom_headers_primary partial UNIQUE ignores non-active rows.
    let requested_by = fresh_uuid(&pool).await;
    let eco_id: i64 = sqlx::query_scalar(
        "INSERT INTO engineering_change_orders (code, description, requested_by)
         VALUES ('ECO-002', 'Replace SKU-A rev A with rev B', $1::UUID) RETURNING id",
    )
    .bind(&requested_by)
    .fetch_one(&pool)
    .await
    .expect("create ECO");

    let bom_b =
        create_bom_header_full(&pool, "SKU-A", 1, "B", true, "draft", Some(eco_id))
            .await;
    add_bom_item(&pool, bom_b, 1, 10, "PHX-ECO2-CB", "PHX-ECO2-RAW", 1, 0.0).await;

    // Approve at T='2026-05-01' (covered by 2026-05 period).
    let approved_by = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_eco_approve($1, '2026-05-01 00:00:00+00'::TIMESTAMPTZ, $2::UUID)",
    )
    .bind(eco_id)
    .bind(&approved_by)
    .execute(&pool)
    .await
    .expect("post_eco_approve");

    // rev A: status='obsolete', obsolete_at = T.
    let (a_status, a_obs_match): (String, bool) = sqlx::query_as(
        "SELECT status, obsolete_at = '2026-05-01 00:00:00+00'::TIMESTAMPTZ
           FROM bom_headers WHERE id = $1",
    )
    .bind(bom_a)
    .fetch_one(&pool)
    .await
    .expect("rev A row");
    assert_eq!(a_status, "obsolete", "rev A obsoleted by ECO");
    assert!(a_obs_match, "rev A.obsolete_at = ECO.effective_at");

    // rev B: status='active', effective_at = T.
    let (b_status, b_eff_match): (String, bool) = sqlx::query_as(
        "SELECT status, effective_at = '2026-05-01 00:00:00+00'::TIMESTAMPTZ
           FROM bom_headers WHERE id = $1",
    )
    .bind(bom_b)
    .fetch_one(&pool)
    .await
    .expect("rev B row");
    assert_eq!(b_status, "active", "rev B activated");
    assert!(b_eff_match, "rev B.effective_at = ECO.effective_at");

    // Post-ECO WO at 2026-05-15 picks rev B.
    let wo_post = create_test_wo(&pool, "PHX-ECO2-POST", "SKU-A", "MAIN", 9).await;
    add_test_routing(&pool, &wo_post, 10, "MILL").await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_start($1::UUID, '2026-05-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_post)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("post_wo_start post-ECO");

    let cb_consumed = stock_consumed_for(&pool, "PHX-ECO2-CB").await;
    assert_eq!(
        rm_issued_qty(&pool, cb_consumed).await,
        9,
        "post-ECO WO drained rev B's component"
    );
    // Cumulative on CA stays at 7 — rev A is obsolete and not pickable.
    assert_eq!(
        rm_issued_qty(&pool, ca_consumed).await,
        7,
        "rev A no longer used after ECO"
    );
}

// ============================================================
// C5i — misc edge cases
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn wo_close_v_absorbs_intermediate_op_residual() {
    // The new wo_complete path walks ALL inv_value_wip(parent, op_*, ccy)
    // accounts for the WO and absorbs any non-zero balance via wo_close_v
    // at final close — extending the legacy path's last-op-only sweep.
    // This catches lot-amortization residue at non-last ops.
    //
    // Setup math (qty_target=100, default_lot_size=100, parent_std=3):
    //   per_lot wo_start charge std=349
    //   per_lot_cum at op10 = (349::numeric / 100)::bigint = 3
    //                         (PG rounds 3.49 → 3)
    //   std_cum_at_op10     = 0 + 3 = 3
    //   pool@op10 actual    = 349 (charge fires once at wo_start)
    //   op_move_v amount    = 100 × 3 = 300
    //   pool@op10 residual  = 349 − 300 = 49  ←
    //   pool@op20 after move = 300
    //   wo_complete value-leg = 100 × 3 = 300 → drains op20 to zero
    //   walk-all sweep absorbs op10 +49 into variance_wo_close.
    //
    // Why std=349 (not 350): PG's ::BIGINT cast rounds half AWAY from
    // zero, so 350/100 = 3.5 → 4, which would over-drain op_move_v
    // (100×4=400 > 349 pool) and trip the inv_value_wip CHECK.

    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Force amortization truncation by setting default_lot_size > 1
    // and overriding SKU-A's std_cost to match std_cum_at_last_op so
    // the only post-close residue is the op10 truncation residual
    // (otherwise parent_std=100 from the fixture would over-drain
    // op20 into a huge negative variance that swamps the test signal).
    sqlx::query("UPDATE skus SET default_lot_size=100 WHERE code='SKU-A'")
        .execute(&pool)
        .await
        .expect("set default_lot_size");
    let posted_by_seed = fresh_uuid(&pool).await;
    let std_key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         SELECT id, 3, '2026-04-01'::DATE, $1::UUID, $2::UUID FROM skus WHERE code='SKU-A'",
    )
    .bind(&posted_by_seed)
    .bind(&std_key)
    .execute(&pool)
    .await
    .expect("override SKU-A std cost to 3");

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    // PG's ::BIGINT cast rounds-half-away-from-zero, NOT integer truncation:
    //   350/100 = 3.5 → 4 (over-drains: op_move_v 400 > pool 350 → CHECK violation).
    // Pick std_amount=349 instead: 349/100 = 3.49 → 3, op_move_v 100×3=300,
    // pool@op10 actual = 349, residual = 349 − 300 = 49.
    add_bom_charge(&pool, bom_id, 1, 10, "oh_std", 349, "wo_start").await;

    let wo_id = create_test_wo(&pool, "PHX-RES-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;
    add_test_routing(&pool, &wo_id, 20, "FINISH").await;

    let var_close = account_id_by_kind_currency(&pool, "variance_wo_close", Some("USD")).await;
    let wip_op10 = account_id_for_selector(
        &pool,
        "inv_value_wip",
        Some("SKU-A"),
        None,
        Some("USD"),
        Some(10),
    )
    .await;
    let wip_op20 = account_id_for_selector(
        &pool,
        "inv_value_wip",
        Some("SKU-A"),
        None,
        Some("USD"),
        Some(20),
    )
    .await;

    wo_start(&pool, &wo_id).await;
    assert_eq!(
        balance(&pool, wip_op10).await,
        349,
        "after wo_start: pool@op10 = 349 (wo_start charge)"
    );

    op_move(&pool, &wo_id, 10, 20, 100).await;
    assert_eq!(
        balance(&pool, wip_op10).await,
        49,
        "after op_move: pool@op10 = 349 − 300 = 49 (lot-amortization residual)"
    );
    assert_eq!(balance(&pool, wip_op20).await, 300, "after op_move: pool@op20 = 300");

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "SELECT post_wo_complete($1::UUID, 100, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
    )
    .bind(&wo_id)
    .bind(&posted_by)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("post_wo_complete");

    assert_eq!(
        balance(&pool, wip_op10).await,
        0,
        "wo_close residual sweep drained op10 to zero"
    );
    assert_eq!(balance(&pool, wip_op20).await, 0, "op20 fully drained at wo_complete");
    assert_eq!(
        balance(&pool, var_close).await,
        49,
        "variance_wo_close absorbed the 49 op10 residual"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn consumption_policy_backflush_at_complete_raises_p0035() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Flip SKU-A's consumption_policy off the default 'forward'.
    sqlx::query("UPDATE skus SET consumption_policy='backflush_at_complete' WHERE code='SKU-A'")
        .execute(&pool)
        .await
        .expect("set consumption_policy");

    // Minimal valid BOM so the gate is reached (not short-circuited
    // by an earlier P0029 / P0026 / etc.).
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_charge(&pool, bom_id, 1, 10, "oh_std", 100, "wo_start").await;

    let wo_id = create_test_wo(&pool, "PHX-CP-BF1", "SKU-A", "MAIN", 10).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    expect_sqlstate("P0035", || async {
        let posted_by = fresh_uuid(&pool).await;
        let key = fresh_uuid(&pool).await;
        sqlx::query(
            "SELECT post_wo_start($1::UUID, '2026-04-15'::DATE, $2::UUID, $3::UUID, NULL)",
        )
        .bind(&wo_id)
        .bind(&posted_by)
        .bind(&key)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn phantom_cycle_a_to_b_to_a_raises_p0032() {
    // A direct self-reference (A's BOM line points at A as a phantom)
    // is blocked by the bom_line_self_reference_check trigger (P0034).
    // A two-step cycle (A points at phantom B; B's BOM points back at A
    // as a phantom) is allowed at INSERT time but the recursive CTE in
    // _wo_explode_bom keeps incrementing depth until the depth<16 cap
    // fires, raising P0032 phantom_recursion_limit.

    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    fresh_location(&pool, "PHX-CYC-RAW").await;
    let _a = fresh_phantom_sku(&pool, "PHX-CYC-A").await;
    let _b = fresh_phantom_sku(&pool, "PHX-CYC-B").await;
    fresh_sku(&pool, "PHX-CYC-LEAF").await; // unused but available if expander needs leaf

    // A's BOM points at B (phantom). B's BOM points back at A (phantom).
    let bom_a = create_bom_header(&pool, "PHX-CYC-A").await;
    add_bom_item(&pool, bom_a, 1, 10, "PHX-CYC-B", "PHX-CYC-RAW", 1, 0.0).await;

    let bom_b = create_bom_header(&pool, "PHX-CYC-B").await;
    add_bom_item(&pool, bom_b, 1, 10, "PHX-CYC-A", "PHX-CYC-RAW", 1, 0.0).await;

    expect_sqlstate("P0032", || async {
        sqlx::query("SELECT _wo_explode_bom($1, '2026-04-15'::DATE)")
            .bind(bom_a)
            .execute(&pool)
            .await
            .map(|_| ())
    })
    .await;
}
