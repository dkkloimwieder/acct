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
// C5b — yield gross-up via scrap_pct on item lines
// ============================================================
//
// scrap_pct is "planned material loss during assembly" — cutting waste,
// evaporation, breakage. _wo_emit_bom_lines grosses up the rm_issue
// qty so the survivors equal qty_per_parent × p_qty:
//   adj_qty = CEIL(p_qty × qty_per_parent / (1 - scrap_pct/100))
// The grossed-up extra units physically leave raw inventory; the WIP
// pool is charged adj_qty × component_std_cost.

#[tokio::test(flavor = "multi_thread")]
async fn yield_grossup_five_percent_scrap_grosses_to_211() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Component scaffold: PHX-Y-C @ std=10, raw at PHX-Y-RAW.
    fresh_location(&pool, "PHX-Y-RAW").await;
    scaffold_component(&pool, "PHX-Y-C", "PHX-Y-RAW", 10, 10_000).await;
    let comp_id = sku_id(&pool, "PHX-Y-C").await;

    // SKU-A is the parent; its accounts are seeded by the fixture.
    // Build a bom_header for SKU-A pointing at PHX-Y-C with scrap_pct=5,
    // qty_per_parent=2, applies_at_op=10.
    let bom_id = create_bom_header(&pool, "SKU-A").await;
    add_bom_item(&pool, bom_id, 1, 10, "PHX-Y-C", "PHX-Y-RAW", 2, 5.0).await;

    // WO at qty_target=100 with routing op10 only.
    let wo_id = create_test_wo(&pool, "PHX-Y-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

    // Run post_wo_start.
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
    .expect("post_wo_start");

    // adj_qty = CEIL(100 × 2 / (1 - 0.05)) = CEIL(200 / 0.95) = CEIL(210.526) = 211.
    let consumed = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts WHERE kind='stock_consumed' AND sku_id = $1::UUID",
    )
    .bind(&comp_id)
    .fetch_one(&pool)
    .await
    .expect("stock_consumed lookup");

    let issued = rm_issued_qty(&pool, consumed).await;
    assert_eq!(
        issued, 211,
        "scrap_pct=5 grosses up qty: CEIL(100 × 2 / 0.95) = 211 (got {issued})"
    );

    // The grossed-up qty went out of raw inventory.
    let raw_qty = account_id_stock_available(&pool, "PHX-Y-C", "PHX-Y-RAW").await;
    let raw_balance = balance(&pool, raw_qty).await;
    assert_eq!(raw_balance, 10_000 - 211, "raw drained by 211 units");
}

#[tokio::test(flavor = "multi_thread")]
async fn yield_grossup_multi_component_with_different_scrap_pcts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Two components with different scrap_pcts.
    fresh_location(&pool, "PHX-YM-RAW").await;
    scaffold_component(&pool, "PHX-YM-C1", "PHX-YM-RAW", 5, 10_000).await;
    scaffold_component(&pool, "PHX-YM-C2", "PHX-YM-RAW", 8, 10_000).await;
    let c1_id = sku_id(&pool, "PHX-YM-C1").await;
    let c2_id = sku_id(&pool, "PHX-YM-C2").await;

    let bom_id = create_bom_header(&pool, "SKU-A").await;
    // C1: scrap=10%, qty=1 per parent. CEIL(100 × 1 / 0.90) = 112.
    add_bom_item(&pool, bom_id, 1, 10, "PHX-YM-C1", "PHX-YM-RAW", 1, 10.0).await;
    // C2: scrap=0%, qty=3 per parent. 100 × 3 = 300 (no gross-up).
    add_bom_item(&pool, bom_id, 2, 10, "PHX-YM-C2", "PHX-YM-RAW", 3, 0.0).await;

    let wo_id = create_test_wo(&pool, "PHX-YM-001", "SKU-A", "MAIN", 100).await;
    add_test_routing(&pool, &wo_id, 10, "MILL").await;

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
    .expect("post_wo_start");

    let c1_consumed = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts WHERE kind='stock_consumed' AND sku_id = $1::UUID",
    )
    .bind(&c1_id)
    .fetch_one(&pool)
    .await
    .expect("c1 stock_consumed lookup");
    let c2_consumed = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts WHERE kind='stock_consumed' AND sku_id = $1::UUID",
    )
    .bind(&c2_id)
    .fetch_one(&pool)
    .await
    .expect("c2 stock_consumed lookup");

    assert_eq!(rm_issued_qty(&pool, c1_consumed).await, 112,
        "C1 with scrap=10% grosses up to CEIL(100/0.9)=112");
    assert_eq!(rm_issued_qty(&pool, c2_consumed).await, 300,
        "C2 with scrap=0% has no gross-up: 100 × 3 = 300");
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
