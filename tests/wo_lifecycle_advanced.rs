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
