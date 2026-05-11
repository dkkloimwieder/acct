//! T1 probes for per-lot value-level subledger ↔ GL recon
//! (mig 0060, acct-20y0).
//!
//! Coverage:
//!   R1 — clean lot_fifo receipt + depletion cycle: no alerts.
//!   R2 — phantom inventory_movements row with wrong lot_id:
//!        check #13 fires (sub diverges from GL per-lot).
//!   R3 — pli.lot_id stripped to NULL on a depletion leg: check
//!        #13 fires (GL drops the lot from its grouping while
//!        sub still attributes value to that lot).
//!   R4 — multi-period boundary: per-period attribution holds
//!        across two open periods.
//!   R5 — lot_transfer DR/CR sides reconcile per-lot: 0 alerts.
//!   R6 — wo_complete lot_fifo parent cycle: 0 alerts.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Local scaffolding
// ============================================================

async fn fresh_sku_lot(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, 'lot'::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
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
         VALUES ($1::account_kind, $2::ledger_kind, $3, $4::UUID, $5::UUID, $6, $7::balance_direction)
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

async fn open_lot_component(pool: &PgPool, comp: &str, raw_loc: &str) {
    let _ = open_account(pool, "stock_available", "qty", None, Some(comp), Some(raw_loc), None, "debit").await;
    let _ = open_account(pool, "inv_value_raw", "value", Some("USD"), Some(comp), Some(raw_loc), None, "debit").await;
    let _ = open_account(pool, "stock_consumed", "qty", None, Some(comp), None, None, "debit").await;
}

async fn seed_lot(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
    lot_code: &str,
) -> i64 {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, $4, 'USD', 'raw',
            $5::DATE, $6::UUID, $7::UUID, NULL, $8
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .bind(json!({ "lot_code": lot_code }))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("seed_lot {lot_code}: {e}"));

    sqlx::query_scalar::<_, i64>(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID AND lot_code = $3",
    )
    .bind(sku)
    .bind(loc)
    .bind(lot_code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn deplete_lot(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    qty: i64,
    business_date: &str,
    lot_id: i64,
) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3, NULL, 'USD', 'raw',
            $4::DATE, $5::UUID, $6::UUID, NULL, $7
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(-qty)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .bind(json!({ "lot_id": lot_id }))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("deplete_lot: {e}"));
}

async fn count_alerts_kind(pool: &PgPool, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM reconciliation_alerts WHERE alert_kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn run_recon(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(pool)
        .await
        .unwrap()
}

// ============================================================
// R1 — clean lot_fifo receipt + depletion: zero alerts.
// ============================================================

#[tokio::test]
async fn clean_lot_fifo_cycle_no_alerts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku_lot(&pool, "R1-SKU").await;
    let loc = fresh_location(&pool, "R1-LOC").await;
    open_lot_component(&pool, &sku, &loc).await;

    let lot = seed_lot(&pool, &sku, &loc, 50, 100, "2026-04-10", "R1-LOT").await;
    deplete_lot(&pool, &sku, &loc, 20, "2026-04-12", lot).await;

    sqlx::query("DELETE FROM reconciliation_alerts").execute(&pool).await.unwrap();
    run_recon(&pool).await;

    assert_eq!(count_alerts_kind(&pool, "subledger_gl_lot_divergence").await, 0);
    // Coarser check #7 also clean (since per-lot check is a subset).
    assert_eq!(count_alerts_kind(&pool, "subledger_gl_divergence").await, 0);
}

// ============================================================
// R2 — phantom inventory_movements row: check #13 fires.
// ============================================================

#[tokio::test]
async fn phantom_movement_with_mismatched_lot_id_fires() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku_lot(&pool, "R2-SKU").await;
    let loc = fresh_location(&pool, "R2-LOC").await;
    open_lot_component(&pool, &sku, &loc).await;

    let lot = seed_lot(&pool, &sku, &loc, 50, 100, "2026-04-10", "R2-LOT").await;
    deplete_lot(&pool, &sku, &loc, 10, "2026-04-12", lot).await;

    // Synthesize: pick an arbitrary existing inventory_movements row
    // for this product/location and UPDATE its lot_id to a different
    // (non-existent) value. The subledger value sum-per-lot at the
    // mutated lot_id now diverges from GL which still attributes
    // the value to the real lot.
    let actor_movement: Option<i64> = sqlx::query_scalar(
        "SELECT movement_id FROM inventory_movements
          WHERE product_id = $1::UUID AND location_id = $2::UUID
            AND lot_id IS NOT NULL
          ORDER BY movement_id LIMIT 1",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_optional(&pool)
    .await
    .unwrap();

    if let Some(mid) = actor_movement {
        // Bump lot_id to a guaranteed-non-existent value.
        sqlx::query("UPDATE inventory_movements SET lot_id = 999999 WHERE movement_id = $1")
            .bind(mid)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("DELETE FROM reconciliation_alerts").execute(&pool).await.unwrap();
        run_recon(&pool).await;

        assert!(
            count_alerts_kind(&pool, "subledger_gl_lot_divergence").await >= 1,
            "phantom mismatched lot_id must surface as per-lot divergence"
        );
    } else {
        panic!("R2 setup: no inventory_movements row found");
    }
}

// ============================================================
// R3 — pli.lot_id stripped: check #13 fires.
// ============================================================

#[tokio::test]
async fn pli_lot_id_stripped_fires() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku_lot(&pool, "R3-SKU").await;
    let loc = fresh_location(&pool, "R3-LOC").await;
    open_lot_component(&pool, &sku, &loc).await;

    let lot = seed_lot(&pool, &sku, &loc, 50, 100, "2026-04-10", "R3-LOT").await;
    deplete_lot(&pool, &sku, &loc, 10, "2026-04-12", lot).await;

    // Pick one of the depletion pli rows and NULL out lot_id.
    // The subledger still has lot_id stamped on inventory_movements,
    // so the per-lot sum stands; the GL side drops that posting
    // from the per-lot CTE filter, creating divergence.
    let pli_id: Option<i64> = sqlx::query_scalar(
        "SELECT pli.posting_line_id
           FROM posting_line_inventory pli
           JOIN posting_lines pl ON pl.id = pli.posting_line_id
           JOIN accounts a ON a.id = pl.credit_account_id
          WHERE pli.lot_id IS NOT NULL
            AND a.sku_id = $1::UUID
            AND a.location_id = $2::UUID
            AND a.kind = 'inv_value_raw'
          LIMIT 1",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_optional(&pool)
    .await
    .unwrap();

    if let Some(plid) = pli_id {
        sqlx::query("UPDATE posting_line_inventory SET lot_id = NULL WHERE posting_line_id = $1")
            .bind(plid)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("DELETE FROM reconciliation_alerts").execute(&pool).await.unwrap();
        run_recon(&pool).await;

        assert!(
            count_alerts_kind(&pool, "subledger_gl_lot_divergence").await >= 1,
            "stripped pli.lot_id must surface as divergence"
        );
    } else {
        panic!("R3 setup: no pli row with lot_id found");
    }
}

// ============================================================
// R4 — multi-period: per-period × per-lot partitioning holds.
// ============================================================

#[tokio::test]
async fn multi_period_per_lot_recon_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku_lot(&pool, "R4-SKU").await;
    let loc = fresh_location(&pool, "R4-LOC").await;
    open_lot_component(&pool, &sku, &loc).await;

    // Period 1 (Apr): receipt + partial depletion.
    let lot = seed_lot(&pool, &sku, &loc, 50, 100, "2026-04-10", "R4-LOT").await;
    deplete_lot(&pool, &sku, &loc, 15, "2026-04-12", lot).await;

    // Period 2 (May): further depletion of the same lot.
    deplete_lot(&pool, &sku, &loc, 10, "2026-05-05", lot).await;

    sqlx::query("DELETE FROM reconciliation_alerts").execute(&pool).await.unwrap();
    run_recon(&pool).await;

    assert_eq!(count_alerts_kind(&pool, "subledger_gl_lot_divergence").await, 0);

    // Verify per-period rows exist on both sides at lot grain.
    let n_periods: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT pl.period_id)::BIGINT
           FROM posting_lines pl
           JOIN posting_line_inventory pli ON pli.posting_line_id = pl.id
           JOIN accounts a ON a.id IN (pl.debit_account_id, pl.credit_account_id)
          WHERE pli.lot_id IS NOT NULL
            AND a.sku_id = $1::UUID
            AND a.kind::TEXT LIKE 'inv_value_%'",
    )
    .bind(&sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(n_periods >= 2, "spans at least 2 periods");
}

// ============================================================
// R5 — lot_transfer: DR/CR sides per-lot reconcile cleanly.
// ============================================================

#[tokio::test]
async fn lot_transfer_per_lot_recon_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku_lot(&pool, "R5-SKU").await;
    let from_loc = fresh_location(&pool, "R5-FROM").await;
    let to_loc = fresh_location(&pool, "R5-TO").await;

    open_lot_component(&pool, &sku, &from_loc).await;
    open_lot_component(&pool, &sku, &to_loc).await;

    let lot_a = seed_lot(&pool, &sku, &from_loc, 50, 100, "2026-04-10", "R5-LOT-A").await;

    // Transfer 20 units of LOT-A from FROM-LOC to TO-LOC.
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    let lines = json!([{
        "sku_id":    sku,
        "qty":       20,
        "lot_id":    lot_a,
    }]);
    sqlx::query_scalar::<_, String>(
        "SELECT post_lot_transfer(
            $1::UUID, $2::UUID, $3, $4::DATE, $5::UUID, $6::UUID, NULL
         )::text",
    )
    .bind(&from_loc)
    .bind(&to_loc)
    .bind(&lines)
    .bind("2026-04-15")
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|e| panic!("post_lot_transfer: {e}"));

    sqlx::query("DELETE FROM reconciliation_alerts").execute(&pool).await.unwrap();
    run_recon(&pool).await;

    assert_eq!(count_alerts_kind(&pool, "subledger_gl_lot_divergence").await, 0);
}

// ============================================================
// R6 — wo_complete lot_fifo parent cycle: clean per-lot recon.
// ============================================================

#[tokio::test]
async fn wo_complete_lot_fifo_parent_recon_clean() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent = fresh_sku_lot(&pool, "R6-PARENT").await;
    let comp = fresh_sku_lot(&pool, "R6-COMP").await;
    let raw_loc = fresh_location(&pool, "R6-RAW").await;
    let fg_loc = fresh_location(&pool, "R6-FG").await;

    // Parent WIP + FG accounts.
    let _ = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let _ = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let _ = open_account(&pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, "debit").await;
    let _ = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, "debit").await;
    open_lot_component(&pool, &comp, &raw_loc).await;

    let lot_a = seed_lot(&pool, &comp, &raw_loc, 50, 100, "2026-04-10", "R6-COMP-A").await;

    // Build a minimal WO + BOM and run it.
    let wo_no_uuid = fresh_uuid(&pool).await;
    let wo_id: String = sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind("R6-WO")
    .bind(&parent)
    .bind(&fg_loc)
    .bind(5_i64)
    .bind(&wo_no_uuid)
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'MILL')")
        .bind(&wo_id)
        .execute(&pool)
        .await
        .unwrap();

    let bom_id: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(&parent)
    .fetch_one(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', 100,
                 $2::UUID, $3::UUID, 2)",
    )
    .bind(bom_id)
    .bind(&comp)
    .bind(&raw_loc)
    .execute(&pool)
    .await
    .unwrap();

    let posted_by = fresh_uuid(&pool).await;
    let key1 = fresh_uuid(&pool).await;
    let pins = json!({ comp.to_string(): lot_a });
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, $5)::text",
    )
    .bind(&wo_id)
    .bind("2026-04-15")
    .bind(&posted_by)
    .bind(&key1)
    .bind(&pins)
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|e| panic!("wo_start: {e}"));

    let key2 = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(&wo_id)
    .bind(5_i64)
    .bind("2026-04-16")
    .bind(&posted_by)
    .bind(&key2)
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|e| panic!("wo_complete: {e}"));

    sqlx::query("DELETE FROM reconciliation_alerts").execute(&pool).await.unwrap();
    run_recon(&pool).await;

    assert_eq!(count_alerts_kind(&pool, "subledger_gl_lot_divergence").await, 0);
    assert_eq!(count_alerts_kind(&pool, "subledger_gl_divergence").await, 0);
}
