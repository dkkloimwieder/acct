//! T1 probes for sxl2.4 wrappers B: post_so_ship + post_wo_complete
//! extended for tracked_by='lot_and_serial' (mig 0064, acct-sxl2.4).
//!
//!   X1 — post_so_ship with unit_ids ships N units (status='shipped',
//!        customer_id stamped, type=2 issue events, audit column).
//!   X2 — post_so_ship lot_and_serial without unit_ids → P0037.
//!   X3 — post_so_ship unit_ids length mismatch → P0006.
//!   X4 — post_so_ship unit_ids of wrong product → P0006.
//!   X5 — post_so_ship unit_ids across two lots → P0006.
//!   X6 — post_wo_complete auto-generates FG serials when
//!        p_output_serials NULL (lot_code-U<seq> format).
//!   X7 — post_wo_complete with p_output_serials uses caller-supplied
//!        unit_serials + external_serials.
//!   X8 — post_wo_complete p_output_serials length mismatch → P0006.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

// ============================================================
// Scaffolding (mirrors lot_fg_lifecycle_t1 but with
// tracked_by='lot_and_serial' on the FG parent).
// ============================================================

async fn fresh_loc(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_customer(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO customers (code, name, default_currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Cust {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn fresh_sku(
    pool: &PgPool,
    code: &str,
    cost_method: &str,
    tracked_by: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', $2::cost_method, $3::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .bind(cost_method)
    .bind(tracked_by)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn set_std_cost(pool: &PgPool, sku_id: &str, cost: i64) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, $2, '2026-01-01', $3::UUID, $4::UUID)",
    )
    .bind(sku_id).bind(cost).bind(&posted_by).bind(&key)
    .execute(pool).await.unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn open_account(
    pool: &PgPool,
    kind: &str,
    ledger_kind: &str,
    currency: Option<&str>,
    sku_id: Option<&str>,
    location_id: Option<&str>,
    routing_op: Option<i32>,
    counterparty_id: Option<&str>,
    normal_side: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (
            kind, ledger_kind, currency, sku_id, location_id,
            routing_op, counterparty_id, normal_side
         ) VALUES ($1::account_kind, $2::ledger_kind, $3,
                   $4::UUID, $5::UUID, $6, $7::UUID,
                   $8::balance_direction)
         RETURNING id",
    )
    .bind(kind).bind(ledger_kind).bind(currency)
    .bind(sku_id).bind(location_id).bind(routing_op)
    .bind(counterparty_id).bind(normal_side)
    .fetch_one(pool).await.unwrap()
}

struct Sf {
    parent: String,
    fg_loc: String,
    customer_id: String,
}

/// Build a lot_fifo + lot_and_serial FG parent with a standard
/// component. Returns identifiers for use in tests.
async fn scaffold(pool: &PgPool, suffix: &str) -> Sf {
    let parent = fresh_sku(
        pool,
        &format!("FG-LS-{suffix}"),
        "lot_fifo",
        "lot_and_serial",
    )
    .await;

    let comp = fresh_sku(pool, &format!("CMP-LS-{suffix}"), "standard", "none").await;
    set_std_cost(pool, &comp, 50).await;
    let raw_loc = fresh_loc(pool, &format!("LS-{suffix}-RAW")).await;
    let fg_loc = fresh_loc(pool, &format!("LS-{suffix}-FG")).await;
    let customer_id = fresh_customer(pool, &format!("LS-{suffix}-CUST")).await;

    open_account(pool, "stock_available", "qty", None, Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "inv_value_raw", "value", Some("USD"), Some(&comp), Some(&raw_loc), None, None, "debit").await;
    open_account(pool, "stock_consumed", "qty", None, Some(&comp), None, None, None, "debit").await;

    open_account(pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), None, "debit").await;
    open_account(pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), None, "debit").await;
    open_account(pool, "stock_available", "qty", None, Some(&parent), Some(&fg_loc), None, None, "debit").await;
    open_account(pool, "inv_value_fg", "value", Some("USD"), Some(&parent), Some(&fg_loc), None, None, "debit").await;

    open_account(pool, "customer_pool", "qty", None, None, None, None, Some(&customer_id), "debit").await;
    open_account(pool, "ar_unsettled", "value", Some("USD"), None, None, None, Some(&customer_id), "debit").await;

    // Seed 50 units of standard component via inventory_adjustment.
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 50, NULL, 'USD', 'raw',
            '2026-04-10'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    )
    .bind(&comp).bind(&raw_loc).bind(&posted_by).bind(&key)
    .fetch_one(pool).await.unwrap();

    // BOM: 1 of comp per parent at op 10.
    let bom_id: i64 = sqlx::query_scalar(
        "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
         VALUES ($1::UUID, 1, 'A', TRUE, 'active') RETURNING id",
    )
    .bind(&parent).fetch_one(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO bom_lines
            (bom_id, line_no, kind, basis, applies_at_op, fire_at, yield_pct,
             component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', 100,
                 $2::UUID, $3::UUID, 1)",
    )
    .bind(bom_id).bind(&comp).bind(&raw_loc).execute(pool).await.unwrap();

    Sf { parent, fg_loc, customer_id }
}

async fn create_wo(
    pool: &PgPool,
    wo_no: &str,
    parent: &str,
    fg_loc: &str,
    qty: i64,
) -> String {
    let posted_by = fresh_uuid(pool).await;
    let wo_id = sqlx::query_scalar::<_, String>(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ($1, $2::UUID, $3::UUID, $4, 'USD', $5::UUID) RETURNING id::text",
    )
    .bind(wo_no).bind(parent).bind(fg_loc).bind(qty).bind(&posted_by)
    .fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'ASSEMBLE')")
        .bind(&wo_id).execute(pool).await.unwrap();
    wo_id
}

async fn call_wo_start(pool: &PgPool, wo_id: &str, bd: &str) {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_wo_start($1::UUID, $2::DATE, $3::UUID, $4::UUID, NULL, NULL)::text",
    )
    .bind(wo_id).bind(bd).bind(&posted_by).bind(&key)
    .fetch_one(pool).await.expect("wo_start");
}

async fn call_wo_complete(
    pool: &PgPool,
    wo_id: &str,
    qty: i64,
    bd: &str,
    output_serials: Option<serde_json::Value>,
) -> Result<String, sqlx::Error> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_wo_complete($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL, $6::JSONB)::text",
    )
    .bind(wo_id).bind(qty).bind(bd).bind(&posted_by).bind(&key).bind(output_serials)
    .fetch_one(pool).await
}

async fn create_so(pool: &PgPool, customer_id: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_orders (customer_id, status) VALUES ($1::UUID, 'open') RETURNING id::text",
    )
    .bind(customer_id).fetch_one(pool).await.unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn add_so_line(
    pool: &PgPool,
    so_id: &str,
    line_no: i32,
    sku_id: &str,
    ship_loc_id: &str,
    qty_ordered: i64,
    unit_price: i64,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO sales_order_lines
            (so_id, line_no, sku_id, ship_location_id, qty_ordered,
             unit_price, currency, tax_amount)
         VALUES ($1::UUID, $2, $3::UUID, $4::UUID, $5, $6, 'USD', 0)
         RETURNING id::text",
    )
    .bind(so_id).bind(line_no).bind(sku_id).bind(ship_loc_id)
    .bind(qty_ordered).bind(unit_price)
    .fetch_one(pool).await.unwrap()
}

async fn call_so_ship(
    pool: &PgPool,
    so_id: &str,
    lines: serde_json::Value,
    bd: &str,
) -> Result<String, sqlx::Error> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_so_ship($1::UUID, $2, $3::DATE, $4::UUID, $5::UUID, NULL)::text",
    )
    .bind(so_id).bind(lines).bind(bd).bind(&posted_by).bind(&key)
    .fetch_one(pool).await
}

/// Drive WO -> FG inventory with N units; returns Vec of unit_ids.
async fn seed_fg_units(
    pool: &PgPool,
    sf: &Sf,
    suffix: &str,
    qty: i64,
    serials: Option<&[&str]>,
) -> Vec<i64> {
    let wo_id = create_wo(pool, &format!("WO-{suffix}"), &sf.parent, &sf.fg_loc, qty).await;
    call_wo_start(pool, &wo_id, "2026-04-15").await;

    let output_serials = serials.map(|s| {
        json!({ "1": { "unit_serials": s } })
    });
    call_wo_complete(pool, &wo_id, qty, "2026-04-16", output_serials)
        .await
        .expect("wo_complete");

    sqlx::query_scalar::<_, i64>(
        "SELECT unit_id FROM inventory_units
          WHERE product_id = $1::UUID
          ORDER BY serial_no",
    )
    .bind(&sf.parent)
    .fetch_all(pool)
    .await
    .unwrap()
}

// ============================================================
// post_so_ship tests
// ============================================================

#[tokio::test]
async fn x1_so_ship_with_unit_ids_ships_units_and_emits_events() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X1").await;
    let units = seed_fg_units(&pool, &sf, "X1", 3, Some(&["X1-U-1", "X1-U-2", "X1-U-3"])).await;
    assert_eq!(units.len(), 3);

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 2, 100_00).await;

    let ship_ids = vec![units[0], units[1]];
    call_so_ship(
        &pool,
        &so_id,
        json!([{
            "so_line_id": so_line_id,
            "qty_shipped": 2,
            "unit_ids": ship_ids,
        }]),
        "2026-04-17",
    )
    .await
    .expect("x1 so_ship");

    // Both shipped units flipped + customer_id stamped.
    let shipped_rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT status::text, customer_id::text FROM inventory_units
          WHERE unit_id = ANY($1) ORDER BY unit_id",
    )
    .bind(&ship_ids)
    .fetch_all(&pool).await.unwrap();
    assert_eq!(shipped_rows.len(), 2);
    for r in &shipped_rows {
        assert_eq!(r.0, "shipped");
        assert_eq!(r.1.as_deref(), Some(sf.customer_id.as_str()));
    }

    // Third unit untouched.
    let avail: String = sqlx::query_scalar(
        "SELECT status::text FROM inventory_units WHERE unit_id = $1",
    )
    .bind(units[2])
    .fetch_one(&pool).await.unwrap();
    assert_eq!(avail, "available");

    // Two type=2 events emitted with location_id_from set.
    let n_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_unit_events
          WHERE unit_id = ANY($1) AND event_type = 2",
    )
    .bind(&ship_ids)
    .fetch_one(&pool).await.unwrap();
    assert_eq!(n_events, 2);

    // Audit column stamped.
    let audit: Vec<i64> = sqlx::query_scalar(
        "SELECT unit_ids FROM so_shipment_lines WHERE so_line_id = $1::UUID",
    )
    .bind(&so_line_id)
    .fetch_one(&pool).await.unwrap();
    let mut sorted = audit.clone();
    sorted.sort();
    let mut expected = ship_ids.clone();
    expected.sort();
    assert_eq!(sorted, expected);
}

#[tokio::test]
async fn x2_so_ship_lot_and_serial_without_unit_ids_raises_p0037() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X2").await;
    let _units = seed_fg_units(&pool, &sf, "X2", 2, None).await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 1, 100_00).await;

    expect_sqlstate("P0037", || async {
        call_so_ship(
            &pool, &so_id,
            json!([{ "so_line_id": so_line_id, "qty_shipped": 1 }]),
            "2026-04-17",
        ).await
    }).await;
}

#[tokio::test]
async fn x3_so_ship_unit_ids_length_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X3").await;
    let units = seed_fg_units(&pool, &sf, "X3", 2, None).await;

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 2, 100_00).await;

    let one_id = vec![units[0]]; // length 1 vs qty 2
    expect_sqlstate("P0006", || async {
        call_so_ship(
            &pool, &so_id,
            json!([{
                "so_line_id": so_line_id,
                "qty_shipped": 2,
                "unit_ids": one_id.clone(),
            }]),
            "2026-04-17",
        ).await
    }).await;
}

#[tokio::test]
async fn x4_so_ship_unit_ids_of_wrong_product_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X4").await;
    let units_a = seed_fg_units(&pool, &sf, "X4A", 1, Some(&["X4-A-1"])).await;

    // A second product with its own units.
    let sf_b = scaffold(&pool, "X4B").await;
    let _units_b = seed_fg_units(&pool, &sf_b, "X4B", 1, Some(&["X4-B-1"])).await;

    // Try to ship product A's unit against product B's SO line.
    let so_id = create_so(&pool, &sf_b.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf_b.parent, &sf_b.fg_loc, 1, 100_00).await;

    expect_sqlstate("P0006", || async {
        call_so_ship(
            &pool, &so_id,
            json!([{
                "so_line_id": so_line_id,
                "qty_shipped": 1,
                "unit_ids": [units_a[0]],
            }]),
            "2026-04-17",
        ).await
    }).await;
}

#[tokio::test]
async fn x5_so_ship_unit_ids_across_two_lots_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X5").await;
    // Two separate WOs => two FG lots.
    let units_a = seed_fg_units(&pool, &sf, "X5A", 1, Some(&["X5-A-1"])).await;

    // Second WO (use different bd to avoid period collisions on close).
    let wo_b = create_wo(&pool, "WO-X5B", &sf.parent, &sf.fg_loc, 1).await;
    call_wo_start(&pool, &wo_b, "2026-04-16").await;
    call_wo_complete(&pool, &wo_b, 1, "2026-04-17",
                     Some(json!({ "1": { "unit_serials": ["X5-B-1"] } })))
        .await.expect("wo_b complete");
    let unit_b: i64 = sqlx::query_scalar(
        "SELECT unit_id FROM inventory_units WHERE serial_no = 'X5-B-1'",
    ).fetch_one(&pool).await.unwrap();

    let so_id = create_so(&pool, &sf.customer_id).await;
    let so_line_id = add_so_line(&pool, &so_id, 1, &sf.parent, &sf.fg_loc, 2, 100_00).await;

    let mixed_ids = vec![units_a[0], unit_b];
    expect_sqlstate("P0006", || async {
        call_so_ship(
            &pool, &so_id,
            json!([{
                "so_line_id": so_line_id,
                "qty_shipped": 2,
                "unit_ids": mixed_ids.clone(),
            }]),
            "2026-04-18",
        ).await
    }).await;
}

// ============================================================
// post_wo_complete tests
// ============================================================

#[tokio::test]
async fn x6_wo_complete_auto_generates_fg_serials() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X6").await;

    let wo_id = create_wo(&pool, "WO-X6", &sf.parent, &sf.fg_loc, 3).await;
    call_wo_start(&pool, &wo_id, "2026-04-15").await;
    call_wo_complete(&pool, &wo_id, 3, "2026-04-16", None)
        .await.expect("wo_complete");

    let serials: Vec<String> = sqlx::query_scalar(
        "SELECT serial_no FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sf.parent)
    .fetch_all(&pool).await.unwrap();
    assert_eq!(serials.len(), 3);
    for (i, s) in serials.iter().enumerate() {
        // Auto-gen format is <lot_code>-U<6-digit-seq>; lot_code is
        // WO-<8>-1 for output_no=1.
        assert!(
            s.contains("-U") && s.ends_with(&format!("{:06}", i + 1)),
            "expected auto-gen serial ending -U{:06}, got {s}", i + 1
        );
    }
}

#[tokio::test]
async fn x7_wo_complete_with_p_output_serials_uses_caller_supplied() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X7").await;

    let wo_id = create_wo(&pool, "WO-X7", &sf.parent, &sf.fg_loc, 2).await;
    call_wo_start(&pool, &wo_id, "2026-04-15").await;
    call_wo_complete(
        &pool, &wo_id, 2, "2026-04-16",
        Some(json!({
            "1": {
                "unit_serials":     ["X7-FG-A", "X7-FG-B"],
                "external_serials": ["MFR-X7-1", "MFR-X7-2"],
            }
        })),
    )
    .await.expect("wo_complete");

    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT serial_no, external_serial_no FROM inventory_units
          WHERE product_id = $1::UUID ORDER BY serial_no",
    )
    .bind(&sf.parent)
    .fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "X7-FG-A");
    assert_eq!(rows[0].1.as_deref(), Some("MFR-X7-1"));
    assert_eq!(rows[1].0, "X7-FG-B");
    assert_eq!(rows[1].1.as_deref(), Some("MFR-X7-2"));
}

#[tokio::test]
async fn x8_wo_complete_output_serials_length_mismatch_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sf = scaffold(&pool, "X8").await;

    let wo_id = create_wo(&pool, "WO-X8", &sf.parent, &sf.fg_loc, 3).await;
    call_wo_start(&pool, &wo_id, "2026-04-15").await;
    expect_sqlstate("P0006", || async {
        call_wo_complete(
            &pool, &wo_id, 3, "2026-04-16",
            Some(json!({
                "1": { "unit_serials": ["X8-A", "X8-B"] } // only 2 vs qty 3
            })),
        ).await
    }).await;
}
