//! T1 probes for lot_fifo via `post_inventory_adjustment`
//! (mig 0048, acct-b0j1). Phase E2 follow-up L2.
//!
//! Drives the wrapper end-to-end (not `post_posting_lines` directly):
//!   - +qty branch creates inventory_lots row + stamps audit fields,
//!   - -qty branch requires explicit p_lot_metadata->>'lot_id'
//!     (no FIFO default for adjustments — operator must specify),
//!   - inventory_class='raw' is the only MVP scope (FG/WIP raise P0006),
//!   - inventory_adjustments.lot_id stamp is set both on inflow and
//!     outflow (looked up post-PERFORM for inflow; pinned for outflow).

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

#[allow(dead_code)]
struct Scaffold {
    sku_id: String,
    loc_id: String,
    qty_acct: i64,
    val_acct: i64,
    void_qty: i64,
    void_val: i64,
}

async fn id_text(pool: &PgPool, q: &str, bind: &str) -> String {
    sqlx::query_scalar(q).bind(bind).fetch_one(pool).await.unwrap()
}

async fn fresh_lot_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method, 'lot'::inventory_tracking)
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .expect("insert lot SKU")
}

async fn scaffold_lot(pool: &PgPool, label: &str) -> Scaffold {
    let sku_code = format!("SKU-LOT-{label}");
    let sku = fresh_lot_sku(pool, &sku_code).await;
    let loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;

    let qty_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available', 'qty', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

    let val_acct: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

    // creation_void(qty) and inv_adj_expense(USD) come from fixture.
    let void_qty: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind='creation_void' AND ledger_kind='qty' AND NOT is_closed",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let void_val: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind='inv_adj_expense' AND ledger_kind='value' \
         AND currency='USD' AND NOT is_closed",
    )
    .fetch_one(pool)
    .await
    .unwrap();

    Scaffold {
        sku_id: sku,
        loc_id: loc,
        qty_acct,
        val_acct,
        void_qty,
        void_val,
    }
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

#[allow(clippy::too_many_arguments)]
async fn call_adjust(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    inventory_class: &str,
    business_date: &str,
    idempotency_key: &str,
    lot_metadata: Option<serde_json::Value>,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4, 'USD', $5,
            $6::DATE, $7::UUID, $8::UUID, NULL, $9
         )::text",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(inventory_class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .bind(lot_metadata)
    .fetch_one(pool)
    .await
}

// ============================================================
// L2.1: +qty creates lot + stamps inventory_adjustments.lot_id.
// ============================================================

#[tokio::test]
async fn lot_adjustment_in_creates_lot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2A").await;
    let key = fresh_uuid(&pool).await;

    let doc_id = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 8, Some(150),
        "raw", "2026-04-15", &key,
        Some(json!({ "lot_code": "LOT-A001" })),
    )
    .await
    .expect("lot_fifo +qty should succeed");

    // Per-class ledger.
    assert_eq!(balance(&pool, sf.qty_acct).await, 8);
    assert_eq!(balance(&pool, sf.val_acct).await, 1200);
    assert_eq!(balance(&pool, sf.void_qty).await, -8);
    assert_eq!(balance(&pool, sf.void_val).await, -1200);

    // inventory_lots row with the asserted unit_cost.
    let (lot_id, lot_code, qty_text, uc_text): (i64, String, String, String) = sqlx::query_as(
        "SELECT lot_id, lot_code, original_quantity::TEXT, unit_cost::TEXT
           FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_code, "LOT-A001");
    assert!(qty_text.starts_with("8"), "qty got {qty_text}");
    assert!(uc_text.starts_with("150"), "uc got {uc_text}");

    // inventory_adjustments.lot_id stamped.
    let ia_lot: Option<i64> = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ia_lot, Some(lot_id));
}

// ============================================================
// L2.2: +qty without unit_cost raises P0011.
// ============================================================

#[tokio::test]
async fn lot_adjustment_in_missing_unit_cost_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2B").await;
    let key = fresh_uuid(&pool).await;
    let err = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 5, None,
        "raw", "2026-04-15", &key,
        Some(json!({ "lot_code": "LOT-B001" })),
    )
    .await
    .err()
    .expect("expected P0011");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0011"));
}

// ============================================================
// L2.3: +qty without lot_code raises P0022.
// ============================================================

#[tokio::test]
async fn lot_adjustment_in_missing_lot_code_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2C").await;
    let key = fresh_uuid(&pool).await;

    // Missing entirely.
    let err = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 5, Some(100),
        "raw", "2026-04-15", &key,
        None,
    )
    .await
    .err()
    .expect("expected P0022");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0022"));

    // Empty string.
    let key2 = fresh_uuid(&pool).await;
    let err2 = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 5, Some(100),
        "raw", "2026-04-15", &key2,
        Some(json!({ "lot_code": "" })),
    )
    .await
    .err()
    .expect("expected P0022 on empty");
    assert_eq!(err2.as_database_error().unwrap().code().as_deref(), Some("P0022"));
}

// ============================================================
// L2.4: +qty with optional metadata persisted on inventory_lots.
// ============================================================

#[tokio::test]
async fn lot_adjustment_in_persists_optional_metadata() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2D").await;
    let key = fresh_uuid(&pool).await;

    call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 4, Some(200),
        "raw", "2026-04-15", &key,
        Some(json!({
            "lot_code":            "LOT-D001",
            "manufacture_date":    "2026-02-01",
            "expiration_date":     "2027-02-01",
            "supplier_lot_number": "SUP-XYZ-99",
            "quality_status":      "pending",
            "attributes":          { "country": "FR" }
        })),
    )
    .await
    .expect("metadata adjustment");

    let (mfg, exp, sup, qs, attrs): (
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<serde_json::Value>,
    ) = sqlx::query_as(
        "SELECT manufacture_date::TEXT, expiration_date::TEXT,
                supplier_lot_number, quality_status, attributes
           FROM inventory_lots
          WHERE product_id = $1::UUID",
    )
    .bind(&sf.sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(mfg.as_deref(), Some("2026-02-01"));
    assert_eq!(exp.as_deref(), Some("2027-02-01"));
    assert_eq!(sup.as_deref(), Some("SUP-XYZ-99"));
    assert_eq!(qs, "pending");
    assert_eq!(attrs.unwrap()["country"], json!("FR"));
}

// ============================================================
// L2.5: -qty with explicit lot_id depletes that lot + writes
// inventory_lot_events 'issue' rows.
// ============================================================

#[tokio::test]
async fn lot_adjustment_out_with_pin_depletes_lot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2E").await;

    // Seed a lot.
    let key1 = fresh_uuid(&pool).await;
    let _doc_in = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 10, Some(80),
        "raw", "2026-04-15", &key1,
        Some(json!({ "lot_code": "LOT-E001" })),
    )
    .await
    .expect("seed lot");

    let lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = 'LOT-E001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Deplete 4 units from this lot.
    let key2 = fresh_uuid(&pool).await;
    let doc_out = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, -4, None,
        "raw", "2026-04-16", &key2,
        Some(json!({ "lot_id": lot_id })),
    )
    .await
    .expect("lot_fifo -qty with explicit pin");

    // Per-class ledger after: qty 10-4=6, val 800-320=480.
    assert_eq!(balance(&pool, sf.qty_acct).await, 6);
    assert_eq!(balance(&pool, sf.val_acct).await, 480);

    // inventory_adjustments.lot_id stamped to the pinned lot.
    let ia_lot: Option<i64> = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_adjustments WHERE id = $1::UUID",
    )
    .bind(&doc_out)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ia_lot, Some(lot_id));

    // inventory_lot_events: one 'issue' row (event_type=1) with -4 qty_delta.
    let (et, qd): (i16, String) = sqlx::query_as(
        "SELECT event_type, quantity_change::TEXT
           FROM inventory_lot_events
          WHERE lot_id = $1
          ORDER BY event_id",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(et, 1);
    assert!(qd.starts_with("-4"), "qd got {qd}");

    // Residual = 10 - 4 = 6.
    let receipt_date: String = sqlx::query_scalar(
        "SELECT receipt_date::TEXT FROM inventory_lots WHERE lot_id = $1",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let residual: String = sqlx::query_scalar(
        "SELECT _inventory_lot_remaining_qty($1, $2::DATE)::TEXT",
    )
    .bind(lot_id)
    .bind(&receipt_date)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(residual.starts_with("6"), "residual got {residual}");
}

// ============================================================
// L2.6: -qty without lot_id raises P0022 (no FIFO default).
// ============================================================

#[tokio::test]
async fn lot_adjustment_out_without_pin_raises_p0022() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2F").await;

    // Seed a lot first so the pool isn't empty (rules out other errors).
    let key_in = fresh_uuid(&pool).await;
    call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 5, Some(50),
        "raw", "2026-04-15", &key_in,
        Some(json!({ "lot_code": "LOT-F001" })),
    )
    .await
    .expect("seed");

    // No lot_metadata at all.
    let key1 = fresh_uuid(&pool).await;
    let err = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, -2, None,
        "raw", "2026-04-16", &key1,
        None,
    )
    .await
    .err()
    .expect("expected P0022");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0022"));

    // lot_metadata present but no lot_id key.
    let key2 = fresh_uuid(&pool).await;
    let err2 = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, -2, None,
        "raw", "2026-04-16", &key2,
        Some(json!({ "notes": "no pin" })),
    )
    .await
    .err()
    .expect("expected P0022");
    assert_eq!(err2.as_database_error().unwrap().code().as_deref(), Some("P0022"));
}

// ============================================================
// L2.7: -qty with unit_cost asserted raises P0011.
// ============================================================

#[tokio::test]
async fn lot_adjustment_out_with_unit_cost_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2G").await;
    let key_in = fresh_uuid(&pool).await;
    call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 5, Some(50),
        "raw", "2026-04-15", &key_in,
        Some(json!({ "lot_code": "LOT-G001" })),
    )
    .await
    .expect("seed");

    let lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = 'LOT-G001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let key = fresh_uuid(&pool).await;
    let err = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, -2, Some(99),
        "raw", "2026-04-16", &key,
        Some(json!({ "lot_id": lot_id })),
    )
    .await
    .err()
    .expect("expected P0011");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0011"));
}

// ============================================================
// L2.8: lot_fifo on inventory_class != 'raw' raises P0006.
// ============================================================

#[tokio::test]
async fn lot_adjustment_non_raw_class_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2H").await;

    // We also need an inv_value_fg account to get past the
    // account-existence check before the class restriction fires
    // (the class restriction is OUTSIDE the per-method block so it
    // fires regardless — but the wrapper looks up the value account
    // first and would raise P0010 before reaching the class check).
    let _val_fg: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_fg', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sf.sku_id)
    .bind(&sf.loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let key = fresh_uuid(&pool).await;
    let err = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 5, Some(100),
        "fg", "2026-04-15", &key,
        Some(json!({ "lot_code": "LOT-H001" })),
    )
    .await
    .err()
    .expect("expected P0006");
    assert_eq!(err.as_database_error().unwrap().code().as_deref(), Some("P0006"));
}

// ============================================================
// L2.9: idempotent replay returns same doc_id, no duplicate lot.
// ============================================================

#[tokio::test]
async fn lot_adjustment_idempotent_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2I").await;
    let key = fresh_uuid(&pool).await;
    let metadata = json!({ "lot_code": "LOT-I001" });

    let d1 = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 6, Some(75),
        "raw", "2026-04-15", &key,
        Some(metadata.clone()),
    )
    .await
    .expect("first");
    let d2 = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 6, Some(75),
        "raw", "2026-04-15", &key,
        Some(metadata),
    )
    .await
    .expect("replay");
    assert_eq!(d1, d2);

    let lot_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sf.sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(lot_count, 1, "no duplicate lot on replay");

    // Ledger unchanged on replay.
    assert_eq!(balance(&pool, sf.qty_acct).await, 6);
    assert_eq!(balance(&pool, sf.val_acct).await, 450);
}

// ============================================================
// L2.10: -qty short on the named lot raises (P0006 from
// _lot_walk_layers OR 23514 from stock_available no-negative
// CHECK — either is acceptable rejection).
// ============================================================

#[tokio::test]
async fn lot_adjustment_out_residual_short_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold_lot(&pool, "L2J").await;
    let key_in = fresh_uuid(&pool).await;
    call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, 3, Some(100),
        "raw", "2026-04-15", &key_in,
        Some(json!({ "lot_code": "LOT-J001" })),
    )
    .await
    .expect("seed");

    let lot_id: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots WHERE lot_code = 'LOT-J001'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Try to deplete 5 from a lot that only has 3.
    let key = fresh_uuid(&pool).await;
    let err = call_adjust(
        &pool, &sf.sku_id, &sf.loc_id, -5, None,
        "raw", "2026-04-16", &key,
        Some(json!({ "lot_id": lot_id })),
    )
    .await
    .err()
    .expect("short lot must reject");
    let code = err
        .as_database_error()
        .and_then(|e| e.code().map(|c| c.into_owned()))
        .unwrap_or_default();
    assert!(
        code == "P0006" || code == "23514",
        "expected P0006 or 23514, got {code}"
    );
}
