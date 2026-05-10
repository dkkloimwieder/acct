//! T1 probes for `post_lot_transfer` (mig 0057, acct-fzzw).
//!
//! Covers:
//!   T1  pinned transfer (lot_id supplied) — single source lot
//!   T2  unpinned transfer (FIFO walk by receipt_date)
//!   T3  unpinned transfer + skus.allocation_strategy='fefo'
//!   T4  multi-lot walk (qty > one lot's residual; two dest rows)
//!   T5  cross-currency rejection (P0006/P0010)
//!   T6  same-from-and-to-location rejection (P0006/CHECK)
//!   T7  pinned-lot-short rejection (P0006)
//!   T8  idempotent replay returns same id
//!   T9  recon clean post-transfer (lot_residual_mismatch=0)
//!   T10 round-trip transfer (A→B then B→A)

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

#[allow(dead_code)]
struct Scaffold {
    sku_id: String,
    from_loc: String,
    to_loc: String,
    qty_from: i64,
    val_from: i64,
    qty_to: i64,
    val_to: i64,
    void_qty: i64,
    void_val: i64,
}

async fn id_text(pool: &PgPool, q: &str, bind: &str) -> String {
    sqlx::query_scalar(q).bind(bind).fetch_one(pool).await.unwrap()
}

async fn fresh_lot_sku(
    pool: &PgPool,
    code: &str,
    strategy: &str,
) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by, allocation_strategy)
         VALUES ($1, 'EA', 'lot_fifo'::cost_method,
                 'lot'::inventory_tracking, $2::allocation_strategy)
         RETURNING id::text",
    )
    .bind(code)
    .bind(strategy)
    .fetch_one(pool)
    .await
    .expect("insert lot SKU")
}

async fn open_value_account(
    pool: &PgPool,
    sku_id: &str,
    loc_id: &str,
    currency: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw'::account_kind, 'value'::ledger_kind, $3,
                 $1::UUID, $2::UUID, 'debit'::balance_direction)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_id)
    .bind(currency)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn open_qty_account(pool: &PgPool, sku_id: &str, loc_id: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available'::account_kind, 'qty'::ledger_kind,
                 $1::UUID, $2::UUID, 'debit'::balance_direction)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn scaffold(
    pool: &PgPool,
    label: &str,
    strategy: &str,
    currency_from: &str,
    currency_to: &str,
) -> Scaffold {
    let sku_code = format!("SKU-LXF-{label}");
    let sku = fresh_lot_sku(pool, &sku_code, strategy).await;
    let from_loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "MAIN").await;
    let to_loc = id_text(pool, "SELECT id::text FROM locations WHERE code = $1", "ALT").await;

    let qty_from = open_qty_account(pool, &sku, &from_loc).await;
    let val_from = open_value_account(pool, &sku, &from_loc, currency_from).await;
    let qty_to = open_qty_account(pool, &sku, &to_loc).await;
    let val_to = open_value_account(pool, &sku, &to_loc, currency_to).await;

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
        from_loc,
        to_loc,
        qty_from,
        val_from,
        qty_to,
        val_to,
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

/// Seed an inventory_lots row at FROM via post_inventory_adjustment.
async fn seed_lot(
    pool: &PgPool,
    sf: &Scaffold,
    qty: i64,
    unit_cost: i64,
    lot_code: &str,
    business_date: &str,
    expiration_date: Option<&str>,
) -> i64 {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    let mut meta = json!({ "lot_code": lot_code });
    if let Some(d) = expiration_date {
        meta["expiration_date"] = json!(d);
    }

    sqlx::query_scalar::<_, String>(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4, 'USD', 'raw',
            $5::DATE, $6::UUID, $7::UUID, NULL, $8
         )::text",
    )
    .bind(&sf.sku_id)
    .bind(&sf.from_loc)
    .bind(qty)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .bind(meta)
    .fetch_one(pool)
    .await
    .expect("seed lot via inventory_adjustment");

    sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
            AND lot_code = $3
          ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&sf.sku_id)
    .bind(&sf.from_loc)
    .bind(lot_code)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn call_transfer(
    pool: &PgPool,
    sf: &Scaffold,
    lines: serde_json::Value,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_lot_transfer($1::UUID, $2::UUID, $3, $4::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(&sf.from_loc)
    .bind(&sf.to_loc)
    .bind(lines)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

async fn lot_residual(pool: &PgPool, lot_id: i64) -> i64 {
    sqlx::query_scalar::<_, String>(
        "SELECT (il.original_quantity + COALESCE(
                  (SELECT SUM(quantity_change) FROM inventory_lot_events
                    WHERE lot_id = il.lot_id),
                  0))::TEXT
           FROM inventory_lots il
          WHERE il.lot_id = $1",
    )
    .bind(lot_id)
    .fetch_one(pool)
    .await
    .unwrap()
    .split('.')
    .next()
    .unwrap()
    .parse::<i64>()
    .unwrap()
}

// ============================================================
// T1: pinned transfer (specific lot_id) — single-source-lot transfer.
// ============================================================

#[tokio::test]
async fn pinned_transfer_single_lot() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T1", "fifo", "USD", "USD").await;
    let lot_id = seed_lot(&pool, &sf, 10, 100, "LOT-T1", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    let doc_id = call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 6, "lot_id": lot_id }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("transfer should succeed");

    // Ledger: qty_from -6, qty_to +6, val_from -600, val_to +600.
    assert_eq!(balance(&pool, sf.qty_from).await, 4); // 10 seeded - 6 transferred
    assert_eq!(balance(&pool, sf.qty_to).await, 6);
    assert_eq!(balance(&pool, sf.val_from).await, 400);
    assert_eq!(balance(&pool, sf.val_to).await, 600);

    // Source lot residual decremented.
    assert_eq!(lot_residual(&pool, lot_id).await, 4);

    // New dest lot row at TO with copied lot_code.
    let dest_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sf.sku_id)
    .bind(&sf.to_loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dest_count, 1);

    // adjust_out event on source lot.
    let evt: (i16, String) = sqlx::query_as(
        "SELECT event_type, quantity_change::TEXT FROM inventory_lot_events
          WHERE lot_id = $1",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(evt.0, 8);
    assert!(evt.1.starts_with("-6"), "got {}", evt.1);

    // lot_transfer_lines audit fields stamped.
    let line_audit: (String, String) = sqlx::query_as(
        "SELECT total_amount::TEXT, unit_cost::TEXT
           FROM lot_transfer_lines WHERE transfer_id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(line_audit.0, "600");
    assert!(line_audit.1.starts_with("100"), "got {}", line_audit.1);
}

// ============================================================
// T2: unpinned FIFO walk (default strategy='fifo').
// ============================================================

#[tokio::test]
async fn unpinned_fifo_walk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T2", "fifo", "USD", "USD").await;
    let lot_a = seed_lot(&pool, &sf, 5, 100, "LOT-T2-A", "2026-04-05", None).await;
    let lot_b = seed_lot(&pool, &sf, 5, 200, "LOT-T2-B", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 4 }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("transfer should succeed");

    // FIFO: lot_a (earlier receipt_date) drained first.
    assert_eq!(lot_residual(&pool, lot_a).await, 1);
    assert_eq!(lot_residual(&pool, lot_b).await, 5);

    // 5*100 + 5*200 = 1500 seeded; 4*100 = 400 transferred → val_from = 1100,
    // val_to = 400.
    assert_eq!(balance(&pool, sf.val_from).await, 1100);
    assert_eq!(balance(&pool, sf.val_to).await, 400);
}

// ============================================================
// T3: unpinned FEFO walk (allocation_strategy='fefo').
// ============================================================

#[tokio::test]
async fn unpinned_fefo_walk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T3", "fefo", "USD", "USD").await;
    // lot_a receives FIRST (older receipt) but expires LATER.
    // lot_b receives SECOND but expires SOONER → FEFO drains lot_b first.
    let lot_a = seed_lot(&pool, &sf, 5, 100, "LOT-T3-A", "2026-04-05", Some("2027-12-31")).await;
    let lot_b = seed_lot(&pool, &sf, 5, 200, "LOT-T3-B", "2026-04-10", Some("2026-08-01")).await;

    let key = fresh_uuid(&pool).await;
    call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 3 }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("FEFO transfer should succeed");

    // FEFO: lot_b (earlier expiry) drained first.
    assert_eq!(lot_residual(&pool, lot_a).await, 5);
    assert_eq!(lot_residual(&pool, lot_b).await, 2);
}

// ============================================================
// T4: multi-lot walk (qty exceeds one lot's residual).
// ============================================================

#[tokio::test]
async fn multi_lot_walk_creates_two_dest_rows() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T4", "fifo", "USD", "USD").await;
    let lot_a = seed_lot(&pool, &sf, 4, 100, "LOT-T4-A", "2026-04-05", None).await;
    let lot_b = seed_lot(&pool, &sf, 6, 200, "LOT-T4-B", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 7 }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("multi-lot walk should succeed");

    // FIFO: lot_a fully drained (4), lot_b drained 3.
    assert_eq!(lot_residual(&pool, lot_a).await, 0);
    assert_eq!(lot_residual(&pool, lot_b).await, 3);

    // Two dest rows at TO (one per consumed source lot).
    let dest_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sf.sku_id)
    .bind(&sf.to_loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dest_count, 2);

    // Two adjust_out events (one per consumed source lot).
    let evt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_lot_events
          WHERE event_type = 8 AND lot_id IN ($1, $2)",
    )
    .bind(lot_a)
    .bind(lot_b)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(evt_count, 2);

    // Ledger value: 4*100 + 3*200 = 1000 transferred.
    assert_eq!(balance(&pool, sf.val_to).await, 1000);
}

// ============================================================
// T5: cross-currency rejection (FROM=USD, TO=EUR → P0010).
// ============================================================

#[tokio::test]
async fn cross_currency_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T5", "fifo", "USD", "EUR").await;
    let _lot = seed_lot(&pool, &sf, 5, 100, "LOT-T5", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    let err = call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 3 }]),
        "2026-04-15",
        &key,
    )
    .await
    .err()
    .expect("expected currency rejection");
    let code = err.as_database_error().unwrap().code().unwrap_or_default().to_string();
    assert_eq!(code, "P0010", "got {code}");
}

// ============================================================
// T6: same FROM and TO location rejected.
// ============================================================

#[tokio::test]
async fn same_location_rejected() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T6", "fifo", "USD", "USD").await;
    let _lot = seed_lot(&pool, &sf, 5, 100, "LOT-T6", "2026-04-10", None).await;

    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    let err = sqlx::query_scalar::<_, String>(
        "SELECT post_lot_transfer($1::UUID, $2::UUID, $3, $4::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(&sf.from_loc)
    .bind(&sf.from_loc) // same loc both sides
    .bind(json!([{ "sku_id": sf.sku_id, "qty": 3 }]))
    .bind("2026-04-15")
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(&pool)
    .await
    .err()
    .expect("expected same-loc rejection");
    let code = err.as_database_error().unwrap().code().unwrap_or_default().to_string();
    assert_eq!(code, "P0006", "got {code}");
}

// ============================================================
// T7: pinned-lot residual short raises P0006.
// ============================================================

#[tokio::test]
async fn pinned_lot_short_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T7", "fifo", "USD", "USD").await;
    let lot_id = seed_lot(&pool, &sf, 3, 100, "LOT-T7", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    let err = call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 5, "lot_id": lot_id }]),
        "2026-04-15",
        &key,
    )
    .await
    .err()
    .expect("expected residual_short");
    let code = err.as_database_error().unwrap().code().unwrap_or_default().to_string();
    assert_eq!(code, "P0006", "got {code}");
}

// ============================================================
// T8: idempotent replay returns same id without writing twice.
// ============================================================

#[tokio::test]
async fn idempotent_replay() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T8", "fifo", "USD", "USD").await;
    let lot_id = seed_lot(&pool, &sf, 5, 100, "LOT-T8", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    let first = call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 3, "lot_id": lot_id }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("first call");
    let second = call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 3, "lot_id": lot_id }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("replay should succeed");
    assert_eq!(first, second);

    // Ledger unchanged on second call.
    assert_eq!(balance(&pool, sf.val_to).await, 300);
    assert_eq!(lot_residual(&pool, lot_id).await, 2);

    // Only one adjust_out event total.
    let evt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_lot_events
          WHERE event_type = 8 AND lot_id = $1",
    )
    .bind(lot_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(evt_count, 1);
}

// ============================================================
// T9: recon checks pass (lot_residual_mismatch=0,
// lot_negative_residual=0).
// ============================================================

#[tokio::test]
async fn recon_clean_after_transfer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T9", "fifo", "USD", "USD").await;
    let _a = seed_lot(&pool, &sf, 5, 100, "LOT-T9-A", "2026-04-05", None).await;
    let _b = seed_lot(&pool, &sf, 5, 200, "LOT-T9-B", "2026-04-10", None).await;

    let key = fresh_uuid(&pool).await;
    call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 6 }]),
        "2026-04-15",
        &key,
    )
    .await
    .expect("transfer ok");

    let alerts: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(alerts, 0, "expected zero alerts after lot_transfer");
}

// ============================================================
// T10: round-trip (A→B then B→A) leaves balances + residuals
// in expected state.
// ============================================================

#[tokio::test]
async fn round_trip_transfer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sf = scaffold(&pool, "T10", "fifo", "USD", "USD").await;
    let lot_id = seed_lot(&pool, &sf, 10, 100, "LOT-T10", "2026-04-05", None).await;

    // First transfer: A → B (qty 6).
    let key1 = fresh_uuid(&pool).await;
    call_transfer(
        &pool,
        &sf,
        json!([{ "sku_id": sf.sku_id, "qty": 6, "lot_id": lot_id }]),
        "2026-04-15",
        &key1,
    )
    .await
    .expect("A->B ok");

    // Find dest lot at TO.
    let dest_lot: i64 = sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND location_id = $2::UUID
          ORDER BY created_at DESC LIMIT 1",
    )
    .bind(&sf.sku_id)
    .bind(&sf.to_loc)
    .fetch_one(&pool)
    .await
    .unwrap();

    // Reverse transfer: B → A (qty 6) using the dest lot.
    // Build a "reverse scaffold" that swaps from/to.
    let posted_by = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    sqlx::query_scalar::<_, String>(
        "SELECT post_lot_transfer($1::UUID, $2::UUID, $3, $4::DATE, $5::UUID, $6::UUID, NULL)::text",
    )
    .bind(&sf.to_loc)
    .bind(&sf.from_loc)
    .bind(json!([{ "sku_id": sf.sku_id, "qty": 6, "lot_id": dest_lot }]))
    .bind("2026-04-16")
    .bind(&posted_by)
    .bind(&key2)
    .fetch_one(&pool)
    .await
    .expect("B->A ok");

    // Net qty balances back to original distribution at FROM (10), zero at TO.
    assert_eq!(balance(&pool, sf.qty_from).await, 10);
    assert_eq!(balance(&pool, sf.qty_to).await, 0);

    // Original lot at A: 4 retained + 6 returned = 10? No — the returned 6
    // is a NEW lot row at A (receipt_date=2026-04-16), not a refund to
    // the original lot. Original residual stays at 10-6=4; the returned 6
    // becomes a fresh lot row.
    assert_eq!(lot_residual(&pool, lot_id).await, 4);

    // Three lots total: original at A (residual 4), dest at B (residual 0),
    // re-shipped lot at A (residual 6).
    let total_lots: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_lots WHERE product_id = $1::UUID",
    )
    .bind(&sf.sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(total_lots, 3);

    // Recon clean.
    let alerts: i32 = sqlx::query_scalar("SELECT run_daily_reconciliation()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(alerts, 0);

    // Suppress unused warnings.
    let _ = sf.void_qty;
    let _ = sf.void_val;
}
