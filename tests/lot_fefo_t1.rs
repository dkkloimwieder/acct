//! T1 probes for FEFO allocator (mig 0054, acct-mxll). Pins
//! the per-SKU `allocation_strategy` enum + branched ORDER BY in
//! `_lot_walk_layers`. FIFO baseline preserved for back-compat;
//! FEFO walks by expiration_date ASC NULLS LAST, lot_id ASC.
//! Specific lot pin bypasses the strategy.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

const USD_CV: &str = "creation_void";

async fn fresh_lot_sku_with_strategy(
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

async fn open_lot_accounts(pool: &PgPool, sku_id: &str) -> (i64, i64) {
    let loc_id: String = sqlx::query_scalar(
        "SELECT id::text FROM locations WHERE code = 'MAIN'",
    )
    .fetch_one(pool)
    .await
    .unwrap();

    let stock: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         VALUES ('stock_available'::account_kind, 'qty'::ledger_kind,
                 $1::UUID, $2::UUID, 'debit'::balance_direction)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(&loc_id)
    .fetch_one(pool)
    .await
    .unwrap();

    let val: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw'::account_kind, 'value'::ledger_kind, 'USD',
                 $1::UUID, $2::UUID, 'debit'::balance_direction)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(&loc_id)
    .fetch_one(pool)
    .await
    .unwrap();

    (stock, val)
}

fn lot_receipt_event(
    debit_value: i64,
    credit_void_value: i64,
    qty: i64,
    amount: i64,
    business_date: &str,
    idempotency_key: &str,
    lot_code: &str,
    expiration_date: Option<&str>,
) -> serde_json::Value {
    let mut ev = json!({
        "reason":            "cycle_count_adj",
        "document_kind":     "lot_receipt",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  debit_value,
        "credit_account_id": credit_void_value,
        "amount":            amount,
        "qty":               qty,
        "business_date":     business_date,
        "idempotency_key":   idempotency_key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
        "lot_code":          lot_code,
    });
    if let Some(exp) = expiration_date {
        ev["expiration_date"] = json!(exp);
    }
    ev
}

fn lot_issue_event(
    debit_cogs: i64,
    credit_value: i64,
    qty: i64,
    business_date: &str,
    idempotency_key: &str,
    specific_lot_id: Option<i64>,
) -> serde_json::Value {
    let mut ev = json!({
        "reason":            "so_ship",
        "document_kind":     "lot_issue",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  debit_cogs,
        "credit_account_id": credit_value,
        "qty":               qty,
        "business_date":     business_date,
        "idempotency_key":   idempotency_key,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    if let Some(lot_id) = specific_lot_id {
        ev["lot_id"] = json!(lot_id);
    }
    ev
}

async fn cogs_usd(pool: &PgPool) -> i64 {
    account_id_by_kind_currency(pool, "cogs", Some("USD")).await
}

async fn lot_id_by_code(pool: &PgPool, sku_id: &str, lot_code: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lots
          WHERE product_id = $1::UUID AND lot_code = $2",
    )
    .bind(sku_id)
    .bind(lot_code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn first_consumed_lot_id(pool: &PgPool, idempotency_key: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lot_events e
           JOIN posting_lines pl ON pl.id = e.posting_line_id
          WHERE pl.idempotency_key = $1::UUID
            AND e.event_type = 1
          ORDER BY e.event_id ASC
          LIMIT 1",
    )
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn consumed_lot_ids_in_order(
    pool: &PgPool,
    idempotency_key: &str,
) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT lot_id FROM inventory_lot_events e
           JOIN posting_lines pl ON pl.id = e.posting_line_id
          WHERE pl.idempotency_key = $1::UUID
            AND e.event_type = 1
          ORDER BY e.event_id ASC",
    )
    .bind(idempotency_key)
    .fetch_all(pool)
    .await
    .unwrap()
}

// ============================================================
// F1 — FEFO walks earliest-expiring lot first (not earliest receipt)
// ============================================================

#[tokio::test]
async fn fefo_walks_earliest_expiry_not_earliest_receipt() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-FEFO-1", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Lot A: received 2026-04-01, expires 2027-12-01 (later expiry).
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 5_00, "2026-04-01", &key_a,
            "LOT-A", Some("2027-12-01"),
        )]),
        false,
    ).await.unwrap();

    // Lot B: received LATER 2026-04-10, but expires SOONER 2026-12-01.
    let key_b = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 10_00, "2026-04-10", &key_b,
            "LOT-B", Some("2026-12-01"),
        )]),
        false,
    ).await.unwrap();

    let lot_a_id = lot_id_by_code(&pool, &sku_id, "LOT-A").await;
    let lot_b_id = lot_id_by_code(&pool, &sku_id, "LOT-B").await;

    // Issue 30. FEFO must drain LOT-B first (earlier expiry) even
    // though LOT-A was received earlier.
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(void_value, val, 30, "2026-04-15", &key_i, None);
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    let consumed = consumed_lot_ids_in_order(&pool, &key_i).await;
    assert_eq!(consumed, vec![lot_b_id], "FEFO consumes LOT-B (earlier expiry) first");
    assert_ne!(consumed[0], lot_a_id);

    // Posting amount: 30 @ LOT-B's unit_cost (10_00 / 50 = 20c) = 600
    let amt: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key_i)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(amt, 6_00, "30 @ LOT-B's 20c = 6_00");
}

// ============================================================
// F2 — FEFO with NULL expiry: NULL sorts LAST
// ============================================================

#[tokio::test]
async fn fefo_null_expiry_sorts_last() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-FEFO-2", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Lot A: received first, NULL expiry (unexpiring stock — reserve).
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 5_00, "2026-04-01", &key_a, "LOT-A", None,
        )]),
        false,
    ).await.unwrap();

    // Lot B: received later but has expiry — must drain first.
    let key_b = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 10_00, "2026-04-10", &key_b,
            "LOT-B", Some("2027-06-01"),
        )]),
        false,
    ).await.unwrap();

    let lot_a_id = lot_id_by_code(&pool, &sku_id, "LOT-A").await;
    let lot_b_id = lot_id_by_code(&pool, &sku_id, "LOT-B").await;

    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(void_value, val, 30, "2026-04-15", &key_i, None);
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    let consumed = consumed_lot_ids_in_order(&pool, &key_i).await;
    assert_eq!(consumed, vec![lot_b_id],
               "FEFO drains expiring lot first; NULL-expiry lot held in reserve");
    assert_ne!(consumed[0], lot_a_id);

    // Issue 60 more — drains remaining LOT-B (20) + spills into LOT-A (40).
    let key_i2 = fresh_uuid(&pool).await;
    let ev_i2 = lot_issue_event(void_value, val, 60, "2026-04-16", &key_i2, None);
    call_post_posting_lines(&pool, json!([ev_i2]), false).await.unwrap();

    let consumed2 = consumed_lot_ids_in_order(&pool, &key_i2).await;
    assert_eq!(consumed2, vec![lot_b_id, lot_a_id],
               "FEFO splits: rest of expiring B then spills into NULL-expiry A");
}

// ============================================================
// F3 — FEFO with all NULL expiry: falls back to lot_id ASC
// ============================================================

#[tokio::test]
async fn fefo_all_null_expiry_falls_back_to_lot_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-FEFO-3", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // All 3 lots receive with NULL expiry.
    for (i, code) in ["LOT-X", "LOT-Y", "LOT-Z"].iter().enumerate() {
        let key = fresh_uuid(&pool).await;
        let bd = format!("2026-04-{:02}", 10 + i);
        call_post_posting_lines(
            &pool,
            json!([lot_receipt_event(
                val, void_value, 30, 3_00, &bd, &key, code, None,
            )]),
            false,
        ).await.unwrap();
    }

    let lot_x_id = lot_id_by_code(&pool, &sku_id, "LOT-X").await;
    let lot_y_id = lot_id_by_code(&pool, &sku_id, "LOT-Y").await;

    // Issue 30 — all NULL expiries are tied; lot_id ASC tie-break → LOT-X.
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(void_value, val, 30, "2026-04-20", &key_i, None);
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    assert!(lot_x_id < lot_y_id, "X was created before Y");
    let consumed = consumed_lot_ids_in_order(&pool, &key_i).await;
    assert_eq!(consumed, vec![lot_x_id],
               "All NULL expiry → lot_id ASC tie-break selects lowest id (LOT-X)");
}

// ============================================================
// F4 — FIFO SKU (default): existing receipt_date walk preserved
// ============================================================

#[tokio::test]
async fn fifo_default_strategy_walks_by_receipt_date() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Default strategy is 'fifo' — no explicit setting needed, but be explicit
    // here to document the contract.
    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-FIFO-4", "fifo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Lot A: received earlier, expires LATER.
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 5_00, "2026-04-01", &key_a,
            "LOT-A", Some("2027-12-01"),
        )]),
        false,
    ).await.unwrap();

    // Lot B: received later, expires SOONER (would FEFO first if FEFO).
    let key_b = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 10_00, "2026-04-10", &key_b,
            "LOT-B", Some("2026-12-01"),
        )]),
        false,
    ).await.unwrap();

    let lot_a_id = lot_id_by_code(&pool, &sku_id, "LOT-A").await;
    let lot_b_id = lot_id_by_code(&pool, &sku_id, "LOT-B").await;

    // Issue 30 — FIFO must drain LOT-A first (earlier receipt, despite later expiry).
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(void_value, val, 30, "2026-04-15", &key_i, None);
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    let consumed = consumed_lot_ids_in_order(&pool, &key_i).await;
    assert_eq!(consumed, vec![lot_a_id],
               "FIFO consumes LOT-A (earlier receipt) first, ignoring expiry");
    assert_ne!(consumed[0], lot_b_id);
}

// ============================================================
// F5 — Default 'fifo' for SKUs created without explicit strategy
// ============================================================

#[tokio::test]
async fn default_strategy_is_fifo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Insert SKU WITHOUT specifying allocation_strategy — should default to 'fifo'.
    let sku_id: String = sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method, tracked_by)
         VALUES ('SKU-DEFAULT', 'EA', 'lot_fifo'::cost_method,
                 'lot'::inventory_tracking)
         RETURNING id::text",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let strategy: String = sqlx::query_scalar(
        "SELECT allocation_strategy::text FROM skus WHERE id = $1::UUID",
    )
    .bind(&sku_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(strategy, "fifo", "default allocation_strategy is 'fifo'");
}

// ============================================================
// F6 — Mid-life strategy change shifts walk on next issue
// ============================================================

#[tokio::test]
async fn strategy_change_mid_life_shifts_walk_on_next_issue() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-MID", "fifo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Lot A: received first, expires LATER.
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 5_00, "2026-04-01", &key_a,
            "LOT-A", Some("2027-12-01"),
        )]),
        false,
    ).await.unwrap();

    // Lot B: received later, expires SOONER.
    let key_b = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 10_00, "2026-04-10", &key_b,
            "LOT-B", Some("2026-12-01"),
        )]),
        false,
    ).await.unwrap();

    let lot_a_id = lot_id_by_code(&pool, &sku_id, "LOT-A").await;
    let lot_b_id = lot_id_by_code(&pool, &sku_id, "LOT-B").await;

    // First issue under FIFO — drains LOT-A.
    let key_i1 = fresh_uuid(&pool).await;
    let ev_i1 = lot_issue_event(void_value, val, 20, "2026-04-12", &key_i1, None);
    call_post_posting_lines(&pool, json!([ev_i1]), false).await.unwrap();
    assert_eq!(first_consumed_lot_id(&pool, &key_i1).await, lot_a_id,
               "first issue under FIFO drains LOT-A");

    // Switch SKU to FEFO.
    sqlx::query("UPDATE skus SET allocation_strategy = 'fefo' WHERE id = $1::UUID")
        .bind(&sku_id)
        .execute(&pool)
        .await
        .unwrap();

    // Next issue must drain LOT-B (earlier expiry).
    let key_i2 = fresh_uuid(&pool).await;
    let ev_i2 = lot_issue_event(void_value, val, 20, "2026-04-13", &key_i2, None);
    call_post_posting_lines(&pool, json!([ev_i2]), false).await.unwrap();
    assert_eq!(first_consumed_lot_id(&pool, &key_i2).await, lot_b_id,
               "after FEFO switch, next issue drains LOT-B (earlier expiry)");
}

// ============================================================
// F7 — Specific lot pin overrides FEFO strategy
// ============================================================

#[tokio::test]
async fn specific_lot_pin_overrides_fefo() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-PIN", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let cogs = cogs_usd(&pool).await;

    // Lot A: later expiry. Lot B: earlier expiry (would be FEFO target).
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 5_00, "2026-04-01", &key_a,
            "LOT-A", Some("2027-12-01"),
        )]),
        false,
    ).await.unwrap();
    let key_b = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 50, 10_00, "2026-04-10", &key_b,
            "LOT-B", Some("2026-12-01"),
        )]),
        false,
    ).await.unwrap();

    let lot_a_id = lot_id_by_code(&pool, &sku_id, "LOT-A").await;
    let lot_b_id = lot_id_by_code(&pool, &sku_id, "LOT-B").await;

    // Pin to LOT-A explicitly — must drain LOT-A despite FEFO strategy.
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(cogs, val, 20, "2026-04-15", &key_i, Some(lot_a_id));
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    let consumed = consumed_lot_ids_in_order(&pool, &key_i).await;
    assert_eq!(consumed, vec![lot_a_id],
               "specific lot pin overrides FEFO; LOT-A consumed");
    assert_ne!(consumed[0], lot_b_id);

    // Posting amount: 20 @ LOT-A's 10c = 2_00.
    let amt: i64 = sqlx::query_scalar(
        "SELECT amount FROM posting_lines WHERE idempotency_key = $1::UUID",
    )
    .bind(&key_i)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(amt, 2_00, "20 @ LOT-A's 10c = 2_00");
}

// ============================================================
// F8 — FEFO with tied expiry: lot_id ASC tie-break is deterministic
// ============================================================

#[tokio::test]
async fn fefo_tied_expiry_tiebreaks_by_lot_id() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-TIE", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Three lots, identical expiry, staggered receipt order.
    for (i, code) in ["LOT-FIRST", "LOT-SECOND", "LOT-THIRD"].iter().enumerate() {
        let key = fresh_uuid(&pool).await;
        let bd = format!("2026-04-{:02}", 5 + i);
        call_post_posting_lines(
            &pool,
            json!([lot_receipt_event(
                val, void_value, 25, 2_50, &bd, &key, code, Some("2027-01-01"),
            )]),
            false,
        ).await.unwrap();
    }

    let lot_first_id = lot_id_by_code(&pool, &sku_id, "LOT-FIRST").await;
    let lot_second_id = lot_id_by_code(&pool, &sku_id, "LOT-SECOND").await;
    assert!(lot_first_id < lot_second_id);

    // Issue 25 — tied expiry, lot_id ASC selects LOT-FIRST.
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(void_value, val, 25, "2026-04-15", &key_i, None);
    call_post_posting_lines(&pool, json!([ev_i]), false).await.unwrap();

    let consumed = consumed_lot_ids_in_order(&pool, &key_i).await;
    assert_eq!(consumed, vec![lot_first_id],
               "tied expiry → lot_id ASC tie-break selects lowest id");
}

// ============================================================
// F9 — Mixed FEFO + explicit pin via reservation lot_specific
// ============================================================
//
// Confirms that a pinned reservation works regardless of the
// SKU's allocation_strategy. (E2.5-followup wires reserve →
// allocate → ship; the strategy only affects unpinned walks.)

#[tokio::test]
async fn fefo_with_specific_lot_short_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-SHORT", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // Lot A: 30 received.
    let key_a = fresh_uuid(&pool).await;
    call_post_posting_lines(
        &pool,
        json!([lot_receipt_event(
            val, void_value, 30, 3_00, "2026-04-01", &key_a,
            "LOT-A", Some("2027-01-01"),
        )]),
        false,
    ).await.unwrap();

    let lot_a_id = lot_id_by_code(&pool, &sku_id, "LOT-A").await;

    // Pin to LOT-A and request 50 — short by 20; raises P0006 with lot_residual_short
    // message (specific-pin code path).
    let key_i = fresh_uuid(&pool).await;
    let ev_i = lot_issue_event(void_value, val, 50, "2026-04-10", &key_i, Some(lot_a_id));
    let err = call_post_posting_lines(&pool, json!([ev_i]), false)
        .await
        .unwrap_err();
    let dberr = err.as_database_error().unwrap();
    let code = dberr.code().unwrap().to_string();
    assert_eq!(code, "P0006");
    let msg = dberr.message();
    // The error path could be either lot_residual_short (specific-pin walk) or
    // the qty-leg's no-negative CHECK (23514). For specific-pin requests, the
    // ledger amount is computed first; lot_residual_short fires from the walk.
    // If the qty-leg fires first, message will mention the CHECK.
    assert!(
        msg.contains("lot_residual_short") || msg.contains("stock"),
        "expected lot_residual_short or stock-related rejection, got: {}", msg,
    );
}

// ============================================================
// F10 — FEFO walks deterministically across multi-receipt + multi-issue
// ============================================================
//
// End-to-end smoke: 4 receipts (3 expiring + 1 NULL) + 3 issues
// drain LOT-Y → LOT-X in strict FEFO order; LOT-W (later expiry)
// and LOT-NULL (no expiry) stay untouched.

#[tokio::test]
async fn fefo_walks_deterministically_across_multi_issue() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = fresh_lot_sku_with_strategy(&pool, "SKU-MULTI", "fefo").await;
    let (_stock, val) = open_lot_accounts(&pool, &sku_id).await;
    let void_value = account_id_by_kind_currency(&pool, USD_CV, Some("USD")).await;
    let _cogs = cogs_usd(&pool).await;

    // 3 lots with staggered expiry + 1 NULL.
    // Each lot: 40 qty, $4 = 400 cents → unit_cost 10c.
    let lots = [
        ("LOT-W",    "2026-04-01", Some("2027-09-01")),
        ("LOT-X",    "2026-04-03", Some("2027-03-01")),
        ("LOT-Y",    "2026-04-05", Some("2026-11-01")),
        ("LOT-NULL", "2026-04-07", None),
    ];
    for (code, recv_d, exp_d) in lots.iter() {
        let key = fresh_uuid(&pool).await;
        call_post_posting_lines(
            &pool,
            json!([lot_receipt_event(
                val, void_value, 40, 4_00, recv_d, &key, code, *exp_d,
            )]),
            false,
        ).await.unwrap();
    }

    // 3 issues. FEFO order: LOT-Y (Nov 2026) drains first.
    //   issue 1: 20 → all from LOT-Y (residual 20 left)
    //   issue 2: 35 → 20 from LOT-Y (drains it), 15 from LOT-X
    //   issue 3: 15 → all from LOT-X (residual 10 left in LOT-X)
    for (qty, bd) in [(20, "2026-04-10"), (35, "2026-04-12"), (15, "2026-04-14")].iter() {
        let key = fresh_uuid(&pool).await;
        let ev = lot_issue_event(void_value, val, *qty, bd, &key, None);
        call_post_posting_lines(&pool, json!([ev]), false).await.unwrap();
    }

    // Per-lot residuals via the helper.
    let residual = |lot_code: &'static str| {
        let pool = pool.clone();
        let sku_id = sku_id.clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT _inventory_lot_remaining_qty(il.lot_id, il.receipt_date)::TEXT
                   FROM inventory_lots il
                  WHERE il.product_id = $1::UUID AND il.lot_code = $2",
            )
            .bind(&sku_id)
            .bind(lot_code)
            .fetch_one(&pool).await.unwrap()
            .parse::<f64>().unwrap() as i64
        }
    };

    assert_eq!(residual("LOT-Y").await, 0,
               "FEFO drained earliest-expiry LOT-Y entirely");
    assert_eq!(residual("LOT-X").await, 10,
               "Mar-2027 expiry LOT-X partially drained (15+10=25 of 40)");
    assert_eq!(residual("LOT-W").await, 40,
               "Sep-2027 expiry LOT-W untouched");
    assert_eq!(residual("LOT-NULL").await, 40,
               "NULL-expiry LOT-NULL held in reserve, untouched");

    // I1-I7 ledger invariants hold across the whole workload.
    assert_invariants_hold(&pool, "fefo_multi_issue").await;
}
