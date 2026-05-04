//! acct-fii — post_cost_adjustment uses per-class signed qty SUM on the
//! value pool, not stock_available qty (which is cross-class). This file
//! exercises the bug fix extensively across configurations:
//!
//!   1. Same-location raw + fg core regression (pre-fix would over-adjust)
//!   2. Same-location with cross-class qty drift seeded after each adjust
//!   3. Same-location with multi-currency raw + fg
//!   4. Distinct-location baseline (should still work; gate non-restrictive)
//!   5. Empty class pool: P0010 raises (per-class qty == 0)
//!   6. Non-empty class with empty other class (qty entirely raw, fg=0)
//!   7. cost_adjust on raw doesn't poison fg's running avg and vice versa
//!   8. Two adjustments back-to-back same class on shared location
//!   9. Adjustment after WO consumption from raw (rm_issue_to_wo via wac)
//!  10. Adjustment after PO receipt at same location (raw class only)
//!  11. Idempotency replay returns same doc id, no double-post
//!  12. delta = 0 path (target == prior) — audit row only, no transfer
//!  13. Concurrent classes with different signs (raw write-up + fg write-down)
//!  14. Exact-cost case (no truncation): pool ends at target × qty
//!  15. Audit row records the per-class qty (not cross-class)

mod common;

use common::*;
use sqlx::PgPool;
use serde_json::json;

// ============================================================
// Local scaffolding
// ============================================================

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
    .unwrap_or_else(|e| panic!("open {kind}: {e}"))
}

async fn balance(pool: &PgPool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("balance")
}

/// Per-class signed qty SUM (matches the divisor the function now uses).
async fn class_qty(pool: &PgPool, val_acct: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(CASE
                  WHEN t.debit_account_id  = $1 THEN  t.qty
                  WHEN t.credit_account_id = $1 THEN -t.qty
                END), 0)::BIGINT
           FROM transfers t
          WHERE $1 IN (t.debit_account_id, t.credit_account_id)
            AND t.qty IS NOT NULL",
    )
    .bind(val_acct)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Seed a class-specific pool by 2-leg cycle_count_adj batch
/// (qty side via stock_available, value side via inv_value_<class>).
async fn seed_class(
    pool: &PgPool,
    qty_acct: i64,
    val_acct: i64,
    qty: i64,
    value: i64,
    business_date: &str,
) {
    let posted_by = fresh_uuid(pool).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let val_currency: String = sqlx::query_scalar("SELECT currency FROM accounts WHERE id = $1")
        .bind(val_acct)
        .fetch_one(pool)
        .await
        .unwrap();
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some(&val_currency)).await;
    let doc_id = fresh_uuid(pool).await;
    let events = json!([
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": qty_acct, "credit_account_id": void_qty,
          "amount": qty, "qty": qty, "business_date": business_date,
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
        { "reason": "cycle_count_adj", "document_kind": "seed", "document_id": doc_id,
          "debit_account_id": val_acct, "credit_account_id": void_val,
          "amount": value, "qty": qty, "business_date": business_date,
          "idempotency_key": fresh_uuid(pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("seed_class: {e}"));
}

async fn cost_adjust(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    currency: &str,
    class: &str,
    target: i64,
    business_date: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment(
            $1::UUID, $2::UUID, $3, $4, $5, $6::DATE, $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(currency)
    .bind(class)
    .bind(target)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

async fn cost_adjust_with_key(
    pool: &PgPool,
    sku: &str,
    loc: &str,
    currency: &str,
    class: &str,
    target: i64,
    business_date: &str,
    key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_cost_adjustment(
            $1::UUID, $2::UUID, $3, $4, $5, $6::DATE, $7::UUID, $8::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(currency)
    .bind(class)
    .bind(target)
    .bind(business_date)
    .bind(&posted_by)
    .bind(key)
    .fetch_one(pool)
    .await
}

// ============================================================
// Tests
// ============================================================

/// Core regression: SKU X at loc L has 100 raw @ $10 + 50 fg @ $20.
/// stock_available(X, L) = 150 (combined). post_cost_adjustment on
/// raw to target=$12 should set raw avg = $12 (not $18 over-shoot
/// from cross-class qty pollution).
#[tokio::test(flavor = "multi_thread")]
async fn same_location_raw_and_fg_cost_adjust_raw_only() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "SHARED1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "SHARED1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    // Seed: 100 raw @ $10. Both qty side (stock_available) and value side (raw).
    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    // Seed: 50 fg @ $20. Same qty account (stock_available accumulates).
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    // Confirm shared qty pool, separate value pools.
    assert_eq!(balance(&pool, qty_acct).await, 150, "stock_available combined");
    assert_eq!(balance(&pool, raw_v).await, 1000);
    assert_eq!(balance(&pool, fg_v).await, 1000);

    // Per-class qty: raw=100, fg=50 (signed SUM on each value pool's transfers).
    assert_eq!(class_qty(&pool, raw_v).await, 100, "raw class qty");
    assert_eq!(class_qty(&pool, fg_v).await, 50, "fg class qty");

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await
        .expect("cost_adjust raw");

    // Pre-fix: would post delta = 12*150 - 1000 = $800 → raw=$1800.
    // Post-fix: delta = 12*100 - 1000 = $200 → raw=$1200.
    assert_eq!(balance(&pool, raw_v).await, 1200, "raw at target * raw_qty (no over-shoot)");
    assert_eq!(balance(&pool, fg_v).await, 1000, "fg pool untouched");

    // Audit row records per-class qty (= 100), not cross-class (= 150).
    let audit_qty: i64 = sqlx::query_scalar(
        "SELECT pool_qty FROM inventory_cost_adjustments
          WHERE sku_id = $1::UUID AND inventory_class = 'raw'
          ORDER BY posted_at DESC LIMIT 1",
    )
    .bind(&sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_qty, 100, "audit pool_qty = per-class qty");
}

/// Symmetric: cost_adjust on fg side doesn't poison raw.
#[tokio::test(flavor = "multi_thread")]
async fn same_location_raw_and_fg_cost_adjust_fg_only() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "SHARED2", "wac_perpetual").await;
    let loc = fresh_location(&pool, "SHARED2-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    cost_adjust(&pool, &sku, &loc, "USD", "fg", 25, "2026-04-15").await
        .expect("cost_adjust fg");

    // delta = 25*50 - 1000 = $250 → fg=$1250. Raw untouched.
    assert_eq!(balance(&pool, fg_v).await, 1250);
    assert_eq!(balance(&pool, raw_v).await, 1000);
}

/// Both classes adjusted in sequence; each adjusts its own class only.
#[tokio::test(flavor = "multi_thread")]
async fn same_location_both_classes_independently_adjusted() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "SHARED3", "wac_perpetual").await;
    let loc = fresh_location(&pool, "SHARED3-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await; // raw $10
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;   // fg $20

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 14, "2026-04-15").await.unwrap();
    // raw: 14*100 - 1000 = $400 → raw=$1400.
    assert_eq!(balance(&pool, raw_v).await, 1400);
    assert_eq!(balance(&pool, fg_v).await, 1000, "fg untouched after raw adjust");

    cost_adjust(&pool, &sku, &loc, "USD", "fg", 18, "2026-04-15").await.unwrap();
    // fg: 18*50 - 1000 = -$100 (write-down) → fg=$900.
    assert_eq!(balance(&pool, fg_v).await, 900);
    assert_eq!(balance(&pool, raw_v).await, 1400, "raw untouched after fg adjust");
}

/// Class drift after PO-style receipt on raw (no fg activity): qty-side
/// adjustment stays correct under further raw inflows.
#[tokio::test(flavor = "multi_thread")]
async fn cost_adjust_then_more_raw_receipt_then_adjust_again() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "DRIFT1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "DRIFT1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 1200);

    // Add 100 more raw @ $14 = $1400. raw class qty=200, value=$2600.
    seed_class(&pool, qty_acct, raw_v, 100, 1400, "2026-04-16").await;
    assert_eq!(class_qty(&pool, raw_v).await, 200);
    assert_eq!(balance(&pool, raw_v).await, 2600);

    // Adjust raw to $15. delta = 15*200 - 2600 = $400 → raw=$3000.
    cost_adjust(&pool, &sku, &loc, "USD", "raw", 15, "2026-04-17").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 3000);
    assert_eq!(balance(&pool, fg_v).await, 1000, "fg untouched");
}

/// Distinct-location baseline (the prior workaround pattern). Verify
/// the fix doesn't change behavior when classes are already isolated.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_location_baseline_unchanged() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "DIST1", "wac_perpetual").await;
    let raw_loc = fresh_location(&pool, "DIST1-RAW").await;
    let fg_loc = fresh_location(&pool, "DIST1-FG").await;

    let raw_q = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&raw_loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&raw_loc), None, "debit").await;
    let fg_q = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&fg_loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&fg_loc), None, "debit").await;

    seed_class(&pool, raw_q, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, fg_q, fg_v, 50, 1000, "2026-04-10").await;

    cost_adjust(&pool, &sku, &raw_loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 1200);
    assert_eq!(balance(&pool, fg_v).await, 1000);
}

/// Empty class pool raises P0010 (per-class qty == 0 even though
/// stock_available has qty from the OTHER class).
#[tokio::test(flavor = "multi_thread")]
async fn empty_class_with_other_class_populated_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "EMPTY1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "EMPTY1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let _fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    // Only raw seeded; fg pool is empty.
    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    assert_eq!(balance(&pool, qty_acct).await, 100);

    let err = cost_adjust(&pool, &sku, &loc, "USD", "fg", 20, "2026-04-15").await
        .expect_err("expected P0010 empty fg class");
    let sqlstate = err.as_database_error().and_then(|e| e.code().map(|c| c.to_string()));
    assert_eq!(sqlstate, Some("P0010".to_string()));
    assert!(format!("{err}").contains("non-empty class pool"),
            "error message references non-empty class pool: {err}");
}

/// Pre-fix would have allowed adjusting an empty class because
/// stock_available > 0 (other class). Confirm post-fix raises.
#[tokio::test(flavor = "multi_thread")]
async fn empty_raw_with_populated_fg_raises_p0010() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "EMPTY2", "wac_perpetual").await;
    let loc = fresh_location(&pool, "EMPTY2-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let _raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    let err = cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await
        .expect_err("expected P0010 empty raw class");
    let sqlstate = err.as_database_error().and_then(|e| e.code().map(|c| c.to_string()));
    assert_eq!(sqlstate, Some("P0010".to_string()));
}

/// Idempotency replay returns same doc id without re-posting.
#[tokio::test(flavor = "multi_thread")]
async fn idempotency_replay_same_class_no_double_post() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "IDEM1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "IDEM1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    let key = fresh_uuid(&pool).await;
    let id1 = cost_adjust_with_key(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15", &key).await.unwrap();
    let id2 = cost_adjust_with_key(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15", &key).await.unwrap();
    assert_eq!(id1, id2, "replay returns same doc id");

    // Pool was adjusted once (delta $200), not twice ($400).
    assert_eq!(balance(&pool, raw_v).await, 1200);
}

/// delta = 0 path: target == prior_unit. Audit row recorded, no transfer.
#[tokio::test(flavor = "multi_thread")]
async fn target_equals_prior_records_audit_no_transfer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "ZERO1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "ZERO1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    let var_acct = account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;
    let var_pre = balance(&pool, var_acct).await;

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 10, "2026-04-15").await.unwrap();

    assert_eq!(balance(&pool, raw_v).await, 1000, "no change");
    assert_eq!(balance(&pool, var_acct).await - var_pre, 0, "no transfer posted");

    // Audit row exists with delta=0.
    let (audit_qty, delta): (i64, i64) = sqlx::query_as(
        "SELECT pool_qty, delta_value FROM inventory_cost_adjustments
          WHERE sku_id = $1::UUID AND inventory_class = 'raw'",
    )
    .bind(&sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audit_qty, 100);
    assert_eq!(delta, 0);
}

/// Adjustment after raw consumption (out-flow): per-class qty decrements.
/// Mirrors what would happen after rm_issue_to_wo on a wac_perpetual
/// component.
#[tokio::test(flavor = "multi_thread")]
async fn cost_adjust_after_class_consumption() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "CONS1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "CONS1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    // Simulate raw consumption: 30 units @ $10 leave to a parent WIP.
    // We just need credit on raw_v + qty_acct with qty=30.
    let parent = fresh_sku(&pool, "CONS1-P", "standard").await;
    let _stock_wip_q = open_account(&pool, "stock_wip", "qty", None, Some(&parent), None, Some(10), "debit").await;
    let wip_v = open_account(&pool, "inv_value_wip", "value", Some("USD"), Some(&parent), None, Some(10), "debit").await;
    let consumed_q = open_account(&pool, "stock_consumed", "qty", None, Some(&sku), None, None, "debit").await;

    let posted_by = fresh_uuid(&pool).await;
    let doc_id = fresh_uuid(&pool).await;
    let events = json!([
        { "reason": "rm_issue_to_wo", "document_kind": "wo_event", "document_id": doc_id,
          "debit_account_id": consumed_q, "credit_account_id": qty_acct,
          "amount": 30, "qty": 30, "business_date": "2026-04-12",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
        { "reason": "rm_issue_to_wo", "document_kind": "wo_event", "document_id": doc_id,
          "debit_account_id": wip_v, "credit_account_id": raw_v,
          "amount": 300, "qty": 30, "business_date": "2026-04-12",
          "idempotency_key": fresh_uuid(&pool).await, "posted_by": posted_by },
    ]);
    sqlx::query("SELECT post_transfers($1, FALSE)")
        .bind(events)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(class_qty(&pool, raw_v).await, 70, "raw class qty after consumption");
    assert_eq!(balance(&pool, raw_v).await, 700, "raw value after consumption");

    // Adjust raw to $12. delta = 12*70 - 700 = $140 → raw=$840.
    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 840);
    assert_eq!(balance(&pool, fg_v).await, 1000, "fg untouched");
}

/// Multi-currency raw + fg at same location: each currency's pool is
/// further class-isolated by the currency dimension on inv_value_*.
#[tokio::test(flavor = "multi_thread")]
async fn multi_currency_same_location_class_isolation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "MULTI1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "MULTI1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_usd = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let raw_eur = open_account(&pool, "inv_value_raw", "value", Some("EUR"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_usd = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    // Ensure variance and creation_void EUR companions exist for the
    // EUR seed path and for cost_adjust's variance lookup.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side)
         VALUES ('creation_void', 'value', 'EUR', 'unrestricted')
         ON CONFLICT DO NOTHING",
    ).execute(&pool).await.unwrap();

    seed_class(&pool, qty_acct, raw_usd, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, raw_eur, 80, 800, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_usd, 50, 1000, "2026-04-10").await;

    // stock_available combined = 230. Each per-class qty must be isolated.
    assert_eq!(balance(&pool, qty_acct).await, 230);
    assert_eq!(class_qty(&pool, raw_usd).await, 100);
    assert_eq!(class_qty(&pool, raw_eur).await, 80);
    assert_eq!(class_qty(&pool, fg_usd).await, 50);

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    assert_eq!(balance(&pool, raw_usd).await, 1200, "USD raw at target");
    assert_eq!(balance(&pool, raw_eur).await, 800, "EUR raw untouched");
    assert_eq!(balance(&pool, fg_usd).await, 1000, "USD fg untouched");
}

/// Sequential adjustments same class should compose: each adjust uses
/// the running class qty (which doesn't change unless qty inflows occur).
#[tokio::test(flavor = "multi_thread")]
async fn sequential_adjustments_same_class_compose() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "SEQ1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "SEQ1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 1200);

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 8, "2026-04-16").await.unwrap();
    // delta = 8*100 - 1200 = -$400 → raw=$800.
    assert_eq!(balance(&pool, raw_v).await, 800);

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 11, "2026-04-17").await.unwrap();
    // delta = 11*100 - 800 = $300 → raw=$1100.
    assert_eq!(balance(&pool, raw_v).await, 1100);

    assert_eq!(balance(&pool, fg_v).await, 1000, "fg untouched throughout");
}

/// Write-up + write-down on different classes back-to-back.
#[tokio::test(flavor = "multi_thread")]
async fn raw_writeup_then_fg_writedown_same_location() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "WUWD1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "WUWD1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let var_acct = account_id_by_kind_currency(&pool, "variance_cost_adjustment", Some("USD")).await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;
    let var_pre = balance(&pool, var_acct).await;

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 14, "2026-04-15").await.unwrap();
    // raw write-up: delta = 14*100 - 1000 = +$400. dr inv_value_raw / cr var.
    cost_adjust(&pool, &sku, &loc, "USD", "fg", 14, "2026-04-15").await.unwrap();
    // fg write-down: delta = 14*50 - 1000 = -$300. dr var / cr inv_value_fg.

    assert_eq!(balance(&pool, raw_v).await, 1400);
    assert_eq!(balance(&pool, fg_v).await, 700);
    // Net variance: +400 - 300 = +100 (debits up by 400, then 300; credits up by 400 (raw side) and... )
    // Wait: cost_adjust write-up posts dr inv / cr var → var balance DECREASES (credited).
    //        write-down posts dr var / cr inv → var balance INCREASES (debited).
    // Net: -400 (raw write-up) + 300 (fg write-down) = -100.
    assert_eq!(balance(&pool, var_acct).await - var_pre, -100, "net variance from up+down");
}

/// Audit row's pool_qty column is per-class (post-fix). Pre-fix it was
/// cross-class. This test pins the post-fix invariant.
#[tokio::test(flavor = "multi_thread")]
async fn audit_pool_qty_is_per_class_not_cross_class() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "AUDIT1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "AUDIT1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 50, 1000, "2026-04-10").await;

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    cost_adjust(&pool, &sku, &loc, "USD", "fg", 25, "2026-04-15").await.unwrap();

    let raw_audit_qty: i64 = sqlx::query_scalar(
        "SELECT pool_qty FROM inventory_cost_adjustments
          WHERE sku_id = $1::UUID AND inventory_class = 'raw'",
    )
    .bind(&sku).fetch_one(&pool).await.unwrap();
    let fg_audit_qty: i64 = sqlx::query_scalar(
        "SELECT pool_qty FROM inventory_cost_adjustments
          WHERE sku_id = $1::UUID AND inventory_class = 'fg'",
    )
    .bind(&sku).fetch_one(&pool).await.unwrap();
    assert_eq!(raw_audit_qty, 100);
    assert_eq!(fg_audit_qty, 50);
}

/// Symmetric cycle: adjust raw, then add fg activity, then adjust fg.
/// Confirms cross-class stock_available activity doesn't bleed into
/// the per-class divisor for either side.
#[tokio::test(flavor = "multi_thread")]
async fn cross_class_activity_doesnt_pollute_either_class_divisor() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "ISO1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "ISO1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 12, "2026-04-15").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 1200);

    // Now add a substantial fg pool. stock_available jumps to 600 combined.
    seed_class(&pool, qty_acct, fg_v, 500, 10000, "2026-04-16").await;
    assert_eq!(balance(&pool, qty_acct).await, 600, "stock_available combined");
    assert_eq!(class_qty(&pool, raw_v).await, 100, "raw class unchanged");
    assert_eq!(class_qty(&pool, fg_v).await, 500);

    // Adjust raw to $13. With pre-fix cross-class divisor (qty=600):
    //   pre-fix: delta = 13*600 - 1200 = $6600 → raw=$7800 ❌
    // Post-fix per-class divisor (qty=100): delta = 13*100 - 1200 = $100 → raw=$1300 ✓
    cost_adjust(&pool, &sku, &loc, "USD", "raw", 13, "2026-04-17").await.unwrap();
    assert_eq!(balance(&pool, raw_v).await, 1300, "raw correct under cross-class drift");
    assert_eq!(balance(&pool, fg_v).await, 10000, "fg untouched");

    // Symmetric: adjust fg to $22.
    // Pre-fix delta = 22*600 - 10000 = $3200 → fg=$13200 ❌
    // Post-fix delta = 22*500 - 10000 = $1000 → fg=$11000 ✓
    cost_adjust(&pool, &sku, &loc, "USD", "fg", 22, "2026-04-18").await.unwrap();
    assert_eq!(balance(&pool, fg_v).await, 11000);
    assert_eq!(balance(&pool, raw_v).await, 1300);
}

/// Many fluctuations: add, subtract, adjust repeatedly. Class divisor
/// must stay correct.
#[tokio::test(flavor = "multi_thread")]
async fn many_inflows_outflows_then_adjust() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = fresh_sku(&pool, "FLUX1", "wac_perpetual").await;
    let loc = fresh_location(&pool, "FLUX1-LOC").await;

    let qty_acct = open_account(&pool, "stock_available", "qty", None, Some(&sku), Some(&loc), None, "debit").await;
    let raw_v = open_account(&pool, "inv_value_raw", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;
    let fg_v = open_account(&pool, "inv_value_fg", "value", Some("USD"), Some(&sku), Some(&loc), None, "debit").await;

    // Multiple raw inflows.
    seed_class(&pool, qty_acct, raw_v, 100, 1000, "2026-04-10").await;
    seed_class(&pool, qty_acct, raw_v, 50, 600, "2026-04-11").await;   // raw qty 150, val 1600
    seed_class(&pool, qty_acct, raw_v, 25, 400, "2026-04-12").await;   // raw qty 175, val 2000

    // Multiple fg activity (different SKU role at same location).
    seed_class(&pool, qty_acct, fg_v, 30, 600, "2026-04-10").await;
    seed_class(&pool, qty_acct, fg_v, 70, 1500, "2026-04-13").await;   // fg qty 100, val 2100

    assert_eq!(class_qty(&pool, raw_v).await, 175);
    assert_eq!(class_qty(&pool, fg_v).await, 100);
    assert_eq!(balance(&pool, qty_acct).await, 275, "stock_available combined");

    cost_adjust(&pool, &sku, &loc, "USD", "raw", 15, "2026-04-15").await.unwrap();
    // delta = 15*175 - 2000 = $625 → raw=$2625.
    assert_eq!(balance(&pool, raw_v).await, 2625);
    assert_eq!(balance(&pool, fg_v).await, 2100);

    cost_adjust(&pool, &sku, &loc, "USD", "fg", 18, "2026-04-15").await.unwrap();
    // delta = 18*100 - 2100 = -$300 → fg=$1800.
    assert_eq!(balance(&pool, fg_v).await, 1800);
    assert_eq!(balance(&pool, raw_v).await, 2625);
}
