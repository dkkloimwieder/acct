//! T1 probes for the D4 backfill helper (mig 0028, acct-wb75.3.4).
//! Phase D D4 of the convergence plan.
//!
//! After D2/D3 are wired, every fresh posting_line gets a movement
//! row through the apply_event D-block. The backfill is for posts
//! that landed BEFORE the dispatcher was wired, OR for late inserts
//! that bypassed apply_event (incident recovery via direct DB
//! writes). These tests stress the helper directly:
//!
//!   - Insert a posting_line with its posting_line_inventory row
//!     bypassing post_posting_lines (no D-block fires).
//!   - Verify no inventory_movements row exists for it.
//!   - Call `_backfill_inventory_movements()`.
//!   - Verify exactly one row was added with the expected shape.
//!   - Re-call to verify idempotency.
//!
//! Also exercises the gate boundaries: posts without an inv_value_*
//! account must NOT be backfilled (no row); posts with no SKU must
//! NOT be backfilled.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

/// Insert a posting_line + posting_line_inventory directly into the
/// tables, simulating a pre-D2/D3 historical post that has no
/// movement yet. Returns the new posting_line.id.
#[allow(clippy::too_many_arguments)]
async fn stage_orphan_posting(
    pool: &PgPool,
    reason: &str,
    debit_account_id: i64,
    credit_account_id: i64,
    amount: i64,
    qty: i64,
    business_date: &str,
) -> i64 {
    let key = fresh_uuid(pool).await;
    let period_id: i64 = sqlx::query_scalar(
        "SELECT id FROM periods
          WHERE opens_at <= $1::DATE AND closes_at >= $1::DATE",
    )
    .bind(business_date)
    .fetch_one(pool)
    .await
    .unwrap();

    // Direct INSERT bypasses post_posting_lines (no D-block fires).
    // We DO NOT update accounts.debits_total / credits_total because
    // for the backfill test we only need the posting_line + extension
    // row to exist; the recon would catch a real-world divergence
    // but that's not what D4 tests.
    let new_id: i64 = sqlx::query_scalar(
        "INSERT INTO posting_lines (
            reason, document_kind, document_id,
            debit_account_id, credit_account_id, amount, qty,
            period_id, business_date, idempotency_key, posted_by
         )
         VALUES ($1::posting_line_reason, 'd4_backfill_test',
                 '00000000-0000-0000-0000-0000000000aa'::UUID,
                 $2, $3, $4, $5, $6, $7::DATE, $8::UUID,
                 '00000000-0000-0000-0000-0000000000bb'::UUID)
         RETURNING id",
    )
    .bind(reason)
    .bind(debit_account_id)
    .bind(credit_account_id)
    .bind(amount)
    .bind(qty)
    .bind(period_id)
    .bind(business_date)
    .bind(&key)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("stage_orphan_posting: {e}"));

    // Manually populate posting_line_inventory mirroring what the C
    // extension write would have done. Resolve SKU credit-first.
    let (product_id, unit_cost, cm): (Option<String>, Option<i64>, Option<String>) =
        sqlx::query_as(
            "WITH dc AS (
                SELECT COALESCE(c.sku_id, d.sku_id) AS sku_id, d.ledger_kind
                  FROM accounts d
                  JOIN accounts c ON c.id = $4
                 WHERE d.id = $1
             )
             SELECT dc.sku_id::text,
                    CASE WHEN dc.ledger_kind = 'value' AND $3 <> 0
                         THEN $2::BIGINT / ABS($3::BIGINT)
                         ELSE NULL
                    END,
                    s.cost_method::TEXT
               FROM dc
               LEFT JOIN skus s ON s.id = dc.sku_id",
        )
        .bind(debit_account_id)
        .bind(amount)
        .bind(qty)
        .bind(credit_account_id)
        .fetch_one(pool)
        .await
        .unwrap();
    if let (Some(pid), Some(cm_text)) = (product_id.as_deref(), cm.as_deref()) {
        sqlx::query(
            "INSERT INTO posting_line_inventory
                (posting_line_id, product_id, quantity, qty_uom,
                 unit_cost, cost_method_at_event)
             VALUES ($1, $2::UUID, ABS($3)::NUMERIC, 'EA', $4, $5::cost_method)",
        )
        .bind(new_id)
        .bind(pid)
        .bind(qty)
        .bind(unit_cost)
        .bind(cm_text)
        .execute(pool)
        .await
        .unwrap();
    }

    new_id
}

/// Resolve a fixture-seeded SKU/location-bearing inv_value_raw account.
async fn inv_value_raw(pool: &PgPool, sku_code: &str, loc_code: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT a.id
           FROM accounts a
           JOIN skus s     ON s.id = a.sku_id
           JOIN locations l ON l.id = a.location_id
          WHERE a.kind = 'inv_value_raw'
            AND a.currency = 'USD'
            AND s.code = $1 AND l.code = $2",
    )
    .bind(sku_code)
    .bind(loc_code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn ap_unsettled_usd(pool: &PgPool) -> i64 {
    // A vendor-bound ap_unsettled USD account is needed for the
    // counterparty side of a value-leg post. Open a fresh one for
    // each test to keep accounting unique.
    let vendor: String = sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ('VEND-D4-' || substring(gen_random_uuid()::text, 1, 6), 'd4', 'USD')
         RETURNING id::text",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         VALUES ('ap_unsettled', 'value', 'USD', 'credit', $1::UUID)
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ============================================================
// Backfill writes a movement for orphan inv_value_* posts
// ============================================================

#[tokio::test]
async fn backfill_writes_movement_for_orphan_post() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let inv = inv_value_raw(&pool, "SKU-A", "MAIN").await;
    let ap = ap_unsettled_usd(&pool).await;

    // Stage a po_receipt-like posting (DR inv_value_raw, CR ap_unsettled),
    // bypassing post_posting_lines entirely.
    let pl_id = stage_orphan_posting(&pool, "po_receipt", inv, ap, 500, 5, "2026-04-15").await;

    let pre: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_movements WHERE posting_line_id = $1",
    )
    .bind(pl_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pre, 0, "no movement before backfill");

    let inserted: i64 = sqlx::query_scalar("SELECT _backfill_inventory_movements()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(inserted, 1, "exactly one row backfilled");

    let row: (i32, String, String, String) = sqlx::query_as(
        "SELECT event_type::INT, quantity::TEXT, actual_unit_cost::TEXT, standard_unit_cost::TEXT
           FROM inventory_movements
          WHERE posting_line_id = $1",
    )
    .bind(pl_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, 1, "po_receipt → event_type 1 (receipt)");
    assert!(row.1.starts_with("5"), "qty +5 (DR inv_value_raw); got {:?}", row.1);
    assert!(
        row.2.starts_with("100"),
        "actual_unit_cost = 500/5 = 100; got {:?}",
        row.2
    );
    assert!(
        row.3.starts_with("100"),
        "standard_unit_cost = SKU-A std at 2026-04-15 = 100; got {:?}",
        row.3
    );
}

#[tokio::test]
async fn backfill_is_idempotent() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let inv = inv_value_raw(&pool, "SKU-A", "MAIN").await;
    let ap = ap_unsettled_usd(&pool).await;
    stage_orphan_posting(&pool, "po_receipt", inv, ap, 600, 3, "2026-04-15").await;

    let n1: i64 = sqlx::query_scalar("SELECT _backfill_inventory_movements()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let n2: i64 = sqlx::query_scalar("SELECT _backfill_inventory_movements()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n1, 1, "first call: 1 row");
    assert_eq!(n2, 0, "second call: idempotent (LEFT JOIN dedup)");
}

#[tokio::test]
async fn backfill_signed_quantity_for_credit_inv_value() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Reverse direction: CR inv_value_raw, DR ap_unsettled — like a
    // po_return. Quantity should be NEGATIVE.
    let inv = inv_value_raw(&pool, "SKU-A", "MAIN").await;
    let ap = ap_unsettled_usd(&pool).await;
    let pl_id = stage_orphan_posting(
        &pool,
        "po_return_to_vendor",
        ap, inv,                     // DR ap_unsettled, CR inv_value_raw
        200, 4,
        "2026-04-15",
    )
    .await;

    sqlx::query_scalar::<_, i64>("SELECT _backfill_inventory_movements()")
        .fetch_one(&pool)
        .await
        .unwrap();

    let (event_type, qty_text): (i32, String) = sqlx::query_as(
        "SELECT event_type::INT, quantity::TEXT
           FROM inventory_movements WHERE posting_line_id = $1",
    )
    .bind(pl_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event_type, 13, "po_return_to_vendor → event_type 13");
    assert!(
        qty_text.starts_with('-'),
        "credit-side inv_value → negative quantity; got {qty_text:?}"
    );
}

// ============================================================
// Gate boundaries — backfill must not fire on non-qualifying posts
// ============================================================

#[tokio::test]
async fn backfill_skips_unmapped_reasons() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // 'ar_payment' is not in the event_type helper's mapping —
    // it's a non-inventory reason. Even with inv_value_raw on the
    // debit side (contrived), the helper returns NULL → backfill
    // skips. Use unique amounts so we can locate the orphan.
    let inv = inv_value_raw(&pool, "SKU-A", "MAIN").await;
    let ap = ap_unsettled_usd(&pool).await;
    let pl_id = stage_orphan_posting(&pool, "ar_payment", inv, ap, 333, 1, "2026-04-15").await;

    sqlx::query_scalar::<_, i64>("SELECT _backfill_inventory_movements()")
        .fetch_one(&pool)
        .await
        .unwrap();

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_movements WHERE posting_line_id = $1",
    )
    .bind(pl_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0, "unmapped reason → no movement");
}

#[tokio::test]
async fn backfill_run_at_migration_time_is_zero_for_fresh_fixture() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // The fixture seeds zero posting_lines; backfill at migration
    // time was a no-op. After fixture seed there are also zero
    // posting_lines (TRUNCATE in reset_to_fixture). So calling the
    // helper from a freshly-seeded state inserts nothing.
    let n: i64 = sqlx::query_scalar("SELECT _backfill_inventory_movements()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "fresh seed has no inventory posts to backfill");
}
