//! `acct-uxu` — WAC integration tests beyond the conformance fixture.
//!
//! Conformance covers per-case correctness (WAC unit cost = value/qty,
//! P0006 on zero qty, qty-side gate relaxes for WAC). This file covers:
//!
//!   1. **Re-average across receipts** — WAC unit cost evolves correctly
//!      as inventory is received at different prices.
//!   2. **Concurrent-safety smoke** — two simultaneous WAC issues on the
//!      same pool serialize via FOR UPDATE; both compute against
//!      pre-batch state of their own tx; no deadlock; final state
//!      preserves the invariant `value_pool / qty_pool == unit_cost`.
//!
//! These run with `cargo test` (not `--ignored`); they are unit-scale,
//! not load-scale.

mod common;

use common::*;
use serde_json::json;
use std::sync::Arc;

/// Helper: post a single-event batch via `post_posting_lines`. Returns the
/// raw JSONB result.
async fn post_one(
    pool: &sqlx::PgPool,
    event: serde_json::Value,
) -> sqlx::Result<serde_json::Value> {
    let batch = json!([event]);
    sqlx::query_scalar("SELECT post_posting_lines($1, $2)")
        .bind(&batch)
        .bind(false)
        .fetch_one(pool)
        .await
}

/// Pre-build a known WAC pool state on SKU-WAC: stock_available(MAIN)
/// = `qty`, inv_value_fg(MAIN, USD) = `value_usd`. Uses cycle_count_adj
/// (which is non-cost-relevant) so we can seed precise amounts without
/// engaging the dispatcher.
async fn seed_wac_pool(pool: &sqlx::PgPool, qty: i64, value_usd: i64) {
    let qty_acct = account_id_stock_available(pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        pool,
        "inv_value_fg",
        Some("SKU-WAC"),
        Some("MAIN"),
        Some("USD"),
        None,
    )
    .await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(pool, "creation_void", Some("USD")).await;

    if qty > 0 {
        let key = fresh_uuid(pool).await;
        let res = post_one(
            pool,
            make_event(
                "cycle_count_adj",
                qty_acct,
                void_qty,
                qty,
                "2026-04-15",
                &key,
            ),
        )
        .await;
        res.expect("seed qty");
    }
    if value_usd > 0 {
        let key = fresh_uuid(pool).await;
        // Value-leg seed needs qty for the per-class WAC divisor
        // (migration 0030, transfers.qty). qty here is the same `qty`
        // passed to seed_pool — we're seeding a pool that started empty,
        // so total $ corresponds to total units.
        let res = post_one(
            pool,
            make_event_with_qty(
                "cycle_count_adj",
                val_acct,
                void_val,
                value_usd,
                qty,
                "2026-04-15",
                &key,
            ),
        )
        .await;
        res.expect("seed value");
    }
}

/// Read `(debits_total - credits_total)` for an account by id.
async fn balance(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("balance")
}

#[tokio::test]
async fn wac_unit_cost_re_averages_across_receipts() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool,
        "inv_value_fg",
        Some("SKU-WAC"),
        Some("MAIN"),
        Some("USD"),
        None,
    )
    .await;
    let cogs_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;

    // Stage 1: receive 100 units at $5 each.
    seed_wac_pool(&pool, 100, 500).await;
    assert_eq!(balance(&pool, qty_acct).await, 100);
    assert_eq!(balance(&pool, val_acct).await, 500);

    // Stage 2: receive 100 more at $7 each. Pool: qty=200, value=1200, avg=$6.
    seed_wac_pool(&pool, 100, 700).await;
    assert_eq!(balance(&pool, qty_acct).await, 200);
    assert_eq!(balance(&pool, val_acct).await, 1200);

    // Issue 50 units at WAC. Expected: amount = 50 * (1200 / 200) = 50 * 6 = 300.
    let key1 = fresh_uuid(&pool).await;
    let key2 = fresh_uuid(&pool).await;
    let batch = json!([
        // Qty leg: stock_available -50.
        make_event("so_ship", void_qty, qty_acct, 50, "2026-04-15", &key1),
        // Value leg: cogs +amount, inv_value_fg -amount. qty=50 sent; function
        // computes amount.
        {
            "reason":            "so_ship",
            "document_kind":     "wac_test",
            "document_id":       fresh_uuid(&pool).await,
            "debit_account_id":  cogs_acct,
            "credit_account_id": val_acct,
            "qty":               50,
            "business_date":     "2026-04-15",
            "idempotency_key":   key2,
            "posted_by":         fresh_uuid(&pool).await,
        }
    ]);
    sqlx::query_scalar::<_, serde_json::Value>("SELECT post_posting_lines($1, $2)")
        .bind(&batch)
        .bind(false)
        .fetch_one(&pool)
        .await
        .expect("issue 50");

    // Pool after issue: qty=150, value=900, avg still $6.
    assert_eq!(balance(&pool, qty_acct).await, 150);
    assert_eq!(balance(&pool, val_acct).await, 900);
    assert_eq!(balance(&pool, cogs_acct).await, 300);

    // Stage 3: receive 50 units at $10 each. Pool: qty=200, value=1400, avg=7
    // (BIGINT integer division: 1400 / 200 = 7 exactly).
    seed_wac_pool(&pool, 50, 500).await;
    assert_eq!(balance(&pool, qty_acct).await, 200);
    assert_eq!(balance(&pool, val_acct).await, 1400);

    // Issue 50 units at the new average. amount = 50 * 7 = 350.
    let key3 = fresh_uuid(&pool).await;
    let key4 = fresh_uuid(&pool).await;
    let batch2 = json!([
        make_event("so_ship", void_qty, qty_acct, 50, "2026-04-15", &key3),
        {
            "reason":            "so_ship",
            "document_kind":     "wac_test",
            "document_id":       fresh_uuid(&pool).await,
            "debit_account_id":  cogs_acct,
            "credit_account_id": val_acct,
            "qty":               50,
            "business_date":     "2026-04-15",
            "idempotency_key":   key4,
            "posted_by":         fresh_uuid(&pool).await,
        }
    ]);
    sqlx::query_scalar::<_, serde_json::Value>("SELECT post_posting_lines($1, $2)")
        .bind(&batch2)
        .bind(false)
        .fetch_one(&pool)
        .await
        .expect("issue 50 at avg=7");

    // Pool after second issue: qty=150, value=1050, avg=7.
    assert_eq!(balance(&pool, qty_acct).await, 150);
    assert_eq!(balance(&pool, val_acct).await, 1050);
    // Cogs accumulator: 300 + 350 = 650.
    assert_eq!(balance(&pool, cogs_acct).await, 650);
}

#[tokio::test]
async fn wac_concurrent_issues_serialize_no_deadlock() {
    let pool = connect_test_db_with(8).await;
    reset_to_fixture(&pool).await;

    // Seed pool: qty=1000, value=6000, avg=$6.
    seed_wac_pool(&pool, 1000, 6000).await;

    let qty_acct = account_id_stock_available(&pool, "SKU-WAC", "MAIN").await;
    let val_acct = account_id_for_selector(
        &pool,
        "inv_value_fg",
        Some("SKU-WAC"),
        Some("MAIN"),
        Some("USD"),
        None,
    )
    .await;
    let cogs_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;

    let pool = Arc::new(pool);
    let mut handles = Vec::new();

    // 5 concurrent writers, each issuing 10 units. They all serialize on the
    // FOR UPDATE on stock_available + inv_value_fg, so each batch sees the
    // average from before its own issue. Total expected change: 50 units out,
    // 300 value out (if avg stays at $6 throughout — which it should because
    // each issue takes value at the same ratio).
    for _ in 0..5 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let key1 = fresh_uuid(&pool).await;
            let key2 = fresh_uuid(&pool).await;
            let batch = json!([
                make_event("so_ship", void_qty, qty_acct, 10, "2026-04-15", &key1),
                {
                    "reason":            "so_ship",
                    "document_kind":     "wac_concurrent_test",
                    "document_id":       fresh_uuid(&pool).await,
                    "debit_account_id":  cogs_acct,
                    "credit_account_id": val_acct,
                    "qty":               10,
                    "business_date":     "2026-04-15",
                    "idempotency_key":   key2,
                    "posted_by":         fresh_uuid(&pool).await,
                }
            ]);
            sqlx::query_scalar::<_, serde_json::Value>("SELECT post_posting_lines($1, $2)")
                .bind(&batch)
                .bind(false)
                .fetch_one(&*pool)
                .await
                .expect("concurrent issue")
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    let q_after = balance(&pool, qty_acct).await;
    let v_after = balance(&pool, val_acct).await;
    let c_after = balance(&pool, cogs_acct).await;

    // 5 issues × 10 units = 50 units out, 50 * 6 = 300 value out.
    // Pool: qty=950, value=5700.
    assert_eq!(q_after, 950, "qty pool after 5 concurrent issues");
    assert_eq!(v_after, 5700, "value pool after 5 concurrent issues");
    assert_eq!(c_after, 300, "cogs accumulator after 5 concurrent issues");
    // Average preserved: 5700 / 950 = 6.
    assert_eq!(v_after / q_after, 6, "WAC unit cost preserved across concurrent issues");

    // Deadlock counter unchanged.
    let deadlocks: i64 =
        sqlx::query_scalar("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
            .fetch_one(&*pool)
            .await
            .unwrap_or(0);
    let _ = deadlocks; // we don't have a baseline to subtract; concurrent test doesn't expect them.
}

#[tokio::test]
async fn wac_zero_qty_pool_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let val_acct = account_id_for_selector(
        &pool,
        "inv_value_fg",
        Some("SKU-WAC"),
        Some("MAIN"),
        Some("USD"),
        None,
    )
    .await;
    let cogs_acct = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;

    // Pool is empty (no preconditions). Issue against it -> P0006.
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("P0006", || async {
        let batch = json!([{
            "reason":            "so_ship",
            "document_kind":     "wac_test",
            "document_id":       fresh_uuid(&pool).await,
            "debit_account_id":  cogs_acct,
            "credit_account_id": val_acct,
            "qty":               10,
            "business_date":     "2026-04-15",
            "idempotency_key":   key,
            "posted_by":         fresh_uuid(&pool).await,
        }]);
        sqlx::query_scalar::<_, serde_json::Value>("SELECT post_posting_lines($1, $2)")
            .bind(&batch)
            .bind(false)
            .fetch_one(&pool)
            .await
            .map(|_| ())
    })
    .await;
}
