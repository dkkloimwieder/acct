//! Phase C C0 — `posting_line_inventory` backfill preflight audit
//! (acct-wb75.2.1). Phase C's backfill computes
//! `posting_line_inventory.product_id` from
//! `COALESCE(credit_account.sku_id, debit_account.sku_id)` per the
//! credit-first R2 rule. Any existing posting_line with qty IS NOT NULL
//! whose neither side carries an sku_id would violate the future
//! `product_id NOT NULL` constraint at backfill time.
//!
//! These probes assert the invariant holds across the breadth of
//! qty-emitting reasons exercised by `_post_posting_lines_apply_event`:
//! qty-leg events (cycle_count_adj, bin_move) where qty == amount and
//! both sides ledger_kind='qty'; value-leg cost events (wo_start,
//! op_move_v, wo_complete_v, scrap_v, so_ship) that carry an explicit
//! `qty` field. Per `research/posting-lines-convergence-plan.md` §9.4.
//!
//! Acceptance: each scenario's audit query returns 0 exceptions; the
//! synthetic-violation sanity test confirms the query DOES surface the
//! shape it's meant to catch (so a future regression isn't silently
//! masked by a query bug).

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

/// The canonical preflight query: posting_lines with qty NOT NULL but
/// neither side resolves to an sku_id.
const AUDIT_SQL: &str = r#"
    SELECT COUNT(*)::BIGINT
      FROM posting_lines pl
      JOIN accounts d ON d.id = pl.debit_account_id
      JOIN accounts c ON c.id = pl.credit_account_id
     WHERE pl.qty IS NOT NULL
       AND COALESCE(c.sku_id, d.sku_id) IS NULL
"#;

/// Secondary preflight: value-leg posting with qty=0 would divide-by-zero
/// in the future unit_cost = amount/qty calc (the backfill SQL guards
/// with NULLIF, but a 0-qty value-leg is suspicious regardless).
const ZERO_QTY_AUDIT_SQL: &str = r#"
    SELECT COUNT(*)::BIGINT
      FROM posting_lines pl
     WHERE pl.qty = 0
"#;

async fn audit_count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn audit_clean_on_pristine_fixture() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    assert_eq!(audit_count(&pool, AUDIT_SQL).await, 0);
    assert_eq!(audit_count(&pool, ZERO_QTY_AUDIT_SQL).await, 0);
}

#[tokio::test]
async fn audit_clean_after_qty_leg_postings() {
    // cycle_count_adj (stock_available ← creation_void) and bin_move
    // (stock_available → stock_available across locs). Both are qty-leg
    // (ledger_kind='qty' on both sides); qty inferred from amount.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 50).await;

    let main_stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let out_stock = account_id_stock_available(&pool, "SKU-A", "OUT").await;
    let key = fresh_uuid(&pool).await;
    let bin_move = make_event("bin_move", out_stock, main_stock, 10, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([bin_move]), false)
        .await
        .expect("post bin_move");

    assert_eq!(audit_count(&pool, AUDIT_SQL).await, 0);
    assert_eq!(audit_count(&pool, ZERO_QTY_AUDIT_SQL).await, 0);
}

#[tokio::test]
async fn audit_clean_after_wo_start_qty_leg() {
    // wo_start writes a qty-leg into stock_wip. SKU resolves via stock_wip
    // debit side; creation_void credit has NULL sku_id.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let stock_wip: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'stock_wip' AND s.code = 'SKU-A' AND a.routing_op = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;

    let key = fresh_uuid(&pool).await;
    let event = make_event("wo_start", stock_wip, void_qty, 7, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post wo_start qty-leg");

    assert_eq!(audit_count(&pool, AUDIT_SQL).await, 0);
}

#[tokio::test]
async fn audit_clean_after_value_leg_op_move_v() {
    // op_move_v is the BOM2 caller-supplied-amount value-leg reason. It
    // posts inv_value_wip (sku, op=10) → inv_value_wip (sku, op=20),
    // both with sku_id populated. Carries explicit `qty` field.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let wip10_value: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'inv_value_wip' AND s.code = 'SKU-A' AND a.routing_op = 10",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let wip20_value: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'inv_value_wip' AND s.code = 'SKU-A' AND a.routing_op = 20",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let void_val =
        account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;

    // Seed the wip10 value pool so op_move_v's credit doesn't push a
    // debit-normal account negative. Use a non-cost-event reason so it
    // bypasses the dispatcher's cost-method gate.
    let seed_key = fresh_uuid(&pool).await;
    let seed = make_event_with_qty(
        "cost_adjustment",
        wip10_value,
        void_val,
        500,
        5,
        "2026-04-15",
        &seed_key,
    );
    call_post_posting_lines(&pool, json!([seed]), false)
        .await
        .expect("seed wip10 value pool");

    let key = fresh_uuid(&pool).await;
    let event = make_event_with_qty(
        "op_move_v",
        wip20_value,
        wip10_value,
        500,
        5,
        "2026-04-15",
        &key,
    );
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post op_move_v");

    assert_eq!(audit_count(&pool, AUDIT_SQL).await, 0);
    assert_eq!(audit_count(&pool, ZERO_QTY_AUDIT_SQL).await, 0);
}

#[tokio::test]
async fn audit_clean_after_so_ship_qty_leg() {
    // so_ship's qty-leg is stock_available → ship_in_transit (qty class).
    // SKU resolves via either side. Use a minimal direct post.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    seed_stock(&pool, "SKU-A", "MAIN", 100).await;
    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;

    let ship_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts
            (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_in_transit', 'qty', s.id, l.id, 'debit'
           FROM skus s, locations l
          WHERE s.code = 'SKU-A' AND l.code = 'MAIN'
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let key = fresh_uuid(&pool).await;
    let event = make_event("so_ship", ship_qty, stock, 10, "2026-04-15", &key);
    call_post_posting_lines(&pool, json!([event]), false)
        .await
        .expect("post so_ship qty-leg");

    assert_eq!(audit_count(&pool, AUDIT_SQL).await, 0);
}

#[tokio::test]
async fn audit_query_detects_synthetic_violation() {
    // Bypasses the dispatcher to insert a row that violates the
    // invariant: qty IS NOT NULL on both sides have NULL sku_id. This
    // confirms the audit query DOES surface the shape it claims to.
    //
    // creation_void (qty) ↔ creation_void (qty) — both sides NULL
    // sku_id. Append-only trigger blocks UPDATE/DELETE but NOT INSERT,
    // so a direct INSERT goes through (the dispatcher would never
    // produce this combination, but the audit still has to be the net).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Create two distinct creation_void qty accounts (the fixture only
    // has one) so debit != credit constraint passes.
    let void1: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'creation_void'
            AND ledger_kind = 'qty' LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let void2: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, normal_side)
         VALUES ('creation_void', 'qty', 'unrestricted')
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let period_id: i64 =
        sqlx::query_scalar("SELECT id FROM periods WHERE opens_at <= DATE '2026-04-15'
                              AND closes_at >= DATE '2026-04-15'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let idem = fresh_uuid(&pool).await;

    sqlx::query(
        "INSERT INTO posting_lines
            (reason, document_kind, document_id, debit_account_id,
             credit_account_id, amount, qty, period_id, business_date,
             idempotency_key, posted_by)
         VALUES ('cycle_count_adj', 'test', gen_random_uuid(),
                 $1, $2, 5, 5, $3, '2026-04-15', $4::UUID,
                 '00000000-0000-0000-0000-0000000000bb'::UUID)",
    )
    .bind(void1)
    .bind(void2)
    .bind(period_id)
    .bind(&idem)
    .execute(&pool)
    .await
    .expect("synthetic violation insert");

    let n = audit_count(&pool, AUDIT_SQL).await;
    assert_eq!(n, 1, "audit query must detect the synthetic violation");
}
