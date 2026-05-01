//! `acct-x4t` / `acct-hlr.1` — smoke test for the standard-cost gate.
//!
//! Acceptance scope (per the bd issue):
//!   * Brand-new standard SKU with no standard_costs row: cost-relevant
//!     operations raise P0018.
//!   * Backdated business_date before earliest effective_at: raises P0018.
//!   * After seed_standard_cost helper inserts a row: same operation
//!     succeeds.
//!   * Existing fixture SKUs (SKU-A et al) still resolve via the new
//!     standard_costs table — verifies the data migration in 0027.
//!
//! The full gate matrix (multi-location, multi-currency, every dispatch
//! branch) lives in hlr.3 (acct-n0f).

mod common;

use common::*;

/// Insert a standard-method SKU mid-test (no standard_costs row).
/// Returns the new SKU's UUID as text.
async fn insert_standard_sku(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert sku {code}: {e}"))
}

/// Open the value account inv_value_fg(sku, MAIN, USD) so post_transfers
/// has a debit target. Reads MAIN's id from the fixture.
async fn open_inv_value_fg(pool: &sqlx::PgPool, sku_id: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = 'MAIN'
         RETURNING id",
    )
    .bind(sku_id)
    .fetch_one(pool)
    .await
    .expect("open inv_value_fg")
}

/// Open stock_available(sku, MAIN) so post_inventory_adjustment can find it.
async fn open_stock_available(pool: &sqlx::PgPool, sku_id: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
           FROM locations l WHERE l.code = 'MAIN'
         RETURNING id",
    )
    .bind(sku_id)
    .fetch_one(pool)
    .await
    .expect("open stock_available")
}

#[tokio::test]
async fn helper_raises_p0018_when_no_standard_exists() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = insert_standard_sku(&pool, "STD-NEW").await;

    expect_sqlstate("P0018", || async {
        sqlx::query("SELECT resolve_standard_cost_at($1::UUID, '2026-04-15'::DATE)")
            .bind(&sku_id)
            .execute(&pool)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn helper_raises_p0018_when_business_date_predates_first_standard() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = insert_standard_sku(&pool, "STD-FUTURE").await;
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, 50, '2026-04-01'::DATE,
                 '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid())",
    )
    .bind(&sku_id)
    .execute(&pool)
    .await
    .expect("seed future-effective standard");

    // Backdated query: '2026-03-15' < earliest effective_at '2026-04-01'.
    expect_sqlstate("P0018", || async {
        sqlx::query("SELECT resolve_standard_cost_at($1::UUID, '2026-03-15'::DATE)")
            .bind(&sku_id)
            .execute(&pool)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn helper_succeeds_when_standard_in_effect() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = insert_standard_sku(&pool, "STD-OK").await;
    seed_standard_cost(&pool, "STD-OK", 250).await;

    let cost: i64 =
        sqlx::query_scalar("SELECT resolve_standard_cost_at($1::UUID, '2026-04-15'::DATE)")
            .bind(&sku_id)
            .fetch_one(&pool)
            .await
            .expect("resolve");
    assert_eq!(cost, 250);
}

#[tokio::test]
async fn post_inventory_adjustment_blocks_unestablished_standard_sku_p0018() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = insert_standard_sku(&pool, "STD-ADJ-FAIL").await;
    let _ = open_stock_available(&pool, &sku_id).await;
    let _ = open_inv_value_fg(&pool, &sku_id).await;

    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(&pool).await;
    let idem = fresh_uuid(&pool).await;

    expect_sqlstate("P0018", || async {
        sqlx::query(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, 10, NULL::BIGINT, 'USD',
                'fg', '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL
             )",
        )
        .bind(&sku_id)
        .bind(&loc_id)
        .bind(&posted_by)
        .bind(&idem)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn post_inventory_adjustment_succeeds_after_seed_standard_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = insert_standard_sku(&pool, "STD-ADJ-OK").await;
    let _ = open_stock_available(&pool, &sku_id).await;
    let val_acct = open_inv_value_fg(&pool, &sku_id).await;
    seed_standard_cost(&pool, "STD-ADJ-OK", 75).await;

    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(&pool).await;
    let idem = fresh_uuid(&pool).await;

    let _: String = sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, 10, NULL::BIGINT, 'USD',
            'fg', '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL
         )::text",
    )
    .bind(&sku_id)
    .bind(&loc_id)
    .bind(&posted_by)
    .bind(&idem)
    .fetch_one(&pool)
    .await
    .expect("post_inventory_adjustment");

    // Value side posted at the seeded standard (75 * 10 = 750).
    let val_balance: i64 = sqlx::query_scalar(
        "SELECT debits_total - credits_total FROM accounts WHERE id = $1",
    )
    .bind(val_acct)
    .fetch_one(&pool)
    .await
    .expect("balance");
    assert_eq!(val_balance, 750);
}

#[tokio::test]
async fn post_transfers_value_event_blocks_unestablished_standard_sku_p0018() {
    // wo_complete is in the cost-relevant set — _post_transfers_compute_amount
    // is called for these reasons. With no standard cost established for the
    // SKU on the debit-side inv_value_fg, P0018 surfaces from the helper.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku_id = insert_standard_sku(&pool, "STD-TRF-FAIL").await;
    let val_acct = open_inv_value_fg(&pool, &sku_id).await;
    let void_val = account_id_by_kind_currency(&pool, "creation_void", Some("USD")).await;

    let event = serde_json::json!({
        "reason":            "wo_complete",
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  val_acct,
        "credit_account_id": void_val,
        "qty":               5,
        "business_date":     "2026-04-15",
        "idempotency_key":   fresh_uuid(&pool).await,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });

    expect_sqlstate("P0018", || async {
        call_post_transfers(&pool, serde_json::json!([event]), false)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn fixture_skus_resolve_via_migrated_standards() {
    // Sanity: the fixture data migration in 0027 + the seed.sql rewrite
    // means SKU-A through SKU-H still have their standard cost. SKU-A=100.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cost: i64 = sqlx::query_scalar(
        "SELECT resolve_standard_cost_at(s.id, '2026-04-15'::DATE)
           FROM skus s WHERE s.code = 'SKU-A'",
    )
    .fetch_one(&pool)
    .await
    .expect("resolve SKU-A");
    assert_eq!(cost, 100);
}
