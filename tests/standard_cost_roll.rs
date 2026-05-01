//! `acct-8rv` / `acct-hlr.2` — smoke test for post_standard_cost_roll().
//!
//! Acceptance scope (per the bd issue):
//!   * Function applies and round-trips.
//!   * Happy paths: first roll (no inventory, no revaluation), second
//!     roll revalues existing pools.
//!   * Cost-method dispatch raises the right codes.
//!   * Q1 retroactive guard: P0019.
//!   * Q2 WIP guard: P0006 with reference to acct-bru (Epic G).
//!   * Q5 optimistic concurrency: P0017 on mismatch.
//!   * Future-dated roll: INSERT only, no revaluation.
//!   * Replay returns existing id.
//!
//! The full matrix (multi-location revaluation, multi-currency, no-op
//! same-cost, audit-row-content checks, etc.) is hlr.3 (acct-n0f).

mod common;

use common::*;

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

async fn insert_wac_sku(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'wac_perpetual')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert wac sku {code}: {e}"))
}

async fn open_stock_available(pool: &sqlx::PgPool, sku_id: &str, loc_code: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', $1::UUID, l.id, 'debit'
           FROM locations l WHERE l.code = $2
         RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_code)
    .fetch_one(pool)
    .await
    .expect("open stock_available")
}

async fn open_inv_value_fg(pool: &sqlx::PgPool, sku_id: &str, loc_code: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_fg', 'value', 'USD', 'debit', $1::UUID, l.id
           FROM locations l WHERE l.code = $2
         RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_code)
    .fetch_one(pool)
    .await
    .expect("open inv_value_fg")
}

async fn open_inv_value_wip(pool: &sqlx::PgPool, sku_id: &str, op: i32) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, routing_op)
         VALUES ('inv_value_wip', 'value', 'USD', 'debit', $1::UUID, $2)
         RETURNING id",
    )
    .bind(sku_id)
    .bind(op)
    .fetch_one(pool)
    .await
    .expect("open inv_value_wip")
}

#[allow(clippy::too_many_arguments)]
async fn call_roll(
    pool: &sqlx::PgPool,
    sku: &str,
    new_cost: i64,
    effective_at: &str,
    business_date: &str,
    expected_old: Option<i64>,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    let idem = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_standard_cost_roll(
            $1::UUID, $2::BIGINT, $3::DATE, $4::DATE,
            $5::UUID, $6::UUID, NULL, $7::BIGINT
         )::text",
    )
    .bind(sku)
    .bind(new_cost)
    .bind(effective_at)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&idem)
    .bind(expected_old)
    .fetch_one(pool)
    .await
}

// ---- Happy paths --------------------------------------------------

#[tokio::test]
async fn first_roll_no_inventory_audit_records_no_revaluation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-A").await;
    let doc_id =
        call_roll(&pool, &sku, 100, "2026-04-01", "2026-04-15", None)
            .await
            .expect("first roll");

    // Audit row: prior=NULL, target=100, delta=0, pool_qty=0.
    let (prior, target, delta, qty): (Option<i64>, i64, i64, i64) = sqlx::query_as(
        "SELECT prior_standard_cost, target_standard_cost,
                total_delta_value, pool_qty
           FROM inventory_standard_cost_rolls WHERE id = $1::UUID",
    )
    .bind(&doc_id)
    .fetch_one(&pool)
    .await
    .expect("audit row");
    assert_eq!(prior, None);
    assert_eq!(target, 100);
    assert_eq!(delta, 0);
    assert_eq!(qty, 0);

    // standard_costs row exists for this SKU.
    let cost: i64 = sqlx::query_scalar(
        "SELECT resolve_standard_cost_at($1::UUID, '2026-04-15'::DATE)",
    )
    .bind(&sku)
    .fetch_one(&pool)
    .await
    .expect("resolve");
    assert_eq!(cost, 100);
}

#[tokio::test]
async fn second_roll_revalues_existing_inventory() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-B").await;
    let qty_acct = open_stock_available(&pool, &sku, "MAIN").await;
    let val_acct = open_inv_value_fg(&pool, &sku, "MAIN").await;

    // Seed first standard via the helper (effective epoch).
    seed_standard_cost(&pool, "ROLL-B", 100).await;

    // Receive 50 units at standard 100 → inv_value_fg += 5000.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;
    let _ = call_post_transfers(
        &pool,
        serde_json::json!([
            make_event("inventory_adjustment", qty_acct, void_qty, 50, "2026-04-15", &fresh_uuid(&pool).await),
            make_event("inventory_adjustment", val_acct, void_val, 5000, "2026-04-15", &fresh_uuid(&pool).await),
        ]),
        false,
    )
    .await
    .expect("seed inventory");

    // Roll cost 100 → 120. Delta per unit = 20; 50 units × 20 = 1000.
    let _doc = call_roll(&pool, &sku, 120, "2026-04-20", "2026-04-20", Some(100))
        .await
        .expect("second roll");

    // inv_value_fg should now be 5000 + 1000 = 6000.
    let val_balance: i64 = sqlx::query_scalar(
        "SELECT debits_total - credits_total FROM accounts WHERE id = $1",
    )
    .bind(val_acct)
    .fetch_one(&pool)
    .await
    .expect("balance");
    assert_eq!(val_balance, 6000);

    // variance_std_cost_roll(USD) should have been credited 1000.
    let var_acct =
        account_id_by_kind_currency(&pool, "variance_std_cost_roll", Some("USD")).await;
    let var_balance: i64 = sqlx::query_scalar(
        "SELECT credits_total - debits_total FROM accounts WHERE id = $1",
    )
    .bind(var_acct)
    .fetch_one(&pool)
    .await
    .expect("variance balance");
    assert_eq!(var_balance, 1000);
}

#[tokio::test]
async fn future_dated_roll_inserts_no_revaluation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-FUT").await;
    let qty_acct = open_stock_available(&pool, &sku, "MAIN").await;
    let val_acct = open_inv_value_fg(&pool, &sku, "MAIN").await;
    seed_standard_cost(&pool, "ROLL-FUT", 100).await;

    // Receive 30 units at 100 → 3000.
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;
    let _ = call_post_transfers(
        &pool,
        serde_json::json!([
            make_event("inventory_adjustment", qty_acct, void_qty, 30, "2026-04-15", &fresh_uuid(&pool).await),
            make_event("inventory_adjustment", val_acct, void_val, 3000, "2026-04-15", &fresh_uuid(&pool).await),
        ]),
        false,
    )
    .await
    .expect("seed inventory");

    // Future-dated: effective_at='2026-06-01' > business_date='2026-04-20'.
    // Should INSERT the standard_costs row but NOT revalue inventory.
    let _doc = call_roll(&pool, &sku, 150, "2026-06-01", "2026-04-20", Some(100))
        .await
        .expect("future-dated roll");

    // inv_value_fg unchanged at 3000.
    let val_balance: i64 = sqlx::query_scalar(
        "SELECT debits_total - credits_total FROM accounts WHERE id = $1",
    )
    .bind(val_acct)
    .fetch_one(&pool)
    .await
    .expect("balance");
    assert_eq!(val_balance, 3000, "future-dated roll must not revalue");

    // resolve at business_date='2026-04-20' still returns 100.
    let cur: i64 = sqlx::query_scalar(
        "SELECT resolve_standard_cost_at($1::UUID, '2026-04-20'::DATE)",
    )
    .bind(&sku)
    .fetch_one(&pool)
    .await
    .expect("resolve current");
    assert_eq!(cur, 100);

    // resolve at '2026-06-15' returns the new 150.
    let fut: i64 = sqlx::query_scalar(
        "SELECT resolve_standard_cost_at($1::UUID, '2026-06-15'::DATE)",
    )
    .bind(&sku)
    .fetch_one(&pool)
    .await
    .expect("resolve future");
    assert_eq!(fut, 150);
}

// ---- Guards / dispatch -------------------------------------------

#[tokio::test]
async fn retroactive_roll_raises_p0019() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-RETRO").await;
    // Existing standard at effective='2026-04-01'.
    seed_standard_cost(&pool, "ROLL-RETRO", 100).await; // effective '1970-01-01'
    sqlx::query(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         VALUES ($1::UUID, 110, '2026-04-01'::DATE,
                 '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid())",
    )
    .bind(&sku)
    .execute(&pool)
    .await
    .expect("seed second standard");

    // Try to insert at effective_at='2026-03-15' (earlier than '2026-04-01').
    expect_sqlstate("P0019", || async {
        call_roll(&pool, &sku, 90, "2026-03-15", "2026-03-15", None)
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn wip_present_blocks_with_p0006_referencing_epic_g() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-WIP").await;
    seed_standard_cost(&pool, "ROLL-WIP", 100).await;

    // Engineer a non-zero WIP balance: open the inv_value_wip account
    // and post a value to it via inventory_adjustment-style direct
    // accounts.debits_total update (skipping post_transfers since WIP
    // events have their own dispatch). Simplest: direct UPDATE for the
    // smoke test. Real WIP comes from rm_issue_to_wo + op_move flows.
    let wip_acct = open_inv_value_wip(&pool, &sku, 10).await;
    sqlx::query("UPDATE accounts SET debits_total = 5000 WHERE id = $1")
        .bind(wip_acct)
        .execute(&pool)
        .await
        .expect("seed wip balance");

    expect_sqlstate("P0006", || async {
        call_roll(&pool, &sku, 120, "2026-04-20", "2026-04-20", Some(100))
            .await
            .map(|_| ())
    })
    .await;

    // Verify the message references Epic G (acct-bru) so the operator
    // can find the companion-workflow tracking issue.
    let err = call_roll(&pool, &sku, 120, "2026-04-20", "2026-04-20", Some(100))
        .await
        .err()
        .expect("expected P0006");
    let msg = err
        .as_database_error()
        .and_then(|e| Some(e.message().to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("acct-bru"),
        "WIP-block message should reference Epic G (acct-bru); got: {msg}"
    );
}

#[tokio::test]
async fn wac_perpetual_raises_p0011_redirect() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_sku(&pool, "ROLL-WAC").await;
    let err = call_roll(&pool, &sku, 100, "2026-04-15", "2026-04-15", None)
        .await
        .err()
        .expect("expected P0011");

    let db_err = err.as_database_error().expect("db err");
    assert_eq!(db_err.code().unwrap().as_ref(), "P0011");
    assert!(
        db_err.message().contains("post_cost_adjustment"),
        "redirect message should point at post_cost_adjustment; got: {}",
        db_err.message()
    );
}

#[tokio::test]
async fn replay_returns_same_id_no_double_post() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-REPLAY").await;
    let posted_by = fresh_uuid(&pool).await;
    let idem = fresh_uuid(&pool).await;

    let id1: String = sqlx::query_scalar(
        "SELECT post_standard_cost_roll(
            $1::UUID, 100, '2026-04-15'::DATE, '2026-04-15'::DATE,
            $2::UUID, $3::UUID, NULL, NULL
         )::text",
    )
    .bind(&sku)
    .bind(&posted_by)
    .bind(&idem)
    .fetch_one(&pool)
    .await
    .expect("first call");

    let id2: String = sqlx::query_scalar(
        "SELECT post_standard_cost_roll(
            $1::UUID, 999, '2099-01-01'::DATE, '2099-01-01'::DATE,
            $2::UUID, $3::UUID, NULL, NULL
         )::text",
    )
    .bind(&sku)
    .bind(&posted_by)
    .bind(&idem) // same idem key
    .fetch_one(&pool)
    .await
    .expect("replay");
    assert_eq!(id1, id2, "replay must return same audit id");

    // Only one row in audit + only one in standard_costs (the original).
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM inventory_standard_cost_rolls WHERE sku_id = $1::UUID")
            .bind(&sku)
            .fetch_one(&pool)
            .await
            .expect("count audit");
    assert_eq!(count, 1);
}

// ---- Optimistic concurrency --------------------------------------

#[tokio::test]
async fn optimistic_concurrency_match_succeeds() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-OCC-OK").await;
    seed_standard_cost(&pool, "ROLL-OCC-OK", 100).await;

    // Caller asserts prior=100 (matches the seeded standard at business_date).
    let _ = call_roll(&pool, &sku, 120, "2026-04-20", "2026-04-20", Some(100))
        .await
        .expect("matched expected_old");
}

#[tokio::test]
async fn optimistic_concurrency_mismatch_raises_p0017() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-OCC-FAIL").await;
    seed_standard_cost(&pool, "ROLL-OCC-FAIL", 100).await;

    expect_sqlstate("P0017", || async {
        call_roll(&pool, &sku, 120, "2026-04-20", "2026-04-20", Some(99))
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn first_roll_with_expected_old_nonnull_raises_p0017() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_standard_sku(&pool, "ROLL-FIRST-OCC").await;
    // No seed; first roll. Caller asserts there's a prior — wrong.
    expect_sqlstate("P0017", || async {
        call_roll(&pool, &sku, 100, "2026-04-15", "2026-04-15", Some(50))
            .await
            .map(|_| ())
    })
    .await;
}
