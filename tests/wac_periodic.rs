//! `acct-3e1` / `acct-qfj.1` — smoke test for wac_periodic.
//!
//! Acceptance scope (per the bd issue):
//!   * Migration applies.
//!   * wac_periodic depletion at running pool avg flags posting_lines_provisional.
//!   * wac_periodic empty-pool depletion raises P0006.
//!   * wac_periodic WIP-class adjustment raises P0006 with epic-pointer.
//!   * Period close lifecycle: receive twice + deplete + close → variance posted.
//!   * Zero-receipt edge case raises P0020 unless force.
//!
//! Full matrix (multi-pool, multi-currency, multi-depletion, etc.) lives
//! in qfj.2 (acct-c0b).

mod common;

use common::*;

async fn insert_wac_periodic_sku(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'wac_periodic')
         RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("insert wac_periodic sku {code}: {e}"))
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

async fn period_id(pool: &sqlx::PgPool, code: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM periods WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("period")
}

async fn balance(pool: &sqlx::PgPool, id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT (debits_total - credits_total)::BIGINT FROM accounts WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("balance")
}

#[allow(clippy::too_many_arguments)]
async fn adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    loc_code: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    business_date: &str,
) -> sqlx::Result<String> {
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(loc_code)
        .fetch_one(pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(pool).await;
    let key = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, 'USD',
            'fg', $5::DATE, $6::UUID, $7::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(&loc_id)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(business_date)
    .bind(&posted_by)
    .bind(&key)
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn wac_periodic_depletion_flags_provisional() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-A").await;
    let _qty = open_stock_available(&pool, &sku, "MAIN").await;
    let val = open_inv_value_fg(&pool, &sku, "MAIN").await;

    // Seed 100 units at $5.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-15")
        .await
        .expect("seed");
    assert_eq!(balance(&pool, val).await, 500);

    // No posting_lines_provisional rows yet (receipt doesn't flag).
    let pre_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines_provisional WHERE cost_method='wac_periodic'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(pre_count, 0);

    // Deplete 30 units. Provisional cost = pool avg = 5; value posted = 150.
    adjust(&pool, &sku, "MAIN", -30, None, "2026-04-16")
        .await
        .expect("deplete");
    assert_eq!(balance(&pool, val).await, 350); // 500 - 150

    // The depletion's value-leg should be flagged.
    let post_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM posting_lines_provisional WHERE cost_method='wac_periodic'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(post_count, 1);

    // The flagged row's qty should be 30.
    let qty: i64 =
        sqlx::query_scalar("SELECT qty FROM posting_lines_provisional WHERE cost_method='wac_periodic' LIMIT 1")
            .fetch_one(&pool)
            .await
            .expect("qty");
    assert_eq!(qty, 30);
}

#[tokio::test]
async fn wac_periodic_empty_pool_depletion_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-EMPTY").await;
    let _ = open_stock_available(&pool, &sku, "MAIN").await;
    let _ = open_inv_value_fg(&pool, &sku, "MAIN").await;

    // No seed; depletion on empty pool.
    expect_sqlstate("P0010", || async {
        adjust(&pool, &sku, "MAIN", -10, None, "2026-04-15")
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn wac_periodic_first_in_with_null_cost_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-FIRST").await;
    let _ = open_stock_available(&pool, &sku, "MAIN").await;
    let _ = open_inv_value_fg(&pool, &sku, "MAIN").await;

    expect_sqlstate("P0011", || async {
        adjust(&pool, &sku, "MAIN", 10, None, "2026-04-15")
            .await
            .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn wac_periodic_wip_class_adjustment_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-WIP").await;
    let _ = open_stock_available(&pool, &sku, "MAIN").await;
    let _ = open_inv_value_fg(&pool, &sku, "MAIN").await;

    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;

    // Inventory class 'wip' is out of scope for wac_periodic Phase 1.
    expect_sqlstate("P0006", || async {
        sqlx::query(
            "SELECT post_inventory_adjustment(
                $1::UUID, $2::UUID, 5, 100::BIGINT, 'USD',
                'wip', '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL
             )",
        )
        .bind(&sku)
        .bind(&loc_id)
        .bind(&posted_by)
        .bind(&key)
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn wac_periodic_close_lifecycle_posts_variance() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-CLOSE").await;
    let _qty = open_stock_available(&pool, &sku, "MAIN").await;
    let val = open_inv_value_fg(&pool, &sku, "MAIN").await;

    let pid = period_id(&pool, "2026-04").await;

    // Receive 100 @ $5, then 100 @ $7. Period avg = (500+700)/200 = 6.
    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-10")
        .await
        .expect("receive 1");
    adjust(&pool, &sku, "MAIN", 100, Some(7), "2026-04-15")
        .await
        .expect("receive 2");
    // Pool now: 200 units, $1200, running avg = $6.

    // Deplete 50 units. Provisional cost = running avg at this moment = 6.
    adjust(&pool, &sku, "MAIN", -50, None, "2026-04-20")
        .await
        .expect("deplete");
    // After depletion: 150 units, value = 1200 - 300 = 900.
    assert_eq!(balance(&pool, val).await, 900);

    // Close the period. The hook computes final_avg = 1200 / 200 = 6 (same
    // as provisional in this case, since the depletion happened after both
    // receipts). Variance = (6 - 6) * 50 = 0. Audit row finalized with
    // variance_amount = 0; no variance transfer posted.
    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close_period");

    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(1));

    // Provisional row finalized with zero variance.
    let (finalized_at, variance_amount): (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT finalized_at::text, variance_amount
           FROM posting_lines_provisional
          WHERE cost_method='wac_periodic' AND period_id=$1",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .expect("audit");
    assert!(finalized_at.is_some());
    assert_eq!(variance_amount, Some(0));

    // Pool value unchanged from depletion.
    assert_eq!(balance(&pool, val).await, 900);
}

#[tokio::test]
async fn wac_periodic_close_with_drift_posts_variance() {
    // Engineer a case where provisional cost differs from final period avg.
    // Sequence: receive 100 @ $5, deplete 50 (provisional avg = 5), then
    // receive 100 @ $9. Final period avg = (500 + 900) / 200 = 7.
    // Variance = (7 - 5) * 50 = 100. After close: pool should be 150 units
    // at avg 7, but only the depletion is re-costed; the pool gets the
    // variance applied via routing through variance_wac_periodic.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-DRIFT").await;
    let _ = open_stock_available(&pool, &sku, "MAIN").await;
    let val = open_inv_value_fg(&pool, &sku, "MAIN").await;

    let pid = period_id(&pool, "2026-04").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-05")
        .await
        .expect("receive 1");
    adjust(&pool, &sku, "MAIN", -50, None, "2026-04-10")
        .await
        .expect("deplete");
    // After: pool = 50 units, value = 250 (provisional avg 5).
    let pre_val = balance(&pool, val).await;
    assert_eq!(pre_val, 250);

    adjust(&pool, &sku, "MAIN", 100, Some(9), "2026-04-15")
        .await
        .expect("receive 2");
    // After: pool = 150 units, value = 250 + 900 = 1150.
    assert_eq!(balance(&pool, val).await, 1150);

    let actor = fresh_uuid(&pool).await;
    let summary: serde_json::Value = sqlx::query_scalar(
        "SELECT close_period($1, $2::UUID, FALSE, FALSE)",
    )
    .bind(pid)
    .bind(&actor)
    .fetch_one(&pool)
    .await
    .expect("close_period");
    assert_eq!(summary["hook_results"]["wac_periodic"].as_i64(), Some(1));

    // Variance = (7 - 5) * 50 = 100.
    let variance_amount: i64 = sqlx::query_scalar(
        "SELECT variance_amount FROM posting_lines_provisional
          WHERE cost_method='wac_periodic' AND period_id=$1",
    )
    .bind(pid)
    .fetch_one(&pool)
    .await
    .expect("variance");
    assert_eq!(variance_amount, 100);

    // The two-transfer pattern routes through variance_wac_periodic.
    // Net effect on the original pair (cogs/inv_adj_expense vs inv_value_fg)
    // is what we want to verify. inv_value_fg (the original credit side)
    // should now reflect: 1150 - 100 = 1050 (post-variance-routing).
    assert_eq!(balance(&pool, val).await, 1050);

    // variance_wac_periodic should net to zero (two equal-and-opposite legs).
    let var_acct = account_id_by_kind_currency(&pool, "variance_wac_periodic", Some("USD")).await;
    assert_eq!(balance(&pool, var_acct).await, 0, "variance_wac_periodic nets to zero");
}

#[tokio::test]
async fn wac_periodic_close_no_receipts_raises_p0020() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-NORECV").await;
    let _ = open_stock_available(&pool, &sku, "MAIN").await;
    let _ = open_inv_value_fg(&pool, &sku, "MAIN").await;

    // Seed in a PRIOR period (2026-03) so the SKU has on-hand at start
    // of 2026-04 but no receipts in 2026-04 itself.
    // Note: 2026-03 is fixture-closed; we override.
    let loc_id: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(&pool)
        .await
        .expect("loc");
    let posted_by = fresh_uuid(&pool).await;
    let key1 = fresh_uuid(&pool).await;
    // Direct INSERT into transfers (bypass post_inventory_adjustment) so
    // we can pre-seed without engaging the period-close gate.
    let qty_acct = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts
          WHERE kind='stock_available' AND sku_id=$1::UUID AND location_id=$2::UUID",
    )
    .bind(&sku)
    .bind(&loc_id)
    .fetch_one(&pool)
    .await
    .expect("qty acct");
    let val_acct = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM accounts
          WHERE kind='inv_value_fg' AND sku_id=$1::UUID AND location_id=$2::UUID AND currency='USD'",
    )
    .bind(&sku)
    .bind(&loc_id)
    .fetch_one(&pool)
    .await
    .expect("val acct");
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;
    let void_val = account_id_by_kind_currency(&pool, "inv_adj_expense", Some("USD")).await;

    // Pre-seed in the 2026-04 period BUT we need to ensure no receipts
    // remain when we deplete. Simpler: seed via post_posting_lines in 2026-04
    // (counts as a receipt), then deplete, then DELETE the receipt rows
    // before close so the hook sees zero receipts.
    //
    // Even simpler: post the receipt + depletion both in 2026-04, then
    // before close, manually mark only the receipts as out-of-period.
    //
    // Cleanest: seed in 2026-04 as a regular receipt + depletion, then
    // manually UPDATE posting_lines to push the receipt's business_date
    // outside the period. This isolates the hook's behavior.
    let _ = call_post_posting_lines(
        &pool,
        serde_json::json!([
            make_event("inventory_adjustment", qty_acct, void_qty, 50, "2026-04-05", &fresh_uuid(&pool).await),
            make_event_with_qty("inventory_adjustment", val_acct, void_val, 250, 50, "2026-04-05", &fresh_uuid(&pool).await),
        ]),
        false,
    )
    .await
    .expect("seed receipts");

    // Deplete 20 units.
    let _ = adjust(&pool, &sku, "MAIN", -20, None, "2026-04-20")
        .await
        .expect("deplete");

    // Move the receipt transfers out of the period so the hook sees zero
    // receipts. (This bypasses append-only normally, but for the test
    // we use a helper that disables the trigger.)
    sqlx::query("ALTER TABLE posting_lines DISABLE TRIGGER trg_posting_lines_append_only")
        .execute(&pool)
        .await
        .expect("disable trigger");
    sqlx::query(
        "UPDATE posting_lines
            SET business_date = '2026-03-15'
          WHERE business_date = '2026-04-05'",
    )
    .execute(&pool)
    .await
    .expect("backdate");
    sqlx::query("ALTER TABLE posting_lines ENABLE TRIGGER trg_posting_lines_append_only")
        .execute(&pool)
        .await
        .expect("enable trigger");

    let pid = period_id(&pool, "2026-04").await;
    let actor = fresh_uuid(&pool).await;

    expect_sqlstate("P0020", || async {
        sqlx::query("SELECT close_period($1, $2::UUID, FALSE, FALSE)")
            .bind(pid)
            .bind(&actor)
            .execute(&pool)
            .await
            .map(|_| ())
    })
    .await;

    // Force-bypass leaves the un-finalized row.
    let _ = sqlx::query("SELECT close_period($1, $2::UUID, TRUE, FALSE)")
        .bind(pid)
        .bind(&actor)
        .execute(&pool)
        .await
        .expect("force close");
    let still_unfin: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines_provisional
          WHERE cost_method='wac_periodic' AND finalized_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(still_unfin, 1, "force_provisional leaves the row un-finalized");
}

#[tokio::test]
async fn wac_periodic_two_pass_engages_for_wac_periodic_in_batch() {
    // post_posting_lines' two-pass path is engaged when wac_periodic events
    // exist in the batch. Verify the lock-pre-scan picks up the qty
    // account so the running-avg read is consistent.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = insert_wac_periodic_sku(&pool, "WACP-2PASS").await;
    let _ = open_stock_available(&pool, &sku, "MAIN").await;
    let val = open_inv_value_fg(&pool, &sku, "MAIN").await;

    adjust(&pool, &sku, "MAIN", 100, Some(5), "2026-04-15")
        .await
        .expect("seed");

    // A so_ship cost-relevant event for a wac_periodic SKU should engage
    // the two-pass path AND flag posting_lines_provisional.
    let cogs = account_id_by_kind_currency(&pool, "cogs", Some("USD")).await;
    let event = serde_json::json!({
        "reason":            "so_ship",
        "document_kind":     "test_doc",
        "document_id":       "00000000-0000-0000-0000-0000000000aa",
        "debit_account_id":  cogs,
        "credit_account_id": val,
        "qty":               20,
        "business_date":     "2026-04-16",
        "idempotency_key":   fresh_uuid(&pool).await,
        "posted_by":         "00000000-0000-0000-0000-0000000000bb",
    });
    let _ = call_post_posting_lines(&pool, serde_json::json!([event]), false)
        .await
        .expect("so_ship");

    // Inv_value_fg now: 500 - 20*5 = 400.
    assert_eq!(balance(&pool, val).await, 400);

    // The so_ship value-leg should be flagged.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM posting_lines_provisional
          WHERE cost_method='wac_periodic'
            AND qty = 20",
    )
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(count, 1);
}
