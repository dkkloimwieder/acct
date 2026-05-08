//! T1 probes for FIFO via `post_inventory_adjustment` (mig 0035, acct-8skl).
//! Phase E1 follow-up W2.
//!
//! Exercises both directions:
//!   - +qty (adjustment-in): caller supplies unit_cost → creates a layer.
//!   - -qty (adjustment-out): wrapper walks layers under FOR UPDATE,
//!     emits value-leg with walked total; apply_event re-walks for
//!     depletion writeback.
//!
//! MVP restricts FIFO adjustment to inventory_class='raw'; 'fg'/'wip'
//! raise P0006 pointing at W4 (FG-FIFO via post_so_ship).

mod common;

use common::*;

async fn sku_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn loc_id(pool: &sqlx::PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap()
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

/// Open the inv_value_raw account for SKU-FIF/MAIN/USD that the fixture
/// doesn't pre-create. Returns the account id.
async fn open_inv_value_raw_for_fif(pool: &sqlx::PgPool, sku: &str, loc: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(sku)
    .bind(loc)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn adjust(
    pool: &sqlx::PgPool,
    sku: &str,
    loc: &str,
    qty_delta: i64,
    unit_cost: Option<i64>,
    currency: &str,
    inv_class: &str,
    business_date: &str,
    idempotency_key: &str,
) -> sqlx::Result<String> {
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "SELECT post_inventory_adjustment(
            $1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, $5,
            $6, $7::DATE, $8::UUID, $9::UUID, NULL
         )::text",
    )
    .bind(sku)
    .bind(loc)
    .bind(qty_delta)
    .bind(unit_cost)
    .bind(currency)
    .bind(inv_class)
    .bind(business_date)
    .bind(&posted_by)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await
}

// ============================================================
// W2.1: +qty creates exactly one layer at supplied unit_cost.
// ============================================================

#[tokio::test]
async fn fifo_in_creates_one_layer_at_supplied_unit_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-FIF", "MAIN").await;
    let val_acct = open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    let key = fresh_uuid(&pool).await;
    adjust(&pool, &sku, &loc, 8, Some(150), "USD", "raw", "2026-04-15", &key)
        .await
        .expect("FIFO adjustment-in");

    assert_eq!(balance(&pool, qty_acct).await, 8);
    assert_eq!(balance(&pool, val_acct).await, 8 * 150);

    let layers: Vec<(String, String)> = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT FROM cost_layers
         WHERE product_id = $1::UUID AND location_id = $2::UUID
         ORDER BY layer_id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(layers.len(), 1);
    assert!(layers[0].0.starts_with("8"));
    assert!(layers[0].1.starts_with("150"));
}

// ============================================================
// W2.2: +qty without unit_cost raises P0011.
// ============================================================

#[tokio::test]
async fn fifo_in_without_unit_cost_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    let key = fresh_uuid(&pool).await;
    let res = adjust(&pool, &sku, &loc, 5, None, "USD", "raw", "2026-04-15", &key).await;
    let err = res.expect_err("FIFO adjustment-in without unit_cost must raise");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned()).as_deref(),
        Some("P0011"),
    );
}

// ============================================================
// W2.3: -qty walks one layer when sufficient.
// ============================================================

#[tokio::test]
async fn fifo_out_walks_one_layer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    let qty_acct = account_id_stock_available(&pool, "SKU-FIF", "MAIN").await;
    let val_acct = open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    // Seed 10 @ 100.
    adjust(&pool, &sku, &loc, 10, Some(100), "USD", "raw", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed");

    // Issue 4 → consumes from the single layer.
    adjust(&pool, &sku, &loc, -4, None, "USD", "raw", "2026-04-16",
           &fresh_uuid(&pool).await).await.expect("issue");

    assert_eq!(balance(&pool, qty_acct).await, 6);
    assert_eq!(balance(&pool, val_acct).await, 600);

    // One depletion of 4 @ 100 = 400.
    let (dq, uc, ca): (String, String, String) = sqlx::query_as(
        "SELECT depleted_quantity::TEXT, unit_cost::TEXT, cost_amount::TEXT
           FROM cost_layer_depletions
          WHERE layer_id IN (SELECT layer_id FROM cost_layers
                              WHERE product_id = $1::UUID
                                AND location_id = $2::UUID)",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(dq.starts_with("4"), "depleted_qty={dq}");
    assert!(uc.starts_with("100"), "unit_cost={uc}");
    assert!(ca.starts_with("400"), "cost_amount={ca}");
}

// ============================================================
// W2.4: -qty spans multiple layers in FIFO order; cost_amount
// matches posting_line.amount exactly.
// ============================================================

#[tokio::test]
async fn fifo_out_spans_layers_in_fifo_order() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    let val_acct = open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    // Layer 1: 5 @ 100 (= 500), Layer 2: 5 @ 200 (= 1000).
    adjust(&pool, &sku, &loc, 5, Some(100), "USD", "raw", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed 1");
    adjust(&pool, &sku, &loc, 5, Some(200), "USD", "raw", "2026-04-16",
           &fresh_uuid(&pool).await).await.expect("seed 2");

    // Issue 8 → 5 from L1 ($500) + 3 from L2 ($600) = $1100.
    adjust(&pool, &sku, &loc, -8, None, "USD", "raw", "2026-04-17",
           &fresh_uuid(&pool).await).await.expect("issue");

    // Pool: 10 qty / $1500. Issue 8 → consumes $500 (L1) + $600 (L2) = $1100.
    // Remaining: 2 qty / $400 (2 @ $200 from L2).
    assert_eq!(balance(&pool, val_acct).await, 400, "remaining: 2 @ 200 = 400");

    // Depletions ordered by layer (layer_id is a sequence, so L1 < L2).
    let depletions: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT d.depleted_quantity::TEXT, d.unit_cost::TEXT, d.cost_amount::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID
          ORDER BY cl.layer_id, d.depletion_id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 2, "2 depletions expected");
    assert!(depletions[0].0.starts_with("5") && depletions[0].1.starts_with("100"));
    assert!(depletions[1].0.starts_with("3") && depletions[1].1.starts_with("200"));

    // SUM(cost_amount) = posting_line.amount on the value-leg credit.
    let pl_amount: i64 = sqlx::query_scalar(
        "SELECT pl.amount FROM posting_lines pl
          WHERE pl.credit_account_id = $1
            AND pl.reason = 'inventory_adjustment'
            AND pl.qty IS NOT NULL
          ORDER BY pl.id DESC LIMIT 1",
    )
    .bind(val_acct)
    .fetch_one(&pool)
    .await
    .unwrap();
    let dep_sum: String = sqlx::query_scalar(
        "SELECT COALESCE(SUM(d.cost_amount), 0)::TEXT
           FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep_sum.split('.').next().unwrap(), pl_amount.to_string());
    assert_eq!(pl_amount, 1100);
}

// ============================================================
// W2.5: -qty with asserted unit_cost raises P0011.
// ============================================================

#[tokio::test]
async fn fifo_out_with_asserted_unit_cost_raises_p0011() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    adjust(&pool, &sku, &loc, 10, Some(50), "USD", "raw", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed");

    let res = adjust(&pool, &sku, &loc, -3, Some(60), "USD", "raw", "2026-04-16",
                     &fresh_uuid(&pool).await).await;
    let err = res.expect_err("FIFO depletion with asserted unit_cost must raise");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned()).as_deref(),
        Some("P0011"),
    );
}

// ============================================================
// W2.6: -qty with insufficient pool raises P0006 (layers exhausted).
// ============================================================

#[tokio::test]
async fn fifo_out_exhausted_layers_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    adjust(&pool, &sku, &loc, 5, Some(100), "USD", "raw", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("seed");

    // Try to issue 10 — qty-leg's stock_available CHECK (no negative
    // inventory) fires BEFORE the value-leg's FIFO walk. SQLSTATE 23514.
    let res = adjust(&pool, &sku, &loc, -10, None, "USD", "raw", "2026-04-16",
                     &fresh_uuid(&pool).await).await;
    let err = res.expect_err("over-depletion must raise");
    let code = err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned());
    assert!(
        code.as_deref() == Some("23514") || code.as_deref() == Some("P0006"),
        "expected 23514 or P0006, got {code:?}",
    );
}

// ============================================================
// W2.7: FG class still raises P0006 (FG-FIFO is W4 follow-up).
// ============================================================

#[tokio::test]
async fn fifo_fg_class_raises_p0006() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;

    // Open a synthetic inv_value_fg account so the missing-account
    // guard doesn't fire before the cost-method gate.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_fg', 'value', 'USD', $1::UUID, $2::UUID, 'debit')",
    )
    .bind(&sku)
    .bind(&loc)
    .execute(&pool)
    .await
    .unwrap();

    let res = adjust(&pool, &sku, &loc, 3, Some(100), "USD", "fg", "2026-04-15",
                     &fresh_uuid(&pool).await).await;
    let err = res.expect_err("FIFO + fg class must raise P0006");
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).map(|c| c.into_owned()).as_deref(),
        Some("P0006"),
    );
}

// ============================================================
// W2.8: idempotent replay — same key returns same doc; layer not
// duplicated; depletion not duplicated.
// ============================================================

#[tokio::test]
async fn fifo_idempotent_replay_no_duplicate_state() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    let in_key = fresh_uuid(&pool).await;
    let doc1_in = adjust(&pool, &sku, &loc, 6, Some(75), "USD", "raw", "2026-04-15", &in_key)
        .await
        .expect("first in");
    let doc2_in = adjust(&pool, &sku, &loc, 6, Some(75), "USD", "raw", "2026-04-15", &in_key)
        .await
        .expect("replay in");
    assert_eq!(doc1_in, doc2_in);

    let layer_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layers
         WHERE product_id = $1::UUID AND location_id = $2::UUID",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layer_count, 1, "replay must not duplicate the layer");

    let out_key = fresh_uuid(&pool).await;
    let doc1_out = adjust(&pool, &sku, &loc, -2, None, "USD", "raw", "2026-04-16", &out_key)
        .await
        .expect("first out");
    let doc2_out = adjust(&pool, &sku, &loc, -2, None, "USD", "raw", "2026-04-16", &out_key)
        .await
        .expect("replay out");
    assert_eq!(doc1_out, doc2_out);

    let dep_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layer_depletions d
           JOIN cost_layers cl ON cl.layer_id = d.layer_id
                              AND cl.receipt_date = d.layer_receipt_date
          WHERE cl.product_id = $1::UUID AND cl.location_id = $2::UUID",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dep_count, 1, "replay must not duplicate depletion");
}

// ============================================================
// W2.9: recon check #8 stays clean across full FIFO adjust cycle.
// ============================================================

#[tokio::test]
async fn fifo_adjust_passes_recon() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let sku = sku_id(&pool, "SKU-FIF").await;
    let loc = loc_id(&pool, "MAIN").await;
    open_inv_value_raw_for_fif(&pool, &sku, &loc).await;

    adjust(&pool, &sku, &loc, 10, Some(50), "USD", "raw", "2026-04-15",
           &fresh_uuid(&pool).await).await.expect("in 1");
    adjust(&pool, &sku, &loc, 5, Some(80), "USD", "raw", "2026-04-16",
           &fresh_uuid(&pool).await).await.expect("in 2");
    adjust(&pool, &sku, &loc, -7, None, "USD", "raw", "2026-04-17",
           &fresh_uuid(&pool).await).await.expect("out");

    let _ = sqlx::query_scalar::<_, i64>("SELECT run_daily_reconciliation()::BIGINT")
        .fetch_one(&pool)
        .await
        .unwrap();
    let alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM reconciliation_alerts
         WHERE alert_kind = 'fifo_layer_residual_mismatch'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(alerts, 0, "FIFO recon clean after in/in/out cycle");
}
