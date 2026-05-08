//! T1 probes for FIFO dispatcher + apply_event extension (mig 0032,
//! acct-wb3f). Phase E1 E1.2 of the convergence plan.
//!
//! Drives `post_posting_lines` directly with FIFO SKU-FIF events to
//! exercise the dispatcher walk + receipt-side layer creation +
//! issue-side depletion writeback. Bypasses entry-point wrappers
//! (`post_po_receipt`, `post_so_ship`, `post_scrap`) which still
//! hard-block FIFO at their own gates — wrapper integration is
//! follow-up scope; these tests pin the lower level.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

struct FifScaffold {
    sku: String,
    loc: String,
    inv_raw: i64,
    stock_avail: i64,
    ap_unsettled: i64,
    ven_qty: i64,
    inv_adj_expense: i64,
    void_qty: i64,
}

async fn fresh_vendor_uuid(pool: &PgPool, label: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $1, 'USD') RETURNING id::text",
    )
    .bind(label)
    .fetch_one(pool)
    .await
    .expect("insert vendor")
}

/// Open the FIFO accounts that aren't already in fixture and return
/// the IDs needed for receipt + issue posts.
async fn scaffold_fif(pool: &PgPool) -> FifScaffold {
    let sku: String = sqlx::query_scalar("SELECT id::text FROM skus WHERE code = 'SKU-FIF'")
        .fetch_one(pool)
        .await
        .unwrap();
    let loc: String = sqlx::query_scalar("SELECT id::text FROM locations WHERE code = 'MAIN'")
        .fetch_one(pool)
        .await
        .unwrap();
    let vendor = fresh_vendor_uuid(pool, "VEND-FIF-E1").await;

    let inv_raw: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, sku_id, location_id, normal_side)
         VALUES ('inv_value_raw', 'value', 'USD', $1::UUID, $2::UUID, 'debit')
         RETURNING id",
    )
    .bind(&sku)
    .bind(&loc)
    .fetch_one(pool)
    .await
    .unwrap();

    let ap_unsettled: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, currency, counterparty_id, normal_side)
         VALUES ('ap_unsettled', 'value', 'USD', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    let ven_qty: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (kind, ledger_kind, counterparty_id, normal_side)
         VALUES ('vendor_pool', 'qty', $1::UUID, 'credit')
         RETURNING id",
    )
    .bind(&vendor)
    .fetch_one(pool)
    .await
    .unwrap();

    let stock_avail = account_id_stock_available(pool, "SKU-FIF", "MAIN").await;
    let inv_adj_expense = account_id_by_kind_currency(pool, "inv_adj_expense", Some("USD")).await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;

    FifScaffold {
        sku,
        loc,
        inv_raw,
        stock_avail,
        ap_unsettled,
        ven_qty,
        inv_adj_expense,
        void_qty,
    }
}

/// Post a FIFO receipt of `qty` units at `unit_cost`. Returns the
/// value-leg posting_line.id.
async fn fifo_receipt(
    pool: &PgPool,
    s: &FifScaffold,
    qty: i64,
    unit_cost: i64,
    business_date: &str,
) -> i64 {
    let qty_key = fresh_uuid(pool).await;
    let val_key = fresh_uuid(pool).await;
    let amount = qty * unit_cost;
    let qty_event = make_event(
        "po_receipt",
        s.stock_avail,
        s.ven_qty,
        qty,
        business_date,
        &qty_key,
    );
    let val_event = make_event_with_qty(
        "po_receipt",
        s.inv_raw,
        s.ap_unsettled,
        amount,
        qty,
        business_date,
        &val_key,
    );
    let result = call_post_posting_lines(pool, json!([qty_event, val_event]), false)
        .await
        .expect("fifo receipt post_posting_lines");
    assert_eq!(result[0]["result"], "ok", "qty leg: {result}");
    assert_eq!(result[1]["result"], "ok", "val leg: {result}");
    sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
        .bind(&val_key)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Post a FIFO issue of `qty` units via the 'scrap' reason (which
/// goes through the dispatcher). Returns the value-leg posting_line.id.
/// Returns Result so caller can probe for P0006 layer-exhaustion.
async fn fifo_issue(
    pool: &PgPool,
    s: &FifScaffold,
    qty: i64,
    business_date: &str,
) -> sqlx::Result<i64> {
    let qty_key = fresh_uuid(pool).await;
    let val_key = fresh_uuid(pool).await;
    let qty_event = make_event(
        "scrap",
        s.void_qty,
        s.stock_avail,
        qty,
        business_date,
        &qty_key,
    );
    // Amount placeholder; dispatcher overrides via FIFO walk.
    let val_event = make_event_with_qty(
        "scrap",
        s.inv_adj_expense,
        s.inv_raw,
        0,
        qty,
        business_date,
        &val_key,
    );
    call_post_posting_lines(pool, json!([qty_event, val_event]), false).await?;
    let pl_id: i64 =
        sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
            .bind(&val_key)
            .fetch_one(pool)
            .await
            .unwrap();
    Ok(pl_id)
}

// ============================================================
// Strategy registry
// ============================================================

#[tokio::test]
async fn fifo_strategy_registered() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let row: (String, String, bool) = sqlx::query_as(
        "SELECT compute_fn_name, event_kind::TEXT, flag_provisional
           FROM cost_method_strategies WHERE cost_method = 'fifo'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "_compute_amount_fifo_outbound");
    assert_eq!(row.1, "outbound");
    assert!(!row.2, "fifo doesn't flag provisional");
}

// ============================================================
// Receipt-side layer creation
// ============================================================

#[tokio::test]
async fn receipt_creates_one_layer_at_unit_cost() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    let pl_id = fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;

    let row: (String, String, String, String, i64, String) = sqlx::query_as(
        "SELECT product_id::TEXT, location_id::TEXT,
                original_quantity::TEXT, unit_cost::TEXT,
                receipt_posting_line_id, cost_currency
           FROM cost_layers
          WHERE receipt_posting_line_id = $1",
    )
    .bind(pl_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(row.0, s.sku);
    assert_eq!(row.1, s.loc);
    assert!(row.2.starts_with("10"), "qty got {:?}", row.2);
    assert!(row.3.starts_with("100"), "unit_cost got {:?}", row.3);
    assert_eq!(row.4, pl_id);
    assert_eq!(row.5, "USD");

    let layer_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM cost_layers WHERE product_id = $1::UUID")
            .bind(&s.sku)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(layer_count, 1, "exactly one layer per receipt");
}

#[tokio::test]
async fn two_receipts_create_two_layers_in_order() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 5, 10, "2026-04-15").await;
    fifo_receipt(&pool, &s, 5, 20, "2026-04-16").await;

    let layers: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT receipt_date::TEXT, original_quantity::TEXT, unit_cost::TEXT
           FROM cost_layers
          WHERE product_id = $1::UUID
          ORDER BY receipt_date ASC",
    )
    .bind(&s.sku)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(layers.len(), 2);
    assert!(layers[0].0.starts_with("2026-04-15"));
    assert!(layers[0].2.starts_with("10"));
    assert!(layers[1].0.starts_with("2026-04-16"));
    assert!(layers[1].2.starts_with("20"));
}

// ============================================================
// Issue-side single-layer consumption
// ============================================================

#[tokio::test]
async fn single_layer_full_consumption_writes_one_depletion() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;
    let issue_pl = fifo_issue(&pool, &s, 5, "2026-04-16").await.unwrap();

    let depletions: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT depleted_quantity::TEXT, unit_cost::TEXT, cost_amount
           FROM cost_layer_depletions
          WHERE posting_line_id = $1
          ORDER BY depletion_id",
    )
    .bind(issue_pl)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 1, "one layer consumed → one depletion row");
    assert!(depletions[0].0.starts_with("5"));
    assert!(depletions[0].1.starts_with("100"));
    assert_eq!(depletions[0].2, 500);

    let amount: i64 =
        sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
            .bind(issue_pl)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(amount, 500, "posting_line.amount = SUM(depletions.cost_amount)");
}

// ============================================================
// Multi-layer spanning issue
// ============================================================

#[tokio::test]
async fn multi_layer_spanning_issue_writes_multiple_depletions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 5, 10, "2026-04-15").await;
    fifo_receipt(&pool, &s, 5, 20, "2026-04-16").await;
    let issue_pl = fifo_issue(&pool, &s, 8, "2026-04-17").await.unwrap();

    let depletions: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT depleted_quantity::TEXT, unit_cost::TEXT, cost_amount
           FROM cost_layer_depletions
          WHERE posting_line_id = $1
          ORDER BY layer_receipt_date ASC",
    )
    .bind(issue_pl)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(depletions.len(), 2, "issue spans two layers");

    // First layer (oldest, $10): consumed in full = 5.
    assert!(depletions[0].0.starts_with("5"));
    assert!(depletions[0].1.starts_with("10"));
    assert_eq!(depletions[0].2, 50);
    // Second layer ($20): consumed 3 of 5.
    assert!(depletions[1].0.starts_with("3"));
    assert!(depletions[1].1.starts_with("20"));
    assert_eq!(depletions[1].2, 60);

    let amount: i64 =
        sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
            .bind(issue_pl)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(amount, 110, "5×10 + 3×20 = 110 (FIFO weighted)");
}

#[tokio::test]
async fn dispatcher_amount_equals_sum_depletions_cost_amount() {
    // Stronger version of the prior test: invariant pinned across any
    // multi-layer issue.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 7, 13, "2026-04-15").await;
    fifo_receipt(&pool, &s, 4, 17, "2026-04-16").await;
    fifo_receipt(&pool, &s, 9, 23, "2026-04-17").await;
    let issue_pl = fifo_issue(&pool, &s, 15, "2026-04-18").await.unwrap();

    let amount: i64 =
        sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
            .bind(issue_pl)
            .fetch_one(&pool)
            .await
            .unwrap();
    let sum_depletions: i64 = sqlx::query_scalar(
        "SELECT SUM(cost_amount)::BIGINT FROM cost_layer_depletions WHERE posting_line_id = $1",
    )
    .bind(issue_pl)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        amount, sum_depletions,
        "posting_line.amount must equal SUM(depletions.cost_amount)"
    );
    // Sanity: 7×13 + 4×17 + 4×23 = 91 + 68 + 92 = 251
    assert_eq!(amount, 251);
}

// ============================================================
// Layer exhaustion
// ============================================================

#[tokio::test]
async fn layer_exhaustion_raises_p0006() {
    // Probe just the value-leg to isolate the FIFO walk failure path
    // from the qty-side stock_available no-negative CHECK. Receive
    // qty=5 (1 layer of 5), then ask the dispatcher to walk for
    // qty=10 — layers exhausted → P0006.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 5, 100, "2026-04-15").await;

    expect_sqlstate("P0006", || async {
        let val_key = fresh_uuid(&pool).await;
        let val_event = make_event_with_qty(
            "scrap",
            s.inv_adj_expense,
            s.inv_raw,
            0,
            10,
            "2026-04-16",
            &val_key,
        );
        call_post_posting_lines(&pool, json!([val_event]), false)
            .await
            .map(|_| ())
    })
    .await;

    // The FIFO walk raises before any posting_line is written, so
    // the transaction rollback leaves the layer's residual untouched.
    let layer_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM cost_layers WHERE product_id = $1::UUID")
            .bind(&s.sku)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(layer_count, 1, "receipt's layer survived the rollback");

    let depl_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_layer_depletions
          WHERE layer_id IN (SELECT layer_id FROM cost_layers WHERE product_id = $1::UUID)",
    )
    .bind(&s.sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(depl_count, 0, "no depletions written on rollback");
}

// ============================================================
// posting_line_inventory.cost_layer_id stamps to first consumed
// ============================================================

#[tokio::test]
async fn posting_line_inventory_cost_layer_id_set_to_first_consumed() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 5, 10, "2026-04-15").await;
    fifo_receipt(&pool, &s, 5, 20, "2026-04-16").await;
    let issue_pl = fifo_issue(&pool, &s, 8, "2026-04-17").await.unwrap();

    let cost_layer_id: Option<i64> = sqlx::query_scalar(
        "SELECT cost_layer_id FROM posting_line_inventory WHERE posting_line_id = $1",
    )
    .bind(issue_pl)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(cost_layer_id.is_some(), "cost_layer_id must be stamped");

    let earliest: i64 = sqlx::query_scalar(
        "SELECT layer_id FROM cost_layers
          WHERE product_id = $1::UUID
          ORDER BY receipt_date ASC, layer_id ASC LIMIT 1",
    )
    .bind(&s.sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        cost_layer_id.unwrap(),
        earliest,
        "stamped layer_id should be the earliest (first consumed) layer"
    );
}

#[tokio::test]
async fn receipt_posting_line_inventory_cost_method_is_fifo() {
    // Snapshot R7: pli.cost_method_at_event captures the SKU's
    // cost_method at the time of the post.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    let receipt_pl = fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;

    let cm: String = sqlx::query_scalar(
        "SELECT cost_method_at_event::TEXT FROM posting_line_inventory WHERE posting_line_id = $1",
    )
    .bind(receipt_pl)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cm, "fifo");
}

// ============================================================
// inventory_movements rows still written (D-block gate widened)
// ============================================================

#[tokio::test]
async fn fifo_post_writes_inventory_movements_row() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    let receipt_pl = fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;

    let row: (String, String, String) = sqlx::query_as(
        "SELECT product_id::TEXT, quantity::TEXT, actual_unit_cost::TEXT
           FROM inventory_movements
          WHERE posting_line_id = $1",
    )
    .bind(receipt_pl)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, s.sku);
    assert!(row.1.starts_with("10"), "movement quantity = +10 (DR side)");
    assert!(row.2.starts_with("100"), "actual_unit_cost = receipt unit_cost");
}

// ============================================================
// Layer immutability survives the dispatcher walk
// ============================================================

#[tokio::test]
async fn layers_are_not_mutated_by_walk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;
    let snapshot_before: (String, String, String) = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT, created_at::TEXT
           FROM cost_layers WHERE product_id = $1::UUID",
    )
    .bind(&s.sku)
    .fetch_one(&pool)
    .await
    .unwrap();

    fifo_issue(&pool, &s, 7, "2026-04-16").await.unwrap();

    let snapshot_after: (String, String, String) = sqlx::query_as(
        "SELECT original_quantity::TEXT, unit_cost::TEXT, created_at::TEXT
           FROM cost_layers WHERE product_id = $1::UUID",
    )
    .bind(&s.sku)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(snapshot_before, snapshot_after, "layer row immutable across issue");

    // Residual computed via helper.
    let layer_id: i64 = sqlx::query_scalar(
        "SELECT layer_id FROM cost_layers WHERE product_id = $1::UUID",
    )
    .bind(&s.sku)
    .fetch_one(&pool)
    .await
    .unwrap();
    let remaining: String = sqlx::query_scalar(
        "SELECT _cost_layer_remaining_qty($1, '2026-04-15'::DATE)::TEXT",
    )
    .bind(layer_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(remaining.starts_with("3"), "10 - 7 = 3 remaining; got {remaining}");
}

// ============================================================
// Two issues drain a single layer in sequence
// ============================================================

#[tokio::test]
async fn sequential_issues_drain_same_layer_progressively() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = scaffold_fif(&pool).await;

    fifo_receipt(&pool, &s, 10, 100, "2026-04-15").await;
    let pl1 = fifo_issue(&pool, &s, 3, "2026-04-16").await.unwrap();
    let pl2 = fifo_issue(&pool, &s, 4, "2026-04-17").await.unwrap();

    let pl1_amount: i64 = sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
        .bind(pl1)
        .fetch_one(&pool)
        .await
        .unwrap();
    let pl2_amount: i64 = sqlx::query_scalar("SELECT amount FROM posting_lines WHERE id = $1")
        .bind(pl2)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(pl1_amount, 300, "first issue consumes 3 from sole layer @ 100");
    assert_eq!(pl2_amount, 400, "second issue consumes 4 more @ 100");

    let layer_id: i64 =
        sqlx::query_scalar("SELECT layer_id FROM cost_layers WHERE product_id = $1::UUID")
            .bind(&s.sku)
            .fetch_one(&pool)
            .await
            .unwrap();
    let remaining: String = sqlx::query_scalar(
        "SELECT _cost_layer_remaining_qty($1, '2026-04-15'::DATE)::TEXT",
    )
    .bind(layer_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        remaining.starts_with("3"),
        "10 - 3 - 4 = 3 remaining; got {remaining}"
    );
}
