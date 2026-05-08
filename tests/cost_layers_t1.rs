//! T1 probes for `cost_layers` + `cost_layer_depletions` (mig 0031,
//! acct-3b3a). Phase E1 E1.1 of the convergence plan. Pin schema
//! invariants of the FIFO foundation BEFORE the dispatcher in E1.2
//! wires real writes. The dispatcher does not yet write to either
//! table; these probes drive direct INSERTs to verify FK constraints,
//! append-only triggers, partition routing, and the residual helper.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;

/// Stage a real posting_line so we can exercise the FK target.
/// Mirrors the helper in `inventory_movements_t1.rs`.
async fn stage_posting_line(pool: &PgPool, sku_code: &str, qty: i64) -> i64 {
    let stock = account_id_stock_available(pool, sku_code, "MAIN").await;
    let void_qty = account_id_by_kind_currency(pool, "creation_void", None).await;
    let key = fresh_uuid(pool).await;
    let event = make_event("cycle_count_adj", stock, void_qty, qty, "2026-04-15", &key);
    let result = call_post_posting_lines(pool, json!([event]), false)
        .await
        .expect("stage post_posting_lines");
    assert_eq!(result[0]["result"], "ok", "stage: {result}");
    sqlx::query_scalar("SELECT id FROM posting_lines WHERE idempotency_key = $1::UUID")
        .bind(&key)
        .fetch_one(pool)
        .await
        .expect("fetch staged posting_line.id")
}

async fn sku_id(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM skus WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("sku lookup")
}

async fn loc_id(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar("SELECT id::text FROM locations WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("location lookup")
}

/// Insert a hand-crafted cost_layers row pointing at the given
/// posting_line. Returns the layer_id. Used by depletions/recon
/// tests that need a layer to exist before they probe.
async fn stage_layer(
    pool: &PgPool,
    sku_code: &str,
    loc_code: &str,
    pl_id: i64,
    receipt_date: &str,
    qty: &str,
    unit_cost: &str,
) -> i64 {
    let s = sku_id(pool, sku_code).await;
    let l = loc_id(pool, loc_code).await;
    sqlx::query_scalar(
        "INSERT INTO cost_layers
            (product_id, location_id, receipt_posting_line_id,
             receipt_date, original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, $2::UUID, $3, $4::DATE, $5::NUMERIC, $6::NUMERIC, 'USD')
         RETURNING layer_id",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl_id)
    .bind(receipt_date)
    .bind(qty)
    .bind(unit_cost)
    .fetch_one(pool)
    .await
    .expect("stage cost_layers row")
}

// ============================================================
// Partitioning: 24 monthly partitions per table, fixture spot-check
// ============================================================

#[tokio::test]
async fn baked_partitions_span_2026_to_2027_for_both_tables() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let layers_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM pg_inherits i
           JOIN pg_class c ON c.oid = i.inhparent
          WHERE c.relname = 'cost_layers'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layers_count, 24, "cost_layers: 24 monthly partitions baked");

    let depletions_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM pg_inherits i
           JOIN pg_class c ON c.oid = i.inhparent
          WHERE c.relname = 'cost_layer_depletions'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(depletions_count, 24, "cost_layer_depletions: 24 monthly partitions baked");

    // Spot-check the fixture-relevant partition exists for both.
    let layers_apr_2026: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pg_class WHERE relname = 'cost_layers_2026_04'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let depl_apr_2026: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pg_class WHERE relname = 'cost_layer_depletions_2026_04'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layers_apr_2026, 1, "cost_layers_2026_04 (fixture period) must exist");
    assert_eq!(depl_apr_2026, 1, "cost_layer_depletions_2026_04 (fixture period) must exist");
}

#[tokio::test]
async fn insert_outside_partition_window_fails_on_layers() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ($1::UUID, $2::UUID, $3, '2030-04-15', 5.0, 100.0, 'USD')",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn insert_outside_partition_window_fails_on_depletions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl_recv = stage_posting_line(&pool, "SKU-A", 1).await;
    let pl_iss = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl_recv, "2026-04-15", "5.0", "100.0").await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO cost_layer_depletions
                (layer_id, layer_receipt_date, issue_date,
                 depleted_quantity, unit_cost, cost_amount, posting_line_id)
             VALUES ($1, '2026-04-15', '2030-05-15', 1.0, 100.0, 100, $2)",
        )
        .bind(layer_id)
        .bind(pl_iss)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn rollover_helper_idempotent_for_both_tables() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Re-create existing partition: must not error.
    sqlx::query("SELECT _create_cost_layers_partition('2026-04-01'::DATE)")
        .execute(&pool)
        .await
        .expect("layers idempotent re-create");
    sqlx::query("SELECT _create_cost_layer_depletions_partition('2026-04-01'::DATE)")
        .execute(&pool)
        .await
        .expect("depletions idempotent re-create");

    // New partition outside bake window.
    sqlx::query("SELECT _create_cost_layers_partition('2028-03-01'::DATE)")
        .execute(&pool)
        .await
        .expect("create layers partition");
    sqlx::query("SELECT _create_cost_layer_depletions_partition('2028-03-01'::DATE)")
        .execute(&pool)
        .await
        .expect("create depletions partition");

    let l_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pg_class WHERE relname = 'cost_layers_2028_03'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let d_exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM pg_class WHERE relname = 'cost_layer_depletions_2028_03'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(l_exists, 1);
    assert_eq!(d_exists, 1);

    // Rolled-over partition accepts inserts.
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;
    sqlx::query(
        "INSERT INTO cost_layers
            (product_id, location_id, receipt_posting_line_id,
             receipt_date, original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, $2::UUID, $3, '2028-03-15', 5.0, 100.0, 'USD')",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl)
    .execute(&pool)
    .await
    .expect("insert into newly-created layers partition");
}

// ============================================================
// FK constraints
// ============================================================

#[tokio::test]
async fn fk_layer_product_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ('00000000-0000-0000-0000-deadbeef0000'::UUID,
                     $1::UUID, $2, '2026-04-15', 5.0, 100.0, 'USD')",
        )
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_layer_location_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ($1::UUID, '00000000-0000-0000-0000-deadbeef0001'::UUID,
                     $2, '2026-04-15', 5.0, 100.0, 'USD')",
        )
        .bind(&s)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_layer_receipt_posting_line_id_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ($1::UUID, $2::UUID, 99999999, '2026-04-15', 5.0, 100.0, 'USD')",
        )
        .bind(&s)
        .bind(&l)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_layer_cost_book_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, cost_book_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ($1::UUID, 99, $2::UUID, $3, '2026-04-15', 5.0, 100.0, 'USD')",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_depletion_layer_composite_violation() {
    // Depletion's (layer_id, layer_receipt_date) must exist in cost_layers.
    // Pointing at a non-existent layer raises 23503.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl_iss = stage_posting_line(&pool, "SKU-A", 1).await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layer_depletions
                (layer_id, layer_receipt_date, issue_date,
                 depleted_quantity, unit_cost, cost_amount, posting_line_id)
             VALUES (99999999, '2026-04-15', '2026-04-20',
                     1.0, 100.0, 100, $1)",
        )
        .bind(pl_iss)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_depletion_layer_composite_partial_match_fails() {
    // Layer exists at receipt_date='2026-04-15' but the depletion
    // points at the same layer_id with a DIFFERENT receipt_date.
    // Composite FK rejects: receipt_date is part of the key.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl_recv = stage_posting_line(&pool, "SKU-A", 1).await;
    let pl_iss = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl_recv, "2026-04-15", "5.0", "100.0").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layer_depletions
                (layer_id, layer_receipt_date, issue_date,
                 depleted_quantity, unit_cost, cost_amount, posting_line_id)
             VALUES ($1, '2026-04-16', '2026-04-20', 1.0, 100.0, 100, $2)",
        )
        .bind(layer_id)
        .bind(pl_iss)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn fk_depletion_posting_line_violation() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let pl_recv = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl_recv, "2026-04-15", "5.0", "100.0").await;

    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO cost_layer_depletions
                (layer_id, layer_receipt_date, issue_date,
                 depleted_quantity, unit_cost, cost_amount, posting_line_id)
             VALUES ($1, '2026-04-15', '2026-04-20',
                     1.0, 100.0, 100, 99999999)",
        )
        .bind(layer_id)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// CHECK constraints
// ============================================================

#[tokio::test]
async fn check_layer_quantity_positive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ($1::UUID, $2::UUID, $3, '2026-04-15', 0.0, 100.0, 'USD')",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn check_layer_unit_cost_non_negative() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO cost_layers
                (product_id, location_id, receipt_posting_line_id,
                 receipt_date, original_quantity, unit_cost, cost_currency)
             VALUES ($1::UUID, $2::UUID, $3, '2026-04-15', 5.0, -1.0, 'USD')",
        )
        .bind(&s)
        .bind(&l)
        .bind(pl)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn check_depletion_quantity_positive() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl_recv = stage_posting_line(&pool, "SKU-A", 1).await;
    let pl_iss = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl_recv, "2026-04-15", "5.0", "100.0").await;

    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO cost_layer_depletions
                (layer_id, layer_receipt_date, issue_date,
                 depleted_quantity, unit_cost, cost_amount, posting_line_id)
             VALUES ($1, '2026-04-15', '2026-04-20', 0.0, 100.0, 100, $2)",
        )
        .bind(layer_id)
        .bind(pl_iss)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// Defaults
// ============================================================

#[tokio::test]
async fn defaults_legal_entity_and_cost_book_are_one() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let s = sku_id(&pool, "SKU-A").await;
    let l = loc_id(&pool, "MAIN").await;

    let layer_id: i64 = sqlx::query_scalar(
        "INSERT INTO cost_layers
            (product_id, location_id, receipt_posting_line_id,
             receipt_date, original_quantity, unit_cost, cost_currency)
         VALUES ($1::UUID, $2::UUID, $3, '2026-04-15', 5.0, 100.0, 'USD')
         RETURNING layer_id",
    )
    .bind(&s)
    .bind(&l)
    .bind(pl)
    .fetch_one(&pool)
    .await
    .unwrap();

    let row: (i16, i16) =
        sqlx::query_as("SELECT legal_entity_id, cost_book_id FROM cost_layers WHERE layer_id = $1")
            .bind(layer_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row, (1, 1));
}

// ============================================================
// Append-only triggers
// ============================================================

#[tokio::test]
async fn cost_layers_blocks_update() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl, "2026-04-15", "5.0", "100.0").await;

    expect_sqlstate("P9999", || async {
        sqlx::query("UPDATE cost_layers SET unit_cost = 999.0 WHERE layer_id = $1")
            .bind(layer_id)
            .execute(&pool)
            .await
    })
    .await;
}

#[tokio::test]
async fn cost_layers_blocks_delete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl, "2026-04-15", "5.0", "100.0").await;

    expect_sqlstate("P9999", || async {
        sqlx::query("DELETE FROM cost_layers WHERE layer_id = $1")
            .bind(layer_id)
            .execute(&pool)
            .await
    })
    .await;
}

#[tokio::test]
async fn cost_layer_depletions_blocks_update_and_delete() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl_recv = stage_posting_line(&pool, "SKU-A", 1).await;
    let pl_iss = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl_recv, "2026-04-15", "5.0", "100.0").await;

    let depl_id: i64 = sqlx::query_scalar(
        "INSERT INTO cost_layer_depletions
            (layer_id, layer_receipt_date, issue_date,
             depleted_quantity, unit_cost, cost_amount, posting_line_id)
         VALUES ($1, '2026-04-15', '2026-04-20', 1.0, 100.0, 100, $2)
         RETURNING depletion_id",
    )
    .bind(layer_id)
    .bind(pl_iss)
    .fetch_one(&pool)
    .await
    .unwrap();

    expect_sqlstate("P9999", || async {
        sqlx::query(
            "UPDATE cost_layer_depletions
                SET depleted_quantity = 999.0
              WHERE depletion_id = $1 AND issue_date = '2026-04-20'",
        )
        .bind(depl_id)
        .execute(&pool)
        .await
    })
    .await;

    expect_sqlstate("P9999", || async {
        sqlx::query(
            "DELETE FROM cost_layer_depletions
              WHERE depletion_id = $1 AND issue_date = '2026-04-20'",
        )
        .bind(depl_id)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// _cost_layer_remaining_qty helper math
// ============================================================

#[tokio::test]
async fn remaining_qty_returns_null_for_missing_layer() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let result: Option<String> = sqlx::query_scalar(
        "SELECT _cost_layer_remaining_qty(99999999, '2026-04-15'::DATE)::TEXT",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(result.is_none(), "missing layer must return NULL");
}

#[tokio::test]
async fn remaining_qty_equals_original_with_no_depletions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl, "2026-04-15", "5.0", "100.0").await;

    let remaining: String = sqlx::query_scalar(
        "SELECT _cost_layer_remaining_qty($1, '2026-04-15'::DATE)::TEXT",
    )
    .bind(layer_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 5.0 with NUMERIC(19,6) renders as "5.000000".
    assert!(
        remaining.starts_with("5"),
        "remaining qty without depletions should equal original 5.0; got {remaining:?}"
    );
}

#[tokio::test]
async fn remaining_qty_decreases_with_depletions() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let pl_recv = stage_posting_line(&pool, "SKU-A", 1).await;
    let pl_iss = stage_posting_line(&pool, "SKU-A", 1).await;
    let layer_id = stage_layer(&pool, "SKU-A", "MAIN", pl_recv, "2026-04-15", "5.0", "100.0").await;

    sqlx::query(
        "INSERT INTO cost_layer_depletions
            (layer_id, layer_receipt_date, issue_date,
             depleted_quantity, unit_cost, cost_amount, posting_line_id)
         VALUES ($1, '2026-04-15', '2026-04-20', 2.0, 100.0, 200, $2)",
    )
    .bind(layer_id)
    .bind(pl_iss)
    .execute(&pool)
    .await
    .expect("first depletion");

    sqlx::query(
        "INSERT INTO cost_layer_depletions
            (layer_id, layer_receipt_date, issue_date,
             depleted_quantity, unit_cost, cost_amount, posting_line_id)
         VALUES ($1, '2026-04-15', '2026-04-22', 1.5, 100.0, 150, $2)",
    )
    .bind(layer_id)
    .bind(pl_iss)
    .execute(&pool)
    .await
    .expect("second depletion");

    let remaining: String = sqlx::query_scalar(
        "SELECT _cost_layer_remaining_qty($1, '2026-04-15'::DATE)::TEXT",
    )
    .bind(layer_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    // 5.0 - 2.0 - 1.5 = 1.5
    assert!(
        remaining.starts_with("1.5"),
        "5 - 2 - 1.5 should leave 1.5; got {remaining:?}"
    );
}

// ============================================================
// Composite primary key
// ============================================================

#[tokio::test]
async fn cost_layers_pk_is_layer_id_plus_receipt_date() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Inspect the catalog: the partitioned-table PK columns must be
    // (layer_id, receipt_date).
    let cols: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname
           FROM pg_index i
           JOIN pg_class c    ON c.oid = i.indrelid AND c.relname = 'cost_layers'
           JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
          WHERE i.indisprimary
          ORDER BY array_position(i.indkey, a.attnum)",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        cols,
        vec!["layer_id".to_string(), "receipt_date".to_string()],
        "PK must be (layer_id, receipt_date) for partition compatibility"
    );
}

#[tokio::test]
async fn depletions_pk_is_depletion_id_plus_issue_date() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let cols: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname
           FROM pg_index i
           JOIN pg_class c    ON c.oid = i.indrelid AND c.relname = 'cost_layer_depletions'
           JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
          WHERE i.indisprimary
          ORDER BY array_position(i.indkey, a.attnum)",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        cols,
        vec!["depletion_id".to_string(), "issue_date".to_string()]
    );
}

// ============================================================
// posting_line_inventory.cost_layer_id is a soft pointer (no FK)
// ============================================================

#[tokio::test]
async fn cost_layer_id_on_pli_has_no_fk() {
    // Composite PK on cost_layers means a single-column FK won't
    // work; the column is documented as a soft pointer audited by
    // E1.3 recon. Confirm the catalog has no FK from
    // posting_line_inventory.cost_layer_id to cost_layers.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let fk_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT
           FROM information_schema.table_constraints tc
           JOIN information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name
          WHERE tc.table_name = 'posting_line_inventory'
            AND tc.constraint_type = 'FOREIGN KEY'
            AND kcu.column_name = 'cost_layer_id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(fk_count, 0, "cost_layer_id must remain a soft pointer");
}
