//! `acct-wl1` / Slice B.2 + `acct-mov` / BOM2 C4 — T1 invariant probes
//! for the conversion-cycle tables (work_orders, wo_routings, wo_events)
//! and the BOM2 model (bom_headers, bom_lines, absorption_classes,
//! wo_outputs, engineering_change_orders).
//!
//! Pins the table-level CHECK / FK / UNIQUE constraints so a schema
//! regression surfaces here rather than as confused matrix-test
//! failures. The legacy `boms` and `wo_routing_burdens` probes have
//! been retired in favor of the bom_headers/bom_lines shape (those
//! tables are scheduled for removal in the C3 cleanup migration).

mod common;

use common::*;
use sqlx::PgPool;

async fn one_sku(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO skus (code, uom, cost_method)
         VALUES ($1, 'EA', 'standard') RETURNING id::text",
    )
    .bind(code)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn one_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn one_wo(pool: &PgPool) -> String {
    let parent = one_sku(pool, "T1-P").await;
    let loc = one_location(pool, "T1-FG").await;
    let posted_by = fresh_uuid(pool).await;
    sqlx::query_scalar(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ('T1-WO', $1::UUID, $2::UUID, 10, 'USD', $3::UUID) RETURNING id::text",
    )
    .bind(&parent)
    .bind(&loc)
    .bind(&posted_by)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ============================================================
// work_orders
// ============================================================

#[tokio::test]
async fn work_orders_qty_target_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-A").await;
    let loc = one_location(&pool, "T1-L-A").await;
    let posted_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
             VALUES ('T1-WO-A', $1::UUID, $2::UUID, 0, 'USD', $3::UUID)",
        )
        .bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn work_orders_completed_plus_scrapped_over_target_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-B").await;
    let loc = one_location(&pool, "T1-L-B").await;
    let posted_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target,
                                       qty_completed, qty_scrapped, currency, posted_by)
             VALUES ('T1-WO-B', $1::UUID, $2::UUID, 10, 6, 5, 'USD', $3::UUID)",
        )
        .bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn work_orders_bad_status_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-C").await;
    let loc = one_location(&pool, "T1-L-C").await;
    let posted_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, status, currency, posted_by)
             VALUES ('T1-WO-C', $1::UUID, $2::UUID, 10, 'bogus', 'USD', $3::UUID)",
        )
        .bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn work_orders_duplicate_wo_no_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let parent = one_sku(&pool, "T1-P-D").await;
    let loc = one_location(&pool, "T1-L-D").await;
    let posted_by = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
         VALUES ('T1-WO-DUP', $1::UUID, $2::UUID, 10, 'USD', $3::UUID)",
    ).bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO work_orders (wo_no, parent_sku_id, fg_location_id, qty_target, currency, posted_by)
             VALUES ('T1-WO-DUP', $1::UUID, $2::UUID, 10, 'USD', $3::UUID)",
        ).bind(&parent).bind(&loc).bind(&posted_by).execute(&pool).await
    }).await;
}

// ============================================================
// wo_routings
// ============================================================

#[tokio::test]
async fn wo_routings_routing_op_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 0, 'OP0')")
            .bind(&wo).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_routings_duplicate_op_violates_pk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'OP1')")
        .bind(&wo).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query("INSERT INTO wo_routings (wo_id, routing_op, op_name) VALUES ($1::UUID, 10, 'OP1B')")
            .bind(&wo).execute(&pool).await
    }).await;
}

// ============================================================
// bom_headers
// ============================================================

async fn one_bom_header_full(
    pool: &PgPool,
    parent_code: &str,
    alt: i32,
    rev: &str,
    is_primary: bool,
    status: &str,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO bom_headers
            (parent_sku_id, alternate_no, revision_no, is_primary, status)
         SELECT id, $2, $3, $4, $5 FROM skus WHERE code = $1
         RETURNING id",
    )
    .bind(parent_code)
    .bind(alt)
    .bind(rev)
    .bind(is_primary)
    .bind(status)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn bom_headers_duplicate_alt_rev_violates_unique() {
    // UNIQUE (parent_sku_id, alternate_no, revision_no).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BH-A").await;
    one_bom_header_full(&pool, "T1-BH-A", 1, "A", true, "active").await;
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
             SELECT id, 1, 'A', FALSE, 'draft' FROM skus WHERE code = 'T1-BH-A'",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_headers_two_primary_active_per_parent_alt_violates_partial_unique() {
    // bom_headers_primary partial UNIQUE: at most one row with
    // is_primary=true AND status='active' per (parent_sku, alternate_no).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BH-B").await;
    one_bom_header_full(&pool, "T1-BH-B", 1, "A", true, "active").await;
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
             SELECT id, 1, 'B', TRUE, 'active' FROM skus WHERE code = 'T1-BH-B'",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_headers_bad_status_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BH-C").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no, is_primary, status)
             SELECT id, 1, 'A', FALSE, 'bogus' FROM skus WHERE code = 'T1-BH-C'",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_headers_alternate_no_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BH-D").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no)
             SELECT id, 0, 'A' FROM skus WHERE code = 'T1-BH-D'",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_headers_inverted_effectivity_window_violates_check() {
    // CHECK (effective_at < obsolete_at).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BH-E").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_headers (parent_sku_id, alternate_no, revision_no,
                                       effective_at, obsolete_at)
             SELECT id, 1, 'A', '2026-06-01'::TIMESTAMPTZ, '2026-01-01'::TIMESTAMPTZ
               FROM skus WHERE code = 'T1-BH-E'",
        )
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// bom_lines  (replaces legacy `boms` + `wo_routing_burdens` probes)
// ============================================================

#[tokio::test]
async fn bom_lines_item_qty_per_parent_zero_violates_check() {
    // The kind-discriminator CHECK requires qty_per_parent > 0 for
    // kind='item'.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BL-PA").await;
    let comp = one_sku(&pool, "T1-BL-CA").await;
    let loc = one_location(&pool, "T1-BL-LA").await;
    let bom = one_bom_header_full(&pool, "T1-BL-PA", 1, "A", true, "active").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                     component_sku_id, component_loc_id, qty_per_parent)
             VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', $2::UUID, $3::UUID, 0)",
        )
        .bind(bom)
        .bind(&comp)
        .bind(&loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_lines_self_reference_raises_p0034() {
    // Component SKU = parent SKU is a degenerate cycle. Trigger
    // _bom_line_self_reference_guard raises P0034.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let sku = one_sku(&pool, "T1-BL-SELF").await;
    let loc = one_location(&pool, "T1-BL-SELFL").await;
    let bom = one_bom_header_full(&pool, "T1-BL-SELF", 1, "A", true, "active").await;
    expect_sqlstate("P0034", || async {
        sqlx::query(
            "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                     component_sku_id, component_loc_id, qty_per_parent)
             VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', $2::UUID, $3::UUID, 1)",
        )
        .bind(bom)
        .bind(&sku)
        .bind(&loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_lines_duplicate_composite_pk_violates_unique() {
    // PK is (bom_id, line_no).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BL-DP").await;
    let comp = one_sku(&pool, "T1-BL-DC").await;
    let loc = one_location(&pool, "T1-BL-DL").await;
    let bom = one_bom_header_full(&pool, "T1-BL-DP", 1, "A", true, "active").await;
    sqlx::query(
        "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                 component_sku_id, component_loc_id, qty_per_parent)
         VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', $2::UUID, $3::UUID, 1)",
    )
    .bind(bom)
    .bind(&comp)
    .bind(&loc)
    .execute(&pool)
    .await
    .unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                     component_sku_id, component_loc_id, qty_per_parent)
             VALUES ($1, 1, 'item', 'per_unit', 10, 'op_arrival', $2::UUID, $3::UUID, 2)",
        )
        .bind(bom)
        .bind(&comp)
        .bind(&loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_lines_charge_must_be_per_lot_violates_check() {
    // basis × kind invariant: kind='charge' AND basis='per_unit' is illegal.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BL-PC").await;
    let bom = one_bom_header_full(&pool, "T1-BL-PC", 1, "A", true, "active").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                     absorption_class_id, std_amount)
             SELECT $1, 1, 'charge', 'per_unit', 10, 'op_arrival', ac.id, 100
               FROM absorption_classes ac WHERE ac.code = 'oh_std'",
        )
        .bind(bom)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_lines_per_unit_with_fire_at_wo_start_violates_check() {
    // basis × fire_at invariant: per_unit lines fire only at op_arrival.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BL-PF").await;
    let bom = one_bom_header_full(&pool, "T1-BL-PF", 1, "A", true, "active").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                     absorption_class_id, std_amount)
             SELECT $1, 1, 'service', 'per_unit', 10, 'wo_start', ac.id, 5
               FROM absorption_classes ac WHERE ac.code = 'labor_std'",
        )
        .bind(bom)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn bom_lines_item_with_absorption_class_set_violates_check() {
    // kind='item' must NOT carry absorption_class_id / std_amount.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    one_sku(&pool, "T1-BL-IAC").await;
    let comp = one_sku(&pool, "T1-BL-IACC").await;
    let loc = one_location(&pool, "T1-BL-IACL").await;
    let bom = one_bom_header_full(&pool, "T1-BL-IAC", 1, "A", true, "active").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_lines (bom_id, line_no, kind, basis, applies_at_op, fire_at,
                                     component_sku_id, component_loc_id, qty_per_parent,
                                     absorption_class_id, std_amount)
             SELECT $1, 1, 'item', 'per_unit', 10, 'op_arrival',
                    $2::UUID, $3::UUID, 1, ac.id, 100
               FROM absorption_classes ac WHERE ac.code = 'oh_std'",
        )
        .bind(bom)
        .bind(&comp)
        .bind(&loc)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// absorption_classes
// ============================================================

#[tokio::test]
async fn absorption_classes_duplicate_code_violates_unique() {
    // labor_std is seeded by the fixture; a second insert with the
    // same code violates UNIQUE.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO absorption_classes (code, display_name, applied_account_kind)
             VALUES ('labor_std', 'dup', 'labor_applied')",
        )
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn absorption_classes_applied_equals_expense_violates_check() {
    // CHECK (applied_account_kind <> expense_account_kind).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO absorption_classes
                (code, display_name, applied_account_kind, expense_account_kind)
             VALUES ('t1-ac-bad', 'bad', 'labor_applied', 'labor_applied')",
        )
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// wo_outputs
// ============================================================

#[tokio::test]
async fn wo_outputs_qty_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_outputs (wo_id, output_no, output_sku_id, fg_location_id, qty,
                                      allocation_method, allocation_pct)
             SELECT $1::UUID, 1, w.parent_sku_id, w.fg_location_id, 0, 'fixed_ratio', 100
               FROM work_orders w WHERE w.id = $1::UUID",
        )
        .bind(&wo)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn wo_outputs_bad_allocation_method_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_outputs (wo_id, output_no, output_sku_id, fg_location_id, qty,
                                      allocation_method, allocation_pct)
             SELECT $1::UUID, 1, w.parent_sku_id, w.fg_location_id, 5, 'bogus', 100
               FROM work_orders w WHERE w.id = $1::UUID",
        )
        .bind(&wo)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn wo_outputs_allocation_pct_above_100_violates_check() {
    // allocation_pct between 0 and 100 inclusive (per-row CHECK).
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_outputs (wo_id, output_no, output_sku_id, fg_location_id, qty,
                                      allocation_method, allocation_pct)
             SELECT $1::UUID, 1, w.parent_sku_id, w.fg_location_id, 5, 'fixed_ratio', 150
               FROM work_orders w WHERE w.id = $1::UUID",
        )
        .bind(&wo)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn wo_outputs_duplicate_pk_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    sqlx::query(
        "INSERT INTO wo_outputs (wo_id, output_no, output_sku_id, fg_location_id, qty,
                                  allocation_method, allocation_pct)
         SELECT $1::UUID, 1, w.parent_sku_id, w.fg_location_id, 5, 'fixed_ratio', 100
           FROM work_orders w WHERE w.id = $1::UUID",
    )
    .bind(&wo)
    .execute(&pool)
    .await
    .unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO wo_outputs (wo_id, output_no, output_sku_id, fg_location_id, qty,
                                      allocation_method, allocation_pct)
             SELECT $1::UUID, 1, w.parent_sku_id, w.fg_location_id, 7, 'fixed_ratio', 50
               FROM work_orders w WHERE w.id = $1::UUID",
        )
        .bind(&wo)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// engineering_change_orders
// ============================================================

#[tokio::test]
async fn eco_approved_without_required_fields_violates_check() {
    // status='approved' requires approved_by + approved_at + effective_at;
    // CHECK enforces the workflow invariant.
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let requested_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO engineering_change_orders (code, description, status, requested_by)
             VALUES ('T1-ECO-BAD', 'no approver', 'approved', $1::UUID)",
        )
        .bind(&requested_by)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn eco_rejected_without_reason_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let requested_by = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO engineering_change_orders (code, description, status, requested_by)
             VALUES ('T1-ECO-NR', 'no reason', 'rejected', $1::UUID)",
        )
        .bind(&requested_by)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn eco_duplicate_code_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let requested_by = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO engineering_change_orders (code, description, requested_by)
         VALUES ('T1-ECO-DUP', 'first', $1::UUID)",
    )
    .bind(&requested_by)
    .execute(&pool)
    .await
    .unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO engineering_change_orders (code, description, requested_by)
             VALUES ('T1-ECO-DUP', 'second', $1::UUID)",
        )
        .bind(&requested_by)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// wo_events
// ============================================================

#[tokio::test]
async fn wo_events_bad_event_kind_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'bogus', '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_start_with_routing_op_from_violates_composite_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    // 'start' must have all routing_op_* and qty NULL.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, routing_op_from, business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'start', 10, '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_op_move_missing_to_op_violates_composite_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, routing_op_from, qty,
                                    business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'op_move', 10, 5, '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_qty_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    // qty IS NULL OR qty > 0.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, routing_op_from, qty,
                                    business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'scrap', 10, 0, '2026-04-15', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}

#[tokio::test]
async fn wo_events_duplicate_idempotency_key_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let wo = one_wo(&pool).await;
    let posted_by = fresh_uuid(&pool).await;
    let key = fresh_uuid(&pool).await;
    sqlx::query(
        "INSERT INTO wo_events (wo_id, event_kind, business_date, posted_by, idempotency_key)
         VALUES ($1::UUID, 'start', '2026-04-15', $2::UUID, $3::UUID)",
    ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await.unwrap();
    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO wo_events (wo_id, event_kind, business_date, posted_by, idempotency_key)
             VALUES ($1::UUID, 'start', '2026-04-16', $2::UUID, $3::UUID)",
        ).bind(&wo).bind(&posted_by).bind(&key).execute(&pool).await
    }).await;
}
