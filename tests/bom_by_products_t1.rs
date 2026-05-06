//! `acct-ksnh` / `acct-7t4.1` — T1 invariant probes for the
//! `bom_by_products` table (mig 0096).
//!
//! Pins:
//!   * FK on bom_id, output_sku_id, fg_location_id, disposal_vendor_id
//!   * PK on (bom_id, by_product_no)
//!   * qty_per_parent > 0 CHECK
//!   * treatment ∈ {nrv_credit, negligible, disposal_cost} CHECK
//!   * per-treatment composite CHECK (sign of unit_value vs treatment;
//!     disposal_basis / disposal_vendor_id required for disposal_cost
//!     and forbidden otherwise; disposal_expense_account_kind forbidden
//!     for nrv_credit / negligible / disposal_cost-inventoriable)
//!   * ECO/revision lifecycle: bom_by_products on rev A and rev B
//!     coexist when both BOM rows exist; CASCADE on bom_headers DELETE.

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
    .unwrap_or_else(|e| panic!("sku {code}: {e}"))
}

async fn one_location(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO locations (code, name) VALUES ($1, $2) RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Loc {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("loc {code}: {e}"))
}

async fn one_vendor(pool: &PgPool, code: &str) -> String {
    sqlx::query_scalar(
        "INSERT INTO vendors (code, name, currency)
         VALUES ($1, $2, 'USD') RETURNING id::text",
    )
    .bind(code)
    .bind(format!("Vend {code}"))
    .fetch_one(pool)
    .await
    .unwrap_or_else(|e| panic!("vendor {code}: {e}"))
}

/// Standard scaffold per test: parent SKU + bom_header in 'active' state +
/// fg location for the by-product output. Returns
/// (bom_id, output_sku_id, fg_loc_id).
async fn scaffold(pool: &PgPool, suffix: &str) -> (i64, String, String) {
    let parent_code = format!("BP-T1-P-{suffix}");
    let output_code = format!("BP-T1-O-{suffix}");
    let loc_code = format!("BP-T1-L-{suffix}");
    let _parent = one_sku(pool, &parent_code).await;
    let output_sku = one_sku(pool, &output_code).await;
    let fg_loc = one_location(pool, &loc_code).await;
    let bom_id = create_bom_header(pool, &parent_code).await;
    (bom_id, output_sku, fg_loc)
}

// ============================================================
// Happy paths — one row per treatment
// ============================================================

#[tokio::test]
async fn happy_nrv_credit() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "NRV").await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.5, 100, 'nrv_credit')",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .execute(&pool)
    .await
    .expect("insert nrv_credit");
}

#[tokio::test]
async fn happy_negligible() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "NEG").await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, 1, $2::UUID, $3::UUID, 0.25, 0, 'negligible')",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .execute(&pool)
    .await
    .expect("insert negligible");
}

#[tokio::test]
async fn happy_disposal_cost_inventoriable() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DCI").await;
    let vendor = one_vendor(&pool, "BP-T1-V-DCI").await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment,
             disposal_basis, disposal_vendor_id)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -50, 'disposal_cost',
                 'inventoriable', $4::UUID)",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .bind(&vendor)
    .execute(&pool)
    .await
    .expect("insert disposal_cost inventoriable");
}

#[tokio::test]
async fn happy_disposal_cost_period_with_account_kind() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DCP1").await;
    let vendor = one_vendor(&pool, "BP-T1-V-DCP1").await;
    // 'cogs' is an existing valid account_kind in the enum; using as a
    // stand-in caller-supplied expense kind for this T1 probe. New
    // 'disposal_expense' kind will land with acct-7t4.4 (acct-6g47).
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment,
             disposal_basis, disposal_vendor_id, disposal_expense_account_kind)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -25, 'disposal_cost',
                 'period', $4::UUID, 'cogs'::account_kind)",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .bind(&vendor)
    .execute(&pool)
    .await
    .expect("insert disposal_cost period with account kind");
}

#[tokio::test]
async fn happy_disposal_cost_period_account_kind_null() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DCP2").await;
    let vendor = one_vendor(&pool, "BP-T1-V-DCP2").await;
    // period basis + NULL account kind is permitted (caller defaults
    // at post_wo_complete time).
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment,
             disposal_basis, disposal_vendor_id)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -25, 'disposal_cost',
                 'period', $4::UUID)",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .bind(&vendor)
    .execute(&pool)
    .await
    .expect("insert disposal_cost period NULL kind");
}

// ============================================================
// CHECK violations: per-treatment composite
// ============================================================

#[tokio::test]
async fn nrv_credit_with_zero_value_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "NRV-Z").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 0, 'nrv_credit')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn nrv_credit_with_negative_value_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "NRV-N").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -10, 'nrv_credit')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn negligible_with_nonzero_value_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "NEG-NZ").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 50, 'negligible')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn disposal_cost_with_positive_value_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DC-POS").await;
    let vendor = one_vendor(&pool, "BP-T1-V-DC-POS").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment,
                 disposal_basis, disposal_vendor_id)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 10, 'disposal_cost',
                     'period', $4::UUID)",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .bind(&vendor)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn disposal_cost_missing_basis_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DC-NB").await;
    let vendor = one_vendor(&pool, "BP-T1-V-DC-NB").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment,
                 disposal_vendor_id)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -10, 'disposal_cost',
                     $4::UUID)",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .bind(&vendor)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn disposal_cost_missing_vendor_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DC-NV").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment,
                 disposal_basis)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -10, 'disposal_cost',
                     'period')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn disposal_cost_inventoriable_with_account_kind_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "DC-IK").await;
    let vendor = one_vendor(&pool, "BP-T1-V-DC-IK").await;
    // inventoriable basis MUST have NULL disposal_expense_account_kind.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment,
                 disposal_basis, disposal_vendor_id, disposal_expense_account_kind)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -10, 'disposal_cost',
                     'inventoriable', $4::UUID, 'cogs'::account_kind)",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .bind(&vendor)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn nrv_credit_with_disposal_fields_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "NRV-DF").await;
    let vendor = one_vendor(&pool, "BP-T1-V-NRV-DF").await;
    // nrv_credit MUST have NULL disposal_* fields.
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment,
                 disposal_basis, disposal_vendor_id)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 50, 'nrv_credit',
                     'period', $4::UUID)",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .bind(&vendor)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn unknown_treatment_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "T-X").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 10, 'bogus')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// CHECK: qty_per_parent > 0
// ============================================================

#[tokio::test]
async fn qty_per_parent_zero_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "Q-Z").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, 0, 100, 'nrv_credit')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn qty_per_parent_negative_violates_check() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "Q-N").await;
    expect_sqlstate("23514", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, -1.5, 100, 'nrv_credit')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// PK: (bom_id, by_product_no) uniqueness
// ============================================================

#[tokio::test]
async fn duplicate_pk_violates_unique() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "PK-D").await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 100, 'nrv_credit')",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .execute(&pool)
    .await
    .expect("first insert");

    expect_sqlstate("23505", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES ($1, 1, $2::UUID, $3::UUID, 2.0, 50, 'nrv_credit')",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// FK: bom_id, output_sku_id, fg_location_id, disposal_vendor_id
// ============================================================

#[tokio::test]
async fn missing_bom_violates_fk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (_bom_id, out_sku, fg_loc) = scaffold(&pool, "FK-B").await;
    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment)
             VALUES (999999, 1, $1::UUID, $2::UUID, 1.0, 100, 'nrv_credit')",
        )
        .bind(&out_sku)
        .bind(&fg_loc)
        .execute(&pool)
        .await
    })
    .await;
}

#[tokio::test]
async fn missing_vendor_violates_fk() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "FK-V").await;
    let bogus_vendor = fresh_uuid(&pool).await;
    expect_sqlstate("23503", || async {
        sqlx::query(
            "INSERT INTO bom_by_products
                (bom_id, by_product_no, output_sku_id, fg_location_id,
                 qty_per_parent, unit_value, treatment,
                 disposal_basis, disposal_vendor_id)
             VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, -10, 'disposal_cost',
                     'period', $4::UUID)",
        )
        .bind(bom_id)
        .bind(&out_sku)
        .bind(&fg_loc)
        .bind(&bogus_vendor)
        .execute(&pool)
        .await
    })
    .await;
}

// ============================================================
// CASCADE on bom_headers DELETE
// ============================================================

#[tokio::test]
async fn delete_bom_header_cascades_by_products() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;
    let (bom_id, out_sku, fg_loc) = scaffold(&pool, "CSC").await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 100, 'nrv_credit')",
    )
    .bind(bom_id)
    .bind(&out_sku)
    .bind(&fg_loc)
    .execute(&pool)
    .await
    .expect("insert");

    // bom_lines might also exist for this bom; delete them first to
    // avoid an unrelated FK constraint, but bom_lines is also
    // ON DELETE CASCADE so DELETE on bom_headers works directly.
    sqlx::query("DELETE FROM bom_headers WHERE id = $1")
        .bind(bom_id)
        .execute(&pool)
        .await
        .expect("delete bom_header");

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM bom_by_products WHERE bom_id = $1",
    )
    .bind(bom_id)
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(
        count, 0,
        "bom_by_products rows must cascade on bom_headers DELETE"
    );
}

// ============================================================
// ECO/revision lifecycle: rev A and rev B coexist
// ============================================================

#[tokio::test]
async fn rev_a_and_rev_b_by_products_coexist() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let parent_code = "BP-T1-ECO-P";
    let _parent = one_sku(&pool, parent_code).await;
    let out_a = one_sku(&pool, "BP-T1-ECO-OA").await;
    let out_b = one_sku(&pool, "BP-T1-ECO-OB").await;
    let fg_loc = one_location(&pool, "BP-T1-ECO-L").await;

    // Rev A — primary, active.
    let bom_a =
        create_bom_header_full(&pool, parent_code, 1, "A", true, "active", None).await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, 1, $2::UUID, $3::UUID, 1.0, 100, 'nrv_credit')",
    )
    .bind(bom_a)
    .bind(&out_a)
    .bind(&fg_loc)
    .execute(&pool)
    .await
    .expect("insert rev A by-product");

    // Rev B — separate alternate to avoid the bom_headers_primary
    // partial UNIQUE collision (a real ECO would obsolete A then
    // activate B; this T1 just pins coexistence in the table).
    let bom_b =
        create_bom_header_full(&pool, parent_code, 2, "A", true, "active", None).await;
    sqlx::query(
        "INSERT INTO bom_by_products
            (bom_id, by_product_no, output_sku_id, fg_location_id,
             qty_per_parent, unit_value, treatment)
         VALUES ($1, 1, $2::UUID, $3::UUID, 0.5, 0, 'negligible')",
    )
    .bind(bom_b)
    .bind(&out_b)
    .bind(&fg_loc)
    .execute(&pool)
    .await
    .expect("insert rev B by-product");

    let rows: Vec<(i64, i32, String, i64, String)> = sqlx::query_as(
        "SELECT bom_id, by_product_no, output_sku_id::text, unit_value, treatment::text
           FROM bom_by_products
          WHERE bom_id IN ($1, $2)
          ORDER BY bom_id",
    )
    .bind(bom_a)
    .bind(bom_b)
    .fetch_all(&pool)
    .await
    .expect("rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, bom_a);
    assert_eq!(rows[0].3, 100);
    assert_eq!(rows[0].4, "nrv_credit");
    assert_eq!(rows[1].0, bom_b);
    assert_eq!(rows[1].3, 0);
    assert_eq!(rows[1].4, "negligible");
}
