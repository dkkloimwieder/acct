//! `acct-17x` — `periods.periods_no_overlap` EXCLUDE constraint.
//!
//! Adjacent (non-overlapping) periods must remain legal; overlapping
//! periods must raise SQLSTATE 23P01 (exclusion_violation) at INSERT
//! time.

mod common;

use common::{connect_test_db, expect_sqlstate, reset_to_fixture};

#[tokio::test]
async fn adjacent_periods_are_legal() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Fixture seeds 2026-03/04/05/06 already; 07 is adjacent to 06's end.
    sqlx::query(
        "INSERT INTO periods (code, opens_at, closes_at)
         VALUES ('2026-07', '2026-07-01', '2026-07-31')",
    )
    .execute(&pool)
    .await
    .expect("adjacent period must succeed");
}

#[tokio::test]
async fn overlapping_periods_raise_23p01() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // Try to insert a period that overlaps the fixture's 2026-04 period.
    expect_sqlstate("23P01", || async {
        sqlx::query(
            "INSERT INTO periods (code, opens_at, closes_at)
             VALUES ('2026-04-overlap', '2026-04-15', '2026-05-15')",
        )
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn period_fully_contained_in_existing_raises_23p01() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    expect_sqlstate("23P01", || async {
        sqlx::query(
            "INSERT INTO periods (code, opens_at, closes_at)
             VALUES ('2026-04-mid', '2026-04-10', '2026-04-20')",
        )
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn period_fully_containing_existing_raises_23p01() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    expect_sqlstate("23P01", || async {
        sqlx::query(
            "INSERT INTO periods (code, opens_at, closes_at)
             VALUES ('Q2-2026', '2026-04-01', '2026-06-30')",
        )
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}

#[tokio::test]
async fn single_day_overlap_at_boundary_raises_23p01() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    // 2026-04-30 is the last day of fixture's 2026-04 period.
    // Inserting a period that starts on that exact day is a 1-day overlap,
    // not an adjacency, and must be rejected.
    expect_sqlstate("23P01", || async {
        sqlx::query(
            "INSERT INTO periods (code, opens_at, closes_at)
             VALUES ('2026-04-tail', '2026-04-30', '2026-05-15')",
        )
        .execute(&pool)
        .await
        .map(|_| ())
    })
    .await;
}
