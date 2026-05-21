//! acct-sp11 acceptance: same (trx_type, source_id) twice → second
//! submission raises 23505 (UNIQUE violation).
//!
//! design-v3 §2.2: trx UNIQUE (trx_type, source_id). Path A relies on
//! the SQL-level UNIQUE constraint to reject duplicate submissions
//! atomically. Verifies the constraint surfaces as 23505 to the caller
//! and that the first submission's rows are preserved.

mod common;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn duplicate_submission_raises_unique_violation() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fifo_fixture(&pool).await;

    let first = submit(
        &pool,
        "po_receipt",
        42,
        "2026-05-21T12:00:00+00:00",
        &po_receipt_line(pool_id, 10, 50, inv, ap),
    )
    .await
    .expect("first submit");

    let err = submit(
        &pool,
        "po_receipt",
        42, // same source_id
        "2026-05-21T12:00:00+00:00",
        &po_receipt_line(pool_id, 5, 60, inv, ap),
    )
    .await
    .expect_err("duplicate must error");

    let sqlstate = err
        .as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.into_owned())
        .unwrap_or_default();
    assert_eq!(sqlstate, "23505", "expected unique_violation, got {sqlstate}: {err}");

    // First submission's rows still present.
    let trx_count: i64 = sqlx::query_scalar("SELECT count(*) FROM trx")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(trx_count, 1, "exactly one trx row");

    // Sanity that the first trx is still the one with our source_id.
    let surviving_id: i64 =
        sqlx::query_scalar("SELECT id FROM trx WHERE trx_type = 'po_receipt' AND source_id = 42")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(surviving_id, first);
}
