//! acct-sp11 acceptance: deplete more than is on hand → SQLSTATE 23000,
//! no rows written.
//!
//! Verifies that the LedgerError::InsufficientInventory branch raises
//! the integrity_constraint_violation SQLSTATE (per acct-d74b mapping)
//! and that the caller's user-tx rolls back atomically — no trx,
//! trx_line, pool_state delta, or posting_line is left behind.

mod common;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn oversold_raises_23000_and_writes_nothing() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fifo_fixture(&pool).await;

    // Seed a 5-qty layer first.
    submit(
        &pool,
        "po_receipt",
        1,
        "2026-05-21T12:00:00+00:00",
        &po_receipt_line(pool_id, 5, 100, inv, ap),
    )
    .await
    .expect("seed receipt");

    let trx_count_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM trx").fetch_one(&pool).await.unwrap();
    let pl_count_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM posting_line").fetch_one(&pool).await.unwrap();

    // Try to deplete 10 — only 5 available.
    let err = submit(
        &pool,
        "transfer_shipment",
        99,
        "2026-05-21T12:30:00+00:00",
        &depletion_line(pool_id, 10, 100, inv, ap),
    )
    .await
    .expect_err("oversold must error");

    let sqlstate = err
        .as_database_error()
        .and_then(|e| e.code())
        .map(|c| c.into_owned())
        .unwrap_or_default();
    assert_eq!(
        sqlstate, "23000",
        "expected integrity_constraint_violation, got {sqlstate}: {err}"
    );

    // No new rows.
    let trx_count_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM trx").fetch_one(&pool).await.unwrap();
    let pl_count_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM posting_line").fetch_one(&pool).await.unwrap();
    assert_eq!(trx_count_after, trx_count_before, "no new trx row");
    assert_eq!(pl_count_after, pl_count_before, "no new posting_line row");

    // Layer untouched.
    let layer_qty: i64 = sqlx::query_scalar(
        "SELECT qty FROM pool_state WHERE pool_id = $1 AND layer_seq = 1",
    )
    .bind(pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layer_qty, 5, "layer qty must be unchanged");
}
