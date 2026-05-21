//! acct-sp11 acceptance: one PO receipt lands all 4 tables consistent.
//!
//! Submit a single po_receipt with qty=10 unit_cost=50 into a FIFO pool;
//! verify trx + trx_line + pool_state + posting_line row counts and the
//! cross-table relationships (trx_line.trx_id, posting_line.trx_line_id,
//! amount = qty × unit_cost).

mod common;

use common::*;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn single_po_receipt_lands_consistent_rows() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    let (_, _, pool_id, inv, ap) = seed_fifo_fixture(&pool).await;

    let trx_id = submit(
        &pool,
        "po_receipt",
        101,
        "2026-05-21T12:00:00+00:00",
        &po_receipt_line(pool_id, 10, 50, inv, ap),
    )
    .await
    .expect("submit");

    assert!(trx_id > 0, "trx.id should be positive");

    // trx
    let (trx_type, src): (String, i64) =
        sqlx::query_as("SELECT trx_type::text, source_id FROM trx WHERE id = $1")
            .bind(trx_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(trx_type, "po_receipt");
    assert_eq!(src, 101);

    // trx_line — exactly one row, pool_id matches, trx_seq starts at 1
    let (tl_id, tl_pool, qty, uc, ts): (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT id, pool_id, qty, unit_cost, trx_seq FROM trx_line WHERE trx_id = $1",
    )
    .bind(trx_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tl_pool, pool_id);
    assert_eq!(qty, 10);
    assert_eq!(uc, 50);
    assert_eq!(ts, 1);

    // pool_state — one FIFO layer at seq=1
    let (layer_seq, ps_qty, ps_uc, last_tl): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT layer_seq, qty, unit_cost, last_trx_line_id \
           FROM pool_state WHERE pool_id = $1",
    )
    .bind(pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(layer_seq, 1);
    assert_eq!(ps_qty, 10);
    assert_eq!(ps_uc, 50);
    assert_eq!(last_tl, tl_id);

    // posting_line — one row, amount=500, references trx_line
    let (pl_tl, et, amt, deb, cred): (i64, String, i64, i64, i64) = sqlx::query_as(
        "SELECT trx_line_id, event_type::text, amount, debit_account, credit_account \
           FROM posting_line WHERE trx_line_id = $1",
    )
    .bind(tl_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pl_tl, tl_id);
    assert_eq!(et, "inventory_receipt");
    assert_eq!(amt, 500);
    assert_eq!(deb, inv);
    assert_eq!(cred, ap);

    // pool_lock — lazy-created
    let pl_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pool_lock WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pl_count, 1);
}
