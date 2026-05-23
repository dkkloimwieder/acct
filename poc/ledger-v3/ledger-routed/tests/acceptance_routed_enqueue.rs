//! acct-p0d8 acceptance: one Path B enqueue materializes a complete
//! consistent set of rows.
//!
//! design-v3 §8.3 first bullet: "Submit, verify submission accepted;
//! poll for trx existence by (trx_type, source_id); confirm trx
//! appears after processing." Plus the §4.4/§5.6 invariant that one
//! receipt produces (1 trx, 1 trx_line, 1 pool_state layer, 1
//! posting_line) with consistent amount = qty × unit_cost.
//!
//! Mirror shape of acceptance_direct_single_trx but exercised through
//! the routed pipeline (enqueue → router → committer → bulk_write →
//! COMMIT → cleanup).

mod common;

use common::*;
use std::time::Duration;

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn single_enqueue_lands_consistent_rows() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal before enqueue"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    let submission_seq = enqueue(
        &pool,
        "po_receipt",
        201,
        "2026-05-22T12:00:00+00:00",
        &po_receipt_line(pool_id, 10, 50, inv, ap),
    )
    .await
    .expect("enqueue");

    assert!(submission_seq > 0, "shmem submission_seq should be positive");

    let trx_id = poll_for_trx(&pool, "po_receipt", 201, Duration::from_secs(10))
        .await
        .expect("trx must materialize via routed pipeline");

    let (trx_type, src): (String, i64) =
        sqlx::query_as("SELECT trx_type::text, source_id FROM trx WHERE id = $1")
            .bind(trx_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(trx_type, "po_receipt");
    assert_eq!(src, 201);

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

    let (ps_qty, ps_uc, last_tl): (i64, i64, i64) = sqlx::query_as(
        "SELECT qty, unit_cost, last_trx_line_id FROM pool_state WHERE pool_id = $1",
    )
    .bind(pool_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ps_qty, 10);
    // WAC stores value_sum in unit_cost (qty × per-unit = 10 × 50 = 500).
    assert_eq!(ps_uc, 500);
    assert_eq!(last_tl, tl_id);

    let (pl_tl, amt, deb, cred): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT trx_line_id, amount, debit_account, credit_account \
           FROM posting_line WHERE trx_line_id = $1",
    )
    .bind(tl_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pl_tl, tl_id);
    assert_eq!(amt, 500);
    assert_eq!(deb, inv);
    assert_eq!(cred, ap);

    let drains: i64 = sqlx::query_scalar("SELECT ledger_routed_committer_drains_total()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        drains >= 1,
        "committer_drains_total must register at least one drain: got {drains}"
    );
}

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn enqueue_returns_monotonic_submission_seq() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    let s1 = enqueue(
        &pool,
        "po_receipt",
        301,
        "2026-05-22T12:00:00+00:00",
        &po_receipt_line(pool_id, 1, 10, inv, ap),
    )
    .await
    .expect("enqueue 1");

    let s2 = enqueue(
        &pool,
        "po_receipt",
        302,
        "2026-05-22T12:00:01+00:00",
        &po_receipt_line(pool_id, 1, 10, inv, ap),
    )
    .await
    .expect("enqueue 2");

    assert!(
        s2 > s1,
        "submission_seq must be strictly monotonic across enqueue calls: \
         s1={s1} s2={s2}"
    );

    assert!(poll_for_trx(&pool, "po_receipt", 301, Duration::from_secs(10))
        .await
        .is_some());
    assert!(poll_for_trx(&pool, "po_receipt", 302, Duration::from_secs(10))
        .await
        .is_some());
}
