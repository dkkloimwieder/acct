//! Smoke test for the outbox drain worker (acct-tyq sub: acct-6hm).
//! Seeds 10_000 pending rows — one with a CHECK-violating amount=0 — and
//! verifies the worker drains them with the right terminal status counts.

mod common;

use common::outbox_worker::{DrainConfig, drain_loop};
use common::{
    account_id_by_kind_currency, account_id_stock_available, connect_test_db, reset_to_fixture,
};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const TOTAL_ROWS: i64 = 10_000;
const BAD_ROW_INDEX: i64 = 5_000;

#[tokio::test]
async fn worker_drains_seeded_outbox_with_one_failure() {
    let pool = connect_test_db().await;
    reset_to_fixture(&pool).await;

    let stock = account_id_stock_available(&pool, "SKU-A", "MAIN").await;
    let void_qty = account_id_by_kind_currency(&pool, "creation_void", None).await;

    sqlx::query(
        "INSERT INTO ledger_outbox (events)
         SELECT jsonb_build_array(jsonb_build_object(
             'reason',            'cycle_count_adj',
             'document_kind',     'test_doc',
             'document_id',       '00000000-0000-0000-0000-0000000000aa',
             'debit_account_id',  $1::BIGINT,
             'credit_account_id', $2::BIGINT,
             'amount',            CASE WHEN i = $3::BIGINT THEN 0 ELSE 1 END,
             'business_date',     '2026-04-15',
             'idempotency_key',   gen_random_uuid()::text,
             'posted_by',         '00000000-0000-0000-0000-0000000000bb'
         ))
         FROM generate_series(1, $4::BIGINT) AS i",
    )
    .bind(stock)
    .bind(void_qty)
    .bind(BAD_ROW_INDEX)
    .bind(TOTAL_ROWS)
    .execute(&pool)
    .await
    .expect("seed outbox");

    let pending_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ledger_outbox WHERE status = 'pending'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pending_before, TOTAL_ROWS);

    let drain_to_empty = Arc::new(AtomicBool::new(true));
    let hard_stop = Arc::new(AtomicBool::new(false));
    let stats = drain_loop(pool.clone(), DrainConfig::default(), drain_to_empty, hard_stop)
        .await
        .expect("drain_loop");

    assert_eq!(stats.rows_committed, (TOTAL_ROWS - 1) as u64);
    assert_eq!(stats.rows_failed, 1);

    let (pending, committed, failed): (i64, i64, i64) = sqlx::query_as(
        "SELECT
             COUNT(*) FILTER (WHERE status = 'pending'),
             COUNT(*) FILTER (WHERE status = 'committed'),
             COUNT(*) FILTER (WHERE status = 'failed')
           FROM ledger_outbox",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, 0);
    assert_eq!(committed, TOTAL_ROWS - 1);
    assert_eq!(failed, 1);

    let (failed_sqlstate, failed_attempts): (Option<String>, i32) = sqlx::query_as(
        "SELECT error_sqlstate, attempt_count
           FROM ledger_outbox
          WHERE status = 'failed'
          LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(failed_sqlstate.as_deref(), Some("23514"));
    assert_eq!(failed_attempts, 1);

    let sample_committed: i32 = sqlx::query_scalar(
        "SELECT COUNT(*)::INT
           FROM ledger_outbox
          WHERE status = 'committed' AND committed_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sample_committed, 0, "committed rows must have committed_at set");

    let posted_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM transfers")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        posted_count,
        TOTAL_ROWS - 1,
        "every committed row should have produced exactly one transfer"
    );
}
