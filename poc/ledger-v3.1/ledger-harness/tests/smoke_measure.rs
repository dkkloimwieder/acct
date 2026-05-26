//! Smoke test for measure.rs (P4 acct-2ttr.8).
//!
//! Drives ~50 `ledger_submit_trx_c` receipts against poc_v3_1, takes foreground
//! start/end snapshots, asserts non-zero deltas for xact_commit and WAL LSN
//! bytes — proves the counter plumbing actually flows against the v3.1 SPI.
//!
//! Ignored by default (needs a running poc_v3_1 with ledger_direct_c installed).

#[path = "../src/measure.rs"]
mod measure;

use measure::{take_snapshot, MeasureCollector, MeasureReport};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

const DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3_1";

#[tokio::test]
#[ignore = "needs running poc_v3_1 with ledger_direct_c installed"]
async fn collector_captures_nonzero_deltas() {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5))
        .connect(DSN)
        .await
        .expect("connect");

    // Minimal fixture (v3.1 tables carry explicit BIGINT ids).
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, trx_line, trx, \
                       pool_state, pool_lock, pool, standard_cost, sku, location, account \
                       RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO sku (id, code, name) VALUES (1,'S','S')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO location (id, code, name) VALUES (1,'L','L')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account (id, code, name, type) VALUES (1000,'inv','Inv','asset'::account_type)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account (id, code, name, type) VALUES (2000,'ap','AP','liability'::account_type)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO pool (id, sku_id, location_id, method, provisional_basis) \
         VALUES (1, 1, 1, 'fifo'::pool_method, 'running_avg'::pool_provisional_basis)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let collector = MeasureCollector::spawn(pool.clone(), 200);
    let start = take_snapshot(&pool).await.expect("start snapshot");

    for src in 1..=50 {
        let lines = serde_json::json!([{
            "pool_id": 1, "line_type": "po_receipt_line", "source_id": src,
            "qty": 1, "unit_cost": 10, "debit_account": 1000, "credit_account": 2000
        }]);
        sqlx::query("SELECT ledger_submit_trx_c('po_receipt', $1, '2026-05-25T12:00:00+00:00', $2::jsonb)")
            .bind(src as i64)
            .bind(&lines)
            .execute(&pool)
            .await
            .unwrap();
    }

    // PG 18 flushes per-backend stats ~1s from idle backends; sleep so all pool
    // connections flush their xact_commit before the end snapshot.
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let end = take_snapshot(&pool).await.expect("end snapshot");
    let mut report: MeasureReport = collector.shutdown().await;
    report.xact_commit_delta = end.xact_commit - start.xact_commit;
    report.xact_rollback_delta = end.xact_rollback - start.xact_rollback;
    report.wal_lsn_bytes_delta = end.wal_lsn_bytes - start.wal_lsn_bytes;

    assert!(
        report.xact_commit_delta >= 50,
        "expected >= 50 commits, got {} (samples={}, errors={})",
        report.xact_commit_delta, report.samples_taken, report.poll_errors
    );
    assert!(report.wal_lsn_bytes_delta > 0, "wal_lsn_bytes delta should be > 0");
    assert_eq!(report.poll_errors, 0);
    assert!(report.samples_taken >= 2);

    let bytes_per_commit = report.wal_bytes_per_commit();
    assert!(bytes_per_commit > 0.0 && bytes_per_commit.is_finite());
    eprintln!(
        "[smoke_measure] commits={}, wal_bytes={}, bytes/commit={:.0}, wait_events={}",
        report.xact_commit_delta, report.wal_lsn_bytes_delta, bytes_per_commit,
        report.wait_events.len()
    );
}
