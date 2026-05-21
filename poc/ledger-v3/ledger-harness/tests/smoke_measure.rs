//! Smoke test for measure.rs (acct-vd83).
//!
//! Drives ~50 ledger_submit_trx calls against poc_v3, takes foreground
//! start/end snapshots, asserts non-zero deltas for xact_commit and WAL
//! LSN bytes — proves the counter plumbing actually flows.

#[path = "../src/measure.rs"]
mod measure;

use measure::{take_snapshot, MeasureCollector, MeasureReport};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

const DSN: &str = "postgres://acct:acct_dev@localhost:5111/poc_v3";

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_direct installed"]
async fn collector_captures_nonzero_deltas() {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5))
        .connect(DSN)
        .await
        .expect("connect");

    // Minimal fixture for receipts.
    sqlx::query(
        "TRUNCATE TABLE posting_line_dimension, posting_line, trx_line, trx, \
                       pool_state, pool_lock, pool, sku, location, account \
                       RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .unwrap();
    let sku_id: i64 = sqlx::query_scalar("INSERT INTO sku (code, name) VALUES ('S','S') RETURNING id")
        .fetch_one(&pool)
        .await
        .unwrap();
    let loc_id: i64 = sqlx::query_scalar("INSERT INTO location (code, name) VALUES ('L','L') RETURNING id")
        .fetch_one(&pool)
        .await
        .unwrap();
    let inv: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('1','Inv','asset') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let ap: i64 = sqlx::query_scalar(
        "INSERT INTO account (code, name, type) VALUES ('2','AP','liability') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let pool_id: i64 = sqlx::query_scalar(
        "INSERT INTO pool (sku_id, location_id, method) VALUES ($1, $2, 'fifo') RETURNING id",
    )
    .bind(sku_id)
    .bind(loc_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    let collector = MeasureCollector::spawn(pool.clone(), 200);
    let start = take_snapshot(&pool).await.expect("start snapshot");

    for src in 1..=50 {
        let lines = format!(
            "[{{\"pool_id\":{pool_id},\"line_type\":\"po_receipt_line\",\
              \"source_id\":{src},\"qty\":1,\"unit_cost\":10,\
              \"debit_account\":{inv},\"credit_account\":{ap}}}]"
        );
        let json: serde_json::Value = serde_json::from_str(&lines).unwrap();
        sqlx::query("SELECT ledger_submit_trx('po_receipt', $1, '2026-05-21T12:00:00+00:00', $2::jsonb)")
            .bind(src as i64)
            .bind(json)
            .execute(&pool)
            .await
            .unwrap();
    }

    // PG 18 pgstat_report_stat() flushes per-backend stats every
    // ~1s from idle backends. Sleep long enough that all 4 pool
    // connections have flushed their xact_commit counts before the
    // end snapshot. (For real measurement runs >10s the lag sits in
    // the noise.)
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let end = take_snapshot(&pool).await.expect("end snapshot");
    let mut report: MeasureReport = collector.shutdown().await;
    report.xact_commit_delta = end.xact_commit - start.xact_commit;
    report.xact_rollback_delta = end.xact_rollback - start.xact_rollback;
    report.wal_lsn_bytes_delta = end.wal_lsn_bytes - start.wal_lsn_bytes;

    assert!(
        report.xact_commit_delta >= 50,
        "expected >= 50 commits, got {} (samples={}, errors={})",
        report.xact_commit_delta,
        report.samples_taken,
        report.poll_errors
    );
    assert!(
        report.wal_lsn_bytes_delta > 0,
        "wal_lsn_bytes delta should be > 0, got {}",
        report.wal_lsn_bytes_delta
    );
    assert_eq!(report.poll_errors, 0);
    assert!(report.samples_taken >= 2);

    let bytes_per_commit = report.wal_bytes_per_commit();
    assert!(
        bytes_per_commit > 0.0 && bytes_per_commit.is_finite(),
        "wal_bytes_per_commit = {bytes_per_commit}"
    );
    eprintln!(
        "[smoke_measure] commits={}, wal_bytes={}, bytes/commit={:.0}, wait_events={}",
        report.xact_commit_delta,
        report.wal_lsn_bytes_delta,
        bytes_per_commit,
        report.wait_events.len()
    );
}
