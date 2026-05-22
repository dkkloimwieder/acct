//! acct-p0d8 acceptance: postmaster restart with in-flight submissions.
//!
//! design-v3 §10.5 fifth bullet: "Postmaster crash: shmem lost.
//! In-flight submissions lost. No DB orphans (no trx rows were written
//! for in-flight work). Previously-committed trx rows are durable and
//! intact."
//!
//! Sequence:
//!   1. submit + poll for trx_1 (proof the pipeline is healthy)
//!   2. submit (po_receipt, 9202) inside a held caller tx → committer
//!      ejects, no trx materializes
//!   3. docker restart acct-postgres → shmem evaporates, BGW workers
//!      respawn after recovery, host-side connection pool is force-closed
//!   4. reconnect, verify:
//!      - trx_1 still exists (durability)
//!      - no trx row for 9202 (in-flight lost cleanly, no DB orphan)
//!      - no trx_line / posting_line / pool_state remnants for 9202
//!      - system accepts + processes a fresh submission (po_receipt, 9203)
//!
//! The restart is invoked via `docker restart acct-postgres` from
//! inside the test. This is permitted because the cluster-per-binary
//! harness already restarts between binaries; doing it inside a single
//! binary just makes the restart-and-verify cycle explicit.

mod common;

use common::*;
use std::process::Command;
use std::time::Duration;

const RESTART_WAIT: Duration = Duration::from_secs(8);
const POLL_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed; will restart acct-postgres mid-test"]
async fn postmaster_restart_preserves_durable_loses_inflight() {
    // ── Phase 1: pre-restart health ──────────────────────────────────
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(10)).await,
        "recovery_complete must signal pre-restart"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    let _ = enqueue(
        &pool,
        "po_receipt",
        9201,
        "2026-05-22T12:00:00+00:00",
        &po_receipt_line(pool_id, 5, 20, inv, ap),
    )
    .await
    .expect("first enqueue");
    let id_9201 = poll_for_trx(&pool, "po_receipt", 9201, POLL_TIMEOUT)
        .await
        .expect("pre-restart trx must materialize");

    // Cache durable assertions BEFORE restart to compare after.
    let pre_trx_id: i64 = id_9201;
    let (pre_qty, pre_uc): (i64, i64) =
        sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(pre_qty, 5);
    assert_eq!(pre_uc, 20);

    // ── Phase 2: in-flight submission inside held caller tx ──────────
    let mut conn_a = pool.acquire().await.expect("acquire Session A");
    sqlx::query("BEGIN")
        .execute(&mut *conn_a)
        .await
        .expect("BEGIN Session A");
    let lines = po_receipt_line(pool_id, 99, 99, inv, ap);
    let lines_json: serde_json::Value =
        serde_json::from_str(&lines).expect("po_receipt_line JSON parses");
    sqlx::query("SELECT ledger_enqueue_trx('po_receipt', 9202, $1, $2::jsonb)")
        .bind("2026-05-22T12:00:01+00:00")
        .bind(&lines_json)
        .execute(&mut *conn_a)
        .await
        .expect("enqueue inside held tx");

    // Give the committer time to observe the in-flight caller and eject
    // at least once — confirms the submission is genuinely in flight.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let no_trx_pre_restart: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx \
          WHERE trx_type = 'po_receipt'::trx_type AND source_id = 9202",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        no_trx_pre_restart, 0,
        "in-flight submission must NOT have a trx row pre-restart"
    );

    // Drop the pool so sqlx doesn't try to keep dead connections alive
    // across the restart. New post-restart connections use a fresh pool.
    drop(conn_a);
    drop(pool);

    // ── Phase 3: postmaster restart ──────────────────────────────────
    let status = tokio::task::spawn_blocking(|| {
        Command::new("docker")
            .args(["restart", "acct-postgres"])
            .status()
            .expect("spawn docker restart")
    })
    .await
    .expect("join docker restart task");
    assert!(status.success(), "docker restart acct-postgres must succeed");
    tokio::time::sleep(RESTART_WAIT).await;

    // ── Phase 4: post-restart verification ───────────────────────────
    let pool = connect_pool().await;
    assert!(
        wait_for_recovery_complete(&pool, Duration::from_secs(15)).await,
        "recovery_complete must signal AFTER restart"
    );

    let post_trx_9201: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM trx WHERE trx_type = 'po_receipt'::trx_type AND source_id = 9201",
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(
        post_trx_9201,
        Some(pre_trx_id),
        "durable trx 9201 must survive postmaster restart with same id"
    );

    let post_trx_9202: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM trx \
          WHERE trx_type = 'po_receipt'::trx_type AND source_id = 9202",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        post_trx_9202, 0,
        "in-flight trx 9202 must NOT exist post-restart (no DB orphan)"
    );

    // pool_state still reflects only trx 9201's effect, not 9202's.
    let (post_qty, post_uc): (i64, i64) =
        sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        post_qty, pre_qty,
        "pool_state qty must match pre-restart"
    );
    assert_eq!(post_uc, pre_uc);

    let line_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM trx_line WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(line_count, 1, "only the durable 9201's trx_line survives");

    // ── Phase 5: system accepts + processes a fresh submission ───────
    let _ = enqueue(
        &pool,
        "po_receipt",
        9203,
        "2026-05-22T13:00:00+00:00",
        &po_receipt_line(pool_id, 3, 10, inv, ap),
    )
    .await
    .expect("post-restart enqueue");
    let id_9203 = poll_for_trx(&pool, "po_receipt", 9203, POLL_TIMEOUT)
        .await
        .expect("fresh trx must materialize post-restart");
    assert!(id_9203 > pre_trx_id);

    let (qty_after_9203, _): (i64, i64) =
        sqlx::query_as("SELECT qty, unit_cost FROM pool_state WHERE pool_id = $1")
            .bind(pool_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        qty_after_9203,
        pre_qty + 3,
        "pool_state must reflect both 9201 and 9203 receipts (no 9202)"
    );
}
