//! Acceptance tests for `ledger_staging_drain` (SPIKE-A shape, design-v3.2
//! §3): claim semantics (SKIP LOCKED, id order, limit), per-submission failure
//! isolation via subtransactions, outcome marking, and no-double-apply under
//! concurrent committers.

mod common;

use common::*;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "needs running poc_v3_2 with ledger_direct installed"]
async fn staging_drain() {
    let pool = connect_pool().await;

    // ── empty queue returns 0 ───────────────────────────────────────────
    reset_state(&pool).await;
    assert_eq!(drain(&pool, 10).await, 0, "empty queue");

    // ── happy path: enqueue → drain → done + ledger state matches direct ─
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "wac", "running_avg").await;
    enqueue(&pool, "po_receipt", 1, json!([line(f.pool_id, "po_receipt_line", 10, 100)])).await;
    enqueue(&pool, "inv_adjustment", 2, json!([line(f.pool_id, "inv_adjustment_line", -4, 0)]))
        .await;

    assert_eq!(drain(&pool, 10).await, 2, "claims both rows");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'done'").await,
        2,
        "both marked done"
    );
    // Same math as the direct path: 10@100 then deplete 4 at avg 100.
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((6, 100, 600)), "drained state");
    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, 2);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM ledger_inbox WHERE drained_at IS NULL").await,
        0,
        "drained_at stamped"
    );

    // ── failure isolation: a bad submission fails alone ─────────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "wac", "running_avg").await;
    enqueue(&pool, "po_receipt", 10, json!([line(f.pool_id, "po_receipt_line", 5, 100)])).await;
    // Overdraw: WAC gate rejects (insufficient) — must fail alone.
    let bad_id =
        enqueue(&pool, "inv_adjustment", 11, json!([line(f.pool_id, "inv_adjustment_line", -50, 0)]))
            .await;
    enqueue(&pool, "inv_adjustment", 12, json!([line(f.pool_id, "inv_adjustment_line", -2, 0)]))
        .await;

    assert_eq!(drain(&pool, 10).await, 3, "claims all three");
    let (status, error): (String, Option<String>) =
        sqlx::query_as("SELECT status, error FROM ledger_inbox WHERE id = $1")
            .bind(bad_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "failed", "overdraw row failed");
    assert!(
        error.as_deref().unwrap_or_default().contains("insufficient inventory"),
        "error text recorded: {error:?}"
    );
    assert_eq!(
        count(&pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'done'").await,
        2,
        "neighbors unaffected"
    );
    // Receipt 5@100 applied, overdraw rolled back, deplete 2 applied.
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((3, 100, 300)), "isolated failure");
    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, 2, "failed trx rolled back");

    // Duplicate (trx_type, source_id) inside the queue: second marks failed.
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    enqueue(&pool, "po_receipt", 20, json!([line(f.pool_id, "po_receipt_line", 1, 10)])).await;
    enqueue(&pool, "po_receipt", 20, json!([line(f.pool_id, "po_receipt_line", 1, 10)])).await;
    assert_eq!(drain(&pool, 10).await, 2);
    assert_eq!(
        count(&pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'failed'").await,
        1,
        "duplicate idempotency-key row failed"
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, 1, "applied exactly once");

    // ── limit respected; drain continues where it left off ──────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    for i in 0..5 {
        enqueue(&pool, "po_receipt", 30 + i, json!([line(f.pool_id, "po_receipt_line", 1, 10)]))
            .await;
    }
    assert_eq!(drain(&pool, 2).await, 2, "limit 2");
    assert_eq!(drain(&pool, 2).await, 2, "limit 2 again");
    assert_eq!(drain(&pool, 2).await, 1, "tail");
    assert_eq!(drain(&pool, 2).await, 0, "drained dry");
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((5, 10, 50)));

    // ── concurrent committers: SKIP LOCKED, no double-apply ────────────
    reset_state(&pool).await;
    let f = seed_fixture(&pool, "fifo", "running_avg").await;
    for i in 0..50 {
        enqueue(&pool, "po_receipt", 100 + i, json!([line(f.pool_id, "po_receipt_line", 1, 10)]))
            .await;
    }
    let mut handles = Vec::new();
    for _ in 0..4 {
        let committer = connect_pool().await;
        handles.push(tokio::spawn(async move {
            let mut claimed = 0i64;
            loop {
                let n = drain(&committer, 7).await;
                if n == 0 {
                    break;
                }
                claimed += n;
            }
            claimed
        }));
    }
    let mut total_claimed = 0;
    for h in handles {
        total_claimed += h.await.expect("committer task");
    }
    assert_eq!(total_claimed, 50, "every row claimed exactly once");
    assert_eq!(count(&pool, "SELECT count(*) FROM trx").await, 50, "no double-apply");
    assert_eq!(
        count(&pool, "SELECT count(*) FROM ledger_inbox WHERE status = 'done'").await,
        50
    );
    assert_eq!(aggregate(&pool, f.pool_id).await, Some((50, 10, 500)), "commutative total");

    println!("acceptance_staging_drain: all assertions passed");
}
