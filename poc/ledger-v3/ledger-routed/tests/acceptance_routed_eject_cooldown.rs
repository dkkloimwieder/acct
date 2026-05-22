//! acct-6bp9 acceptance: caller-tx eject mechanism + cooldown filter.
//!
//! design-v3 §5.4 step 4 specifies: the committer queries pg_xact_status
//! on the caller's user_tx_xid; in_progress submissions are CASd back
//! into pending, eject_count is incremented, and last_eject_at_ns is
//! stamped. The router skips entries inside `eject_cooldown_ms` of
//! their last eject (§5.3 step 2) to prevent a busy re-eject loop.
//!
//! Two test cases:
//!
//!   1. `held_caller_tx_ejects_then_commits_then_processes` — the
//!      acct-6bp9 bd deliverable. Open a tx in Session A, enqueue
//!      inside it, hold for several seconds. Observe eject_total_count
//!      grow; observe no trx row exists. COMMIT Session A; observe
//!      trx materializes.
//!
//!   2. `cooldown_throttles_router_repick_after_eject` — raise
//!      `eject_cooldown_ms` to 500ms (well above the router's ~50ms
//!      wait_latch interval) via ALTER SYSTEM + pg_reload_conf and
//!      verify the eject count grows much more slowly than the router
//!      tick count over a 3s hold window.

mod common;

use common::*;
use std::time::Duration;

const RECOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const POST_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);
const HOLD_DURATION: Duration = Duration::from_secs(3);

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn held_caller_tx_ejects_then_commits_then_processes() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, RECOVERY_TIMEOUT).await,
        "recovery_complete must signal before enqueue can route"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    let before_ejects = eject_count(&pool).await;

    let mut conn_a = pool.acquire().await.expect("acquire Session A connection");
    sqlx::query("BEGIN")
        .execute(&mut *conn_a)
        .await
        .expect("BEGIN Session A");

    let lines = po_receipt_line(pool_id, 10, 50, inv, ap);
    let lines_json: serde_json::Value =
        serde_json::from_str(&lines).expect("po_receipt_line JSON parses");
    sqlx::query("SELECT ledger_enqueue_trx('po_receipt', 9001, $1, $2::jsonb)")
        .bind("2026-05-22T12:00:00+00:00")
        .bind(&lines_json)
        .execute(&mut *conn_a)
        .await
        .expect("enqueue inside open tx");

    tokio::time::sleep(HOLD_DURATION).await;

    let mid_ejects = eject_count(&pool).await;
    assert!(
        mid_ejects > before_ejects,
        "eject_total_count must grow while caller tx is held: \
         before={before_ejects} mid={mid_ejects}"
    );

    let exists: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM trx WHERE trx_type = 'po_receipt'::trx_type AND source_id = 9001",
    )
    .fetch_optional(&pool)
    .await
    .expect("SELECT trx pre-commit");
    assert!(
        exists.is_none(),
        "trx row must NOT exist while caller tx is held open: got id={exists:?}"
    );

    sqlx::query("COMMIT")
        .execute(&mut *conn_a)
        .await
        .expect("COMMIT Session A");
    drop(conn_a);

    let id = poll_for_trx(&pool, "po_receipt", 9001, POST_COMMIT_TIMEOUT).await;
    assert!(
        id.is_some(),
        "trx must materialize within {POST_COMMIT_TIMEOUT:?} after caller commit"
    );

    let pl_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM posting_line pl \
           JOIN trx_line tl ON tl.id = pl.trx_line_id \
          WHERE tl.trx_id = $1",
    )
    .bind(id.unwrap())
    .fetch_one(&pool)
    .await
    .expect("count posting_line for new trx");
    assert_eq!(pl_count, 1, "exactly one posting_line for single-line trx");
}

#[tokio::test]
#[ignore = "needs running poc_v3 with ledger_routed installed"]
async fn cooldown_throttles_router_repick_after_eject() {
    let pool = connect_pool().await;
    reset_state(&pool).await;
    assert!(
        wait_for_recovery_complete(&pool, RECOVERY_TIMEOUT).await,
        "recovery_complete must signal before enqueue can route"
    );
    let (_, _, pool_id, inv, ap) = seed_fixture(&pool, "wac").await;

    sqlx::query("ALTER SYSTEM SET ledger_routed.eject_cooldown_ms = '500'")
        .execute(&pool)
        .await
        .expect("ALTER SYSTEM SET cooldown");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await
        .expect("pg_reload_conf");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let before_ticks = router_ticks_total(&pool).await;
    let before_ejects = eject_count(&pool).await;

    let mut conn_a = pool.acquire().await.expect("acquire Session A connection");
    sqlx::query("BEGIN")
        .execute(&mut *conn_a)
        .await
        .expect("BEGIN Session A");

    let lines = po_receipt_line(pool_id, 5, 10, inv, ap);
    let lines_json: serde_json::Value =
        serde_json::from_str(&lines).expect("po_receipt_line JSON parses");
    sqlx::query("SELECT ledger_enqueue_trx('po_receipt', 9002, $1, $2::jsonb)")
        .bind("2026-05-22T12:00:00+00:00")
        .bind(&lines_json)
        .execute(&mut *conn_a)
        .await
        .expect("enqueue inside open tx");

    tokio::time::sleep(HOLD_DURATION).await;

    let mid_ticks = router_ticks_total(&pool).await;
    let mid_ejects = eject_count(&pool).await;
    let delta_ticks = mid_ticks - before_ticks;
    let delta_ejects = mid_ejects - before_ejects;

    assert!(
        delta_ticks > 0,
        "router must have ticked at least once during hold: \
         before={before_ticks} mid={mid_ticks}"
    );
    assert!(
        delta_ejects > 0,
        "at least one eject must register during hold: \
         before={before_ejects} mid={mid_ejects}"
    );
    assert!(
        delta_ejects * 4 < delta_ticks,
        "500ms cooldown over ~3s should suppress re-ejects: \
         ejects={delta_ejects} ticks={delta_ticks} (expected ejects << ticks/4)"
    );

    sqlx::query("COMMIT")
        .execute(&mut *conn_a)
        .await
        .expect("COMMIT Session A");
    drop(conn_a);

    let id = poll_for_trx(&pool, "po_receipt", 9002, POST_COMMIT_TIMEOUT).await;
    assert!(
        id.is_some(),
        "trx must materialize within {POST_COMMIT_TIMEOUT:?} after caller commit"
    );

    sqlx::query("ALTER SYSTEM RESET ledger_routed.eject_cooldown_ms")
        .execute(&pool)
        .await
        .expect("ALTER SYSTEM RESET cooldown");
    sqlx::query("SELECT pg_reload_conf()")
        .execute(&pool)
        .await
        .expect("pg_reload_conf after RESET");
}
