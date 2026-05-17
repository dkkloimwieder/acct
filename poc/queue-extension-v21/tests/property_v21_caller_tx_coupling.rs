//! Property test for M5c.2 (acct-1hyx) caller-tx coupling. Probes
//! random mixes of caller outcomes (committed / aborted / in_progress)
//! against the eject-or-fail terminal-state contract.
//!
//! Invariants asserted per scenario:
//!
//!   P1 — committed-caller envelopes reach state ∈ {'committed',
//!        'replayed'} (no spurious failures).
//!   P2 — aborted-caller envelopes reach state='failed' with
//!        error_code='caller_tx_aborted' (lazy INSERT — caller's tx
//!        is final so PG sees the prior 'queued' row as a dead tuple
//!        and INSERT proceeds without XactLockTableWait).
//!   P3 — in_progress-caller envelopes (held open past the test's
//!        eject budget AND past caller_tx_timeout_ms) are silently
//!        DROPPED: the committer does NOT lazy-INSERT submission_status
//!        for in_progress terminal-fail (would XactLockTableWait on the
//!        caller's still-open tx). From the admin tx's view, no status
//!        row exists for these correlation_ids. The shmem staging
//!        entry's valid returns to 0 once cleanup runs.
//!   P4 — I-committer-non-blocking: no committer tick exceeded a wall-
//!        time threshold (proxy for "never slept on a caller"). The
//!        threshold is generous (5s) — anything past that indicates
//!        the committer is sleeping/blocking on a caller's tx.
//!
//! Run via:
//!   cargo test --release --test property_v21_caller_tx_coupling \
//!     --features pg18 --no-default-features -- --ignored --nocapture
//!
//! PROPTEST_CASES env var overrides case count (default 8).

#![cfg(test)]

mod common;

use common::{POC_DSN, connect_pool, reset_state};
use proptest::prelude::*;
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::PgPool;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
enum CallerOutcome {
    Committed,
    Aborted,
    InProgress,
}

fn outcome_strategy() -> impl Strategy<Value = CallerOutcome> {
    prop_oneof![
        Just(CallerOutcome::Committed),
        Just(CallerOutcome::Aborted),
        Just(CallerOutcome::InProgress),
    ]
}

fn scenario_strategy() -> impl Strategy<Value = Vec<CallerOutcome>> {
    prop::collection::vec(outcome_strategy(), 3..=6)
}

async fn enqueue_one(
    conn: &mut PgConnection,
    cid: Uuid,
    sku: i64,
    chrono: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": 1,
        "qty": 5,
        "unit_cost": 100,
        "business_date_jdate": 9999,
        "doc_chrono": chrono,
        "document_id": 9_400_000 + chrono,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, 1]], "wip": [] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, $2, $3::jsonb, $4::jsonb, false)")
        .bind(cid)
        .bind("po_receipt")
        .bind(payload)
        .bind(pool_keys)
        .execute(conn)
        .await
        .map(|_| ())
}

struct ScenarioEnvelope {
    cid: Uuid,
    outcome: CallerOutcome,
    /// Held connection for InProgress envelopes (BEGIN still open at
    /// the end of setup). None for Committed/Aborted.
    held_conn: Option<PgConnection>,
}

async fn run_scenario(pool: &PgPool, outcomes: Vec<CallerOutcome>) {
    reset_state(pool).await;

    // Use a non-pool admin connection for tick driving. The eject
    // GUCs were already set globally via the runner's setup; no
    // per-scenario GUC mucking needed.
    let mut admin = PgConnection::connect(POC_DSN).await.expect("admin conn");

    sqlx::query("BEGIN").execute(&mut admin).await.unwrap();

    let mut envelopes: Vec<ScenarioEnvelope> = Vec::with_capacity(outcomes.len());
    for (i, outcome) in outcomes.iter().enumerate() {
        let cid = Uuid::new_v4();
        let sku = 9_400_001 + i as i64;
        let chrono = (i + 1) as i64;
        match outcome {
            CallerOutcome::Committed => {
                let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
                sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
                enqueue_one(&mut conn, cid, sku, chrono).await.unwrap();
                sqlx::query("COMMIT").execute(&mut conn).await.unwrap();
                envelopes.push(ScenarioEnvelope { cid, outcome: *outcome, held_conn: None });
            }
            CallerOutcome::Aborted => {
                let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
                sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
                enqueue_one(&mut conn, cid, sku, chrono).await.unwrap();
                sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
                envelopes.push(ScenarioEnvelope { cid, outcome: *outcome, held_conn: None });
            }
            CallerOutcome::InProgress => {
                let mut conn = PgConnection::connect(POC_DSN).await.expect("conn");
                sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
                enqueue_one(&mut conn, cid, sku, chrono).await.unwrap();
                // DO NOT commit. Hold the connection in the vec so the
                // tx stays in_progress for the entire scenario.
                envelopes.push(ScenarioEnvelope {
                    cid,
                    outcome: *outcome,
                    held_conn: Some(conn),
                });
            }
        }
    }

    // Wait past caller_tx_timeout_ms so the timeout branch is reachable
    // for held-open envelopes. 2200ms > 2000ms timeout.
    tokio::time::sleep(Duration::from_millis(2200)).await;

    // Drive drain+tick cycles. P4: bound each tick to < 5s.
    for _ in 0..(envelopes.len() * 5 + 10) {
        let drain_start = Instant::now();
        let _: i64 = sqlx::query_scalar("SELECT poc_v21_test_router_drain()")
            .fetch_one(&mut admin)
            .await
            .unwrap_or(0);
        let _: bool = sqlx::query_scalar("SELECT poc_v21_test_committer_tick()")
            .fetch_one(&mut admin)
            .await
            .unwrap_or(false);
        let elapsed = drain_start.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "P4 I-committer-non-blocking: tick took {elapsed:?} (>5s) — committer is blocked on a caller"
        );

        // Check whether all non-InProgress envelopes have terminal
        // status rows visible. In_progress callers don't write
        // submission_status from the committer side (P3 — drop semantics),
        // so we only require committed + aborted to be visible.
        let non_in_progress_cids: Vec<Uuid> = envelopes
            .iter()
            .filter(|e| !matches!(e.outcome, CallerOutcome::InProgress))
            .map(|e| e.cid)
            .collect();
        let cnt: (i64,) = sqlx::query_as(
            "SELECT COUNT(*)::BIGINT FROM poc_v21_submission_status \
             WHERE correlation_id = ANY($1::uuid[]) \
               AND state IN ('committed', 'failed', 'replayed')",
        )
        .bind(&non_in_progress_cids)
        .fetch_one(&mut admin)
        .await
        .unwrap_or((0,));
        if cnt.0 == non_in_progress_cids.len() as i64 {
            break;
        }
    }

    // Drop held connections so they don't block the test pool.
    for env in envelopes.iter_mut() {
        if let Some(mut conn) = env.held_conn.take() {
            let _ = sqlx::query("ROLLBACK").execute(&mut conn).await;
        }
    }

    // Assertions on terminal states.
    let cids: Vec<Uuid> = envelopes.iter().map(|e| e.cid).collect();
    let rows: Vec<(Uuid, String, Option<String>)> = sqlx::query_as(
        "SELECT correlation_id, state, error_code FROM poc_v21_submission_status \
         WHERE correlation_id = ANY($1::uuid[])",
    )
    .bind(&cids)
    .fetch_all(&mut admin)
    .await
    .expect("status rows");
    let by_cid: std::collections::HashMap<Uuid, (String, Option<String>)> = rows
        .into_iter()
        .map(|(cid, state, err)| (cid, (state, err)))
        .collect();

    for env in &envelopes {
        let visible = by_cid.get(&env.cid);
        match env.outcome {
            CallerOutcome::Committed => {
                let (state, _err) = visible.unwrap_or_else(|| {
                    panic!("P1 committed-caller {} must have a status row", env.cid)
                });
                assert!(
                    state == "committed" || state == "replayed",
                    "P1 committed-caller {} should reach committed/replayed, got {}",
                    env.cid,
                    state
                );
            }
            CallerOutcome::Aborted => {
                let (state, err) = visible.unwrap_or_else(|| {
                    panic!("P2 aborted-caller {} must have a lazy 'failed' row", env.cid)
                });
                assert_eq!(
                    state, "failed",
                    "P2 aborted-caller {} must be failed",
                    env.cid
                );
                assert_eq!(err.as_deref(), Some("caller_tx_aborted"));
            }
            CallerOutcome::InProgress => {
                // P3 — committer does NOT write submission_status for
                // in_progress terminal-fail. From the admin tx's view
                // (separate connection), the caller's still-open
                // 'queued' row is invisible. Net: no row visible here.
                assert!(
                    visible.is_none(),
                    "P3 in_progress-caller {} must have NO visible status row (got {:?})",
                    env.cid,
                    visible
                );
            }
        }
    }

    sqlx::query("ROLLBACK").execute(&mut admin).await.unwrap();
}

#[test]
#[ignore]
fn prop_v21_caller_tx_coupling() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let pool = runtime.block_on(connect_pool());

    // Sighup GUCs — set globally via ALTER SYSTEM + pg_reload_conf
    // once at runner start; revert at runner end. Scenarios share the
    // tight eject budget (max=2, timeout=2000ms).
    runtime.block_on(async {
        sqlx::query("ALTER SYSTEM SET poc_v21.max_eject_count = 2")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("ALTER SYSTEM SET poc_v21.caller_tx_timeout_ms = 2000")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("SELECT pg_reload_conf()")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    });

    let cases: u32 = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases,
        max_shrink_iters: 8,
        ..proptest::test_runner::Config::default()
    });
    let strategy = scenario_strategy();
    let proptest_result = runner.run(&strategy, |outcomes| {
        let pool = pool.clone();
        runtime.block_on(async move {
            run_scenario(&pool, outcomes).await;
        });
        Ok(())
    });

    // Always revert the global GUCs, even on proptest failure.
    runtime.block_on(async {
        let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.max_eject_count")
            .execute(&pool)
            .await;
        let _ = sqlx::query("ALTER SYSTEM RESET poc_v21.caller_tx_timeout_ms")
            .execute(&pool)
            .await;
        let _ = sqlx::query("SELECT pg_reload_conf()").execute(&pool).await;
    });

    proptest_result.expect("proptest run");
}
