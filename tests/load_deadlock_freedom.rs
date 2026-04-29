//! T4 — Deadlock-freedom load test. **OPT-IN**, gated behind `#[ignore]`.
//!
//! Per Part IV §13's "Done when invariant tests pass" requirement and
//! the original target of "100 concurrent batches, random subsets,
//! 30 min, zero deadlocks". Defaults are smaller (32 writers / 30 s)
//! so a quick sanity run is tractable; spec-target values are reached
//! by env override.
//!
//! Trigger:
//!
//!   ./scripts/run-tests.sh deadlock -- --ignored --test-threads=1
//!
//! Env knobs (both optional; defaults shown):
//!
//!   T4_DURATION_SECS=30   wall-clock duration of the load phase
//!   T4_WRITERS=32         number of concurrent tokio writers
//!
//! Construction (deliberately narrow — see `acct-93b.19` notes):
//!
//!   Every event is `debit=<curated qty debit-normal account>,
//!   credit=creation_void(qty)`. creation_void is `unrestricted`, so
//!   no balance CHECK can fire. P0001..P0005 cannot fire either:
//!   accounts are open, both sides are qty (P0002), no currency
//!   (P0003 not applicable), business_date is in the open period
//!   (P0004/P0005 ok). Therefore the **only** failure mode this test
//!   should ever surface is a deadlock — non-deadlock errors
//!   indicate a real regression in `post_transfers`.
//!
//!   The debit pool includes both stock_wip (SKU-A op 10/20) accounts
//!   so the W2 op_move dispatch is exercised under contention — that
//!   was the scenario folded over from T3 (see acct-93b.19 notes).
//!
//! Future scaling work — including bumping Postgres max_connections
//! to support the spec-target 100 writers — is tracked separately.

mod common;

use common::*;
use serde_json::json;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
struct DebitAccount {
    id: i64,
    reason: &'static str,
    routing_op: Option<i32>,
}

/// XorShift64. Fast, non-crypto, period 2^64-1. Plenty for picking
/// random debit accounts and producing unique idempotency_keys at
/// load-test scale.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0xdead_beef_cafe_babe;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn deadlock_freedom_under_concurrent_post_transfers() {
    let duration_secs: u64 = env::var("T4_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let n_writers: u32 = env::var("T4_WRITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let duration = Duration::from_secs(duration_secs);

    let pool = connect_test_db_with(n_writers + 2).await;
    reset_to_fixture(&pool).await;

    // ---- Build the curated debit pool. ----
    let void_qty_id: i64 = sqlx::query_scalar(
        "SELECT id FROM accounts
          WHERE kind = 'creation_void'
            AND ledger_kind = 'qty'
            AND sku_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("creation_void(qty)");

    let mut accounts: Vec<DebitAccount> = Vec::new();

    for loc in &["MAIN", "OUT"] {
        let id: i64 = sqlx::query_scalar(
            "SELECT a.id FROM accounts a
               JOIN skus s      ON s.id = a.sku_id
               JOIN locations l ON l.id = a.location_id
              WHERE a.kind = 'stock_available'
                AND s.code = 'SKU-A'
                AND l.code = $1",
        )
        .bind(loc)
        .fetch_one(&pool)
        .await
        .expect("stock_available SKU-A");
        accounts.push(DebitAccount {
            id,
            reason: "cycle_count_adj",
            routing_op: None,
        });
    }

    for op in &[10i32, 20] {
        let id: i64 = sqlx::query_scalar(
            "SELECT a.id FROM accounts a
               JOIN skus s ON s.id = a.sku_id
              WHERE a.kind = 'stock_wip'
                AND s.code = 'SKU-A'
                AND a.routing_op = $1",
        )
        .bind(op)
        .fetch_one(&pool)
        .await
        .expect("stock_wip SKU-A");
        accounts.push(DebitAccount {
            id,
            reason: "op_move",
            routing_op: Some(*op),
        });
    }

    let id: i64 = sqlx::query_scalar(
        "SELECT a.id FROM accounts a
           JOIN skus s ON s.id = a.sku_id
          WHERE a.kind = 'stock_consumed'
            AND s.code = 'SKU-A'",
    )
    .fetch_one(&pool)
    .await
    .expect("stock_consumed SKU-A");
    accounts.push(DebitAccount {
        id,
        reason: "cycle_count_adj",
        routing_op: None,
    });

    // SKU-WAC stock_wip: must use cycle_count_adj — op_move would
    // raise P0006 against the WAC SKU.
    for op in &[10i32, 20] {
        let id: i64 = sqlx::query_scalar(
            "SELECT a.id FROM accounts a
               JOIN skus s ON s.id = a.sku_id
              WHERE a.kind = 'stock_wip'
                AND s.code = 'SKU-WAC'
                AND a.routing_op = $1",
        )
        .bind(op)
        .fetch_one(&pool)
        .await
        .expect("stock_wip SKU-WAC");
        accounts.push(DebitAccount {
            id,
            reason: "cycle_count_adj",
            routing_op: None,
        });
    }

    let accounts: Arc<Vec<DebitAccount>> = Arc::new(accounts);
    eprintln!(
        "T4: writers={} duration={}s debit_pool_size={} creation_void_id={}",
        n_writers,
        duration_secs,
        accounts.len(),
        void_qty_id
    );

    // ---- Run the load. ----
    let deadlocks_before = pg_deadlock_count(&pool).await;
    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_writers as usize);
    for w in 0..n_writers {
        let pool_w = pool.clone();
        let accounts_w = accounts.clone();
        let ok_w = ok_count.clone();
        let err_w = err_count.clone();
        handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((w as u64) << 32) ^ 0xa5a5_a5a5_5a5a_5a5a;

            while start.elapsed() < duration {
                let n_events = 5 + (xorshift(&mut rng) % 16) as usize;
                let mut events = Vec::with_capacity(n_events);
                for _ in 0..n_events {
                    let a = accounts_w[(xorshift(&mut rng) as usize) % accounts_w.len()];
                    let key = format!(
                        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                        xorshift(&mut rng) as u32,
                        xorshift(&mut rng) as u16,
                        xorshift(&mut rng) as u16,
                        xorshift(&mut rng) as u16,
                        xorshift(&mut rng) & 0xffff_ffff_ffff,
                    );
                    let mut ev =
                        make_event(a.reason, a.id, void_qty_id, 1, "2026-04-15", &key);
                    if let Some(op) = a.routing_op {
                        ev.as_object_mut()
                            .unwrap()
                            .insert("routing_op".into(), json!(op));
                    }
                    events.push(ev);
                }
                let batch = json!(events);
                match call_post_transfers(&pool_w, batch, false).await {
                    Ok(_) => {
                        ok_w.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let code = e
                            .as_database_error()
                            .and_then(|d| d.code().map(|c| c.into_owned()))
                            .unwrap_or_else(|| "no-code".to_string());
                        if code == "40P01" {
                            panic!("DEADLOCK on writer {w}: {e}");
                        }
                        eprintln!("writer {w} unexpected error [{code}]: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for h in handles {
        h.await.expect("writer panic");
    }
    let elapsed = start.elapsed();

    let deadlocks_after = pg_deadlock_count(&pool).await;
    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let total = ok + err;
    eprintln!(
        "T4: elapsed={:.1}s batches={} ok={} err={} throughput={:.1}/s",
        elapsed.as_secs_f64(),
        total,
        ok,
        err,
        total as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "T4: deadlocks delta = {} ({} -> {})",
        deadlocks_after - deadlocks_before,
        deadlocks_before,
        deadlocks_after
    );

    assert_eq!(
        deadlocks_after - deadlocks_before,
        0,
        "deadlock counter rose during run: {} -> {}",
        deadlocks_before,
        deadlocks_after
    );
    assert_eq!(
        err, 0,
        "non-deadlock errors during run; see eprintln output above"
    );
    assert!(total > 0, "no batches executed — duration too short?");

    let imbalances: Vec<(String, Option<String>, i64)> = sqlx::query_as(
        "SELECT ledger_kind, currency, SUM(debits_total - credits_total)::BIGINT
           FROM accounts
          GROUP BY ledger_kind, currency
         HAVING SUM(debits_total - credits_total) <> 0",
    )
    .fetch_all(&pool)
    .await
    .expect("imbalance query");
    assert!(
        imbalances.is_empty(),
        "per-ledger imbalance after load: {imbalances:?}"
    );
}
