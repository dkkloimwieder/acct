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
    let events_min: usize = env::var("T4_EVENTS_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let events_max: usize = env::var("T4_EVENTS_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    assert!(
        events_min <= events_max && events_min >= 1,
        "T4_EVENTS_MIN must be >=1 and <= T4_EVENTS_MAX (got min={events_min}, max={events_max})"
    );
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
        "T4: writers={} duration={}s events_per_batch={}-{} debit_pool_size={} creation_void_id={}",
        n_writers,
        duration_secs,
        events_min,
        events_max,
        accounts.len(),
        void_qty_id
    );

    // ---- pg_stat reset + pre-snapshots. ----
    // Reset pg_stat_statements so the post-run top-queries snapshot is
    // attributable to this run. pg_stat_database / pg_stat_io don't
    // reset on demand here (resetting cluster-wide IO stats requires
    // pg_stat_reset_shared which we leave alone), so we diff them.
    let _ = sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&pool)
        .await; // ignore result; extension may not be present in all env
    let stat_db_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));

    // pg_stat_io: sum across contexts/objects for client backend.
    let stat_io_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        "SELECT
           COALESCE(SUM(reads),     0)::BIGINT,
           COALESCE(SUM(read_bytes),0)::BIGINT,
           COALESCE(SUM(writes),    0)::BIGINT,
           COALESCE(SUM(write_bytes),0)::BIGINT,
           COALESCE(SUM(extends),   0)::BIGINT,
           COALESCE(SUM(hits),      0)::BIGINT,
           COALESCE(SUM(fsyncs),    0)::BIGINT
         FROM pg_stat_io
         WHERE backend_type = 'client backend'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0));

    // WAL position before run.
    let wal_lsn_before: String =
        sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&pool)
            .await
            .unwrap_or_default();

    // ---- Run the load. ----
    let deadlocks_before = stat_db_before.4;
    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_writers as usize);
    for w in 0..n_writers {
        let pool_w = pool.clone();
        let accounts_w = accounts.clone();
        let ok_w = ok_count.clone();
        let err_w = err_count.clone();
        let ev_w = event_count.clone();
        handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((w as u64) << 32) ^ 0xa5a5_a5a5_5a5a_5a5a;
            // Per-writer latency log in microseconds.
            let mut latencies_us: Vec<u32> = Vec::new();

            let span = (events_max - events_min + 1) as u64;
            while start.elapsed() < duration {
                let n_events = events_min + (xorshift(&mut rng) % span) as usize;
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
                let t0 = Instant::now();
                let res = call_post_transfers(&pool_w, batch, false).await;
                let dur_us = t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
                latencies_us.push(dur_us);
                match res {
                    Ok(_) => {
                        ok_w.fetch_add(1, Ordering::Relaxed);
                        ev_w.fetch_add(n_events as u64, Ordering::Relaxed);
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
            latencies_us
        }));
    }

    let mut all_latencies: Vec<u32> = Vec::new();
    for h in handles {
        let writer_lats = h.await.expect("writer panic");
        all_latencies.extend(writer_lats);
    }
    let elapsed = start.elapsed();

    // ---- Post-snapshots + percentiles. ----
    let stat_db_after = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));
    let deadlocks_after = stat_db_after.4;

    let stat_io_after = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        "SELECT
           COALESCE(SUM(reads),     0)::BIGINT,
           COALESCE(SUM(read_bytes),0)::BIGINT,
           COALESCE(SUM(writes),    0)::BIGINT,
           COALESCE(SUM(write_bytes),0)::BIGINT,
           COALESCE(SUM(extends),   0)::BIGINT,
           COALESCE(SUM(hits),      0)::BIGINT,
           COALESCE(SUM(fsyncs),    0)::BIGINT
         FROM pg_stat_io
         WHERE backend_type = 'client backend'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0));

    let wal_lsn_after: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    // Compute WAL byte delta server-side to avoid LSN math in Rust.
    let wal_bytes_delta: i64 = if !wal_lsn_before.is_empty() && !wal_lsn_after.is_empty() {
        sqlx::query_scalar(
            "SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::BIGINT",
        )
        .bind(&wal_lsn_after)
        .bind(&wal_lsn_before)
        .fetch_one(&pool)
        .await
        .unwrap_or(0)
    } else {
        0
    };

    let top_queries: Vec<(String, i64, f64, f64)> = sqlx::query_as(
        "SELECT left(query, 80) AS q,
                calls::BIGINT,
                total_exec_time,
                mean_exec_time
           FROM pg_stat_statements
          WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND query NOT ILIKE 'SELECT pg_stat_%'
            AND query NOT ILIKE 'SELECT xact_commit%'
          ORDER BY total_exec_time DESC
          LIMIT 10",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let events = event_count.load(Ordering::Relaxed);
    let total = ok + err;

    // Percentile computation (sort, pick at quantiles).
    all_latencies.sort_unstable();
    let pct = |q: f64| -> u32 {
        if all_latencies.is_empty() {
            return 0;
        }
        let idx =
            ((q * (all_latencies.len() as f64)).floor() as usize).min(all_latencies.len() - 1);
        all_latencies[idx]
    };
    let p50 = pct(0.50);
    let p95 = pct(0.95);
    let p99 = pct(0.99);
    let p99_9 = pct(0.999);
    let max_lat = *all_latencies.last().unwrap_or(&0);

    eprintln!("====================== T4 PERF SUMMARY ======================");
    eprintln!(
        "duration_s={:.2} writers={} debit_pool_size={}",
        elapsed.as_secs_f64(),
        n_writers,
        accounts.len()
    );
    eprintln!(
        "batches: total={} ok={} err={} throughput={:.1}/s",
        total,
        ok,
        err,
        total as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "events:  total={} throughput={:.1}/s",
        events,
        events as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "latency_us: p50={} p95={} p99={} p99.9={} max={} (n={})",
        p50,
        p95,
        p99,
        p99_9,
        max_lat,
        all_latencies.len()
    );
    eprintln!(
        "deadlocks: delta={} ({} -> {})",
        deadlocks_after - deadlocks_before,
        deadlocks_before,
        deadlocks_after
    );
    let xact_commit_d  = stat_db_after.0 - stat_db_before.0;
    let xact_rollbk_d  = stat_db_after.1 - stat_db_before.1;
    let blks_read_d    = stat_db_after.2 - stat_db_before.2;
    let blks_hit_d     = stat_db_after.3 - stat_db_before.3;
    eprintln!(
        "pg_stat_database: xact_commit_delta={} xact_rollback_delta={} blks_read_delta={} blks_hit_delta={}",
        xact_commit_d, xact_rollbk_d, blks_read_d, blks_hit_d
    );

    let io_reads_d   = stat_io_after.0 - stat_io_before.0;
    let io_rbytes_d  = stat_io_after.1 - stat_io_before.1;
    let io_writes_d  = stat_io_after.2 - stat_io_before.2;
    let io_wbytes_d  = stat_io_after.3 - stat_io_before.3;
    let io_extends_d = stat_io_after.4 - stat_io_before.4;
    let io_hits_d    = stat_io_after.5 - stat_io_before.5;
    let io_fsyncs_d  = stat_io_after.6 - stat_io_before.6;
    eprintln!(
        "pg_stat_io (client backend, summed across contexts): reads_delta={} read_bytes_delta={} writes_delta={} write_bytes_delta={} extends_delta={} hits_delta={} fsyncs_delta={}",
        io_reads_d, io_rbytes_d, io_writes_d, io_wbytes_d, io_extends_d, io_hits_d, io_fsyncs_d
    );

    eprintln!(
        "wal_bytes_delta={} (lsn {} -> {})",
        wal_bytes_delta, wal_lsn_before, wal_lsn_after
    );

    eprintln!("pg_stat_statements (top 10 by total_exec_time on this DB):");
    for (q, calls, total_ms, mean_ms) in &top_queries {
        eprintln!(
            "  calls={:>10} total_ms={:>10.1} mean_ms={:>8.3}  query={}",
            calls, total_ms, mean_ms, q
        );
    }
    eprintln!("=============================================================");

    // Machine-parseable single-line CSV for the multi-run aggregator
    // (scripts/run-perf-baseline.sh). Fields below — keep in sync if
    // you add or remove metrics.
    eprintln!(
        "T4_CSV_HEADER: duration_s,writers,batches_total,batches_ok,batches_err,events_total,throughput_bps,throughput_evps,p50_us,p95_us,p99_us,p999_us,max_us,deadlocks_delta,xact_commit_delta,xact_rollback_delta,blks_read_delta,blks_hit_delta,io_reads_delta,io_read_bytes_delta,io_writes_delta,io_write_bytes_delta,io_extends_delta,io_hits_delta,io_fsyncs_delta,wal_bytes_delta"
    );
    eprintln!(
        "T4_CSV_VALUES: {:.3},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        elapsed.as_secs_f64(),
        n_writers,
        total,
        ok,
        err,
        events,
        total as f64 / elapsed.as_secs_f64(),
        events as f64 / elapsed.as_secs_f64(),
        p50,
        p95,
        p99,
        p99_9,
        max_lat,
        deadlocks_after - deadlocks_before,
        xact_commit_d,
        xact_rollbk_d,
        blks_read_d,
        blks_hit_d,
        io_reads_d,
        io_rbytes_d,
        io_writes_d,
        io_wbytes_d,
        io_extends_d,
        io_hits_d,
        io_fsyncs_d,
        wal_bytes_delta
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
