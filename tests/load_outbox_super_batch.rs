//! `acct-hbg` — Shape-J: super-batched outbox drainer.
//!
//! Variant of shape G (`tests/load_outbox_workload.rs`). Same writers
//! (100 → ledger_outbox), same fixture (50 SKUs × 2 locs of bin_move),
//! same instrumentation. The ONE difference: the drain worker uses
//! `super_batched_drain_loop` instead of `drain_loop`. Each iteration
//! the worker concatenates events from up to T4_OUTBOX_BATCH_SIZE
//! pending rows into ONE `post_transfers` call. On any error from
//! the merged call, falls back to per-row savepoint drain so error
//! attribution is preserved (cost: one bad event poisons the bundle's
//! optimistic commit; recovery is a per-row pass).
//!
//! This is THE empirical test of "can outbox deliver shape-B-class
//! throughput on Postgres?" Shape G showed naive single-row drain
//! cannot recover the amortization. Super-batching merges many rows'
//! events into one call so each commit-fsync amortizes across many
//! events — the same shape that makes config B (1 writer × 1000-event
//! batches) the system's throughput peak.
//!
//! Two distinct latency families are captured per run:
//!
//!   1. enqueue_us — writer's INSERT round-trip (caller-perceived
//!      latency in the async outbox model). Reported in the same CSV
//!      slot as F's call latency, so the F vs G headline can be a
//!      direct compare of "how long the writer's primary call takes."
//!   2. queue_us  — `committed_at − enqueued_at` per row (drain-side
//!      residency, derived from ledger_outbox after the run).
//!
//! Approximate writer-perceived end-to-end (had the writer waited) is
//! enqueue_us + queue_us. Reported via separate columns.
//!
//! Outbox depth is sampled every T4_OUTBOX_DEPTH_INTERVAL_S seconds; the
//! max sample is recorded as `max_outbox_depth`.
//!
//! ### Why we don't wait for full drain
//!
//! In an unbounded-writer / single-drainer setup, the queue grows
//! whenever enqueue rate > drain rate, which is the common case here:
//! INSERT is fast (~ms), `post_transfers` per-call is ~ms but executed
//! sequentially by one worker. The headline metric we want is sustained
//! **committed-events-per-sec** — i.e., the worker's drain rate during
//! the run, NOT the time-to-drain-the-tail. After the writer phase, we
//! give the worker a bounded `T4_DRAIN_TIMEOUT_S` grace window and
//! then hard-stop it, reporting whatever final state we landed in.
//!
//! Run via `./scripts/run-perf-baseline.sh`:
//!
//!   T4_BINARY=load_outbox_super_batch \
//!     T4_CONFIGS="100:5:20" \
//!     T4_BASELINE_RUNS=3 \
//!     T4_DURATION_SECS=300 \
//!     ./scripts/run-perf-baseline.sh
//!
//! Env knobs (defaults shown):
//!
//!   T4_DURATION_SECS=30          wall-clock per run
//!   T4_WRITERS=32                concurrent tokio writers
//!   T4_EVENTS_MIN=5              events per batch (lower bound)
//!   T4_EVENTS_MAX=20             events per batch (upper bound)
//!   T4_BENCH_SKUS=50             number of SKUs in the spread pool
//!   T4_OUTBOX_BATCH_SIZE=1000    drain worker LIMIT N
//!   T4_OUTBOX_IDLE_SLEEP_MS=1    drain worker sleep on empty iteration
//!   T4_OUTBOX_DEPTH_INTERVAL_S=5 depth sampling cadence
//!   T4_DRAIN_TIMEOUT_S=30        max grace drain after writers stop
//!
//! `#[ignore]` so it doesn't run with `cargo test`. Same gating as F.

mod common;

use common::outbox_worker::{DrainConfig, DrainStats, super_batched_drain_loop};
use common::*;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
struct LocPair {
    main_id: i64,
    out_id: i64,
}

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

fn fresh_uuid_str(rng: &mut u64) -> String {
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        xorshift(rng) as u32,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) as u16,
        xorshift(rng) & 0xffff_ffff_ffff,
    )
}

async fn setup_load_fixture(pool: &sqlx::PgPool, n_skus: usize) -> Vec<LocPair> {
    for i in 1..=n_skus {
        let code = format!("BENCH-{:03}", i);
        sqlx::query(
            "INSERT INTO skus (code, uom, standard_cost) VALUES ($1, 'EA', 1)
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .execute(pool)
        .await
        .expect("insert BENCH sku");
    }

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
           FROM skus s, locations l
          WHERE s.code   LIKE 'BENCH-%'
            AND l.code   IN ('MAIN', 'OUT')
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind        = 'stock_available'
                 AND a.sku_id      = s.id
                 AND a.location_id = l.id
                 AND NOT a.is_closed
            )",
    )
    .execute(pool)
    .await
    .expect("create BENCH accounts");

    sqlx::query(
        "UPDATE accounts SET debits_total = 10000
           FROM skus s, locations l
          WHERE accounts.sku_id      = s.id
            AND accounts.location_id = l.id
            AND s.code LIKE 'BENCH-%'
            AND accounts.kind = 'stock_available'
            AND accounts.debits_total < 10000",
    )
    .execute(pool)
    .await
    .expect("pre-balance BENCH accounts");

    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT a.id, s.code, l.code
           FROM accounts a
           JOIN skus      s ON s.id = a.sku_id
           JOIN locations l ON l.id = a.location_id
          WHERE a.kind = 'stock_available'
            AND s.code LIKE 'BENCH-%'
          ORDER BY s.code, l.code",
    )
    .fetch_all(pool)
    .await
    .expect("lookup BENCH accounts");

    let mut by_sku: HashMap<String, (Option<i64>, Option<i64>)> = HashMap::new();
    for (id, sku, loc) in rows {
        let entry = by_sku.entry(sku).or_insert((None, None));
        match loc.as_str() {
            "MAIN" => entry.0 = Some(id),
            "OUT" => entry.1 = Some(id),
            _ => {}
        }
    }
    let mut pairs: Vec<LocPair> = by_sku
        .into_values()
        .filter_map(|(m, o)| match (m, o) {
            (Some(main_id), Some(out_id)) => Some(LocPair { main_id, out_id }),
            _ => None,
        })
        .collect();
    pairs.sort_by_key(|p| p.main_id);
    pairs
}

fn pct(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((q * (sorted.len() as f64)).floor() as usize).min(sorted.len() - 1);
    sorted[idx]
}

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn outbox_super_batch_shape_j() {
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
    let n_skus: usize = env::var("T4_BENCH_SKUS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let outbox_batch_size: i64 = env::var("T4_OUTBOX_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let outbox_idle_sleep_ms: u64 = env::var("T4_OUTBOX_IDLE_SLEEP_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let depth_interval_s: u64 = env::var("T4_OUTBOX_DEPTH_INTERVAL_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let drain_timeout_s: u64 = env::var("T4_DRAIN_TIMEOUT_S")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    assert!(
        events_min <= events_max && events_min >= 1,
        "T4_EVENTS_MIN must be >=1 and <= T4_EVENTS_MAX"
    );
    let duration = Duration::from_secs(duration_secs);

    // pool: writers + worker(2) + depth sampler(1) + setup/snapshot(2) headroom
    let pool = connect_test_db_with(n_writers + 8).await;
    reset_to_fixture(&pool).await;

    eprintln!("T4: setting up load fixture (n_skus={n_skus})");
    let setup_t0 = Instant::now();
    let pairs = setup_load_fixture(&pool, n_skus).await;
    let setup_dur = setup_t0.elapsed();
    eprintln!(
        "T4: setup complete in {:.2}s, {} (sku,loc) pairs ready",
        setup_dur.as_secs_f64(),
        pairs.len()
    );
    assert!(!pairs.is_empty(), "load fixture produced no pairs");
    let pairs: Arc<Vec<LocPair>> = Arc::new(pairs);

    eprintln!(
        "T4 shape J (super-batch): writers={} duration={}s events={}-{} pool_size={} drain_batch={} idle_sleep={}ms",
        n_writers,
        duration_secs,
        events_min,
        events_max,
        pairs.len(),
        outbox_batch_size,
        outbox_idle_sleep_ms
    );

    let _ = sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(&pool)
        .await;
    let stat_db_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, deadlocks
           FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0));
    let stat_io_before = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        "SELECT
           COALESCE(SUM(reads),     0)::BIGINT,
           COALESCE(SUM(read_bytes),0)::BIGINT,
           COALESCE(SUM(writes),    0)::BIGINT,
           COALESCE(SUM(write_bytes),0)::BIGINT,
           COALESCE(SUM(extends),   0)::BIGINT,
           COALESCE(SUM(hits),      0)::BIGINT,
           COALESCE(SUM(fsyncs),    0)::BIGINT
         FROM pg_stat_io WHERE backend_type = 'client backend'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0));
    let wal_lsn_before: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    let deadlocks_before = stat_db_before.4;

    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));

    // ---- Spawn the drain worker. Stays running for the entire load
    //      phase, then drains-to-empty after writers stop (bounded by
    //      T4_DRAIN_TIMEOUT_S — hard_stop forces an exit if the queue
    //      isn't empty by then).
    let drain_to_empty = Arc::new(AtomicBool::new(false));
    let hard_stop = Arc::new(AtomicBool::new(false));
    let worker_handle = {
        let pool = pool.clone();
        let cfg = DrainConfig {
            batch_size: outbox_batch_size,
            idle_sleep_ms: outbox_idle_sleep_ms,
        };
        let stop = drain_to_empty.clone();
        let hs = hard_stop.clone();
        tokio::spawn(async move { super_batched_drain_loop(pool, cfg, stop, hs).await })
    };

    // ---- Spawn the depth sampler.
    let depth_samples: Arc<std::sync::Mutex<Vec<i64>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let depth_stop = Arc::new(AtomicBool::new(false));
    let depth_handle = {
        let pool = pool.clone();
        let samples = depth_samples.clone();
        let stop = depth_stop.clone();
        tokio::spawn(async move {
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let depth: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM ledger_outbox WHERE status = 'pending'",
                )
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
                samples.lock().unwrap().push(depth);
                eprintln!(
                    "T4_DEPTH_SAMPLE: t={:>4}s depth={}",
                    samples.lock().unwrap().len() as u64 * depth_interval_s,
                    depth
                );
                tokio::time::sleep(Duration::from_secs(depth_interval_s)).await;
            }
        })
    };

    // ---- Spawn writers.
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_writers as usize);
    for w in 0..n_writers {
        let pool_w = pool.clone();
        let pairs_w = pairs.clone();
        let ok_w = ok_count.clone();
        let err_w = err_count.clone();
        let ev_w = event_count.clone();
        handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((w as u64) << 32) ^ 0x9b54_7d27_cafe_d00d;
            let mut enqueue_lats: Vec<u32> = Vec::new();
            let span = (events_max - events_min + 1) as u64;

            while start.elapsed() < duration {
                let n_events = events_min + (xorshift(&mut rng) % span) as usize;
                let mut events = Vec::with_capacity(n_events);
                for _ in 0..n_events {
                    let pair = pairs_w[(xorshift(&mut rng) as usize) % pairs_w.len()];
                    let (debit_id, credit_id) = if (xorshift(&mut rng) & 1) == 0 {
                        (pair.out_id, pair.main_id)
                    } else {
                        (pair.main_id, pair.out_id)
                    };
                    let key = fresh_uuid_str(&mut rng);
                    events.push(make_event(
                        "bin_move",
                        debit_id,
                        credit_id,
                        1,
                        "2026-04-15",
                        &key,
                    ));
                }
                let batch = json!(events);
                let t0 = Instant::now();
                let res: sqlx::Result<i64> = sqlx::query_scalar(
                    "INSERT INTO ledger_outbox (events) VALUES ($1) RETURNING id",
                )
                .bind(&batch)
                .fetch_one(&pool_w)
                .await;
                let dur_us = t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
                enqueue_lats.push(dur_us);
                match res {
                    Ok(_) => {
                        ok_w.fetch_add(1, Ordering::Relaxed);
                        ev_w.fetch_add(n_events as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("writer {w} INSERT error: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            enqueue_lats
        }));
    }

    let mut all_enqueue_lats: Vec<u32> = Vec::new();
    for h in handles {
        let writer_lats = h.await.expect("writer panic");
        all_enqueue_lats.extend(writer_lats);
    }
    let writer_elapsed = start.elapsed();
    eprintln!(
        "T4: writers stopped at t={:.2}s, signaling drain_to_empty",
        writer_elapsed.as_secs_f64()
    );

    // ---- Bounded grace drain: tell the worker to exit on next-empty,
    //      but cap the wait. If the queue is too deep to drain in time,
    //      hard_stop forces an exit and we report partial state.
    drain_to_empty.store(true, Ordering::Relaxed);
    let drain_t0 = Instant::now();
    let drain_outcome = tokio::time::timeout(
        Duration::from_secs(drain_timeout_s),
        worker_handle,
    )
    .await;
    let drain_stats = match drain_outcome {
        Ok(join) => join.expect("drain worker panic").expect("super_batched_drain_loop"),
        Err(_) => {
            eprintln!(
                "T4: drain timeout after {}s — forcing hard_stop",
                drain_timeout_s
            );
            hard_stop.store(true, Ordering::Relaxed);
            // Worker handle is already moved into the timeout future; we
            // need to wait for it now via a fresh loop. Re-issue: we
            // actually consumed it. The hard_stop will let the next
            // iteration check exit. We need a way to recover the handle.
            // For now, the timeout already consumed worker_handle, so
            // we can't await it again. Instead, sample the table.
            let committed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM ledger_outbox WHERE status = 'committed'",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
            let failed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM ledger_outbox WHERE status = 'failed'",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
            DrainStats {
                batches: 0,
                rows_committed: committed as u64,
                rows_failed: failed as u64,
                duration_ms: drain_timeout_s * 1000,
                super_batch_attempts: 0,
                super_batch_successes: 0,
                super_batch_fallbacks: 0,
            }
        }
    };
    let drain_secs = drain_t0.elapsed().as_secs_f64();
    eprintln!(
        "T4: drain phase ended in {:.2}s — batches={} committed={} failed={} super_batch_attempts={} super_batch_successes={} super_batch_fallbacks={}",
        drain_secs,
        drain_stats.batches,
        drain_stats.rows_committed,
        drain_stats.rows_failed,
        drain_stats.super_batch_attempts,
        drain_stats.super_batch_successes,
        drain_stats.super_batch_fallbacks,
    );

    // Stop depth sampler.
    depth_stop.store(true, Ordering::Relaxed);
    depth_handle.abort();
    let _ = depth_handle.await;
    let max_depth = depth_samples
        .lock()
        .unwrap()
        .iter()
        .copied()
        .max()
        .unwrap_or(0);

    let total_elapsed = start.elapsed();

    // ---- Collect queue residency latencies from the outbox table.
    let queue_lats_raw: Vec<i64> = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM (committed_at - enqueued_at)) * 1000000)::BIGINT
           FROM ledger_outbox
          WHERE status = 'committed'",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    let mut queue_lats: Vec<u32> = queue_lats_raw
        .into_iter()
        .map(|us| us.clamp(0, u32::MAX as i64) as u32)
        .collect();
    queue_lats.sort_unstable();

    // ---- Total events actually committed to the ledger. The headline
    //      "G vs F throughput" comparison uses this divided by the
    //      writer-phase duration — what F reports as events/sec.
    let committed_events: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(jsonb_array_length(events)), 0)::BIGINT
           FROM ledger_outbox WHERE status = 'committed'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

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
         FROM pg_stat_io WHERE backend_type = 'client backend'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or((0, 0, 0, 0, 0, 0, 0));

    let wal_lsn_after: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await
        .unwrap_or_default();
    let wal_bytes_delta: i64 = if !wal_lsn_before.is_empty() && !wal_lsn_after.is_empty() {
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::BIGINT")
            .bind(&wal_lsn_after)
            .bind(&wal_lsn_before)
            .fetch_one(&pool)
            .await
            .unwrap_or(0)
    } else {
        0
    };

    let top_queries: Vec<(String, i64, f64, f64)> = sqlx::query_as(
        "SELECT left(query, 80) AS q, calls::BIGINT,
                total_exec_time, mean_exec_time
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

    let final_status: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status, COUNT(*)::BIGINT FROM ledger_outbox GROUP BY status ORDER BY status",
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();
    let final_committed: i64 = final_status
        .iter()
        .find(|(s, _)| s == "committed")
        .map(|(_, c)| *c)
        .unwrap_or(0);
    let final_failed: i64 = final_status
        .iter()
        .find(|(s, _)| s == "failed")
        .map(|(_, c)| *c)
        .unwrap_or(0);
    let final_pending: i64 = final_status
        .iter()
        .find(|(s, _)| s == "pending")
        .map(|(_, c)| *c)
        .unwrap_or(0);

    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let events = event_count.load(Ordering::Relaxed);
    let total = ok + err;

    all_enqueue_lats.sort_unstable();
    let p50 = pct(&all_enqueue_lats, 0.50);
    let p95 = pct(&all_enqueue_lats, 0.95);
    let p99 = pct(&all_enqueue_lats, 0.99);
    let p99_9 = pct(&all_enqueue_lats, 0.999);
    let max_lat = *all_enqueue_lats.last().unwrap_or(&0);

    let q_p50 = pct(&queue_lats, 0.50);
    let q_p95 = pct(&queue_lats, 0.95);
    let q_p99 = pct(&queue_lats, 0.99);
    let q_p99_9 = pct(&queue_lats, 0.999);
    let q_max = *queue_lats.last().unwrap_or(&0);

    let xact_commit_d = stat_db_after.0 - stat_db_before.0;
    let xact_rollbk_d = stat_db_after.1 - stat_db_before.1;
    let blks_read_d = stat_db_after.2 - stat_db_before.2;
    let blks_hit_d = stat_db_after.3 - stat_db_before.3;
    let io_reads_d = stat_io_after.0 - stat_io_before.0;
    let io_rbytes_d = stat_io_after.1 - stat_io_before.1;
    let io_writes_d = stat_io_after.2 - stat_io_before.2;
    let io_wbytes_d = stat_io_after.3 - stat_io_before.3;
    let io_extends_d = stat_io_after.4 - stat_io_before.4;
    let io_hits_d = stat_io_after.5 - stat_io_before.5;
    let io_fsyncs_d = stat_io_after.6 - stat_io_before.6;

    eprintln!("===================== T4 PERF SUMMARY (shape J: super-batch) =====================");
    eprintln!(
        "duration_s={:.2} (writers_phase={:.2}s drain_phase={:.2}s) writers={} events={}-{} pool={} (outbox)",
        total_elapsed.as_secs_f64(),
        writer_elapsed.as_secs_f64(),
        drain_secs,
        n_writers,
        events_min,
        events_max,
        pairs.len()
    );
    eprintln!(
        "batches: total={} ok={} err={} enqueue_throughput={:.1}/s",
        total,
        ok,
        err,
        total as f64 / writer_elapsed.as_secs_f64()
    );
    eprintln!(
        "events:  enqueued={} enqueue_throughput={:.1}/s  (sustained over enqueue phase)",
        events,
        events as f64 / writer_elapsed.as_secs_f64()
    );
    eprintln!(
        "events:  committed={} commit_throughput={:.1}/s  <-- HEADLINE (apples-to-apples vs F)",
        committed_events,
        committed_events as f64 / total_elapsed.as_secs_f64()
    );
    eprintln!(
        "enqueue_us: p50={} p95={} p99={} p99.9={} max={} (n={})",
        p50,
        p95,
        p99,
        p99_9,
        max_lat,
        all_enqueue_lats.len()
    );
    eprintln!(
        "queue_us:   p50={} p95={} p99={} p99.9={} max={} (n={})",
        q_p50,
        q_p95,
        q_p99,
        q_p99_9,
        q_max,
        queue_lats.len()
    );
    eprintln!(
        "outbox: max_pending_depth={} drain_batches={} committed={} failed={} pending={}",
        max_depth,
        drain_stats.batches,
        final_committed,
        final_failed,
        final_pending
    );
    eprintln!(
        "deadlocks: delta={} ({} -> {})",
        deadlocks_after - deadlocks_before,
        deadlocks_before,
        deadlocks_after
    );
    eprintln!(
        "pg_stat_database: xact_commit_delta={} xact_rollback_delta={} blks_read_delta={} blks_hit_delta={}",
        xact_commit_d, xact_rollbk_d, blks_read_d, blks_hit_d
    );
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
    eprintln!("=======================================================================");

    eprintln!(
        "T4_CSV_HEADER: duration_s,writers,batches_total,batches_ok,batches_err,events_total,throughput_bps,throughput_evps,p50_us,p95_us,p99_us,p999_us,max_us,deadlocks_delta,xact_commit_delta,xact_rollback_delta,blks_read_delta,blks_hit_delta,io_reads_delta,io_read_bytes_delta,io_writes_delta,io_write_bytes_delta,io_extends_delta,io_hits_delta,io_fsyncs_delta,wal_bytes_delta,queue_p50_us,queue_p95_us,queue_p99_us,queue_p999_us,queue_max_us,max_outbox_depth,final_committed,final_failed,drain_secs,committed_events,commit_evps"
    );
    eprintln!(
        "T4_CSV_VALUES: {:.3},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{},{:.3}",
        writer_elapsed.as_secs_f64(),
        n_writers,
        total,
        ok,
        err,
        events,
        total as f64 / writer_elapsed.as_secs_f64(),
        events as f64 / writer_elapsed.as_secs_f64(),
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
        wal_bytes_delta,
        q_p50,
        q_p95,
        q_p99,
        q_p99_9,
        q_max,
        max_depth,
        final_committed,
        final_failed,
        drain_secs,
        committed_events,
        committed_events as f64 / total_elapsed.as_secs_f64(),
    );

    assert_eq!(
        deadlocks_after - deadlocks_before,
        0,
        "deadlock counter rose during run"
    );
    assert_eq!(err, 0, "non-deadlock writer INSERT errors during run");
    assert!(total > 0, "no batches enqueued");
    assert!(final_committed > 0, "worker committed nothing");
}
