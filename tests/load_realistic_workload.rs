//! `acct-2ey` — Realistic-shape load test (cross-account spread).
//!
//! Direct counterpart to `tests/load_deadlock_freedom.rs`. Same
//! instrumentation, same hardware, same harness — only the
//! **workload** differs.
//!
//! The deadlock-freedom test is a worst-case lock-contention probe:
//! every event credits a single shared row (`creation_void(qty)`).
//! Realistic application traffic doesn't look like that. Concurrent
//! invoice handlers / shipment handlers / inventory adjustments all
//! touch *different* accounts most of the time.
//!
//! This test simulates a `bin_move` workload — moving stock between
//! two physical locations — across **50 SKUs**. Each event posts
//! `debit = stock_available(SKU_X, dst)`, `credit = stock_available(
//! SKU_X, src)` for a randomly-chosen SKU and direction. With 50 SKUs
//! × 2 locations = 100 distinct accounts, lock contention spreads
//! across the pool instead of converging on one row.
//!
//! Run via `./scripts/run-perf-baseline.sh` with the dedicated env:
//!
//!   T4_BINARY=load_realistic_workload \
//!     T4_CONFIGS="100:5:20" \
//!     T4_BASELINE_RUNS=3 \
//!     T4_DURATION_SECS=300 \
//!     ./scripts/run-perf-baseline.sh
//!
//! Env knobs (all optional, defaults shown):
//!
//!   T4_DURATION_SECS=30   wall-clock per run
//!   T4_WRITERS=32         concurrent tokio writers
//!   T4_EVENTS_MIN=5       events per batch (lower bound)
//!   T4_EVENTS_MAX=20      events per batch (upper bound)
//!   T4_BENCH_SKUS=50      number of SKUs in the spread pool
//!
//! The test is `#[ignore]` so it doesn't run with `cargo test` by
//! default. Same gating as the deadlock-freedom test.

mod common;

use common::*;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
struct LocPair {
    /// stock_available(SKU, MAIN) account id
    main_id: i64,
    /// stock_available(SKU, OUT) account id
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

/// Set up the load fixture: insert N "BENCH-NNN" SKUs, create
/// stock_available accounts at MAIN and OUT for each, pre-balance
/// each account to 10 000 units so subsequent bin_move events have
/// headroom in either direction. Idempotent (uses ON CONFLICT for
/// SKUs and a NOT EXISTS guard for accounts).
async fn setup_load_fixture(pool: &sqlx::PgPool, n_skus: usize) -> Vec<LocPair> {
    // 1. SKUs
    for i in 1..=n_skus {
        let code = format!("BENCH-{:03}", i);
        sqlx::query(
            "INSERT INTO skus (code, uom) VALUES ($1, 'EA')
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .execute(pool)
        .await
        .expect("insert BENCH sku");
    }

    sqlx::raw_sql(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         SELECT s.id, 1, '1970-01-01'::DATE,
                '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
           FROM skus s
          WHERE s.code LIKE 'BENCH-%'
            AND NOT EXISTS (SELECT 1 FROM standard_costs sc WHERE sc.sku_id = s.id)",
    )
    .execute(pool)
    .await
    .expect("backfill BENCH standard_costs");

    // 2. stock_available accounts at MAIN + OUT for every BENCH SKU.
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

    // 3. Pre-balance every BENCH stock_available account to 10 000
    //    units. Direct UPDATE is acceptable for setup — the L2 CHECK
    //    still fires (debits >= credits on debit-normal). The
    //    per-ledger sum invariant is intentionally violated for the
    //    duration of the test (we have no matching credits for these
    //    debits) — the load test does not exercise the §7 daily
    //    reconciliation.
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

    // 4. Look up account ids and group by SKU.
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

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn realistic_workload_cross_account_spread() {
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
    assert!(
        events_min <= events_max && events_min >= 1,
        "T4_EVENTS_MIN must be >=1 and <= T4_EVENTS_MAX"
    );
    let duration = Duration::from_secs(duration_secs);

    let pool = connect_test_db_with(n_writers + 4).await;
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
        "T4: writers={} duration={}s events_per_batch={}-{} pool_size={} (cross-account spread)",
        n_writers,
        duration_secs,
        events_min,
        events_max,
        pairs.len()
    );

    // ---- pg_stat reset + pre-snapshots. ----
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

    // ---- Run the load. ----
    let deadlocks_before = stat_db_before.4;
    let ok_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));

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
            let mut latencies_us: Vec<u32> = Vec::new();
            let span = (events_max - events_min + 1) as u64;

            while start.elapsed() < duration {
                let n_events = events_min + (xorshift(&mut rng) % span) as usize;
                let mut events = Vec::with_capacity(n_events);
                for _ in 0..n_events {
                    let pair = pairs_w[(xorshift(&mut rng) as usize) % pairs_w.len()];
                    // 50/50: MAIN→OUT or OUT→MAIN
                    let (debit_id, credit_id) = if (xorshift(&mut rng) & 1) == 0 {
                        (pair.out_id, pair.main_id)
                    } else {
                        (pair.main_id, pair.out_id)
                    };
                    let key = fresh_uuid_str(&mut rng);
                    let ev = make_event("bin_move", debit_id, credit_id, 1, "2026-04-15", &key);
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

    let ok = ok_count.load(Ordering::Relaxed);
    let err = err_count.load(Ordering::Relaxed);
    let events = event_count.load(Ordering::Relaxed);
    let total = ok + err;

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

    eprintln!("====================== T4 PERF SUMMARY ======================");
    eprintln!(
        "duration_s={:.2} writers={} events_per_batch={}-{} pool_size={} (cross-account)",
        elapsed.as_secs_f64(),
        n_writers,
        events_min,
        events_max,
        pairs.len()
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
    eprintln!("=============================================================");

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
        "deadlock counter rose during run"
    );
    assert_eq!(err, 0, "non-deadlock errors during run");
    assert!(total > 0, "no batches executed");
}
