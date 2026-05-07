//! `acct-9i6` — Reservation interleaving load test.
//!
//! Direct extension of `tests/load_realistic_workload.rs` (shape F).
//! Same cross-account spread fixture (50 BENCH SKUs × 2 locations =
//! 100 stock_available accounts), but the workload is split across
//! two writer types running concurrently:
//!
//! 1. **Posters** (default 70 % of writers) — same `bin_move` workload
//!    as F: each batch posts 5–20 events of `debit/credit =
//!    stock_available(sku, loc)` for random SKU and direction.
//! 2. **Reservers** (default 30 % of writers) — each call invokes
//!    `reserve_inventory(sku, loc, 1, so_id, fresh_so_line_id, 1hr)`
//!    on a randomly-chosen (sku, loc).
//!
//! Both contend for the same `stock_available` row locks:
//!
//!   * `post_posting_lines` locks accounts in ascending-id order via
//!     `FOR UPDATE` inside its transaction.
//!   * `reserve_inventory` takes a single `FOR UPDATE` on the matching
//!     `stock_available` row before reading promisable.
//!
//! The load test answers: under realistic mixed traffic, do we deadlock?
//! Does reserver latency stay bounded? How does poster throughput
//! degrade vs F's 1 274 evps when 30 % of the writer pool is reservers
//! competing for the same account locks?
//!
//! Stock is pre-balanced to **100 M units** per (sku, loc) so the
//! benchmark isn't confounded by stock exhaustion — reservations grow
//! without release across the run, and bin_move is qty-neutral overall.
//!
//! Run via `./scripts/run-perf-baseline.sh`:
//!
//!   T4_BINARY=load_reservation_interleave \
//!     T4_CONFIGS="100:5:20" \
//!     T4_BASELINE_RUNS=3 \
//!     T4_DURATION_SECS=300 \
//!     ./scripts/run-perf-baseline.sh
//!
//! Env knobs (defaults shown):
//!
//!   T4_DURATION_SECS=30   wall-clock per run
//!   T4_WRITERS=32         total concurrent tokio writers
//!   T4_EVENTS_MIN=5       events per poster batch (lower bound)
//!   T4_EVENTS_MAX=20      events per poster batch (upper bound)
//!   T4_BENCH_SKUS=50      number of SKUs in the spread pool
//!   T4_RESERVE_PCT=30     percent of writers that are reservers
//!
//! `#[ignore]` so it doesn't run with `cargo test` by default.

mod common;

use common::*;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn pct(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((q * (sorted.len() as f64)).floor() as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Setup: 50 BENCH SKUs, 2 locations (MAIN, OUT), 100 stock_available
/// accounts pre-balanced to 100 M units. Returns `(account-pair list,
/// per-sku UUID strings, per-loc UUID strings)` for direct use by the
/// reservers (which need the UUIDs to call reserve_inventory).
async fn setup_fixture(
    pool: &sqlx::PgPool,
    n_skus: usize,
) -> (
    Vec<(i64, i64)>,           // (main_id, out_id) per sku
    Vec<(String, String)>,     // (sku_uuid, sku_code) ordered the same as above
    HashMap<String, String>,   // location code → uuid
) {
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
        "UPDATE accounts SET debits_total = 100000000
           FROM skus s, locations l
          WHERE accounts.sku_id      = s.id
            AND accounts.location_id = l.id
            AND s.code LIKE 'BENCH-%'
            AND accounts.kind = 'stock_available'
            AND accounts.debits_total < 100000000",
    )
    .execute(pool)
    .await
    .expect("pre-balance BENCH accounts");

    let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
        "SELECT a.id, s.code, s.id::text, l.code, l.id::text
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

    let mut by_sku: HashMap<String, (Option<(i64, String)>, Option<(i64, String)>)> =
        HashMap::new();
    let mut loc_uuids: HashMap<String, String> = HashMap::new();
    for (id, sku_code, sku_uuid, loc_code, loc_uuid) in rows {
        loc_uuids.insert(loc_code.clone(), loc_uuid);
        let entry = by_sku.entry(sku_code.clone()).or_insert((None, None));
        match loc_code.as_str() {
            "MAIN" => entry.0 = Some((id, sku_uuid)),
            "OUT" => entry.1 = Some((id, sku_uuid)),
            _ => {}
        }
    }
    let mut sku_keys: Vec<String> = by_sku.keys().cloned().collect();
    sku_keys.sort();

    let mut acct_pairs: Vec<(i64, i64)> = Vec::with_capacity(sku_keys.len());
    let mut sku_info: Vec<(String, String)> = Vec::with_capacity(sku_keys.len());
    for code in sku_keys {
        if let (Some((m, sku_uuid)), Some((o, _))) = by_sku.remove(&code).unwrap() {
            acct_pairs.push((m, o));
            sku_info.push((sku_uuid, code));
        }
    }
    (acct_pairs, sku_info, loc_uuids)
}

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn reservation_interleave_workload() {
    let duration_secs: u64 = env::var("T4_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let n_total: u32 = env::var("T4_WRITERS")
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
    let reserve_pct: u32 = env::var("T4_RESERVE_PCT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    assert!(reserve_pct <= 100, "T4_RESERVE_PCT must be 0..=100");
    assert!(events_min <= events_max && events_min >= 1);
    let duration = Duration::from_secs(duration_secs);

    let n_reservers: u32 = (n_total * reserve_pct).div_euclid(100);
    let n_posters: u32 = n_total - n_reservers;
    assert!(
        n_posters >= 1 && n_reservers >= 1,
        "need at least 1 of each type; got posters={n_posters} reservers={n_reservers}"
    );

    let pool = connect_test_db_with(n_total + 8).await;
    reset_to_fixture(&pool).await;

    eprintln!("T4: setting up fixture (n_skus={n_skus})");
    let setup_t0 = Instant::now();
    let (acct_pairs, sku_info, loc_uuids) = setup_fixture(&pool, n_skus).await;
    eprintln!(
        "T4: fixture setup in {:.2}s — {} (sku,loc) account pairs",
        setup_t0.elapsed().as_secs_f64(),
        acct_pairs.len()
    );
    assert!(!acct_pairs.is_empty());

    // Each reserver gets one sales_order to attach reservations to.
    let mut reserver_so_ids: Vec<String> = Vec::with_capacity(n_reservers as usize);
    for _ in 0..n_reservers {
        reserver_so_ids.push(fresh_sales_order(&pool).await);
    }

    let acct_pairs = Arc::new(acct_pairs);
    let sku_info = Arc::new(sku_info);
    let main_uuid = loc_uuids
        .get("MAIN")
        .expect("MAIN location uuid")
        .clone();
    let out_uuid = loc_uuids.get("OUT").expect("OUT location uuid").clone();

    eprintln!(
        "T4 reservation-interleave: total={} (posters={} reservers={}) duration={}s events={}-{} pool={}",
        n_total,
        n_posters,
        n_reservers,
        duration_secs,
        events_min,
        events_max,
        acct_pairs.len()
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
    let deadlocks_before = stat_db_before.4;
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

    let post_ok = Arc::new(AtomicU64::new(0));
    let post_err = Arc::new(AtomicU64::new(0));
    let post_events = Arc::new(AtomicU64::new(0));
    let res_ok = Arc::new(AtomicU64::new(0));
    let res_null = Arc::new(AtomicU64::new(0));
    let res_err = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut poster_handles = Vec::with_capacity(n_posters as usize);
    for w in 0..n_posters {
        let pool_w = pool.clone();
        let pairs_w = acct_pairs.clone();
        let ok_w = post_ok.clone();
        let err_w = post_err.clone();
        let ev_w = post_events.clone();
        poster_handles.push(tokio::spawn(async move {
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
                    let (main_id, out_id) =
                        pairs_w[(xorshift(&mut rng) as usize) % pairs_w.len()];
                    let (debit_id, credit_id) = if (xorshift(&mut rng) & 1) == 0 {
                        (out_id, main_id)
                    } else {
                        (main_id, out_id)
                    };
                    let key = fresh_uuid_str(&mut rng);
                    events.push(make_event(
                        "bin_move", debit_id, credit_id, 1, "2026-04-15", &key,
                    ));
                }
                let batch = json!(events);
                let t0 = Instant::now();
                let res = call_post_posting_lines(&pool_w, batch, false).await;
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
                            panic!("DEADLOCK on poster {w}: {e}");
                        }
                        eprintln!("poster {w} unexpected error [{code}]: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies_us
        }));
    }

    let mut reserver_handles = Vec::with_capacity(n_reservers as usize);
    for r in 0..n_reservers {
        let pool_w = pool.clone();
        let sku_info_w = sku_info.clone();
        let main_uuid_w = main_uuid.clone();
        let out_uuid_w = out_uuid.clone();
        let so_id = reserver_so_ids[r as usize].clone();
        let ok_w = res_ok.clone();
        let null_w = res_null.clone();
        let err_w = res_err.clone();
        reserver_handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((r as u64) << 16) ^ 0xa11c_d00d_beef_face;
            let mut latencies_us: Vec<u32> = Vec::new();

            while start.elapsed() < duration {
                let (sku_uuid, _sku_code) =
                    &sku_info_w[(xorshift(&mut rng) as usize) % sku_info_w.len()];
                let loc_uuid = if (xorshift(&mut rng) & 1) == 0 {
                    &main_uuid_w
                } else {
                    &out_uuid_w
                };
                let so_line_id = fresh_uuid_str(&mut rng);

                let t0 = Instant::now();
                let res: sqlx::Result<Option<String>> = sqlx::query_scalar(
                    "SELECT reserve_inventory(
                        $1::UUID, $2::UUID, $3::BIGINT, $4::UUID, $5::UUID,
                        clock_timestamp() + INTERVAL '1 hour'
                     )::text",
                )
                .bind(sku_uuid)
                .bind(loc_uuid)
                .bind(1_i64)
                .bind(&so_id)
                .bind(&so_line_id)
                .fetch_one(&pool_w)
                .await;
                let dur_us = t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
                latencies_us.push(dur_us);

                match res {
                    Ok(Some(_)) => {
                        ok_w.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        null_w.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let code = e
                            .as_database_error()
                            .and_then(|d| d.code().map(|c| c.into_owned()))
                            .unwrap_or_else(|| "no-code".to_string());
                        if code == "40P01" {
                            panic!("DEADLOCK on reserver {r}: {e}");
                        }
                        eprintln!("reserver {r} unexpected error [{code}]: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies_us
        }));
    }

    let mut poster_lats: Vec<u32> = Vec::new();
    for h in poster_handles {
        let lats = h.await.expect("poster panic");
        poster_lats.extend(lats);
    }
    let mut reserver_lats: Vec<u32> = Vec::new();
    for h in reserver_handles {
        let lats = h.await.expect("reserver panic");
        reserver_lats.extend(lats);
    }
    let elapsed = start.elapsed();

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

    let total_active_reservations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM inventory_reservations WHERE status = 'active'",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    let p_ok = post_ok.load(Ordering::Relaxed);
    let p_err = post_err.load(Ordering::Relaxed);
    let p_events = post_events.load(Ordering::Relaxed);
    let r_ok = res_ok.load(Ordering::Relaxed);
    let r_null = res_null.load(Ordering::Relaxed);
    let r_err = res_err.load(Ordering::Relaxed);
    let p_total = p_ok + p_err;
    let r_total = r_ok + r_null + r_err;

    poster_lats.sort_unstable();
    reserver_lats.sort_unstable();
    let p_p50 = pct(&poster_lats, 0.50);
    let p_p95 = pct(&poster_lats, 0.95);
    let p_p99 = pct(&poster_lats, 0.99);
    let p_p999 = pct(&poster_lats, 0.999);
    let p_max = *poster_lats.last().unwrap_or(&0);
    let r_p50 = pct(&reserver_lats, 0.50);
    let r_p95 = pct(&reserver_lats, 0.95);
    let r_p99 = pct(&reserver_lats, 0.99);
    let r_p999 = pct(&reserver_lats, 0.999);
    let r_max = *reserver_lats.last().unwrap_or(&0);

    // Combined latencies (used in F-style CSV columns 9-13).
    let mut all_lats = poster_lats.clone();
    all_lats.extend_from_slice(&reserver_lats);
    all_lats.sort_unstable();
    let c_p50 = pct(&all_lats, 0.50);
    let c_p95 = pct(&all_lats, 0.95);
    let c_p99 = pct(&all_lats, 0.99);
    let c_p999 = pct(&all_lats, 0.999);
    let c_max = *all_lats.last().unwrap_or(&0);

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

    eprintln!("============= T4 PERF SUMMARY (reservation interleave) =============");
    eprintln!(
        "duration_s={:.2} writers_total={} (posters={} reservers={}) reserve_pct={}%",
        elapsed.as_secs_f64(),
        n_total,
        n_posters,
        n_reservers,
        reserve_pct
    );
    eprintln!(
        "POSTERS:   batches={} ok={} err={} bps={:.1} events={} evps={:.1}",
        p_total,
        p_ok,
        p_err,
        p_total as f64 / elapsed.as_secs_f64(),
        p_events,
        p_events as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "POSTERS:   p50={} p95={} p99={} p99.9={} max={} (n={})",
        p_p50,
        p_p95,
        p_p99,
        p_p999,
        p_max,
        poster_lats.len()
    );
    eprintln!(
        "RESERVERS: ops={} ok={} null={} err={} rps={:.1}",
        r_total,
        r_ok,
        r_null,
        r_err,
        r_total as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "RESERVERS: p50={} p95={} p99={} p99.9={} max={} (n={})",
        r_p50,
        r_p95,
        r_p99,
        r_p999,
        r_max,
        reserver_lats.len()
    );
    eprintln!(
        "TOTAL ACTIVE RESERVATIONS in inventory_reservations: {}",
        total_active_reservations
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
        "pg_stat_io: reads_d={} read_bytes_d={} writes_d={} write_bytes_d={} extends_d={} hits_d={} fsyncs_d={}",
        io_reads_d, io_rbytes_d, io_writes_d, io_wbytes_d, io_extends_d, io_hits_d, io_fsyncs_d
    );
    eprintln!(
        "wal_bytes_delta={} (lsn {} -> {})",
        wal_bytes_delta, wal_lsn_before, wal_lsn_after
    );
    eprintln!("pg_stat_statements top 10:");
    for (q, calls, total_ms, mean_ms) in &top_queries {
        eprintln!(
            "  calls={:>10} total_ms={:>10.1} mean_ms={:>8.3}  query={}",
            calls, total_ms, mean_ms, q
        );
    }
    eprintln!("====================================================================");

    // CSV: keep F's column layout positions 1-26 so the run-perf-baseline.sh
    // aggregator continues to produce a useful headline comparison. Combined
    // metrics fill the F-shaped slots; per-type detail appended after.
    let combined_total = p_total + r_total;
    let combined_events = p_events + r_ok + r_null; // count each reserve attempt as one "event"
    eprintln!(
        "T4_CSV_HEADER: duration_s,writers,batches_total,batches_ok,batches_err,events_total,throughput_bps,throughput_evps,p50_us,p95_us,p99_us,p999_us,max_us,deadlocks_delta,xact_commit_delta,xact_rollback_delta,blks_read_delta,blks_hit_delta,io_reads_delta,io_read_bytes_delta,io_writes_delta,io_write_bytes_delta,io_extends_delta,io_hits_delta,io_fsyncs_delta,wal_bytes_delta,n_posters,n_reservers,poster_bps,poster_evps,poster_p50_us,poster_p95_us,poster_p99_us,poster_p999_us,poster_max_us,reserver_rps,reserver_ok,reserver_null,reserver_err,reserver_p50_us,reserver_p95_us,reserver_p99_us,reserver_p999_us,reserver_max_us,active_reservations"
    );
    eprintln!(
        "T4_CSV_VALUES: {:.3},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{:.3},{},{},{},{},{},{},{},{},{}",
        elapsed.as_secs_f64(),
        n_total,
        combined_total,
        p_ok + r_ok,
        p_err + r_err,
        combined_events,
        combined_total as f64 / elapsed.as_secs_f64(),
        combined_events as f64 / elapsed.as_secs_f64(),
        c_p50,
        c_p95,
        c_p99,
        c_p999,
        c_max,
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
        n_posters,
        n_reservers,
        p_total as f64 / elapsed.as_secs_f64(),
        p_events as f64 / elapsed.as_secs_f64(),
        p_p50,
        p_p95,
        p_p99,
        p_p999,
        p_max,
        r_total as f64 / elapsed.as_secs_f64(),
        r_ok,
        r_null,
        r_err,
        r_p50,
        r_p95,
        r_p99,
        r_p999,
        r_max,
        total_active_reservations,
    );

    assert_eq!(
        deadlocks_after - deadlocks_before,
        0,
        "deadlock counter rose during run"
    );
    assert_eq!(p_err, 0, "non-deadlock poster errors during run");
    assert_eq!(r_err, 0, "non-deadlock reserver errors during run");
    assert!(p_total > 0, "no poster batches executed");
    assert!(r_total > 0, "no reserver ops executed");
}
