//! `acct-jwg` — Multi-currency value-side load test.
//!
//! Extends `tests/load_realistic_workload.rs` (shape F) and
//! `tests/load_reservation_interleave.rs` (shape H, `acct-9i6`) by
//! adding a value-side workload that runs concurrently with qty-side
//! traffic. Two writer types share the test:
//!
//! 1. **Qty writers** (default 50 % of total) — same `bin_move`
//!    workload as F across 50 BENCH SKUs × 2 locations.
//! 2. **Value writers** (default 50 % of total) — each batch posts
//!    5–20 `ar_invoice` events on **per-currency value accounts**
//!    (cash / ar / revenue / ap). Currency for each event is picked
//!    randomly USD-or-EUR.
//!
//! Per-currency value accounts (currently 1 each per currency) stay
//! hot under load — like shape D's shared-credit pattern, but on the
//! value ledger. The headline question: how does qty-side throughput
//! change when value-side traffic competes for connections / WAL /
//! buffer cache, and how does value-side latency look on hot rows?
//!
//! The Phase 0 fixture seeds USD `cash/ar/ap/revenue` but only EUR
//! `cash/revenue`. Setup fills in EUR `ar/ap` so the workload can
//! exercise both currencies symmetrically.
//!
//! Run via `./scripts/run-perf-baseline.sh`:
//!
//!   T4_BINARY=load_value_workload \
//!     T4_CONFIGS="100:5:20" \
//!     T4_BASELINE_RUNS=3 \
//!     T4_DURATION_SECS=300 \
//!     ./scripts/run-perf-baseline.sh
//!
//! Env knobs (defaults):
//!
//!   T4_DURATION_SECS=30
//!   T4_WRITERS=32         total writers
//!   T4_EVENTS_MIN=5
//!   T4_EVENTS_MAX=20
//!   T4_BENCH_SKUS=50      qty workload pool
//!   T4_VALUE_PCT=50       percent of writers running value workload
//!
//! `#[ignore]` so it doesn't run with default `cargo test`.

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

#[derive(Clone)]
struct ValueAccountSet {
    /// (debit_normal_ids, credit_normal_ids) for each currency.
    /// Index 0 = USD, 1 = EUR.
    by_currency: [(Vec<i64>, Vec<i64>); 2],
}

async fn setup_qty_fixture(pool: &sqlx::PgPool, n_skus: usize) -> Vec<(i64, i64)> {
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
    let mut pairs: Vec<(i64, i64)> = by_sku
        .into_values()
        .filter_map(|(m, o)| match (m, o) {
            (Some(m), Some(o)) => Some((m, o)),
            _ => None,
        })
        .collect();
    pairs.sort_by_key(|&(m, _)| m);
    pairs
}

async fn setup_value_accounts(pool: &sqlx::PgPool) -> ValueAccountSet {
    // Fill in EUR ar/ap which the small fixture omits (it only seeds
    // EUR cash/revenue for the currency-mismatch tests).
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side)
         SELECT v.kind::account_kind, 'value', 'EUR', v.side::balance_direction
           FROM (VALUES ('ar', 'debit'), ('ap', 'credit')) AS v(kind, side)
          WHERE NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind        = v.kind::account_kind
                 AND a.currency    = 'EUR'
                 AND a.ledger_kind = 'value'
                 AND NOT a.is_closed
            )",
    )
    .execute(pool)
    .await
    .expect("seed EUR ar/ap");

    let mut by_currency: [(Vec<i64>, Vec<i64>); 2] = Default::default();
    for (idx, currency) in ["USD", "EUR"].iter().enumerate() {
        let rows: Vec<(i64, String, String)> = sqlx::query_as(
            "SELECT id, kind::text, normal_side::text
               FROM accounts
              WHERE ledger_kind = 'value'
                AND currency    = $1
                AND sku_id      IS NULL
                AND kind IN ('cash', 'ar', 'ap', 'revenue')
                AND NOT is_closed
              ORDER BY id",
        )
        .bind(currency)
        .fetch_all(pool)
        .await
        .expect("lookup value accounts by currency");
        for (id, _kind, side) in rows {
            match side.as_str() {
                "debit" => by_currency[idx].0.push(id),
                "credit" => by_currency[idx].1.push(id),
                _ => {}
            }
        }
    }
    assert!(
        !by_currency[0].0.is_empty() && !by_currency[0].1.is_empty(),
        "USD value pool empty"
    );
    assert!(
        !by_currency[1].0.is_empty() && !by_currency[1].1.is_empty(),
        "EUR value pool empty"
    );
    ValueAccountSet { by_currency }
}

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn value_workload_multi_currency() {
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
    let value_pct: u32 = env::var("T4_VALUE_PCT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    assert!(value_pct <= 100);
    assert!(events_min <= events_max && events_min >= 1);
    let duration = Duration::from_secs(duration_secs);

    let n_value: u32 = (n_total * value_pct).div_euclid(100);
    let n_qty: u32 = n_total - n_value;
    assert!(n_qty >= 1 && n_value >= 1);

    let pool = connect_test_db_with(n_total + 8).await;
    reset_to_fixture(&pool).await;

    eprintln!("T4: setup");
    let setup_t0 = Instant::now();
    let qty_pairs = setup_qty_fixture(&pool, n_skus).await;
    let value_set = setup_value_accounts(&pool).await;
    eprintln!(
        "T4: setup in {:.2}s — {} qty pairs, USD {} debit/{} credit, EUR {} debit/{} credit",
        setup_t0.elapsed().as_secs_f64(),
        qty_pairs.len(),
        value_set.by_currency[0].0.len(),
        value_set.by_currency[0].1.len(),
        value_set.by_currency[1].0.len(),
        value_set.by_currency[1].1.len(),
    );
    let qty_pairs = Arc::new(qty_pairs);
    let value_set = Arc::new(value_set);

    eprintln!(
        "T4 multi-currency value: writers={} (qty={} value={}) duration={}s events={}-{}",
        n_total, n_qty, n_value, duration_secs, events_min, events_max
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

    let qty_ok = Arc::new(AtomicU64::new(0));
    let qty_err = Arc::new(AtomicU64::new(0));
    let qty_events = Arc::new(AtomicU64::new(0));
    let val_ok = Arc::new(AtomicU64::new(0));
    let val_err = Arc::new(AtomicU64::new(0));
    let val_events = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // ---- Qty writers (F-style)
    let mut qty_handles = Vec::with_capacity(n_qty as usize);
    for w in 0..n_qty {
        let pool_w = pool.clone();
        let pairs_w = qty_pairs.clone();
        let ok_w = qty_ok.clone();
        let err_w = qty_err.clone();
        let ev_w = qty_events.clone();
        qty_handles.push(tokio::spawn(async move {
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
                            panic!("DEADLOCK on qty {w}: {e}");
                        }
                        eprintln!("qty {w} unexpected error [{code}]: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies_us
        }));
    }

    // ---- Value writers
    let mut val_handles = Vec::with_capacity(n_value as usize);
    for w in 0..n_value {
        let pool_w = pool.clone();
        let vset = value_set.clone();
        let ok_w = val_ok.clone();
        let err_w = val_err.clone();
        let ev_w = val_events.clone();
        val_handles.push(tokio::spawn(async move {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut rng = nanos ^ ((w as u64) << 24) ^ 0xfeed_beef_cafe_babe;
            let mut latencies_us: Vec<u32> = Vec::new();
            let span = (events_max - events_min + 1) as u64;

            while start.elapsed() < duration {
                let n_events = events_min + (xorshift(&mut rng) % span) as usize;
                let mut events = Vec::with_capacity(n_events);
                for _ in 0..n_events {
                    let cur_idx = (xorshift(&mut rng) as usize) & 1;
                    let (debits, credits) = &vset.by_currency[cur_idx];
                    let debit_id = debits[(xorshift(&mut rng) as usize) % debits.len()];
                    let credit_id = credits[(xorshift(&mut rng) as usize) % credits.len()];
                    let key = fresh_uuid_str(&mut rng);
                    events.push(make_event(
                        "ar_invoice",
                        debit_id,
                        credit_id,
                        1,
                        "2026-04-15",
                        &key,
                    ));
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
                            panic!("DEADLOCK on value {w}: {e}");
                        }
                        eprintln!("value {w} unexpected error [{code}]: {e}");
                        err_w.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            latencies_us
        }));
    }

    let mut qty_lats: Vec<u32> = Vec::new();
    for h in qty_handles {
        qty_lats.extend(h.await.expect("qty panic"));
    }
    let mut val_lats: Vec<u32> = Vec::new();
    for h in val_handles {
        val_lats.extend(h.await.expect("value panic"));
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

    let q_ok = qty_ok.load(Ordering::Relaxed);
    let q_err = qty_err.load(Ordering::Relaxed);
    let q_events = qty_events.load(Ordering::Relaxed);
    let v_ok = val_ok.load(Ordering::Relaxed);
    let v_err = val_err.load(Ordering::Relaxed);
    let v_events = val_events.load(Ordering::Relaxed);
    let q_total = q_ok + q_err;
    let v_total = v_ok + v_err;

    qty_lats.sort_unstable();
    val_lats.sort_unstable();
    let q_p50 = pct(&qty_lats, 0.50);
    let q_p95 = pct(&qty_lats, 0.95);
    let q_p99 = pct(&qty_lats, 0.99);
    let q_p999 = pct(&qty_lats, 0.999);
    let q_max = *qty_lats.last().unwrap_or(&0);
    let v_p50 = pct(&val_lats, 0.50);
    let v_p95 = pct(&val_lats, 0.95);
    let v_p99 = pct(&val_lats, 0.99);
    let v_p999 = pct(&val_lats, 0.999);
    let v_max = *val_lats.last().unwrap_or(&0);

    let mut all_lats = qty_lats.clone();
    all_lats.extend_from_slice(&val_lats);
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

    eprintln!("============= T4 PERF SUMMARY (multi-currency value) =============");
    eprintln!(
        "duration_s={:.2} writers_total={} (qty={} value={}) value_pct={}%",
        elapsed.as_secs_f64(),
        n_total,
        n_qty,
        n_value,
        value_pct
    );
    eprintln!(
        "QTY:       batches={} ok={} err={} bps={:.1} events={} evps={:.1}",
        q_total,
        q_ok,
        q_err,
        q_total as f64 / elapsed.as_secs_f64(),
        q_events,
        q_events as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "QTY:       p50={} p95={} p99={} p99.9={} max={} (n={})",
        q_p50,
        q_p95,
        q_p99,
        q_p999,
        q_max,
        qty_lats.len()
    );
    eprintln!(
        "VALUE:     batches={} ok={} err={} bps={:.1} events={} evps={:.1}",
        v_total,
        v_ok,
        v_err,
        v_total as f64 / elapsed.as_secs_f64(),
        v_events,
        v_events as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "VALUE:     p50={} p95={} p99={} p99.9={} max={} (n={})",
        v_p50,
        v_p95,
        v_p99,
        v_p999,
        v_max,
        val_lats.len()
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
    eprintln!("==================================================================");

    let combined_total = q_total + v_total;
    let combined_events = q_events + v_events;
    eprintln!(
        "T4_CSV_HEADER: duration_s,writers,batches_total,batches_ok,batches_err,events_total,throughput_bps,throughput_evps,p50_us,p95_us,p99_us,p999_us,max_us,deadlocks_delta,xact_commit_delta,xact_rollback_delta,blks_read_delta,blks_hit_delta,io_reads_delta,io_read_bytes_delta,io_writes_delta,io_write_bytes_delta,io_extends_delta,io_hits_delta,io_fsyncs_delta,wal_bytes_delta,n_qty,n_value,qty_bps,qty_evps,qty_p50_us,qty_p95_us,qty_p99_us,qty_p999_us,qty_max_us,value_bps,value_evps,value_p50_us,value_p95_us,value_p99_us,value_p999_us,value_max_us"
    );
    eprintln!(
        "T4_CSV_VALUES: {:.3},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{}",
        elapsed.as_secs_f64(),
        n_total,
        combined_total,
        q_ok + v_ok,
        q_err + v_err,
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
        n_qty,
        n_value,
        q_total as f64 / elapsed.as_secs_f64(),
        q_events as f64 / elapsed.as_secs_f64(),
        q_p50,
        q_p95,
        q_p99,
        q_p999,
        q_max,
        v_total as f64 / elapsed.as_secs_f64(),
        v_events as f64 / elapsed.as_secs_f64(),
        v_p50,
        v_p95,
        v_p99,
        v_p999,
        v_max,
    );

    assert_eq!(
        deadlocks_after - deadlocks_before,
        0,
        "deadlock counter rose during run"
    );
    assert_eq!(q_err, 0, "non-deadlock qty errors during run");
    assert_eq!(v_err, 0, "non-deadlock value errors during run");
    assert!(q_total > 0 && v_total > 0, "both writer types must run");
}
