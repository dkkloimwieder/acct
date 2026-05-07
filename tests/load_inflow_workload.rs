//! `acct-umd` — Slice A inflow workload load test (shape N).
//!
//! Counterpart to `tests/load_realistic_workload.rs`. Same instrumentation
//! (pg_stat_database / pg_stat_io snapshots, WAL bytes, pg_stat_statements
//! top-10, p50/p95/p99 latency, deadlock guard). Workload differs:
//!
//! Each cycle is a vendor-side document pair:
//!   1. `post_po_receipt(po_id, [{po_line_id, qty_received: N}], ...)` —
//!      emits 2 events (qty leg + value leg) since std==po so no PPV.
//!   2. `post_ap_bill(vendor_id, USD, [{kind:'po_match', po_line_id, qty:N,
//!      unit_cost:1, amount:N}], ...)` — emits 1 event (ap_unsettled DR /
//!      ap CR clearance).
//!
//! Per cycle: 2 doc-function calls → 3 post_posting_lines events. Latency
//! recorded is end-to-end (receipt start → bill commit).
//!
//! Setup pre-creates N vendors, one PO per vendor with one PO line
//! (qty_ordered=10_000_000 so sustained load doesn't exhaust), one
//! BENCH SKU per vendor (standard cost=1, so PO unit_cost=1 means no
//! PPV), the vendor-partitioned ap/ap_unsettled accounts, and the
//! shared variance_ppv account. The shape spreads across N distinct
//! POs so cumulative-receipt contention on a single po_line is rare.
//!
//! Run via:
//!
//!   T4_BINARY=load_inflow_workload \
//!     T4_DURATION_SECS=300 T4_BASELINE_RUNS=3 \
//!     ./scripts/run-perf-baseline.sh
//!
//! Env knobs (defaults):
//!
//!   T4_DURATION_SECS=30     wall clock per run
//!   T4_WRITERS=32           concurrent tokio writers
//!   T4_BENCH_VENDORS=100    distinct vendor/PO/SKU triples in the pool
//!   T4_QTY_PER_CYCLE=5      qty per receipt+bill cycle
//!
//! `#[ignore]` so cargo test default skips it.

mod common;

use common::*;
use serde_json::json;
use sqlx::PgPool;
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

/// Simple holder of the three UUIDs a writer needs each cycle.
#[derive(Clone)]
struct Workspot {
    po_id: String,
    po_line_id: String,
    vendor_id: String,
}

async fn setup_inflow_fixture(pool: &PgPool, n_vendors: usize) -> Vec<Workspot> {
    // Vendors.
    for i in 1..=n_vendors {
        let code = format!("LOAD-V-{:03}", i);
        sqlx::query(
            "INSERT INTO vendors (code, name, currency)
             VALUES ($1, $2, 'USD')
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .bind(format!("Load test vendor {i}"))
        .execute(pool)
        .await
        .expect("insert load vendor");
    }

    // SKUs (one per vendor, standard cost=1).
    for i in 1..=n_vendors {
        let code = format!("LOAD-S-{:03}", i);
        sqlx::query(
            "INSERT INTO skus (code, uom, cost_method)
             VALUES ($1, 'EA', 'standard')
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(&code)
        .execute(pool)
        .await
        .expect("insert load sku");
    }

    // Standard cost rows for the LOAD SKUs (cost=1, effective forever).
    sqlx::raw_sql(
        "INSERT INTO standard_costs (sku_id, cost, effective_at, posted_by, idempotency_key)
         SELECT s.id, 1, '1970-01-01'::DATE,
                '00000000-0000-0000-0000-000000000000'::UUID, gen_random_uuid()
           FROM skus s
          WHERE s.code LIKE 'LOAD-S-%'
            AND NOT EXISTS (SELECT 1 FROM standard_costs sc WHERE sc.sku_id = s.id)",
    )
    .execute(pool)
    .await
    .expect("seed LOAD standard_costs");

    // Per-SKU stock_available + inv_value_raw at MAIN.
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, sku_id, location_id, normal_side)
         SELECT 'stock_available', 'qty', s.id, l.id, 'debit'
           FROM skus s, locations l
          WHERE s.code LIKE 'LOAD-S-%' AND l.code = 'MAIN'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind='stock_available' AND a.sku_id=s.id
                 AND a.location_id=l.id AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create LOAD stock_available accounts");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, sku_id, location_id)
         SELECT 'inv_value_raw', 'value', 'USD', 'debit', s.id, l.id
           FROM skus s, locations l
          WHERE s.code LIKE 'LOAD-S-%' AND l.code = 'MAIN'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind='inv_value_raw' AND a.sku_id=s.id
                 AND a.location_id=l.id AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create LOAD inv_value_raw accounts");

    // Per-vendor accounts (vendor_pool, ap_unsettled USD, ap USD).
    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, normal_side, counterparty_id)
         SELECT 'vendor_pool', 'qty', 'credit', v.id
           FROM vendors v
          WHERE v.code LIKE 'LOAD-V-%'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind='vendor_pool' AND a.counterparty_id=v.id
                 AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create LOAD vendor_pool accounts");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         SELECT 'ap_unsettled', 'value', 'USD', 'credit', v.id
           FROM vendors v
          WHERE v.code LIKE 'LOAD-V-%'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind='ap_unsettled' AND a.counterparty_id=v.id
                 AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create LOAD ap_unsettled accounts");

    sqlx::query(
        "INSERT INTO accounts (kind, ledger_kind, currency, normal_side, counterparty_id)
         SELECT 'ap', 'value', 'USD', 'credit', v.id
           FROM vendors v
          WHERE v.code LIKE 'LOAD-V-%'
            AND NOT EXISTS (
              SELECT 1 FROM accounts a
               WHERE a.kind='ap' AND a.counterparty_id=v.id
                 AND a.currency='USD' AND NOT a.is_closed)",
    )
    .execute(pool)
    .await
    .expect("create LOAD ap accounts");

    // POs (one per vendor) with one PO line each. qty_ordered very
    // high so sustained receipts don't exhaust the line.
    sqlx::query(
        "INSERT INTO purchase_orders (vendor_id, status)
         SELECT v.id, 'open'
           FROM vendors v
          WHERE v.code LIKE 'LOAD-V-%'
            AND NOT EXISTS (
              SELECT 1 FROM purchase_orders po WHERE po.vendor_id = v.id)",
    )
    .execute(pool)
    .await
    .expect("create LOAD POs");

    sqlx::query(
        "INSERT INTO purchase_order_lines
            (po_id, line_no, sku_id, location_id, qty_ordered, unit_cost, currency)
         SELECT po.id, 1, s.id, l.id, 10000000, 1, 'USD'
           FROM purchase_orders po
           JOIN vendors v ON v.id = po.vendor_id
           JOIN skus s ON s.code = REPLACE(v.code, 'LOAD-V-', 'LOAD-S-')
           JOIN locations l ON l.code = 'MAIN'
          WHERE v.code LIKE 'LOAD-V-%'
            AND NOT EXISTS (
              SELECT 1 FROM purchase_order_lines pl WHERE pl.po_id = po.id)",
    )
    .execute(pool)
    .await
    .expect("create LOAD PO lines");

    // Pull (po_id, po_line_id, vendor_id) triples back as text UUIDs.
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT po.id::text, pl.id::text, v.id::text
           FROM purchase_orders po
           JOIN vendors v ON v.id = po.vendor_id
           JOIN purchase_order_lines pl ON pl.po_id = po.id
          WHERE v.code LIKE 'LOAD-V-%'
          ORDER BY v.code",
    )
    .fetch_all(pool)
    .await
    .expect("lookup LOAD workspots");

    rows.into_iter()
        .map(|(po_id, po_line_id, vendor_id)| Workspot {
            po_id,
            po_line_id,
            vendor_id,
        })
        .collect()
}

#[tokio::test]
#[ignore = "load test — runs T4_DURATION_SECS (default 30); see file header"]
async fn inflow_workload_receipt_plus_bill() {
    let duration_secs: u64 = env::var("T4_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let n_writers: u32 = env::var("T4_WRITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    let n_vendors: usize = env::var("T4_BENCH_VENDORS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let qty_per_cycle: i64 = env::var("T4_QTY_PER_CYCLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    assert!(qty_per_cycle > 0, "T4_QTY_PER_CYCLE must be positive");
    let duration = Duration::from_secs(duration_secs);

    let pool = connect_test_db_with(n_writers + 4).await;
    reset_to_fixture(&pool).await;

    eprintln!("T4: setting up inflow fixture (n_vendors={n_vendors})");
    let setup_t0 = Instant::now();
    let workspots = setup_inflow_fixture(&pool, n_vendors).await;
    let setup_dur = setup_t0.elapsed();
    eprintln!(
        "T4: setup complete in {:.2}s, {} workspots ready",
        setup_dur.as_secs_f64(),
        workspots.len()
    );
    assert!(!workspots.is_empty(), "inflow setup produced no workspots");
    let workspots: Arc<Vec<Workspot>> = Arc::new(workspots);

    eprintln!(
        "T4: writers={n_writers} duration={duration_secs}s qty_per_cycle={qty_per_cycle} pool_size={} (inflow receipt+bill)",
        workspots.len()
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

    let amount = qty_per_cycle * 1; // unit_cost=1, std=1 (no PPV)

    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_writers as usize);
    for w in 0..n_writers {
        let pool_w = pool.clone();
        let workspots_w = workspots.clone();
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
            let posted_by = "00000000-0000-0000-0000-0000000000bb".to_string();

            while start.elapsed() < duration {
                let spot = &workspots_w[(xorshift(&mut rng) as usize) % workspots_w.len()];

                let receipt_key = fresh_uuid_str(&mut rng);
                let bill_key = fresh_uuid_str(&mut rng);
                let receipt_lines = json!([{
                    "po_line_id": spot.po_line_id,
                    "qty_received": qty_per_cycle,
                }]);
                let bill_lines = json!([{
                    "kind": "po_match",
                    "po_line_id": spot.po_line_id,
                    "qty": qty_per_cycle,
                    "unit_cost": 1,
                    "amount": amount,
                }]);

                let t0 = Instant::now();

                let receipt_res = sqlx::query(
                    "SELECT post_po_receipt(
                        $1::UUID, $2, '2026-04-15'::DATE, $3::UUID, $4::UUID, NULL)",
                )
                .bind(&spot.po_id)
                .bind(&receipt_lines)
                .bind(&posted_by)
                .bind(&receipt_key)
                .execute(&pool_w)
                .await;

                if let Err(e) = receipt_res {
                    let code = e
                        .as_database_error()
                        .and_then(|d| d.code().map(|c| c.into_owned()))
                        .unwrap_or_else(|| "no-code".to_string());
                    if code == "40P01" {
                        panic!("DEADLOCK on writer {w} (receipt): {e}");
                    }
                    eprintln!("writer {w} receipt error [{code}]: {e}");
                    err_w.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let bill_res = sqlx::query(
                    "SELECT post_ap_bill(
                        $1::UUID, $2, $3, '2026-04-15'::DATE,
                        $4::UUID, $5::UUID, NULL)",
                )
                .bind(&spot.vendor_id)
                .bind("USD")
                .bind(&bill_lines)
                .bind(&posted_by)
                .bind(&bill_key)
                .execute(&pool_w)
                .await;

                let dur_us = t0.elapsed().as_micros().min(u32::MAX as u128) as u32;
                latencies_us.push(dur_us);

                match bill_res {
                    Ok(_) => {
                        ok_w.fetch_add(1, Ordering::Relaxed);
                        // 2 events from receipt (qty + value, no PPV) + 1 event from bill.
                        ev_w.fetch_add(3, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let code = e
                            .as_database_error()
                            .and_then(|d| d.code().map(|c| c.into_owned()))
                            .unwrap_or_else(|| "no-code".to_string());
                        if code == "40P01" {
                            panic!("DEADLOCK on writer {w} (bill): {e}");
                        }
                        eprintln!("writer {w} bill error [{code}]: {e}");
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
        "duration_s={:.2} writers={} qty_per_cycle={} pool_size={} (inflow receipt+bill)",
        elapsed.as_secs_f64(),
        n_writers,
        qty_per_cycle,
        workspots.len()
    );
    eprintln!(
        "cycles: total={} ok={} err={} throughput={:.1}/s",
        total,
        ok,
        err,
        total as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "events: total={} throughput={:.1}/s (3 events per ok cycle: receipt qty + receipt value + bill clearance)",
        events,
        events as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "cycle_latency_us (receipt + bill end-to-end): p50={} p95={} p99={} p99.9={} max={} (n={})",
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
        "pg_stat_io (client backend, summed): reads_delta={} read_bytes_delta={} writes_delta={} write_bytes_delta={} extends_delta={} hits_delta={} fsyncs_delta={}",
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
        "T4_CSV_HEADER: duration_s,writers,qty_per_cycle,cycles_total,cycles_ok,cycles_err,events_total,throughput_cps,throughput_evps,p50_us,p95_us,p99_us,p999_us,max_us,deadlocks_delta,xact_commit_delta,xact_rollback_delta,blks_read_delta,blks_hit_delta,io_reads_delta,io_read_bytes_delta,io_writes_delta,io_write_bytes_delta,io_extends_delta,io_hits_delta,io_fsyncs_delta,wal_bytes_delta"
    );
    eprintln!(
        "T4_CSV_VALUES: {:.3},{},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        elapsed.as_secs_f64(),
        n_writers,
        qty_per_cycle,
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
    assert!(total > 0, "no cycles executed");
}
