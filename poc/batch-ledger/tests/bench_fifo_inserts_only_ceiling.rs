//! acct-vh5y — FIFO pure-INSERT-only ceiling bench.
//!
//! No FIFO logic. No reads. No function calls. Just three multi-row INSERT
//! statements per batch into the relevant tables, sized to match a real
//! FIFO batch's durable write volume:
//!
//!   - 1000 rows into posting_lines (1 per envelope at batch=1000)
//!   - 300  rows into cost_layers (30% receipts; receipt_posting_line_id NULL)
//!   - 700  rows into cost_layer_depletions (70% issues × 1 layer/issue avg;
//!          layer_id + issue_posting_line_id FK to pre-seeded rows)
//!
//! This is the upper bound: whatever WAL + indexes + FK validation cost
//! for that row volume per second. Any real FIFO design's per-batch CPU
//! pays on TOP of this; the ceiling is "infinite CPU but the same writes".
//!
//! Pre-seed: 100 accounts + 10000 cost_layers + 10000 posting_lines so the
//! depletion FKs always resolve to a valid row without any per-batch read.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn fifo_inserts_only_ceiling() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let issue_pct = env_or::<u32>("POC_BENCH_ISSUE_PCT", 70);
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    println!(
        "==> inserts-only ceiling bench: workers={} duration={}s batch_size={} issue_pct={}",
        workers, duration_secs, batch_size, issue_pct
    );

    // Reset target tables.
    println!("==> truncating target tables");
    sqlx::query(
        "TRUNCATE cost_layer_depletions, cost_layers, posting_lines RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .expect("truncate");

    // Pre-seed accounts (100 pool accounts + 100 counterparty accounts).
    println!("==> pre-seeding 200 accounts");
    let acct_ids: Vec<i64> = (8_000_000_000_000i64..8_000_000_000_200i64).collect();
    let acct_codes: Vec<String> = acct_ids.iter().map(|id| format!("bench-{}", id)).collect();
    let acct_kinds: Vec<&str> = (0..200)
        .map(|i| if i < 100 { "inv_value_raw" } else { "credit_normal" })
        .collect();
    sqlx::query(
        "INSERT INTO accounts (id, code, kind, currency) \
         SELECT u.id, u.code, u.kind::account_kind, 'USD' \
         FROM unnest($1::bigint[], $2::text[], $3::text[]) AS u(id, code, kind) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(&acct_ids)
    .bind(&acct_codes)
    .bind(&acct_kinds)
    .execute(&pool)
    .await
    .expect("seed accounts");

    // Pre-seed 10000 posting_lines so depletions can FK to them.
    println!("==> pre-seeding 10000 posting_lines");
    let seed_pl_amount: Vec<i64> = (0..10000).map(|_| 1000).collect();
    let seed_pl_qty: Vec<i64> = (0..10000).map(|_| 10).collect();
    let seed_pl_idemp: Vec<Uuid> = (0..10000).map(|_| Uuid::new_v4()).collect();
    let seed_pl_debit: Vec<i64> = (0..10000).map(|i| acct_ids[100 + (i % 100)]).collect();
    let seed_pl_credit: Vec<i64> = (0..10000).map(|i| acct_ids[i % 100]).collect();
    let seed_pl_currency: Vec<&str> = (0..10000).map(|_| "USD").collect();
    let seed_pl_bdate: Vec<chrono::NaiveDate> = (0..10000)
        .map(|_| chrono::NaiveDate::from_ymd_opt(2026, 5, 13).unwrap())
        .collect();
    let seed_pl_ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO posting_lines (debit_account_id, credit_account_id, amount, currency, idempotency_key, business_date, qty) \
         SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::uuid[], $6::date[], $7::bigint[]) \
         RETURNING id",
    )
    .bind(&seed_pl_debit)
    .bind(&seed_pl_credit)
    .bind(&seed_pl_amount)
    .bind(&seed_pl_currency)
    .bind(&seed_pl_idemp)
    .bind(&seed_pl_bdate)
    .bind(&seed_pl_qty)
    .fetch_all(&pool)
    .await
    .expect("seed pl");

    // Pre-seed 10000 cost_layers.
    println!("==> pre-seeding 10000 cost_layers");
    let seed_cl_pool: Vec<i64> = (0..10000).map(|i| acct_ids[i % 100]).collect();
    let seed_cl_qty: Vec<i64> = (0..10000).map(|_| 1_000_000).collect();
    let seed_cl_uc: Vec<i64> = (0..10000).map(|_| 1000).collect();
    let seed_cl_rdate: Vec<chrono::NaiveDate> = (0..10000)
        .map(|_| chrono::NaiveDate::from_ymd_opt(2026, 5, 13).unwrap())
        .collect();
    let seed_layer_ids: Vec<i64> = sqlx::query_scalar(
        "INSERT INTO cost_layers (pool_account_id, qty_remaining, unit_cost, receipt_date) \
         SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::date[]) \
         RETURNING id",
    )
    .bind(&seed_cl_pool)
    .bind(&seed_cl_qty)
    .bind(&seed_cl_uc)
    .bind(&seed_cl_rdate)
    .fetch_all(&pool)
    .await
    .expect("seed cl");

    println!(
        "==> pre-seed complete: {} posting_lines, {} cost_layers",
        seed_pl_ids.len(),
        seed_layer_ids.len()
    );

    // Bench loop.
    let receipts_per_batch = (batch_size as u32 * (100 - issue_pct) / 100) as usize;
    let issues_per_batch = batch_size - receipts_per_batch;
    println!(
        "==> per batch: {} posting_lines, {} new cost_layers, {} new depletions",
        batch_size, receipts_per_batch, issues_per_batch
    );

    let batches_done = Arc::new(AtomicU64::new(0));
    let rows_done = Arc::new(AtomicU64::new(0));
    let latencies_us = Arc::new(std::sync::Mutex::new(Vec::<u64>::with_capacity(100_000)));

    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);
    let mut handles = Vec::new();

    let seed_pl_ids_arc = Arc::new(seed_pl_ids);
    let seed_layer_ids_arc = Arc::new(seed_layer_ids);
    let acct_ids_arc = Arc::new(acct_ids);

    for worker_id in 0..workers {
        let pool = pool.clone();
        let batches_done = batches_done.clone();
        let rows_done = rows_done.clone();
        let latencies_us = latencies_us.clone();
        let seed_pl_ids = seed_pl_ids_arc.clone();
        let seed_layer_ids = seed_layer_ids_arc.clone();
        let acct_ids = acct_ids_arc.clone();

        handles.push(tokio::spawn(async move {
            let mut counter: u64 = (worker_id as u64) << 32;
            let mut local_lat: Vec<u64> = Vec::with_capacity(1024);
            loop {
                if Instant::now() >= deadline {
                    break;
                }
                let t0 = Instant::now();

                // Build posting_lines payload.
                let mut pl_debit: Vec<i64> = Vec::with_capacity(batch_size);
                let mut pl_credit: Vec<i64> = Vec::with_capacity(batch_size);
                let mut pl_amount: Vec<i64> = Vec::with_capacity(batch_size);
                let mut pl_currency: Vec<&str> = Vec::with_capacity(batch_size);
                let mut pl_idemp: Vec<Uuid> = Vec::with_capacity(batch_size);
                let mut pl_bdate: Vec<chrono::NaiveDate> = Vec::with_capacity(batch_size);
                let mut pl_qty: Vec<i64> = Vec::with_capacity(batch_size);
                for i in 0..batch_size {
                    let pool_idx = (counter as usize + i) % 100;
                    pl_debit.push(acct_ids[100 + pool_idx]);
                    pl_credit.push(acct_ids[pool_idx]);
                    pl_amount.push(10_000);
                    pl_currency.push("USD");
                    pl_idemp.push(Uuid::new_v4());
                    pl_bdate.push(chrono::NaiveDate::from_ymd_opt(2026, 5, 13).unwrap());
                    pl_qty.push(10);
                }
                let pl_res = sqlx::query(
                    "INSERT INTO posting_lines (debit_account_id, credit_account_id, amount, currency, idempotency_key, business_date, qty) \
                     SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::uuid[], $6::date[], $7::bigint[])",
                )
                .bind(&pl_debit)
                .bind(&pl_credit)
                .bind(&pl_amount)
                .bind(&pl_currency)
                .bind(&pl_idemp)
                .bind(&pl_bdate)
                .bind(&pl_qty)
                .execute(&pool)
                .await;

                if let Err(e) = pl_res {
                    eprintln!("worker {} pl err: {}", worker_id, e);
                    counter += 1;
                    continue;
                }

                // Build cost_layers payload.
                let mut cl_pool: Vec<i64> = Vec::with_capacity(receipts_per_batch);
                let mut cl_qty: Vec<i64> = Vec::with_capacity(receipts_per_batch);
                let mut cl_uc: Vec<i64> = Vec::with_capacity(receipts_per_batch);
                let mut cl_rdate: Vec<chrono::NaiveDate> =
                    Vec::with_capacity(receipts_per_batch);
                for i in 0..receipts_per_batch {
                    let pool_idx = (counter as usize + i) % 100;
                    cl_pool.push(acct_ids[pool_idx]);
                    cl_qty.push(1000);
                    cl_uc.push(1000);
                    cl_rdate.push(chrono::NaiveDate::from_ymd_opt(2026, 5, 13).unwrap());
                }
                if !cl_pool.is_empty() {
                    let cl_res = sqlx::query(
                        "INSERT INTO cost_layers (pool_account_id, qty_remaining, unit_cost, receipt_date) \
                         SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::date[])",
                    )
                    .bind(&cl_pool)
                    .bind(&cl_qty)
                    .bind(&cl_uc)
                    .bind(&cl_rdate)
                    .execute(&pool)
                    .await;
                    if let Err(e) = cl_res {
                        eprintln!("worker {} cl err: {}", worker_id, e);
                        counter += 1;
                        continue;
                    }
                }

                // Build cost_layer_depletions payload.
                let mut dp_layer: Vec<i64> = Vec::with_capacity(issues_per_batch);
                let mut dp_pl: Vec<i64> = Vec::with_capacity(issues_per_batch);
                let mut dp_qty: Vec<i64> = Vec::with_capacity(issues_per_batch);
                let mut dp_cost: Vec<i64> = Vec::with_capacity(issues_per_batch);
                for i in 0..issues_per_batch {
                    let layer_idx = (counter as usize + i) % seed_layer_ids.len();
                    let pl_idx = (counter as usize + i) % seed_pl_ids.len();
                    dp_layer.push(seed_layer_ids[layer_idx]);
                    dp_pl.push(seed_pl_ids[pl_idx]);
                    dp_qty.push(1);
                    dp_cost.push(1000);
                }
                if !dp_layer.is_empty() {
                    let dp_res = sqlx::query(
                        "INSERT INTO cost_layer_depletions (layer_id, issue_posting_line_id, qty_consumed, cost_amount) \
                         SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[])",
                    )
                    .bind(&dp_layer)
                    .bind(&dp_pl)
                    .bind(&dp_qty)
                    .bind(&dp_cost)
                    .execute(&pool)
                    .await;
                    if let Err(e) = dp_res {
                        eprintln!("worker {} dp err: {}", worker_id, e);
                        counter += 1;
                        continue;
                    }
                }

                let elapsed_us = t0.elapsed().as_micros() as u64;
                local_lat.push(elapsed_us);
                batches_done.fetch_add(1, Ordering::Relaxed);
                rows_done.fetch_add(
                    (batch_size + receipts_per_batch + issues_per_batch) as u64,
                    Ordering::Relaxed,
                );
                counter = counter.wrapping_add(batch_size as u64);
            }
            let mut all = latencies_us.lock().unwrap();
            all.extend_from_slice(&local_lat);
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();
    let total_batches = batches_done.load(Ordering::Relaxed);
    let total_rows = rows_done.load(Ordering::Relaxed);
    let tps_rows = total_rows as f64 / elapsed.as_secs_f64();
    let tps_envelopes = (total_batches as f64 * batch_size as f64) / elapsed.as_secs_f64();
    let tps_batches = total_batches as f64 / elapsed.as_secs_f64();

    let mut lats: Vec<u64> = latencies_us.lock().unwrap().clone();
    lats.sort_unstable();
    let pct = |q: f64| -> u64 {
        if lats.is_empty() {
            0
        } else {
            lats[((lats.len() as f64 * q).min(lats.len() as f64 - 1.0)) as usize]
        }
    };

    println!();
    println!("==============================================================================");
    println!("INSERTS-ONLY CEILING:");
    println!("  workers={} duration={:?}", workers, elapsed);
    println!("  batches={} rows={}", total_batches, total_rows);
    println!(
        "  throughput: batches={:.1}/s, envelopes={:.1}/s, transfers={:.1}/s",
        tps_batches, tps_envelopes, tps_rows
    );
    println!(
        "  batch-latency (us): p50={} p95={} p99={} p99.9={} max={}",
        pct(0.50),
        pct(0.95),
        pct(0.99),
        pct(0.999),
        lats.last().copied().unwrap_or(0)
    );
    println!("==============================================================================");
}
