//! Fan-out bench — every envelope picks a distinct (debit, credit) pair from
//! a large pool. Tests the "no hot row" shape: workers don't contend on
//! account rows; bottleneck is per-row INSERT + index + WAL cost.
//!
//! Sets a baseline floor against which fan-in's contention overhead can be
//! measured. Mimics the realistic-traffic-spread shape (shape F in
//! perf_baseline_v0).
//!
//! POC_BENCH_FUNCTION selects post_batch vs post_batch_append_only.

use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[derive(Default)]
struct Stats {
    batches_attempted: AtomicU64,
    batches_ok: AtomicU64,
    batches_err: AtomicU64,
    transfers_ok: AtomicU64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn fan_out_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let accounts = env_or::<usize>("POC_BENCH_ACCOUNTS", 5000);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let fn_name = std::env::var("POC_BENCH_FUNCTION")
        .unwrap_or_else(|_| "post_batch_append_only".to_string());
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    sqlx::query("TRUNCATE posting_lines, accounts RESTART IDENTITY CASCADE")
        .execute(&pool)
        .await
        .expect("truncate");

    // If we're benching the shmem path, ensure the extension is loaded
    // and wipe both the shmem hash and its durable rollup so prior runs
    // don't pollute the measurement.
    if fn_name.contains("shmem") {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
            .execute(&pool).await.expect("create ext");
        sqlx::query("TRUNCATE account_balances_rollup")
            .execute(&pool).await.expect("trunc rollup");
        sqlx::query("SELECT ledger_shmem_reset()")
            .execute(&pool).await.expect("shmem reset");
    }

    // Bulk-seed accounts: alternating debit/credit kinds.
    sqlx::query(
        "INSERT INTO accounts (code, currency, kind)
         SELECT 'fan-out-' || lpad(g::text, 5, '0'),
                'USD',
                CASE WHEN g % 2 = 0 THEN 'debit_normal'::account_kind
                     ELSE 'credit_normal'::account_kind END
         FROM generate_series(0, $1 - 1) g"
    )
    .bind(accounts as i64)
    .execute(&pool)
    .await
    .expect("bulk seed");

    let debit_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM accounts WHERE kind = 'debit_normal'")
        .fetch_all(&pool).await.expect("debits");
    let credit_ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM accounts WHERE kind = 'credit_normal'")
        .fetch_all(&pool).await.expect("credits");

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_before");

    eprintln!(
        "Fan-out bench: fn={fn_name}, workers={workers}, accounts={accounts} ({}d/{}c), batch={batch_size}, duration={duration_secs}s",
        debit_ids.len(), credit_ids.len()
    );

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();
    let sql = format!("SELECT * FROM {fn_name}($1)");

    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let d_pool = debit_ids.clone();
        let c_pool = credit_ids.clone();
        let sql = sql.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);

            while Instant::now() < deadline {
                let mut envelopes = Vec::with_capacity(batch_size);
                for j in 0..batch_size {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let r = rng as usize;
                    let debit = d_pool[r % d_pool.len()];
                    let credit = c_pool[(r / 7) % c_pool.len()];
                    let amount = 1 + ((r / 11) % 1_000) as i64;
                    envelopes.push(json!({
                        "envelope_idx":       j as i32,
                        "debit_account_id":   debit,
                        "credit_account_id":  credit,
                        "amount":             amount,
                        "idempotency_key":    Uuid::new_v4().to_string(),
                        "business_date":      "2026-05-12",
                    }));
                }
                let envelopes_value = Value::Array(envelopes);
                s.batches_attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = sqlx::query(&sql)
                    .bind(envelopes_value)
                    .execute(&p)
                    .await;
                samples_ns.push(t0.elapsed().as_nanos() as u64);
                match res {
                    Ok(_) => {
                        s.batches_ok.fetch_add(1, Ordering::Relaxed);
                        s.transfers_ok.fetch_add(batch_size as u64, Ordering::Relaxed);
                    }
                    Err(_) => {
                        s.batches_err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            samples_ns
        }));
    }

    let mut all_samples: Vec<u64> = Vec::new();
    for h in handles {
        all_samples.extend(h.await.expect("worker"));
    }
    all_samples.sort_unstable();
    let wall_secs = wall_start.elapsed().as_secs_f64();

    let dl_after: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_after");

    let attempted = stats.batches_attempted.load(Ordering::Relaxed);
    let ok = stats.batches_ok.load(Ordering::Relaxed);
    let err = stats.batches_err.load(Ordering::Relaxed);
    let trans = stats.transfers_ok.load(Ordering::Relaxed);

    let pct = |q: f64| -> u64 {
        if all_samples.is_empty() {
            0
        } else {
            let idx = ((all_samples.len() as f64 - 1.0) * q).round() as usize;
            all_samples[idx] / 1_000
        }
    };

    eprintln!("\n========= Fan-out bench (fn={fn_name}, batch={batch_size}, {accounts} accts) =========");
    eprintln!("workers: {workers}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("batches_attempted: {attempted}");
    eprintln!("batches_ok:        {ok}");
    eprintln!("batches_err:       {err}");
    eprintln!("transfers_ok:      {trans}");
    eprintln!(
        "throughput: batches={:.1}/s, transfers={:.1}/s",
        attempted as f64 / wall_secs,
        trans as f64 / wall_secs
    );
    eprintln!(
        "batch-latency (us): p50={} p95={} p99={} p99.9={} max={}",
        pct(0.50),
        pct(0.95),
        pct(0.99),
        pct(0.999),
        all_samples.last().copied().unwrap_or(0) / 1_000
    );
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!("=====================================================================\n");

    assert!(ok > 0, "no successful batches — bench broken");
}
