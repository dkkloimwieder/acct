//! P3 — batch API simplest case, batch-size sweep.
//!
//! Measures throughput at batch sizes [1, 10, 100, 1000, 8000]. Same hardware /
//! same 50 accounts × 20 workers / 60s as P2, so the numbers compare directly.
//!
//! Run via `./bench/run-p3.sh` (which iterates batch sizes). Direct invocation:
//!
//!     POC_BENCH_BATCH_SIZE=1000 cargo test --manifest-path poc/batch-ledger/Cargo.toml \
//!         --release --test bench_p3_batch_simple -- --ignored --nocapture --test-threads=1 \
//!         p3_batch_bench

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
async fn p3_batch_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let accounts = env_or::<usize>("POC_BENCH_ACCOUNTS", 50);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
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

    let mut acct_ids: Vec<i64> = Vec::with_capacity(accounts);
    for i in 0..accounts {
        let kind = if i % 2 == 0 { "debit_normal" } else { "credit_normal" };
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO accounts (code, currency, kind) VALUES ($1, 'USD', $2::account_kind) RETURNING id",
        )
        .bind(format!("acct-{i:03}"))
        .bind(kind)
        .fetch_one(&pool)
        .await
        .expect("seed");
        acct_ids.push(id);
    }
    let debit_ids: Vec<i64> = acct_ids.iter().step_by(2).copied().collect();
    let credit_ids: Vec<i64> = acct_ids.iter().skip(1).step_by(2).copied().collect();

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_before");

    eprintln!(
        "P3 bench starting: workers={workers}, accounts={accounts}, duration={duration_secs}s, batch_size={batch_size}"
    );

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let d_pool = debit_ids.clone();
        let c_pool = credit_ids.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);

            while Instant::now() < deadline {
                // Build a batch of `batch_size` envelopes.
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
                        "business_date":      "2026-05-11",
                    }));
                }
                let envelopes_value = Value::Array(envelopes);

                s.batches_attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = sqlx::query("SELECT * FROM post_batch($1)")
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

    let batches_attempted = stats.batches_attempted.load(Ordering::Relaxed);
    let batches_ok = stats.batches_ok.load(Ordering::Relaxed);
    let batches_err = stats.batches_err.load(Ordering::Relaxed);
    let transfers_ok = stats.transfers_ok.load(Ordering::Relaxed);

    let pct = |q: f64| -> u64 {
        if all_samples.is_empty() {
            0
        } else {
            let idx = ((all_samples.len() as f64 - 1.0) * q).round() as usize;
            all_samples[idx] / 1_000
        }
    };
    let p50 = pct(0.50);
    let p95 = pct(0.95);
    let p99 = pct(0.99);
    let p999 = pct(0.999);
    let max = all_samples.last().copied().unwrap_or(0) / 1_000;

    eprintln!("\n=========== P3 batch bench (batch_size={batch_size}) ===========");
    eprintln!("workers: {workers}, accounts: {accounts}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("batches_attempted: {batches_attempted}");
    eprintln!("batches_ok:        {batches_ok}");
    eprintln!("batches_err:       {batches_err}");
    eprintln!("transfers_ok:      {transfers_ok}");
    eprintln!(
        "throughput: batches={:.1}/s, transfers={:.1}/s",
        batches_attempted as f64 / wall_secs,
        transfers_ok as f64 / wall_secs
    );
    eprintln!("batch-latency (us): p50={p50} p95={p95} p99={p99} p99.9={p999} max={max}");
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!("=============================================================\n");

    assert!(batches_ok > 0, "no successful batches — bench broken");
}
