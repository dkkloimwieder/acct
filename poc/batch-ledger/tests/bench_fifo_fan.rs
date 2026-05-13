//! FIFO fan-in / fan-out bench. Parallel to `bench_wac_fan.rs` for the
//! FIFO cost method.
//!
//! Routes via POC_BENCH_FUNCTION:
//!   - "post_batch_fifo" (mig 0020 mutable baseline; mig 0009's body
//!     under a stable name)
//!   - "post_batch_fifo_maximal" (future Rust-dispatcher variant)
//!
//! Workload: 30% fifo_receipt + 70% fifo_issue, pools pre-seeded with
//! 5 layers × 1M qty each so workers can't drain them within a 60s run.
//! Fan-in uses 1 hot pool (all writers contend on its layers); fan-out
//! spreads across N pools.

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
async fn fifo_fan_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let pools_count = env_or::<usize>("POC_BENCH_POOLS", 1);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let issue_pct = env_or::<u32>("POC_BENCH_ISSUE_PCT", 70);
    let shape = std::env::var("POC_BENCH_SHAPE").unwrap_or_else(|_| "fan_in".to_string());
    let bench_fn =
        std::env::var("POC_BENCH_FUNCTION").unwrap_or_else(|_| "post_batch_fifo".to_string());
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    sqlx::query(
        "TRUNCATE posting_lines, accounts, cost_layers, cost_layer_depletions RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .expect("truncate");

    if bench_fn.ends_with("_shmem") || bench_fn.ends_with("_maximal") {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
            .execute(&pool)
            .await
            .expect("ext");
        sqlx::query("TRUNCATE account_balances_rollup RESTART IDENTITY")
            .execute(&pool)
            .await
            .expect("rollup truncate");
        sqlx::query("SELECT ledger_shmem_reset()")
            .execute(&pool)
            .await
            .expect("shmem reset");
    }

    let ap_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('fifo-ap', 'USD', 'credit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("ap");
    let cogs_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('fifo-cogs', 'USD', 'debit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("cogs");

    sqlx::query(
        "INSERT INTO accounts (code, currency, kind, balance, qty)
         SELECT 'fifo-pool-' || lpad(g::text, 5, '0'), 'USD', 'inv_value_raw', 0, 0
           FROM generate_series(0, $1 - 1) g",
    )
    .bind(pools_count as i64)
    .execute(&pool)
    .await
    .expect("bulk seed pools");

    let pool_ids: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM accounts WHERE kind = 'inv_value_raw' ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("pool ids");
    assert_eq!(pool_ids.len(), pools_count, "pool seed count mismatch");

    // Pre-seed each pool with 5 layers × 1M qty = 5M qty/pool. Enough for any
    // 60s bench at workers × batch=1000 × 70% issue × max-take=10.
    for layer_no in 0..5 {
        let mut envs = Vec::with_capacity(pool_ids.len());
        for (i, pid) in pool_ids.iter().enumerate() {
            envs.push(json!({
                "envelope_idx": i as i32,
                "kind": "fifo_receipt",
                "debit_account_id": pid,
                "credit_account_id": ap_id,
                "qty": 1_000_000_i64,
                "unit_cost": 100_i64 + layer_no * 10,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": format!("2026-05-{:02}", 1 + layer_no),
            }));
        }
        // Use post_batch_fifo for the seed (covers all benches uniformly).
        sqlx::query("SELECT * FROM post_batch_fifo($1)")
            .bind(Value::Array(envs))
            .execute(&pool)
            .await
            .expect("layer seed");
    }

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_before");

    eprintln!(
        "FIFO bench: shape={shape}, pools={pools_count}, workers={workers}, batch={batch_size}, issue_pct={issue_pct}%, duration={duration_secs}s, fn={bench_fn}"
    );

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let q_call = format!("SELECT * FROM {}($1)", bench_fn);
    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let pids = pool_ids.clone();
        let shape = shape.clone();
        let q_call = q_call.clone();
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
                    let pid = if shape == "fan_in" {
                        pids[0]
                    } else {
                        pids[r % pids.len()]
                    };
                    let is_issue = ((r / 13) % 100) < issue_pct as usize;
                    if is_issue {
                        let qty = 1 + ((r / 11) % 10) as i64;
                        envelopes.push(json!({
                            "envelope_idx": j as i32,
                            "kind": "fifo_issue",
                            "debit_account_id": cogs_id,
                            "credit_account_id": pid,
                            "qty": qty,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-06-11",
                        }));
                    } else {
                        let qty = 1 + ((r / 11) % 100) as i64;
                        let unit_cost = 80 + ((r / 17) % 40) as i64;
                        envelopes.push(json!({
                            "envelope_idx": j as i32,
                            "kind": "fifo_receipt",
                            "debit_account_id": pid,
                            "credit_account_id": ap_id,
                            "qty": qty,
                            "unit_cost": unit_cost,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-06-11",
                        }));
                    }
                }
                let envelopes_value = Value::Array(envelopes);
                s.batches_attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = sqlx::query(&q_call)
                    .bind(envelopes_value)
                    .execute(&p)
                    .await;
                samples_ns.push(t0.elapsed().as_nanos() as u64);
                match res {
                    Ok(_) => {
                        s.batches_ok.fetch_add(1, Ordering::Relaxed);
                        s.transfers_ok
                            .fetch_add(batch_size as u64, Ordering::Relaxed);
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
    let transfers = stats.transfers_ok.load(Ordering::Relaxed);

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

    eprintln!();
    eprintln!(
        "========= FIFO fan bench (fn={}, shape={}, pools={}, issue_pct={}, batch={}) =========",
        bench_fn, shape, pools_count, issue_pct, batch_size
    );
    eprintln!("workers: {workers}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("batches_attempted: {attempted}");
    eprintln!("batches_ok:        {ok}");
    eprintln!("batches_err:       {err}");
    eprintln!("transfers_ok:      {transfers}");
    eprintln!(
        "throughput: batches={:.1}/s, transfers={:.1}/s",
        ok as f64 / wall_secs,
        transfers as f64 / wall_secs
    );
    eprintln!("batch-latency (us): p50={p50} p95={p95} p99={p99} p99.9={p999} max={max}");
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!(
        "=============================================================================="
    );
}
