//! P4-PAC — WAC PAC (preset snapshot + fixed-cost-per-batch) bench.
//! Pure-SQL CTE chain, no plpgsql FOR LOOP. Comparison point for P4 strict WAC.

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
async fn p4pac_wac_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let pools = env_or::<usize>("POC_BENCH_POOLS", 20);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url).await.expect("connect");

    sqlx::query("TRUNCATE posting_lines, accounts, cost_layers, cost_layer_depletions RESTART IDENTITY CASCADE")
        .execute(&pool).await.expect("truncate");

    let ap_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('pac-ap', 'USD', 'credit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("ap");
    let cogs_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('pac-cogs', 'USD', 'debit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("cogs");
    let mut pool_ids: Vec<i64> = Vec::with_capacity(pools);
    for i in 0..pools {
        let kind = if i % 2 == 0 { "inv_value_raw" } else { "inv_value_fg" };
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO accounts (code, currency, kind) VALUES ($1, 'USD', $2::account_kind) RETURNING id"
        ).bind(format!("pac-pool-{i:02}")).bind(kind).fetch_one(&pool).await.expect("pool");
        pool_ids.push(id);
    }

    // Pre-seed pools.
    let mut seed_envs = Vec::new();
    for (i, pid) in pool_ids.iter().enumerate() {
        seed_envs.push(json!({
            "envelope_idx": i as i32, "kind": "wac_pac_receipt",
            "debit_account_id": pid, "credit_account_id": ap_id,
            "qty": 1_000_000_i64, "unit_cost": 100_i64,
            "idempotency_key": Uuid::new_v4().to_string(),
            "business_date": "2026-05-12",
        }));
    }
    sqlx::query("SELECT * FROM post_batch($1)").bind(Value::Array(seed_envs))
        .execute(&pool).await.expect("seed");

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'"
    ).fetch_one(&pool).await.expect("dl_before");

    eprintln!("P4-PAC WAC bench: workers={workers}, pools={pools}, duration={duration_secs}s, batch_size={batch_size}");

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let pids = pool_ids.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            while Instant::now() < deadline {
                let mut envelopes = Vec::with_capacity(batch_size);
                for j in 0..batch_size {
                    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                    let r = rng as usize;
                    let pid = pids[r % pids.len()];
                    let is_receipt = (r >> 8) % 10 < 3;
                    if is_receipt {
                        envelopes.push(json!({
                            "envelope_idx": j as i32, "kind": "wac_pac_receipt",
                            "debit_account_id": pid, "credit_account_id": ap_id,
                            "qty": 1 + ((r >> 16) % 100) as i64,
                            "unit_cost": 80 + ((r >> 24) % 40) as i64,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-05-12",
                        }));
                    } else {
                        envelopes.push(json!({
                            "envelope_idx": j as i32, "kind": "wac_pac_issue",
                            "debit_account_id": cogs_id, "credit_account_id": pid,
                            "qty": 1 + ((r >> 16) % 10) as i64,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-05-12",
                        }));
                    }
                }
                s.batches_attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = sqlx::query("SELECT * FROM post_batch($1)")
                    .bind(Value::Array(envelopes)).execute(&p).await;
                samples_ns.push(t0.elapsed().as_nanos() as u64);
                match res {
                    Ok(_) => {
                        s.batches_ok.fetch_add(1, Ordering::Relaxed);
                        s.transfers_ok.fetch_add(batch_size as u64, Ordering::Relaxed);
                    }
                    Err(_) => { s.batches_err.fetch_add(1, Ordering::Relaxed); }
                }
            }
            samples_ns
        }));
    }

    let mut all_samples: Vec<u64> = Vec::new();
    for h in handles { all_samples.extend(h.await.expect("worker")); }
    all_samples.sort_unstable();
    let wall_secs = wall_start.elapsed().as_secs_f64();

    let dl_after: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'"
    ).fetch_one(&pool).await.expect("dl_after");

    let attempted = stats.batches_attempted.load(Ordering::Relaxed);
    let ok = stats.batches_ok.load(Ordering::Relaxed);
    let err = stats.batches_err.load(Ordering::Relaxed);
    let trans = stats.transfers_ok.load(Ordering::Relaxed);
    let pct = |q: f64| -> u64 {
        if all_samples.is_empty() { 0 } else {
            let idx = ((all_samples.len() as f64 - 1.0) * q).round() as usize;
            all_samples[idx] / 1_000
        }
    };

    eprintln!("\n========= P4-PAC WAC bench (batch_size={batch_size}) =========");
    eprintln!("workers: {workers}, pools: {pools}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("batches_attempted: {attempted}");
    eprintln!("batches_ok:        {ok}");
    eprintln!("batches_err:       {err}");
    eprintln!("transfers_ok:      {trans}");
    eprintln!("throughput: batches={:.1}/s, transfers={:.1}/s",
        attempted as f64 / wall_secs, trans as f64 / wall_secs);
    eprintln!("batch-latency (us): p50={} p95={} p99={} p99.9={} max={}",
        pct(0.50), pct(0.95), pct(0.99), pct(0.999),
        all_samples.last().copied().unwrap_or(0) / 1_000);
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!("=============================================================\n");
    assert!(ok > 0);
}
