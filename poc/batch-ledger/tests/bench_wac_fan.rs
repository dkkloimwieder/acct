//! WAC fan-in / fan-out bench — multi-step transactional perf for cost-dispatched
//! workloads. Tests the contention shape that real ledger WAC dispatch produces.
//!
//! Each batch is a single transaction containing N envelopes that share a WAC
//! pool (fan-in) or each target a distinct pool (fan-out). post_batch_wac takes
//! FOR UPDATE on involved pool rows at batch start; the depth/spread of that
//! lock set is what we're measuring.
//!
//! POC_BENCH_SHAPE = "fan_in" | "fan_out"
//! POC_BENCH_POOLS = number of WAC pools to seed (1 for fan_in, 5000 for fan_out)
//! POC_BENCH_ISSUE_PCT = % envelopes that are wac_issue (rest are wac_receipt)
//!
//! For fan_in: workers all hit the same pool → FOR UPDATE serializes. Worst case
//! for cost-dispatch contention.
//! For fan_out: workers spread across distinct pools → minimum cross-worker
//! contention, but each batch still acquires N distinct FOR UPDATEs.

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
async fn wac_fan_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let pools_count = env_or::<usize>("POC_BENCH_POOLS", 1);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let issue_pct = env_or::<u32>("POC_BENCH_ISSUE_PCT", 0);
    let shape = std::env::var("POC_BENCH_SHAPE").unwrap_or_else(|_| "fan_in".to_string());
    // POC_BENCH_FUNCTION routes to a specific SQL fn — "post_batch" (default,
    // mutable WAC via mig 0006) or "post_batch_wac_shmem" (mig 0014, shmem
    // apply). Same envelope shape; the bench harness doesn't care which
    // applies.
    let bench_fn = std::env::var("POC_BENCH_FUNCTION").unwrap_or_else(|_| "post_batch".to_string());
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

    if bench_fn == "post_batch_wac_shmem" {
        // Shmem path: also flush the durable rollup + shmem hash so M6
        // lazy-load can't pick up stale (account_id, balance, qty) from
        // a prior run that happened to land on the same BIGSERIAL ID.
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

    // Seed external counter-party accounts (one debit_normal AP-style, one credit_normal COGS-style).
    let ap_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('wac-ap', 'USD', 'credit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("ap");
    let cogs_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('wac-cogs', 'USD', 'debit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("cogs");

    // Bulk-seed WAC pools. Pre-fund with a receipt so issues don't underflow.
    sqlx::query(
        "INSERT INTO accounts (code, currency, kind, balance, qty)
         SELECT 'wac-pool-' || lpad(g::text, 5, '0'), 'USD', 'inv_value_raw',
                1000000, 10000
         FROM generate_series(0, $1 - 1) g"
    )
    .bind(pools_count as i64)
    .execute(&pool)
    .await
    .expect("bulk seed pools");

    let pool_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE kind = 'inv_value_raw' ORDER BY id"
    ).fetch_all(&pool).await.expect("pool ids");
    assert_eq!(pool_ids.len(), pools_count, "pool seed count mismatch");

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    ).fetch_one(&pool).await.expect("dl_before");

    eprintln!(
        "WAC bench: shape={shape}, pools={pools_count}, workers={workers}, batch={batch_size}, issue_pct={issue_pct}%, duration={duration_secs}s"
    );

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(workers);
    let q_call = format!("SELECT * FROM {}($1)", bench_fn);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let p_pool = pool_ids.clone();
        let shape = shape.clone();
        let q_call = q_call.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            while Instant::now() < deadline {
                let mut envelopes = Vec::with_capacity(batch_size);
                for j in 0..batch_size {
                    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                    let r = rng as usize;
                    // Pick a pool: fan_in uses pool 0 always; fan_out picks random.
                    let pool_id = if shape == "fan_in" {
                        p_pool[0]
                    } else {
                        p_pool[r % p_pool.len()]
                    };
                    // Determine envelope kind by issue_pct
                    let is_issue = ((r / 13) % 100) < issue_pct as usize;
                    let qty = 1 + ((r / 11) % 10) as i64;
                    let unit_cost = 10 + ((r / 17) % 5) as i64;
                    if is_issue {
                        envelopes.push(json!({
                            "envelope_idx": j as i32,
                            "kind": "wac_issue",
                            "debit_account_id": cogs_id,
                            "credit_account_id": pool_id,
                            "qty": qty,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-05-12",
                        }));
                    } else {
                        envelopes.push(json!({
                            "envelope_idx": j as i32,
                            "kind": "wac_receipt",
                            "debit_account_id": pool_id,
                            "credit_account_id": ap_id,
                            "qty": qty,
                            "unit_cost": unit_cost,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-05-12",
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
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
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
    eprintln!("\n========= WAC fan bench (fn={bench_fn}, shape={shape}, pools={pools_count}, issue_pct={issue_pct}, batch={batch_size}) =========");
    eprintln!("workers: {workers}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
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
    eprintln!("==============================================================================\n");

    assert!(ok > 0, "no successful batches — bench broken");
}
