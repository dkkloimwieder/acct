//! P4-spec — specific/actual costing throughput bench.
//! Pure-SQL CTE chain. Each issue specifies a unit_id; cost looked up directly.

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
async fn p4spec_specific_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let pools = env_or::<usize>("POC_BENCH_POOLS", 20);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 30);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url).await.expect("connect");

    sqlx::query("TRUNCATE posting_lines, accounts, cost_layers, cost_layer_depletions, inventory_units RESTART IDENTITY CASCADE")
        .execute(&pool).await.expect("truncate");

    let ap_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('spec-ap', 'USD', 'credit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("ap");
    let cogs_id: i64 = sqlx::query_scalar(
        "INSERT INTO accounts (code, currency, kind) VALUES ('spec-cogs', 'USD', 'debit_normal') RETURNING id"
    ).fetch_one(&pool).await.expect("cogs");
    let mut pool_ids: Vec<i64> = Vec::with_capacity(pools);
    for i in 0..pools {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO accounts (code, currency, kind) VALUES ($1, 'USD', 'inv_value_raw') RETURNING id"
        ).bind(format!("spec-pool-{i:02}")).fetch_one(&pool).await.expect("pool");
        pool_ids.push(id);
    }

    // Pre-seed via direct INSERT (faster than going through post_batch_specific).
    // Need enough units that 90%-issue workload at ~50K tps × 30s won't drain a pool.
    let units_per_pool = 100_000_usize;
    eprintln!("Pre-seeding {} units per pool × {} pools = {} total units (direct INSERT)...",
        units_per_pool, pool_ids.len(), units_per_pool * pool_ids.len());
    for pid in &pool_ids {
        sqlx::query(
            "INSERT INTO inventory_units (pool_account_id, unit_cost, status)
             SELECT $1, 100, 'available' FROM generate_series(1, $2)"
        ).bind(pid).bind(units_per_pool as i64).execute(&pool).await.expect("seed");
        // Update accounts.balance + qty to match.
        sqlx::query(
            "UPDATE accounts SET balance = $1, qty = $2 WHERE id = $3"
        ).bind((units_per_pool as i64) * 100).bind(units_per_pool as i64).bind(pid)
            .execute(&pool).await.expect("acct seed");
    }
    eprintln!("Seed done.");

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'"
    ).fetch_one(&pool).await.expect("dl");

    eprintln!("P4-spec bench: workers={workers}, pools={pools}, duration={duration_secs}s, batch_size={batch_size}");

    // Fetch available unit_id pool per pool_id for the workers to draw from.
    // We compute non-overlapping unit_id ranges per pool for randomized issue.
    let pool_unit_ranges: Vec<(i64, i64, i64)> = {
        let mut v = Vec::new();
        for pid in &pool_ids {
            let (lo, hi): (i64, i64) = sqlx::query_as(
                "SELECT MIN(id), MAX(id) FROM inventory_units WHERE pool_account_id = $1 AND status = 'available'"
            ).bind(pid).fetch_one(&pool).await.expect("range");
            v.push((*pid, lo, hi));
        }
        v
    };

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let ranges = pool_unit_ranges.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            // Each worker gets a private subset of unit_ids by partitioning each pool's range.
            // That way different workers don't try to consume the same unit and conflict
            // on the unit's UNIQUE consumed transition.
            let worker_count = 20_i64;
            let wi64 = wi as i64;
            while Instant::now() < deadline {
                let mut envelopes = Vec::with_capacity(batch_size);
                for j in 0..batch_size {
                    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                    let r = rng as usize;
                    let (pid, lo, hi) = ranges[r % ranges.len()];
                    let is_receipt = (r >> 8) % 10 < 1;  // 10% receipts, 90% issues
                    if is_receipt {
                        envelopes.push(json!({
                            "envelope_idx": j as i32, "kind": "specific_receipt",
                            "debit_account_id": pid, "credit_account_id": ap_id,
                            "unit_cost": 100 + ((r >> 16) % 50) as i64,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-05-12",
                        }));
                    } else {
                        // Partition unit space across workers. Worker wi consumes units
                        // with (id % worker_count) == wi.
                        let range_size = hi - lo + 1;
                        let mut unit_id = lo + ((rng as i64).rem_euclid(range_size));
                        // adjust to worker's stripe
                        let diff = ((wi64 - unit_id) % worker_count + worker_count) % worker_count;
                        unit_id += diff;
                        if unit_id > hi { unit_id -= worker_count; }
                        envelopes.push(json!({
                            "envelope_idx": j as i32, "kind": "specific_issue",
                            "debit_account_id": cogs_id, "credit_account_id": pid,
                            "unit_id": unit_id,
                            "idempotency_key": Uuid::new_v4().to_string(),
                            "business_date": "2026-05-12",
                        }));
                    }
                }
                s.batches_attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = sqlx::query("SELECT * FROM post_batch_specific($1)")
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

    eprintln!("\n========= P4-spec specific bench (batch_size={batch_size}) =========");
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
    eprintln!("=================================================================\n");
    assert!(ok > 0);
}
