//! acct-fhq7 — FIFO arena throughput-under-rollback bench.
//!
//! ## What this measures
//!
//! Same workload shape as `bench_fifo_fan.rs` (FIFO maximal F variant by
//! default, 30%/70% receipt/issue mix, fan-in / fan-out pool shape) but
//! each batch is wrapped in an explicit `BEGIN ... <apply> ... COMMIT
//! | ROLLBACK` and the COMMIT/ROLLBACK decision is randomized per batch
//! at `POC_BENCH_ROLLBACK_PCT`.
//!
//! Goal: characterize A2's shadow approach throughput as rollback rate
//! varies across 0% / 1% / 5% / 10%. This is the canonical perf
//! comparison axis for any acct-fhq7 reconciliation-architecture
//! rewrite (Approach B/C/E/F) — the rewrite must stay within 10% of A2
//! at 5% rollback rate per acct-fhq7's acceptance criteria.
//!
//! ## Why not sweep higher rollback rates
//!
//! 25/50% rollback is not realistic ERP workload shape (rollbacks are
//! typically <10% even under contention). The 0-10% window is the
//! relevant signal range. Higher rates surface as a stress signal at
//! 10% if the architecture choice has any rollback-tax cliff.
//!
//! ## Operator usage
//!
//! ```bash
//! for pct in 0 1 5 10; do
//!   POC_BENCH_ROLLBACK_PCT=$pct \
//!   POC_BENCH_FUNCTION=post_batch_fifo_maximal_F \
//!   POC_BENCH_SHAPE=fan_out \
//!   POC_BENCH_DURATION_SECS=60 \
//!   cargo test --release --test bench_fifo_rollback_inject -- \
//!     --ignored --nocapture
//! done
//! ```
//!
//! Default settings match `bench_fifo_fan`: 20 workers, batch=1000,
//! 70% issue, 60s duration. 5 layers × 1M qty per pool pre-seeded so
//! issues never exhaust even at 0% rollback.

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
    batches_committed: AtomicU64,
    batches_rolled_back: AtomicU64,
    batches_err: AtomicU64,
    transfers_committed: AtomicU64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn fifo_rollback_inject_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let pools_count = env_or::<usize>("POC_BENCH_POOLS", 1);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let issue_pct = env_or::<u32>("POC_BENCH_ISSUE_PCT", 70);
    let layer_qty = env_or::<i64>("POC_BENCH_LAYER_QTY", 1_000_000);
    let rollback_pct = env_or::<u32>("POC_BENCH_ROLLBACK_PCT", 0);
    assert!(
        rollback_pct <= 100,
        "POC_BENCH_ROLLBACK_PCT must be in [0, 100]; got {rollback_pct}"
    );
    let shape = std::env::var("POC_BENCH_SHAPE").unwrap_or_else(|_| "fan_in".to_string());
    let bench_fn = std::env::var("POC_BENCH_FUNCTION")
        .unwrap_or_else(|_| "post_batch_fifo_maximal_F".to_string());
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

    let bench_fn_lc = bench_fn.to_lowercase();
    let is_f_shmem = bench_fn_lc.ends_with("_maximal_f");
    if bench_fn.ends_with("_shmem")
        || bench_fn.ends_with("_maximal")
        || bench_fn.ends_with("_inline")
        || is_f_shmem
    {
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
        if is_f_shmem {
            sqlx::query("SELECT fifo_arena_reset()")
                .execute(&pool)
                .await
                .expect("fifo arena reset");
        }
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

    // Pre-seed 5 × layer_qty per pool — enough for any 60s bench at
    // workers × batch=1000 × 70% issue × max-take=10.
    for layer_no in 0..5 {
        let mut envs = Vec::with_capacity(pool_ids.len());
        for (i, pid) in pool_ids.iter().enumerate() {
            envs.push(json!({
                "envelope_idx": i as i32,
                "kind": "fifo_receipt",
                "debit_account_id": pid,
                "credit_account_id": ap_id,
                "qty": layer_qty,
                "unit_cost": 100_i64 + layer_no * 10,
                "idempotency_key": Uuid::new_v4().to_string(),
                "business_date": format!("2026-05-{:02}", 1 + layer_no),
            }));
        }
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
        "FIFO rollback-inject bench: shape={shape}, pools={pools_count}, workers={workers}, \
         batch={batch_size}, issue_pct={issue_pct}%, rollback_pct={rollback_pct}%, \
         duration={duration_secs}s, fn={bench_fn}"
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

                // Per-batch rollback decision. Drive from rng so each
                // worker's stream is independent + reproducible-ish.
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let do_rollback = (rng % 100) < rollback_pct as u64;

                s.batches_attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let tx_outcome: Result<bool, sqlx::Error> = async {
                    let mut tx = p.begin().await?;
                    sqlx::query(&q_call)
                        .bind(&envelopes_value)
                        .execute(&mut *tx)
                        .await?;
                    if do_rollback {
                        tx.rollback().await?;
                        Ok(false)
                    } else {
                        tx.commit().await?;
                        Ok(true)
                    }
                }
                .await;
                samples_ns.push(t0.elapsed().as_nanos() as u64);
                match tx_outcome {
                    Ok(true) => {
                        s.batches_committed.fetch_add(1, Ordering::Relaxed);
                        s.transfers_committed
                            .fetch_add(batch_size as u64, Ordering::Relaxed);
                    }
                    Ok(false) => {
                        s.batches_rolled_back.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let prev = s.batches_err.fetch_add(1, Ordering::Relaxed);
                        if prev < 2 {
                            eprintln!("worker {wi} batch err #{prev}: {e}");
                        }
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
    let committed = stats.batches_committed.load(Ordering::Relaxed);
    let rolled_back = stats.batches_rolled_back.load(Ordering::Relaxed);
    let err = stats.batches_err.load(Ordering::Relaxed);
    let transfers = stats.transfers_committed.load(Ordering::Relaxed);

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

    let rollback_observed_pct = if attempted == 0 {
        0.0
    } else {
        100.0 * (rolled_back as f64) / (attempted as f64)
    };

    eprintln!();
    eprintln!(
        "========= FIFO rollback-inject (fn={}, shape={}, pools={}, issue_pct={}, rollback_pct={}, batch={}) =========",
        bench_fn, shape, pools_count, issue_pct, rollback_pct, batch_size
    );
    eprintln!("workers: {workers}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("batches_attempted:   {attempted}");
    eprintln!("batches_committed:   {committed}");
    eprintln!(
        "batches_rolled_back: {rolled_back}  (observed {:.2}% of attempted; target {}%)",
        rollback_observed_pct, rollback_pct
    );
    eprintln!("batches_err:         {err}");
    eprintln!("transfers_committed: {transfers}");
    eprintln!(
        "throughput: committed={:.1}/s, attempted={:.1}/s, transfers={:.1}/s",
        committed as f64 / wall_secs,
        attempted as f64 / wall_secs,
        transfers as f64 / wall_secs
    );
    eprintln!("batch-latency (us): p50={p50} p95={p95} p99={p99} p99.9={p999} max={max}");
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!(
        "=================================================================================="
    );
}
