//! P2 — pgledger-equivalent baseline bench.
//!
//! Goal: hit ~10K transfers/sec on the dev container to calibrate our
//! environment against pgledger's reported number (M3 MacBook + PG 17.5 + 50
//! accounts × 20 workers, https://www.pgrs.net/2025/05/16/pgledger-in-postgresql-is-fast/).
//!
//! Ignored by default. Run via the bench script:
//!     ./bench/run-p2.sh
//!
//! Or directly:
//!     cargo test --manifest-path poc/batch-ledger/Cargo.toml --release \
//!       --test bench_p2_pgledger -- --ignored --nocapture p2_pgledger_baseline_bench

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
    attempted: AtomicU64,
    ok: AtomicU64,
    err: AtomicU64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn p2_pgledger_baseline_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let accounts = env_or::<usize>("POC_BENCH_ACCOUNTS", 50);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);

    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let sync_commit_off = env_or::<u8>("POC_BENCH_SYNC_COMMIT_OFF", 0) != 0;

    let mut pool_opts = sqlx::postgres::PgPoolOptions::new().max_connections((workers as u32) + 4);
    if sync_commit_off {
        pool_opts = pool_opts.after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit = off")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        });
    }
    let pool = pool_opts.connect(&url).await.expect("connect");

    // Fresh state each run; eliminates buffer-pool warmth bleed across replicates.
    sqlx::query("TRUNCATE posting_lines, accounts RESTART IDENTITY CASCADE")
        .execute(&pool)
        .await
        .expect("truncate");

    // Seed `accounts` accounts: even indices debit_normal, odd credit_normal.
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
    // Separate debit/credit pools.
    let debit_ids: Vec<i64> = acct_ids.iter().step_by(2).copied().collect();
    let credit_ids: Vec<i64> = acct_ids.iter().skip(1).step_by(2).copied().collect();

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_before");

    eprintln!(
        "P2 bench starting: workers={workers}, accounts={accounts}, duration={duration_secs}s, sync_commit_off={sync_commit_off}"
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
            let mut samples_ns: Vec<u64> = Vec::with_capacity(200_000);
            // xorshift64 seeded by worker index.
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            while Instant::now() < deadline {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let r = rng as usize;
                let debit = d_pool[r % d_pool.len()];
                let credit = c_pool[(r / 7) % c_pool.len()];
                let amount = 1 + ((r / 11) % 1_000) as i64;
                let idem = Uuid::new_v4();

                s.attempted.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                let res = sqlx::query_scalar::<_, i64>("SELECT post_transfer($1, $2, $3, $4)")
                    .bind(debit)
                    .bind(credit)
                    .bind(amount)
                    .bind(idem)
                    .fetch_one(&p)
                    .await;
                samples_ns.push(t0.elapsed().as_nanos() as u64);
                match res {
                    Ok(_) => {
                        s.ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        s.err.fetch_add(1, Ordering::Relaxed);
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

    let attempted = stats.attempted.load(Ordering::Relaxed);
    let ok = stats.ok.load(Ordering::Relaxed);
    let err = stats.err.load(Ordering::Relaxed);

    let pct = |q: f64| -> u64 {
        if all_samples.is_empty() {
            0
        } else {
            let idx = ((all_samples.len() as f64 - 1.0) * q).round() as usize;
            all_samples[idx] / 1_000 // ns -> us
        }
    };
    let p50 = pct(0.50);
    let p95 = pct(0.95);
    let p99 = pct(0.99);
    let p999 = pct(0.999);
    let max = all_samples.last().copied().unwrap_or(0) / 1_000;

    eprintln!("\n=========== P2 pgledger baseline ===========");
    eprintln!("workers: {workers}, accounts: {accounts}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("attempted: {attempted}");
    eprintln!("ok:        {ok}");
    eprintln!("err:       {err}");
    eprintln!(
        "throughput: attempted={:.1}/s, ok={:.1}/s",
        attempted as f64 / wall_secs,
        ok as f64 / wall_secs
    );
    eprintln!("latency (us): p50={p50} p95={p95} p99={p99} p99.9={p999} max={max}");
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!("============================================\n");

    assert!(ok > 0, "no successful transfers — environment broken");
}
