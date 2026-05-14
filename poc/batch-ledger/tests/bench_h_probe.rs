//! acct-h7c4 / zm69.h0 — Candidate H SERIALIZABLE-ceiling probe.
//!
//! See poc/ledger-extension/docs/fifo-arena-correctness-audit-2026-05-14.md
//! §Candidate H + §Phase 2 entry — H probe.
//!
//! ## What this measures
//!
//! The architectural ceiling of the append-only-layers + deferred-
//! constraint design under SERIALIZABLE isolation. PURE-SQL probe:
//! no ledger_extension involvement, no shmem, no bgworker. Tables
//! `cost_layers_h` + `cost_consumptions_h` per mig 0026.
//!
//! Three regimes (select with `POC_H_REGIME`):
//!
//! - `disjoint`  : N groups pre-seeded, random group per op. Best-
//!                 case ceiling (~no contention).
//! - `contended` : 1 thin layer_group, all writers race. SSI conflict
//!                 ceiling. Match R-MB6 shape.
//! - `realistic` : 50 groups, random group + random qty mix. Match
//!                 bench_fifo_rollback_inject shape for direct comp
//!                 to A2's 37.4 committed/s baseline.
//!
//! Isolation level selectable via `POC_H_ISOLATION = serializable |
//! read_committed`. RC under `contended` is the write-skew control:
//! some commits should pass with negative post-hoc effective qty.
//!
//! ## Operator usage
//!
//! ```bash
//! # Regime 1 — disjoint ceiling under SERIALIZABLE
//! POC_H_REGIME=disjoint POC_H_ISOLATION=serializable \
//!   POC_H_DURATION_SECS=60 POC_H_WORKERS=20 \
//!   cargo test --release --test bench_h_probe -- --ignored --nocapture
//!
//! # Regime 2 — pathological contended
//! POC_H_REGIME=contended POC_H_ISOLATION=serializable \
//!   cargo test --release --test bench_h_probe -- --ignored --nocapture
//!
//! # Regime 3 — realistic mix (LOAD-BEARING)
//! POC_H_REGIME=realistic POC_H_ISOLATION=serializable \
//!   cargo test --release --test bench_h_probe -- --ignored --nocapture
//!
//! # Control: regime 2 under RC (expect: some commits pass invariant-violating)
//! POC_H_REGIME=contended POC_H_ISOLATION=read_committed \
//!   cargo test --release --test bench_h_probe -- --ignored --nocapture
//! ```

use sqlx::Row;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[derive(Default)]
struct Stats {
    attempted: AtomicU64,
    committed: AtomicU64,
    retries_total: AtomicU64,
    aborted_final: AtomicU64,
    err_other: AtomicU64,
}

#[derive(Clone, Copy)]
enum Regime {
    Disjoint,
    Contended,
    Realistic,
}

impl Regime {
    fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "disjoint" => Regime::Disjoint,
            "contended" => Regime::Contended,
            "realistic" => Regime::Realistic,
            other => panic!("POC_H_REGIME must be disjoint|contended|realistic; got {other}"),
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Regime::Disjoint => "disjoint",
            Regime::Contended => "contended",
            Regime::Realistic => "realistic",
        }
    }
}

#[derive(Clone, Copy)]
enum Iso {
    Serializable,
    ReadCommitted,
}

impl Iso {
    fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "serializable" | "ssl" | "ssi" => Iso::Serializable,
            "read_committed" | "rc" => Iso::ReadCommitted,
            other => panic!("POC_H_ISOLATION must be serializable|read_committed; got {other}"),
        }
    }
    fn sql_set(&self) -> &'static str {
        match self {
            Iso::Serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            Iso::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Iso::Serializable => "serializable",
            Iso::ReadCommitted => "read_committed",
        }
    }
}

fn is_serialization_failure(e: &sqlx::Error) -> bool {
    // SQLSTATE 40001 (serialization_failure) raised by:
    //   - the deferred constraint trigger on over-consume
    //   - SERIALIZABLE SSI on dependency-cycle detection
    if let sqlx::Error::Database(db) = e {
        if let Some(code) = db.code() {
            return code == "40001";
        }
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn h_probe_bench() {
    let regime = Regime::parse(
        &std::env::var("POC_H_REGIME").unwrap_or_else(|_| "realistic".to_string()),
    );
    let iso = Iso::parse(
        &std::env::var("POC_H_ISOLATION").unwrap_or_else(|_| "serializable".to_string()),
    );
    let workers = env_or::<usize>("POC_H_WORKERS", 20);
    let duration_secs = env_or::<u64>("POC_H_DURATION_SECS", 60);
    let groups_count = match regime {
        Regime::Disjoint => env_or::<usize>("POC_H_GROUPS", 5_000),
        Regime::Realistic => env_or::<usize>("POC_H_GROUPS", 50),
        Regime::Contended => 1,
    };
    let group_qty = match regime {
        Regime::Disjoint => env_or::<i64>("POC_H_GROUP_QTY", 10_000_000),
        Regime::Realistic => env_or::<i64>("POC_H_GROUP_QTY", 1_000_000),
        Regime::Contended => env_or::<i64>("POC_H_GROUP_QTY", 1_000),
    };
    let max_retries = env_or::<u32>("POC_H_MAX_RETRIES", 10);

    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    // Clean slate.
    sqlx::query("TRUNCATE cost_consumptions_h, cost_layers_h RESTART IDENTITY")
        .execute(&pool)
        .await
        .expect("truncate");

    // Pre-seed layers: one per group. layer_group_id = 1..=groups_count.
    sqlx::query(
        "INSERT INTO cost_layers_h (layer_group_id, qty, unit_cost, source_kind)
         SELECT g, $1, 100, 'receipt'
           FROM generate_series(1, $2::int) g",
    )
    .bind(group_qty)
    .bind(groups_count as i32)
    .execute(&pool)
    .await
    .expect("seed layers");

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_before");

    eprintln!();
    eprintln!(
        "========= H probe (regime={}, iso={}, groups={}, group_qty={}, workers={}, duration={}s) =========",
        regime.name(),
        iso.name(),
        groups_count,
        group_qty,
        workers,
        duration_secs
    );

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            while Instant::now() < deadline {
                // Build envelope (pick group + qty).
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let r = rng as usize;
                let group_id = match regime {
                    Regime::Disjoint | Regime::Realistic => {
                        (1 + (r % groups_count)) as i64
                    }
                    Regime::Contended => 1,
                };
                let qty = match regime {
                    Regime::Disjoint | Regime::Contended => 1i64,
                    // Realistic: random qty [1, 5] to match
                    // bench_fifo_rollback_inject's mix shape (which uses [1, 10]
                    // for issues; smaller here keeps thin groups consumable).
                    Regime::Realistic => 1 + ((r / 13) % 5) as i64,
                };

                s.attempted.fetch_add(1, Ordering::Relaxed);

                let t0 = Instant::now();
                let mut retries = 0u32;
                let outcome: Result<(), sqlx::Error> = loop {
                    let mut tx = match p.begin().await {
                        Ok(t) => t,
                        Err(e) => break Err(e),
                    };
                    if let Err(e) = sqlx::query(iso.sql_set()).execute(&mut *tx).await {
                        break Err(e);
                    }
                    let r = sqlx::query(
                        "INSERT INTO cost_consumptions_h (layer_group_id, qty, unit_cost)
                         VALUES ($1, $2, 100)",
                    )
                    .bind(group_id)
                    .bind(qty)
                    .execute(&mut *tx)
                    .await;
                    let commit_res = match r {
                        Ok(_) => tx.commit().await,
                        Err(e) => {
                            // INSERT itself failed (rare in this shape;
                            // would be CHECK / constraint at INSERT, but
                            // the deferred trigger doesn't fire here).
                            let _ = tx.rollback().await;
                            Err(e)
                        }
                    };
                    match commit_res {
                        Ok(()) => break Ok(()),
                        Err(ref e) if is_serialization_failure(e) => {
                            retries += 1;
                            if retries >= max_retries {
                                break commit_res;
                            }
                            // Back off briefly (avoid tight retry storm).
                            tokio::time::sleep(Duration::from_micros(
                                (50 * retries) as u64,
                            ))
                            .await;
                            continue;
                        }
                        Err(_) => break commit_res,
                    }
                };
                samples_ns.push(t0.elapsed().as_nanos() as u64);
                s.retries_total.fetch_add(retries as u64, Ordering::Relaxed);
                match outcome {
                    Ok(()) => {
                        s.committed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(ref e) if is_serialization_failure(e) => {
                        s.aborted_final.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let prev = s.err_other.fetch_add(1, Ordering::Relaxed);
                        if prev < 3 {
                            eprintln!("worker {wi} err #{prev}: {e}");
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

    let attempted = stats.attempted.load(Ordering::Relaxed);
    let committed = stats.committed.load(Ordering::Relaxed);
    let retries_total = stats.retries_total.load(Ordering::Relaxed);
    let aborted_final = stats.aborted_final.load(Ordering::Relaxed);
    let err_other = stats.err_other.load(Ordering::Relaxed);

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

    let retries_per_commit = if committed == 0 {
        0.0
    } else {
        retries_total as f64 / committed as f64
    };
    let abort_rate_pct = if attempted == 0 {
        0.0
    } else {
        100.0 * aborted_final as f64 / attempted as f64
    };

    // Sanity audit: are any layer_groups in a negative-effective state
    // post-bench? Under SERIALIZABLE this should be empty; under RC
    // contended, this surfaces the write-skew control signal.
    let overconsume_rows = sqlx::query(
        "SELECT
           l.layer_group_id,
           COALESCE(SUM(l.qty), 0)::BIGINT AS total_layers,
           COALESCE((SELECT SUM(qty) FROM cost_consumptions_h c
                     WHERE c.layer_group_id = l.layer_group_id), 0)::BIGINT AS total_consumed,
           (COALESCE(SUM(l.qty), 0)
             - COALESCE((SELECT SUM(qty) FROM cost_consumptions_h c
                         WHERE c.layer_group_id = l.layer_group_id), 0))::BIGINT AS effective
         FROM cost_layers_h l
         GROUP BY l.layer_group_id
         HAVING COALESCE(SUM(l.qty), 0)
              - COALESCE((SELECT SUM(qty) FROM cost_consumptions_h c
                          WHERE c.layer_group_id = l.layer_group_id), 0) < 0
         ORDER BY effective ASC
         LIMIT 5",
    )
    .fetch_all(&pool)
    .await
    .expect("overconsume audit");
    let overconsume_groups = overconsume_rows.len();

    eprintln!("workers: {workers}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("attempted:           {attempted}");
    eprintln!("committed:           {committed}");
    eprintln!("aborted_final:       {aborted_final}  ({abort_rate_pct:.2}% of attempted)");
    eprintln!("err_other:           {err_other}");
    eprintln!("retries_total:       {retries_total}  ({retries_per_commit:.3}/commit)");
    eprintln!(
        "throughput: committed={:.1}/s, attempted={:.1}/s",
        committed as f64 / wall_secs,
        attempted as f64 / wall_secs
    );
    eprintln!("latency (us): p50={p50} p95={p95} p99={p99} p99.9={p999} max={max}");
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!(
        "post-bench overconsume_groups: {overconsume_groups} (>0 means invariant violated)"
    );
    for row in &overconsume_rows {
        let gid: i64 = row.get("layer_group_id");
        let total_layers: i64 = row.get("total_layers");
        let total_consumed: i64 = row.get("total_consumed");
        let effective: i64 = row.get("effective");
        eprintln!(
            "  group={gid} layers={total_layers} consumed={total_consumed} effective={effective}"
        );
    }
    eprintln!(
        "==================================================================================="
    );

    // Surface the load-bearing assertion only when iso=SERIALIZABLE.
    // RC + contended is intentionally allowed to leak overconsumes as
    // the write-skew control signal.
    if matches!(iso, Iso::Serializable) {
        assert_eq!(
            overconsume_groups, 0,
            "SERIALIZABLE invariant violated: {} groups have negative effective qty",
            overconsume_groups
        );
    }
}
