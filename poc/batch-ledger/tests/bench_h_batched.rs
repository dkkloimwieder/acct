//! acct-zm69 / zm69.h6 — Batched Candidate H throughput bench.
//!
//! See poc/ledger-extension/docs/fifo-arena-correctness-audit-2026-05-14.md
//! §Candidate H and bench_h_probe.rs.
//!
//! ## What this measures
//!
//! Production-shape (batch=1000) ceiling of the Candidate H design.
//! Single-row probe (bench_h_probe) hit 1,239 committed/s realistic;
//! batched shape should land near A2's 37.4 committed/s of 1000-envelope
//! batches (= ~37K transfers/s).
//!
//! Two function variants compared:
//!
//! - `post_batch_h` (mig 0027): set-based INSERT into cost_layers_h +
//!   cost_consumptions_h. The mig 0026 DEFERRABLE FOR-EACH-ROW trigger
//!   fires N times at COMMIT (one per issue row).
//!
//! - `post_batch_h_app` (mig 0028): set-based INSERT into _app tables
//!   (no trigger) + explicit set-based batch check at end of plpgsql
//!   wrapper. O(distinct touched groups) SUM pairs instead of O(rows).
//!   Characterizes the floor cost without per-row trigger fires.
//!
//! ## Open question this answers
//!
//! "Does the FOR-EACH-ROW deferred trigger fire 1000× per commit kill
//! us at batch=1000?" — if `post_batch_h_app` ≫ `post_batch_h` then yes
//! and we need a different invariant-enforcement shape; if they're
//! close then the deferred-trigger design is fine as-is.
//!
//! ## Operator usage
//!
//! ```bash
//! # FOR-EACH-ROW deferred trigger variant
//! POC_BENCH_FUNCTION=post_batch_h \
//!   POC_BENCH_BATCH_SIZE=1000 \
//!   POC_BENCH_DURATION_SECS=60 \
//!   POC_BENCH_WORKERS=20 \
//!   cargo test --release --test bench_h_batched -- --ignored --nocapture
//!
//! # App-level batch check variant
//! POC_BENCH_FUNCTION=post_batch_h_app \
//!   POC_BENCH_BATCH_SIZE=1000 \
//!   cargo test --release --test bench_h_batched -- --ignored --nocapture
//!
//! # Batch size sweep
//! for bs in 100 1000 10000; do
//!   POC_BENCH_FUNCTION=post_batch_h POC_BENCH_BATCH_SIZE=$bs \
//!     cargo test --release --test bench_h_batched -- --ignored --nocapture
//! done
//! ```

use serde_json::{Value, json};
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
    batches_attempted: AtomicU64,
    batches_committed: AtomicU64,
    batches_aborted_final: AtomicU64,
    batches_err: AtomicU64,
    retries_total: AtomicU64,
    transfers_committed: AtomicU64,
}

fn is_serialization_failure(e: &sqlx::Error) -> bool {
    // SQLSTATE 40001 raised by:
    //   - the deferred constraint trigger (post_batch_h) on over-consume
    //   - the explicit batch check (post_batch_h_app) on over-consume
    //   - SERIALIZABLE SSI dependency-cycle detection
    if let sqlx::Error::Database(db) = e {
        if let Some(code) = db.code() {
            return code == "40001";
        }
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn h_batched_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let groups_count = env_or::<usize>("POC_BENCH_GROUPS", 50);
    let duration_secs = env_or::<u64>("POC_BENCH_DURATION_SECS", 60);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let issue_pct = env_or::<u32>("POC_BENCH_ISSUE_PCT", 70);
    let group_qty = env_or::<i64>("POC_BENCH_GROUP_QTY", 1_000_000_000);
    let max_retries = env_or::<u32>("POC_BENCH_MAX_RETRIES", 10);
    let bench_fn = std::env::var("POC_BENCH_FUNCTION")
        .unwrap_or_else(|_| "post_batch_h".to_string());
    // Isolation: pure-SQL H needs SERIALIZABLE (the trigger-based
    // invariant only catches concurrent writers under SSI). The ext
    // variants serialize via shmem CAS (post_batch_h_ext) or row-level
    // FOR UPDATE (post_batch_h_ext_fifo_walk) — RC is correct.
    let default_iso = match bench_fn.to_lowercase().as_str() {
        "post_batch_h_ext"
        | "post_batch_h_ext_fifo_walk"
        | "post_batch_h_ext_layer_shmem"
        | "post_batch_h_ext_deferred"
        | "h_apply_batch_fifo" => "read_committed",
        _ => "serializable",
    };
    let iso_str =
        std::env::var("POC_BENCH_ISOLATION").unwrap_or_else(|_| default_iso.to_string());
    let iso_sql = match iso_str.to_lowercase().as_str() {
        "serializable" | "ssl" | "ssi" => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "read_committed" | "rc" => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        other => panic!("POC_BENCH_ISOLATION must be serializable|read_committed; got {other}"),
    };

    let bench_fn_lc = bench_fn.to_lowercase();
    let (layers_table, cons_table, uses_extension) = match bench_fn_lc.as_str() {
        "post_batch_h" => ("cost_layers_h", "cost_consumptions_h", false),
        "post_batch_h_app" => ("cost_layers_h_app", "cost_consumptions_h_app", false),
        "post_batch_h_ext"
        | "post_batch_h_ext_fifo_walk"
        | "post_batch_h_ext_layer_shmem"
        | "post_batch_h_ext_deferred"
        | "h_apply_batch_fifo" => {
            ("cost_layers_h_ext", "cost_consumptions_h_ext", true)
        }
        other => panic!(
            "POC_BENCH_FUNCTION unsupported: {other}. Valid: post_batch_h | post_batch_h_app | post_batch_h_ext | post_batch_h_ext_fifo_walk | post_batch_h_ext_layer_shmem | post_batch_h_ext_deferred | h_apply_batch_fifo"
        ),
    };

    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    // Clean slate. For the ext path, also truncate the depletions
    // table (only used by post_batch_h_ext_fifo_walk; harmless for the
    // qty-only post_batch_h_ext path).
    let truncate_sql = if uses_extension {
        format!(
            "TRUNCATE cost_layer_depletions_h_ext, {cons_table}, {layers_table} RESTART IDENTITY"
        )
    } else {
        format!("TRUNCATE {cons_table}, {layers_table} RESTART IDENTITY")
    };
    sqlx::query(&truncate_sql)
        .execute(&pool)
        .await
        .expect("truncate");

    if uses_extension {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
            .execute(&pool)
            .await
            .expect("create ext");
        sqlx::query("SELECT h_arena_reset()")
            .execute(&pool)
            .await
            .expect("h_arena_reset");
        // Reset per-layer arena too (used by path 2). Harmless for
        // other ext variants — they don't touch h_layer_arena.
        // Tolerate older builds where h_layer_arena_reset isn't yet
        // installed; ignore the function-not-found error in that case.
        let _ = sqlx::query("SELECT h_layer_arena_reset()")
            .execute(&pool)
            .await;
    }

    // Pre-seed N layer_groups so issues never exhaust at realistic mix
    // (workers × batch_size × issue_pct × max_qty per group is far below
    // group_qty per group). The ext layers table requires qty_remaining
    // populated (mig 0030); seed it equal to qty.
    let seed_sql = if uses_extension {
        format!(
            "INSERT INTO {layers_table} (layer_group_id, qty, qty_remaining, unit_cost, source_kind)
             SELECT g, $1, $1, 100, 'receipt'
               FROM generate_series(1, $2::int) g"
        )
    } else {
        format!(
            "INSERT INTO {layers_table} (layer_group_id, qty, unit_cost, source_kind)
             SELECT g, $1, 100, 'receipt'
               FROM generate_series(1, $2::int) g"
        )
    };
    sqlx::query(&seed_sql)
        .bind(group_qty)
        .bind(groups_count as i32)
        .execute(&pool)
        .await
        .expect("seed layers");

    if uses_extension {
        // Mirror the durable seed in the h_arena shmem so consumption
        // commits don't trip the invariant check. One h_apply_delta per
        // group, all inside a single txn so the PRE_COMMIT applies the
        // seed atomically.
        let seed_sql = format!(
            "DO $$
             DECLARE g INT;
             BEGIN
               FOR g IN 1..{}::int LOOP
                 PERFORM h_apply_delta(g::BIGINT, {}::BIGINT);
               END LOOP;
             END$$",
            groups_count, group_qty
        );
        sqlx::query(&seed_sql)
            .execute(&pool)
            .await
            .expect("h_arena seed");

        // For paths 2 (layer_shmem) and 4 (h_apply_batch_fifo), also
        // seed h_layer_arena cells so FIFO walks find a non-zero residual
        // on pre-seeded layers. Tolerate older extension builds without
        // h_layer_create.
        let fn_lc = bench_fn.to_lowercase();
        if fn_lc == "post_batch_h_ext_layer_shmem" || fn_lc == "h_apply_batch_fifo" {
            let layer_seed_sql = format!(
                "DO $$
                 DECLARE r RECORD;
                 BEGIN
                   FOR r IN SELECT layer_id FROM cost_layers_h_ext ORDER BY layer_id LOOP
                     PERFORM h_layer_create(r.layer_id, {}::BIGINT);
                   END LOOP;
                 END$$",
                group_qty
            );
            sqlx::query(&layer_seed_sql)
                .execute(&pool)
                .await
                .expect("h_layer_arena seed");
        }
    }

    let dl_before: i64 = sqlx::query_scalar(
        "SELECT deadlocks::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc'",
    )
    .fetch_one(&pool)
    .await
    .expect("dl_before");

    eprintln!();
    eprintln!(
        "========= H batched (fn={}, iso={}, batch={}, groups={}, issue_pct={}%, workers={}, duration={}s) =========",
        bench_fn, iso_str, batch_size, groups_count, issue_pct, workers, duration_secs
    );

    let stats = Arc::new(Stats::default());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let wall_start = Instant::now();

    let q_call = format!("SELECT {}($1)", bench_fn);
    let mut handles = Vec::with_capacity(workers);
    for wi in 0..workers {
        let s = stats.clone();
        let p = pool.clone();
        let q_call = q_call.clone();
        handles.push(tokio::spawn(async move {
            let mut samples_ns: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            while Instant::now() < deadline {
                // Build envelopes for this batch.
                let mut envelopes = Vec::with_capacity(batch_size);
                for _ in 0..batch_size {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let r = rng as usize;
                    let group_id = (1 + (r % groups_count)) as i64;
                    let is_issue = ((r / 13) % 100) < issue_pct as usize;
                    if is_issue {
                        // Issue qty [1, 5] — match bench_h_probe Realistic shape.
                        let qty = 1 + ((r / 11) % 5) as i64;
                        envelopes.push(json!({
                            "kind": "issue",
                            "layer_group_id": group_id,
                            "qty": qty,
                            "unit_cost": 100,
                        }));
                    } else {
                        // Receipt qty [1, 100], unit_cost ~[80, 119].
                        let qty = 1 + ((r / 11) % 100) as i64;
                        let unit_cost = 80 + ((r / 17) % 40) as i64;
                        envelopes.push(json!({
                            "kind": "receipt",
                            "layer_group_id": group_id,
                            "qty": qty,
                            "unit_cost": unit_cost,
                        }));
                    }
                }
                let envelopes_value = Value::Array(envelopes);

                s.batches_attempted.fetch_add(1, Ordering::Relaxed);

                let t0 = Instant::now();
                let mut retries = 0u32;
                let outcome: Result<(), sqlx::Error> = loop {
                    let mut tx = match p.begin().await {
                        Ok(t) => t,
                        Err(e) => break Err(e),
                    };
                    if let Err(e) =
                        sqlx::query(iso_sql)
                            .execute(&mut *tx)
                            .await
                    {
                        let _ = tx.rollback().await;
                        break Err(e);
                    }
                    let r = sqlx::query(&q_call)
                        .bind(&envelopes_value)
                        .execute(&mut *tx)
                        .await;
                    let commit_res = match r {
                        Ok(_) => tx.commit().await,
                        Err(e) => {
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
                        s.batches_committed.fetch_add(1, Ordering::Relaxed);
                        s.transfers_committed
                            .fetch_add(batch_size as u64, Ordering::Relaxed);
                    }
                    Err(ref e) if is_serialization_failure(e) => {
                        s.batches_aborted_final.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let prev = s.batches_err.fetch_add(1, Ordering::Relaxed);
                        if prev < 3 {
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
    let aborted_final = stats.batches_aborted_final.load(Ordering::Relaxed);
    let err = stats.batches_err.load(Ordering::Relaxed);
    let retries_total = stats.retries_total.load(Ordering::Relaxed);
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

    // Sanity audit: under SERIALIZABLE no group should end with negative
    // effective qty.
    let overconsume_groups: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*)::BIGINT FROM (
           SELECT l.layer_group_id
             FROM {layers_table} l
            GROUP BY l.layer_group_id
           HAVING COALESCE(SUM(l.qty), 0)
                - COALESCE((SELECT SUM(qty) FROM {cons_table} c
                             WHERE c.layer_group_id = l.layer_group_id), 0) < 0
         ) sub"
    ))
    .fetch_one(&pool)
    .await
    .expect("overconsume audit");

    eprintln!("workers: {workers}, duration: {duration_secs}s (wall: {wall_secs:.2}s)");
    eprintln!("batches_attempted:    {attempted}");
    eprintln!("batches_committed:    {committed}");
    eprintln!(
        "batches_aborted_final: {aborted_final}  ({abort_rate_pct:.2}% of attempted)"
    );
    eprintln!("batches_err:          {err}");
    eprintln!(
        "retries_total:        {retries_total}  ({retries_per_commit:.3}/commit)"
    );
    eprintln!("transfers_committed:  {transfers}");
    eprintln!(
        "throughput: committed-batches={:.1}/s, transfers={:.1}/s",
        committed as f64 / wall_secs,
        transfers as f64 / wall_secs
    );
    eprintln!("batch-latency (us): p50={p50} p95={p95} p99={p99} p99.9={p999} max={max}");
    eprintln!("deadlocks delta: {}", dl_after - dl_before);
    eprintln!(
        "post-bench overconsume_groups: {overconsume_groups} (>0 means invariant violated)"
    );
    eprintln!(
        "==================================================================================="
    );

    assert_eq!(
        overconsume_groups, 0,
        "SERIALIZABLE invariant violated: {} groups have negative effective qty",
        overconsume_groups
    );
}
