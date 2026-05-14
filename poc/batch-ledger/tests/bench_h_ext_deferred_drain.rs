//! acct-xida / zm69.h10 — Path 3 deferred drain throughput probe.
//!
//! Two-phase: (1) hot path runs N seconds, accumulating pending
//! consumption rows; (2) single-threaded drain processes them; we
//! report hot-path qty/s, drain consumption-rows/s, and pending
//! residue at quiescence.
//!
//! Path 3's value proposition is hot-path speed at the cost of
//! deferred attribution. Hot-path qty/s should approach plain H+ext
//! (mig 0029); drain throughput is path 3's true bottleneck for
//! end-to-end attributed work.

use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const DEFAULT_URL: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn deferred_drain_bench() {
    let workers = env_or::<usize>("POC_BENCH_WORKERS", 20);
    let groups_count = env_or::<usize>("POC_BENCH_GROUPS", 5000);
    let hot_secs = env_or::<u64>("POC_BENCH_HOT_SECS", 30);
    let batch_size = env_or::<usize>("POC_BENCH_BATCH_SIZE", 1000);
    let issue_pct = env_or::<u32>("POC_BENCH_ISSUE_PCT", 70);
    let group_qty = env_or::<i64>("POC_BENCH_GROUP_QTY", 1_000_000_000);

    let url = std::env::var("POC_DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections((workers as u32) + 4)
        .connect(&url)
        .await
        .expect("connect");

    sqlx::query(
        "TRUNCATE cost_layer_depletions_h_ext, cost_consumptions_h_ext, cost_layers_h_ext \
         RESTART IDENTITY",
    )
    .execute(&pool)
    .await
    .expect("truncate");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS ledger_extension")
        .execute(&pool)
        .await
        .expect("ext");
    sqlx::query("SELECT h_arena_reset()")
        .execute(&pool)
        .await
        .expect("h_arena_reset");

    // Seed durable + shmem.
    sqlx::query(
        "INSERT INTO cost_layers_h_ext (layer_group_id, qty, qty_remaining, unit_cost, source_kind)
         SELECT g, $1, $1, 100, 'receipt' FROM generate_series(1, $2::int) g",
    )
    .bind(group_qty)
    .bind(groups_count as i32)
    .execute(&pool)
    .await
    .expect("seed layers");

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

    // ── Phase 1: hot-path workers ──
    eprintln!();
    eprintln!(
        "===== Path 3 deferred drain bench: workers={workers} batch={batch_size} groups={groups_count} hot_secs={hot_secs} ====="
    );
    eprintln!();
    eprintln!("[Phase 1] hot-path workers running for {hot_secs}s...");

    let stats = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(hot_secs);
    let wall_start = Instant::now();

    let mut handles = Vec::new();
    for wi in 0..workers {
        let p = pool.clone();
        let s = stats.clone();
        handles.push(tokio::spawn(async move {
            let mut rng: u64 = (wi as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            let mut committed = 0u64;
            while Instant::now() < deadline {
                let mut envelopes = Vec::with_capacity(batch_size);
                for _ in 0..batch_size {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let r = rng as usize;
                    let group_id = (1 + (r % groups_count)) as i64;
                    let is_issue = ((r / 13) % 100) < issue_pct as usize;
                    if is_issue {
                        let qty = 1 + ((r / 11) % 5) as i64;
                        envelopes.push(json!({
                            "kind": "issue",
                            "layer_group_id": group_id,
                            "qty": qty,
                        }));
                    } else {
                        let qty = 1 + ((r / 11) % 100) as i64;
                        envelopes.push(json!({
                            "kind": "receipt",
                            "layer_group_id": group_id,
                            "qty": qty,
                            "unit_cost": 100,
                        }));
                    }
                }
                let envs_v = Value::Array(envelopes);
                let r = sqlx::query("SELECT post_batch_h_ext_deferred($1)")
                    .bind(&envs_v)
                    .execute(&p)
                    .await;
                if r.is_ok() {
                    committed += 1;
                }
            }
            committed
        }));
    }

    let mut total_committed = 0u64;
    for h in handles {
        total_committed += h.await.expect("worker");
    }
    let hot_secs_actual = wall_start.elapsed().as_secs_f64();
    stats.store(total_committed, Ordering::Relaxed);

    let pending_after_hot: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_consumptions_h_ext WHERE fifo_processed_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("pending");

    let total_consumed_qty_hot: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(qty), 0)::BIGINT FROM cost_consumptions_h_ext WHERE fifo_processed_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("pending qty");

    eprintln!(
        "[Phase 1 done] committed_batches={} ({:.1}/s), pending_consumptions={} ({:.0} qty), wall={:.2}s",
        total_committed,
        total_committed as f64 / hot_secs_actual,
        pending_after_hot,
        total_consumed_qty_hot,
        hot_secs_actual
    );

    // ── Phase 2: drain (single-writer) ──
    eprintln!("[Phase 2] draining pending consumption rows...");
    let drain_start = Instant::now();
    let mut drain_batches = 0u64;
    let mut total_drained = 0i64;
    loop {
        let drained: i64 = sqlx::query_scalar("SELECT drain_deferred_fifo(1000)::BIGINT")
            .fetch_one(&pool)
            .await
            .expect("drain");
        if drained == 0 {
            break;
        }
        drain_batches += 1;
        total_drained += drained;
    }
    let drain_secs = drain_start.elapsed().as_secs_f64();

    let pending_after_drain: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM cost_consumptions_h_ext WHERE fifo_processed_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("pending post drain");

    eprintln!(
        "[Phase 2 done] drained_consumptions={} via {} drain calls in {:.2}s ({:.0}/s)",
        total_drained,
        drain_batches,
        drain_secs,
        total_drained as f64 / drain_secs
    );
    eprintln!("[Phase 2 residue] pending_consumptions={}", pending_after_drain);

    // ── Correctness checks ──
    let overconsume: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM fifo_overconsume_check_h_ext()",
    )
    .fetch_one(&pool)
    .await
    .expect("overconsume");
    eprintln!("[Correctness] fifo_overconsume_check_h_ext rows = {} (expect 0)", overconsume);

    // ── Summary ──
    eprintln!();
    eprintln!("===== SUMMARY =====");
    eprintln!(
        "hot_path:     {:.0} batches/s = {} transfers/s (over {:.2}s)",
        total_committed as f64 / hot_secs_actual,
        (total_committed * batch_size as u64) as f64 / hot_secs_actual,
        hot_secs_actual
    );
    eprintln!(
        "drain:        {:.0} consumption-rows/s (over {:.2}s)",
        total_drained as f64 / drain_secs,
        drain_secs
    );
    eprintln!(
        "end-to-end:   {:.0} attributed-consumptions/s (drained / total_wall)",
        total_drained as f64 / (hot_secs_actual + drain_secs)
    );
    eprintln!("===================");

    assert_eq!(overconsume, 0, "FIFO over-attribution after drain");
    assert_eq!(pending_after_drain, 0, "drain did not reach quiescence");
}
