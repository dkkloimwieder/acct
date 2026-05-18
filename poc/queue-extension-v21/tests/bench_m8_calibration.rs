//! M8.1 (acct-toj6) — pre-bake-off calibration per spec §5.1.
//!
//! Two measurements:
//!
//!   1. **fsync latency** — time per fsync, used to set
//!      `poc_v21.committer_lease_ms = max(100, 10 × fsync_p99_ms)`.
//!      We don't have `pg_test_fsync` in-tree, so the probe forces
//!      a real WAL flush via `synchronous_commit=on` + a 1-byte
//!      INSERT into a dedicated throwaway table. Commit returns
//!      only after the WAL fsync; p50/p99/p99.9 over 100 commits
//!      bound the storage's fsync latency.
//!
//!   2. **cold-start vs steady-state** — run S1 fan_out_simple
//!      twice: first iteration on empty pool_locks (every envelope
//!      lazily creates its lock row); second on warm pool_locks
//!      (UPSERT no-ops). Throughput delta documents the warm-up
//!      cost and bounds what the bake-off harness should discard.
//!
//! Output: `bench/calibration-m81.md`.
//!
//! Run via:
//!   cargo test --release --test bench_m8_calibration \
//!     --features pg18 --no-default-features \
//!     -- --ignored --nocapture --test-threads=1

#![cfg(test)]

#[path = "common/m8_runner.rs"]
mod m8_runner;

use m8_runner::{
    Shape, WorkloadConfig, connect_pool, percentile, pre_seed_shape, reset_state, run_shape,
};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};

const FSYNC_PROBE_COUNT: usize = 100;
const CALIBRATION_DURATION_SECS: u64 = 10;
const CALIBRATION_N: usize = 4;

async fn probe_fsync_latency(pool: &PgPool) -> Vec<u64> {
    // Throwaway table — TEMP would do but TEMP commit fsync is
    // sometimes skipped, so use a regular table that round-trips
    // WAL on every commit. DROP after.
    sqlx::query("CREATE TABLE IF NOT EXISTS poc_v21_fsync_probe (id BIGSERIAL PRIMARY KEY, t BIGINT)")
        .execute(pool)
        .await
        .expect("create fsync probe table");
    sqlx::query("TRUNCATE poc_v21_fsync_probe")
        .execute(pool)
        .await
        .expect("truncate fsync probe");
    // Force synchronous_commit=on cluster-wide for the probe window;
    // restore after. ALTER SYSTEM + pg_reload_conf is the only way
    // a non-superuser-locked session can flip the cluster default,
    // but tests run as user `acct` which OWNS the database and has
    // SUPERUSER on the dev cluster — verified in scripts/dev-up.sh.
    let prior_sc: (String,) = sqlx::query_as("SHOW synchronous_commit")
        .fetch_one(pool)
        .await
        .unwrap_or_else(|_| ("on".to_string(),));
    sqlx::query("SET synchronous_commit = 'on'")
        .execute(pool)
        .await
        .expect("set synchronous_commit=on");

    let mut latencies: Vec<u64> = Vec::with_capacity(FSYNC_PROBE_COUNT);
    for i in 0..FSYNC_PROBE_COUNT {
        let t0 = Instant::now();
        sqlx::query("INSERT INTO poc_v21_fsync_probe (t) VALUES ($1)")
            .bind(i as i64)
            .execute(pool)
            .await
            .expect("fsync probe insert");
        let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
        latencies.push(dt_us);
    }

    // Restore synchronous_commit for the session.
    let restore_sql = format!("SET synchronous_commit = '{}'", prior_sc.0);
    let _ = sqlx::query(&restore_sql).execute(pool).await;

    sqlx::query("DROP TABLE poc_v21_fsync_probe")
        .execute(pool)
        .await
        .ok();

    latencies.sort_unstable();
    latencies
}

async fn run_cold_warm_pair(n: usize, duration: u64) -> (f64, f64) {
    let pool = Arc::new(connect_pool((n as u32) + 4).await);
    reset_state(&pool).await.expect("reset_state cold");
    pre_seed_shape(&pool, Shape::S1FanOutSimple, n)
        .await
        .expect("pre_seed cold");
    // Cold: no pool_locks rows exist. First-touch UPSERT creates them.
    let cfg = WorkloadConfig {
        shape: Shape::S1FanOutSimple,
        n_backends: n,
        duration_secs: duration,
        s9_margin: 0,
    };
    let cold = run_shape(pool.clone(), &cfg).await;
    // Don't TRUNCATE pool_locks before the warm run — that's the
    // whole point. Just drain + TRUNCATE the cost tables so the
    // warm run starts from a clean cost-table state but warm
    // pool_locks. Reuse reset_state but skip pool_locks TRUNCATE...
    // actually reset_state TRUNCATEs pool_locks unconditionally;
    // instead pre-warm pool_locks explicitly post-reset.
    reset_state(&pool).await.expect("reset_state warm-reset");
    pre_seed_shape(&pool, Shape::S1FanOutSimple, n)
        .await
        .expect("pre_seed warm");
    // Pre-create pool_lock rows for the shape's SKU range so the
    // warm run hits UPSERT-no-op rather than UPSERT-insert.
    let g = Shape::S1FanOutSimple.g() as i64;
    sqlx::query(
        "INSERT INTO poc_v21_pool_locks (sku_id, location_id) \
         SELECT s, $1 FROM generate_series(1, $2) AS s \
         ON CONFLICT DO NOTHING",
    )
    .bind(1_i64) // LOCATION_ID
    .bind(g)
    .execute(&*pool)
    .await
    .expect("pre-warm pool_locks");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let warm = run_shape(pool.clone(), &cfg).await;

    (cold.throughput_eps, warm.throughput_eps)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_calibration() {
    let pool = connect_pool(8).await;

    // ── Phase 1: fsync probe ──────────────────────────────────────────
    println!("=== M8.1 calibration: fsync probe (n={FSYNC_PROBE_COUNT}) ===");
    let fsync_us = probe_fsync_latency(&pool).await;
    let fsync_p50 = percentile(&fsync_us, 50.0);
    let fsync_p99 = percentile(&fsync_us, 99.0);
    let fsync_p999 = percentile(&fsync_us, 99.9);
    let fsync_p99_ms = (fsync_p99 as f64 / 1000.0).ceil() as u64;
    let recommended_lease_ms = 100u64.max(10 * fsync_p99_ms);
    println!(
        "  fsync p50={fsync_p50}µs p99={fsync_p99}µs p99.9={fsync_p999}µs \
         → recommended committer_lease_ms={recommended_lease_ms}"
    );

    // ── Phase 2: cold-start vs steady-state ──────────────────────────
    println!(
        "=== M8.1 calibration: cold vs warm S1 (N={CALIBRATION_N} \
         duration={CALIBRATION_DURATION_SECS}s) ==="
    );
    let (cold_tput, warm_tput) =
        run_cold_warm_pair(CALIBRATION_N, CALIBRATION_DURATION_SECS).await;
    let warmup_delta_pct = if cold_tput > 0.0 {
        (warm_tput - cold_tput) / cold_tput * 100.0
    } else {
        0.0
    };
    println!(
        "  cold tput={cold_tput:.0} ev/s · warm tput={warm_tput:.0} ev/s · \
         delta={warmup_delta_pct:+.1}%"
    );

    // ── Phase 3: write calibration markdown ──────────────────────────
    let _ = std::fs::create_dir_all("bench");
    let ts = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let mut body = String::new();
    body.push_str("# M8.1 (acct-toj6) — pre-bake-off calibration\n\n");
    body.push_str(&format!("Generated: {ts}\n\n"));
    body.push_str("## fsync latency probe\n\n");
    body.push_str(&format!(
        "Probe: {FSYNC_PROBE_COUNT} sequential 1-byte INSERT+COMMIT \
         on a throwaway table with `synchronous_commit=on`.\n\n"
    ));
    body.push_str(&format!("| metric | value µs |\n|---|---|\n"));
    body.push_str(&format!("| p50 | {fsync_p50} |\n"));
    body.push_str(&format!("| p99 | {fsync_p99} |\n"));
    body.push_str(&format!("| p99.9 | {fsync_p999} |\n\n"));
    body.push_str(&format!(
        "**Recommendation**: `poc_v21.committer_lease_ms = {recommended_lease_ms}` \
         (= max(100, 10 × p99_ms={fsync_p99_ms})).\n\n"
    ));

    body.push_str("## cold-start vs steady-state\n\n");
    body.push_str(&format!(
        "Shape: S1 fan_out_simple at N={CALIBRATION_N} duration={CALIBRATION_DURATION_SECS}s. \
         Cold = empty pool_locks; warm = pool_locks pre-populated.\n\n"
    ));
    body.push_str(&format!(
        "| run | throughput ev/s |\n|---|---|\n| cold | {cold_tput:.0} |\n| warm | {warm_tput:.0} |\n\n"
    ));
    body.push_str(&format!(
        "**Steady-state delta**: {warmup_delta_pct:+.1}%. Bake-off harness \
         should discard the first 5s of every cell to clear UPSERT-insert \
         overhead.\n"
    ));

    std::fs::write("bench/calibration-m81.md", body).expect("write calibration md");
    println!("==> wrote bench/calibration-m81.md");
}
