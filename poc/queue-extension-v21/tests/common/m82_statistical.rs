//! M8.2 (acct-6h3o) — statistical bench runner per spec §5.3 + §5.4.
//!
//! Builds on M8.1's `m8_runner` helpers (enqueue, pick_sku, pre_seed,
//! reset_state) to add:
//!
//!   * **5×60s replication** per (shape, N) cell with 30s rest between
//!     runs.
//!   * **hdrhistogram** for per-run latency (3-digit sigfig, 1µs–60s).
//!   * **pg_locks_sampler** at 100ms intervals (optional per cell).
//!   * **Counter snapshots** (router + committer + per-method) captured
//!     before/after each run as deltas.
//!   * **Median + IQR** aggregation across the 5 runs of a cell, plus
//!     IQR/median noise flag.
//!   * **JSON output** per cell to `bench/results-m8/`.
//!
//! Per-run shape:
//!   1. `reset_state` (drains shmem, TRUNCATEs, resets router stats)
//!   2. `pre_seed_shape` per M8.1
//!   3. Snapshot all counters
//!   4. Spawn N backends submit-and-poll on `run.duration_secs` window
//!   5. Optional pg_locks_sampler runs alongside
//!   6. Merge backend hdrhistograms; snapshot counters again
//!   7. Sleep `rest_secs` before next run

#![allow(dead_code)]

use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use uuid::Uuid;

#[path = "m8_runner.rs"]
pub mod m8_runner;

#[path = "pg_locks_sampler.rs"]
pub mod pg_locks_sampler;

pub use m8_runner::{LOCATION_ID, POC_DSN, Shape};
use m8_runner::{build_components, pick_event_kind, pick_output_sku, pick_sku, pick_wip};
use pg_locks_sampler::PgLocksSampler;

// ──────────────────────────────────────────────────────────────────────
// Hist config
// ──────────────────────────────────────────────────────────────────────

/// hdrhistogram bounds: 1µs..=60_000_000µs (60s), 3 significant digits.
/// Matches spec §5.3 ("3-digit precision, 60s recording window").
fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hdrhistogram bounds")
}

// ──────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct CounterSnapshot {
    // Router-side
    pub superbatch_count: i64,
    pub total_envelopes: i64,
    pub force_pack_count: i64,
    pub max_envelope_count: i64,
    pub ticks_total: i64,
    pub entries_scanned_total: i64,
    pub cross_sb_for_update_waits: i64,
    pub histogram_buckets: [i64; 8],
    pub envelopes_per_sb_p50: f64,
    pub envelopes_per_sb_p99: f64,
    // Committer-side
    pub committer_claim_count: i64,
    pub committer_takeover_count: i64,
    pub committer_tx_failures: i64,
    pub committer_drains_total: i64,
    pub committer_pipeline_ns_total: i64,
    pub committer_pipeline_count: i64,
    pub eject_count: i64,
    pub backpressure_count: i64,
    // Per-method (FIFO/AVG/STD)
    pub method_dispatch_fifo: i64,
    pub method_dispatch_avg: i64,
    pub method_dispatch_std: i64,
    pub method_error_fifo: i64,
    pub method_error_avg: i64,
    pub method_error_std: i64,
}

impl CounterSnapshot {
    pub fn delta(&self, before: &CounterSnapshot) -> CounterDelta {
        let mut hist = [0_i64; 8];
        for i in 0..8 {
            hist[i] = self.histogram_buckets[i] - before.histogram_buckets[i];
        }
        CounterDelta {
            superbatch_count: self.superbatch_count - before.superbatch_count,
            total_envelopes: self.total_envelopes - before.total_envelopes,
            force_pack_count: self.force_pack_count - before.force_pack_count,
            max_envelope_count: self.max_envelope_count, // not delta — max-of-period not tracked
            ticks_total: self.ticks_total - before.ticks_total,
            entries_scanned_total: self.entries_scanned_total
                - before.entries_scanned_total,
            cross_sb_for_update_waits: self.cross_sb_for_update_waits
                - before.cross_sb_for_update_waits,
            histogram_buckets: hist,
            envelopes_per_sb_p50_end: self.envelopes_per_sb_p50,
            envelopes_per_sb_p99_end: self.envelopes_per_sb_p99,
            committer_claim_count: self.committer_claim_count
                - before.committer_claim_count,
            committer_takeover_count: self.committer_takeover_count
                - before.committer_takeover_count,
            committer_tx_failures: self.committer_tx_failures
                - before.committer_tx_failures,
            committer_drains_total: self.committer_drains_total
                - before.committer_drains_total,
            committer_pipeline_ns_total: self.committer_pipeline_ns_total
                - before.committer_pipeline_ns_total,
            committer_pipeline_count: self.committer_pipeline_count
                - before.committer_pipeline_count,
            eject_count: self.eject_count - before.eject_count,
            backpressure_count: self.backpressure_count - before.backpressure_count,
            method_dispatch_fifo: self.method_dispatch_fifo - before.method_dispatch_fifo,
            method_dispatch_avg: self.method_dispatch_avg - before.method_dispatch_avg,
            method_dispatch_std: self.method_dispatch_std - before.method_dispatch_std,
            method_error_fifo: self.method_error_fifo - before.method_error_fifo,
            method_error_avg: self.method_error_avg - before.method_error_avg,
            method_error_std: self.method_error_std - before.method_error_std,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CounterDelta {
    pub superbatch_count: i64,
    pub total_envelopes: i64,
    pub force_pack_count: i64,
    pub max_envelope_count: i64,
    pub ticks_total: i64,
    pub entries_scanned_total: i64,
    pub cross_sb_for_update_waits: i64,
    pub histogram_buckets: [i64; 8],
    pub envelopes_per_sb_p50_end: f64,
    pub envelopes_per_sb_p99_end: f64,
    pub committer_claim_count: i64,
    pub committer_takeover_count: i64,
    pub committer_tx_failures: i64,
    pub committer_drains_total: i64,
    pub committer_pipeline_ns_total: i64,
    pub committer_pipeline_count: i64,
    pub eject_count: i64,
    pub backpressure_count: i64,
    pub method_dispatch_fifo: i64,
    pub method_dispatch_avg: i64,
    pub method_dispatch_std: i64,
    pub method_error_fifo: i64,
    pub method_error_avg: i64,
    pub method_error_std: i64,
}

#[derive(Debug, Clone)]
pub struct RunResult {
    pub duration_secs: u64,
    pub total_envelopes: u64,
    pub committed: u64,
    pub failed: u64,
    pub throughput_eps: f64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub p9999_us: u64,
    pub max_us: u64,
    /// Merged histogram across all backends — preserved so per-cell
    /// aggregation can re-derive percentiles.
    pub histogram: Histogram<u64>,
    pub counters: CounterDelta,
    /// Pipeline-time avg ns per drain (committer_pipeline_ns_total /
    /// committer_pipeline_count) over the run window. Feeds the §5.6
    /// bottleneck classifier and gx1z.1.10/.12 decision math.
    pub avg_pipeline_ns_per_drain: f64,
    pub top_wait_event: Option<(String, String, i64)>,
    /// Sampler-on / sampler-off mode for the perturbation check.
    pub sampler_on: bool,
}

#[derive(Debug, Clone)]
pub struct CellConfig {
    pub shape: Shape,
    pub n_backends: usize,
    pub method_mix: &'static str, // "fifo" | "avg" | "std" | "mixed"
    pub guc_overrides: BTreeMap<String, String>,
    pub runs: usize,
    pub duration_secs: u64,
    pub rest_secs: u64,
    pub sampler_on: bool,
    /// hdrhistogram unit for printed median+IQR.
    pub label: String,
}

impl CellConfig {
    pub fn cell_id(&self) -> String {
        let guc_part = if self.guc_overrides.is_empty() {
            String::new()
        } else {
            let kvs: Vec<String> = self
                .guc_overrides
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            format!("_{}", kvs.join("_"))
        };
        let sampler_tag = if self.sampler_on { "" } else { "_nosampler" };
        format!(
            "{}_N={}_{}{guc_part}{sampler_tag}",
            self.shape.name(),
            self.n_backends,
            self.method_mix,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub median: f64,
    pub iqr: f64,
    pub min: f64,
    pub max: f64,
    pub iqr_over_median_pct: f64,
}

impl Stats {
    pub fn from_samples(mut xs: Vec<f64>) -> Self {
        if xs.is_empty() {
            return Self::default();
        }
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = xs.len();
        let median = if n % 2 == 1 {
            xs[n / 2]
        } else {
            (xs[n / 2 - 1] + xs[n / 2]) / 2.0
        };
        let q1 = percentile_f64(&xs, 0.25);
        let q3 = percentile_f64(&xs, 0.75);
        let iqr = q3 - q1;
        let iqr_over_median_pct = if median.abs() > 1e-9 {
            (iqr / median) * 100.0
        } else {
            0.0
        };
        Self {
            median,
            iqr,
            min: xs[0],
            max: xs[n - 1],
            iqr_over_median_pct,
        }
    }
}

fn percentile_f64(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (q * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Debug, Clone)]
pub struct CellResult {
    pub cfg: CellConfig,
    pub runs: Vec<RunResult>,
    pub evps_stats: Stats,
    pub p50_us_stats: Stats,
    pub p99_us_stats: Stats,
    pub p999_us_stats: Stats,
    pub avg_envs_per_sb_stats: Stats,
    pub avg_pipeline_ns_stats: Stats,
}

// ──────────────────────────────────────────────────────────────────────
// Counter snapshot read
// ──────────────────────────────────────────────────────────────────────

pub async fn snapshot_counters(pool: &PgPool) -> CounterSnapshot {
    let mut snap = CounterSnapshot::default();

    // Router stats — TableIterator → vec of (name, f64)
    let router: Vec<(String, f64)> =
        sqlx::query_as("SELECT stat_name, stat_value FROM poc_v21_router_stats()")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    for (k, v) in router {
        let vi = v as i64;
        match k.as_str() {
            "superbatch_count" => snap.superbatch_count = vi,
            "total_envelopes" => snap.total_envelopes = vi,
            "force_pack_count" => snap.force_pack_count = vi,
            "max_envelope_count" => snap.max_envelope_count = vi,
            "ticks_total" => snap.ticks_total = vi,
            "entries_scanned_total" => snap.entries_scanned_total = vi,
            "cross_sb_for_update_waits" => snap.cross_sb_for_update_waits = vi,
            "envelopes_per_sb_p50" => snap.envelopes_per_sb_p50 = v,
            "envelopes_per_sb_p99" => snap.envelopes_per_sb_p99 = v,
            s if s.starts_with("histogram_bucket_") => {
                if let Some(idx) = s.strip_prefix("histogram_bucket_").and_then(|t| t.parse::<usize>().ok()) {
                    if idx < 8 {
                        snap.histogram_buckets[idx] = vi;
                    }
                }
            }
            _ => {}
        }
    }

    // Committer stats
    let committer: Vec<(String, f64)> =
        sqlx::query_as("SELECT stat_name, stat_value FROM poc_v21_committer_stats()")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    for (k, v) in committer {
        let vi = v as i64;
        match k.as_str() {
            "committer_claim_count" => snap.committer_claim_count = vi,
            "committer_takeover_count" => snap.committer_takeover_count = vi,
            "committer_tx_failures" => snap.committer_tx_failures = vi,
            "committer_drains_total" => snap.committer_drains_total = vi,
            _ => {}
        }
    }

    snap.committer_pipeline_ns_total =
        sqlx::query_scalar("SELECT poc_v21_committer_pipeline_ns_total()")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    snap.committer_pipeline_count =
        sqlx::query_scalar("SELECT poc_v21_committer_pipeline_count()")
            .fetch_one(pool)
            .await
            .unwrap_or(0);
    snap.eject_count = sqlx::query_scalar("SELECT poc_v21_eject_count()")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    snap.backpressure_count = sqlx::query_scalar("SELECT poc_v21_backpressure_count()")
        .fetch_one(pool)
        .await
        .unwrap_or(0);

    // Per-method
    let method: Vec<(String, f64)> =
        sqlx::query_as("SELECT stat_name, stat_value FROM poc_v21_method_stats()")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    for (k, v) in method {
        let vi = v as i64;
        match k.as_str() {
            "fifo_dispatch_count" => snap.method_dispatch_fifo = vi,
            "avg_dispatch_count" => snap.method_dispatch_avg = vi,
            "std_dispatch_count" => snap.method_dispatch_std = vi,
            "fifo_error_count" => snap.method_error_fifo = vi,
            "avg_error_count" => snap.method_error_avg = vi,
            "std_error_count" => snap.method_error_std = vi,
            _ => {}
        }
    }

    snap
}

// ──────────────────────────────────────────────────────────────────────
// Single-run orchestrator
// ──────────────────────────────────────────────────────────────────────

pub async fn run_single(
    pool: Arc<PgPool>,
    shape: Shape,
    n_backends: usize,
    duration_secs: u64,
    sampler_on: bool,
) -> RunResult {
    let duration = Duration::from_secs(duration_secs);
    let barrier = Arc::new(Barrier::new(n_backends));

    let counters_before = snapshot_counters(&pool).await;
    let sampler = if sampler_on {
        Some(PgLocksSampler::spawn((*pool).clone(), 100).await)
    } else {
        None
    };

    let mut handles = Vec::with_capacity(n_backends);
    for backend_idx in 0..n_backends {
        let pool = pool.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let mut hist = new_hist();
            let mut committed: u64 = 0;
            let mut failed: u64 = 0;

            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                .wrapping_add((backend_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let mut rng = SmallRng::seed_from_u64(seed);

            barrier.wait().await;
            let start = Instant::now();
            let mut iter: usize = 0;
            while start.elapsed() < duration {
                let cid = Uuid::new_v4();
                let chrono_iter: i64 =
                    (backend_idx as i64) * 100_000_000 + iter as i64 + 1;
                let primary = pick_sku(shape, backend_idx, n_backends, iter, &mut rng);

                let t0 = Instant::now();
                let enq_res = enqueue_for_shape(
                    &pool, shape, cid, primary, backend_idx, iter, chrono_iter, &mut rng,
                )
                .await;

                match enq_res {
                    Ok(()) => {
                        let state = wait_terminal_single(
                            &pool,
                            cid,
                            Duration::from_secs(10),
                        )
                        .await;
                        let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
                        match state.as_deref() {
                            Some("committed") | Some("replayed") => {
                                // Clamp to histogram bounds (1µs..60s).
                                let v = dt_us.max(1).min(60_000_000);
                                let _ = hist.record(v);
                                committed += 1;
                            }
                            _ => failed += 1,
                        }
                    }
                    Err(_) => failed += 1,
                }
                iter += 1;
            }
            (hist, committed, failed)
        }));
    }

    let mut merged = new_hist();
    let mut total_committed: u64 = 0;
    let mut total_failed: u64 = 0;
    for h in handles {
        let (hist, c, f) = h.await.expect("backend task panicked");
        merged.add(&hist).expect("hist merge");
        total_committed += c;
        total_failed += f;
    }

    let sampler_report = if let Some(s) = sampler {
        Some(s.shutdown().await)
    } else {
        None
    };
    let top_wait_event = sampler_report.and_then(|r| r.top_wait_event());

    let counters_after = snapshot_counters(&pool).await;
    let counters = counters_after.delta(&counters_before);

    let avg_pipeline_ns_per_drain = if counters.committer_pipeline_count > 0 {
        counters.committer_pipeline_ns_total as f64
            / counters.committer_pipeline_count as f64
    } else {
        0.0
    };

    let total_envelopes = total_committed + total_failed;
    let throughput_eps = (total_committed as f64) / duration_secs as f64;
    let p50_us = merged.value_at_quantile(0.50);
    let p90_us = merged.value_at_quantile(0.90);
    let p99_us = merged.value_at_quantile(0.99);
    let p999_us = merged.value_at_quantile(0.999);
    let p9999_us = merged.value_at_quantile(0.9999);
    let max_us = merged.max();

    RunResult {
        duration_secs,
        total_envelopes,
        committed: total_committed,
        failed: total_failed,
        throughput_eps,
        p50_us,
        p90_us,
        p99_us,
        p999_us,
        p9999_us,
        max_us,
        histogram: merged,
        counters,
        avg_pipeline_ns_per_drain,
        top_wait_event,
        sampler_on,
    }
}

// ──────────────────────────────────────────────────────────────────────
// Per-cell orchestrator: setup + 5 runs × duration + 30s rest
// ──────────────────────────────────────────────────────────────────────

pub async fn run_cell(pool: Arc<PgPool>, cfg: &CellConfig) -> CellResult {
    let mut runs: Vec<RunResult> = Vec::with_capacity(cfg.runs);

    for run_idx in 0..cfg.runs {
        eprintln!(
            "  [cell={} run={}/{}] reset + pre_seed",
            cfg.cell_id(),
            run_idx + 1,
            cfg.runs
        );
        m8_runner::reset_state(&pool).await.expect("reset_state");
        m8_runner::pre_seed_shape(&pool, cfg.shape, cfg.n_backends)
            .await
            .expect("pre_seed_shape");
        // Apply per-method overlay if mix specifies single-method
        match cfg.method_mix {
            "fifo" => { /* default — FIFO layers seeded by pre_seed; nothing extra */ }
            "avg" => seed_all_avg(&pool, cfg.shape).await,
            "std" => seed_all_std(&pool, cfg.shape).await,
            "mixed" => { /* S8 pre_seed handles thirds */ }
            _ => panic!("unknown method_mix={}", cfg.method_mix),
        }
        // Brief warm window so reset stats settle.
        tokio::time::sleep(Duration::from_millis(500)).await;

        eprintln!(
            "  [cell={} run={}/{}] running {}s (sampler={})",
            cfg.cell_id(),
            run_idx + 1,
            cfg.runs,
            cfg.duration_secs,
            cfg.sampler_on
        );
        let r = run_single(
            pool.clone(),
            cfg.shape,
            cfg.n_backends,
            cfg.duration_secs,
            cfg.sampler_on,
        )
        .await;
        eprintln!(
            "  [cell={} run={}/{}] evps={:.0} p50={}µs p99={}µs committed={} failed={}",
            cfg.cell_id(),
            run_idx + 1,
            cfg.runs,
            r.throughput_eps,
            r.p50_us,
            r.p99_us,
            r.committed,
            r.failed,
        );
        runs.push(r);

        if run_idx + 1 < cfg.runs {
            eprintln!("  [cell={}] rest {}s", cfg.cell_id(), cfg.rest_secs);
            tokio::time::sleep(Duration::from_secs(cfg.rest_secs)).await;
        }
    }

    let evps: Vec<f64> = runs.iter().map(|r| r.throughput_eps).collect();
    let p50: Vec<f64> = runs.iter().map(|r| r.p50_us as f64).collect();
    let p99: Vec<f64> = runs.iter().map(|r| r.p99_us as f64).collect();
    let p999: Vec<f64> = runs.iter().map(|r| r.p999_us as f64).collect();
    let avg_eps: Vec<f64> = runs
        .iter()
        .map(|r| {
            if r.counters.superbatch_count > 0 {
                r.counters.total_envelopes as f64 / r.counters.superbatch_count as f64
            } else {
                0.0
            }
        })
        .collect();
    let avg_pipe: Vec<f64> = runs
        .iter()
        .map(|r| r.avg_pipeline_ns_per_drain)
        .collect();

    CellResult {
        cfg: cfg.clone(),
        evps_stats: Stats::from_samples(evps),
        p50_us_stats: Stats::from_samples(p50),
        p99_us_stats: Stats::from_samples(p99),
        p999_us_stats: Stats::from_samples(p999),
        avg_envs_per_sb_stats: Stats::from_samples(avg_eps),
        avg_pipeline_ns_stats: Stats::from_samples(avg_pipe),
        runs,
    }
}

// Per-method overlays for single-method cells.
async fn seed_all_avg(pool: &PgPool, shape: Shape) {
    let g = shape.g() as i64;
    let skus: Vec<i64> = (1..=g).collect();
    if skus.is_empty() {
        return;
    }
    let methods: Vec<String> = skus.iter().map(|_| "avg".to_string()).collect();
    let _ = sqlx::query(
        "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
         SELECT s, m FROM UNNEST($1::bigint[], $2::text[]) AS t(s, m) \
         ON CONFLICT (sku_id) DO UPDATE SET method_id = EXCLUDED.method_id",
    )
    .bind(&skus)
    .bind(&methods)
    .execute(pool)
    .await;
    // Seed AVG state for all SKUs (matches pre_seed_mixed_method semantics).
    let _ = sqlx::query(
        "INSERT INTO poc_v21_avg_pool_state \
            (sku_id, location_id, avg_unit_cost, total_qty, last_updated_at, last_committer_tx_id) \
         SELECT s, $1, $2, $3, now(), 0 \
           FROM UNNEST($4::bigint[]) AS t(s) \
         ON CONFLICT (sku_id, location_id) DO UPDATE SET \
             avg_unit_cost = EXCLUDED.avg_unit_cost, total_qty = EXCLUDED.total_qty",
    )
    .bind(LOCATION_ID)
    .bind(100_i64)
    .bind(1_000_000_000_i64)
    .bind(&skus)
    .execute(pool)
    .await;
}

async fn seed_all_std(pool: &PgPool, shape: Shape) {
    let g = shape.g() as i64;
    let skus: Vec<i64> = (1..=g).collect();
    if skus.is_empty() {
        return;
    }
    let methods: Vec<String> = skus.iter().map(|_| "std".to_string()).collect();
    let _ = sqlx::query(
        "INSERT INTO poc_v21_sku_method_assignments (sku_id, method_id) \
         SELECT s, m FROM UNNEST($1::bigint[], $2::text[]) AS t(s, m) \
         ON CONFLICT (sku_id) DO UPDATE SET method_id = EXCLUDED.method_id",
    )
    .bind(&skus)
    .bind(&methods)
    .execute(pool)
    .await;
    let _ = sqlx::query(
        "INSERT INTO poc_v21_standard_costs (sku_id, location_id, unit_cost, effective_from) \
         SELECT s, $1, $2, now() - interval '1 day' \
           FROM UNNEST($3::bigint[]) AS t(s)",
    )
    .bind(LOCATION_ID)
    .bind(100_i64)
    .bind(&skus)
    .execute(pool)
    .await;
    // acct-ed7u: invalidate committer cache after seeding std costs.
    let _ = sqlx::query("SELECT poc_v21_invalidate_committer_caches()")
        .execute(pool)
        .await;
}

// ──────────────────────────────────────────────────────────────────────
// Enqueue dispatch — mirrors m8_runner's match on Shape (we can't reuse
// the inline closure from `run_shape` because we need our own counters
// + hdrhist capture; the enqueue helpers themselves stay private to
// m8_runner so we replicate the dispatch here in a thin wrapper).
// ──────────────────────────────────────────────────────────────────────

async fn enqueue_for_shape(
    pool: &PgPool,
    shape: Shape,
    cid: Uuid,
    primary: i64,
    backend_idx: usize,
    iter: usize,
    chrono_iter: i64,
    rng: &mut SmallRng,
) -> Result<(), sqlx::Error> {
    match shape {
        Shape::S1FanOutSimple | Shape::S5HotPool => {
            enqueue_inv_adjust(pool, cid, primary, 1, chrono_iter).await
        }
        Shape::S2FanOutWo
        | Shape::S3FanContestedWo
        | Shape::S4FanInWo
        | Shape::S6LargeWo
        | Shape::S7VeryLargeWo => {
            let comps = build_components(shape, primary, iter, rng);
            let out = (pick_output_sku(shape, primary), LOCATION_ID, 1_i64);
            let (wo_id, op_id) = pick_wip(backend_idx, iter);
            enqueue_wo_complete(pool, cid, wo_id, op_id, &comps, out, chrono_iter).await
        }
        Shape::S8MixedEventMixedMethod => {
            let kind = pick_event_kind(iter);
            if kind == "inv_adjust" {
                enqueue_inv_adjust(pool, cid, primary, 1, chrono_iter).await
            } else {
                let comps = build_components(shape, primary, iter, rng);
                let out = (pick_output_sku(shape, primary), LOCATION_ID, 1_i64);
                let (wo_id, op_id) = pick_wip(backend_idx, iter);
                enqueue_wo_complete(pool, cid, wo_id, op_id, &comps, out, chrono_iter)
                    .await
            }
        }
        Shape::S9CausalChain => {
            // S9 needs the 3-event triplet (PoReceipt → WoComplete → SoShipment)
            // per backend; M8.2 N-sweep treats this shape as out-of-scope for
            // the acceptance gate (single-shape S2). If invoked, fall through
            // with a placeholder — the caller's per-cell config controls
            // shape selection.
            unreachable!("S9 not supported in M8.2 N-sweep runner")
        }
    }
}

async fn enqueue_inv_adjust(
    pool: &PgPool,
    cid: Uuid,
    sku: i64,
    qty: i64,
    iter: i64,
) -> Result<(), sqlx::Error> {
    let payload = serde_json::json!({
        "sku_id": sku,
        "location_id": LOCATION_ID,
        "qty": -qty,
        "unit_cost": 0,
        "issue_id": iter,
        "business_date_jdate": 20221,
        "doc_chrono": iter,
        "document_id": 7_000_000_i64 + iter,
    });
    let pool_keys = serde_json::json!({ "sku": [[sku, LOCATION_ID]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'inv_adjust', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await?;
    Ok(())
}

async fn enqueue_wo_complete(
    pool: &PgPool,
    cid: Uuid,
    wo_id: i64,
    op_id: i64,
    components: &[(i64, i64, i64)],
    output: (i64, i64, i64),
    iter: i64,
) -> Result<(), sqlx::Error> {
    let comps_json: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, q)| serde_json::json!([s, l, q]))
        .collect();
    let payload = serde_json::json!({
        "wip_account": [wo_id, op_id],
        "components": comps_json,
        "output": [output.0, output.1, output.2],
        "business_date_jdate": 20221,
        "doc_chrono": iter,
        "document_id": 8_000_000_i64 + iter,
    });
    let mut sku_keys: Vec<serde_json::Value> = components
        .iter()
        .map(|(s, l, _)| serde_json::json!([s, l]))
        .collect();
    sku_keys.push(serde_json::json!([output.0, output.1]));
    let pool_keys = serde_json::json!({ "sku": sku_keys, "wip": [[wo_id, op_id]] });
    sqlx::query("SELECT poc_v21_enqueue($1::uuid, 'wo_complete', $2::jsonb, $3::jsonb, false)")
        .bind(cid)
        .bind(&payload)
        .bind(&pool_keys)
        .execute(pool)
        .await?;
    Ok(())
}

async fn wait_terminal_single(
    pool: &PgPool,
    cid: Uuid,
    timeout: Duration,
) -> Option<String> {
    let start = Instant::now();
    loop {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT state::text FROM poc_v21_submission_status \
              WHERE correlation_id = $1 \
                AND state IN ('committed', 'failed', 'replayed')",
        )
        .bind(cid)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        if let Some((s,)) = row {
            return Some(s);
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ──────────────────────────────────────────────────────────────────────
// Pool helper
// ──────────────────────────────────────────────────────────────────────

pub async fn build_pool(n_backends: usize, sampler_on: bool) -> PgPool {
    // N backend conns + 4 for setup/poll + 1 for sampler if on.
    let extras: u32 = if sampler_on { 6 } else { 5 };
    let max_conns = (n_backends as u32) + extras;
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(30))
        .connect(POC_DSN)
        .await
        .expect("connect to acct_poc_queue_v21")
}

// ──────────────────────────────────────────────────────────────────────
// JSON output
// ──────────────────────────────────────────────────────────────────────

pub fn cell_to_json(cell: &CellResult) -> serde_json::Value {
    let runs_json: Vec<serde_json::Value> = cell
        .runs
        .iter()
        .map(|r| {
            let counters = &r.counters;
            serde_json::json!({
                "duration_s": r.duration_secs,
                "events_ok": r.committed,
                "events_failed": r.failed,
                "throughput_evps": r.throughput_eps,
                "p50_us": r.p50_us,
                "p90_us": r.p90_us,
                "p99_us": r.p99_us,
                "p999_us": r.p999_us,
                "p9999_us": r.p9999_us,
                "max_us": r.max_us,
                "sampler_on": r.sampler_on,
                "avg_pipeline_ns_per_drain": r.avg_pipeline_ns_per_drain,
                "top_wait_event": r.top_wait_event.as_ref().map(|(wet, we, c)| {
                    serde_json::json!({
                        "wait_event_type": wet,
                        "wait_event": we,
                        "sum_backends": c,
                    })
                }),
                "router": {
                    "superbatch_count": counters.superbatch_count,
                    "total_envelopes": counters.total_envelopes,
                    "force_pack_count": counters.force_pack_count,
                    "max_envelope_count": counters.max_envelope_count,
                    "ticks_total": counters.ticks_total,
                    "entries_scanned_total": counters.entries_scanned_total,
                    "cross_sb_for_update_waits": counters.cross_sb_for_update_waits,
                    "envelope_histogram": counters.histogram_buckets,
                    "envelopes_per_sb_p50_end": counters.envelopes_per_sb_p50_end,
                    "envelopes_per_sb_p99_end": counters.envelopes_per_sb_p99_end,
                },
                "committer": {
                    "claim_count": counters.committer_claim_count,
                    "takeover_count": counters.committer_takeover_count,
                    "tx_failures": counters.committer_tx_failures,
                    "drains_total": counters.committer_drains_total,
                    "pipeline_ns_total": counters.committer_pipeline_ns_total,
                    "pipeline_count": counters.committer_pipeline_count,
                    "eject_count": counters.eject_count,
                    "backpressure_count": counters.backpressure_count,
                },
                "method_dispatch": {
                    "fifo": counters.method_dispatch_fifo,
                    "avg": counters.method_dispatch_avg,
                    "std": counters.method_dispatch_std,
                    "fifo_errors": counters.method_error_fifo,
                    "avg_errors": counters.method_error_avg,
                    "std_errors": counters.method_error_std,
                },
            })
        })
        .collect();

    let guc_overrides: serde_json::Value = serde_json::Value::Object(
        cell.cfg
            .guc_overrides
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect(),
    );

    serde_json::json!({
        "cell": cell.cfg.cell_id(),
        "shape": cell.cfg.shape.name(),
        "n": cell.cfg.n_backends,
        "method_mix": cell.cfg.method_mix,
        "guc_overrides": guc_overrides,
        "sampler_on": cell.cfg.sampler_on,
        "runs": runs_json,
        "stats": {
            "evps": {
                "median": cell.evps_stats.median,
                "iqr": cell.evps_stats.iqr,
                "min": cell.evps_stats.min,
                "max": cell.evps_stats.max,
                "iqr_over_median_pct": cell.evps_stats.iqr_over_median_pct,
                "noise_flag": cell.evps_stats.iqr_over_median_pct > 10.0,
            },
            "p50_us": stats_to_json(&cell.p50_us_stats),
            "p99_us": stats_to_json(&cell.p99_us_stats),
            "p999_us": stats_to_json(&cell.p999_us_stats),
            "avg_envelopes_per_sb": stats_to_json(&cell.avg_envs_per_sb_stats),
            "avg_pipeline_ns_per_drain": stats_to_json(&cell.avg_pipeline_ns_stats),
        },
    })
}

fn stats_to_json(s: &Stats) -> serde_json::Value {
    serde_json::json!({
        "median": s.median,
        "iqr": s.iqr,
        "min": s.min,
        "max": s.max,
        "iqr_over_median_pct": s.iqr_over_median_pct,
        "noise_flag": s.iqr_over_median_pct > 10.0,
    })
}

pub fn write_cell_json(cell: &CellResult, dir: &Path) -> std::io::Result<std::path::PathBuf> {
    std::fs::create_dir_all(dir)?;
    let filename = format!("{}.json", cell.cfg.cell_id());
    let path = dir.join(&filename);
    let body = serde_json::to_string_pretty(&cell_to_json(cell))?;
    std::fs::write(&path, body)?;
    Ok(path)
}

// ──────────────────────────────────────────────────────────────────────
// CPU affinity verification — spec acceptance requires this.
// ──────────────────────────────────────────────────────────────────────

/// Read /proc/self/status and return the Cpus_allowed_list line.
/// Returns "unknown" if /proc isn't readable. Used by acceptance tests
/// to assert that `taskset -c X-Y cargo test` actually pinned us.
pub fn cpus_allowed_list() -> String {
    match std::fs::read_to_string("/proc/self/status") {
        Ok(s) => {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("Cpus_allowed_list:") {
                    return rest.trim().to_string();
                }
            }
            "unknown".to_string()
        }
        Err(_) => "unknown".to_string(),
    }
}
