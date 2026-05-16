//! M9.1 (acct-4d4n.20) — workload-generator harness shared across the 6
//! bake-off shapes (spec §5.2). Tokio + sqlx external-client model:
//!
//!   - N tokio tasks per shape (one per "backend"), each owning a PgPool
//!     connection slot. tokio Barrier rendezvous so all start the loop
//!     within a single scheduling tick.
//!   - Each task tight-loops `SELECT poc_ledger_apply(...)` for the
//!     configured duration. Per-call latency captured via
//!     `Instant::now()` deltas into a per-task `Vec<u64>` (microseconds).
//!   - Aggregator sorts the merged latency vec and computes p50/p99/p99.9.
//!   - Bottleneck classifier snapshot taken at start + end; label fetched
//!     from `poc_ledger_bottleneck_classify(...)`.
//!
//! ## Method dispatch
//!
//! Shapes 1–5 default to method='mock' to isolate queue + committer
//! transport (matches the M4.1 fan_out bench convention; bypasses
//! cost-method SPI snapshot). Shape 6 (mixed_method) rotates
//! fifo/avg/std per call so dispatch overhead is the measurand and the
//! 33/33/33 proportion is observable.
//!
//! ## Pre-seed
//!
//! Mock path needs no cost-table seed. Shape 6 seeds one massive FIFO
//! layer + one AVG row + one STD row per SKU so 60s of sustained issues
//! never drain a pool.
//!
//! ## SKU selection
//!
//! - fan_in:      always sku_id=1.
//! - fan_out:     disjoint per-backend subset of `g/n_backends` SKUs.
//! - balanced:    uniform random over 1..=g.
//! - zipfian:     inverse-CDF over precomputed cumulative table (α=1.0).
//! - small_batch: same as balanced (b=1 per spec line 901; "batch" is a
//!                committer-side coalescing knob, not a caller-side
//!                burst).
//! - mixed:       uniform random over 1..=g; method rotates fifo→avg→std.

use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

use super::pg_locks_sampler::{PgLocksSampler, SamplerReport};

pub const DSN: &str = "postgres://acct:acct_dev@localhost:5111/acct_poc_queue";
pub const LOCATION_ID: i64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    FanIn,
    FanOut,
    Balanced,
    Zipfian,
    SmallBatch,
    MixedMethod,
}

impl Shape {
    pub fn name(&self) -> &'static str {
        match self {
            Shape::FanIn => "fan_in",
            Shape::FanOut => "fan_out",
            Shape::Balanced => "balanced",
            Shape::Zipfian => "zipfian",
            Shape::SmallBatch => "small_batch",
            Shape::MixedMethod => "mixed_method",
        }
    }

    pub fn g(&self) -> usize {
        match self {
            Shape::FanIn => 1,
            Shape::FanOut => 5000,
            Shape::Balanced => 50,
            Shape::Zipfian => 1000,
            Shape::SmallBatch => 50,
            Shape::MixedMethod => 50,
        }
    }
}

pub struct WorkloadConfig {
    pub shape: Shape,
    pub n_backends: usize,
    pub duration_secs: u64,
}

#[derive(Debug)]
pub struct ShapeResult {
    pub name: &'static str,
    pub g: usize,
    pub n_backends: usize,
    pub duration_secs: u64,
    pub total_applies: u64,
    pub errors: u64,
    pub throughput: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub classifier_label: String,
    pub method_breakdown: Option<HashMap<String, u64>>,
}

pub async fn connect_pool(max_conns: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_conns)
        .acquire_timeout(Duration::from_secs(30))
        .connect(DSN)
        .await
        .expect("connect to acct_poc_queue")
}

/// Sweep idle client backends from prior runs, TRUNCATE PoC tables,
/// reset all 16 shards, reset telemetry surfaces.
pub async fn reset_state(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT pg_terminate_backend(pid)
           FROM pg_stat_activity
          WHERE datname='acct_poc_queue'
            AND pid <> pg_backend_pid()
            AND backend_type = 'client backend'",
    )
    .execute(pool)
    .await?;

    // CASCADE picks up depletions → layers and compensation tables.
    sqlx::query(
        "TRUNCATE \
            poc_test_rows, \
            poc_pool_locks, poc_pool_lock_anchors, \
            poc_cost_consumptions, poc_cost_depletions, \
            poc_cost_compensation_consumptions, \
            poc_cost_compensation_depletions, \
            poc_cost_layers, poc_cost_avg, poc_standard_costs \
            RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;

    for s in 0_i32..16 {
        sqlx::query("SELECT poc_ledger_shard_reset($1::INT)")
            .bind(s)
            .execute(pool)
            .await?;
    }

    sqlx::query("SELECT poc_ledger_method_stats_reset()")
        .execute(pool)
        .await?;
    sqlx::query("SELECT poc_ledger_recovery_stats_reset()")
        .execute(pool)
        .await?;
    sqlx::query("SELECT poc_ledger_bottleneck_stats_reset()")
        .execute(pool)
        .await?;

    Ok(())
}

/// Per-shape pre-seed. Mock shapes (1–5) seed nothing; mixed_method
/// seeds per-SKU FIFO layer + AVG row + STD row sized to survive any
/// reasonable 60s × N=4 workload without depletion.
pub async fn pre_seed(pool: &PgPool, shape: Shape) -> Result<(), sqlx::Error> {
    if shape != Shape::MixedMethod {
        return Ok(());
    }
    let g = shape.g();
    for sku in 1_i64..=(g as i64) {
        sqlx::query(
            "INSERT INTO poc_cost_layers (sku_id, location_id, qty, unit_cost, source_kind) \
             VALUES ($1, $2, 1000000000, 100, 'seed')",
        )
        .bind(sku)
        .bind(LOCATION_ID)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO poc_cost_avg (sku_id, location_id, running_qty, running_value) \
             VALUES ($1, $2, 1000000000, 100000000000)",
        )
        .bind(sku)
        .bind(LOCATION_ID)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO poc_standard_costs (sku_id, unit_cost) VALUES ($1, 100)",
        )
        .bind(sku)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Inverse-CDF table for Zipfian(α) over rank 1..=g.
pub fn build_zipf_cdf(g: usize, alpha: f64) -> Vec<f64> {
    let weights: Vec<f64> = (1..=g)
        .map(|k| 1.0 / (k as f64).powf(alpha))
        .collect();
    let total: f64 = weights.iter().sum();
    let mut cdf = Vec::with_capacity(g);
    let mut acc = 0.0_f64;
    for w in &weights {
        acc += w / total;
        cdf.push(acc);
    }
    cdf
}

pub fn pick_sku(
    shape: Shape,
    backend_idx: usize,
    n_backends: usize,
    iter: usize,
    rng: &mut SmallRng,
    zipf_cdf: Option<&[f64]>,
) -> i64 {
    let g = shape.g();
    match shape {
        Shape::FanIn => 1,
        Shape::FanOut => {
            // Disjoint subsets per backend; rotate within subset.
            let per = (g / n_backends).max(1);
            let base = backend_idx * per + 1;
            let off = iter % per;
            (base + off).min(g) as i64
        }
        Shape::Balanced | Shape::SmallBatch | Shape::MixedMethod => {
            rng.random_range(1_i64..=(g as i64))
        }
        Shape::Zipfian => {
            let cdf = zipf_cdf.expect("zipf_cdf required for Zipfian shape");
            let u: f64 = rng.random();
            let idx = cdf.partition_point(|&p| p < u);
            ((idx + 1).min(g)) as i64
        }
    }
}

pub fn pick_method(shape: Shape, iter: usize) -> &'static str {
    match shape {
        Shape::MixedMethod => ["fifo", "avg", "std"][iter % 3],
        _ => "mock",
    }
}

pub async fn fetch_snapshot(pool: &PgPool) -> sqlx::types::Json<serde_json::Value> {
    let row: (sqlx::types::Json<serde_json::Value>,) =
        sqlx::query_as("SELECT poc_ledger_bottleneck_snapshot()")
            .fetch_one(pool)
            .await
            .expect("fetch bottleneck snapshot");
    row.0
}

pub async fn fetch_classifier_label(
    pool: &PgPool,
    snap_start: &serde_json::Value,
    snap_end: &serde_json::Value,
    wall_ms: i64,
) -> String {
    let row: (String,) = sqlx::query_as(
        "SELECT poc_ledger_bottleneck_classify($1::jsonb, $2::jsonb, $3, 0, 0)",
    )
    .bind(sqlx::types::Json(snap_start))
    .bind(sqlx::types::Json(snap_end))
    .bind(wall_ms)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|_| ("unknown".to_string(),));
    row.0
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub async fn run_shape(pool: Arc<PgPool>, cfg: &WorkloadConfig) -> ShapeResult {
    let n = cfg.n_backends;
    let duration = Duration::from_secs(cfg.duration_secs);
    let barrier = Arc::new(Barrier::new(n));

    let zipf_cdf: Option<Arc<Vec<f64>>> = if cfg.shape == Shape::Zipfian {
        Some(Arc::new(build_zipf_cdf(cfg.shape.g(), 1.0)))
    } else {
        None
    };

    // Snapshot BEFORE workers start so classify covers the workload window
    // (not setup latency).
    let snap_start = fetch_snapshot(&pool).await;
    let wall_t0 = Instant::now();

    let mut handles = Vec::with_capacity(n);
    for backend_idx in 0..n {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let zipf = zipf_cdf.clone();
        let shape = cfg.shape;
        handles.push(tokio::spawn(async move {
            let mut latencies: Vec<u64> = Vec::with_capacity(200_000);
            let mut errors: u64 = 0;
            let mut method_counts: HashMap<String, u64> = HashMap::new();

            // SmallRng is Send; seed per-backend so each task picks
            // an independent stream. SystemTime + backend_idx avoids
            // accidental seed collisions across runs.
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
                let sku = pick_sku(
                    shape,
                    backend_idx,
                    n,
                    iter,
                    &mut rng,
                    zipf.as_deref().map(|v| v.as_slice()),
                );
                let method = pick_method(shape, iter);
                if shape == Shape::MixedMethod {
                    *method_counts.entry(method.to_string()).or_insert(0) += 1;
                }
                let issue_id: i64 =
                    (backend_idx as i64) * 10_000_000_000 + iter as i64;
                let t0 = Instant::now();
                let r: Result<(i32,), sqlx::Error> = sqlx::query_as(
                    "SELECT error_code FROM poc_ledger_apply($1, $2, $3, $4, $5)",
                )
                .bind(sku)
                .bind(LOCATION_ID)
                .bind(1_i64)
                .bind(issue_id)
                .bind(method)
                .fetch_one(&*pool)
                .await;
                let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
                match r {
                    Ok((ec,)) => {
                        if ec == 0 {
                            latencies.push(dt_us);
                        } else {
                            errors += 1;
                        }
                    }
                    Err(_) => {
                        errors += 1;
                    }
                }
                iter += 1;
            }
            (latencies, errors, method_counts)
        }));
    }

    let mut all_lat: Vec<u64> = Vec::new();
    let mut total_err: u64 = 0;
    let mut total_methods: HashMap<String, u64> = HashMap::new();
    for h in handles {
        let (lat, err, methods) =
            h.await.expect("backend task panicked");
        all_lat.extend(lat);
        total_err += err;
        for (k, v) in methods {
            *total_methods.entry(k).or_insert(0) += v;
        }
    }

    let wall_ms = wall_t0.elapsed().as_millis() as i64;
    let snap_end = fetch_snapshot(&pool).await;
    let classifier_label =
        fetch_classifier_label(&pool, &snap_start.0, &snap_end.0, wall_ms).await;

    all_lat.sort_unstable();
    let n_lat = all_lat.len();
    let throughput = (n_lat as f64) / cfg.duration_secs as f64;
    let p50 = percentile(&all_lat, 50.0);
    let p99 = percentile(&all_lat, 99.0);
    let p999 = percentile(&all_lat, 99.9);

    ShapeResult {
        name: cfg.shape.name(),
        g: cfg.shape.g(),
        n_backends: cfg.n_backends,
        duration_secs: cfg.duration_secs,
        total_applies: n_lat as u64,
        errors: total_err,
        throughput,
        p50_us: p50,
        p99_us: p99,
        p999_us: p999,
        classifier_label,
        method_breakdown: if cfg.shape == Shape::MixedMethod {
            Some(total_methods)
        } else {
            None
        },
    }
}

// ──────────────────────────────────────────────────────────────────────
// M9.2 (acct-4d4n.21): statistical runner — histogram-based capture +
// 5×60s replication + IQR aggregation + optional pg_locks sampler.
// ──────────────────────────────────────────────────────────────────────

const HIST_HIGH_US: u64 = 60 * 60 * 1_000_000; // 1h in µs — generous ceiling
const HIST_SIG_FIG: u8 = 3;

/// One 60s run of a workload cell. Histograms capture per-call latency
/// with bounded memory; counters track throughput + errors. Sampler
/// report is present only when the cell ran with sampling enabled.
#[derive(Debug, Clone)]
pub struct CellRun {
    pub run_idx: usize,
    pub total_applies: u64,
    pub errors: u64,
    pub throughput: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub deadlocks_delta: i64,
    pub classifier_label: String,
    pub sampler: Option<SamplerReport>,
}

/// Per-N aggregate. Median + IQR (Q3 − Q1) per metric over the N runs.
#[derive(Debug, Clone)]
pub struct CellResult {
    pub shape: String,
    pub n_backends: usize,
    pub runs: Vec<CellRun>,
    pub throughput_med: f64,
    pub throughput_iqr: f64,
    pub p50_med_us: u64,
    pub p50_iqr_us: u64,
    pub p99_med_us: u64,
    pub p99_iqr_us: u64,
    pub p999_med_us: u64,
    pub p999_iqr_us: u64,
    pub deadlocks_med: i64,
    pub sampler_enabled: bool,
}

pub struct CellConfig {
    pub shape: Shape,
    pub n_backends: usize,
    pub duration_secs: u64,
    pub runs: usize,
    pub settle_secs: u64,
    pub with_sampler: bool,
    pub sampler_interval_ms: u64,
}

impl Default for CellConfig {
    fn default() -> Self {
        Self {
            shape: Shape::FanIn,
            n_backends: 4,
            duration_secs: 60,
            runs: 5,
            settle_secs: 30,
            with_sampler: true,
            sampler_interval_ms: 100,
        }
    }
}

/// Histogram-based variant of `run_shape`. Returns hdrhistogram for
/// merged-cross-task latency + counters + classifier label.
async fn run_shape_h(
    pool: Arc<PgPool>,
    cfg: &CellConfig,
) -> (Histogram<u64>, u64, u64, String, HashMap<String, u64>) {
    let n = cfg.n_backends;
    let duration = Duration::from_secs(cfg.duration_secs);
    let barrier = Arc::new(Barrier::new(n));
    let zipf_cdf: Option<Arc<Vec<f64>>> = if cfg.shape == Shape::Zipfian {
        Some(Arc::new(build_zipf_cdf(cfg.shape.g(), 1.0)))
    } else {
        None
    };

    let snap_start = fetch_snapshot(&pool).await;
    let wall_t0 = Instant::now();

    let mut handles = Vec::with_capacity(n);
    for backend_idx in 0..n {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let zipf = zipf_cdf.clone();
        let shape = cfg.shape;
        handles.push(tokio::spawn(async move {
            let mut hist: Histogram<u64> =
                Histogram::new_with_bounds(1, HIST_HIGH_US, HIST_SIG_FIG)
                    .expect("histogram bounds");
            let mut applies: u64 = 0;
            let mut errors: u64 = 0;
            let mut method_counts: HashMap<String, u64> = HashMap::new();

            let seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0)
                .wrapping_add(
                    (backend_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                );
            let mut rng = SmallRng::seed_from_u64(seed);

            barrier.wait().await;
            let start = Instant::now();
            let mut iter: usize = 0;
            while start.elapsed() < duration {
                let sku = pick_sku(
                    shape,
                    backend_idx,
                    n,
                    iter,
                    &mut rng,
                    zipf.as_deref().map(|v| v.as_slice()),
                );
                let method = pick_method(shape, iter);
                if shape == Shape::MixedMethod {
                    *method_counts.entry(method.to_string()).or_insert(0) += 1;
                }
                let issue_id: i64 =
                    (backend_idx as i64) * 10_000_000_000 + iter as i64;
                let t0 = Instant::now();
                let r: Result<(i32,), sqlx::Error> = sqlx::query_as(
                    "SELECT error_code FROM poc_ledger_apply($1, $2, $3, $4, $5)",
                )
                .bind(sku)
                .bind(LOCATION_ID)
                .bind(1_i64)
                .bind(issue_id)
                .bind(method)
                .fetch_one(&*pool)
                .await;
                let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;
                match r {
                    Ok((ec,)) => {
                        if ec == 0 {
                            let _ = hist.record(dt_us.max(1).min(HIST_HIGH_US));
                            applies += 1;
                        } else {
                            errors += 1;
                        }
                    }
                    Err(_) => {
                        errors += 1;
                    }
                }
                iter += 1;
            }
            (hist, applies, errors, method_counts)
        }));
    }

    let mut merged: Histogram<u64> =
        Histogram::new_with_bounds(1, HIST_HIGH_US, HIST_SIG_FIG).unwrap();
    let mut total_applies: u64 = 0;
    let mut total_errors: u64 = 0;
    let mut total_methods: HashMap<String, u64> = HashMap::new();
    for h in handles {
        let (hist, applies, errors, methods) =
            h.await.expect("backend task panicked");
        merged.add(&hist).expect("histogram merge");
        total_applies += applies;
        total_errors += errors;
        for (k, v) in methods {
            *total_methods.entry(k).or_insert(0) += v;
        }
    }

    let wall_ms = wall_t0.elapsed().as_millis() as i64;
    let snap_end = fetch_snapshot(&pool).await;
    let label =
        fetch_classifier_label(&pool, &snap_start.0, &snap_end.0, wall_ms).await;
    let _ = total_errors;
    (merged, total_applies, total_errors, label, total_methods)
}

pub async fn fetch_deadlocks(pool: &PgPool) -> i64 {
    let row: (i64,) = sqlx::query_as(
        "SELECT COALESCE(deadlocks, 0)::BIGINT FROM pg_stat_database WHERE datname = 'acct_poc_queue'"
    )
    .fetch_one(pool)
    .await
    .unwrap_or_else(|_| (0,));
    row.0
}

/// Run a single cell — `runs` × duration_secs with `settle_secs` gap.
pub async fn run_cell(pool: Arc<PgPool>, cfg: &CellConfig) -> CellResult {
    let mut runs: Vec<CellRun> = Vec::with_capacity(cfg.runs);
    for r in 0..cfg.runs {
        reset_state(&pool).await.expect("reset_state");
        pre_seed(&pool, cfg.shape).await.expect("pre_seed");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let dl_before = fetch_deadlocks(&pool).await;
        let sampler = if cfg.with_sampler {
            Some(
                PgLocksSampler::spawn((*pool).clone(), cfg.sampler_interval_ms)
                    .await,
            )
        } else {
            None
        };

        let (hist, applies, errors, label, _methods) =
            run_shape_h(pool.clone(), cfg).await;
        let dl_after = fetch_deadlocks(&pool).await;

        let report = if let Some(s) = sampler {
            Some(s.shutdown().await)
        } else {
            None
        };

        let p50 = hist.value_at_quantile(0.50);
        let p99 = hist.value_at_quantile(0.99);
        let p999 = hist.value_at_quantile(0.999);
        let throughput = (applies as f64) / cfg.duration_secs as f64;

        runs.push(CellRun {
            run_idx: r,
            total_applies: applies,
            errors,
            throughput,
            p50_us: p50,
            p99_us: p99,
            p999_us: p999,
            deadlocks_delta: dl_after - dl_before,
            classifier_label: label,
            sampler: report,
        });

        if r + 1 < cfg.runs {
            tokio::time::sleep(Duration::from_secs(cfg.settle_secs)).await;
        }
    }

    // Per-metric median + IQR over the N runs.
    let throughputs: Vec<f64> = runs.iter().map(|r| r.throughput).collect();
    let p50s: Vec<u64> = runs.iter().map(|r| r.p50_us).collect();
    let p99s: Vec<u64> = runs.iter().map(|r| r.p99_us).collect();
    let p999s: Vec<u64> = runs.iter().map(|r| r.p999_us).collect();
    let dls: Vec<i64> = runs.iter().map(|r| r.deadlocks_delta).collect();

    let (t_med, t_iqr) = stats_f64(&throughputs);
    let (p50_med, p50_iqr) = stats_u64(&p50s);
    let (p99_med, p99_iqr) = stats_u64(&p99s);
    let (p999_med, p999_iqr) = stats_u64(&p999s);
    let dl_med = median_i64(&dls);

    CellResult {
        shape: cfg.shape.name().to_string(),
        n_backends: cfg.n_backends,
        runs,
        throughput_med: t_med,
        throughput_iqr: t_iqr,
        p50_med_us: p50_med,
        p50_iqr_us: p50_iqr,
        p99_med_us: p99_med,
        p99_iqr_us: p99_iqr,
        p999_med_us: p999_med,
        p999_iqr_us: p999_iqr,
        deadlocks_med: dl_med,
        sampler_enabled: cfg.with_sampler,
    }
}

fn quantile_sorted_f64(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (q * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn quantile_sorted_u64(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (q * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn stats_f64(vs: &[f64]) -> (f64, f64) {
    let mut s = vs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = quantile_sorted_f64(&s, 0.50);
    let q1 = quantile_sorted_f64(&s, 0.25);
    let q3 = quantile_sorted_f64(&s, 0.75);
    (med, q3 - q1)
}

fn stats_u64(vs: &[u64]) -> (u64, u64) {
    let mut s = vs.to_vec();
    s.sort_unstable();
    let med = quantile_sorted_u64(&s, 0.50);
    let q1 = quantile_sorted_u64(&s, 0.25);
    let q3 = quantile_sorted_u64(&s, 0.75);
    (med, q3.saturating_sub(q1))
}

fn median_i64(vs: &[i64]) -> i64 {
    if vs.is_empty() {
        return 0;
    }
    let mut s = vs.to_vec();
    s.sort_unstable();
    s[s.len() / 2]
}

// ──────────────────────────────────────────────────────────────────────
// M9.3 (acct-4d4n.22): GUC sweep — apply ALTER SYSTEM + pg_reload_conf
// per cell across (batch_window_us × batch_size_max × synchronous_commit)
// for fan_out + small_batch shapes. Both poc_ledger.batch_window_us and
// poc_ledger.batch_size_max are GucContext::Sighup (verified in src/lib.rs),
// so the reload propagates to the committer bgworker + new pool conns.
// synchronous_commit is PG-builtin userset; ALTER SYSTEM SET + reload
// updates the cluster default which any session without a SET LOCAL picks
// up.
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct GucCombo {
    pub batch_window_us: i32,
    pub batch_size_max: i32,
    pub sync_commit: bool,
}

impl GucCombo {
    pub fn label(&self) -> String {
        format!(
            "bw={}us bs={} sc={}",
            self.batch_window_us,
            self.batch_size_max,
            if self.sync_commit { "on" } else { "off" }
        )
    }
}

#[derive(Debug)]
pub struct GucSweepCell {
    pub shape: String,
    pub guc: GucCombo,
    pub cell: CellResult,
}

/// Apply one GUC combo cluster-wide via ALTER SYSTEM + pg_reload_conf.
/// Each statement runs on its own pool connection (sqlx auto-commit per
/// `query`), avoiding the multi-stmt-in-one-txn pitfall noted in
/// `feedback_psql_c_single_transaction`. ALTER SYSTEM can't legally run
/// inside a transaction block anyway. Settles 500ms after reload so the
/// committer bgworker observes the new values before the workload starts.
pub async fn apply_gucs(pool: &PgPool, c: GucCombo) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "ALTER SYSTEM SET poc_ledger.batch_window_us = {}",
        c.batch_window_us
    ))
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "ALTER SYSTEM SET poc_ledger.batch_size_max = {}",
        c.batch_size_max
    ))
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "ALTER SYSTEM SET synchronous_commit = '{}'",
        if c.sync_commit { "on" } else { "off" }
    ))
    .execute(pool)
    .await?;
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}

/// Reset all three GUCs to their compiled-in defaults via ALTER SYSTEM
/// RESET + pg_reload_conf. Called once at sweep end so subsequent
/// non-sweep work doesn't inherit the last cell's GUCs.
pub async fn reset_gucs(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("ALTER SYSTEM RESET poc_ledger.batch_window_us")
        .execute(pool)
        .await?;
    sqlx::query("ALTER SYSTEM RESET poc_ledger.batch_size_max")
        .execute(pool)
        .await?;
    sqlx::query("ALTER SYSTEM RESET synchronous_commit")
        .execute(pool)
        .await?;
    sqlx::query("SELECT pg_reload_conf()")
        .execute(pool)
        .await?;
    Ok(())
}

/// Read back the live GUC values (post-reload). Used by callers that
/// want to log the observed values into the per-cell JSON for audit.
pub async fn observed_gucs(pool: &PgPool) -> (i32, i32, String) {
    let bw: (String,) =
        sqlx::query_as("SELECT current_setting('poc_ledger.batch_window_us')")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|_| ("0".to_string(),));
    let bs: (String,) =
        sqlx::query_as("SELECT current_setting('poc_ledger.batch_size_max')")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|_| ("0".to_string(),));
    let sc: (String,) =
        sqlx::query_as("SELECT current_setting('synchronous_commit')")
            .fetch_one(pool)
            .await
            .unwrap_or_else(|_| ("?".to_string(),));
    (
        bw.0.parse().unwrap_or(0),
        bs.0.parse().unwrap_or(0),
        sc.0,
    )
}

// ──────────────────────────────────────────────────────────────────────

/// Format a result block for human reading + persistence.
pub fn fmt_result(r: &ShapeResult) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "shape: {:<14} g={:<5} N={} duration={}s\n",
        r.name, r.g, r.n_backends, r.duration_secs
    ));
    s.push_str(&format!(
        "  applies: {} (errors: {})\n",
        r.total_applies, r.errors
    ));
    s.push_str(&format!(
        "  throughput: {:.0} events/sec\n",
        r.throughput
    ));
    s.push_str(&format!(
        "  latency: p50={}µs p99={}µs p99.9={}µs\n",
        r.p50_us, r.p99_us, r.p999_us
    ));
    s.push_str(&format!(
        "  bottleneck_classify: {}\n",
        r.classifier_label
    ));
    if let Some(m) = &r.method_breakdown {
        let total: u64 = m.values().sum();
        let pct = |k: &str| -> f64 {
            m.get(k).copied().unwrap_or(0) as f64 * 100.0 / total.max(1) as f64
        };
        s.push_str(&format!(
            "  method mix: fifo={:.1}% avg={:.1}% std={:.1}%\n",
            pct("fifo"),
            pct("avg"),
            pct("std")
        ));
    }
    s
}
