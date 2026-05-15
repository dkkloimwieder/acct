//! M9.1 (acct-4d4n.20) — workload-generator entry-points for the 6
//! bake-off shapes (spec §5.2). Each entry-point is `#[ignore]`'d so the
//! default `cargo test` run doesn't fire them; opt-in via
//! `cargo test --test bench_m9_workloads -- --ignored --nocapture`.
//!
//! Env knobs:
//!   POC_M9_N        — backend count (default 4)
//!   POC_M9_DURATION — seconds per shape (default 60)
//!   POC_M9_OUTPUT   — output file path (default bench/results-m91-shapes.md)
//!   POC_M9_SHAPE    — run a single shape by name (omit to run all 6)
//!
//! Each test that runs ALL shapes writes a fresh markdown to OUTPUT;
//! single-shape runs append.

#![cfg(test)]

mod common;

use common::m9_runner::{
    connect_pool, fmt_result, pre_seed, reset_state, run_shape, Shape,
    ShapeResult, WorkloadConfig,
};
use std::sync::Arc;
use std::time::Duration;

fn env_n() -> usize {
    std::env::var("POC_M9_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
}

fn env_duration() -> u64 {
    std::env::var("POC_M9_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn env_output() -> String {
    std::env::var("POC_M9_OUTPUT")
        .unwrap_or_else(|_| "bench/results-m91-shapes.md".to_string())
}

async fn run_one(shape: Shape) -> ShapeResult {
    let n = env_n();
    let duration = env_duration();
    // Pool sized to N + 4 head-room for setup/snapshot queries.
    let pool = Arc::new(connect_pool((n as u32) + 4).await);
    reset_state(&pool).await.expect("reset_state");
    pre_seed(&pool, shape).await.expect("pre_seed");
    // Small breath after reset so any reaped backends fully clear.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cfg = WorkloadConfig {
        shape,
        n_backends: n,
        duration_secs: duration,
    };
    run_shape(pool, &cfg).await
}

fn write_results(results: &[ShapeResult]) {
    let out = env_output();
    let n = env_n();
    let dur = env_duration();
    let mut body = String::new();
    body.push_str(&format!(
        "# M9.1 (acct-4d4n.20) — workload-generator shapes\n\n"
    ));
    body.push_str(&format!(
        "Per-shape run at N={n} backends × duration={dur}s. Workload-generator harness `tests/bench_m9_workloads.rs`.\n\n"
    ));
    body.push_str("Methods: shapes 1–5 use `mock` (queue+committer isolation; matches M4.1 convention). Shape 6 rotates `fifo`→`avg`→`std` per call.\n\n");
    body.push_str("Latency captured via per-call `Instant::now()` deltas in microseconds, sorted-then-percentile aggregated. Classifier label sourced from `poc_ledger_bottleneck_classify` over the start/end snapshot pair for the workload window.\n\n");
    body.push_str("## Summary\n\n");
    body.push_str(
        "| shape | g | applies | errors | throughput evps | p50 µs | p99 µs | p99.9 µs | classifier |\n",
    );
    body.push_str(
        "|---|---|---|---|---|---|---|---|---|\n",
    );
    for r in results {
        body.push_str(&format!(
            "| {} | {} | {} | {} | {:.0} | {} | {} | {} | {} |\n",
            r.name,
            r.g,
            r.total_applies,
            r.errors,
            r.throughput,
            r.p50_us,
            r.p99_us,
            r.p999_us,
            r.classifier_label,
        ));
    }
    body.push_str("\n## Per-shape detail\n\n");
    for r in results {
        body.push_str("```\n");
        body.push_str(&fmt_result(r));
        body.push_str("```\n\n");
    }
    // ISO-8601 UTC via /bin/date — keeps the harness dep-free.
    let ts = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    body.push_str(&format!("Generated: {ts}\n"));

    std::fs::write(&out, body).expect("write results file");
    println!("==> wrote {}", out);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_fan_in() {
    let r = run_one(Shape::FanIn).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_fan_out() {
    let r = run_one(Shape::FanOut).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_balanced() {
    let r = run_one(Shape::Balanced).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_zipfian() {
    let r = run_one(Shape::Zipfian).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_small_batch() {
    let r = run_one(Shape::SmallBatch).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_mixed_method() {
    let r = run_one(Shape::MixedMethod).await;
    println!("{}", fmt_result(&r));

    // Pin the spec acceptance: shape 6 should dispatch each of fifo/avg/std
    // within ±5% of 33.3%. Per-call uniform rotation deterministically
    // hits 33/33/33; the assertion guards against drift in pick_method.
    if let Some(m) = &r.method_breakdown {
        let total: u64 = m.values().sum();
        for k in ["fifo", "avg", "std"] {
            let pct = m.get(k).copied().unwrap_or(0) as f64 * 100.0
                / total.max(1) as f64;
            assert!(
                (28.3..=38.3).contains(&pct),
                "method {k} dispatched {pct:.2}% of {total} applies (expected 33.3%±5)"
            );
        }
    }

    write_results(&[r]);
}

/// Sequential all-shapes run. Slower than the individual entry-points
/// because each shape resets + pre-seeds + drives 60s of load, so this
/// totals ~6×duration. Useful for the M9.1 acceptance run.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_all_shapes() {
    let shapes = [
        Shape::FanIn,
        Shape::FanOut,
        Shape::Balanced,
        Shape::Zipfian,
        Shape::SmallBatch,
        Shape::MixedMethod,
    ];
    let env_single = std::env::var("POC_M9_SHAPE").ok();
    let mut results: Vec<ShapeResult> = Vec::new();
    for s in shapes {
        if let Some(ref name) = env_single {
            if s.name() != name {
                continue;
            }
        }
        let r = run_one(s).await;
        println!("{}", fmt_result(&r));
        results.push(r);
        // Gap between shapes to let the postmaster settle.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    write_results(&results);
}
