//! M8.1 (acct-toj6) — workload-generator entry-points for the 9
//! bake-off shapes (spec §5.2 + Amendment 01 §S9).
//!
//! Each entry-point is `#[ignore]`'d so default `cargo test` doesn't
//! fire them; opt-in via:
//!
//!   cargo test --release --test bench_m8_workloads \
//!     --features pg18 --no-default-features \
//!     -- --ignored --nocapture --test-threads=1
//!
//! Env knobs:
//!   POC_M8_N         — backend count (default 4)
//!   POC_M8_DURATION  — seconds per shape (default 60)
//!   POC_M8_OUTPUT    — output md path (default bench/results-m81-shapes.md)
//!   POC_M8_SHAPE     — run single shape by name (`s1_fan_out_simple`, ...)
//!   POC_M8_S9_MARGIN — S9 margin (default 0; -1 → must-fail)

#![cfg(test)]

#[path = "common/m8_runner.rs"]
mod m8_runner;

use m8_runner::{
    Shape, ShapeResult, WorkloadConfig, connect_pool, fmt_result, pre_seed_shape, reset_state,
    run_shape,
};
use std::sync::Arc;
use std::time::Duration;

fn env_n() -> usize {
    std::env::var("POC_M8_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4)
}

fn env_duration() -> u64 {
    std::env::var("POC_M8_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn env_output() -> String {
    std::env::var("POC_M8_OUTPUT").unwrap_or_else(|_| "bench/results-m81-shapes.md".to_string())
}

fn env_s9_margin() -> i64 {
    std::env::var("POC_M8_S9_MARGIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

async fn run_one(shape: Shape) -> ShapeResult {
    let n = env_n();
    let duration = env_duration();
    let s9_margin = env_s9_margin();

    // Pool sized to N + 4 head-room for setup/poll queries.
    let pool = Arc::new(connect_pool((n as u32) + 4).await);
    reset_state(&pool).await.expect("reset_state");
    pre_seed_shape(&pool, shape, n).await.expect("pre_seed");
    // Breath after reset so any reaped backends fully clear.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cfg = WorkloadConfig {
        shape,
        n_backends: n,
        duration_secs: duration,
        s9_margin,
    };
    run_shape(pool, &cfg).await
}

fn ensure_bench_dir() {
    if let Some(parent) = std::path::Path::new(&env_output()).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
}

fn write_results(results: &[ShapeResult]) {
    ensure_bench_dir();
    let out = env_output();
    let n = env_n();
    let dur = env_duration();
    let mut body = String::new();
    body.push_str("# M8.1 (acct-toj6) — workload-generator shapes\n\n");
    body.push_str(&format!(
        "Per-shape run at N={n} backends × duration={dur}s. \
         Workload-generator harness `tests/bench_m8_workloads.rs`.\n\n"
    ));
    body.push_str(
        "Latency = enqueue → submission_status terminal-state observed. \
         Submit-and-poll per backend (single outstanding envelope).\n\n",
    );
    body.push_str("## Summary\n\n");
    body.push_str(
        "| shape | g | K | committed | failed | throughput evps | \
         p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |\n",
    );
    body.push_str(
        "|---|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for r in results {
        body.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.0} | {} | {} | {} | {:.2} | {} |\n",
            r.name,
            r.g,
            r.k,
            r.committed,
            r.failed,
            r.throughput_eps,
            r.p50_us,
            r.p99_us,
            r.p999_us,
            r.avg_envelopes_per_sb,
            r.router_superbatch_delta,
        ));
    }
    body.push_str("\n## Per-shape detail\n\n");
    for r in results {
        body.push_str("```\n");
        body.push_str(&fmt_result(r));
        body.push_str("```\n\n");
    }
    let ts = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    body.push_str(&format!("\nGenerated: {ts}\n"));

    std::fs::write(&out, body).expect("write results file");
    println!("==> wrote {out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s1_fan_out_simple() {
    let r = run_one(Shape::S1FanOutSimple).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s2_fan_out_wo() {
    let r = run_one(Shape::S2FanOutWo).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s3_fan_contested_wo() {
    let r = run_one(Shape::S3FanContestedWo).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s4_fan_in_wo() {
    let r = run_one(Shape::S4FanInWo).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s5_hot_pool() {
    let r = run_one(Shape::S5HotPool).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s6_large_wo() {
    let r = run_one(Shape::S6LargeWo).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s7_very_large_wo() {
    let r = run_one(Shape::S7VeryLargeWo).await;
    println!("{}", fmt_result(&r));
    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s8_mixed_event_mixed_method() {
    let r = run_one(Shape::S8MixedEventMixedMethod).await;
    println!("{}", fmt_result(&r));

    // Pin the spec acceptance: inv_adjust ≈ wo_complete ≈ 50/50
    // within ±5%. Per-iter alternation deterministically hits 50/50;
    // assertion guards against drift.
    if let Some(m) = &r.method_breakdown {
        let total: u64 = m.values().sum();
        for k in ["inv_adjust", "wo_complete"] {
            let pct =
                m.get(k).copied().unwrap_or(0) as f64 * 100.0 / total.max(1) as f64;
            assert!(
                (45.0..=55.0).contains(&pct),
                "event {k} fired {pct:.2}% of {total} envelopes (expected 50%±5)"
            );
        }
    }

    write_results(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_s9_causal_chain() {
    let r = run_one(Shape::S9CausalChain).await;
    println!("{}", fmt_result(&r));

    // S9 invariant per Amendment 01: margin ≥ 0 commits clean; margin
    // = -1 must produce failed E2 (cascade-failed E3). The runner
    // treats margin=-1 with E2-failed as "committed" in the
    // workload-invariant sense.
    let margin = env_s9_margin();
    if margin == 0 {
        assert!(
            r.failed == 0 || (r.failed as f64) / (r.total_envelopes.max(1) as f64) < 0.01,
            "S9 margin=0: expected no spurious failures, got failed={}/{}",
            r.failed,
            r.total_envelopes,
        );
    }
    write_results(&[r]);
}

/// Sequential all-shapes run. ~9 × duration_secs total wall-clock.
/// Useful for the M8.1 acceptance run at N=4 60s.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m8_all_shapes() {
    let single = std::env::var("POC_M8_SHAPE")
        .ok()
        .and_then(|s| Shape::from_name(&s));
    let shapes: Vec<Shape> = match single {
        Some(s) => vec![s],
        None => Shape::all().to_vec(),
    };

    let mut results: Vec<ShapeResult> = Vec::new();
    for s in shapes {
        let r = run_one(s).await;
        println!("{}", fmt_result(&r));
        results.push(r);
        // Gap between shapes for committer settle.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    write_results(&results);
}
