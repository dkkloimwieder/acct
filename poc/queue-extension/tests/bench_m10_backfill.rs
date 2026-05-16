//! M10.1 (acct-4d4n.23) — P1/P3 backfill cells.
//!
//! Spec §4.2 criteria need fan_out at N=1 and N=16, both at default
//! GUCs, both with the same 5×60s + 30s settle methodology as
//! M9.2/M9.3. M9.3 swept fan_out at N ∈ {4, 32, 128}; these two
//! complete the comparison set:
//!
//!   P1 (fan_out N=32 ≥ 24× N=1): need N=1 baseline.
//!       M9.3 N=32 sc=on best ≈ 5337 tps. Bar: N=1 ≤ ~222 tps.
//!   P3 (fan_out 16 backends @ 50% peak; p99 < 50ms): need N=16 cell.
//!       50% of M9.3 N=128 sc=on peak (6379 tps) ≈ 3190 tps.
//!       Natural fan_out throughput at N=16 lands close to that
//!       (between N=4=1077 and N=32=5337) without explicit throttling.
//!
//! Default GUCs (per src/lib.rs registration):
//!   poc_ledger.batch_window_us  = 500
//!   poc_ledger.batch_size_max   = 1024
//!   synchronous_commit          = on
//!
//! Run via:
//!   cargo test --release --test bench_m10_backfill m10_backfill_all \
//!     --features pg18 --no-default-features -- --ignored --nocapture
//!
//! Env knobs (smoke):
//!   POC_M10_DURATION   — seconds per run        (default 60)
//!   POC_M10_RUNS       — replications per cell  (default 5)
//!   POC_M10_SETTLE     — settle gap secs        (default 30)
//!   POC_M10_OUTPUT_MD  — markdown path
//!   POC_M10_OUTPUT_JS  — JSON path

#![cfg(test)]

mod common;

use common::m9_runner::{
    apply_gucs, connect_pool, reset_gucs, run_cell, CellConfig, GucCombo, Shape,
};
use std::sync::Arc;

fn env_dur() -> u64 {
    std::env::var("POC_M10_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn env_runs() -> usize {
    std::env::var("POC_M10_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

fn env_settle() -> u64 {
    std::env::var("POC_M10_SETTLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}

fn env_out_md() -> String {
    std::env::var("POC_M10_OUTPUT_MD")
        .unwrap_or_else(|_| "bench/results-m10-backfill.md".to_string())
}

fn env_out_js() -> String {
    std::env::var("POC_M10_OUTPUT_JS")
        .unwrap_or_else(|_| "bench/results-m10-backfill.json".to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m10_backfill_all() {
    let ns: [usize; 2] = [1, 16];
    let pool = Arc::new(connect_pool((16_u32) + 8).await);

    // Pin GUCs to the recommended-design-v2 defaults (Q5/Q6 leans):
    // bw=500 bs=1024 sc=on. M9.3 measured peak octant for fan_out
    // sits at exactly this corner.
    let guc = GucCombo {
        batch_window_us: 500,
        batch_size_max: 1024,
        sync_commit: true,
    };
    apply_gucs(&pool, guc).await.expect("apply_gucs");

    let mut cells = Vec::new();
    for &n in &ns {
        let cfg = CellConfig {
            shape: Shape::FanOut,
            n_backends: n,
            duration_secs: env_dur(),
            runs: env_runs(),
            settle_secs: env_settle(),
            with_sampler: true,
            sampler_interval_ms: 100,
        };
        eprintln!(
            "==> M10 backfill cell shape=fan_out N={} bw=500 bs=1024 sc=on",
            n
        );
        let cell = run_cell(pool.clone(), &cfg).await;
        eprintln!(
            "    tps med={:.0} IQR={:.0}  p50 med={}µs  p99 med={}µs IQR={}µs  deadlocks_med={}",
            cell.throughput_med,
            cell.throughput_iqr,
            cell.p50_med_us,
            cell.p99_med_us,
            cell.p99_iqr_us,
            cell.deadlocks_med
        );
        cells.push(cell);
    }

    reset_gucs(&pool).await.expect("reset_gucs");

    // Render
    let n1 = &cells[0];
    let n16 = &cells[1];
    let mut s = String::new();
    s.push_str("# M10.1 (acct-4d4n.23) — P1/P3 backfill\n\n");
    s.push_str(&format!(
        "Per-cell {runs} × {dur}s with {settle}s settle gap. Default GUCs: bw=500 bs=1024 sc=on.\n\n",
        runs = env_runs(),
        dur = env_dur(),
        settle = env_settle()
    ));
    s.push_str("## fan_out cells\n\n");
    s.push_str("| N | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med |\n");
    s.push_str("|---|---|---|---|---|---|---|---|\n");
    for c in &cells {
        s.push_str(&format!(
            "| {} | {:.0} | {:.0} | {} | {} | {} | {} | {} |\n",
            c.n_backends,
            c.throughput_med,
            c.throughput_iqr,
            c.p50_med_us,
            c.p99_med_us,
            c.p99_iqr_us,
            c.p999_med_us,
            c.deadlocks_med,
        ));
    }
    s.push_str("\n## Per-run detail\n\n");
    s.push_str("| N | run | applies | errors | tps | p50 µs | p99 µs | p99.9 µs | deadlocks Δ | classifier |\n");
    s.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
    for c in &cells {
        for run in &c.runs {
            s.push_str(&format!(
                "| {} | {} | {} | {} | {:.0} | {} | {} | {} | {} | {} |\n",
                c.n_backends,
                run.run_idx,
                run.total_applies,
                run.errors,
                run.throughput,
                run.p50_us,
                run.p99_us,
                run.p999_us,
                run.deadlocks_delta,
                run.classifier_label,
            ));
        }
    }

    // P1 + P3 evaluation
    let n1_tps = n1.throughput_med;
    let n16_tps = n16.throughput_med;
    let n16_p99_ms = (n16.p99_med_us as f64) / 1000.0;
    // P1 target uses M9.3 fan_out N=32 sc=on best ≈ 5337 tps.
    let m93_n32_best_tps: f64 = 5337.0;
    let p1_ratio = m93_n32_best_tps / n1_tps.max(1.0);
    let p1_pass = p1_ratio >= 24.0;
    // P3 target: p99 < 50 ms.
    let p3_pass = n16_p99_ms < 50.0;

    s.push_str("\n## Criteria evaluation\n\n");
    s.push_str(&format!(
        "- **P1** (fan_out N=32 ≥ 24× N=1): N=32 best from M9.3 ≈ {:.0} tps vs N=1 baseline {:.0} tps; ratio = **{:.1}×**. **{}**\n",
        m93_n32_best_tps,
        n1_tps,
        p1_ratio,
        if p1_pass { "PASS" } else { "FAIL" }
    ));
    s.push_str(&format!(
        "- **P3** (fan_out N=16 p99 < 50ms): measured p99 = **{:.1} ms**. **{}**\n",
        n16_p99_ms,
        if p3_pass { "PASS" } else { "FAIL" }
    ));

    let ts = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    s.push_str(&format!("\nGenerated: {ts}\n"));

    std::fs::write(env_out_md(), s).expect("write md");

    // JSON
    let records: Vec<serde_json::Value> = cells
        .iter()
        .map(|c| {
            let per_run: Vec<serde_json::Value> = c
                .runs
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "run_idx": r.run_idx,
                        "applies": r.total_applies,
                        "errors": r.errors,
                        "throughput": r.throughput,
                        "p50_us": r.p50_us,
                        "p99_us": r.p99_us,
                        "p999_us": r.p999_us,
                        "deadlocks_delta": r.deadlocks_delta,
                        "classifier": r.classifier_label,
                    })
                })
                .collect();
            serde_json::json!({
                "shape": c.shape,
                "n_backends": c.n_backends,
                "guc": { "batch_window_us": 500, "batch_size_max": 1024, "synchronous_commit": "on" },
                "throughput_med": c.throughput_med,
                "throughput_iqr": c.throughput_iqr,
                "p50_med_us": c.p50_med_us,
                "p99_med_us": c.p99_med_us,
                "p99_iqr_us": c.p99_iqr_us,
                "p999_med_us": c.p999_med_us,
                "deadlocks_med": c.deadlocks_med,
                "runs": per_run,
            })
        })
        .collect();
    let top = serde_json::json!({
        "spec": "M10.1 P1/P3 backfill",
        "config": {
            "shape": "fan_out",
            "ns": ns,
            "runs_per_cell": env_runs(),
            "duration_secs": env_dur(),
            "settle_secs": env_settle(),
            "guc": { "batch_window_us": 500, "batch_size_max": 1024, "synchronous_commit": "on" },
        },
        "evaluations": {
            "p1": { "n32_best_tps_m93": m93_n32_best_tps, "n1_tps": n1_tps, "ratio": p1_ratio, "pass": p1_pass },
            "p3": { "n16_p99_ms": n16_p99_ms, "bar_ms": 50.0, "pass": p3_pass },
        },
        "cells": records,
    });
    std::fs::write(
        env_out_js(),
        serde_json::to_string_pretty(&top).expect("serialize"),
    )
    .expect("write json");

    println!("==> wrote {} + {}", env_out_md(), env_out_js());

    assert!(p1_pass, "P1 failed: ratio {:.1}× < 24×", p1_ratio);
    assert!(p3_pass, "P3 failed: p99 {:.1}ms ≥ 50ms", n16_p99_ms);
}
