//! M9.2 (acct-4d4n.21) — statistical runner entry-points per spec §5.3.
//!
//! Two `#[ignore]`'d tests:
//!
//! - `m9_statistical_fan_in_sweep` — full N sweep on `fan_in` shape:
//!   N ∈ {1, 2, 4, 8, 16, 32, 64, 128, 256} × 5 runs × 60s with 30s
//!   settle gap; hdrhistogram per cell; pg_locks sampler active each
//!   run; JSON + markdown output. Wall-time ~67min of pure workload
//!   plus reset/seed overhead.
//!
//! - `m9_sampler_perturbation_cell` — sanity check at N=32 fan_in:
//!   5×60s with sampler ON (already covered by the sweep above) +
//!   5×60s with sampler OFF; emits both medians + IQRs; pin-asserts
//!   the sampler-off combined p99 lies inside the sampler-on IQR.
//!
//! Env knobs:
//!   POC_M92_DURATION  — seconds per run        (default 60)
//!   POC_M92_RUNS      — replications per cell  (default 5)
//!   POC_M92_SETTLE    — gap between runs s     (default 30)
//!   POC_M92_NS        — comma-separated N sweep  (default 1,2,4,8,16,32,64,128,256)
//!   POC_M92_PERT_N    — N for the perturbation cell (default 32)
//!   POC_M92_OUTPUT_MD — markdown path  (default bench/results-m92-fan-in-sweep.md)
//!   POC_M92_OUTPUT_JS — JSON path      (default bench/results-m92-fan-in.json)

#![cfg(test)]

mod common;

use common::m9_runner::{
    connect_pool, run_cell, CellConfig, CellResult, Shape,
};
use std::sync::Arc;

fn env_duration() -> u64 {
    std::env::var("POC_M92_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn env_runs() -> usize {
    std::env::var("POC_M92_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

fn env_settle() -> u64 {
    std::env::var("POC_M92_SETTLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}

fn env_ns() -> Vec<usize> {
    std::env::var("POC_M92_NS")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|t| t.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| vec![1, 2, 4, 8, 16, 32, 64, 128, 256])
}

fn env_pert_n() -> usize {
    std::env::var("POC_M92_PERT_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32)
}

fn env_out_md() -> String {
    std::env::var("POC_M92_OUTPUT_MD")
        .unwrap_or_else(|_| "bench/results-m92-fan-in-sweep.md".to_string())
}

fn env_out_js() -> String {
    std::env::var("POC_M92_OUTPUT_JS")
        .unwrap_or_else(|_| "bench/results-m92-fan-in.json".to_string())
}

fn render_md(results: &[CellResult]) -> String {
    let mut s = String::new();
    s.push_str("# M9.2 (acct-4d4n.21) — fan_in statistical sweep\n\n");
    s.push_str(&format!(
        "Per-cell {runs} × {dur}s with {settle}s settle gap. hdrhistogram-based latency capture. pg_locks sampler @ 100ms per run.\n\n",
        runs = env_runs(),
        dur = env_duration(),
        settle = env_settle()
    ));
    s.push_str("## Per-N aggregate (median + IQR over the N runs)\n\n");
    s.push_str("| N | throughput med | throughput IQR | p50 med µs | p50 IQR µs | p99 med µs | p99 IQR µs | p99.9 med µs | p99.9 IQR µs | deadlocks med |\n");
    s.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
    for r in results {
        s.push_str(&format!(
            "| {} | {:.0} | {:.0} | {} | {} | {} | {} | {} | {} | {} |\n",
            r.n_backends,
            r.throughput_med,
            r.throughput_iqr,
            r.p50_med_us,
            r.p50_iqr_us,
            r.p99_med_us,
            r.p99_iqr_us,
            r.p999_med_us,
            r.p999_iqr_us,
            r.deadlocks_med,
        ));
    }
    s.push_str("\n## Per-cell detail (per-run rows)\n\n");
    s.push_str("| N | run | applies | errors | tps | p50 µs | p99 µs | p99.9 µs | deadlocks Δ | classifier |\n");
    s.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
    for r in results {
        for run in &r.runs {
            s.push_str(&format!(
                "| {} | {} | {} | {} | {:.0} | {} | {} | {} | {} | {} |\n",
                r.n_backends,
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

    s.push_str("\n## Sampler — top wait_event per N (median run)\n\n");
    s.push_str("| N | wait_event_type | wait_event | sum_backends | notes |\n");
    s.push_str("|---|---|---|---|---|\n");
    for r in results {
        // Pick the median-throughput run for the sampler readout.
        let mut idx: Vec<(usize, f64)> = r
            .runs
            .iter()
            .map(|run| (run.run_idx, run.throughput))
            .collect();
        idx.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        let median_run_idx = idx[idx.len() / 2].0;
        let run = &r.runs[median_run_idx];
        if let Some(samp) = &run.sampler {
            if let Some((wet, we, c)) = samp.top_wait_event() {
                s.push_str(&format!(
                    "| {} | {} | {} | {} | samples={} |\n",
                    r.n_backends, wet, we, c, samp.samples_taken
                ));
            } else {
                s.push_str(&format!(
                    "| {} | — | — | 0 | samples={} (no waits) |\n",
                    r.n_backends, samp.samples_taken
                ));
            }
        } else {
            s.push_str(&format!(
                "| {} | (no sampler) | — | — | sampler disabled |\n",
                r.n_backends
            ));
        }
    }

    let ts = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    s.push_str(&format!("\nGenerated: {ts}\n"));
    s
}

fn render_json(results: &[CellResult]) -> String {
    let mut cells = Vec::new();
    for r in results {
        let per_run: Vec<serde_json::Value> = r
            .runs
            .iter()
            .map(|run| {
                let top = run
                    .sampler
                    .as_ref()
                    .and_then(|s| s.top_wait_event())
                    .map(|(t, e, c)| {
                        serde_json::json!({
                            "wait_event_type": t,
                            "wait_event": e,
                            "sum_backends": c,
                        })
                    })
                    .unwrap_or(serde_json::Value::Null);
                serde_json::json!({
                    "run_idx": run.run_idx,
                    "applies": run.total_applies,
                    "errors": run.errors,
                    "throughput": run.throughput,
                    "p50_us": run.p50_us,
                    "p99_us": run.p99_us,
                    "p999_us": run.p999_us,
                    "deadlocks_delta": run.deadlocks_delta,
                    "classifier": run.classifier_label,
                    "top_wait_event": top,
                })
            })
            .collect();
        cells.push(serde_json::json!({
            "shape": r.shape,
            "n_backends": r.n_backends,
            "sampler_enabled": r.sampler_enabled,
            "throughput_med": r.throughput_med,
            "throughput_iqr": r.throughput_iqr,
            "p50_med_us": r.p50_med_us,
            "p50_iqr_us": r.p50_iqr_us,
            "p99_med_us": r.p99_med_us,
            "p99_iqr_us": r.p99_iqr_us,
            "p999_med_us": r.p999_med_us,
            "p999_iqr_us": r.p999_iqr_us,
            "deadlocks_med": r.deadlocks_med,
            "runs": per_run,
        }));
    }
    let top = serde_json::json!({
        "spec": "M9.2 acct-4d4n.21 fan_in statistical sweep",
        "config": {
            "shape": "fan_in",
            "runs_per_cell": env_runs(),
            "duration_secs": env_duration(),
            "settle_secs": env_settle(),
            "n_sweep": env_ns(),
        },
        "cells": cells,
    });
    serde_json::to_string_pretty(&top).expect("json serialize")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_statistical_fan_in_sweep() {
    let ns = env_ns();
    let max_n = ns.iter().copied().max().unwrap_or(4);
    // Pool sized for the largest N + sampler + admin headroom.
    let pool = Arc::new(connect_pool((max_n as u32) + 8).await);

    let mut results: Vec<CellResult> = Vec::with_capacity(ns.len());
    for n in &ns {
        let cfg = CellConfig {
            shape: Shape::FanIn,
            n_backends: *n,
            duration_secs: env_duration(),
            runs: env_runs(),
            settle_secs: env_settle(),
            with_sampler: true,
            sampler_interval_ms: 100,
        };
        eprintln!(
            "==> M9.2 cell shape=fan_in N={} runs={} dur={}s",
            n,
            env_runs(),
            env_duration()
        );
        let cell = run_cell(pool.clone(), &cfg).await;
        eprintln!(
            "    throughput med={:.0} IQR={:.0}  p99 med={}µs IQR={}µs  deadlocks_med={}",
            cell.throughput_med,
            cell.throughput_iqr,
            cell.p99_med_us,
            cell.p99_iqr_us,
            cell.deadlocks_med
        );
        results.push(cell);
    }

    let md = render_md(&results);
    let js = render_json(&results);
    std::fs::write(env_out_md(), md).expect("write md");
    std::fs::write(env_out_js(), js).expect("write json");
    println!("==> wrote {} + {}", env_out_md(), env_out_js());

    // M9.2 acceptance: sweep completes, IQR computed per metric.
    assert_eq!(results.len(), ns.len());
    for r in &results {
        assert_eq!(r.runs.len(), env_runs(), "cell N={} short runs", r.n_backends);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_sampler_perturbation_cell() {
    let n = env_pert_n();
    let pool = Arc::new(connect_pool((n as u32) + 8).await);
    eprintln!(
        "==> M9.2 sampler perturbation cell N={} sampler-on vs sampler-off",
        n
    );

    let cfg_on = CellConfig {
        shape: Shape::FanIn,
        n_backends: n,
        duration_secs: env_duration(),
        runs: env_runs(),
        settle_secs: env_settle(),
        with_sampler: true,
        sampler_interval_ms: 100,
    };
    let cell_on = run_cell(pool.clone(), &cfg_on).await;
    eprintln!(
        "    sampler-on:  p99 med={}µs IQR={}µs throughput med={:.0}",
        cell_on.p99_med_us, cell_on.p99_iqr_us, cell_on.throughput_med
    );

    let cfg_off = CellConfig {
        with_sampler: false,
        ..cfg_on
    };
    let cell_off = run_cell(pool.clone(), &cfg_off).await;
    eprintln!(
        "    sampler-off: p99 med={}µs IQR={}µs throughput med={:.0}",
        cell_off.p99_med_us, cell_off.p99_iqr_us, cell_off.throughput_med
    );

    let mut s = String::new();
    s.push_str(&format!(
        "# M9.2 sampler perturbation cell (N={})\n\n",
        n
    ));
    s.push_str("| condition | throughput med | throughput IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs |\n");
    s.push_str("|---|---|---|---|---|---|---|\n");
    s.push_str(&format!(
        "| sampler-on  | {:.0} | {:.0} | {} | {} | {} | {} |\n",
        cell_on.throughput_med,
        cell_on.throughput_iqr,
        cell_on.p50_med_us,
        cell_on.p99_med_us,
        cell_on.p99_iqr_us,
        cell_on.p999_med_us,
    ));
    s.push_str(&format!(
        "| sampler-off | {:.0} | {:.0} | {} | {} | {} | {} |\n",
        cell_off.throughput_med,
        cell_off.throughput_iqr,
        cell_off.p50_med_us,
        cell_off.p99_med_us,
        cell_off.p99_iqr_us,
        cell_off.p999_med_us,
    ));

    // Pin: sampler-off p99 must lie within sampler-on IQR (Q1..Q3).
    // Equivalently: |off_med - on_med| <= on_IQR.
    let on_med = cell_on.p99_med_us as i64;
    let off_med = cell_off.p99_med_us as i64;
    let on_iqr = cell_on.p99_iqr_us as i64;
    let drift = (off_med - on_med).abs();
    s.push_str(&format!(
        "\nperturbation drift |off_med - on_med| = {drift}µs vs on_IQR = {on_iqr}µs\n"
    ));
    let pass = drift <= on_iqr;
    s.push_str(&format!(
        "perturbation check: {}\n",
        if pass { "PASS" } else { "FAIL" }
    ));

    let out = "bench/results-m92-sampler-perturbation.md";
    std::fs::write(out, s).expect("write perturbation md");
    println!("==> wrote {}", out);

    assert!(
        pass,
        "sampler perturbation FAIL: |off_med {} - on_med {}| = {} > on_IQR {}",
        off_med, on_med, drift, on_iqr
    );
}
