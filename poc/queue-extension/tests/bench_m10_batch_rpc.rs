//! acct-22xt — caller-side batch RPC (b=1000) characterization.
//!
//! The queue PoC validated at b=1 (one event per RPC) per spec §5.2
//! "b=1 for the PoC; multi-item batches deferred". The shmem rollup PoC
//! (acct-sw4i) measured 67K fan_in / 43.5K fan_out at b=1000. The 6×
//! gap should be caller-side RPC amortization, not architecture. This
//! bench measures queue PoC at b=1000 to confirm.
//!
//! Scope (locked):
//!   3 shapes × 2 N × b=1000 × 5×60s runs / 30s settle. Default GUCs
//!   (bw=500 bs=1024 sc=on). Total 6 cells × 420s = 42 min wall.
//!
//! Per-cell driver: each backend repeatedly calls
//! `poc_ledger_apply_batch(events JSONB)` with a 1000-element envelope
//! array; each envelope's sku is picked by the same `pick_sku` logic
//! as M9.x (so fan_in stays at sku=1, fan_out walks the per-backend
//! disjoint subset). Backends sync via Barrier; per-call latency is
//! the round-trip for the whole b=1000 batch (NOT per-event). Throughput
//! reports events/sec (RPCs × 1000), to compare like-for-like against
//! the M9.x events/sec headline.
//!
//! Run via:
//!   cargo test --release --test bench_m10_batch_rpc m10_batch_rpc_all \
//!     --features pg18 --no-default-features -- --ignored --nocapture
//!
//! Env knobs (smoke):
//!   POC_M10BR_DURATION  — seconds per run        (default 60)
//!   POC_M10BR_RUNS      — replications per cell  (default 5)
//!   POC_M10BR_SETTLE    — settle gap secs        (default 30)
//!   POC_M10BR_B         — batch size per call    (default 1000)
//!   POC_M10BR_OUTPUT_MD — markdown path
//!   POC_M10BR_OUTPUT_JS — JSON path

#![cfg(test)]

mod common;

use common::m9_runner::{
    build_zipf_cdf, connect_pool, fetch_classifier_label, fetch_deadlocks,
    fetch_snapshot, pick_method, pick_sku, pre_seed, reset_state, Shape,
    LOCATION_ID,
};
use common::pg_locks_sampler::PgLocksSampler;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

const HIST_HIGH_US: u64 = 60_000_000;
const HIST_SIG_FIG: u8 = 3;

fn env_dur() -> u64 {
    std::env::var("POC_M10BR_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}
fn env_runs() -> usize {
    std::env::var("POC_M10BR_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}
fn env_settle() -> u64 {
    std::env::var("POC_M10BR_SETTLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}
fn env_b() -> usize {
    std::env::var("POC_M10BR_B")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}
fn env_out_md() -> String {
    std::env::var("POC_M10BR_OUTPUT_MD")
        .unwrap_or_else(|_| "bench/results-m10-batch-rpc.md".to_string())
}
fn env_out_js() -> String {
    std::env::var("POC_M10BR_OUTPUT_JS")
        .unwrap_or_else(|_| "bench/results-m10-batch-rpc.json".to_string())
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

struct RunOutcome {
    batches_ok: u64,
    batches_err: u64,
    events_ok: u64,
    events_err: u64,
    batch_p50_us: u64,
    batch_p99_us: u64,
    batch_p999_us: u64,
    throughput_evps: f64,
    classifier_label: String,
    deadlocks_delta: i64,
}

async fn run_one(
    pool: Arc<PgPool>,
    shape: Shape,
    n_backends: usize,
    b: usize,
    duration_secs: u64,
) -> RunOutcome {
    let duration = Duration::from_secs(duration_secs);
    let barrier = Arc::new(Barrier::new(n_backends));
    let zipf_cdf: Option<Arc<Vec<f64>>> = if shape == Shape::Zipfian {
        Some(Arc::new(build_zipf_cdf(shape.g(), 1.0)))
    } else {
        None
    };

    let dl_before = fetch_deadlocks(&pool).await;
    let snap_start = fetch_snapshot(&pool).await;
    let wall_t0 = Instant::now();

    let mut handles = Vec::with_capacity(n_backends);
    for backend_idx in 0..n_backends {
        let pool = pool.clone();
        let barrier = barrier.clone();
        let zipf = zipf_cdf.clone();
        handles.push(tokio::spawn(async move {
            let mut hist: Histogram<u64> =
                Histogram::new_with_bounds(1, HIST_HIGH_US, HIST_SIG_FIG)
                    .expect("histogram bounds");
            let mut batches_ok: u64 = 0;
            let mut batches_err: u64 = 0;
            let mut events_ok: u64 = 0;
            let mut events_err: u64 = 0;

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
                // Build a b-event envelope. Each envelope picks an
                // sku via the same logic as the single-event bench;
                // method follows the shape (mock for non-mixed).
                let mut envs: Vec<serde_json::Value> = Vec::with_capacity(b);
                for j in 0..b {
                    let sku = pick_sku(
                        shape,
                        backend_idx,
                        n_backends,
                        iter.wrapping_add(j),
                        &mut rng,
                        zipf.as_deref().map(|v| v.as_slice()),
                    );
                    let method = pick_method(shape, iter.wrapping_add(j));
                    // issue_id MUST be unique per call (committer
                    // dedup keys on (issue_id, method) per spec).
                    // Stripe by backend × iter × j to avoid collision.
                    let issue_id: i64 =
                        (backend_idx as i64) * 1_000_000_000_000
                            + (iter as i64) * 100_000
                            + j as i64;
                    envs.push(json!({
                        "sku_id": sku,
                        "location_id": LOCATION_ID,
                        "qty": 1_i64,
                        "issue_id": issue_id,
                        "method": method,
                    }));
                }
                let envs_json = serde_json::Value::Array(envs);

                let t0 = Instant::now();
                let r: Result<Vec<(i32,)>, sqlx::Error> = sqlx::query_as(
                    "SELECT error_code FROM poc_ledger_apply_batch($1::jsonb)",
                )
                .bind(sqlx::types::Json(&envs_json))
                .fetch_all(&*pool)
                .await;
                let dt_us = (t0.elapsed().as_nanos() / 1000) as u64;

                match r {
                    Ok(rows) => {
                        // Record one latency sample per batch (Q3
                        // lean: batch-grain latency, not per-event).
                        let _ = hist.record(dt_us.max(1).min(HIST_HIGH_US));
                        batches_ok += 1;
                        for (ec,) in rows.iter() {
                            if *ec == 0 {
                                events_ok += 1;
                            } else {
                                events_err += 1;
                            }
                        }
                    }
                    Err(_) => {
                        batches_err += 1;
                    }
                }
                iter = iter.wrapping_add(b);
            }
            (hist, batches_ok, batches_err, events_ok, events_err)
        }));
    }

    let mut merged: Histogram<u64> =
        Histogram::new_with_bounds(1, HIST_HIGH_US, HIST_SIG_FIG).unwrap();
    let mut tot_batches_ok: u64 = 0;
    let mut tot_batches_err: u64 = 0;
    let mut tot_events_ok: u64 = 0;
    let mut tot_events_err: u64 = 0;
    for h in handles {
        let (hist, bok, berr, eok, eerr) =
            h.await.expect("backend task panicked");
        merged.add(&hist).expect("hist merge");
        tot_batches_ok += bok;
        tot_batches_err += berr;
        tot_events_ok += eok;
        tot_events_err += eerr;
    }

    let wall_ms = wall_t0.elapsed().as_millis() as i64;
    let snap_end = fetch_snapshot(&pool).await;
    let label =
        fetch_classifier_label(&pool, &snap_start.0, &snap_end.0, wall_ms).await;
    let dl_after = fetch_deadlocks(&pool).await;

    let p50 = merged.value_at_quantile(0.50);
    let p99 = merged.value_at_quantile(0.99);
    let p999 = merged.value_at_quantile(0.999);
    let throughput = (tot_events_ok as f64) / (duration_secs as f64);

    RunOutcome {
        batches_ok: tot_batches_ok,
        batches_err: tot_batches_err,
        events_ok: tot_events_ok,
        events_err: tot_events_err,
        batch_p50_us: p50,
        batch_p99_us: p99,
        batch_p999_us: p999,
        throughput_evps: throughput,
        classifier_label: label,
        deadlocks_delta: dl_after - dl_before,
    }
}

struct CellAgg {
    shape: Shape,
    n_backends: usize,
    runs: Vec<RunOutcome>,
}

impl CellAgg {
    fn med_iqr_f(&self, get: impl Fn(&RunOutcome) -> f64) -> (f64, f64) {
        let mut v: Vec<f64> = self.runs.iter().map(&get).collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if v.is_empty() {
            return (0.0, 0.0);
        }
        let med = v[v.len() / 2];
        let q1 = v[v.len() / 4];
        let q3 = v[(v.len() * 3) / 4];
        (med, q3 - q1)
    }
    fn med_iqr_u(&self, get: impl Fn(&RunOutcome) -> u64) -> (u64, u64) {
        let mut v: Vec<u64> = self.runs.iter().map(&get).collect();
        v.sort_unstable();
        if v.is_empty() {
            return (0, 0);
        }
        let med = v[v.len() / 2];
        let q1 = v[v.len() / 4];
        let q3 = v[(v.len() * 3) / 4];
        (med, q3 - q1)
    }
    fn med_i(&self, get: impl Fn(&RunOutcome) -> i64) -> i64 {
        let mut v: Vec<i64> = self.runs.iter().map(&get).collect();
        v.sort_unstable();
        if v.is_empty() {
            return 0;
        }
        v[v.len() / 2]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore]
async fn m10_batch_rpc_all() {
    // Locked scope: 3 shapes × 2 N × b=1000.
    let shapes = [Shape::FanIn, Shape::FanOut, Shape::SmallBatch];
    let ns: [usize; 2] = [32, 128];
    let b = env_b();
    let dur = env_dur();
    let runs = env_runs();
    let settle = env_settle();

    let pool = Arc::new(connect_pool(128 + 8).await);

    let mut cells: Vec<CellAgg> = Vec::new();
    for &shape in &shapes {
        for &n in &ns {
            eprintln!(
                "==> M10 batch-RPC cell shape={:?} N={} b={} (dur={}s × {} runs, settle={}s)",
                shape, n, b, dur, runs, settle
            );
            let mut agg = CellAgg {
                shape,
                n_backends: n,
                runs: Vec::with_capacity(runs),
            };
            for r in 0..runs {
                reset_state(&pool).await.expect("reset_state");
                pre_seed(&pool, shape).await.expect("pre_seed");
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Sampler enabled lightly — interval 200ms to bound
                // perturbation per the M9.2 sampler-sanity rule.
                let sampler = PgLocksSampler::spawn((*pool).clone(), 200)
                    .await;

                let out = run_one(pool.clone(), shape, n, b, dur).await;

                let _report = sampler.shutdown().await;

                eprintln!(
                    "    run {}: evps={:.0} batches_ok={} batches_err={} \
                     ev_ok={} ev_err={} p50={}µs p99={}µs p99.9={}µs \
                     deadlocks_Δ={} class={}",
                    r,
                    out.throughput_evps,
                    out.batches_ok,
                    out.batches_err,
                    out.events_ok,
                    out.events_err,
                    out.batch_p50_us,
                    out.batch_p99_us,
                    out.batch_p999_us,
                    out.deadlocks_delta,
                    out.classifier_label
                );
                agg.runs.push(out);

                if r + 1 < runs {
                    tokio::time::sleep(Duration::from_secs(settle)).await;
                }
            }
            cells.push(agg);
        }
    }

    // Render MD
    let mut s = String::new();
    s.push_str("# acct-22xt — Queue PoC caller-side batch RPC (b=N)\n\n");
    s.push_str(&format!(
        "Per-cell {runs} × {dur}s with {settle}s settle. Batch size b={b}. \
         Default GUCs (bw=500 bs=1024 sc=on).\n\n",
        runs = runs,
        dur = dur,
        settle = settle,
        b = b
    ));
    s.push_str("## Throughput (events/sec, batch-grain latency µs)\n\n");
    s.push_str("| shape | N | evps med | evps IQR | batch p50 µs | batch p99 µs | batch p99.9 µs | deadlocks med |\n");
    s.push_str("|---|---|---|---|---|---|---|---|\n");
    for c in &cells {
        let (evps_med, evps_iqr) = c.med_iqr_f(|r| r.throughput_evps);
        let (p50_med, _) = c.med_iqr_u(|r| r.batch_p50_us);
        let (p99_med, _) = c.med_iqr_u(|r| r.batch_p99_us);
        let (p999_med, _) = c.med_iqr_u(|r| r.batch_p999_us);
        let dl_med = c.med_i(|r| r.deadlocks_delta);
        s.push_str(&format!(
            "| {} | {} | {:.0} | {:.0} | {} | {} | {} | {} |\n",
            shape_name(c.shape),
            c.n_backends,
            evps_med,
            evps_iqr,
            p50_med,
            p99_med,
            p999_med,
            dl_med,
        ));
    }
    s.push_str("\n## Per-run detail\n\n");
    s.push_str("| shape | N | run | batches_ok | events_ok | events_err | evps | p50 µs | p99 µs | p99.9 µs | deadlocks Δ | classifier |\n");
    s.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    for c in &cells {
        for (i, r) in c.runs.iter().enumerate() {
            s.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {:.0} | {} | {} | {} | {} | {} |\n",
                shape_name(c.shape),
                c.n_backends,
                i,
                r.batches_ok,
                r.events_ok,
                r.events_err,
                r.throughput_evps,
                r.batch_p50_us,
                r.batch_p99_us,
                r.batch_p999_us,
                r.deadlocks_delta,
                r.classifier_label,
            ));
        }
    }

    // Comparison context (reference numbers; not measured here)
    s.push_str("\n## Comparison context\n\n");
    s.push_str(
        "Reference numbers from the prior PoCs at the same shapes \
         (different harnesses, same hardware, same DB-on-Docker rig):\n\n",
    );
    s.push_str("| source | b | fan_in | fan_out | notes |\n");
    s.push_str("|---|---|---|---|---|\n");
    s.push_str(
        "| Queue PoC M9.2 (acct-4d4n.21) | 1 | ~11878 evps @ N=256 | ~6379 evps @ N=128 | poc-validation-spec headline |\n",
    );
    s.push_str(
        "| Queue PoC M10 backfill (acct-4d4n.23) | 1 | 364 evps @ N=1 | 376 evps @ N=1 | for P1 ratio |\n",
    );
    s.push_str(
        "| Shmem rollup PoC (acct-sw4i) | 1000 | ~67000 evps | ~43500 evps | poc/ledger-extension/bench/results-shmem-apply.md |\n",
    );

    let ts = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|x| x.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    s.push_str(&format!("\nGenerated: {ts}\n"));

    std::fs::write(env_out_md(), s).expect("write md");

    // JSON
    let records: Vec<serde_json::Value> = cells
        .iter()
        .map(|c| {
            let (evps_med, evps_iqr) = c.med_iqr_f(|r| r.throughput_evps);
            let (p50_med, p50_iqr) = c.med_iqr_u(|r| r.batch_p50_us);
            let (p99_med, p99_iqr) = c.med_iqr_u(|r| r.batch_p99_us);
            let (p999_med, p999_iqr) = c.med_iqr_u(|r| r.batch_p999_us);
            let dl_med = c.med_i(|r| r.deadlocks_delta);
            let per_run: Vec<serde_json::Value> = c
                .runs
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    json!({
                        "run_idx": i,
                        "batches_ok": r.batches_ok,
                        "batches_err": r.batches_err,
                        "events_ok": r.events_ok,
                        "events_err": r.events_err,
                        "throughput_evps": r.throughput_evps,
                        "batch_p50_us": r.batch_p50_us,
                        "batch_p99_us": r.batch_p99_us,
                        "batch_p999_us": r.batch_p999_us,
                        "deadlocks_delta": r.deadlocks_delta,
                        "classifier": r.classifier_label,
                    })
                })
                .collect();
            json!({
                "shape": shape_name(c.shape),
                "n_backends": c.n_backends,
                "b": b,
                "throughput_evps_med": evps_med,
                "throughput_evps_iqr": evps_iqr,
                "batch_p50_med_us": p50_med,
                "batch_p50_iqr_us": p50_iqr,
                "batch_p99_med_us": p99_med,
                "batch_p99_iqr_us": p99_iqr,
                "batch_p999_med_us": p999_med,
                "batch_p999_iqr_us": p999_iqr,
                "deadlocks_med": dl_med,
                "runs": per_run,
            })
        })
        .collect();

    let top = json!({
        "spec": "acct-22xt — caller-side batch RPC characterization",
        "config": {
            "shapes": shapes.iter().map(|s| shape_name(*s)).collect::<Vec<_>>(),
            "ns": ns,
            "b": b,
            "runs_per_cell": runs,
            "duration_secs": dur,
            "settle_secs": settle,
            "guc": { "batch_window_us": 500, "batch_size_max": 1024, "synchronous_commit": "on" },
        },
        "reference": {
            "queue_b1_fan_in_n256_m92": 11878.0,
            "queue_b1_fan_out_n128_m93": 6379.0,
            "shmem_rollup_b1000_fan_in": 67000.0,
            "shmem_rollup_b1000_fan_out": 43500.0,
        },
        "cells": records,
    });
    std::fs::write(
        env_out_js(),
        serde_json::to_string_pretty(&top).expect("serialize"),
    )
    .expect("write json");

    println!("==> wrote {} + {}", env_out_md(), env_out_js());

    // Acceptance: characterization, no PASS/FAIL gate (Q7 lean).
    // But assert at least one cell completed cleanly so a silent
    // total failure doesn't pass.
    let any_ok = cells.iter().any(|c| {
        c.runs
            .iter()
            .any(|r| r.batches_ok > 0 && r.events_ok > 0)
    });
    assert!(any_ok, "no cell produced any successful events");
}

fn shape_name(s: Shape) -> &'static str {
    s.name()
}

// Silence unused-import warnings when feature-gated paths don't trigger.
#[allow(dead_code)]
fn _unused() {
    let _ = percentile;
}
