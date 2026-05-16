//! M9.3 (acct-4d4n.22) — GUC sweep + bottleneck classifier integration
//! per spec §5.5 + §5.7.
//!
//! Sweep dimensions:
//!   batch_window_us    ∈ {100, 500, 2000}      (poc_ledger Sighup GUC)
//!   batch_size_max     ∈ {64, 1024, 16384}     (poc_ledger Sighup GUC)
//!   synchronous_commit ∈ {on, off}             (PG builtin; off = no-durability ceiling)
//!
//! Shapes (per spec §5.5 — the two regimes where committer batching is
//! most differentiating):
//!   shape 2 fan_out      (g=5000; linear-scaling regime)
//!   shape 5 small_batch  (b=100 / g=50; rapid-fire regime)
//!
//! N subset (per M9.2 plateau evidence — fan_in throughput plateaus
//! ~N=128 and classifier transitions to B5:wake at N≥16):
//!   N ∈ {4, 32, 128} — spans sub-saturation / mid-contention / saturation
//!
//! Per cell: 5 × 60s with 30s settle gaps (matches M9.2 methodology),
//! hdrhistogram, pg_locks sampler @ 100ms, deadlocks delta, classifier
//! label.
//!
//! Output:
//!   bench/results-m93-guc-sweep.md     — flat detail table + peak-tps
//!                                        heatmap per (shape × sync_commit)
//!   bench/results-m93-guc-sweep.json   — full record per cell for M10
//!                                        consumption
//!
//! Env knobs (smoke / dry-run overrides):
//!   POC_M93_DURATION   — seconds per run               (default 60)
//!   POC_M93_RUNS       — replications per cell         (default 5)
//!   POC_M93_SETTLE     — settle gap secs               (default 30)
//!   POC_M93_NS         — comma-separated N             (default 4,32,128)
//!   POC_M93_BW         — comma-separated batch_window  (default 100,500,2000)
//!   POC_M93_BS         — comma-separated batch_size    (default 64,1024,16384)
//!   POC_M93_SC         — comma-separated 'on','off'    (default on,off)
//!   POC_M93_SHAPES     — comma-separated names         (default fan_out,small_batch)
//!   POC_M93_OUTPUT_MD  — markdown path
//!   POC_M93_OUTPUT_JS  — JSON path
//!   POC_M93_SKIP_RESET — '1' to skip ALTER SYSTEM RESET at end (debug)

#![cfg(test)]

mod common;

use common::m9_runner::{
    apply_gucs, connect_pool, observed_gucs, reset_gucs, run_cell, CellConfig,
    GucCombo, GucSweepCell, Shape,
};
use std::sync::Arc;

fn env_dur() -> u64 {
    std::env::var("POC_M93_DURATION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60)
}

fn env_runs() -> usize {
    std::env::var("POC_M93_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

fn env_settle() -> u64 {
    std::env::var("POC_M93_SETTLE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}

fn env_ns() -> Vec<usize> {
    std::env::var("POC_M93_NS")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|t| t.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| vec![4, 32, 128])
}

fn env_bws() -> Vec<i32> {
    std::env::var("POC_M93_BW")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![100, 500, 2000])
}

fn env_bss() -> Vec<i32> {
    std::env::var("POC_M93_BS")
        .ok()
        .map(|s| s.split(',').filter_map(|t| t.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![64, 1024, 16384])
}

fn env_scs() -> Vec<bool> {
    std::env::var("POC_M93_SC")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|t| matches!(t.trim().to_ascii_lowercase().as_str(), "on" | "true" | "1"))
                .collect()
        })
        .unwrap_or_else(|| vec![true, false])
}

fn env_shapes() -> Vec<Shape> {
    std::env::var("POC_M93_SHAPES")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|t| match t.trim() {
                    "fan_in" => Some(Shape::FanIn),
                    "fan_out" => Some(Shape::FanOut),
                    "balanced" => Some(Shape::Balanced),
                    "zipfian" => Some(Shape::Zipfian),
                    "small_batch" => Some(Shape::SmallBatch),
                    "mixed_method" => Some(Shape::MixedMethod),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_else(|| vec![Shape::FanOut, Shape::SmallBatch])
}

fn env_out_md() -> String {
    std::env::var("POC_M93_OUTPUT_MD")
        .unwrap_or_else(|_| "bench/results-m93-guc-sweep.md".to_string())
}

fn env_out_js() -> String {
    std::env::var("POC_M93_OUTPUT_JS")
        .unwrap_or_else(|_| "bench/results-m93-guc-sweep.json".to_string())
}

fn render_md(cells: &[GucSweepCell]) -> String {
    let mut s = String::new();
    s.push_str("# M9.3 (acct-4d4n.22) — GUC sweep + classifier integration\n\n");
    s.push_str(&format!(
        "Per-cell {runs} × {dur}s with {settle}s settle gap. hdrhistogram-based latency capture. pg_locks sampler @ 100ms.\n",
        runs = env_runs(),
        dur = env_dur(),
        settle = env_settle()
    ));
    s.push_str("GUC application: `ALTER SYSTEM SET <key> = <value>; SELECT pg_reload_conf();` per cell. ");
    s.push_str("Both `poc_ledger.batch_window_us` and `poc_ledger.batch_size_max` are `GucContext::Sighup` (verified in `src/lib.rs`); ");
    s.push_str("`synchronous_commit` is PG builtin userset, cluster-default modified via the same ALTER SYSTEM path.\n\n");
    s.push_str("> **Durability note:** `synchronous_commit=off` rows are the peak-only / no-durability ceiling per spec §5.5. ");
    s.push_str("Production deployment requires `synchronous_commit=on`; the `off` rows are reported for ceiling comparison only.\n\n");

    // Group: shape × sync_commit slice → flat per-N table (rows=bw, cols=bs)
    let shapes: Vec<String> = {
        let mut v: Vec<String> = cells.iter().map(|c| c.cell.shape.clone()).collect();
        v.sort();
        v.dedup();
        v
    };
    let scs: Vec<bool> = vec![true, false];
    let ns: Vec<usize> = {
        let mut v: Vec<usize> = cells.iter().map(|c| c.cell.n_backends).collect();
        v.sort();
        v.dedup();
        v
    };
    let bws: Vec<i32> = {
        let mut v: Vec<i32> = cells.iter().map(|c| c.guc.batch_window_us).collect();
        v.sort();
        v.dedup();
        v
    };
    let bss: Vec<i32> = {
        let mut v: Vec<i32> = cells.iter().map(|c| c.guc.batch_size_max).collect();
        v.sort();
        v.dedup();
        v
    };

    for shape in &shapes {
        for &sc in &scs {
            if !cells.iter().any(|c| {
                c.cell.shape == *shape && c.guc.sync_commit == sc
            }) {
                continue;
            }
            s.push_str(&format!(
                "## {shape}, synchronous_commit={sc_label}\n\n",
                shape = shape,
                sc_label = if sc { "on" } else { "off" }
            ));

            // Peak-tps heatmap per N: rows=bw, cols=bs.
            for &n in &ns {
                s.push_str(&format!("### N={n} — throughput med (events/sec)\n\n"));
                s.push_str("| bw_us \\ bs_max |");
                for &bs in &bss {
                    s.push_str(&format!(" {bs} |"));
                }
                s.push_str("\n|---|");
                for _ in &bss {
                    s.push_str("---|");
                }
                s.push('\n');
                for &bw in &bws {
                    s.push_str(&format!("| {bw} |"));
                    for &bs in &bss {
                        let v = cells.iter().find(|c| {
                            c.cell.shape == *shape
                                && c.guc.sync_commit == sc
                                && c.cell.n_backends == n
                                && c.guc.batch_window_us == bw
                                && c.guc.batch_size_max == bs
                        });
                        match v {
                            Some(g) => s.push_str(&format!(" {:.0} |", g.cell.throughput_med)),
                            None => s.push_str(" — |"),
                        }
                    }
                    s.push('\n');
                }
                s.push_str(&format!("\n### N={n} — p99 med (µs)\n\n"));
                s.push_str("| bw_us \\ bs_max |");
                for &bs in &bss {
                    s.push_str(&format!(" {bs} |"));
                }
                s.push_str("\n|---|");
                for _ in &bss {
                    s.push_str("---|");
                }
                s.push('\n');
                for &bw in &bws {
                    s.push_str(&format!("| {bw} |"));
                    for &bs in &bss {
                        let v = cells.iter().find(|c| {
                            c.cell.shape == *shape
                                && c.guc.sync_commit == sc
                                && c.cell.n_backends == n
                                && c.guc.batch_window_us == bw
                                && c.guc.batch_size_max == bs
                        });
                        match v {
                            Some(g) => s.push_str(&format!(" {} |", g.cell.p99_med_us)),
                            None => s.push_str(" — |"),
                        }
                    }
                    s.push('\n');
                }
                s.push('\n');
            }

            // Flat detail
            s.push_str("### Detail (per cell)\n\n");
            s.push_str("| N | bw_us | bs_max | tps med | tps IQR | p50 med µs | p99 med µs | p99 IQR µs | p99.9 med µs | deadlocks med | classifier (median run) | top wait_event (median run) |\n");
            s.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|\n");
            for &n in &ns {
                for &bw in &bws {
                    for &bs in &bss {
                        let v = match cells.iter().find(|c| {
                            c.cell.shape == *shape
                                && c.guc.sync_commit == sc
                                && c.cell.n_backends == n
                                && c.guc.batch_window_us == bw
                                && c.guc.batch_size_max == bs
                        }) {
                            Some(g) => g,
                            None => continue,
                        };
                        let cell = &v.cell;
                        let mut sorted: Vec<(usize, f64)> = cell
                            .runs
                            .iter()
                            .map(|r| (r.run_idx, r.throughput))
                            .collect();
                        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                        let median_run_idx = sorted[sorted.len() / 2].0;
                        let median_run = &cell.runs[median_run_idx];
                        let classifier = &median_run.classifier_label;
                        let top_wait = median_run
                            .sampler
                            .as_ref()
                            .and_then(|samp| samp.top_wait_event())
                            .map(|(t, e, _)| format!("{}:{}", t, e))
                            .unwrap_or_else(|| "—".to_string());
                        s.push_str(&format!(
                            "| {} | {} | {} | {:.0} | {:.0} | {} | {} | {} | {} | {} | {} | {} |\n",
                            n,
                            bw,
                            bs,
                            cell.throughput_med,
                            cell.throughput_iqr,
                            cell.p50_med_us,
                            cell.p99_med_us,
                            cell.p99_iqr_us,
                            cell.p999_med_us,
                            cell.deadlocks_med,
                            classifier,
                            top_wait,
                        ));
                    }
                }
            }
            s.push('\n');
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

fn render_json(cells: &[GucSweepCell]) -> String {
    let mut records = Vec::new();
    for c in cells {
        let per_run: Vec<serde_json::Value> = c
            .cell
            .runs
            .iter()
            .map(|run| {
                let top = run
                    .sampler
                    .as_ref()
                    .and_then(|s| s.top_wait_event())
                    .map(|(t, e, count)| {
                        serde_json::json!({
                            "wait_event_type": t,
                            "wait_event": e,
                            "sum_backends": count,
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
        records.push(serde_json::json!({
            "shape": c.cell.shape,
            "n_backends": c.cell.n_backends,
            "guc": {
                "batch_window_us": c.guc.batch_window_us,
                "batch_size_max": c.guc.batch_size_max,
                "synchronous_commit": if c.guc.sync_commit { "on" } else { "off" },
            },
            "durability_void": !c.guc.sync_commit,
            "throughput_med": c.cell.throughput_med,
            "throughput_iqr": c.cell.throughput_iqr,
            "p50_med_us": c.cell.p50_med_us,
            "p50_iqr_us": c.cell.p50_iqr_us,
            "p99_med_us": c.cell.p99_med_us,
            "p99_iqr_us": c.cell.p99_iqr_us,
            "p999_med_us": c.cell.p999_med_us,
            "p999_iqr_us": c.cell.p999_iqr_us,
            "deadlocks_med": c.cell.deadlocks_med,
            "runs": per_run,
        }));
    }
    let top = serde_json::json!({
        "spec": "M9.3 acct-4d4n.22 GUC sweep + classifier integration",
        "config": {
            "shapes": env_shapes().iter().map(|s| s.name()).collect::<Vec<_>>(),
            "runs_per_cell": env_runs(),
            "duration_secs": env_dur(),
            "settle_secs": env_settle(),
            "ns": env_ns(),
            "batch_window_us": env_bws(),
            "batch_size_max": env_bss(),
            "synchronous_commit": env_scs().iter().map(|b| if *b { "on" } else { "off" }).collect::<Vec<_>>(),
        },
        "cells": records,
    });
    serde_json::to_string_pretty(&top).expect("json serialize")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn m9_guc_sweep_all() {
    let shapes = env_shapes();
    let ns = env_ns();
    let bws = env_bws();
    let bss = env_bss();
    let scs = env_scs();
    let runs = env_runs();
    let dur = env_dur();
    let settle = env_settle();

    let max_n = ns.iter().copied().max().unwrap_or(4);
    let pool = Arc::new(connect_pool((max_n as u32) + 8).await);

    let total_cells = shapes.len() * bws.len() * bss.len() * scs.len() * ns.len();
    eprintln!(
        "==> M9.3 GUC sweep: shapes={:?} N={:?} bw={:?} bs={:?} sc={:?} runs={} dur={}s settle={}s",
        shapes.iter().map(|s| s.name()).collect::<Vec<_>>(),
        ns,
        bws,
        bss,
        scs.iter().map(|b| if *b { "on" } else { "off" }).collect::<Vec<_>>(),
        runs,
        dur,
        settle,
    );
    eprintln!(
        "    total cells = {} ; per-cell wall ≈ {}s ; total wall ≈ {}m",
        total_cells,
        runs * dur as usize + (runs - 1) * settle as usize,
        (total_cells * (runs * dur as usize + (runs - 1) * settle as usize)) / 60,
    );

    let mut sweep: Vec<GucSweepCell> = Vec::with_capacity(total_cells);
    let mut cell_idx = 0_usize;

    for shape in &shapes {
        for &bw in &bws {
            for &bs in &bss {
                for &sc in &scs {
                    let guc = GucCombo {
                        batch_window_us: bw,
                        batch_size_max: bs,
                        sync_commit: sc,
                    };
                    apply_gucs(&pool, guc).await.expect("apply_gucs");
                    let (obw, obs, osc) = observed_gucs(&pool).await;
                    if obw != bw || obs != bs {
                        eprintln!(
                            "    !! GUC observed drift: bw={} (asked {}), bs={} (asked {}), sc={} (asked {})",
                            obw, bw, obs, bs, osc, if sc { "on" } else { "off" }
                        );
                    }
                    for &n in &ns {
                        cell_idx += 1;
                        let cfg = CellConfig {
                            shape: *shape,
                            n_backends: n,
                            duration_secs: dur,
                            runs,
                            settle_secs: settle,
                            with_sampler: true,
                            sampler_interval_ms: 100,
                        };
                        eprintln!(
                            "==> cell {}/{} shape={} N={} bw={} bs={} sc={}",
                            cell_idx,
                            total_cells,
                            shape.name(),
                            n,
                            bw,
                            bs,
                            if sc { "on" } else { "off" }
                        );
                        let cell = run_cell(pool.clone(), &cfg).await;
                        eprintln!(
                            "    tps med={:.0} IQR={:.0}  p99 med={}µs IQR={}µs  deadlocks_med={}",
                            cell.throughput_med,
                            cell.throughput_iqr,
                            cell.p99_med_us,
                            cell.p99_iqr_us,
                            cell.deadlocks_med
                        );
                        sweep.push(GucSweepCell {
                            shape: shape.name().to_string(),
                            guc,
                            cell,
                        });

                        // Persist partial results every 6 cells so a long
                        // run is recoverable if the rig dies. Writes
                        // overwrite — final write is the canonical one.
                        if cell_idx % 6 == 0 {
                            let md = render_md(&sweep);
                            let js = render_json(&sweep);
                            let _ = std::fs::write(env_out_md(), md);
                            let _ = std::fs::write(env_out_js(), js);
                        }
                    }
                }
            }
        }
    }

    let md = render_md(&sweep);
    let js = render_json(&sweep);
    std::fs::write(env_out_md(), md).expect("write md");
    std::fs::write(env_out_js(), js).expect("write json");
    println!("==> wrote {} + {}", env_out_md(), env_out_js());

    if std::env::var("POC_M93_SKIP_RESET").ok().as_deref() != Some("1") {
        reset_gucs(&pool).await.expect("reset_gucs");
    }

    // Acceptance: every cell produced the expected number of runs;
    // every run carries a classifier label (I9.3.2).
    assert_eq!(sweep.len(), total_cells);
    for c in &sweep {
        assert_eq!(c.cell.runs.len(), runs);
        for r in &c.cell.runs {
            assert!(
                !r.classifier_label.is_empty(),
                "empty classifier label in shape={} N={} guc={}",
                c.cell.shape,
                c.cell.n_backends,
                c.guc.label()
            );
        }
    }
}
