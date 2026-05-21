# ledger-harness

Multi-session measurement binary for ledger-v3. Drives synthetic workloads against either `ledger-direct` (Path A) or `ledger-routed` (Path B) and records throughput / latency / lock-wait / WAL / eject metrics per design-v3 §8.5. Not a test — a binary that exists to produce comparable JSON results files for the Phase 6 crossover characterization (§10.4).

## API surface

Three CLI subcommands:

```bash
# One-time pool universe seed (10k pools × configurable method assignment).
cargo run -p ledger-harness -- seed-pools --count 10000 --skus 1000 --locations 10

# Drive a scenario through one path.
cargo run -p ledger-harness -- run \
    --scenario {s1..s6}        \
    --path {direct,routed}     \
    --duration 60s             \
    --output results/sN-path-TS.json

# Cross-path equivalence (§8.4) — same workload through both paths, diff trx + pool_state.
cargo run -p ledger-harness -- equivalence --scenario s1
```

Scenarios per design-v3 §9.5: S1 baseline (10 callers, uniform, simple, WAC), S2 routing lock-amortization (200 callers, Zipf, simple, WAC), S3 per-trx intensity (10 callers, uniform, complex, mixed), S4 production stress (200 callers, Zipf, complex, mixed), S5 pathological hot-pool (1000 callers, 1 pool — direct loses), S6 pathological disjoint (1000 callers, disjoint stripes — routed loses).

Driver: `sqlx::PgPool` with `max_connections = callers + 4`; each caller is a `tokio::spawn` looping submit-or-enqueue. Per-task `hdrhistogram` for latency; 1Hz pollers for `pg_stat_database` / `pg_stat_wal` / `pg_stat_activity`; 10Hz `pg_locks` sampler (ported from v21). Toggle sampler via `LEDGER_V3_PRINT_SAMPLER=1` env var (mirrors v21's `POC_PL3B_PRINT_SAMPLER`).

## How exercised

This is the measurement runner, not a test target — it doesn't get a `cargo test` invocation. Workflow:

1. Bring up the DB and install whichever path you want to measure (`scripts/install-direct.sh` or `scripts/install-routed.sh`).
2. Seed the pool universe once.
3. Run scenarios; results land in `results/<scenario>-<path>-<timestamp>.json`.
4. Phase 3 / Phase 5 measurement runs (acct-cs5k / acct-s5h2) sweep all six scenarios with 60s warmup + 5-min measurement windows. Phase 6 (acct-29p8) builds the crossover heatmap from those JSON files.

The harness has unit tests on its workload-distribution helpers (`workload.rs` zipf samples, disjoint stripe correctness) — `cargo test -p ledger-harness`. These don't need a DB. Cluster-per-binary doesn't apply.

## Source layout (filled in by follow-up issues)

- `src/main.rs` / `src/cli.rs` — clap-derived dispatch (acct-bitp)
- `src/pool_universe.rs` — N pools × M skus × L locations seeder + per-distribution pickers (acct-llt2)
- `src/workload.rs` — OverlapMode × Complexity × MethodMix → `Vec<LineParam>` (acct-y3v1)
- `src/scenarios.rs` — S1–S6 builders (acct-7ywx)
- `src/driver_direct.rs` — multi-session Path A driver (acct-ykyl)
- `src/driver_routed.rs` — enqueue + poll Path B driver (acct-qiaz)
- `src/measure.rs` — per-task histograms + 1Hz PG-stat pollers (acct-vd83)
- `src/sampler.rs` — `pg_locks` sampler port from v21 (acct-le53)
- `src/report.rs` — JSON output per §F schema (acct-giun)
