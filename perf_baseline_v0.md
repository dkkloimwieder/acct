# Perf baseline v0 (Phase 0, simplest schema)

This file records the reference perf measurements for the **simplest possible** version of the implementation — Phase 0 schema only, with the simplest load-test batch construction (`tests/load_deadlock_freedom.rs`). Per the 2026-04-29 directive (Part VII Q2 resolved), every Phase 1 complexity addition (customers, work_orders, routings, BOMs, alternate cost methods) is diff'd against these numbers so that "did adding feature X regress throughput / latency?" is answerable.

These numbers are **not SLOs**. They're a yardstick.

## Methodology — multi-run

The dev hardware is a consumer laptop (i7-1185G7, 4 cores / 8 threads) on a multi-tenant desktop kernel. A single 30-minute run on this rig is not statistically reliable — background processes, thermal throttling, and OS scheduling jitter dominate at the percentile tails. So we take **N runs back-to-back** and report the variance.

Configuration for this baseline:

- `T4_BASELINE_RUNS=3` runs back-to-back
- `T4_DURATION_SECS=600` (10 minutes per run)
- `T4_WRITERS=100` (spec-target concurrency)
- ~60–100 K batches per run — well above what's needed for stable p99 within a run
- ~30 minutes total wall clock per baseline pass

The driver is `scripts/run-perf-baseline.sh`. Per run it captures, in addition to the test's own latency / throughput numbers:

- `pg_stat_database` deltas: `xact_commit`, `xact_rollback`, `blks_read`, `blks_hit`, `deadlocks`.
- `pg_stat_io` deltas (PG 18) summed across contexts for `client backend`: `reads`, `read_bytes`, `writes`, `write_bytes`, `extends`, `hits`, `fsyncs`.
- WAL bytes generated (via `pg_current_wal_lsn` + `pg_wal_lsn_diff`).
- `pg_stat_statements` top 10 queries by `total_exec_time` (reset at run start).
- Host-side `vmstat` samples to a side log: CPU `us`/`sy`/`id`/`wa`/`st` and context-switch rate. **The vmstat capture is what tells us whether perf variance is "Postgres" or "everything else on the laptop."**

Run 1 is "cold" (Postgres buffer cache empty); runs 2 + 3 are "warm." We report all separately.

## Run metadata

- **Date:** 2026-04-29
- **Issue:** `acct-1ia`
- **Schema state:** migrations 0001–0016 applied; no Phase 1 tables.
- **Methodology:** 3 × 10 min × 100 writers (`scripts/run-perf-baseline.sh`).
- **Git ref at run time:** `da0dbf93` (followed by this commit, which adds the perf instrumentation + this file).
- **Total wall clock:** 1810 s (~30 min, including DB drop+recreate + cargo overhead between runs).

## Environment

| Layer | Detail |
|---|---|
| Host kernel | Linux 6.17.0-22-generic x86_64 |
| Host CPU | 11th Gen Intel(R) Core(TM) i7-1185G7 @ 3.00 GHz, 4 cores (8 threads) |
| Host RAM | 60 GiB total |
| Container | acct-postgres image (`db/Dockerfile`: `FROM postgres:18` + `postgresql-18-cron`) |
| Postgres | 18.3 (Debian 18.3-1.pgdg13+1) |
| Extensions | pg_stat_statements, pg_cron |
| Storage | named Docker volume `acct-pgdata` on the host filesystem |

## Postgres configuration

| GUC | Value |
|---|---|
| `io_method` | `io_uring` |
| `max_connections` | 200 |
| `shared_preload_libraries` | `pg_stat_statements,pg_cron` |
| `cron.database_name` | `acct` |
| `seccomp` (container) | `unconfined` (dev only; production-hardening tracked as `acct-hbp`) |

All other GUCs at PG 18 defaults.

## Workload — the simplest batch shape

`tests/load_deadlock_freedom.rs` configuration:

- **Writers:** 100 concurrent tokio tasks, each holding its own connection.
- **Batch shape:** every event posts `debit = <random pick from a curated qty-debit-pool>`, `credit = creation_void(qty)`. The credit side is shared across all writers — maximum lock-contention on a single account row.
- **Debit pool size:** 7 accounts (stock_available × 2 locations + stock_wip × 2 routing_ops + stock_consumed + 2 stock_wip on SKU-WAC).
- **Events per batch:** 5–20, uniformly distributed.
- **Reasons:** `cycle_count_adj` for non-WIP accounts, `op_move` for stock_wip(SKU-A) (exercises W2 cost dispatch on the standard-cost path), `cycle_count_adj` for stock_wip(SKU-WAC) (op_move there would raise P0006).

This shape is **deliberately narrow**: no balance CHECKs, no period boundary issues, no idempotency replay. The only failure mode possible under correct operation is a deadlock — exactly what the test is for. These numbers do **not** reflect realistic application traffic; that's tracked as `acct-2ey`.

## Results — per run

### Throughput + latency (latencies in **ms** for readability)

| # | Batches | b/s | ev/s | p50 | p95 | p99 | p99.9 | max | Deadlocks |
|---|---|---|---|---|---|---|---|---|---|
| 1 (cold) | 25 810 | 42.9 | 536.4 | 1 428 | 7 716 | 12 590 | 20 361 | 28 066 | 0 |
| 2 (warm) | 27 956 | 46.4 | 582.1 | 1 314 | 7 072 | 11 614 | 19 421 | 32 735 | 0 |
| 3 (warm) | 29 215 | 48.5 | 604.3 | 1 255 | 6 767 | 10 823 | 17 496 | 32 137 | 0 |

### `pg_stat_database` deltas

| # | xact_commit | xact_rollback | blks_read | blks_hit |
|---|---|---|---|---|
| 1 (cold) | 26 129 | 0 | 26 | 365 138 948 |
| 2 (warm) | 28 280 | 0 | 31 | 389 685 308 |
| 3 (warm) | 29 531 | 0 | 33 | 383 768 517 |

### `pg_stat_io` deltas (client backend, summed across contexts)

| # | reads | read KB | writes | write MB | extends | hits | fsyncs |
|---|---|---|---|---|---|---|---|
| 1 (cold) | 31 | 248 | 25 882 | 535 | 18 168 | 365 088 602 | 25 826 |
| 2 (warm) | 39 | 312 | 28 040 | 578 | 19 792 | 389 611 182 | 27 976 |
| 3 (warm) | 43 | 344 | 29 302 | 584 | 20 436 | 383 677 303 | 29 236 |

### WAL volume

| # | bytes | MB | rate (MB/s) |
|---|---|---|---|
| 1 (cold) | 336 056 376 | 320.5 | 0.53 |
| 2 (warm) | 362 781 064 | 346.0 | 0.57 |
| 3 (warm) | 375 568 664 | 358.2 | 0.60 |

### Host CPU / IO (vmstat means, n=125 samples per run at 5 s interval)

| # | us% | sy% | id% (min/max) | wa% | st% | ctx-switches/s |
|---|---|---|---|---|---|---|
| 1 (cold) | 25.4 | 2.0 | 49.9 (48/57) | 18.7 | 0.0 | 16 924 |
| 2 (warm) | 24.7 | 2.0 | 50.7 (51/57) | 18.5 | 0.0 | 16 620 |
| 3 (warm) | 24.7 | 2.0 | 51.0 (49/58) | 18.4 | 0.0 | 17 268 |

## Aggregate — across all 3 runs

| Metric | min | median | mean | max | range |
|---|---|---|---|---|---|
| Throughput (b/s) | 42.88 | 46.43 | 45.94 | 48.52 | 5.64 (12%) |
| Throughput (ev/s) | 536.4 | 582.1 | 574.3 | 604.3 | 67.9 (12%) |
| p50 (ms) | 1 255 | 1 314 | 1 332 | 1 428 | 173 (13%) |
| p95 (ms) | 6 767 | 7 072 | 7 185 | 7 716 | 949 (13%) |
| p99 (ms) | 10 823 | 11 614 | 11 676 | 12 590 | 1 767 (15%) |
| p99.9 (ms) | 17 496 | 19 421 | 19 093 | 20 361 | 2 865 (15%) |
| max (ms) | 28 066 | 32 137 | 30 979 | 32 735 | 4 669 (15%) |
| WAL bytes/run | 336 M | 363 M | 358 M | 376 M | 40 M |
| io_writes/run | 25 882 | 28 040 | 27 741 | 29 302 | 3 420 |
| io_fsyncs/run | 25 826 | 27 976 | 27 679 | 29 236 | 3 410 |

Cross-run variance is **tight (~10–15 %)** despite running on a noisy consumer laptop — see Observations for why.

## Top queries — representative warm run (run 3, `pg_stat_statements`)

| calls | total_ms | mean_ms | query (truncated) |
|---|---|---|---|
| 29 215 | 59 724 865 | 2 044.32 | `SELECT post_transfers($1, $2)` |
| 2 | 0.2 | 0.113 | (T4 harness pg_stat_io snapshot) |
| 2 | 0.0 | 0.003 | `SELECT pg_current_wal_lsn()::text` |
| 1 | 0.0 | 0.002 | `SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::BIGINT` |

`post_transfers` accounts for essentially 100 % of database CPU+wait time: 59 724 865 ms across 29 215 calls is mean 2 044 ms per call. That reconciles with our Rust-side p50 of 1 314 ms on this run (Rust-side measures only the wait that yielded back to tokio; PG-side `total_exec_time` includes lock-wait inside the function).

## Observations

1. **Lock contention dominates.** 100 writers serialize on the single `creation_void(qty)` row's `FOR UPDATE` lock — every batch needs it (it's the credit side of every event in the simplest workload). With ~10–20 events per batch and per-event lock acquire+release, p50 ≈ 1.3 s and p99 ≈ 11.6 s are explained almost entirely by the queue depth. The bottleneck is **structural to the test shape**, not platform.

2. **CPU is half-idle.** `id ≈ 50 %`, `us ≈ 25 %`, `sy ≈ 2 %`. We're effectively using ~2.2 of 8 hardware threads at peak. Adding more writers wouldn't help — they'd queue longer on the same lock.

3. **iowait ≈ 18.5 %** is non-trivial. ~28 K fsync/run and ~580 MB of write_bytes/run mean storage participates in commit latency. But `id` 50 % > `wa` 18 % > `us` 25 %, so the dominant cost is *waiting for the lock*, not waiting for IO.

4. **WAL ≈ 0.6 MB/s** (~13 KB/batch). For ~46 batches/s × ~12 events/batch × small (BIGINT) updates, this is right-sized.

5. **Cold→warm gain is modest.** Run 1 (cold): 42.9 b/s. Run 3 (warm): 48.5 b/s. ~13 % gain across cache state, low because lock-queue cost dwarfs cache lookups.

6. **Variance is *tight* despite noisy hardware.** 10–15 % range across throughput and percentiles — much tighter than I'd predicted for a desktop kernel. The reason: the FOR UPDATE bottleneck is so dominant that platform jitter (scheduler, interrupts, background processes) is a small fraction of total batch latency.

7. **Zero deadlocks across 82 981 batches / ~1 037 K events.** The ascending-id `FOR UPDATE` lock ordering in `post_transfers` does what the spec claims.

8. **`acct-2ey` will dramatically change these numbers** (in the right direction). Once writers contend on different accounts (cross-ledger / multi-currency / reservation-interleaved batches), the FOR UPDATE serialization spreads across many lock targets and throughput should climb substantially. **Treat the current numbers as a pessimistic floor**, not a representative steady state.

## Caveats

- **Single-machine consumer laptop.** Numbers reflect a developer rig, not production-class hardware. Absolute throughput is an artifact of the test rig; *relative* changes vs this baseline are the load-bearing comparison.
- **Single shared credit account.** All writers contend on `creation_void(qty)`. This stresses the lock-order-correctness proof but is not realistic. `acct-2ey` will phase in cross-account-set, cross-ledger, multi-currency, and reservation-interleaved batches.
- **Sync `post_transfers` only.** No outbox, no async projection. Per Part VII Q3. Outbox-vs-sync benchmark is `acct-tyq`, gated on this baseline.
- **Standard cost only.** Non-`standard` cost methods are P0006 in Phase 0. WAC/FIFO/lot perf characterization is downstream of `acct-8gg` + a fresh baseline run.
- **No reservation traffic in load mix.** `reserve_inventory()` is exercised by T3 but not under load. `acct-2ey`.
- **No OS-level CPU / IO capture** beyond what `pg_stat_database` and `pg_stat_io` expose. `vmstat` / `iostat` capture is a future refinement.
- **No NUMA / CPU pinning, no isolated cores.** The kernel scheduler treats Postgres + cargo test + everything else equally. This *is* the variance source.

## How to reproduce

```bash
./scripts/dev-up.sh
./scripts/run-migrations.sh
./scripts/run-perf-baseline.sh                      # 5 × 5 min defaults
T4_BASELINE_RUNS=10 ./scripts/run-perf-baseline.sh  # more runs for tighter variance
T4_DURATION_SECS=600 ./scripts/run-perf-baseline.sh # 10 min/run for tail accuracy
```

The script prints a combined `T4 PERF SUMMARY` block per run plus an aggregate; per-run logs land in `/tmp/t4_baseline_<timestamp>/`.

## Re-running this baseline

This file should be regenerated whenever:

1. A Phase 1 schema addition lands (compare aggregate numbers — flag regressions > ~25% on warm-median throughput or > ~50% on p99).
2. Postgres major version changes.
3. Significant container / OS / kernel change on the dev rig.
4. The load test's batch shape changes (`acct-2ey` will trigger this).

Append a new section dated below the current one rather than overwriting; the history of v0 → v1 → v2 baselines is the diff trail that detects creep.
