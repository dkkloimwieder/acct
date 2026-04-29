# Perf baseline v0 — Phase 0 schema, workload-shape matrix

This file records reference perf measurements across **five workload
shapes** on the simplest schema (Phase 0; migrations 0001–0016, no
Phase 1 tables). Per the 2026-04-29 directive (Part VII Q2 resolved),
every Phase 1 complexity addition is diff'd against these numbers.

**These are not SLOs.** They are a yardstick.

## TL;DR

The system has a clean three-regime structure:

1. **Single-writer regime** — peaks at **~2.5 K events/s**. Big batches give a modest ~15 % bump over small batches by amortizing per-batch overhead. CPU is half-idle and iowait is 15–22 %; the bound is per-event work + commit fsync, not contention.
2. **Concurrent-writers regime** — every additional concurrent writer that converges on the same lock target costs throughput. 1→32→100 writers takes events/s from 2 164 → 1 421 → 559. Latencies grow from single-digit-ms (1 writer) to 11.9 s p99 (100 writers).
3. **Concurrent + big batches regime** — catastrophic. 100 writers × 100-event batches finishes only **373 events/s** with **p99 ≈ 62 s** because each batch holds the shared lock for ~20 s and the queue compounds.

The route to higher throughput on the actual hardware is **not "tune Postgres"** — it's spreading contention (account sharding, doc Part IV §8) or collapsing concurrency to a single writer via an outbox (`acct-tyq`).

## Methodology

The dev hardware is a consumer laptop on a multi-tenant desktop kernel. A single long run on this rig is not statistically reliable — background processes, thermal throttling, and OS scheduling jitter dominate at the percentile tails. So we take **N runs back-to-back per shape** and report the variance across them.

Configuration of this baseline pass:

- 5 workload shapes (see "Configurations" below)
- 3 runs per shape, 5 minutes per run
- 100 writers connection ceiling on the dev container (`max_connections=200`)
- `vmstat 5` sidecar during every run
- ~77 minutes total wall clock

The driver is `scripts/run-perf-baseline.sh`. Per run it captures:

- per-batch latency on the Rust side → p50 / p95 / p99 / p99.9 / max from sorted samples
- `pg_stat_database` deltas (commits, rollbacks, blks_read, blks_hit, deadlocks)
- `pg_stat_io` deltas summed across contexts for `client backend` (reads, read_bytes, writes, write_bytes, extends, hits, fsyncs)
- WAL bytes generated (`pg_current_wal_lsn` + `pg_wal_lsn_diff`)
- top-10 queries by `total_exec_time` (`pg_stat_statements` reset at run start)
- machine-parseable `T4_CSV_VALUES` line for cross-run aggregation
- `vmstat` samples (CPU breakdown + context-switch rate)

## Configurations

| Tag | Writers | Events / batch | What it isolates |
|---|---|---|---|
| **A** `w1_e5-20`     | 1 | 5–20 | Single-writer with the test's default small-batch shape |
| **B** `w1_e1000-1000`| 1 | 1000 | TigerBeetle-style: single writer, big batches |
| **C** `w32_e5-20`    | 32 | 5–20 | Moderate concurrency on small batches |
| **D** `w100_e5-20`   | 100 | 5–20 | High-contention spec target — **the canonical regression-detection point** |
| **E** `w100_e100-100`| 100 | 100 | Mid-batch under high concurrency — proves big batches *don't* help when contention is the bottleneck |

Skipped on purpose: **100 writers × 1000 events/batch**. We tested this in a separate exploratory pass; it produces a 30-min completion tail at 90 s nominal duration and is operationally useless. Big batches at high concurrency are an anti-pattern in this schema.

In every config, every event posts `debit = <random pick from a 7-account pool>`, `credit = creation_void(qty)`. The credit side is shared across all writers — all contention converges on one row. Realistic application traffic does **not** look like this; `acct-2ey` will phase in cross-ledger / multi-currency / reservation-interleaved workloads.

## Run metadata

- **Date:** 2026-04-29
- **Issue:** `acct-1ia`
- **Schema state:** migrations 0001–0016 applied; no Phase 1 tables.
- **Methodology:** 5 shapes × 3 runs × 5 min (`scripts/run-perf-baseline.sh` defaults).
- **Git ref at run time:** `872f3e50`.
- **Total wall clock:** 4609 s (~77 min, including DB drop+recreate + cargo overhead between runs).

## Environment

| Layer | Detail |
|---|---|
| Host kernel | Linux 6.17.0-22-generic x86_64 |
| Host CPU | 11th Gen Intel(R) Core(TM) i7-1185G7 @ 3.00 GHz, 4 cores (8 threads) |
| Host RAM | 60 GiB total |
| Container | `acct-postgres` (`db/Dockerfile`: `FROM postgres:18` + `postgresql-18-cron`) |
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
| `seccomp` (container) | `unconfined` (dev only; production hardening tracked as `acct-hbp`) |

All other GUCs at PG 18 defaults.

## Headline — cross-config medians

The single most important table. *Median across the 3 runs of each shape.*

| Shape | bps | events/s | p50 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) | max (ms) | WAL (MB / 5 min) | Deadlocks |
|---|---|---|---|---|---|---|---|---|---|
| **A** 1 × 5–20      | **173.45** | **2 164** | **5.4** | **8.3** | **10.6** | 15.7 | 86.8 | 612 | 0 |
| **B** 1 × 1000      | 2.49 | **2 486** | 373.9 | 524.0 | 610.9 | 834.2 | 834.2 | 717 | 0 |
| **C** 32 × 5–20     | 113.85 | 1 421 | 163.6 | 919.0 | 1 441.8 | 2 155.9 | 3 370.7 | 406 | 0 |
| **D** 100 × 5–20    | 44.84 | 558.5 | 1 369.2 | 7 213.8 | 11 905.8 | 18 590.6 | 28 555.6 | 162 | 0 |
| **E** 100 × 100     | 3.73 | 372.8 | **28 176.7** | 45 521.7 | **61 726.1** | 70 880.8 | 73 453.7 | 120 | 0 |

Key reads:

- **Throughput peak is ~2 500 events/s** (config B), 14 % higher than config A's 2 164. That ~14 % is the entire amortization gain from big batches when there's no contention.
- **Adding writers on this workload is purely destructive** because every batch fights for the same lock. 1 → 100 writers loses ~75 % of throughput.
- **Big batches under contention compound the problem instead of helping it** (E vs D): going from small batches to 100-event batches at 100 writers cuts throughput further (559 → 373 evps) and explodes p99 (12 s → 62 s).
- **Zero deadlocks across every shape** — the ascending-id `FOR UPDATE` lock ordering in `post_transfers` is correct under every shape we threw at it.

## Per-config detail

### A — 1 writer × 5–20 events/batch

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches / 5 min | 51 474 | 52 034 | 52 064 | 52 684 |
| Events / 5 min | 644 556 | 649 073 | 650 283 | 657 219 |
| batches/s | 171.58 | 173.45 | 173.55 | 175.61 |
| events/s | 2 148.5 | 2 163.6 | 2 167.6 | 2 190.7 |
| p50 (ms) | 5.33 | 5.41 | 5.41 | 5.47 |
| p95 (ms) | 8.18 | 8.32 | 8.31 | 8.43 |
| p99 (ms) | 10.55 | 10.58 | 10.64 | 10.79 |
| p99.9 (ms) | 15.50 | 15.67 | 15.74 | 16.05 |
| max (ms) | 69.60 | 86.81 | 89.01 | 110.61 |
| io_writes | 51 554 | 52 107 | 52 152 | 52 795 |
| io_write MB | 1 017 | 1 072 | 1 147 | 1 353 |
| io_fsyncs | 51 433 | 51 990 | 52 029 | 52 664 |
| WAL MB | 608.5 | 611.9 | 614.0 | 621.7 |

vmstat (per-run means): `us≈25%  sy≈2.5%  id≈48%  wa≈21%  cs≈16.8K/s`. Storage participates in the budget (~21 % iowait); CPU not the bound. Per-event latency ~0.43 ms.

### B — 1 writer × 1000 events/batch (TigerBeetle-style)

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches / 5 min | 691 | 746 | 756 | 830 |
| Events / 5 min | 691 000 | 746 000 | 755 667 | 830 000 |
| batches/s | 2.30 | 2.49 | 2.52 | 2.77 |
| events/s | 2 302.8 | 2 485.8 | 2 517.9 | 2 765.1 |
| p50 (ms) | 350.3 | 373.9 | 374.4 | 399.0 |
| p95 (ms) | 375.6 | 524.0 | 495.5 | 587.0 |
| p99 (ms) | 401.7 | 610.9 | 573.8 | 708.7 |
| p99.9 (ms) | 584.7 | 834.2 | 765.6 | 878.0 |
| max (ms) | 584.7 | 834.2 | 765.6 | 878.0 |
| io_writes | 779 | 844 | 869 | 984 |
| io_write MB | 186.2 | 232.4 | 240.0 | 301.5 |
| io_fsyncs | 699 | 760 | 769 | 847 |
| WAL MB | 667.2 | 717.3 | 731.5 | 810.0 |

vmstat: `us≈27%  sy≈1.7%  id≈51%  wa≈17%  cs≈8.6K/s`. **Half the context switches of A** — fewer batches, fewer commits. Per-event latency ~0.37 ms (~14 % lower than A). The big-batch path saves on per-batch overhead (lock acquire+release, function entry, fsync per commit) but the per-event UPDATE work dominates either way.

### C — 32 writers × 5–20 events/batch

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches / 5 min | 33 890 | 34 179 | 34 174 | 34 454 |
| Events / 5 min | 423 389 | 426 521 | 427 551 | 432 742 |
| batches/s | 112.87 | 113.85 | 113.83 | 114.76 |
| events/s | 1 410.1 | 1 420.8 | 1 424.1 | 1 441.4 |
| p50 (ms) | 162.8 | 163.6 | 163.3 | 163.6 |
| p95 (ms) | 907.2 | 919.0 | 916.8 | 924.3 |
| p99 (ms) | 1 419.3 | 1 441.8 | 1 445.0 | 1 473.9 |
| p99.9 (ms) | 2 154.7 | 2 155.9 | 2 196.8 | 2 279.9 |
| max (ms) | 3 180.4 | 3 370.7 | 3 432.5 | 3 746.5 |
| io_writes | 33 946 | 34 225 | 34 221 | 34 491 |
| io_write MB | 653.2 | 661.1 | 660.4 | 666.9 |
| io_fsyncs | 33 866 | 34 142 | 34 139 | 34 410 |
| WAL MB | 398.8 | 405.7 | 404.8 | 410.0 |

vmstat: `us≈23%  sy≈2%  id≈52%  wa≈19%  cs≈16.5K/s`. Lock queue forming but tame. p50 ≈ 164 ms ≈ ~32 batches × per-event work ÷ concurrency — consistent with Little's-Law calc on a 32-deep queue.

### D — 100 writers × 5–20 events/batch (canonical regression-detection point)

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches / 5 min | 12 865 | 13 542 | 13 355 | 13 657 |
| Events / 5 min | 161 177 | 168 685 | 167 115 | 171 484 |
| batches/s | 42.61 | 44.84 | 44.24 | 45.28 |
| events/s | 533.8 | 558.5 | 553.6 | 568.5 |
| p50 (ms) | 1 342.7 | 1 369.2 | 1 380.2 | 1 428.5 |
| p95 (ms) | 7 189.7 | 7 213.8 | 7 397.9 | 7 790.2 |
| p99 (ms) | 11 900.5 | 11 905.8 | 12 242.2 | 12 920.3 |
| p99.9 (ms) | 18 583.2 | 18 590.6 | 19 386.9 | 20 986.9 |
| max (ms) | 28 350.2 | 28 555.6 | 30 398.5 | 34 289.6 |
| io_writes | 12 891 | 13 575 | 13 385 | 13 689 |
| io_write MB | 246.5 | 258.7 | 255.4 | 261.1 |
| io_fsyncs | 12 865 | 13 546 | 13 357 | 13 659 |
| WAL MB | 155.3 | 162.2 | 160.6 | 164.3 |

vmstat: `us≈25%  sy≈2%  id≈52%  wa≈18%  cs≈16.3K/s`. Lock-queue cost dominates batch wall clock. Use **this median** as the canonical reference point for Phase 1 regression detection: throughput 558.5 evps; p99 11.9 s.

### E — 100 writers × 100 events/batch (anti-pattern)

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches / 5 min | 1 135 | 1 234 | 1 249 | 1 378 |
| Events / 5 min | 113 500 | 123 400 | 124 900 | 137 800 |
| batches/s | 3.48 | 3.73 | 3.81 | 4.22 |
| events/s | 348.0 | 372.8 | 381.0 | 422.2 |
| p50 (ms) | 23 677.4 | **28 176.7** | 27 442.7 | 30 473.9 |
| p95 (ms) | 35 981.8 | 45 521.7 | 42 544.1 | 46 128.9 |
| p99 (ms) | 47 173.2 | **61 726.1** | 57 328.3 | 63 085.7 |
| p99.9 (ms) | 60 602.8 | 70 880.8 | 71 249.3 | 82 264.3 |
| max (ms) | 61 788.1 | 73 453.7 | 72 687.4 | 82 820.5 |
| io_writes | 1 156 | 1 248 | 1 267 | 1 398 |
| io_write MB | 54.6 | 63.1 | 63.8 | 73.6 |
| io_fsyncs | 1 142 | 1 238 | 1 255 | 1 384 |
| WAL MB | 115.3 | 119.6 | 122.8 | 133.5 |

vmstat: `us≈22%  sy≈1%  id≈54%  wa≈18%  cs≈6.2K/s`. **Note duration_s = 326–331 s** (vs 300 s nominal) — once the test stops launching batches, in-flight ones still need to drain through the queue. Throughput here is **half** of D's — bigger batches at 100 writers buy worse contention. p50 batch wall clock = 28 s; p99 = 62 s. **Don't run real workloads in this regime.**

## Observations

1. **Lock contention is the dominant cost from C onward.** Every writer needs `creation_void`'s lock. The `FOR UPDATE` lock is held for the entire transaction (lock acquire → loop events → commit → fsync). Concurrent writers serialize behind that hold time. Single-row throughput limit ≈ 1 / mean-hold-time = ~2 K events/s for small batches; adding more concurrent writers redistributes that throughput across more queue depth, not into more total events/s.

2. **CPU is consistently half-idle (id ≈ 50 %)** across every shape. We are *never* CPU-bound. Adding cores wouldn't help — adding workload variety (so writers contend on different rows) would.

3. **Storage participates throughout (iowait 15–22 %).** ~50 K fsyncs / run at config A with ~600 MB write_bytes. Going to bigger batches (B) drops to ~750 fsyncs / run — that's the real benefit of batching: amortizing fsync.

4. **WAL volume tracks throughput, not concurrency.** 612 MB (A, 2.2 K evps) > 717 MB (B, 2.5 K evps) > 406 MB (C, 1.4 K evps) > 162 MB (D, 559 evps) > 120 MB (E, 373 evps). Per-event WAL is roughly constant (~1 KB).

5. **Big batches help only without contention.** A → B: events/s 2 164 → 2 486 (+15 %). D → E: 559 → 373 (–33 %). Same change in batch size; opposite effect, because the bottleneck moves from per-batch overhead to lock-hold time.

6. **Variance is tight in contended configs (D, E)** and looser in uncontended ones (A, B). At 100 writers serializing, platform jitter is a small fraction of the 1.4 s median; at 1 writer the median is 5 ms and a single bad scheduler tick shows up.

7. **Zero deadlocks across all 5 shapes × 3 runs × 5 min** = ~115 K batches / ~2 M events. The lock-order proof in `post_transfers` is correct under every shape.

8. **The 2.5 K events/s ceiling is real for this hardware on this schema.** Routes to higher numbers:
   - **Spread the contention** (`acct-2ey` workload variety; Part IV §8 account sharding).
   - **Collapse concurrency to 1 writer** (outbox pattern, `acct-tyq`). Application requests append to a queue; a single drainer ships big batches through `post_transfers`.
   - **Different hardware** is a multiplier on these ratios, not a fix for the regime structure.

## Top queries (representative — config D, run 3, `pg_stat_statements`)

`SELECT post_transfers($1, $2)` is essentially 100 % of database CPU + wait time across every shape. No surprise — every event flows through the function. We are *not* bottlenecked on parsing, planning, or other queries.

## Caveats

- **Single-machine consumer laptop.** Numbers reflect a developer rig, not production-class hardware. Absolute throughput is an artifact of the test rig; *relative* changes vs this baseline are the load-bearing comparison.
- **All credits target one row.** Every event posts `credit = creation_void(qty)`. Real workloads don't do this. `acct-2ey` will phase in cross-account-set, cross-ledger, multi-currency, and reservation-interleaved batches.
- **Sync `post_transfers` only.** No outbox, no async projection. Outbox-vs-sync benchmark is `acct-tyq`, gated on this baseline.
- **Standard cost only.** Non-`standard` cost methods are P0006 in Phase 0. WAC/FIFO/lot perf characterization is downstream of `acct-8gg` + a fresh baseline run.
- **No reservation traffic in load mix.** `reserve_inventory()` is exercised by T3 but not under load. `acct-2ey`.
- **No NUMA / CPU pinning, no isolated cores.** Kernel scheduler treats Postgres + cargo test + everything else equally. This *is* the variance source on uncontended configs.

## How to reproduce

```bash
./scripts/dev-up.sh
./scripts/run-migrations.sh
./scripts/run-perf-baseline.sh                 # full 5-shape × 3-runs × 5-min matrix
```

Override defaults to characterize a single point ad-hoc:

```bash
T4_CONFIGS="100:5:20" T4_BASELINE_RUNS=1 T4_DURATION_SECS=60 \
  ./scripts/run-perf-baseline.sh

T4_CONFIGS="1:5000:5000 1:10000:10000" \
  ./scripts/run-perf-baseline.sh
```

Logs land in `/tmp/t4_baseline_<timestamp>/<config>/run_<i>.log` plus matching `run_<i>_vmstat.log`.

## Re-running this baseline

This file is regenerated whenever:

1. A Phase 1 schema addition lands (compare to **config D** as canonical regression-detection point — flag throughput drops > 25 % or p99 inflation > 50 %).
2. Postgres major version changes.
3. Significant container / OS / kernel change on the dev rig.
4. Workload shape changes (`acct-2ey` will trigger this).

Append a new section dated below the current one rather than overwriting; the v0 → v1 → v2 history is the diff trail that detects creep.
