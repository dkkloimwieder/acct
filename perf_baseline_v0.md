# Perf baseline v0 — Phase 0 schema, workload-shape matrix

This file records reference perf measurements across **seven workload
shapes** on the simplest schema (Phase 0; migrations 0001–0017, no
Phase 1 tables). Per the 2026-04-29 directive (Part VII Q2 resolved),
every Phase 1 complexity addition is diff'd against these numbers.

**These are not SLOs.** They are a yardstick.

## TL;DR

The system has a clean five-regime structure:

1. **Single-writer regime (A, B)** — peaks at **~2.5 K events/s**. Big batches give a modest ~15 % bump over small batches by amortizing per-batch overhead. CPU is half-idle and iowait is 15–22 %; the bound is per-event work + commit fsync, not contention.
2. **Concurrent + converged-on-one-row (C, D)** — every additional concurrent writer that hits the same lock target costs throughput. 1→32→100 writers takes events/s from 2 164 → 1 421 → 559. Latencies grow from single-digit-ms (1 writer) to **11.9 s p99** (100 writers).
3. **Concurrent + big batches under contention (E)** — catastrophic. 100 writers × 100-event batches finishes only **373 events/s** with **p99 ≈ 62 s** because each batch holds the shared lock for ~20 s and the queue compounds.
4. **Concurrent + spread across accounts (F)** — the realistic shape. 100 writers, same small batches as D, but spread across 50 SKUs × 2 locations = 100 distinct accounts. Throughput **2.3× D** (1 274 evps), **p50 26× lower** (51 ms vs 1 369 ms), **p99 1.4× lower** (8.3 s). CPU usage climbs from 25 % → 40 % — the system is doing actual work instead of queuing. **This is much closer to what real workloads look like.**
5. **Naive outbox: writers → ledger_outbox → 1 drainer (G)** — caller-perceived tail latency collapses (**133 ms p99** vs F's 8.3 s — 62× lower) because writers no longer queue on `post_transfers` row locks; they just `INSERT` and return. But total committed-events throughput **falls to 140 evps** — 9× LESS than F. The single sequential drainer can't match what 100 contending writers manage in parallel, because shape B's amortization comes from packing 1 000 events into ONE `post_transfers` call, not from running 1 000 calls in series. End-to-end latency (caller-submit → ledger-commit) is dominated by queue residency: p50 **186 seconds** as the queue grows unbounded throughout the run.

The route to higher throughput on this hardware is **not "tune Postgres"** — it's spreading contention (the natural state of realistic workloads, plus account sharding for unavoidable hot rows per Part IV §8). The naive single-drainer outbox (G) does NOT collapse concurrency into shape-B-class throughput; doing that requires a **super-batched** drainer that merges events from many outbox rows into one `post_transfers` call (filed as `acct-hbg`).

## Methodology

The dev hardware is a consumer laptop on a multi-tenant desktop kernel. A single long run on this rig is not statistically reliable — background processes, thermal throttling, and OS scheduling jitter dominate at the percentile tails. So we take **N runs back-to-back per shape** and report the variance across them.

Configuration of this baseline pass:

- 7 workload shapes (see "Configurations" below)
- 3 runs per shape, 5 minutes per run (G adds a 60 s grace drain after the writer phase)
- 100 writers connection ceiling on the dev container (`max_connections=200`)
- `vmstat 5` sidecar during every run
- ~95 minutes total wall clock (A–F: ~77 min on `872f3e50`; G: ~18 min on `ff5d6b5`, same day)

The driver is `scripts/run-perf-baseline.sh`. Per run it captures:

- per-batch latency on the Rust side → p50 / p95 / p99 / p99.9 / max from sorted samples
- `pg_stat_database` deltas (commits, rollbacks, blks_read, blks_hit, deadlocks)
- `pg_stat_io` deltas summed across contexts for `client backend` (reads, read_bytes, writes, write_bytes, extends, hits, fsyncs)
- WAL bytes generated (`pg_current_wal_lsn` + `pg_wal_lsn_diff`)
- top-10 queries by `total_exec_time` (`pg_stat_statements` reset at run start)
- machine-parseable `T4_CSV_VALUES` line for cross-run aggregation
- `vmstat` samples (CPU breakdown + context-switch rate)

## Configurations

| Tag | Writers | Events / batch | Workload | What it isolates |
|---|---|---|---|---|
| **A** `w1_e5-20`     | 1 | 5–20 | shared credit | Single-writer with the test's default small-batch shape |
| **B** `w1_e1000-1000`| 1 | 1000 | shared credit | TigerBeetle-style: single writer, big batches |
| **C** `w32_e5-20`    | 32 | 5–20 | shared credit | Moderate concurrency on small batches |
| **D** `w100_e5-20`   | 100 | 5–20 | shared credit | High-contention spec target — pessimum, **canonical regression-detection point for the worst case** |
| **E** `w100_e100-100`| 100 | 100 | shared credit | Mid-batch under high concurrency — proves big batches *don't* help when contention is the bottleneck |
| **F** `w100_e5-20`   | 100 | 5–20 | **cross-account spread (50 SKUs × 2 locations = 100 accounts)** | Realistic-shape concurrency — same envelope as D but contention spreads. **The realistic-traffic regression-detection point.** |
| **G** `w100_e5-20` (outbox) | 100 | 5–20 | **same cross-account spread as F**, but writers `INSERT` into `ledger_outbox` and one drain worker dispatches to `post_transfers` | Naive single-drainer outbox vs direct sync. **The outbox-vs-sync comparison point** (`acct-tyq`). |

Skipped on purpose: **100 writers × 1000 events/batch**. We tested this in a separate exploratory pass; it produces a 30-min completion tail at 90 s nominal duration and is operationally useless. Big batches at high concurrency are an anti-pattern in this schema.

**Workload shapes:**

- **A–E (`tests/load_deadlock_freedom.rs`)** — every event posts `debit = <random pick from a 7-account pool>`, `credit = creation_void(qty)`. The credit side is shared across all writers — all contention converges on one row.
- **F (`tests/load_realistic_workload.rs`, `acct-2ey`)** — `bin_move` events across 50 SKUs × 2 locations. Each event picks a random SKU and direction (MAIN→OUT or OUT→MAIN). Both `debit` and `credit` rotate across the 100-account pool. Contention spreads. **Closer to realistic application traffic.**
- **G (`tests/load_outbox_workload.rs`, `acct-tyq`)** — same workload as F, but writers `INSERT` the event batch into `ledger_outbox` (returns immediately, no ledger lock). One sequential drain worker (`tests/common/outbox_worker.rs`) pulls pending rows in batches of 1 000 with `FOR UPDATE SKIP LOCKED` and runs each row through `post_transfers`. Per-row error isolation via savepoints. After the writer phase, the worker gets a 60 s grace drain window before being hard-stopped; the run reports both enqueue throughput and **commit throughput** (the apples-to-apples comparison vs F's events/s). Two latency families captured: writer-side `enqueue_us` (INSERT round-trip) and `queue_us` = `committed_at − enqueued_at` (drain residency).

## Run metadata

- **Date:** 2026-04-29
- **Issues:** `acct-1ia` (shapes A–E), `acct-2ey` (shape F), `acct-tyq` / `acct-epu` (shape G).
- **Schema state:** A–F on migrations 0001–0016; G on 0001–0017 (adds `ledger_outbox`).
- **Methodology:** 7 shapes × 3 runs × 5 min (`scripts/run-perf-baseline.sh`); G also runs a 60 s grace drain after the writer phase.
- **Git refs at run time:** `872f3e50` (A–E), `997a680` (F), `ff5d6b5` (G).
- **Total wall clock:** ~95 min combined (A–E ~50 min; F ~27 min; G ~18 min).

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
| **A** 1 × 5–20  · shared credit | **173.45** | **2 164** | **5.4** | **8.3** | **10.6** | 15.7 | 86.8 | 612 | 0 |
| **B** 1 × 1000  · shared credit | 2.49 | **2 486** | 373.9 | 524.0 | 610.9 | 834.2 | 834.2 | 717 | 0 |
| **C** 32 × 5–20 · shared credit | 113.85 | 1 421 | 163.6 | 919.0 | 1 441.8 | 2 155.9 | 3 370.7 | 406 | 0 |
| **D** 100 × 5–20 · shared credit (worst case) | 44.84 | 558.5 | 1 369.2 | 7 213.8 | 11 905.8 | 18 590.6 | 28 555.6 | 162 | 0 |
| **E** 100 × 100 · shared credit (anti-pattern) | 3.73 | 372.8 | **28 176.7** | 45 521.7 | **61 726.1** | 70 880.8 | 73 453.7 | 120 | 0 |
| **F** 100 × 5–20 · **cross-account spread** | **102.16** | **1 274.2** | **51.4** | 5 410.5 | 8 250.7 | 14 469.2 | 16 767.8 | 431 | 0 |
| **G** 100 × 5–20 · outbox + 1 drainer | (enqueue 1 304.5; commit 11.7) † | **140.4 ‡** | **72.1 §** | **105.8 §** | **133.1 §** | **183.2 §** | 4 461 § | 587 | 0 |

† For G, `bps` = enqueue rate, since writers no longer call `post_transfers` directly. `1 304.5 / s` is the rate at which writer batches land in the outbox; `11.7 / s` is the rate at which drained outbox rows commit (median 4 057 / 360 s).
‡ For G, `events/s` is **commit throughput** (`committed_events / total_elapsed`), apples-to-apples with F's events/s. Enqueue throughput is **16 302 ev/s** — but those events sit in the queue for minutes (see queue_us below), and only 140 ev/s actually land in the ledger.
§ For G, the latency columns measure **writer-perceived `enqueue_us`** (INSERT round-trip). Drain residency is enormous and is reported separately:
  | metric | G `queue_us` median |
  |---|---|
  | p50 | 186 s |
  | p99 | 336 s |
  | max | 338 s |
  Writer-perceived end-to-end (had they waited) ≈ `enqueue_us + queue_us` ≈ 186 s p50.

Key reads:

- **Throughput peak is ~2 500 events/s** (config B), 14 % higher than config A's 2 164. That ~14 % is the entire amortization gain from big batches when there's no contention.
- **Adding writers on the shared-credit workload is purely destructive** (A → C → D) because every batch fights for the same lock. 1 → 100 writers loses ~75 % of throughput.
- **Big batches under shared-credit contention compound the problem instead of helping it** (E vs D): going from small batches to 100-event batches at 100 writers cuts throughput further (559 → 373 evps) and explodes p99 (12 s → 62 s).
- **Spreading contention recovers most of the loss** (F vs D): same writer count, same batch size, just rotating across 100 accounts instead of 1. Throughput **2.3× higher** (1 274 vs 559 evps), **p50 26× lower** (51 vs 1 369 ms), **p99 1.4× lower** (8.3 vs 11.9 s). The FOR UPDATE lock-queue cost dominates D; F approaches what the schema can actually do.
- **Naive outbox is throughput-catastrophic but tail-latency-excellent** (G vs F): same workload, same writer count, same batch shape — but writers `INSERT` instead of calling `post_transfers`. Caller p99 collapses 8.3 s → 133 ms (62× lower) because writers never queue on the ledger lock. **But** total commit throughput crashes from 1 274 → 140 evps (9× lower). The single drainer can't match what 100 contending writers do in parallel, because per-call `post_transfers` overhead doesn't amortize across rows the way packed events inside a single call do (shape B). The path to shape-B-class throughput via outbox requires a **super-batched** drainer that merges multiple rows' events into one `post_transfers` call — filed as `acct-hbg`.
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

### F — 100 writers × 5–20 events/batch, cross-account spread (50 SKUs × 2 locations)

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches / 5 min | 30 560 | 30 751 | 31 028 | 31 773 |
| Events / 5 min | 381 480 | 383 536 | 387 178 | 396 519 |
| batches/s | 101.55 | 102.16 | 103.09 | 105.55 |
| events/s | 1 267.6 | 1 274.2 | 1 286.3 | 1 317.2 |
| p50 (ms) | 50.04 | 51.43 | 51.12 | 51.90 |
| p95 (ms) | 5 156.7 | 5 410.5 | 5 424.6 | 5 706.5 |
| p99 (ms) | 7 724.7 | 8 250.7 | 8 121.6 | 8 389.4 |
| p99.9 (ms) | 12 377.8 | 14 469.2 | 13 792.8 | 14 531.5 |
| max (ms) | 14 956.7 | 16 767.8 | 17 007.2 | 19 297.0 |
| io_writes | 30 653 | 30 831 | 31 123 | 31 884 |
| io_write MB | 656.5 | 660.0 | 721.0 | 846.5 |
| io_fsyncs | 30 568 | 30 746 | 31 038 | 31 801 |
| WAL MB | 428.5 | 431.2 | 435.8 | 447.8 |

vmstat: `us≈40%  sy≈2.5%  id≈38%  wa≈16%  cs≈14.9K/s`. **CPU usage 40 %** vs D's 25 % — the system is doing more actual work per second instead of waiting in line. Storage budget is roughly proportional to throughput; iowait is similar to D. Workload setup includes inserting 50 BENCH-NNN SKUs and 100 stock_available accounts pre-balanced to 10K units each at the start of every run (~1–2 s overhead, not in the measured window).

### F vs D — the realistic-traffic headline

The **same envelope** (100 writers × small batches × 5 min × 3 runs) — only the workload shape differs. F substitutes "credit always converges on `creation_void`" with "credit and debit both rotate across 100 distinct accounts." Outcomes:

| Metric | D (shared credit) | F (cross-account) | F / D |
|---|---|---|---|
| events/s | 558.5 | 1 274.2 | **2.28×** |
| p50 (ms) | 1 369.2 | 51.4 | 0.038 (≈ 26.6× lower) |
| p95 (ms) | 7 213.8 | 5 410.5 | 0.75 |
| p99 (ms) | 11 905.8 | 8 250.7 | 0.69 |
| max (ms) | 28 555.6 | 16 767.8 | 0.59 |
| WAL/5min (MB) | 162 | 431 | 2.66× |
| CPU user % | 24.6 | 40.2 | +63 % |
| CPU idle % | 52.5 | 38.2 | -27 pp |

**What this says:**

- **Throughput more than doubles, p50 collapses by 26×.** Most application latency disappears when contention spreads.
- **The tail still drags.** p95/p99 improve 25–35 % but stay multi-second. With 50 SKUs and 100 writers each holding ~12 accounts in flight per batch, birthday-paradox-style overlaps on individual SKUs are still common; some batches still queue.
- **CPU user % climbs 25 → 40 %** — half-idle drops. Spreading contention converts queue-wait into actual work.
- **WAL throughput climbs proportionally** (162 → 431 MB / 5 min). Per-event WAL is roughly constant; we're just doing more events.
- **Zero deadlocks** across the same ~85 K-batch sample size as D. Lock-order proof holds under spread.

For Phase 1 regression detection going forward, **use F's median** as the realistic-traffic reference (1 274 evps; p50 51 ms; p99 8.25 s). Use D's median as the worst-case-contention regression-detection reference (when you specifically want to test that the lock-order proof still works under hot-row pressure).

### G — 100 writers × 5–20 events/batch, outbox + 1 drainer (cross-account spread)

Same workload as F, same fixture (50 BENCH SKUs × 2 locations, pre-balanced to 10 K). Writers `INSERT` event batches into `ledger_outbox`; one sequential drain worker pulls pending rows in batches of 1 000 with `FOR UPDATE SKIP LOCKED` and runs each through `post_transfers`. After the 5-min writer phase, the worker gets a 60 s grace drain window and is hard-stopped (the queue would otherwise take **hours** to drain at the worker's natural rate — see queue depth below).

Two latency families are measured: writer-side `enqueue_us` (INSERT round-trip) and `queue_us` = `committed_at − enqueued_at` for committed rows.

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches enqueued / 5 min | 367 760 | 391 394 | 388 446 | 406 184 |
| Events enqueued / 5 min | 4 600 085 | 4 890 993 | 4 856 136 | 5 077 330 |
| Enqueue batches/s | 1 225.7 | 1 304.5 | 1 294.7 | 1 353.8 |
| Enqueue events/s | 15 332 | 16 302 | 16 186 | 16 923 |
| **Commit events / run** (committed in writer-phase + 60 s grace) | 50 383 | **50 588** | 50 520 | 50 590 |
| **Commit events/s (writer + drain phase, 360 s)** | 139.9 | **140.4** | 140.3 | 140.5 |
| `enqueue_us` p50 (ms) | 68.6 | **72.1** | 72.9 | 77.9 |
| `enqueue_us` p95 (ms) | 101.9 | 105.8 | 104.8 | 106.6 |
| `enqueue_us` p99 (ms) | 123.6 | **133.1** | 130.4 | 134.5 |
| `enqueue_us` p99.9 (ms) | 176.7 | 183.2 | 181.8 | 185.5 |
| `enqueue_us` max (ms) | 4 263 | 4 461 | 4 445 | 4 612 |
| `queue_us` p50 (s) | 177.5 | **186.0** | 186.6 | 196.3 |
| `queue_us` p99 (s) | 330.4 | 335.7 | 334.0 | 336.0 |
| `queue_us` max (s) | 333.0 | 338.2 | 336.6 | 338.4 |
| **Max outbox depth** | 365 742 | **388 331** | 385 733 | 403 127 |
| Final committed (rows) | 4 018 | 4 057 | 4 046 | 4 063 |
| Drain-phase wall clock (s) | 60.16 | 60.17 | 60.19 | 60.23 |
| io_writes | 79 762 | 83 266 | 83 478 | 87 405 |
| io_fsyncs | 64 655 | 66 473 | 66 255 | 67 636 |
| WAL MB | 561.4 | 587.4 | 598.4 | 646.2 |

vmstat: `us≈47%  sy≈8.8%  id≈28%  wa≈12%  cs≈42K/s`. **CPU is busier than F (47 % vs 40 % user, system time 8.8 % vs 2.5 %), and context-switch rate is ~3× F (42 K/s vs 14.9 K/s).** The system is doing more *small* work units per second: each outbox row triggers ~4 statements (SAVEPOINT / `post_transfers` / RELEASE / UPDATE), so the worker thrashes between micro-tasks. INSERT is fast — 0.21 ms mean per `pg_stat_statements` — but `post_transfers` calls average 38 ms each, and the worker can only run them sequentially. Drain rate from `pg_stat_statements`: ~12 calls/s × ~12 events/call ≈ 144 events/s, matching the headline.

### G vs F — naive outbox is faster for the caller, slower for the ledger

The same workload (100 writers, cross-account spread, 5–20 events/batch). Only the *path* differs: F calls `post_transfers` directly; G writes to a queue and lets a single drainer dispatch.

| Metric | F (sync) | G (outbox, 1 drainer) | G / F |
|---|---|---|---|
| Caller-perceived p50 (ms) | 51.4 | 72.1 | 1.40× (G slightly slower) |
| Caller-perceived p99 (ms) | 8 250.7 | **133.1** | **0.016× (62× lower)** |
| Caller-perceived max (ms) | 16 767.8 | 4 461 | 0.27× |
| Committed events/s | 1 274.2 | **140.4** | **0.11× (9× lower)** |
| Caller-side end-to-end p50 (had they waited) | 51 ms | **186 000 ms** | 3 600× (G unbounded) |
| WAL/5 min (MB) | 431 | 587 | 1.36× |
| CPU user % | 40.2 | 46.6 | +16 % |
| CPU sys % | 2.5 | 8.8 | +3.5× |
| Context switches/s | 14.9 K | 42 K | +2.8× |

**What this says.** Outbox decouples the caller from the ledger lock — and that buys a dramatically tighter caller p99 (62× lower) because 100 INSERTs don't fight each other for one row's lock the way 100 `post_transfers` calls do. **But** total commit throughput is bottlenecked at the single drainer's per-call `post_transfers` cost, which doesn't amortize across rows the way packed events within a single call do (shape B's win). The drainer measures ~12 `post_transfers` calls/s × ~12 events/call ≈ 144 evps — within rounding of the headline 140.4.

The queue grows unbounded throughout the run (max depth 388 K rows; queue_us p50 = 186 s) because enqueue rate (16 302 ev/s) far exceeds commit rate (140 ev/s). End-to-end caller latency (had the caller waited for ledger commit) would be **186 seconds p50** — the row's queue residency. So the apparent caller-p99 win is real *only if the application can tolerate eventual semantics*; for sync-required flows (an invoice posting, a payment), F is dominant by every measure.

The path to actually getting shape-B-class throughput out of an outbox is to **super-batch**: drain N pending rows and concatenate their event arrays into ONE `post_transfers` call. That recovers per-call amortization at the cost of coarser error attribution (one bad event poisons the bundle; recover via per-row fallback). Filed as `acct-hbg`.

**For Phase 1 regression detection, F remains the reference.** G is filed alongside as the outbox-vs-sync comparison point — re-run G if/when D3 (the "synchronous `post_transfers`" decision) is reopened.

## Observations

1. **Lock contention is the dominant cost from C onward in the shared-credit shapes.** Every writer needs `creation_void`'s lock. The `FOR UPDATE` lock is held for the entire transaction (lock acquire → loop events → commit → fsync). Concurrent writers serialize behind that hold time. Single-row throughput limit ≈ 1 / mean-hold-time = ~2 K events/s for small batches; adding more concurrent writers redistributes that throughput across more queue depth, not into more total events/s.

2. **CPU is consistently 30–55 % idle across every shape.** Even F with 100 writers and contention spread gets only to ~38 % idle. We are *never* CPU-bound. Adding cores wouldn't help; the next leverage point is more workload spread or platform-level changes (storage layer for iowait, kernel for ctx-switch overhead).

3. **Storage participates throughout (iowait 15–22 %).** ~50 K fsyncs / run at config A with ~600 MB write_bytes. Going to bigger batches (B) drops to ~750 fsyncs / run — that's the real benefit of batching: amortizing fsync.

4. **WAL volume tracks throughput, not concurrency.** 612 MB (A, 2.2 K evps) > 717 MB (B, 2.5 K evps) > **431 MB (F, 1.27 K evps)** > 406 MB (C, 1.4 K evps) > 162 MB (D, 559 evps) > 120 MB (E, 373 evps). Per-event WAL is roughly constant (~1 KB).

5. **Big batches help only without contention.** A → B: events/s 2 164 → 2 486 (+15 %). D → E: 559 → 373 (–33 %). Same change in batch size; opposite effect, because the bottleneck moves from per-batch overhead to lock-hold time.

6. **Spreading contention is dramatically cheaper than batch-size optimization.** D → F (same writers, same batch size, just different credit-side accounts): +128 % throughput, –96 % p50 latency. D → E (same writers, larger batches, same shared credit): –33 % throughput, +1 957 % p50 latency. Account architecture beats batch tuning every time.

7. **Variance is tight in contended configs (D, E, F)** and looser in uncontended ones (A, B). At 100 writers serializing, platform jitter is a small fraction of the 1.4 s median; at 1 writer the median is 5 ms and a single bad scheduler tick shows up.

8. **Zero deadlocks across all 7 shapes × 3 runs × 5 min** = ~1.4 M batches / ~9 M events (G's 1.17 M batches dominate the count via fast INSERTs). The lock-order proof in `post_transfers` is correct under every shape including the SKIP LOCKED outbox drain pattern.

9. **The ~2.5 K events/s single-writer ceiling is real for this hardware on this schema.** Routes to higher numbers:
   - **Spread the contention** (F demonstrates this — 2.3× lift just from cross-account workload). Real ERP traffic does this naturally; account sharding (Part IV §8) is the explicit Phase 1 mechanism for when natural spread isn't enough.
   - **Collapse concurrency to 1 writer** (outbox pattern, `acct-tyq`) — only works if the drainer **super-batches** events from multiple outbox rows into one `post_transfers` call. The naive single-row-per-call drain (G) measures **140 evps**, ~9× LOWER than F's direct-sync 1 274 evps, because per-call `post_transfers` overhead dominates without amortization. The super-batched variant is filed as `acct-hbg`.
   - **Different hardware** is a multiplier on these ratios, not a fix for the regime structure.

10. **Outbox is a latency/throughput tradeoff, not a free win.** G's caller p99 is 62× lower than F's (133 ms vs 8.25 s) because writers don't queue on `post_transfers` row locks. But G's commit throughput is 9× lower because the single sequential drainer is the bottleneck. End-to-end latency (caller-submitted → ledger-committed) is dominated by queue residency: 186 s p50, 336 s p99 in our run. Outbox makes sense if the application can tolerate eventual semantics on the ledger and the workload's tail-latency budget at the caller is more constrained than its throughput budget. For the typical ERP flow (an invoice posting that needs an accept/reject decision now), F is dominant by every measure. The "yes, adopt outbox" decision needs `acct-hbg`'s super-batched throughput data before it can be made on perf grounds.

## Top queries (representative — config D, run 3, `pg_stat_statements`)

`SELECT post_transfers($1, $2)` is essentially 100 % of database CPU + wait time across every shape. No surprise — every event flows through the function. We are *not* bottlenecked on parsing, planning, or other queries.

## Caveats

- **Single-machine consumer laptop.** Numbers reflect a developer rig, not production-class hardware. Absolute throughput is an artifact of the test rig; *relative* changes vs this baseline are the load-bearing comparison.
- **All credits target one row.** Every event posts `credit = creation_void(qty)`. Real workloads don't do this. `acct-2ey` will phase in cross-account-set, cross-ledger, multi-currency, and reservation-interleaved batches.
- **Outbox characterized for naive single-drainer only.** Shape G measures the 1-row-per-`post_transfers`-call drain pattern. The super-batched variant (multiple rows' events merged into one call) is the path to shape-B-class throughput; not yet built. Filed as `acct-hbg`. D3 (sync `post_transfers`) is unchanged on these results — outbox would regress throughput 9× without super-batching, and even with it the error-attribution tradeoff requires a separate decision.
- **Standard cost only.** Non-`standard` cost methods are P0006 in Phase 0. WAC/FIFO/lot perf characterization is downstream of `acct-8gg` + a fresh baseline run.
- **No reservation traffic in load mix.** `reserve_inventory()` is exercised by T3 but not under load. `acct-2ey`.
- **No NUMA / CPU pinning, no isolated cores.** Kernel scheduler treats Postgres + cargo test + everything else equally. This *is* the variance source on uncontended configs.

## How to reproduce

```bash
./scripts/dev-up.sh
./scripts/run-migrations.sh

# Shapes A-E (shared-credit, deadlock-freedom probe)
./scripts/run-perf-baseline.sh

# Shape F (cross-account spread, realistic shape)
T4_BINARY=load_realistic_workload T4_CONFIGS="100:5:20" \
  ./scripts/run-perf-baseline.sh

# Shape G (outbox + 1 drainer, naive single-row-per-call pattern)
T4_BINARY=load_outbox_workload T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_DRAIN_TIMEOUT_S=60 \
  ./scripts/run-perf-baseline.sh
```

Override defaults to characterize a single point ad-hoc:

```bash
# One config × one run, fast
T4_CONFIGS="100:5:20" T4_BASELINE_RUNS=1 T4_DURATION_SECS=60 \
  ./scripts/run-perf-baseline.sh

# Bigger batch sweep on single writer
T4_CONFIGS="1:5000:5000 1:10000:10000" \
  ./scripts/run-perf-baseline.sh

# Larger SKU pool for shape F (default is 50; bigger pool = less overlap)
T4_BINARY=load_realistic_workload T4_BENCH_SKUS=200 T4_CONFIGS="100:5:20" \
  T4_BASELINE_RUNS=1 T4_DURATION_SECS=60 \
  ./scripts/run-perf-baseline.sh
```

Logs land in `/tmp/t4_baseline_<timestamp>/<config>/run_<i>.log` plus matching `run_<i>_vmstat.log`.

## Re-running this baseline

This file is regenerated whenever:

1. A Phase 1 schema addition lands. Compare two reference points:
   - **Config F** for realistic-traffic regression detection (1 274 evps; p50 51 ms; p99 8.25 s). This is the number that approximates real workload behavior.
   - **Config D** for worst-case-contention regression detection (559 evps; p50 1 369 ms; p99 11.9 s). This is what you check when you specifically want to confirm hot-row behavior hasn't regressed.
   - Flag throughput drops > 25 % or p99 inflation > 50 % on either reference.
2. Postgres major version changes.
3. Significant container / OS / kernel change on the dev rig.
4. Workload shape changes (e.g., `acct-jwg` multi-currency or `acct-9i6` reservation interleaving will trigger this).
5. The `post_transfers` function changes shape (e.g., `acct-0ig` Option B amount-from-function, or new cost methods from `acct-8gg`). G's commit rate is essentially `1 / mean_post_transfers_call_time`, so any change to per-call cost shows up there immediately.
6. **Re-run G specifically** if D3 (sync `post_transfers`) is reopened or if `acct-hbg`'s super-batched variant lands — the comparison point for "do we adopt outbox?" is G's commit rate vs F's events/s vs the super-batch result.

Append a new section dated below the current one rather than overwriting; the v0 → v1 → v2 history is the diff trail that detects creep.
