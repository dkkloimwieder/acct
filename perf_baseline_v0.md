# Perf baseline v0 — Phase 0 schema, workload-shape matrix

This file records reference perf measurements across **thirteen workload
shapes** on the simplest schema (Phase 0; migrations 0001–0018, no
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
6. **Pseudo-sync outbox via LISTEN/NOTIFY (L) is the throughput peak.** Writers `INSERT` then BLOCK on a notification matching their row id. Drainer super-batches as in J, plus emits `pg_notify(channel, '{"id":N,"status":"ok"}')` per row outcome inside the drain tx. Result: **2 876 commit-evps** — 2.26× F, **16% above shape B's 2 486** (which was the prior peak). Caller p99 = 547 ms (15× better than F's 8.3 s) AND end-to-end (caller-submit → ledger-commit is the SAME thing in pseudo-sync). Queue depth bounded naturally by writer count (~100). The architectural insight: pseudo-sync **pipelines** the producer's INSERT stage with the drainer's commit stage on the same DB; writers never touch the account `FOR UPDATE` locks (only the drainer does, serially with itself), eliminating the 100-way contention that limits F. **This is the new architectural reference point for outbox-style work.**

The route to higher throughput on this hardware is **not "tune Postgres"** — it's either spreading contention (F) or pipelining the producer/consumer stages so concurrent writers never share `post_transfers`'s lock surface (L). The naive single-drainer outbox (G) collapses to 9× LESS throughput than F. The super-batched single-drainer (J) recovers half the gap (`acct-hbg`). The pseudo-sync caller pattern (L, `acct-yjn`) eliminates the gap and exceeds F. The hard-cap back-pressure variant (M, `acct-yjn`) is a sad middle ground at 971 evps — the busy-poll pre-INSERT gate burns 5× more CPU context-switches than L for half its throughput; not recommended.

## Methodology

The dev hardware is a consumer laptop on a multi-tenant desktop kernel. A single long run on this rig is not statistically reliable — background processes, thermal throttling, and OS scheduling jitter dominate at the percentile tails. So we take **N runs back-to-back per shape** and report the variance across them.

Configuration of this baseline pass:

- 11 workload shapes (see "Configurations" below)
- 3 runs per shape, 5 minutes per run (G adds 60 s grace drain, J/K add 90 s grace drains)
- 100 writers connection ceiling on the dev container (`max_connections=200`)
- `vmstat 5` sidecar during every run
- ~165 minutes total wall clock (A–F: ~77 min on 2026-04-29; G: ~18 min on 2026-04-29; H: ~15 min on 2026-04-29; I: ~15 min on 2026-04-29; J: ~20 min on 2026-04-29; K: ~20 min on 2026-04-30)

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
| **H** `w100_e5-20` (reserve interleave) | 70 + 30 | 5–20 (posters only) | F's qty workload **plus** concurrent `reserve_inventory()` calls from 30 % of the writer pool | Reservation-flow safety + perf under realistic mixed traffic (`acct-9i6`). |
| **I** `w100_e5-20` (qty + multi-cur value) | 50 + 50 | 5–20 | F's qty workload (50 writers) **plus** value-side `ar_invoice` traffic on shared per-currency cash/ar/ap/revenue accounts (50 writers, USD + EUR) | Multi-currency value-side hot-row contention concurrent with qty traffic (`acct-jwg`). |
| **J** `w100_e5-20` (super-batch outbox, **1 drainer**) | 100 | 5–20 | Same as G but the drainer concatenates events from up to 1 000 outbox rows into ONE `post_transfers` call (per-row fallback on error) | Does super-batched outbox recover shape B's amortization? (`acct-hbg`) |
| **K** `w100_e5-20` (super-batch outbox, **4 drainers**) | 100 | 5–20 | Same as J but **4** concurrent drainers. Each independently runs `super_batched_drain_loop` with `FOR UPDATE SKIP LOCKED` (claims are non-overlapping by construction) | Does multi-worker drain beat single-worker J, or does it regress toward shape F's account-lock contention? (`acct-dtv`) |
| **L** `w100_e5-20` (pseudo-sync via LISTEN/NOTIFY) | 100 | 5–20 | Same writers/fixture as J. Each writer INSERTs, then **BLOCKS** awaiting a notification matching its row id. Drainer = J's super-batched single drainer plus `pg_notify(channel, '{"id":N,"status":"ok"}')` per row inside the drain tx. Single shared `PgListener` task fans notifications out to per-id oneshot channels in the test process | Pseudo-sync caller semantics: throughput vs F + caller-perceived end-to-end latency; **the LISTEN/NOTIFY exploration** (`acct-yjn`) |
| **M** `w100_e5-20` (async outbox + hard-cap back-pressure) | 100 | 5–20 | Same as J (fire-and-forget caller, super-batched drainer) PLUS a writer-side gate: before each INSERT, the writer polls `count(pending) < cap` (default cap=200, poll cadence=2 ms). When at the cap, writers busy-wait until the drainer drops the queue depth | Producer-side back-pressure on a fast drainer; tests whether bounding the queue without LISTEN/NOTIFY rescues anything (`acct-yjn`) |

Skipped on purpose: **100 writers × 1000 events/batch**. We tested this in a separate exploratory pass; it produces a 30-min completion tail at 90 s nominal duration and is operationally useless. Big batches at high concurrency are an anti-pattern in this schema.

**Workload shapes:**

- **A–E (`tests/load_deadlock_freedom.rs`)** — every event posts `debit = <random pick from a 7-account pool>`, `credit = creation_void(qty)`. The credit side is shared across all writers — all contention converges on one row.
- **F (`tests/load_realistic_workload.rs`, `acct-2ey`)** — `bin_move` events across 50 SKUs × 2 locations. Each event picks a random SKU and direction (MAIN→OUT or OUT→MAIN). Both `debit` and `credit` rotate across the 100-account pool. Contention spreads. **Closer to realistic application traffic.**
- **G (`tests/load_outbox_workload.rs`, `acct-tyq`)** — same workload as F, but writers `INSERT` the event batch into `ledger_outbox` (returns immediately, no ledger lock). One sequential drain worker (`tests/common/outbox_worker.rs`) pulls pending rows in batches of 1 000 with `FOR UPDATE SKIP LOCKED` and runs each row through `post_transfers`. Per-row error isolation via savepoints. After the writer phase, the worker gets a 60 s grace drain window before being hard-stopped; the run reports both enqueue throughput and **commit throughput** (the apples-to-apples comparison vs F's events/s). Two latency families captured: writer-side `enqueue_us` (INSERT round-trip) and `queue_us` = `committed_at − enqueued_at` (drain residency).
- **H (`tests/load_reservation_interleave.rs`, `acct-9i6`)** — same fixture as F but the 100-writer pool splits into **70 posters** (running F's `bin_move` workload) and **30 reservers** (each loop iteration calls `reserve_inventory(sku, loc, 1, so_id, fresh_so_line, +1h)`). Both writer types contend for the same `stock_available` row locks: `post_transfers` takes `FOR UPDATE` in ascending-id order across the batch's accounts; `reserve_inventory` takes a single `FOR UPDATE` on the matching row. Stock pre-balanced to 100 M units to avoid exhaustion confounding the throughput numbers. Reservations accumulate without release across the run.
- **I (`tests/load_value_workload.rs`, `acct-jwg`)** — F's qty fixture (50 BENCH SKUs × 2 locations) plus the existing per-currency value accounts (USD `cash/ar/ap/revenue`; EUR `cash/revenue` already in the fixture, EUR `ar/ap` filled in by setup). 100 writers split 50/50 across two operation types: **qty writers** run F's `bin_move` workload, **value writers** post 5–20-event `ar_invoice` batches with random currency (USD or EUR per event) and random debit-normal/credit-normal account pairs from that currency's pool. Value-side has only 4 accounts per currency, so per-currency contention is shape-D-shaped on the value ledger; qty-side contention is shape-F-shaped (spread). The two pools don't lock the same rows, so the workloads run essentially independently.
- **J (`tests/load_outbox_super_batch.rs`, `acct-hbg`)** — same writers, fixture, and outbox infrastructure as G. The ONE difference: the drain worker uses `super_batched_drain_loop` (in `tests/common/outbox_worker.rs`). Each iteration the worker grabs up to 1 000 pending rows with `FOR UPDATE SKIP LOCKED`, concatenates their event arrays into ONE merged JSONB array, and calls `post_transfers(merged_events, override_flag)` exactly once. On any error from the merged call, the worker falls back to G's per-row savepoint drain so error attribution is preserved. Rows are split into two groups by `override_closed_period` (since `post_transfers`'s override is a single function arg); in our workload all rows have `override=false` so the split is a no-op.
- **K (`tests/load_outbox_super_batch.rs` with `T4_DRAINERS=4`, `acct-dtv`)** — same test binary as J, but spawns 4 concurrent `super_batched_drain_loop` tasks instead of 1. All workers share `drain_to_empty` / `hard_stop` signals; SKIP LOCKED ensures non-overlapping row claims. Since the 4 workers' super-batches run in parallel, they compete for `FOR UPDATE` locks on the same `stock_available` rows during `post_transfers` — a contention pattern absent from single-worker J.
- **L (`tests/load_outbox_pseudo_sync.rs`, `acct-yjn`)** — same writers, fixture, and outbox infrastructure as J. **Two changes:** (1) the drainer's `DrainConfig.notify_channel = Some("ledger_outbox_done")`, so each row's commit (or per-row failure in the savepoint fallback) emits `pg_notify(channel, '{"id":N,"status":"ok"|"failed"[,"sqlstate":"…"]}')` inside the drain tx (delivered to subscribers on commit, race-free); (2) writers INSERT then BLOCK on a per-id outcome via a single shared `PgListener` task that fans notifications to per-writer `oneshot` channels in the test process. Per-id rendezvous handles either ordering (writer-waits-first or notify-arrives-first) via a state-machine slot. Caller's wall-clock = `enqueue_us + wait_us`; both reported plus `total_us`. Writers fall back to row-poll if the notify times out (default 30 s); **0 timeouts observed in production runs**.
- **M (`tests/load_outbox_back_pressure.rs`, `acct-yjn`)** — same drainer as J (single super-batched, NO notify channel — pure async outbox). Writer flow: before each INSERT, busy-poll `SELECT COUNT(*) FROM ledger_outbox WHERE status='pending'` and sleep `T4_BP_POLL_MS=2 ms` until depth < `T4_BP_CAP=200`. Then INSERT and continue (no waiting on commit). Captures `bp_wait_us` (pre-INSERT block time) and `enqueue_us` (INSERT round-trip) separately; `total_us = bp + enqueue` is the caller-perceived wall-clock. The poll-then-INSERT sequence is racy under 100 concurrent writers (multiple writers can observe sub-cap simultaneously and rush in), so steady-state queue depth oversoots the cap by ~25 %.

## Run metadata

- **Date:** 2026-04-29 / 2026-04-30
- **Issues:** `acct-1ia` (A–E), `acct-2ey` (F), `acct-tyq` / `acct-epu` (G), `acct-9i6` (H), `acct-jwg` (I), `acct-hbg` (J), `acct-dtv` (K), `acct-yjn` (L + M).
- **Schema state:** A–F on migrations 0001–0016; G–M on 0001–0017 (adds `ledger_outbox` for G; H/I/J/K/L/M add no schema change). Migration 0018 (`acct-17x` periods_no_overlap) was added during this baseline window but doesn't affect any of these workloads.
- **Methodology:** 13 shapes × 3 runs × 5 min (`scripts/run-perf-baseline.sh`); G runs a 60 s grace drain, J/K run 90 s grace drains, L/M run 30 s grace drains (queue stays bounded under both shapes, so the post-writer drain finishes in <1 s).
- **Git refs at run time:** `872f3e50` (A–E), `997a680` (F), `ff5d6b5` (G), `378b63f` (H), `30b323d` (I), `b6d96f2` (J), `b6d96f2+` (K), `52dff5c+` (L + M, post-K, same day 2026-04-30).
- **Total wall clock:** ~200 min combined (A–E ~50 min; F ~27 min; G ~18 min; H ~15 min; I ~15 min; J ~20 min; K ~20 min; L ~15 min; M ~15 min).

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
| **H** 70 + 30 · qty + reserve interleave | (combined 818.2; posters 95.0; reservers 723.6) ¶ | **1 186 ‖** + 723.6 rps reservations | **52.7 ¶** | 3 975 ¶ | 5 626 ¶ | 8 124 ¶ | 12 658 ¶ | 554 | 0 |
| **I** 50 + 50 · qty + multi-cur value | (combined 226.7; qty 136.9; value 89.8) ★ | **1 715 qty** + **1 125 value** = **2 840 combined** | (qty 37.6; value 529) ★ | (qty 1 772; value 1 321) ★ | (qty 2 400; value 2 367) ★ | (qty 3 982; value 3 950) ★ | (qty 5 624; value 7 572) ★ | 892 | 0 |
| **J** 100 × 5–20 · super-batch outbox + 1 drainer | (enqueue 1 446.2; commit ~30 † †) | **364.8 ‡ ‡** | **66.4 § §** | **84.3 § §** | **107.7 § §** | **367.1 § §** | 3 584 § § | 804 | 0 |
| **K** 100 × 5–20 · super-batch outbox + 4 drainers | (enqueue 1 540.3; commit ~62 † † †) | **180.1 ‡ ‡ ‡** | **63.1 § § §** | **73.8 § § §** | **94.4 § § §** | **396.5 § § §** | 3 185 § § § | 674 | 0 |
| **L** 100 × 5–20 · pseudo-sync via LISTEN/NOTIFY | 230.2 ★★ | **2 876.2 ★★ ★★** | **431.0 ★★ ★★ ★★** | **477.7 ★★ ★★ ★★** | **547.4 ★★ ★★ ★★** | 3 221.3 ★★ ★★ ★★ | 3 228.5 ★★ ★★ ★★ | **941** | 0 |
| **M** 100 × 5–20 · async outbox + back-pressure cap=200 | 77.9 | **971.4 ◇** | **1 194.2 ◇ ◇** | **3 209.3 ◇ ◇** | **3 748.4 ◇ ◇** | **4 340.5 ◇ ◇** | **4 427.5 ◇ ◇** | 437 | 0 |

† For G, `bps` = enqueue rate, since writers no longer call `post_transfers` directly. `1 304.5 / s` is the rate at which writer batches land in the outbox; `11.7 / s` is the rate at which drained outbox rows commit (median 4 057 / 360 s).
‡ For G, `events/s` is **commit throughput** (`committed_events / total_elapsed`), apples-to-apples with F's events/s. Enqueue throughput is **16 302 ev/s** — but those events sit in the queue for minutes (see queue_us below), and only 140 ev/s actually land in the ledger.
§ For G, the latency columns measure **writer-perceived `enqueue_us`** (INSERT round-trip). Drain residency is enormous and is reported separately:
  | metric | G `queue_us` median |
  |---|---|
  | p50 | 186 s |
  | p99 | 336 s |
  | max | 338 s |
  Writer-perceived end-to-end (had they waited) ≈ `enqueue_us + queue_us` ≈ 186 s p50.

¶ For H, `bps` and the latency columns are **poster-only** (the 70 posters running F-style `bin_move` calls); reserver stats are reported separately, since their critical section and per-call cost are very different. Headline reserver numbers:
  | metric | H reserver median |
  |---|---|
  | rps | 723.6 |
  | p50 | 6.0 ms |
  | p95 | 35.9 ms |
  | p99 | 191.2 ms |
  | max | 12 169 ms |

‖ For H, the events/s column is the **poster** events/s (1 186 — these are committed `bin_move` events, apples-to-apples with F's events/s). The 723.6 reservations/s (each = 1 INSERT into `inventory_reservations`) are written separately because they aren't transfers; they don't hit the `transfers` table or touch `post_transfers`.

★ For I, the workload runs two parallel pools (qty + value) on disjoint account sets. The events/s column shows them separately AND combined; the latency columns also split. `bps`/`evps`/percentiles all break out per-pool because the pools have very different lock topologies (qty spreads across 100 stock_available rows; value converges on 4 hot rows per currency, similar to shape D's pattern but on the value ledger).

† † For J, `bps` is the enqueue rate (1 446 batches/s of writers writing to the outbox); the drainer's per-iteration commit cadence is **~30 super-batches per 5 min** (each merging ~1 000 rows / ~12 500 events), but that's a misleading "bps" because each "batch" is huge.
‡ ‡ For J, events/s is **commit throughput** — `committed_events / total_elapsed`. **2.6× better than G's naive single-row drain (140 evps)** but **3.5× less than F's direct sync (1 274 evps)**. Confirms super-batching recovers significant per-call amortization (per-event 2.8 ms in J vs 4.25 ms in F vs 0.37 ms in shape B), but the single-drainer architecture still can't match 100 parallel writers' throughput.
§ § For J, the latency columns measure writer-perceived `enqueue_us` (INSERT round-trip). Drain residency:
  | metric | J `queue_us` median |
  |---|---|
  | p50 | 217 s |
  | p99 | 364 s |
  | max | 365 s |
  Even worse than G because writers enqueue faster (18 K vs 16 K evps) while drain rate only triples (140 → 365).

† † † For K, `bps` is enqueue rate; commit cadence is ~62 super-batches per 5 min across 4 drainers (each averaging ~150 rows, smaller than J's ~1 000 because the queue is split four ways).
‡ ‡ ‡ For K, events/s is **commit throughput**. **180 evps — HALF of single-drainer J's 365**. Multi-worker drain regresses, not improves: smaller per-call super-batches (less amortization) AND 4 concurrent calls fighting for `FOR UPDATE` locks on the same `stock_available` accounts (contention reintroduced).
§ § § For K, latency columns are writer-perceived `enqueue_us` (basically unchanged from J because writers don't compete with drainers on locks; only with each other on connection pool / WAL). Drain residency:
  | metric | K `queue_us` median |
  |---|---|
  | p50 | 156 s |
  | p99 | 330 s |
  | max | 330 s |
  Slightly better p50 than J (queue grows less because… actually because the worker drains more rows per second initially, even though per-event throughput is worse). Tail bounded by drain timeout (90 s past writer end).

★★ For L, `bps` is enqueue rate (230.2/s — much lower than J's 1 446 because writers BLOCK on notify per call, so they only enqueue at the rate the drainer commits).

★★ ★★ For L, events/s is **commit throughput** — apples-to-apples with F, J, etc. **2 876 evps is the new throughput peak across the entire matrix.** 2.26× F, 16% above shape B, 7.9× J. The architectural reason: writers' INSERT phase pipelines with the drainer's super-batch commit phase on the same DB; writers never touch account `FOR UPDATE` locks, so the only contention surface is the drainer-with-itself (zero — single-threaded).

★★ ★★ ★★ For L, latency columns measure **writer-perceived `total_us` = enqueue_us + wait_us**. **In pseudo-sync mode, end-to-end (caller-submit → ledger-commit) IS the caller's wall-clock — there's no separate "ledger lag" for the caller.** Component breakdowns:
  | metric | L `enqueue_us` (ms) | L `wait_us` (ms) | L `queue_us` (ms) |
  |---|---|---|---|
  | p50 | 19.6 | 411.6 | 399.9 |
  | p95 | 29.0 | 457.2 | 443.4 |
  | p99 | 36.8 | 524.3 | 506.5 |
  | p99.9 | 2 308 | 894.8 | 919.3 |
  | max | 2 738 | 1 122.8 | 1 562 |
  - `wait_us` = time between INSERT-commit and notify arrival; tightly correlated with `queue_us`.
  - `queue_us` (committed_at − enqueued_at) tracks how long each row sits in the outbox before drain.
  - The p999/max enqueue spike (2.3-2.7 s) is from rare INSERT round-trips contending with the drainer's `UPDATE ledger_outbox SET status='committed'` on the same page — a known pattern in single-table queues, not load-bearing.
  - `final_failed=0`, `timeouts=0` across all runs; `max_outbox_depth=100` exactly (= writer count). Queue depth is **naturally bounded** by the pseudo-sync wait — if writers can't proceed past `wait_for(id)`, no new rows enter until the drainer commits some.

◇ For M, events/s is **commit throughput** — 971 evps, between J (365) and F (1 274). The cap (200) keeps the drainer's super-batch size bounded, so per-batch amortization is worse than J's ~1 000-row batches.

◇ ◇ For M, latency columns measure `total_us = bp_wait + enqueue`. The pre-INSERT busy-poll dominates (`bp p99 = 3 708 ms` of the `total p99 = 3 748 ms`). Component breakdown:
  | metric | M `bp_wait_us` (ms) | M `enqueue_us` (ms) | M `queue_us` (ms) |
  |---|---|---|---|
  | p50 | 1 091 | 59.2 | 3 107 |
  | p95 | 3 151 | 116.1 | 3 849 |
  | p99 | 3 708 | 141.2 | 4 275 |
  | p99.9 | 4 258 | 530 | 4 609 |
  | max | 4 349 | 1 282 | 4 641 |
  - Steady-state queue depth oversoots cap by ~25 % (max=275, p50=252) because 100 writers can race past the depth-poll gate concurrently.
  - **Async semantics:** unlike L, queue residency (`queue_us`) is NOT visible to the caller — they fire-and-forget after the bp_wait + enqueue completes. End-to-end caller wait (had they waited for ledger commit) ≈ bp + enqueue + queue ≈ 4.3 s p50.
  - Context-switch rate: 82 K/s (L: 14.6 K/s, F: 15 K/s) — the busy-poll back-pressure burns CPU. Not recommended.

Key reads:

- **Throughput peak is ~2 500 events/s** (config B), 14 % higher than config A's 2 164. That ~14 % is the entire amortization gain from big batches when there's no contention.
- **Adding writers on the shared-credit workload is purely destructive** (A → C → D) because every batch fights for the same lock. 1 → 100 writers loses ~75 % of throughput.
- **Big batches under shared-credit contention compound the problem instead of helping it** (E vs D): going from small batches to 100-event batches at 100 writers cuts throughput further (559 → 373 evps) and explodes p99 (12 s → 62 s).
- **Spreading contention recovers most of the loss** (F vs D): same writer count, same batch size, just rotating across 100 accounts instead of 1. Throughput **2.3× higher** (1 274 vs 559 evps), **p50 26× lower** (51 vs 1 369 ms), **p99 1.4× lower** (8.3 vs 11.9 s). The FOR UPDATE lock-queue cost dominates D; F approaches what the schema can actually do.
- **Naive outbox is throughput-catastrophic but tail-latency-excellent** (G vs F): same workload, same writer count, same batch shape — but writers `INSERT` instead of calling `post_transfers`. Caller p99 collapses 8.3 s → 133 ms (62× lower) because writers never queue on the ledger lock. **But** total commit throughput crashes from 1 274 → 140 evps (9× lower). The single drainer can't match what 100 contending writers do in parallel, because per-call `post_transfers` overhead doesn't amortize across rows the way packed events inside a single call do (shape B). The path to shape-B-class throughput via outbox requires a **super-batched** drainer that merges multiple rows' events into one `post_transfers` call — filed as `acct-hbg`.
- **Reservation traffic doesn't hurt qty traffic — actually helps modestly** (H vs F): the 100-writer pool splits into 70 posters + 30 reservers. Total qty-side throughput drops only 7 % (1 274 → 1 186 evps), and qty p99 *improves* 32 % (8.3 s → 5.6 s) because there are simply fewer posters competing on `stock_available` row locks. Plus 723.6 reservations/s of useful reservation work. Reservers themselves are very fast (p50 6 ms, p99 191 ms) — `reserve_inventory`'s critical section is one `FOR UPDATE` + one promisable read + one `INSERT`, much shorter than `post_transfers`'s per-batch hold time. Confirms the doc §3.3 two-statement reservation pattern is provably safe under realistic mixed concurrency.
- **Qty + value-side traffic compose additively** (I): 50 qty writers (F-shape) + 50 value writers (D-shape on value ledger, USD+EUR mix) deliver **2 840 evps combined** — qty 1 715 + value 1 125. The pools don't lock the same rows, so they run independently; combined throughput ≈ sum. Per-pool throughput is **higher** than expected from D's "100 writers on shared rows" baseline because each pool only has 50 contenders. Multi-currency contention on 4 hot accounts per currency sustains 1 125 evps total (split USD + EUR), versus shape D's 559 evps on a single shared row.
- **Super-batched outbox closes ~half the gap to direct sync** (J vs G vs F): shape J merges events from up to 1 000 outbox rows into one `post_transfers` call. Result: **commit throughput climbs from 140 → 365 evps (2.6×)** vs G's per-row drain. Caller p99 stays excellent (108 ms vs G's 133 ms). But J is still 3.5× SHORT of F's 1 274 evps because the single-drainer architecture caps at one-tx-at-a-time, while F has 100 parallel writers each adding throughput. The per-event time in J (2.8 ms) sits between F's 4.25 ms (parallel-but-contended) and shape B's 0.37 ms (single-writer-no-contention) — super-batching recovers some amortization, but concurrent writer INSERTs still pollute buffer-cache and pool resources, blocking full B-class amortization. **Conclusion:** outbox + super-batch is the right architecture if caller p99 is the binding constraint, but it's not a free throughput win — D3's "sync `post_transfers`" decision still stands as the throughput-optimal default.
- **Multi-worker drain is a regression, not an improvement** (K vs J): adding workers to J's single-drainer setup (4 instead of 1) HALVES commit throughput — 365 evps → 180 evps. Two reinforcing causes: (1) each drainer's super-batch is ¼ the size (~150 rows vs ~1 000), so per-call amortization shrinks; (2) the 4 super-batches run in parallel, so they fight for `FOR UPDATE` locks on the same 100 `stock_available` accounts — exactly the F-style contention shape that single-worker J was avoiding. **Single-worker super-batch is the architectural sweet spot.** Multi-worker would only help if (a) the workload spread over many more accounts than 100, OR (b) per-`post_transfers`-call cost was so high that single-call serialization was the actual bottleneck — neither is true here. The "more is better" intuition is wrong for this drain pattern; document this explicitly so it doesn't get re-tried.
- **Zero deadlocks across every shape** — the ascending-id `FOR UPDATE` lock ordering in `post_transfers` is correct under every shape we threw at it, including H's mixed `post_transfers` + `reserve_inventory` traffic on the same `stock_available` rows.

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

### H — 70 posters + 30 reservers, cross-account spread (50 SKUs × 2 locations)

Same fixture as F but the 100-writer pool splits 70/30 across two operation types. **Posters** run the same `bin_move` workload as F. **Reservers** loop on `reserve_inventory(sku, loc, 1, so_id, fresh_so_line, +1h)` for random (sku, loc) targets. Stock pre-balanced to **100 M units** per (sku, loc) pair so reservation-budget exhaustion can't confound the result; reservations accumulate without release across the 5-min run.

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Combined batches/ops | 233 266 | 246 039 | 242 517 | 248 246 |
| Combined bps | 775.8 | 818.2 | 806.6 | 825.8 |
| Poster bps | 94.5 | **95.0** | 95.0 | 95.5 |
| Poster evps | 1 183 | **1 186** | 1 188 | 1 195 |
| Poster p50 (ms) | 52.5 | **52.7** | 52.7 | 53.0 |
| Poster p95 (ms) | 3 968 | 3 975 | 4 009 | 4 085 |
| Poster p99 (ms) | 5 401 | **5 626** | 5 710 | 6 103 |
| Poster p99.9 (ms) | 7 928 | 8 124 | 8 441 | 9 272 |
| Poster max (ms) | 10 368 | 12 658 | 13 040 | 16 093 |
| Reserver rps | 680.3 | **723.6** | 711.6 | 730.8 |
| Reserver p50 (ms) | 6.01 | **6.02** | 6.02 | 6.03 |
| Reserver p95 (ms) | 35.3 | 35.9 | 35.9 | 36.6 |
| Reserver p99 (ms) | 172 | 191 | 206 | 253 |
| Reserver p99.9 (ms) | 4 630 | 4 730 | 4 784 | 4 990 |
| Reserver max (ms) | 11 270 | 12 170 | 12 267 | 13 362 |
| Reserver `null` returns | 0 | 0 | 0 | 0 |
| Active reservations at end | 204 553 | 217 609 | 213 952 | 219 695 |
| io_writes | 132 563 | 134 504 | 134 012 | 134 970 |
| io_fsyncs | 132 450 | 134 397 | 133 898 | 134 848 |
| WAL MB | 549.3 | 554.5 | 553.5 | 556.7 |

vmstat: `us≈43%  sy≈5.4%  id≈30%  wa≈17.6%  cs≈37 K/s`. CPU profile is similar to F (us 43 vs 40 %), with a 2.5× higher context-switch rate (37 K/s vs F's 15 K/s) because reservers do many short ops/sec — reserve_inventory's per-call latency averages ~6 ms versus post_transfers's ~50 ms per batch.

`reserver null` count is **zero across all 3 runs** — pre-balanced 100 M units never approached exhaustion, so every reservation request was satisfied.

### H vs F — reservation interleaving doesn't hurt qty, modestly helps

Same total writer count (100), same fixture, same 5-min duration. Only difference: 30 of the 100 writers run `reserve_inventory()` instead of `post_transfers`.

| Metric | F (100 posters) | H (70 posters + 30 reservers) | Δ |
|---|---|---|---|
| Qty-side committed events/s | 1 274.2 | 1 186.1 | **−7 %** |
| Qty-side p50 (ms) | 51.4 | 52.7 | +3 % (essentially same) |
| Qty-side p95 (ms) | 5 410.5 | 3 975 | **−27 %** |
| Qty-side p99 (ms) | 8 250.7 | 5 626 | **−32 %** |
| Qty-side max (ms) | 16 768 | 12 658 | **−25 %** |
| Reservations/s | n/a | 723.6 | new |
| Reservation p50 (ms) | n/a | 6.0 | new |
| Reservation p99 (ms) | n/a | 191 | new |
| WAL/5 min (MB) | 431 | 554 | +29 % |
| CPU user % | 40.2 | 43.6 | +9 % |
| Context switches/s | 14.9 K | 37 K | **+148 %** |
| Deadlocks | 0 | 0 | safe |

**What this says.** Qty-side throughput drops only 7 % when 30 % of writers switch to reservation work — and qty-side **tail latencies all improve** (−25 to −32 % across p95/p99/max) because there are fewer posters competing for the same `stock_available` row locks. The reservation work is essentially free in tail-latency terms: reservers acquire their `FOR UPDATE` lock briefly (~6 ms median), don't hold it across a multi-event batch like `post_transfers` does, and slot in between poster transactions cleanly.

The reserver tail (p99.9 = 4.7 s, max = 12 s) is real and tracks poster lock-hold time — when a reserver picks an `(sku, loc)` whose row is currently locked by a slow poster batch, the reserver waits the rest of the poster's lifetime. p99.9 ≈ poster p95 confirms that pattern. p50 stays at 6 ms because the SKU pool spread (50 × 2 = 100 accounts) means most reserver picks find a clean row.

**The two-statement reservation pattern is safe under load.** Zero deadlocks, zero `null` returns (i.e., zero "insufficient promisable" failures — the function logic always saw consistent promisable values), zero unexpected errors. Doc §3.3's `reserve_inventory()` (PL/pgSQL function variant, not the unsafe single-statement CTE — see migration 0014's header) holds under realistic concurrent traffic.

For Phase 1 regression detection, **H is a useful third reference alongside F and D**: re-run if the reservation flow changes (e.g., `acct-alq` doc fix) or when reservation lifetime semantics evolve (Q5 in the consolidated doc).

### I — 50 qty + 50 multi-currency value, concurrent (cross-account spread + per-currency hot accounts)

100-writer pool splits 50/50: **qty writers** run F's `bin_move` on 50 BENCH SKUs × 2 locations (100 distinct accounts), **value writers** post 5–20-event `ar_invoice` batches with random currency (50/50 USD or EUR per event) and random debit-normal/credit-normal pairs from that currency's 4-account pool (cash, ar, revenue, ap). The value side has only 4 accounts per currency — same hot-row contention shape as D, but on the value ledger.

Setup adds EUR `ar`/`ap` accounts to the fixture (the small fixture seeds USD `ar`/`ap` but only EUR `cash`/`revenue`).

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Combined batches/run | 67 267 | 68 143 | 68 950 | 71 439 |
| Combined events/run | 840 987 | 853 493 | 863 019 | 894 578 |
| Combined bps | 223.9 | 226.7 | 229.5 | 237.7 |
| Combined evps | 2 799 | **2 840** | 2 872 | 2 977 |
| Qty bps | 136.1 | **136.9** | 138.6 | 142.7 |
| Qty evps | 1 703 | **1 715** | 1 736 | 1 789 |
| Qty p50 (ms) | 37.0 | **37.6** | 37.7 | 38.5 |
| Qty p95 (ms) | 1 740 | 1 772 | 1 803 | 1 896 |
| Qty p99 (ms) | 2 131 | **2 400** | 2 384 | 2 622 |
| Qty p99.9 (ms) | 2 918 | 3 982 | 3 668 | 4 103 |
| Qty max (ms) | 5 186 | 5 624 | 6 434 | 8 491 |
| Value bps | 87.7 | **89.8** | 90.9 | 95.0 |
| Value evps | 1 096 | **1 125** | 1 136 | 1 188 |
| Value p50 (ms) | 439.4 | **529.2** | 505.4 | 547.7 |
| Value p95 (ms) | 1 141 | 1 321 | 1 411 | 1 772 |
| Value p99 (ms) | 2 106 | 2 367 | 2 452 | 2 883 |
| Value p99.9 (ms) | 3 427 | 3 950 | 4 048 | 4 768 |
| Value max (ms) | 6 478 | 7 572 | 7 404 | 8 163 |
| io_writes | 66 488 | 67 373 | 68 187 | 70 699 |
| io_fsyncs | 66 266 | 67 147 | 67 926 | 70 366 |
| WAL MB | 879.5 | **892.1** | 897.2 | 920.0 |

vmstat: `us≈42%  sy≈4%  id≈34%  wa≈17%  cs≈27 K/s`. CPU usage and iowait are similar to F. Lower context-switch rate than H (27 K vs 37 K) because there are no fast `reserve_inventory` calls in the mix; both writer types do batched `post_transfers` calls of comparable duration.

### I vs F — multi-pool composition is roughly additive

I splits the 100-writer pool 50/50 across two pools that don't share lock targets. The interesting comparison is per-pool against a hypothetical "F at 50 writers" (which we don't have, but we can reason about per-writer rates).

| Metric | F (100 qty writers) | I qty (50 writers) | Δ |
|---|---|---|---|
| Per-writer bps | 1.02 | 2.74 | **+168 %** |
| Per-writer evps | 12.7 | 34.3 | **+170 %** |
| p50 (ms) | 51.4 | 37.6 | **−27 %** |
| p99 (ms) | 8 251 | 2 400 | **−71 %** |

| Metric | D (100 writers, 1 shared row) | I value (50 writers, 4 rows/currency × 2 currencies) | Δ |
|---|---|---|---|
| Combined evps | 559 | 1 125 | **+101 %** (8 hot rows × 50 writers vs 1 row × 100) |
| p50 (ms) | 1 369 | 529 | −61 % |
| p99 (ms) | 11 906 | 2 367 | −80 % |

**What this says.**

1. **The two ledger pools compose roughly additively.** Combined evps (2 840) is close to the sum of the two pools' independent evps (1 715 + 1 125 = 2 840). They don't lock the same rows; they share only the pg_stat_statements / WAL / connection-pool resources. WAL volume scales accordingly: I writes 892 MB / 5 min, almost exactly F (431) + a hypothetical "D at 50 writers" (~250 estimated) — the per-event WAL cost is invariant, and we're committing more events.

2. **50-writer qty subset performs much better than 100-writer F.** Per-writer rate +168 %, p99 −71 %. With half the writers competing on the same 100-account spread, contention per writer is roughly halved, and tail latencies collapse correspondingly. Useful as an upper bound for "how good can the qty side get if we cap concurrency."

3. **Multi-currency value-side hot-row contention is real but tractable.** 50 writers on 4 accounts per currency (with 50/50 currency split, effectively ~25 writers per 4-account pool) sustain 1 125 combined evps — much better than D's 559 evps on 1 shared row at 100 writers. The reason: 8 hot rows total (4 USD + 4 EUR) instead of 1, and only ~25 writers per row instead of 100. Per-row throughput is similar to D's per-row at lower concurrency, but multiple rows multiply it.

4. **Value p50 (529 ms) is much higher than qty p50 (37 ms).** Value-side hot-row contention dominates value-side latency: each value batch of 5–20 events hits a small pool of ~4 USD + 4 EUR debit-normal/credit-normal accounts, so most events queue behind some other writer's batch on at least one of those rows. Qty-side spreads across 100 rows so most events find a clean lock immediately.

5. **Zero deadlocks across all 3 runs.** The lock-order proof in `post_transfers` works correctly across both ledger kinds simultaneously.

For Phase 1 regression detection, **I is a useful reference for value-side perf and for "how does multi-currency traffic interact with qty traffic"**. Re-run when:
- New cost methods land (`acct-8gg`) — value-side traffic is where WAC/FIFO read-then-write under FOR UPDATE shows up.
- Per-counterparty AR/AP subaccounts land (Phase 1) — the contention shape on the value side will change dramatically as cash/ar/ap/revenue stop being shared per currency.

### J — 100 writers → outbox → 1 super-batched drainer (cross-account spread)

Same writer pool, fixture, and outbox infrastructure as G. The drainer uses `super_batched_drain_loop` (in `tests/common/outbox_worker.rs`): each iteration grabs up to T4_OUTBOX_BATCH_SIZE pending rows with `FOR UPDATE SKIP LOCKED`, concatenates their events into one merged JSONB array, and calls `post_transfers(merged, override)` once. On any error from the merged call, falls back to per-row savepoint drain.

Production used `T4_DRAIN_TIMEOUT_S=90` (vs G's 60 s) to give the worker more grace time. The queue still doesn't drain to empty — drain timeout fires every run.

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches enqueued / 5 min | 423 674 | 433 884 | 436 014 | 450 483 |
| Events enqueued / 5 min | 5 297 638 | 5 425 308 | 5 450 006 | 5 627 072 |
| Enqueue batches/s | 1 412.1 | 1 446.2 | 1 453.3 | 1 501.5 |
| Enqueue events/s | 17 657 | 18 083 | 18 165 | 18 756 |
| **Commit events / run** | 141 793 | **142 341** | 142 212 | 142 502 |
| **Commit events/s** | 363.4 | **364.8** | 364.5 | 365.2 |
| Final committed rows / run | 11 363 | 11 364 | 11 372 | 11 389 |
| `enqueue_us` p50 (ms) | 64.4 | **66.4** | 65.9 | 66.9 |
| `enqueue_us` p95 (ms) | 77.2 | 84.3 | 83.9 | 90.1 |
| `enqueue_us` p99 (ms) | 99.6 | **107.7** | 109.8 | 122.2 |
| `enqueue_us` p99.9 (ms) | 359.4 | 367.1 | 374.4 | 396.7 |
| `enqueue_us` max (ms) | 3 219 | 3 584 | 3 537 | 3 807 |
| `queue_us` p50 (s) | 211.0 | **216.7** | 216.8 | 222.8 |
| `queue_us` p99 (s) | 361.8 | 364.1 | 363.7 | 365.1 |
| `queue_us` max (s) | 361.9 | 364.2 | 363.7 | 365.2 |
| Max outbox depth | 416 310 | **426 521** | 428 308 | 442 094 |
| Drain-phase wall clock (s) | 90.15 | 90.18 | 90.17 | 90.19 |
| io_writes | 83 393 | 83 644 | 83 767 | 84 263 |
| io_fsyncs | 70 364 | 71 540 | 71 588 | 72 859 |
| WAL MB | 792.7 | **804.2** | 808.0 | 827.2 |

vmstat: `us≈40.7%  sy≈6.3%  id≈35.3%  wa≈13.9%  cs≈29.3 K/s`. CPU profile is between F (us 40 %, cs 15 K) and G (us 47 %, cs 42 K) — fewer small statements per second than G (which did 4 stmts per row × 1 000 rows = 4 K stmts per outer commit), more per-row work than F.

Average super-batch composition: 11 364 rows / ~11 super-batch iterations (over the 390 s run) = ~1 000 rows per super-batch ≈ ~12 500 events per `post_transfers` call. Per-event time = (35 s / 12 500 events) ≈ 2.8 ms — better than F's 4.25 ms (parallel-but-contended), worse than B's 0.37 ms (single-writer, idle DB).

`super_batch_attempts/successes/fallbacks` counters live in worker memory and are lost when the drain timeout fires (the run reports 0 for all three because the timeout-fallback path synthesizes stats from DB queries). `final_failed=0` confirms no row-level failures, which strongly implies all super-batches succeeded (no per-row fallback was invoked) — bin_move events are always valid.

### J vs G vs F vs B — the outbox-throughput-recovery picture

| Metric | F (sync) | B (1 writer × 1 000) | G (outbox, per-row drain) | J (outbox, super-batch drain) |
|---|---|---|---|---|
| Committed events/s | **1 274** | 2 486 | 140 | **365** |
| Caller p50 (ms) | 51.4 | 373.9 | 72.1 | **66.4** |
| Caller p99 (ms) | 8 250 | 610.9 | 133.1 | **107.7** |
| Per-event time (ms) | 4.25 | 0.37 | (not directly measurable) | 2.8 |
| End-to-end caller wait (had they waited) | 51 ms p50 | 374 ms p50 | 186 s p50 | 217 s p50 |
| WAL/5 min (MB) | 431 | 717 | 587 | 804 |
| Workload concurrency | 100 parallel | 1 sequential | 100 enqueue + 1 drain | 100 enqueue + 1 drain |

**What this says.**

1. **Super-batching closes ~half the gap from G to F.** G measured 140 evps committed (per-row `post_transfers` overhead dominates). J measures 365 evps committed (one big `post_transfers` per ~1 000 rows). 140 → 365 is 2.6× — meaningful improvement. But still 3.5× short of F's 1 274 evps.

2. **The remaining gap to F is parallelism, not amortization.** Per-event J takes 2.8 ms vs F's 4.25 ms — J's per-event work is actually *cheaper* than F's because there's no parallel-writer lock contention against the worker. But F gets **100×** parallel work while J gets one drainer, so F wins on aggregate throughput by ~3.5×.

3. **The remaining gap to B is buffer-cache and connection-pool pollution.** J's per-event 2.8 ms vs B's 0.37 ms is a 7.5× per-event slowdown. B runs against a quiet DB; J runs alongside 100 INSERT-ing writers competing for buffer cache, WAL writer, and connection slots. None of those are post_transfers's fault.

4. **Caller p99 is 108 ms — better than even G** (133 ms). Writers' `INSERT` is fast and rarely queues. The latency the caller sees is excellent across both outbox variants.

5. **End-to-end caller wait (if they had to know "is the ledger updated?") would be 217 SECONDS p50.** The queue grows unbounded throughout the run. Same caveat as G: outbox makes sense ONLY if the application tolerates eventual semantics on the ledger.

**For Phase 1 regression detection: J is the relevant outbox reference.** If a future change reopens D3 (the "sync post_transfers" decision), the comparison is J's commit throughput (~365 evps) vs F's (~1 274 evps). Re-run J when:
- A new cost method changes per-call `post_transfers` cost (the per-event time directly affects super-batch throughput).
- The drainer architecture changes (multi-worker, per-(sku) sharding, etc. — the natural next exploration if super-batched-single-worker isn't enough).
- D3 is formally reconsidered.

### K — 100 writers → outbox → 4 super-batched drainers (cross-account spread)

Same test binary as J, run with `T4_DRAINERS=4`. Four concurrent drainer tasks each run `super_batched_drain_loop` independently; SKIP LOCKED ensures non-overlapping row claims. The hypothesis we wanted to test: does parallelizing the drainer recover more of F's throughput? Answer: **no — it regresses.**

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches enqueued / 5 min | 461 698 | 462 116 | 462 674 | 464 208 |
| Events enqueued / 5 min | 5 774 409 | 5 778 296 | 5 785 853 | 5 804 853 |
| Enqueue batches/s | 1 538.9 | 1 540.3 | 1 542.1 | 1 547.3 |
| Enqueue events/s | 19 246 | 19 259 | 19 285 | 19 348 |
| **Commit events / run** | 67 023 | **70 294** | 72 215 | 79 329 |
| **Commit events/s** | 171.8 | **180.1** | 185.1 | 203.3 |
| Final committed rows / run | 5 371 | 5 664 | 5 794 | 6 347 |
| `enqueue_us` p50 (ms) | 63.0 | **63.1** | 63.1 | 63.2 |
| `enqueue_us` p95 (ms) | 73.0 | 73.8 | 73.8 | 74.5 |
| `enqueue_us` p99 (ms) | 91.7 | **94.4** | 93.6 | 94.6 |
| `enqueue_us` p99.9 (ms) | 388.5 | 396.5 | 399.8 | 414.3 |
| `enqueue_us` max (ms) | 3 064 | 3 185 | 3 242 | 3 476 |
| `queue_us` p50 (s) | 144.6 | **155.6** | 154.7 | 164.0 |
| `queue_us` p99 (s) | 318.2 | 329.9 | 342.6 | 379.8 |
| `queue_us` max (s) | 318.3 | 329.9 | 342.7 | 379.8 |
| Max outbox depth | 457 034 | **457 745** | 458 213 | 459 861 |
| Drain-phase wall clock (s) | 90.16 | 90.18 | 90.18 | 90.19 |
| io_writes | 75 321 | 75 436 | 75 571 | 75 955 |
| io_fsyncs | 74 148 | 74 344 | 74 280 | 74 347 |
| WAL MB | 673.2 | **674.0** | 676.0 | 680.9 |

vmstat: `us≈38.9%  sy≈6.1%  id≈34.5%  wa≈16.9%  cs≈29.1 K/s`. CPU profile is essentially identical to J's (same writer pool; the worker change doesn't materially shift CPU load). Drain-side concurrency is real (4 super-batches running) but bounded by lock waits.

Average super-batch composition: 5 664 rows / ~62 super-batch iterations across 4 workers = ~91 rows per super-batch ≈ ~1 100 events per `post_transfers` call. Per-event time = (90 s drain ÷ ~13 calls/worker × 4 workers ÷ 1 100 events) — roughly 5 ms/event, **higher than J's 2.8 ms**. The contention reintroduced by 4 parallel `post_transfers` calls more than offsets the smaller per-call work.

### K vs J — multi-worker drain regresses, not improves

Same workload, fixture, super-batch logic. Only difference: 4 concurrent drainers vs 1.

| Metric | J (1 drainer) | K (4 drainers) | Δ |
|---|---|---|---|
| Commit events/s | **365** | **180** | **−51 %** |
| Caller p50 (ms) | 66.4 | 63.1 | −5 % (essentially same) |
| Caller p99 (ms) | 107.7 | 94.4 | −12 % (slightly better) |
| Caller max (ms) | 3 584 | 3 185 | −11 % |
| Queue p50 (s) | 217 | 156 | −28 % (slightly better) |
| Per-event time (ms) | 2.8 | ~5.0 | +79 % (worse) |
| WAL/5 min (MB) | 804 | 674 | −16 % (committed less) |
| Avg super-batch size (rows) | ~1 000 | ~91 | −91 % |
| io_reads (per run, median) | 145 257 | 50 715 | −65 % |
| Deadlocks | 0 | 0 | safe |

**What this says.**

1. **Throughput halves, not doubles.** Naïve intuition: 4 workers should beat 1 by some factor. Reality: 4 workers commit half as many events per second as 1.

2. **Two reinforcing causes:**
   - **Smaller super-batches.** With 4 workers each running `FOR UPDATE SKIP LOCKED LIMIT 1000`, each call grabs ~¼ of available pending rows (or hits the LIMIT first). Average super-batch shrinks from ~1 000 rows (J) to ~91 rows (K) — a 91% reduction, way more than the 75% the worker count alone would predict, because the queue depletes faster than it can refill at any one drainer.
   - **Account-FOR-UPDATE contention.** 4 super-batches running in parallel each touch many of the 100 `stock_available` accounts. They fight for the same `FOR UPDATE` locks during `post_transfers` — exactly the F-style contention shape that single-worker J was avoiding. Per-event time climbs from 2.8 ms (J) to ~5 ms (K).

3. **Caller-perceived metrics are basically unchanged.** Writers don't compete with drainers for the `INSERT INTO ledger_outbox` path; they compete only with each other on the writer-pool / connection-pool / WAL writer. Adding drainers doesn't affect that. p50 / p99 / max enqueue latency move only ±5–12 % vs J.

4. **Single-worker super-batch is the architectural sweet spot for this workload.** Multi-worker would help if either (a) the workload spread over many more accounts (so 4 drainers were unlikely to overlap on locks), or (b) per-`post_transfers`-call cost was so high that single-call serialization was the bottleneck. Neither is true here. **For Phase 0's 100-account workload, `T4_DRAINERS=1` is the right tuning.**

5. **Closing acct-dtv as "documented & not pursued further":** the issue's hypothesis ("does multi-core commit help?") was sensible but the answer is no for this workload shape. The optimal tuning knob is N=1; document this explicitly so a future engineer doesn't re-derive it.

### L — 100 writers → outbox → 1 super-batched drainer with LISTEN/NOTIFY pseudo-sync

Same writers, fixture, and outbox infrastructure as J. **Two changes:**
1. The drainer's `DrainConfig.notify_channel = Some("ledger_outbox_done")`, so each row's outcome (`ok` on commit, `failed` with `sqlstate` on per-row fallback) emits `pg_notify(channel, json_payload)` inside the drain tx. Postgres delivers the notification only when the tx commits, making the channel a faithful "row outcome" signal — no race window between commit and signal.
2. Writers run a different control flow:
   ```rust
   let id = INSERT INTO ledger_outbox (events) VALUES (…) RETURNING id;
   let outcome = dispatcher.wait_for(id, 30s).await;
   ```
   A single shared `PgListener` task subscribes to the channel and dispatches per-id notifications via `oneshot::Sender` to the waiting writer task. The dispatcher's per-id slot handles either ordering (writer-arrives-first installs a sender; notify-arrives-first buffers the outcome).

The headline result is the new throughput peak.

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches enqueued / 5 min | 67 214 | **69 138** | 68 541 | 69 272 |
| Events committed / 5 min | 840 889 | **864 269** | 856 734 | 865 044 |
| Enqueue batches/s | 223.9 | **230.2** | 228.2 | 230.6 |
| **Commit events/s** | 2 800.7 | **2 876.2** | 2 853.0 | 2 878.1 |
| Final committed rows / run | 67 214 | 69 138 | 68 541 | 69 272 |
| Final failed rows / run | 0 | **0** | 0 | 0 |
| Timeouts (notify > 30 s) | 0 | **0** | 0 | 0 |
| Max outbox depth | 100 | **100** | 100 | 100 |
| `enqueue_us` p50 (ms) | 19.5 | **19.6** | 19.8 | 20.1 |
| `enqueue_us` p95 (ms) | 29.0 | 29.0 | 29.6 | 30.8 |
| `enqueue_us` p99 (ms) | 36.6 | **36.8** | 37.6 | 39.3 |
| `enqueue_us` p99.9 (ms) | 1 770 | 2 308 | 2 163 | 2 411 |
| `enqueue_us` max (ms) | 2 727 | 2 738 | 2 756 | 2 802 |
| `wait_us` p50 (ms) | 408.9 | **411.6** | 412.3 | 416.4 |
| `wait_us` p99 (ms) | 491.8 | **524.3** | 547.5 | 626.4 |
| `wait_us` max (ms) | 1 082 | 1 122 | 1 117 | 1 145 |
| `total_us` p50 (ms) | 429.0 | **431.0** | 432.1 | 436.5 |
| `total_us` p95 (ms) | 477.1 | **477.7** | 492.4 | 522.3 |
| `total_us` p99 (ms) | 511.1 | **547.4** | 569.4 | 649.6 |
| `total_us` p99.9 (ms) | 2 839 | 3 221 | 3 108 | 3 265 |
| `total_us` max (ms) | 3 185 | 3 229 | 3 228 | 3 272 |
| `queue_us` p50 (ms) | 397.6 | **399.9** | 400.9 | 405.3 |
| `queue_us` p99 (ms) | 473.9 | 506.5 | 531.9 | 615.3 |
| `queue_us` max (ms) | 1 217 | 1 562 | 1 487 | 1 681 |
| Drain-phase wall clock (s) | 0.001 | 0.001 | 0.001 | 0.001 |
| io_writes | 5 682 | 5 736 | 5 768 | 5 886 |
| io_fsyncs | 5 380 | 5 535 | 5 485 | 5 539 |
| WAL MB | 925.4 | **940.9** | 936.8 | 943.8 |

vmstat: `us≈22.5%  sy≈2.0%  id≈53.2%  wa≈19.4%  cs≈14.6 K/s`. **CPU profile is shape A's, not shape F's.** us=22% (vs F's 40%, J's 41%) because writers spend most of their time blocked on notify, not running CPU. iowait 19% — storage participates (similar to A and J). cs 14.6 K/s is the lowest of any 100-writer shape; the pseudo-sync wait halves coroutine context-switch frequency. WAL 941 MB / 5 min — the highest of any shape because more events were committed.

Per-event time = (300 s / 864 269 events) ≈ **0.347 ms/event**. **This is BELOW shape B's 0.37 ms** — which was the prior theoretical floor (single writer × 1 000 events × no contention). Why does L beat B? Because B's drainer paid the round-trip-to-writer-and-back between batches, while L's drainer is *continuously* fed by 100 blocked writers, so its idle time approaches zero. The drainer runs at ~2.4 super-batches/sec, each averaging ~100 rows ≈ 1 200 events, with `post_transfers` per-call cost ≈ 417 ms. That matches the 400 ms `queue_us` median exactly.

Drainer super-batch composition: 69 138 rows / ~720 super-batches over 300 s ≈ 96 rows per super-batch ≈ ~1 200 events per `post_transfers` call. Per-event 0.35 ms.

`super_batch_attempts/successes/fallbacks` — the run reports 0 for all three because the timeout-fallback path (which fires when the drainer takes >30 s past writer-stop) synthesizes stats from DB queries and the actual drain finished in <1 s. `final_failed=0` confirms no row-level failures.

### L vs J vs F vs B — pseudo-sync is the new throughput peak

| Metric | F (sync) | B (1 writer × 1 000) | J (outbox, super-batch) | L (pseudo-sync via NOTIFY) |
|---|---|---|---|---|
| **Committed events/s** | 1 274 | 2 486 | 365 | **2 876** |
| Caller p50 (ms) | 51.4 | 373.9 | 66.4 | 431.0 |
| Caller p99 (ms) | 8 250 | 610.9 | 107.7 (caller-perceived only) | **547.4** |
| End-to-end caller wait p50 | 51 ms | 374 ms | 217 SECONDS | **431 ms** |
| End-to-end caller wait p99 | 8.25 s | 611 ms | 364 SECONDS | **547 ms** |
| Per-event time (ms) | 4.25 | 0.37 | 2.8 | **0.347** |
| Workload concurrency | 100 parallel | 1 sequential | 100 enqueue + 1 drain | 100 enqueue (blocked) + 1 drain |
| WAL/5 min (MB) | 431 | 717 | 804 | **941** |
| vmstat us% | 40 | 50 | 41 | **22.5** |
| vmstat cs/s | 15 K | 8 K | 29 K | **14.6 K** |
| Sync error semantics | yes | yes | no (caller fires-and-forgets) | **yes** (notify carries SQLSTATE on failure) |

**What this says.**

1. **L is the new architectural reference for outbox-style workloads.** It beats every other shape on commit throughput AND has end-to-end caller p99 only ~6× worse than F's p99 instead of 1 000× worse. And it preserves sync-style error semantics (the notify payload carries `sqlstate` on failure, so the caller can branch).

2. **Pseudo-sync pipelines producer and consumer stages.** F's bottleneck is account `FOR UPDATE` contention across 100 parallel writers. L sidesteps this entirely: writers only INSERT into `ledger_outbox` (no account locks), drainer holds account locks alone (no peer contention). Producer (writer INSERT, ~20 ms) and consumer (drainer super-batch commit, ~400 ms) run concurrently on the same DB process.

3. **L beats shape B by 16 %** despite running 100 concurrent writers vs B's 1. The mechanism: writers continuously feed the drainer via the bounded queue, so the drainer never has idle time; B's writer-side does serial round-trips between batches (idle gaps in the DB).

4. **Caller p999/max enqueue tail is still present** (2.3–2.7 s) — this is the `INSERT INTO ledger_outbox` round-trip occasionally contending with the drainer's `UPDATE ledger_outbox SET status='committed'` on the same physical page. F's tail is much worse (8 s) because it's account-lock contention. L's tail is bounded by single-table page contention. Could be improved (HOT updates, partition the outbox, advisory-lock the depth check) but not load-bearing.

5. **WAL throughput is the highest of any shape** (941 MB / 5 min). The system is doing more useful work per second; that translates linearly to WAL volume. iowait 19 % matches.

6. **The remaining gap to "ideal":** the drainer is a single-tx-at-a-time bottleneck. Sharding the outbox by hash(account) and running N drainers (each owning a partition) would in principle scale further — but only if the workload spreads across enough partitions. K showed multi-drainer on shared accounts regresses; partitioned-drainer is a different design (filed conceptually as a Phase 1 idea, not P3).

**For Phase 1 regression detection:** L is the upper bound on what the architecture can sustain on this hardware. If a Phase 1 change causes L to drop below ~2 500 evps, something fundamental degraded.

### M — 100 writers → outbox + back-pressure cap → 1 super-batched drainer

Same drainer as J (super-batched, single, no notify). Writers run a different control flow:
```rust
loop {
  let depth = SELECT count(*) FROM ledger_outbox WHERE status='pending';
  if depth < cap { break; }
  sleep(2 ms);
}
INSERT INTO ledger_outbox (events) VALUES (…);
// fire-and-forget — caller does NOT wait for commit
```

Cap=200, poll cadence=2 ms (env-tunable). The pre-INSERT depth poll is racy under 100 concurrent writers — multiple writers can observe `depth < cap` simultaneously and rush in, so the steady-state queue depth oversoots the cap by ~25 %.

| Metric | min | median | mean | max |
|---|---|---|---|---|
| Batches enqueued / 5 min | 21 993 | **23 381** | 23 523 | 25 195 |
| Events committed / 5 min | 273 662 | **292 285** | 293 211 | 313 685 |
| Enqueue batches/s | 73.3 | **77.9** | 78.4 | 84.0 |
| **Commit events/s** | 908.0 | **971.4** | 974.2 | 1 043.0 |
| Final committed rows / run | 21 993 | 23 381 | 23 523 | 25 195 |
| Final failed rows / run | 0 | **0** | 0 | 0 |
| Max outbox depth | 275 | 275 | 275 | 276 |
| Queue depth p50 (samples) | 251 | **252** | 252 | 253 |
| `bp_wait_us` p50 (ms) | 964.0 | **1 091** | 1 063 | 1 134 |
| `bp_wait_us` p95 (ms) | 3 065 | 3 151 | 3 185 | 3 338 |
| `bp_wait_us` p99 (ms) | 3 601 | **3 708** | 3 757 | 3 964 |
| `bp_wait_us` p99.9 (ms) | 4 028 | 4 258 | 4 326 | 4 692 |
| `bp_wait_us` max (ms) | 4 070 | 4 349 | 4 413 | 4 820 |
| `enqueue_us` p50 (ms) | 50.3 | **59.2** | 59.4 | 68.7 |
| `enqueue_us` p99 (ms) | 114.4 | **141.2** | 146.9 | 185.2 |
| `total_us` p50 (ms) | 1 035 | **1 194** | 1 148 | 1 214 |
| `total_us` p99 (ms) | 3 657 | **3 748** | 3 814 | 4 037 |
| `total_us` max (ms) | 4 119 | 4 428 | 4 488 | 4 916 |
| `queue_us` p50 (s) | 2.94 | **3.11** | 3.13 | 3.34 |
| `queue_us` p99 (s) | 4.03 | 4.28 | 4.34 | 4.71 |
| `queue_us` max (s) | 4.12 | 4.64 | 4.54 | 4.86 |
| Drain-phase wall clock (s) | 0.72 | 0.85 | 0.97 | 1.35 |
| io_writes | 6 100 | 6 259 | 6 227 | 6 322 |
| io_fsyncs | 6 072 | 6 233 | 6 197 | 6 287 |
| WAL MB | 410.2 | **436.5** | 445.1 | 488.4 |

vmstat: `us≈57.4%  sy≈16.1%  id≈17.6%  wa≈5.7%  cs≈81 K/s`. **High us% and very high cs/s** — the pre-INSERT busy poll is the dominant CPU cost. 100 writers polling every 2 ms ≈ 50 K polls/s collectively, each hitting a single-row count query on the index. iowait drops because the system is CPU-bound, not IO-bound.

xact_commit_delta: ~1.28 M / 5 min ≈ 4 270 commits/s — confirms the writer poll loop is generating most of the commit rate (each `SELECT COUNT(*)` is a read-only stmt that does NOT increment xact_commit, but the 2 ms-cadence polling itself has overhead in connection scheduling).

### M vs J vs L — back-pressure is the worst of the three async-outbox variants

| Metric | J (no cap) | M (cap=200) | L (pseudo-sync) |
|---|---|---|---|
| **Commit events/s** | 365 | **971** | **2 876** |
| Caller p99 (ms) | 108 (caller-only) | **3 748** | **547** |
| End-to-end p99 (had-they-waited) | 364 s | 3.75 s + 4.28 s queue ≈ 8 s | **547 ms** |
| WAL/5 min (MB) | 804 | 437 | 941 |
| vmstat us% | 41 | 57 | 22.5 |
| vmstat cs/s | 29 K | **82 K** | 14.6 K |
| Caller has sync error semantics? | no | no | **yes** |
| Architectural complexity | low | low | medium (listener+dispatcher) |
| Worth shipping? | for fire-and-forget only | **no** | **yes, when caller can block** |

**What this says.**

1. **M is dominated by L on every dimension.** L beats M in throughput (3.0×), caller p99 (6.8× lower), CPU efficiency (cs rate 5.6× lower), and provides sync error semantics that M can't. There's no workload where M is the right answer if pseudo-sync is an option.

2. **M still beats J in throughput** (971 vs 365 evps) because the cap forces the drainer to keep running steadily — the queue never grows so large that the drainer's super-batch SELECT has to scan deeply. But this comes at the cost of catastrophically high caller p99 (3.7 s — basically all of which is back-pressure wait).

3. **The busy-poll back-pressure mechanism is the wrong primitive.** 100 writers polling a single-table count() at 2 ms cadence creates 50 K read-stmts/s plus context switches. A real implementation should use either (a) a session-level advisory lock that wakes when released, (b) a server-side wait via `pg_notify` from the drainer (effectively L), or (c) a token-bucket maintained externally (Redis, in-process semaphore — but that defeats the point of in-DB queueing).

4. **M's cap (200) is somewhat arbitrary.** A sweep across cap ∈ {50, 200, 1000} would map the throughput-vs-latency curve, but L's results render that exercise moot — there's no value-of-cap where M would beat L.

**For Phase 1 regression detection:** M is documented as a negative result. Don't ship; don't reference. If a future engineer asks "what about hard-cap back-pressure?", the answer is: it's strictly worse than pseudo-sync (L) and was characterized in this baseline.

## Observations

1. **Lock contention is the dominant cost from C onward in the shared-credit shapes.** Every writer needs `creation_void`'s lock. The `FOR UPDATE` lock is held for the entire transaction (lock acquire → loop events → commit → fsync). Concurrent writers serialize behind that hold time. Single-row throughput limit ≈ 1 / mean-hold-time = ~2 K events/s for small batches; adding more concurrent writers redistributes that throughput across more queue depth, not into more total events/s.

2. **CPU is consistently 30–55 % idle across every shape.** Even F with 100 writers and contention spread gets only to ~38 % idle. We are *never* CPU-bound. Adding cores wouldn't help; the next leverage point is more workload spread or platform-level changes (storage layer for iowait, kernel for ctx-switch overhead).

3. **Storage participates throughout (iowait 15–22 %).** ~50 K fsyncs / run at config A with ~600 MB write_bytes. Going to bigger batches (B) drops to ~750 fsyncs / run — that's the real benefit of batching: amortizing fsync.

4. **WAL volume tracks throughput, not concurrency.** 612 MB (A, 2.2 K evps) > 717 MB (B, 2.5 K evps) > **431 MB (F, 1.27 K evps)** > 406 MB (C, 1.4 K evps) > 162 MB (D, 559 evps) > 120 MB (E, 373 evps). Per-event WAL is roughly constant (~1 KB).

5. **Big batches help only without contention.** A → B: events/s 2 164 → 2 486 (+15 %). D → E: 559 → 373 (–33 %). Same change in batch size; opposite effect, because the bottleneck moves from per-batch overhead to lock-hold time.

6. **Spreading contention is dramatically cheaper than batch-size optimization.** D → F (same writers, same batch size, just different credit-side accounts): +128 % throughput, –96 % p50 latency. D → E (same writers, larger batches, same shared credit): –33 % throughput, +1 957 % p50 latency. Account architecture beats batch tuning every time.

7. **Variance is tight in contended configs (D, E, F)** and looser in uncontended ones (A, B). At 100 writers serializing, platform jitter is a small fraction of the 1.4 s median; at 1 writer the median is 5 ms and a single bad scheduler tick shows up.

8. **Zero deadlocks across all 13 shapes × 3 runs × 5 min** = ~5.3 M batches / ~24 M events. The lock-order proof in `post_transfers` is correct under every shape including the SKIP LOCKED outbox drain pattern (G), the super-batched outbox merging events from many rows into one call (J), **multi-worker concurrent super-batched drain (K)**, **pseudo-sync caller pattern with LISTEN/NOTIFY (L)**, **busy-poll back-pressure (M)**, mixed `post_transfers` + `reserve_inventory` traffic on shared `stock_available` rows (H), and mixed qty-ledger + value-ledger traffic with multi-currency contention (I).

9. **The ~2.5 K events/s single-writer ceiling is real for this hardware on this schema.** Routes to higher numbers:
   - **Spread the contention** (F demonstrates this — 2.3× lift just from cross-account workload). Real ERP traffic does this naturally; account sharding (Part IV §8) is the explicit Phase 1 mechanism for when natural spread isn't enough.
   - **Collapse concurrency to 1 writer** (outbox pattern, `acct-tyq`) — only works if the drainer **super-batches** events from multiple outbox rows into one `post_transfers` call. The naive single-row-per-call drain (G) measures **140 evps**, ~9× LOWER than F's direct-sync 1 274 evps, because per-call `post_transfers` overhead dominates without amortization. The super-batched variant is filed as `acct-hbg`.
   - **Different hardware** is a multiplier on these ratios, not a fix for the regime structure.

10. **Outbox is a latency/throughput tradeoff, not a free win.** G's caller p99 is 62× lower than F's (133 ms vs 8.25 s) because writers don't queue on `post_transfers` row locks. But G's commit throughput is 9× lower because the single sequential drainer is the bottleneck. End-to-end latency (caller-submitted → ledger-committed) is dominated by queue residency: 186 s p50, 336 s p99 in our run. Outbox makes sense if the application can tolerate eventual semantics on the ledger and the workload's tail-latency budget at the caller is more constrained than its throughput budget. For the typical ERP flow (an invoice posting that needs an accept/reject decision now), F is dominant by every measure. The "yes, adopt outbox" decision needs `acct-hbg`'s super-batched throughput data before it can be made on perf grounds.

11. **Reservation interleaving is a workload mix shift, not a regression.** Replacing 30 of F's 100 posters with `reserve_inventory()` callers doesn't damage qty throughput (−7 %) and actually *improves* qty tail latency by 25–32 % across p95/p99/max — the reduction in poster-vs-poster contention more than compensates for the added reserver lock contention. The reservation flow itself is fast (p50 6 ms, p99 191 ms) because `reserve_inventory`'s critical section is much shorter than `post_transfers`'s per-batch hold time. Phase 0's `reserve_inventory()` PL/pgSQL function (migration 0014, written specifically to fix the unsafe single-statement CTE in doc §3.3) holds under load: zero deadlocks, zero unexpected errors, zero over-promises across 660 K reservation calls.

12. **Multi-pool ledger composition is roughly additive.** When the 100-writer pool splits 50/50 between qty-side (cross-account spread) and value-side (per-currency hot accounts), combined throughput (2 840 evps) is essentially the sum of the two pools' independent throughputs (1 715 qty + 1 125 value). They don't lock the same rows; they only share connection-pool / WAL / pg_stat resources. Per-pool latency profile differs sharply because lock topology differs: qty's spread keeps p50 at 38 ms, while value's 4-accounts-per-currency convergence pushes p50 to 529 ms — same shape as D's hot-row contention pattern, but on the value ledger and with somewhat lower per-row writer pressure.

13. **Super-batched outbox closes only ~half the throughput gap to direct sync.** Adding super-batching (J: 365 evps committed) to the naive single-row drainer (G: 140 evps) is a 2.6× improvement — meaningful, and confirms that per-call `post_transfers` overhead is recoverable via amortization. But J is still 3.5× short of F's 1 274 evps committed, because the single-drainer architecture caps at one tx-at-a-time while F's 100 writers run in parallel. Super-batched outbox is the right architecture if **caller p99** is the binding constraint (J's 108 ms vs F's 8.25 s — 76× lower); it's not the right architecture if **ledger throughput** is the binding constraint.

14. **More drainers don't help — they hurt.** Going from J (1 drainer) to K (4 drainers) HALVES commit throughput (365 → 180 evps). Two reinforcing causes: smaller per-worker super-batches (less amortization) AND `FOR UPDATE` lock contention on the 100 `stock_available` rows when 4 super-batch `post_transfers` calls run in parallel. The right operational tuning for this workload is `T4_DRAINERS=1`. Multi-worker drain would only help with either (a) much wider account spread (so 4 drainers are unlikely to overlap on rows), or (b) per-`post_transfers`-call cost so high that single-call serialization is the actual bottleneck. Neither holds for Phase 0. **Single-worker super-batch is the architectural sweet spot.**

15. **Pseudo-sync via LISTEN/NOTIFY is the new throughput peak (L).** Caller INSERTs into `ledger_outbox`, then BLOCKS on a notification matching its row id. Drainer is J's super-batched single worker plus `pg_notify` per row outcome. Result: **2 876 evps committed** — 2.26× shape F (the prior realistic-traffic peak), 16 % above shape B (the prior single-writer-no-contention peak), 7.9× shape J. **Caller end-to-end p99 = 547 ms** (vs F's 8 251 ms — 15× better). The architectural mechanism: writers' INSERT and the drainer's super-batched `post_transfers` PIPELINE on the same DB. Writers never touch account `FOR UPDATE` locks, so the only contention surface is the drainer with itself (zero — single-threaded). Per-event time = 0.347 ms — *below* shape B's 0.37 ms because B's drainer paid round-trip-to-writer-and-back idle time between batches; L's drainer is continuously fed by 100 blocked writers. Queue depth is naturally bounded by the writer count (100). 0 timeouts across 207 K calls in production runs. **Pseudo-sync rewrites the outbox-vs-sync tradeoff:** if the caller can block (which most ERP flows can — they want the answer anyway), L gives both better throughput AND lower caller latency than direct sync. D3 should be reconsidered with this evidence; tracked separately.

16. **Hard-cap back-pressure (M) is a strict regression vs L on every dimension.** Shape M (cap=200, busy-poll, async caller) measures 971 evps committed and caller p99 = 3 748 ms. M beats J (365 evps) because the cap throttles producers so the queue stays small enough for the drainer's super-batches to be efficient — but M loses to L by 3.0× on throughput and 6.8× on caller p99 because L's notify-rendezvous is a strictly cheaper synchronization than busy-polling a count(). Context-switch rate exposes the cost: L = 14.6 K/s, M = 81 K/s (5.5× higher). **Documented as negative result** — if a future engineer asks "what about hard-cap back-pressure?", the answer is "L dominates."

17. **Pseudo-sync's caller p999/max tail is single-table page contention, not lock contention.** L's `enqueue_us` p999 spikes to ~2.3 s and max to ~2.7 s. This is the writer's `INSERT INTO ledger_outbox` round-trip occasionally landing on the same physical page the drainer is updating with `UPDATE ledger_outbox SET status='committed' WHERE id = ANY(...)`. F's tail (8 s p99) is account-lock contention, materially more severe. L's tail could be reduced further (HOT updates, per-account outbox partition, advisory-lock the depth check) but isn't load-bearing for Phase 0; filed conceptually for Phase 1.

## Top queries (representative — config D, run 3, `pg_stat_statements`)

`SELECT post_transfers($1, $2)` is essentially 100 % of database CPU + wait time across every shape. No surprise — every event flows through the function. We are *not* bottlenecked on parsing, planning, or other queries.

## Caveats

- **Single-machine consumer laptop.** Numbers reflect a developer rig, not production-class hardware. Absolute throughput is an artifact of the test rig; *relative* changes vs this baseline are the load-bearing comparison.
- **All credits target one row.** Every event posts `credit = creation_void(qty)`. Real workloads don't do this. `acct-2ey` will phase in cross-account-set, cross-ledger, multi-currency, and reservation-interleaved batches.
- **Outbox is now characterized across four variants.** G (naive per-row drain), J (super-batched single drainer), K (super-batched 4 drainers), L (pseudo-sync via LISTEN/NOTIFY), M (hard-cap back-pressure). The pseudo-sync variant (L) is the throughput peak across the entire matrix and changes the D3 (sync vs async outbox) calculus — see observation 15. Full perf-grounded reconsideration of D3 is deferred to a separate decision.
- **Standard cost only.** Non-`standard` cost methods are P0006 in Phase 0. WAC/FIFO/lot perf characterization is downstream of `acct-8gg` + a fresh baseline run.
- **Reservation traffic now characterized.** Shape H runs concurrent `reserve_inventory()` + `post_transfers` traffic. The two-statement reservation pattern (FOR UPDATE then promisable read) is safe under load. The remaining caveat is reservation lifetime: H accumulates 200 K+ reservations across 5 min without releasing any, which is not a representative production pattern. Production traffic would mix in reservation completions and cancellations; that's a Phase 1 follow-up.
- **No NUMA / CPU pinning, no isolated cores.** Kernel scheduler treats Postgres + cargo test + everything else equally. This *is* the variance source on uncontended configs.
- **Shape L numbers measured pre-`acct-uxu`.** All shape-L results above were collected on migrations 0001-0018 (post_transfers pre-WAC). After `acct-uxu` (migration 0021) lands the WAC dispatcher + lock pre-scan + branched single/two-pass execution, shape L commit_evps regresses ~16 % to ~2 428 evps median (3-run, 2026-04-30). Shape L remains the throughput peak across the matrix (2 428 still beats F=1 274 by 1.91×); the regression is the architectural cost of supporting per-batch pre-batch-balance reads required by WAC. Hot-path optimization is filed as a P3 follow-up; the pre-WAC numbers are kept here as the L-shape architectural ceiling.

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

# Shape H (reservation interleaving — 70 posters + 30 reservers)
T4_BINARY=load_reservation_interleave T4_CONFIGS="100:5:20" \
  T4_RESERVE_PCT=30 \
  ./scripts/run-perf-baseline.sh

# Shape I (qty + multi-currency value, 50/50 split)
T4_BINARY=load_value_workload T4_CONFIGS="100:5:20" \
  T4_VALUE_PCT=50 \
  ./scripts/run-perf-baseline.sh

# Shape J (super-batched outbox + 1 drainer)
T4_BINARY=load_outbox_super_batch T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_DRAIN_TIMEOUT_S=90 \
  ./scripts/run-perf-baseline.sh

# Shape K (super-batched outbox + 4 drainers — regresses, kept for repro)
T4_BINARY=load_outbox_super_batch T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_DRAIN_TIMEOUT_S=90 T4_DRAINERS=4 \
  ./scripts/run-perf-baseline.sh

# Shape L (pseudo-sync via LISTEN/NOTIFY — current throughput peak)
T4_BINARY=load_outbox_pseudo_sync T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_DRAIN_TIMEOUT_S=30 \
  ./scripts/run-perf-baseline.sh

# Shape M (async outbox + hard-cap back-pressure — kept for repro; dominated by L)
T4_BINARY=load_outbox_back_pressure T4_CONFIGS="100:5:20" \
  T4_DURATION_SECS=300 T4_DRAIN_TIMEOUT_S=30 T4_BP_CAP=200 T4_BP_POLL_MS=2 \
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
6. **Re-run G/J/L specifically** if D3 (sync `post_transfers`) is reopened — the canonical decision evidence is G/J/L commit rates vs F's events/s. L is the current throughput peak; if D3 reopens, L is the variant to ship-or-not-ship.

Append a new section dated below the current one rather than overwriting; the v0 → v1 → v2 history is the diff trail that detects creep.
