# Phase 5 v2 — Path B (routed) with fire-and-forget harness

**Supersedes [phase5-summary.md](../phase5/phase5-summary.md).** The v1 numbers
were pessimized by the harness driver's per-caller polling loop (each caller
blocked on `EXISTS(SELECT 1 FROM trx ...)` at 1ms tick until its own submission
materialized before submitting the next one). Per-caller throughput was
bounded to `1 / (ack_latency + committed_latency)`, conflating throughput
with latency. acct-tk58 refactored to fire-and-forget callers + a dedicated
observer task that timestamps first appearance of each source_id; throughput
is now `materialized_count / window_duration`.

**Run:** 2026-05-22T16:23 UTC against poc_v3 / acct-postgres container
(PG 18, `io_method=io_uring`, ledger_routed production build).
200-caller cap, 30s window, docker restart between scenarios. 10k pool
universe.

## Results (v2 — corrected)

| Scenario | Callers | Overlap         | Complexity | Throughput trx/s | Ack p99 µs | Cmtd p99 µs | Materialized | Attempts | Unseen | CG avg | CG p99 | Pipeline ns avg | Drains | Top wait                              |
|----------|--------:|-----------------|------------|-----------------:|-----------:|------------:|-------------:|---------:|-------:|-------:|-------:|----------------:|-------:|---------------------------------------|
| s1       |      10 | Uniform         | Simple     |           1660.8 |      30949 |    43117445 |        49839 |    49883 |     44 |   1.10 |      3 |         974,984 |  45433 | Extension:Extension                   |
| s2       |     200 | Zipf(1.5)       | Simple     |           1052.5 |    1149239 |    23538434 |        31780 |    38335 |   6555 |   7.34 |     63 |      12,577,798 |   5222 | LWLock:ledger_v3_staging_queue        |
| s3       |      10 | Uniform         | Complex    |            590.8 |     169869 |    55062822 |        17770 |    25402 |   7632 |  15.47 |     63 |     324,260,997 |    732 | Extension:Extension                   |
| s4       |     200 | Zipf(1.2)       | Complex    |            393.1 |    3508535 |    59190018 |        12207 |    21337 |   9029 |  12.98 |     63 |     874,536,883 |    247 | LWLock:ledger_v3_staging_queue        |
| s5       |     200 | Single hot pool | Simple     |           1055.4 |    1368391 |    35802578 |        32058 |    34262 |   2204 |  24.74 |     63 |     154,585,968 |   1213 | LWLock:ledger_v3_staging_queue        |
| s6       |     200 | Disjoint        | Simple     |            907.1 |    1937768 |    39594229 |        27488 |    27760 |    270 |   1.55 |      7 |       3,963,878 |  17966 | LWLock:ledger_v3_staging_queue        |

(`Throughput` = `Materialized / 30s`. `Attempts` = ack-confirmed enqueues.
`Unseen` = enqueued but didn't materialize within the 30s drain-deadline —
queue-residence overflow from fire-and-forget bursts. `Cmtd p99` is
queue-residence + commit time; the tens-of-seconds tail is the post-window
drain effect, not steady-state latency.)

## v1 vs v2 throughput delta

| Scenario | v1 (polling) | v2 (fire-and-forget) | v2/v1 |
|----------|-------------:|---------------------:|------:|
| s1       |        170.1 |              1,660.8 | 9.8×  |
| s2       |        419.9 |              1,052.5 | 2.5×  |
| s3       |        134.9 |                590.8 | 4.4×  |
| s4       |         59.1 |                393.1 | 6.7×  |
| s5       |        379.0 |              1,055.4 | 2.8×  |
| s6       |        293.7 |                907.1 | 3.1×  |

The polling harness pessimized Path B throughput by **2.5×–9.8×** across the
scenario set. Highest pessimization on s1 because the polling-bound per-caller
rate (~17/s) was an order of magnitude below the drain ceiling.

## Cross-path comparison vs Phase 3 (direct) — corrected regime map

| Scenario | Direct trx/s | Routed v2 trx/s | Winner    | Note                                                       |
|----------|-------------:|----------------:|-----------|------------------------------------------------------------|
| s1       |       2190.8 |          1660.8 | **A 1.3×**| Direct still wins low-concurrency uniform                  |
| s2       |        544.0 |          1052.5 | **B 1.9×**| **FLIPPED from v1.** Routed wins Zipf mid-band             |
| s3       |          0.0 |           590.8 | **B ∞**   | Path A rolled back ≈100%; Path B materialized cleanly      |
| s4       |          0.0 |           393.1 | **B ∞**   | Same shape as s3                                           |
| s5       |          0.5 |          1055.4 | **B 2,110×**| Hot-pool ceiling lifted from 758× to **2,110×**          |
| s6       |       1544.7 |           907.1 | **A 1.7×**| Direct still wins disjoint workload                        |

**Crossover map cleaner under v2:** Path A wins only where overlap is
genuinely absent and per-submission tx is cheap (s1 uniform, s6 disjoint).
Path B wins everything else — concentrated overlap (s5), Zipf mid-band (s2),
and any complex-workload regime where Path A's pool_lock contention rolled
back (s3, s4).

## Observations

### s2 flipped — routed wins Zipf mid-band

Phase 5 v1 showed Direct 544 vs Routed 420 (A wins 1.3×). v2 shows Direct 544
vs Routed 1052 (**B wins 1.9×**). Same scenario, same workload, same Path B
extension code — only the harness measurement methodology changed. The
polling-bound number undersold routed by **2.5×** in this cell. Direct's win
in the polling-bound view was an artifact of the harness shape, not a
property of the path.

This is the most important regime correction in v2: Zipf-distributed
contention (one of the more realistic production workloads) **belongs to
Path B**, not Path A.

### Hot-pool win extends to 2,110×

s5 went from 379→1055 trx/s on the Path B side, sharpening the headline win
vs Phase 3's 0.5 trx/s from 758× to **2,110×**. commit_group_avg climbed from
21.20→24.74 because the higher submission rate gives the router more
candidates per window to pack.

### Top wait events surface real routed-side contention

v1 top-wait was uniformly `Client:ClientRead` — an artifact of the polling
shape. v2 surfaces the actual bottlenecks:

- `LWLock:ledger_v3_staging_queue` dominates high-caller scenarios (s2/s4/s5/s6 with 200 callers). 200 callers contending for slot allocation in shmem staging when the committer can't drain fast enough.
- `Extension:Extension` dominates low-caller scenarios (s1/s3 at 10 callers). Background-worker pipeline activity (router scan + committer drain).
- `LWLock:ledger_v3_spillover_arena` sub-leading on s3 (complex 10-50 line submissions → arena pressure on the JSONB payload serialization).

These point at concrete optimization targets the polling-bound v1 hid.

### Committed-latency tail reflects queue-drain-past-window, not steady state

`committed_p99` ranges 23-59 seconds across scenarios. Misleading at first
glance — the queue residence for the slowest 1% of submissions during the
fire-and-forget burst is genuinely that long, because callers are enqueueing
faster than the committer can drain. When callers stop at 30s and the queue
keeps draining, residents enqueued late in the window wait the full drain-tail.

For steady-state latency assessment, look at the p50 — typically in the 2-30
second range under sustained overload. For a non-overloaded workload the p99
would track within a few × p50.

### Submitted-but-unseen: real loss under overload

Up to **9,029 submissions** (s4, 42% of attempts) successfully enqueued but
didn't materialize within the 30s drain deadline. This is real backpressure
loss in the measurement window — not an error (the system would drain them
eventually) but real evidence that fire-and-forget at 200 callers exceeds the
drain ceiling for complex workloads. The 16384-slot staging queue saturates
within seconds.

For production this would not happen at sustained load — callers' submission
rate is bounded by their business workload, not by the harness's "as fast as
possible" loop. The fire-and-forget burst pattern intentionally probes the
ceiling.

### WAL bytes per trx scales with rollback-on-enqueue tail

s3/s4 (complex workloads) show WAL b/trx in the 14,000-19,000 range vs
1,400-3,900 for simple. This is the bulk-write size scaling with line count,
which is unchanged from v1. Higher absolute throughput (vs v1) means
proportionally more WAL volume — but per-trx WAL is workload-shape, not
methodology-shape.

## Follow-ups

- **acct-69c7** (Phase 5 followup): Full 5-min × 1000-caller run for s5/s6
  with PG tuning. Both v2 and v1 are 30s × 200-caller; the larger run shape
  is still gated on `max_connections >= 1100` + container memlock.
- **Phase 6 characterization (acct-29p8)** consumes this v2 data, not v1.
  Dependency `acct-tk58` now closed so 29p8 is properly unblocked.
- **Staging-queue contention** (LWLock:ledger_v3_staging_queue dominant in
  high-caller scenarios) suggests potential router throughput uplift from
  multi-region staging or per-shard staging queues. Out of scope for Phase 6
  characterization; file as a P3 optimization candidate if the design
  iteration phase opens.
- **Observer cadence**: 10ms gives ~10ms resolution on committed latency.
  Doesn't affect throughput accuracy. If finer latency resolution is needed
  for Phase 6 cells, drop to 5ms — observer overhead is single-task so the
  scaling cost is trivial.
