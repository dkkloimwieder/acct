# acct-at8x — latency vs offered load: the sustainable-rate curve (routed committer)

**Question.** acct-hh7b measured latency only under full-blast open-loop overload (multi-second
ack p50 = staging-ring admission backpressure at saturation, not intrinsic latency). The number
that matters for an SLO is missing: **what latency does the shipped config deliver at a given
offered rate, where is the knee, and what is the max sustainable rate under a p99 band?**

**Bottom line.**
- **Below saturation the routed committer is fast everywhere:** admission (ack) is
  **micro-seconds** (160 µs–1 ms p50 at every operable rate) and enqueue→durable-commit is
  **~60–90 ms p50 / ~105–160 ms p99** on both workloads. hh7b's multi-second figures are
  confirmed as pure overload backpressure.
- **The commit-latency floor (~60 ms) is the 50 ms BGWorker tick cadence** (`router.rs:107`,
  `committer.rs:138` `wait_latch(50ms)`) — not the batch window, not the cap. Sub-50 ms commit
  latency would require shortening the tick (or latch-kicking on enqueue), nothing else.
- **The latency knee is sharp and sits just below each workload's throughput ceiling:**
  - **s10 intertwined: knee between 850 and 925 trx/s.** 850 → 99 ms p50 / 293 ms p99;
    925 → 0.9 s / 1.9 s. The pool-lock serialization floor (~1k, hh7b §3) is what the latency
    wall expresses.
  - **s2 spread: knee between 3,500 and 4,000 trx/s for p99.** 2.5–3.5k are clean
    (p99 110–120 ms); at 4k+ p99 jumps to ~16 s while **p50 stays ≤ 232 ms all the way to
    10k+**. The tail is bimodal: the disjoint majority commits fast; the zipf-1.5 hot pool's
    serialized sub-stream crosses its *serial* drain capacity and its queue grows for the rest
    of the run (tail ≈ run length — unbounded growth, not a fixed 16 s).
- **Zero drops at every rate** — the 16384-slot ring absorbs surges; past-knee overload
  expresses as committed-latency growth, never loss.

**Max sustainable offered rate under a committed-p99 SLO (production defaults, this host):**

| SLO (committed p99) | s10 intertwined (50 callers) | s2 spread (64 pools / 64 callers) |
|---|--:|--:|
| ≤ 150 ms | ~750 | ~3,500 |
| ≤ 300 ms | ~850 | ~3,500 |
| ≤ 2 s | ~925 | < 4,000 |
| p50-only ≤ 250 ms | ~850–900 | ~10,000 |

---

## Methodology

New harness capability (this issue): `--target-rate <trx/s>` paces the routed caller pool —
the total rate splits evenly across callers with **staggered absolute-schedule interval
pacing**; a caller that falls behind (enqueue blocked) sheds debt rather than burst-repaying,
so past saturation the achieved rate degrades honestly below offered (both recorded).
Production single-push path (`--batch-size 1`, one trx per `ledger_enqueue_trx_c`).

Two latency surfaces per cell (the report already carried both):
- `ack_latency_us` — **admission**: the enqueue call returning (blocks only on ring-full backpressure).
- `committed_latency_us` — **full pipeline**: enqueue instant → observer-seen materialize instant.

GUCs at **production defaults** (committer_count 4, batch_size_max 200, batch_window_us 500,
pack on, router_window_size 1000). DUR=20 s, 2 reps, median shown; load-gated with the noisy-host
posture (workstation, Chrome up; `load1_end` recorded per cell — the high-rate s2 cells partly
self-load). Fresh seed per scenario; pgbouncer :6432.

## s10 — Pareto 80/20 intertwined, complex receipts, 50 callers

| offered | achieved | ack p50 / p99 | committed p50 / p99 | cg |
|--:|--:|--:|--:|--:|
| 100 | 100% | 567 µs / 1 ms | 63 ms / 108 ms | 5 |
| 250 | 100% | 314 µs / 815 µs | 68 ms / 110 ms | 12 |
| 500 | 100% | 218 µs / 596 µs | 74 ms / 116 ms | 21 |
| 750 | 100% | 246 µs / 626 µs | 90 ms / 157 ms | 27 |
| **850** | 100% | — | **99 ms / 293 ms** | 33 |
| **925** | 100% | — | **922 ms / 1.9 s** | 91 |
| 1000 | 100% | 268 µs / 1 ms | 1.5 s / 3.2 s | 129 |
| 1250 | 100% | 274 µs / 1 ms | 3.5 s / 11.2 s | 172 |
| 1500 | 100% | 291 µs / 1 ms | 4.5 s / 24.9 s | 187 |

Intertwined work hits its wall exactly where hh7b's throughput floor predicted (~1k,
pool-lock-serialized, locks/trx ≈ 4.6–14). Admission never backs up at these rates (the ring
holds the 20 s excess), so the wall is invisible to ack and entirely visible to committed.
**Operating rule: keep offered intertwined load ≤ ~850/s per this hardware, or shed/queue
upstream; no formation or committer knob raises this (hh7b §3) — only reducing per-pool lock
serialization would.**

## s2 — 64-pool zipf(1.5) spread, simple receipts, 64 callers

| offered | achieved | ack p50 / p99 | committed p50 / p99 | cg |
|--:|--:|--:|--:|--:|
| 1000 | 100% | 159 µs / 393 µs | 62 ms / 105 ms | 50 |
| 2000 | 100% | 176 µs / 511 µs | 65 ms / 108 ms | 98 |
| **2500** | 100% | — | **67 ms / 110 ms** | 119 |
| **3000** | 100% | — | **69 ms / 120 ms** | 142 |
| **3500** | 100% | — | **71 ms / 120 ms** | 163 |
| 4000 | 100% | 389 µs / 2 ms | 86 ms / 16.2 s | 126 |
| 6000 | 100% | 640 µs / 3 ms | 108 ms / 16.6 s | 134 |
| 8000 | 100% | 926 µs / 6 ms | 137 ms / 19.6 s | 143 |
| 10000 | 99% | 1 ms / 8 ms | 189 ms / 16.4 s | 132 |
| 12000 | 88% | 6 ms / 10 ms | 230 ms / 15.9 s | 140 |
| 14000 | 76% | 6 ms / 10 ms | 232 ms / 18.5 s | 140 |

Two regimes in one workload:
- The **disjoint majority** flows to ~10k/s with p50 under 250 ms (achieved saturates ~10.5k,
  76–88% of offered at 12–14k — the full-blast ceiling, reproduced under pacing).
- The **zipf hot pool** is a serial sub-stream: same-pool work fuses into one affinity
  component per tick and drains one group at a time under one pool_lock. Between 3.5k and 4k
  offered, the hot pool's share crosses its serial capacity; its queue then grows for the rest
  of the run and the p99 tail ≈ run length (16 s on a 20 s run — it would keep growing on a
  longer run). **A zipf-skewed workload's p99 SLO is set by its hottest pool's serial rate,
  not by aggregate capacity.**

## Cross-cutting conclusions

1. **The system has no latency problem below the knee** — the hh7b multi-second numbers were
   saturation artifacts of the unpaced bench, as suspected.
2. **Per-pool serialization is the latency wall in both regimes** (intertwined: many shared
   pools; spread: the single hottest pool). Formation/committer knobs do not move it (hh7b).
3. **The ~50 ms tick cadence is the latency floor.** If a future SLO needs sub-50 ms commits,
   the lever is the router/committer `wait_latch(50ms)` (e.g. latch-kick on enqueue), an
   isolated and cheap change — not formation policy.
4. **Backpressure-by-blocking works**: zero drops at up to 14k offered; overload degrades
   latency, not correctness.

## Caveats

- Noisy host (Chrome; high-rate s2 cells also self-load to load1 3.5–6.6). Knee *positions*
  were measured on quiet-gated cells (load1 1.1–1.9) and are structurally robust; absolute
  rates scale with hardware — re-calibrate per deployment.
- 20 s cells: past-knee p99 values are run-length-bounded (queue still growing at cell end);
  they understate steady-state overload latency. Below-knee values are steady-state.
- Single rep pair per cell; s10/s2 only. Deep (s7) and disjoint (s6) curves not run — their
  ceilings are known from hh7b and their knee mechanism (row-lock / WAL) is the same shape.

## Artifacts

- CSVs: `latency_vs_load_{s10,s2}.csv` + knee refinements `latency_vs_load_{s10,s2}_knee.csv`;
  log `latency_vs_load.log`.
- Runner: `bench/sweep-latency-vs-load.sh`.
- Harness: `--target-rate` pacing (`cli.rs`, `main.rs`, `driver_routed.rs` caller_loop
  absolute-schedule pacer with stagger + debt-shedding).
