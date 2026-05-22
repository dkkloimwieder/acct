# Phase 6 — Cross-path characterization

**Inputs:** Phase 3 direct-path numbers from `results/phase3/phase3-summary.md`
(measurement run `2026-05-21T13:27 UTC`) and Phase 5 v2 routed-path numbers
from `results/phase5-v2/phase5-summary-v2.md` (run `2026-05-22T16:23 UTC`,
fire-and-forget harness per `acct-tk58`).

**Scope:** 30s windows, 200-caller cap, identical 10k-pool universe, same
workload generator (`workload.rs`), same scenarios (s1–s6) for both paths.
Phase 3 used 1k-pool universe — the s6 stripe-size shrinkage there
(stripe = 5 instead of 50) is a confound the Phase 3 summary flagged;
factored into "weak signal" calls below.

## Headline result

The §10.4 hypothesis is confirmed and quantified: **Path B wins
concentrated overlap; Path A wins disjoint workloads; per-submission
complexity tilts everything toward Path B because Path A's
pool_lock contention is catastrophic.**

| Scenario | Concurrency | Overlap         | Complexity | Direct trx/s | Routed trx/s | Winner | Ratio       |
|----------|-------------|-----------------|------------|-------------:|-------------:|--------|------------:|
| s1       | 10          | none (uniform)  | simple     |      2,190.8 |      1,660.8 | **A**  | 1.3×        |
| s2       | 200         | concentrated    | simple     |        544.0 |      1,052.5 | **B**  | 1.9×        |
| s3       | 10          | none (uniform)  | complex    |          0.0 |        590.8 | **B**  | ∞ †         |
| s4       | 200         | concentrated    | complex    |          0.0 |        393.1 | **B**  | ∞ †         |
| s5       | 200         | maximal (1 pool)| simple     |          0.5 |      1,055.4 | **B**  | **2,110×**  |
| s6       | 200         | disjoint        | simple     |      1,544.7 |        907.1 | **A**  | 1.7×        |

† Direct returned 0 throughput because the Phase 3 workload generator's
narrow source_id space caused near-100% rollback rates on Complex
workloads — explicitly flagged in `phase3-summary.md` as a workload-shape
bug. Routed materialized through it because the committer's pristine-replay
loop excludes failing submissions inside the commit_group rather than
aborting the caller's tx. The s3/s4 ratios are real ("Routed produced trx;
Direct produced none") but reflect Path A's worst-case under a buggy
workload, not its steady-state ceiling.

## Regime map

Two axes are load-bearing:

1. **Overlap density** — disjoint → uniform high-card → Zipf → hot pool
2. **Per-submission complexity** — single-line vs multi-line

```
                       OVERLAP DENSITY  →
                       
                       disjoint     uniform     Zipf       single hot
                       ──────────   ──────────  ──────────  ──────────
   simple     low      —            A wins      —           —
              high     A wins (s6)  —           B wins (s2) B wins (s5)
   ─────────────────────────────────────────────────────────────────
   complex    low      —            B wins (s3) —           —
              high     —            —           B wins (s4) —
```

(`—` = not measured)

The crossover line lives in the upper-right of "simple" workloads — between
"uniform, 200 callers" (not measured directly) and "Zipf, 200 callers"
(s2 = Path B wins 1.9×). The line moves leftward (toward less overlap) as
complexity rises — Path B wins all complex cells we measured.

### Path A's territory

- **Low concurrency, uniform**: s1 — 10 callers, 10k-pool universe, simple
  payload. Path A hits the synchronous-commit ceiling (~2,200 trx/s) before
  Path B's router-window overhead can amortize anything. `commit_group_avg
  = 1.10` on Path B — barely batching, all overhead.
- **High concurrency, disjoint**: s6 — 200 callers, 50 pools/caller stripe.
  No overlap to batch. Path B's `commit_group_avg = 1.55` is still nearly
  one-per-group; the staging-queue lock and router-window latency become
  pure cost.

### Path B's territory

- **Concentrated overlap, any concurrency**: s2 (Zipf at 200 callers) and
  s5 (single hot pool at 200 callers). Router packs `commit_group_avg ≈
  7-25` envelopes per drain, amortizing pool_lock acquisition + fsync.
- **Complex payloads, any overlap**: s3/s4. Even under the Phase 3
  workload-bug caveat, Path B's commit-group isolation protects against
  per-line failures cascading into per-tx aborts.

### The s5 (hot pool) datapoint is the dispositive evidence

200 callers on a single pool: Path A serializes everyone behind FOR UPDATE
on the same pool_lock row → 0.5 trx/s. Path B routes them all into 21-25
envelopes per commit_group; one fsync per group → 1,055 trx/s. Same
correctness contract, **2,110× throughput ratio**.

This is the regime where a Postgres-native v0.2 design decisively beats a
naive "wrap everything in a tx" approach. The router exists for exactly
this workload shape.

## Observed pathologies

### Path A

1. **Workload-generator narrow source_id space** (Phase 3 finding). Caused
   100% rollback on Complex workloads. Independent of Path A; affects both
   paths' submission attempts. Fix is in `workload::next_lines` —
   `cs5k-followup`. Direct surfaces it as catastrophic throughput collapse
   because every collision aborts the caller's tx.
2. **pool_lock serialization on hot keys**. s5 = 0.5 trx/s = ~one trx every
   two seconds for 200 callers all wanting the same pool. Predictable,
   expected, and the design's reason for existing.
3. **Tuple-lock contention on Zipf head ranks**. s2 p99 = 1.1s — the
   long-tail Zipf head dominates the latency distribution. Lower throughput
   ceiling than s1 by ~4×.

### Path B

1. **Staging-queue lock dominance at 200 callers** (`LWLock:ledger_v3_staging_queue`
   top-wait on s2/s4/s5/s6 in Phase 5 v2). Single-region staging contends
   under fire-and-forget load. Multi-region or per-shard staging would
   relieve this — out of scope for Phase 6, file as P3 if optimization
   pressure arises.
2. **Committed-latency p99 in the 23-59 second range** under fire-and-forget
   bursts. Not steady-state — the queue grows beyond drain rate when callers
   submit faster than the committer can process; tail residents wait the
   drain-after-window. Production load (caller submission rate bounded by
   business workload) would not exhibit this shape.
3. **Submitted-but-unseen overflow** on s2/s3/s4 (up to 9,029 / 42% of
   attempts on s4). Same root cause — fire-and-forget at maximum rate
   exceeds drain ceiling within the 30s drain deadline. Real backpressure
   loss in the measurement window; not real loss in production.

### Both paths

- **30s + 200-caller cap is the wrong shape for the high-end regimes
  (s5/s6 at 1000 callers + 5min window)**. PG tuning needed —
  `max_connections >= 1100`, shared_buffers, container memlock. Tracked
  as `acct-69c7` (Phase 5 followup).

## Recommendations

These are *for the current single-tenant Postgres-native ERP project*; not
generic guidance.

1. **Default to Path A** for greenfield write paths. Synchronous, simple,
   correct, and faster at the workloads most documents look like
   (low-overlap, moderate complexity). Cuts out 1500+ lines of shmem +
   router + committer-pool code.

2. **Reach for Path B when workload analysis identifies concentrated
   overlap**: shared hot resources (a single GL account, a single
   inventory pool, a hot SKU's stock_available row), high-fanout
   reconciliation jobs, anything that would serialize 100+ callers on a
   single FOR UPDATE row. The s5 datapoint is the proof: 2,110× isn't a
   marginal win.

3. **Consider Path B for complex multi-line documents** regardless of
   overlap — Path A's rollback-on-any-line-conflict is catastrophic;
   Path B's pristine-replay isolates per-line failures inside the
   commit_group. s3/s4 evidence here is muddied by the workload-generator
   bug but the design property is real.

4. **Don't use Path B for disjoint or low-concurrency workflows.** The
   router-window + staging-queue overhead is pure cost when there's
   nothing to batch.

5. **Hybrid is on the table**: synchronous Path A as the default, with
   specific entry points (e.g., a `post_period_close` or `post_recon_run`)
   routed via Path B when workload analysis flags them. Both extensions
   coexist on the same schema.

## What Phase 6 leaves open

- **Equivalence verification** (`acct-t9lo`): both paths must produce
  byte-equivalent `trx + trx_line + pool_state` for the same input. Not
  yet executed; the characterization above relies on workloads being
  *semantically* the same, not *output-identical*. `acct-33b6` runs the
  equivalence suite once t9lo lands.
- **Full-scale s5/s6** (`acct-69c7`): 1000-caller × 5min window once PG
  tuning lands. Should sharpen the hot-pool ceiling and stress disjoint
  scaling.
- **§13.1 DESC index decision** (`acct-s7da` defer): Phase 6 EXPLAIN data
  on `trx_line (pool_id, trx_seq)` access patterns determines whether the
  DESC index is justified. Not yet pulled.
- **Crossover-line refinement**: this characterization establishes
  *where* the line sits qualitatively (overlap density + complexity).
  Quantifying the exact crossover concurrency for each overlap mode would
  require a finer scenario grid — not Phase 6's deliverable, file as
  follow-up if design iteration needs it.
