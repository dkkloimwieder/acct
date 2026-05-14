# H+ext FIFO cost-attribution paths — comparison

**Date:** 2026-05-14
**Drivers:** `poc/batch-ledger/bench/run-h-paths.sh`, `bench_h_ext_deferred_drain.rs`
**Tests:** `poc/batch-ledger/tests/h_ext_fifo_correctness_t1.rs` (parameterized via `POC_TEST_FUNCTION`)
**Schema migrations:** 0030 (path 1 base) + 0031 (path 1 deadlock fix) + 0032 (path 2 base) + 0033 (path 3) + 0034 (path 2 fix: drop durable qty_remaining UPDATE)
**Extension modules:** `h_arena` (group invariant — mig 0029) + `h_layer_arena` (per-layer CAS — added for path 2)
**DB:** acct_poc, Postgres 18 tuned-conf, 20 workers, 60s per shape, READ COMMITTED

## Throughput comparison

| Approach | b=1000 g=5000 fan_out | b=1000 g=1 fan_in | b=1000 g=50 | b=100 g=50 |
|---|---:|---:|---:|---:|
| **A2 baseline** (shadow + replay) | 37,400 | — | — | — |
| **Plain H+ext** (qty invariant only; no per-layer attribution) | 242,279 | 287,081 | 279,391 | 181,566 |
| **Path 1**: FOR UPDATE walk | 9,997 | 390 | 2,522 | 2,414 |
| **Path 2**: per-layer shmem CAS | 3,348 | 1,321 | 3,632 | 3,643 |
| **Path 3**: deferred drain (hot path) | 218,713 | 261,851 | 256,030 | 130,586 |
| **Path 3**: deferred drain (end-to-end attribution) | **2,853** | — | — | — |

All in transfers/s. End-to-end attributed-consumptions/s for path 3 = total_drained / (hot_secs + drain_secs).

## Correctness — all three paths pass

| Probe | Path 1 | Path 2 | Path 3 |
|---|:-:|:-:|:-:|
| `fifo_overconsume_check_h_ext` | 0 | 0 | 0 (post-drain) |
| `qty_remaining` vs depletions | 0 drift | n/a (path 2 keeps residual in shmem) | 0 drift (post-drain) |
| `h_arena` vs durable residual | 0 drift | 0 drift | 0 drift |
| Deadlocks | 0 | 0 | 0 |

Probe shape: 16 workers × 5 batches × 5 issues across 4 groups × 10 thin layers (50 qty each). The R-MB6-equivalent stress shape for layer attribution.

## What surfaced — perf, not correctness

The user predicted "we will run into correctness issues." That hypothesis is **falsified for the qty / per-layer-attribution invariants** under each of the three designs. All three preserve the invariant under concurrent issues. **The interesting findings are all on the perf axis.**

### Finding 1 — Path 1 has a deadlock vector that mig 0030 missed

Initial mig 0030's wrapper iterated issues in jsonb-array order (i.e., randomized per worker). Cross-group FOR UPDATE acquisition order varied across workers → classic cycle deadlock. Correctness probe: 8 / 80 batches committed (90% deadlock loss). **Fix in mig 0031: sort issues by `layer_group_id` before walking** — globally-consistent lock order, no cross-group cycle. Probe after fix: 80 / 80 commits, 0 deadlocks.

This is the same bug class as A2's R-MB6 — an attribution-time concurrency race — but here it manifested as deadlock loss rather than silent over-attribution. Either symptom is acceptable to surface, but the deadlock retried into final failure under the bench harness's retry budget.

### Finding 2 — Per-layer shmem CAS does NOT beat FOR UPDATE when plpgsql LOOP dominates

Path 2 originally (mig 0032) mirrored the CAS-decrement to a durable `UPDATE qty_remaining` — same row-level lock contention as path 1, plus the CAS overhead. Throughput nearly identical to path 1 (22.8 vs 24.1 commits/s @ b=100 g=50). **Fix in mig 0034: drop the durable UPDATE** — shmem residual becomes truth.

Post-fix path 2 is barely faster than path 1 at fan_out (3,348 vs 9,997 — actually slower at fan_out, modestly faster at fan_in). The bottleneck is now the **per-issue plpgsql `FOR` loop overhead**: each issue does `SELECT layers ORDER BY ... + N × (h_layer_decrement + INSERT depletion) + UPDATE consumption.unit_cost`. That's ~3-5 SQL roundtrips per issue × 700 issues per batch = ~2-4K SPI roundtrips per batch. plpgsql cannot match the bulk-INSERT throughput of plain H+ext.

Implication: realizing CAS's perf benefit needs a **native batch wrapper** — single FFI call takes the whole envelope JSONB, walks shmem internally, returns depletion records as a recordset. That's a substantially larger native build than h_layer_arena alone.

### Finding 3 — Path 3's hot path is fast, drain is the new bottleneck

Path 3 hot path is essentially plain H+ext (218K transfers/s at b=1000 g=5000) — 6× A2 baseline. **But the drain runs at 2,909 consumption-rows/s** (single-writer, plpgsql FIFO walk). End-to-end attributed-consumptions/s = 2,853 — slower than A2 baseline and dramatically slower than the hot path.

Drain throughput is fundamentally limited by the same plpgsql + FOR UPDATE shape as path 1. Single-drainer is the obvious next concern; multi-drainer needs layer-level locking discipline. Even at multi-drainer ideal scaling, drain is unlikely to keep pace with hot-path throughput at sustained load → **unbounded pending backlog**.

The path 3 "value proposition" is conditional: it works if your workload can tolerate **eventually-consistent FIFO attribution** with a backlog that may grow during bursts. For workloads where every consumption needs cost AT WRITE TIME (most ERP cost-flow paths posting GL legs in-line), path 3 doesn't help.

### Finding 4 — Plain H+ext is the actual ceiling, but only solves the qty invariant

Plain H+ext's 242K transfers/s at fan_out g=5000 is the throughput ceiling for any H-shaped design. All three FIFO cost-attribution paths add ~10-80× slowdown. The headline takeaway from the original H+ext bench (6.5× A2) was correct **for the qty-invariant-only workload** — equivalent to WAC or standard cost-method, where consumption.unit_cost is determined by group running-avg or constant, not by per-layer FIFO walk.

**The reason none of the three FIFO paths approach the plain H+ext ceiling is that FIFO attribution is inherently serialization-heavy** — per consumption you must produce N depletion rows in receipt-order against N specific layers, regardless of where the residual state lives.

## Comparison to A2 — the unflattering view

A2's 37,400 transfers/s at the same shape **includes** per-layer attribution via cost_layer_depletions. Path 1 (FOR UPDATE) is 4× slower; path 2 (shmem CAS but plpgsql LOOP overhead) is 11× slower; path 3 (end-to-end with drain) is 13× slower. **A2's per-backend shadow + replay is more efficient than any of the plpgsql-wrapped alternatives.**

The reason: A2 amortizes per-batch work inside ONE native function call. plpgsql wrappers cross the SPI boundary repeatedly (per issue, per layer) and pay function-call overhead each time. The shmem CAS path 2 only realizes its CAS benefit if the WALK itself is native — i.e., a `h_apply_batch_fifo(envelopes JSONB) -> SETOF depletion_record`.

## Path 4 (file as followup) — native batch FIFO

The architectural answer that *could* compete with or beat A2: a single native function that takes the full batch JSONB, walks shmem layer state in receipt-order internally (no SPI roundtrip per layer), and returns depletion records. Two key wins:

1. **One FFI call per batch** instead of ~3-5K plpgsql/SPI roundtrips.
2. **Native FIFO walk against shmem** — atomic per-layer CAS as before, but no plpgsql loop body.

This is "path 2 done right." Cost: substantially more native code (FIFO ordering metadata, native JSONB parsing, output-record construction). Equivalent in surface to the existing FIFO arena (fifo.rs) but using append-only durable tables + shmem invariant instead of shadow + replay.

## Recommendations

1. **Plain H+ext stands as the architecture for qty-invariant ledgers under WAC/standard cost methods** (242K transfers/s, 6.5× A2, correct by construction, no per-layer attribution gap). No regression vs current A2 for the workloads it serves.

2. **None of paths 1/2/3 are viable A2 replacements for FIFO cost-flow at production batch.** Path 1 is correctness-clean but throughput is 4× under A2. Path 2's CAS benefit is shadowed by plpgsql overhead. Path 3's hot path is fast but drain is the bottleneck and pending backlog grows unboundedly.

3. **Path 4 (native batch FIFO) is the architecturally interesting next step** if H+ext is to replace A2 for full FIFO cost flow. File as `zm69.h11`.

4. Path 1 (FOR UPDATE) is the simplest correct baseline. Could be acceptable for low-throughput FIFO ledger paths where 2-10K transfers/s is sufficient (e.g., monthly close batches, not hot-path issue posting).

## Open followups

- **zm69.h11** — Path 4: native batch FIFO wrapper. Single FFI call per batch; native shmem walk; returns depletion records.
- **zm69.h12** — Multi-drainer path 3 with per-group serialization. Quantify whether multi-drainer can close the drain throughput gap.
- **zm69.h13** — Path 2 + h_layer_arena per-group ordered list (intrusive linked list in shmem). Eliminates the durable `SELECT ORDER BY born_at` per issue. May or may not be enough — plpgsql FOR overhead still dominates the per-decrement call.
- **Path 1 fan_in mitigation** — wo-/batch-grouping consumers to the same group together (advisory lock per group) to reduce wait queue thrashing at the per-layer FOR UPDATE level. Unlikely to beat 1K transfers/s at g=1, 20w.

## Raw logs

- Path 1: `/tmp/h_ext_path1_run1/`
- Paths 2 + 3 sweep: `/tmp/h_paths_run2/`
- Path 3 end-to-end (drain): `/tmp/claude-1000/.../tasks/bd2100ow4.output`
