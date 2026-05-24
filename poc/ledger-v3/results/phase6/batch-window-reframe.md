# acct-w2dn — batch_window_us reframe (DISPROVEN by measurement)

**Issue:** acct-w2dn (P2; Part B of acct-e5fz)
**Run window:** 2026-05-24
**Initial bench:** `results/phase6/bench-w2dn/run-2026-05-24T04-33-17Z.log` (3-cell, cross-session)
**Resweep:** `results/phase6/bench-w2dn/resweep-2026-05-24T12-44-22Z.log` (10-cell, single-session)

## Headline

**The Part B premise — that `batch_size_max` is "just a soft safety cap" once `batch_window_us` is primary — does not survive measurement on v3 workloads.** The cap is genuinely load-bearing for committer pipeline throughput on complex (multi-line) submissions: pipeline_ns_avg scales superlinearly with `cg_size × lines_per_submission`, so the cap bounds lock-hold time on contended pool tails. No single default helps the hot-pool simple workload (s5, wants larger cap) without regressing the complex workload (s4, wants the cap held tight).

**Ship as a partial reframe:** keep `batch_size_max = 50` default (Part A's recommendation stands); bump the GUC range max from 10 000 → 100 000 so operators on hot-pool simple workloads can opt into larger caps without source changes. Doc-comments updated to explain the workload-dependent tuning.

## What changed in source

| File | Change |
|------|--------|
| `ledger-routed/src/lib.rs:78` | `BATCH_SIZE_MAX::new(50)` — **unchanged** (default holds). |
| `ledger-routed/src/lib.rs:213` | GUC range max `10_000` → `100_000`. Operator opt-in to larger caps. |
| `ledger-routed/src/lib.rs:208-216` | GUC doc-comment rewritten to explain the load-bearing role + workload-tuning guidance. |
| `ledger-routed/src/lib.rs:219-226` | `batch_window_us` doc-comment clarified ("once window elapses, emit with whatever's accumulated up to batch_size_max"). |
| `ledger-routed/src/router.rs:1-30` | Module-level pipeline doc updated to flag `batch_size_max` as load-bearing for committer throughput on complex workloads. |

**No behavior change.** Router emit logic and chunking loop are unchanged. The reframe is purely a documentation + operator-knob expansion.

## What the bench actually shows — single-session resweep

Single-session removes the cross-session noise that confused the initial 3-cell w2dn validation run (s2 jumped 1308→1602 between sessions at functionally-equivalent configs). Same 30s × 20 callers × 1000-pool universe as acct-e5fz Part A.

### s4 (zipf-1.2 complex) — bsm=50 dominates; monotonic drop above it

| bsm    | tx/s  | cg_avg | pipeline_ns_avg (ms) | drains | Δ vs bsm=50 |
|--------|------:|-------:|---------------------:|-------:|------------:|
| 50     | **576.2** | 27.34 |              519.3   |    454 | (baseline)  |
| 1 000  |   349.8   | 74.66 |            1 332.5   |     92 |   −39%      |
| 5 000  |   312.5   | 79.06 |            1 658.0   |     72 |   −46%      |
| 10 000 |   293.1   | 71.24 |            1 598.6   |     74 |   −49%      |
| 50 000 |   322.7   | 73.54 |            1 377.0   |     88 |   −44%      |

Throughput drops monotonically from bsm=50 to bsm=10 000, then plateaus around 300 tx/s. The mechanism: at bsm=50 the largest batches are capped (avg=27 because the router doesn't always fill it); above bsm=200 cg_size grows naturally to ~75, pipeline_ns_avg balloons from 519 ms to 1 333 ms. Committer is the bottleneck; lock-hold blocks subsequent commit_groups.

**The size cap protects s4-shape workloads. Removing it is a 39-49% throughput loss.**

### s5 (single hot pool simple) — peaks at bsm=5000 then plateaus

| bsm    | tx/s   | cg_avg | pipeline_ns_avg (ms) | ack p99 (ms) | Δ vs bsm=50 |
|--------|-------:|-------:|---------------------:|-------------:|------------:|
| 50     | 1 820.2 |  36.63 |               103.8  |       63.1   | (baseline)  |
| 1 000  | 2 458.3 | 143.36 |               281.8  |      232.5   |   +35%      |
| 5 000  | **2 579.9** | 160.60 |             321.7  |      223.1   |   +42%      |
| 10 000 | 2 437.4 | 171.72 |               345.3  |      212.1   |   +34%      |
| 50 000 | 2 438.1 | 165.69 |               331.8  |      220.2   |   +34%      |

s5 peaks at bsm=5 000 (2 580 tx/s) and plateaus. Going beyond bsm=10 000 doesn't help — the natural commit_group size at the default `batch_window_us=500 µs` settles around 160-170 regardless of cap. The latency cost is real: p99 ack jumps from 63 ms (bsm=50) to 220-230 ms (bsm=1000+).

**s5 wants bsm≈5 000 for a 42% throughput win at a 3.5× ack latency cost.**

### s2 (zipf-1.5 simple) — flat (omitted from resweep)

Per acct-e5fz Part A: throughput is flat across all bsm values (±4%). The workload's dispersion means windows close on time before any pool accumulates 50+ submissions; the cap doesn't bind. Re-confirming was redundant for the resweep.

## Why the reframe doesn't work

The Part B framing assumed:

> `batch_size_max` becomes a SOFT SAFETY CAP (e.g. 50 000) to bound worst-case memory under spike load — not the throughput cliff it is today.

The empirical surface contradicts this:

1. **Lock-hold time is the dominant per-batch cost on complex workloads** — not arena memory. At bsm=50 000 on s4, the committer's `pipeline_ns_avg` is 1 377 ms — vs 519 ms at bsm=50. The cap was protecting against lock-hold runaway, not memory exhaustion.

2. **Lock-hold scales continuously with `cg_size × lines_per_submission`, not in cliffs.** There's no single "safety bound" that's both large enough to be invisible for hot-pool workloads AND tight enough to throttle complex workloads. The two regimes want fundamentally different sizes.

3. **The router's existing time-window gate (`batch_window_us`) is already a primary cap on residency.** The size cap is a *complementary* cost-bound, not a redundant one. The two together — "emit when oldest waits ≥ window OR accumulated ≥ size" — is the load-bearing combination. Removing either is a regression on at least one workload.

The intuition that pushed the reframe (v21's m8-ceiling raised bsm to 5 000 for headline numbers) was workload-specific (single-pool throughput-only, no latency budget) and doesn't transfer.

## Workload-tuning recommendation

| Workload shape | Recommended `batch_size_max` | Recommended `batch_window_us` | Rationale |
|----------------|------------------------------|-------------------------------|-----------|
| **Default / mixed**           | 50 (default)    | 500 (default) | s4-protective; latency-friendly. Lowest-regret pick across measured workloads. |
| **Hot-pool simple** (s5-like) | 5 000           | 500           | +42% throughput at 3.5× ack latency. Sweet spot per resweep; bsm>10 000 doesn't help further. |
| **High-latency-budget simple**| 5 000           | 2 000–5 000   | Speculative — larger window may grow commit_groups further on the simple path. **Not measured here**; would be the natural follow-up if the use case surfaces. |
| **Complex (multi-line)**      | 50 (default)    | 500 (default) | Default IS the protection. Raising bsm regresses 39-49%. |

Operator opt-in:
```sql
-- For a deployment with confirmed hot-pool simple workload
ALTER SYSTEM SET ledger_routed.batch_size_max = 5000;
SELECT pg_reload_conf();
```

The GUC range was bumped to 100 000 (from 10 000) so operators have room to go beyond the measured sweet spot if a future workload justifies it. Default holds at 50 so deployments that don't tune are protected.

## Why ship anything at all if the source default doesn't change?

The reframe still earns its keep in two ways:

1. **Documentation correctness.** The pre-w2dn doc-comment said "Hard cap on submissions packed into a single commit_group. Larger batches amortize sub-tx + WAL costs; smaller batches keep per-batch wall-time below committer_lease_ms." That framing implies "larger is better for throughput, modulo a lease ceiling" — which the e5fz + w2dn resweeps show is incomplete (it's true for simple hot-pool workloads, false for complex workloads). The updated doc-comment lays out the workload-dependent guidance with citations.

2. **Operator opt-in to the measured sweet spot.** Pre-w2dn the GUC was clamped to ≤ 10 000. Going to 100 000 doesn't change default behavior but lets deployments with confirmed hot-pool workloads tune to e.g. 5 000 without source changes. Empirically, bsm=5 000 IS a real +42% win on s5-shape workloads; the wider knob makes it accessible.

## Cross-references

- **acct-e5fz** (closed 2026-05-24) — Part A measurement that established bsm=50 as the lowest-regret default. The single-session resweep here corroborates and extends that finding.
- **acct-aywu** (closed 2026-05-23) — order-sensitive method classifier on the router. Order-sensitive groups bypass the size cap entirely (their per-pool ordering invariant requires whole-group emission). The bsm tuning matters only for split-safe methods (WAC / wac_periodic / STD).
- **acct-tm09** (closed 2026-05-23) — per-pool sequence numbers. Independent of batch_size_max; correctness, not throughput.
- **v21 m8-ceiling bench** (`poc/queue-extension-v21/bench/results-m8-ceiling/README.md`) — the "raise bsm to 5 000" precedent. Workload-specific to single-pool throughput-only; not transferable.

## What this DOES NOT change

- **No router emit-logic change.** Chunking loop unchanged. Time-window gate unchanged.
- **No correctness changes.** aywu + tm09 carry order-sensitive correctness; bsm doesn't affect either.
- **No new bench script for the canonical sweep.** The e5fz `run-bench-e5fz.sh` + this resweep's `run-bench-w2dn-resweep.sh` are the empirical record.
- **No further bench-window sweep.** A bench_window_us sweep is plausible follow-up but is NOT in scope here per the workload-tuning matrix note above.

## Reproducing

```bash
# The resweep that disproved the reframe (10 cells, single session)
bash poc/ledger-v3/scripts/run-bench-w2dn-resweep.sh

# Verify the operator opt-in path
docker exec acct-postgres psql -U acct -d poc_v3 \
  -c "ALTER SYSTEM SET ledger_routed.batch_size_max = 5000;" \
  -c "SELECT pg_reload_conf();" \
  -c "SHOW ledger_routed.batch_size_max;"
# (run a hot-pool workload, observe +35-42% throughput on s5-shape)
docker exec acct-postgres psql -U acct -d poc_v3 \
  -c "ALTER SYSTEM RESET ledger_routed.batch_size_max;" \
  -c "SELECT pg_reload_conf();"
```
