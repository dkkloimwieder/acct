# Phase 6 cross-path equivalence sweep — S1–S6

**Run:** 2026-05-22T19:01:40Z
**Index:** `results/phase6/equivalence/run-2026-05-22T19-01-40Z.log`
**Issue:** acct-33b6 (Phase 6 epic acct-dipt 3/5)
**Companion docs:** `CHARACTERIZATION.md` (perf side), `phase5-v2/phase5-summary-v2.md` (throughput baseline)

## Scope

Cross-path equivalence per design-v3 §8.4. The harness runs an identical
deterministic submission list through Path A (`ledger_submit_trx`,
synchronous direct write) and Path B (`ledger_enqueue_trx`, async shmem
+ router + committer pool), snapshots `trx + trx_line + posting_line +
pool_state` after each, and diffs the canonicalized form. The
diff splits findings into two buckets: structural/non-WAC errors
(fail-loud) and WAC unit_cost drift on shared pools (informational
by default per acct-mcey; `--strict` upgrades them to errors).

Universe: 100 pools (10 SKUs × 10 locations, all `wac`). Submissions
per caller: 50 (default). Callers per scenario: per `scenarios.rs`
spec, capped to 20 when natural count > universe_count.

## Results

| Scenario | Workload (overlap × complexity) | Callers | Trx | Lines | Pools | Lenient | Strict T1 / T2 / T3 |
|---|---|---:|---:|---:|---:|:---:|:---:|
| s1 | uniform / simple    | 10 |  500 |   500 | 100 | ✓ identical              | — |
| s2 | zipf(1.5) / simple  | 20 | 1000 |  1000 |  71 | ✓ identical              | ✗ / ✗ / ✓ |
| s3 | uniform / complex   | 10 |  500 | 15327 | 100 | ✓ identical              | — |
| s4 | zipf(1.2) / complex | 20 | 1000 | 29785 | 100 | ✓ (17 WAC drifts, bound) | ✗ / ✗ / ✗ |
| s5 | single-hot-pool     | 20 | 1000 |  1000 |   1 | ✓ identical              | ✗ / ✗ / ✗ |
| s6 | disjoint stripes    | 20 | 1000 |  1000 |  20 | ✓ identical              | — |

**Lenient gate**: structural equivalence. All six scenarios pass. Trx
counts, line counts, posting_line counts, and pool_state row counts
are identical between paths. All `(pool_id, layer_seq, qty)` tuples
match. The only inter-path differences are WAC unit_cost truncation
deltas on the highest-overlap shared pools.

**Strict gate**: byte-identical including WAC unit_cost. Race-conditional
on s2/s4/s5 — depends on committer-pool scheduling order. s2 hit
identical 1/3 trials; s4 was 0/3 with the most drift; s5 was 0/3 with
a single deterministic-pool, ±1 Δ.

## WAC drift catalog

All observed drifts (lenient s4 + 9 strict trials, ~70 individual
deltas across the sweep):

- **Method**: 100% `wac`. Zero drifts on any other column or pool method.
- **Magnitude**: |Δ| ∈ {1, 2, 3, 4}. Mode = 1. Max observed = 4 (s2 strict T2, pool=2 qty=8570 A=461 B=465).
- **Sign**: both directions; ~50/50 split. Net is not biased toward over- or under-valuation.
- **Pool concentration**: drift surfaces on the zipf-hot pools (pool_id 1–30 area for s4; pool=1 for s5). Cold pools never drift in this run.
- **Qty alignment**: every drifted row has matching `qty` between A and B. Drift is purely on `unit_cost`, which is the divisor's-domain truncation per acct-mcey.

## Observations

1. **Zero non-WAC errors across 15 runs**. No structural diffs, no qty
   mismatches, no posting_line differences, no missing/extra rows.
   Cross-path equivalence holds for everything except the documented
   WAC truncation property.

2. **s5 is the cleanest demonstrator**. Single pool, every submission
   contends, every strict trial produces exactly 1 drift of magnitude
   ±1 on the same `(pool=1, qty=52101)` row. Three trials' Path B
   values: 248, 248, 250 against Path A's 249. The bound `|Δ| ≤ 1`
   here matches what theory predicts for a single-pool full-mix
   regime (worst-case truncation accumulates over N depletions but
   re-converges as the pool grows).

3. **s4 produces the most drift** (10 / 7 / 19 across strict trials).
   Zipf(1.2) is a flatter distribution than zipf(1.5), so load spreads
   across more shared pools; complex workloads multiply lines per
   submission, multiplying the number of WAC ops per commit_group.
   This is the regime that motivated acct-iwlq (scaled fixed-point):
   not because correctness fails, but because per-pool drift becomes
   visible in audit-style byte equivalence.

4. **s2 race frequency 33% identical** (1/3 strict trials). Lenient
   passed identical, which means the same workload can occasionally
   produce zero drift even at 1000-submission zipf(1.5). Confirms
   the bound is loose and the worst-case is uncommon.

5. **s1, s3, s6 are byte-identical** at this scale. s3 is notable —
   500 submissions × ~30 lines each = 15327 lines, complex workload,
   100 pools — produces zero drift even without `--strict`. Suggests
   the trigger is high per-pool depletion concurrency, not raw
   work volume.

## Correctness story

Path A and Path B produce equivalent ledgers for all six scenarios at
the structural level. The documented WAC truncation property (acct-mcey,
shipped in `equivalence.rs::DiffResult` split + `--strict` opt-in)
absorbs the running-average reordering deltas as expected. No new
P0 follow-ups surface from this sweep.

Cross-references:
- **acct-mcey** (closed) — classifies WAC drift in the diff; mitigation in place.
- **acct-iwlq** (open, P2) — proposes scaled fixed-point `unit_cost` (×100 / ×10000 multiplier) so per-cent truncation is pushed below the audit threshold, eliminating drift mechanically without per-path coordination. Larger refactor — schema + ledger-core + bulk-write scaling.
- **CHARACTERIZATION.md** — the perf companion to this correctness sweep.

## Acceptance

acct-33b6 acceptance per `bd show`: "any divergence is a P0 bug per
design-v3 §10.1; file follow-up issues as found." No P0 divergences
found. Path B is equivalent to Path A under all six characterization
scenarios, modulo the explicitly-documented WAC integer-truncation
property already tracked by acct-mcey / acct-iwlq.

Phase 6 epic acct-dipt: 3/5 (60%) after this close. Remaining children
acct-adte and acct-s7da are tagged `[defer]` parking-lot items.
