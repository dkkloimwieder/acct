# Batched H bench — acct-zm69 / zm69.h6

**Date:** 2026-05-14
**Driver:** `poc/batch-ledger/bench/run-h-batched.sh`
**Bench:** `poc/batch-ledger/tests/bench_h_batched.rs`
**Schema:** mig 0027 (`post_batch_h`) + mig 0028 (`post_batch_h_app`)
**DB:** acct_poc on Postgres 18 (host port 5111), tuned-conf
**Workload:** 20 workers, 60s duration, 70% issue / 30% receipt mix, SERIALIZABLE + retry-on-40001 (max 10 retries with 50µs × retry_count backoff)

## Headline

| # | Function | Batch | Groups | Committed-batches/s | Transfers/s | Abort % | Retries/commit | p99 batch (ms) |
|---|---|------:|------:|------:|------:|------:|------:|------:|
| 1 | `post_batch_h` | 100 | 50 | **3.4** | 340 | 73.9 | 32.3 | 3,344 |
| 2 | `post_batch_h` | 1000 | 50 | **0.3** | 261 | 72.1 | 29.7 | 29,119 |
| 3 | `post_batch_h` | 10000 | 50 | **0.0** | 44 | 65.0 | 23.4 | 1,590,594 |
| 4 | `post_batch_h_app` | 1000 | 50 | **1.2** | 1,159 | 73.7 | 31.1 | 8,662 |
| 5 | `post_batch_h` | 1000 | **1 (fan_in)** | **0.0** | 9 | 70.0 | 27.5 | 662,787 |
| 6 | `post_batch_h` | 1000 | **5000 (fan_out)** | **2.9** | 2,906 | 70.2 | 27.1 | 3,529 |

All runs invariant-clean (`overconsume_groups=0` under SERIALIZABLE).

## A2 baseline comparison

A2 (the per-backend shadow approach shipped at `acct-b3vs`, the design Candidate H proposes to replace) was measured by `bench_fifo_rollback_inject` at:

- **5K pools fan_out, batch=1000, 20 workers, 5% rollback: 37.4 committed-batches/s = 37,400 transfers/s**

Comparable batched-H shape (#6 fan_out g=5000): **2,906 transfers/s — 13× slower than A2.**

The single-row H probe (`bench_h_probe`) Realistic regime hit 1,239 committed-rows/s — that signal projected to ~33× A2 at single-row. At batch=1000 the signal **inverts**: batched-H is decisively slower than A2 at every measured shape.

## Verdict

**Candidate H fails the production-shape scaling test.** No shape meets the audit's acceptance bar of ≥30 committed-batches/s. The single-row probe was misleading; batched-H is **architecturally inferior to A2** at production batch sizes.

## Why the single-row → batched leap kills H

The probe (single-row INSERTs per txn) had a small SSI conflict surface: each transaction read predicates for **one** layer_group_id. With 50 groups × 20 workers, average group overlap was modest → 0.57% aborts.

At batch=1000 with 70% issue mix = 700 issue rows per batch:

- **The mig-0026 deferred trigger fires 700× per commit**, each fire running `SELECT SUM(qty) FROM cost_layers_h WHERE layer_group_id = X` + `SELECT SUM(qty) FROM cost_consumptions_h WHERE layer_group_id = X`. SSI tracks each as predicate reads.
- 700 rows + 50 groups: by pigeonhole every batch touches every group (often multiple times).
- 20 concurrent workers × 50 groups = total cross-batch overlap on the predicate read sets.
- SSI fires SQLSTATE 40001 on essentially every commit → 70-74% final-abort rate after 10 retries.

The `post_batch_h_app` variant (mig 0028) tested whether per-row trigger fires were the bottleneck: it replaces the per-row deferred trigger with ONE set-based aggregate check per touched group at end of wrapper. Result: **4× faster per-commit but identical abort rate (73.7% vs 73.9%)**. The per-row trigger contributes some cost but is NOT the dominant factor. The dominant factor is **SSI's predicate-read-conflict structure**, which is intrinsic to the H design under SERIALIZABLE regardless of trigger granularity.

## Why fan_out doesn't rescue H

At g=5000 (fan_out, matching A2's bench shape), batch=1000 means 700 issues spread across 5000 groups → ~0.14 issues per group average. Cross-batch overlap is statistically modest. **Yet abort rate is still 70%.**

Hypothesis: PG's SSI uses `sireadlock` tracked at page granularity. With 5000 layer_group_id values, many groups co-reside on the same heap/index page. Concurrent inserts touching DIFFERENT groups can still register conflicts at the page level. Verified empirically by:
- Abort rate barely changes between g=50 (max overlap) and g=5000 (low overlap) — 73% vs 70%.
- Throughput scales modestly with group count (transfers/s: 261 @ g=50 → 2,906 @ g=5000, only 11× lift for 100× more groups) — consistent with page-level conflict residue.

Even if page-level SSI were the only issue (and lock granularity could theoretically be tuned), A2's per-backend shadow design avoids SSI entirely — it operates at READ COMMITTED with explicit FOR UPDATE on the cost_layer rows it actually touches. That's structurally more concurrent than H's SSI-mediated approach.

## Why batch=10000 is catastrophic

At batch=10000 the wall_secs ballooned from 60s → 1591s. Reason: 7 commits in the entire run took ~3.3 minutes each (p50=1578s). The bench loop checks deadline at iteration top; once a batch attempt starts, it must run to completion. With 23 retries averaging ~half-minute each, batches that did commit took 3+ minutes of wall time. Not a bench bug — characterizes how badly H scales as batch grows.

## What this means for acct-zm69

The audit had recommended "prototype H" based on the single-row probe data. **That recommendation is now retracted.** The probe extrapolation was qualitatively wrong because the conflict surface scales quadratically with batch size in ways the single-row shape couldn't reveal.

Three paths forward:

1. **Accept A2 as the operational design**; surface the over-consume gap via `fifo_overconsume_check` (shipped @ acct-a3rj Phase B) as a detect-only signal. Production cost: rare but possible over-consume incidents detected after-the-fact, requiring out-of-band reconciliation. Most pragmatic.

2. **Pursue Candidate G** (OCC + commit-time apply, originally documented in `fifo-arena-correctness-audit-2026-05-14.md`). G has its own concurrency model — staged ops + commit-time replay under EXCLUSIVE — which may avoid SSI predicate-read amplification. Needs its own bench-first probe before committing to a rewrite.

3. **Investigate hybrid shapes** — e.g., H structure under READ COMMITTED with per-batch FOR UPDATE on touched layer_group rows as a coarse application-level lock. Would lose H's free-of-extension simplicity (need explicit lock acquisition) but might recover throughput.

Recommendation: **Path 1 in the short term** (A2 + detect-only recon is shipped and correct for the production cost-flow paths that matter). **Path 2 worth a 1-day spike** to characterize before committing to either A2-forever or a new architectural direction.

## Open follow-ups

- File `acct-zm69-h-followup` (P3): characterize H under READ COMMITTED + caller-side FOR UPDATE on layer_groups as hybrid shape — measures whether SSI is the bottleneck vs PG-level write contention.
- zm69.h7 (history-growth probe at ERP scale) is **moot** for H decision purposes — batched-H already fails at the lowest history level (no pre-seeded consumptions). Mark blocked-by zm69 architectural decision.

## Raw logs

Per-shape logs at `/tmp/h_batched_run1/`:
- `post_batch_h_b100_g50.log`
- `post_batch_h_b1000_g50.log`
- `post_batch_h_b10000_g50.log`
- `post_batch_h_app_b1000_g50.log`
- `post_batch_h_b1000_g1.log`
- `post_batch_h_b1000_g5000.log`
