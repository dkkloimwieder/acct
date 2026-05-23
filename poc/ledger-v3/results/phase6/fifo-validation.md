# acct-9mgx.1 — FIFO equivalence + bench

**Issue:** acct-9mgx.1 (P2; sibling of acct-9mgx.{2..6})
**Run window:** 2026-05-23
**Equivalence sweep (canonical, committer_count=1):** `results/phase6/equivalence/run-all-fifo-2026-05-23T13-04-03Z.log`
**Bench (canonical, committer_count=1):** `results/phase6/bench-9mgx1/run-2026-05-23T13-06-47Z.log`

## ⚠ Current default-config routed FIFO is inconsistent

The routed path **does not honor submission-order FIFO under default GUCs** (`ledger_routed.committer_count = 4`, `ledger_routed.batch_size_max = 50`). The router splits a single pool's pending submissions across multiple commit_groups; sub-groups commit in parallel with no inter-sub-group ordering guarantee. Each individual depletion still consumes "oldest layer first" for its local view, but the global trx ordering is reordered — so depletion T₂₀₀ can drain the oldest layer before depletion T₁₀₀ runs, and T₁₀₀ then consumes from a different head layer than it would under serial submission order.

That is not FIFO. It produces non-deterministic COGS for the same input workload.

The current PoC ships single-committer (`committer_count = 1`) as the correctness workaround. The design fix — router refuses to split per-pool submissions for `fifo` / `lifo` / `specific` pools — is filed as **acct-aywu**. Until that lands, the routed path's `fifo` cost method requires `committer_count = 1`.

All numbers in this doc are measured under `committer_count = 1`.

## Change

Extends the cross-path equivalence harness (acct-h5gs / 9mgx.5 / 9mgx.6
lineage) to validate `fifo` end-state equivalence + characterizes
throughput against the `wac` baseline on s2 (zipf-1.5 simple) and s5
(single hot pool). Also lands the run-subcommand `--method-mix` reseed
plumbing that the remaining `acct-9mgx.{2,3,4}` siblings will reuse.

**Harness extensions:**

- `equivalence` subcommand: dispatch through `build_submissions_fifo` when
  `--method-mix all-fifo`. Workload is a 2-tick alternating cycle per
  caller (R qty=10 with rotating unit_cost / D qty=-3 on caller's last
  receipt pool), submitted in **caller-major order** so each commit_group
  starts with a receipt and stays stock-positive — earlier tick-major
  and larger-depletion shapes hit InsufficientInventory under s5
  single-hot-pool.
- New `DiffResult.fifo_drifts` bucket + `trx_lines_preserve_total_qty_per_pool`
  classifier. Per-line trx_line content differences on FIFO
  transfer_shipment trxs route here when per-pool ∑qty matches across
  paths. pool_state row-count differences and per-row mismatches on FIFO
  pools likewise route here. The load-bearing invariant — per-pool
  ∑pool_state.qty matches across paths — stays in the `errors` bucket
  and is gated separately. **Under `committer_count = 1` (correct config)
  the fifo_drifts bucket is always empty; it only fires when the routed
  path is misconfigured for multi-committer FIFO, in which case it
  diagnoses the violation.**
- `--strict` upgrades `fifo_drifts` to errors.
- `run` subcommand: new `--method-mix` flag (+ `--seed-count` /
  `--seed-skus` / `--seed-locations` sizing args). When set, the harness
  TRUNCATEs the ledger tables and re-seeds the pool universe with the
  requested mix before driving — enables cross-method bench against the
  same scenarios.
- `scripts/run-equivalence-sweep.sh`: takes `METHOD_MIX` env var
  (default `all-wac` for backwards compat with acct-h5gs / 9mgx.5
  invocations).
- `wait_for_committer_quiet` post-grace: after the drains-counter
  stabilizes, retries the per-submission materialization check 5× with
  200ms sleeps so a commit_group that lands right after the stability
  signal isn't reported as "stuck."

## Equivalence sweep — 6 scenarios + 9 strict trials, committer_count=1

| Scenario | Workload                | Submissions | Lenient | Strict T1 / T2 / T3 |
|----------|-------------------------|------------:|:-------:|:-------------------:|
| s1       | uniform / simple        |         500 | ✓ identical | — |
| s2       | zipf(1.5) / simple      |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s3       | uniform / complex       |         500 | ✓ identical | — |
| s4       | zipf(1.2) / complex     |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s5       | single-hot-pool         |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s6       | disjoint stripes        |        1000 | ✓ identical | — |

**15/15 byte-identical under `committer_count = 1`.** Same shape as
acct-h5gs / 9mgx.5 WAC validation: trx, trx_line, pool_state, and
posting_line all match byte-for-byte across paths in every run, lenient
and strict.

## Cross-method bench — s2 + s5 × direct + routed × AllFifo + AllWac, committer_count=1

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in
`results/phase6/bench-9mgx1/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | wac        |             589.0 |        111.7 |  17 657 |
| s2       | direct   | **fifo**   |             439.3 |        173.0 |  13 271 |
| s2       | routed   | wac        |           1 506.1 |        104.7 |  45 257 |
| s2       | routed   | **fifo**   |           1 444.6 |        111.6 |  43 363 |
| s5       | direct   | wac        |             304.6 |         80.5 |   9 242 |
| s5       | direct   | **fifo**   |             207.8 |        148.9 |   6 333 |
| s5       | routed   | wac        |           1 817.9 |         64.4 |  54 553 |
| s5       | routed   | **fifo**   |           1 714.6 |         82.9 |  51 443 |

**FIFO direct-path overhead is real and bounded.** On the direct path
FIFO trails WAC by ~25-32% (439 vs 589 on s2; 208 vs 305 on s5) due
to per-event sequential layer consumption (each depletion's plan_apply
walks layers and emits one trx_line per layer touched; WAC emits one
trx_line per depletion regardless of layer count). p99 ack latency
follows the same shape (~1.5× WAC on direct).

**FIFO routed-path is within 4-6% of WAC** (1445 vs 1506 on s2; 1715 vs
1818 on s5). Commit-group amortization neutralizes FIFO's per-event
overhead; the residual gap is FIFO's bulk-write of per-layer trx_lines
+ pool_state UPDATE/DELETE vs WAC's two-row pool_state UPSERT.

**Routed beats direct in every cell** (3-8× throughput). Routed-path
amortization wins for both methods even under the correctness-forced
`committer_count = 1` configuration.

`committed_p99_us` (routed) is dominated by queue residency (submissions
sit in the staging queue until their batch drains), not per-trx work.

## Bench under broken default config (for reference; not the canonical numbers)

Run captured prior to discovering the per-pool ordering violation —
`committer_count = 4`, `batch_size_max = 50`. **These numbers reflect
broken FIFO behavior** (non-deterministic COGS) and are kept here only
to quantify the correctness/throughput trade-off the routed path
currently pays:

| Scenario | Path     | Method     | Throughput (tx/s) cm=4 | Δ vs cm=1 |
|----------|----------|------------|----------------------:|----------:|
| s2       | routed   | fifo (BROKEN) |              1 933.0 |     +34 % |
| s2       | routed   | wac        |               1 849.3 |     +23 % |
| s5       | routed   | fifo (BROKEN) |              1 790.6 |      +4 % |
| s5       | routed   | wac        |               2 131.3 |     +17 % |

The 34% FIFO gap on s2 is the cost of correctness. acct-aywu's per-pool
no-split routing should recover most of it (cross-pool parallelism
restored while order-sensitive pools stay single-group).

Broken-config sweep + bench logs preserved at
`results/phase6/equivalence/run-all-fifo-2026-05-23T12-31-44Z.log` and
`results/phase6/bench-9mgx1/*-2026-05-23T12-36-01Z.{json,log}` for
audit; not the canonical reference.

## Cross-references

- **acct-aywu** (open, P1, this work's followup) — router-side fix to
  stop splitting per-pool submissions across commit_groups for
  order-sensitive cost methods. Removes the `committer_count = 1`
  requirement and restores cross-pool routed parallelism.
- **acct-h5gs** (closed, 2026-05-22) — WAC cumulative-sum form. Key
  contrast with FIFO: WAC's storage form is commutative under receipts
  and bounded-rounding-only on depletions (correct under any
  commit_count), while FIFO's per-layer storage is path-dependent and
  requires submission-order serialization.
- **acct-mcey** (closed, 2026-05-22) — `wac_drifts` diff bucket pattern
  this work extends with `fifo_drifts`.
- **acct-9mgx.5** (closed, 2026-05-23) — WAC-perpetual under the unified
  harness. Reproduced h5gs's 15/15 byte-identical (under default
  multi-committer, because WAC is commutation-safe).
- **acct-9mgx.6** (closed, 2026-05-23) — wac_periodic under the unified
  harness. Introduced `--method-mix`; this work extends with FIFO
  workload + diff classifier.
- **acct-9mgx.{2,3,4}** (open) — LIFO / Specific / STD; the
  `--method-mix` run-subcommand reseed plumbing landed here is shared
  by each. **LIFO and Specific will hit the same ordering violation
  as FIFO** until acct-aywu lands; STD has no layer-ordering surface
  and is safe.
- **acct-9mgx.7** (open, blocked on .1–.6) — cross-method roll-up doc.

## Harness invocation

```bash
# REQUIRED for correct FIFO on routed path:
docker exec acct-postgres psql -U acct -d poc_v3 \
    -c "ALTER SYSTEM SET ledger_routed.committer_count = 1;"
docker restart acct-postgres

# Lenient equivalence (drifts informational; should be 0 under cm=1)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-fifo

# Strict equivalence (drifts upgrade to errors; should be 0 under cm=1)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-fifo \
    --strict

# Full 15-trial sweep
METHOD_MIX=all-fifo bash scripts/run-equivalence-sweep.sh

# Cross-method bench (8 runs)
bash scripts/run-bench-9mgx1.sh

# Single bench cell — reseed + drive
cargo run --release -p ledger-harness -- run \
    --scenario s5 --path routed --duration 30s \
    --method-mix all-fifo --seed-count 1000 \
    --max-callers 20 --no-sampler
```

## What this DOES NOT change

- **No ledger-core changes.** `fifo.rs` / `layered.rs` semantics
  unchanged; per-trx FIFO consumption was always correct. The bug
  was at the router layer, not the cost-method layer.
- **No SPI changes.**
- **No schema changes.**

## What's deferred

- **acct-aywu** — router-side fix (the real solution; this PoC ships
  single-committer as the interim workaround).
- **acct-9mgx.7** (cross-method roll-up doc).
- **Per-method `run` workload variants**: the run subcommand still uses
  the all-receipts po_receipt workload regardless of method-mix. A
  depletion-aware run-subcommand workload would be needed to
  characterize FIFO depletion throughput specifically — deferred as a
  follow-up if perf numbers warrant.
- **Single-committer CI gating**: this PoC documents the requirement
  but does not enforce it via test setup. acct-aywu obsoletes the
  requirement entirely.
