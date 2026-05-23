# acct-9mgx.1 — FIFO equivalence + bench

**Issue:** acct-9mgx.1 (P2; sibling of acct-9mgx.{2..6})
**Run window:** 2026-05-23
**Equivalence sweep (canonical, committer_count=1, with acct-aywu):** `results/phase6/equivalence/run-all-fifo-2026-05-23T14-49-38Z.log`
**Bench (canonical, committer_count=1, with acct-aywu):** `results/phase6/bench-9mgx1/run-2026-05-23T15-03-10Z.log`
**Pre-aywu reference (cm=1):** `results/phase6/equivalence/run-all-fifo-2026-05-23T13-04-03Z.log` + `bench-9mgx1/run-2026-05-23T13-06-47Z.log`

## Routed FIFO correctness status

`committer_count = 1` produces byte-identical FIFO across Path A and Path B (15/15 sweep below). This is the gold-standard correctness baseline.

`committer_count = 4` (default) is broken in two distinct ways:

1. **Intra-window split** — when one router scan finds more than `batch_size_max` submissions for the same FIFO pool, it splits them across multiple commit_groups that commit in parallel with no inter-sub-group ordering. **Fixed by acct-aywu**: router learns each pool's cost method and emits order-sensitive groups (`fifo` / `lifo` / `specific`) whole, regardless of `batch_size_max`. Verified via the new `ledger_routed_router_order_sensitive_groups_total()` counter.

2. **Inter-window race** — across multiple router ticks, two windows can emit independent commit_groups for the same FIFO pool. Those groups race on `trx_line UNIQUE(pool_id, trx_seq)`; pristine-replay excludes the loser, silently dropping one submission per race. Empirically: cm=4 s4 sweep with aywu in place still produces 999/1000 trx (vs 1000/1000 under cm=1). **NOT fixed by aywu** — scoped to **acct-tm09** (per-pool sequence numbers + happens-before DAG).

Until acct-tm09 lands, default-GUC routed FIFO is still incorrect. cm=1 remains the required production config for the FIFO cost method.

All numbers in this doc are measured under `committer_count = 1` with acct-aywu in place.

**This cm=1 sweep is the gold-standard FIFO correctness baseline.** acct-tm09
(the router-side fix that will lift the cm=1 requirement) pins its acceptance
to "re-run this 15-trial sweep under default GUCs and verify byte-identical
to the cm=1 reference at
`results/phase6/equivalence/run-all-fifo-2026-05-23T14-49-38Z.log`." Per-run
equivalence-vs-Path-A is the per-run check; equivalence-vs-cm=1-routed is
the regression check that catches any cm-dependent divergence the Path-A
comparison would miss.

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

## Cross-method bench — s2 + s5 × direct + routed × AllFifo + AllWac, committer_count=1 (post-aywu)

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in
`results/phase6/bench-9mgx1/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | wac        |             617.5 |         95.0 |  18 485 |
| s2       | direct   | **fifo**   |             451.4 |        162.9 |  13 622 |
| s2       | routed   | wac        |           1 459.2 |        108.1 |  43 829 |
| s2       | routed   | **fifo**   |           1 583.2 |        134.6 |  47 570 |
| s5       | direct   | wac        |             294.7 |         95.3 |   8 832 |
| s5       | direct   | **fifo**   |             194.8 |        175.9 |   5 966 |
| s5       | routed   | wac        |           1 732.0 |         66.2 |  52 047 |
| s5       | routed   | **fifo**   |           2 037.7 |        241.3 |  61 138 |

**FIFO direct-path overhead is real and bounded.** On the direct path
FIFO trails WAC by ~27-34% (451 vs 618 on s2; 195 vs 295 on s5) due to
per-event sequential layer consumption (each depletion's plan_apply
walks layers and emits one trx_line per layer touched; WAC emits one
trx_line per depletion regardless of layer count). p99 ack latency
follows the same shape (~1.7-1.9× WAC on direct).

**Routed FIFO now BEATS routed WAC** (1583 vs 1459 on s2; 2038 vs 1732
on s5). The aywu no-split rule packs order-sensitive pools into much
larger commit_groups (s5 routed fifo `commit_group_avg = 123.8` vs
WAC's `37.96`) which amortizes the per-batch fixed cost more
aggressively. The cm=1 single-committer config means WAC's potential
cross-pool parallelism is unavailable to either method, so the larger
average commit_group is pure win for FIFO. p99 ack latency goes up
correspondingly (241ms on s5 routed fifo vs 66ms WAC) — that's the
straightforward latency cost of preserving submission order: bigger
groups take longer to pipeline. Under cm=4 + acct-tm09, WAC would
likely reclaim its throughput lead via cross-pool parallelism.

**Routed beats direct in every cell** (3-10× throughput).

`committed_p99_us` (routed) is dominated by queue residency (submissions
sit in the staging queue until their batch drains), not per-trx work.

### Pre-aywu cm=1 numbers (for delta reference)

| Scenario | Path     | Method     | tx/s pre-aywu | tx/s post-aywu | Δ        |
|----------|----------|------------|--------------:|---------------:|---------:|
| s2       | routed   | wac        |       1 506.1 |        1 459.2 | −3 %     |
| s2       | routed   | **fifo**   |       1 444.6 |        1 583.2 | **+10 %**|
| s5       | routed   | wac        |       1 817.9 |        1 732.0 | −5 %     |
| s5       | routed   | **fifo**   |       1 714.6 |        2 037.7 | **+19 %**|

WAC deltas are within run-to-run noise (WAC pools still split at
batch_size_max=50; aywu does not change WAC behavior). FIFO gains track
the predicted no-split throughput win, larger on the hotter pool (s5).

## Bench under broken default config (for reference; not the canonical numbers)

Run captured prior to discovering the per-pool ordering violation —
`committer_count = 4`, `batch_size_max = 50`. **These numbers reflect
broken FIFO behavior** (non-deterministic COGS) and are kept here only
to quantify the correctness/throughput trade-off the routed path
currently pays:

| Scenario | Path     | Method     | Throughput (tx/s) cm=4 | Δ vs cm=1 post-aywu |
|----------|----------|------------|----------------------:|--------------------:|
| s2       | routed   | fifo (BROKEN) |              1 933.0 |              +22 % |
| s2       | routed   | wac        |               1 849.3 |              +27 % |
| s5       | routed   | fifo (BROKEN) |              1 790.6 |              −12 % |
| s5       | routed   | wac        |               2 131.3 |              +23 % |

acct-aywu has CLOSED the cm=1-vs-broken-cm=4 throughput gap for FIFO
on s5 (post-aywu cm=1 fifo at 2037 tx/s actually BEATS the cm=4 broken
number of 1791 tx/s). s2 still trails the broken cm=4 number by 22%,
which is the cross-pool parallelism that cm=4 unlocks but cm=1 can't
use — recovered by acct-tm09 once it lands.

Broken-config sweep + bench logs preserved at
`results/phase6/equivalence/run-all-fifo-2026-05-23T12-31-44Z.log` and
`results/phase6/bench-9mgx1/*-2026-05-23T12-36-01Z.{json,log}` for
audit; not the canonical reference.

## Cross-references

- **acct-aywu** (closed 2026-05-23, this work's followup) — router-side
  fix to stop splitting per-pool submissions across commit_groups for
  order-sensitive cost methods (intra-window). LANDED. Closes the
  intra-window split but does not address the inter-window race;
  cm=1 is still required until acct-tm09 lands. Exposed
  `ledger_routed_router_order_sensitive_groups_total()` as the
  attribution counter.
- **acct-tm09** (open, P1) — inter-window per-pool sequence numbers
  and happens-before DAG. The remaining barrier to default-GUC routed
  FIFO correctness. After tm09, the cm=1 requirement is fully lifted
  and routed FIFO regains cross-pool parallelism.
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
# REQUIRED for correct FIFO on routed path (until acct-tm09 lands):
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

- **acct-tm09** — router-side fix for inter-window per-pool ordering;
  the last barrier to default-GUC routed FIFO correctness. The cm=1
  workaround stays in place until tm09 lands.
- **acct-9mgx.7** (cross-method roll-up doc).
- **Per-method `run` workload variants**: the run subcommand still uses
  the all-receipts po_receipt workload regardless of method-mix. A
  depletion-aware run-subcommand workload would be needed to
  characterize FIFO depletion throughput specifically — deferred as a
  follow-up if perf numbers warrant.
- **Single-committer CI gating**: this PoC documents the requirement
  but does not enforce it via test setup. acct-tm09 obsoletes the
  requirement entirely.
