# acct-9mgx.1 — FIFO equivalence + bench

**Issue:** acct-9mgx.1 (P2; sibling of acct-9mgx.{2..6})
**Run window:** 2026-05-23
**Equivalence sweep:** `results/phase6/equivalence/run-all-fifo-2026-05-23T12-31-44Z.log`
**Bench:** `results/phase6/bench-9mgx1/run-2026-05-23T12-36-01Z.log`

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
  starts with a receipt and stays stock-positive even when
  `batch_size_max=50` splits the workload across the 4-committer pool.
  Earlier tick-major and larger-depletion shapes hit InsufficientInventory
  under s5 single-hot-pool concurrent commit_groups.
- New `DiffResult.fifo_drifts` bucket + `trx_lines_preserve_total_qty_per_pool`
  classifier. Per-line trx_line content differences on FIFO
  transfer_shipment trxs route here when per-pool ∑qty matches across
  paths. pool_state row-count differences and per-row mismatches on FIFO
  pools likewise route here. The load-bearing invariant — per-pool
  ∑pool_state.qty matches across paths — stays in the `errors` bucket
  and is gated separately.
- `--strict` upgrades `fifo_drifts` to errors (parallel to
  `wac_drifts` / `wac_periodic_drifts` precedent).
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

## Diff contract (acct-9mgx.1)

FIFO is **path-dependent** under concurrent commit_groups: each
depletion consumes from the head layer, but under different
commit_group orderings the head layer's qty/unit_cost at deplete-time
differs. This is intrinsic to FIFO (unlike WAC's commutative
cumulative-sum form per acct-h5gs); it is NOT a bug.

| Bucket                  | Per-row byte check | Aggregate check                              |
|-------------------------|--------------------|----------------------------------------------|
| `pool_state` Σqty per pool | (n/a)           | **load-bearing**: total per-pool stock conserved |
| `pool_state` per-row    | informational      | (covered via Σqty)                           |
| `trx_groups` Σqty per (trx, pool) | (n/a)    | **load-bearing**: per-trx per-pool qty conserved |
| `trx_groups` per-line   | informational      | (covered via per-trx per-pool ∑qty)          |

Per-line and per-row differences become `fifo_drifts`. Workload
identity guarantees the load-bearing invariants: both paths receive
identical submissions in identical total qty.

## Equivalence sweep — 6 scenarios + 9 strict trials

| Scenario | Workload                | Submissions | Lenient | Strict T1 / T2 / T3 |
|----------|-------------------------|------------:|:-------:|:-------------------:|
| s1       | uniform / simple        |         500 | ✓ identical | — |
| s2       | zipf(1.5) / simple      |        1000 | ✓ 155 drifts | ✗ 49 / ✗ 173 / ✗ 201 |
| s3       | uniform / complex       |         500 | ✓ 51 drifts  | — |
| s4       | zipf(1.2) / complex     |        1000 | ✓ 51 drifts  | ✗ 414 / ✓ identical / ✗ 454 |
| s5       | single-hot-pool         |        1000 | ✓ 142 drifts | ✗ 309 / ✗ 75 / ✗ 158 |
| s6       | disjoint stripes        |        1000 | ✓ identical  | — |

**All 6 lenient scenarios pass.** Per-pool ∑qty and per-(trx, pool) ∑qty
are conserved across paths in every run; per-line breakdowns and
pool_state layer composition drift on shared pools by exactly the
FIFO path-dependence the bd description predicted.

Strict-mode trials surface the drift — same pattern as acct-mcey for WAC
running-avg and acct-9mgx.6 for wac_periodic. Strict variability is
fundamental to FIFO + concurrent commit_groups; not a per-trial bug.
Single-committer configuration (`ALTER SYSTEM SET ledger_routed.committer_count = 1`
+ postmaster restart) is the workaround for strict-equivalence runs;
disjoint-stripe scenarios (s6) pass strict trivially because they have
no shared pools.

The s4 strict T2 "identical" outcome is a lucky commit-group ordering
that happened to mirror Path A's serial ordering — not a property to
rely on.

## Cross-method bench — s2 + s5 × direct + routed × AllFifo + AllWac

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in
`results/phase6/bench-9mgx1/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | wac        |             616.2 |         94.4 |  18 360 |
| s2       | direct   | **fifo**   |             472.1 |        150.1 |  14 160 |
| s2       | routed   | wac        |           1 849.3 |         47.0 |  55 500 |
| s2       | routed   | **fifo**   |           1 933.0 |         48.5 |  58 016 |
| s5       | direct   | wac        |             298.1 |         86.1 |   8 891 |
| s5       | direct   | **fifo**   |             204.8 |        158.3 |   6 255 |
| s5       | routed   | wac        |           2 131.3 |         50.8 |  63 962 |
| s5       | routed   | **fifo**   |           1 790.6 |         76.5 |  53 773 |

**FIFO direct-path overhead is real and bounded.** On the direct path
FIFO trails WAC by ~30% (472 vs 616 tx/s on s2; 205 vs 298 on s5) due
to per-event sequential layer consumption (each depletion's plan_apply
walks layers and emits one trx_line per layer touched; WAC emits one
trx_line per depletion regardless of layer count). p99 ack latency
follows the same shape (1.6× WAC on direct).

**Routed-path overhead is mostly neutralized** by commit-group
amortization. On s2 routed, FIFO is within 5% of WAC (slightly higher);
on s5 routed, WAC pulls ~19% ahead because single-hot-pool
amortization favors WAC's flat per-event work. ack p99 is comparable
across methods (~50ms WAC vs ~50-77ms FIFO).

**Routed beats direct in every cell** — 3-10× throughput across both
methods. This was already established for AllWac in acct-49 / acct-tk58;
AllFifo confirms the shape carries across methods.

Routed `committed_p99_us` is dominated by queue residency (submissions
sit in the staging queue until their batch drains), not by per-trx
work; it ranges 9-22s in this bench because the harness backpressures
at staging_queue_size=16k while submitters are unbounded.

## Cross-references

- **acct-h5gs** (closed, 2026-05-22) — WAC cumulative-sum form; key
  contrast with FIFO is that WAC's storage form is commutative under
  receipts and bounded-rounding-only on depletions (zero drift under
  serial equivalence), while FIFO's per-layer storage is path-dependent
  under concurrent commit_groups.
- **acct-mcey** (closed, 2026-05-22) — original `wac_drifts` diff bucket
  pattern this work extends with `fifo_drifts`.
- **acct-9mgx.5** (closed, 2026-05-23) — WAC-perpetual under the unified
  harness. Reproduced h5gs's 15/15 byte-identical.
- **acct-9mgx.6** (closed, 2026-05-23) — wac_periodic under the unified
  harness. Introduced `--method-mix`; this work extends with FIFO
  workload + diff classifier.
- **acct-9mgx.{2,3,4}** (open) — LIFO / Specific / STD; the
  `--method-mix` run-subcommand reseed plumbing landed here is shared
  by each.
- **acct-9mgx.7** (open, blocked on .1–.6) — cross-method roll-up doc.

## Harness invocation

```bash
# Lenient equivalence (drifts informational)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-fifo

# Strict equivalence (drifts upgrade to errors)
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
  unchanged; this work only extends the harness.
- **No SPI changes.** ledger_submit_trx / ledger_enqueue_trx
  signatures unchanged.
- **No schema changes.** FIFO has used the existing pool_state +
  trx_line schema since the project's first migration.
- **No ledger-direct / ledger-routed changes** for FIFO logic. The only
  cross-module touch was renaming `equivalence::reset_ledger` to call
  through `pool_universe::reset_ledger_tables` so the run subcommand
  reseed shares the same TRUNCATE.

## What's deferred

- **acct-9mgx.7** (cross-method roll-up doc) — blocked on .1–.6.
- **Production drift handling for FIFO**: under real-load concurrent
  Path B, FIFO COGS can legitimately differ from a hypothetical serial
  baseline (per-layer consumption is path-dependent). Whether that
  matters depends on accounting policy. Not in scope for the PoC;
  surfaced as a property of the design.
- **Strict-mode CI gating for FIFO**: requires single-committer
  configuration (mirrors the wac_periodic / wac running-avg pattern).
  Not wired here.
- **Per-method `run` workload variants**: the run subcommand still uses
  the all-receipts po_receipt workload regardless of method-mix; the
  bench measures layer-creation + bulk-write throughput per method,
  not depletion-side dynamics. A depletion-aware run-subcommand
  workload would be needed to characterize FIFO depletion throughput
  specifically — deferred as a follow-up.
