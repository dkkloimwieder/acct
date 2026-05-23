# acct-9mgx.1 — FIFO equivalence + bench

**Issue:** acct-9mgx.1 (P2; sibling of acct-9mgx.{2..6})
**Run window:** 2026-05-23
**Equivalence sweep (canonical, default GUCs, with acct-aywu + acct-tm09):** `results/phase6/equivalence/run-all-fifo-2026-05-23T15-43-46Z.log`
**Bench (canonical, default GUCs, with acct-aywu + acct-tm09):** `results/phase6/bench-9mgx1/run-2026-05-23T15-46-50Z.log`
**Prior cm=1 reference (aywu only):** `results/phase6/equivalence/run-all-fifo-2026-05-23T14-49-38Z.log` + `bench-9mgx1/run-2026-05-23T15-03-10Z.log`
**Pre-aywu reference (cm=1):** `results/phase6/equivalence/run-all-fifo-2026-05-23T13-04-03Z.log` + `bench-9mgx1/run-2026-05-23T13-06-47Z.log`

## Routed FIFO correctness status

**Default GUCs (`committer_count = 4`, `batch_size_max = 50`) now produce byte-identical FIFO across Path A and Path B (15/15 sweep below).** The cm=1 workaround is no longer required after acct-aywu + acct-tm09 landed.

Two distinct correctness gaps were closed:

1. **Intra-window split** — when one router scan finds more than `batch_size_max` submissions for the same FIFO pool, it splits them across multiple commit_groups that commit in parallel with no inter-sub-group ordering. **Fixed by acct-aywu** (2026-05-23): router learns each pool's cost method and emits order-sensitive groups (`fifo` / `lifo` / `specific`) whole, regardless of `batch_size_max`. Verified via the `ledger_routed_router_order_sensitive_groups_total()` counter.

2. **Inter-window race** — across multiple router ticks, two windows can emit independent commit_groups for the same FIFO pool. Those groups race on `trx_line UNIQUE(pool_id, trx_seq)`; pristine-replay excludes the loser, silently dropping one submission per race. **Fixed by acct-tm09** (2026-05-23): per-pool sequence numbers in a 16384-slot shmem table + spin-sleep predecessor wait in the committer. Verified via the `ledger_routed_committer_tm09_waits_total()` + `_tm09_wait_timeouts_total()` counters.

All numbers in this doc are measured under DEFAULT GUCs with both acct-aywu and acct-tm09 in place.

**This cm=4 sweep is the gold-standard FIFO correctness baseline.** Future
routed FIFO work pins acceptance to "re-run this 15-trial sweep under default
GUCs and verify byte-identical to the cm=4 reference at
`results/phase6/equivalence/run-all-fifo-2026-05-23T15-43-46Z.log`."

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

## Cross-method bench — s2 + s5 × direct + routed × AllFifo + AllWac, default GUCs (post-tm09)

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in
`results/phase6/bench-9mgx1/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | wac        |             544.8 |        128.2 |  16 316 |
| s2       | direct   | **fifo**   |             407.5 |        213.0 |  12 352 |
| s2       | routed   | wac        |           1 759.8 |         50.0 |  52 799 |
| s2       | routed   | **fifo**   |           1 189.2 |         58.6 |  35 680 |
| s5       | direct   | wac        |             293.4 |        106.0 |   8 730 |
| s5       | direct   | **fifo**   |             197.3 |        173.5 |   6 026 |
| s5       | routed   | wac        |           1 853.5 |         58.4 |  55 618 |
| s5       | routed   | **fifo**   |           2 266.6 |        216.5 |  68 149 |

**FIFO direct-path overhead is real and bounded.** Same shape as
pre-tm09: FIFO trails WAC by ~25-34% due to per-event sequential
layer consumption + per-layer trx_line emission. Direct-path numbers
are unaffected by tm09 (the routing-layer fix has no impact on Path A).

**Routed-path: same-pool vs cross-pool tradeoff is clear.**
- **s2 (zipf-1.5, dispersed)**: WAC's cross-pool parallelism wins —
  routed WAC at 1760 tx/s vs routed FIFO at 1189 tx/s (FIFO 32%
  slower). FIFO pays the predecessor-wait cost on the modestly hot
  pools without WAC's full parallelism benefit. `committer_tm09_waits_total`
  fires for every non-first commit_group on the same pool.
- **s5 (single hot pool)**: FIFO's giant amortized commit_groups win —
  routed FIFO at 2267 tx/s vs routed WAC at 1854 tx/s (FIFO 22%
  faster). With all submissions targeting one pool, both methods
  effectively serialize, but FIFO's commit_group_avg (140) drowns
  WAC's (38) in per-batch amortization.

The two scenarios bracket the regime: tm09 makes routed FIFO
production-viable under default GUCs across the full spectrum, with
the speed/parallelism trade-off determined by workload pool overlap.

**Routed beats direct in every cell** (3-12× throughput).

`committed_p99_us` (routed) is dominated by queue residency, not per-trx work.

### Delta tracking across acct-9mgx.1 / aywu / tm09

| Scenario | Path     | Method     | tx/s pre-aywu (cm=1) | tx/s aywu (cm=1) | tx/s tm09 (cm=4) |
|----------|----------|------------|---------------------:|-----------------:|-----------------:|
| s2       | routed   | wac        |              1 506.1 |          1 459.2 |          1 759.8 |
| s2       | routed   | **fifo**   |              1 444.6 |          1 583.2 |          1 189.2 |
| s5       | routed   | wac        |              1 817.9 |          1 732.0 |          1 853.5 |
| s5       | routed   | **fifo**   |              1 714.6 |          2 037.7 |          2 266.6 |

WAC gains under tm09 come from cm=4's cross-pool parallelism (WAC pools
don't gate on predecessor commits). FIFO gains on s5 come from
sustaining the no-split amortization across multiple router windows.
FIFO regression on s2 is the predecessor-wait cost on dispersed
workloads — the WAIT IS the price of correctness. Per acct-xjhq
(CV+broadcast follow-up) the wait latency can be reduced; the
spin-sleep here is the simplest correct implementation.

## Broken-config reference (pre-fix audit)

Pre-aywu, pre-tm09 numbers under default cm=4 produced non-deterministic
COGS and silently dropped trx via inter-window UNIQUE_VIOLATION race.
Preserved for audit only:

| Scenario | Path     | Method     | Throughput (tx/s) BROKEN cm=4 | Notes |
|----------|----------|------------|------------------------------:|-------|
| s2       | routed   | fifo (BROKEN) |                      1 933.0 | reordered COGS |
| s2       | routed   | wac        |                       1 849.3 | (WAC commutative; correct) |
| s5       | routed   | fifo (BROKEN) |                      1 790.6 | reordered + 1 trx dropped |
| s5       | routed   | wac        |                       2 131.3 | (WAC commutative; correct) |

Broken-config sweep + bench logs preserved at
`results/phase6/equivalence/run-all-fifo-2026-05-23T12-31-44Z.log` and
`results/phase6/bench-9mgx1/*-2026-05-23T12-36-01Z.{json,log}` for
audit; not the canonical reference.

Post-tm09 correct numbers under default cm=4 (canonical, see above)
trade-off across scenarios:
- s2 routed fifo: 1189 tx/s (vs broken 1933) — 38% throughput cost for
  correctness. Predecessor-wait dominates on dispersed pools where each
  group has few siblings.
- s5 routed fifo: 2267 tx/s (vs broken 1791) — actually BEATS broken
  number by 27%. Single hot pool's amortization from no-split (aywu)
  dominates predecessor-wait cost (tm09); same-pool serialization
  matches what the WAIT enforces anyway.

## Cross-references

- **acct-aywu** (closed 2026-05-23, this work's followup) — router-side
  fix to stop splitting per-pool submissions across commit_groups for
  order-sensitive cost methods (intra-window). LANDED. Exposed
  `ledger_routed_router_order_sensitive_groups_total()` as the
  attribution counter.
- **acct-tm09** (closed 2026-05-23, the other half of the routed-FIFO
  correctness story) — per-pool sequence numbers in a 16384-slot
  shmem table (`PoolSeqTable`); spin-sleep committer wait on
  predecessor commits before pool_lock acquisition. Closes the
  inter-window race; default cm=4 GUCs now correct. Exposed
  `ledger_routed_committer_tm09_waits_total()` /
  `_tm09_wait_timeouts_total()` / `_tm09_wait_ns_total()`.
- **acct-xjhq** (open, P2) — replace tm09's spin-sleep with
  PgLwLock + ConditionVariable broadcast on commit. Reduces CPU%
  in committer waits and unblocks faster. Spin-sleep is the
  starter implementation; CV+broadcast is the right design once
  the surface is stable.
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
# Default GUCs (committer_count = 4) are now correct for FIFO after
# acct-aywu + acct-tm09 landed. The cm=1 workaround is no longer
# required.

# Lenient equivalence (drifts informational; should be 0)
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

- **acct-xjhq** — replace tm09 spin-sleep with PgLwLock + CV broadcast.
  Reduces CPU% and tail latency on long predecessor waits.
- **acct-e5fz** — re-evaluate `batch_size_max` default + reframe as
  time-window primary cap. Now unblocked (was waiting on aywu).
- **acct-9mgx.7** (cross-method roll-up doc).
- **Per-method `run` workload variants**: the run subcommand still uses
  the all-receipts po_receipt workload regardless of method-mix. A
  depletion-aware run-subcommand workload would be needed to
  characterize FIFO depletion throughput specifically — deferred as a
  follow-up if perf numbers warrant.
- **acct-5ppc / acct-iu6u / acct-p4iw** — v3 terminology cleanup
  (rename `superbatch` -> `commit_group`, `envelope` -> `submission`,
  strip v21 references from comments). Code passes tests; the rename
  is editorial.
