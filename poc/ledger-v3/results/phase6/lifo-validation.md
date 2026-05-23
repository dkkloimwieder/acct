# acct-9mgx.2 — LIFO equivalence + bench

**Issue:** acct-9mgx.2 (P2; sibling of acct-9mgx.{1,3,4,5,6})
**Run window:** 2026-05-23
**Equivalence sweep:** `results/phase6/equivalence/run-all-lifo-2026-05-23T23-09-27Z.log`
**Bench:** `results/phase6/bench-9mgx2/run-2026-05-23T23-12-36Z.log`

## Routed LIFO correctness status

**Default GUCs (`committer_count = 4`, `batch_size_max = 50`) produce byte-identical LIFO across Path A and Path B (15/15 sweep below).** No new infrastructure was required — LIFO inherits the correctness fixes that landed for FIFO.

LIFO is order-sensitive: depletions consume the newest layer first. The same two correctness gaps that bit FIFO under default cm=4 apply identically to LIFO:

1. **Intra-window split** — order-sensitive commit_groups must emit whole, regardless of `batch_size_max`. **Already covered by acct-aywu** (2026-05-23): the router's `is_order_sensitive_method` classifier returns `true` for `"lifo"` (alongside `"fifo"` and `"specific"`), so LIFO groups bypass the chunking path.

2. **Inter-window race** — across router ticks, two commit_groups for the same LIFO pool must commit in router-emit order. **Already covered by acct-tm09** (2026-05-23): per-pool sequence numbers in the 16384-slot shmem table + committer's spin-sleep predecessor wait apply uniformly to all order-sensitive methods.

`pool_is_order_sensitive` is queried via the same `pool.method` lookup path; LIFO pools join the same sequence-stamping and predecessor-wait flow as FIFO. Both correctness fixes were validated by unit tests that explicitly cover `"lifo"` (`router.rs:1309`).

## Change

Extends the cross-path equivalence harness (acct-h5gs / 9mgx.5 / 9mgx.6 / 9mgx.1 / 9mgx.4 lineage) to validate `lifo` end-state equivalence + characterizes throughput against the `wac` baseline on s2 (zipf-1.5 simple) and s5 (single hot pool).

**Harness extensions:**

- `MethodMix::AllLifo` variant added to `cli.rs`.
- `pool_universe::method_for_index` extended with the `"lifo"` branch for `AllLifo`.
- `equivalence` subcommand: dispatch through `build_submissions_lifo` when `--method-mix all-lifo`. Workload is a 2-tick alternating cycle per caller (R qty=10 with rotating unit_cost / D qty=-3 on caller's last receipt pool), submitted in **caller-major order** so each commit_group starts with a receipt and stays stock-positive.
- New `DiffResult.lifo_drifts` bucket + `trx_lines_preserve_total_qty_per_pool_lifo` classifier. Per-line trx_line content differences on LIFO transfer_shipment trxs would route here when per-pool ∑qty matches across paths. pool_state row-count differences and per-row mismatches on LIFO pools likewise. The load-bearing invariant — per-pool ∑pool_state.qty matches across paths — stays in the `errors` bucket and is gated separately. **Under default cm=4 with aywu + tm09, the bucket never fires** (all 15 trials byte-identical) — it stands as defensive classification for any future regression that lets LIFO layer composition diverge.
- `--strict` upgrades `lifo_drifts` to `errors` symmetric to `fifo_drifts`.
- New `scripts/run-bench-9mgx2.sh`: 8 cells = s2/s5 × direct/routed × all-lifo/all-wac.

## Equivalence sweep — 6 scenarios + 9 strict trials, default committer_count=4

| Scenario | Workload                | Submissions | Lenient | Strict T1 / T2 / T3 |
|----------|-------------------------|------------:|:-------:|:-------------------:|
| s1       | uniform / simple        |         500 | ✓ identical | — |
| s2       | zipf(1.5) / simple      |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s3       | uniform / complex       |         500 | ✓ identical | — |
| s4       | zipf(1.2) / complex     |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s5       | single-hot-pool         |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s6       | disjoint stripes        |        1000 | ✓ identical | — |

**15/15 byte-identical under default `committer_count = 4`.** Same byte-equivalence shape as WAC-perpetual (acct-9mgx.5), post-aywu/tm09 FIFO (acct-9mgx.1), and STD (acct-9mgx.4). The order-sensitive-method router classifier + per-pool sequence numbers cover both FIFO and LIFO symmetrically.

## Cross-method bench — s2 + s5 × direct + routed × AllLifo + AllWac, default GUCs

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in `results/phase6/bench-9mgx2/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | lifo       |             466.1 |        149.9 |  13 993 |
| s2       | direct   | **wac**    |             611.5 |         95.1 |  18 335 |
| s2       | routed   | lifo       |           1 432.6 |         49.1 |  43 002 |
| s2       | routed   | **wac**    |           1 908.4 |         49.9 |  57 304 |
| s5       | direct   | lifo       |             203.0 |        158.7 |   6 185 |
| s5       | direct   | **wac**    |             296.8 |         87.2 |   8 856 |
| s5       | routed   | **lifo**   |           2 365.8 |        184.7 |  71 117 |
| s5       | routed   | wac        |           2 129.5 |         52.5 |  63 903 |

**WAC beats LIFO in 3 of 4 cells; LIFO wins routed s5.** The pattern is consistent with order-sensitivity overhead:

- **Direct (single-trx-per-commit)**: LIFO pays per-event for layer Insert + per-row Update of the tail + occasional cross-layer span Delete; WAC's single-row UPSERT is cheaper. s2 direct: LIFO 24% slower (466 vs 612); s5 direct: LIFO 32% slower (203 vs 297).

- **Routed s2 (zipf-1.5 dispersed)**: LIFO 25% slower (1433 vs 1908). The router still emits ~7.6-submission commit_groups for both; LIFO's per-event layer maintenance overhead and tm09's predecessor-wait dominate. Pipeline_ns_avg 7.06ms vs WAC 11.07ms — committer per-batch is comparable, but LIFO commits roughly proportional to its lower throughput.

- **Routed s5 (single hot pool)**: **LIFO 11% faster (2366 vs 2130)**. Commit_group_avg jumps to 118 for LIFO vs 37 for WAC. tm09's per-pool predecessor-wait serializes commit_groups on the single hot pool, and aywu's order-sensitive bypass disables `batch_size_max` chunking for `lifo` pools — so the router naturally emits one large commit_group per drain pass rather than 3-4 smaller chunks. Larger commit_groups amortize per-COMMIT fsync + pool_lock + snapshot hydration costs across more submissions. WAC's groups stay small because it doesn't get the chunking bypass.

**s5 routed LIFO at 2366 tx/s** is just behind STD s5 routed (2414 tx/s) and ahead of FIFO s5 routed (2267) and WAC s5 routed (2150) at the top of the phase 6 bench matrix. The bigger commit_groups are the operative win for order-sensitive methods on hot pools.

**Routed beats direct in every cell** (3-12× throughput).

`committed_p99_us` (routed) is dominated by queue residency, not per-trx work. The `ack_p99_us` 49-185ms reflects the enqueue→ack round trip and is the relevant latency signal. s5 routed LIFO's ack p99 of 185ms is higher than WAC's 52ms — the consequence of larger commit_groups: any one submission's ack lands when its whole commit_group COMMITs.

## Cross-references

- **acct-9mgx.1** (closed 2026-05-23) — FIFO equivalence + bench. Lands the shared `--method-mix` infrastructure this work extends; the order-sensitive correctness story (intra-window split + inter-window race) was discovered there.
- **acct-9mgx.4** (closed 2026-05-23) — STD equivalence + bench. Closest harness sibling (caller-major workload shape); std is split-safe so it gets different commit_group sizing.
- **acct-9mgx.5** (closed 2026-05-23) — WAC-perpetual under the unified harness. Provides the WAC baseline for this bench.
- **acct-9mgx.6** (closed 2026-05-23) — wac_periodic. Introduced the bench-9mgx{N} pattern this work mirrors.
- **acct-aywu** (closed 2026-05-23) — order-sensitive cost-method classifier on the router. Returns `true` for `"lifo"`, so LIFO commit_groups are emitted whole regardless of `batch_size_max`. Load-bearing for LIFO correctness under cm > 1.
- **acct-tm09** (closed 2026-05-23) — per-pool sequence numbers for FIFO/LIFO/Specific. Stamps sequence on LIFO pools; committer's predecessor wait covers cross-window ordering.
- **acct-9mgx.3** (open) — Specific-id. Last open `--method-mix` extension before the cross-method roll-up doc.
- **acct-9mgx.7** (open, blocked on .3) — cross-method roll-up doc.

## Harness invocation

```bash
# Lenient equivalence
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-lifo

# Strict equivalence — should be 0 drifts under default cm=4 (aywu + tm09 in place)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-lifo \
    --strict

# Full 15-trial sweep
METHOD_MIX=all-lifo bash scripts/run-equivalence-sweep.sh

# Cross-method bench (8 runs)
bash scripts/run-bench-9mgx2.sh

# Single bench cell — reseed + drive
cargo run --release -p ledger-harness -- run \
    --scenario s5 --path routed --duration 30s \
    --method-mix all-lifo --seed-count 1000 \
    --max-callers 20 --no-sampler
```

## What this DOES NOT change

- **No ledger-core changes.** `lifo.rs` semantics unchanged.
- **No router/committer changes.** aywu's classifier already covers `"lifo"`; tm09's sequence-stamping doesn't need per-method branches.
- **No SPI changes.**
- **No schema changes.**

## What's deferred

- **acct-9mgx.3** (Specific) — last cost-method validation before .7.
- **acct-9mgx.7** (cross-method roll-up doc) — synthesizes FIFO/LIFO/Specific/STD/WAC-perpetual/WAC-periodic into a single comparison.
- **Per-method `run` workload variants**: the run subcommand still uses the all-receipts po_receipt workload regardless of method-mix. A depletion-aware run-subcommand workload would characterize LIFO depletion throughput specifically; equivalence-side workload here exercises both.
