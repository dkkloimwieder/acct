# acct-9mgx.3 — Specific-id equivalence + bench

**Issue:** acct-9mgx.3 (P2; sibling of acct-9mgx.{1,2,4,5,6})
**Run window:** 2026-05-23 → 2026-05-24
**Equivalence sweep:** `results/phase6/equivalence/run-all-specific-2026-05-24T00-52-23Z.log`
**Bench:** `results/phase6/bench-9mgx3/run-2026-05-24T00-55-28Z.log`

## Routed Specific correctness status

**Default GUCs (`committer_count = 4`, `batch_size_max = 50`) produce byte-identical Specific across Path A and Path B (15/15 sweep below).** No new infrastructure was required — Specific inherits the correctness fixes that landed for FIFO/LIFO.

`apply_specific` is structurally `apply_fifo` with a different method check (per `ledger-core/src/specific.rs`); the layered consumption path is shared. The same two correctness gaps that bit FIFO under default cm=4 apply identically to Specific:

1. **Intra-window split** — order-sensitive commit_groups must emit whole. **Already covered by acct-aywu**: the router's `is_order_sensitive_method` classifier returns `true` for `"specific"` (alongside `"fifo"` and `"lifo"`).

2. **Inter-window race** — across router ticks, two commit_groups for the same Specific pool must commit in router-emit order. **Already covered by acct-tm09**: per-pool sequence numbers + committer's spin-sleep predecessor wait apply uniformly to all order-sensitive methods.

Both correctness fixes are validated by router unit tests that explicitly cover `"specific"` (`router.rs:1310`).

## Workload shape

The K=1 invariant from design-v3 §3.5 ("each unit is its own pool, identity_key = unit_id, qty=1 layer") is a usage convention enforced by the caller, not by the helper. The equivalence harness deliberately reuses the FIFO workload shape (qty=10 receipts, qty=-3 depletions, identity_key=0 default) to exercise the same path-equivalence properties as FIFO. This is sufficient because:

- `apply_specific` dispatches into `apply_layered(..., LayerOrder::Ascending)` — the same code as FIFO.
- The dispatcher-side concerns (method recognition, pool-method round-trip, snapshot hydration) are exercised identically.
- The K=1 convention only changes upstream call patterns; the ledger-side correctness is unchanged.

A per-unit Specific workload (one pool per unit_id, qty=±1, with seeded distinct `identity_key`) is the natural extension if a future requirement needs to validate the K=1-respecting shape. It is not in scope here.

## Change

Extends the cross-path equivalence harness (acct-h5gs / 9mgx.5 / 9mgx.6 / 9mgx.1 / 9mgx.2 / 9mgx.4 lineage) to validate `specific` end-state equivalence + characterizes throughput against the `wac` baseline on s2 (zipf-1.5 simple) and s5 (single hot pool).

**Harness extensions:**

- `MethodMix::AllSpecific` variant added to `cli.rs`.
- `pool_universe::method_for_index` extended with the `"specific"` branch for `AllSpecific`.
- `equivalence` subcommand: dispatch through `build_submissions_specific` when `--method-mix all-specific`. Workload mirrors the FIFO 2-tick alternating cycle per caller, caller-major submission order.
- New `DiffResult.specific_drifts` bucket + `trx_lines_preserve_total_qty_per_pool_specific` classifier. Per-line trx_line content differences on Specific transfer_shipment trxs would route here when per-pool ∑qty matches across paths. The load-bearing invariant — per-pool ∑pool_state.qty matches across paths — stays in the `errors` bucket. **Under default cm=4 with aywu + tm09, the bucket never fires** (all 15 trials byte-identical) — defensive classification only.
- `--strict` upgrades `specific_drifts` to `errors`.
- New `scripts/run-bench-9mgx3.sh`: 8 cells = s2/s5 × direct/routed × all-specific/all-wac.

## Equivalence sweep — 6 scenarios + 9 strict trials, default committer_count=4

| Scenario | Workload                | Submissions | Lenient | Strict T1 / T2 / T3 |
|----------|-------------------------|------------:|:-------:|:-------------------:|
| s1       | uniform / simple        |         500 | ✓ identical | — |
| s2       | zipf(1.5) / simple      |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s3       | uniform / complex       |         500 | ✓ identical | — |
| s4       | zipf(1.2) / complex     |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s5       | single-hot-pool         |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s6       | disjoint stripes        |        1000 | ✓ identical | — |

**15/15 byte-identical under default `committer_count = 4`.** Same byte-equivalence shape as WAC-perpetual (acct-9mgx.5), post-aywu/tm09 FIFO (acct-9mgx.1), LIFO (acct-9mgx.2), and STD (acct-9mgx.4). The order-sensitive-method router classifier + per-pool sequence numbers cover FIFO, LIFO, and Specific symmetrically.

## Cross-method bench — s2 + s5 × direct + routed × AllSpecific + AllWac, default GUCs

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in `results/phase6/bench-9mgx3/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | specific   |             463.9 |        152.6 |  13 923 |
| s2       | direct   | **wac**    |             614.7 |         96.1 |  18 440 |
| s2       | routed   | specific   |           1 409.8 |         48.9 |  42 303 |
| s2       | routed   | **wac**    |           1 869.6 |         46.7 |  56 091 |
| s5       | direct   | specific   |             204.9 |        157.9 |   6 260 |
| s5       | direct   | **wac**    |             296.8 |         88.3 |   8 960 |
| s5       | routed   | **specific** |         2 409.1 |        201.7 |  72 473 |
| s5       | routed   | wac        |           2 112.6 |         53.3 |  63 385 |

**Pattern matches LIFO.** WAC wins 3 of 4 cells; Specific wins routed s5.

- **Direct (single-trx-per-commit)**: Specific pays per-event layer Insert + per-row Update of the head + occasional cross-layer span Delete; WAC's single-row UPSERT is cheaper. s2 direct: Specific 25% slower (464 vs 615); s5 direct: Specific 31% slower (205 vs 297). Within bench noise of the LIFO equivalents.

- **Routed s2 (zipf-1.5 dispersed)**: Specific 25% slower (1410 vs 1870). Commit_groups average ~7.6 for both; per-event layer maintenance + tm09 predecessor-wait dominate.

- **Routed s5 (single hot pool)**: **Specific 14% faster (2409 vs 2113)**. Commit_group_avg jumps to 130 for Specific vs 37 for WAC — the same dynamic as LIFO (118 vs 37): aywu's chunking bypass + tm09's predecessor-wait drive larger commit_groups on order-sensitive methods, amortizing per-COMMIT fsync + pool_lock + snapshot hydration costs across more submissions.

**s5 routed Specific at 2409 tx/s** is second-fastest of the phase 6 bench matrix (STD s5 routed = 2414, Specific s5 routed = 2409, LIFO s5 routed = 2366, FIFO s5 routed = 2267, WAC s5 routed = 2150).

**Routed beats direct in every cell** (3-12× throughput).

`ack_p99_us` of 201ms on routed Specific s5 is consistent with the large commit_group_avg — any submission's ack lands when its whole commit_group COMMITs.

## Cross-references

- **acct-9mgx.1** (closed 2026-05-23) — FIFO equivalence + bench. Lands the shared `--method-mix` infrastructure this work extends.
- **acct-9mgx.2** (closed 2026-05-23) — LIFO equivalence + bench. Closest harness sibling (same order-sensitive class, near-identical bench numbers).
- **acct-9mgx.4** (closed 2026-05-23) — STD equivalence + bench. Provides the caller-major workload pattern this work mirrors.
- **acct-9mgx.5** (closed 2026-05-23) — WAC-perpetual under the unified harness. Provides the WAC baseline for this bench.
- **acct-9mgx.6** (closed 2026-05-23) — wac_periodic. Introduced the bench-9mgx{N} pattern.
- **acct-aywu** (closed 2026-05-23) — order-sensitive cost-method classifier on the router. Returns `true` for `"specific"`. Load-bearing for Specific correctness under cm > 1.
- **acct-tm09** (closed 2026-05-23) — per-pool sequence numbers for FIFO/LIFO/Specific. Stamps sequence on Specific pools; committer's predecessor wait covers cross-window ordering.
- **acct-9mgx.7** (open, now unblocked) — cross-method comparison roll-up doc. All six cost-method validations now landed; .7 synthesizes the matrix.

## Harness invocation

```bash
# Lenient equivalence
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-specific

# Strict equivalence — should be 0 drifts under default cm=4
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-specific \
    --strict

# Full 15-trial sweep
METHOD_MIX=all-specific bash scripts/run-equivalence-sweep.sh

# Cross-method bench (8 runs)
bash scripts/run-bench-9mgx3.sh

# Single bench cell — reseed + drive
cargo run --release -p ledger-harness -- run \
    --scenario s5 --path routed --duration 30s \
    --method-mix all-specific --seed-count 1000 \
    --max-callers 20 --no-sampler
```

## What this DOES NOT change

- **No ledger-core changes.** `specific.rs` semantics unchanged.
- **No router/committer changes.** aywu's classifier already covers `"specific"`; tm09's sequence-stamping doesn't need per-method branches.
- **No SPI changes.**
- **No schema changes.**

## What's deferred

- **acct-9mgx.7** (cross-method roll-up doc) — now unblocked; synthesizes the six cost-method validations into a single comparison.
- **Per-unit Specific workload** (one pool per unit_id, qty=±1, distinct `identity_key`) — natural extension if a future requirement needs to validate the K=1-respecting shape.
- **Per-method `run` workload variants** — the run subcommand still uses the all-receipts po_receipt workload regardless of method-mix.
