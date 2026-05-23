# acct-9mgx.4 — STD equivalence + bench

**Issue:** acct-9mgx.4 (P2; sibling of acct-9mgx.{1,2,3,5,6})
**Run window:** 2026-05-23
**Equivalence sweep:** `results/phase6/equivalence/run-all-std-2026-05-23T17-18-39Z.log`
**Bench:** `results/phase6/bench-9mgx4/run-2026-05-23T17-22-03Z.log`

## Routed STD correctness status

**Default GUCs (`committer_count = 4`, `batch_size_max = 50`) produce byte-identical STD across Path A and Path B (15/15 sweep below).** No ordering-correctness fix required for STD — the cost-method has no pool_state surface to diverge on.

STD pools store no rows in `pool_state`. The dispatcher reads `pool.method` then writes one `trx_line` (qty + caller-supplied `unit_cost`) and one `posting_line` (amount = `abs(qty) × unit_cost`) per event. Receipts and depletions emit identical row shapes. Because there is no per-pool mutable state, two concurrent commit_groups touching the same STD pool produce trx in any order without affecting equivalence — there is nothing to read or write that the other group could observe stale.

acct-aywu's order-sensitive classifier returns `false` for `"std"`, so the router never forces STD groups into the no-split path; STD commit_groups are chunked freely by `batch_size_max` for maximum throughput. acct-tm09's per-pool sequence numbers are likewise not assigned for STD pools (`pool_is_order_sensitive` returns false); the committer's predecessor-wait loop is bypassed for STD groups.

This is the strongest equivalence guarantee of any cost method in v3 — no drift bucket needed, no lenient/strict distinction needed, no path-dependent commit_group ordering surface.

## Change

Extends the cross-path equivalence harness (acct-h5gs / 9mgx.5 / 9mgx.6 / 9mgx.1 lineage) to validate `std` end-state equivalence + characterizes throughput against the `wac` baseline on s2 (zipf-1.5 simple) and s5 (single hot pool).

**Harness extensions:**

- `MethodMix::AllStd` variant added to `cli.rs`.
- `pool_universe::method_for_index` extended with the `"std"` branch for `AllStd`.
- `equivalence` subcommand: dispatch through `build_submissions_std` when `--method-mix all-std`. Workload is a 2-tick alternating cycle per caller (R qty=10 / D qty=-3) in caller-major order. **Both receipts and depletions supply `unit_cost`** (cycling 100, 107, 114, … per round) since STD has no pool_state to look the cost up from — the caller is authoritative per design-v3 §3.4 and locked plan Q4.
- `diff_snapshots` requires NO STD-specific branch. STD pools produce zero `pool_state` rows on both A and B sides; the existing pool_state diff loop naturally yields no rows to compare. The bd description's "diff classification needs adjustment to skip pool_state comparison for STD pools" is automatically satisfied by STD's empty pool_state.
- New `scripts/run-bench-9mgx4.sh` (mirror of `run-bench-9mgx1.sh`): 8 cells = s2/s5 × direct/routed × all-std/all-wac.

## Equivalence sweep — 6 scenarios + 9 strict trials, default committer_count=4

| Scenario | Workload                | Submissions | Lenient | Strict T1 / T2 / T3 |
|----------|-------------------------|------------:|:-------:|:-------------------:|
| s1       | uniform / simple        |         500 | ✓ identical | — |
| s2       | zipf(1.5) / simple      |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s3       | uniform / complex       |         500 | ✓ identical | — |
| s4       | zipf(1.2) / complex     |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s5       | single-hot-pool         |        1000 | ✓ identical | ✓ / ✓ / ✓ |
| s6       | disjoint stripes        |        1000 | ✓ identical | — |

**15/15 byte-identical under default `committer_count = 4`.** Same byte-equivalence shape as WAC-perpetual (acct-9mgx.5) and post-aywu/tm09 FIFO (acct-9mgx.1), reached without any router or committer changes — STD doesn't need them.

## Cross-method bench — s2 + s5 × direct + routed × AllStd + AllWac, default GUCs

20 callers, 30s duration, 1000-pool universe. Per-run JSONs in `results/phase6/bench-9mgx4/`.

| Scenario | Path     | Method     | Throughput (tx/s) | p99 ack (ms) | Commits |
|----------|----------|------------|------------------:|-------------:|--------:|
| s2       | direct   | **std**    |             615.9 |         96.8 |  18 336 |
| s2       | direct   | wac        |             605.6 |        100.2 |  18 087 |
| s2       | routed   | **std**    |           2 192.2 |         42.3 |  65 788 |
| s2       | routed   | wac        |           1 755.1 |         48.3 |  52 663 |
| s5       | direct   | **std**    |             304.3 |         91.0 |   9 211 |
| s5       | direct   | wac        |             297.0 |         88.9 |   8 876 |
| s5       | routed   | **std**    |           2 414.0 |         46.3 |  72 436 |
| s5       | routed   | wac        |           2 149.7 |         51.2 |  64 538 |

**STD beats WAC in every cell.** The margin scales with the routed path:

- **Direct (single-trx-per-commit)**: STD ~2% faster (within noise). The pool_lock acquisition + pool_state snapshot hydration that WAC pays per event are cheap for low contention; the STD savings are real but small.
- **Routed s2 (zipf-1.5 dispersed)**: STD 25% faster (2192 vs 1755). The committer's pool_lock acquire + snapshot hydration + pool_state writeback are amortized across the commit_group for WAC; STD skips all three. Fewer WAL bytes per commit, lower pipeline_ns_avg (8.4ms vs 10.4ms).
- **Routed s5 (single hot pool)**: STD 12% faster (2414 vs 2150). Both methods effectively serialize through the same pool, so the per-event savings dominate the throughput delta.

**s5 routed STD at 2414 tx/s is the fastest cell measured across the entire phase 6 bench matrix** (vs FIFO s5 routed 2267, WAC s5 routed 2150, WAC s2 routed 1760). STD's lack of pool_state mutation is the operative advantage when the workload concentrates on one pool — no read-modify-write traffic at all.

**Routed beats direct in every cell** (3-8× throughput).

`committed_p99_us` (routed) is dominated by queue residency (7-12 seconds at the 30s bench duration when the queue saturates), not per-trx work. The `ack_p99_us` 42-51ms reflects the enqueue→ack round trip and is the relevant latency signal.

## Cross-references

- **acct-9mgx.5** (closed 2026-05-23) — WAC-perpetual under the unified harness. Provides the WAC baseline for this bench.
- **acct-9mgx.1** (closed 2026-05-23) — FIFO equivalence + bench. Lands the `--method-mix` infrastructure this work extends.
- **acct-9mgx.6** (closed 2026-05-23) — wac_periodic. Introduced the bench-9mgx{N} pattern this work mirrors.
- **acct-aywu** (closed 2026-05-23) — order-sensitive cost-method classifier on the router. Returns `false` for `"std"`, so STD groups are chunked normally by `batch_size_max`. No-op for STD.
- **acct-tm09** (closed 2026-05-23) — per-pool sequence numbers for FIFO/LIFO/Specific. Not assigned for STD pools (classifier-gated); committer's predecessor wait is bypassed.
- **acct-9mgx.{2,3}** (open) — LIFO / Specific. The `--method-mix` plumbing is shared; both should reach byte-identical 15/15 under default cm=4 GUCs (aywu + tm09 handle their order-sensitivity).
- **acct-9mgx.7** (open, blocked on .2/.3) — cross-method roll-up doc.

## Harness invocation

```bash
# Lenient equivalence
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-std

# Strict equivalence — should be 0 drifts (STD has no drift surface)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --method-mix all-std \
    --strict

# Full 15-trial sweep
METHOD_MIX=all-std bash scripts/run-equivalence-sweep.sh

# Cross-method bench (8 runs)
bash scripts/run-bench-9mgx4.sh

# Single bench cell — reseed + drive
cargo run --release -p ledger-harness -- run \
    --scenario s5 --path routed --duration 30s \
    --method-mix all-std --seed-count 1000 \
    --max-callers 20 --no-sampler
```

## What this DOES NOT change

- **No ledger-core changes.** `standard.rs` semantics unchanged; STD was already correct per-trx.
- **No router/committer changes.** STD groups already classified as split-safe by the order-sensitive method classifier; no per-pool sequence numbers needed.
- **No SPI changes.**
- **No schema changes.**

## What's deferred

- **acct-9mgx.7** (cross-method roll-up doc).
- **Per-method `run` workload variants**: the run subcommand still uses the all-receipts po_receipt workload regardless of method-mix. A depletion-aware run-subcommand workload would characterize STD depletion throughput specifically; equivalence-side workload here exercises both.
- **STD-specific `standard_costs` lookup**: per locked plan Q4, ledger-core trusts the caller-supplied `unit_cost`. A future revision could read from `Snapshot.std_cost_of` populated from a `standard_costs` table at snapshot hydration time. Not in scope for this PoC.
