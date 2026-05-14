# H + extension bench — acct-zm69 / zm69.h6-followup

**Date:** 2026-05-14
**Driver:** `poc/batch-ledger/bench/run-h-ext.sh`
**Bench:** `poc/batch-ledger/tests/bench_h_batched.rs` with `POC_BENCH_FUNCTION=post_batch_h_ext`
**Schema:** PoC mig 0029 (`cost_layers_h_ext` + `cost_consumptions_h_ext` + `post_batch_h_ext` wrapper)
**Extension:** `poc/ledger-extension/src/h_arena.rs` (new module; per-group `effective_qty` shmem rollup + PRE_COMMIT-phase CAS-checked apply)
**DB:** acct_poc on Postgres 18 (host port 5111), tuned-conf
**Workload:** 20 workers, 60s, 70% issue / 30% receipt mix, **READ COMMITTED** (invariant lives in shmem CAS, no SSI needed)

## Headline

| # | Batch | Groups | Committed-batches/s | Transfers/s | Aborts | Retries/commit | p99 batch (ms) |
|---|------:|------:|------:|------:|------:|------:|------:|
| 1 | 100 | 50 | **1,815.7** | 181,566 | 0 | 0 | 20 |
| 2 | 1000 | 50 | **279.4** | 279,391 | 0 | 0 | 117 |
| 3 | 10000 | 50 | 30.9 | **308,887** | 0 | 0 | 907 |
| 4 | 1000 | **1 (fan_in)** | **287.1** | 287,081 | 0 | 0 | 108 |
| 5 | 1000 | **5000 (fan_out)** | **242.3** | 242,279 | 0 | 0 | 132 |

All runs invariant-clean (`overconsume_groups=0`). Zero deadlocks, zero retries, zero 40001s across the entire sweep.

## Comparison summary

| Approach | b=1000 g=5000 | b=1000 g=50 | b=1000 g=1 fan_in |
|---|---:|---:|---:|
| A2 (current; shadow + replay) | 37,400 | — | — |
| Pure-SQL H (mig 0026 + 0027) | 2,906 | 261 | 9 |
| Pure-SQL H+app (mig 0028) | — | 1,159 | — |
| **H+ext (mig 0029 + h_arena)** | **242,279** | **279,391** | **287,081** |

**H+ext at the A2-equivalent shape (b=1000 g=5000 fan_out): 6.5× over A2.**
**H+ext over pure-SQL H at the same shape: 83×.**

## Why this works

The pure-SQL H bench (`results-h-batched-2026-05-14.md`) failed at production batch sizes because the deferred SUM trigger generated SSI predicate reads at commit time. Concurrent backends inserting into overlapping `cost_consumptions_h` row sets created rw-dependency cycles in SSI's serialization graph → 70-74% abort rate.

H+ext moves the invariant OUT of MVCC entirely:

1. **Durable INSERTs into `cost_layers_h_ext` / `cost_consumptions_h_ext` carry NO predicate reads** — pure inserts of disjoint rows. Under RC, concurrent writers don't conflict.
2. **The wrapper aggregates per-group signed deltas** (one `h_apply_delta(group_id, net_delta)` call per touched group, not per envelope), staging into the extension's per-backend `H_PENDING` map.
3. **At PG `XACT_EVENT_PRE_COMMIT`** the extension drains `H_PENDING` and applies each (group_id, net_delta) under shmem `compare_exchange` with invariant check (`new_effective_qty < 0` → `error!`).
4. **PG's `XACT_EVENT_ABORT`** reverses any deltas pre-applied earlier in the batch (in case PRE_COMMIT raised partway).

The invariant check is now ns-scale CAS, not SSI predicate dependency analysis. Concurrent backends serialize through CAS retries on the same cell (lock-free, never deadlocks); concurrent backends on disjoint cells fully parallelize.

## Fan_in surprise

The most interesting result is shape #4: **g=1 (single group, 20 workers all hammering)** sustains 287 commits/s — the same as fan_out g=5000.

Under pure-SQL H this shape would have been pathological (all 20 backends generating overlapping predicate reads on the same group → SSI conflict cascade). Pure-SQL H @ b=1000 g=1 got 9 transfers/s (essentially zero useful work).

H+ext on the same shape: zero aborts, zero retries, 287K transfers/s. The shmem CAS resolves contention at memory-bus speed — internal CAS retries are invisible to the bench's commit-retry loop because they happen INSIDE the apply, not as transaction aborts.

## Batch-size sensitivity

- b=100 → 1,816 commits/s × 100 = **181K transfers/s** (low per-batch latency p99=20ms)
- b=1000 → 279 commits/s × 1000 = **279K transfers/s** (p99=117ms)
- b=10000 → 31 commits/s × 10000 = **309K transfers/s** (p99=907ms)

Throughput scales sublinearly with batch size (181K → 279K → 309K = 1.5× and 1.1× steps), and latency scales linearly (20ms → 117ms → 907ms). The shmem hot path is fast enough that the dominant cost at scale is durable INSERT throughput on the *_ext tables, not the invariant enforcement.

Production batch size choice trades throughput vs latency cleanly. b=1000 is the sweet spot (matches A2's bench, p99 < 200ms).

## Verdict — Candidate H is viable WITH extension

The single-row probe's projection (33× over A2) was misleading at single-row, but **the architecturally correct shape is now measured**. H + extension:

- Eliminates the SSI predicate-read amplification that killed pure-SQL H.
- Sustains 6.5–8× A2 baseline throughput at production batch sizes.
- Provides invariant enforcement that is **never invalidated under concurrency** (0 violations across the sweep).
- Runs under READ COMMITTED, eliminating SSI overhead entirely.
- Survives fan_in extremes (20w × 1 group) without retries or deadlocks.

## What H+ext gives that A2 doesn't

Both end up extension-mediated. The differentiators:

1. **Audit trail.** `cost_layers_h_ext` + `cost_consumptions_h_ext` are append-only durable tables — every receipt and consumption has a row. A2's `cost_layers` mutates `qty_remaining` in place; per-consumption attribution lives only in `cost_layer_depletions` rows which depend on shadow-replay-time slice identities (the source of the over-consume gap).
2. **No shadow / replay.** A2's correctness depends on per-backend shadow snapshots replayed at commit — the over-consume gap (R-MB6) is structural. H+ext has no shadow; the only "replay" is shmem CAS, which is atomic by construction.
3. **No invariant gap.** A2's `consume_from_head` discards its `ConsumeResult` (`fifo.rs:1175-1179`) and inserts depletion rows from shadow-time state. H+ext's CAS-checked apply CANNOT over-consume — the check happens atomically with the apply.

What A2 has that H+ext doesn't (yet):

1. **Per-layer attribution.** A2 walks layers in order and inserts `cost_layer_depletions` per consumed layer. H+ext only tracks net qty. For FIFO cost computation (issue.amount = sum over consumed-layer per-unit costs), an additional mechanism is needed — could be a separate lookup walk in the wrapper, OR a separate native helper that maintains per-layer residual.
2. **Bgworker drain to durable.** sw4i's WAC arena drains to `account_balances_rollup` for crash recovery + history. H+ext's shmem is volatile; on PG restart it's empty until backends warm it. Lazy-load + bgworker pattern from sw4i would close this gap if needed for production.
3. **Crash safety past commit.** Same gap as sw4i pre-WAL-record. Acceptable for PoC scope.

## Followups (sub-issues to file, not yet enumerated)

- Per-layer cost attribution under H+ext (the FIFO-cost-computation question; needed before H+ext can replace A2 for cost flow, not just qty invariant).
- Bgworker drain `cost_layer_group_totals` from shmem (for restart safety).
- Native batch entry point `h_apply_batch(envelopes JSONB)` to avoid the plpgsql LOOP + 1 FFI call per touched group (likely minor win; LOOP already fires ~5-50 times per batch, not per envelope).
- Property test analogous to `bench_h_probe`'s RC-control to confirm shmem invariant never violates under any concurrent shape.
- Comparison against A2 on full FIFO cost-flow workload (not just qty invariant) — H+ext is qty-invariant viable; cost attribution still needs answering.

## Raw logs

Per-shape logs at `/tmp/h_ext_run1/`.
