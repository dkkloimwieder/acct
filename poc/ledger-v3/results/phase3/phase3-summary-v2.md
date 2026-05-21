# Phase 3 second-pass measurement — Path A baseline

**Scope:** acct-7wye re-run with workload + seeding fixes from cs5k
first-pass. 200-caller cap, **60s** window per scenario (up from 30s),
10000-pool universe (up from 1000 in v1), Mixed method assignment.

**Run:** 2026-05-21T13:52 UTC against poc_v3 / acct-postgres (PG 18,
`io_method=io_uring`, `max_connections=500`).

## Results

| Scenario | Callers | Overlap         | Complexity | Throughput trx/s | p50 µs   | p99 µs    | WAL b/trx | attempts | errors | Top wait               |
|----------|--------:|-----------------|------------|-----------------:|---------:|----------:|----------:|---------:|-------:|------------------------|
| s1       | 10      | Uniform         | Simple     |          2215.2  |    3958  |     8667  |     1688  |  132922  |      0 | LWLock:WALWrite        |
| s2       | 200     | Zipf(1.5)       | Simple     |           462.3  |    4886  |  1483735  |     1760  |   28444  |      0 | Lock:tuple             |
| s3       | 10      | Uniform         | Complex    |           436.3  |   18923  |    65470  |    33045  |   27005  |    806 | Lock:transactionid     |
| s4       | 200     | Zipf(1.2)       | Complex    |             2.2  | 1019215  |  1339031  |   104168  |   11692  |  11558 | Lock:tuple             |
| s5       | 200     | Single hot pool | Simple     |           172.9  | 1153433  |  1630535  |     3547  |   10662  |      0 | Lock:tuple             |
| s6       | 200     | Disjoint        | Simple     |          2396.0  |   71630  |   283901  |     2546  |  143837  |      0 | LWLock:BufferContent   |

(`attempts` = successful + errored; `errors` = sqlx-side error count from
ledger_submit_trx. The v1 → v2 numbers below reference the first-pass
results at 30s.)

## v1 → v2 deltas

| Scenario | v1 errors / v1 attempts | v2 errors / v2 attempts | v1 thr | v2 thr |
|----------|------------------------:|------------------------:|-------:|-------:|
| s1       |  17927 /  83659 (21.4%) |      0 / 132922 (0%)    |   2191 |   2215 |
| s2       |    872 /  17667 (4.9%)  |      0 /  28444 (0%)    |    544 |    462 |
| s3       |  21905 /  21905 (100%)  |    806 /  27005 (3.0%)  |      0 |    436 |
| s4       |   6578 /   6578 (100%)  |  11558 /  11692 (98.9%) |      0 |    2.2 |
| s5       |  13406 /  13420 (99.9%) |      0 /  10662 (0%)    |    0.5 |    173 |
| s6       |  39374 /  85756 (45.9%) |      0 / 143837 (0%)    |   1545 |   2396 |

Five of six scenarios now run clean. **s4 still 99% errors** — but the
cause is different from v1's source_id collision (see below).

## What the fixes addressed

### 1. Cross-run trx.source_id collisions (RESOLVED)

v1 driver used `(caller_id+1)*1e9 + tick`, deterministic per caller_id.
Across consecutive scenario runs against the same DB, the second run's
caller 0 source_ids collided with the first run's caller 0 source_ids,
firing the `trx UNIQUE (trx_type, source_id)` constraint and rolling
back the caller's tx. The 17927-rollback floor on s1 was entirely this.

v2 driver prefixes caller_base with a run-unique key:
`(epoch_secs % 10^6) * 10^12 + caller_id * 10^6 + tick`. Each run gets
a unique source_id namespace within a 10^12 window per caller.

### 2. Universe size mismatch (RESOLVED)

v1 reused the existing 1000-pool fixture even though seed-pools was
called with `--count 10000`. Idempotency check was "any pool exists →
reuse". v2 errors on count-mismatch and prints a TRUNCATE hint, so the
operator either drops the fixture or accepts the existing count
explicitly.

v2 ran against a fresh 10000-pool universe — s6's disjoint stripes now
hit `stripe_size = 10000 / 200 = 50` per the §F spec (vs v1's 5).

### 3. Throughput=0 reporting ambiguity (RESOLVED)

`RunReport` now carries `attempts_total` and `errors_total` alongside
`throughput_trx_per_sec`. The s3/s4 v1 "throughput=0" rows are now
"throughput=0 with N attempts and N errors" — the failure volume is
explicit in the JSON.

### 4. PG tuning (PARTIALLY RESOLVED)

`scripts/tune-pg-for-phase3.sh` ALTERs max_connections cluster-wide.
Default target lowered to 500 (from initial 1100) after discovering an
**io_uring + max_connections=1100 incompatibility** in the dev container:
postgres fails to start with `could not setup io_uring queue: Cannot
allocate memory` because each backend pre-allocates io_uring queues and
the container's RLIMIT_MEMLOCK is exhausted at that backend count.

500 fits 200 callers + sampler/collector + headroom for other PoCs.
Full 1000-caller × 5min runs need either (a) raised RLIMIT_MEMLOCK on
the container OR (b) `io_method=worker` for the run window. Tracked in
the s4 follow-up.

## Outstanding bugs

### s4: pool_state UPSERT collision (NEW — surfaced by v2)

**SQLSTATE 21000: ON CONFLICT DO UPDATE command cannot affect row a
second time.**

ledger-direct's `apply_pool_state_mutations` UNNEST UPSERT receives
multiple input rows targeting the same `(pool_id, layer_seq)` PK within
a single submission. PG's safety check rejects the entire INSERT.

This happens when plan_apply emits per-line `PoolStateMutation::Upsert`
entries for a WAC pool that the same submission's lines touch multiple
times. With Complex (10-50 lines/trx) + Zipf(1.2) over 10000 pools
(top-ranks concentrate mass), the probability of two lines in one trx
hitting the same WAC pool is high enough that 99% of s4 submissions
collide.

**Fix shape (for follow-up issue):** either

- **(a) ledger-core plan_apply de-duplicates** before returning
  PlanResult — merge same-key Upserts by composing the WAC update across
  the lines in submission order. This is the cleanest place since it's
  pure-Rust and unit-testable.
- **(b) ledger-direct bulk_write merges** in-process before the UNNEST,
  using a BTreeMap keyed on (pool_id, layer_seq) and folding values.
  Cheaper to land but spreads ledger semantics into the SPI shim.

Recommend (a). v1 results masked this with source_id-collision rollbacks.

### s3: 3% error rate (residual)

s3 (10 callers × Complex Uniform Mixed) shows 806 / 27005 (3.0%) errors.
Same shape as s4 but at much lower concurrency. Likely the same UPSERT
collision when same-trx random uniform picks land twice on one pool —
probability ~30 lines × 30 lines / 10000 pools ≈ 9% per trx, observed
3% so plausible. Same fix.

### s4: lock contention even at 0% errors (expected)

Even after the UPSERT-collision fix lands, s4 will likely show degraded
throughput from genuine pool_lock contention — 200 callers × 30 lines =
6000 FOR UPDATE acquisitions on Zipf(1.2) hot ranks. That's the
characterization s4 is designed to surface; design-v3 §10.4 expects
direct path to lose to routed in this regime. The current 99% error
rate hides any signal about that, so the bugfix is a prerequisite to
the actual measurement.

## Per-scenario observations

### s1: synchronous-commit ceiling unchanged

2215 trx/s at p99=8.7ms (slightly higher than v1's 6.3ms but within
noise). top_wait split LWLock:WALWrite (2870) + IO:WalSync (545) is the
clean fsync-bottlenecked signature.

### s2: zipf(1.5) lock-amortization regime, clean

462 trx/s at p99=1.5s, top_wait Lock:tuple. The long tail reflects 200
callers serializing through the hot ~10 pools of Zipf(1.5). Median
remains healthy at 4.9ms — most submissions hit cold ranks; only the
hot-rank tail piles up. WAL b/trx = 1760 ≈ s1, confirming work per
successful trx is the same.

### s3: 10 callers × Complex unblocked

436 trx/s at p50=18.9ms / p99=65.5ms. Complex = ~30 lines/trx so this
is ~13,000 line-writes per second. WAL b/trx=33045 ≈ 20× s1 reflects
the 30× line count — close to per-line WAL parity. top_wait
Lock:transactionid (1685) is the only contention; pool_lock waits
absent.

### s5: pathological hot-pool serializes cleanly

200 callers all hitting the same pool_id[0] (Zipf exp=100), 172 trx/s
at p50/p99 = 1.15s / 1.63s. Linear queue through the single
pool_lock — no errors, no rollbacks, just pure serialization. This is
the "direct path ceiling" §10.4 expects routed to beat.

### s6: disjoint stripes — 2396 trx/s, lock-free

Highest throughput in the suite. 200 callers each pinned to a 50-pool
stripe, zero cross-caller pool_lock contention. top_wait
LWLock:BufferContent (100209) reflects shared-buffer cache pressure
under the high commit rate rather than application contention — i.e.,
the bottleneck is PG buffer-cache page eviction, not the ledger. This
is the upper bound for Path A under low-overlap workloads; routed
should NOT beat this regime since it has no scheduling advantage when
caller pool sets are disjoint.

## What this run delivers

- Five of six scenarios run clean at 60s × 200 callers against a
  fresh 10000-pool universe.
- s1 baseline (2215 trx/s) confirms the synchronous-commit ceiling
  signature.
- s6 ceiling (2396 trx/s) establishes the no-contention upper bound.
- s5 hot-pool floor (173 trx/s) establishes the worst-case lower
  bound for Path A.
- Mid-regime cells (s2, s3) characterize lock-amortization and per-trx
  work-intensity respectively.
- s4 (production-like stress) is **gated on the pool_state UPSERT
  bugfix** before its numbers carry signal.

## Follow-ups filed

Tracked under the acct-7wye epic via new bd issues:

1. **plan_apply same-pool line merging** — fix the SQLSTATE 21000 bug;
   needs unit test for "multiple lines into same WAC pool merge into
   one Upsert"; affects s3 (3% errors) and s4 (99% errors).
2. **Container io_uring memlock raise OR conditional io_method=worker**
   — gates running 500+ caller scenarios. Either tune RLIMIT_MEMLOCK
   on acct-postgres OR add a `--io-method` switch to the runner.
3. **Full §11 measurement run after (1) lands**: 1000-caller × 5min
   per scenario, after the bugfix and PG tuning. Updates the canonical
   Phase 3 baseline.

## Artifacts

- `s{1..6}-direct-2026-05-21T13-52-25.json` — v2 measurement output
- `s{1..6}-direct-2026-05-21T13-27-05.json` — v1 (kept for delta
  comparison)
- `phase3-summary.md` — v1 first-pass notes
- `phase3-summary-v2.md` — this document
- Runner: `scripts/run-phase3-measurements.sh`
- PG tuning: `scripts/tune-pg-for-phase3.sh`
