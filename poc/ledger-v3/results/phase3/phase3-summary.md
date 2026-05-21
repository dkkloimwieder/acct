# Phase 3 first-pass measurement — Path A baseline

**Scope:** 200-caller cap, 30s window per scenario. Full 5-min × 1000-caller
run gated on PG tuning (`max_connections >= 1100`, shared_buffers, container
restart) — tracked as cs5k-followup.

**Run:** 2026-05-21T13:27 UTC against poc_v3 / acct-postgres container
(PG 18, `io_method=io_uring`).

## Results

| Scenario | Callers | Overlap         | Complexity | Throughput trx/s | p50 µs   | p99 µs    | WAL b/trx | commits | rollbacks | Top wait              |
|----------|---------|-----------------|------------|-----------------:|---------:|----------:|----------:|--------:|----------:|-----------------------|
| s1       | 10      | Uniform         | Simple     |          2190.8  |    3905  |     6336  |     1726  |  66484  |    17927  | LWLock:WALWrite       |
| s2       | 200     | Zipf(1.5)       | Simple     |           544.0  |    4440  |  1105199  |     1838  |  18002  |      864  | Lock:tuple            |
| s3       | 10      | Uniform         | Complex    |             0.0  |       0  |        0  |    57710  |    757  |    21905  | Lock:transactionid    |
| s4       | 200     | Zipf(1.2)       | Complex    |             0.0  |       0  |        0  |     6664  |   1399  |     6553  | Lock:tuple            |
| s5       | 200     | Single hot pool | Simple     |             0.5  |  485228  |   492830  |     3135  |   1422  |    13246  | Lock:tuple            |
| s6       | 200     | Disjoint        | Simple     |          1544.7  |   93323  |   353894  |     2430  |  47020  |    38874  | LWLock:BufferContent  |

(`Throughput` = successful submissions / second. `commits` / `rollbacks` are
PG-side `pg_stat_database` deltas across the run window. `Throughput=0.0` on
s3/s4 reflects `LatencyHistogram::merge_all` receiving zero samples — every
caller submission either errored before INSERT or got rolled back, so no
`hist.record(elapsed)` fired.)

## Observations

### Universe-size constraint affected results

The runner asked seed-pools for 10000 pools but the existing 1000-pool
fixture was reused (idempotency skip on `count>0`). The 1000-pool universe
under-allocates contention surface for the high-caller scenarios:

- s6 (disjoint stripes) wanted `stripe_size = 10000 / 200 = 50`; got
  `1000 / 200 = 5`. Smaller stripes mean fewer pool_ids per caller, more
  caller-internal collisions on (pool_id, trx_seq) UNIQUE.
- s2 / s4 (Zipf) wanted Zipf over 10000 pools; got Zipf over 1000. The
  effective hot-set is identical (Zipf concentrates on rank-1), but the
  long-tail probability mass shifts toward the head.

This biases s2/s4/s5/s6 toward higher contention than the §F definition.
Follow-up should reseed at the full 10000 and re-measure. Filed as part of
cs5k-followup.

### Throughput-zero scenarios (s3, s4)

s3 (10 callers × Complex 10-50 lines/trx) and s4 (200 callers × Complex)
both show ~96-97% rollback rates and `Lock:transactionid` / `Lock:tuple`
wait dominance. The workload only emits positive-qty po_receipts (no
depletions), so InsufficientInventory isn't the cause. Two hypotheses:

1. **trx_line UNIQUE collisions**. `workload::next_lines` generates each
   line's `source_id` independently via `rng.random_range(1..=1_000_000)`.
   At 10 callers × 30 lines × 1500 trx/s × 30s ≈ 13M lines drawn from a
   1M space, collisions on `(line_type, source_id)` are expected. The
   `trx_line UNIQUE (pool_id, trx_seq)` constraint catches them; the
   caller's tx rolls back.

2. **Pool_lock contention deadlocks**. With Complex (10-50 lines per
   submission), each trx touches a large fraction of the 1000-pool
   universe under FOR UPDATE. Sorted lock acquisition prevents deadlocks
   in theory; in practice, holding 30 pool_locks for the duration of the
   plan_apply + bulk-write loop serializes the 10 callers.

The high `wal_bytes_per_trx=57710` on s3 (vs ~2000 elsewhere) supports
(1) — rolled-back trx still emit WAL for the INSERT attempts before the
constraint violation. Investigation tracked as cs5k-followup.

### s1 baseline holds

s1 (10 callers × Simple uniform) hits 2190 trx/s at p50=3.9ms / p99=6.3ms
with top-wait LWLock:WALWrite and IO:WalSync — i.e., bottlenecked on disk
fsync, not on application contention. This matches the §10.4 expectation
that low-overlap simple workloads hit the synchronous-commit ceiling for
Path A.

The 27% rollback rate on s1 (17927 / 66484) is consistent with the source_id
collision hypothesis above — at 2200 trx/s × 1 line / trx × 30s = 66k lines
drawn from 1M source_id space, collisions are non-zero.

### s2 lock-amortization signature visible

s2 (200 callers, Zipf(1.5) Simple) hits 544 trx/s at p50=4.4ms but
p99=1.1s, with Lock:tuple dominating waits. The long tail is consistent
with hot-pool serialization on the Zipf head ranks. The PG-side
rollbacks are low (864), meaning most failed submissions came from a
different path (likely sqlx error during the 200-caller concurrency).

### s6 has surprisingly high rollback ratio

s6 (200 callers, disjoint stripes, Simple) ran at 1544 trx/s but
rollbacks = 38874 (~83% of submissions). With true disjoint stripes,
there should be no pool_lock contention between callers. The dominant
wait LWLock:BufferContent suggests shared-buffer cache pressure rather
than tuple-lock contention. Likely traceable to (1) the 1000-pool universe
shrinking the stripe_size to 5, and (2) source_id collision rate scaling
with throughput. Follow-up investigation in cs5k-followup.

## What this first-pass establishes

- The full Path A pipeline (SPI → pool_lock → hydrate → plan_apply →
  bulk-write → commit) runs end-to-end against 200 concurrent callers
  for 30s without crashes or extension-side errors.
- The harness end-to-end (CLI → workload → driver → sampler → collector
  → JSON report) produces well-shaped output across all six scenarios.
- s1 baseline is the only "clean" measurement; other scenarios surface
  workload-generation bugs (source_id space too narrow) and PG-tuning
  prerequisites (max_connections cap, shared_buffers for 200-caller load).

## What gates the full run (cs5k-followup)

Issues to file as part of cs5k-followup:

1. Widen `workload::next_lines` source_id space from 10^6 to 10^12 (or
   gate per-caller like the driver's `caller_base + tick`).
2. Reseed pool universe at 10000 pools (drop existing fixture or add a
   `--force` flag to seed-pools to re-create at the requested count).
3. PG tuning: `max_connections >= 1100`, `shared_buffers` bump,
   container restart, postgresql.conf overlay.
4. Re-run with 60s warmup + 5-min measurement per design-v3 §11.
5. Reconcile the `Throughput=0.0` reporting artifact (rolled-back trx
   don't record into the hist; consider counting attempts vs commits
   separately in the JSON schema).

## Artifacts

- `s1-direct-2026-05-21T13-27-05.json` … `s6-direct-2026-05-21T13-27-05.json`
- Runner: `scripts/run-phase3-measurements.sh`
- Log: `/tmp/phase3-run.log` (transient)
