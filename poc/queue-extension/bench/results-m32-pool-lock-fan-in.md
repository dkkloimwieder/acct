# M3.2 (acct-4d4n.8) Q-A pool-lock granularity — fan_in bench

Workload: 8 concurrent backends, single (sku=4242, loc=1) pool, method='mock', duration 15s per run, 3 runs per cell.

Throughput is rows-into-poc_test_rows divided by wall-clock elapsed (each apply produces one row).

## Per-run throughput (transfers/s)

| mode | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| none | 612 | 747 | 775 | **747** |
| pool_locks | 747 | 722 | 743 | **743** |
| pool_lock_anchors | 741 | 731 | 557 | **731** |

Generated: 2026-05-15T13:57:17Z

## Verdict — NO SIGNAL

Medians span 731–747 tps (~2% spread). Per-mode ranges:

- `none`: 612–775 (range 163, IQR ~30 if dropping the 612 low outlier)
- `pool_locks`: 722–747 (range 25 — tightest)
- `pool_lock_anchors`: 557–741 (range 184, dragged by one 557 outlier)

Every mode's IQR contains every other mode's median. The M3.2 acceptance
criterion "one wins outside the other's IQR" is NOT met — the three
modes are statistically indistinguishable at this workload shape.

### Why pool-lock is redundant at M3 scope

At single-shard fan_in (the workload Q-A is concerned with), M3.1's
committer-PID CAS election already serializes committer-to-committer
work within a shard:

- Only one backend can hold `committer_pid` at a time.
- A pool maps deterministically to one shard via `pool_hash & (shard_count - 1)`.
- Therefore at M3.x scope, "committer-to-committer on same pool" =
  "committer-to-committer on same shard" = already serialized by CAS.

The SQL-level pool-lock adds an extra SPI (INSERT-ON-CONFLICT-DO-UPDATE)
per drain without changing the contention envelope. The committer-PID
CAS is faster than an SQL row-lock; it's the load-bearing serialization
primitive.

### When pool-lock might matter

Cross-shard pool collisions only become possible if multiple shards
process events targeting the same `(sku, location)`. That happens if:

1. **M4.1 multi-shard hash routing introduces cross-shard collisions** —
   the current hash routes one pool to one shard, so this is not yet
   reachable. Re-evaluate Q-A at M4.1.
2. **Cost methods read state shared across pools** (e.g., a cross-pool
   parent-SKU cost roll-up). Not in the PoC scope; deferred to
   design-v2.

### Decision

- Default `poc_ledger.pool_lock_mode = 'none'` (committed in the GUC
  registration). Wins by ~2% median + tied range bottom; lightest SPI
  surface; the CAS election already serializes.
- Retain `pool_locks` and `pool_lock_anchors` behind the GUC for
  re-measurement at M4.1 once cross-shard scenarios exist.
- Spec §7 Q-A resolution noted alongside the table file (see
  `poc/design_research/poc-validation-spec.md`).

## What the bench does NOT cover

- M4.1 multi-shard cross-pool collisions (Q-A's real motivating
  scenario; currently unreachable at M3 scope).
- Non-mock cost methods (FIFO/AVG/STD) where snapshot reads under
  FOR UPDATE on cost-table rows already provide row-level
  serialization independent of the pool-lock. Mock skips snapshot
  build, so this bench isolates the pool-lock effect cleanly.
- Cross-method batches (M2.1's grouping by `(pool, method)` would
  split into multiple `process_group` calls — each independently
  acquires the pool-lock; minor amplification at high method-mix
  workloads).
- M5b lease takeover under killed-committer scenarios (separate
  failure path; out of M3.2 scope).
