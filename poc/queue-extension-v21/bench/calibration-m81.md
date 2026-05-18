# M8.1 (acct-toj6) — pre-bake-off calibration

Generated: 2026-05-18T01:47:29Z

## fsync latency probe

Probe: 100 sequential 1-byte INSERT+COMMIT on a throwaway table with `synchronous_commit=on`.

| metric | value µs |
|---|---|
| p50 | 2056 |
| p99 | 4870 |
| p99.9 | 4873 |

**Recommendation**: `poc_v21.committer_lease_ms = 100` (= max(100, 10 × p99_ms=5)).

## cold-start vs steady-state

Shape: S1 fan_out_simple at N=4 duration=10s. Cold = empty pool_locks; warm = pool_locks pre-populated.

| run | throughput ev/s |
|---|---|
| cold | 67 |
| warm | 67 |

**Steady-state delta**: -0.9%. Bake-off harness should discard the first 5s of every cell to clear UPSERT-insert overhead.
