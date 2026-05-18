# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s6_large_wo | 5000 | 15 | 332 | 24 | 6 | 55932 | 104772 | 104855 | 4.00 | 89 |

## Per-shape detail

```
shape: s6_large_wo                  g=5000  K=15  N=4 duration=60s
  envelopes: total=356 committed=332 failed=24
  throughput: 6 env/sec
  latency: p50=55932µs p99=104772µs p99.9=104855µs
  router: total_envelopes Δ=356 superbatch Δ=89 avg_eps=4.00
```


Generated: 2026-05-18T01:51:25Z
