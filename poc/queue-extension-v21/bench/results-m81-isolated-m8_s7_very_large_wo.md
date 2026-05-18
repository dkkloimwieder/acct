# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s7_very_large_wo | 5000 | 50 | 58 | 14 | 1 | 145399 | 5190075 | 5190080 | 2.00 | 36 |

## Per-shape detail

```
shape: s7_very_large_wo             g=5000  K=50  N=4 duration=60s
  envelopes: total=72 committed=58 failed=14
  throughput: 1 env/sec
  latency: p50=145399µs p99=5190075µs p99.9=5190080µs
  router: total_envelopes Δ=72 superbatch Δ=36 avg_eps=2.00
```


Generated: 2026-05-18T01:52:32Z
