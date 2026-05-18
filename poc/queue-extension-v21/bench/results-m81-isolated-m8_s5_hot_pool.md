# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s5_hot_pool | 100 | 1 | 4285 | 0 | 71 | 59674 | 73170 | 99830 | 1.23 | 3488 |

## Per-shape detail

```
shape: s5_hot_pool                  g=100   K=1   N=4 duration=60s
  envelopes: total=4285 committed=4285 failed=0
  throughput: 71 env/sec
  latency: p50=59674µs p99=73170µs p99.9=99830µs
  router: total_envelopes Δ=4289 superbatch Δ=3488 avg_eps=1.23
```


Generated: 2026-05-18T01:50:13Z
