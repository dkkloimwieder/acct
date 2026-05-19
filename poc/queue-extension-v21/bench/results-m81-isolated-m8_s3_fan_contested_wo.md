# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s3_fan_contested_wo | 50 | 5 | 4 | 24 | 0 | 156353 | 219020 | 219020 | 1.17 | 24 |

## Per-shape detail

```
shape: s3_fan_contested_wo          g=50    K=5   N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=156353µs p99=219020µs p99.9=219020µs
  router: total_envelopes Δ=28 superbatch Δ=24 avg_eps=1.17
```


Generated: 2026-05-19T02:55:24Z
