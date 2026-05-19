# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s6_large_wo | 5000 | 15 | 4 | 24 | 0 | 52504 | 99535 | 99535 | 1.00 | 28 |

## Per-shape detail

```
shape: s6_large_wo                  g=5000  K=15  N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=52504µs p99=99535µs p99.9=99535µs
  router: total_envelopes Δ=28 superbatch Δ=28 avg_eps=1.00
```


Generated: 2026-05-19T02:57:38Z
