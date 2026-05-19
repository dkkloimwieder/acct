# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s4_fan_in_wo | 1 | 5 | 0 | 24 | 0 | 0 | 0 | 0 | 4.00 | 6 |

## Per-shape detail

```
shape: s4_fan_in_wo                 g=1     K=5   N=4 duration=60s
  envelopes: total=24 committed=0 failed=24
  throughput: 0 env/sec
  latency: p50=0µs p99=0µs p99.9=0µs
  router: total_envelopes Δ=24 superbatch Δ=6 avg_eps=4.00
```


Generated: 2026-05-19T02:56:31Z
