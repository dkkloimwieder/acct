# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s8_mixed_event_mixed_method | 1000 | 5 | 172 | 11 | 3 | 63488 | 5453789 | 9528774 | 1.00 | 183 |

## Per-shape detail

```
shape: s8_mixed_event_mixed_method  g=1000  K=5   N=4 duration=60s
  envelopes: total=183 committed=172 failed=11
  throughput: 3 env/sec
  latency: p50=63488µs p99=5453789µs p99.9=9528774µs
  router: total_envelopes Δ=183 superbatch Δ=183 avg_eps=1.00
  event_mix: inv_adjust=50.3%
  event_mix: wo_complete=49.7%
```


Generated: 2026-05-19T02:59:53Z
