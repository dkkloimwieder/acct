# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s2_fan_out_wo | 5000 | 5 | 4 | 24 | 0 | 62125 | 63924 | 63924 | 1.00 | 28 |

## Per-shape detail

```
shape: s2_fan_out_wo                g=5000  K=5   N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=62125µs p99=63924µs p99.9=63924µs
  router: total_envelopes Δ=28 superbatch Δ=28 avg_eps=1.00
```


Generated: 2026-05-19T03:03:42Z
