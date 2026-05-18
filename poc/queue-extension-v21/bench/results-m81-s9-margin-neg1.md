# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=30s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s9_causal_chain | 10 | 5 | 50 | 8 | 2 | 165206 | 4921799 | 4921799 | 3.92 | 38 |

## Per-shape detail

```
shape: s9_causal_chain              g=10    K=5   N=4 duration=30s
  envelopes: total=58 committed=50 failed=8
  throughput: 2 env/sec
  latency: p50=165206µs p99=4921799µs p99.9=4921799µs
  router: total_envelopes Δ=149 superbatch Δ=38 avg_eps=3.92
  s9_margin: -1
```


Generated: 2026-05-18T01:48:42Z
