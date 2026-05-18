# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s8_mixed_event_mixed_method | 1000 | 5 | 84 | 12 | 1 | 121473 | 5337786 | 5337788 | 3.84 | 25 |

## Per-shape detail

```
shape: s8_mixed_event_mixed_method  g=1000  K=5   N=4 duration=60s
  envelopes: total=96 committed=84 failed=12
  throughput: 1 env/sec
  latency: p50=121473µs p99=5337786µs p99.9=5337788µs
  router: total_envelopes Δ=96 superbatch Δ=25 avg_eps=3.84
  event_mix: inv_adjust=50.0%
  event_mix: wo_complete=50.0%
```


Generated: 2026-05-18T01:53:39Z
