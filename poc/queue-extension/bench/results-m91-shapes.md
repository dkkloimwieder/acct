# M9.1 (acct-4d4n.20) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m9_workloads.rs`.

Methods: shapes 1–5 use `mock` (queue+committer isolation; matches M4.1 convention). Shape 6 rotates `fifo`→`avg`→`std` per call.

Latency captured via per-call `Instant::now()` deltas in microseconds, sorted-then-percentile aggregated. Classifier label sourced from `poc_ledger_bottleneck_classify` over the start/end snapshot pair for the workload window.

## Summary

| shape | g | applies | errors | throughput evps | p50 µs | p99 µs | p99.9 µs | classifier |
|---|---|---|---|---|---|---|---|---|
| fan_in | 1 | 96018 | 0 | 1600 | 2910 | 4593 | 7739 | idle |
| fan_out | 5000 | 65251 | 0 | 1088 | 3638 | 5773 | 38170 | idle |
| balanced | 50 | 65916 | 0 | 1099 | 3641 | 5501 | 30102 | idle |
| zipfian | 1000 | 65882 | 0 | 1098 | 3641 | 5972 | 30635 | idle |
| small_batch | 50 | 65745 | 0 | 1096 | 3652 | 5559 | 26562 | idle |
| mixed_method | 50 | 64573 | 0 | 1076 | 3654 | 6139 | 35738 | idle |

## Per-shape detail

```
shape: fan_in         g=1     N=4 duration=60s
  applies: 96018 (errors: 0)
  throughput: 1600 events/sec
  latency: p50=2910µs p99=4593µs p99.9=7739µs
  bottleneck_classify: idle
```

```
shape: fan_out        g=5000  N=4 duration=60s
  applies: 65251 (errors: 0)
  throughput: 1088 events/sec
  latency: p50=3638µs p99=5773µs p99.9=38170µs
  bottleneck_classify: idle
```

```
shape: balanced       g=50    N=4 duration=60s
  applies: 65916 (errors: 0)
  throughput: 1099 events/sec
  latency: p50=3641µs p99=5501µs p99.9=30102µs
  bottleneck_classify: idle
```

```
shape: zipfian        g=1000  N=4 duration=60s
  applies: 65882 (errors: 0)
  throughput: 1098 events/sec
  latency: p50=3641µs p99=5972µs p99.9=30635µs
  bottleneck_classify: idle
```

```
shape: small_batch    g=50    N=4 duration=60s
  applies: 65745 (errors: 0)
  throughput: 1096 events/sec
  latency: p50=3652µs p99=5559µs p99.9=26562µs
  bottleneck_classify: idle
```

```
shape: mixed_method   g=50    N=4 duration=60s
  applies: 64573 (errors: 0)
  throughput: 1076 events/sec
  latency: p50=3654µs p99=6139µs p99.9=35738µs
  bottleneck_classify: idle
  method mix: fifo=33.3% avg=33.3% std=33.3%
```

## Notes

- All 6 shapes complete cleanly at N=4×60s with 0 errors per shape.
- Shapes 1–5 (mock dispatch) cluster at ~1.1K evps; fan_in (g=1) is highest at 1.6K
  because all 4 backends hit a single shard so committer hand-off stays warm.
- Per-call p50 ~3.6ms is dominated by sqlx round-trip + commit; intra-extension
  work is small relative to network/commit.
- Classifier returns `idle` for all shapes at this load magnitude. B3
  (plan_apply CPU) is sub-millisecond per call so the 60s aggregate (~few
  million ns) is <0.01% of the 60-billion-ns wall window. B5 wake-latency
  fires only when an apply parks waiting for a peer-drain. At N=4 with
  one shard per shape (fan_in) or shards spread across 16 (fan_out) most
  apply calls inline-drain on their own committer election. Non-idle
  classifications expected to appear at higher N (M9.2 sweep) where peer
  contention is real.
- Method mix in shape 6 hits exactly 33.3%/33.3%/33.3% under per-call
  uniform rotation (`["fifo","avg","std"][iter % 3]`). The
  ±5% assertion in `m9_mixed_method` is a regression net against drift
  in `pick_method`.

Generated: 2026-05-15T22:41:42Z
