# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s1_fan_out_simple | 5000 | 1 | 4108 | 0 | 68 | 54423 | 102526 | 103934 | 3.98 | 1033 |
| s2_fan_out_wo | 5000 | 5 | 993 | 20 | 17 | 54399 | 103434 | 204298 | 3.93 | 258 |
| s3_fan_contested_wo | 50 | 5 | 3 | 24 | 0 | 3106747 | 5264338 | 5264338 | 1.17 | 23 |
| s4_fan_in_wo | 1 | 5 | 1 | 24 | 0 | 2365028 | 2365028 | 2365028 | 1.00 | 25 |
| s5_hot_pool | 100 | 1 | 4 | 24 | 0 | 7605616 | 7610313 | 7610313 | 1.27 | 22 |
| s6_large_wo | 5000 | 15 | 4 | 24 | 0 | 2756367 | 2758684 | 2758684 | 4.00 | 7 |
| s7_very_large_wo | 5000 | 50 | 4 | 24 | 0 | 7512214 | 7514525 | 7514525 | 2.00 | 14 |
| s8_mixed_event_mixed_method | 1000 | 5 | 4 | 24 | 0 | 7928393 | 7928404 | 7928404 | 3.50 | 8 |
| s9_causal_chain | 10 | 5 | 1352 | 0 | 23 | 162059 | 258223 | 2235778 | 3.98 | 1019 |

## Per-shape detail

```
shape: s1_fan_out_simple            g=5000  K=1   N=4 duration=60s
  envelopes: total=4108 committed=4108 failed=0
  throughput: 68 env/sec
  latency: p50=54423µs p99=102526µs p99.9=103934µs
  router: total_envelopes Δ=4110 superbatch Δ=1033 avg_eps=3.98
```

```
shape: s2_fan_out_wo                g=5000  K=5   N=4 duration=60s
  envelopes: total=1013 committed=993 failed=20
  throughput: 17 env/sec
  latency: p50=54399µs p99=103434µs p99.9=204298µs
  router: total_envelopes Δ=1013 superbatch Δ=258 avg_eps=3.93
```

```
shape: s3_fan_contested_wo          g=50    K=5   N=4 duration=60s
  envelopes: total=27 committed=3 failed=24
  throughput: 0 env/sec
  latency: p50=3106747µs p99=5264338µs p99.9=5264338µs
  router: total_envelopes Δ=27 superbatch Δ=23 avg_eps=1.17
```

```
shape: s4_fan_in_wo                 g=1     K=5   N=4 duration=60s
  envelopes: total=25 committed=1 failed=24
  throughput: 0 env/sec
  latency: p50=2365028µs p99=2365028µs p99.9=2365028µs
  router: total_envelopes Δ=25 superbatch Δ=25 avg_eps=1.00
```

```
shape: s5_hot_pool                  g=100   K=1   N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=7605616µs p99=7610313µs p99.9=7610313µs
  router: total_envelopes Δ=28 superbatch Δ=22 avg_eps=1.27
```

```
shape: s6_large_wo                  g=5000  K=15  N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=2756367µs p99=2758684µs p99.9=2758684µs
  router: total_envelopes Δ=28 superbatch Δ=7 avg_eps=4.00
```

```
shape: s7_very_large_wo             g=5000  K=50  N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=7512214µs p99=7514525µs p99.9=7514525µs
  router: total_envelopes Δ=28 superbatch Δ=14 avg_eps=2.00
```

```
shape: s8_mixed_event_mixed_method  g=1000  K=5   N=4 duration=60s
  envelopes: total=28 committed=4 failed=24
  throughput: 0 env/sec
  latency: p50=7928393µs p99=7928404µs p99.9=7928404µs
  router: total_envelopes Δ=28 superbatch Δ=8 avg_eps=3.50
  event_mix: wo_complete=42.9%
  event_mix: inv_adjust=57.1%
```

```
shape: s9_causal_chain              g=10    K=5   N=4 duration=60s
  envelopes: total=1352 committed=1352 failed=0
  throughput: 23 env/sec
  latency: p50=162059µs p99=258223µs p99.9=2235778µs
  router: total_envelopes Δ=4056 superbatch Δ=1019 avg_eps=3.98
  s9_margin: 0
```


Generated: 2026-05-18T01:43:13Z

## Notes (M8.1 wrap-up, acct-toj6)

### acct-0frn impact

The all-shapes sequential run above is **cross-test contaminated** by [acct-0frn]
(committer hangs on sequential `wo_complete` envelopes with overlapping component
SKUs). Once the bug fires in S3 or S4, leftover `in_flight=N` state in shmem
prevents `reset_state` from settling within its 15s timeout. Subsequent shapes
(S5–S8 in this run) see only ~28 envelopes routed before their backends time
out.

Per-shape isolated runs (docker-restart between, see
`results-m81-isolated-<shape>.md`) give the **clean** per-shape numbers:

| shape | committed | failed | tput evps | p50 µs | p99 µs | notes |
|---|---|---|---|---|---|---|
| s1_fan_out_simple | 4108 | 0 | 68 | 54k | 102k | clean (above) |
| s2_fan_out_wo | 993 | 20 | 17 | 54k | 103k | stride workaround |
| s3_fan_contested_wo | 3 | 24 | 0 | 3106k | 5264k | **blocked acct-0frn** |
| s4_fan_in_wo | 1 | 24 | 0 | 2365k | 2365k | **blocked acct-0frn** |
| s5_hot_pool (isolated) | 4285 | 0 | 71 | 60k | 73k | clean |
| s6_large_wo (isolated) | 332 | 24 | 6 | 56k | 105k | acct-0frn wraps at iter 333 |
| s7_very_large_wo (isolated) | 58 | 14 | 1 | 145k | 5190k | acct-0frn wraps at iter 100 |
| s8_mixed_event (isolated) | 84 | 12 | 1 | 121k | 5337k | wo_complete half hits bug |
| s9_causal_chain margin=0 | 1352 | 0 | 23 | 162k | 258k | clean (1352 triplets = 4056 events) |

### Calibration

`bench/calibration-m81.md` — fsync p99=4.9ms → keep
`poc_v21.committer_lease_ms = 100` (default). Cold-vs-warm S1 delta -0.9% on
NVMe; no measurable cold-start penalty.

### S9 cascade invariant

`bench/results-m81-s9-margin-neg1.md` — N=4 30s with margin=-1 (WO consumes 11
of a layer holding 10). 50/58 triplets cascade-success (E2 explicitly reaches
state='failed', E3 not submitted). 8 triplets timed out in wait_terminal_single
at 10s rather than reaching explicit 'failed' state — likely a related but
distinct slow path for InsufficientInventory failures on `wo_complete`; tracked
under the same acct-0frn investigation.

### What ships at M8.1

- 9-shape workload-generator harness (`tests/common/m8_runner.rs`, ~760 lines).
- Per-shape entry-points (`tests/bench_m8_workloads.rs`).
- Pre-bake-off calibration (`tests/bench_m8_calibration.rs`).
- Per-shape N=4 60s runs captured in `bench/results-m81-*.md`.
- S9 margin=0 (must-commit) and margin=-1 (must-cascade-fail) invariants asserted
  in `m8_s8_mixed_event_mixed_method` and `m8_s9_causal_chain` test bodies.

### Gates on M8.2

- acct-0frn must close before M8.2's statistical runner can produce non-degenerate
  numbers for S3, S4, and the wo_complete portion of S6/S7/S8. The v2.1 epic
  acct-gx1z now has acct-0frn in its blocks list.

[acct-0frn]: ../../../.beads/  "committer hangs on sequential wo_complete envelopes with overlapping component SKUs"
