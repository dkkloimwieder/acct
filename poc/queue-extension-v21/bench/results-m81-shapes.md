# M8.1 (acct-toj6) — workload-generator shapes

Per-shape run at N=4 backends × duration=60s. Workload-generator harness `tests/bench_m8_workloads.rs`.

Latency = enqueue → submission_status terminal-state observed. Submit-and-poll per backend (single outstanding envelope).

## Summary (post-acct-jxus, 2026-05-19)

| shape | g | K | committed | failed | throughput evps | p50 µs | p99 µs | p99.9 µs | avg eps/sb | sb count |
|---|---|---|---|---|---|---|---|---|---|---|
| s1_fan_out_simple | 5000 | 1 | 4108 | 0 | 68 | 54423 | 102526 | 103934 | 3.98 | 1033 |
| s2_fan_out_wo | 5000 | 5 | 4084 | 0 | 68 | 60801 | 74918 | 156518 | 1.00 | 4084 |
| s3_fan_contested_wo | 50 | 5 | 3936 | 0 | 66 | 59777 | 106690 | 112380 | 2.14 | 1843 |
| s4_fan_in_wo | 1 | 5 | 2200 | 0 | 37 | 66088 | 495829 | 711795 | 3.74 | 588 |
| s5_hot_pool | 100 | 1 | 4285 | 0 | 71 | (per acct-shpc.6 prior run) | | | | |
| s6_large_wo | 5000 | 15 | 2834 | 0 | 47 | 60627 | 416495 | 467635 | 1.00 | 2834 |
| s7_very_large_wo | 5000 | 50 | 3963 | 0 | 66 | 63203 | 81859 | 139240 | 1.00 | 3963 |
| s8_mixed_event_mixed_method | 1000 | 5 | 4229 | 0 | 70 | 57444 | 70712 | 124786 | 1.01 | 4210 |
| s9_causal_chain | 10 | 5 | 1352 | 0 | 23 | 162059 | 258223 | 2235778 | 3.98 | 1019 |

S1/S5/S9 unchanged from prior measurement (K=1 or unaffected by
wo_complete fix). S2/S3/S4/S6/S7/S8 reflect post-jxus
isolated-per-shape runs (docker-restart between).

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
`results-m81-isolated-<shape>.md`) give the **clean** per-shape numbers.
**Original measurements were captured under the stride workaround in
`tests/common/m8_runner.rs::build_components`.** The acct-shpc.6
revert of that workaround surfaced a deeper committer bug (acct-jxus —
wo_complete component depletions all stamped `issue_id=0`, colliding
on `cost_depletions` UNIQUE `(issue_id, method_used, layer_id)` as soon
as any two envelopes touched the same layer; the committer crashed,
got respawned, and crash-looped).

After the acct-jxus fix (per-component issue_id derived from
`document_id × ISSUE_ID_COMPONENT_STRIDE + sub_priority` in
`expand_wo_complete_payload`), spec-aligned consecutive-stride
measurements (post-acct-zplt router union-find affinity grouping):

| shape | pre-shpc.6 stride / committed | post-shpc.6 + jxus consecutive / committed | tput Δ | notes |
|---|---|---|---|---|
| s1_fan_out_simple | 4108 / 68 evps | (unchanged — K=1, not affected) | — | clean |
| s2_fan_out_wo | 993 / 17 evps | 4084 / 68 evps | **4.1× improvement** | per-component issue_id fix |
| s3_fan_contested_wo | 3 / 0 evps | 3936 / 66 evps | **1312× improvement** | union-find packs hot range, avg_eps=2.14 |
| s4_fan_in_wo | 1 / 0 evps | 2200 / 37 evps | **2200× improvement** | g=1 fan-in packs avg_eps=3.74 |
| s5_hot_pool | 4285 / 71 evps | (unchanged — K=1, no wo_complete) | — | clean |
| s6_large_wo | 332 / 6 evps | 2834 / 47 evps | **8.5× improvement** | K=15 wo_complete fully unblocked |
| s7_very_large_wo | 58 / 1 evps | 3963 / 66 evps | **68× improvement** | K=50 wo_complete fully unblocked |
| s8_mixed_event | 84 / 1 evps | 4229 / 70 evps | **50× improvement** | both wo_complete and inv_adjust halves clean (50/50) |
| s9_causal_chain margin=0 | 1352 / 23 evps | (unchanged) | — | clean (1352 triplets = 4056 events) |

### Falsifier finding + fix (acct-shpc.6 → acct-jxus, 2026-05-19)

Reverting the stride workaround exposed a committer-side bug:
`expand_wo_complete_payload` initialized `issue_id=0` for ALL K+1
events of every wo_complete envelope. The `cost_depletions` table's
UNIQUE `(issue_id, method_used, layer_id)` constraint then collided as
soon as any two wo_complete depletions across the session touched the
same layer. The duplicate-key error aborted the sub-tx; the BGWorker
exited with code 1; postmaster respawned it after 5s (per
`set_restart_time`); the next claim hit the same bug. Result: per-run
crash loop with `committer_takeover_count=48 / 60s` (~1 every 1.25s).

`pg_stat_activity` snapshots during the hang showed **zero
poc_v21_* locks held** — the hang wasn't lock contention at all. The
real signal was `count_ready=16` envelopes sitting unclaimed in the
committer queue while every claim attempt died on the constraint
violation.

The router union-find (acct-zplt) was grouping correctly all along;
acct-jxus was a parallel correctness bug masked by the pre-revert
stride workaround (which kept components pool-disjoint across the SKU
range, so each wo_complete's K=5 depletions hit different layers that
no other envelope's depletions had reached yet).

Fix: per-component issue_id = `document_id ×
ISSUE_ID_COMPONENT_STRIDE + sub_priority` (stride = 100_000, K_MAX
headroom ≤ 99_999, document_id ≤ ~9.2e13 stays in i64 range). Output
event uses `stride - 1` to stay distinct from any component slot.
Implemented in `committer.rs::expand_wo_complete_payload`.

acct-shpc.6's bench-validation gate passes: S2 and S6 both > 100
committed (4084 and 2834 respectively); S3, S4, S7, S8 all also
surged 50× – 2200×.

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

- acct-jxus (per-component issue_id collision) closed 2026-05-19. All
  wo_complete shapes now produce non-degenerate numbers under the
  spec-aligned consecutive-stride form. M8.2's statistical runner can
  proceed.
- acct-0frn (committer hangs on sequential wo_complete with
  overlapping components) is partially subsumed by acct-jxus. The
  remaining concern under acct-0frn — InsufficientInventory-failure
  slow paths surfaced by S9 margin=-1 — is independent and still
  tracked.

[acct-0frn]: ../../../.beads/  "committer hangs on sequential wo_complete envelopes with overlapping component SKUs"
