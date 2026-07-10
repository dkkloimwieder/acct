# ledger-v3.1 crossover re-measurement — statistics discipline (acct-0at4.10.4)

_`s5/s7 from run1 (2026-07-10T12-18-43), s2/s4 from run2; 5 reps/cell, distinct --seed, shuffled, quiet-gated, DURATION=30s, method=all-fifo, pgbouncer pool=24`_

Each cell is **N independent reps** at distinct `--seed` values (so the workload streams differ rep-to-rep), run in **shuffled order** under the `wait_for_quiet_host` gate. Point = median of the reps; band = **percentile bootstrap 95% CI** (2000 resamples, fixed PRNG). The production decision consumes the band, not a lone number.


## Per-cell throughput (median ± bootstrap 95% CI)

| scenario | mode | n | median trx/s | 95% CI | min | max |
|---|---|--:|--:|--:|--:|--:|
| s5 | direct-per-call | 5 | 404.3 | [156.9, 406.0] | 156.9 | 406.0 |
| s5 | routed | 5 | 8962.2 | [8642.2, 9295.8] | 8642.2 | 9295.8 |
| s7 | direct-per-call | 5 | 1173.9 | [1157.9, 1192.5] | 1157.9 | 1192.5 |
| s7 | routed | 5 | 8195.3 | [4512.0, 8587.1] | 4512.0 | 8587.1 |
| s2 | direct-per-call | 5 | 731.4 | [723.8, 739.0] | 723.8 | 739.0 |
| s2 | routed | 5 | 9143.7 | [8873.2, 9405.2] | 8873.2 | 9405.2 |
| s4 | direct-per-call | 5 | 267.1 | [264.0, 273.7] | 264.0 | 273.7 |
| s4 | routed | 5 | 1304.7 | [986.6, 1351.1] | 986.6 | 1351.1 |

## Crossover verdict — routed vs direct-per-call

| scenario | direct med | routed med | ratio r/d | MWU p (2-sided) | CIs disjoint? | verdict |
|---|--:|--:|--:|--:|:--:|---|
| s5 | 404.3 | 8962.2 | 22.17× | 0.0122 | yes | **routed** (CIs separated) |
| s7 | 1173.9 | 8195.3 | 6.98× | 0.0122 | yes | **routed** (CIs separated) |
| s2 | 731.4 | 9143.7 | 12.50× | 0.0122 | yes | **routed** (CIs separated) |
| s4 | 267.1 | 1304.7 | 4.88× | 0.0122 | yes | **routed** (CIs separated) |

## Stated steady-state rule

- **Per-rep throughput** (`throughput_trx_per_sec`) is measured over the harness's post-barrier window: every caller rendezvous at a start barrier before the timer starts, so intra-run connection/warmup ramp is excluded by construction. Each rep is therefore one steady-state sample; the sample unit for the CI is the **rep**, not a sub-run interval.
- **Time-series consumers** (sustained/drift per-interval rate series) use `steady_state_window(cov_thresh=0.05)`: discard the leading samples until the rolling coefficient-of-variation stays below 5%, deriving the warmup cut from the data instead of the fixed `rates[3:]` + tail-drop heuristic.

