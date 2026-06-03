# Sustained routed workload — s2 (300s)

Workload: s2 · mode=routed · GUCs: affinity_scheme=0,batch_size_max=50,batch_window_us=20000,committer_count=4,router_pack_disjoint=on

Throughput (window delta): 7,682 trx/s · commit_group_size_avg=42.4 · drains=54,304 · trx_committed=2,304,700

lines/trx: trx_line=1.00, posting_line=1.00


## Throughput-rate distribution (per-second, n=270 samples, ramp/drain trimmed)

| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| trx/s | 3,934.5 | 8,120.2 | 8,282.5 | 8,498.5 | 9,408.2 | 13,147.0 | 7,993.6 | 1,382.0 |
| trx_line/s | 3,934.5 | 8,120.2 | 8,282.5 | 8,498.5 | 9,408.2 | 13,147.0 | 7,993.6 | 1,382.0 |
| posting_line/s | 3,934.5 | 8,120.2 | 8,282.5 | 8,498.5 | 9,408.2 | 13,147.0 | 7,993.6 | 1,382.0 |

## Caller latency (µs)

| | p50 | p95 | p99 |
|---|--:|--:|--:|
| ack (enqueue) | 97,648 | 205,389 | 266,469 |
| committed (end-to-end) | 3,051,356 | 3,898,605 | 4,238,344 |

## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)

Total committer txn wall-time over the window: 705.09s across 2,304,700 trx (committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).

| span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| pool_lock | 130.84 | 18.6% | 56.77 |
| hydrate | 34.83 | 4.9% | 15.11 |
| apply | 231.65 | 32.9% | 100.51 |
| commit(fsync) | 120.55 | 17.1% | 52.30 |
| prep | 187.22 | 26.6% | 81.24 |

_prep refold (acct-e95d):_

| sub-span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| prep.decode | 162.80 | 23.1% | 70.64 |
| prep.xact | 9.62 | 1.4% | 4.18 |
| prep.dedup | 12.47 | 1.8% | 5.41 |
| prep.other | 2.33 | 0.3% | 1.01 |

## Wait-event segmentation (committer pg_stat_activity sampler)

- samples: 12,036 (idle 128)

- busy_frac: 0.989  (committer utilization; low ⇒ ceiling is upstream/ingress)

- of busy — on-CPU: 0.280 · row-lock: 0.104 · shmem-LWLock: 0.535


## Resilience counters
- pool_lock_acquisitions=252,798 (window) · aggregate_upserts=252,798 (window)
- (cumulative since restart) dedup_skips=0 · dropped=0 · tx_failures=0 · poisoned=0 · deadlock_retries=0 · takeover=0

