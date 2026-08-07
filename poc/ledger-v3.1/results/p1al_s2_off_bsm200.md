# Sustained routed workload — s2 (300s)

Workload: s2 · mode=routed · GUCs: affinity_scheme=0,batch_size_max=200,batch_window_us=20000,committer_count=4,router_pack_disjoint=off

Throughput (window delta): 4,934 trx/s · commit_group_size_avg=8.1 · drains=182,177 · trx_committed=1,480,300

lines/trx: trx_line=1.00, posting_line=1.00


## Throughput-rate distribution (per-second, n=274 samples, ramp/drain trimmed)

| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| trx/s | 2,273.5 | 5,112.0 | 5,288.0 | 5,459.2 | 5,715.1 | 8,290.0 | 5,061.2 | 859.2 |
| trx_line/s | 2,273.5 | 5,112.0 | 5,288.0 | 5,459.2 | 5,715.1 | 8,290.0 | 5,061.2 | 859.2 |
| posting_line/s | 2,273.5 | 5,112.0 | 5,288.0 | 5,459.2 | 5,715.1 | 8,290.0 | 5,061.2 | 859.2 |

## Caller latency (µs)

| | p50 | p95 | p99 |
|---|--:|--:|--:|
| ack (enqueue) | 157,155 | 309,067 | 390,594 |
| committed (end-to-end) | 3,454,009 | 15,946,743 | 58,787,364 |

## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)

Total committer txn wall-time over the window: 926.43s across 1,480,300 trx (committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).

| span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| pool_lock | 16.31 | 1.8% | 11.02 |
| hydrate | 75.56 | 8.2% | 51.05 |
| apply | 164.58 | 17.8% | 111.18 |
| commit(fsync) | 477.83 | 51.6% | 322.79 |
| prep | 192.14 | 20.7% | 129.80 |

_prep refold (acct-e95d):_

| sub-span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| prep.decode | 156.32 | 16.9% | 105.60 |
| prep.xact | 20.37 | 2.2% | 13.76 |
| prep.dedup | 12.62 | 1.4% | 8.53 |
| prep.other | 2.83 | 0.3% | 1.91 |

## Wait-event segmentation (committer pg_stat_activity sampler)

- samples: 12,044 (idle 88)

- busy_frac: 0.993  (committer utilization; low ⇒ ceiling is upstream/ingress)

- of busy — on-CPU: 0.274 · row-lock: 0.001 · shmem-LWLock: 0.533


## Resilience counters
- pool_lock_acquisitions=182,177 (window) · aggregate_upserts=182,177 (window)
- (cumulative since restart) dedup_skips=0 · dropped=0 · tx_failures=0 · poisoned=0 · deadlock_retries=0 · takeover=0

