# Sustained routed workload — s2 (300s)

Workload: s2 · mode=routed · GUCs: affinity_scheme=0,batch_size_max=200,batch_window_us=20000,committer_count=4

Throughput (window delta): 4,856 trx/s · commit_group_size_avg=8.1 · drains=179,720 · trx_committed=1,456,900

lines/trx: trx_line=1.00, posting_line=1.00


## Throughput-rate distribution (per-second, n=272 samples, ramp/drain trimmed)

| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| trx/s | 1,673.0 | 5,040.8 | 5,314.5 | 5,464.0 | 5,639.9 | 8,539.0 | 5,043.4 | 898.1 |
| trx_line/s | 1,673.0 | 5,040.8 | 5,314.5 | 5,464.0 | 5,639.9 | 8,539.0 | 5,043.4 | 898.1 |
| posting_line/s | 1,673.0 | 5,040.8 | 5,314.5 | 5,464.0 | 5,639.9 | 8,539.0 | 5,043.4 | 898.1 |

## Caller latency (µs)

| | p50 | p95 | p99 |
|---|--:|--:|--:|
| ack (enqueue) | 159,514 | 310,640 | 403,701 |
| committed (end-to-end) | 3,447,717 | 17,213,423 | 58,049,167 |

## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)

Total committer txn wall-time over the window: 927.22s across 1,456,900 trx (committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).

| span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| pool_lock | 17.68 | 1.9% | 12.14 |
| hydrate | 74.97 | 8.1% | 51.46 |
| apply | 168.19 | 18.1% | 115.44 |
| commit(fsync) | 478.99 | 51.7% | 328.77 |
| prep | 187.39 | 20.2% | 128.62 |

_prep refold (acct-e95d):_

| sub-span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| prep.decode | 152.34 | 16.4% | 104.56 |
| prep.xact | 19.72 | 2.1% | 13.53 |
| prep.dedup | 12.54 | 1.4% | 8.60 |
| prep.other | 2.80 | 0.3% | 1.92 |

## Wait-event segmentation (committer pg_stat_activity sampler)

- samples: 12,020 (idle 92)

- busy_frac: 0.992  (committer utilization; low ⇒ ceiling is upstream/ingress)

- of busy — on-CPU: 0.277 · row-lock: 0.001 · shmem-LWLock: 0.527


## Resilience counters
- pool_lock_acquisitions=179,720 (window) · aggregate_upserts=179,720 (window)
- (cumulative since restart) dedup_skips=0 · dropped=0 · tx_failures=0 · poisoned=0 · deadlock_retries=0 · takeover=0

