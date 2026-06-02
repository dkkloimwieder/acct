# Sustained routed workload — s2 (300s)

Workload: s2 · mode=routed · GUCs: affinity_scheme=0,batch_size_max=200,batch_window_us=20000,committer_count=1

Throughput (window delta): 14,517 trx/s · commit_group_size_avg=180.5 · drains=24,121 · trx_committed=4,355,000

lines/trx: trx_line=1.00, posting_line=1.00


## Throughput-rate distribution (per-second, n=278 samples, ramp/drain trimmed)

| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| trx/s | 5,096.0 | 14,303.5 | 14,765.0 | 16,984.0 | 17,416.0 | 17,752.0 | 14,916.2 | 2,473.7 |
| trx_line/s | 5,096.0 | 14,303.5 | 14,765.0 | 16,984.0 | 17,416.0 | 17,752.0 | 14,916.2 | 2,473.7 |
| posting_line/s | 5,096.0 | 14,303.5 | 14,765.0 | 16,984.0 | 17,416.0 | 17,752.0 | 14,916.2 | 2,473.7 |

## Caller latency (µs)

| | p50 | p95 | p99 |
|---|--:|--:|--:|
| ack (enqueue) | 831 | 11,804 | 14,843 |
| committed (end-to-end) | 1,026,555 | 1,134,559 | 1,264,582 |

## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)

Total committer txn wall-time over the window: 299.15s across 4,355,000 trx (committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).

| span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| pool_lock | 2.01 | 0.7% | 0.46 |
| hydrate | 7.14 | 2.4% | 1.64 |
| apply | 216.45 | 72.4% | 49.70 |
| commit(fsync) | 53.70 | 18.0% | 12.33 |
| prep | 19.85 | 6.6% | 4.56 |

_prep refold (acct-e95d):_

| sub-span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| prep.decode | 5.68 | 1.9% | 1.31 |
| prep.xact | 3.45 | 1.2% | 0.79 |
| prep.dedup | 9.47 | 3.2% | 2.17 |
| prep.other | 1.25 | 0.4% | 0.29 |

## Wait-event segmentation (committer pg_stat_activity sampler)

- samples: 3,002 (idle 22)

- busy_frac: 0.993  (committer utilization; low ⇒ ceiling is upstream/ingress)

- of busy — on-CPU: 0.832 · row-lock: 0.000 · shmem-LWLock: 0.004


## Resilience counters
- pool_lock_acquisitions=24,121 (window) · aggregate_upserts=24,121 (window)
- (cumulative since restart) dedup_skips=0 · dropped=0 · tx_failures=0 · poisoned=0 · deadlock_retries=0 · takeover=0

