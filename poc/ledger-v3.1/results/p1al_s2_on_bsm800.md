# Sustained routed workload — s2 (300s)

Workload: s2 · mode=routed · GUCs: affinity_scheme=0,batch_size_max=800,batch_window_us=20000,committer_count=4,router_pack_disjoint=on

Throughput (window delta): 11,696 trx/s · commit_group_size_avg=489.5 · drains=7,168 · trx_committed=3,508,900

lines/trx: trx_line=1.00, posting_line=1.00


## Throughput-rate distribution (per-second, n=271 samples, ramp/drain trimmed)

| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| trx/s | 2,275.0 | 11,797.5 | 12,538.0 | 13,363.0 | 14,800.0 | 16,800.0 | 12,125.9 | 2,327.5 |
| trx_line/s | 2,275.0 | 11,797.5 | 12,538.0 | 13,363.0 | 14,800.0 | 16,800.0 | 12,125.9 | 2,327.5 |
| posting_line/s | 2,275.0 | 11,797.5 | 12,538.0 | 13,363.0 | 14,800.0 | 16,800.0 | 12,125.9 | 2,327.5 |

## Caller latency (µs)

| | p50 | p95 | p99 |
|---|--:|--:|--:|
| ack (enqueue) | 57,704 | 172,490 | 214,695 |
| committed (end-to-end) | 2,283,798 | 3,351,248 | 3,797,942 |

## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)

Total committer txn wall-time over the window: 712.35s across 3,508,900 trx (committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).

| span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| pool_lock | 261.05 | 36.6% | 74.40 |
| hydrate | 6.09 | 0.9% | 1.73 |
| apply | 290.00 | 40.7% | 82.65 |
| commit(fsync) | 17.08 | 2.4% | 4.87 |
| prep | 138.14 | 19.4% | 39.37 |

_prep refold (acct-e95d):_

| sub-span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| prep.decode | 118.00 | 16.6% | 33.63 |
| prep.xact | 2.98 | 0.4% | 0.85 |
| prep.dedup | 15.57 | 2.2% | 4.44 |
| prep.other | 1.58 | 0.2% | 0.45 |

## Wait-event segmentation (committer pg_stat_activity sampler)

- samples: 12,040 (idle 196)

- busy_frac: 0.984  (committer utilization; low ⇒ ceiling is upstream/ingress)

- of busy — on-CPU: 0.294 · row-lock: 0.222 · shmem-LWLock: 0.470


## Resilience counters
- pool_lock_acquisitions=215,564 (window) · aggregate_upserts=215,564 (window)
- (cumulative since restart) dedup_skips=0 · dropped=0 · tx_failures=0 · poisoned=0 · deadlock_retries=0 · takeover=0

