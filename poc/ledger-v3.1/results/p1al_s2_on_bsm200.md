# Sustained routed workload — s2 (300s)

Workload: s2 · mode=routed · GUCs: affinity_scheme=0,batch_size_max=200,batch_window_us=20000,committer_count=4,router_pack_disjoint=on

Throughput (window delta): 10,101 trx/s · commit_group_size_avg=148.3 · drains=20,440 · trx_committed=3,030,350

lines/trx: trx_line=1.00, posting_line=1.00


## Throughput-rate distribution (per-second, n=271 samples, ramp/drain trimmed)

| rate | min | Q1 | median | Q3 | p95 | max | mean | stdev |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| trx/s | 4,869.0 | 10,451.0 | 10,900.0 | 11,341.0 | 12,381.5 | 13,415.0 | 10,500.8 | 1,809.1 |
| trx_line/s | 4,869.0 | 10,451.0 | 10,900.0 | 11,341.0 | 12,381.5 | 13,415.0 | 10,500.8 | 1,809.1 |
| posting_line/s | 4,869.0 | 10,451.0 | 10,900.0 | 11,341.0 | 12,381.5 | 13,415.0 | 10,500.8 | 1,809.1 |

## Caller latency (µs)

| | p50 | p95 | p99 |
|---|--:|--:|--:|
| ack (enqueue) | 73,400 | 163,446 | 208,666 |
| committed (end-to-end) | 2,608,857 | 3,711,959 | 4,005,560 |

## Committer time breakdown (where wall-time went, per-run delta, summed over all committers)

Total committer txn wall-time over the window: 599.19s across 3,030,350 trx (committer_count from GUCs; µs/trx is summed committer wall-time ÷ trx).

| span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| pool_lock | 80.95 | 13.5% | 26.71 |
| hydrate | 13.43 | 2.2% | 4.43 |
| apply | 256.84 | 42.9% | 84.76 |
| commit(fsync) | 44.87 | 7.5% | 14.81 |
| prep | 203.09 | 33.9% | 67.02 |

_prep refold (acct-e95d):_

| sub-span | seconds | % of txn | µs/trx |
|---|--:|--:|--:|
| prep.decode | 184.60 | 30.8% | 60.92 |
| prep.xact | 4.81 | 0.8% | 1.59 |
| prep.dedup | 12.06 | 2.0% | 3.98 |
| prep.other | 1.62 | 0.3% | 0.53 |

## Wait-event segmentation (committer pg_stat_activity sampler)

- samples: 12,044 (idle 144)

- busy_frac: 0.988  (committer utilization; low ⇒ ceiling is upstream/ingress)

- of busy — on-CPU: 0.269 · row-lock: 0.062 · shmem-LWLock: 0.635


## Resilience counters
- pool_lock_acquisitions=260,850 (window) · aggregate_upserts=260,850 (window)
- (cumulative since restart) dedup_skips=0 · dropped=0 · tx_failures=0 · poisoned=0 · deadlock_retries=0 · takeover=0

