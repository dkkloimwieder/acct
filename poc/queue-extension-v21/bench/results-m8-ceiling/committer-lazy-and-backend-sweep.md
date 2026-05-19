# committer_lazy + backend-count sweep (post-cyu6)

Follow-up to acct-cyu6: test options (1) `status_insert_mode=committer_lazy`
and (2) higher backend counts. Same payload, same `batch_size_max=5000`, same
method=std, same hardware (taskset 0-7, 8-core).

## Results

| config | distribution | submit evps | drain evps | **e2e evps** | avg SB | max SB | committed | p50 | p99 | top wait_event |
|---|---|---|---|---|---|---|---|---|---|---|
| caller_intx N=8 | single (sku=1) | 1,965 | 181,402 | **1,944** | 84.3 | 111 | 100,000 | 45 ms | 86 ms | LWLock/WALWrite |
| **committer_lazy N=8** | single | 9,916 | 108,470 | **9,085** | 205.7 | 672 | 100,000 | 481 ms | 1,102 ms | Client/ClientRead |
| committer_lazy N=32 | single | 6,745 | 84,954 | 6,249 | 168.6 | 1,000 | 100,000 | 2,615 ms | 3,355 ms | Extension/Extension |
| caller_intx N=8 | uniform_1000 | 1,681 | 8,943 | **1,415** | 1.88 | 6 | 100,000 | 3.6 s | 62 s | LWLock/WALWrite |
| committer_lazy N=8 | uniform_1000 | 1,742 | 10,262 | 1,489 | 1.92 | 5 | 100,000 | 3.5 s | 65 s | Extension/Extension |
| committer_lazy N=32 | uniform_1000 | 1,308 | 10,898 | 1,168 | 1.92 | 5 | 100,000 | 4.8 s | 84 s | **LWLock/poc_v21_staging_queue** |

## Headline

* **committer_lazy is a 4.7× e2e win on hot-pool workloads** (1,944 → 9,085
  evps at sku=1 N=8). The per-call `submission_status` INSERT was the dominant
  enqueue cost when backpressure was active.
* **committer_lazy is marginal for scattered workloads** (1,415 → 1,489 evps,
  +5%). uniform_1000's bottleneck isn't the status INSERT.
* **More backends HURT performance.** N=32 dropped e2e by 22-31% vs N=8 on
  both distributions. The single STAGING_QUEUE LWLock serializes contenders;
  more contenders = more wait time.
* **At N=32 uniform_1000, top wait_event = `LWLock/poc_v21_staging_queue`** —
  the staging-queue LWLock named explicitly. This is now the binding
  constraint, not WAL, not committer-side LWLocks, not Extension wait.
* Latency goes up significantly with committer_lazy because the staging
  buffer actually fills now (was always full at backpressure but the committer
  was draining faster relative to ingress). Tradeoff: 4.7× throughput vs ~13×
  worse p99 in absolute ms. Per-envelope latency is queue-wait-dominated at
  saturation regardless of mode.

## Implication for the architecture

The bench validates that **the committer is healthy** (8.9k-108k drain rate
across all configs) and the **enqueue path is the actual cap** at ~10k evps
under hot-pool committer_lazy N=8.

To unlock higher throughput from this point, the next architectural lever is
**sharding the STAGING_QUEUE LWLock** — multiple staging queues, multiple
LWLocks, hash- or backend-pinned routing. Adding more committer workers
doesn't help (drain is not the bottleneck). Adding more backends doesn't help
either (LWLock contention dominates).

`acct-pl3b` (batch ingress API) sidesteps this entirely by collapsing N
per-call LWLock acquires into one per-batch LWLock acquire.

## PG WAL settings during this run

```
synchronous_commit = on
wal_sync_method = fdatasync
wal_buffers = 64 MB (8192 × 8 KB)
wal_compression = lz4
wal_level = replica
commit_delay = 20 µs     (group commit enabled)
commit_siblings = 5
wal_writer_delay = 200 ms
wal_writer_flush_after = 1 MB
max_wal_size = 8 GB / min_wal_size = 2 GB
checkpoint_timeout = 900 s
checkpoint_completion_target = 0.9
io_method = io_uring
```

Already well-tuned. Group commit is active (commit_delay/commit_siblings).
WAL compression on. io_uring on. No obvious knobs to turn here.

## Files

```
bench/results-m8-ceiling/po-single-N100000_N8backends_committer_lazy.json
bench/results-m8-ceiling/po-single-N100000_N32backends_committer_lazy.json
bench/results-m8-ceiling/po-uniform_1000-N100000_N8backends_committer_lazy.json
bench/results-m8-ceiling/po-uniform_1000-N100000_N32backends_committer_lazy.json
```

Baseline (caller_intx) results:
```
bench/results-m8-ceiling/po-single-N100000.json
bench/results-m8-ceiling/po-uniform_1000-N100000.json
```

## Conclusions

1. **Make `committer_lazy` the v2.1 default** for hot-pool workloads. Memory + GUC
   default change. Trade durability of status row write at enqueue for 4.7×
   throughput.
2. **Document N=8 as the sweet spot** for current single-staging-queue
   architecture. More backends regress; not more.
3. **Next architectural work: shard the staging queue.** This is the path to
   pushing past 10k evps on per-call ingress without batch API.
4. **`acct-pl3b` (batch ingress) is still the qualitative win** — one
   client-side call submits N envelopes with one LWLock acquire chain. This
   sidesteps both the per-call cap AND the per-call LWLock contention.
