# WAC multi-step transactional perf

Multi-step transactional perf for cost-dispatched workloads. Each batch is
ONE transaction containing N envelopes that share a WAC pool (fan-in) or
each target a distinct pool (fan-out). post_batch (FIFO v2 body, supports
all kinds) takes FOR UPDATE on involved pool rows at batch start; the
depth and spread of that lock set is what we're measuring.

## Question

> Does WAC's FOR UPDATE pre-lock pattern shape throughput differently than
> simple-transfer batches? How does multi-step (multi-envelope, multi-pool)
> behave under realistic BOM-consumption-style fan-out?

## Methodology

- Tuned PG conf, 20 workers, batch_size=1000, 3×60s replicates, 15s gaps.
- Six scenarios = 2 shapes × 3 issue mixes:
  - Fan-in: 1 hot pool, all envelopes target it.
  - Fan-out: 5,000 pools, each envelope picks a random one (~1000 distinct
    pools per batch on average).
  - Issue mix: 0%, 20%, 50% issues (rest are receipts). Issues require RYW
    on pool state set by prior receipts in the batch.
- Pools pre-funded (balance=$1M, qty=10K each) to avoid issue underflow.

## Results

| Scenario | median tps | p99 ms | per-row µs |
|---|---|---|---|
| Fan-in pure receipt | 22,038 | 1,144 | 45 |
| Fan-in 80r/20i mix | 21,156 | 1,113 | 47 |
| Fan-in 50r/50i mix | 20,634 | 1,092 | 48 |
| Fan-out pure receipt | 4,285 | 5,511 | 233 |
| Fan-out 80r/20i mix | 4,311 | 5,189 | 232 |
| Fan-out 50r/50i mix | 4,136 | 5,635 | 242 |

## Findings

**F1. WAC fan-in vs fan-out: 5× throughput gap.** Same pattern as
simple-transfer benches but at lower base throughput. Confirms the
cold-lock-acquisition cost on many distinct pools dominates — not contention
on the hot pool. With FOR UPDATE pre-lock on ~1000 distinct accounts per
batch, fan-out pays ~200µs/row just acquiring locks.

**F2. Read-your-writes within the batch is essentially free.** 0% / 20% /
50% issue mixes all land within 5% of each other on both shapes. The
jsonb running-map RYW pattern works without measurable overhead. Multi-step
transactions with internal pool-state dependencies don't pay extra for
the dependency itself; the cost is in the lock-acquisition side.

**F3. Per-row cost decomposition (vs simple-transfer floor)**:

```
Simple transfer floor:               13 µs/row  (acct-togd batch sweep)
+ WAC running state + 1 hot pool:    45 µs/row  (fan-in; +32µs WAC dispatch)
+ Cold-lock acquisition × ~1000:    233 µs/row  (fan-out; +220µs lock churn)
```

The WAC dispatch overhead (jsonb running state, per-envelope qty/value
arithmetic, in-batch staging) is ~32µs/row — modest, well-amortized at
batch=1000.

The cold-lock churn is ~200µs/row in fan-out — the LARGEST single cost
contribution measured anywhere in the PoC. This is the lever sw4i
specifically targets.

**F4. Realistic BOM-consumption analog**: a 50-component WO complete in
acct = single txn touching 50 distinct pool accounts = fan-out shape at
smaller scale. Lock acquisition cost: ~50 × 200µs ≈ 10ms just on FOR UPDATE
overhead per WO close. Sw4i drops this to per-bucket spinlock (microseconds).

**F5. Latency p99 tracks fan-in vs fan-out cleanly**:
- Fan-in: ~1.1s (batch=1000 takes 45-50ms per worker; queue depth adds the rest)
- Fan-out: ~5.5s (per-batch wallclock dominated by serial lock acquisition)

**F6. Issue mix doesn't shape latency much.** The running-state map updates
within a batch are cheap; the bottleneck is up-front lock acquisition,
which is the same regardless of how many envelopes are issues vs receipts.

## Implication for sw4i value (revised projections)

| Workload class | Today | Post-sw4i estimate | Lift |
|---|---|---|---|
| Simple transfer fan-in | 20K tps | 77K | 3.8× |
| Simple transfer fan-out | 7K tps | 69K | 9.6× |
| **WAC fan-in** | **22K tps** | **~75K (per-row floor)** | **~3.4×** |
| **WAC fan-out** | **4K tps** | **~30-40K** | **~7-10×** |

The realistic-shape WAC workloads (BOM consumption, multi-output WO close,
multi-line SO ship on WAC SKUs) match the WAC fan-out shape. These gain
the most from sw4i. Combined with native cost dispatch (acct-fngj for FIFO,
similar for WAC retroactive close hook), the multi-step transactional
shape stops being a structural bottleneck.

## Implication for multi-step transactional design

**There's no "multi-step is expensive" effect beyond per-step cost.** A 50-step
txn isn't 50× as expensive as a 1-step txn — it's exactly the sum of the
per-step costs minus any txn-overhead amortization. Postgres' txn machinery
is well-amortized; the cost is dominated by per-row work.

The dragons in multi-step are:
1. Cold-lock acquisition when steps touch many distinct accounts (lever: sw4i)
2. Per-step cost-method dispatch in plpgsql (lever: native dispatch fngj)
3. Internal state dependencies that require RYW — NOT measurably expensive
   when done via in-batch running state

This sharpens what the extension PoC needs to validate: not "can it do
multi-step transactions" (yes, trivially) but "can it eliminate the
per-distinct-account FOR UPDATE cost".

## Files

- `tests/bench_wac_fan.rs` — fan-in/fan-out WAC harness, env-driven.
- `bench/run-wac-multi-step.sh` — sweep driver.
- Per-run logs in `/tmp/poc-wac-multi-step/` (reproducible via the sweep).
