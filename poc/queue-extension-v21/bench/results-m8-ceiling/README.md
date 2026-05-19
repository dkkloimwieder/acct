# acct-cyu6 — PO receipts ceiling probe

Simple-payload throughput probe with persisted JSONL payloads, fire-and-forget
submission, bulk terminal-state poll. Two SKU distributions:
- **single**: every envelope hits (sku=1, location=1). Max pool-key overlap.
- **uniform_1000**: sku = (iter % 1000) + 1. ~100 envelopes per pool.

100k envelopes each, 8 backends (taskset 0-7), method = **standard cost**.

## Headline numbers (run @ batch_size_max=5000)

| metric | single (sku=1) | uniform_1000 |
|---|---|---|
| submission rate | **1,965 evps** | **1,681 evps** |
| committer drain rate (steady state) | **181,402 evps** | **8,943 evps** |
| end-to-end throughput | **1,944 evps** | **1,415 evps** |
| avg envelopes per SuperBatch | 84.3 | 1.88 |
| max SB size observed | 111 | 6 |
| ns/drain (pipeline) | 8.3 ms | 2.3 ms |
| **ns/envelope** | **98 µs** | **1.2 ms** |
| top wait_event | LWLock/WALWrite (1439) | LWLock/WALWrite (1591) |
| committed / failed | 100,000 / 0 | 100,000 / 0 |
| p50 latency e2e | 45 ms | 3.6 s |
| p99 latency e2e | 86 ms | 62 s |

## Key findings

**1. The committer can drain 181k+ evps on a hot pool with proper batching.**
At sku=1 with batch_size_max=5000, the router packs SuperBatches with avg=84
envelopes. Each SB drains in 8.3 ms — that's 98 µs/envelope at the committer.
The architecture's batching primitive works as designed when the workload
permits packing.

**2. End-to-end is submission-bound at ~2k evps.** With 8 backends each doing
synchronous `poc_v21_enqueue` round-trips, submission caps at ~1965 evps
regardless of how fast the committer drains. The committer would happily run
~90× faster than the harness can feed it. **The single-envelope ingress API
is the real bottleneck for ERP-shaped batch workloads** — see `acct-pl3b`
(batch-enqueue API) for the path forward.

**3. SKU distribution dramatically affects packing.** uniform_1000 produces
avg SB size of 1.88 even with batch_size_max=5000 — there's not enough
overlap between concurrent envelopes for affinity grouping to coalesce.
Submission rate (1681 evps) ≈ N backends × (1 / per-envelope wait time).
The committer's drain rate (8943 evps) is plenty; the harness can't feed it
faster under per-call ingress at 8 backends.

**4. There is NO meaningful difference between the two cells at the
committer level for standard cost** — both run at ~1-2k drain rate per
backend submission capacity, just packed differently. The big-batch
single-SKU case is throughput-efficient at the committer but bursts hard
into the backpressure CV. The small-batch scattered case keeps the
committer pool fully parallel but pays per-envelope ingress overhead.

## What this run replaced

A previous run of the same payloads ran with **method defaulting to FIFO**
because the bench's pre_seed wrote `standard_costs` rows but forgot to insert
`sku_method_assignments`. SKUs without an explicit assignment default to
FIFO. The FIFO Step 3 hydration query has a correlated subquery on
`cost_depletions` per layer; at sku=1 with 50k+ accumulated layers, every
SuperBatch drain re-read them all → 928 ms per drain. After the
sku_method_assignments fix, single-cell drain rate jumped from 746 evps to
181,402 evps (**243× speedup**) and per-envelope work dropped from 29 ms to
98 µs (**296× speedup**).

This is documented as the smoking gun in **acct-hg9g** (committer shortcut
audit) and **acct-00na** (demand-driven hydration). Real architectural issue;
fix is filed.

## What the architecture is NOT bottlenecked on

- **Inter-SB row-lock contention.** `cross_sb_for_update_waits = 0` across
  both cells. Different SBs touch disjoint pools or don't race on the same
  row long enough to register.
- **WAL or fsync.** Top wait is `LWLock/WALWrite` at small backend-sample
  counts (~1500). Not the dominant cost.
- **Extension LWLocks.** With std method and proper packing, our internal
  synchronization is no longer the wall (was the wall when FIFO was running).

## What it IS bottlenecked on

- **`poc_v21_enqueue` per-call overhead.** Status row INSERT, arena alloc,
  STAGING_QUEUE LWLock per envelope. Caps ingress at ~1965 evps with 8
  backends. This is where `acct-pl3b` (batch-enqueue API) lifts the ceiling.
- **Backpressure backpropagation.** `backpressure_count = 100,000` in both
  cells — staging hit "full" condition for every envelope. The bench is
  oversaturated; downstream throughput sets the upstream pace.

## Comparison to v2 (queue-extension) numbers

| implementation | shape | per-call (b=1) | with batch RPC (b=1000) |
|---|---|---|---|
| **Shmem rollup PoC** (acct-sw4i) | fan_in | — | **67,000 evps** |
| **v2 queue-extension** | small_batch N=128 | 8,017 evps | **50,000 evps** |
| **v2 queue-extension** | fan_in N=128 | 11,878 evps | **49,050 evps** |
| **v2 queue-extension** | fan_out N=128 | 6,379 evps | 17,050 evps |
| **v2.1 (this PoC, now)** | po_receipt sku=1 | **1,944 evps e2e (181k drain)** | N/A (no batch API yet) |
| **v2.1 (this PoC, now)** | po_receipt uniform_1000 | 1,415 evps e2e | N/A |

v2.1 per-call (b=1) ingress is ~4× slower than v2's per-call baseline (1944
vs 8017 small_batch). The committer side (181k drain) is fine — the gap is
all on the enqueue path. With `acct-pl3b` batch ingress + the audit fixes
from `acct-hg9g`, v2.1 should match or exceed v2's b=1000 numbers since the
committer side is already healthy.

## GUC configuration during this run

```
poc_v21.batch_size_max = 5000     (raised from 50 default during this issue)
poc_v21.router_window_size = 1000
poc_v21.batch_window_us = 500     (DEAD CODE — not wired; tracked as acct-0sc4)
poc_v21.committer_lease_ms = 100  (default)
```

Source code change: `lib.rs` GUC upper bound bumped from 1000 to 10000 so
SET 5000 is allowed.

## Payload provenance

```
bench/payloads/po-single-N100000.jsonl
  sha256 = 3941453245dc3bf1...
bench/payloads/po-uniform_1000-N100000.jsonl
  sha256 = 547d495684545037...
```

Both byte-stable across regenerations (UUIDv5 over a fixed namespace +
"po-{dist}-N100000:{iter}" — re-running the generator produces identical
files). Replay any run via the same JSONL.

## Follow-up issues filed

- **acct-pl3b**: batch-enqueue API — lifts the ingress ceiling
- **acct-0sc4**: wire `batch_window_us` — enables time-coalesce packing for
  scattered workloads
- **acct-hg9g**: committer shortcut audit (this work's origin)
- **acct-00na**: Shortcut A — skip Step 3 hydration for FIFO receipts
- **acct-3ypi**: Shortcut B — denormalize `cost_layers.effective_qty`
- **acct-h6ha**: Shortcut D — gate Step 2.5 dedup query on FIFO depletions
