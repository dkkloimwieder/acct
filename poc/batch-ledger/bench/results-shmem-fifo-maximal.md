# acct-lhgh — `post_batch_fifo_maximal`: FIFO dispatch fully in Rust

**Outcome: structural regression vs mutable, NOT a lift. Follow-up paths
`acct-t59i` (L-accounts) and `acct-nosj` (F shmem-LayerArena) carry the
design space forward.**

Mirrors `acct-2g9w` (`results-shmem-wac-maximal.md`) for FIFO. Pushes
the per-pool FIFO layer walk + depletion dispatch into a Rust
`#[pg_extern]` (`ledger_dispatch_fifo_batch`) + single-CTE SQL wrapper
(mig 0021). The hypothesis was that the WAC maximal lift pattern would
transfer (5-13× fan-out, 1.3× fan-in) by replacing mig 0020's O(N²)
`jsonb_set` work with a `HashMap<i64, Vec<LayerRec>>` walk.

Bench falsified the hypothesis. **The new design is 8-18× SLOWER than
mutable** at both shapes.

## Methodology

Identical to `acct-2g9w`'s sweep:

- PG 18.3 in `acct-postgres`, tuned conf.
- 20 workers × batch=1000 × 60s × 3 replicates with 15s gaps between
  runs (within-cell). Fanin maximal run 3 was extended to 612s wall
  due to the per-batch latency being far longer than the 60s deadline
  drain window.
- Fresh `TRUNCATE posting_lines, accounts, cost_layers,
  cost_layer_depletions RESTART IDENTITY CASCADE` at the start of each
  cell. Rollup truncated too via the test harness.
- **70% issue / 30% receipt** — FIFO's expensive path is the issue
  walk, not the receipt. Issue-heavy mix stresses the per-layer
  contention.
- Fan-in: 1 hot pool, 20 writers contend on shared `cost_layers` row
  locks (FOR UPDATE inside `_fifo_walk_layers`).
- Fan-out: 5000 pools, writers spread across distinct pools — but
  because each batch's 1000 envelopes pick pools randomly across all
  5000, by birthday paradox virtually every active pool is touched
  per batch.
- Pools pre-seeded with 5 layers × 1M qty each = 5M qty/pool so
  workers cannot drain layers during a 60s run.

Cell 4 (fan-out maximal) was stopped at run 1/3 after the regression
pattern became unambiguous; run 1 alone (609s wall) was sufficient to
characterize.

## Per-run throughput (tps, posting_lines successfully inserted/s)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable (mig 0020)            |   313 |   312 |   313 | **313** |
| fan-in maximal (mig 0021)            |    36 |    36 |    36 | **36**  |
| fan-out mutable (mig 0020)           |   631 |   573 |   640 | **631** |
| fan-out maximal (mig 0021) — 1 run   |    34 | (stop)| (stop)| **34**  |

## Per-run p99 latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable         | 106,266 | 104,513 | 104,686 | **104,686** |
| fan-in maximal         | 607,970 | 607,620 | 611,840 | **607,970** |
| fan-out mutable        |  33,567 |  41,510 |  35,420 |  **35,420** |
| fan-out maximal — 1 run|       — |       — |       — |  **~608,000** |

(Maximal p99 latency is approximate — 600s+ per batch reflects that 20
writers queue serially on the FOR-UPDATE'd cost_layers rows; each
writer holds locks for its full batch duration, so per-worker wall time
≈ 600s for the worker that arrives last in the queue.)

## Headline deltas (medians, vs mutable baseline)

| Shape | Mutable tps | Maximal tps | Lift vs mutable | Δ p99 |
|---|---:|---:|---:|---:|
| fan-in  |  313 |  **36**  | **-89% (8.7× slower)** | +480% (107s → 608s) |
| fan-out |  631 |  **34**  | **-95% (18.6× slower)** | +1620% (35s → 608s) |

**Zero deadlocks** across all measured cells. The serialization is
correctness-preserving — just enormously expensive.

## Root cause

The maximal dispatcher pre-fetches all active layers under `FOR UPDATE`
on `cost_layers` for every issue-pool touched by the batch. This is
necessary for correctness: without the row lock, two concurrent issues
on the same pool could both read layer 1 with `qty_remaining=100`, both
decide to consume 50, and both decrement it to 50 — a 50-qty leak.

Lock cardinality comparison:

| Function | What gets FOR UPDATE'd | Typical row count per batch |
|---|---|---|
| mutable (mig 0020) | `accounts.id` for pool + AP + COGS | **~3 rows** |
| maximal (mig 0021) | `cost_layers` rows for issue-pools × active layers | **~5 × 700 = 3,500 rows** (fan-out) |

Per-batch lock acquisition cost scales ~1000× between the two. The
Rust HashMap walk + set-based CTE INSERT win (~3× faster per envelope
in absolute work) is drowned by the lock acquisition overhead.

Under fan-in (1 pool, 5 layers, 20 writers), all writers queue on the
same 5 rows. Per-batch wall ≈ 30s (per-batch work is fast under SHARED
view, but FOR UPDATE serializes them so each writer waits for the
queue ahead). Throughput collapses to 1/(queue_depth) of mutable.

Under fan-out, despite 5000 pools, the random pool picker means each
batch of 1000 envelopes touches ~700 unique pools, and 20 writers
collectively touch most of the 5000 pools — so FOR UPDATE locks
~3,500 layer rows per batch and writers still serialize heavily.

## What this validates and what it doesn't

- **Validates**: the Rust dispatcher + set-based CTE pipeline itself is
  correct (5/5 correctness tests pass: T1 cross-batch, T2 oldest-first,
  T3 in-batch sentinel resolution, T4 idempotent replay, T5 8-writer
  fan-in coupled writes; `recon drift=0`).
- **Does NOT validate**: the design choice to do row-level FOR UPDATE
  on `cost_layers`. That choice was necessary to preserve correctness
  WITHOUT relying on account-level serialization (mutable's mechanism)
  or moving state into shmem (acct-2g9w's mechanism for WAC).

## The architectural realization

WAC's per-pool state is **scalar** (running avg = value + qty), which
fits a single shmem cell with AtomicU128 CAS — lock-free updates at
hot rows. FIFO's per-pool state is an **ordered list of layers** which
does NOT fit a single fixed-size shmem cell. The natural FIFO shape
fundamentally differs from WAC's.

Two viable paths surface:

### `acct-t59i` — L-accounts (smaller-scope cleanup)

Move serialization from `cost_layers` row locks to `accounts.id`
account-row locks at the SQL wrapper level. Mirrors mig 0020 mutable's
serialization model exactly — no new correctness contract introduced
(any FIFO write necessarily touches the pool account, and Postgres'
row-level lock enforces serialization). Keeps the Rust dispatcher +
set-based CTE pipeline; drops `FOR UPDATE` from the dispatcher's SPI.

Expected: should approach WAC-shape lift since per-batch Rust work
remains, only the lock overhead is reduced.

Scope: half a day. One migration (replaces mig 0021 with a v2 body).

### `acct-nosj` — F shmem-LayerArena (architectural shift)

New shmem region: variable-length per-pool layer queues, slab-allocated
LayerRecs, per-pool head/tail pointers + queue_lock. A2-compliant
staging via PENDING_STACK extension. Bgworker drain to durable
`cost_layers`. Lazy-load on PG restart.

Mirrors `acct-sw4i`'s WAC shmem-native approach, adapted for FIFO's
ordered-list shape.

Scope: 10-14 days, 9 sub-issues filed at claim time.

## Recommendation

L-accounts first (half day). If it matches mutable correctness AND
exceeds its perf meaningfully, F becomes optional. If L-accounts only
caps at the account-lock-serialization ceiling (no meaningful lift),
F is the only path forward.

## Files

- Bench harness: `poc/batch-ledger/tests/bench_fifo_fan.rs`
- Bench sweep driver: `poc/batch-ledger/bench/run-fifo-maximal-sweep.sh`
- Mig 0020 (mutable baseline): `poc/batch-ledger/db/migrations/0020_post_batch_fifo_named.up.sql`
- Mig 0021 (maximal, current regression-bearing): `poc/batch-ledger/db/migrations/0021_post_batch_fifo_maximal.up.sql`
- Extension dispatcher: `poc/ledger-extension/src/lib.rs::ledger_dispatch_fifo_batch`
- Correctness tests (5/5 green): `poc/batch-ledger/tests/fifo_shmem_correctness_maximal_t1.rs`
- Raw bench logs: `/tmp/poc-oqje-bench/`
- This document: `poc/batch-ledger/bench/results-shmem-fifo-maximal.md`

## Follow-ups

- `acct-t59i` — L-accounts: wrapper FOR UPDATE accounts.id; drop FOR UPDATE on cost_layers. P3.
- `acct-nosj` — F: shmem-cached LayerArena. P3. Blocked-on acct-t59i (gates on whether L's measurement justifies F's scope).
