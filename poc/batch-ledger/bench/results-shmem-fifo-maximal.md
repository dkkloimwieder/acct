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

- `acct-t59i` — L-accounts: wrapper FOR UPDATE accounts.id; drop FOR UPDATE on cost_layers. P3. **CLOSED 2026-05-13: no lift — see addendum below.**
- `acct-nosj` — F: shmem-cached LayerArena. P3. Blocked-on acct-t59i (gates on whether L's measurement justifies F's scope).

---

# Addendum 2026-05-13: acct-t59i L-accounts measurement

**Outcome: another regression — L-accounts is 8-17× slower than mutable.
The dispatcher + set-based CTE design is fundamentally heavier per batch
than mutable's plpgsql FOR LOOP + jsonb_set, even with lock cardinality
matched.**

## What L-accounts changed

- Mig 0022 supersedes mig 0021's body. LANGUAGE switches sql → plpgsql.
- Wrapper-top `PERFORM ... FOR UPDATE ORDER BY accounts.id` over all
  account ids touched by the batch (debit + credit, both sides).
  Mirrors mig 0020 mutable's serialization model exactly.
- `ledger_dispatch_fifo_batch` SPI swap: `client.update` → `client.select`;
  `Spi::connect_mut` → `Spi::connect`; `FOR UPDATE` stripped from the
  cost_layers SELECT.

Correctness preserved: tests/fifo_shmem_correctness_maximal_t1.rs **5/5
green** (T1 cross-batch, T2 oldest-first, T3 in-batch sentinel, T4
idempotent replay, T5 8-writer fan-in coupled writes; recon drift=0).

## Bench results (3 replicates × 60s; 11/12 runs — fan-out maximal-L cell 4 killed mid-run-3 because the pattern was unambiguous at 2/3)

### Throughput (tps)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable (mig 0020)        | 367.6 | 370.2 | 370.4 | **370** |
| fan-in maximal-L (mig 0022)      |  46.4 |  46.4 |  46.8 |  **46** |
| fan-out mutable (mig 0020)       | 793.4 | 793.6 | 792.2 | **793** |
| fan-out maximal-L (mig 0022) — 2 |  46.3 |  46.2 | (kill)|  **46** |

### p99 batch latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable        |  87,073 |  87,904 |  89,013 |  **87,904** |
| fan-in maximal-L      | 431,649 | 432,466 | 429,493 | **431,649** |
| fan-out mutable       |  29,270 |  29,609 |  29,237 |  **29,270** |
| fan-out maximal-L     | 432,605 | 434,720 |  (kill) | **433,663** |

### Headline deltas (medians, vs mutable baseline)

| Shape | Mutable tps | Maximal-L tps | Lift vs mutable | Note |
|---|---:|---:|---:|---|
| fan-in  | 370 |  **46** | **-88% (8.0× slower)**  | Marginal improvement vs mig 0021 (36 tps → 46 tps; +28%) |
| fan-out | 793 |  **46** | **-94% (17.2× slower)** | Marginal improvement vs mig 0021 (34 tps → 46 tps; +35%) |

Zero deadlocks. Serialization is correct — just slow.

## Root cause: dispatcher + CTE per-batch cost dominates, NOT lock cardinality

Comparing mig 0020 (mutable) to mig 0022 (maximal-L) under the SAME
lock cardinality (FOR UPDATE on ~3 accounts.id rows per batch):

- mig 0020 per-batch wall (1000 envelopes, serialized through one pool):
  ~2.7s (fan-in tps 370 ÷ 1 effective writer + queue depth).
- mig 0022 per-batch wall: ~21.5s.

So **per-batch CPU cost is ~8× higher** for maximal-L than mutable, even
with identical serialization. The Rust dispatcher + set-based CTE chain
(parse JSONB, SPI fetch, HashMap walk, 7+ chained CTEs with sentinel
resolution and bilateral drain bookkeeping) costs MORE per batch than
mig 0020's plpgsql FOR LOOP + jsonb_set + TEMP TABLE inserts — despite
mig 0020's O(N²) jsonb_set work.

Where the cost concentrates (hypothesized, not profiled):

1. plpgsql `RETURN QUERY WITH (...)` invoking a `#[pg_extern]` SETOF
   function inside a CTE — TableIterator row materialization is not
   free, and each row crosses Rust→PG plan-tree boundary.
2. The 7+ CTE chain (input, existing, non_replay_input, dispatched,
   legs, depls, inserted_pl, in_batch_drain, inserted_layers,
   sentinel_map, resolved_depls, inserted_dep, pre_existing_drain,
   updated_layers) forces large intermediate result-set materialization.
3. Multiple JOINs on `idempotency_key` (UUID) across inserted_pl,
   non_replay_input, sentinel_map increase hash-join + memory cost.

mig 0020's plpgsql FOR LOOP does its work in tight in-memory state
without crossing extension/SQL boundaries per row.

## Implications

**The WAC maximal pattern (acct-2g9w's 5.55× lift) does not transfer to
FIFO at all.**

WAC's per-pool state is scalar (running avg in a single shmem cell, CAS
fetch_add). FIFO's per-pool state is an ordered list of layers — even
with the Rust dispatcher in place, the wire-protocol cost of streaming
~7K rows back to plpgsql + chaining them through CTEs to land in
durable tables overwhelms any algorithmic win from going from O(N²)
jsonb_set to O(N) HashMap walks.

The architectural read for `acct-nosj` (F shmem-LayerArena):

- F's value proposition WAS the lock-free hot path against shmem-resident
  per-pool layer queues, mirroring acct-2g9w's WAC win.
- BUT: even shmem-native FIFO writes still need to drain to durable
  `cost_layers` rows eventually — bgworker drain is bottleneck-bound.
- The drain still moves the same ~7K rows/batch through the SQL plane.
  Unless the *durable side* can be eliminated for the hot path, the
  per-batch cost ceiling may not budge much from mig 0022's level.
- Mig 0020 mutable's ~793 tps fan-out / ~370 tps fan-in is the realistic
  FIFO ceiling under the current durable-row-per-leg-and-depletion model.

## Recommendation

**Keep mig 0020 mutable as the production FIFO path.** No follow-up F
unless workload reality demands it. The 793 tps fan-out / 370 tps fan-in
is unlikely to be the bottleneck for any realistic FIFO ERP workload
(period close + commodity provisional are the only known high-volume
FIFO surfaces; both batch-bound rather than tps-bound).

If F is pursued anyway, projections should be revised DOWN — the
"5-13× lift over mutable" hypothesis from F's claim-time design is
NOT supported by maximal-L's evidence. Realistic F ceiling, assuming
shmem-native ordered-list ops are 2-5× faster than dispatcher round-trip,
is roughly mutable-parity to ~2× mutable. That's a ~10-14 day investment
for ~mutable to 2× mutable. Probably not worth it.

## Files

- This document: `poc/batch-ledger/bench/results-shmem-fifo-maximal.md`
- Mig 0022 (L-accounts; current): `poc/batch-ledger/db/migrations/0022_post_batch_fifo_maximal_l_accounts.up.sql`
- Mig 0021 (cost_layers FOR UPDATE; superseded): `poc/batch-ledger/db/migrations/0021_post_batch_fifo_maximal.up.sql`
- Mig 0020 (mutable; production reference): `poc/batch-ledger/db/migrations/0020_post_batch_fifo_named.up.sql`
- Extension dispatcher: `poc/ledger-extension/src/lib.rs::ledger_dispatch_fifo_batch`
- Correctness tests (5/5 green on mig 0022): `poc/batch-ledger/tests/fifo_shmem_correctness_maximal_t1.rs`
- Raw bench logs: `/tmp/poc-oqje-bench/` (now contains BOTH mig 0021 numbers from acct-oqje run and mig 0022 numbers from acct-t59i run — same directory, different code under test on different days)
