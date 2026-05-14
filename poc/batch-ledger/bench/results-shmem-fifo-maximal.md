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

# Addendum 2026-05-13 #2: acct-vh5y inline-no-shmem probe + pure-INSERT ceiling

**Result reframes the F debate. Inline gives 12× lift over mutable from a
~1-day change. F-shmem (acct-e9tf) buys the remaining 5.5× to ceiling.**

## What inline does

Mig 0023 exposes `fifo_apply_batch_inline` (Rust pg_extern) as
`post_batch_fifo_maximal_inline`. ONE Rust function does everything: parse
envelopes, FOR UPDATE accounts.id, fetch cost_layers via SPI, FIFO walk in
Rust HashMap, multi-row INSERT posting_lines / cost_layers /
cost_layer_depletions / UPDATE cost_layers, stage_apply balance/qty to
shmem. No plpgsql wrapper. No 7-CTE chain. No TableIterator marshaling 7K
rows out to plpgsql.

Identical correctness contract to mig 0022 (5/5 T1-T5 green on the
adapted `tests/fifo_apply_batch_inline_t1.rs`).

## Headline numbers (1-replicate fan-out, 20w × 60s × batch=1000, 70/30)

| Path | tps fan-out | vs mutable | vs ceiling |
|---|---:|---:|---:|
| mutable (mig 0020)        |    793 |     1× |  1.5% |
| maximal-L (mig 0022)      |     46 |  0.06× | 0.087% |
| **inline (mig 0023)**     | **9,487** | **12×** | **18%** |
| **PURE INSERT CEILING**   | **52,832** | **66.6×** | **100%** |

p99 batch wall: inline 2.4s vs mig 0022 432s (180× better). Ceiling p99:
582ms.

## Pure-INSERT ceiling methodology

`tests/bench_fifo_inserts_only_ceiling.rs`. No FIFO logic, no reads, no
plpgsql, no Rust pg_extern. Three multi-row sqlx INSERTs per batch:

- 1000 rows into `posting_lines`
- 300 rows into `cost_layers` (receipts; `receipt_posting_line_id` NULL)
- 700 rows into `cost_layer_depletions` (issues; FK to 10K pre-seeded
  posting_lines + 10K pre-seeded cost_layers, picked round-robin)

20 writers × 60s. The number 52,832 envelopes/s = 105,665 row-writes/s.
Each batch commits in ~400ms (p50); 20 workers × (1 / 0.4s) ≈ 50 batches/s
≈ 50K envelopes/s — pretty much matches measurement.

The ceiling is bounded by **WAL + index updates + FK validation** for that
row volume. Any FIFO design's per-batch CPU pays on top of this; the ceiling
is "infinite CPU but the same writes."

## Architectural read

The inline path lands at **18% of the ceiling**. Compare:
- Mutable: 1.5% of ceiling. plpgsql FOR LOOP + O(N²) jsonb_set was the cost.
- Mig 0022: 0.087%. The dispatcher+CTE wrapper was a 17× regression below
  even mutable, dominating everything else.
- Inline: 18%. Eliminating the wrapper recovers an absolute majority of the
  remaining gap; per-batch CPU is no longer the bottleneck.

**Ceiling has 5.5× headroom against inline.** That headroom is exclusively
durable-write cost — the cost_layers INSERTs + cost_layer_depletions INSERTs
+ cost_layers UPDATEs that have to land in WAL synchronously with
posting_lines. F-shmem (acct-e9tf) targets exactly this: move cost_layers
state into shmem, drain to durable via bgworker, per-batch durable writes
drop from ~2000 rows to ~1000 (posting_lines only).

F's realistic ceiling: inline × ~3-4× = 30-40K tps fan-out. Approaches WAC
shmem's 43.5K. ~10-14 day investment for the remaining 3-4×.

The 12× already in hand from inline is the architectural win. F's remaining
lift is real but diminishing-returns.

## Recommendation

Ship inline as the production FIFO maximal path. acct-e9tf F-shmem is now
a value judgement: is going from 9.5K → ~35K tps fan-out worth 10-14 days
for the codebase? Depends on whether realistic FIFO ERP workloads exceed
9.5K tps per backend.

## Files

- This document: `poc/batch-ledger/bench/results-shmem-fifo-maximal.md`
- Mig 0023 (inline; current production candidate): `poc/batch-ledger/db/migrations/0023_fifo_apply_batch_inline.up.sql`
- Mig 0022 (L-accounts; superseded): `poc/batch-ledger/db/migrations/0022_post_batch_fifo_maximal_l_accounts.up.sql`
- Mig 0021 (cost_layers FOR UPDATE; superseded): `poc/batch-ledger/db/migrations/0021_post_batch_fifo_maximal.up.sql`
- Mig 0020 (mutable; production-comparable reference): `poc/batch-ledger/db/migrations/0020_post_batch_fifo_named.up.sql`
- Extension fn: `poc/ledger-extension/src/lib.rs::fifo_apply_batch_inline`
- Correctness tests (5/5 green): `poc/batch-ledger/tests/fifo_apply_batch_inline_t1.rs`
- Ceiling bench: `poc/batch-ledger/tests/bench_fifo_inserts_only_ceiling.rs`

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

---

# Addendum 2026-05-13 #3: acct-ylmo F-shmem measurement (sub 4 / acct-0450)

**Outcome: F-shmem hits 41K tps fan-out / 31K tps fan-in pure-issue.
Sub 4's half-α bgworker drain is the load-bearing lift over inline.
Target (30-40K fan-out) HIT.**

## What F-shmem is now

After sub 4 (`acct-0450`):

- FIFO arena lives in shmem (16384 buckets, MAX_LAYERS=64, ~42 MB).
- Apply path (`fifo_apply_batch_maximal` / `post_batch_fifo_maximal_F`)
  walks FIFO in shmem under per-cell LWLock, stages drains into
  per-cell `pending_drain[64]` ring, posting_lines / cost_layers /
  cost_layer_depletions INSERTs land inline as audit trail.
- `ledger_drain` bgworker UPSERTs `cost_layers.qty_remaining` at
  `drain_interval_ms` (100 ms default) cadence — the previously
  apply-path-inline UPDATE is now amortized batched.
- Overflow falls back to inline UPDATE (overflow at >64
  distinct-layer-slices per cell per tick — rare under realistic
  flow rates).

Half-α architecture: receipts still INSERT into `cost_layers`
inline (durable; crash-safe; no reconstruct logic needed).

## Methodology

- PG 18.3 in `acct-postgres`, tuned conf, ledger_drain bgworker
  running at 100 ms cadence with FIFO drain extension enabled.
- 20 workers × batch=1000 × 60s × 3 replicates × 15s gaps for F-shmem
  cells. 1 replicate for mutable + inline anchors on the same
  PG / commit / box.
- **Workload shape pivot: 100% issue, 0% receipt.** F-shmem's
  MAX_LAYERS=64 ring cannot accommodate the prior 70% issue / 30%
  receipt mix (each receipt adds a layer; 300 receipts per
  1000-envelope batch → cap hit in ~21% of first batch).
  Spill-to-durable is sub 3 / acct-b8ub — out of scope for sub 4 /
  sub 7. Issue-dominant ("warehouse outflow") workload is the
  realistic F regime until spill-to-durable ships.
- Each layer pre-seeded with 1 B qty (5 layers × 5 B qty/pool) so
  a 60s pure-issue run cannot fully drain any layer. Keeps the
  ring stable at 5 layers throughout the bench — exercises the
  steady-state per-issue path.
- `fifo_arena_reset()` called at the start of every shmem-touching
  cell. Otherwise FIFO_ARENA retains stale layer_ids from prior
  cells, breaking lazy-seed against post-TRUNCATE cost_layers.

## Headline numbers

### Throughput (tps, posting_lines successfully committed/s)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---:|---:|---:|---:|
| fan-in mutable (mig 0020)      | 12,565 |        —   |        —   | **12,565** |
| fan-in inline  (mig 0023)      | 25,054 |        —   |        —   | **25,054** |
| **fan-in F  (mig 0024 + sub 4)**  | **28,973** | **31,094** | **32,066** | **31,094** |
| fan-out mutable (mig 0020)     |    755 |        —   |        —   | **755** |
| fan-out inline  (mig 0023)     | 13,984 |        —   |        —   | **13,984** |
| **fan-out F (mig 0024 + sub 4)**   | **40,695** | **41,011** | **41,464** | **41,011** |

F-shmem is 3-replicate stable (variance < 5% per shape).

### p99 batch latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---:|---:|---:|---:|
| fan-in mutable      |  1,872 |     —  |     —  | **1,872** |
| fan-in inline       |    877 |     —  |     —  |   **877** |
| **fan-in F**        |    991 |    915 |    889 |   **915** |
| fan-out mutable     | 26,918 |     —  |     —  |**26,918** |
| fan-out inline      |  1,737 |     —  |     —  | **1,737** |
| **fan-out F**       |    714 |    733 |    691 |   **714** |

### Deltas (medians, pure-issue workload, comparable within this run)

| Shape | F-shmem | vs mutable | vs inline | vs WAC fan-out (43.5K, prior memo) |
|---|---:|---:|---:|---:|
| fan-in  | **31,094** | **2.5× faster** | **1.24× faster** | ~71% of WAC headline |
| fan-out | **41,011** | **54× faster**  | **2.93× faster** | ~94% of WAC headline |

Zero deadlocks across all cells; zero `batches_err`.

## What this validates

1. **Target hit.** acct-ylmo bd-issue asked for "30-40K tps fan-out".
   F-shmem lands at 41K — top of range, robust across 3 replicates.
2. **Sub 4 is the load-bearing lift.** Inline (sub 0; mig 0023) is
   the 12× lift over mutable; sub 4 layers another 3× over inline
   at fan-out (41K / 14K) and 1.24× at fan-in (31K / 25K).
3. **F-shmem approaches WAC's shmem-native ceiling.** WAC fan-out
   median is 43.5K tps (acct-sw4i M9). F fan-out is 41K — 94% of
   WAC despite FIFO's structurally heavier per-pool state
   (ordered list vs scalar running avg).
4. **Bgworker drain composes correctly with the apply path.** The
   `pending_drain` ring + cell-LWLock discipline means apply and
   drain serialize cleanly on the cells they touch; no deadlocks,
   no torn observations of `qty_remaining`.

## What it doesn't measure

1. **Mixed-receipt workloads.** 70/30 issue/receipt is blocked by
   MAX_LAYERS=64; sub 3 (`acct-b8ub` spill-to-durable) is the gate.
   Real ERP workloads with bursty receipts (PO match days) would
   need the spill-to-durable path that preserves strict per-unit
   FIFO when the in-shmem cap is exceeded.
2. **Layer churn at the head.** This bench's 5-layer seed never
   fully drains a layer; sub 4's `pending_drain` only stages drain
   deltas (not head-advance / fully-drained-layer signals). A
   "layers drain to head" path needs separate measurement.
3. **Crash recovery.** sub 4 keeps cost_layers INSERTs inline so
   posting_lines IS truth — crash recovery is trivially
   posting_lines-driven. But this run never tests an actual crash
   mid-tick. Sub 5 (`acct-u324`) is the crash-recovery sub.
4. **High receipt rate impact on overflow fallback.** PENDING_DRAIN_CAP=64
   overflows fall back to inline UPDATE — but tested only at
   pure-issue. Bursty receipt workload may exercise overflow
   path more, and that path negates the drain win.

## What this means for the epic

- **acct-e9tf F architecture validated.** Closing the epic at sub 4
  is justified: target hit; correctness pinned; bgworker drain
  composes; no known regression.
- **acct-b8ub (sub 3 spill-to-durable) becomes optional**, gated on
  a workload driver. If no acct ERP feature pushes a pool past
  MAX_LAYERS=256 live layers, sub 3 stays parked. Pure-issue 41K
  is enough for the current envelope.
- **acct-u324 (sub 5 crash recovery)** + **acct-6g2u (sub 6 recon)**
  remain on the path. Sub 4's half-α architecture means sub 5 is
  not a heavy lift (posting_lines IS truth; no shmem→durable
  reconstruction needed).

## Recommendation

F-shmem (sub 2 + sub 4) is the production FIFO maximal path for
issue-dominant workloads. The 31-41K tps headline:

- 2.5-54× over mutable (the prior production path)
- 1.24-3.0× over inline (the no-shmem upper bound)
- Within 6-29% of WAC's shmem-native ceiling

For mixed-receipt workloads that push past MAX_LAYERS=256, the
inline path (mig 0023) remains the recommended fallback until
spill-to-durable (sub 3 / acct-b8ub) ships.

## Files

- This document: `poc/batch-ledger/bench/results-shmem-fifo-maximal.md`
- Sweep driver: `poc/batch-ledger/bench/run-fifo-F-sweep.sh`
- Bench harness: `poc/batch-ledger/tests/bench_fifo_fan.rs`
  (now reads `POC_BENCH_LAYER_QTY` for variable per-layer pre-seed;
  routes F via `_maximal_f` suffix → `fifo_arena_reset()` call)
- Mig 0024 (F entry-point): `poc/batch-ledger/db/migrations/0024_fifo_apply_batch_maximal.up.sql`
- Extension fn: `poc/ledger-extension/src/fifo.rs::fifo_apply_batch_maximal`
  + `fifo::do_fifo_drain_tick` (sub 4)
- Sub 4 T1 tests (4/0): `poc/batch-ledger/tests/fifo_drain_t1.rs`
- Raw bench logs: `/tmp/poc-ylmo-bench/`

---

# Addendum 2026-05-13 #4: MAX_LAYERS=256 + lazy-seed bug fix + mixed-flow regime

**Outcome: F-shmem holds at 40.8K tps fan-out under realistic 70/30
issue/receipt mix. Same as pure-issue (41.2K). Lift over inline:
3.4×; over mutable: 52×.**

## What changed

1. **MAX_LAYERS bumped 64 → 256.** Arena grew from ~42 MB to ~117 MB
   (still modest for a high-perf ledger). Headroom for bursty receipt
   workloads — a typical PO-match day burst of 100-200 receipts now
   fits comfortably without overflow.

2. **Lazy-seed bug fix (Phase 6).** Originally Phase 6 queued a cell
   for lazy-seed only when the batch contained at least one `fifo_issue`
   envelope. Cells whose first batch was receipt-only got `seeded=1`
   stamped without ever loading durable `cost_layers` into the ring.
   Subsequent issues on that pool then saw only the post-bench-start
   receipts, missed the bench-pre-seeded durable layers, and reported
   "fifo_issue short by N units (pool X exhausted in shmem ring)" —
   even though `cost_layers` had 5 layers × 1M qty per pool of headroom.
   Fix: Phase 6 queues seed for any first-touch cell regardless of
   envelope kind. Lazy-seed runs in Phase 8 before any envelope apply,
   so durable layers land in the ring HEAD before this batch's
   receipts append at the tail — strict FIFO order preserved.

3. **Coalescing (sub 3 / acct-b8ub) reframed.** Coalescing merges two
   oldest layers' qty + weighted-avg unit_cost — that's NOT strict
   FIFO at per-unit grain (merged layers post averaged cost). The
   correct alternative for overflow handling is **spill-to-durable**:
   when the ring hits MAX_LAYERS, future receipts go directly to
   `cost_layers` (skip shmem) and issues that walk past the in-memory
   range fall through to SPI. Preserves strict FIFO; pays slow-path
   cost only on overflow. Sub 3 is now under review pending a
   workload driver that actually exceeds 256 (PO-match-day burst
   simulator etc).

## Methodology (v2)

Same as addendum #3 except:

- 9 cells now (4 anchor 1-rep + 3 F 3-rep = 15 runs).
- Two workload shapes: **pure-issue (100/0)** and **mixed (70/30)**.
- Mixed shape on fan-out ONLY. Fan-in concentrates 4-20 workers all
  on one cell; at 20w × 60s × 30% receipts ≈ 360K receipts on a
  single ring → cap blown regardless of value (256 or 1024 or 4096).
  Fan-in mixed is a pathological shape; sub 3 territory.
- Mixed shape pre-seeds with `LAYER_QTY=1_000_000` (1M/layer);
  pure-issue uses `1_000_000_000` (1B/layer) — enough that no layer
  ever fully drains on either shape.
- `POC_BENCH_LAYER_QTY` added as bench-harness env-var.

## Headline numbers (v2, 20 workers × 60s × batch=1000, batches_err=0
   across all 15 runs)

### Pure issue (100/0)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---:|---:|---:|---:|
| fan-in mutable   | 13,834 |   —    |   —    | **13,834** |
| fan-in inline    | 24,907 |   —    |   —    | **24,907** |
| **fan-in F**     | **30,380** | **31,365** | **32,792** | **31,365** |
| fan-out mutable  |    757 |   —    |   —    | **757** |
| fan-out inline   | 13,838 |   —    |   —    | **13,838** |
| **fan-out F**    | **41,152** | **41,565** | **40,984** | **41,152** |

### Mixed (70/30) — fan-out only

| Scenario | run 1 | run 2 | run 3 | median |
|---|---:|---:|---:|---:|
| fan-out mutable_70  |    783 |   —    |   —    |    **783** |
| fan-out inline_70   | 11,845 |   —    |   —    | **11,845** |
| **fan-out F_70**    | **40,534** | **40,824** | **41,142** | **40,824** |

### p99 batch latency (ms)

| Scenario | F median p99 | inline | mutable |
|---|---:|---:|---:|
| fan-in pure-issue   |  936 |  1,059 |  1,580 |
| fan-out pure-issue  |  675 |  1,731 | 26,960 |
| fan-out mixed       |  691 |  1,945 | 29,588 |

### Deltas vs inline / mutable (medians)

| Scenario | F tps | vs inline | vs mutable |
|---|---:|---:|---:|
| fan-in 100/0  | 31,365 |  1.26×  |   2.27× |
| fan-out 100/0 | 41,152 |  2.97×  |  54.4×  |
| fan-out 70/30 | 40,824 |  **3.44×** |  **52.1×** |

F's win **survives the realistic mix** (40.8K vs 41.2K at pure-issue
— within 1% — and 3.44× over inline at 70/30 vs 2.97× at 100/0).
Inline degrades at 70/30 because of the extra `cost_layers INSERT`
on the apply path per receipt; F-shmem stages those layers into the
ring and lets the bgworker handle `qty_remaining` UPSERTs out-of-band.

### Zero failures across all 15 runs

`batches_err=0` for every cell. No deadlocks. No "fifo_issue short"
errors. No `pending_drain` overflow falls into inline. Stable across
3 replicates per F cell (max-min variance < 8%).

## What's still gated on sub 3 (spill-to-durable)

- **Fan-in mixed shape.** 20 writers concentrating all receipts on
  one ring will fill the cap at any reasonable value. This is the
  natural workload for sub 3 territory.
- **Sustained net-receipt-positive flow per pool.** A small set of
  hot pools receiving many sustained receipts (e.g., a single-vendor
  bulk receiving operation) can exceed 256 over a multi-minute run
  on fan-out too. The bench's i.i.d. uniform-receipt-target shape
  hides this; real workloads concentrate on a smaller set.

## Files updated for v2

- Bench harness: `poc/batch-ledger/tests/bench_fifo_fan.rs`
  - new `POC_BENCH_LAYER_QTY` env var
  - first-2 errors logged so a regression surfaces without
    digging through the err counter
  - F-shmem routing now triggers `fifo_arena_reset()` before runs
- Sweep driver: `poc/batch-ledger/bench/run-fifo-F-sweep.sh`
  - per-cell ISSUE_PCT and LAYER_QTY (was global env vars)
  - 9 cells across both workload shapes
- Extension: `poc/ledger-extension/src/fifo.rs`
  - `MAX_LAYERS = 256`
  - Phase 6 lazy-seed filter dropped (covers receipt-only first-touch)
  - `fifo_arena_reset()` (bench/test helper) added
- Lwlock T1 cap assertion updated to 256.
