# acct-lhgh — `post_batch_fifo_maximal`: FIFO dispatch fully in Rust

Mirrors `acct-2g9w` (`results-shmem-wac-maximal.md`) for FIFO. Pushes
the per-pool FIFO layer walk + depletion dispatch from plpgsql into a
Rust `#[pg_extern]` (`ledger_dispatch_fifo_batch`). Mig 0009's per-pool
`jsonb_set` over the layer list collapses to a HashMap<i64, Vec<LayerRec>>
in Rust, with one SPI fetch under FOR UPDATE at batch start.

The FIFO mutable baseline is dominated by O(N²) jsonb cost on the
per-pool layer list — every layer mutation rewrites the entire JSONB
array via `jsonb_set`. At batch=1000 × 70% issue × 5 layers per touched
pool, that's ~3,500 jsonb_set calls per batch. The Rust dispatcher
replaces this with in-place Vec mutation; the wrapper splits the
TableIterator output into legs + depletions via CTEs.

This document holds the mutable-baseline row populated by Sub 1
(`acct-jqr4`). The maximal row is filled by Sub 3 (`acct-oqje`) after
Sub 2 (`acct-m6q3`) ships the Rust dispatcher and SQL wrapper.

## Methodology

Identical to `acct-2g9w` (`results-shmem-wac-maximal.md`):

- PG 18.3 in `acct-postgres`, tuned conf.
- 20 workers × batch=1000 × 60s × **3 replicates with 15s gaps** (Sub 3).
  Sub 1 captures a single confirmation probe.
- Fresh `TRUNCATE posting_lines, accounts, cost_layers, cost_layer_depletions`
  RESTART IDENTITY CASCADE at the start of each cell.
- **70% issue / 30% receipt** — heavier than acct-2g9w's 0% issue (which
  exercised qty-leg only). FIFO's layer walk is on the **issue** path,
  so issue-heavy mix is what stresses the dispatcher.
- Fan-in: 1 hot pool, 20 writers contend on shared `cost_layers` row
  locks (FOR UPDATE inside `_fifo_walk_layers`).
- Fan-out: 5000 pools, writers spread across distinct pools — distinct
  layer chains, less contention.
- Each pool pre-seeded with 5 layers × 1M qty = 5M qty/pool so workers
  cannot drain layers during a 60s run.

## Per-run throughput (tps, posting_lines successfully inserted/s)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable (mig 0020) — Sub 1 probe |   330 | _TBD_ | _TBD_ | **~330** |
| fan-in maximal (mig 0021)               | _TBD_ | _TBD_ | _TBD_ | **TBD**  |
| fan-out mutable (mig 0020) — Sub 1 probe |  686 | _TBD_ | _TBD_ | **~686** |
| fan-out maximal (mig 0021)              | _TBD_ | _TBD_ | _TBD_ | **TBD**  |

**Sub 1 confirmation probe** (single 60s run, 2026-05-13, tip 7455924
+ uncommitted mig 0020): fan-in 330 tps, fan-out 686 tps. Both within
±11% of the state-memo numbers (305 / 620). Captured as the mutable
baseline for the maximal lift calculation in Sub 3.

## Per-run p99 latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable          | 96,876 | _TBD_ | _TBD_ | **~96,876** |
| fan-in maximal          |  _TBD_ | _TBD_ | _TBD_ | **TBD** |
| fan-out mutable         | 33,567 | _TBD_ | _TBD_ | **~33,567** |
| fan-out maximal         |  _TBD_ | _TBD_ | _TBD_ | **TBD** |

## Comparison context (for reference)

FIFO mutable is 75-80× slower than WAC mutable at the same shape, and
the gap is larger at fan-in (the contended pool's layer list is the
hot O(N²) JSONB target):

| Method        | fan-in tps | fan-out tps |
|---|---:|---:|
| FIFO mutable (mig 0020) — measured today |   330 |    686 |
| WAC mutable  (mig 0006) — `acct-2g9w`    | 25,045 |  4,341 |
| WAC maximal  (mig 0019) — `acct-2g9w`    | 71,683 | 59,331 |

The WAC mutable→maximal lift was 2.86× / 13.67×. FIFO's structural
ceiling differs: the dispatcher win is per-envelope cost reduction,
but FOR UPDATE on shared `cost_layers` rows STILL serializes
fan-in workers identically. Fan-in lift will be modest;
fan-out lift should approach the WAC band (5-13×) because per-pool
layer walks are independent.

## Headline deltas (Sub 3 to fill)

| Shape | mutable tps | maximal tps | Lift | mutable p99 | maximal p99 | Δ p99 |
|---|---:|---:|---:|---:|---:|---:|
| fan-in  |   330 | _TBD_ | _TBD_ | 96,876 ms | _TBD_ | _TBD_ |
| fan-out |   686 | _TBD_ | _TBD_ | 33,567 ms | _TBD_ | _TBD_ |

## What Sub 2 + Sub 3 will validate

1. **Fan-out is the load-bearing shape for FIFO too.** mig 0020's
   per-envelope plpgsql `jsonb_set` on per-pool layer lists is O(N²)
   in layers touched per pool per batch. Eliminating it in Rust
   produces an N²-vs-N collapse. acct-2g9w hit 5.24× at fan-out for
   WAC; FIFO target band is similar.

2. **Fan-in lift will be modest.** At 1 hot pool × 20 writers, FOR
   UPDATE on the shared `cost_layers` rows serializes writers. The
   dispatcher reduces per-envelope cost but doesn't reduce row-lock
   contention. Expected lift band: 1.2-2×.

3. **p99 latency collapse on fan-out should be dramatic.** mig 0020
   holds the txn open for ~50ms/envelope under contention (jsonb_set
   compounding); the Rust dispatcher does the same work in ~5ms/
   envelope. Combined with per-batch CTE consolidation (no temp
   table), transaction-hold time should drop ~10×.

4. **Zero deadlocks across the sweep.** FIFO uses row-level FOR
   UPDATE on `cost_layers` ordered by `(pool_account_id, receipt_date,
   id)`, deterministic across writers.

## Memory bound (out of Sub 2 / Sub 3 scope; tracked in `acct-16kr`)

The Rust dispatcher allocates `HashMap<i64, Vec<LayerRec>>` on the
PG backend's heap. At 48 B per LayerRec, the soft cap derived from
work_mem (64 MB × 0.5 = 32 MB) gives ~670K layers per batch. For
the bench config (5 pre-seeded layers per pool × up to 5000 pools
= 25K layers fetched per batch), well within budget. Adversarial
testing (10K+ layers per pool, 100+ pools touched per batch) is
the `acct-16kr` epic's domain.

## Files

- Mig 0020 (Sub 1 — mutable baseline, this run): `poc/batch-ledger/db/migrations/0020_post_batch_fifo_named.up.sql`
- Bench harness (Sub 1): `poc/batch-ledger/tests/bench_fifo_fan.rs`
- Extension dispatcher (Sub 2): `poc/ledger-extension/src/lib.rs` (`ledger_dispatch_fifo_batch`)
- Mig 0021 (Sub 2 — maximal wrapper): `poc/batch-ledger/db/migrations/0021_post_batch_fifo_maximal.up.sql`
- Correctness tests (Sub 2): `poc/batch-ledger/tests/fifo_shmem_correctness_maximal_t1.rs`
- Bench sweep (Sub 3): `poc/batch-ledger/bench/run-fifo-maximal-sweep.sh`
- This document: `poc/batch-ledger/bench/results-shmem-fifo-maximal.md`
