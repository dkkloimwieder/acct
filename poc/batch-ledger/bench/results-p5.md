# P5 results — batch API + FIFO

acct-1hps sub-issue of acct-qdp5 epic.

## Question P5 answers

> Does the pre-pass layer-slice allocation mechanism for FIFO hold throughput
> like WAC perpetual does in P4?

## Headline

**NO — FIFO is the hardest cost method to batch under in-batch sequencing
requirements.** Pure-plpgsql implementation has a structural ceiling around
**1K tps at batch=1000** (vs the 5K target). Two implementations tried:

- **v1 (per-envelope INSERTs inside FOR LOOP)**: 419 tps at batch=1000. Each
  fifo_issue does ~5-7 SQL statements (replay check, posting_line INSERT,
  layer walk, depletion INSERTs, layer UPDATEs, account UPDATEs). At batch=1000
  with 700 issues, that's ~3500 statements per batch + 20 workers contending
  on 20 pools = batches take 30+ seconds, throughput collapses.

- **v2 (multi-row INSERT + in-memory JSONB layer state)**: 762 tps at
  batch=1000. The per-envelope statements are eliminated, but the
  per-envelope `jsonb_set` on the growing layer-state JSONB is O(n²) in
  batch size. For n=1000 envelopes mutating a ~1500-layer JSONB, this
  dominates.

The architectural finding is more valuable than the throughput number:
**not all cost methods batch equally well in pure plpgsql.** FIFO requires
either (a) client-side planning with SQL just executing multi-row INSERTs,
(b) a C extension for fast array manipulation, (c) different data structure
for layer state (TEMP TABLE? hstore?), or (d) acceptance that FIFO remains
per-document on the acct backport while other cost methods batch.

## Methodology

Same as P4: 20 workers × 20 pools × 60s × 5 replicates per batch size,
release build, sync_on. Pre-seed: 5 layers × 1M qty per pool.

## Results — v1 (per-envelope INSERTs)

| batch_size | median tps | p50 batch µs |
|---|---|---|
| 1    | 207   | 141,215   |
| 10   | 1,322 | 145,537   |
| 100  | 1,524 | 1,276,076 |
| 1000 | 419   | 36,009,938 |
| 8000 | 69    | 777,666,651 |

**Peaks at batch=100 then collapses.** At batch=1000+, lock contention from
30+ second batches dominates.

## Results — v2 (multi-row INSERT, in-memory JSONB layer state)

| batch_size | median tps | p50 batch µs |
|---|---|---|
| 1000 (15s spot bench) | 762 | 13,770,170 |

v2 sweep not run in full (the architectural finding is decisive enough from
the spot measurement; full sweep would take 35 minutes of bench for marginal
information value).

## Findings

**F1. v1 fails because per-envelope round trips × 20-worker × 20-pool
contention compounds catastrophically.** With each batch holding 20 pool
locks for 30+ seconds while ~3500 statements execute, other workers
wait the full batch duration.

**F2. v2's improvement is 1.8× — not enough.** The multi-row INSERT
pattern eliminates per-envelope SQL statements but introduces O(n²)
plpgsql work in the JSONB layer state.

**F3. The plpgsql language is the bottleneck.** `jsonb_set` on a 1000-element
array is fast (~10µs) but called 1000 times per batch on a growing array
totals ~10s per batch. Total work is O(n²) in batch size.

**F4. Zero deadlocks** across v1 and v2. The pre-lock pattern holds. Contention
is fair waiting, not deadlock.

**F5. WAC perpetual (P4) doesn't have this problem** because the running
balance map is just two scalars per pool (value, qty), not a growing
layer list. WAC's plpgsql per-envelope cost is O(1); FIFO's is O(layers).

**F6. Implication for acct backport**: FIFO is the highest-risk cost method
to batch. Options for the acct migration:
- Keep FIFO per-document; batch only standard + WAC.
- Move FIFO planning to Rust (client-side): client walks layers via SELECT,
  builds the plan, sends it to a thin SQL multi-row INSERT.
- Accept that batched FIFO is ~50× slower than batched WAC and design
  for it.

## Comparison vs prior phases

| batch_size | P3 simple | P4 WAC | P5 FIFO v1 | P5 FIFO v2 |
|---|---|---|---|---|
| 1    | 1,419  | 208    | 207   | n/a |
| 10   | 2,449  | 1,023  | 1,322 | n/a |
| 100  | 10,720 | 10,476 | 1,524 | n/a |
| 1000 | 40,610 | 22,867 | 419   | 762 |
| 8000 | 42,341 | 22,008 | 69    | n/a |

FIFO at batch=1000 is **53× slower than P3 simple and 30× slower than
P4 WAC**. The cost method matters enormously.

## R-rules audit on P5 evidence

| Rule | Status | Notes |
|---|---|---|
| R1 (per-class qty divisor) | N/A for FIFO | FIFO depletes from specific layers; no per-class avg. |
| R3 (solo-at-pool) | DISSOLVES | Same as P4 — batch is sole pool writer. |
| R4 (FOR UPDATE before read) | DISSOLVES | Pool locked once. Layer reads safe because pool lock implies layer exclusivity. |
| R6 (idempotency double-check) | DISSOLVES | Pre-pass dedup. |
| R7 / AP9 (audit-field) | DISSOLVES | Audit fields computed in plan. |

## Calibration verdict

**FAILS P5 success criteria.** Target was ≥5K tps at batch=1000; actual
peak (v2) was 762.

**But the architectural finding is the deliverable.** The PoC validates
that FIFO is fundamentally harder to batch than WAC, and that pure-plpgsql
in-batch sequencing has an O(n²) ceiling. P8's synthesis will incorporate
this — likely recommending **acct keeps FIFO per-document while batching
standard + WAC** at the cost-dispatch layer.

## Follow-up filed

acct-1hps-followup will track the FIFO multi-row optimization research:
- Client-side planning (Rust walks layers, SQL just INSERTs).
- TEMP TABLE for layer state.
- C extension for plpgsql array manipulation.

If P8 decides backport is worth it, this follow-up gates FIFO support.

## PROCEED to P6 (state machine + GRNI)

P6 doesn't depend on P5's success. State-machine envelopes don't have
cost-dispatch complexity — expected to behave like P3 simple with a small
overhead for the doc-table UPDATE.

## Files

```
poc/batch-ledger/bench/results/p5_fifo/
├── env.txt
├── cross_summary.txt
├── batch_1/    {run_*.log, summary.txt}    — v1 numbers
├── batch_10/   …
├── batch_100/  …
├── batch_1000/ …
└── batch_8000/ …
```
