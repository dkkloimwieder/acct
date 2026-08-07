# design-v3.2 recalc engine (a): strict layer-walk + chronological re-sort (R-1) + backdated re-cost (R-2)

> **Status: DESIGN (acct-q1oj.1, 2026-07-10).** The foundational child of the v3.2 recalc/close workstream
> (acct-q1oj / `design-v3.2.md` §5). Defines the per-pool authoritative-costing algorithm — the serial fold
> at the center of the surviving architecture (design-v3.1 §18/§19). Siblings (b) acct-q1oj.2 cross-pool
> scheduler, (c) acct-q1oj.3 cadence/backlog, (d) acct-q1oj.4 state schema, and (e) acct-q1oj.5 close-time
> all build on this. **Design-only:** algorithm + data-access shape + the open decisions that fall to the
> siblings. No migrations, no code here.

## 1. Role and correctness contract

Under the alt-C posture (design-v3.1 §16) the hot path posts **no cost leg** — it appends `trx` / `trx_line`
with the observed qty and a *provisional* unit_cost (running average or standard, §3.5) and touches only the
aggregate `pool_state` row. Recalc is the **sole costing engine** (§18): its entire job is to reconstruct
authoritative strict FIFO/LIFO cost from the recorded `trx_line` stream and post the value leg to the GL.

This child designs the core of that job for **one layer-tracked pool**. WAC / STD / specific pools produce
their final cost on the hot path already (§3.1 / §3.3 / §3.4) and need no reconstruction; only FIFO and LIFO
diverge into provisional mode (§3.5) and therefore only they are recalc's subject.

**The correctness contract is fixed by the acct-0at4.7 replay oracle** (`bench/replay-oracle-results.md`),
not invented here. That oracle already built the authoritative "TRUE" arm as a throwaway; production recalc
must reproduce it. The oracle established two load-bearing facts this design is bound by:

- **The recorded `(qty, unit_cost, id)` triple reproduces the *provisional* record exactly** (100 % on 149 392
  real concurrent depletions) — `id` faithfully reflects application order because each single-line
  submission's `id` is drawn under the same aggregate-tuple lock that serialized the running-average update
  (oracle §1a).
- **But `id`-order does NOT reconstruct *authoritative* FIFO** when business order ≠ commit order — the
  backdated-receipt case (oracle §1b, vignette A: id-order returns 300, authoritative is 100, off by 200 %).
  Authoritative reconstruction requires a business-chronology key **and** a chronological recalc sort. That is
  R-1 and R-2 below.

## 2. The reference algorithm — strict layer-walk

Lifted from the oracle's `StrictPool` (`bench/replay-oracle/src/main.rs`), which is the specification:

- **State:** an ordered sequence of open layers, each `(qty_remaining, unit_cost)`. FIFO consumes the
  **oldest** layer first (front); LIFO the **newest** (back).
- **Receipt** of `(Q, C)`: append a layer `(Q, C)` at the back.
- **Depletion** of `Q`: consume from the consuming end, layer by layer, `take = min(remaining, layer.qty)`
  each step, accumulating `value += take · layer.unit_cost` (i128) and decrementing `layer.qty`; pop a layer
  when it reaches 0. The depletion's **authoritative unit_cost** is `banker_div(value, Q)` — the
  banker-rounded weighted average of the layer costs it consumed, at the same 1e-6 fixed precision the ledger
  uses everywhere (§3.0). Insufficient on-hand → the "ill-defined" case (see §5, alt-C allow-negative).

In production this is the same math over **persistent** layer state (`pool_state.layer_id > 0`) instead of an
in-memory `VecDeque`. The v3.1 schema already reserves the shape: `pool_state.layer_id > 0` is a materialized
layer row, and the convention is `pool_state.layer_id = trx_line.id` of the receipt that created it (§2.2).
Path C never materializes these on the hot path; **recalc is what writes them.**

## 3. What a recalc pass produces (per pool)

For each depletion in the pool, in business order:

1. **Authoritative unit_cost** — the layer-walk result above.
2. **Variance = authoritative − provisional** (`provisional` = the recorded `trx_line.unit_cost`). This is the
   value recalc posts as the cost-adjustment GL leg, routed through `posting_account_map` (§3.7). Its
   magnitude is what the oracle §2b sized — **directional under a cost trend** (FIFO overstates, LIFO
   understates), which is the quantitative proof recalc is load-bearing, not cosmetic (§19).
3. **Consumed-layer linkage** — which receipt layer(s) fed this depletion, and how much of each (the audit
   trail §13 / §14.4 asks for). See the split-linkage decision, §6.

And for the pool as a whole: the **final materialized layer state** (`pool_state.layer_id > 0` rows with
remaining qty) and the reconciled aggregate.

## 4. R-1 — chronological re-sort

The recalc feed (design-v3.2 §6 / v3.1 §17) is a logical-decoding slot delivering `trx_line` in **commit
order**. The layer-walk must run in **business-effective order**. So the first thing recalc does per pool is
re-sort:

> **R-1. Order each pool's stream by `(pool_id, posted_at, id)`** — `posted_at` (business/effective date)
> primary, `id` the deterministic within-date tiebreak.

Two requirements fall out:

- **Write-path obligation (recorded in the oracle §1c).** `trx.posted_at` MUST carry the *real business
  date*, not a wall-clock stamp. The v3.1 harness stamps a compile-time-constant `posted_at` on every run
  submission (distinct values across a whole run: **2**), so there is no business-order signal at all today.
  Recalc is only correct if the production write path supplies true effective dates. This is a v3.2
  hot-path/SPI contract note (surface to design-v3.2 §3/§4), not something recalc can repair after the fact.
- **Data-access obligation (hand to child (d) acct-q1oj.4).** Recalc scans each pool's whole stream in
  `(posted_at, id)` order, repeatedly (R-2). `trx_line` does **not** denormalize `posted_at` today (§2.2), so
  the sort needs either (i) a denormalized `trx_line.posted_at` + a `(pool_id, posted_at, id)` index, or (ii) a
  `JOIN trx` on every scan over the migration-0010 `(pool_id, id)` index then an in-memory sort.
  **Recommendation D1: denormalize `posted_at` onto `trx_line` and index `(pool_id, posted_at, id)`** — the
  JOIN-then-sort is recalc's hot inner loop and R-2 re-runs it; paying one denormalized column removes a join
  and makes the ordered scan an index-only walk. Final call is child (d)'s (schema), but (a) requires the
  ordered access path to exist.

`id` remains the *correct* within-date tiebreak precisely because the oracle proved it faithful to
application order under concurrency (§1a); it is demoted from primary key to tiebreak, not discarded.

## 5. R-2 — backdated receipts force re-costing (why recalc ≠ forward scan)

This is the hard part and the reason incremental streaming does not bound recalc's per-event work.

**The problem.** A receipt whose `posted_at = T` precedes depletions already costed in its pool inserts a
layer *before* them in business order. Those depletions consumed the wrong layers and must be **re-costed**.
But the feed delivers that receipt in **commit** order — i.e., late, after those depletions were already seen.
A naive forward-streaming scan has already assigned their authoritative cost and cannot fix it without going
back. (Oracle §1b vignette A is exactly this: the cheap lot business-dated first but committed last; a
forward `id`-order replay returns 300, the business-order replay returns 100.)

**The design: per-pool replay anchored at a re-cost floor.** Recalc is not a forward scan over the global
feed; it is a **per-pool replay in `(posted_at, id)` order** (R-1). A backdated event at `T` establishes a
**re-cost floor** = the earliest `(posted_at, id)` position at or after `T`; every depletion at or after the
floor in that pool must be recomputed. Two ways to honor the floor:

- **(i) Full-pool replay from opening.** Rebuild the layer sequence from the pool's opening position and walk
  the entire `(posted_at, id)` stream. **This is exactly what the oracle does**, so it carries zero
  reproduction risk and is idempotent by construction (a re-run over an unchanged stream yields identical
  costs → zero net new adjustment). Cost: O(pool events) per pass — the throughput inequality (§19) in its
  rawest form.
- **(ii) Checkpoint + replay-from-floor.** Persist periodic layer-state checkpoints (a snapshot of the layer
  sequence at a `(posted_at, id)` boundary). On a backdated event at `T`, restore the latest checkpoint `≤ T`
  and replay forward only from there — bounding re-work to events since that checkpoint. Cost: checkpoint
  storage, restore, and invalidation complexity.

> **Recommendation D2: full-pool replay (i) as the v3.2 correctness baseline; checkpointing (ii) deferred to
> child (b)/(c) as the optimization.** Full replay *is* the oracle — correct-first, trivially idempotent, no
> reproduction risk — and matches the parent-repo posture "correctness > performance; baseline before
> complexity." Checkpointing is a real optimization but only earns its keep once the cross-pool scheduler
> (b) and cadence model (c) show full-replay is the binding cost; it layers on additively (the re-cost-floor
> concept defined here is what a checkpoint restores *to*). The **re-cost floor** is (a)'s abstraction; the
> **checkpoint mechanism** is (b)/(d)'s.

**Bounding the fan-out even under (i).** A recalc pass need only replay pools whose stream *advanced* since the
last pass — the feed's commit-ordered cursor (§6) tells recalc which `pool_id`s received new events. Within an
advanced pool the unit of work is a full replay; **across** pools the work parallelizes (child (b)). So the
per-event work is unbounded *within a pool* (R-2), but the *set of pools* touched per pass is bounded by feed
activity. That split — bounded pool-set, unbounded within-pool replay — is the precise shape child (c) must
model for backlog/cadence.

## 6. Layer materialization and the split-linkage decision

- **Layer rows.** Recalc writes `pool_state.layer_id > 0` (one per open receipt layer, `qty` = remaining,
  `layer_id = ` the receipt `trx_line.id`), plus the reconciled aggregate at `layer_id = 0`. This is the
  persistent form of the oracle's `VecDeque`.
- **Consumed-layer linkage — the schema gap.** A single depletion can consume **multiple** layers (the
  oracle's `while remaining > 0` loop). The v3.1 `trx_line.source_trx_line_id` is a **single** self-reference
  (§2.2) and cannot represent a multi-layer consumption. Options:
  - (a) populate `source_trx_line_id` only when a depletion consumes exactly one layer; NULL / dominant-layer
    otherwise — **lossy**, breaks the §14.4 marking / §13 audit-linkage requirement.
  - (b) emit **one cost-adjustment `posting_line` per consumed layer** (each carrying its own layer reference
    and per-layer amount) — keeps the `trx_line` intact, decomposes the GL value leg per layer.
  - (c) an explicit **consumption-linkage table** `(depletion_trx_line_id, layer_trx_line_id, qty, cost)`.

  > **Recommendation D3: emit per-consumed-layer linkage — form (b) or (c), decided by child (d).** (a)'s
  > algorithm produces, per depletion, a list of `(layer_trx_line_id, qty_taken, layer_unit_cost)` tuples; how
  > they persist (per-layer posting_line vs linkage table) is (d)'s schema call, but the algorithm MUST emit
  > per-layer granularity — a single `source_trx_line_id` is insufficient and would silently lose the audit
  > trail the deferred-recalc surface (§13) commits to.

## 7. ledger-core: the real `fifo.rs` / `lifo.rs`

design-v3.1 §8 ships `fifo.rs` / `lifo.rs` as `MethodMismatch` stubs precisely because Path C's hot path never
runs strict layer math. Recalc needs them for real. This child specifies them as the persistence-backed
counterpart of the oracle's `StrictPool`:

- **Interface:** given the pool's opening layer state (materialized `pool_state.layer_id > 0` rows, or empty)
  and the `(posted_at, id)`-ordered event stream, produce, per depletion, the authoritative unit_cost + the
  consumed-layer tuples (§6), and the final layer state.
- **Implementation:** the oracle's `StrictPool` front/back consumption and `banker_div` weighted-average,
  unchanged in math — same i128 accumulation, same 1e-6 precision (§3.0). The only production delta from the
  throwaway is that layers are hydrated from / persisted to `pool_state` rather than held in a `VecDeque`.
- **No hot-path exposure:** these run only in recalc. A hot-path dispatch into `fifo.rs`/`lifo.rs` remains a
  configuration bug and keeps raising `MethodMismatch` (§8) — provisional mode (§3.5) still owns the hot path.

## 8. Correctness validation (acceptance)

The oracle is the golden reference; validation reuses it rather than inventing a new one:

1. **Reproduce the oracle's TRUE arm exactly.** For the real s18 dump and the synthetic sweep, recalc's
   authoritative costs — replayed by `(pool_id, posted_at, id)` — must equal the oracle's strict-arm output
   line-for-line (the oracle already computed these; use as golden).
2. **Variance equals `recorded − true` per oracle §2b.** In particular the **trend** rows must reproduce the
   *directional* bias (FIFO +, LIFO −) — the load-bearing check that recalc corrects an accumulating
   misstatement, not a mean-zero jitter.
3. **R-2 vignette A passes.** The backdated-cheaper-receipt case must return the authoritative 100, not the
   `id`-order 300 — the oracle's falsification case becomes a passing recalc test.
4. **Idempotency.** Re-running recalc over an unchanged stream posts zero net new adjustment (full-replay,
   D2(i), makes this trivially true; the durable re-run contract is child (d)'s).
5. **Conservation (acct-0at4.5).** Σ layer value + posted GL value == Σ receipts value per pool — the sweep is
   *more* central under alt-C allow-negative and guards the ill-defined-depletion case (§5).

The oracle also flagged a **test-bed gap** (§2a / §2c): the harness's constant-cost deep seed produces zero
costing variance (a lock/throughput fixture, not a cost-divergence fixture). A recalc correctness bed needs a
receipt-cost-volatility knob (varied costs comparable to pool value) before it can exercise the divergence
offline — carry this into the v3.2 testing strategy (design-v3.2 §7).

## 9. Interfaces to sibling children

- **→ (b) acct-q1oj.2** — (a) defines the per-pool unit of work (full-pool replay, D2(i)) and the re-cost-floor
  abstraction; (b) schedules replays across pools and, if full-replay is the binding cost, adds the checkpoint
  mechanism (D2(ii)) and the materialization strategy.
- **→ (c) acct-q1oj.3** — (a)'s bounded-pool-set / unbounded-within-pool-replay shape (§5) is the input to the
  backlog-depth and cadence-vs-load model.
- **→ (d) acct-q1oj.4** — (a) requires: the `(pool_id, posted_at, id)` ordered access path (D1); persistent
  layer state (`pool_state.layer_id > 0`); per-consumed-layer linkage (D3); and re-cost-floor / checkpoint
  bookkeeping. The durable progress cursor is the §6 slot, **not** a home-grown watermark (§17).
- **→ (e) acct-q1oj.5** — (a) produces the authoritative valuation that (e) finalizes at close.

## 10. Open design decisions (for review — resolved by the named sibling)

- **D1** — denormalize `trx_line.posted_at` + index `(pool_id, posted_at, id)` *(recommend yes)* → child (d).
- **D2** — R-2 strategy: full-pool replay first *(recommend)*, checkpoint later → child (b)/(c).
- **D3** — multi-layer consumption linkage: per-layer posting_line vs linkage table *(recommend per-layer
  granularity; form decided by (d))* → child (d).
- **D4** — re-cost-floor granularity: per-pool-from-opening *(a's baseline)* vs from-checkpoint *(b's
  optimization)* → child (b).

None of D1–D4 gate this child's correctness: full-pool replay in `(pool_id, posted_at, id)` order reproduces
the oracle exactly regardless of how the siblings later optimize storage and scheduling. They are the seams
where (a) hands work to (b)/(c)/(d), recorded so the decomposition stays honest.
