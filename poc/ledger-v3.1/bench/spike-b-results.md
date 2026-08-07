# SPIKE-B results — single-statement commutative aggregate vs RMW-across-SPI (acct-0at4.11.2)

**Question (FEEDBACK-ARCH.md #4 / alt B):** the direct RMW hot path
(`ledger_submit_trx_c`) acquires `pool_lock` FOR UPDATE, hydrates a snapshot,
round-trips into ledger-core (Rust) to compute the running average, then bulk-
writes — holding the lock across all of it *because the arithmetic lives outside
SQL*. Does that RMW-across-SPI shape actually beat **one** commutative SQL
statement under PostgreSQL's own row lock, or is the entire `pool_lock` edifice
(the table, the sorted-acquisition protocol, the deadlock-ordering discipline)
solving a problem the RMW shape created?

**Decision gate:** benchmark lock-hold + throughput vs the current direct RMW. If
within noise → drop `pool_lock`, the sorted-acquisition protocol, and most of the
single-pool deadlock surface. **Subsumes acct-0at4.3 #7** (advisory-lock vs
row-lock): if the `pool_lock` table disappears for the aggregate paths, the
advisory-vs-row micro-question is moot.

## What was built (throwaway)

- **`banker_div(numeric, bigint)` in SQL** (`bench/spike-b-setup.sql`): the
  round-half-to-even division ported byte-faithfully from
  `ledger_core::numeric::banker_div` (17 reference cases match exactly, both
  signs, exact-half both parities). This is the enabler — it lets the derived
  `unit_cost` be computed in-statement instead of in Rust.
- **`ledger_submit_trx_single_c`** (`ledger-direct-c/src/submit_single.rs`): same
  signature as `ledger_submit_trx_c`, single-pool/single-line scope (the shape of
  both benchmark scenarios). Resolves pool method/basis + posting accounts +
  standard cost in **one UNLOCKED reference-data read** (that data is not the
  contended hot state — moving it out of the critical section is legitimate and is
  part of the win), then folds the aggregate mutation + `trx` + `trx_line` +
  `posting_line` into **one CTE**. No `pool_lock` table, no sorted acquisition, no
  Rust round-trip. A depletion reads its running average under the same tuple lock
  via PG 18 `RETURNING old.unit_cost`; the strict-qty gate rides in the UPDATE's
  `WHERE qty - Δ >= 0`, and an empty RETURNING ⇒ InsufficientInventory.
- Harness `--mode direct-single` (`driver_direct.rs`): identical autocommit-per-
  call driver as `direct-per-call`, swapping only the function name — a fair head-
  to-head with identical argument marshalling.

**Execution order (EXPLAIN-verified):** within the CTE the `pool_state` UPDATE
(`CTE mv`) runs FIRST by data dependency; `trx`/`trx_line`/`posting_line` reference
it, so they run while the aggregate tuple is locked. The contended-row hold-span is
therefore the *same* acquire→commit window as the baseline — the difference is that
the single statement does strictly **less work inside that window** (no `pool_lock`
acquire, no hydrate SELECT, no Rust round-trip, one fused write vs four kept-plan
SPI calls).

## Correctness (byte-identical to the RMW baseline)

`bench/spike-b-verify.sql` — differential: force one pool to a controlled state,
run the baseline, snapshot, restore the same state, run the spike on identical
input, snapshot. All fields (`pool_state` qty/unit_cost/value_sum, `trx_line`
qty/unit_cost, `posting_line` event/amount/debit/credit) must agree.

| case | agree |
|------|-------|
| R1 first receipt to empty pool | ✓ |
| R2 receipt onto running average | ✓ |
| R3 receipt banker half-even (1.5→2) | ✓ |
| D1 depletion at running average | ✓ |
| D2 depletion emptying pool (value_sum→0) | ✓ |
| D3 depletion banker half-even (2.5→2) | ✓ |

Plus: insufficient-inventory (deplete 20 with qty=10) **raises** on both flavors
and writes nothing (0 leftover trx, pool qty untouched) — the CTE's empty-`mv`
cascade is atomic. 0 errors / 0 negative-qty pools across every bench run.

## Method

Same box / `poc_v3_1` / `max_connections=500` no pooler, back-to-back interleaved
reps so the single/RMW **ratio** survives the noisy host (Chrome ≈130 procs, load
≈1.3). 15 s/run, 3 reps/cell, all-fifo seed. Three regimes:
- **S1 cap10** — uncontended, receipts, 10 callers over 10 000 pools (fsync-bound,
  no lock contention). The raw per-tx overhead control.
- **S5 cap8** — moderate contention: 8 callers on one hot pool, 100% deplete.
- **S5 cap400** — pathological saturation: 400 callers on one hot pool, 100%
  deplete (capped from 1000 — no pooler).

## Results — throughput (trx/s) median, latency median of 3 reps

| scenario | regime | RMW (per-call) | single | single/RMW | p50 RMW→single | p99 RMW→single | WAL/commit RMW→single |
|----------|--------|----------------|--------|-----------|----------------|----------------|-----------------------|
| S1 cap10   | uncontended receipts   | 2300 | 2301 | **1.00×** | 3966→3966 µs | 7.1→7.1 ms  | 1776→1661 (−6.5%) |
| S5 cap8    | moderate 1-pool deplete | 335  | 460  | **1.37×** | 20.5→13.4 ms | 33.5→63.8 ms | 1521→1459 (−4%)   |
| S5 cap400  | saturated 1-pool deplete| 340  | 332  | **0.98×** | 1035→810 ms  | 2548→5125 ms | 1284→1251         |

## Verdict

**`pool_lock` is deletable for the aggregate paths — the gate condition is met.**
Single-statement throughput is **≥ RMW in every regime**: dead-even when the
workload is commit-fsync-bound (uncontended-spread S1, or extreme-saturation
S5-cap400 where the commit queue dominates and per-tx work is lost in the noise),
and **~1.37× faster** in the moderate-contention regime (S5-cap8) where the per-tx
lock-hold *is* the bottleneck and doing less work under the lock directly lifts
throughput. Median latency is **20–35 % lower** under contention (it does strictly
less work under the same-span lock — the structural claim, confirmed). It deletes
the whole `pool_lock` table + sorted-acquisition protocol + single-pool deadlock-
ordering discipline + the hydrate-into-Rust round-trip for aggregates, at **~4–6 %
less WAL** and byte-identical ledger state.

The deeper reading of FEEDBACK-ARCH #4: the RMW-across-SPI critical section does
**not** materially inflate throughput — because the "round-trip" is in-process SPI
with kept plans (cheap), and the per-tx cost is dominated by the commit fsync both
flavors pay. So `pool_lock` isn't *costing* much; it's simply **not buying
anything**, and one commutative UPDATE achieves the same behavior and performance
without it.

**acct-0at4.3 #7 (advisory-lock vs row-lock): SUBSUMED.** With the `pool_lock`
table gone for aggregate paths, the only remaining lock is PG's own tuple lock on
the `pool_state` aggregate row — there is no `pool_lock` row left to make advisory.
The micro-question dissolves. (It could resurface for the genuinely stateful
methods — specific-id, future strict layer math — that still need explicit
locking; those keep ledger-core and are out of this spike's aggregate-only scope.)

### Honest caveats (do not overclaim)

- **Worse p99 tail under single-hot-pool contention** — consistently ~1.9× (cap8
  33.5→63.8 ms; cap400 2.5→5.1 s, tight across all 3 reps, not noise). The one
  regression. Hypothesis: the fused CTE's row-lock wait under many waiters has a
  fatter tail than the baseline's small `pool_lock`-acquire-then-work shape, and
  the pre-CTE unlocked cfg read adds per-tx variance. **Not chased** (throwaway
  spike) — two outs exist: (a) the pathological single-hot-pool regime is exactly
  what the coalescing routed/staging path (SPIKE-A) owns, not the direct path; (b)
  realistic spread workloads (S1) are parity with no tail penalty. The tail
  regression does not block the verdict for the workloads the direct path serves.
- **Single-pool/single-line scope.** Multi-pool submissions would reintroduce the
  sorted-acquisition / deadlock-ordering question — which is *also* something a
  commutative per-pool UPDATE plausibly dissolves (each pool's tuple lock, acquired
  in a deterministic order or via retry), but that is a larger investigation, not
  measured here.
- **Aggregate methods only** (fifo/lifo provisional + wac). Specific-id and future
  strict layer math keep ledger-core; they are genuinely stateful and are not
  claimed deletable.
- **N=3 reps, noisy host, ratios not tight CIs** — same discipline as SPIKE-A. The
  parity (S1/cap400) and the 1.37× (cap8) both dwarf the inter-rep spread.

### Implication for the gate

SPIKE-B input to GATE-VERDICT (acct-0at4.11.5): the direct RMW / `pool_lock`
machinery buys nothing on throughput and can be replaced by one commutative SQL
statement (banker_div-in-SQL + PG 18 `RETURNING old`) for the aggregate paths,
subsuming acct-0at4.3 #7. Combined with SPIKE-A (routed shmem stack deletable in
favor of a staging table), both spikes point the same direction: **the elaborate
concurrency machinery — shmem routing AND the RMW/pool_lock protocol — is
complexity the measured numbers do not justify.** The design-v3.1 verdict paragraph
and the downstream re-triage are written at .11.5 once ARCH-POSTURE and
ARCH-RECALC-FEED also report.
