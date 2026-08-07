# Offline strict-FIFO/LIFO replay oracle — results (acct-0at4.7)

**Question (FEEDBACK-TESTING.md #6 / FEEDBACK.md #11 — the highest-business-value
test of the set).** Path C records FIFO/LIFO depletions at a *provisional*
unit_cost (the running average, or the standard cost) and touches only the
aggregate row — it never walks layers on the hot path (design-v3.1 §3.5). Under
the alt-C gate verdict (§16/§18) **recalc is the sole costing engine**, and its
whole job is to reconstruct authoritative strict-FIFO/LIFO cost *from the recorded
`trx_line` stream later*. Two things were untested/unmeasured:

1. **Premise.** Is the recorded stream *sufficient* to reconstruct authoritative
   FIFO/LIFO — or do backdated receipts (§14.5) / chunk reordering (§14.2) make it
   ill-defined? If ill-defined, the surviving architecture is falsified — cheaply,
   now, before recalc/close is built.
2. **Variance magnitude.** How far does the recorded provisional cost sit from the
   true strict cost, per basis (running_avg vs standard) and per cost-volatility
   profile? **That number is what Path C's production go/no-go consumes** and what
   `acct-0at4.12` needs to size the recalc adjustment storm.

## What was built (throwaway — `bench/replay-oracle/`)

A standalone, workspace-excluded Rust bin. Deliberately asymmetric:

- **RECORDED (provisional) arm = the SHIPPED transform.** It calls
  `ledger_core::plan_apply_provisional` / `banker_div` directly — *not* a
  re-implementation. This is the exact code the SQL hot path mirrors (SPIKE-B
  proved the SQL byte-identical), so the "recorded" number carries zero
  reproduction risk.
- **TRUE (strict) arm = the throwaway layer-walk oracle.** Production `ledger-core`
  has only `MethodMismatch` stubs for strict FIFO/LIFO (§8) — which is *why* this
  oracle has to exist. It maintains a layer deque, consumes front (FIFO) / back
  (LIFO), and prices each depletion at the banker-rounded weighted layer cost (the
  same 1e-6 fixed-precision rounding, §3.0). It does not need production quality.

Three modes: `ordering` (falsification vignettes), `synth` (variance sweep),
`real <tsv-dir>` (replay a real harness dump). Driver: `bench/replay-oracle-run.sh`
reseeds `poc_v3_1` all-fifo depth-10, drives s18 (mixed receipts+depletions, 50
callers, Pareto 80/20), dumps `pool`/seed-layers/`trx_line`, and runs all three.
Read-only characterization; no docker restart needed (the reseed TRUNCATEs clean).

---

## 1. Premise — is the stream reconstruction-sufficient?

### 1a. Reconstruction fidelity on a REAL concurrent dump

Replaying the real s18 dump's run-time `trx_line` rows **in `trx_line.id` order**
through the shipped provisional transform, seeded from each pool's reconstructed
opening aggregate, reproduces the recorded `trx_line.unit_cost` for **every**
depletion:

- pools in dump: 10 000 · run-time trx_lines: 298 279 · **depletions checked: 149 392**
- **recorded == reconstructed(id-order): 149 392 / 149 392 = 100.0000%**
- ill-defined depletions (no covering layer in strict replay): **0**

So `trx_line.id` faithfully reflects *application order* even under 50 concurrent
callers on a Pareto-hot pool: each single-line submission's `id` is drawn under the
same aggregate-tuple lock that serializes the running-average update, so the
allocation order the stream records *is* the order the math was applied. The
§14.6 "a lower-id row committed later" hazard does **not** corrupt the recorded
provisional value here.

### 1b. …but id-order is NOT sufficient to reconstruct *authoritative* FIFO

Reproducing the *provisional* record is not the same as reconstructing *true*
FIFO. The `ordering` vignettes build one fifo/running_avg pool where the intended
business order differs from commit order (a backdated receipt, §14.5):

| vignette | recorded provisional (commit) | true FIFO by `trx_line.id` | true FIFO by `posted_at` | verdict |
|----------|------------------------------:|---------------------------:|-------------------------:|---------|
| **A** — backdated cheaper receipt (business ≠ commit) | 300 | **300** | **100** | ❌ id-order reconstruction (300) ≠ authoritative (100), **off by 200 (200% of true)** |
| **B** — same lots, no backdating (business == commit) | 200 | 100 | 100 | ✅ id-order reconstruction == authoritative |

Vignette A is the falsification case: a cheap lot business-dated *first* but
committed *last* (arrived late). A recalc that replays the persisted stream by
`trx_line.id` consumes the wrong layer and returns 300; the authoritative answer,
replaying by business date, is 100. **Nothing in the recorded `(qty, unit_cost,
id)` triple lets you recover the 100** — only a faithful business-chronology key
(`posted_at`) plus a chronological recalc sort does. Vignette B (business order ==
commit order) reconstructs exactly.

### 1c. The current stream carries no intra-run chronology

The real dump's `posted_at` is **degenerate** — two constant values, one for the
seed date and one for the entire run:

- distinct `posted_at` across the stream: **2** (`2026-01-01` × 100 000 seed
  receipts; `2026-05-25` × 298 279 run lines)

The harness stamps a compile-time-constant `posted_at` on every run submission, so
within the measurement window there is *no* business-order signal at all — `id` is
the only usable key. That is fine *today* only because the harness injects no
backdated receipts; if it did, the stream (as currently populated) could not be
corrected.

### Premise verdict

**The stream is reconstruction-sufficient if and only if (a) it carries a faithful
business-chronology key and (b) recalc sorts by it.** The recorded
`(qty, unit_cost, id)` fields reproduce the *provisional* record exactly (100% on
149 k real depletions) but do **not** reconstruct *authoritative* FIFO/LIFO when
business order diverges from commit order. The precise input that breaks it:
**backdated events — any case where `trx.posted_at` business-order ≠ `trx_line.id`
commit-order** (§14.5; the concurrent-reorder cousin is §14.2/§14.6).

This does **not** falsify the alt-C architecture — it *sharpens its contract*, and
lands exactly on the caveat already recorded for the recalc feed (§17): the
logical-decoding slot delivers commit-order, and **the within-pool re-sort to
business chronology stays recalc's job.** Two concrete requirements fall out for
the recalc/close phase (`acct-0at4.12`):

- **R-1.** `trx.posted_at` MUST carry the real business/effective date (not a
  wall-clock stamp), and `trx_line` reconstruction MUST order by
  `(pool_id, posted_at, id)` — `id` as the deterministic within-date tiebreak, not
  the primary key.
- **R-2.** A backdated receipt whose `posted_at` precedes already-costed depletions
  forces those depletions to be *re-costed* — this is the adjustment-storm driver
  `acct-0at4.12` must size, and the reason recalc cannot be a simple forward scan.

---

## 2. Variance magnitude

### 2a. Native anchor (real s18 dump) — structurally zero, and why

Over the same 149 392 real depletions, provisional-vs-true-FIFO variance is
**exactly zero** (mean |Δ| = 0, p99 = 0, 100% exact). Cause: the deep-seed uses a
single constant layer cost (1.0 = 1 000 000 µ-units) and run-time receipt costs
(1–1000 µ-units) are ~1000× smaller, so the running average never moves off the
seed and true-FIFO front layers all share that one cost. **The harness's default
workload cannot measure costing variance** — it is a lock/throughput fixture, not a
cost-divergence fixture. Hence the controlled synthetic sweep below; the real dump's
role is the premise check (§1), where it is decisive.

### 2b. Synthetic sweep — the go/no-go numbers

300 pools × 300 interleaved lines/pool (≈50% depletions), receipt costs centred on
500 000 µ-units. Variance = `recorded − true` in µ-units; `rel` = mean|Δ| /
mean(true). Volatility profiles: **low** ±2%, **med** ±40%, **high** ±90% uniform;
**trend** = monotone rising cost (front cheap, latest dear).

| method | basis | profile | depl. | exact% | mean Δ | mean\|Δ\| | p50\|Δ\| | p90\|Δ\| | p99\|Δ\| | max\|Δ\| | rel |
|--------|-------|---------|------:|-------:|-------:|--------:|-------:|-------:|-------:|-------:|----:|
| fifo | running_avg | low (±2%)      | 44887 | 0.6  | +9      | 3483   | 2872   | 7876   | 9900   | 12094  | 0.70%  |
| fifo | running_avg | med (±40%)     | 45286 | 0.7  | +180    | 70348  | 58392  | 158195 | 198498 | 241896 | 14.07% |
| fifo | running_avg | high (±90%)    | 44953 | 0.6  | +998    | 159260 | 132931 | 359886 | 445641 | 539065 | 31.94% |
| fifo | running_avg | trend (rising) | 44933 | 1.4  | +57957  | 57975  | 58415  | 99176  | 136287 | 174376 | 18.75% |
| fifo | standard    | low (±2%)      | 44728 | 21.9 | +34     | 3431   | 2980   | 7977   | 9752   | 10000  | 0.69%  |
| fifo | standard    | med (±40%)     | 44848 | 22.0 | +438    | 68232  | 58592  | 159035 | 195041 | 199988 | 13.66% |
| fifo | standard    | high (±90%)    | 44831 | 22.0 | -806    | 153203 | 131112 | 358839 | 439078 | 449938 | 30.59% |
| fifo | standard    | trend (rising) | 44981 | 0.1  | +190541 | 190999 | 200632 | 300000 | 300000 | 300000 | 61.72% |
| lifo | running_avg | low (±2%)      | 45024 | 0.7  | +4      | 4391   | 4176   | 8365   | 10022  | 11944  | 0.88%  |
| lifo | running_avg | med (±40%)     | 45152 | 0.6  | -1532   | 87664  | 83577  | 167314 | 200375 | 230105 | 17.47% |
| lifo | running_avg | high (±90%)    | 45170 | 0.7  | +1483   | 197130 | 188672 | 376617 | 449645 | 523244 | 39.28% |
| lifo | running_avg | trend (rising) | 45140 | 1.3  | -121283 | 121576 | 122696 | 204518 | 243403 | 271599 | 24.94% |
| lifo | standard    | low (±2%)      | 44901 | 1.4  | -11     | 4499   | 4304   | 8588   | 9843   | 10000  | 0.90%  |
| lifo | standard    | med (±40%)     | 45202 | 1.2  | +263    | 90021  | 86507  | 171735 | 196990 | 199999 | 18.01% |
| lifo | standard    | high (±90%)    | 44815 | 1.2  | -1871   | 201896 | 192662 | 385898 | 442628 | 450000 | 40.23% |
| lifo | standard    | trend (rising) | 44927 | 0.4  | +6409   | 151321 | 152000 | 274292 | 300000 | 300000 | 30.66% |

**Readings that matter for the go/no-go:**

- **Volatility scales the error monotonically.** running_avg FIFO: 0.70% (low) →
  14% (med) → 32% (high) of unit cost. At high cost dispersion the provisional
  ledger is ~⅓ wrong per depletion on average, p99 ≈ 45–54% — the mid-period GL is
  materially misstated until recalc trues it up.
- **Under a cost *trend* the error is BIASED, not mean-zero.** running_avg is
  directional and opposite by method: FIFO **+57 957** (running average *overstates*
  vs the cheap consumed front) vs LIFO **−121 283** (running average *understates*
  vs the dear newest layer). A biased error does not wash out across many
  depletions — it accumulates into a real period-level misstatement recalc must
  correct. This is the strongest argument that recalc is load-bearing, not cosmetic.
- **`standard` basis is only as good as the standard.** When the standard equals
  the true mean and dispersion is low it matches often (≈22% exact), but under a
  cost trend it is the *worst* basis (fifo/standard/trend rel **61.7%**) because a
  fixed standard cannot track a moving front at all.
- **Method matters for magnitude.** LIFO diverges more than FIFO from a running
  average (lifo/high 39–40% vs fifo/high 31–32%) because LIFO's consumed cost is the
  newest layer, furthest from the blended average.

---

## Bottom line

- **Premise: conditionally sufficient — CONFIRMED with a sharpened contract, not
  falsified.** The recorded stream reproduces the provisional record exactly (100%
  on 149 k real concurrent depletions) and reconstructs authoritative FIFO/LIFO
  **iff** `posted_at` carries true business dates and recalc sorts
  `(pool_id, posted_at, id)`. The breaking input is precisely characterized:
  backdated events (business-order ≠ commit-order). Feeds `acct-0at4.12` as
  requirements R-1/R-2 above; consistent with the §17 recalc-feed decision.
- **Variance: measured and material.** 0.7%→40% of unit cost across the volatility
  range, and **directionally biased under a cost trend** (FIFO +, LIFO −) — the
  quantitative basis for the Path C production go/no-go and the recalc
  adjustment-storm sizing.
- **Tooling finding (surface to `acct-0at4.12`).** The harness's constant-cost deep
  seed produces zero costing variance; a recalc/close test bed needs varied receipt
  costs comparable to pool value (a `seed`/`workload` cost-volatility knob) before
  it can exercise the divergence this oracle characterizes offline.

Reproduce: `bench/replay-oracle-run.sh` (needs `poc_v3_1` + the built harness).
