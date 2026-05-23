# acct-9mgx.5 — WAC-perpetual equivalence validation (canonical)

**Issue:** acct-9mgx.5 (P2; depends on acct-9mgx.1 for the `--method-mix` harness extension, which actually shipped early as collateral under acct-9mgx.6)
**Run window:** 2026-05-23
**Sweep timestamp:** 2026-05-23T11-52-11Z
**Sweep index:** `results/phase6/equivalence/run-2026-05-23T11-52-11Z.log`

## Purpose

Re-run the WAC-perpetual cross-path equivalence sweep under the unified
`--method-mix` harness shape introduced in acct-9mgx.6, for cross-method
apples-to-apples comparison with wac_periodic / FIFO / LIFO / Specific /
STD siblings (acct-9mgx.{1..4,6}). This is the **canonical** WAC-perpetual
equivalence reference; supersedes `h5gs-cumulative-sum-validation.md`
which captured the original structural-fix validation under the focused
AllWac-only harness shape.

## What changed since h5gs

- **acct-s6fa** (2026-05-22) added `wac_periodic` as a sibling cost
  method (new enum variant, posting_lines_provisional table, close
  hook). Additive — does not touch the WAC code path.
- **acct-9mgx.6** (2026-05-23) extended the equivalence harness with
  `--method-mix` (`AllWac` | `AllWacPeriodic` | `AllFifo` | `Mixed`),
  reset_ledger TRUNCATEs the new provisional table, conditional period
  seed + close-period dispatch, and new diff buckets
  (`wac_periodic_drifts`, aggregate-per-pool provisional invariant).
  All additions are gated on `MethodMix::AllWacPeriodic`; `AllWac` runs
  exercise the same code path as pre-9mgx.6.

The expectation per the bd description was strict: "Bench numbers
should reproduce h5gs validation: ALL 15 sweep runs byte-identical. If
they do not, the harness extension introduced a regression."

## Result: 15/15 byte-identical

| Scenario | Workload                | Submissions |     trx |  trx_line | pool_state | posting_line | Lenient | Strict T1 / T2 / T3 |
|----------|-------------------------|------------:|--------:|----------:|-----------:|-------------:|:-------:|:-------------------:|
| s1       | uniform / simple        |         500 |     500 |       500 |        100 |          500 |    ✓    |          —          |
| s2       | zipf(1.5) / simple      |        1000 |    1000 |      1000 |         71 |         1000 |    ✓    |       ✓ / ✓ / ✓     |
| s3       | uniform / complex       |         500 |     500 |    15 327 |        100 |       15 327 |    ✓    |          —          |
| s4       | zipf(1.2) / complex     |        1000 |    1000 |    29 785 |        100 |       29 785 |    ✓    |       ✓ / ✓ / ✓     |
| s5       | single-hot-pool         |        1000 |    1000 |      1000 |          1 |         1000 |    ✓    |       ✓ / ✓ / ✓     |
| s6       | disjoint stripes        |        1000 |    1000 |      1000 |         20 |         1000 |    ✓    |          —          |

**Every run identical at byte level. Zero `wac_drifts` across the
full sweep, lenient and strict.** Exactly reproduces the h5gs table
(see `h5gs-cumulative-sum-validation.md` § "Validation sweep") row for
row, value for value. The harness extension is additive and introduces
no regression to the AllWac path.

## Why this works

Unchanged from h5gs — the WAC cumulative-sum form
(`pool_state.unit_cost` stores total `value_sum` for WAC rows; running
average is computed on demand) makes receipts additive-commutative and
makes each depletion a single bounded round on identical inputs. See
`h5gs-cumulative-sum-validation.md` § "Why this works" for the full
argument. Nothing in `ledger-core/src/wac.rs` has changed.

## Harness invocation

```bash
# Single scenario (lenient)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --submissions-per-caller 50
    # --method-mix all-wac is the default

# Single scenario (strict)
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --submissions-per-caller 50 \
    --strict

# Full 15-trial sweep (6 lenient + 9 strict trials × 3 race-conditional scenarios)
bash scripts/run-equivalence-sweep.sh
```

## What this DOES NOT change

- **No ledger-core changes.** `wac.rs` cumulative-sum logic unchanged since acct-h5gs.
- **No schema changes.** Migration 0006 (catalog comment doc-only) remains the relevant baseline; migration 0007 (acct-s6fa wac_periodic) does not touch the WAC code path.
- **No ledger-direct / ledger-routed changes** that affect AllWac runs. The 9mgx.6 bulk-write insert_provisional_postings extension is no-op when `plan_result.provisional_postings` is empty (always the case for WAC).

## Cross-references

- **acct-h5gs** (closed, 2026-05-22) — original WAC cumulative-sum
  structural fix. The implementation lives there; this doc only
  re-validates it under the new harness shape. See
  `h5gs-cumulative-sum-validation.md` for the full structural argument
  (math contract, why receipts commute, how depletions round
  deterministically, comparison vs the discarded acct-iwlq scaling
  band-aid).
- **acct-mcey** (closed, 2026-05-22) — surfaced the running-average
  truncation drift that motivated h5gs; shipped the `wac_drifts` diff
  bucket and `--strict` opt-in.
- **acct-33b6** (closed, 2026-05-22) — pre-h5gs running-average sweep
  baseline (`equivalence-summary.md`).
- **acct-9mgx.6** (closed, 2026-05-23) — wac_periodic equivalence
  sibling. Introduced the `--method-mix` harness extension that this
  validation runs against. See `wac-periodic-validation.md`.
- **acct-9mgx.{1,2,3,4}** (open) — remaining method siblings
  (FIFO, LIFO, Specific, STD) under the unified harness.
- **acct-9mgx.7** (open, blocked on .1–.6) — cross-method roll-up doc.
