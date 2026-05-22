# acct-h5gs — WAC cumulative-sum validation

**Run:** 2026-05-22T20:54:24Z
**Issue:** acct-h5gs (P2; structural replacement for acct-mcey running-avg drift)
**Companion docs:** `equivalence-summary.md` (pre-h5gs baseline at 19:01:40Z)

## Change

Replace WAC's running-average storage with cumulative-sum. The
`pool_state.unit_cost` column is reinterpreted per-method:

| Field on pool_state | FIFO/LIFO/Specific  | WAC (post-h5gs) |
|---|---|---|
| `qty`       | layer qty           | qty_sum (total)   |
| `unit_cost` | per-layer unit cost | value_sum (total) |

For WAC, the running per-unit cost is computed on demand as
`unit_cost / qty` and never stored. See migration 0006 + `ledger-core/src/wac.rs`
for the per-method storage contract and the apologetic naming discussion.

**Math contract (WAC):**
- Receipt of `(Q, C)`: `qty += Q; unit_cost += Q × C` — exact, commutative, associative.
- Depletion of `Q`: `amount = (Q × unit_cost) / qty` (single bounded round); `qty -= Q; unit_cost -= amount` (exact subtraction).

**Touch points:**
- `db/migrations/0006_pool_state_wac_value_sum.{up,down}.sql` — column comments (no schema change)
- `ledger-core/src/wac.rs` — rewritten apply_wac / receipt / deplete
- `ledger-core/src/snapshot.rs` — PoolStateRow doc clarifying per-method semantic
- `ledger-core/tests/method_wac.rs` — 11 tests rewritten in cumulative-sum terms
- `ledger-harness/src/equivalence.rs` — diff classifier doc + messaging updated for value_sum semantic

Hydration + bulk_write + all extension code unchanged (column reads/writes
are method-agnostic — only the interpretation of stored values shifts).

## Validation sweep

Same 15-run shape (6 lenient + 9 strict trials × 3 race-conditional scenarios).

| Scenario | Workload | Trx | Lines | Lenient | Strict T1 / T2 / T3 |
|---|---|---:|---:|:---:|:---:|
| s1 | uniform / simple    |  500 |   500 | ✓ identical | — |
| s2 | zipf(1.5) / simple  | 1000 |  1000 | ✓ identical | ✓ / ✓ / ✓ |
| s3 | uniform / complex   |  500 | 15327 | ✓ identical | — |
| s4 | zipf(1.2) / complex | 1000 | 29785 | ✓ identical | ✓ / ✓ / ✓ |
| s5 | single-hot-pool     | 1000 |  1000 | ✓ identical | ✓ / ✓ / ✓ |
| s6 | disjoint stripes    | 1000 |  1000 | ✓ identical | — |

**Every run identical at byte level. Zero drift across the full sweep.**

## Comparison vs prior approaches

| | Pre-mcey (running-avg) | acct-iwlq (M=10000 scaling, discarded) | acct-h5gs (cumulative-sum) |
|---|---|---|---|
| s5 strict | 3/3 fail (always ±1)             | 2/3 fail (rare) | **3/3 pass** |
| s4 strict | 3/3 fail (10-19 drifts)          | 3/3 fail (8-17 drifts) | **3/3 pass** |
| s2 strict | 2/3 fail                         | 2/3 fail | **3/3 pass** |
| s3 lenient | 0 drifts                        | 56 drifts (sub-unit) | **0 drifts** |
| Max \|Δ\| human-unit | 4                       | 0.0006 | **0** |
| Receipt-side compounding error | YES               | YES (smaller grain) | **NO (additive exact)** |
| Implementation surface | n/a                     | 5 files + 1 mig | **5 files + 1 mig** |
| Schema change | n/a                              | column comments | column comments |

Cumulative-sum is structurally superior AND comparable in implementation
surface to the scaling band-aid that preceded it.

## Why this works

**Receipt-only workloads (s5).** Receipts are pure additive ops on
`(qty, value_sum)`. Path A processes them serially; Path B's router +
committers process them through different commit_group ordering. The
final `(qty, value_sum)` after applying the same set of receipts is
identical regardless of order — additive commutative. Hence
byte-identical pool_state.

**Workloads with depletions (s2, s3, s4).** Each depletion does a
single bounded round `amount = (Q × value_sum) / qty`. In the
equivalence harness's serial-submission shape, both paths see the
**same** `(qty, value_sum)` at each depletion (submissions arrive in
identical order; both paths process them in enqueue order; commit_group
ordering preserves submission order within the test). Therefore
the rounded `amount` is identical, and `value_sum -= amount` produces
identical post-depletion state. Hence byte-identical pool_state +
trx_line + posting_line.

**Disjoint stripes (s6).** No shared pools, so commit_group ordering
across pools is irrelevant — each pool's state mutates only via its
own caller. Byte-identical.

## When cumulative-sum WOULD show drift

The equivalence harness's serial-submission constraint masks one
genuine drift source: **concurrent commit_groups touching the same pool
under a real load test**. If two committers race to acquire `pool_lock`
on the same pool and the loser sees post-winner state, the loser's
depletion-time round operates on different `(qty, value_sum)` than the
winner's. The drift is per-event bounded (single rounding per
depletion, not compounding) and would surface as small `value_sum`
deltas in the `wac_drifts` bucket of a real-load equivalence test.

This is the property documented in `equivalence.rs::DiffResult` —
informational under default; upgrade to errors via `--strict`. The
equivalence harness's per-doc-comment "single-threaded by design"
constraint isolates ledger-core's correctness from real concurrency,
which is what we want for a structural equivalence check.

## What this DOES NOT change

- **No schema migration** — column types, constraints, indexes unchanged. Only catalog comments document the new per-method semantic.
- **No hydration or bulk_write code change** — the extensions read/write the same columns. The reinterpretation lives entirely in `ledger-core/src/wac.rs` (with the user-facing display rule `running_avg = unit_cost / qty` consumed by equivalence's wac_drifts diff line).
- **No ledger-core public API change** — `plan_apply` / `Snapshot` / `PlanResult` signatures unchanged. PoolStateRow gains a doc-comment about per-method semantics; no field additions.
- **No effect on FIFO / LIFO / Specific / STD** — those methods still interpret the column as per-layer unit cost.

## The footgun (acknowledged, documented)

`pool_state.unit_cost` for WAC rows now stores a **total value**, not
a per-unit cost. Ad-hoc operator queries `SELECT unit_cost FROM
pool_state` on a WAC row will see (e.g.) `500` instead of `50` for a
pool with 10 units at $50 each. Migration 0006 includes the canonical
display query:

```sql
SELECT pool_id, qty,
       CASE pool.method
         WHEN 'wac' THEN unit_cost::float / NULLIF(qty, 0)
         ELSE unit_cost::float
       END AS running_unit_cost
  FROM pool_state JOIN pool ON pool.id = pool_state.pool_id;
```

The PoC accepts this footgun (zero schema/hydration/bulk_write churn).
A future productionization should split into two named columns
(`value_sum BIGINT NULL` + drop NOT NULL on `unit_cost` for WAC rows)
or rename to a method-agnostic identifier. The Option<i64> threading
through PoolStateRow / PoolStateMutation / 4-variant pool_state_mutations
that this requires was traded away for the smaller diff — see acct-h5gs
discussion + the AskUserQuestion design call on 2026-05-22.

## Cross-references

- **acct-mcey** (closed, 2026-05-22) — surfaced the running-average truncation drift; shipped the wac_drifts diff bucket + `--strict` opt-in.
- **acct-33b6** (closed, 2026-05-22) — characterized drift across all six scenarios under the running-average model (pre-h5gs baseline at `equivalence-summary.md`).
- **acct-iwlq** (closed superseded, 2026-05-22) — M=10000 scaling band-aid validated then discarded in favor of structural fix.
- **acct-h5gs** (this) — structural fix via cumulative-sum reinterpretation.
