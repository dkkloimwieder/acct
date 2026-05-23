# acct-9mgx.6 — wac_periodic equivalence validation

**Issue:** acct-9mgx.6 (P2; depends on acct-s6fa wac_periodic implementation)
**Run window:** 2026-05-22 — 2026-05-23

## Change

Extends the cross-path equivalence harness (acct-t9lo / acct-33b6 /
acct-h5gs lineage) to validate `wac_periodic` end-state equivalence
across Path A (direct, synchronous in caller tx) and Path B (routed,
async shmem-staging + committer pool).

**Harness extensions:**

- `cli::MethodMix::AllWacPeriodic` variant + `--method-mix` flag on the
  equivalence subcommand. `--method-mix all-wac-periodic` flips three
  switches together:
  1. Seeded pool universe uses `pool_method = wac_periodic`.
  2. An `accounting_period` row is INSERTed before each path's
     submissions (`2026-05-01` … `2026-05-31`, state `'open'`).
  3. `ledger_close_period(period_id)` is called after each path drains.
- New `build_submissions_wac_periodic` workload generator. Alternating
  rounds per caller: even tick = receipt (`qty=10`, `unit_cost = 10 + (tick/2)*7`
  so running avg evolves), odd tick = depletion (`qty=-2`) on the
  caller's most-recent receipt pool. Safe across all overlap shapes —
  every depletion lands on a pool that has at least `qty=10` available.
- Snapshot extended with `posting_lines_provisional` (canonicalized to
  `(pool_id, qty, provisional_amount, variance_amount, has_variance_posting)`).
  Variance posting_lines flow through the existing `trx_groups` channel
  (they live under the `revaluation_run` trx the close hook emits).

## Diff contract (acct-9mgx.6)

Per the bd description: "Expected: byte-equivalent after close.
Within-period provisional postings may differ on amount (running avg
of intermediate state), but final + variance posting should converge."

The diff is split into three buckets:

| Bucket                  | Per-row byte check | Aggregate check                              |
|-------------------------|--------------------|----------------------------------------------|
| `pool_state`            | **load-bearing**   | (n/a)                                        |
| `posting_lines_provisional` | informational  | **load-bearing**: count + Σ qty + Σ(prov+var) per pool |
| `trx_groups` (revaluation_run + depletion trxs on wac_periodic pools) | informational | (covered via provisional aggregate)          |

The structural invariant per pool: `Σ provisional_amount + Σ variance_amount = final_avg × Σ depletion_qty`,
deterministic from the workload alone. Both paths apply the same
receipts (commutative under cumulative-sum WAC) and the same depletions
(same total qty), so this sum MUST match across paths — even when the
split between provisional and variance shifts row-by-row.

Per-row provisional / depletion-trx_line differences are classified as
`wac_periodic_drifts`: each depletion's `provisional_amount` depends on
the running avg at deplete-time, and that avg depends on commit_group
ordering (which is router-driven on Path B and submission-order on
Path A). Variance per row then mirrors: `variance = final_avg × qty − provisional_amount`.

Drifts are informational under default; upgraded to errors via
`--strict` (matching the acct-mcey pattern for cumulative-sum WAC drift).

## Sweep — 6 scenarios × 50 submissions/caller

Workload shape: receipts at qty=10, varying unit_cost; paired depletions
at qty=2. Single accounting_period spans all submissions; closed after
each path drains.

| Scenario | Workload                | Submissions | trx (incl. close) | Pool_state byte-equiv | Provisional aggregate match | Drift count |
|----------|-------------------------|------------:|------------------:|:---------------------:|:---------------------------:|------------:|
| s1       | uniform / simple        |         500 |               501 |          ✓            |             ✓               | 0 (identical) |
| s2       | zipf(1.5) / simple      |        1000 |              1001 |          ✓            |             ✓               |          29 |
| s3       | uniform / complex       |         500 |               501 |          ✓            |             ✓               |          69 |
| s4       | zipf(1.2) / complex     |        1000 |              1001 |          ✓            |             ✓               |         111 |
| s5       | single-hot-pool         |        1000 |              1001 |          ✓            |             ✓               |          52 |
| s6       | disjoint stripes        |        1000 |              1001 |          ✓            |             ✓               | 0 (identical) |

**All 6 scenarios: pool_state byte-equivalent across paths AND
per-pool sum(provisional + variance) match.** This is the load-bearing
correctness property.

Drift count correlates with shared-pool contention shape:
- **s6 disjoint = 0** (no shared pools → no concurrent commit_groups on
  same pool → no per-depletion running_avg divergence).
- **s1 uniform simple = 0** (single-line submissions + uniform overlap;
  router rarely packs same-pool submissions into one commit_group at
  this submission rate).
- **s5 hot pool = 52** (one pool, all 1000 submissions contend; high
  raw drift but bounded by single-pool fan-in).
- **s4 zipf complex = 111** (heaviest contention shape — many pools
  with multiple submissions; the highest drift count, still bounded).

## Strict-mode trials (s2, s4, s5)

`--strict` upgrades wac_periodic drifts to errors. Path B's router
commit_group ordering is timing-sensitive, so strict runs are inherently
non-deterministic — same workload produces different drift counts run to
run depending on when the router window scan fires relative to enqueues.

| Scenario | Strict T1                            | Strict T2                            | Strict T3                            |
|----------|--------------------------------------|--------------------------------------|--------------------------------------|
| s2       | FAILED (74 drifts)                   | OK (0 drifts; happened to be lucky)  | FAILED (51 drifts)                   |
| s4       | FAILED (31 drifts)                   | OK (0 drifts)                         | OK (0 drifts)                         |
| s5       | OK (0 drifts)                         | FAILED (162 drifts)                  | OK (0 drifts)                         |

Strict variability is **expected, not a bug**. The wac_periodic drift
class is fundamentally a consequence of concurrent commit_group ordering
on shared pools (acct-mcey's analog for the periodic close); the harness
does not pin Path B to single-committer for these trials. To require
strict-pass on a wac_periodic run, configure
`ALTER SYSTEM SET ledger_routed.committer_count = 1` + postmaster
restart (same workaround documented for acct-mcey's WAC strict mode).

## Cross-references

- **acct-s6fa** (closed, 2026-05-22) — wac_periodic implementation
  (schema migration 0007, ledger-core wac_periodic.rs, ledger_close_period
  SPI, posting_lines_provisional FK + finalization machinery).
- **acct-h5gs** (closed, 2026-05-22) — WAC cumulative-sum form that
  wac_periodic clones for its receipt/depletion math.
- **acct-mcey** (closed, 2026-05-22) — original WAC drift classifier
  that this work extends with the `wac_periodic_drifts` bucket.
- **acct-9mgx.1** (open, not blocking) — FIFO bench + shared
  `--method-mix` harness extension; the `--method-mix` plumbing landed
  here as collateral so acct-9mgx.6 could run independently. `.1` will
  add `MethodMix::AllFifo` handling to seed/workload paths.

## Harness invocation

```bash
# Lenient (default): drifts are informational
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --submissions-per-caller 50 \
    --method-mix all-wac-periodic

# Strict: drifts upgrade to errors
cargo run --release -p ledger-harness -- equivalence \
    --scenario s4 \
    --submissions-per-caller 50 \
    --method-mix all-wac-periodic \
    --strict
```

## What this DOES NOT change

- **No ledger-core changes.** wac_periodic.rs (acct-s6fa) is unchanged;
  this work only extends the harness.
- **No SPI changes.** ledger_close_period was shipped under acct-s6fa.
- **No schema changes.** Migration 0007 (acct-s6fa) is the relevant
  schema baseline.
- **No ledger-direct / ledger-routed changes.** The bulk_write
  `insert_provisional_postings` symmetry was shipped under acct-s6fa.

## What's deferred to siblings

- **acct-9mgx.7** (cross-method comparison roll-up doc) — blocks on
  .1–.6 completing.
- **Performance characterization under wac_periodic** — equivalence ≠
  bench. The equivalence binary's single-threaded submission shape is
  the wrong tool for throughput measurement; the bench scenarios live
  in the `run` subcommand and would need a wac_periodic mode there too
  (deferred — not in acct-9mgx.6's bd description, which targets
  equivalence specifically).
