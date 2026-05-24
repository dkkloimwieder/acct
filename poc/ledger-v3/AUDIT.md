# AUDIT — PoC v3 design + implementation coherence + quality

**Issue:** acct-b4z4 (P1)
**Run window:** 2026-05-24
**HEAD at audit:** `48362012c02549f604c08650eb51ee544a2c7e19`
**Posture:** Pass 1 fast walk + skeleton. Pass 2 deep-dives are scoped as follow-ups (see §10).
**Findings filing:** batch-at-end; P0/P1 issues filed at audit completion and cross-referenced from each section.
**Design doc edits:** out-of-scope per bd issue; §1 catalogs divergences and proposes patches the user can decide whether to apply.

## 0. Headline

Eight dimensions audited. Verdicts:

| # | Dimension | Verdict |
|---|-----------|---------|
| 1 | Design-impl coherence       | ✗ requires follow-up — spec doc has drifted significantly from shipped (wac_periodic + h5gs + close_period not in design-v3.md) |
| 2 | R1-R7 class-confusion analog | ◐ Pass 1 framework-fit check only; deep per-rule walk deferred to Pass 2 |
| 3 | Schema coherence            | ◐ Migrations 0001-0005 match spec; 0006-0007 are additive without spec landing (overlap with D1) |
| 4 | Doc + comment hygiene       | ◐ Migration 0006 has heavy historical-comment violations; source code mostly clean |
| 5 | Naming consistency          | ◐ Source code clean post-rename; result docs (Phase 5/6) still leak v21 terms (`envelope`, `superbatch`) |
| 6 | Test coverage gaps          | ◐ One entry-point (`ledger_close_period`) lacks a property test per acct-1cer convention |
| 7 | Perf hot-spots not measured | ◐ 9 GUCs ship at defaults without measurement backing; cataloged below |
| 8 | Open-question status (Q1-Q10) | ◐ Most resolved by shipped work; status table below |

**Headline takeaway:** the shipped code is coherent and well-tested; the **design-v3.md spec doc is out of date** in load-bearing ways. The Phase 6 push (wac_periodic + h5gs + 9mgx series + e5fz/w2dn tuning) all landed correctly but the spec wasn't updated alongside. Bringing the spec doc current is the single highest-leverage cleanup before Phase 7 (re-baseline + production-shape workloads) lands.

---

## 1. Design-impl coherence

**Scope:** walk `poc/design_research/design-v3.md` (701 lines) section-by-section against shipped 4-crate workspace (37 source files, 9894 LOC) + 7 migration pairs.

**Verdict:** ✗ requires follow-up. Significant additive divergence; spec doc has not been updated alongside the Phase 6 push.

### Divergences (with proposed spec patches)

#### D1.1 [P1] `wac_periodic` method missing from spec §2.1 + §3

design-v3 §2.1 lists `pool_method ENUM ('fifo','lifo','wac','std','specific')` — **5 variants**.

Shipped:
- migration 0007 `ALTER TYPE pool_method ADD VALUE 'wac_periodic'` — now **6 variants**
- `ledger-core/src/method.rs::PoolMethod` has `WacPeriodic` variant
- `ledger-core/src/wac_periodic.rs` (260 lines) implements the per-method body
- `posting_lines_provisional` table added in 0007
- `ledger-direct/src/close_period.rs` (573 lines) implements `ledger_close_period` for the periodic close
- Acceptance test `acceptance_direct_wac_periodic_close.rs` covers the close flow

Proposed patch to design-v3.md:
- Insert new §3.6 `wac_periodic` with the Oracle PAC convention semantics (depletions post at running pool average mid-period; close hook recomputes `final_avg = Σ(in-period receipt value) / Σ(in-period qty)` and posts variance per provisional row)
- Update §2.1 enum to include `'wac_periodic'`
- Update §2.2 to add `posting_lines_provisional` table
- Update §12 to NOT exclude period close (it IS implemented for wac_periodic; the broader "all-methods period close" is still out of scope)
- Add §3.6 cross-reference to acct-9mgx.6 (wac_periodic equivalence + close path) and acct-s6fa (close orchestration)

#### D1.2 [P1] `h5gs` WAC cumulative-sum semantic shift missing from spec §3.1

design-v3 §3.1 describes WAC's `pool_state.unit_cost` as the running average unit cost, recomputed at receipt via `(old_qty × old_unit_cost + Q × C) / new_qty`.

Shipped (migration 0006):
- `pool_state.unit_cost` for WAC pools now stores **cumulative `value_sum`** = `Σ(receipt_qty × receipt_unit_cost) − Σ(per-depletion rounded amount)`
- Per-receipt update is `unit_cost += Q × C` (exact, commutative, associative) — no per-receipt rounding error
- Running per-unit cost is computed on demand as `unit_cost / NULLIF(qty, 0)`; never stored
- The column name is now a footgun for ad-hoc SQL (it stores a total for WAC, per-unit for other methods) — documented in 0006 preamble

Proposed patch to design-v3.md:
- Rewrite §3.1 WAC receipt formula to the cumulative-sum form
- Add a "per-method storage contract" note that `pool_state.unit_cost` carries different semantics per method (per-layer for FIFO/LIFO/specific, cumulative `value_sum` for WAC, same form as WAC for WacPeriodic given they share storage)
- Add cross-reference to acct-h5gs and acct-mcey (the equivalence drift that motivated the fix)
- Note the column-naming footgun explicitly in §13 (concerns/open questions)

#### D1.3 [P2] `ledger_close_period` entry point absent from §4 + §12

design-v3 §12 explicitly says: *"Period close mechanics. accounting_period table exists but the close hook (drain-to-zero pattern) is not implemented in the PoC."*

Shipped:
- `ledger-direct/src/close_period.rs::ledger_close_period(period_id BIGINT) RETURNS JSONB`
- Synchronous in caller's user-tx; per-pool variance recompute against `wac_periodic` provisional rows
- 11-step pipeline (period lock → state='closing' → final_avg per pool → variance posting_lines → pool_state cumulative-sum correction → state='closed')

Proposed patch to design-v3.md:
- Add new §4.5 "Period close (ledger_close_period)" — full pipeline; note it's wac_periodic-driven and only fires when provisional rows exist in the period
- Update §12 to clarify: "Period close mechanics for non-wac_periodic methods (drain-to-zero pattern) is not implemented" — narrowing the original scope statement
- Cross-reference acct-s6fa

#### D1.4 [P2] SPI signature uses JSONB `lines`, not SQL ARRAY of struct per spec §4.1 + §5.1

design-v3 §4.1: `lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost, debit_account, credit_account)`
design-v3 §5.1: same.

Shipped (`ledger-direct/src/submit.rs`, `ledger-routed/src/enqueue.rs`):
- `lines: pgrx::JsonB` — array of objects; `line_type` arrives as caller-supplied string text decoded against a private enum-text → `LineType` map

This is a deliberate pgrx-ergonomic simplification (SQL composite-array binding is awkward through pgrx 0.18); the cost is loss of SQL-side enum type-checking on the caller's payload.

Proposed patch to design-v3.md:
- Update §4.1 + §5.1 to document the JSONB shape with example
- Add §4.1.1 note explaining why JSONB was chosen over the original ARRAY-of-record approach
- Move the spec-text examples to use JSONB syntax

#### D1.5 [P3] §6 `ledger-core` file inventory omits four new files

design-v3 §6 lists: `method.rs / fifo.rs / lifo.rs / wac.rs / std.rs / specific.rs / snapshot.rs / plan.rs / error.rs` — **9 files**.

Shipped:
- Above 9 (with `std.rs` renamed `standard.rs` to avoid shadowing `::std`)
- **`wac_periodic.rs`** (260 lines, per-method body for acct-9mgx.6)
- **`layered.rs`** (246 lines, shared code for FIFO/LIFO/specific — three layered methods share their layer-walk logic)
- **`seq.rs`** (18 lines, per-pool monotonic trx_seq counter helper)
- **`standard.rs`** (92 lines; renamed from `std.rs` to avoid Rust import collision)

Proposed patch to design-v3.md:
- Update §6 file list to match shipped
- Add brief one-line description for `layered.rs` (shared logic) + `wac_periodic.rs` (per acct-9mgx.6) + `seq.rs` (helper)

#### D1.6 [P3] Order-sensitive method classifier (acct-aywu) is shipped but unspec'd

design-v3 §5.3 step 4 says: *"For components exceeding `batch_size_max` (default 50 trxs): split into chunks."*

Shipped (acct-aywu):
- Router classifies methods as order-sensitive ({fifo, lifo, specific}) vs split-safe ({wac, wac_periodic, std})
- Order-sensitive components do NOT chunk-split — they emit whole, bypassing `batch_size_max`
- Reason: per-pool ordering invariant on layered methods requires all overlapping submissions to land in one commit_group in seq order

Proposed patch to design-v3.md:
- Update §5.3 step 4 to "For components exceeding `batch_size_max` that are method-split-safe (wac, wac_periodic, std): split into chunks. Order-sensitive components (fifo, lifo, specific) bypass the cap and emit whole."
- Add §5.3.1 "Order-sensitive grouping" explaining why
- Cross-reference acct-aywu

#### D1.7 [P3] Per-pool sequence numbers (acct-tm09) shipped but unspec'd

design-v3 §13.1 says `trx_seq` is per-pool monotonic, allocated at hydration via MAX-scan.

Shipped (acct-tm09):
- The committer's predecessor-wait coordinates concurrent commit_groups touching the same pool so their per-pool trx_seq values land in submission order
- Spin-sleep coordination primitive; CV+broadcast replacement deferred (acct-xjhq)

Proposed patch to design-v3.md:
- Add §5.4.X "Predecessor-wait coordination" describing the per-pool seq ordering invariant across commit_groups
- Cross-reference acct-tm09 + acct-xjhq

#### D1.8 [P3] §13.1 DESC index decision is unresolved (acct-s7da still open)

design-v3 §13.1 mitigation 1 mentions adding `CREATE UNIQUE INDEX trx_line_pool_seq_desc ON trx_line (pool_id, trx_seq DESC)` if MAX scans dominate.

Status: acct-s7da open. No measurement yet establishes the need.

Proposed patch: leave §13.1 as-is. Status quo is correct.

---

## 2. R1-R7 class-confusion analog

**Scope:** apply CLAUDE.md's R1-R7 framework (motivated by the parent acct project's `_post_posting_lines_apply_event`) to ledger-v3's two execution paths.

**Verdict:** ◐ Pass 1 framework-fit check only. Deep per-rule walk deferred to a follow-up (filed below).

### Framework-fit observations

The R1-R7 rules are framed around acct's specific multi-method dispatch on a single shared apply-event helper. Ledger-v3's analog is two distinct functions:
- **ledger-direct**: `submit.rs::ledger_submit_trx` — 8-step pipeline, N=1 line-set per call
- **ledger-routed**: `committer.rs::process_commit_group` (~1080 LOC file) — claim + hydrate + pristine-replay loop calling `plan_apply` per submission

Many R1-R7 rules either don't apply directly or apply in adapted form:

| R | Acct framing | Ledger-v3 analog |
|---|--------------|------------------|
| R1 | Per-class qty divisors from per-class signed SUM, NEVER from cross-class state | N/A — v3 has no per-class pool partition; one pool, one method |
| R2 | Cost-method dispatch resolves SKU from CREDIT side | Analog: `plan_apply` resolves method from `snapshot.method_of[&line.pool_id]` per-line, not per-submission. No CREDIT-side asymmetry to worry about (line carries its own pool_id) |
| R3 | Solo-occupancy gates on shared-pool mutations | **APPLIES**. Both paths acquire `pool_lock FOR UPDATE` before any pool_state read/write. Pass 2 should verify Snapshot read-after-lock for ALL pools touched (including any lazy-create paths) |
| R4 | Pool reads under FOR UPDATE on the SAME account | **APPLIES strongly**. Pass 2 should walk hydration.rs (both ledger-direct + ledger-routed) and confirm the `SELECT FROM pool_state` happens AFTER the pool_lock FOR UPDATE completes for ALL pools in `touched`, not just the first |
| R5 | Single-leg variance on debit-normal pools | **APPLIES to close_period**. Variance in `ledger_close_period` flows through ORIGINAL depletion's debit/credit (with direction swap on negative) — a deliberate PoC simplification. Pass 2 should verify the cumulative-sum bookkeeping correction (-Σ variance) doesn't double-count or skew an active pool. Acceptance test exists; property test does not (see §6) |
| R6 | Idempotency replay checks before AND after FOR UPDATE | **APPLIES to routed path**. Pristine-snapshot replay in `process_commit_group` runs AFTER lock acquisition; the lock prevents another committer from mutating pool_state mid-replay. But duplicate-submission detection on `trx UNIQUE(trx_type, source_id)` fires at INSERT time — pristine-replay must exclude the duplicate and retry. Pass 2 should verify the exclusion path handles partial duplicates correctly |
| R7 | Document-audit fields come from post-lock dispatcher output | **APPLIES**. `trx_line.unit_cost` snapshotting under WAC depletions (post-h5gs) is `amount / qty_depleted` where `amount` is the rounded post-lock value. Pass 2 should verify both paths persist this consistently |

### Filing posture

R3, R4, R5, R6, R7 each warrant a Pass 2 walk against the actual code. None obviously broken in the spot-check, but the spot-check was shallow.

**Filed as Pass 2 follow-up:** acct-<TBD> "R1-R7 deep walk on ledger-direct + ledger-routed" — body should enumerate per-rule, per-path verdicts.

---

## 3. Schema coherence

**Scope:** 7 migration pairs (0001-0007) cross-checked against design-v3 §2.

**Verdict:** ◐ — load-bearing constraints all match spec; the additive migrations (0006-0007) overlap with §1 D1.1/D1.2 divergences.

### Load-bearing constraints verified present

| Constraint | Spec ref | Shipped |
|-----------|----------|---------|
| `pool_state PRIMARY KEY (pool_id, layer_seq)` — index-ordered traversal | §2.2 / §4.2 step 4 | ✓ migration 0003 |
| `trx UNIQUE (trx_type, source_id)` — duplicate-submission detection | §2.2 / §4.2 step 7 / §5.4 step 9 | ✓ migration 0003 |
| `trx_line UNIQUE (pool_id, trx_seq)` — per-pool monotonic seq | §2.2 / §13.1 | ✓ migration 0003 |
| `pool UNIQUE (sku_id, location_id, identity_key)` — pool dedup | §2.2 | ✓ migration 0003 |
| `posting_line_dimension PRIMARY KEY (posting_line_id, dimension_type)` | §2.3 | ✓ migration 0004 |

### Migration-by-migration

| Mig | Shape | Notes |
|-----|-------|-------|
| 0001 enums | DDL: 6 CREATE TYPE | Matches spec §2.1 exactly |
| 0002 reference tables | DDL: account, accounting_period, sku, location | Matches spec §2.3 + §2.4 |
| 0003 ledger tables | DDL: pool, pool_state, pool_lock, trx, trx_line + their UNIQUEs | Matches spec §2.2 |
| 0004 posting tables | DDL: posting_line, posting_line_dimension | Matches spec §2.3 |
| 0005 indexes | 6 explicit CREATE INDEX | Matches spec §2.2 + §2.3 |
| **0006 wac_value_sum** | **COMMENT ON COLUMN only, no DDL** | acct-h5gs cumulative-sum semantic landing. No physical schema change; serves as audit trail point. See §1 D1.2 + §4 D4.1 |
| **0007 wac_periodic** | ALTER TYPE pool_method ADD VALUE + CREATE TABLE posting_lines_provisional | acct-s6fa. Not in spec doc; see §1 D1.1 |

### Findings

- **[P3] D3.1** Migration 0006 is comment-only. This is an unusual pattern — typically a migration carries DDL. The preamble explains the rationale (catalog visibility + audit trail). Acceptable for the PoC; if Phase 7 productionization wants the WAC value_sum semantic explicit in the schema, the natural fix is splitting `pool_state.unit_cost` into separate `value_sum` and `per_unit_cost` columns rather than overloading via per-method comment. Tracked as proposed productionization task in acct-adte catalog (already filed).

- See §1 D1.1 and D1.2 for the substantive 0006/0007 spec divergences.

---

## 4. Doc + comment hygiene

**Scope:** per saved memory rules (no historical comments, no time estimates, greenfield-only); audited all `.rs` + `.sql` + `.md` in `poc/ledger-v3/`.

**Verdict:** ◐ — source code largely clean; migration 0006 has heavy violations; result docs have minor leaks.

### Findings

#### D4.1 [P2] Migration 0006 contains heavy historical-comparison comments

`db/migrations/0006_pool_state_wac_value_sum.up.sql` lines 16-23, 38-53, 66-83 contain comparison-to-prior-state language: *"Prior to acct-h5gs (under the running-average model)"*, *"Path B multi-committer reordering, the accumulated drift between Path A (serial) and Path B (parallel commit_groups) became visible"*, *"acct-h5gs replaces the per-row running-average storage with a CUMULATIVE SUM"*.

This violates two saved memory rules:
- `feedback_greenfield_no_historical_comments.md`: strip "pre-fix:" / "the original rule was" / hedge phrasing comparing to prior state; comments describe current behavior only, git log is the history
- `migration-files-are-immutable-content-addressed`: post-shipped or current-state documentation belongs in db/README.md or spec docs, NEVER in shipped migration files

**Compounding factor:** migration files are immutable after first apply (sqlx checksum-verifies). Fixing this requires either:
- (a) Accepting the violation as a tradeoff for forensic value in the migration history
- (b) A new migration that supersedes the comments via `COMMENT ON COLUMN ... IS '...'` rewrites — paying a migration slot to walk back doc-hygiene drift

Recommend (a) for the PoC + add the cleanup expectation to Phase 7 productionization scope (acct-adte). Filing this finding as P2 for visibility, not for active cleanup.

#### D4.2 [P3] bd-id attribution comments throughout source

Files contain bd-id attribution patterns like `"Wired by acct-nmlc"`, `"acct-ddnu — FIFO body"`, `"ledger_routed 0.0.1 (acct-29a1 scaffolding)"`. These are useful for cross-referencing to the bd issue history but rot as the project evolves (bd ids can be retired/superseded).

CLAUDE.md doesn't prohibit. They're attribution markers analogous to JIRA-id comments — debatable whether they belong in source vs commit messages. Status: borderline; not actively cleaned. P3 for visibility.

#### D4.3 [P3] Result docs (Phase 3/5/6) contain "Pre-fix" / "the original" historical language

`results/phase6/fifo-validation.md` line 142 has section heading `"## Broken-config reference (pre-fix audit)"` documenting what was wrong before a fix landed. `results/phase3/phase3-summary-v3.md` line 179 has `"v2 (pre-fix)"` in result-set labels.

These are forensic-record documents documenting bench history. The "no historical comments" rule was framed for source code, not retrospective measurement docs — these arguably need historical context to be readable. P3 for visibility; not recommending cleanup.

#### D4.4 [P3] Spec doc `design-v3.md` has zero historical-comparison language

design-v3.md itself is clean greenfield prose; no "previously" / "originally" leaks. ✓

### Time estimates

Grep for `~?[0-9]+\s?(hour|day|week|minute)s?`:
- `ledger-harness/src/driver_direct.rs:99` — `"Wraparound is ~11.5 days"` (physical property of a u32 ns counter; not an estimate)
- `results/phase3/phase3-summary-v3.md:94` — `"across 372k successful trx in 6 minutes of wall time"` (measurement, not estimate)

No actual time-estimate-as-estimate violations in source. ✓

---

## 5. Naming consistency

**Scope:** post-v3-rename trio (commit_group, submission) leak check. Source code + docs.

**Verdict:** ◐ — source clean; result docs leak.

### Findings

#### D5.1 [P3] v21 terminology in result docs

`grep -rn -E "superbatch|envelope"` against `poc/ledger-v3/`:
- `results/phase6/CHARACTERIZATION.md:80,89` — uses "envelope" in pipeline math
- `results/phase5/phase5-summary.md:25,59,60,110` — uses "envelope" + "envelopes" in throughput math
- `results/phase6/fifo-validation.md:261` — references the rename itself (justified; documenting the rename)

These docs were authored before the v3 rename trio (9ef8572 — `commit_group`/`submission`) and not updated alongside. Proposed cleanup: bulk find-and-replace `envelope → submission` and `superbatch → commit_group` in `results/phase5/` and `results/phase6/`, then commit as a doc-hygiene pass.

#### D5.2 ✓ Source code is clean post-rename

`grep` against `src/**/*.rs` + `db/migrations/*.sql` → zero hits for `superbatch` / `envelope`. The 9ef8572 cleanup commit landed thoroughly across source.

---

## 6. Test coverage gaps

**Scope:** per-entry-point property tests (CLAUDE.md acct-1cer convention); equivalence harness coverage matrix.

**Verdict:** ◐ — one entry-point lacks a property test; otherwise per-entry coverage is good.

### Entry-point property-test coverage

| Entry point | Acceptance tests | Property tests | Verdict |
|-------------|------------------|----------------|---------|
| `ledger_submit_trx` (ledger-direct) | 5 binaries (single, insufficient_inventory, duplicate, concurrent_overlap, concurrent_disjoint) | `property_direct_pipeline`, `property_direct_wac_guard` | ✓ |
| `ledger_close_period` (ledger-direct) | 1 binary (`acceptance_direct_wac_periodic_close`) | **none** | ✗ D6.1 |
| `ledger_enqueue_trx` (ledger-routed) | 6 binaries (enqueue, affinity, eject, orphan_recovery, postmaster_restart, duplicate) | `property_routed_pipeline`, `property_routed_replay` | ✓ |

#### D6.1 [P2] `ledger_close_period` has no property test

Per CLAUDE.md acct-1cer convention: *"Every entry-point function (post_* document wrapper) MUST ship with a sibling tests/property_<fn>.rs ... in the same change as the function."*

`ledger_close_period` shipped via acct-s6fa with only `acceptance_direct_wac_periodic_close.rs` (specific scenario). No `property_close_period.rs` random-workload probe exists.

Proposed scope: random workloads of wac_periodic pools with varying mid-period depletion mixes; close the period; assert (a) all unfinalized provisional rows are finalized, (b) variance sums per pool reconcile to cumulative-sum bookkeeping, (c) the `pool_state.unit_cost` (value_sum) post-close equals pre-close minus Σ variance, (d) period state transitions are atomic (no partial close on plan_apply failure).

### Equivalence harness coverage matrix

The Phase 6 equivalence work (acct-9mgx series) covers:

| Method | Equivalence harness | Strict mode | Coverage doc |
|--------|---------------------|-------------|--------------|
| wac (perpetual) | ✓ via `build_submissions_wac` (s1-s6) | ✓ post-h5gs | `equivalence-summary.md`, `h5gs-cumulative-sum-validation.md`, `wac-perpetual-validation.md` |
| wac_periodic | ✓ via `build_submissions_wac_periodic` | ✓ | `wac-periodic-validation.md` |
| fifo | ✓ via `build_submissions_fifo` | ✓ | `fifo-validation.md` |
| lifo | ✓ via `build_submissions_lifo` | ✓ | `lifo-validation.md` |
| specific | ✓ via `build_submissions_specific` | ✓ | `specific-validation.md` |
| std | ✓ via `build_submissions_std` | ✓ | `std-validation.md` |
| Mixed (cross-method in one submission) | **Not exercised** | — | gap D6.2 |

#### D6.2 [P2] Cross-method-mixed equivalence harness coverage is missing

The harness drives each method as `MethodMix::AllWac` / `AllFifo` / etc. There is a `MethodMix::Mixed` variant but no equivalence test that drives it. Phase 6 cross-method-comparison.md notes this as a Phase 7 gap.

Proposed scope: equivalence-harness extension to drive `Mixed` workloads (e.g. 60% wac, 30% fifo, 10% specific in one submission run) and diff Path A vs Path B byte-identical. Important because production deployments will be mixed-method.

---

## 7. Performance hot-spots not yet measured

**Scope:** catalog GUCs / tunables that ship at defaults without measurement justification. Do NOT bench them under this issue.

**Verdict:** ◐ — 9 unmeasured tunables identified.

### GUC inventory + measurement status

| GUC | Default | Context | Measured by | Status |
|-----|---------|---------|-------------|--------|
| `ledger_routed.batch_size_max` | 50 | Sighup | acct-e5fz Part A + acct-w2dn resweep | ✓ |
| `ledger_routed.batch_window_us` | 500 | Sighup | acct-w2dn (single-point at default + reframe disproven) | ◐ partial |
| `ledger_routed.router_window_size` | 1000 | Sighup | — | ✗ unmeasured |
| `ledger_routed.committer_count` | 4 | Postmaster | — | ✗ unmeasured |
| `ledger_routed.committer_lease_ms` | (default) | Sighup | — | ✗ unmeasured |
| `ledger_routed.max_eject_count` | (default) | Sighup | — | ✗ unmeasured |
| `ledger_routed.caller_tx_timeout_ms` | (default) | Sighup | — | ✗ unmeasured |
| `ledger_routed.eject_cooldown_ms` | 10 | Sighup | — | ✗ unmeasured |
| `ledger_routed.queue_full_timeout_ms` | (default) | Sighup | — | ✗ unmeasured |
| `ledger_routed.snapshot_layer_limit_per_pool` | (default) | Sighup | — | ✗ unmeasured |

#### D7.1 [P3] 9 GUC tunables ship at defaults without measurement backing

This is expected for a PoC — many of these defaults are inherited from queue-extension-v21's empirical defaults. The risk is that production workloads outside v21's measured range hit a different sweet spot.

Proposed Phase 7 framing: build a parallel GUC-sweep matrix (one per tunable) gated on production-shape workload characterization. acct-69c7 (1000-caller × 5-min routed run, needs pgbouncer per saved memory) is the natural anchor scenario.

Filing as a Phase-7-prep tracking issue rather than per-GUC: acct-<TBD> "Phase 7 GUC sweep matrix".

---

## 8. Open-question status (Q1-Q10 from original PoC plan §H)

| Q | Question | Default in plan | Status |
|---|----------|-----------------|--------|
| Q1 | Chronological ordering in routed commit_group — ledger-core or ledger-routed? | ledger-routed sorts | **Resolved**: ledger-routed sorts by `(posted_at, enqueued_at_micros)` before replay; per-pool seq ordering enforced via acct-tm09 predecessor-wait |
| Q3 | pool_lock lazy ON CONFLICT vs trigger-seeded? | Lazy | **Resolved**: lazy ON CONFLICT shipped (submit.rs::pool_lock::acquire_pool_locks). Trigger-seeded follow-up not filed (no measured need) |
| Q4 | STD: caller-supplied unit_cost vs internal standard_costs lookup? | Caller-supplied | **Resolved**: caller-supplied; matches design-v3 §3.4 + ledger-core/src/standard.rs |
| Q7 | Migration runner: sqlx-cli vs raw psql? | sqlx-cli | **Resolved**: sqlx-cli + `sqlx::migrate!` macro shipped |
| Q8 | Method assignment seeding | Harness seeder | **Resolved**: harness `seed-pools` subcommand handles seeding |
| Q9 | ledger-direct synthetic enqueue for equivalence harness? | Skip — call native SPI | **Resolved**: equivalence harness calls each path's native SPI; cross-method-comparison.md is the synthesis |
| Q10 | Recovery handles valid==3 left by committer-died-after-COMMIT-before-cleanup? | Verify v21's CAS covers it | **Resolved-in-test**: covered by `acceptance_routed_orphan_recovery.rs` + `acceptance_routed_postmaster_restart.rs` |

Q2, Q5, Q6 from the original plan are not in §H — they may have been folded into other deferred items. Cross-reference with acct-adte catalog if Pass 2 audits open questions in depth.

**Verdict:** All explicit open questions either resolved by shipped work or covered by acceptance tests. No outstanding gating questions for Phase 7.

---

## 9. Phase 7 framing recommendation

The Phase 6 `cross-method-comparison.md` recommends Phase 7 framing on three axes:
- Re-baseline against the now-tuned router (post acct-e5fz + acct-w2dn)
- Production-shape workloads (vs synthetic scenario builders)
- Scale-out via pgbouncer (acct-69c7, 1000-caller × 5-min routed run)

This audit's findings do NOT change that framing. The high-leverage Phase 7-blocker is:

**Bring design-v3.md current** before Phase 7 measurement-cycle begins. Reasons:
1. The spec is the doc that anchors Phase 7 hypotheses ("does the routed path beat direct under workload X?"). A stale spec means hypotheses get framed against the wrong reference.
2. The wac_periodic + h5gs landings change the per-method cost model that the bench math assumes (cumulative-sum WAC has different rounding behavior than running-average WAC; equivalence numbers depend on it).
3. Phase 7 GUC-sweep work (D7.1) needs a current spec to know which GUCs are load-bearing vs nice-to-have.

If §1 D1.1 + D1.2 + D1.3 patches land, design-v3.md goes from "describes Phase 0 spec, doesn't match Phase 6 reality" → "describes Phase 6 shipped". Recommend doing this BEFORE claiming Phase 7 scoping work.

---

## 10. Pass 2 deep-dive scope (filed as follow-up)

Pass 1 spot-checked the architecture and surfaced load-bearing divergences. The following warrant a Pass 2 walk:

- **R3-R7 deep walk** on ledger-direct's submit pipeline + ledger-routed's `committer::process_commit_group` (1080 LOC). Per-rule, per-path verdicts. (D2)
- **bulk_write parity** between ledger-direct (364 lines) + ledger-routed (371 lines) — are the FK-ordered write sequences byte-equivalent on identical inputs?
- **hydration parity** between ledger-direct (127 lines) + ledger-routed (107 lines) — does the snapshot construction order and defensive re-sort match exactly?
- **Recovery flow walk** — `cleanup.rs` (396 lines) three-case CAS handling + `recovery.rs` (47 lines) router-orphan sweep — against design-v3 §5.5 invariants
- **router.rs (1349 lines) + committer.rs (1080 lines) full read** — pipeline correctness beyond the head-of-file doc

Filed as: acct-<TBD> "Pass 2 audit: deep walk on ledger-routed correctness paths".

---

## Findings index

| ID | Severity | Section | Title | Filed as |
|----|----------|---------|-------|----------|
| D1.1 | P1 | §1 | wac_periodic method missing from spec | acct-x22s |
| D1.2 | P1 | §1 | h5gs WAC cumulative-sum semantic missing from spec | acct-szon |
| D1.3 | P2 | §1 | ledger_close_period missing from spec | acct-x22s (folded) |
| D1.4 | P2 | §1 | SPI JSONB-vs-ARRAY signature divergence | acct-cssx |
| D1.5 | P3 | §1 | §6 file inventory drift | acct-x22s (folded) |
| D1.6 | P3 | §1 | acct-aywu order-sensitive routing unspec'd | acct-cssx (folded) |
| D1.7 | P3 | §1 | acct-tm09 predecessor-wait unspec'd | acct-cssx (folded) |
| D1.8 | P3 | §1 | §13.1 DESC index status | acct-s7da (existing) |
| D2.1 | P1 | §2 + §10 | R1-R7 deep walk + Pass 2 full deep-dive scope | acct-o00c |
| D3.1 | P3 | §3 | 0006 comment-only migration pattern | (no filing; Phase 7 productionization) |
| D4.1 | P2 | §4 | Migration 0006 historical comments | (no active cleanup; tradeoff documented in §4) |
| D4.2 | P3 | §4 | bd-id attribution comments in source | (no filing; visibility only) |
| D4.3 | P3 | §4 | Result docs "pre-fix" / "the original" | (no filing; forensic records) |
| D5.1 | P3 | §5 | v21 terms in result docs | acct-otup |
| D6.1 | P2 | §6 | ledger_close_period lacks property test | acct-hxxu |
| D6.2 | P2 | §6 | Cross-method-mixed equivalence harness gap | acct-uwsp |
| D7.1 | P3 | §7 | 9 GUC tunables unmeasured | acct-snoa (Phase 7 prep) |
