# AUDIT — PoC v3.1 (Path C) design-impl coherence + quality

Pass 1: design-vs-impl coherence + a quality survey across the four crates.
Pass 2 (deep correctness walk on the routed/direct hot paths + R1–R7 + the unsafe
shmem core) lives in `AUDIT-PASS2.md`.

> **Status (acct-gd3g reconciliation).** This is the assess-only, point-in-time review (4 crates
> at the time; a 5th, `ledger-spi-common`, was extracted by the D5.1 fix). Every fix-needed finding
> has since been resolved under epic `acct-yojk` (15/15 closed) — see the per-finding **Resolution**
> column in the findings index. The prose below is preserved as the as-assessed state; it is not
> corrected in place.

Scope: `poc/ledger-v3.1/` — `ledger-core` (~1.3k LOC), `ledger-direct-c` (~0.8k),
`ledger-routed-c` (~5.5k), `ledger-harness` (~3.0k). Spec: `poc/design_research/design-v3.1.md`
(worktree copy, authoritative). Severity: **P1** correctness/blocking · **P2** real but
non-blocking (maintenance hazard / dead code / missing guard) · **P3** consistency / hygiene.
Verdicts: **clean / divergence / dead / fix-needed**.

## 0. Headline

**The implementation faithfully realizes design-v3.1 Path C, and the cost-correctness core
(`ledger-core`) is high quality.** Overflow safety (i128-before-multiply + `checked_*`
throughout), the no-negative-inventory invariant, banker's rounding, the WAC/STD/specific
strict modules, the provisional FIFO/LIFO aggregate-only path, and `coalesce_aggregates` are
all correct and well-tested. The direct flavor (`ledger_submit_trx_c`) and the routed flavor
(`ledger_enqueue_trx_c` + router + committer + recovery) implement the §5/§6 contracts with
correct lock ordering, memory ordering on the shmem atomics, drop-and-continue (§14.2), the
§6.7 batching collapse, and a defensively-bounded arena allocator.

**The dominant blemish is carried-over scaffolding from the copy-adapt of `ledger-v3` (Path
B / strict).** `ledger-routed-c` retains 13 shmem counter fields that are declared but never
read or written under Path C (the `tm09` predecessor-wait counters, per-stage timing,
audit counters, the order-sensitive-groups counter), a dead `committer_lease_ms` GUC, and
~24 stale "Path B / design-v3 / PoolSeqTable / tm09" references in module headers and
comments. None of this is a correctness defect — the dead fields are harmless zeros — but it
is misleading provenance and dead weight in a per-cluster shmem struct, and it is the bulk of
the "where did it diverge / consistency" answer. **No P1 findings.** Findings are 1×P2-cluster
(dead routed scaffolding), a handful of P2 (paired-file hazard, routed property-test gap), and
the rest P3 (hygiene, doc, minor missing guards).

Findings index at the end. Counts: P1 = 0, P2 = 4, P3 = 11.

## 1. Design-impl coherence

Walked spec §2/§3/§4/§5/§6/§8/§13/§14 against the code. Coherence is high; the spec is
implementable as written with the divergences below. Items the impl correctly realizes
without divergence (not findings, recorded for completeness): fifo/lifo strict stubs
returning `MethodMismatch` (§3.2/§8); drop-and-continue with no pristine-replay (§14.2 — the
spec explicitly says pristine-replay is *not* needed); both provisional bases running_avg +
standard (§14.3); §6.7 collapse (one aggregate UPSERT per pool from the post-pass snapshot);
trivial postmaster recovery + router-self-owned orphan sweep (§6.5); split-chunk enqueue-order
preservation (§14.2); the no-negative-inventory rejection (§3.6/§13).

### Divergences

#### D1.1 [P3] SPI takes JSONB `lines`, not the §4 ARRAY-of-composite — `divergence`
`submit.rs:66` / `enqueue.rs:51` take `lines: pgrx::JsonB`; spec §4 (`design-v3.1.md:440-453`)
declares `lines: ARRAY of (line_type, source_id, pool_id, qty, unit_cost, debit_account,
credit_account)`. Reason: passing an array of an anonymous composite through pgrx is awkward
and brittle; JSONB is the pragmatic, stable wire shape (the same call the harness/equivalence
drivers build via `build_lines_json`). Behaviorally equivalent. The sibling flagged the
identical choice as its D1.4. **Proposed spec patch:** amend §4 to specify `lines: JSONB`
(array of objects) with the field list, noting the composite-array form was the original
sketch.

#### D1.2 [P3] `variance_account` added to the line tuple — `divergence` (gap-fill)
The line shape carries `variance_account: Option<i64>` (`plan.rs:87`, `submit.rs:39`,
`enqueue.rs` line JSON), absent from the §4 tuple. §3.3 requires a variance account for STD
receipts whose actual ≠ standard, and the code already documents the gap (`plan.rs:83-87`,
`error.rs:38-40` → `MissingVarianceAccount`). This is a correct gap-fill, not drift.
**Proposed spec patch:** add `variance_account` (nullable) to the §4 line tuple and cross-ref
§3.3.

#### D1.3 [P3] `coalesce_aggregates` is unspec'd — `divergence` (build-time, acct-036x)
`PlanResult::coalesce_aggregates` (`plan.rs:169`) collapses per-pool `UpsertAggregate`
mutations to one (keep-last) so a single direct submission touching a pool twice doesn't emit
a duplicate `(pool_id, layer_id=0)` row that the `ON CONFLICT DO UPDATE` batch would reject
("cannot affect row a second time"). Added by `acct-036x` (commit `d4c2c5c`) after the spec
was written; §5.1 step 8 / §8 don't mention it. Correct and well-commented (each
`UpsertAggregate` carries full post-line state, not a delta, so keep-last is sound). The
routed committer reaches the same one-aggregate-per-pool shape differently (the §6.7
reconstruction from the post-pass snapshot in `committer::plan_and_write`). **Proposed spec
patch:** note in §5.1 step 8 / §8 that ledger-core coalesces aggregate mutations per pool.

#### D1.4 [P3] Harness enforces distinct pools per submission — `divergence` (harness limitation)
`workload.rs` / `pool_universe.rs` generate submissions with DISTINCT pool_ids per
submission. §10.3 frames multi-pool submissions generically. The distinctness is an
implementation constraint that follows directly from D1.3's root cause: the direct
`bulk_write` `ON CONFLICT (pool_id, layer_id) DO UPDATE` cannot touch the same aggregate row
twice in one statement, so the *generator* avoids producing same-pool-twice submissions rather
than relying solely on the coalesce. (Coalesce handles it for correctness; the harness avoids
it for clean measurement.) Documented in `workload.rs:79-84`. **Proposed action:** note the
limitation in README + §10.3; no code change.

#### D1.5 [P3] `standard_cost` seeding lives in the harness — `divergence` (build-time, acct-0z5m)
`pool_universe::seed` inserts `standard_cost` rows for std-method pools (`acct-0z5m`, commit
`9b6e6b5`). §10.4 (method mix) doesn't call this out; without it, std/standard-basis pools
abort with `MissingStandardCost` and confound mixed-method scenarios (S3/S4). Correct fix.
**Proposed action:** note in §10.4 that the seeder must populate `standard_cost` for any
std-method or standard-basis pool.

## 2. R1–R7 class-confusion framework fit

The R1–R7 checklist (CLAUDE.md L99–108) was written for the production multi-class ERP ledger
(raw / fg / wip pools sharing a (sku, location)). Path C's model is structurally narrower —
**one `pool` = one (sku_id, location_id, identity_key); a pool is a single inventory class** —
so several rules are vacuous here. The full per-entry-point walk (with the "N/A because…"
justifications, which are themselves the audit record) is in `AUDIT-PASS2.md` §1. Framework-fit
summary:

| Rule | Applies to Path C? | Verdict (detail in Pass 2 §1) |
|------|--------------------|-------------------------------|
| **R1** per-class qty divisor from per-class signed SUM, not a cross-class pool | **Vacuous** — one class per pool; the qty divisor is the pool's own aggregate row | clean (N/A) |
| **R2** cost dispatch resolves SKU from the CREDIT (depletion) side | **Partial** — dispatch is on qty SIGN per line, not cross-class SKU; no debit/credit-side ambiguity | clean |
| **R3** solo-occupancy gate on shared-pool mutation | **Vacuous** — no shared pool across documents/classes; the §6.7 collapse is per-pool by construction | clean (N/A) |
| **R4** reads under FOR UPDATE on the SAME row before writes | **Applies** — satisfied via the `pool_lock` mutex-table model (both flavors lock `pool_lock` rows ascending before reading/writing `pool_state`) | clean |
| **R5** single-leg variance on debit-normal pools drained in-period | **Vacuous** — no close-hook variance; STD variance is a receipt-time 2-leg posting, no drained-pool case | clean (N/A) |
| **R6** idempotency check before AND after the lock | **Applies** — routed: pre-flight `dedup_against_trx` + trx UNIQUE backstop; direct: UNIQUE backstop (synchronous, acceptable) | clean |
| **R7** audit fields from post-lock dispatcher output | **Applies** — `trx_line.unit_cost` is persisted straight from `PlanResult.trx_lines` (dispatcher output under the lock), never re-derived pre-lock | clean |

### Filing posture
No R1–R7 violation found. The "applies" rules (R4/R6/R7) are satisfied; the "vacuous" rules
are recorded as N/A-by-construction so a future reviewer (or a recalc/close phase that adds
multi-class pools) knows they were considered, not skipped.

## 3. Schema coherence

`db/migrations/0001`–`0005` match spec §2.1–§2.4 closely.

### Verified present
- `pool` with `provisional_basis pool_provisional_basis NOT NULL DEFAULT 'running_avg'`, the
  `UNIQUE (sku_id, location_id, identity_key)`, and the `CHECK (method != 'specific' OR
  identity_key != 0)` (`0003:3-13`) — matches §2.2.
- `pool_state` is the simple v3.1 shape `(pool_id, layer_id, qty, unit_cost, created_at)`,
  PK `(pool_id, layer_id)`, **no** `value_sum` / `last_trx_line_id` (`0003:27-34`) — matches
  §2.2 and the `ledger-core` `PoolStateRow`.
- `standard_cost (sku_id, location_id, unit_cost, updated_at)` PK `(sku, location)` (`0003:15-21`).
- `trx` with `UNIQUE (trx_type, source_id)` idempotency key (`0003:44-51`); `trx_line` with
  nullable self-ref `source_trx_line_id` (`0003:57-67`).
- `pool_method`/`pool_provisional_basis`/`trx_type`/`line_type`/`posting_event_type`/`account_type`/
  `dimension_type` enums (`0001`) match the `ledger-core` `LineType`/`PostingEventType` mirrors and
  the harness `MethodMix`. `cost_adjustment` correctly absent (recalc/close, §13; noted in `0001:1-3`).
- §2.2/§2.3 indexes (`0005`).

### Findings

#### D3.1 [P3] No `qty >= 0` CHECK on `pool_state` — `divergence` (code-invariant only)
No-negative-inventory is enforced in `ledger-core` (`wac.rs:133`, `standard.rs:143`,
`specific.rs:108`) via `InsufficientInventory`, not by the schema. Per §3.6/§13 negative
inventory is out of scope and the invariant is a code rule. Acceptable for the PoC; a `CHECK
(qty >= 0)` on the aggregate would be a cheap defense-in-depth backstop (and is consistent
with the "defensive guards are load-bearing in ERP" posture). **Proposed action:** optional
hardening; document the choice in §2.2.

#### D3.2 [P3] Specific K=1 not schema-enforced — `divergence` (caller contract)
`specific.rs` documents (`:10-11`) that the K=1 single-receipt invariant is the caller's
responsibility; a second receipt to a specific pool would materialize a second layer, which
the strict deplete (lowest-layer-first) tolerates but the spec's K=1 framing doesn't intend.
No guard. PoC-acceptable (specific is exercised narrowly). Note in §3.4.

## 4. Doc + comment hygiene

#### D4.1 [P2] Stale "Path B / design-v3 / PoolSeqTable / tm09" provenance in `ledger-routed-c` — `dead`/hygiene
The routed crate was copy-adapted from `ledger-v3/ledger-routed` (Path B, strict) and retains
~24 references to the wrong design/path across module headers and comments:
- `shmem.rs:1` "Shmem layout for ledger-routed (**Path B**)"; `:5` "PoolSeqTable carry the
  load-bearing atomics" (no PoolSeqTable exists in v3.1); `:6` "Single pool_id namespace per
  design-**v3** §1"; comments citing "design-v3 §5.3/§5.4" (`:71`, `:257`) and a
  "pristine-replay loop" (`:262`) that Path C removed. 16 hits in this file alone.
- `recovery.rs:1` "Recovery BGWorker for ledger-routed (**Path B**)"; `:8` "Why this is
  trivial for **v3**"; cites "design-v3 §5.5 / §10.5" (should be §6.5).
- `enqueue.rs:1` "**Path B** SPI entry point: `ledger_enqueue_trx_c` (design-v3 §5.1, §5.4
  step 0)"; example `posted_at` `'2026-05-21'` (a v3-era date).
- Scattered v3 bd-ids in comments (`acct-aywu`, `acct-p0d8`, `acct-17p5`, `acct-29a1`, `acct-zedi`,
  `acct-usn2`) instead of the v3.1 epic (`acct-2ttr.*`).

`router.rs`, `committer.rs`, `lib.rs`, `cleanup.rs`, `payload.rs` mostly cite §6.x / design-v3.1
correctly — the rot is concentrated in `shmem.rs`, `recovery.rs`, `enqueue.rs`. Misleading
provenance; a reader chasing "§5.4 step 0" finds the wrong spec. **Proposed action:** rewrite
the three headers + the scattered refs to design-v3.1 / Path C / §6.x and the `acct-2ttr.*`
lineage. (Paired with D5.1 — same root cause, same fix PR.)

#### D4.2 [P3] Greenfield-comment rule — mostly clean
Spot-checks for "Pre-fix:" / "originally" / "the old…" historical-comparison language found
none in the v3.1 source (the greenfield-no-historical-comments rule holds). `committer.rs:899`
and `router.rs:912` carry "copy-paste; resist premature abstraction" notes (intentional, see
D5.1). No time estimates in source. Clean.

## 5. Naming / structural consistency

#### D5.1 [P2] Triplicated `pool_lock` / `hydration` / `bulk_write` (direct-c ⇄ routed-c) — `divergence` (maintenance hazard)
`ledger-direct-c/src/{pool_lock,hydration,bulk_write}.rs` and the `ledger-routed-c/src/`
copies are **byte-identical except their doc comments** (verified by diff). `bulk_write.rs`
even carries `#![allow(dead_code)]` for the `apply_plan_result` wrapper that only the direct
flavor uses. `decode_line_type` is a third copy (`submit.rs` ⇄ `committer.rs:901`, with a
"copy-paste; resist premature abstraction" note). The sibling flagged the identical shape as
its D2.1 [P2] "paired-file maintenance hazard": a fix to one copy (e.g. a lock-ordering or
hydration change) silently skips the other. **Proposed action:** extract a shared
`ledger-spi-common` crate (pool_lock + hydration + bulk_write primitives + the line_type
decoder) depended on by both `-c` extensions. Tracked as a fix issue (assess-only here).

#### D5.2 [P3] v3.1 terminology is otherwise clean in source
Apart from D4.1, the source uses Path C / v3.1 / `_c` naming consistently. Crate names,
SPI names (`ledger_submit_trx_c`, `ledger_enqueue_trx_c`), GUC prefix (`ledger_routed_c.*`),
shmem lock tranches (`ledger_v31_*`) are all correct.

## 6. Test coverage

Per-crate test inventory (Phase G): `ledger-core` unit (`plan_apply_provisional.rs` 219,
`plan_apply_strict.rs` 277); `ledger-direct-c` acceptance (`acceptance_direct_methods.rs` 199,
`acceptance_direct_lock_and_concurrency.rs` 126) + `property_ledger_submit_trx_c.rs` (199) +
`common/mod.rs` (251); `ledger-routed-c` acceptance (enqueue 128, affinity 154, committer 277,
orphan_recovery 249) + `common/mod.rs` (522); `ledger-harness` smoke (measure 102, sampler 32).
Plus rich `#[cfg(test)]` unit modules inside most source files (banker_div edges, union-find,
cooldown, arena alloc/free incl. a corrupted-self-cycle test, cleanup's three CAS cases, the
orphan-sweep helpers). ledger-core unit coverage is strong; the unsafe shmem/arena/router
helpers have good pure-fn unit tests.

#### D6.1 [P2] No property test for the routed entry point `ledger_enqueue_trx_c` — `fix-needed` (coverage gap)
`acct-1cer` requires a `property_<fn>.rs` per entry point. `ledger_submit_trx_c` has one;
`ledger_enqueue_trx_c` (the routed flavor) has only acceptance tests (enqueue / affinity /
committer / recovery), no random-scenario property test. The routed path is the higher-risk
surface (unsafe shmem, drop-and-continue, dedup). **Proposed action:** add
`property_ledger_enqueue_trx_c.rs` probing random multi-caller workloads against the §11.1
equivalence invariant (aggregate qty matches direct). Tracked as a fix issue.

#### D6.2 [P3] No shared `assert_invariants_hold` (I1–I7) harness — `divergence`
The `acct-1cer` convention references `tests/common/mod.rs::assert_invariants_hold` (the I1–I7
invariant set). v3.1 has per-crate `common/mod.rs` helpers (DB reset, fixtures) but no named
invariant function; invariants are asserted ad hoc per test. PoC-acceptable (the production
ledger owns the I1–I7 catalog), but a small shared invariant fn (aggregate qty ≥ 0; Σ posting
debits = credits per trx; no `layer_id>0` rows for FIFO/LIFO/WAC/STD) would tighten the net.
**Proposed action:** optional; note as a deliberate PoC scope choice in README.

## 7. Perf / GUC inventory (not yet measured)

`ledger-routed-c/src/lib.rs:50-63` defines 13 GUCs. The P5 report (`results/POC-REPORT.md`)
swept the load-bearing ones (`batch_size_max`, `committer_count`, `router_window_size`, and the
depth/overlap/mode axes). The rest ship at defaults without measurement backing:
`queue_full_timeout_ms` (5000), `batch_window_us` (500), `max_eject_count` (10000),
`caller_tx_timeout_ms` (30000), `eject_cooldown_ms` (10), `spillover_arena_mb` (128),
`staging_queue_size` (16384), `committer_queue_size` (2048). Mirrors the sibling's D7.1 [P3] —
acceptable PoC posture (correctness > tuning), recorded so a future perf pass knows which knobs
are unexercised.

#### D7.1 [P3] `committer_lease_ms` GUC + getter are dead — `dead`
`COMMITTER_LEASE_MS` / `committer_lease_ms_now()` (`lib.rs:54,79`) have no call site (verified).
The lease was the `tm09` predecessor-wait deadline in the v3 strict committer; Path C removed
predecessor-wait, and orphan recovery now uses `is_committer_alive` (`kill(pid,0)` + generation),
not a lease deadline. The GUC description still says "orphan-recovery threshold." Dead
carryover. **Proposed action:** remove the GUC + getter, or document why it's retained.
(Bundle with D4.1/D8.1 — same copy-adapt root cause.)

## 8. Dead shmem scaffolding (the largest consistency finding)

#### D8.1 [P2] 13 `CommitterQueue`/`StagingEntry` fields declared but never read or written — `dead`
Verified by grep (writes=0, reads=0 for each): `router_order_sensitive_groups_total`
(`shmem.rs:156`, "acct-aywu" — Path C chunks uniformly, no order-sensitive no-split case);
`committer_tm09_waits_total` / `committer_tm09_wait_timeouts_total` / `committer_tm09_wait_ns_total`
(`:275-283` — the PoolSeqTable predecessor-wait, removed in Path C); `committer_stage_parse_ns`
/ `_pre_apply_ns` / `_apply_ns` / `_bulk_insert_ns` / `_post_ns` (`:266-270` — per-stage timing
never wired; the committer only updates `committer_pipeline_ns_total/count`);
`audit_reclaims_count` / `audit_orphans_recovered_count` / `audit_lost_submissions_count` /
`audit_last_run_at_ns` (`:215-218` — slot-leak audit counters never wired). These are pure
carryover from `ledger-v3`. They are harmless zeros at runtime, but they (a) mislead — comments
imply a `tm09` predecessor-wait and per-stage timing that Path C does not have; (b) bloat the
`CommitterQueue` struct that is allocated once per cluster in shared memory. **Proposed action:**
delete the 13 fields and their stale-comment blocks; the related observability SPIs that the
harness actually uses (`committer_pool_lock_acquisitions_total`, `_aggregate_upserts_total`,
`_trx_committed_total`, `_dedup_skips_total`, `_dropped_submissions_total`, `_poisoned_total`,
`_deadlock_retries_total`, `_takeover_count`) are all live and stay. Pairs with D4.1/D7.1 as one
"de-Path-B the routed crate" cleanup issue.

## 9. §14 open-question status (which the impl settled)

For the doc-update pass — the impl resolves several of the spec's own open questions:

- **§14.2 pristine-replay** → SETTLED. Confirmed not used; `committer::plan_and_write` is
  drop-and-continue (per-submission trial clone, discard on Err), exactly as §14.2 prescribes,
  including submission_id-ascending order and split-chunk order preservation.
- **§14.3 provisional basis** → SETTLED. Both `running_avg` (default, reads aggregate
  unit_cost) and `standard` (reads `standard_cost`) are implemented and dispatched in
  `provisional.rs:67-82`.
- **§14.1 aggregate unit_cost semantics** → as measured (P5 artifact d: aggregate qty matches
  across flavors; provisional unit_cost may diverge by order — expected).
- **§14.5/§14.6 trx_line ordering + identity-vs-commit order** → the hot path correctly never
  depends on cross-trx id ordering; these remain recalc/close concerns (deferred).

## 10. Pass 2 scope

`AUDIT-PASS2.md` carries: the full per-entry-point R1–R7 walk (with the vacuous-rule
justifications); the `bulk_write` / `hydration` / `pool_lock` parity diff (D5.1 detail); the
recovery-flow walk (cleanup three-CAS, router orphan sweep phase-ordering, identity liveness);
the deep body read of `router.rs` + `committer.rs` + `shmem.rs` + `arena.rs` (memory-ordering
verdict on each atomic, CAS state machines, the UNIQUE-survivor-poisons-the-group asymmetry,
the test-injection-on-production-path and `parse_caller_status` string-fragility notes, and the
per-submission `snapshot.clone()` allocation note).

## Findings index

| ID | Sev | Verdict | Title | Resolution (epic `acct-yojk`) |
|----|-----|---------|-------|-------------------------------|
| D1.1 | P3 | divergence | SPI JSONB vs §4 ARRAY-of-composite | doc-patched §4 + §15 (audit commit `6ee51c0`); JSONB is the chosen wire shape, no code change |
| D1.2 | P3 | divergence | `variance_account` added to line tuple | doc-patched §4 + §15 (`6ee51c0`); correct gap-fill, kept |
| D1.3 | P3 | divergence | `coalesce_aggregates` unspec'd (acct-036x) | doc-patched §5.1/§8/§15 (`6ee51c0`); behavior already in code (acct-036x) |
| D1.4 | P3 | divergence | harness distinct-pool-per-submission limitation | doc-patched README + §15 (`6ee51c0`); harness convention, no code change |
| D1.5 | P3 | divergence | harness `standard_cost` seeding (acct-0z5m) | doc-patched §15 (`6ee51c0`); behavior already in code (acct-0z5m) |
| D3.1 | P3 | divergence | no `qty>=0` CHECK on pool_state (code invariant) | **fixed** acct-yojk.6 (`69d5595`): `CHECK (layer_id <> 0 OR qty >= 0)` on pool_state (migration `0006`); §15 updated |
| D3.2 | P3 | divergence | specific K=1 not schema-enforced | **fixed** acct-yojk.7 (`239e141`): `LedgerError::SpecificPoolOccupied` rejects a 2nd receipt to a specific pool |
| D4.1 | P2 | dead/hygiene | stale Path B / design-v3 / PoolSeqTable / tm09 refs | **fixed** acct-yojk.1 (`d891a9a`): de-Path-B'd the routed crate (headers + scattered refs → design-v3.1 / §6.x / acct-2ttr.*) |
| D4.2 | P3 | clean | greenfield-comment + time-estimate scrub passed | no action |
| D5.1 | P2 | divergence | triplicated pool_lock/hydration/bulk_write copies | **fixed** acct-yojk.2 (`9e6cbc0`): extracted `ledger-spi-common` rlib (pool_lock + hydration + bulk_write + line_type), depended on by both `-c` crates |
| D5.2 | P3 | clean | v3.1 naming otherwise consistent | no action |
| D6.1 | P2 | fix-needed | no property test for `ledger_enqueue_trx_c` | **fixed** acct-yojk.3 (`19fe35a`): `property_ledger_enqueue_trx_c.rs` (random multi-caller workloads vs §11.1 equivalence) |
| D6.2 | P3 | divergence | no shared `assert_invariants_hold` harness | **fixed** acct-yojk.8 (`2ae2473`): shared `assert_aggregate_method_invariants` (I1/I2/I4/I5/I7) |
| D7.1 | P3 | dead | `committer_lease_ms` GUC + getter unused | **fixed** acct-yojk.1 (`d891a9a`): GUC + getter removed (de-Path-B) |
| D8.1 | P2 | dead | 13 dead shmem counter fields (tm09 / stage / audit / order-sensitive) | **fixed** acct-yojk.1 (`d891a9a`): the 13 fields + stale-comment blocks deleted (de-Path-B) |

P1 = 0 · P2 = 4 (D4.1, D5.1, D6.1, D8.1) · P3 = 11. D4.1 + D7.1 + D8.1 shared one root cause
(copy-adapt from Path B) and one fix — closed together as "de-Path-B the routed crate"
(acct-yojk.1).

**Reconciliation (acct-gd3g, post-follow-up):** every fix-needed finding above is resolved — the
four P2s and D3.1/D3.2/D6.2 fixed in code under epic `acct-yojk` (15/15 closed), the five P3 D1.x
divergences folded into the docs by the audit commit itself. The Pass-2 findings (P2.1–P2.6) and
the arena-leak bug found during the acct-yojk.5 body-read (fixed in acct-yojk.15, `49cc894`) are
reconciled in `AUDIT-PASS2.md`. No finding left stale.
