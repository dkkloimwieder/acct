# AUDIT-PASS2 — deep walk on ledger-v3.1 correctness paths

Pass 2 to `AUDIT.md` (Pass 1). Full per-entry-point R1–R7 walk, cross-crate parity diff,
recovery-flow walk, and a body-level read of the unsafe shmem core (`router.rs`,
`committer.rs`, `shmem.rs`, `arena.rs`) with a memory-ordering verdict per primitive.

> **Status (acct-gd3g reconciliation).** Assess-only, point-in-time. All six P2.x findings and the
> §2 triplication are resolved under epic `acct-yojk` (15/15) — see the **Resolution** column and
> reconciliation note in the findings index. Two behavioral notes in §0 below (the whole-group
> poison and the per-submission clone) describe the as-assessed state; the poison granularity was
> changed by acct-yojk.9 (re-drive) and the clone was measured and kept (acct-yojk.12). Prose is
> not corrected in place.

Severity as in Pass 1. New findings here are numbered `P2.x`; Pass-1 findings are referenced
by their `Dx.y` id.

## 0. Headline

The correctness paths are sound. **R4 (lock-before-read), R6 (idempotency), and R7 (audit
field from post-lock output) are satisfied; R1/R3/R5 are vacuous for Path C's single-class
pool model** (justified per-rule below, not skipped). Memory ordering on every shmem atomic is
correct: the data-before-flag discipline (plain field writes → `Release` CAS; reader
`Acquire`-load) holds in enqueue, router emit, eject, cleanup, and the orphan sweep. The arena
allocator is defensively bounded against double-free cycles. Drop-and-continue matches §14.2
exactly. Two behavioral notes worth a reviewer's eye (neither a defect at PoC scope): a
UNIQUE-constraint survivor poisons the whole commit_group rather than dropping one submission
(§6.8-acknowledged), and the committer clones the working snapshot per submission. New findings:
P2.1 (UNIQUE-survivor poison granularity, P3), P2.2 (`parse_caller_status` string fragility,
P3), P2.3 (test-injection reads on the production path, P3), P2.4 (per-submission
`snapshot.clone()`, P3). The acct-yojk.5 coverage gap is now closed — `payload.rs` (arena codec)
and the full harness are body-read in §6, both **clean**, adding P2.5 (equivalence drain-wait is a
fail-safe quiescence heuristic, P3) and P2.6 (lifo/specific absent from load scenarios, P3).

## 1. R1–R7 walk per entry point

Path C model recap: a `pool` row is one `(sku_id, location_id, identity_key)` — **a single
inventory class**. `pool_state` is one aggregate row per pool (`layer_id=0`) plus, for
`specific` only, K=1 layer rows. There is no raw/fg/wip class sharing a `(sku, location)`, which
is the substrate the production R1/R2/R3/R5 rules guard against. Dispatch is on the SIGN of
`line.qty` (≥0 receipt / <0 depletion), not on a cross-class SKU resolution.

### `ledger_core::plan_apply_provisional` (provisional.rs) + wac/standard/specific
- **R1 (per-class qty divisor)** — VACUOUS. The only division is WAC's `banker_div(numerator,
  new_qty)` (`wac.rs:80`); `new_qty` is the pool's own aggregate qty (`old_qty + line.qty`),
  read from this pool's `layer_id=0` row — never a cross-class `stock_available`-style pooled
  sum. There is no second class to confuse. **clean (N/A).**
- **R2 (credit-side SKU dispatch)** — clean. Method dispatch is `snapshot.method_of[pool_id]`
  (`provisional.rs:29-34`); receipt/depletion is `line.qty.cmp(&0)`. No debit/credit-side SKU
  ambiguity exists because a line names exactly one `pool_id`. The provisional-basis lookup
  (`provisional_basis_of[pool_id]`, `provisional.rs:68`) is the depletion-source pool by
  construction. **clean.**
- **R3 (solo-occupancy gate)** — VACUOUS. No shared pool across documents; each submission's
  lines name their own pools, and the snapshot mutates in place per line. **clean (N/A).**
- **R4 (FOR UPDATE before read)** — N/A at this layer (pure Rust, no DB); enforced by callers
  (see direct/routed below).
- **R5 (single-leg variance on drained debit-normal pool)** — VACUOUS. No close-hook variance.
  STD's variance is a receipt-time posting computed from `(c_actual − c_std)` (`standard.rs:103-126`),
  correctly routed favorable/unfavorable; it is not a drained-in-period-pool 2-vs-1-leg case.
  **clean (N/A).**
- **R6 (idempotency dual-check)** — N/A at this layer (the entry points own it).
- **R7 (audit field from post-lock output)** — clean. `TrxLineOutput.unit_cost` is set inside
  the dispatcher to the computed value (actual for receipts; running-avg/standard for
  WAC/provisional depletions; `c_std` for STD; layer cost for specific) — `wac.rs:89,146`,
  `standard.rs:76,156`, `specific.rs:53,123`. Callers persist it verbatim; nothing re-derives a
  cost from a pre-lock read.

### `ledger_submit_trx_c` (ledger-direct-c/submit.rs)
- **R4** — clean. `pool_lock::acquire_pool_locks(&touched)` (`submit.rs:124`) takes `SELECT 1
  FROM pool_lock WHERE pool_id=$1 FOR UPDATE` ascending (`pool_lock.rs:34-60`, defensive
  sort+dedup) BEFORE `hydrate_snapshot` reads `pool_state` (`submit.rs:129`). The serialization
  point is the `pool_lock` mutex-table row, not a `pool_state` row lock — the documented v3.1
  locking model (`hydration.rs:17-18` warns the caller must hold the lock). Reads and writes for
  a pool both occur under its `pool_lock` row. **clean.**
- **R6** — clean (synchronous variant). No pre-flight dedup; the `trx` UNIQUE `(trx_type,
  source_id)` (`insert_trx`, `bulk_write.rs:24-40`) is the backstop — a duplicate raises and the
  caller's own user-tx aborts. R6's dual-check matters for the async committer (where a
  duplicate must not poison a batch); for a synchronous caller-tx the single constraint check is
  sufficient and correct.
- **R7** — clean. `bulk_write::insert_trx_lines` persists `o.unit_cost` from `PlanResult.trx_lines`
  (`bulk_write.rs:63`); the cost is the dispatcher's post-lock output, not a re-read.
- R1/R2/R3/R5 inherit ledger-core's verdicts (this layer only marshals).

### `ledger_enqueue_trx_c` + committer (ledger-routed-c)
- **R4** — clean. `attempt_commit_phase` (`committer.rs:465`) calls
  `pool_lock::acquire_pool_locks(pool_ids)` then `hydrate_snapshot(pool_ids)` then `plan_and_write`
  — all inside one `BeginInternalSubTransaction`. On a deadlock retry the subtx rolls back and
  re-acquires locks + re-hydrates a fresh snapshot (`:459-464` comment, `:473-484`). Locks span
  the whole commit_group's deduped pool union (`:367-372`). **clean.**
- **R6** — clean (dual-layer). Pre-flight `dedup_against_trx` (`committer.rs:678`) queries `trx`
  for existing `(trx_type, source_id)` + within-batch first-wins BEFORE the write phase; the
  `trx` UNIQUE is the structural backstop after. The dedup compares `trx_type::text` vs input
  text (`:692-695`) so an unknown trx_type can't fail an enum cast. See P2.1 for the
  backstop-fires-mid-batch behavior.
- **R7** — clean. Same `bulk_write::insert_trx_lines` persists dispatcher output;
  `plan_and_write` reconstructs the per-pool aggregate from the post-pass snapshot
  (`committer.rs:558-570`), i.e. post-lock state.
- R1/R2/R3/R5 inherit ledger-core (the committer marshals; it adds no class/cost logic).

**Conclusion:** no R1–R7 violation. The applies-rules are satisfied; the vacuous rules are
N/A-by-construction (single-class pools), recorded so a future multi-class recalc/close phase
re-checks them rather than assuming.

## 2. Cross-crate parity: pool_lock / hydration / bulk_write (D5.1 detail)

`diff` of `ledger-direct-c/src/{pool_lock,hydration,bulk_write}.rs` vs the `ledger-routed-c/src/`
copies: **identical except the module doc comments** (direct cites §5.1; routed cites §6.4 +
"shared shape"). `bulk_write.rs` routed copy adds `#![allow(dead_code)]` for the
`apply_plan_result` wrapper (direct-only; the committer drives the primitives directly to get
the §6.7 per-pool-aggregate collapse — `committer.rs:542-570`). `decode_line_type` is a third
copy (`submit.rs:42` ⇄ `committer.rs:901`, the latter annotated "copy-paste; resist premature
abstraction").

Hazard (= sibling D2.1 [P2]): a correctness fix to one copy (lock ordering, hydration query, a
new `pool_state` column in `bulk_write`) silently skips the other. The copies are correct
*today* and behaviorally aligned; the risk is drift over future edits. The "resist premature
abstraction" note was a reasonable build-time call, but with the path now stable a shared
`ledger-spi-common` crate is the right consolidation. Filed as a fix issue (assess-only here).

## 3. Recovery flow walk

### 3.1 cleanup (cleanup.rs) — three CAS cases
`try_release_staging_slot` (`:55`) handles (1) success `valid==3 ∧ cg_id==ours` → CAS 3→0;
(2) ejected `valid==1, cg_id==0, eject>0` → both CAS fail, arena left for re-pack; (3)
router-died-mid-stamp `valid==2, eject==0` → CAS 3→0 fails, CAS 2→0 fallback frees. The
`observed_cg == commit_group_id` guard (`:65`) prevents a stale cleanup from freeing a slot a
concurrent router already re-packed under a new cg_id. Reads `cg_id`/`eject` with `Acquire`
pairing the eject path's `Release` stores. Lock ordering documented sequential, never nested
(`:37-40`) — STAGING/COMMITTER and ARENA guards taken one at a time. Five unit tests cover all
three cases + the cg-mismatch + the in-flight-eject-blocks-2→0 edge. **clean.**

### 3.2 recovery.rs — postmaster-start worker
Trivial by design (`recovery.rs:1-27`): shmem is zero on PG start, so there are no orphans to
sweep; it just `Release`-stores `recovery_complete=1` so router/committers open for traffic.
Router/committers `Acquire`-spin on it (`router.rs:74-83`). Coherent with §6.5. (Header text is
stale — D4.1.) **clean** (logic).

### 3.3 router orphan sweep (router.rs::try_recover_router_orphan, :767)
Runs on every router (re)launch (`set_restart_time(5s)`), before the tick loop. Three phases:
Phase 1 (`sweep_queue_phase`, CQ `valid==1`) re-stamps linked staging entries the dead router
left at `valid==2, cg_id∈{0, this cg}` → store cg_id Release + CAS 2→3 (completes the
interrupted data-before-flag block). Phase 2 (CQ `valid==2`, dead committer per
`is_committer_alive`) reverts CQ 2→1 + clears identity so a live committer re-claims; relies on
the committer's pre-flight dedup as the recovery source of truth (no pristine-replay). Phase 3
(`sweep_staging_phase`, staging `valid==2` whose cg_id isn't backed by an active CQ) reverts
2→1 + resets cg_id.

**Phase ordering is load-bearing and correctly documented** (`:755-760`): Phase 1+2 MUST run
before Phase 3, because a staging entry the queue phase is about to re-stamp carries `cg_id==0`
(router died before the cg_id store) and so is absent from `active_cg_ids` — running Phase 3
first would revert it as an orphan and drop a recoverable submission. Helpers
(`try_restamp_staging_to_routed`, `try_revert_orphan_staging`) are pure, idempotent, and have
12 unit tests (promote/skip/mismatch/idempotent). **clean.** (Matches the discipline behind the
sibling's `acct-x9tm` phase-ordering note.)

### 3.4 identity.rs — committer liveness
`claim_committer_identity` (`:27`) two-pass: reclaim `pid==0` slots, then take over dead-PID
slots (`kill(pid,0)==ESRCH`); `generation.fetch_add` makes recycled slots detectable. `release_*`
bumps generation BEFORE clearing pid (`:80-81`) so a stale CQ reference no longer matches.
`is_committer_alive` (`:89`) checks pid≠0 ∧ generation matches ∧ `kill(pid,0)≠ESRCH`, treating
EPERM as alive (conservative). PID-recycling-safe via generation. **clean.**

## 4. Deep body read — memory ordering + state machines

### 4.1 shmem.rs — atomics & layout
Staging `valid` state machine 0=empty/1=pending/2=processing/3=routed/4=abandoned; committer
`valid` 0=empty/1=ready/2=in_flight/3=completed/4=poisoned. The CV lazy-init
(`ensure_backpressure_cv_initialized`, `:362`) correctly handles that PG18's
`ConditionVariable.wakeup` proclist sentinel is `INVALID_PROC_NUMBER` (−1), NOT zero-init — a
3-state CAS gate runs `ConditionVariableInit` exactly once. `now_us`/`now_ns` clamp
`GetCurrentTimestamp()` at 0 to avoid the negative-cast-wraps-to-huge-u64 bug. 64-byte
alignment on `CommitterIdentitySlot` avoids false sharing. **clean** — except D8.1 (13 dead
fields) and D4.1 (stale "Path B / PoolSeqTable" header).

### 4.2 enqueue.rs — publish ordering
Field stamps (`request_seq`, offsets, `user_tx_xid`, …) then `valid.compare_exchange(0,1,Release,
Relaxed)` (`enqueue.rs:289-302`): the Release publishes the plain writes before the slot becomes
pending. Done under `STAGING_QUEUE.exclusive()`. Forces caller XID via
`GetCurrentTransactionId` (`:96`) so the committer's `pg_xact_status` triage has something to
read. Backpressure: CV wait with deadline, frees partial arena allocs on QueueFull
(`:159-184`). **clean** (ordering). Header is stale (D4.1).

### 4.3 router.rs — emit + affinity
Data-before-flag in `emit_commit_group` (`:302-332`): `commit_group_id.store(cg_id, Release)`
THEN `valid.compare_exchange(2,3,Release,Relaxed)` per staging entry; the CQ entry's plain
fields are stamped before its `valid.compare_exchange(0,1,Release,Relaxed)` (`:274-285`).
Rollback paths (`rollback_packed_to_pending` 2→1, `free_commit_group_arena`) on arena/CQ
exhaustion are correct and leak-free. Union-find has path-compression + union-by-rank
(`:386-423`); affinity grouping orders components by min request_seq and members by request_seq
(oldest-first dispatch). Eject-cooldown filter is a pure, well-tested helper. **clean.**

### 4.4 committer.rs — claim, drop-and-continue, §6.7 collapse, §6.8 errors
- Identity election (`claim_next_committer_entry`, `:203`): CAS `committer_bgw_generation`
  0→my_gen (AcqRel), store slot (Release), CAS `valid` 1→2 (Acquire); on the rare valid-CAS loss
  it resets generation+slot. Correct single-winner election.
- Drop-and-continue (`plan_and_write`, `:510`): per submission `trial = snapshot.clone()`;
  `plan_apply_provisional`; on Ok adopt the trial + queue the plan, on Err `dropped += 1` and the
  snapshot is untouched. No pristine-replay — exactly §14.2. The §6.7 collapse: per-submission
  trx/trx_line/posting_line + each submission's *layer* mutations, then ONE aggregate UPSERT per
  touched pool reconstructed from the final snapshot (`:558-570`) — immune to duplicate-pool
  because it reads the post-pass aggregate, not per-submission deltas.
- §6.8 (`attempt_commit_phase`, `:465`): whole lock→hydrate→apply→write phase in a nested
  subtx; `PgTryBuilder ... .catch_others` threads the caught SQLSTATE out as `PhaseStep::Caught`
  (no captured cell — satisfies the unwind-safety bound, a clean pattern); 40P01/40001 →
  Retryable (exp backoff ≤5) → else Poison. Pipeline timing + counters via `Relaxed` (under no
  ordering requirement). **clean.**

#### P2.1 [P3] A UNIQUE-survivor poisons the whole commit_group — `divergence` (acknowledged)
If a `(trx_type, source_id)` slips past `dedup_against_trx` and hits the `trx` UNIQUE at
`insert_trx` (a race between the dedup SELECT and the INSERT — reachable only under
recovery-reclaim reprocessing, since the router assigns each staging slot to one group), the
ERROR longjmps to `catch_others`, classifies as `Fatal`, and `Poisoned` discards **every**
submission in the group, not just the duplicate. This is coarser than drop-and-continue (which
drops a single bad submission). It is explicitly acknowledged as PoC posture (`committer.rs:38-43`,
§6.8 "finer per-SQLSTATE handling is production hardening") and is rare by construction.
**Proposed action:** none for the PoC; note it as a known robustness gap. A production fix would
catch 23505 specifically and re-drive the group minus the offending submission.

#### P2.2 [P3] `parse_caller_status` depends on PG's literal "in progress" string — `divergence`
`committer.rs:812-819` matches `pg_xact_status` text `"committed"`/`"aborted"`/`"in progress"`;
anything else → `Unknown` → kept. A PG wording change would silently reclassify in-progress
callers as Unknown-keep, committing work for a still-open caller tx. Mirrors the sibling's D5.2.
Low risk (PG's strings are stable; the keep-on-Unknown is optimistic-but-bounded by the eject
timeout). **Proposed action:** note the coupling; consider asserting on the known set.

#### P2.3 [P3] Test-injection atomics read on the production path — `divergence`
`maybe_inject_deadlock`/`_fatal`/`_stall` (`committer.rs:622-671`) and the router's
`test_inject_router_delay_us`/`test_reorder_router_stores` (`router.rs:306-314`) `Acquire`/`Relaxed`-
load shmem atomics on every commit phase / emit, even in production builds (they load 0 and
skip). Documented as intentional ("production builds Acquire-load 0/skip", "shmem layout
doesn't drift across feature variants"). Mirrors the sibling's D5.4. Negligible cost; noted for
completeness. **Proposed action:** optional — feature-gate the read sites behind
`cfg(feature="test_hooks")`.

#### P2.4 [P3] Per-submission `snapshot.clone()` in the committer hot path — `divergence` (perf)
`plan_and_write` clones the whole working `Snapshot` per submission (`committer.rs:519`) to get
a discardable trial. For the §11.3 hot-pool batching scenario each clone is tiny (one pool), so
the measured win is unaffected; for large multi-pool commit_groups it is O(submissions × pools)
HashMap clones. Mirrors the sibling's D5.3 (hot-path allocations). **Proposed action:** optional
— apply-then-rollback on a single snapshot (revert the touched pools on Err) instead of
clone-per-submission, if a future perf pass shows it matters.

## 5. arena.rs — allocator safety
Bump + LIFO-freelist, first-fit, 8-byte aligned, 8-byte block header. Two safety properties
verified: (a) the offset-0 sentinel — first alloc bumps past offset 0 so no real block header
ever lives at 0 (which would alias the freelist-empty sentinel and lose the block),
`arena.rs:86-88`; (b) **bounded freelist walk** — both `alloc` and `freelist_count` cap the walk
at max-block-count and, on overrun, abandon the corrupted list (leak) + fall through to bump,
rather than spin forever under the LWLock without a CHECK_FOR_INTERRUPTS (which would also wedge
SIGTERM) — `:90-110`, with an explicit corrupted-self-cycle unit test (`:339-360`). `free` does
no double-free validation (documented caller contract, `:42-44`); the alloc-side cycle guard is
the safety net. Reasonable PoC posture (and a genuinely thoughtful "leak beats wedge" tradeoff).
**clean.**

## 6. Coverage-gap close (acct-yojk.5): payload.rs + harness body-read

Pass 1/2 read every correctness-critical file at the body level but assessed `payload.rs` (the
arena codec) and the harness internals via interfaces + their own unit tests. acct-yojk.5 closes
that — both are now read line-by-line. **No P1/P2.**

### 6.1 payload.rs — arena codec (`ledger-routed-c/src/payload.rs`)
JSON-over-arena: lines block (`u32` LE length prefix + JSON) then submission block; `line_offset`
back-patched after the lines alloc. **clean.**
- **Bounds:** `read_bytes_bounded` (`:232-245`) `checked_add`s `offset+len` and rejects `end >
  arena_bytes`, so every decode read is bounded; `line_count` is re-validated against the actual
  decoded vector via a `u64` compare (`:206-211`), no truncation. Round-trip + tamper + OOB +
  bad-JSON unit tests plus a 64-case proptest (`:411-442`).
- **Sentinel soundness:** `free_submission` and the `line_offset==0`-means-unset convention rely
  on offset 0 never being a live allocation — confirmed against `arena.rs:86-88` (first alloc
  bumps past 0; first real payload lands at 16). So the `!= 0` free guards (`:224-229`) cannot
  skip a live block.
- **Error-path cleanup:** every failure after the lines alloc frees `lines_offset` before
  returning Err (`:151,160,168`), so the arena outstanding-count can't drift on the error path —
  correct by inspection and matched by `free_submission_returns_arena_outstanding_to_zero`
  (`:355-362`). *Note: no unit test forces an arena-full mid-encode, so the cleanup branch itself
  is unexercised — rolls into the D6.1 routed test gap.*
- **Sub-P3 defensive notes (all safe as written, no action):** `lines_block_len =
  lines_bytes_len.saturating_add(4)` (`:136`) masks the +4 overflow rather than erroring, but a
  blob that large fails `arena.alloc` (MB-scale arena) long before — unreachable. `submission
  .line_offset + 4` (`:201`) is an unchecked `u32` add, but the prior bounded 4-byte read at
  `line_offset` (`:194`) already gates `line_offset` to arena-sized (small) values, so it cannot
  wrap. Header says "Path B" + "design plan §E" — **stale (D4.1)**; add `payload.rs` to the
  de-Path-B file set.

### 6.2 harness internals (`ledger-harness/src/*`)
All 14 modules read. Well-engineered, honest measurement tooling. **clean.**
- **Equivalence (`equivalence.rs`)** is the routed≡direct keystone: aggregate `qty` MUST match
  (order-independent net sum), `unit_cost` MAY diverge (order-sensitive running avg) — faithful to
  §11.1. The failure direction is safe: the drain-wait (`:209-231`) is a quiescence heuristic, so
  a timeout or mid-run committer stall snapshots a partially-drained state and surfaces as a
  *false FAIL*, never a false PASS — it cannot mask a real divergence. **New: P2.5 [P3].**
- **Throughput count is authoritative (`driver_routed.rs`):** the incremental `id > last_id`
  observer can skip out-of-order-committed rows, but the final full-range reconciling sweep
  (`:141-155`) re-captures every committed `source_id`, so `trx_count` is exact; stragglers only
  lose latency fidelity (10 ms poll quantization, best-effort tail). Confirms the 10
  `committer_*` counter getters are **live** (`read_routed_counters :341-370`) — a different set
  from the 13 dead shmem fields (D8.1).
- **Depth-insensitivity is honest (`seed.rs`):** `deepen` writes real `pool_state` layer-row
  volume directly via SQL (constant `unit_cost`/layer, no rounding to reconcile), and Path C
  provably touches only `layer_id=0` — so §11.2 measures an O(1) aggregate touch *against a pool
  that genuinely has N layers present*, not "fast because no layers exist."
- **Codec pinned from both ends:** `build_lines_json` (`driver_common.rs`) feeds both flavors
  identically; its shape is asserted by its own test and independently by the SPI property test.
- **Method-coverage boundary (`scenarios.rs` / `pool_universe.rs`):** no throughput/equivalence
  scenario uses `lifo` or `specific` as its primary method, and `Mixed` is fifo/wac/std only —
  faithful to the §10.6 scenario set (which also omits them); `lifo`/`specific` are covered by the
  cost-core unit/property tests, not by a load run. **New: P2.6 [P3].** Worth a one-line caveat in
  the P5 report so method coverage is read honestly.
- `pool_universe.rs` holds the acct-0z5m std_cost seeding (one row per (sku,location), no dup PKs)
  and the TRUNCATE list correctly omits the v3-only `posting_lines_provisional`. `workload.rs`
  generates distinct pools per submission by construction — an alignment with §5.1 dedup, *not* a
  §10.3 limitation (and a side effect: `coalesce_aggregates`/acct-036x is exercised only by the
  SPI property test, never the harness). `sampler.rs`/`measure.rs` are observational; `report.rs`
  percentile extraction (ns→µs) is correct; `cli.rs` flags the pgbouncer requirement (acct-8cn2).

#### P2.5 [P3] equivalence drain-wait is a quiescence heuristic — `quality` (harness robustness)
`equivalence.rs:209-231` returns `Ok` on timeout/early-quiet and diffs whatever materialized; a
partial drain becomes a confusing qty-mismatch FAIL rather than an explicit error. Fails safe (no
false PASS), but **proposed action:** assert `SELECT count(*) FROM trx == submissions.len()`
before diffing so a partial drain reports as "drain incomplete" not "equivalence broken." Rolls
into the P3 bucket (acct-yojk.4).

#### P2.6 [P3] lifo/specific absent from load + equivalence scenarios — `coverage` (acknowledged)
`scenarios.rs` S1–S8 and `MethodMix::Mixed` never drive `lifo` or `specific` as a primary method.
Faithful to §10.6; those methods are covered by ledger-core unit/property tests. **Proposed
action:** one-line caveat in the P5 report (method coverage). Rolls into acct-yojk.4.

## 7. Verdict

`ledger-v3.1` is a faithful, well-engineered Path C PoC. The correctness machinery — overflow
safety, locking, memory ordering, recovery, drop-and-continue, the §6.7 collapse, the arena
guard, the arena codec, and honest measurement tooling — is sound and well-tested. No P1. The
actionable work is cleanup of copy-adapt residue (D4.1 + D7.1 + D8.1 as one "de-Path-B the routed
crate" issue, now incl. `payload.rs`; D5.1 shared-crate extraction; D6.1 routed property test)
plus a handful of P3 documentation/hygiene notes. The six P2.x notes here are all either
acknowledged PoC posture, sibling-parallel, or fail-safe harness robustness — none need a
PoC-scope change.

## Findings index (Pass 2)

| ID | Sev | Verdict | Title | Resolution (epic `acct-yojk`) |
|----|-----|---------|-------|-------------------------------|
| §1 | — | clean | R1–R7 walk: R4/R6/R7 satisfied, R1/R3/R5 vacuous-by-construction | no action |
| §2 | (D5.1) | divergence | pool_lock/hydration/bulk_write triplication | **fixed** acct-yojk.2 (`9e6cbc0`): `ledger-spi-common` rlib |
| §3 | — | clean | cleanup 3-CAS, orphan-sweep phase order, identity liveness | no action |
| §4 | — | clean | memory ordering on every shmem atomic (data-before-flag) | no action |
| P2.1 | P3 | divergence | UNIQUE-survivor poisons whole commit_group | **fixed** acct-yojk.9 (`6fe3979`): `PhaseOutcome::DuplicateRace` re-drives the group minus the offender; poisons only if the re-dedup resolves no offender |
| P2.2 | P3 | divergence | parse_caller_status PG-string coupling | **fixed** acct-yojk.10 (`e9eb7eb`): `CallerTxStatus::Unrecognized` (WARN + eject, never keep) so a PG wording drift fails loud |
| P2.3 | P3 | divergence | test-injection reads on production path | **fixed** acct-yojk.11 (`f14b779`): reads/calls `#[cfg(feature = "test_hooks")]`-gated, compiled out of production |
| P2.4 | P3 | divergence | per-submission snapshot.clone() | **assessed → won't-fix** acct-yojk.12 (`cc28d11`): measured ~96–180 ns/clone vs ~19 µs/submission SPI ≈ 0.5–1%; kept; bench `ledger-core/examples/clone_bench.rs` |
| §5 | — | clean | arena allocator safety (offset-0 sentinel + bounded-walk cycle guard) | no action |
| §6.1 | — | clean | payload.rs arena codec (bounds, sentinel, error-path cleanup) | header de-Path-B'd in acct-yojk.1; **arena-leak bug** found in this body-read **fixed** acct-yojk.15 (`49cc894`) — see note below |
| §6.2 | — | clean | harness body-read: equivalence keystone + authoritative trx count + honest depth seeding | no action |
| P2.5 | P3 | quality | equivalence drain-wait quiescence heuristic (fails safe) | **fixed** acct-yojk.13 (`38339a8`): asserts trx baseline-delta == submissions before diffing (partial drain → distinct "drain incomplete" error) |
| P2.6 | P3 | coverage | lifo/specific absent from load + equivalence scenarios | **fixed** acct-yojk.14 (`fcd2d6f`): method-coverage caveat in `results/POC-REPORT.md` |

**Arena-leak follow-on (acct-yojk.15, `49cc894`).** The §6.1 codec body-read concluded "clean,"
but the deeper acct-yojk.5/.15 trace found the committer's cleanup freed only the submission block,
not the lines block — leaking ~1 arena block per committed submission (outstanding count never
returned to 0). Fixed by freeing the lines blob and tracking its offset on `StagingEntry`
(`line_offset`). This is the one correctness bug the audit's interface-level §6.1 read missed and
the body-read caught — recorded here so the index isn't read as "§6.1 had no issues."

**Reconciliation (acct-gd3g, post-follow-up):** all six P2.x and the §2 triplication are resolved
under epic `acct-yojk` (15/15 closed). One newly-found divergence — two stale `committer.rs`
comments (module header + `Poisoned` variant doc) still list "UNIQUE survived dedup" as a *direct*
poison cause, contradicting the acct-yojk.9 re-drive — is filed as a follow-up (code-comment edit,
not patched inline per the gd3g docs-only constraint).
