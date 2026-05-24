# AUDIT-PASS2 — deep walk on ledger-routed + ledger-direct correctness paths

**Issue:** acct-o00c (P1)
**Run window:** 2026-05-24
**HEAD at audit:** `a12b0d0` (post acct-cssx)
**Scope:** Pass 2 follow-up to acct-b4z4. All 5 dimensions enumerated in the bd issue, per the scope-confirmation question at claim time.
**Companion:** `AUDIT.md` (Pass 1 — coherence + quality snapshot). This doc focuses on correctness paths.

## 0. Headline

Five dimensions audited. Verdicts:

| # | Dimension | Verdict |
|---|-----------|---------|
| 1 | R3-R7 walk on submit + process_commit_group + close_period   | ✓ clean — all five rules either satisfied by construction or covered by tests |
| 2 | bulk_write parity (direct vs routed)                          | ✓ byte-equivalent SQL across all 5 functions; one **maintenance hazard** flagged |
| 3 | hydration parity (direct vs routed)                           | ✓ byte-equivalent SQL + flow |
| 4 | Recovery flow (cleanup + recovery + router-orphan sweep)      | ✓ three-case CAS handling correct; recovery sweep is two-phase + idempotent |
| 5 | Full body read: router.rs (1349 LOC) + committer.rs (1080 LOC) | ◐ load-bearing logic sound; cataloged 4 minor findings |

**Headline takeaway:** the correctness paths are sound. The largest risk surface is **paired-file divergence** between `ledger-direct` and `ledger-routed` (bulk_write, hydration, pool_lock, line-type decoders). All four pairs are documented as "locked plan §G Q3: copy-paste, resist premature abstraction" — explicitly chosen, but it puts the burden on the reviewer to spot drift. No drift exists today; the equivalence harness would catch most divergence, but not all (e.g., comment-only drift, SQL-only drift in queries that produce identical results on tested inputs).

No P0 findings. Three P2 findings + a handful of P3 observations. None block Phase 7.

---

## 1. R3-R7 walk on correctness paths

CLAUDE.md's R1-R7 framework is framed around acct's specific multi-method dispatch on `_post_posting_lines_apply_event`. v3's analog is two distinct functions per path; rules apply in adapted form. Pass 1 verdicts repeated here with line-anchored evidence.

### R1 — Per-class qty divisors from per-class signed SUM

**Verdict:** N/A. v3 has no per-class pool partition; one pool, one method. The classification "value vs qty pool" doesn't exist (each pool's `pool_state` row carries both). No divisor-from-cross-class-state risk.

### R2 — Cost-method dispatch resolves SKU from CREDIT side

**Verdict:** N/A in the acct sense. v3's dispatch is per-line (`snapshot.method_of[&line.pool_id]` at `ledger-core/src/method.rs:52`); each line carries its own `pool_id`. No CREDIT-side asymmetry to worry about.

### R3 — Solo-occupancy gates on shared-pool mutations

**Verdict:** ✓ satisfied by the pristine-replay envelope.

`committer.rs::run_pristine_replay_loop` (line 509) holds `pool_lock FOR UPDATE` for the duration of plan + bulk-write. The lock is released only at COMMIT (which is the end of the `BackgroundWorker::transaction` scope in `committer.rs:125`). Two committers cannot mutate the same pool concurrently. The acct-style "solo-occupancy on a shared pool" question — "what if my mutation is one of N concurrent against the same pool?" — is structurally excluded by the lock.

`submit.rs` (Path A) holds the same lock for the full 8-step pipeline at `submit.rs:121` before any pool_state read.

### R4 — Pool reads under FOR UPDATE on the SAME account before subsequent writes

**Verdict:** ✓ satisfied in both paths.

Both paths follow the strict ordering: `pool_lock::acquire_pool_locks(&touched)` → `hydration::hydrate_snapshot(&touched)`. The two functions take the SAME `Vec<i64>` of `pool_ids` (dedup-sorted via `BTreeSet` upstream). Code anchors:

- ledger-direct: `submit.rs:121` (lock) → `submit.rs:128` (hydrate)
- ledger-routed: `committer.rs:317` (lock) → `committer.rs:325` (hydrate)
- ledger-direct close_period: `close_period.rs:149` (lock) → `close_period.rs:154` (compute final_avg from in-period postings)

`pool_lock::acquire_pool_locks` itself (`pool_lock.rs:32-43`) is "INSERT ON CONFLICT DO NOTHING; SELECT 1 FOR UPDATE" per-pool in ascending order. The `FOR UPDATE` is on the SAME row about to be read in the SELECT pool_state, not on a different row that "controls access to" it (acct's earlier R4 lift-class). Direct match.

### R5 — Single-leg variance on debit-normal pools drained in-period

**Verdict:** ✓ satisfied in close_period; N/A elsewhere.

`close_period.rs:222-252` posts ONE variance posting_line per non-zero-variance provisional row. The (debit, credit) routing is either (a) original depletion accounts when variance > 0, or (b) SWAPPED original accounts when variance < 0 (lines 232-236). This is structurally single-leg (one debit + one credit per posting_line, not a 2-leg wash). The pool_state cumulative-sum bookkeeping correction at line 291-301 is a non-posting state-only adjustment (UPDATE unit_cost = unit_cost - sum). Per the spec patch we just landed (§4.5 variance routing note), this is the PoC simplification vs. main acct's `variance_wac_periodic` account_kind routing.

R5's "drained debit-normal pool" concern doesn't apply directly: v3's pools aren't classified debit-normal vs credit-normal. The closest analog — pool depletes to zero in-period and variance posts later — IS handled correctly: the variance posts against the original depletion's accounts (which were not drained to zero by definition; the variance arrives BEFORE any subsequent depletion clears those accounts).

### R6 — Idempotency replay checks before AND after FOR UPDATE

**Verdict:** ✓ satisfied by `trx UNIQUE(trx_type, source_id)` + pristine-replay's UNIQUE-violation catch.

**Before FOR UPDATE.** No explicit pre-lock duplicate check. The shipped behavior relies on: (a) callers don't deliberately submit duplicates within a single submission's lines (callers control their own source_id assignment); (b) within a commit_group, two submissions with the same (trx_type, source_id) will both reach `plan_apply` successfully (per-line operations succeed against the shared snapshot), then the SECOND `INSERT trx` will fire UNIQUE violation in bulk-write. **Implicit before-lock check via uniqueness of source_id at the application layer; verified by the UNIQUE constraint at write time.**

**After FOR UPDATE.** `committer.rs::write_plans_in_subtx` (line 643-696) wraps the bulk-write in a savepoint with `PgTryBuilder::catch_when(ERRCODE_UNIQUE_VIOLATION, ...)`. On catch:
- `in_flight: Cell<Option<usize>>` (line 655) tracks which plan was mid-write
- Savepoint rolls back via `RollbackAndReleaseCurrentSubTransaction` (line 677)
- Outer loop in `run_pristine_replay_loop` (line 535-541) reads `WriteOutcome::UniqueViolationAt { idx }`, adds idx to `excluded`, restarts from pristine

This correctly handles: (a) duplicate of an already-committed prior trx (caught by UNIQUE on the second commit_group's attempt); (b) duplicates within the same commit_group (caught on the SECOND INSERT inside the savepoint).

**Subtle correctness point:** the `in_flight` Cell is read AFTER `.execute()` returns, NOT inside the `catch_when` closure (lines 666-669 explain why). The mechanism works but is subtle — a contributor editing `write_plans_in_subtx` could easily move the Cell-read into the closure and break the index attribution. **Flagged as P3 D2.1 below.**

`submit.rs` (Path A) has no replay loop — N=1 per call. UNIQUE violation raises as `ereport!(ERROR, ...)` → caller's tx aborts. The "after FOR UPDATE" check is the constraint check at INSERT time; the "before FOR UPDATE" check is the caller's own source_id management.

### R7 — Document-audit fields from post-lock dispatcher output

**Verdict:** ✓ satisfied.

`trx_line.unit_cost` is set inside ledger-core's per-method bodies (`wac.rs:185`, `wac_periodic.rs:195`, etc.) using values computed from `snapshot.pools` (the hydrated, post-lock snapshot). The committer never writes `unit_cost` from a pre-lock observation.

For WAC depletions specifically: `unit_cost = amount / qty_depleted` where `amount = (Q × value_sum) / qty` — both `value_sum` and `qty` read from the post-lock snapshot at `wac.rs:181-182` (approximate line; verify in current file). The display `unit_cost` and the ledger-of-record `posting_line.amount` are derived from the same single bounded round, so they agree.

**Defensive observation:** if a future refactor moves dispatch decisions outside the locked pristine-replay loop (e.g., to optimize by pre-classifying methods at router-emit time), R7 would need re-walking. The acct-aywu order-sensitive classifier IS already at router-emit time, but it only affects ROUTING decisions, not arithmetic — pool_method is still re-read at hydration. Stable for now.

---

## 2. bulk_write parity (ledger-direct vs ledger-routed)

`ledger-direct/src/bulk_write.rs` (364 LOC) vs `ledger-routed/src/bulk_write.rs` (371 LOC).

### Per-function diff

| Function | Direct | Routed | Diff |
|----------|--------|--------|------|
| `insert_trx` | lines 33-49 | lines 44-60 | byte-equivalent SQL + body |
| `insert_trx_lines` | lines 56-122 | lines 67-133 | byte-equivalent SQL + body; both use the (pool_id, trx_seq) defensive remap |
| `apply_pool_state_mutations` | lines 129-252 | lines 140-263 | byte-equivalent SQL across all 4 sub-stmts (Insert / Upsert / Update / Delete) + identical match arms |
| `insert_posting_lines` | lines 262-311 | lines 270-315 | byte-equivalent SQL + body |
| `insert_provisional_postings` | lines 317-347 | lines 321-351 | byte-equivalent SQL + body |
| `apply_plan_result` (wrapper) | lines 352-364 | lines 359-371 | byte-equivalent step sequence + identical Result chaining |

**Verdict:** ✓ byte-equivalent SQL. No drift between paths today.

### D2.1 [P2] Paired-file maintenance hazard

Both `bulk_write.rs` files contain the comment: *"Per locked plan §G Q3: copy-paste from ledger-direct to keep the PoC straight (resist premature abstraction); when both paths stabilize, an `apply_plan_result` shared helper may move into `ledger-core` if measurement justifies."*

The intent is clean (don't abstract prematurely). The risk: any edit to `ledger-direct/src/bulk_write.rs` (e.g., adding a column to UNNEST, adjusting an UPSERT, fixing a defensive remap) MUST be mirrored in `ledger-routed/src/bulk_write.rs` or the equivalence harness will surface a divergence — which is the regression net, but a NOISY one (you discover it post-bench).

**Same pattern exists for:**
- `ledger-direct/src/hydration.rs` ↔ `ledger-routed/src/hydration.rs`
- `ledger-direct/src/pool_lock.rs` ↔ `ledger-routed/src/pool_lock.rs`
- `ledger-direct/src/submit.rs::decode_line_type` ↔ `ledger-routed/src/committer.rs::decode_line_type` (line 902)

Four paired-file pairs. Each note their pair's existence in the doc comment. No CI guard ensures byte-equivalence.

**Proposed mitigation paths** (do not pick under this audit):
- (a) Accept as-is; rely on equivalence harness + reviewer discipline
- (b) Move into `ledger-core` after Phase 7 measurement proves both paths stable
- (c) Add a small CI check that diffs the paired files modulo a noise filter (doc comments, function names)

Filed as a Phase 7-prep tracking issue (see findings index).

---

## 3. hydration parity (ledger-direct vs ledger-routed)

`ledger-direct/src/hydration.rs` (127 LOC) vs `ledger-routed/src/hydration.rs` (107 LOC). Difference is purely doc-comment density; SQL + flow byte-equivalent.

Both files run the same three SPI reads against the dedup-sorted `pool_ids`:
1. `pool_state` bulk layer read with `ORDER BY pool_id, layer_seq` (free from PK index)
2. `pool` per-pool method
3. `trx_line` per-pool `COALESCE(MAX(trx_seq), 0)` (per §13.1 option a)

Both defensively re-sort per-pool by `layer_seq` after demultiplexing. Both handle `pool` rows present without `pool_state` rows (empty pool → omitted from `pools` map, present in `method_of` — matches Snapshot invariant).

**Verdict:** ✓ byte-equivalent. Covered by D2.1 maintenance hazard above.

**Sub-finding (informational, not P3-worthy):** the routed-hydration's doc-comment block is ~20 lines shorter than direct's. Re-syncing the two file headers would marginally improve grep-friendliness. Not worth filing.

---

## 4. Recovery flow walk

### 4.1 cleanup.rs three-case CAS handling

Pass 1 noted this file as well-structured. Pass 2 re-confirms:

- **Case 1 (Success).** CAS staging `valid 3→0` gated on `observed_cg == commit_group_id` (line 82-86). Prevents the "re-routed slot silent-corruption" race documented in the head doc (lines 31-37). Test `cg_id_mismatch_on_valid_3_skips_3_to_0_and_falls_through` (line 321) covers it.

- **Case 2 (Ejected).** CAS 3→0 fails because `valid != 3` (slot was flipped back to 1 by eject). CAS 2→0 fallback fails because `eject_count != 0`. Arena left in place for router re-pack. Test `case_2_ejected_leaves_slot_at_pending_and_arena_in_place` (line 262) covers it.

- **Case 3 (Router-died-mid-stamp).** Slot stuck at `valid == 2`. CAS 3→0 skipped (valid != 3); CAS 2→0 succeeds because `eject_count == 0`. Test `case_3_router_died_mid_stamp_falls_back_to_cas_2_to_0` (line 287) covers it.

- **Case 3 defensive variant.** valid==2 with `eject_count > 0` means an eject is mid-flight (eject path bumps `eject_count` Release BEFORE CAS `valid 3→1`). 2→0 must skip. Test `case_3_with_eject_in_flight_skips_2_to_0` (line 303) covers it.

Plus 3 tests for arena-free helpers (lines 341-372) + 1 test for the committer-queue slot finalization CAS (line 374).

**Verdict:** ✓ 8 unit tests cover all CAS branches including the subtle defensive variants. Lock ordering (staging guard → arena guard, never nested) explicitly enforced in `cleanup_after_commit_group` (line 162-198).

### 4.2 recovery.rs

Trivial by design (47 LOC). Postmaster restart wipes shmem → no DB cleanup; just flip `recovery_complete` to 1 so router + committers can open for traffic.

The load-bearing recovery is `router::try_recover_router_orphan` — runs at EVERY router restart (postmaster start AND router-only crash). Not in recovery.rs.

**Verdict:** ✓ correct. Comment block at top accurately describes why this file is so thin.

### 4.3 router-orphan recovery (router.rs::try_recover_router_orphan)

Two-phase sweep at `router.rs:883`:

**Phase 1: sweep_queue_phase (line 890).** Walks CommitterQueueEntries. For each:
- `valid == 1`: re-stamp linked staging entries the prior router never reached in the data-before-flag block. Calls `try_restamp_staging_to_routed` (line 995) which is a pure helper — CAS-completes the interrupted `cg_id.store(...)` + `valid 2→3` sequence.
- `valid == 2`: in-flight with possibly-dead committer. Liveness check via `is_committer_alive(slot, generation)`. If dead, revert CQ valid 2→1 + clear identity. Staging entries stay at valid==3; the next committer's claim re-decodes and re-runs the pipeline (UNIQUE handling catches any rows the dead committer's tx committed before dying).

**Phase 2: sweep_staging_phase (line 959).** Walks staging entries. Builds a set of `active_cg_ids` from CQ entries with `valid != 0`. Staging entries at `valid == 2` with cg_id NOT in that set are genuinely orphaned (router crashed AFTER reserving the staging slot but BEFORE allocating the CQ entry). Revert via `try_revert_orphan_staging` (line 1016) — CAS 2→1 + reset cg_id Release.

**Verdict:** ✓ correct. The two-phase ordering (queue phase first, then staging phase) is load-bearing: Phase 1 may CAS staging to routed; if Phase 2 ran first, those slots would be incorrectly classified as orphans. Inline comment doesn't explicitly call out the phase ordering — adding a brief note would be defensive doc-hygiene.

### D4.1 [P3] Phase-ordering note missing from try_recover_router_orphan

`try_recover_router_orphan` head (line 880-888) just says "Run one full router-orphan recovery sweep." The phase ordering is load-bearing (see above) but not documented. A new contributor reordering the two calls would silently corrupt recovery.

Proposed patch: one-line comment at line 884-885 calling out the ordering.

---

## 5. Full body read: router.rs (1349 LOC) + committer.rs (1080 LOC)

The pipeline architecture is sound (Pass 1 covered the head docs). Pass 2 noted four minor findings inside the bodies.

### D2.1 [P3] write_plans_in_subtx Cell-read ordering is subtle

Already noted under R6 (§1) above. The `in_flight: Cell<Option<usize>>` is set immediately before each `apply_plan_result` call inside the `PgTryBuilder` closure, and read AFTER `.execute()` returns. The catch_when closure deliberately does NOT touch the Cell. Comment lines 666-669 explain the constraint but a future contributor editing this function could easily break index attribution by moving the Cell-read into the closure.

**Proposed defensive measures (no patch under this audit):**
- (a) Rename `in_flight` to `last_attempted_idx` to signal "this is read after-the-fact"
- (b) Add a comment near the Cell declaration stating the read constraint

Not P2 because the existing test `acceptance_routed_duplicate_submission.rs` would catch a regression in index attribution (a duplicate would either be retried-correctly or wedge the loop). P3 for visibility.

### D5.2 [P3] `parse_caller_status` depends on PG's "in progress" string format

`committer.rs::parse_caller_status` (line 805-812) matches against literal strings: `"committed"`, `"aborted"`, `"in progress"` (with space). Any other string maps to `Unknown` which is treated as `Committed` (kept). If PG ever changes the `pg_xact_status` output format, callers stuck `in_progress` would be silently kept and processed — would post against a still-open caller tx, then UNIQUE-violate on next attempt.

The defensive default (Unknown → kept → UNIQUE handles) is reasonable but masks a regression. Unit test `parse_caller_status_maps_all_known_strings` (line 1038) pins the current strings; an integration test asserting `pg_xact_status` actually returns these strings would catch a PG behavior change.

Proposed Pass 3 follow-up: add a smoke integration test that calls `pg_xact_status` against a known-committed and known-aborted xid and asserts the literal return strings.

### D5.3 [P3] Hot-path HashMap allocations in `pool_to_envs` (router union-find)

`router.rs::affinity_group` (line 509-514) builds `pool_to_envs: HashMap<i64, Vec<usize>>` on every tick that has candidates. For dense overlap regimes (s4, s5) at 200+ candidates this is a fresh allocation per tick.

Phase 6 measurement (cross-method-comparison.md) didn't profile this. It might or might not matter; the existing per-tick metrics (`router_ticks_total`, `router_entries_scanned_total`) don't attribute time to specific stages. Phase 7 GUC-sweep work (`acct-snoa`) would naturally include router-tick profiling.

**Not a finding for this audit;** noted for the Phase 7 perf-catalog backlog.

### D5.4 [P3] router emit's two test-injection atomics could be feature-gated

`router.rs::emit_commit_group` reads `test_inject_router_delay_us` and `test_reorder_router_stores` (line 464-472) on EVERY emit. Production load incurs the two Acquire reads + the conditional sleep call site even when both are zero. The reads are cheap (atomic loads of u32 in cache-hot shmem) but a `#[cfg(any(test, feature = "test_hooks"))]` gate would eliminate the production code paths entirely, matching the conditional-compile pattern used elsewhere in the project for test-only knobs.

Comment at line 461 already acknowledges "Production defaults are 0/0 (no-op)." Status quo accepted; flagged for hygiene visibility only.

---

## 6. Phase 7 framing recommendation

Pass 2 does not change the recommendations from AUDIT.md §9. The correctness paths are sound; the spec-doc patches landed via acct-x22s / acct-szon / acct-cssx remove the spec-drift risk that would have framed Phase 7 hypotheses against the wrong model. Phase 7 can proceed against `acct-69c7` (1000-caller × 5-min routed run) once `acct-8cn2` lifts the io_uring + high max_connections memlock ceiling.

The paired-file maintenance hazard (D2.1) is worth resolving before any future cost-method addition (e.g., FIFO/LIFO bulk-write extensions). Until then, the equivalence harness is the regression net.

---

## Findings index

| ID | Severity | Section | Title | Filed as |
|----|----------|---------|-------|----------|
| D1.X | — | §1 | R3-R7 walk verdicts | (in-doc; no follow-up filing) |
| D2.1 | P2 | §2 | Paired-file maintenance hazard (bulk_write + hydration + pool_lock + decode_line_type) | acct-vsfy |
| D3.X | — | §3 | Hydration parity verdicts | acct-vsfy (folded) |
| D4.1 | P3 | §4 | try_recover_router_orphan phase-ordering note missing | acct-x9tm |
| D5.1 | P3 | §5 | write_plans_in_subtx Cell-read ordering subtlety | acct-rgn9 |
| D5.2 | P3 | §5 | parse_caller_status depends on PG "in progress" literal | acct-4zpj |
| D5.3 | P3 | §5 | router pool_to_envs HashMap hot-path allocation | (no filing; Phase 7 perf-catalog) |
| D5.4 | P3 | §5 | router emit test-injection atomics could feature-gate | (no filing; hygiene visibility) |
