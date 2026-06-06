# AUDIT-PASS3 — ledger-v3.1: docs↔code, code quality, test coverage (suites run), complexity disposition

Pass 3 in the `AUDIT.md` (Pass 1) → `AUDIT-PASS2.md` (Pass 2) lineage. Four dimensions: **(A)**
docs↔code gaps (spec claim ledger + results-doc reconciliation + provenance + undocumented
surfaces), **(B)** code quality (per-module structural walk, both halves), **(C)** test coverage
(map + gap classification + a one-time suite run), **(D)** unnecessary complexity (per-item
disposition). Tracked as epic **acct-mvq4**; one child per section below, findings filed as
additional epic children as they emerge.

> **Status.** Assess-only, point-in-time (started 2026-06-05). Pass-3 **re-verifies** Pass-1/
> Pass-2 findings (registry §0.5) rather than inheriting them; the prior passes' prose is not
> corrected in place (their own convention). Sections A–D land incrementally, one commit per
> child issue.

**Severity** P1/P2/P3 as in prior passes. **Finding ids**: `A#` docs↔code, `Q#` quality,
`C#` coverage, `X#` complexity (no collision with Pass-1 `Dx.y` / Pass-2 `P2.x`).
**Claim-row verdicts**: CONFIRMED / DIVERGENT→`A#` / STALE-doc→`A#` / UNDOCUMENTED→`A#` /
N-A-deferred. **Complexity verdicts**: REMOVE / QUARANTINE / KEEP+document / PROMOTE /
DEFER-to-`<issue>`.

---

## 0. Instrument & inventories (acct-mvq4.1)

### 0.1 bd dedup map — live-verified 2026-06-05

Findings that overlap an OPEN issue are **annotated on that issue, not re-filed**. Two
overlaps below (acct-ozln, acct-uwsp) were found by the live sweep and were NOT in the
planning-time map — exactly why this map is rebuilt from live `bd` state.

| Issue | Live state | Overlap surface | Audit action |
|---|---|---|---|
| `acct-vsfy` | OPEN P2 | paired-file direct↔routed copy-paste hazard (bulk_write/hydration/pool_lock/decode_line_type); partially superseded by the `ledger-spi-common` extraction (acct-yojk.2 — `decode_line_type` now single-sourced in `line_type.rs`) | .3 reconciles: recommend close-as-superseded or pin the genuine residual |
| `acct-xdkd` | OPEN P3 | bench restore-traps restoring STALE defaults (50/off) — 5 scripts named in the issue | .3 routes class-(a) script findings here |
| `acct-x9bg` | OPEN P3 | harness fixed 30 s `drain_deadline` end-of-drain tail | .3/.5 route drain-tail findings here |
| `acct-ozln` | OPEN P2 | **router `collect_candidates` share-lock O(ring) scan** (staging-side cursor; sibling of acct-m4g5.2) | .4 router walk cross-refs; do not re-file |
| `acct-m4g5` | IN_PROGRESS P2 | affinity evaluation epic (other work stream) | .7 records a disposition recommendation ONLY |
| `acct-m4g5.1` | IN_PROGRESS P2 | cc=2 vs cc=1 disjoint baseline precondition | input only |
| `acct-m4g5.2` | OPEN P2 [BUG] | **committer claim-loop bug: per-committer cursor; O(ring) scan under shared LWLock** | .4 committer walk cross-refs; do not re-file |
| `acct-m4g5.3` / `.4` | OPEN P2 | affinity split test / full sweep + keep-kill verdict | .7 DEFER target for affinity disposition |
| `acct-czz4.4` | OPEN P2 | batch_size_max default synthesis → POC-REPORT | .2/.7 cross-ref batch-sizing notes |
| `acct-jdgm` | OPEN P3 | commit-group sizing: bsm default + cc=1 ingress ceiling | .7 cross-ref |
| `acct-uwsp` | OPEN P2 | **cross-method-mixed equivalence harness extension** (known coverage gap) | .5 coverage map cross-refs; do not re-file |
| `acct-hh7b` | IN_PROGRESS P2, **HELD** at Phase 1→2 | window-only formation; its results docs are audit INPUTS | DO NOT advance/modify |
| `acct-235v` | **CLOSED** | window-sweep dial characterization | history only (planning map mislabeled it held — corrected) |
| `acct-ytd9` | OPEN P0 | `ledger-v3-PAUSE` — sibling stream's pause gate | out of scope (v3, not v3.1) |

### 0.2 Surface enumerations

**`#[pg_extern]` = 55 total** (53 routed-c + 2 direct-c):

| File | Count | Names |
|---|--:|---|
| `ledger-direct-c/src/lib.rs` | 1 | `ledger_direct_c_hello` |
| `ledger-direct-c/src/submit.rs` | 1 | `ledger_submit_trx_c` |
| `ledger-routed-c/src/enqueue.rs` | 4 | `ledger_enqueue_trx_c`, `ledger_enqueue_trx_batch_c`, `ledger_routed_c_staging_state_counts`, `ledger_routed_c_staging_request_seq_max` |
| `ledger-routed-c/src/committer.rs` | 1 | `ledger_routed_c_bench_apply` (feature-gated, committer.rs:855 — exact feature verified at .7) |
| `ledger-routed-c/src/router.rs` | 12 | ungated observability: `..._committer_queue_state_counts`, `..._ready_commit_groups`; **test_hooks-gated ×10**: `..._test_set_bgworker_paused`, `..._test_set_committer_paused`, `..._test_router_pid`, `..._test_set_committer_stall_us`, `..._test_committer_stall_hits`, `..._test_set_inject_deadlock_count`, `..._test_set_inject_fatal`, `..._test_set_inject_unique`, `..._test_run_router_recovery_sweep`, `..._test_inject_orphan_cq` |
| `ledger-routed-c/src/lib.rs` | 36 | arena ×5 (`..._arena_{total_allocs,total_frees,outstanding,bump_offset,freelist_count}`); committer counters ×13 (`..._committer_{drains,pool_lock_acquisitions,aggregate_upserts,trx_committed,dedup_skips,dropped_submissions,duplicate_redrives,tx_failures,poisoned,deadlock_retries}_total`, `..._committer_takeover_count`, `..._affinity_{owned_claims,steals}_total`); `..._recovery_complete`; router counters ×7 (`..._router_{ticks,commit_group_count,total_submissions,entries_scanned,window_defers}_total`-family + `..._router_max_group_size`, `..._router_submission_histogram`); pipeline span counters ×9 (`..._committer_{pipeline_ns,pipeline_count,pool_lock_ns,hydrate_ns,apply_ns,txn_ns,decode_ns,xact_ns,dedup_ns}_total`); `ledger_routed_c_hello` |

**GUCs = 15** (lib.rs:130–280; 13 int + 1 bool + 1 string — planning-time "16" double-counted
`target_database`):
`staging_queue_size` (16384, Postmaster), `committer_queue_size` (2048, Postmaster),
`spillover_arena_mb` (128, Postmaster), `queue_full_timeout_ms` (5000), `router_window_size`
(1000), `batch_size_max` (**200**, acct-p1al flip), `batch_window_us` (500), `max_eject_count`
(10000), `caller_tx_timeout_ms` (30000), `committer_count` (4, read at init), `eject_cooldown_ms`
(10), `affinity_scheme` (0 = off, acct-0usf), `affinity_steal_ms` (5, acct-0usf),
`router_pack_disjoint` (**true**, acct-p1al flip), `target_database` ("poc_v3_1", Postmaster).
Defaults re-verified against spec text at .2 (the §6.3 "default 50" staleness is a
pre-confirmed claim-row target).

**Scenarios = 21**: `s1`–`s21` (scenarios.rs:72–92 dispatch; specs from :128). S1–S4 baseline
(uniform/zipf × simple/complex × wac/mixed), S5–S9 FIFO deep-pool family (single-hot /
disjoint / zipf-deep / partial-deplete / multi-touch), S10–S21 Pareto receipts/builds/mixed
family. Per-scenario params land in the .5 coverage map + .7 usage map.

**CLI surface** (`ledger-harness/src/cli.rs`): global `--dsn`; `seed-pools` {`--count`,
`--skus`, `--locations`, `--method-mix`, `--depth`}; `run` {`--scenario`, `--mode`,
`--duration`, `--output`, `--no-sampler`, `--max-callers`, `--batch-size`, `--target-rate`,
`--depth`, `--method-mix`, `--seed-count`, `--seed-skus`, `--seed-locations`, `--seed-depth`,
`--multi-touch-pct`, `--touch-dist`, `--pareto-hot-pool-pct`, `--pareto-hot-traffic-pct`};
`equivalence` {`--scenario`, `--submissions-per-caller`, `--callers`, `--method-mix`,
`--depth`}. Usage map at .7.

### 0.3 Canonical-invocation matrix (4-way)

`scripts/run-tests.sh` covers ONLY the acceptance/property binaries of the two `-c` crates
(verified by reading: `set -uo pipefail`; `WITH_TEST_HOOKS=1` install per path; discovers
`tests/{acceptance,property}_*.rs`; `docker restart` + `RESTART_WAIT`(5 s) per binary;
`cargo test --features pg18,test_hooks --no-default-features --test <bin> -- --ignored
--test-threads=1 --nocapture`; `FAIL_FAST` default 1). Full coverage needs four classes:

| # | Class | What runs | Command (from `poc/ledger-v3.1/`) | Needs cluster? |
|---|---|---|---|---|
| a | Pure units | ledger-core 38 `#[test]` (10+17 tests/, 11 numeric.rs) + spi-common 2 | `cargo test -p ledger-core -p ledger-spi-common` | no |
| b | Integration (`--ignored`) | direct-c 3 binaries (14 `#[ignore]`, `#[tokio::test]`-style) + routed-c 5 binaries (24 `#[ignore]`) | `bash scripts/run-tests.sh --path both` | YES — installs test_hooks `.so`, restarts per binary |
| c | `#[pg_test]` hellos | direct-c ×1, routed-c ×2 | `cargo pgrx test pg18` in each `-c` crate | pgrx-managed instance (not the shared cluster) |
| d | Harness smokes | `smoke_measure.rs`, `smoke_sampler.rs` (both `#[ignore]`, DSN `localhost:5111/poc_v3_1`) | `cargo test -p ledger-harness -- --ignored` | YES — live seeded `poc_v3_1` + `ledger_direct_c` installed |

### 0.4 Artifact census (live, 2026-06-05)

- `results/`: **1,129 files** — 791 `.json`, 56 `.csv`, 13 `.md`, remainder logs/samples/
  snapshots/scratch (≈346 dot-prefixed scratch files invisible to plain `ls`).
- Git-tracked: **69**. A retention policy **already exists** — `results/.gitignore` ignores
  `*.json`, `*.sampler.txt`, `*.log` (header: "Keep the directory; ignore the generated JSON /
  sampler dumps") — the planning-time claim "no policy beyond /target" was wrong.
- **7 strays are policy violations, not policy absence**: deliverables that SHOULD be tracked
  per the policy but are uncommitted (`sustained_s2_cc4_c16_p64.md`, `sustained_s2_cc4_c16_p64.csv`,
  `batch_window_sweep.csv`) + scratch shapes the policy doesn't cover (`.sustained_snap0`,
  `.sustained_snap1`, `sustained_s2.runlog`, `sustained_s2_rate.samples`). Disposition at .7.
- Workspace `.gitignore` = `/target` only; repo-root `.gitignore` does not mention v3.1.

### 0.5 Prior-finding registry (re-verification targets for .2/.3/.4)

- **Pass-1 (`AUDIT.md`), 16 findings**: D1.1 JSONB-vs-ARRAY · D1.2 variance_account gap-fill ·
  D1.3 coalesce_aggregates unspec'd · D1.4 harness distinct-pools · D1.5 standard_cost seeding ·
  D2.1 (R-framework fit) · D3.1 no qty≥0 CHECK · D3.2 specific K=1 caller contract · D4.1 stale
  Path-B/tm09 provenance (P2) · D4.2 greenfield comments · D5.1 triplicated
  pool_lock/hydration/bulk_write (P2) · D5.2 terminology · D6.1 no routed property test (P2,
  fix-needed) · D6.2 no shared invariants harness · D7.1 dead `committer_lease_ms` GUC ·
  D8.1 13 dead shmem fields (P2).
- **Pass-2 (`AUDIT-PASS2.md`), 6 findings**: P2.1 UNIQUE-survivor poison granularity · P2.2
  `parse_caller_status` fragility · P2.3 test-injection reads on production path · P2.4
  per-submission `snapshot.clone()` · P2.5 (harness drain-wait heuristic) · P2.6.
- **Resolution state to verify against**: epic `acct-yojk` 15/15 closed (per the PASS2
  reconciliation header: all six P2.x + the §2 triplication resolved; poison granularity
  changed by yojk.9; clone measured-and-kept by yojk.12; provenance cleanup yojk.1;
  spi-common extraction yojk.2; payload.rs coverage yojk.5; arena lines-block free yojk.15).

### 0.6 Instrument corrections (pinned so later children don't inherit stale claims)

1. `cleanup.rs:240-242/264/297` `expect()`s sit in `#[cfg(test)]` unit helpers — NOT
   production paths. The .3/.4 panic tables record them as non-findings.
2. `identity_slot_for_committer_pid` no longer exists (zero grep hits) — dead-code sweeps
   search by behavior, not by exploration-era names.
3. GUC count is **15**, not 16.
4. A results-retention policy already exists (`results/.gitignore`); the .7 verdict refines
   it rather than inventing one.
5. `acct-235v` is CLOSED (planning map said held).
6. direct-c integration tests and harness smokes are `#[tokio::test]`-style (plain
   `grep '#\[test\]'` counts them as 0); routed-c integration tests are plain `#[test]`.
   Exact per-binary counts land in the .5 map.

---

## A. docs ↔ code (acct-mvq4.2 — 2026-06-05)

**Method.** One verdict row per heading of `poc/design_research/design-v3.1.md` (68 headings),
each verified against live code/migrations (not against prior-pass prose); all 13 `results/*.md`
reconciled headline↔backing-artifact with numeric spot-checks; the Pass-1 D4.1 provenance grep
re-run; every §0.2-enumerated surface checked for doc coverage. Findings A1–A13 in §A.5;
duplicates routed per the §0.1 map (A2 cross-refs `acct-czz4.4`/`acct-jdgm`, no re-files).

### A.1 Spec claim ledger

| § | Heading | Verdict | Evidence / notes |
|---|---|---|---|
| 1 | Purpose | CONFIRMED | Hot-path-only PoC; provisional FIFO/LIFO; both flavors shipped; recalc/close absent (§13 rows). |
| 2 | Schema (intro) | CONFIRMED | Greenfield; 8 migrations, no migration-from-existing scaffolding. |
| 2.1 | Enums | CONFIRMED | `0001` byte-equivalent to spec (7 enums); `cost_adjustment` absent as specified. |
| 2.2 | Cost-ledger tables | CONFIRMED w/ A3 | `0003`+`0005` match pool/standard_cost/pool_state/pool_lock/trx/trx_line + indexes exactly. §2.2's `pool_state` SQL block omits `value_sum` (+ its CHECK, `0007`) that §3.0 prose carries → **A3**. |
| 2.3 | Journal-side tables | CONFIRMED | `0004`+`0005` exact (account, posting_line, posting_line_dimension + 3 indexes). |
| 2.4 | Period/reference | CONFIRMED | `0002` exact; ID-allocation note (identity vs application-managed) matches shipped DDL. |
| 3 | Method semantics (intro) | CONFIRMED | Strict WAC/STD/specific, provisional FIFO/LIFO — `provisional.rs` dispatch. |
| 3.0 | Numeric repr + rounding | CONFIRMED w/ A4 | `banker_div` verbatim (numeric.rs:21–45); `MICRO_UNITS_PER_CURRENCY_UNIT=1e6`; value_sum model implemented (0007 + wac.rs). `wac.rs:4-6` module header asserts the PRE-value_sum model → **A4**. |
| 3.1 | WAC | CONFIRMED | wac.rs: exact i128 value_sum accumulation, banker_div derivation, qty=0 guard, InsufficientInventory, depletion subtracts posted amount, empty-pool residual zero-clear. |
| 3.2 | FIFO and LIFO | CONFIRMED | Strict layer math absent from Path C; fifo.rs/lifo.rs are MethodMismatch stubs. |
| 3.3 | STD | CONFIRMED | standard.rs: C_std recording, variance leg w/ favorable flip + MissingVarianceAccount, aggregate row mirrors C_std, InsufficientInventory on depletion (standard.rs:161). |
| 3.4 | Specific-id | CONFIRMED w/ A7 | Strict K=1; `source_trx_line_id = layer_id` link; DeleteLayer on consume. Code is now STRONGER than spec: `SpecificPoolOccupied` (specific.rs:56) rejects double-receipt — spec says unenforced/UB → **A7**. Pass-1 D3.2 residual re-verified still-true: qty=1 half of the contract stays caller-side (partial deplete of an oversized layer deletes the layer but leaves aggregate qty > 0). |
| 3.5 | Provisional mode | CONFIRMED | provisional.rs: receipts = WAC aggregate update; depletion basis dispatch (running_avg ↔ standard); NULL source_trx_line_id; aggregate-only. |
| 3.6 | Negative inventory | CONFIRMED | Deferred as specified; `0006` CHECK is the §15-documented backstop; no `allow_negative` anywhere. |
| 3.7 | Posting-account resolution | CONFIRMED | `0008` exact (14 account cols + nullable variance_acct); receipt-direction storage + depletion swap (snapshot.rs pair_for + callers); line_type→operation map exact; MissingPostingAccounts fail-loud (lazily at line processing — A13 note). |
| 4 | SPI surface | CONFIRMED w/ A6 | Both entry points exist; JSONB lines divergence annotated in-spec (D1.1 ✓ re-verified). `trx_type`/`posted_at` ship as TEXT (RFC3339), not enum/TIMESTAMPTZ — annotation covers only `lines` → **A6**. |
| 5 | Direct flavor (intro) | CONFIRMED | Synchronous in caller's tx. |
| 5.1 | Function logic | CONFIRMED w/ A5, A13 | submit.rs + spi-common implement steps 1–9. Optimistic pool_lock pattern exact (pool_lock.rs). Deltas: hydrate splits the step-3 LEFT JOIN into routing+aggregate SELECTs; missing standard_cost raises at use, not hydration (→ A13). Step-7 parenthetical "no inventory for STD" contradicts §3.3 + code → **A5**. |
| 5.2 | Lock-hold properties | CONFIRMED | Aggregate-only hydration/write for FIFO/LIFO (hydration.rs step 2); validated empirically (POC-REPORT (a)). |
| 5.3 | Per-trx SPI count | CONFIRMED-approx w/ A13 | Counts shift: +1 (split hydrate) +1 (posting_account_map seek, documented in §3.7 but absent from this list). Order-of-magnitude claims hold. |
| 5.4 | Failure handling | CONFIRMED | ereport!(ERROR) on every LedgerError / arg failure; caller's tx aborts. |
| 5.5 | Caller-side batching | CONFIRMED | direct-batched mode shipped (cli `--batch-size`, default 50 ✓ §10.0); measured §11.4. (POC-REPORT (b): under cross-caller overlap it deadlocks — a measured result, not a divergence.) |
| 6 | Routed flavor (intro) | CONFIRMED | shmem staging + router + committer pool shipped. |
| 6.1 | Submission | CONFIRMED w/ A10 | No DB write at enqueue ✓ (enqueue.rs); returns shmem request_seq ✓; harness polls trx by (trx_type, source_id) ✓ (driver_routed observer). Backpressure (CV-wait `queue_full_timeout_ms` → ERRCODE_INSUFFICIENT_RESOURCES) undocumented in spec → **A10**. |
| 6.2 | Shmem layout | CONFIRMED w/ A8 | 4 regions ✓. Staging states 0–3 match; state 4 = `abandoned`, not spec's (self-unused) `in_flight` → **A8**. CQ states 0–3 match (`done`→`completed` naming); `poisoned`=4 is the §6.8-sanctioned extension ✓. Identity registry: per-slot u32 `generation` (PID-recycling-safe) vs spec's global monotonic u64 token — same guarantee, different shape → A8. `correlation_id` field beyond spec's sketch. |
| 6.3 | Router | DIVERGENT → **A2** | Live: scan on 50 ms latch tick (router.rs:107); `batch_window_us` = oldest-candidate-age coalesce gate (router.rs:152–170), NOT the scan cadence; `batch_size_max` default **200** (spec: "default 50"); `pack_disjoint_components` pass (acct-xdwk/p1al, default ON) absent from spec. Window-read / eject-cooldown skip / union-find / chunking / CAS sequence all CONFIRMED. "Affinity grouping…same committer" holds in the one-group-one-claimer sense (the experimental acct-0usf committer-affinity is a different, default-off mechanism). |
| 6.4 | Committer | CONFIRMED w/ A9 | Steps 1–12 implemented (committer.rs). pg_xact triage exact incl. NULL→keep-optimistic and Unrecognized→WARN+eject (yojk.10 annotation honored, classify_and_eject). Pre-flight dedup ✓; submission_id-order processing within group ✓; drop-and-continue ✓; §6.7 collapse ✓; cleanup transitions ✓. Step-2 framing: BGWorker top-level tx + nested write subtx (not literal per-retry new top-level tx) → A9. Eject terminal budget (`max_eject_count`/`caller_tx_timeout_ms`) implemented; spec alludes only to "the caller-tx eject timeout" → A11. |
| 6.5 | Recovery | CONFIRMED w/ A8 | Router boot sweep ✓ (`try_recover_router_orphan`); committer-death reclaim via generation match + kill(pid,0) ✓; postmaster crash = shmem evaporates ✓. Implementation adds a trivial dedicated recovery BGWorker owning `recovery_complete` (recovery.rs) — beyond spec, benign → A8 note. |
| 6.6 | Per-commit_group SPI count | CONFIRMED-approx w/ A13 | Same deltas as §5.3 (split hydrate + posting_account_map seek + kept-plan prep). Amortization claim confirmed empirically (POC-REPORT (c)). |
| 6.7 | Batching benefit | CONFIRMED | One aggregate UPSERT per pool per group (post-pass snapshot reconstruction, Pass-2 §4.4); measured: 63 896 trx / 2 078 locks (s5). |
| 6.8 | SQL error handling | CONFIRMED w/ A9 | 40P01/40001 → Retryable, backoff 10 ms·2^n cap 1 s, ≤5 retries ✓ (retry_backoff, MAX_DEADLOCK_RETRIES); DuplicateRace re-drive ✓ (yojk.9 annotation honored); poison = terminal CQ state 4 + counter ✓. Deltas → A9: retry re-attempts the subtx write phase only (no repeat pg_xact/dedup; re-dedup deferred to the DuplicateRace path — safety equivalent via the UNIQUE backstop); spec's transient classes "lock-wait timeout" / "connection drop" are not classified Retryable (55P03 would poison; lock_timeout unset; in-process SPI has no connection to drop). |
| 7 | Recalc/close (deferred) | CONFIRMED | Nothing implemented; no cost_adjustment enum values, no watermarks, no settled-state columns (verified in 0001–0008). |
| 8 | ledger-core | CONFIRMED | Layout matches with two notes: `std.rs` shipped as `standard.rs` (avoids `mod std` shadowing — documented in-file), `lib.rs` unmentioned; "plan_apply trait" is a function. Stubs exactly as specified; both entry points exported. |
| 9 | Testing strategy (intro) | CONFIRMED | Three layers exist (unit / integration / comparison). |
| 9.1 | ledger-core unit tests | CONFIRMED | numeric.rs 11 tests cover every listed banker_div case (incl. i128-limits, downcast-panic, WAC-overflow regression); plan_apply_strict.rs (17) + plan_apply_provisional.rs (10) cover every listed strict/provisional bullet, + extras (favorable-variance flip, equal-cost-no-leg, coalesce ×2, lifo-identical). |
| 9.2 | direct-c integration tests | CONFIRMED | All 10 bullets map to shipped tests (acceptance_direct_methods + acceptance_direct_lock_and_concurrency, 14 `#[ignore]`); the §9.2 "primary measurement" bullet is the harness lockhold sweep (POC-REPORT (a)), with `deep_pool_depletion_touches_only_aggregate` + `concurrent_depletions_serialize_without_lost_updates` as the in-suite probes. |
| 9.3 | routed-c integration tests | CONFIRMED w/ note | Bullets 1/2/4/5/7 map directly (5 binaries, 24 `#[ignore]`). Committer-death (3) covered via synthetic orphan-CQ injection + takeover, not a literal worker kill; postmaster-crash (6) covered indirectly (recovery_complete boot test + per-binary docker restart), not a literal crash test → feeds the .5 gap map. |
| 9.4 | Direct vs routed comparison | CONFIRMED | equivalence subcommand + `two_lines_same_pool_one_submission_agrees_with_direct` + property test; §11.1 measured PASS (POC-REPORT (d)). |
| 10 | Workloads (intro) | CONFIRMED | 3 modes + routed = the 4 comparable configurations. |
| 10.0 | Submission modes | CONFIRMED | direct-per-call / direct-batched (default 50 ✓ cli.rs:82) / routed. |
| 10.1 | Caller concurrency | CONFIRMED w/ note | Shipped tiers are 10/50/200/1000; spec's named buckets (1-4/16-64/256-1024) don't align with the 10- and 200-caller scenarios labeled light/heavy in §10.6. Cosmetic — every results table states actual caller counts. |
| 10.2 | Pool overlap | CONFIRMED | Uniform / Zipf / Disjoint / Pareto all shipped (workload.rs). "Pathological 1000-on-one-pool" realized as Zipf{exp:100} (>99 % on pool 0) — approximation noted in scenarios.rs. |
| 10.3 | Trx complexity | CONFIRMED w/ note | Simple (1 line) + Complex (10–20) used; `Complexity::Medium` (2–5) implemented but `#[allow(dead_code)]` — no scenario uses it (→ .7 disposition). |
| 10.4 | Method mix | CONFIRMED | AllFifo/AllWac/AllStd/Mixed(50/30/20 deterministic) + extra AllLifo/AllSpecific; POC-REPORT caveat documents lifo/specific being outside the canonical measured set. |
| 10.5 | Pool depth + seeding | CONFIRMED | seed.rs bulk-inserts layer rows AND matching trx_line receipts per layer (stream-consistent, layer_id = trx_line.id) — exactly the specified mechanism. |
| 10.6 | Workload matrix | CONFIRMED | All 21 scenarios match their §10.6 descriptions per-scenario (S9 multi-touch preset 40 %/1:60,2:30,3:10 ✓; S10–S21 families/variants ✓ incl. depths); S1–S4 caller counts per the §10.1 note. |
| 11 | Success criteria (intro) | CONFIRMED | — |
| 11.1 | Correctness | CONFIRMED w/ A1 x-ref | qty equivalence measured PASS (POC-REPORT (d)); receipts-only unit_cost exactness asserted by property_ledger_enqueue_trx_c (banker_div(Σq·c, Σq) check, :453). The §-text's "identical qty" guarantee inherits A1's caveat: under drop-and-continue, cross-chunk reordering (cc>1) can change the failure set in oversell scenarios — unobserved in all measurements, architecturally possible. |
| 11.2 | Direct demonstration | CONFIRMED | Depths 10/100/1000 flat across every percentile (POC-REPORT (a), reconciled to lockhold-d*.json). |
| 11.3 | Routed demonstration | CONFIRMED | s5: 6.3× direct, ~31× lock collapse (reconciled to s5-routed-2026-05-28 JSON). |
| 11.4 | Crossover identification | CONFIRMED | Full 3-mode × S1–S9 matrix + key-question answer (routed wins under contention; batched inverts to deadlock liability). |
| 11.5 | Failure mode coverage | CONFIRMED | All listed scenarios covered by tests + (g) long-duration cleanliness (zero drop/poison/retry/takeover across 63 cells). |
| 12 | Implementation plan (intro) | CONFIRMED | All five phases delivered (epic acct-2ttr 9/9 ✓ README table). |
| 12-P1 | Phase 1 | CONFIRMED | Schema + ledger-core + unit tests. |
| 12-P2 | Phase 2 | CONFIRMED | ledger-direct-c operational. |
| 12-P3 | Phase 3 | CONFIRMED | ledger-routed-c operational (incl. no-pristine-replay). |
| 12-P4 | Phase 4 | CONFIRMED w/ note | Harness ships all listed capabilities except literal "fsync rate, WAL volume" report fields — commit-span ns (txn−pipeline) is the shipped fsync proxy; WAL volume is not recorded. Variance-magnitude exclusion honored as written. |
| 12-P5 | Phase 5 | CONFIRMED | POC-REPORT.md = the §12 deliverable (crossover map, depth curve, envelope). |
| 13 | Out of scope | CONFIRMED | All 10 items verified genuinely absent (greps: no currency/webhook/account_balance/allow_negative/effective_from/tenant code; no close hooks; no caller-observability surface beyond polling; identity_key plain BIGINT). |
| 14 | Concerns (intro) | CONFIRMED | — |
| 14.1 | Aggregate under Path C | CONFIRMED | Running average maintained on receipts for both bases (provisional.rs receipt path is basis-independent). |
| 14.2 | Pristine-replay not used | DIVERGENT → **A1** | Drop-and-continue ✓, within-group submission_id order ✓, per-submission trial-clone ✓ (Pass-2 §4.4). The "Split-chunk ordering across commit_groups" paragraph and the SETTLED banner's "split-chunk order preservation hold" are FALSE under committer_count>1: chunks of one split component are claimed concurrently by different committers with NO predecessor-wait (router.rs:29-38 states this explicitly); pool_lock serializes but does not order. README §P3.2 and POC-REPORT (d) both already document order-divergence as permitted — the spec §14.2 text is the outlier. |
| 14.3 | Provisional basis choice | CONFIRMED | SETTLED banner accurate; both bases dispatched in provisional.rs; basis×routed interaction text matches code. |
| 14.4 | Reverse operations | CONFIRMED | Negative qty = ordinary depletion; no marking logic (deferred as written). |
| 14.5 | trx_line ordering/id | CONFIRMED | Identity columns; no per-pool sequence (shmem.rs:29-34 documents the deliberate absence). |
| 14.6 | Identity vs commit order | CONFIRMED | Hot path observes no cross-trx id ordering; recalc concern correctly deferred. |
| 15 | Implementation divergences | CONFIRMED-incomplete | All 5 listed divergences re-verified still true (JSONB ✓, coalesce_aggregates ✓, distinct-pools + multi-touch opt-in ✓, standard_cost seeding ✓ pool_universe.rs:193, 0006 CHECK ✓). The list is missing the audit-found divergences: A1 (order preservation), A2 (router defaults/pack), A3 (value_sum DDL), A6 (TEXT wire params), A7 (SpecificPoolOccupied), A8 (state-4/identity/recovery-worker), A9 (retry shape), A10 (backpressure). |

### A.2 Results-doc reconciliation (13 docs)

Spot-checks pull the doc's headline numbers and compare against the named backing artifact.

| Doc | Headline | Backing artifact | Spot-check |
|---|---|---|---|
| POC-REPORT.md | (a)–(g) verdicts: CONFIRMED ×4 premises | lockhold-d*.json, s5-routed-*.json, longdur_*_120s.json, committer_profile_sweep.csv | **4/4 exact**: (a) d1000 tput 1038.1/p50 3928 ✓; (b)/(c) s5 routed 2089.9/cg 30.75/locks 2078/trx 63896/ack-p99 0.79 s ✓; (g) s5 1838/221755/8184/cg 27.10/0/0 ✓; profile s5 median 2814, lock 67 %, 75 rows 0 dups ✓. Nit: "(g) …re-run the 16 sqlx migrations" — 8 versions (16 files) → A12. |
| latency_vs_load.md (acct-at8x) | s10 knee 850→925 (99 ms/293 ms → 922 ms/1.9 s); s2 knee 3.5k→4k; zero drops | latency_vs_load_{s10,s2}[_knee].csv + .log | knee rows **exact**: s10@850 committed p50 94.5/104.2 ms (med 99) p99 149/438 (med 293) ✓; @925 p50 971/872 ms (med 922) ✓ p99 1.9 s ✓. |
| hh7b_window_only_sweep.md | window ≈ no throughput lever; `router_window_size` is the real group ceiling; cc×overlap is the balance axis | hh7b_window_{s2,s5,s6,s7,s10}.csv, hh7b_rwsize_probe_s6.csv | escalation rows **exact**: rws 1000 → med 8631, max_group 1000, locks/trx 0.38, drop 0 ✓; rws 2000 → 8594/2000 ✓. |
| p1al_batch_formation_sweep.md | pack ON 2.06× @ bsm200 on s2; s7 +170 %; s5/s19 neutral; **recommendation = flip pack on + bsm 200** | p1al_s2_*.{md,csv}, batchdiag_s5/s7_*.csv, batch_size_sweep_s19_*.csv | All named CSVs present ✓; recommendation matches live defaults (pack=true, bsm=200) ✓; Phase-2 atomicity-test recommendation shipped (router.rs:1497 pack unit tests) ✓. |
| p1al_s2_off_bsm200.md | 4 934 trx/s, cg 8.1 (cc4, pack off) | generated report (JSONs ignored per policy) | internal arithmetic ✓ (1 480 300/300 s; /182 177 drains = 8.12). |
| p1al_s2_on_bsm50.md / on_bsm200.md / on_bsm800.md | 8 283 / 10 900 / 12 538 trx/s, cg 42/148/490 | generated reports | same-format generated cells; consistent with the sweep doc's §1 table ✓. |
| sustained_s2.md | cc=1: 14 517 trx/s, cg 180.5 | generated report | internal arithmetic ✓ (4 355 000/300 s; /24 121 = 180.5). |
| sustained_s2_cc4_c16_p64.md (**uncommitted stray**) | cc=4: 4 856 trx/s, cg 8.1 | generated report | internal arithmetic ✓; tracking disposition = .7 (§0.4 violation list). |
| apply_inproc_microbench.md | apply ceiling ≈ 36–40 µs/trx; triangulates span ~44 µs within ~10 % | apply_inproc.csv ✓ | cross-instrument triangulation internally consistent; `bench_apply` gate verified = `bench_hooks` feature, committer.rs:1297 (corrects §0.2's ":855" anchor). |
| committer_apply_phase_split.md | parse+analyze+plan ≈ 47 % of committer CPU; levers A/B | committer_resolved.svg ✓ committer_apply.svg ✓ | artifacts present; decision (lever B prepared plans) shipped per acct-sczx lineage, reflected in later docs' prep µs/trx drop ✓. |
| committer_fsync_vs_batch_size.md | fsync amortizes 1/cg; bsm sweep 50→800; "default 50 sits at the worst point" | apply_spans_fsync_rb{50,100,200,400,800}.csv ✓ | all 5 CSVs present; note: "50 (prod default)" row label predates the p1al flip — historical, now misleading without context → A12. |

### A.3 Provenance sweep (Pass-1 D4.1 re-verification)

`grep -rniE "path b|design-v3[^.]|poolseqtable|tm09" ledger-*/src/` → **0 genuine survivors**
across all 5 crates. The two raw hits are the English phrase "path b…" (report.rs:287 "output
path builder", shmem.rs:188 "retry-on-deadlock path be…") — false positives. AUDIT-PASS2's two
admitted committer.rs "Path B" survivors (PASS2 §4.x / :281) are gone — the yojk.1 cleanup
ended up more complete than Pass-2 recorded. D4.1: **resolved, re-verified, improved**.

### A.4 Undocumented-surface sweep

Per-surface doc coverage (spec / README / results docs). "—" = no mention on any doc surface.

| Surface | Coverage | Verdict |
|---|---|---|
| `ledger_submit_trx_c`, `ledger_enqueue_trx_c` | spec §4/§5/§6.1 + README ✓ | documented |
| committer counter getters ×13 + state_counts + ready + recovery_complete | README P3.4 list ✓ | documented |
| `router_max_group_size`, `router_submission_histogram` | hh7b doc §Methodology ✓ | documented |
| arena getters ×5 | hh7b doc §4 (outstanding/bump) ; total_allocs/total_frees/freelist_count — | partial; consumed by bench + leak test → .7 usage map |
| pipeline span getters ×9 | sustained/p1al docs (span tables) + harness JSON ✓ | documented (derived form) |
| router_{ticks,commit_group_count,total_submissions,entries_scanned,window_defers}_total | POC-REPORT (g) derives from them; getter names — | partial → .7 |
| `staging_state_counts` (bench only), `staging_request_seq_max` — | — | undocumented → .7 zero-ref check |
| test_* ×10 (`test_hooks`-gated), `bench_apply` (`bench_hooks`-gated, committer.rs:1297) | AUDIT/PASS2 + apply_inproc doc ✓ | documented (test/bench infra) |
| GUC `router_window_size` / `batch_window_us` / `batch_size_max` / `eject_cooldown_ms` / `committer_count` | spec §6.3/§6.4 (§6.3 stale → A2) | documented |
| GUC `affinity_scheme` | results docs GUC headers + POC-REPORT affinity sections | documented-in-results (experimental) |
| GUC `router_pack_disjoint` | POC-REPORT (g) + p1al docs only; spec/README silent | → A2 (normative-doc gap) |
| GUC `caller_tx_timeout_ms` | spec §6.4 alludes ("caller-tx eject timeout"), unnamed | → A11 |
| GUC `staging_queue_size` / `committer_queue_size` / `spillover_arena_mb` | — ; and they do NOT size the shmem (compile-time consts, NOTICE on mismatch — shmem.rs:13-26; `shmem_sizing_gucs_honored` test pins consistency) | → A11 |
| GUC `queue_full_timeout_ms` / `max_eject_count` / `affinity_steal_ms` / `target_database` | — | → A11 |
| Scenarios s1–s21 | spec §10.6 + POC-REPORT ✓ (README says "S1–S8" → A12) | documented |
| CLI flags (all 3 subcommands) | README quickstart + spec §10.6 (multi-touch, pareto) + latency_vs_load (--target-rate) ✓ | documented |

### A.5 Findings

| ID | Sev | Verdict | Finding |
|---|---|---|---|
| A1 | P2 | DIVERGENT | **§14.2 split-chunk order-preservation claim (and SETTLED-banner clause) vs no-predecessor-wait implementation.** Spec: "chunk 1 fully commits before chunk 2 begins… identical to single-chunk… Determinism is preserved"; banner: "split-chunk order preservation hold as described below." Code: chunks of a split component are claimed concurrently by different committers (claim_next_committer_entry FIFO-scan, cc=4 default); pool_lock serializes without ordering; router.rs:29-38 documents the deliberate drop. README + POC-REPORT (d) already state order-divergence is permitted. Consequence: §14.2's determinism/failure-set argument and §11.1's strict qty-equality reading do not hold cross-chunk in oversell scenarios (unobserved in all measurements). Pass-2 §4.4 verified within-group properties only — the banner overreaches its citation. Doc-fix to §14.2 (+ §15 bullet). |
| A2 | P2 | STALE-doc | **§6.3 router description stale on three axes**: (i) `batch_size_max` "default 50" vs live 200 (acct-p1al); (ii) `batch_window_us` described as the scan cadence — actually an oldest-candidate-age coalesce gate on a 50 ms latch tick (router.rs:107, :152-170; the entire acct-hh7b/235v characterization hangs off this distinction); (iii) the `pack_disjoint_components` pass (default ON) is absent. The two p1al default flips are documented in NO normative doc (spec/README) — only results docs. Cross-refs OPEN `acct-czz4.4` (bsm-default synthesis → POC-REPORT) + `acct-jdgm`; this finding covers the spec §6.3 text itself, not those issues' scopes. |
| A3 | P3 | STALE-doc | §2.2's `pool_state` CREATE TABLE block lacks `value_sum` (+ `pool_state_aggregate_value_sum_nonneg`), which §3.0 prose and migration 0007 carry. Spec self-describes as self-contained; a §2.2-verbatim implementation diverges from §3.0. §15 lists the 0006 CHECK but not 0007's column. |
| A4 | P3 | STALE-comment | wac.rs:4-6 module header: "v3.1 stores the average directly (not a cumulative value_sum)" — asserts the pre-acct-0qps storage model, contradicting §3.0/0007 and the function bodies below it. |
| A5 | P3 | STALE-doc | §5.1 step 7: "against no inventory for STD since STD pools don't track on-hand qty in pool_state" contradicts §3.3 ("STD pools MUST maintain an aggregate row… InsufficientInventory"), §3.6 ("every method"), and standard.rs:161. Code follows §3.3. |
| A6 | P3 | STALE-doc | §4 wire-contract annotation incomplete: shipped SPI signature is `(trx_type TEXT, source_id BIGINT, posted_at TEXT RFC3339, lines JSONB)`; the in-spec note covers only the lines JSONB delta (D1.1), not trx_type/posted_at as TEXT. |
| A7 | P3 | STALE-doc | §3.4 says single-receipt/no-additional-inflow is unenforced ("violation… produces undefined behavior"); code now raises `SpecificPoolOccupied` on receipt-while-stocked (specific.rs:56, D3.2-era guard). Code stronger than spec; qty=1 half of the contract remains caller-side (D3.2 residual re-verified). |
| A8 | P3 | STALE-doc | §6.2/§6.5 shmem + recovery deltas: staging state 4 = `abandoned` (spec lists `in_flight`, unused by its own transition narrative); identity registry = per-slot u32 generation vs spec's global monotonic u64 token (equivalent guarantee); dedicated boot recovery BGWorker owns `recovery_complete` (absent from spec); `correlation_id` staging field unmentioned. |
| A9 | P3 | DIVERGENT | §6.8 retry mechanics: spec says retry "open[s] new PG tx, repeat[s] pg_xact check, repeat[s] dedup"; shipped shape is nested-subtx rollback re-attempting lock→hydrate→apply→write only, with re-dedup deferred to the DuplicateRace path (safety-equivalent via the UNIQUE backstop; pg_xact re-triage loses nothing since kept = committed|unknown). Also: spec's transient classes "lock-wait timeout"/"connection drop" are not classified Retryable in code (55P03 → poison; in-process SPI has no connection). §6.4 step-2 "top-level tx" framing likewise approximate. |
| A10 | P3 | UNDOCUMENTED | §6.1 enqueue backpressure: CV-wait up to `queue_full_timeout_ms` then ERRCODE_INSUFFICIENT_RESOURCES (enqueue.rs §7). The zero-drop claims throughout results docs lean on exactly this mechanism; the spec's submission section is silent on queue-full behavior. |
| A11 | P3 | UNDOCUMENTED | No canonical GUC reference exists; 7 of 15 GUCs appear on no doc surface (`queue_full_timeout_ms`, `max_eject_count`, `affinity_steal_ms`, `target_database`, + the sizing trio). The 3 Postmaster-scope sizing GUCs (`staging_queue_size`/`committer_queue_size`/`spillover_arena_mb`) **document but do not drive** the shmem allocation — compile-time constants govern; mismatch = NOTICE at _PG_init; resize requires recompile (shmem.rs:13-26). Operator-facing trap worth one README table. |
| A12 | P3 | STALE-doc | README/results staleness cluster: README "apply migrations 0001-0005" (8 exist), "Scenarios S1–S8" (21 exist), bench/ listing names 4 of 26 scripts; POC-REPORT (g) "16 sqlx migrations" (8 versions / 16 files); fsync doc's "50 (prod default)" label predates the p1al flip. |
| A13 | P3 | DIVERGENT | §5.1/§5.3/§6.6 pipeline micro-deltas (doc-patch list): step-3 LEFT JOIN shipped as 2 SELECTs (routing + aggregate; equivalent semantics); missing standard_cost (and posting_account_map) raise lazily at line-processing, not at hydration (observable only for qty=0 lines / receipt-only standard-basis batches on misconfigured pools — hydration.rs:11-15 documents the choice); SPI-count lists omit the §3.7 posting_account_map seek and the split-hydrate +1. |

**A-dimension summary**: 68/68 spec headings versed — 60 CONFIRMED (several with notes),
2 DIVERGENT (§6.3→A2, §14.2→A1), §15 CONFIRMED-incomplete; 13/13 results docs reconcile (every
numeric spot-check exact); provenance clean (D4.1 re-verified resolved); 2×P2 + 11×P3 findings,
all doc-side — **no code-behavior defect found in dimension A**. The shipped code is in every
checked divergence either equivalent to or stronger/safer than the spec text.

## B-i. quality: core + spi-common + direct-c + harness + scripts (acct-mvq4.3 — pending)

## B-ii. quality: ledger-routed-c deep (acct-mvq4.4 — pending)

## C-i. coverage map + gap classification (acct-mvq4.5 — pending)

## C-ii. suite-run ledger (acct-mvq4.6 — pending)

## D. complexity disposition (acct-mvq4.7 — pending)

## Findings index (acct-mvq4.8 — pending)

| ID | Sev | Verdict | Title | Filed issue / duplicate-of |
|---|---|---|---|---|
| A1 | P2 | DIVERGENT | §14.2 split-chunk order-preservation claim vs no-predecessor-wait implementation | `acct-mvq4.9` |
| A2 | P2 | STALE-doc | §6.3 stale: bsm default 50 vs 200; batch_window mischaracterized; pack_disjoint absent | `acct-mvq4.10` (x-ref acct-czz4.4, acct-jdgm) |
| A3 | P3 | STALE-doc | §2.2 pool_state DDL block lacks value_sum (0007) | `acct-mvq4.11` |
| A4 | P3 | STALE-comment | wac.rs header asserts pre-value_sum storage model | `acct-mvq4.12` |
| A5 | P3 | STALE-doc | §5.1 step 7 "no inventory for STD" contradicts §3.3 + code | `acct-mvq4.13` |
| A6 | P3 | STALE-doc | §4 wire note omits trx_type/posted_at as TEXT | `acct-mvq4.14` |
| A7 | P3 | STALE-doc | §3.4 unaware of SpecificPoolOccupied guard | `acct-mvq4.15` |
| A8 | P3 | STALE-doc | §6.2/§6.5 shmem state-4 / identity-generation / recovery-worker deltas | `acct-mvq4.16` |
| A9 | P3 | DIVERGENT | §6.8 retry shape: subtx re-attempt, no repeat triage/dedup; transient-class list | `acct-mvq4.17` |
| A10 | P3 | UNDOCUMENTED | §6.1 silent on enqueue backpressure (queue_full_timeout_ms) | `acct-mvq4.18` |
| A11 | P3 | UNDOCUMENTED | No GUC reference; 7 GUCs doc-absent; sizing trio doesn't size shmem | `acct-mvq4.19` |
| A12 | P3 | STALE-doc | README/results staleness cluster (migrations, S1–S8, bench list, "16 migrations", "prod default 50") | `acct-mvq4.20` |
| A13 | P3 | DIVERGENT | §5.1/§5.3/§6.6 pipeline micro-deltas (split hydrate, lazy fail-loud, SPI counts) | `acct-mvq4.21` |
