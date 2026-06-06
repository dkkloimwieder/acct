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

## B-i. quality: core + spi-common + direct-c + harness + scripts (acct-mvq4.3 — 2026-06-06)

**Method.** Per-module walk of the 4 non-routed crates (every `src/` file read in full this
pass), seven questions each: Q1 panic discipline / Q2 unsafe / Q3 atomic ordering / Q4 error
mapping / Q5 resource cleanup / Q6 measurement validity / Q7 structural. Then all 31 shell
scripts (26 `bench/` + 5 `scripts/`) on the S1–S4 checklist. Findings Q1–Q6 in §B-i.7;
class-(a) script findings annotated on OPEN `acct-xdkd` (not re-filed); drain-tail notes on
`acct-x9bg`; `acct-vsfy` reconciled in §B-i.6.

**Sweep-level results.** `unsafe`: **zero blocks in all 4 crates** (Q2 closed wholesale).
Atomics: only harness `AtomicBool` stop-flags (measure/sampler/driver_routed), all
`Relaxed` — correct, since no data is ordered behind the flag (reports hand off via
`JoinHandle.await`, which synchronizes). Panic-family grep: 34 sites, every one classified
below — none unjustified on a production path.

### B-i.1 Module verdicts

**ledger-core (12 src + 2 test files)**

| Module | Verdict | Notes |
|---|---|---|
| lib.rs | CLEAN | Module map + re-exports; doc accurate (strict vs provisional entry points). |
| error.rs | CLEAN | 8 variants, every one doc-commented with §-refs. The closed-enum + no-wildcard consumers (see B-i.3) make variant additions compile-enforced. |
| method.rs | CLEAN | Strict dispatcher; `UnknownPool` via `ok_or` before dispatch. |
| fifo.rs / lifo.rs | CLEAN w/ note | Stubs as spec'd (§8). Note: `MethodMismatch{expected: Fifo, got: Fifo}` — fields degenerate by construction (dispatcher already resolved the method), and the in-stub `UnknownPool` re-lookup is unreachable. Harmless redundancy; the error text reads oddly but the doc on error.rs:16-18 explains the real meaning. |
| numeric.rs | CLEAN | `banker_div` exact §3.0 shape. 1 `expect` (:44) — documented, has a `should_panic` test; `debug_assert` (:22) — caller-guard contract documented, wac.rs honors it. 11 tests incl. i128-limit + overflow regression. |
| plan.rs | CLEAN | `coalesce_aggregates` keep-last logic correct (fast path + enumerate-stable rebuild); LineType/PostingEventType `as_sql` bijective with line_type.rs decoder. |
| snapshot.rs | CLEAN | `unwrap_or((0,0))` (:130) is graceful error-message degradation, fine. `resolve_posting_accounts` returns owned `Copy` to end the borrow — documented. |
| wac.rs | CLEAN w/ **Q1** + A4 | All arithmetic checked-or-i128 → `Overflow`. **Q1**: `aggregate_deplete` (:175) subtracts the posted amount from `value_sum` unclamped when `new_qty > 0` — negative `value_sum` reachable (standard-basis; rounding edge), violates the 0007 CHECK at write. A4 (stale header :4-6) already filed → `acct-mvq4.12`. The :90 `new_qty > 0` receipt guard is unreachable-defensive (receipt implies qty>0) but documented as such. |
| standard.rs | CLEAN | `value_sum = new_qty × C_std` recomputed fresh — cannot go negative. Variance flip + `MissingVarianceAccount` gate only when delta ≠ 0 (zero-delta receipts need no variance account — correct). |
| specific.rs | CLEAN | `SpecificPoolOccupied` guard (A7, doc-side). Deplete clamps aggregate at 0 (`.max(0)` :169). D3.2 residual (partial-deplete of oversized layer leaves aggregate qty > 0) re-verified still-true — documented caller contract, not a new finding. In-memory layer removal (:183-185) makes same-submission double-deplete fail correctly. |
| provisional.rs | CLEAN w/ **Q1** | Dispatch §3.5-exact. Standard-basis depletion (:78-82) prices at `C_std` fully decoupled from pool book value — the Q1 trigger. |
| tests/plan_apply_strict.rs (17) + tests/plan_apply_provisional.rs (10) | CLEAN w/ gap | Cover every §9.1 bullet. Gap (part of Q1): all depletion `value_sum` assertions are positive-residual shapes; the standard-basis test (:110) uses `C_std=90 < avg=100`; no test pins what `value_sum` does when the posted depletion amount exceeds book value. |

**ledger-spi-common (5)**

| Module | Verdict | Notes |
|---|---|---|
| lib.rs | CLEAN | States the single-sourcing rationale (the acct-vsfy supersession evidence, §B-i.6). |
| line_type.rs | CLEAN | Decoder bijective with `LineType::as_sql` (9/9); tests pin all variants + unknown→None. |
| pool_lock.rs | CLEAN | Optimistic pattern spec-exact; in-function sort+dedup documented as defensive. The lazy-create path's re-`SELECT FOR UPDATE` result is discarded — sound because no code path deletes `pool_lock` rows and `ON CONFLICT DO NOTHING` + READ COMMITTED guarantee the row is visible; noted, not a finding. |
| hydration.rs | CLEAN w/ A13 | 5-connect split + lazy fail-loud = A13 (filed → `acct-mvq4.21`). `unwrap_or(0)`-style NULL coercion throughout — moot under current NOT NULL schema; unknown enum text → `continue` → UnknownPool downstream (documented inline). |
| bulk_write.rs | CLEAN | 1 `expect` (:89) locally provable (set two lines above). `ORDER BY ord` + ascending-sort identity-alignment trick documented; `debug_assert`s on RETURNING cardinality ride PG semantics in release. Index panics (:345/:415) ride ledger-core's `trx_line_idx` construction invariant — a violation would ereport (fail-loud), not corrupt. Kept-plan `thread_local` cache intentional process-lifetime (Q5 n/a). |

**ledger-direct-c (3)**

| Module | Verdict | Notes |
|---|---|---|
| lib.rs | CLEAN | No-op `_PG_init` documented (shmem-free); 2 `expect`s are in the `#[pg_test]`. |
| ledger_error_map.rs | CLEAN — **8/8** | See B-i.3. |
| submit.rs | CLEAN | 3 `unreachable!()` — all diverge-typing shims after `ereport!`, documented + `#[allow]`ed. Validation order cheap-fail-first (parse → decode → lock). SQL-level errors (e.g. the UNIQUE idempotency backstop) propagate with their own SQLSTATE via pgrx guard, not masked by `ereport_internal`. |

**ledger-harness (13 src + 2 smokes)**

| Module | Verdict | Notes |
|---|---|---|
| main.rs | CLEAN | All failure paths → exit code 1 with message; `--touch-dist` parsed before any reseed work (documented). |
| cli.rs | CLEAN | `--batch-size` default 50 ✓ §10.0; every flag doc-commented incl. pooler guidance. |
| driver_common.rs | CLEAN | `run_prefix` wraparound (~11.5 days) documented; 1 test pins the SPI wire shape. |
| seed.rs | CLEAN w/ note | `expect` (:69) is insert-then-lookup invariant. Note (Q6-misc): the `pool_state > 0` skip heuristic is any-row, so a partially-seeded universe would be silently accepted — contrast pool_universe's count-exact check; harness-grade, reseed paths TRUNCATE first. |
| pool_universe.rs | CLEAN | Count-exact idempotency with fail-loud mismatch; deterministic `Mixed` (tested 50/30/20); standard_cost seeded for std pools only — and `provisional_basis='running_avg'` for ALL pools (:159), which is why Q1's standard-basis path is bench-unreached. |
| workload.rs | CLEAN | Both panics are documented fail-loud guards on degenerate config (empty universe). `sample()` fallback (:138) documented-unreachable — the honest version of what scenarios.rs:158 does wrong. Exhausted-overlap shrinkage documented (:191-193). Receipt costs 1..=1000 micro-units (1000× below seed cost) — irrelevant to lock/throughput claims, noted for any future cost-magnitude reading. |
| scenarios.rs | CLEAN w/ **Q2** | All 21 builders match §10.6 (re-verified at .2). **Q2**: :158 unreachable `unwrap_or(1)` + hardcoded `1000` caller-count copy. `expected_method_mix` dead_code → .7. |
| measure.rs | CLEAN | Histogram `expect`s constant-bounds-provable. Q6 caveats: `commits_observed` is database-wide (includes observer/sampler poll txns — informational field, not used for throughput); `wal_lsn_bytes` is cluster-global (background WAL from other streams pollutes `wal_bytes_per_trx` on the shared cluster — preload pruning keeps this small). Drop aborts the task ✓. |
| sampler.rs | CLEAN w/ **Q3** | **Q3**: bucketing-rules comment (:169-175) stale vs code (:183-185) — LWLock has its own bucket, IPC added to io. Committer scoping by `backend_type` ✓; 3 queries/tick at 10 Hz with `--no-sampler` escape documented. |
| report.rs | CLEAN | All 8 panic-grep hits in tests. Direct ack==committed documented; routed throughput from observer count; errors excluded from hist but included in `attempts_total` — honest. |
| driver_direct.rs | CLEAN w/ **Q4** | **Q4**: batched mode records pre-rollback successes in the ack hist (:232) — rolled-back work counts toward throughput under mid-batch failure. Conservative w.r.t. POC-REPORT (b)'s conclusion; `commits_observed` allows cross-check. Throughput denominator captured before the 2 s stats-settle sleep ✓. |
| driver_routed.rs | CLEAN | `expect` (:280) locally provable. Observer-before-callers ✓; incremental-scan skip hazard closed by the final reconciling sweep (:192-206) ✓; quiet-heuristic (3×200 ms) can fire early under a 1 s deadlock-backoff stall — effect surfaces visibly as `submitted_but_unseen > 0` and errs conservative (→ x9bg note). Observer poll errors silently `unwrap_or_default` (:453) vs measure.rs's counted `poll_errors` — asymmetry noted (Q6-misc). Pacing math + debt-shed documented and correct. |
| equivalence.rs | CLEAN (model) | Drain quiescence backed by an explicit completeness assertion — early-stop produces a distinct "drain incomplete" error, never a false PASS. The Q6 drain-wait question's best-in-crate answer. Nit: :73 hint "try s1..s8" (Q6-misc). |
| tests/smoke_{measure,sampler}.rs | CLEAN | `#[ignore]`d, DSN-pinned, `#[path]`-include of the src module — smoke-grade, fine. |

### B-i.2 Panic-discipline table (production-path sites)

| Site | Kind | Classification |
|---|---|---|
| numeric.rs:44 | `expect` | Justified: i64 overflow on downcast = application-level numeric error; documented + `should_panic` test. |
| numeric.rs:22 | `debug_assert` | Caller contract (non-zero denominator) documented; wac.rs guards. |
| bulk_write.rs:89 | `expect` | Locally provable (slot populated two lines above). |
| bulk_write.rs:345/:415 | index | Cross-crate invariant (`trx_line_idx` constructed valid by ledger-core); violation ereports, fail-loud. |
| submit.rs:119/:135/:141 | `unreachable!` | Diverge-typing shims after `ereport!`; documented. |
| seed.rs:69 | `expect` | Insert-then-lookup invariant (own UNNEST insert). |
| measure.rs:47/:57/:59 | `expect` | Constant-bounds histograms; merge of same-bounds cannot fail. |
| scenarios.rs:193/:206 | `expect` | Constant-string `TouchDistribution::parse`. |
| workload.rs:299 | `expect` | `Zipf::new` fails only on empty universe / exp≤0 — config error, fail-loud. |
| workload.rs:337 | `panic!` | Explicit documented guard (empty pool_ids). |
| driver_routed.rs:280 | `expect` | Locally provable (`routed: Some(..)` set in the constructor call above). |
| cleanup.rs:240-242/:264/:297 (routed-c) | `expect` | **Re-classified per §0.6: `#[cfg(test)]` helpers, NOT production paths — non-findings** (recorded here as the .3-scope half of that correction; .4 re-records for the routed walk). |
| All other grep hits | — | `#[cfg(test)]` / smoke-test code. |

### B-i.3 Error-map completeness (Q4)

`ledger_error_map.rs::raise_ledger_error` matches **all 8** `LedgerError` variants with **no
wildcard arm** — a future variant fails to compile here, the strongest completeness guarantee.
Every arm carries message + hint + a deliberate SQLSTATE:

| Variant | SQLSTATE | Sane? |
|---|---|---|
| InsufficientInventory | 23000 integrity_constraint_violation | ✓ (§3.6 invariant) |
| MethodMismatch | 22000 data_exception | ✓ (dispatch bug surface) |
| UnknownPool | 42704 undefined_object | ✓ |
| MissingStandardCost | 22000 | ✓ (config error) |
| MissingPostingAccounts | 22000 | ✓ (config error) |
| MissingVarianceAccount | 22000 | ✓ (config error) |
| SpecificPoolOccupied | 23000 | ✓ (K=1 invariant) |
| Overflow | 22003 numeric_value_out_of_range | ✓ |

None of the 8 fall in the routed committer's retryable set (40P01/40001) — correct: every
LedgerError is deterministic, not transient. (The routed-side classification itself is .4
scope.) Q1's failure shape bypasses this map entirely — it surfaces as a raw 23514 CHECK
violation from the write phase, which is part of why it's finding-worthy.

### B-i.4 Harness measurement-validity (Q6 synthesis)

1. **Latency**: per-caller hdrhistograms, merged; errors excluded from latency but included
   in `attempts_total`. Direct ack==committed documented. Routed committed-latency from a
   pre-started observer with a post-drain reconciling sweep — the out-of-order-id skip
   hazard is correctly closed. **One gap: Q4** (batched-mode pre-rollback recording).
2. **Throughput convention (routed)**: submitted-in-window / caller-window; work drained
   after callers stop still counts. Bounded by O(ring/total) ≈ up to ~25 % ambiguity on the
   short 20–30 s cells vs instantaneous drain rate, ≲2 % at the 120–300 s durations every
   headline doc uses. Annotated on `acct-x9bg` (with the 1 s-backoff vs 600 ms quiet-window
   early-fire note; both effects surface visibly as `submitted_but_unseen`).
3. **Counters**: `commits_observed` database-wide and `wal_lsn_bytes` cluster-global —
   informational fields; neither feeds a headline number. Routed counter deltas clamp at 0
   (`max(0)`) and saturating-subtract span derivations are comment-justified.
4. **Quiescence**: equivalence.rs is the model (completeness assert); driver_routed
   deliberately tolerates partial drain (overload cells must) and reports the shortfall.
5. **Seeding honesty**: deep-seed writes trx_line-consistent layers (§10.5-exact);
   pool_universe count-exact idempotency; seed.rs any-row heuristic is the one soft spot
   (Q6-misc note). Perturbation: sampler/collector documented, `--no-sampler` escape, and
   the report records whether it ran.

### B-i.5 Script table (S1 pipefail / S2 GUC-pin→trap / S3 pgrep / S4 psql-multi-stmt)

Sweep results: **S1 31/31** (`run-tests.sh` deliberately `set -uo pipefail` — its own
per-binary FAIL_FAST handling, §0.3); **S3 zero** pgrep/pkill anywhere; **S4 zero**
multi-statement `psql -c` strings (all helpers single-statement `-tAc`; no ON_ERROR_STOP
needed). S2 per script:

| Script | Pins GUCs? | Trap/restore | Verdict |
|---|---|---|---|
| bench-apply-inproc.sh | no | — | CLEAN |
| common.sh | helper defs only | — | CLEAN (model posture: HARNESS_TIMEOUT hard-wrap, load-gate fail-loud, restart_db readiness wait) |
| measure-apply-spans.sh | yes | trap → **bsm 50** | **class (a)** → xdkd ✓ (named there) |
| probe-hh7b-rwsize.sh | yes | trap → 200/500/4/on/1000 | CLEAN (current defaults) |
| run-1sku-batched.sh | yes | trap → **bsm 50** | **class (a)** → xdkd ✓ |
| run-1sku-per-committer.sh | yes | trap → **bsm 50** | **class (a)** → xdkd ✓ |
| run-affinity-steal-tune.sh | yes (scheme/steal) | trap → scheme 0, steal 5 | CLEAN (narrow pin, narrow restore) |
| run-affinity-sweep.sh | yes (scheme) | trap → scheme 0 | CLEAN (narrow) |
| run-basic-2pool.sh | yes (cc) | trap → cc 4 + affinity off | CLEAN (narrow) |
| run-batch-size-sweep.sh | yes | trap → **bsm 50 + pack off** | **class (a)** → xdkd ✓ |
| run-batch-window-sweep.sh | yes (window) | trap → window 500 | CLEAN (narrow) |
| run-caller-batch-probe.sh | yes | trap → **bsm 50** | **class (a)** → xdkd ✓ |
| run-committer-count-sweep.sh | yes (cc + relic) | trap → cc 4 + relic | **Q5(i)**: pins/restores `committer_affinity` — a GUC removed with the lever-2 revert (e9a77d8); pgrx reserves no prefix, so the ALTER lands as a silent placeholder no-op. Benign for results (the affinity code was removed too); relic. |
| run-committer-profile-sweep.sh | **no** | — | **Q5(ii)**: runs at ambient GUCs while LOGGING "committer_count=4 affinity=OFF" as a claim — no SHOW/current_setting verification. |
| run-crossover.sh | no | — | Q5(ii) ambient-assumption class (docker restart each cell mitigates shmem state, not auto.conf pins). |
| run-equivalence.sh | no | — | Q5(ii) class. |
| run-lockhold-sweep.sh | no | — | Q5(ii) class. |
| run-routed-longdur.sh | no | — | Q5(ii) class (5× restart_db; ambient GUCs = "shipped config" by intent). |
| run-sustained-5min.sh | yes | trap → 200/on/500/4 | CLEAN (current defaults) |
| setup-cc1-for-perf.sh | yes (cc 1, bw 20000, bsm 200) | **none — deliberate** | **Q5(iii)**: persistent profiling regime with no restore partner and no header note on how to undo; the pin outlives docker restart (auto.conf). Modern sweeps' `production_defaults()`-at-start mitigates only if the next run is one of them. |
| setup-pgbouncer.sh | no | — | CLEAN |
| sweep-hh7b.sh | no (delegates) | — | CLEAN (orchestrator; children pin) |
| sweep-hh7b-window.sh | yes | trap → 200/on/500/4 | CLEAN (current defaults) |
| sweep-latency-vs-load.sh | yes | trap → 200/on/500/4/1000 | CLEAN — the model class-(a) scripts should converge to |
| sweep-p1al-decisive.sh | no (delegates) | — | CLEAN (orchestrator) |
| sweep-p1al-nonregress.sh | no (delegates) | — | CLEAN (orchestrator) |
| scripts/create-poc-v3-1-db.sh | no | — | CLEAN |
| scripts/install-direct-c.sh | no | — | CLEAN |
| scripts/install-routed-c.sh | preload append | — | CLEAN for v3.1 scope (the cross-stream preload-accumulation hazard is a cluster-policy matter, already covered by standing guidance — not a v3.1 finding) |
| scripts/run-migrations.sh | no | — | CLEAN |
| scripts/run-tests.sh | no (installs test_hooks .so) | — | CLEAN (`set -uo` deliberate) |

Planning-time class-(b) ("pins with no trap", 8 candidates) **dissolved on verification**:
7 of 8 don't pin at all — run-lockhold-sweep / run-committer-profile-sweep / run-crossover /
run-equivalence are the no-pin ambient-assumption class (→ Q5(ii)), sweep-p1al-decisive /
-nonregress / sweep-hh7b are orchestrators whose children pin-and-trap correctly. Only
setup-cc1-for-perf actually pins trap-less, and that is deliberate (→ Q5(iii)).

### B-i.6 acct-vsfy reconciliation

acct-vsfy (paired-file direct↔routed copy-paste hazard) is **superseded in substance** by the
acct-yojk.2 spi-common extraction: `pool_lock` / `hydration` / `bulk_write` / `line_type` are
now single-sourced in `ledger-spi-common` and consumed by both flavors (lib.rs documents
exactly this rationale). Residual pairing that remains is **intra-file and adjacent**:
bulk_write.rs's per-submission vs batch variants (8.1/8.1b, 8.2/8.2b, 8.4/8.4b share a column
extraction shape; 8.4 pair already shares `insert_posting_lines_with_tlid`). That residual is
co-located, commented as paired, and an order of magnitude smaller than the cross-crate
hazard vsfy named. **Recommendation: close acct-vsfy as superseded-by-yojk.2** (annotated on
the issue; closure is the epic owner's call at remediation time).

### B-i.7 Findings

| ID | Sev | Verdict | Finding |
|---|---|---|---|
| Q1 | **P2** | CODE-DEFECT (latent) | **Provisional standard-basis partial depletion can emit aggregate `value_sum < 0` → migration-0007 CHECK violation (23514) at write time, not a typed LedgerError.** `wac::aggregate_deplete` (:175) subtracts the posted amount unclamped whenever `new_qty > 0`; under `ProvisionalBasis::Standard` (provisional.rs:78-82) the posted amount is `qty × C_std`, fully decoupled from the pool's book value. Repro shape: receipt 10 @ 100 (`value_sum=1000`), `standard_cost=150`, deplete 7 → mutation `{qty: 3, value_sum: −50}` → `pool_state_aggregate_value_sum_nonneg` rejects. Realistic: any `C_std` above the running average plus a deep-but-partial depletion (10 % drift breaks at >91 % depletion). The 0007 header's own semantics ("value_sum stays reconcilable to Σ posting_line amounts") *legitimately* goes negative here — the CHECK and the stated semantics conflict. Coverage: unit test (provisional :110-122) and integration test (acceptance_direct_methods :224-239) both use `C_std < avg`; no test asserts the mutation's `value_sum` in the over-book shape; **every harness pool is seeded `running_avg`** (pool_universe.rs:159) so no measured run ever drove the path — all shipped results unaffected. Twin edge on the running-avg basis: banker-rounding-up of the average (e.g. qty 5 / value_sum 3 → avg 1) times a large partial deplete → `value_sum −1`; reachable only at sub-micro-unit costs. Remediation options (clamp like the empty-pool case / typed error / relax CHECK to match the GL semantics) are the fix-issue's call. |
| Q2 | P3 | MISLEADING-CODE | scenarios.rs:158 — `checked_div(1000).unwrap_or(1)`: `checked_div` returns None only on zero divisor; the divisor is the literal 1000, so the `unwrap_or` arm is unreachable and reads as load-bearing when the actual floor is `.max(1)`. The `1000` is also a hardcoded copy of s6's caller count, not derived from it. Contrast workload.rs:138, which documents its unreachable fallback honestly. |
| Q3 | P3 | STALE-comment | sampler.rs:169-175 — `committer_wait_summary` bucketing-rules comment says `'IO' | 'WALSync' | 'LWLock' → io_wait`; the code (:183-185) gives LWLock its own bucket (struct docs agree) and adds IPC to io_wait. Same in-code-comment-contradicts-code class as A4. |
| Q4 | P3 | MEASUREMENT-honesty | driver_direct.rs caller_loop (batched arm, :232) — successes recorded into the ack histogram before a later mid-batch failure rolls the whole tx back; rolled-back submissions count toward `throughput_trx_per_sec`. At POC-REPORT (b)'s high batched error rates the inflation is non-trivial, but it is conservative w.r.t. that doc's conclusion (real batched is worse than reported), and `commits_observed` permits a cross-check. Fix shape: buffer per-batch latencies, flush to hist on commit. |
| Q5 | P3 | SCRIPT-hygiene | Bench GUC-hygiene cluster, the parts beyond acct-xdkd's scope: **(i)** run-committer-count-sweep.sh:33/:36 pins and "restores" `ledger_routed_c.committer_affinity` — removed with the lever-2 revert (e9a77d8); lands as a silent placeholder (pgrx reserves no GUC prefix), benign but masks real rename errors. **(ii)** The no-pin measurement scripts (run-committer-profile-sweep, run-crossover, run-equivalence, run-lockhold-sweep, run-routed-longdur) assume ambient GUCs equal production defaults without a `SHOW`/`current_setting` assert — profile-sweep logs "committer_count=4" as a literal claim; a stale pin (e.g. from setup-cc1-for-perf) would silently mislabel cells. **(iii)** setup-cc1-for-perf.sh deliberately leaves a persistent cc=1/bw=20000 regime with no restore partner and no undo note in its header. One shared verify-or-pin helper in common.sh closes all three. |
| Q6 | P3 | MISC-structural | Cluster of small items, none individually finding-grade: equivalence.rs:73 error hint "try s1..s8" (21 exist); driver_routed.rs:453 observer poll errors silently `unwrap_or_default` (measure.rs counts `poll_errors` — asymmetric observability); seed.rs:43 any-row idempotency heuristic accepts a partially-seeded universe (pool_universe's count-exact check is the model); routed throughput convention (submitted-in-window/window — drain-tail ambiguity bounded by ring/total, annotated on x9bg). |

**B-i summary.** 33 modules + 4 test files versed: 31 CLEAN (several with notes), wac.rs +
provisional.rs carry the one latent code defect (**Q1, P2 — the first code-behavior finding
of Pass 3**; dimension A found none, and Q1 is consistent with that: it lives in a
configuration path no measurement nor doc claim ever exercised). Zero unsafe; panic
discipline sound (every production-path site justified); error map 8/8 compile-enforced;
harness measurement validity solid with one honesty gap (Q4) and documented conventions.
Scripts: S1/S3/S4 fully clean; S2 confirms exactly the five xdkd-named class-(a) scripts (no
new members), dissolves the planning-time class-(b) into orchestrators + the no-pin
ambient-assumption class (Q5). 1×P2 + 5×P3, all filed as epic children.

## B-ii. quality: ledger-routed-c deep (acct-mvq4.4 — 2026-06-06)

**Method.** All 11 `src/` modules re-read in full this pass (6,217 lines incl. inline tests),
same seven questions as B-i plus the two routed-only deliverables: a complete unsafe inventory
with per-site soundness arguments (§B-ii.2) and an ordering verdict for every shared atomic
(§B-ii.3). Prior routed findings re-verified in §B-ii.4; the three checkpoints pre-pinned by
the B-i walk resolved in §B-ii.5. Findings Q7–Q14 in §B-ii.6. Dedup honored: `acct-ozln`
(router O(ring) head-scan) and `acct-m4g5.2` (claim-loop O(ring) scan) are cross-referenced
where their sites appear, never re-filed; affinity.rs gets a quality verdict only (disposition
= .7 / `acct-m4g5`).

### B-ii.1 Module verdicts

| Module | Verdict | Notes |
|---|---|---|
| lib.rs (634) | CLEAN | 36 `#[pg_extern]` getters + hello (§0.2 count re-verified), 15 GUCs, 3 BGWorker registrations (entry-point name strings match the `#[unsafe(no_mangle)]` symbols exactly). Counter getters all Relaxed loads ✓ (pure counters); `recovery_complete` getter Acquire ✓. Note: `committer_count` is `GucContext::Sighup` but worker count is fixed at `_PG_init`; the GUC description documents this. The only runtime reader is the affinity owner stamp (`router.rs:297`) — a SIGHUP change shifts the (default-off) ownership modulo away from the live worker set; steal-aging preserves liveness. Affinity-only, benign. |
| shmem.rs (463) | CLEAN w/ Q14 | Layout sound: `#[repr(C)]` + explicit pads; 64-byte alignment on queue heads, identity slots, arena anchor. Zero-init validity argued per non-trivially-zero field (CV lazy-init gate is the model: 3-state CAS, doc explains the `proclist_head` ≠ 0-valid hazard). `now_us` clamp / `now_ns` saturating-mul documented. `signal_staging_slot_freed` contract (caller already holds the guard; CV has own slock) documented. Q14: `free_slot_wake_count`'s doc-comment claims "tests assert this grew" — no reader exists anywhere (no pg_extern, no test). |
| enqueue.rs (648) | CLEAN | Publish ordering correct: plain stamps under `STAGING_QUEUE.exclusive()` → CAS `valid` 0→1 Release (stale `commit_group_id`/`eject_count` cleared pre-CAS ✓). CV protocol balanced on every path (PrepareToSleep ×2 paired with CancelSleep ×6 across Ok/ArenaFull-deadline/Internal/QueueFull-deadline exits — audited exhaustively); `ProcessInterrupts()` at loop top keeps backpressured callers cancellable. Arena-free-on-QueueFull ✓ (single-push frees and re-allocs per retry; batch keeps allocs live across waits and frees the unpushed remainder on deadline — both documented). Error paths drop the arena guard before `ereport!` ✓. Notes: full-queue retry rescans O(16384) under exclusive (bounded by CV pacing — wakes only on slot-freed broadcast or 1 s timeout); `trx_type_to_id` unknown→0 is benign only because the field is dead (→ Q14). `staging_request_seq_max` is consumed by acceptance_routed_enqueue ✓ live. |
| router.rs (1555) | CLEAN w/ Q7, Q11 x-ref ozln | Tick pipeline matches the A2-corrected §6.3 shape (50 ms latch tick; `batch_window_us` = oldest-candidate-age gate; pack_disjoint first-fit). `collect_candidates` Relaxed `valid` load is sound via LWLock ordering (enqueue publishes under exclusive; the scan's share acquisition happens-after) — same justification class as arena.rs, deserves the same explicit comment (nit). Cooldown filter Acquire loads pair with the eject path's Release stores ✓. **Head never advances**: `StagingQueue.head` has no store anywhere — every scan is O(ring) from slot 0 = `acct-ozln`, re-verified, cross-ref only. `pack_disjoint_components` atomicity pinned by unit tests (exact partition, document-not-line cap) ✓. Emit: CAS 1→2 under share; arena alloc all-or-nothing with rollback-to-pending; CQ claim under exclusive with data-before-flag stamp (plain fields → CAS 0→1 Release); production staging-stamp block carries zero injection reads (`#[cfg(not(test_hooks))]`, yojk.11 ✓). **Q7 root**: CQ publish precedes the staging 2→3 stamps — the group is claimable before its staging entries are routed. Boot sweep: 3-phase ordering documented + load-bearing-order rationale; pure helpers fully unit-tested (12 tests); **Q11**: Phase-2 revert clears identity AFTER the valid CAS with Relaxed stores. `router_cross_commit_group_for_update_waits` incremented, never readable (→ Q14). Busy-loop note: `while router_tick() > 0 {}` defers SIGHUP/pause/SIGTERM handling under sustained backlog — benign for shipped benches (reloads happen between cells), worth knowing for ops. |
| committer.rs (1489) | CLEAN w/ Q7–Q10, Q12, Q13 | The pipeline implements §6.4 steps + §6.8 exactly as documented (see B-ii.5 for the checkpoint walk). Claim protocol: generation-CAS *is* the election (CAS 0→gen AcqRel), `valid` CAS 1→2 commits it; rollback on valid-CAS failure restores the sentinel. Claim scan is `acct-m4g5.2`'s O(ring)-under-share site — cross-ref only. Dedup: kept-plan (`SPI_keepplan`) + Rust-side `enum_range` label cache with the 22P02-outside-subtx rationale documented (acct-e95d). Re-drive loop bounded (`max_redrives = len+1`, each pass removes ≥1) ✓. Backoff comment matches code (10 ms·2^(n−1) cap 1 s, ≤5) ✓. `plan_and_write`: per-submission trial clone (yojk.12, kept-by-measurement) + batched write with FK chain via ordinality-alignment; `snapshot.aggregate(pid)` None-skip is unreachable-defensive (aggregates are never deleted on the provisional path) — silent-skip shape noted. Findings hosted here: **Q7** (eject mutates staging unconditionally — races the router's late stamp), **Q8** (write-phase 23514 poisons whole group), **Q9** (xid-triage SPI-error swallow → keep-all), **Q10** (ERROR-exit orphan trigger gap), **Q12** (CQ share guard held across the whole SPI write phase), **Q13** (u32 xid vs xid8 epoch). `#![allow(dead_code)]` module-wide (also cleanup.rs:42, payload.rs:25) → .7 sweep candidate. bench_apply (`bench_hooks`-gated, :1297-1412): unsafe ctx/owner save-restore documented, always-rollback, warmup discards — sound, bench-only. |
| arena.rs (418) | CLEAN | All ops under `SPILLOVER_ARENA` exclusive; Relaxed justified by the lock (module header says so explicitly — the model comment). Offset-0 sentinel lazy-bump documented; bounded freelist walks at BOTH walk sites (alloc + freelist_count) with the wedged-LWLock-holder rationale; cycle-guard unit test. Misuse (double-free / wrong offset) documented as caller contract; failure mode is bounds-check panic → ereport (fail-loud, not corruption). First-fit without block splitting = internal fragmentation only (size preserved in header); slab fallback documented. 11 tests incl. stress. |
| payload.rs (423) | CLEAN | Encoder frees partial allocs on every error path (yojk.5 ✓, test-pinned `outstanding == 0`). `read_bytes_bounded` checked-add + length validation before every read; `line_count` cross-check; tamper + OOB + proptest round-trip coverage. Note: `submission.line_offset + 4` (:197) is an unchecked u32 add — a hostile/corrupt offset wraps but lands in bounds-checked reads → typed error, no unsafety. `LinesBlockTooSmall` map_err arm is defensive-unreachable (bounded read returns exactly 4 bytes). |
| cleanup.rs (371) | CLEAN | Three-CAS release model + cg-id guard documented in the header and pinned by 9 unit tests (all three cases + eject-blocks-fallback + cg-mismatch + idempotence). Acquire loads pair with the router/committer Release stores per the header's pairing table ✓. Lock ordering strictly sequential (guards scoped per helper, never nested) ✓. `finalize_committer_queue_slot`'s trailing Relaxed identity clears are safe because the next writer (router emit) needs the exclusive lock the finalize share guard excludes — contrast Q11's sweep site, which lacks that protection. The :240-242/:264/:297 `expect`s are `#[cfg(test)]` helpers — §0.6 re-recorded, non-findings. |
| identity.rs (123) | CLEAN | Two-pass claim (free slots, then ESRCH-verified dead-occupant takeover); EPERM treated alive (conservative) ✓; release bumps generation BEFORE clearing pid (stale CQE references fail the match during the window) ✓; `is_committer_alive` rejects generation==0 sentinel + OOB slot. The recycled-PID race (pid/gen loads vs concurrent release) errs toward "alive" → delays takeover, self-corrects next sweep — analyzed sound. Panic on 64-slot exhaustion justified (array size == GUC max). Nit: `_silence_unused_warnings_when_no_callers_yet` shim is stale — committer.rs/router.rs are the callers now → .7. |
| recovery.rs (47) | CLEAN | Trivial by design and the header explains exactly why (shmem evaporates; no submission_status; router-self-owned boot sweep does the real work). Release store / Acquire readers ✓. Nit: `connect_worker_to_spi` is unused by the body (one atomic store needs no SPI) — harmless. |
| affinity.rs (47) | CLEAN (quality only) | Default-off verified end-to-end: GUC default 0; claim-path gate is one compare when off; router stamps `affinity_owner` unconditionally (one mix64 call — measured-benign by acct-p1al-era runs). splitmix64 deterministic, no global state. The ordinal-vs-slot-index caveat (slot ≥ committer_count owns nothing, steals only) is documented in-file. Every site tagged `[acct-0usf affinity — EXPERIMENTAL/REMOVABLE]` ✓ greppable. Disposition belongs to .7 / `acct-m4g5` — record-only here. |

### B-ii.2 Unsafe inventory (45 sites: 39 production, 6 test-only)

Zero raw-pointer arithmetic outside the CV plumbing; no `transmute`; no `static mut`. By category:

| # | Category | Sites | Soundness argument |
|---|---|---:|---|
| 1 | Shmem zero-init `Default` (`std::mem::zeroed`) | shmem.rs:108/:314/:358 | All fields are atomics (zero-valid), PODs, or byte arrays. The one NOT-zero-valid embedded struct (`ConditionVariable.wakeup` proclist) is never used until the 3-state CAS gate runs `ConditionVariableInit` — the hazard is documented at both the field (:92-99) and the init fn (:377-384). |
| 2 | `unsafe impl PGRXSharedMemory` ×3 | shmem.rs:112/:318/:362 | Marker promises no heap pointers / process-local state in shmem: every field is inline POD/atomic; offsets into the arena are u32 indices, not pointers. Holds. |
| 3 | `PgLwLock::new` statics ×3 | shmem.rs:368-373 | c-string tranche names unique per region; registered via `pg_shmem_init!` in `_PG_init` (Postmaster context) before any use. |
| 4 | CV FFI (`ConditionVariableInit` / `Broadcast` / `PrepareToSleep` / `TimedSleep` / `CancelSleep`) | shmem.rs:399/:438, enqueue.rs:111/:363/:564 + 6 CancelSleep | Raw pointer is to a cluster-lifetime shmem address (documented at `backpressure_cv_ptr`); CV has its own internal slock; the sleeping backend never holds the outer LWLock (documented — the signaler would otherwise deadlock). Prepare/Cancel balanced on all six exit paths (audited: Ok, ArenaFull-deadline, Internal, QueueFull-deadline, batch-deadline, batch-Ok). |
| 5 | PG globals reads (`MyProcPid` ×3, `GetCurrentTimestamp`) | identity.rs:29, enqueue.rs:500, router.rs:98, shmem.rs:453 | Always-valid in a connected backend/BGWorker; timestamp clamped at 0 against pre-2000 wrap (documented). |
| 6 | `GetCurrentTransactionId` ×2 | enqueue.rs:97/:243 | In-transaction guaranteed (SQL-callable function); deliberately forces XID allocation (§6.1 step 4). Returns u32 `TransactionId` — see **Q13** for the xid8-epoch consequence. |
| 7 | `ProcessInterrupts` | enqueue.rs:114 | Standard cancellable-wait idiom; called outside any LWLock guard. |
| 8 | `kill(pid, 0)` ×2 | identity.rs:45/:106 | Liveness probe only (signal 0 sends nothing); ESRCH = dead, EPERM = alive-conservative; errno read immediately after the call with no intervening libc. |
| 9 | Subtx FFI (`BeginInternalSubTransaction` / `ReleaseCurrentSubTransaction` / `RollbackAndReleaseCurrentSubTransaction`) | committer.rs:631/:674/:678/:682 | Balanced on all three outcomes of `attempt_commit_phase` (Wrote→Release; Caught→Rollback; SPI-Err→Rollback); Rust panics inside the closure are caught by `PgTryBuilder::catch_others` (→ Caught → Rollback). Savepoint name is a constant with no NUL. Guard objects skipped by a longjmp are released by `AbortSubTransaction`'s `LWLockReleaseAll` — pgrx's designed-for path. |
| 10 | bench ctx/owner save-restore | committer.rs:1344-1363 | `bench_hooks`-gated only. Mirrors pgrx-bench runtime: save `CurrentMemoryContext`/`CurrentResourceOwner`, always-rollback subtx, restore both — documented at the site. |
| 11 | `ProcessConfigFile` ×2 | router.rs:109, committer.rs:140 | Standard BGWorker SIGHUP idiom. |
| 12 | `#[unsafe(no_mangle)]` ×3 | recovery.rs:38, router.rs:87, committer.rs:128 | Exported symbol names match the `set_function` registration strings byte-for-byte (verified). |
| 13 | Test-only `Box::new_zeroed().assume_init()` ×6 | arena.rs:243, payload.rs:249, cleanup.rs:221-229, router.rs:1312 | Same POD/zero-valid argument as category 1; heap-allocated because the structs exceed test-thread stacks (documented). |

### B-ii.3 Atomic-ordering table (load-bearing atomics; counters omitted — all counters are Relaxed and correct)

| Atomic | Writers (ordering) | Readers (ordering) | Verdict |
|---|---|---|---|
| `StagingEntry.valid` | enqueue CAS 0→1 **Release** (after plain stamps, under exclusive); router CAS 1→2 Acquire, 2→3 **Release** (after cg store), 2→1 Release (rollback); eject CAS 3→1 **Release** (after eject-data stores); cleanup CAS 3→0 / 2→0 Release; sweeps CAS 2→3 / 2→1 Release | router scan **Relaxed** (sound via LWLock ordering — enqueue publishes under exclusive, scan share-acquires after; same justification arena.rs documents); cleanup/sweeps Acquire; state_counts Relaxed (observability) | SOUND — data-before-flag honored at publish, stamp, and eject; the Relaxed scan deserves arena.rs-style comment (nit) |
| `StagingEntry.commit_group_id` | router store **Release** before flag CAS ✓; eject store 0 Release; sweep revert store 0 Release | cleanup + sweeps Acquire | SOUND (pairing documented in cleanup.rs header) |
| `StagingEntry.eject_count` / `last_eject_at_ns` | eject fetch_add/store **Release** before the 3→1 CAS ✓ | router cooldown Acquire ✓; cleanup Acquire ✓ | SOUND |
| `StagingQueue.head` | **never stored** | router scan start | DEAD — always 0; this *is* `acct-ozln`'s O(ring) scan (cross-ref, not re-filed) |
| `StagingQueue.tail`, `next_request_seq` | Relaxed under exclusive | Relaxed under exclusive | SOUND (lock-justified) |
| `backpressure_cv_initialized` | CAS 0→1 AcqRel, store 2 Release | load Acquire | SOUND (classic 3-state init gate) |
| `CommitterQueueEntry.valid` | router CAS 0→1 **Release** (after plain stamps, under exclusive); claim CAS 1→2 *Acquire* (store-half relaxed — benign: identity fields carry their own Release, and no reader infers identity from valid==2 alone; a stale-identity read sees gen==0 → skip, self-corrects); cleanup CAS 2→3→0 Release; poison store 4 Release; sweep CAS 2→1 Release | claim/ready/sweep Acquire ✓ | SOUND on the happy paths; see Q11 for the sweep's post-CAS clears |
| `CQE.committer_bgw_generation` / `committer_bgw_slot` | claim: gen CAS 0→g **AcqRel** (the election), slot store **Release**; finalize clears Relaxed (safe: share guard held vs router's exclusive claim); **sweep Phase-2 + claim-rollback clear Relaxed AFTER the valid CAS** | sweep Acquire; claim CAS | **Q11** — the sweep/rollback sites invert data-before-flag; weak-memory hazard (x86-immune) |
| `CQE.committer_acquired_at_ns` / `committer_tx_id` | stored at claim / cleared | **never read** | DEAD (→ Q14) |
| `recovery_complete` | store 1 **Release** (recovery worker) | router/committer/SPI load **Acquire** | SOUND (textbook gate) |
| `router_pid`, test_* flags | SPI store Release / swap AcqRel one-shots | worker Acquire/AcqRel | SOUND (test_hooks-gated reads, yojk.11 ✓) |
| `CommitterIdentitySlot.pid` / `.generation` | claim CAS AcqRel + gen fetch_add AcqRel; release: gen bump AcqRel THEN pid store 0 Release | Acquire loads + `kill()` probe | SOUND — release order makes stale references fail the generation match during the window (documented) |
| `router_max_submission_count_per_group` | CAS-max loop Relaxed | getter Relaxed | SOUND (monotonic max, no data behind it) |
| Arena `freelist_head_offset` / `bump_offset` / counters | Relaxed under exclusive | Relaxed (getters under share) | SOUND (lock-justified, documented in module header) |
| Harness-facing counters (drains, trx_committed, spans, …) | fetch_add Relaxed | getter Relaxed | SOUND (pure counters, no ordered data) |

### B-ii.4 Prior-finding re-verification

| Finding | Fix lineage | State | Evidence |
|---|---|---|---|
| D8.1 — 13 dead shmem fields (never read NOR written) | yojk.1 (`d891a9a`) | **still fixed** | grep for all 13 names (`router_order_sensitive_groups_total`, `committer_tm09_*` ×3, `committer_stage_*` ×5, `audit_*` ×4) → 0 hits. The Q14 octet is a *different* class (written-but-unreadable), not a D8.1 regression. |
| P2.1 — UNIQUE-survivor poison granularity | yojk.9 | **still fixed** | DuplicateRace re-drive shipped: 23505 → re-dedup against `trx` → drop offender → re-drive rest (committer.rs:512-559); bounded (`max_redrives = len+1`); zero-removed safety valve poisons; `duplicate_redrives_total` counts both. Scope caveat → Q8: the re-drive is 23505-specific by design. |
| P2.2 — `parse_caller_status` fragility | yojk.10 | **still fixed** | `Unrecognized` distinct from NULL→`Unknown`; wording drift → WARNING + never-kept (fail-closed) (:1118-1126, :1185-1195); both pinned by unit tests (:1461-1477). Sibling gap found at the same surface → Q9 (the *query-error* path fails open). |
| P2.3 — test-injection reads on production path | yojk.11 | **still fixed** | All injection bodies + call sites + pause-flag reads under `#[cfg(feature = "test_hooks")]` (router.rs:117/:335-363, committer.rs:146/:653-659/:855-925); production stamp block (router.rs:326-334) is injection-free. |
| P2.4 — per-submission `snapshot.clone()` | yojk.12 (measured, kept) | **still present, still justified** | The trial clone (committer.rs:705) *is* the drop-and-continue mechanism; apply-span microbench (acct-q6sx) bounds whole-apply at ~36-40 µs/trx, so the clone is not the ceiling. |
| yojk.15 — arena lines-block free | yojk.15 | **still fixed** | `StagingEntry.line_offset` carried on the slot (shmem.rs:54-59), stamped at push, freed on all paths: cleanup 3-block free (:81-96), enqueue QueueFull rollback, batch remainder-free; cleanup tests assert 3-block accounting. |

### B-ii.5 Pre-pinned checkpoint resolutions (from the B-i walk)

**(a) Q1 routed-side blast radius — CONFIRMED, filed as Q8.** The negative-`value_sum` shape
(acct-mvq4.22) raises no `LedgerError` at plan time — `wac::aggregate_deplete` computes the
negative i64 happily, so `plan_apply_provisional` succeeds and drop-and-continue never sees it.
It surfaces as a raw 23514 from the aggregate UPSERT inside `plan_and_write`, inside the §6.8
subtx. `classify_phase_error` maps 23514 → **Fatal → Poisoned**: the WHOLE commit_group moves to
the valid==4 dead-letter, all submissions lost — up to `batch_size_max` (200) siblings,
including disjoint-pool innocents co-packed by `pack_disjoint`. Retry cannot help (deterministic);
the yojk.9 re-drive is 23505-only (verified: `classify_phase_error` :820-827 has exactly three
non-fatal arms). Worse than one lost group: the pool condition persists, so every subsequent
group containing a partial standard-basis depletion of that pool also poisons — a recurring
group-killer that additionally consumes a CQ ring slot per occurrence (poisoned slots are never
reclaimed; 2048 total).

**(b) Routed-side LedgerError handling — uniform drop-and-continue, CONFIRMED.** All 8 variants
take the same path: `plan_apply_provisional` Err → trial clone discarded → `dropped += 1`
(committer.rs:716) → `dropped_submissions_total`. No variant retries, poisons, or
distinguishes — `ledger_error_map.rs` is direct-c-only; the routed flavor never converts
LedgerError to SQLSTATE. Caller surface: no trx row ever exists for a dropped submission; the
staging slot is released with the group (3→0); the only signals are the counter and the
caller's own poll timeout ("trx exists iff committed" — recovery.rs header). Consistent with
§6.4's documented semantics; the silent-by-design asymmetry vs direct (which ereports a typed
SQLSTATE) is documented in the spec and POC-REPORT. Same uniform treatment for pre-decode drops
(malformed posted_at / unknown line_type → `predrop`).

**(c) Retryable classification — exact, comments match code.** `classify_phase_error` matches
precisely {`ERRCODE_T_R_DEADLOCK_DETECTED` (40P01), `ERRCODE_T_R_SERIALIZATION_FAILURE` (40001)}
→ Retryable; 23505 → DuplicateRace; **everything else Fatal → poison** — 55P03 included, as A9
documents on the doc side. `MAX_DEADLOCK_RETRIES = 5`; backoff = 10 ms·2^(n−1) capped 1 s —
comment (:842-843) and code (:844-847) agree. The §6.8 module-header narrative (:35-48) matches
the implementation in every particular checked.

### B-ii.6 Findings

| ID | Sev | Verdict | Finding |
|---|---|---|---|
| Q7 | **P2** | CODE-DEFECT (race, latent) | **Eject-vs-stamp race: `classify_and_eject` mutates staging entries unconditionally, racing the router's post-publish stamp loop — an ejected submission can be silently lost or its slot permanently leaked.** Root: `emit_commit_group` publishes the CQ entry (claimable) BEFORE the per-entry data-before-flag stamps (router.rs:269-310 vs :321-363), so a committer can claim, decode, and reach triage while staging entries are still at valid==2. The eject path (committer.rs:1166-1180) bumps `eject_count`, zeroes `cg_id`, and CASes 3→1 with **no state gate** — against a slot at 2 the CAS fails but the mutations stand. Two bad interleavings (both walked): (a) eject completes before the router's stamp → router re-stamps cg + CAS 2→3 succeeds → cleanup sees cg-match at valid==3 → **releases the slot as if committed** — the in-progress caller's submission is silently discarded (no trx, no retry, no counter); (b) router's cg store lands, then eject's cg-0 store, then router's CAS 2→3 → slot at valid==3/cg==0/eject>0 — cleanup skips both CAS arms, router scans only valid==1, sweeps only touch valid==2 → **stuck + arena held until postmaster restart**. Preconditions: caller-tx still open at triage (the *designed-for* §6.1 usage — harness autocommit callers almost never arm it, which is why zero ejects appear in all shipped results) + router preempted mid-stamp-loop (noisy-host plausible). Fix shape: gate the eject mutations on the 3→1 CAS succeeding (mutate-after-CAS), or stamp staging before publishing the CQ. |
| Q8 | **P2** | CODE-DEFECT (amplification, latent) | **Routed amplification of Q1: a deterministic per-submission write-phase error (23514 negative value_sum) poisons the entire commit_group — innocent siblings share its fate, recurringly.** Full walk in §B-ii.5(a). Drop-and-continue isolates only plan-time failures; Q1's class bypasses ledger-core typing entirely, and the §6.8 machinery has no per-submission re-drive for non-23505 deterministic errors. Blast radius: up to bsm=200 submissions/occurrence (pack_disjoint co-packs unrelated pools), plus one permanently-consumed CQ slot per occurrence (2048-slot ring), recurring for every future group touching the poisoned-pool shape. Same reachability gate as Q1 (no harness pool is standard-basis), so all shipped results unaffected. Fix is Q1's fix (acct-mvq4.22); a routed-side hardening option (treat deterministic data-error SQLSTATEs like 23505: identify offender, drop, re-drive) is the fix-issue's call. X-refs: acct-mvq4.22, P2.1/yojk.9 lineage. |
| Q9 | **P2** | CODE-DEFECT (fail-open, latent) | **xid-triage SPI-error swallow inverts the caller-tx contract: a failed `pg_xact_status` query keeps EVERYTHING.** `classify_and_eject`'s SPI closure early-returns `None` on any error (`.ok()?` ×3) and the caller does `.unwrap_or_default()` (committer.rs:1097-1114) → empty status map → every lookup misses → `unwrap_or(CallerTxStatus::Unknown)` (:1141) → Unknown = **kept** (:1143). One SPI failure commits work for callers that may be in-progress or aborted — precisely the atomicity the eject loop exists to enforce. Directly contradicts the function's own fail-closed posture for wording drift (yojk.10 made Unrecognized never-keep; the *query-error* path fails open). Reachability: SPI failure in a healthy BGWorker is rare (OOM, interrupt), but the failure direction is wrong. Note the both-ways trap: propagating Err instead → TxError → cleanup releases slots → submissions silently LOST (also wrong). Correct shape: treat triage failure as eject-all (slots return to pending; next tick retries). |
| Q10 | **P2** | CODE-DEFECT (recovery gap) | **Committer ERROR-exit orphans its in-flight commit_group until a router restart that never naturally happens.** The only trigger for §6.5 Phase-2 dead-committer reclaim is `try_recover_router_orphan` at *router* startup (router.rs:105); committers run no sweep at their own (re)start. A committer that exits via ERROR→FATAL→exit(1) — uncaught tick error outside the write subtx (e.g. the label-cache query failing, the documented "abort the committer tick" path at committer.rs:104-106), or an operator `pg_terminate_backend` mid-pipeline — is respawned in 5 s, takes over its old identity slot (generation bumps), and its orphaned CQ entry sits at valid==2 with a dead identity **indefinitely**: claim only takes valid==1, no periodic sweep exists, and the router has no reason to restart. The group's staging slots stay at valid==3 (callers poll forever); a genuine crash (signal) is fine — postmaster reinitializes and shmem evaporates. The acceptance suite exercises the reclaim MECHANISM via synthetic injection + manually-invoked sweep, so the missing TRIGGER is invisible to it. Fix shape: run the Phase-2 sweep at committer startup too (it is already idempotent and safe by CAS election), or sweep periodically from the router tick. |
| Q11 | P3 | CODE-DEFECT (weak-memory, x86-immune) | **Sweep Phase-2 revert and claim-rollback clear CQ identity AFTER the valid CAS with Relaxed stores — inverting the codebase's own data-before-flag discipline.** router.rs:897-901 CASes valid 2→1 (Release) then stores slot=MAX / gen=0 (Relaxed); committer.rs:294-295 mirrors the shape on claim rollback. A racing claimer elects on gen==0 (AcqRel) and stores its own slot ordinal — but nothing orders the sweep's earlier slot=MAX store before the claimer's store on non-TSO hardware, so the MAX can clobber the new owner's slot field: an in-flight entry whose owner reads as slot=u32::MAX → `is_committer_alive` false → a second committer claims the SAME group concurrently → both run cleanup → `free_committer_queue_arena` double-free → duplicate freelist entries → two future allocs can return the same offset (payload cross-corruption; the arena's cycle guard does not catch duplicates). Impossible on x86-TSO (stores retire in program order), so the shipped platform is safe; real on ARM. Contrast `finalize_committer_queue_slot`, whose identical-looking trailing clears ARE safe (share guard held vs the router's exclusive-only claim of empty slots). Fix shape: clear identity BEFORE the valid CAS (data-before-flag), three-line reorder. |
| Q12 | P3 | STRUCTURAL (lock hygiene) | **`attempt_commit_phase` holds the COMMITTER_QUEUE LWLock (share) across the entire SPI write phase** — pool_lock FOR-UPDATE waits (up to deadlock_timeout), hydrate, and the full batched write all run under the guard taken at committer.rs:645, kept alive solely for three span-counter `fetch_add`s. The router's CQ claim needs the exclusive lock (router.rs:272) and PG LWLocks admit new sharers past a waiting exclusive, so with cc=4 under load the router's emission can starve until a gap with zero in-flight write phases — coupling committer write latency into router formation cadence (an unmeasured interaction; formation-cadence attribution in the hh7b/ozln work rests on the tick + scan). No deadlock cycle exists (verified: cleanup/claim take share; callers never touch CQ; row-lock holders don't need this lock to commit), and longjmp'd guards are reclaimed by `AbortSubTransaction`'s LWLockReleaseAll — stall-only, not a wedge. Fix shape: drop the guard before `acquire_pool_locks` and re-acquire transiently per fetch_add (the surrounding code already uses statement-scoped guards everywhere else). X-refs (sibling LWLock-scan sites, not re-filed): acct-ozln, acct-m4g5.2. |
| Q13 | P3 | CODE-DEFECT (production-later) | **32-bit xid stored where xid8 semantics are required.** `ledger_enqueue_trx_c` stages `GetCurrentTransactionId()` (u32 `TransactionId`) widened to u64 (enqueue.rs:97/:243); triage feeds it to `pg_xact_status(x::xid8)`, which expects an epoch-qualified FullTransactionId. Below 2^32 transactions the epoch is 0 and everything agrees; after xid-epoch advance, staged values reference epoch-0 xids → `pg_xact_status` returns NULL (too old) → Unknown → **keep** — in-progress callers' work would be committed. Unreachable in any PoC run (epoch 0 throughout); a correctness time-bomb for a long-lived production deployment. Fix: `GetCurrentFullTransactionId()`. |
| Q14 | P3 | DEAD-OBSERVABILITY | **Nine written-but-unreadable shmem fields, one with a false doc-comment.** No pg_extern, test, or internal reader consumes: `free_slot_wake_count` (doc claims "tests assert this grew" — none does), `committer_claim_count`, `eject_total_count` (ejects are a designed mechanism with zero observability surface), `router_cross_commit_group_for_update_waits` (chunk-split counter — would directly support the §14.2/A1 order-divergence story), `StagingEntry.backend_pid`, `.correlation_id` (zeroed only), `.trx_type_id` (the committer decodes trx_type from the JSON payload instead), `CQE.committer_acquired_at_ns`, `.committer_tx_id` (only ever cleared). Distinct class from D8.1 (those were never-written carryover; these are live writes with no reader — maintenance cost plus misleading "observability" the harness cannot actually see). Disposition per field at .7: expose (eject_total_count, cross_commit_group_waits arguably earn getters) or delete. |

**B-ii summary.** 11/11 modules versed: 8 CLEAN (several with notes), router.rs + committer.rs
carry the findings. The §6.4/§6.8 pipeline, claim election, 3-CAS cleanup, recovery sweep
internals, arena, payload codec, and identity registry all check out against their own
documentation — the module-level doc quality is the best in the workspace, and every prior
routed finding re-verifies still-fixed (§B-ii.4). The new findings concentrate at two seams the
prior passes never walked end-to-end: the **eject path racing the publish-before-stamp window**
(Q7) and the **failure-direction of the triage/recovery machinery** (Q9 fail-open swallow, Q10
trigger gap, Q8 poison blast radius). Unsafe: 45 sites, all category-justified, none
load-bearing-undocumented (§B-ii.2). Atomics: data-before-flag holds at every publish/eject
site; the two inversions are Q11's sweep/rollback clears (x86-immune). 4×P2 + 4×P3, all filed
as epic children; nothing here disturbs any shipped measurement (every P2 sits behind a
precondition no bench run arms: open caller txns at triage, standard-basis pools, SPI failure,
or committer ERROR-exit).

## C-i. coverage map + gap classification (acct-mvq4.5 — 2026-06-06)

**Method.** Static census only — no cluster contact, no GUC changes (the suite RUN is C-ii).
Every `.rs` file in all 5 crates censused with annotation-aware patterns (`#[ignore` open-bracket
for the string-reason form; `#[tokio::test` counted separately from plain `#[test]`;
`proptest!` blocks expanded by reading). **Population: 193 test fns** — 150 offline units
(ledger-core 38 + spi-common 2 + routed-c src-inline 71 + harness src-inline 39), 3 `#[pg_test]`
(routed ×2, direct ×1), 40 `#[ignore]` integration (direct-c 14 + routed-c 24 + harness smokes 2).
Dedup honored: gap rows cite `acct-uwsp`, `acct-mvq4.22/.25/.28/.29/.30/.31/.35` where those
issues already carry the coverage implication; only NEW untested surfaces earn C# findings.

### C-i.0 Census corrections (pinning exact counts; §0.6-style)

1. **routed-c integration tests are `#[tokio::test]`-style too** — §0.6 correction 6's second
   clause ("routed-c integration tests are plain `#[test]`") is itself wrong. ALL `tests/`-dir
   integration tests in the three test-bearing crates are tokio-style; routed-c's plain-`#[test]`
   population is entirely src-inline. (§0.3's class-(b) *counts* were nonetheless correct.)
2. **harness carries 39 src-inline pure units** across 7 files (driver_common 1, driver_routed 2,
   measure 3, pool_universe 2, report 4, scenarios 9, workload 18) — counted by NO class of the
   §0.3 matrix as written: class (a) omits `-p ledger-harness`; class (d)'s `-- --ignored` runs
   only the 2 smokes. Matrix correction in C-i.3.
3. Exact src-inline counts (correcting B-ii prose approximations): router **34** (not ~30:
   affinity_group ×7, union_find ×3, cooldown ×5, histogram ×1, restamp ×4, revert_orphan ×5,
   pack ×9), cleanup **8** (not 9), payload **7** = 6 plain + 1 proptest property
   (`round_trip_property`, 64 cases — the workspace's only proptest site), arena 11 ✓,
   enqueue 6 ✓, committer 5 ✓.
4. `#[ignore]` reason census (40): `"needs running poc_v3_1 with ledger_direct_c installed"` ×15
   (direct-c 14 + smoke_measure), `"…with ledger_routed_c (test_hooks) preloaded"` ×19,
   `"…with ledger_routed_c preloaded"` ×5, bare `"needs running poc_v3_1"` ×1 (smoke_sampler).
5. ledger-core declares `proptest` as a dev-dependency but contains no proptest usage → .7.

### C-i.1 Coverage map

**(a) SPI / entry-point surface** (rows per the §0.2 enumeration; getter/hook families grouped):

| Entry point | Units | Integration (`#[ignore]`) | pg_test | Property | Smoke / bench | Verdict |
|---|---|---|---|---|---|---|
| `ledger_submit_trx_c` | full plan/apply logic via ledger-core 38 (the SPI shell itself has none) | acceptance_direct_methods ×10 + acceptance_direct_lock_and_concurrency ×3 | — | `invariants_hold_across_random_submissions`; + baseline leg of all 3 routed properties | smoke_measure (~50 receipts); equivalence subcommand | **COVERED** — best-covered surface in the workspace |
| `ledger_enqueue_trx_c` | helpers only (enqueue 6: pack_pool_ids ×4, trx_type_to_id ×2 — the push loop is integration-covered) | all 4 routed acceptance binaries (21 tests) drive it | — | ×3 (`routed_aggregate_qty_equivalent…`, `…replay_deterministic`, `…unit_cost_is_value_weighted`) | every routed bench | **COVERED** |
| `ledger_enqueue_trx_batch_c` | none (batch loop, deadline remainder-free, per-chunk lock cycling all untested) | **none** | — | **none** | harness `--batch-size>1` arm + run-1sku-batched.sh / run-caller-batch-probe.sh | **SUITE-DARK → C1** |
| `staging_state_counts` / `staging_request_seq_max` | — | consumed by routed acceptance (2 / 1 refs) | — | — | bench | covered-as-observability |
| `committer_queue_state_counts` / `ready_commit_groups` | — | consumed by routed acceptance (1 / 2 refs) | — | — | — | covered-as-observability |
| lib.rs getter family ×36 | — | committer counters polled via `await_committer_stat` + `arena_outstanding` + `recovery_complete` | — | property tests poll counters | measure.rs consumes 25+ (counters + spans) | covered-as-family; the 9 Q14 fields have NO getter (`acct-mvq4.35`) |
| test_hooks ×10 | — | 9/10 consumed by the acceptance suite (common/mod.rs) | — | — | — | test infra; `test_router_pid` has ZERO consumers → .7 zero-ref disposition |
| `bench_apply` (bench_hooks) | — | — | — | — | bench-apply-inproc.sh only | bench infra by design → .7 |
| hellos | — | — | direct ×1 (`hello_pg_extern_reachable`); routed ×2 (+ `shmem_regions_visible`) | — | — | covered |

**(b) Per-module rows** (test-kind → file:test-name or none; indirect = exercised through a
higher-layer suite):

*ledger-core (12 src + 2 tests-dir files):*

| Module | Coverage | Cell |
|---|---|---|
| lib.rs | n/a | module map / re-exports |
| error.rs | indirect | every variant raised across strict/provisional tests; consumer map compile-enforced (B-i.3) |
| method.rs | tests-dir | plan_apply_strict ×17 (dispatch incl. `unknown_pool_raises`, `aggregate_only_for_wac_and_std`) |
| fifo.rs / lifo.rs | tests-dir | `strict_fifo_returns_method_mismatch` / `strict_lifo_returns_method_mismatch` |
| numeric.rs | **11 src units** | banker_div full §3.0 case table incl. i128-limit + `wac_formula_overflow_regression_needs_i128` |
| plan.rs | tests-dir | coalesce ×2 (provisional) + LineType bijectivity via spi-common decode tests |
| snapshot.rs | indirect | exercised by all 27 plan_apply tests (pure data + resolve fns; no in-file units — fine) |
| wac.rs | tests-dir | strict wac ×5 + provisional running-avg ×2; over-book deplete shape untested → gap row 10 (`acct-mvq4.22`) |
| standard.rs | tests-dir | strict std ×5 incl. favorable flip + equal-cost-no-leg |
| specific.rs | tests-dir + integration | strict ×3 + `specific_receipt_then_depletion_materializes_and_links` / `specific_second_receipt_rejected_while_stocked` |
| provisional.rs | tests-dir | plan_apply_provisional ×10 (both bases, NULL source-link, dispatch-to-strict) |

*ledger-spi-common (5):*

| Module | Coverage | Cell |
|---|---|---|
| lib.rs | n/a | — |
| line_type.rs | **2 src units** | decoder bijectivity (9/9 + unknown→None) |
| pool_lock.rs / hydration.rs / bulk_write.rs | indirect only | ZERO direct tests (SPI-bound; not unit-testable outside PG). Covered via BOTH flavors' acceptance suites — single-sourced post-yojk.2, so the direct path exercises the per-submission variants and the routed path the batch variants (8.1b/8.2b/8.4b). Adequate; noted, not a finding |

*ledger-direct-c (3):*

| Module | Coverage | Cell |
|---|---|---|
| lib.rs | pg_test ×1 | `hello_pg_extern_reachable` |
| ledger_error_map.rs | indirect | 4 of 8 SQLSTATE arms raised through the SPI shell (`std_without_standard_cost_raises`, `missing_posting_account_map_raises`, `insufficient_inventory_raises_and_rolls_back`, `specific_second_receipt_rejected_while_stocked`); MethodMismatch / UnknownPool / MissingVarianceAccount / Overflow raised only in core units, never through SQL. Map is mechanical + compile-enforced (B-i.3) — acceptable |
| submit.rs | integration | 13 acceptance + 1 property (14 `#[ignore]`) |

*ledger-routed-c (11):*

| Module | Coverage | Cell |
|---|---|---|
| lib.rs | pg_test ×2 | hello + `shmem_regions_visible`; getters via harness/tests (map (a)) |
| shmem.rs | pg_test + indirect | structure via `shmem_regions_visible` + every integration transitively; `now_us`/`now_ns` clamps and CV 3-state init have no direct unit (micro) |
| enqueue.rs | **6 src units** + integration | helpers unit-tested; push path via acceptance_routed_enqueue ×5 + everything transitively; backpressure FAILURE arms test-dark → **C2** |
| router.rs | **34 src units** + integration | grouping/cooldown/restamp/revert/pack fully unit-pinned; live via acceptance_routed_affinity_grouping ×4 + boot sweep |
| committer.rs | **5 src units** + integration | decode/parse_caller_status/identity-TL units; pipeline via acceptance_routed_committer ×8 + orphan ×3 (deadlock retry, fatal poison, recovery-op) |
| arena.rs | **11 src units** + integration | incl. corrupted-cycle termination + stress; live reclaim via `committed_group_reclaims_all_arena_blocks` |
| payload.rs | **6 + 1 property@64** | round-trip, tamper, OOB, error-path frees (`outstanding == 0` pinned — yojk.5) |
| cleanup.rs | **8 src units** + indirect | all 3 CAS cases + eject-blocks + cg-mismatch + idempotence; live on every committed group |
| identity.rs | indirect only | dead-pid (ESRCH) reclaim via synthetic orphan acceptance + `takeover_count`; EPERM-alive arm and 64-slot-exhaustion panic untested (micro — folded into gap row 1) |
| recovery.rs | integration | `recovery_complete_at_boot_and_system_operational` |
| affinity.rs | **none** | zero tests (default-off EXPERIMENTAL, acct-0usf). NOTE: acceptance_routed_affinity_grouping tests the ROUTER's union-find grouping, not this module — name collision. Disposition .7 / `acct-m4g5` |

*ledger-harness (15 src + 2 smokes):*

| Module | Coverage | Cell |
|---|---|---|
| cli.rs / main.rs | none | clap glue; parse exercised by every bench invocation — (iii) |
| driver_common.rs | 1 src unit | `build_lines_json_shape_matches_spi_contract` |
| driver_direct.rs | **none** | incl. the batched arm — POC-REPORT (b)'s measurement path; honesty gap already filed (`acct-mvq4.25`); gap row 18 |
| driver_routed.rs | 2 src units | `derive_report_*` (pure math); pacer/observer logic bench-validated only |
| equivalence.rs | none | self-checking subcommand (model quiescence assert per B-i.4); IS itself the §11.1 checker |
| measure.rs | 3 src units + smoke | histograms + WAL math; `collector_captures_nonzero_deltas` (self-seeding: TRUNCATE + minimal fixture) |
| sampler.rs | smoke only | `sampler_captures_ticks` |
| pool_universe.rs | 2 src units | deterministic Mixed 50/30/20 + enum-text map |
| report.rs | 4 src units | percentiles, wait-event sort, path shape, JSON round-trip |
| scenarios.rs | 9 src units | incl. `by_id_resolves_all_canned_scenarios` (×21) + family-shape pins |
| seed.rs | none | any-row idempotency heuristic noted at Q6 (`acct-mvq4.27`) |
| workload.rs | 18 src units | distributions, multi-touch, pareto edge-clamps |

### C-i.2 Untested-surface table (3-way verdicts)

Verdicts: **(i)** verdict-threatening for POC-REPORT conclusions / **(ii)** production-blocking-later /
**(iii)** nice-to-have. Rows 1–9 are the charter-listed surfaces; 10–18 are .3/.4-surfaced additions.

| # | Surface | What IS tested (evidence) | Untested residue | Verdict | Cite / file |
|---|---|---|---|---|---|
| 1 | Staging-ring crash-recovery completeness | Synthetic orphan-CQ + boot sweep (orphan_recovery ×2); router-death restamp/revert pure helpers (9 units); dead-pid ESRCH reclaim via synthetic injection | The ERROR-exit TRIGGER (no sweep at committer respawn); EPERM-alive arm; 64-slot exhaustion; literal postmaster-crash (§9.3 bullet 6, A-dim noted "indirect") | (ii) | `acct-mvq4.31` (Q10) — no new filing |
| 2 | Arena overflow / fragmentation | Unit-complete: `alloc_returns_none_when_arena_full`, first-fit ×2, corrupted-cycle termination, stress (11) | End-to-end exhaustion under live enqueue (ArenaFull → CV wait → deadline error); no integration fills 128 MB | (ii) | folds into **C2** |
| 3 | Drop-and-continue under load | Mechanism: `failed_submission_excluded_via_drop_and_continue`; uniform plan-time isolation verified by read (B-ii.5b) | Sustained mixed-failure load across many concurrent groups — every long-dur bench ran 0 drops (POC-REPORT (g)) | (iii) | mechanism pinned; no headline rests on it |
| 4 | Dedup under sustained deadlock injection | Deadlock retry alone (`deadlock_during_write_retries_then_commits`, count=2); preflight dup alone; racing-dup re-drive alone | Combined deadlock-retry × duplicate (the A9 retry-skips-re-dedup shape — safety rides UNIQUE backstop → DuplicateRace, reasoned never exercised); sustained-injection soak | (ii) | **C3** (NEW) |
| 5 | pack_disjoint at extreme Pareto | 9 pack units (exact partition, caps, document-atomicity); measured live at zipf-1.5 hot pool (latency_vs_load s2 — the production-shaped extreme) | Adversarial synthetic extremes beyond measured shapes | (iii) | — |
| 6 | Idempotency across BGWorker restarts | Dedup is durable-state-based (trx table); preflight + UNIQUE backstop both tested | Resubmit-after-committer-restart end-to-end (overlaps the Q10 orphan scenario) | (ii) | `acct-mvq4.31` overlap |
| 7 | GUC SIGHUP reload mid-run | nothing | Entirely untested (benches reload between cells; router busy-loop defers SIGHUP under backlog — B-ii notes; `committer_count` Sighup-context benignity reasoned only) | (iii) | PoC scope |
| 8 | Provisional cost-variance bound — empirical vs §3.0/§14.3 | Receipts-only unit-cost EXACTNESS (property `…unit_cost_is_value_weighted`, banker_div assert); qty equivalence (POC-REPORT (d)) | Depletion-cost drift magnitude provisional-vs-strict — spec 12-P4 explicitly excluded variance magnitude from the harness (honored as written, A-dim) | (ii) | documented-deferred (§7 recalc tier) |
| 9 | Multi-DB | — | §13 out-of-scope; verified genuinely absent (A-dim §13 row) | N-A | — |
| 10 | Q1 over-book standard-basis depletion (negative value_sum) | All depletion value_sum asserts are positive-residual; every harness pool seeded `running_avg` (pool_universe.rs:159) | The mutation-shape assert IS the repro test the fix will add | carried by finding | `acct-mvq4.22` |
| 11 | Eject path under open-caller-tx load | zero ejects in every shipped result; nothing arms it | Open-tx callers racing the stamp loop (the §6.1 designed-for usage) | carried by finding | `acct-mvq4.28` (Q7) |
| 12 | Triage SPI-failure path | — | fail-open swallow never driven | carried by finding | `acct-mvq4.30` (Q9) |
| 13 | Data-shaped write-phase poison | Poison via `inject_fatal` only (`fatal_error_during_write_poisons_commit_group`) | A real 23514-class data error driving whole-group poison | carried by finding | `acct-mvq4.29` (Q8) |
| 14 | Eject observability | — | `eject_total_count` + 8 siblings have no getter → SQL-level tests CANNOT assert eject counts | carried by finding | `acct-mvq4.35` (Q14) |
| 15 | Cross-method-mixed equivalence | equivalence subcommand + properties run per-mix | mixed-method interleaving equivalence | (ii) | OPEN `acct-uwsp` — never re-file |
| 16 | Enqueue backpressure FAILURE arms | The wait/recover arm is heavily empirically exercised (hh7b full-blast multi-second acks; zero drops at 14k offered) | Queue-full CV-timeout → `ERRCODE_INSUFFICIENT_RESOURCES` and arena-full-deadline error exits: no test drives either; no bench ever fired them (errors=0 everywhere) — the zero-drop story's error surface is test-dark | (ii) | **C2** (NEW) |
| 17 | `ledger_enqueue_trx_batch_c` | enqueue-core shared with single-push (that part covered) | The entire batch entry point: chunk loop, deadline remainder-free, per-chunk lock cycling, partial-push return shape | (ii) | **C1** (NEW) |
| 18 | direct-batched mode semantics | bench-measured (POC-REPORT (b)) | No acceptance test pins whole-batch-rollback semantics; conclusion direction conservative per Q4 | (iii) | `acct-mvq4.25` (Q4) |

No gap classifies as **(i) verdict-threatening** — consistent with dimensions A/B: every
POC-REPORT conclusion rests on surfaces that are either tested or measured, and every latent
defect found so far sits behind a precondition no shipped run arms.

### C-i.3 Invocation-matrix runnability (static prereq check for C-ii)

| Class | Runnable as written? | Evidence | Correction for C-ii |
|---|---|---|---|
| (a) pure units | YES + **extend** | ledger-core is pgrx-free (serde/chrono/uuid/thiserror); spi-common default `pg18` compiles offline | run `cargo test -p ledger-core -p ledger-spi-common -p ledger-harness` — the `-p ledger-harness` addition covers the 39 src units no class ran as written. Expected: **79** tests |
| (b) integration | YES | `scripts/run-tests.sh` discovers `^(acceptance\|property)_.*\.rs$` → exactly the 8 binaries (6 acceptance + 2 property); WITH_TEST_HOOKS install + per-binary `docker restart` + `--ignored --test-threads=1` in-script; FAIL_FAST=1 | Expected: **38** ignored (14 direct + 24 routed). Cluster-touching — the audit's only such step |
| (c) pg_test | YES | cargo-pgrx **0.18.0** == workspace pin `=0.18.0`; `~/.pgrx/config.toml` has `pg18 = /usr/bin/pg_config` + `data-18` initialized; routed-c `pg_test` module sets `shared_preload_libraries='ledger_routed_c'` (shmem boots in the pgrx-managed instance); direct-c empty conf | **Count correction to §0.3**: `cargo pgrx test pg18` runs the src units TOO, not just hellos — expected routed-c **73** (71 units + 2 pg_test), direct-c **1**. The `tests/` binaries compile but stay skipped (`#[ignore]`) — no shared-cluster contact |
| (d) harness smokes | YES | DSN-pinned `postgres://acct:acct_dev@localhost:5111/poc_v3_1`; smoke_measure is self-seeding (TRUNCATE + minimal fixture — state-destructive on poc_v3_1); prereq scripts exist (create-poc-v3-1-db.sh, run-migrations.sh, install-direct-c.sh) | Expected: **2**. Run after (b) or re-seed afterward — the TRUNCATE wipes any seeded universe |

No class overlaps another's test population; corrected totals 79 + 38 + 74 + 2 = **193** ✓ census.

### C-i.4 Findings

| ID | Sev | Verdict | Finding |
|---|---|---|---|
| C1 | P3 | COVERAGE-gap (entry point) | **`ledger_enqueue_trx_batch_c` is suite-dark: one of the four production SPI entry points has zero test coverage of any kind.** No acceptance, property, unit, or pg_test invokes it (grep: only harness driver_routed `--batch-size>1` + run-1sku-batched.sh / run-caller-batch-probe.sh). The batch-specific logic — multi-chunk push loop, per-chunk staging-lock acquisition cycling, deadline-expiry remainder-free (arena blocks for unpushed envelopes), partial-push return contract — is verified only by reading (B-ii enqueue walk: CLEAN) and by bench behavior (acct-ruex). The shared core (`push_entry_into_queue`) is well-covered via the single-push path. Production-blocking-later: any caller adopting the batch API relies on untested seams; the deadline remainder-free path in particular is arena-leak-critical (yojk.15 class). Fix shape: extend acceptance_routed_enqueue with a batch round-trip + a deadline-partial-push case asserting `arena_outstanding` returns to 0. |
| C2 | P3 | COVERAGE-gap (failure arms) | **Enqueue backpressure FAILURE arms are test-dark: the queue-full CV-timeout → `ERRCODE_INSUFFICIENT_RESOURCES` exit and the arena-full-deadline exit have never executed — not in any test, not in any bench.** The wait-and-recover arm is heavily empirically exercised (hh7b full-blast: multi-second acks = CV-wait engaged; zero drops at 14k offered), but no run ever waited past `queue_full_timeout_ms` (5 s) or filled the 128 MB arena, and no test drives either error exit (grep tests for queue_full/INSUFFICIENT/ArenaFull: 0). The zero-drop headline (POC-REPORT, latency_vs_load) leans on backpressure-by-blocking whose error surface is unverified; A10 (`acct-mvq4.18`) covers the spec-doc silence — this finding covers the test gap. CV-protocol balance on those paths was verified by read (B-ii.2 cat. 4). Fix shape: test_hooks-shrunk ring (or GUC-tiny `queue_full_timeout_ms`) acceptance case asserting the SQLSTATE and post-error slot/arena cleanliness. |
| C3 | P3 | COVERAGE-gap (interaction) | **The §6.8 retry path's re-dedup deferral is untested: no test combines deadlock-retry with duplicate arrival, and no soak sustains injection.** `deadlock_during_write_retries_then_commits` (count=2) proves retry-then-commit alone; `duplicate_source_caught_by_preflight_dedup` and `racing_duplicate_redrives_group_minus_offender` prove dedup arms alone. The shipped retry shape (A9 / `acct-mvq4.17`: subtx re-attempt skips re-triage AND re-dedup; safety argument = UNIQUE backstop converts the missed duplicate into 23505 → DuplicateRace re-drive) is reasoned-only — the specific interleaving "duplicate lands between attempt N's rollback and attempt N+1's write" has never executed. The injection hooks needed (`set_inject_deadlock_count` + a concurrent duplicate submitter) already exist. Fix shape: one acceptance case arming both, asserting the offender drops and survivors commit exactly once. |

**C-i summary.** 193 test fns censused and mapped; every §0.2-enumerated entry point has a
verdict (one suite-dark → C1); all 46 modules across 5 crates have coverage rows (zero-direct-test
modules: spi-common's 3 SPI-bound helpers — adequately covered via both flavors; identity/
affinity/shmem — micro-gaps noted; harness glue — (iii)). 18 untested-surface rows: 9
charter-listed + 9 audit-surfaced; **none verdict-threatening** — 7 carried by already-filed
findings or OPEN issues (dedup honored), 3 NEW (C1–C3, all P3 production-blocking-later).
The §0.3 matrix is runnable for C-ii with two corrections: class (a) extends to
`-p ledger-harness` (39 otherwise-unrun units) and class (c) expects full src-unit counts
(73/1), not just hellos. Expected per-class totals: 79 / 38 / 74 / 2 = 193.

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
| Q1 | P2 | CODE-DEFECT (latent) | Provisional standard-basis partial depletion emits negative value_sum → 0007 CHECK violation, not typed error | `acct-mvq4.22` |
| Q2 | P3 | MISLEADING-CODE | scenarios.rs:158 unreachable `unwrap_or` arm + hardcoded s6 caller-count literal | `acct-mvq4.23` |
| Q3 | P3 | STALE-comment | sampler.rs bucketing-rules comment stale vs code (LWLock split-out, IPC) | `acct-mvq4.24` |
| Q4 | P3 | MEASUREMENT-honesty | direct-batched counts pre-rollback successes in throughput | `acct-mvq4.25` |
| Q5 | P3 | SCRIPT-hygiene | relic committer_affinity GUC; no-pin scripts assume ambient defaults; setup-cc1 no restore path | `acct-mvq4.26` (x-ref acct-xdkd) |
| Q6 | P3 | MISC-structural | stale s1..s8 hint; swallowed observer poll errors; seed any-row heuristic; throughput convention | `acct-mvq4.27` (x-ref acct-x9bg) |
| Q7 | P2 | CODE-DEFECT (race) | Eject-vs-stamp race: unconditional eject mutations vs publish-before-stamp window → silent loss or permanent slot leak | `acct-mvq4.28` |
| Q8 | P2 | CODE-DEFECT (amplification) | Q1 routed amplification: write-phase 23514 poisons whole commit_group, recurring + CQ-slot loss | `acct-mvq4.29` (x-ref acct-mvq4.22, yojk.9 lineage) |
| Q9 | P2 | CODE-DEFECT (fail-open) | xid-triage SPI-error swallow keeps everything — inverts the caller-tx contract | `acct-mvq4.30` |
| Q10 | P2 | CODE-DEFECT (recovery gap) | Committer ERROR-exit orphans its in-flight group; Phase-2 reclaim only triggers at router restart | `acct-mvq4.31` |
| Q11 | P3 | CODE-DEFECT (weak-memory) | Sweep/claim-rollback clear CQ identity after the valid CAS (Relaxed) — double-claim hazard on non-TSO | `acct-mvq4.32` |
| Q12 | P3 | STRUCTURAL | COMMITTER_QUEUE share guard held across entire SPI write phase — router exclusive starvation | `acct-mvq4.33` (x-ref acct-ozln, acct-m4g5.2) |
| Q13 | P3 | CODE-DEFECT (production-later) | u32 xid staged where xid8 epoch semantics required — post-epoch triage misclassifies to keep | `acct-mvq4.34` |
| Q14 | P3 | DEAD-OBSERVABILITY | Nine written-never-read shmem fields; free_slot_wake_count doc-comment false | `acct-mvq4.35` |
| C1 | P3 | COVERAGE-gap | `ledger_enqueue_trx_batch_c` suite-dark — zero test coverage on a production SPI entry point | `acct-mvq4.36` |
| C2 | P3 | COVERAGE-gap | Enqueue backpressure failure arms test-dark — queue-full timeout error + arena-full deadline never executed | `acct-mvq4.37` (x-ref acct-mvq4.18/A10) |
| C3 | P3 | COVERAGE-gap | Retry-path re-dedup deferral untested — no deadlock-retry × duplicate combination case | `acct-mvq4.38` (x-ref acct-mvq4.17/A9) |
