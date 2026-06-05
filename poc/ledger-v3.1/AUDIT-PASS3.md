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

## A. docs ↔ code (acct-mvq4.2 — pending)

## B-i. quality: core + spi-common + direct-c + harness + scripts (acct-mvq4.3 — pending)

## B-ii. quality: ledger-routed-c deep (acct-mvq4.4 — pending)

## C-i. coverage map + gap classification (acct-mvq4.5 — pending)

## C-ii. suite-run ledger (acct-mvq4.6 — pending)

## D. complexity disposition (acct-mvq4.7 — pending)

## Findings index (acct-mvq4.8 — pending)

| ID | Sev | Verdict | Title | Filed issue / duplicate-of |
|---|---|---|---|---|
| — | | | *(populated as sections land)* | |
