# Architectural review — contention model + measurement methodology audit (acct-8hv2)

**Date:** 2026-05-11
**Author:** dkk (with assistant)
**Status:** IN PROGRESS

## Why this audit exists

Three commits shipped 2026-05-11 (h73o, 3aak, c4p) under a pattern of *prediction → measurement falsifies → file follow-up*:

| Predicted | Measured |
|---|---|
| h73o addendum: c4p drops `post_so_ship` p99 ~110× (2841→25 ms) | c4p actual: 2841→2043 ms (~1.4× drop) |
| h73o addendum: combined p99 ceiling ~3,100 ms post-c4p | c4p actual: 2902→3931 ms (regressed) |
| 3aak: aggregate batching reduces `post_customer_invoice` setup p99 | within ~6% noise band (no signal) |
| 3zfj: running totals reduce per-line cost | 90× regression via cross-wrapper lock chain (reverted) |

The pattern indicates a credibility problem upstream of the commits — either measurement methodology, architectural reasoning about contention propagation, or framing inconsistencies. This audit *re-measures* the recent commits under replicated conditions, *measures* per-row contention directly (rather than inferring it), and issues *evidence-backed verdicts* on each.

**Scope:** AUDIT only. No architectural code changes. No commit reverts without audit evidence. Output: this document + a reusable `pg_locks` sampler + a replication driver script + bd-tracked follow-ups.

## Methodology

Per the **acct-ezm methodology memory** (5×60 s short runs with 30 s gaps detect small effects better than 1×600 s on noisy rigs), each scenario in this audit comprises:

- **5 replicate runs** of 60 s, with 30 s gaps between runs (let buffer pool settle).
- **Same DB-reset path each run**: `scripts/run-tests.sh` drops + recreates `acct_test`, applies all 70 migrations, seeds the fixture, runs `phase1_mixed_workload` with the scenario's env-var set.
- **Metrics captured** per run:
  - `ops: total / ok / skip / err` + throughput per second.
  - `combined wrapper latency_us`: p50 / p95 / p99 / p99.9 / max across all wrappers.
  - per-op latency (us): p50/p95/p99/max for each of 9 op kinds.
  - `pg_stat_database.deadlocks` delta.
  - h73o per-wrapper × section decomposition (p50/p95/p99/max in us): post_so_ship / post_customer_invoice / post_wo_start / post_op_move × {setup, post_posting_lines, followup [, enqueue]}.
  - `T4_LOCK_SAMPLE=1` (Phase C onward): pg_locks histogram (relation × locktype × granted state).
- **Aggregation:** median + IQR (Q1, Q3) across the 5 replicate runs.
- **Delta classification rule:** any cross-scenario delta whose magnitude falls inside *either* scenario's IQR is **noise**, not signal.

Driver: `scripts/run-audit-replicate.sh` (newly added in this audit). Sampler: `tests/common/pg_locks_sampler.rs` (newly added).

## Phase A — Rig noise band (5×60 s sync baseline)

**Configuration:** `T4_DURATION_SECS=60 T4_WRITERS=32 T4_USE_PSYNC=0 T4_REPORT_TIMINGS=1`. Lock sampler **off** (noise floor without the sampler's perturbation).
**Logs:** `/tmp/audit_8hv2/A1_sync_baseline_20260511_164242/`.
**Wall clock:** 5 runs + 4×30 s gaps = ~9 min.

### Table A1 — Noise band across 5 replicate runs

| Metric | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 | Median | Q1 | Q3 | **IQR %** | Range % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ops_total | 18,915 | 12,635 | 15,274 | 14,210 | 13,499 | 14,210 | 13,499 | 15,274 | **12.5%** | 44.2% |
| ok_count | 14,729 | 9,806 | 11,959 | 11,081 | 10,553 | 11,081 | 10,553 | 11,959 | **12.7%** | 44.4% |
| throughput (ops/s) | 310.1 | 210.3 | 242.8 | 234.8 | 222.8 | 234.8 | 222.8 | 242.8 | **8.5%** | 42.5% |
| deadlocks delta | 51 | 63 | 51 | 56 | 60 | 56 | 51 | 60 | **16.1%** | 21.4% |
| combined p50 (us) | 11,746 | 11,142 | 12,040 | 9,816 | 9,842 | 11,142 | 9,842 | 11,746 | **17.1%** | 20.0% |
| combined p95 (us) | 910,113 | 1,075,105 | 1,040,955 | 1,034,549 | 1,094,312 | 1,040,955 | 1,034,549 | 1,075,105 | **3.9%** | 17.7% |
| combined p99 (us) | 2,114,513 | 3,019,608 | 2,241,374 | 3,095,613 | 2,542,224 | **2,542,224** | 2,241,374 | 3,019,608 | **30.6%** | 38.6% |
| combined p99.9 (us) | 3,990,758 | 7,118,106 | 6,047,026 | 4,966,067 | 6,003,994 | 6,003,994 | 4,966,067 | 6,047,026 | **18.0%** | 52.1% |

### Table A2 — Per-wrapper × section p99 across runs (h73o decomposition, µs)

| wrapper | section | run 1 | run 2 | run 3 | run 4 | run 5 | **Median** | IQR % | Range % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| post_so_ship | setup | 20,479 | 16,682 | 31,637 | 18,501 | 20,899 | **20,479** | 11.7% | 73.0% |
| post_so_ship | post_posting_lines | 2,114,742 | 2,041,269 | 2,138,808 | 3,918,418 | 3,843,589 | **2,138,808** | **80.8%** | 87.8% |
| post_so_ship | followup | 4,324 | 3,530 | 2,911 | 2,319 | 3,051 | 3,051 | 20.3% | 65.7% |
| post_customer_invoice | setup | 2,440,606 | 3,127,467 | 2,798,312 | 4,129,761 | 4,227,417 | **3,127,467** | **42.6%** | 57.1% |
| post_customer_invoice | post_posting_lines | 4,695 | 4,295 | 5,316 | 4,335 | 4,240 | 4,335 | 9.2% | 24.8% |
| post_customer_invoice | followup | 0 | 0 | 0 | 0 | 0 | 0 | — | — |
| post_wo_start | setup | 20,392 | 30,176 | 22,275 | 17,984 | 17,530 | **20,392** | 21.0% | 62.0% |
| post_wo_start | post_posting_lines | 3,145,376 | 6,317,822 | 5,944,901 | 3,955,235 | 3,037,317 | **3,955,235** | **70.8%** | 82.9% |
| post_wo_start | followup | 613 | 702 | 808 | 553 | 604 | 613 | 16.0% | 41.6% |
| post_op_move | setup | 12,439 | 18,789 | 17,629 | 12,695 | 15,754 | **15,754** | 31.3% | 40.3% |
| post_op_move | post_posting_lines | 3,967,700 | 7,061,424 | 3,047,415 | 3,979,226 | 4,941,889 | **3,979,226** | **24.5%** | 100.9% |
| post_op_move | followup | 247 | 300 | 340 | 197 | 293 | 293 | 18.1% | 48.8% |

### Interpretation — what the noise band reveals

**1. The rig is noisier than the prior ezm characterization** (which established ~15–20% variance). This rig's combined p99 carries **30.6% IQR** and 38.6% max-min range across 5×60 s replicates. Tail-latency metrics in transport-heavy sections (`post_so_ship.post_posting_lines`, `post_wo_start.post_posting_lines`) carry **70–80% IQR**.

**2. Tier of metric stability** (most → least stable):
- combined p95 (3.9% IQR) — very stable
- throughput (8.5% IQR) — stable
- per-wrapper × section p99 for setup-bound sections (11–21% IQR) — moderately stable
- combined p99 (30.6% IQR) — borderline-noisy
- per-wrapper × section p99 for transport-bound sections (24–81% IQR) — **highly noisy**

**Implication: future perf comparisons should preferentially report throughput and p95 medians; tail-latency claims (p99, p99.9) require larger deltas to count as signal.**

**3. h73o's transport-domination claims hold robustly across the noise band**:
- post_so_ship: setup p99 ≈ 20 ms vs pp p99 ≈ 2.14 s → **99.0% transport-dominated** ✅
- post_op_move: setup p99 ≈ 16 ms vs pp p99 ≈ 3.98 s → **99.6% transport-dominated** ✅
- post_wo_start: setup p99 ≈ 20 ms vs pp p99 ≈ 3.96 s → **99.5% transport-dominated** ✅
- post_customer_invoice: setup p99 ≈ 3.13 s vs pp p99 ≈ 4.3 ms → **99.86% setup-dominated** ✅

All four decomposition claims survive the 5-run replication; the **identity** of the dominant section is stable even when the absolute magnitude varies 70–80%.

**4. The 3aak verdict is now definitive (without needing C5 rollback measurement).**
- Prior single-run claim: post_customer_invoice setup p99 went 3,127 µs → 3,333 µs (+6.6%).
- Phase A IQR on that exact metric: **42.6%** (1,331,449 µs span between Q1=2,798,312 and Q3=4,129,761).
- The claimed +6.6% delta is **inside the IQR by a factor of ~6**.
- **Verdict: 3aak is a noise term, not a signal.** Mig 0069's SQL is correct as a refactor (clearer aggregate-batching shape) but produces no measurable change in setup p99.

**5. The c4p verdict is INCONCLUSIVE on Phase A alone.**
- Prior single-run claim: post_so_ship caller p99 dropped 2841 → 2043 ms (-28%). The 798 ms delta is *inside* the 80.8% IQR (1,728,847 µs) of so_ship pp p99 alone. Cannot distinguish from noise without a replicated psync measurement.
- Prior single-run claim: combined p99 regressed 2902 → 3931 ms (+35%, +1029 ms). The 1029 ms delta is **larger than** combined p99's IQR (778,234 µs) but **inside** the max-min range (981,100 µs). Borderline; needs replicated psync measurement to verify.
- Phase C will resolve.

**6. Throughput is the cleanest comparison metric available.** IQR of 8.5% means a throughput delta of >10% is signal; <8% is noise. Prior c4p claim of throughput 253 → 183 ops/s (-28%) far exceeds this threshold — likely real, but Phase C will confirm.

## Phase B — pg_locks sampler

**Code shipped** in this audit (uncommitted as of this writing):

- `tests/common/pg_locks_sampler.rs` (~270 lines) — reusable async sampler. Polls `pg_locks` + `pg_stat_activity` every 100 ms during a load run and emits a `SamplerReport` on shutdown.
- `tests/common/mod.rs` — `pub mod pg_locks_sampler;` added.
- `tests/load_phase1_mixed_workload.rs` — `T4_LOCK_SAMPLE=1` env var, sampler spawn/shutdown wiring, +1 connection in pool size when on.

**Sampler design:**

```rust
let sampler = PgLocksSampler::spawn(pool.clone(), 100).await;  // 100ms = 10Hz
// ... run workload ...
let report = sampler.shutdown().await;
// SamplerReport carries:
//   lock_observations: HashMap<(relation, locktype, mode, granted), sum_waiter_count>
//   wait_observations: HashMap<(wait_event_type, wait_event), sum_backend_count>
//   samples_taken, duration_ms, poll_errors
```

**Status:** Code in place. Perturbation check (Phase A re-run with sampler on) **PENDING**.

### Sampler perturbation table (PENDING)

| Metric | Phase A (sampler off) median | Phase A re-run (sampler on) median | Delta | Inside IQR? |
|---|---:|---:|---:|---|
| combined p99 (us) | 2,542,224 | _pending_ | _pending_ | _pending_ |
| throughput (ops/s) | 234.8 | _pending_ | _pending_ | _pending_ |

If perturbation falls outside Phase A's IQR (combined p99: 778 ms IQR; throughput: 8.5% IQR), drop sampling interval to 250 ms and re-check.

## Phase C — Replicated measurements of recent commits

### Table C1 — Sync baseline (5×60 s, sampler ON)

> _Pending._

### Table C2 — Shape-L pseudo-sync (T4_USE_PSYNC=1, 5×60 s, sampler ON)

> _Pending._

### Table C3 — Sync baseline + Rust-side wall-clock (optional)

> _Pending or skipped (decide at audit time based on C1)._

### Table C4 — Psync + Rust-side wall-clock (optional)

> _Pending or skipped._

### Delta classification

> _For each pairwise comparison (e.g., C1 vs C2 combined p99 median), classify as **signal** (delta magnitude exceeds both scenarios' IQRs) or **noise** (inside)._

## Phase D — Per-row contention histogram + revised contention model

### D1. Hot row histogram

> _Pending — from sampler output in Phase C runs. Sum `wait_count` per (relation, row-key) across all 5 runs of C1; sort descending._

| Rank | Relation | Row business-key | wait_count | total_wait_ms | % of total waits |
|---|---|---|---|---|---|
| 1 | | | | | |
| 2 | | | | | |
| … | | | | | |

**Distribution shape:** top 1% rows account for ___% of waits → **hot rows** OR **spread**.

### D2. Wrapper × hot-row cross-map

> _Pending — for each top-10 hot row, which wrappers contend on it?_

| Hot row | Intra-wrapper contenders | Cross-wrapper contenders |
|---|---|---|
| 1 | | |
| … | | |

### D3. Revised contention model

When does each remediation actually help, based on the audit's evidence?

| Remediation | Helps when | Helped 1s6r workload? |
|---|---|---|
| Aggregate batching (3aak shape) | N events in one call would otherwise serialize on the same row | _Pending_ |
| Sharded balances | Contention concentrates on a tiny set of rows | _Pending_ |
| SERIALIZABLE + retry | Contention is uncommon; low retry rate | _Pending_ |
| Shape-L pseudo-sync | Many writers hammer a small set of shared rows; drainer is the single FOR UPDATE holder | _Pending_ |
| None of the above | CPU-bound or WAL-bound, not lock-bound | _Pending_ |

## Phase E — Commit verdicts

> _Backed by Phase A–D evidence. Three possible verdicts per commit: keep / refactor / revert._

| Commit | bd issue | Verdict | Evidence |
|---|---|---|---|
| 5ed8944 | acct-h73o (mig 0068 instrumentation) | _Pending_ | _Pending_ |
| 39d0fe5 | acct-3aak (mig 0069 aggregate batching) | _Pending_ | _Pending_ |
| ed9360d | acct-c4p (mig 0070 + psync_runtime + T4_USE_PSYNC) | _Pending_ | _Pending_ |
| (parked) | acct-3zfj (option-c materialized totals) | Stay parked | Already reverted; 90× regression confirmed at draft time |

## Phase F — Next-step recommendation

> _Based on Phase D's distribution shape:_
>
> - **Clear hot rows surfaced** → file sharded-balances spec for those specific rows (separate epic).
> - **Spread workload, no clear hot rows** → "no architectural intervention warranted; current state acceptable." Close acct-bdq6.
> - **Unclear** → file follow-up for finer-grain instrumentation. acct-bdq6 stays blocked.

**Outcome:** _Pending._

## Methodology that future perf work MUST follow

Derived from this audit's findings:

1. **Replication is mandatory.** Single 600 s runs are not authoritative. Minimum: 5×60 s with 30 s gaps. Report median + IQR. Compare medians only when both fall outside each other's IQRs.
2. **Per-row contention must be measured, not inferred.** Use `pg_locks` + `pg_stat_activity` sampler (see `tests/common/pg_locks_sampler.rs`). The phrase "this workload is spread" is a claim that requires evidence.
3. **Rust-side wall-clock and SQL-side instrumentation are different.** When psync (or any async coupling) is in play, capture both. SQL-side h73o decomposition does NOT capture the Rust-side dispatcher wait — the caller's user-observed latency is SQL-side + Rust-side rendezvous.
4. **Do not extrapolate shape numbers across workloads.** `perf_baseline_v0` shape-L's 100-writer-shared-account regime is a different problem from the 32-writer spread `1s6r` regime. Predictions based on cross-workload extrapolation must be flagged as speculative.
5. **Stop after each phase; surface findings; wait for direction.** Per `treat-proceed-as-scoped-to-the-specific-item` memory.

## Out of scope

- Implementing sharded balances (becomes Phase F follow-up if recommended).
- Implementing acct-bdq6's decision tree (blocked-by this audit; followup).
- Reverting commits without audit evidence supporting the revert.
- Re-architecting cost methods.
- Multi-drainer / partitioned-drainer experiments.
- Production-tier deployment considerations for psync.
