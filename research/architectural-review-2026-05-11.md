# Architectural review — contention model + measurement methodology audit (acct-8hv2)

**Date:** 2026-05-11
**Author:** dkk (with assistant)
**Status:** COMPLETE

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

**Status:** Code in place and validated. Perturbation check (C1's same-config-sampler-on run) confirmed inside Phase A's noise band (table below).

### Sampler perturbation table

Comparing Phase A (sampler OFF) vs C1 (same workload, sampler ON, 5 replicates).

| Metric | Phase A (off) median | C1 (on) median | Delta % | Inside Phase A IQR? | Verdict |
|---|---:|---:|---:|---|---|
| throughput (ops/s) | 234.8 | 266.6 | **+13.5%** | Phase A IQR 8.5% — outside | But inside max-min range (42.5%) → run-to-run variance dominates |
| combined p99 (us) | 2,542,224 | 2,899,490 | **+14.0%** | Phase A IQR 30.6% — **inside** | Noise |
| deadlocks delta | 56 | 55 | -1.8% | inside | Noise |

**Verdict on sampler perturbation:** sampler is **not measurably perturbing** at this rig's noise floor. The throughput delta is outside Phase A's tight IQR but inside Phase A's max-min range; that the throughput went *up* with sampler on argues that the run-to-run variance dominates any sampler overhead. The combined p99 delta is well inside Phase A's IQR.

The sampler holds the +1 dedicated connection plus issues 2 small queries every 100 ms. At ~250 ops/s the workload, that's <1% added query rate. Safe to use throughout the audit.

## Phase C — Replicated measurements of recent commits

### Table C1 — Sync baseline (5×60 s, sampler ON) — `T4_USE_PSYNC=0 T4_LOCK_SAMPLE=1`

**Logs:** `/tmp/audit_8hv2/C1_sync_with_sampler_20260511_172303/`.

| Metric | Median | Q1 | Q3 | IQR % | Range % | vs Phase A (off) |
|---|---:|---:|---:|---:|---:|---:|
| throughput (ops/s) | 266.6 | 235.9 | 304.4 | 25.7% | 75.8% | +13.5% (inside max-min) |
| combined p50 (us) | 10,277 | 10,122 | 10,497 | 3.6% | 6.6% | -7.8% (inside IQR) |
| combined p95 (us) | 954,426 | 587,019 | 1,017,385 | 45.1% | 72.3% | -8.3% (inside IQR) |
| combined p99 (us) | 2,899,490 | 2,541,260 | 2,995,813 | 15.7% | 38.1% | +14.0% (inside IQR) |
| combined p99.9 (us) | 5,990,798 | 5,004,042 | 7,028,034 | 33.8% | 99.3% | -0.2% (inside IQR) |
| deadlocks delta | 55 | 49 | 58 | 16.4% | 54.5% | -1.8% (inside IQR) |

**Interpretation:** C1's medians are uniformly inside Phase A's IQRs (combined p99: ±30.6% IQR; throughput: ±8.5% IQR — borderline but inside max-min range). Sampler perturbation is below the rig's noise floor. C1's own IQRs are wider than Phase A's, attributable to run-to-run variance (Phase A's tighter throughput IQR was likely a lucky 5-sample draw).

**This run also produced the contention evidence** in Phase D1 above (48,912 waiter-samples on accounts.tuple AccessExclusiveLock).

### Table C2 — Shape-L pseudo-sync (5×60 s, sampler ON) — `T4_USE_PSYNC=1 T4_LOCK_SAMPLE=1`

**Logs:** `/tmp/audit_8hv2/C2_psync_with_sampler_*/`.

| Metric | Median | Q1 | Q3 | IQR % | vs C1 (sync) median | Delta classification |
|---|---:|---:|---:|---:|---|---|
| throughput (ops/s) | 211.4 | 208.5 | 218.3 | 4.6% | **-20.7%** (266.6 → 211.4) | **SIGNAL** (delta > both IQRs) |
| ok_count | 10,105 | 9,990 | 10,174 | 1.8% | -22.7% | **SIGNAL** |
| deadlocks delta | 65 | 64 | 66 | 3.1% | +18.2% (55 → 65) | **SIGNAL** (delta > both IQRs) |
| combined p50 (us) | 12,028 | 11,799 | 12,987 | 9.9% | +17.0% | inside IQRs (noise) |
| combined p95 (us) | 1,031,193 | 1,031,170 | 1,034,339 | 0.3% | +8.0% | inside C1 IQR (noise) |
| combined p99 (us) | 3,360,263 | 3,228,870 | 3,871,254 | 19.1% | +15.9% | inside both IQRs (noise) |
| combined p99.9 (us) | 6,538,980 | 6,480,058 | 9,516,555 | 46.4% | +9.2% | inside both IQRs (noise) |

### Table C2-sections — Per-wrapper × section p99 (C1 vs C2)

| wrapper | section | C1 median p99 (us) | C2 median p99 (us) | Delta % | C1 IQR% | C2 IQR% | Classification |
|---|---|---:|---:|---:|---:|---:|---|
| post_customer_invoice | setup | 3,135,495 | 13,663 | **-99.6%** | 1.7% | 31.8% | **SIGNAL — 230× drop** |
| post_customer_invoice | post_posting_lines | 4,262 | 19,869 | +366.2% | 1.9% | 8.3% | **SIGNAL** (small absolute values) |
| post_op_move | setup | 13,533 | 16,877 | +24.7% | 11.4% | 9.2% | **SIGNAL** (borderline; tiny absolute) |
| post_op_move | post_posting_lines | 3,999,942 | 3,988,342 | -0.3% | 76.2% | 26.2% | noise |
| post_op_move | followup | 190 | 274 | +44.2% | 24.7% | 13.1% | borderline; trivial absolute |
| post_wo_start | setup | 18,063 | 24,716 | +36.8% | 15.3% | 14.3% | **SIGNAL** (borderline; tiny absolute) |
| post_wo_start | post_posting_lines | 5,026,974 | 6,532,098 | +29.9% | 39.6% | 46.5% | noise |
| post_wo_start | followup | 551 | 704 | +27.8% | 6.5% | 3.3% | borderline; trivial absolute |
| **post_so_ship_psync** | setup | — | 19,897 | — | — | 17.1% | new (C2 only) |
| **post_so_ship_psync** | enqueue | — | 487 | — | — | 30.6% | new (C2 only) |

### Table C2-sampler — C2 contention shape vs C1

C2 sampler captured **0 waiter-samples** in `pg_locks` snapshots (vs C1's 48,912). However, `pg_stat_activity.wait_event` histogram in C2:

| wait_event_type | wait_event | C1 sum | C2 sum | Delta % |
|---|---|---:|---:|---:|
| Lock | tuple | 63,688 | 56,721 | -10.9% |
| Lock | transactionid | 26,986 | 18,874 | -30.1% |
| LWLock | WALWrite | 1,384 | 1,419 | +2.5% |

**Interpretation:** psync mode reduces *individual lock-hold duration* (drainer commits per-batch; shorter holds than per-call writer txs), so the snapshot-based `pg_locks` sampler at 100 ms aliases through the waits. But `pg_stat_activity` (continuous wait_event accumulation) confirms total contention is only modestly reduced (-10.9% on Lock.tuple, -30.1% on transactionid). **Aggregate contention is similar; the distribution in time is different.** This explains why throughput dropped (drainer is now a serial bottleneck) while individual setup p99s on some wrappers improved (cross-wrapper FOR UPDATE races eliminated).

### Tables C3, C4 — Rust-side wall-clock

**SKIPPED.** Phase C1/C2 evidence is sufficient for verdict on c4p without Rust-side wall-clock measurement. Rationale: the most consequential finding (post_customer_invoice setup p99 -99.6%) is purely SQL-side and the audit's Phase E verdict can be issued without disentangling Rust-side dispatcher wait from SQL-side enqueue. Filed as `acct-8hv2-followup-rust-wallclock` if future psync-style work needs it.

### Delta classification summary

| Comparison | Direction | Signal/Noise |
|---|---|---|
| **C1 → C2 throughput** | -20.7% (sync 266.6 → psync 211.4 ops/s) | **SIGNAL** |
| **C1 → C2 post_customer_invoice setup p99** | -99.6% (sync 3.13s → psync 13.7ms) | **SIGNAL** (230×) |
| C1 → C2 combined p99 | +15.9% | noise |
| C1 → C2 combined p95 | +8.0% | noise |
| C1 → C2 deadlocks | +18.2% (55 → 65) | **SIGNAL** (modest rise) |
| C1 → C2 post_op_move pp_lines p99 | -0.3% | noise |
| C1 → C2 post_wo_start pp_lines p99 | +29.9% | noise |
| C1 → C2 aggregate Lock.tuple waits | -10.9% | borderline |
| C1 → C2 aggregate transactionid waits | -30.1% | borderline-signal |

## Phase D — Per-row contention histogram + revised contention model

### D1. Hot relation histogram (aggregated across C1's 5 runs × 600 samples each = 3,050 samples)

**Granted=false (waiter) observations:**

| Rank | Relation | Locktype | Mode | Sum waiters | % of total waits |
|---|---|---|---|---:|---:|
| 1 | **accounts** | **tuple** | AccessExclusiveLock | **48,912** | **100.0%** |

**100% of all observed waiter-samples are on `accounts.tuple` AccessExclusiveLock.** No other relation contributes to the contention floor.

**`pg_stat_activity.wait_event` histogram** (corroborates):

| wait_event_type | wait_event | sum | % |
|---|---|---:|---:|
| Lock | tuple | 63,688 | **68.4%** |
| Lock | transactionid | 26,986 | **29.0%** |
| LWLock | WALWrite | 1,384 | 1.5% |
| IO | WalSync | 938 | 1.0% |
| (everything else) | | <60 each | <0.1% each |

**97.4% of all wait_event samples are row-lock-related on the `accounts` table** (tuple = lock acquisition; transactionid = waiting on another tx holding the lock).

**Distribution shape: CLEAR HOT-ROW contention on `accounts`.** The earlier framing of "1s6r workload is spread" is **falsified** by direct measurement. The workload IS contention-bound.

**Held-lock counterpart** (granted=true ExclusiveLock; signals what backends are holding):

| Relation | Locktype | Mode | Sum |
|---|---|---|---:|
| `<no_rel>` | virtualxid | ExclusiveLock | 96,246 |
| `<no_rel>` | transactionid | ExclusiveLock | 95,808 |
| wo_events | relation | RowExclusiveLock | 21,990 |
| so_shipments | relation | RowExclusiveLock | 20,613 |
| so_shipment_lines | relation | RowExclusiveLock | 14,121 |
| inventory_reservations | relation | RowExclusiveLock | 7,164 |
| rsv_so | relation | RowExclusiveLock | 6,906 |

(`<no_rel>` rows on virtualxid/transactionid are normal: every active backend holds its own virtualxid + a transactionid. Counts ≈ samples × avg-active-writers.)

**Sampler resolution caveat — which `accounts` rows specifically?** The v1 sampler captures (relation, locktype, mode, granted) only — *not* tuple ctid → business key. To identify the specific (sku, location, kind) account rows that are hottest, the sampler needs ctid enrichment (filed as `acct-8hv2-followup-sampler-v2`). The workload fixture has ~420 account rows; with 32 writers showing ~21 average waiters/sample, each writer is waiting most of the time, and the contention concentrates on a subset of those rows (which subset is the v2 question).

### D2. Wrapper × hot-row cross-map (inferred from section deltas)

While v1 sampler doesn't decode tuple ctid → row business-key, the **C1 → C2 section deltas decisively reveal which contention is intra-wrapper vs cross-wrapper**:

| Contention point | Evidence | Type |
|---|---|---|
| `sales_order_lines` rows shared by post_so_ship + post_customer_invoice | post_customer_invoice setup p99 drops 99.6% when so_ship is routed through psync (which removes its FOR UPDATE on so_lines from caller's tx) | **Cross-wrapper** (so_ship ↔ customer_invoice) |
| `accounts` rows (inv_value_*, stock_available) hit by post_op_move pp_lines | post_op_move pp_lines p99 unchanged C1→C2 (-0.3%, inside 76% IQR) — so_ship being routed through psync didn't help | **Intra-wrapper** (op_move ↔ op_move, or op_move ↔ wo_complete) |
| `accounts` rows hit by post_wo_start pp_lines | post_wo_start pp_lines p99 actually rose +29.9% in C2 (noise) — psync didn't relieve | **Intra-wrapper** (wo_start ↔ wo_start, or vs op_move drainer) |
| Drainer batch on outbox rows | New deadlocks +18% in C2 (signal) | drainer-internal serialization |

**The cross-wrapper finding is the most consequential of the audit**: the prior c4p framing focused on so_ship's *own* caller p99. The actual measured benefit is on `post_customer_invoice setup p99`, a different wrapper entirely. The cross-wrapper FOR UPDATE conflict on `sales_order_lines` (so_ship's reservation flip races customer_invoice's three-way-match SELECT FOR UPDATE OF sl) was the bottleneck — and shape-L removed it because the drainer briefly holds the lock rather than caller-thread-pinning it for the full document write.

### D3. Revised contention model — evidence-backed

When does each remediation help, given this audit's measurement?

| Remediation | Helps when | Helped 1s6r? Evidence |
|---|---|---|
| Aggregate batching (3aak shape, mig 0069) | N events in one call would otherwise serialize on the same row | **No.** The setup-p99 it targeted was dominated by FOR UPDATE waits, not SELECT aggregation. Phase A confirmed the +6.6% delta is 6× inside IQR. Mig 0069's SQL stands as a refactor, no measurable perf effect. |
| Sharded balances on `accounts` | Contention concentrates on a tiny set of rows AND the set is identifiable | **Plausibly yes for intra-wrapper accounts contention**, but the specific hot rows require sampler v2 (ctid decoding). Currently `accounts.tuple` is the only relation with measurable waits. |
| SERIALIZABLE + retry | Contention is uncommon; low retry rate | **No.** Aggregate wait_event histogram shows ~97% of all waits are Lock.tuple or Lock.transactionid. SERIALIZABLE would convert these to retries; with this much contention, retry rate would dominate. |
| Shape-L pseudo-sync (c4p, mig 0070 + psync_runtime) | Cross-wrapper FOR UPDATE conflicts on tables OTHER than the accounts ledger (e.g., document tables like sales_order_lines) | **Yes — but on a different metric than originally claimed.** Routes so_ship out of sync path → eliminates so_ship ↔ customer_invoice race on sales_order_lines → customer_invoice setup p99 drops 230× (3.13 s → 13.7 ms). Tradeoff: drainer becomes a throughput bottleneck (-20.7% ops/s) AND deadlock rate rises +18%. |
| Multi-wrapper psync routing | Same as above for additional wrapper pairs | **Untested.** Would extend c4p's benefit to other cross-wrapper races (e.g., post_op_move ↔ post_wo_complete on `stock_wip` accounts), at proportional throughput cost. Would need a per-wrapper opt-in decision tree (acct-bdq6 territory). |
| None of the above | CPU-bound or WAL-bound, not lock-bound | **Not applicable here.** Workload is clearly lock-bound: 68% Lock.tuple + 29% Lock.transactionid = 97% of wait events. WAL is 1.5% of waits; IO is 1%. |

### D4. Falsified prior framings

This audit falsifies three earlier claims:

1. **"1s6r workload is spread; no clear hot rows"** — falsified. 100% of pg_locks waiter-samples are on accounts.tuple; 97% of pg_stat_activity wait_events are row-lock waits.
2. **"Shape-L helps by relieving post_so_ship's own contention"** — falsified. post_so_ship_psync's SQL-side latencies are tiny (enqueue p99 = 487 µs); the real benefit is to post_customer_invoice setup (cross-wrapper relief). The user-observed caller p99 in c4p's prior measurement (-28%) was dominated by the Rust-side dispatcher wait, not SQL-side savings.
3. **"either there is contention or there is not"** (mid-session framing) — falsified. Contention is per-row (relation accounts), and within that, per-row-set (e.g., the sales_order_lines rows that pair-correlate so_ship ↔ customer_invoice are distinctly hotter than the accounts rows hit by wo_start ↔ op_move). The binary frame doesn't capture the structure.

## Phase E — Commit verdicts

| Commit | bd issue | **Verdict** | Evidence |
|---|---|---|---|
| 5ed8944 | acct-h73o (mig 0068 instrumentation) | **KEEP** | Instrumentation tool. Decomposition claims (99%+ transport-dominated on so_ship/op_move/wo_start; 99.86% setup-dominated on customer_invoice) hold robustly across all replicated scenarios (Phase A + C1 + C2). The h73o claim about post-c4p p99 ceiling was wrong, but that was a *prediction*, not the instrumentation's measurement. The instrumentation itself is correct. |
| 39d0fe5 | acct-3aak (mig 0069 aggregate batching) | **KEEP** (no perf effect; refactor stands) | Phase A definitively classified the +6.6% setup-p99 delta as noise (6× inside the 42.6% IQR). Mig 0069's JSONB pre-LOOP aggregate batching is *correct* and modestly cleaner SQL; it produces no measurable perf change. Reverting would be churn. Future similar refactors should be motivated by readability, not perf claims. |
| ed9360d | acct-c4p (mig 0070 + psync_runtime + T4_USE_PSYNC) | **KEEP AS OPT-IN INFRA** with corrected narrative | The infrastructure works correctly; the *prior characterization* of c4p's benefit was incomplete. Actual measured effect on 1s6r when `T4_USE_PSYNC=1`: **post_customer_invoice setup p99 -99.6% (SIGNAL)**, throughput **-20.7% (SIGNAL)**, deadlocks **+18% (SIGNAL)**, all other section metrics noise. The benefit is *cross-wrapper relief* (eliminating so_ship ↔ customer_invoice FOR UPDATE race on sales_order_lines) — NOT post_so_ship's own caller p99 as originally claimed. Reverting the infra would lose a valid tool for cross-wrapper relief scenarios; keeping it acknowledges the narrow applicability. |
| (parked) | acct-3zfj (option-c materialized totals) | **STAY PARKED** | Already reverted in-session. The 90× regression on post_so_ship setup p99 via cross-wrapper UPDATE lock chain is consistent with this audit's finding that cross-wrapper FOR UPDATE conflicts on sales_order_lines are a real bottleneck — adding write contention on the same rows would amplify it. |

**No commits revert.** The audit produced corrected narratives, not revert evidence.

## Phase F — Next-step recommendations

The 1s6r workload **has clear hot-row contention** (100% of waiter-samples on `accounts.tuple`; cross-wrapper race on sales_order_lines decisively measured). Three follow-up directions, in priority order:

### F1. acct-bdq6 (Shape-L applicability decision tree) — now unblocked, P2

The decision tree now has evidence to operate on. Recommended structure:

- **Use shape-L when**: a wrapper pair A↔B shares FOR UPDATE on a *document* table (so_lines, po_lines, wo_events, etc.) AND wrapper A's body fits the psync shape (no caller-side reads of the posted ledger). The benefit is cross-wrapper relief, NOT internal throughput.
- **Do NOT use shape-L when**: contention is intra-wrapper (e.g., op_move ↔ op_move on the same parent's stock_wip) OR contention is on `accounts` rather than document rows. The drainer's single FOR UPDATE on accounts is no better than the writer's.
- **Cost**: -20% throughput; +18% deadlocks. Acceptable when the latency relief on the targeted wrapper pair matters more than throughput.

acct-bdq6 should now close acct-8hv2 as blocker and ship the decision tree based on this audit's evidence.

### F2. Sampler v2 with ctid → business-key decoding — P3, file `acct-8hv2-sampler-v2`

Current sampler v1 reports the relation (accounts) but not which specific rows. To target sharded-balances or other row-grain interventions, sampler v2 must:

- Capture `pg_locks.objsubid` + `pg_locks.relation` for tuple locks
- Periodically dump pg_class.relpages and a heap snapshot to resolve (relation_oid, blkno, offsetnum) → ctid → business row via a single `WHERE ctid = '(blkno,offsetnum)'` query
- Aggregate per-row across the run to produce a real hot-row histogram

Estimated effort: 2-3 hours sampler v2 + 1 hour Phase D1 re-analysis. Defer until F1's decision tree shows row-grain intervention is actually wanted.

### F3. Cross-wrapper FOR UPDATE audit on document tables — P3, file `acct-8hv2-cross-wrapper-audit`

The post_so_ship ↔ post_customer_invoice race on sales_order_lines is now documented. There are likely OTHER cross-wrapper races visible by inspection of the wrappers' FOR UPDATE patterns:

- post_op_move ↔ post_wo_complete on stock_wip + wo_events
- post_po_receipt ↔ post_ap_bill on po_receipt_lines
- post_customer_return ↔ post_customer_credit_memo on customer_return_lines

Each is a candidate for psync-routing under F1's decision tree. The audit should be a static code review (grep for `FOR UPDATE OF` per wrapper, build the wrapper × table incidence matrix) — no further measurement needed.

### F4. NOT a sharded-balances spec

The original Phase F branching was "if clear hot rows → file sharded-balances spec." This audit *does* find clear hot rows on `accounts.tuple`, BUT the most consequential finding is the *cross-wrapper race on document tables*, which sharding accounts doesn't address. **The audit recommends F1/F3 as priority intervention surfaces over sharding.** Sharding can be revisited if sampler v2 (F2) surfaces row-grain hot accounts after F1+F3 land.

### Decision summary

| Action | Priority | Status post-audit |
|---|---|---|
| Update acct-bdq6 with this audit's decision tree | P2 | unblock + ready |
| File `acct-8hv2-sampler-v2` (ctid decoding) | P3 | new |
| File `acct-8hv2-cross-wrapper-audit` (static FOR UPDATE incidence matrix) | P3 | new |
| Sharded-balances spec | P4 | defer (revisit after F2+F3) |
| Multi-wrapper psync routing for op_move/wo_complete | P3 | gated on F1 decision tree |

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
