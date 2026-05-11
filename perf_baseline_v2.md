# Perf baseline v2 — end-to-end Slice A+B+C mixed workload (`acct-1s6r`)

**Date:** 2026-05-11
**Schema:** 67 migrations through `0067_partition_registry`
**bd issue:** acct-1s6r
**Test binary:** `tests/load_phase1_mixed_workload.rs`
**Methodology:** 32 concurrent tokio writers × 600 s wall clock, weighted-random op mix spanning all three slices.

## TL;DR

**Inflection point reached for acct-c4p.** Combined-wrapper p99 latency is **2.35 s** under realistic 32-writer mixed contention — 4.7× the 500 ms threshold the Q3 deferral cited. Per the load-bearing decision "L's latency-and-throughput-under-contention advantages are real and wanted," this is the measured contention that re-opens the pseudo-sync pivot conversation. 513 Postgres-detected deadlocks in 10 minutes (0.3 % of all ops aborted) corroborate.

**posting_lines partitioning trigger (acct-e8g) NOT fired.** Growth rate is 0.19 MB / s of compressed table data — ~6 GB / year sustained 24/7. Well within single-table comfort zone. Re-evaluate at 50 M rows.

**accounts-explosion question: under-measured by this run.** Account-row delta is 0 because every (sku × location × vendor × customer × currency) partition was pre-seeded at setup. Lazy-creation behavior under sustained traffic needs a follow-up test (dynamically add entities mid-run); filed as `acct-1s6r-followup`.

## Headline numbers

| Metric | Value |
|---|---|
| Wall clock | 600.55 s |
| Concurrent writers | 32 |
| Total ops attempted | 152 193 |
| Ops successful | 118 668 |
| Ops skipped (state-queue starvation) | 33 012 |
| Ops failed (incl. deadlocks) | 513 |
| Throughput (successful) | 197.6 ops/s |
| Throughput (attempted) | 253.4 ops/s |
| **Combined wrapper p50** | **18.6 ms** |
| **Combined wrapper p95** | **993.9 ms** |
| **Combined wrapper p99** | **2 353.5 ms** |
| **Combined wrapper p99.9** | **5 132.7 ms** |
| Max wrapper latency | 10 174.8 ms |
| pg_stat_database.deadlocks delta | **513** |
| xact_commit delta | 149 197 |
| xact_rollback delta | 513 |
| posting_lines rows before / after / delta | 200 / 239 298 / **239 098** |
| posting_lines size before / after / delta | 0.2 MB / 115.8 MB / **115.6 MB** |
| WAL bytes delta | 738.8 MB |
| accounts rows before / after / delta | 371 / 371 / 0 (all pre-seeded) |

## Per-op latency (µs)

p50 / p95 / p99 / max in microseconds; `n` is successful invocations.

| Op | n | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| `po_receipt` | 24 460 | 25 016 | 72 329 | 207 796 | 4 038 114 |
| `ap_bill` | 19 403 | 7 566 | 41 858 | 92 410 | 4 039 935 |
| `wo_start` | 12 120 | 17 411 | **1 997 008** | **4 116 376** | 8 157 368 |
| `op_move` | 4 641 | 10 678 | **1 835 639** | **5 804 627** | 10 174 793 |
| `wo_complete` | 9 557 | 8 192 | 20 408 | 1 992 538 | 7 066 661 |
| `so_ship` | 24 125 | 84 401 | **1 181 630** | **2 271 326** | 5 832 653 |
| `customer_invoice` | 17 114 | 74 250 | **1 376 829** | **2 925 745** | 8 531 482 |
| `ar_payment` | 7 248 | 5 589 | 11 415 | **17 106** | 35 351 |
| `return` | 0 | — | — | — | — |

`ar_payment` is the cleanest shape — single-counterparty / single-currency lookup, no SKU touch, no inventory drain. Cogent baseline for "fastest possible post_posting_lines wrapper" at ~17 ms p99.

`wo_start` / `op_move` / `so_ship` / `customer_invoice` dominate the tail. Common factor: they all touch shared `stock_*` / `inv_value_*` accounts that other writers are competing for, AND each call internally posts 3-6 events through `post_posting_lines`. The p99 ≈ 2-5 s tail is the contention story.

`po_receipt` and `ap_bill` sit in the middle — vendor-partitioned `ap_unsettled` / `vendor_pool` keep them less contended than the SKU-side accounts.

`return` is intentionally unimplemented for this baseline (0.05 weight; mainstream-ERP load tests typically exclude returns from steady-state probes). Filed as `acct-1s6r-followup` if return-path contention coverage is needed.

## pg_stat_statements top 10 (by total_exec_time)

| Calls | Total ms | Mean ms | Query |
|---:|---:|---:|---|
| 24 125 | 6 234 136 | 258.4 | `SELECT post_so_ship(...)` |
| 17 114 | 4 674 735 | 273.2 | `SELECT post_customer_invoice(...)` |
| 12 120 | 3 610 316 | 297.9 | `SELECT post_wo_start(...)` |
| 4 641 | 1 013 650 | 218.4 | `SELECT post_op_move(...)` |
| 24 460 | 928 489 | 38.0 | `SELECT post_po_receipt(...)` |
| 9 557 | 603 308 | 63.1 | `SELECT post_wo_complete(...)` |
| 19 403 | 299 469 | 15.4 | `SELECT post_ap_bill(...)` |
| 7 248 | 15 434 | 2.1 | `SELECT post_ar_payment(...)` |
| 12 160 | 2 952 | 0.2 | `INSERT INTO work_orders (...)` |
| 16 924 | 1 735 | 0.1 | `INSERT INTO wo_routings (...)` |

Wrappers dominate. Auxiliary INSERTs (work_orders / wo_routings) are <0.3 ms mean. No surprising aux-query cost.

## Workload mix actually exercised

Configured weights vs realized successful-call distribution (% of ok ops):

| Op | weight | realized n | realized % |
|---|---:|---:|---:|
| `po_receipt` | 1.00 | 24 460 | 20.6 % |
| `ap_bill` | 0.80 | 19 403 | 16.4 % |
| `wo_start` | 0.50 | 12 120 | 10.2 % |
| `op_move` | 1.50 | 4 641 | 3.9 % |
| `wo_complete` | 0.40 | 9 557 | 8.1 % |
| `so_ship` | 1.00 | 24 125 | 20.3 % |
| `customer_invoice` | 0.70 | 17 114 | 14.4 % |
| `ar_payment` | 0.30 | 7 248 | 6.1 % |
| `return` | 0.05 | 0 (skipped) | — |

`op_move` is under-represented vs configured (`weight 1.5 → expected ~24%, realized 3.9%`). Reason: state-queue starvation. Most picked `op_move` ops find no eligible in-flight WO (single-op BOMs go straight from `wo_start` to `wo_complete` with no `op_move`; multi-op BOMs need an `op_move` between, but they pop the WO and only push back on a different op). Skip rate is 21.7 % overall, concentrated on `op_move` / `wo_complete` (need state from `wo_start`) and `ap_bill` / `customer_invoice` / `ar_payment` (need state from prior receipts / ships / invoices). State-queue dynamics under 32-writer contention are part of the realism.

## Deadlock analysis

513 deadlocks / 152 193 attempted ops = **0.34 %**. Pattern (eye-balled from writer logs):
- Concentrated on `op_move` and `wo_complete`
- Cause: two writers operating on different WOs that share the same `stock_wip(parent_sku, routing_op)` and `inv_value_wip(parent_sku, routing_op)` accounts. Each wrapper locks 4 accounts (qty-src, qty-dst, val-src, val-dst). Postgres lock-acquisition order is non-deterministic across concurrent statements → cycle → deadlock detector aborts one.

**Implications:**
1. The mixed workload exercises the contention shape that the load-bearing decision on `acct-c4p` explicitly anticipated. Shape L (pseudo-sync via LISTEN/NOTIFY, the documented escape hatch) eliminates this category of contention by pipelining writer-INSERT and drainer-commit so writers never touch shared pool accounts. The infrastructure is already built and benched.
2. The deterministic lock-acquisition discipline (LEAST/GREATEST pattern, per R4) is already in place for cost-sensitive paths. The deadlocks here are from non-cost-sensitive qty-leg pairings during multi-op WO traffic where the pool-lock pre-scan in `_post_posting_lines_lock_pre_scan` is correctly ordered but the larger 4-account set in `post_op_move` includes qty + value pairs that the dispatcher doesn't pre-scan deterministically.
3. SERIALIZABLE isolation (`acct-zroo` investigation) would convert these to serialization failures (40001) — retryable, but at a throughput cost the test isn't equipped to measure here.

## Triggers and what this updates

### `acct-c4p` — pseudo-sync pivot (shape L)

**Trigger fired.** Caller p99 = 2.35 s under realistic 32-writer mixed load, vs the cited 500 ms threshold (4.7×). p95 also fires (994 ms). The original decision deferred shape L "until Phase 1 produces measured contention" — that contention is here.

Recommendation: re-open `acct-c4p` and schedule the shape-L migration. The pivot is additive (LISTEN/NOTIFY rendezvous around existing `post_posting_lines`), not a foundation rewrite. Estimate from `perf_baseline_v0.md`: shape L caller p99 was 547 ms vs shape F's 8 250 ms (15× better) — translation to this mixed shape would predict shape L caller p99 ≈ 200-500 ms (10-15× improvement of the 2 350 ms measurement).

### `acct-e8g` — posting_lines partitioning

**Trigger NOT fired.** Growth rate is 115.6 MB / 600 s = 0.19 MB / s. Extrapolated steady-state:
- Single Phase 1 customer: ~16.6 GB / year (at 24/7)
- Multi-tenant or growth: ~30-50 GB / year

Postgres handles a 50 GB heap-table without partitioning. Re-evaluate when the running rate suggests >100 GB / year OR query plan degrades (sequential scans, lock-acquisition latency on cold partitions).

Row count: 239 K rows / 600 s = 398 rows/s sustained. Annualized at 24/7 = ~12.6 M rows/year. Comfortable at one decimal order below the partitioning-recommended threshold.

### Accounts-explosion question

**Under-measured.** Account-table delta = 0 because all (vendor, customer, SKU) entities were pre-seeded at setup. To measure lazy-creation behavior, the next iteration should dynamically introduce new entities mid-run. Filed as `acct-1s6r-followup`.

Expected production behavior: most ERPs see accounts grow O(N_SKUs × N_locations × N_currencies) initially, then flatten. The realistic question is "how fast does N_partitions × per-vendor-per-SKU-per-currency creation accumulate when the customer-/vendor-master grows organically?" — a steady-state extrapolation rather than a one-time baseline.

### `acct-mqi8` — RBAC overhead measurement

**N/A pre-RBAC.** Re-run this load test once RBAC ships to measure permission-check cost on the hot path.

## Caveats

- **Single-laptop methodology.** Same rig as v0 / v1 (consumer kernel, thermal/scheduling jitter dominates at percentile tails — see v0 caveats). Numbers are directional, not statistical.
- **One run.** No 5×60s noise-band measurement (acct-ezm methodology). For repeatability, re-run 3 times and median.
- **Workload state is mediated by a tokio Mutex.** Lock acquisition on `Arc<Mutex<WorkloadState>>` to pop/push queue entries adds ~µs of overhead per op. Doesn't affect the Postgres-side contention shape but contributes to the floor latency at very low writer counts.
- **`tracked_by='lot'` and `tracked_by='lot_and_serial'` not exercised.** The mixed pool is all `tracked_by='none'`. Lot + serial workloads are their own contention shape; file as `acct-1s6r-followup` if needed.
- **Period close not exercised.** Periods 2026-04 / 05 / 06 stay open for the duration; `close_period` orchestration is its own contention shape. File as `acct-1s6r-followup` if needed.
- **All USD.** Cross-currency (acct-3xcg / acct-3dz2) is its own contention shape.

## Re-running

```bash
T4_DURATION_SECS=600 T4_WRITERS=32 \
  ./scripts/run-tests.sh --test load_phase1_mixed_workload \
  -- --ignored --nocapture
```

Env knobs (defaults in parens):

- `T4_DURATION_SECS` (600) — wall clock seconds
- `T4_WRITERS` (32) — concurrent tokio writers
- `T4_BENCH_SKUS` (50) — mixed-method SKU pool
- `T4_BENCH_VENDORS` (10)
- `T4_BENCH_CUSTOMERS` (10)
- `T4_BENCH_WO_SKUS` (5) — FG-only WO parents

For a 30-minute stress run: `T4_DURATION_SECS=1800`.

## Reference data

- `perf_baseline_v0.md` — 13-shape matrix on the 21-migration schema, measured 2026-04-29/30.
- `perf_baseline_v1.md` — re-baseline against the 32-migration schema, measured 2026-05-01.
- `tests/load_realistic_workload.rs` — shape F (cross-account spread, bin_move only).
- `tests/load_inflow_workload.rs` — shape N (Slice A PO+AP cycle only).
- `tests/load_outbox_pseudo_sync.rs` — shape L (the pivot target).

---

## Addendum — wrapper p99 tail decomposition (`acct-h73o`)

**Date:** 2026-05-11
**Schema:** 68 migrations through `0068_wrapper_instrumentation`
**bd issue:** acct-h73o
**Methodology:** Same 32-writer × 600 s rig, same workload mix. UNLOGGED `_wrapper_section_timings` records per-section elapsed µs (3 rows per successful wrapper call) at section boundaries: `setup` (function entry through last lookup) / `post_posting_lines` (the PERFORM call itself) / `followup` (post-PERFORM audit writes + state-machine UPDATEs). Four hottest wrappers instrumented: `post_so_ship`, `post_customer_invoice`, `post_wo_start`, `post_op_move`.

### Per-wrapper decomposition (µs)

| Wrapper | Section | n | p50 | p95 | p99 | max | % of wrapper p99 |
|---|---|---:|---:|---:|---:|---:|---:|
| `post_so_ship` | `setup` | 22 776 | 1 403 | 4 984 | 23 131 | 1 248 058 | **0.8 %** |
| `post_so_ship` | `post_posting_lines` | 22 776 | 76 601 | 1 171 886 | **2 812 373** | 5 831 030 | **99.0 %** |
| `post_so_ship` | `followup` | 22 776 | 708 | 1 161 | 2 312 | 28 189 | 0.1 % |
| `post_customer_invoice` | `setup` | 15 695 | 68 848 | 1 310 348 | **3 127 291** | 6 351 495 | **99.8 %** |
| `post_customer_invoice` | `post_posting_lines` | 15 695 | 1 275 | 2 813 | 5 151 | 26 710 | **0.2 %** |
| `post_customer_invoice` | `followup` | 15 695 | 0 | 0 | 0 | 0 | 0.0 % |
| `post_wo_start` | `setup` | 11 134 | 2 392 | 5 123 | 10 145 | 64 536 | **0.2 %** |
| `post_wo_start` | `post_posting_lines` | 11 134 | 9 925 | 2 053 021 | **4 233 112** | 9 325 620 | **99.7 %** |
| `post_wo_start` | `followup` | 11 134 | 128 | 249 | 559 | 4 415 | 0.0 % |
| `post_op_move` | `setup` | 4 359 | 2 244 | 5 009 | 11 126 | 32 729 | **0.2 %** |
| `post_op_move` | `post_posting_lines` | 4 359 | 4 190 | 1 928 872 | **7 038 184** | 19 908 704 | **99.8 %** |
| `post_op_move` | `followup` | 4 359 | 13 | 24 | 62 | 1 276 | 0.0 % |

### Verdict

**Three of four hot wrappers are >99 % transport-dominated at p99.**
- `post_so_ship`: 99.0 % transport
- `post_wo_start`: 99.7 % transport
- `post_op_move`: 99.8 % transport

**One wrapper (`post_customer_invoice`) is >99.8 % setup-dominated.** The `post_posting_lines` slice within `post_customer_invoice` p99 is only ~5 ms — the 3.1 s tail comes from the per-line work *before* the PERFORM. Root cause is in the body itself: each `so_match` line does `SELECT … FOR UPDATE OF sl` on `sales_order_lines` plus three aggregate SELECTs (`SUM(qty_shipped)` from `so_shipment_lines`, `SUM(qty)` from `customer_invoice_lines`, `SUM(qty_to_ar_unsettled)` from `customer_return_lines`). Under 32-writer concurrency the FOR UPDATE on the same `sales_order_lines` row that a contemporaneous `post_so_ship` is reading creates a serialization chain that the dispatcher's transport-layer fix cannot help.

### `acct-c4p` ROI — confirmed (with caveat)

For the three transport-dominated wrappers, shape L (pseudo-sync via LISTEN/NOTIFY) compresses the slice that dominates p99. Expected per-wrapper p99 improvement once `post_posting_lines` is moved off-thread:
- `post_so_ship`: 2 812 ms → ~25 ms (setup+followup) — **~110× drop**
- `post_wo_start`: 4 233 ms → ~10 ms — **~420× drop**
- `post_op_move`: 7 038 ms → ~11 ms — **~640× drop**

**Combined-wrapper p99 ceiling under c4p alone**: ~3 100 ms (the surviving `post_customer_invoice` setup tail) — i.e. `c4p` alone reduces combined p99 from 2 902 ms to roughly the same number, because the combined-p99 union-tail just shifts from the op_move side to the customer_invoice setup side. **c4p delivers the predicted per-op gain on three wrappers but the combined-wrapper p99 number won't move until `post_customer_invoice`'s setup contention is also fixed.**

`acct-c4p` should claim — the per-wrapper gain is real, deterministic, and concentrated where the workload spends time. But the headline combined-p99 metric in this baseline (2.35 s in the original 1s6r run, 2.90 s in this re-run) is a poor evaluation target for c4p in isolation; report on the per-wrapper p99s instead.

### Filed follow-up

`acct-3aak` (P3) — `post_customer_invoice` setup-side contention: SO-line `FOR UPDATE OF sl` + three aggregate SELECTs hold serialization-relevant locks across the LOOP. The `FOR UPDATE OF sl` is load-bearing — it protects the tolerance check (lines ~779-801 of mig 0018) against a contemporaneous SO-line edit (price renegotiation, qty adjustment, discount, cancellation — all in-scope ERP mutations). Path: (a) **first** batch the three aggregates outside the per-line LOOP (one scan per invoice, not per line) — surgical, no schema change, independently correct; (c) **measure-then-decide** — re-run h73o instrumentation after (a); if setup p99 drops under ~200 ms, stop here; if it stays high, the cross-wrapper lock chain with `post_so_ship` is dominating and materialized per-so_line running totals become the right next step.

### Note on `acct-zroo` (SERIALIZABLE)

This decomposition does not directly inform `acct-zroo`. Under SERIALIZABLE isolation the same physical wait chains would manifest as 40001 retries rather than long FOR UPDATE waits — the wrapper section budget would shift from "long single call" to "retry-loop of shorter calls" at a throughput cost that this rig can't measure. The transport vs setup distinction is orthogonal to the isolation-level decision.

### `acct-3aak` measurement (option (a) shipped — mig 0069)

**Same 32-writer × 600 s rig, post-mig-0069 (per-invoice JSONB hash-map of the three so_match aggregates + in-flight tracker; FOR UPDATE OF sl preserved per ERP-scope reasoning).**

| Wrapper | Section | n | p50 | p95 | p99 | max |
|---|---|---|---:|---:|---:|---:|
| `post_customer_invoice` | `setup` | 14 218 | 71 855 | 1 419 367 | **3 333 209** | 8 381 854 |
| `post_customer_invoice` | `post_posting_lines` | 14 218 | 1 433 | 3 743 | 6 289 | 22 284 |
| `post_customer_invoice` | `followup` | 14 218 | 0 | 0 | 0 | 0 |

**Verdict: option (a) was a noise term.** Setup p99 went 3 127 291 µs → 3 333 209 µs (within the rig's natural variance; deadlocks 513 → 548 between the two runs). The per-line `SELECT SUM` aggregates were NOT the dominant cost. p50 stayed flat (68.8 ms → 71.9 ms), p95 stayed flat (1 310 ms → 1 419 ms). The aggregate-batching is independently correct and saves ~3 scans per `so_match` line at the SQL layer, but does not move the p99 needle on its own.

**What dominates the remaining tail** (per plan-3aak decision tree): the `FOR UPDATE OF sl` per-line serialization chain against contemporaneous writers, plus the per-line `SELECT id INTO v_cust_unsettled` indexed lookup inside the LOOP. Both are within `setup`. The lock chain matches the "Setup p99 stays 500-3000ms" branch — option (c) (trigger-maintained materialized per-`so_line` running totals + per-line account-id memoization across the invoice) is the remaining surgical surface.

**Filed:** `acct-3aak-c` follow-up for option (c). Reopening 3aak isn't necessary — (a) is shipped, measured, dispositioned; (c) is a different work item with different schema impact.

### Re-running this addendum

```bash
T4_DURATION_SECS=600 T4_WRITERS=32 T4_REPORT_TIMINGS=1 \
  ./scripts/run-tests.sh --test load_phase1_mixed_workload \
  --release -- --ignored --nocapture
```

The `_wrapper_section_timings` table is TRUNCATEd at run start; aggregate is appended to the run's stderr summary when `T4_REPORT_TIMINGS=1`.
