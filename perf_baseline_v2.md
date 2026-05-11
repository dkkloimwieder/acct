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
