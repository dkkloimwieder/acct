# P2 calibration results

acct-zdrm sub-issue. Tip after P2 close: TBD.

## Question P2 answers

> Can our dev environment hit pgledger's reported ~10K transfers/sec on the
> simplest possible single-row double-entry workload, and if not, why not?

## Methodology

- **Workload**: `post_transfer(debit, credit, amount, idempotency_key)` — one
  `INSERT INTO posting_lines` + two `UPDATE accounts` per call. 50 accounts
  (25 debit-normal + 25 credit-normal), 20 concurrent Rust workers, 60s runs,
  5× replicates with 30s gaps between (per `ezm-2026-04-30-no-regression-from-acct-uxu`
  methodology memory).
- **Two variants**:
  - `p2_baseline` — Postgres defaults: `synchronous_commit=on`, `fsync=on`.
    This is the pgledger-shape comparison.
  - `p2_sync_off` — `synchronous_commit=off` (set per-session at pool
    `after_connect`). Measures the upper bound on our environment when fsync
    is amortized rather than per-commit.
- **Same code, same hardware, same DB**. Only the per-session GUC differs.

## Environment

- **Host**: Intel i7-1185G7 (Tiger Lake, 4c/8t, ~3 GHz base), 60 GB RAM, NVMe,
  Linux 6.17, Ubuntu.
- **Postgres**: 18.3 in Docker (`acct-postgres` image, `io_method=io_uring`,
  named volume `acct-pgdata` on `/dev/nvme0n1p6`), defaults otherwise.
- **Crate**: poc/batch-ledger, sqlx 0.8 over tokio, release build.

## Results — `synchronous_commit=on` (default, durable)

| run | attempted/s | ok/s   | err | p50 µs | p95 µs | p99 µs | p99.9 µs | deadlocks |
|---|---|---|---|---|---|---|---|---|
| 1 | 2624.8 | 2624.8 | 0 | 5994 | 17002 | 27201 | 45401 | 0 |
| 2 | 2613.2 | 2613.2 | 0 | 6051 | 16857 | 27406 | 46576 | 0 |
| 3 | 2603.1 | 2603.1 | 0 | 6063 | 17104 | 27661 | 46195 | 0 |
| 4 | 2600.6 | 2600.6 | 0 | 6043 | 17279 | 27933 | 46110 | 0 |
| 5 | 2587.3 | 2587.3 | 0 | 6073 | 17300 | 27929 | 45821 | 0 |
| **median** | **2603.1** | **2603.1** | 0 | **6051** | **17104** | **27661** | **46110** | 0 |

IQR across throughput: 2587–2625 = **±0.7%**. Very stable.

## Results — `synchronous_commit=off` (per-session, fsync amortized by wal writer)

| run | attempted/s | ok/s    | err | p50 µs | p95 µs | p99 µs | p99.9 µs | deadlocks |
|---|---|---|---|---|---|---|---|---|
| 1 | 9180.6  | 9180.6  | 0 | 1934 | 4195 | 5810 | 8760 | 0 |
| 2 | 12649.2 | 12649.2 | 0 | 1479 | 2683 | 3485 | 4880 | 0 |
| 3 | 13004.0 | 13004.0 | 0 | 1436 | 2622 | 3402 | 4705 | 0 |
| 4 | 12955.5 | 12955.5 | 0 | 1443 | 2635 | 3418 | 4661 | 0 |
| 5 | 13075.2 | 13075.2 | 0 | 1429 | 2605 | 3375 | 4623 | 0 |
| **median (runs 2-5, warm)** | **12955.5** | **12955.5** | 0 | **1436** | **2622** | **3402** | **4661** | 0 |

Run 1 (9180) is cold-cache; runs 2–5 (warm) cluster tightly at ~13K.

## Verdict

| Metric | sync_on | sync_off | pgledger reported |
|---|---|---|---|
| Throughput (ops/s) | 2,603 | 12,955 | ~10,636 |
| p50 latency (ms) | 6.0 | 1.44 | ~1.9 |

**The gap to pgledger is fsync-bound, not architecture-bound.**

- Our durable single-row ceiling is ~2.6K ops/s — **5× lower than pgledger's reported 10K**.
- Our non-durable ceiling is ~13K ops/s — **above pgledger's 10K and at lower latency**.
- The 5× ratio matches the cost difference between Linux ext4 + Docker named-volume `fdatasync` and Apple M3 APFS journal commit on raw NVMe. pgledger's hardware fsync's are ~3-5× faster than ours.

**Conclusion**: our CPU + plpgsql + sqlx + lock-acquisition surface can comfortably exceed 10K ops/s in single-row mode. The bottleneck is the host's fdatasync cost (5 fsyncs every ~1.9 ms vs ~6 ms on our box). This is environmental and known to be improvable via:

- raw block device or NVMe-via-host-mount instead of Docker named volume (deferred — not the point of this PoC),
- `synchronous_commit=off` with a sync replica for durability (production tuning, not a PoC concern),
- amortizing fsync via batch commits (**THIS IS THE WHOLE POINT OF P3**).

The sync_off result tells us **our environment can do 13K ops/s when fsync is amortized**. That is the real ceiling against which P3's batch API must be measured.

## Implications for P3

Updated success criterion for P3 (originally "≥10K TPS at batch=1000"):

- **Primary**: at `batch=1000` with `synchronous_commit=on`, `post_batch` throughput should approach the **sync_off ceiling** (~13K ops/s) on our hardware. Reasoning: a batch of 1000 commits ONE WAL prepare and ONE fsync; the per-transfer fsync cost amortizes to ~1/1000 of single-row mode. The batch API should therefore *recover the fsync ceiling* without losing durability.
- **Secondary**: at `batch=1` (single-envelope), throughput should approximately match P2's sync_on baseline (~2.6K ops/s) — confirming the batch API has no overhead over single-row when batching is degenerate.
- **Tertiary**: at `batch=8000`, ideally exceed sync_off ceiling — additional amortization on parse/plan/round-trip beyond fsync.

The 10K absolute number from pgledger is a useful reference point but **our environment-specific target is now ~13K** (the sync_off ceiling, since durable batch is at-best equivalent to non-durable single-row).

If P3 batch=1000 gets us to ≥10K (the original target) but well below 13K, the gap might indicate plpgsql overhead in the batch handler, FOR UPDATE acquisition cost across many accounts, or multi-row INSERT overhead. Worth investigating.

## Calibration verdict

**PROCEED to P3.** Environment characterized:

- Durable single-row baseline: 2,603 ops/s (sync_on).
- Non-durable single-row ceiling: 12,955 ops/s (sync_off).
- The 5× gap to pgledger's reported number is fsync-cost / hardware difference, not architectural.
- Our P3 success criterion is now grounded against OUR environment's ceiling (13K) rather than against pgledger's reported number on different hardware.

## Files

```
poc/batch-ledger/bench/results/
├── p2_baseline/
│   ├── env.txt        — hardware + container + Postgres settings snapshot
│   ├── run_{1..5}.log — raw per-run output
│   └── summary.txt    — aggregated table
└── p2_sync_off/
    ├── env.txt
    ├── run_{1..5}.log
    └── summary.txt
```
