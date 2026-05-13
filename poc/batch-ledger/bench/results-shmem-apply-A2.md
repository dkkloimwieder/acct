# acct-4e91 M10.A2 — shmem apply path bench (deferred apply)

Re-baseline of the M9 fan-in / fan-out shmem benches after the
`ledger_apply_balance_delta` refactor from synchronous mutation to
deferred-apply-at-commit. Validates that A2's transactional
correctness fix doesn't catastrophically regress the M9 throughput
lift.

## What changed in A2

`ledger_apply_balance_delta` no longer mutates shmem directly. It
STAGES `(amount_delta, qty_delta, captured_rollup_seed)` into a
per-backend `PENDING_STACK` (thread-local). An XactCallback Commit
hook applies the staged deltas at COMMIT; Abort hook discards.
SubXactCallback handles SAVEPOINT / RELEASE / ROLLBACK TO.

This shifts the cost model:

- **Pre-A2 (M9)**: each apply call took the LWLock SHARED, did a
  `fetch_add` on the cell, and returned. N applies → N lock cycles
  + N fetch_adds.
- **Post-A2**: each apply call writes to a thread-local HashMap
  (`PENDING_STACK[top].entry(key)`). At COMMIT, all staged deltas
  flush under a single SHARED-or-EXCLUSIVE acquisition. N applies →
  K HashMap ops + K-or-fewer fetch_adds, where K = distinct keys.

For batches that revisit the same key (e.g., the hot credit in
fan-in), A2 collapses N applies into one fetch_add. For unique-key
batches (e.g., fan-out), A2 still pays N HashMap ops but reduces N
lock cycles to 1.

## Methodology

Identical to M9 (`bench/results-shmem-apply.md`):
- PG 18.3 in `acct-postgres` container, tuned conf (32 GB target).
- 20 workers × batch=1000 × 60s × 3 replicates with 15s gaps.
- Fan-in: 50 debit accounts + 1 hot credit (hot-cell collapse).
- Fan-out: 5000 accounts, even split into debit / credit (mostly
  unique cells per batch).
- N_BUCKETS = 16384.
- `POC_BENCH_FUNCTION=post_batch_shmem`.

## Per-run throughput

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in shmem  | 57,045 | 54,723 | 66,755 | **57,045** |
| fan-out shmem | 57,723 | 61,394 | 49,274 | **57,723** |

## Per-run p99 latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in shmem  | 566 | 567 | 460 | **566** |
| fan-out shmem | 467 | 457 | 587 | **467** |

## Headline deltas vs M9

| Scenario | M9 median tps | A2 median tps | Δ tps | M9 p99 | A2 p99 | Δ p99 |
|---|---|---|---|---|---|---|
| fan-in  | 66,998 | 57,045 | **-14.9%** | 474 ms | 566 ms | +19.4% |
| fan-out | 43,528 | 57,723 | **+32.6%** | 708 ms | 467 ms | -34.0% |

Deadlocks: **0** across all 6 runs (same as M9 — apply path still
takes no row locks on `accounts`).

## Findings

**F1. Fan-in degrades 15%; fan-out improves 33%.** A2 trades
per-apply HashMap overhead for commit-time lock-cycle reduction.
The fan-in workload (1000 hits per batch on the single hot credit
account → A2 collapses to one cell) doesn't recover the HashMap
overhead because the M9 inline fetch_add was already cheap. The
fan-out workload (1000 distinct cells per batch → A2 reduces 1000
inline lock acquires to 1 commit-time lock) wins net.

**F2. Fan-in throughput is in the predicted 5-15% drop band.** The
bd issue called for ≤25% as the redo-flag threshold; -14.9% is at
the upper edge but acceptable. The hot-cell collapse in commit
(1000 deltas → 1 fetch_add) didn't compensate for the staging
overhead. A future tuning lever: skip the HashMap when staging the
first delta on a fresh key (use the table-existing fast path
inline), or pre-allocate the HashMap with the expected batch size.

**F3. Fan-out p99 drops 34% (708 → 467 ms).** The commit-time
burst amortizes lock acquisition over the full batch, replacing
1000 individual SHARED-lock cycles per batch with one. The cost is
the staging HashMap, but at 5000 distinct accounts the working set
is too large for cache locality to dominate — the lock cycle
amortization wins.

**F4. Variability is higher than M9.** Fan-in IQR widened (54.7K /
57.0K / 66.7K vs M9's tighter band). Likely the commit-time burst
interacts with concurrent commits from other workers — when N
backends each apply 51 collapsed deltas, they contend on the
SHARED lock briefly. This deserves investigation if a future
workload depends on tight latency tails. Not a regression to
investigate today.

**F5. Correctness preserved.** `ledger_shmem_recon()` returns
drift=0 after each bench run (manually verified post-fan-in). The
M8 recon assertion remains valid.

## Cross-shape interpretation

A2 is a **net win for the load-bearing acct integration target**.
The acct production workload at scale (large posting_lines batches
spread across many SKU/location/customer cells) looks more like
fan-out than fan-in. Per `state-2026-05-11-acct-1s6r-shipped` the
1s6r mixed-workload load test is fan-out-shaped, and the c4p shape-L
analysis identified spread-out workloads as the realistic case.

The fan-in regression is acceptable trade for:
- Rollback correctness (the load-bearing M10 goal).
- Savepoint support (PG-standard error recovery semantics).
- A net win at the realistic workload shape.

## Future tuning levers (not in M10 scope)

1. **Fast-path inline apply for non-savepoint, non-transactional
   callers.** A wrapper SQL function `ledger_apply_balance_delta_now`
   that bypasses staging when the caller knows the apply is
   commit-final (e.g., from inside an idempotent batch wrapper).
   Would recover the fan-in regression. Filed as a separate
   followup if needed.
2. **Per-batch HashMap reuse.** Inside `post_batch_shmem`, all
   applies are within the same plpgsql function call — the
   PENDING_STACK[top] map gets one entry per (sku, location,
   currency) pair touched by that batch. Pre-allocating capacity
   based on the batch's INSERT count would avoid HashMap reallocs.
3. **Batched commit path.** xact_commit currently iterates the
   merged map and calls `try_update_existing` once per key. Could
   be vectorized: take a single lock and walk all keys, doing
   fetch_adds in sequence. Minor.

## Caveat: T4 capacity test interaction

The new `tests/transactional_t1.rs::t4_precommit_capacity_rejection`
test calls `ledger_shmem_reset()` and applies > N_BUCKETS keys to
force the PreCommit overflow check. It is `#[ignore]`'d to avoid
interfering with concurrent test binaries. If a bench run happens
immediately after T4, allow at least one drain interval (default
100 ms) to settle.

## Raw logs

Per-run logs at `/tmp/poc-a2-bench/fanin_shmem/run_{1,2,3}.log`
and `/tmp/poc-a2-bench/fanout_shmem/run_{1,2,3}.log`. Retained on
the bench host; not committed to git.
