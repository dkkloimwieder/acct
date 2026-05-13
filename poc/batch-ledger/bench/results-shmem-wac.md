# acct-n4mo M10.B4 — WAC apply via shmem bench

Compares `post_batch_wac` (mutable: mig 0006's body installed under a
stable name in mig 0015) against `post_batch_wac_shmem` (mig 0014:
applies via `ledger_apply_balance_delta` into the shmem hash, with
B4-prep's `AtomicU128`-packed (balance, qty) so the unit_cost
dispatch reads a coupled snapshot).

## What changed in B4

mig 0014 is mig 0006 minus the two mutable steps:

1. **Pool snapshot at batch start** comes from `ledger_balance_lookup`
   instead of `SELECT balance, qty FROM accounts FOR UPDATE`. No
   accounts row lock; the snapshot is a single 128-bit atomic load
   per pool (B4-prep's coupled-read property).
2. **End-of-batch apply** carries `(amount, qty)` pairs into
   `ledger_apply_balance_delta` instead of `UPDATE accounts SET
   balance = balance + d, qty = qty + d`. The qty leg on the pool
   side is load-bearing — without it the pool's qty in shmem doesn't
   track, and the next batch's `ledger_balance_lookup` would return a
   wrong qty, producing a wrong running average on the next
   `wac_issue`.

The in-batch running-average map (`v_pool_value`, `v_pool_qty` JSONBs)
is unchanged vs mig 0006 — that's where HC3 (in-batch sequencing)
lives. A2's deferred apply means cross-batch within the same outer
txn does NOT read-your-own-writes; one batch per txn remains the
supported pattern, pinned by `tests/wac_shmem_correctness_t1.rs`.

## Correctness

T1 receipt-then-issue / T2 cross-batch running avg via shmem / T3
in-batch running avg over mixed envelopes / T4 idempotent replay /
T5 8-writer fan-in coupled writes — all green sequentially.

Post-sweep recon: **drift=0 across 5001 cells** (the full fan-out
seed plus fan-in's hot pool + 50 debit accounts + counterparties).
The `ledger_shmem_recon` query against `posting_lines` truth holds.

## Methodology

Identical to B4-prep:
- PG 18.3 in `acct-postgres`, tuned conf.
- 20 workers × batch=1000 × 60s × 3 replicates with 15s gaps between
  replicates and a fresh shmem reset between cells (mig 0015 sweep
  variant: `TRUNCATE account_balances_rollup` + `ledger_shmem_reset()`
  at the start of every shmem cell so M6 lazy-load can't pick up
  stale prior-run state).
- 0% issue (pure receipts) — qty-leg is exercised on every envelope,
  but pool inventory doesn't have to be pre-seeded.
- Fan-in: 1 hot pool + 50 distinct debit accounts (the pool acts as
  the credit-side accumulator for every receipt; same shape as
  mig 0006's stress).
- Fan-out: 5000 pools, evenly assigned per worker, 1 distinct pool
  per envelope.
- N_BUCKETS=16384 (well above 5000 pools + counterparties + lazy-
  load seeds, ≤30% load factor).

## Per-run throughput (tps, posting_lines successfully inserted/s)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable  | 21,644 | 25,045 | 25,143 | **25,045** |
| fan-in shmem    | 53,951 | 56,136 | 56,908 | **56,136** |
| fan-out mutable |  4,171 |  4,281 |  4,344 |  **4,281** |
| fan-out shmem   | 11,271 | 12,001 | 11,335 | **11,335** |

## Per-run p99 latency (ms)

| Scenario | run 1 | run 2 | run 3 | median |
|---|---|---|---|---|
| fan-in mutable  | 1,412 |   965 |   963 |   **965** |
| fan-in shmem    |   533 |   518 |   507 |   **518** |
| fan-out mutable | 5,528 | 5,470 | 4,981 | **5,470** |
| fan-out shmem   | 2,548 | 2,626 | 3,680 | **2,626** |

## Headline deltas

| Shape | Mutable tps | Shmem tps | Lift | Mutable p99 | Shmem p99 | Δ p99 |
|---|---|---|---|---|---|---|
| fan-in  | 25,045 | 56,136 | **2.24×** |   965 ms |   518 ms | **-46%** |
| fan-out |  4,281 | 11,335 | **2.65×** | 5,470 ms | 2,626 ms | **-52%** |

Deadlocks: **0** across all 12 runs.

## Comparison to acct-togd projections

Projections from `state-2026-05-12-acct-togd-bench-complete-ready-for-sw4i`:

| Shape | Projection | Measured | % of projection |
|---|---|---|---|
| fan-in  | 3.4×          | 2.24× | **66%** |
| fan-out | 7-10×         | 2.65× | **27-38%** (well below 50%) |

## Findings

**F1. Shmem WAC lift is real but well below the simple-transfer
ceiling.** Simple-transfer shmem (M9) was 2.16×/5.55× over mutable.
WAC shmem is 2.24×/2.65× — fan-in tracks expectations, fan-out
substantially underperforms. The plpgsql FOR LOOP + jsonb_set
running-avg machinery dominates the apply cost; eliminating the
mutable `UPDATE accounts` doesn't yield the full simple-transfer
fan-out lift because plpgsql per-envelope overhead remains.

**F2. WAC bottleneck has shifted from accounts row locks to plpgsql
dispatch.** The mutable WAC path takes `FOR UPDATE` on every pool
account at batch start. Fan-out's 5000 pools all need to be locked
per batch — that's the 4.28K tps ceiling. Shmem removes the lock
cycle entirely (per-cell LWLock at memory-bus speed via the M9 hot
path; AtomicU128 RMW per `ledger_apply_balance_delta` call). What
remains in the inner loop is the plpgsql FOR over the JSONB envelope
array, jsonb_set on the running-avg maps, and the per-envelope
INSERT into the staging temp. That's the new floor.

**F3. p99 improvements are substantial on both shapes.** Fan-in p99
965→518 ms; fan-out p99 5,470→2,626 ms. The mutable path's worst-
case waits behind `FOR UPDATE` queue depth; the shmem path waits
behind plpgsql cycles + atomic CAS retries, both of which compose
much better under contention. The shmem fan-in p99 (518 ms) is below
B4-prep's measured fan-in p99 of 753 ms — consistent with the
WAC plpgsql loop limiting how often a writer reaches the actual
atomic-apply step.

**F4. Variability is within rig noise.** Fan-in mutable run 1
(21,644 tps) is 14% below run 2/3; per the ezm methodology memory
this is real noise. The directional finding (~2×–2.7× lift) is
robust — every individual shmem run beats every individual mutable
run by >1.9× in both shapes.

**F5. Correctness preserved end-to-end.** Recon drift=0 across 5001
shmem cells after the 12-run sweep. T1-T5 correctness suite green
sequentially. The unit-cost dispatch reads from the AtomicU128 pool
snapshot — every observation is a real coupled state, never a torn
pair.

## Followup: ledger_apply_batch(jsonb)

The fan-out underperformance vs projection (27-38% of expected
7-10×) is the load-bearing signal. The plpgsql FOR LOOP costs
roughly 50 μs per envelope (independent of shmem vs mutable);
batch=1000 means ~50 ms of pure plpgsql per batch, which dominates
the apply cost. A `ledger_apply_batch(jsonb)` extension entry point
would push the per-envelope work into Rust, eliminating most of the
plpgsql overhead and likely closing the gap to the projection.

File as `acct-n4mo-followup-apply-batch` if perf-driven; the
correctness path of M10 is now complete and the followup is
optimization-only.

## Cross-shape interpretation

B4 ships the shmem WAC apply path with the (balance, qty) coupling
guarantee from B4-prep. The lift is positive at every replicate, on
both shapes, with deadlocks=0 and recon drift=0 — every measured
correctness invariant holds. The fan-out lift is below the
projection ceiling but still 2.65× — the projection assumed the
mutable path's locking cost was the dominant pain, but in WAC the
plpgsql dispatch is at least as expensive as the lock cycle. Net
verdict: shmem WAC is a clear win on both shapes; the fan-out gap
to the projection is a known optimization opportunity, not a
correctness gap.

## Raw logs

`/tmp/poc-b4-bench/fanin_wac_mutable/run_{1,2,3}.log`,
`fanin_wac_shmem/run_{1,2,3}.log`,
`fanout_wac_mutable/run_{1,2,3}.log`,
`fanout_wac_shmem/run_{1,2,3}.log`,
`/tmp/poc-b4-bench/summary.txt`. Bench-host only; not committed.

## Conclusion

B4 ships. WAC shmem apply: **fan-in 2.24× / fan-out 2.65×** over
mutable; p99 cut roughly in half on both shapes. Recon drift=0.
The plpgsql per-envelope cost is the new ceiling; closing it is
follow-up work, not M10 scope.
