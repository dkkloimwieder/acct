# SPIKE-A results — staging-table outbox vs shmem routed (acct-0at4.11.1)

**Question (FEEDBACK-ARCH.md #3 / alt A):** is the shmem routed stack (CQ ring,
arena, router union-find, identity/generation election, the `pg_xact_status`/eject
apparatus) buying anything over a durable, transactional, ordered queue that
Postgres already provides — a table drained with `FOR UPDATE SKIP LOCKED`?

**Decision gate:** if staging-table routed lands within ~20% of shmem routed on
the hot-pool scenarios (S5/S7/S8), the routed crate is deletable.

## What was built (throwaway)

- `ledger_inbox` table (`bench/spike-a-inbox.sql`): caller INSERTs one row per
  submission inside its own tx.
- `ledger_staging_drain_c(limit)` (`ledger-direct-c/src/drain.rs`): claims up to
  `limit` pending rows with `FOR UPDATE SKIP LOCKED ORDER BY id`, applies them as
  one coalesced commit group, marks them done. The apply path
  (`pool_lock::acquire_pool_locks` → `hydration::hydrate_snapshot` →
  `plan_apply_provisional` → batched `bulk_write::*`, one aggregate UPSERT per
  pool) is a byte-identical mirror of the routed committer's `plan_and_write`, so
  only the transport differs.
- Harness `--mode staging` (`driver_staging.rs`): fire-and-forget INSERT callers +
  K committer connections looping the drain; same `trx`-polling observer, same
  throughput/latency measurement as `--mode routed`.

Client-driven committer (chosen option A): K=4 looping connections = routed's
`COMMITTER_COUNT`. This *handicaps* staging with a per-batch client round-trip an
in-postmaster BGWorker would not pay — so it is a conservative lower bound on
staging's advantage.

## Method

- Same box, same `poc_v3_1`, back-to-back interleaved routed/staging reps so the
  staging/routed **ratio** is robust to the noisy host (Chrome ≈120 procs, load
  ≈1.6; absolutes bounce, direction does not).
- 400 callers (capped: `max_connections=500`, no pooler), 4 committers each,
  `drain_batch=200` = routed's `batch_size_max`. 15 s/run, 3 reps/cell.
- S5: single hot pool, depth 10, 100% deplete (per-run reseed). S7: Zipf(1.2),
  DEEP (1000 layers), 100% deplete. S8: Zipf(1.2) complex multi-line, DEEP, 50%
  deplete. (Depth is immaterial to this comparison — both flavors are
  aggregate-only/depth-independent — but S7/S8 are run at their spec depth.)

## Results — materialization throughput (trx/s), median [min–max] of 3 reps

| scenario | routed | staging | staging/routed | routed ack p99 | staging ack p99 |
|----------|--------|---------|----------------|----------------|-----------------|
| S5 (hot pool, d10)    | 2280 [1951–2292] | 9969 [8938–11267] | **4.4×** | 5.2 s | 130 ms |
| S7 (Zipf, deep)       | 2001 [1930–2337] | 10624 [9133–11985] | **5.3×** | 5.0 s | 121 ms |
| S8 (Zipf complex, deep) | 523 [187–1045] | 8880 [8124–9575] | **~17× (8.5–47×)** | 5–68 s | 140–165 ms |

Every routed run shed **~57% of offered load** as `enqueue_errors` (bounded shmem
staging ring backpressure under hot-pool saturation: ~1000 errors/run on S5/S7).
Every staging run had **0 errors, 0 submitted-but-unseen**. On S8, routed spends
**78% of committer time waiting on `pool_lock`** (`span_pool_lock_frac` 0.776) —
the router's cross-chunk lock-overlap pathology on complex multi-pool trx —
collapsing it to hundreds of trx/s; staging sidesteps it.

Correctness: receipts-only conservation is exact (`pool_state.qty == Σ trx_line.qty`
and `value_sum == Σ qty·unit_cost`, 0 mismatches over 12.5k trx); depletion runs
leave 0 negative-qty pools and drain the inbox to empty; the apply is
byte-identical ledger-core.

## Verdict

**The routed crate is deletable on throughput grounds.** The decision gate
(within ~20% → deletable) is not merely met — on the three hot-pool scenarios the
shmem stack was built to win, the table+`SKIP LOCKED` alternative is **4–5× faster
on S5/S7 and ~17× faster on S8** (median; 8.5–47× across reps), with **0 shed vs
~57% shed** and **~40× better
caller ack latency** (130–165 ms vs 5–68 s), while being **durable** (each caller
INSERT is WAL-logged, vs routed's non-durable shmem push) and deleting the entire
CQ-ring / arena / router-union-find / identity-generation / xid-triage-eject
apparatus. The chosen client-driven committer *understates* the win (it pays a
round-trip a BGWorker would not), so the in-postmaster BGWorker variant (option B)
would only widen the gap — it is not needed to reach the verdict.

### Honest caveats (do not overclaim)

- **Closed-loop / full-blast.** Part of staging's win is that the unbounded table
  absorbs the burst while routed's bounded ring sheds it. An open-loop
  at-a-latency-SLO comparison (TEST-5 / acct-0at4.8) would refine the *drain*
  ceiling ratio — but it cannot reverse direction: routed's hot-pool drain ceiling
  (~2000–2300 trx/s, the value it materializes regardless of offered load) is
  itself several-fold below staging's (~10000 trx/s).
- **Bounded backpressure traded for unbounded growth.** Staging's table grows on
  disk (done rows accumulate; the drain reads a partial index on `NOT done`, so the
  scan stays cheap, but production needs DELETE/partition + vacuum). Routed's ring
  is memory-bounded by construction.
- **Capped at 400 callers** (no pooler); the ratio is concurrency-robust across
  reps and scenarios.
- **N=3 reps, noisy host.** Formal CIs (acct-0at4.10 discipline) are unnecessary
  here — the effect (4–40×) dwarfs the inter-rep spread — but the numbers are
  ranges, not tight estimates.

### Implication for the gate

This is the SPIKE-A input to GATE-VERDICT (acct-0at4.11.5). It argues **delete the
routed crate** and adopt the staging-table transport. The design-v3.1 verdict
paragraph and the re-triage of acct-0at4.1 (cross-chunk drop — dissolved by
per-pool `ORDER BY id`) and acct-0at4.2 (xid burn — dissolved by INSERT-in-tx) are
written at .11.5 once SPIKE-B, ARCH-POSTURE, and ARCH-RECALC-FEED also report.
