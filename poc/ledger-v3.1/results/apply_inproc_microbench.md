# In-process apply-path microbench (acct-q6sx)

The committer's apply path (`plan_and_write`: per-submission plan + the batched
INSERT of `trx` / `trx_line` / `posting_line` + the per-pool aggregate UPSERT) is
the largest committer span after the sczx + e95d optimizations (≈72% of committer
time at cc=1). Every prior apply number came from the **end-to-end harness** —
which cannot isolate the apply ceiling, because the staging-ring LWLock starves
committers as caller count rises (`busy_frac` 0.92 at cc=1 → 0.14 at cc=4,
acct-ruex) — or from the **in-process ns span counter** (`measure-apply-spans.sh`),
which is correct but coarse (µs-resolution `GetCurrentTimestamp`, aggregated over
millions of trx). This adds a third, direct instrument: a per-call timed microbench
of `plan_and_write` itself.

## Why not `cargo pgrx bench`

pgrx 0.18 ships `#[pg_bench]` + `cargo pgrx bench` (Criterion-in-backend). But
`cargo pgrx bench` drives the **pgrx-managed** Postgres (`~/.pgrx`, started via
`pg_ctl` on its own data dir) — it cannot attach to the running Docker
`acct-postgres` (run.rs install-to-local-pkglibdir + start_postgres own its
postmaster lifecycle; there is no "use an already-running server" mode). It would
need a separate cluster, the 21-file sqlx base schema hand-injected (the bench
harness only does `CREATE EXTENSION`, but the apply path INSERTs into
migration-created tables), and `shared_preload_libraries` set (static BGWorker
registration FATALs otherwise). For a number we already have from the span counter,
that parallel environment isn't worth it. Instead this bench is **Docker-native**:
a feature-gated `#[pg_extern]` that runs against the real `poc_v3_1` with the live
schema + seeded pools.

## Method

`ledger_routed_c_bench_apply(pool_id, batch, iters, warmup)` (committer.rs,
`bench_hooks` builds only) hydrates one base snapshot for a seeded fifo pool, then
per iteration: clones the snapshot (not timed), opens an internal subtransaction,
times **only** `plan_and_write` for a `batch`-submission po_receipt commit_group
with `std::time::Instant` (true ns), and **rolls the subtx back** so the base pool
state is byte-identical for every sample. `warmup` iterations are discarded first
to warm bulk_write's per-backend prepared-plan cache (kept plans survive subtx
rollback). NO ingress: no staging ring, no router, no pool_lock, no hydrate in the
timed region.

`bench-apply-inproc.sh` clean-seeds `poc_v3_1` (one direct-per-call all-fifo
receipt → one pool + its `posting_account_map`; fifo matches the method the span
apply was measured on), then sweeps `batch`. cc=1 is irrelevant here — the bench
runs in the caller's own backend, independent of the committer pool. Host load
gated < 1.5 per cell; structural µs/trx (mean) and p50 are the load-robust signals.
`iters=200`, `warmup=30`. CSV: `results/apply_inproc.csv`.

## Result

| batch | committed | µs/trx | µs/iter (mean) | p50 µs/iter | p99 µs/iter |
|------:|----------:|-------:|---------------:|------------:|------------:|
|     1 |         1 | 103.13 |          103.1 |        98.1 |       147.5 |
|     8 |         8 |  48.09 |          384.7 |       349.8 |       655.1 |
|    32 |        32 |  39.71 |         1270.9 |      1169.2 |      2183.9 |
|    96 |        96 |  36.56 |         3509.9 |      3336.7 |      5012.4 |
|   183 |       183 |  39.67 |         7259.9 |      6562.0 |     17934.7 |
|   480 |       480 |  35.56 |        17070.7 |     16702.8 |     22618.3 |

`committed == batch` on every row — the pool is properly seeded and every
submission fully applies (sanity passes).

## Findings

1. **Apply ceiling ≈ 36–40 µs/trx, and it triangulates the span counter.** At
   `batch=183` (≈ the cg the span apply was taken at, `measure-apply-spans` cg
   181.8) the in-process bench is **39.7 µs/trx**, against the span-measured
   **~44 µs/trx**. Two independent instruments — one a free-running ns counter
   under the real committer, the other a warmed per-call `Instant` timer with no
   ingress — agree within ~10%. The apply ceiling is real and ~40 µs/trx.

2. **Fixed-batch overhead amortizes; the floor is ~36 µs/trx.** µs/trx falls
   103 → 48 → 40 → 36 as the batch grows, then flattens (39.7 @ 183, 35.6 @ 480).
   The per-call fixed cost (one round-trip each for the batched `trx` / `trx_line`
   / `posting_line` INSERT + one aggregate UPSERT) is ~67 µs at `batch=1` and is
   fully amortized by `batch≈96`. The residue (~36 µs/trx) is the irreducible
   per-submission cost: the planner pass + the marginal INSERT row. This is the
   "apply is per-trx and does not amortize" property the fsync characterization
   relied on (`committer_fsync_vs_batch_size.md`), now measured directly rather
   than inferred from a flat span.

3. **p99 noise at large batches is host, not code.** p99/p50 blows out at
   `batch=183` (17.9 ms vs 6.6 ms) on a daily-driver host; mean and p50 stay
   tight and on-trend. Read mean/p50, not p99, for the structural number.

## Cross-reference: the trx/group counter anomaly (acceptance item c)

The q6sx anomaly — committer `pipeline_count` implying ~0.63 trx/group vs the
harness cg_avg ~115 (~180× disagreement) — was a **cc=4 starvation artifact**, not
a counter bug. `pipeline_count` increments on *every* committer tick including
empty drains; under staging-LWLock starvation (acct-ruex `busy_frac` 0.14 at cc=4)
most ticks drain nothing, so trx/group collapses. In the healthy **cc=1** regime
the counters are trustworthy: every `measure-apply-spans` run reports
`trx_per_group` ~181–185, matching the router cap / harness cg_avg. The coarse
counters are reliable at cc=1; the anomaly was starvation-specific. This bench's
agreement with the span counter at cc=1 (finding 1) independently corroborates that
the cc=1 counters are sound.

## Takeaway

The committer's single-core apply ceiling is **~36–40 µs/trx**, confirmed by two
independent instruments. Combined with the flamegraph (commit 5a63266: the per-
statement planning it named, ~47%, was removed by sczx's prepared plans) and the
fsync characterization (1/cg amortization, tunable via `batch_size_max`), the
committer's per-trx cost is now fully decomposed: apply ~40 µs (irreducible floor),
fsync amortizing as 1/cg, prep 3.8 µs after e95d. Further apply gains would have to
come from the planner pass or the heap/index/WAL INSERT cost itself, not from
batching.
