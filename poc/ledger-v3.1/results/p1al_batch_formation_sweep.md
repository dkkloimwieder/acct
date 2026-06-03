# acct-p1al Phase 1 — router batch-formation × batch_size_max on a multi-SKU spread

**Question.** Sustained-load benchmarking (commit 73467f2) showed that on a multi-SKU /
multi-pool *spread* workload the routed committer is starved of large commit_groups
(~8 documents instead of ~180), defeating the committer-side write batching shipped in
acct-sczx / acct-e95d / acct-q6sx. Does the existing `router_pack_disjoint` lever
(acct-xdwk, default **off**) restore commit_group size toward `batch_size_max` and recover
throughput — and does flipping it on regress the hot-pool / deep-pool aggregation cases
(s5 / s7) the lever was *not* built for?

**Bottom line.** `router_pack_disjoint=on` is **strictly win-or-neutral across the entire
regime space**: a 2–2.7× throughput gain wherever commit_groups fragment (spread, deep
zipf), and measurably neutral where they don't (single hot pool, row-lock-bound mixed). It
never regresses. No queue-fill-first prototype is needed — the existing lever solves the
fragmentation. Recommendation: **flip `router_pack_disjoint` default on and raise the
`batch_size_max` default from 50 → 200.**

Methodology note (host): acct-postgres runs on a noisy daily-driver workstation; Chrome
alone swings routed throughput ~2×. Every cell below is `wait_for_quiet_host`-gated at
start, and **comparisons are between cells measured in the same session against the same
current build** — absolute trx/s is load-sensitive, OFF-vs-ON ratios within a session are
not. (This is also why the *historical* s5 packoff CSV is excluded from the s5 comparison —
it predates the acct-e95d/q6sx build optimizations and ran on a load≈4 host; a fresh
current-build OFF leg was run instead.)

---

## 1. Decisive pass — the spread (s2, cc=4, 16 callers, 64 pools, batch_window=20ms, all-fifo)

300-second sustained routed runs via pgbouncer. `commit_group_size_avg` is in **documents**.
Throughput is the per-second-sampled median (ramp/drain trimmed). Spans are summed committer
wall-time ÷ trx.

| cell | cg (docs) | median trx/s | vs OFF | fsync µs/trx (% of txn) | committed p99 |
|---|--:|--:|--:|--:|--:|
| pack **OFF** bsm200 | 8.1 | 5,288 | 1.00× | 322.8 (51.6%) | **58.8 s** |
| pack ON  bsm50  | 42.4 | 8,283 | 1.57× | 52.3 (17.1%) | 4.24 s |
| pack ON  bsm200 | 148.3 | 10,900 | **2.06×** | 14.8 (7.5%) | 4.01 s |
| pack ON  bsm800 | 489.5 | 12,538 | 2.37× | 4.9 (2.4%) | 3.80 s |

**The fragmentation was strictly dominated, not a throughput/latency trade.** Packing
recovers 2× throughput *and* cuts committed-p99 ~14.6× (58.8 s → 4.0 s). The OFF committer
is fsync-bound (51.6% of wall-time), so its ingress queue backs up → multi-second to
~minute committed latency. ON, fsync amortizes away and the backlog clears.

Committer span shift (µs/trx), OFF → ON bsm200:

| span | OFF | ON bsm200 | note |
|---|--:|--:|---|
| commit (fsync) | 322.8 | 14.8 | the amortized cost — collapses as cg grows |
| hydrate | 51.1 | 4.4 | per-group fixed cost, amortized |
| prep (decode/triage/dedup) | 129.8 | 67.0 | per-group fixed cost, amortized |
| apply | 111.2 | 84.8 | per-trx, ~flat (now the dominant cost) |
| pool_lock | 11.0 | 26.7 | *rises* with cg (more pools per group) |

Wait-event of-busy (committer sampler): shmem-LWLock 53.3% (OFF) → 63.5% (ON bsm200);
row-lock 0.1% → 6.2%. At **bsm800** the knee is past: throughput adds only +15% over bsm200
while `pool_lock` climbs to 74.4 µs/trx, row-lock reaches 22.2% of busy, and the rate gets
burstier (per-second min 2,275, stdev 2,327 vs 1,809 at bsm200) — a new pool-lock ceiling
emerging. **bsm200 is the knee**: most of the fsync amortization captured, contention still
moderate.

---

## 2. Non-regression — does flipping pack_disjoint on hurt the aggregation cases?

Via `run-batch-size-sweep.sh` (cc=4, 20s × N reps, load-gated, named wait diagnostic;
median of reps shown). cg in documents.

### s5 — single hot pool (zipf exp 100, depth 10), fresh OFF vs fresh ON (current build)

| bsm | OFF trx/s | ON trx/s | Δ% | OFF cg | ON cg | OFF wait | ON wait |
|--:|--:|--:|--:|--:|--:|---|---|
| 50  | 7,243 | 7,338 | +1.3 | 46.9 | 47.0 | Lock/tuple | Lock/tuple |
| 200 | 8,467 | 8,869 | +4.7 | 164.2 | 165.9 | Lock/tuple | Lock/tuple |
| 800 | 9,059 | 9,667 | +6.7 | 431.8 | 441.4 | Lock/tuple | Lock/tuple |

**Inert, as predicted.** On a single pool there is exactly one affinity component, so
pack_disjoint (which only fuses *disjoint* components) has nothing to pack — cg is identical
OFF→ON. Throughput is equal within session/load noise; the binding constraint is row-lock on
the one hot pool_state row (`Lock/tuple`), which packing cannot and does not touch. No
regression.

### s7 — deep zipf (1000 layers, zipf 1.2), same-session OFF vs ON

| bsm | OFF trx/s | ON trx/s | Δ% | OFF cg | ON cg | OFF wait | ON wait |
|--:|--:|--:|--:|--:|--:|---|---|
| 50  | 3,072 | 6,841 | +122.7 | 3.7 | 44.3 | LWLock/WALWrite | Lock/transactionid |
| 200 | 3,237 | 8,721 | **+169.4** | 3.8 | 163.0 | LWLock/WALWrite | Lock/transactionid |
| 800 | 2,749 | 8,630 | +213.9 | 3.8 | 452.9 | LWLock/WALWrite | Lock/tuple |

**Not a regression — a second big win.** The deep "Path C home field" fragments *worse* than
the spread (cg≈3.8) and is WAL-write-bound without packing. Packing fuses the disjoint tail
(cg → 163), shifting the bottleneck from commit/WAL to row-lock — the same mechanism as the
spread.

### s19 — Pareto-80/20 mixed (complex deplete+receipt, depth 100), on-disk OFF vs ON

| bsm | OFF trx/s | ON trx/s | Δ% | OFF cg | ON cg |
|--:|--:|--:|--:|--:|--:|
| 50  | 1,215 | 1,228 | +1.1 | 11.1 | 36.2 |
| 200 | 1,321 | 1,337 | +1.2 | 12.7 | 71.7 |

**Neutral.** Packing raises cg 5.6× but throughput is flat (±1.3%) because this workload is
already row-lock-bound (locks/trx ≈ 11–15 from concentrated depletions). Packing amortizes
commit cost that isn't the binding constraint here — so it neither helps nor hurts.

---

## 3. Why pack_disjoint is never harmful (structural)

`pack_disjoint_components` (router.rs) bin-packs only **pool-disjoint** components into a
commit_group, capped at `batch_size_max` *documents*. Same-pool candidates are already fused
into one component by the union-find affinity grouping *before* packing runs. Therefore a
packed group still does **one `pool_lock` + one aggregate UPSERT per pool** — packing never
co-locates two callers on the same pool into contention that didn't already exist. It only
puts *more disjoint pools* under one fsync/commit. That is pure commit-amortization: a large
win when commit/fsync/WAL is the constraint (fragmented disjoint work), and a no-op when
row-lock on a hot pool is the constraint (single/concentrated work).

Document atomicity (the issue's hard invariant) is preserved: packing chunks *sets of
submissions*; a submission is one staging entry carrying all its lines and is never split
(router.rs chunker `chunk_cap = batch_size_max` documents; existing acct-xdwk unit tests).

## 4. Regime summary

| workload | shape | OFF cg | ON cg (bsm200) | OFF→ON throughput | binding constraint |
|---|---|--:|--:|--:|---|
| s2 spread (64-pool) | disjoint | 8.1 | 148 | **+106%** | fsync → row-lock |
| s7 deep zipf | fragmented tail | 3.8 | 163 | **+170%** | WAL-write → row-lock |
| s19 Pareto mixed | concentrated | 12.7 | 72 | +1% (neutral) | row-lock (already) |
| s5 single hot pool | one component | 164 | 166 | ~0% (inert) | row-lock (Lock/tuple) |

## 5. Recommendation (Phase 2)

1. **Flip `router_pack_disjoint` default → on.** Strictly win-or-neutral across every
   measured regime; it is the minimal change (a default flip on an already-shipped, unit-
   tested lever) and needs no new code path. **A queue-fill-first batch-formation prototype
   is NOT warranted** — the existing lever already restores cg to `batch_size_max` on every
   fragmented workload.
2. **Raise `batch_size_max` default 50 → 200.** 200 is the knee: spread 2.06×, s7 +170%,
   committed-p99 ~4 s, with pool_lock/row-lock contention still moderate. bsm800 adds only
   +15% over bsm200 while pool_lock and row-lock contention climb sharply and the rate gets
   burstier — the start of a new ceiling, not a better default. At the current default of 50,
   pack-on already captures ~1.6×, but 200 captures the full amortization.
3. **Tests + re-measure:** an acceptance/property test asserting document atomicity under
   `router_pack_disjoint=on` (no submission split; one staging entry per submission), and a
   re-measurement confirming spread cg + throughput recover and s5/s7 do not regress.

## 6. Coverage / caveats

- batch_size_max points measured on the spread: {50, 200, 800}. 100 and 400 were **not**
  run — the knee is clearly bracketed at 200, but a finer 100/400 pass would pin the default
  more precisely if desired.
- Absolute trx/s is host-load-sensitive on this workstation; all conclusions are OFF-vs-ON
  ratios within a session against the current build. The historical s5/s19 sweep CSVs ran on
  a different build/host and are used only where both OFF and ON legs come from that same
  historical batch (s19); s5 used a fresh current-build OFF leg.
- s5/s7 deep runs used the standard deep-seed recipe (10,000 pools, 1,000 skus, 10 locations;
  depth 10 for s5, 1,000 for s7).

## Artifacts

- Spread: `p1al_s2_{off_bsm200,on_bsm50,on_bsm200,on_bsm800}.{md,csv,json}`
- s5: `batchdiag_s5_cc4_packon.csv`, `batchdiag_s5_cc4_packoff_fresh.csv`
- s7: `batchdiag_s7_cc4_{packoff,packon}.csv`
- s19 (on-disk): `batch_size_sweep_s19_cc4_{packoff,packon}.csv`
- Runners: `bench/sweep-p1al-decisive.sh`, `bench/sweep-p1al-nonregress.sh`;
  `bench/run-sustained-5min.sh` gained a `PACK_DISJOINT` env knob.
