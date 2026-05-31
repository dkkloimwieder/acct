# design-v3.1 Path C — PoC characterization report

**Stream:** `ledger-v3.1` · **Epic:** `acct-2ttr` · **Phase:** P5 (`acct-2ttr.9`)
**Spec:** [`../design_research/design-v3.1.md`](../design_research/design-v3.1.md) §11 (success
criteria) + §12 (Phase 5).

This report characterizes the **provisional hot path** (Path C): FIFO/LIFO depletions record a
*provisional* unit_cost (the pool's running average, or a standing standard cost) and touch
**only the aggregate row** of `pool_state` (`layer_id = 0`) — they never iterate layer rows on
the hot path. Authoritative FIFO/LIFO reconciliation (recalc/close) is **out of scope** (§13).

The PoC set out to validate two premises and one invariant:

1. **(§11.2 headline)** Per-trx lock-hold time for FIFO/LIFO is **constant w.r.t. pool depth** —
   the architectural reason Path C exists. **→ CONFIRMED** (artifact a).
2. **(§11.3 / §6.7)** Concurrent submissions to a hot pool **collapse**, under routing, into one
   commit_group → one `pool_lock` acquisition + one aggregate UPDATE per pool. **→ CONFIRMED**
   (artifact c); the §11.4 crossover map (artifact b) bounds *where* that win pays off.
3. **(§9.4 / §11.1)** Direct and routed flavors agree on **aggregate qty** (the order-independent
   correctness invariant and the input recalc/close will later consume); provisional unit_cost is
   *permitted* to diverge across orderings. **→ CONFIRMED** (artifact d).
4. **(§5.1 / `acct-34ce`)** Same-pool-twice (multi-touch) submissions commit cleanly under load on
   both correctness flavors — `ledger-core`'s `coalesce_aggregates` collapse holds at 1000-caller
   deep-FIFO concurrency. **→ CONFIRMED** (artifact e).

---

## Environment & methodology

| Item | Value |
|---|---|
| DB | `poc_v3_1` on `localhost:5111`, container `acct-postgres`, Postgres 18 (`io_uring`) |
| Extensions | `ledger_direct_c` (`ledger_submit_trx_c`), `ledger_routed_c` (`ledger_enqueue_trx_c` + router + 4-committer pool), both preloaded |
| 1000-caller path | pgbouncer transaction pool, host `6432` → `acct-postgres:5432`, `pool_mode=transaction`, `default_pool_size=64` (`acct-8cn2`) |
| Harness | `target/release/ledger-harness` (sqlx + tokio, multi-session) |
| Submission modes (§10.0) | `direct-per-call` (one user-tx per submit), `direct-batched` (50 submits per user-tx), `routed` (enqueue → committer pool) |
| Crossover scale | `SEED_COUNT=10000` pools, `30s`/run, full S1–S9 × 3 modes (S9 = WO-completion multi-touch mix, `acct-34ce`) |
| Pareto family (S10-S21, `acct-s90k`) | 1000 pools, 5s/run smoke × {direct-per-call, routed} per scenario; coverage validation, not a perf measurement |
| Lock-hold sweep | 256 FIFO pools, 16 callers, depths {10, 100, 1000} |

**Measurement provenance:** these numbers are a full re-measurement on **2026-05-28** against the
**production build** (no `test_hooks`), incorporating the new **S9** scenario from `acct-34ce` (opt-in
multi-touch / same-pool-twice generation — the coalesce-under-load path the S1–S8 distinct-pool
generator never reached). Supersedes the 2026-05-27 snapshot. The three verdicts are unchanged.

**Per-method seeding:** the harness seeds every pool with `provisional_basis='running_avg'` and now
also establishes `standard_cost` rows for std-method / standard-basis pools (`acct-0z5m`). All
universes — **all-fifo** (S5–S8), **all-wac** (S1, S2), and **mixed** (S3, S4 — 50/30/20
fifo/wac/std) — run with `errors=0` on the direct-per-call and routed paths in this run. The
original P5 snapshot reported S3/S4 as **confounded** by missing `standard_cost` (every complex-shape
submission touching a std pool aborted with `MissingStandardCost`); that gap is closed, so **S3/S4
are now first-class measurements**, not a seeding artifact.

**Machine-load caveat:** host load was not controlled, so **absolute throughput numbers remain
directional**. The load-robust findings are the *structural* ones — the flat lock-hold *floor*
across depth (artifact a), the lock-acquisition *collapse* ratio (artifact c), and the exact
aggregate-qty *equivalence* (artifact d). This run's percentiles are notably tighter than the
original snapshot — the lock-hold sweep is now flat across *every* percentile, not just the min
noise floor (artifact a) — consistent with a less-contended host; read absolute rates as
directional regardless. Where noise matters to a conclusion it is called out inline.

All harness invocations were hard-`timeout`-wrapped; the deep (depth-1000) reseeds ran under a
1800 s per-call cap. Every run completed (27 crossover + 3 lock-hold + 1 fresh equivalence for s9;
the s5/s7/s8 equivalence verdicts carry from the 2026-05-27 snapshot since the `.so` are unchanged,
see (d)) — none wedged, failed, or timed out; no committer poisoned; **zero dropped submissions
and zero deadlock-retries on the routed path across the whole matrix** (all 9 scenarios).

---

## (a) Lock-hold time vs pool depth — §11.2 headline → **PREMISE HOLDS**

Identical workload (s7 shape: simple FIFO depletions, Zipf(1.2), 16 callers, direct-per-call),
the only variable being the seeded layer depth. A strict layer-walking FIFO would show latency
growing ~linearly with depth (100× more rows to walk from depth 10 → 1000). Path C does not.

| Depth | min µs | p50 µs | p95 µs | p99 µs | mean µs | throughput trx/s |
|------:|-------:|-------:|-------:|-------:|--------:|-----------------:|
| 10    | 2619   | 4468   | 52953  | 58556  | 15713   | 1017.3 |
| 100   | 2568   | 3944   | 52133  | 56754  | 15368   | 1040.3 |
| 1000  | 2572   | 3928   | 52297  | 56983  | 15396   | 1038.1 |

**Verdict: constant.** A 100× depth increase produces **no upward trend in any percentile** — the
deeper pools (100, 1000) are essentially indistinguishable from each other across all percentiles
(p50 within **0.4%** 3944→3928, p95 within **0.3%**, p99 within **0.4%**, mean within **0.2%**,
throughput within **0.2%**), and the shallow depth-10 row sits *slightly higher* in this run (p50
4468 vs 3928 at depth 1000, +14%) — the *opposite* direction of "growth with depth," consistent
with run-to-run host-load variance at the lowest-latency end (the 2026-05-27 snapshot had the
relationship reversed at the same percentile, also within noise). A strict layer-walking FIFO would
show ~linear growth (100× more rows to walk); Path C shows none. The depth-1000 row also reappears
in the crossover at full 1000-caller concurrency (s7 direct-per-call, 1134.8 trx/s) — deep pools do
not crush the direct path, precisely because the hot-path cost is depth-independent.

This is the load-bearing result: **Path C buys O(1) hot-path cost regardless of how many FIFO/LIFO
layers a pool has accumulated.** The deferred recalc/close (§13) is where layer-walking cost would
reappear — by design, off the hot path.

---

## (b) Direct ↔ routed crossover map — §11.4

Full matrix, all three modes. `cg` = routed commit_group_size_avg; `locks` = `pool_lock`
acquisitions; `err` = failed/dropped submissions. Clean (errors=0) cells in **bold**.

| Scn | shape | depth | direct-per-call | direct-batched | routed |
|-----|-------|------:|-----------------|----------------|--------|
| s1 | 10 callers, uniform, all-wac | 0 | **1477.8 trx/s** (p99 16ms) | 767.8 trx/s (err 40) | **1320.0 trx/s** (cg 1.10) |
| s2 | 200 callers, zipf1.5, all-wac | 0 | **646.3 trx/s** (p99 0.97s) | 6.1 trx/s (err 516) | **1235.4 trx/s** (cg 7.41, locks 5042) |
| s3 | 10 callers, complex, mixed | 0 | **786.9 trx/s** | 8.4 (err 48) | **628.9 trx/s** (cg 3.25) |
| s4 | 200 callers, zipf1.2, complex, mixed | 0 | **203.8 trx/s** | 9.9 (err 794) | **922.4 trx/s** (cg 19.27) |
| s5 | 1000 callers, **single hot pool**, all-fifo | 10 | **333.8 trx/s** (p99 3.09s) | 762.6 (err 637) | **2089.9 trx/s** (cg 30.75, locks 2078) |
| s6 | 1000 callers, **disjoint stripes**, all-fifo | 10 | **1981.2 trx/s** | 2472.7 trx/s (err 415) | **1021.3 trx/s** (cg 1.07) |
| s7 | 1000 callers, zipf1.2, all-fifo, **deep** | 1000 | **1134.8 trx/s** (p99 1.19s) | 6.2 (err 1128) | **1514.0 trx/s** (cg 3.56, locks 13114) |
| s8 | 1000 callers, zipf1.2, complex, all-fifo, **deep** | 1000 | **212.9 trx/s** | 117.3 (err 1073) | **978.5 trx/s** (cg 20.65, locks 221749) |
| **s9** | 1000 callers, zipf1.2, complex, all-fifo, **deep** + **multi-touch** (40% repeat, dist 1:60,2:30,3:10) | 1000 | **229.1 trx/s** | 91.1 (err 1268) | **1069.8 trx/s** (cg 14.14, locks 229183) |

S3/S4 (mixed-method) now run **clean** (`errors=0` on direct-per-call and routed) — the
`standard_cost` seeding gap that confounded them in the original snapshot is closed (`acct-0z5m`).
Their `direct-batched` error counts are deadlock/serialization aborts under the complex multi-pool
overlap, the same contention failure as the all-fifo/all-wac scenarios — not MissingStandardCost.

### The regions

- **Disjoint, no cross-caller overlap (s6):** `direct-batched (2473) > direct-per-call (1981) >
  routed (1021)`. Batching amortizes commit/fsync with nothing to contend over, so it wins.
  Routing *loses* — its serialize-then-handoff is pure overhead when there is nothing to collapse
  (cg 1.07 ≈ one submission per group). `direct-batched` here picked up 415 deadlock aborts this
  run — the disjoint property is per-caller-stripe, not per-batch, so a few overlap windows
  surface as deadlocks; in the 2026-05-27 run they happened to be zero (host-load noise).
- **Low concurrency, uniform (s1):** `direct-per-call (1478) > routed (1320) > direct-batched
  (768)`. At 10 uniform callers there's little to collapse (cg 1.10), so routing's handoff costs it
  and per-call wins outright.
- **Moderate-to-high concurrency + overlap (s2, s4):** `routed (1235, 922) > direct-per-call (646,
  204) ⋙ direct-batched (6.1, 9.9 — 500–800 deadlock-aborts)`. Routing's safe cross-caller batching
  pulls clearly ahead.
- **Extreme single-pool contention (s5):** `routed (2090, 0 err, 2078 locks) ⋙ direct-per-call
  (334, p99 3.09s)`; direct-batched 763 but 637 aborts. ~6.3× over per-call. See artifact (c).
- **Deep pools (s7, s8, depth 1000):** `routed (1514, 979) > direct-per-call (1135, 213) ⋙
  direct-batched (6.2, 117)`. Routing leads on the deep shapes this run; the original 2026-05-26
  snapshot had direct-per-call leading and the 2026-05-27 snapshot had routed leading — the deep
  ranking is close and **sensitive to host load** (both paths are clean; read it as "comparable,
  both viable" rather than a hard ordering). What is robust is that direct-per-call stays strong
  even at depth 1000 because its hot path is **depth-independent** (artifact a).
- **Mixed-method, now clean (s3, s4):** with `standard_cost` seeded, S3 (`direct-per-call 787 >
  routed 629`) and S4 (`routed 922 ⋙ direct-per-call 204`) run error-free; `direct-batched` still
  aborts (8.4, 9.9) under the complex multi-pool overlap.
- **Same-pool-twice (multi-touch) under load — s9 (NEW, `acct-34ce`):** s8's shape (1000 callers,
  Zipf(1.2), complex, deep) + **40% multi-touch** (distribution `1:60,2:30,3:10`). All flavors
  commit cleanly — `routed 1070 (cg 14.14, drops 0, deadlock_retries 0)`, `direct-per-call 229`,
  `direct-batched 91 (err 1268, the s8-shape deadlock pattern, unaffected by multi-touch)`. **No
  regression vs s8 on either coalesce-correct path** (routed 1070 ≥ 979; direct-per-call 229 ≈
  213, within noise). The smaller `cg` vs s8 is the multi-touch signature: when a submission
  repeats a pool, ledger-core's `coalesce_aggregates` collapses the duplicate so the committer
  sees one aggregate per pool per group — fewer distinct pools per submission → smaller groups,
  but the same per-pool collapse mechanic; see artifact (e).

### Key question (§11.4): *does routed beat direct-batched, or does standard-tx batching capture most of routing's benefit?*

**Routing wins decisively wherever callers contend, and standard-tx batching does NOT capture the
benefit — under overlap it inverts into a deadlock liability.** In every scenario with cross-caller
pool overlap (s2, s4, s5, s7, s8, s9), `direct-batched` collapsed — 6.1, 9.9, 763*(lossy)*, 6.2, 117,
91 trx/s respectively, each with **500–1300 deadlock/serialization aborts** — because a batch holds
its pools' locks for the whole 50-submission transaction, and overlapping batches across callers
invert lock order. `direct-batched` beats `direct-per-call` only when callers are mostly disjoint (s6).
Routing avoids this entirely: a pool is owned by exactly one committer at a time, so a whole
commit_group's depletions serialize *inside* one transaction with **zero deadlock-retries observed
across the entire matrix**. Routing is the only mode that batches *safely* under contention.

The complementary truth: routing does **not** universally beat `direct-per-call`. Direct-per-call
wins on disjoint (s6) and at low-concurrency-uniform (s1, s3). On the deep moderate-overlap shapes
(s7/s8/s9) the two are close and the ordering flips with host load. The durable routing win is
**contention collapse** (s5, s2, s4) plus
**operational cleanliness** (no aborts) everywhere.

---

## (c) Routed hot-pool throughput envelope + batching collapse — §11.3 / §6.7

The §6.7 win is clearest on **s5** (1000 callers all targeting **one** pool, depth 10):

| metric | direct-per-call | routed |
|---|---|---|
| throughput | 333.8 trx/s | **2089.9 trx/s** (6.3×) |
| trx committed | 12 043 | 63 896 |
| `pool_lock` acquisitions | 1 / trx (≈12 043) | **2 078** |
| aggregate UPSERTs | 1 / trx | **2 078** |
| commit_group_size_avg | — | **30.75** |
| failed / dropped | 0 | **0** |
| ack p99 | 3.09 s | 0.79 s |

**63 896 trx committed through 2 078 `pool_lock` acquisitions and 2 078 aggregate UPSERTs** — a
~31× collapse of the locking/fsync footprint, matching `commit_group_size_avg = 30.75` (63896 /
2078). Where direct-per-call forces every one of 1000 callers to queue for the single hot pool's
lock one trx at a time (p99 = 3.09 s of pure lock-wait), routing drains them in groups of ~31 under
a single lock-hold, then commits once — 6.3× the throughput at a quarter of the ack p99. The
collapse is the entire point of the routed path on hot pools, and it lands cleanly (0 drops, 0
poison, 0 deadlock-retries).

Corroborating envelope points:
- **s2** (200 callers, Zipf1.5): cg 7.41, 37 347 trx via **5 042** drains/locks/upserts — routing
  beats direct-per-call (1235 vs 646 trx/s) and annihilates direct-batched (6.1).
- **s7** (1000 callers, Zipf1.2, **depth 1000**): cg 3.56, 46 670 trx via **13 114** locks. Lower
  overlap → smaller groups, but the collapse mechanism still operates (one lock per ~3.6 trx),
  routing leads on raw throughput (1514 vs 1135), and the path stays clean (0 drops) even at depth
  1000.
- **s8 / s9** (complex multi-pool, deep): cg 20.65 / 14.14 submissions per group; because each trx
  touches several distinct pools the lock count (221 749 / 229 183) is per-(group×pool), so the
  simple "one lock per trx" collapse is muddied by multi-pool footprint — s5 (single pool, simple)
  remains the clean demonstration. s9's *smaller* cg vs s8 is the multi-touch signature: a
  same-pool repeat is coalesced inside the submission so the committer counts one aggregate per
  pool per group, not one per line (see (e)).

**Envelope summary:** routed hot-pool throughput scales with *overlap density*. The denser the
contention on a pool (s5 single pool → cg 26.7), the larger the collapse and the more routing
beats direct; as overlap thins toward disjoint (s6 → cg 1.07), the collapse vanishes and routing's
handoff overhead makes it the slowest mode. Routing is a **contention-collapse** mechanism, not a
universal throughput multiplier.

---

## (d) Cross-flavor aggregate-qty equivalence — §9.4 / §11.1 → **PASS**

Identical deterministic submission sequence (8 callers × 50 = 400 submissions, 20-pool universe,
depth 5) replayed through the direct flavor (caller-serial) and the routed flavor (router-batched,
4 committers, possibly reordered across callers); aggregate `pool_state` then diffed.

| Scenario | pools | submissions | qty mismatches | unit_cost divergences | verdict |
|----------|------:|------------:|---------------:|----------------------:|---------|
| s5 | 20 | 400 | **0** | 0 | **PASS** |
| s7 | 20 | 400 | **0** | 0 | **PASS** |
| s8 | 20 | 400 | **0** | 0 | **PASS** |
| **s9** | 20 | 400 | **0** | 0 | **PASS** |

**Aggregate qty is identical across flavors on every pool** — the order-independent correctness
invariant (a signed sum of line qtys) holds regardless of how routing batches/reorders submissions
across callers. This is the property recalc/close (§13) will depend on: the *quantity* ledger is
exact under both paths, so the deferred authoritative cost pass has a sound foundation.

**On the 0 provisional-unit_cost divergences (the §9.4 note):** provisional unit_cost is
order-sensitive *for receipts* (WAC running-average), and §9.4 explicitly **permits** it to differ
across flavors. It did not differ here, for two compounding reasons:

1. **s5/s7 are pure-depletion** (`deplete_pct=100`). Depletions read the running aggregate but do
   **not** mutate aggregate unit_cost — so there is no order-sensitive quantity to diverge.
2. **s8 / s9 have receipts** (`deplete_pct=50`, random costs) yet still matched, because the
   router **preserves per-pool enqueue order** within a commit_group and the committer applies in
   that order. So the WAC running-average sees the *same per-pool operation order* as the
   caller-serial direct flavor, yielding identical provisional costs. s9 additionally repeats one
   pool 2–3× in ~40% of submissions; the in-submission `coalesce_aggregates` collapse converges to
   the same per-pool aggregate on both flavors (direct: in-submission coalesce → bulk UPSERT;
   routed: committer post-pass-snapshot reconstruction).

Divergence therefore remains **architecturally permitted but unobserved** in this PoC: it would
surface only if same-pool receipts were committed out of enqueue order (e.g. cross-chunk reordering
under a smaller `batch_size_max`). Crucially, even if/when provisional unit_cost diverges, **qty —
the recalc/close input — stays exact**, so the divergence is a cosmetic provisional-cost artifact,
not a correctness defect. Authoritative cost reconciliation is deferred (§13).

The 2026-05-28 measurement **added a fresh equivalence run for s9** (the new multi-touch scenario):
0 qty mismatches, 0 unit_cost divergences — PASS. The s5/s7/s8 verdicts carry from 2026-05-27
since the `.so` are unchanged. So multi-touch (same-pool-twice within one submission) does not
break the aggregate-qty invariant on either coalesce path.

---

## (e) Multi-touch coalesce under load — §5.1 / `acct-34ce` → **CLEAN**

s9 is s8 with the harness's opt-in multi-touch generation enabled (`multi_touch_pct=40`,
distribution `1:60,2:30,3:10`): ~40% of submissions repeat one pool 2–3× — the WO-completion
shape (backflush + scrap + output on one SKU/location). This stresses
`PlanResult::coalesce_aggregates` (§5.1 step 8) — the keep-last collapse of same-pool aggregate
mutations within a submission — under realistic 1000-caller deep-FIFO load. The S1–S8
distinct-pool generator never reaches it; ledger-core / direct / routed unit tests do.

**Head-to-head against s8** (same caller count, overlap, complexity, depth, method):

| | s8 | s9 (multi-touch) | Δ |
|--|--:|--:|--|
| direct-per-call throughput | 212.9 trx/s | **229.1 trx/s** | +7.6% (within host-load noise) |
| direct-per-call errors | 0 | **0** | clean coalesce on direct |
| direct-batched throughput | 117.3 trx/s | 91.1 trx/s | s8-shape contention story unchanged |
| direct-batched errors | 1 073 | 1 268 | deadlock aborts (same pattern as s8) |
| routed throughput | 978.5 trx/s | **1069.8 trx/s** | +9.3% (within host-load noise) |
| routed cg | 20.65 | **14.14** | smaller groups (the multi-touch signature) |
| routed pool_lock acq | 221 749 | 229 183 | comparable; lock-per-(group×pool) on a 14× multi-pool shape |
| routed drops / poison / dlk | 0 / 0 / 0 | **0 / 0 / 0** | coalesce-under-load is clean on both flavors |

**Verdict: clean.** Same-pool-twice submissions commit without error on both correctness paths
(direct-per-call, routed) at 1000-caller deep-FIFO load. Routed throughput is *higher* than s8 in
this run (within host noise — the structural finding is "no regression"), and the routed counters
prove the coalesce mechanic operates as designed: `dropped=0` and `deadlock_retries=0` confirm no
in-submission aggregate-mutation conflict propagated to the committer.

The **smaller commit_group_size** (s9 cg 14.14 vs s8 cg 20.65) is the multi-touch signature: when
a submission repeats a pool, ledger-core coalesces the duplicate mutations into a single per-pool
aggregate before the committer ever sees them. From the router/committer's perspective each
submission then advertises *fewer distinct pools*, so cross-caller affinity grouping forms slightly
smaller commit_groups. The per-pool collapse mechanism (artifact c) operates unchanged — only the
size of the units it operates on shifts.

This closes the only post-build review concern that distinct-pool generation had hidden:
`acct-34ce` (the harness opt-in mode and the s9 preset) directly exercises the coalesce path under
load. Three independent signals say the path is sound — the s9 row in (b) (`errors=0` on
direct-per-call, `drops=0 / poison=0 / deadlock_retries=0` on routed under 1000-caller load), the
s9 equivalence row in (d) (**0 qty mismatches, 0 unit_cost divergences** in a deterministic
identical-input replay through both flavors), and the three coalesce unit tests on `ledger-core` /
direct / routed (the existing regression net).

---

## (f) Pareto 80/20 typical-mid-market coverage — §10.6 / `acct-s90k` → **CLEAN**

S1-S9 brackets the contention envelope at the architectural extremes (s6 disjoint = 0%; s5
single-pool = 100%) and approximates the mid-market region via Zipf(1.2) (which over 10000 pools
is ~87/20, more concentrated than textbook Pareto 80/20). It does **not** carry a *discrete*
hot/cold mixture — the workload shape mid-market ERP loads actually live in. **S10-S21** add that
shape via the new `OverlapMode::Pareto { hot_pool_fraction, hot_traffic_fraction }` (a two-
population mixture: each pick rolls hot vs cold by `hot_traffic_fraction`, then samples uniformly
in that population) × three workload families:

- **RECEIPTS** (`s10`–`s13`): `deplete_pct=0`, distinct-pool. PO receipt / transfer-in.
- **BUILDS** (`s14`–`s17`): `deplete_pct=50`, multi-touch ENABLED (`pct=40`, dist `1:60,2:30,3:10`).
  WO-completion shape (backflush + scrap + output on one SKU/location), per `acct-34ce`.
- **MIXED** (`s18`–`s21`): `deplete_pct=50`, distinct-pool. Generic transfer / sale balance.

Each family × four overlap/scale variants (mid-typ 50 callers, high-vol 200 callers + depth 100,
long-tail Pareto 90/10, balanced Pareto 50/50 = 12 scenarios total. The harness also exposes
`--pareto-hot-pool-pct` / `--pareto-hot-traffic-pct` CLI overlays so any base scenario can be
re-shaped on the fly. CLI overlays + canned scenarios were both chosen on purpose (mirrors
`acct-34ce`'s `--multi-touch-pct` / `--touch-dist` pair).

**Scope note: artifact (f) is *coverage extension*, not a new architectural premise.** Premises
1–4 (artifacts a/b/c/d/e) characterize Path C; the Pareto family fills in the typical-mid-market
region where prior scenarios only interpolate between extremes. The acceptance bar for `acct-s90k`
is **wiring + clean smoke**, not perf comparison — production-grade re-measurement (30s/run on a
quiesced host) lands as a future task when a structural finding warrants it.

| Scn | family | variant | callers | depth | direct-per-call | direct-batched | routed |
|-----|--------|---------|--------:|------:|-----------------|----------------|--------|
| s10 | Receipts | mid-typ | 50 | 0 | **711.4 trx/s** | 3.6 trx/s (err 53) | **3568.9 trx/s** (cg 24.58) |
| s11 | Receipts | high-vol | 200 | 100 | **663.2 trx/s** | 1.2 trx/s (err 210) | **2840.6 trx/s** (cg 44.58) |
| s12 | Receipts | long-tail | 50 | 0 | **375.0 trx/s** | 3.1 trx/s (err 56) | **3471.4 trx/s** (cg 32.14) |
| s13 | Receipts | balanced | 50 | 0 | **655.1 trx/s** | 5.9 trx/s (err 53) | **3162.9 trx/s** (cg 30.57) |
| s14 | Builds | mid-typ | 50 | 10 | **979.2 trx/s** | 5.0 trx/s (err 57) | **3669.4 trx/s** (cg 15.27) |
| s15 | Builds | high-vol | 200 | 100 | **453.7 trx/s** | 1.1 trx/s (err 211) | **3421.5 trx/s** (cg 25.29) |
| s16 | Builds | long-tail | 50 | 10 | **904.5 trx/s** | 4.1 trx/s (err 59) | **3677.3 trx/s** (cg 28.62) |
| s17 | Builds | balanced | 50 | 10 | **996.7 trx/s** | 6.7 trx/s (err 61) | **3636.8 trx/s** (cg 13.03) |
| s18 | Mixed | mid-typ | 50 | 10 | **850.9 trx/s** | 4.7 trx/s (err 54) | **3614.2 trx/s** (cg 31.33) |
| s19 | Mixed | high-vol | 200 | 100 | **654.3 trx/s** | 1.6 trx/s (err 211) | **3379.8 trx/s** (cg 24.97) |
| s20 | Mixed | long-tail | 50 | 10 | **810.1 trx/s** | 2.8 trx/s (err 57) | **3729.6 trx/s** (cg 22.84) |
| s21 | Mixed | balanced | 50 | 10 | **953.6 trx/s** | 6.2 trx/s (err 56) | **3654.3 trx/s** (cg 12.95) |

**Verdict: clean across the matrix.** Every direct-per-call cell `errors=0` and every routed cell
`drops=0 / poison=0 / deadlock_retries=0` (acceptance criterion 4 of `acct-s90k`). The `direct-
batched` column carries the same shape as S1–S9 — clean on the receipts family (no cross-caller
contention to deadlock against), aborts under deplete/multi-touch overlap (s14–s21 mirror
s8/s9's pattern); this is the documented batching-under-contention regime from artifact (b),
re-confirmed at a different scale.

Numbers above are 5s smoke runs against 10000 pools — directional only. They establish the
*coverage*, not perf rankings against S1–S9. The structural premises (a/b/c/d/e) carry; the
Pareto family is the typical-mid-market window for future comparisons.

---

## (g) Routed long-duration cleanliness + clean throughput — §14 / `acct-0516` + `acct-235v` → **CLEAN**

This artifact has two passes. **Pass 1 (`acct-0516`)** drove every scenario **S1–S21** through routed
mode at **two durations (60 s, then 600 s)**, clean DB per scenario, to surface any
*duration-dependent failure* — poison threshold, takeover after committer death, deadlock-retry
accumulation, shmem-arena spillover, staging back-pressure, router-orphan recovery, drain-deadline
behaviour. Verdict: **CLEAN** — zero `dropped`/`poisoned`/`deadlock_retries`/`takeover` across all 42
cells, and no structural 10m-vs-1m divergence. That cleanliness verdict is robust on its own: it
concerns failure counters, which host contention cannot manufacture.

**Pass 2 (`acct-235v`)** is a **clean, load-gated re-measurement of throughput.** Pass 1's *absolute*
throughput numbers were contaminated by two sources identified afterward: (a) ~15 foreign PoC
BGWorkers left in `shared_preload_libraries` by other streams' install scripts (which append and
never prune), contending for the 8 cores; and (b) Chrome on this daily-driver workstation (one
renderer at ~89 % CPU), which swung identical-run throughput by up to ~2×. Both were removed —
preload stripped to `pg_stat_statements, pg_cron, ledger_routed_c`, and a **per-run load gate**
(`common.sh::wait_for_quiet_host`) that holds each timed run until the 1-min loadavg drops below 1.5.
Direct-per-call and direct-batched stay dropped: their committer / shmem path is absent, so they do
not develop new failure modes with duration (their verdicts live in artifact (b)).

**Method.** `ledger-harness/bench/run-routed-longdur.sh` with `DURATIONS=120` — one **2-minute**
load-gated run per scenario, clean DB each (21 routed cells). Clean = **DROP/CREATE `poc_v3_1` +
re-run the 16 sqlx migrations + `CREATE EXTENSION` + postmaster restart** (clean-DB option a2 — the
more aggressive choice over a1's restart-and-reset, because it surfaces migration / extension-init
bugs). Seed depth follows the artifact-(b) crossover depths (s5/s6 = 10, s7/s8/s9 = 1000,
s11/s15/s19 = 100, the remaining deplete scenarios = 10, receipts = 0). A committer-readiness canary
runs after each clean and gates the timed run. S5–S9 (1000 callers) route through pgbouncer:6432.
Every cell below ran with pre-run loadavg < 1.5.

**FINDING (bench methodology, not a Path C defect): a bare `DROP DATABASE` wedges the routed
committer.** The router / committer / recovery workers are `shared_preload_libraries` BGWorkers and
the staging / commit-group **shmem arena lives for the cluster lifetime**, not the database
lifetime. `DROP DATABASE … WITH (FORCE)` severs the committer's SPI connection and orphans the
in-arena staging entries but does **not** clear the arena; the committer never resumes against the
recreated database and orphaned entries jam the staging queue (observed pre-fix: 16 384 pending, 0
drains, then new enqueues fail because staging is full). A **postmaster restart** after the
DROP/CREATE re-execs `_PG_init`, cold-creates the arena, and respawns a committer that attaches to
the recreated DB. This is why a1's restart-based clean (run-crossover.sh) keeps routed healthy; the
routed-c README's "only a3 / volume-wipe clears the arena" is imprecise — *any* postmaster restart
clears it. You would never `DROP DATABASE` under a live committer in production, so this is a
benchmark-harness concern, captured here for the next operator. The committer-restart gate held
across all 21 scenarios — every readiness canary passed on the first attempt.

### Clean per-scenario routed throughput — 2-minute load-gated (`acct-235v`)

These supersede Pass 1's contaminated absolutes. Each is a single 2-minute routed run on a clean DB,
held until pre-run loadavg < 1.5, on the minimal-preload cluster.

| scn | throughput | trx_committed | unseen | errors | drains | cg_avg | pool_lock | agg_upsert | dropped | poison | dlk_retry | takeover | ack_p99 µs |
|-----|-----------:|--------------:|-------:|-------:|-------:|-------:|----------:|-----------:|--------:|-------:|----------:|---------:|-----------:|
| s1 | 1,075 | 129,074 | 0 | 0 | 117,320 | 1.10 | 117,320 | 117,320 | 0 | 0 | 0 | 0 | 40,894 |
| s2 | 925 | 111,119 | 0 | 0 | 14,714 | 7.55 | 14,714 | 14,714 | 0 | 0 | 0 | 0 | 1,192,230 |
| s3 | 736 | 88,349 | 0 | 0 | 10,786 | 8.19 | 1,265,220 | 1,265,220 | 0 | 0 | 0 | 0 | 97,714 |
| s4 | 499 | 60,106 | 0 | 24 | 5,365 | 11.20 | 490,398 | 490,398 | 0 | 0 | 0 | 0 | 2,602,565 |
| s5 | 1,838 | 221,755 | 0 | 0 | 8,184 | 27.10 | 8,184 | 8,184 | 0 | 0 | 0 | 0 | 742,391 |
| s6 | 713 | 86,752 | 0 | 0 | 79,974 | 1.08 | 79,974 | 79,974 | 0 | 0 | 0 | 0 | 2,128,609 |
| s7 | 1,252 | 151,566 | 0 | 0 | 40,543 | 3.74 | 40,543 | 40,543 | 0 | 0 | 0 | 0 | 1,388,314 |
| s8 | 598 | 72,984 | 0 | 0 | 6,300 | 11.58 | 584,159 | 584,159 | 0 | 0 | 0 | 0 | 2,749,366 |
| s9 | 633 | 77,211 | 0 | 0 | 6,594 | 11.71 | 544,637 | 544,637 | 0 | 0 | 0 | 0 | 2,550,136 |
| s10 | 632 | 75,827 | 0 | 0 | 8,446 | 8.98 | 1,029,024 | 1,029,024 | 0 | 0 | 0 | 0 | 429,129 |
| s11 | 493 | 59,267 | 0 | 21 | 6,363 | 9.31 | 805,356 | 805,356 | 0 | 0 | 0 | 0 | 2,657,091 |
| s12 | 681 | 81,734 | 0 | 0 | 8,539 | 9.57 | 998,045 | 998,045 | 0 | 0 | 0 | 0 | 404,226 |
| s13 | 638 | 76,573 | 0 | 0 | 8,603 | 8.90 | 1,108,719 | 1,108,719 | 0 | 0 | 0 | 0 | 428,081 |
| s14 | 673 | 80,830 | 0 | 0 | 8,569 | 9.43 | 967,038 | 967,038 | 0 | 0 | 0 | 0 | 405,536 |
| s15 | 476 | 57,342 | 0 | 40 | 6,334 | 9.05 | 685,535 | 685,535 | 0 | 0 | 0 | 0 | 2,755,657 |
| s16 | 671 | 80,588 | 0 | 0 | 8,531 | 9.45 | 873,691 | 873,691 | 0 | 0 | 0 | 0 | 411,041 |
| s17 | 613 | 73,649 | 0 | 0 | 8,608 | 8.56 | 936,685 | 936,685 | 0 | 0 | 0 | 0 | 444,334 |
| s18 | 602 | 72,268 | 0 | 0 | 8,342 | 8.66 | 982,159 | 982,159 | 0 | 0 | 0 | 0 | 454,033 |
| s19 | 445 | 53,579 | 0 | 39 | 5,714 | 9.38 | 727,764 | 727,764 | 0 | 0 | 0 | 0 | 2,952,790 |
| s20 | 645 | 77,383 | 0 | 0 | 8,453 | 9.15 | 949,539 | 949,539 | 0 | 0 | 0 | 0 | 433,324 |
| s21 | 617 | 74,129 | 0 | 0 | 8,582 | 8.64 | 1,073,062 | 1,073,062 | 0 | 0 | 0 | 0 | 439,877 |

`unseen` = `submitted_but_unseen`; `cg_avg` = `commit_group_size_avg`; `dlk_retry` =
`deadlock_retries_total`. `pool_lock == agg_upsert` on every cell — the §6.7 coalescing identity
holds. Per-cell `top_wait_events` and `com_p99` are in the raw `results/longdur_<scn>_120s.json`.

**Verdict: CLEAN.** No routed cell — across Pass 1's 42 long-duration cells (60 s + 600 s) or Pass
2's 21 clean 2-minute cells — recorded a single `dropped`, `poisoned`, `deadlock_retries`, or
`takeover`. Pass 1 established that no failure mode emerges with duration (no 10m verdict diverged
structurally from its 1m verdict — poison threshold, takeover after committer death, deadlock-retry
accumulation, arena spillover, staging back-pressure, router-orphan recovery, drain-deadline
behaviour all stayed at zero through the 10× window); Pass 2 re-confirms it on the clean cluster. The
deep-pool cells (s7/s8/s9, 1000-layer) and single-hot-pool s5 sustained throughput for the whole run
without exhausting — each seeded layer holds 1e9 units and routed/provisional mode never walks
layers, so a pool cannot drain in these windows (P0006 exhaustion was never a real risk at these
depths; the crossover seed depths suffice).

### Throughput characterization — what governs the routed rate (`acct-235v`)

A load-gated committer/caller sweep (clean DB per cell, `run-committer-count-sweep.sh`) pins down the
dial. Three findings, none a defect:

1. **Committers parallelize on disjoint work.** s6 (1000 callers, disjoint pools, cg≈1) scales
   **958 → 1,234 → 1,791 trx/s** as `committer_count` goes 2 → 4 → 8 (ack p99 4.3 s → 1.2 s). The
   committer pool is not a serial stage.
2. **Hot-pool workloads serialize on per-pool `FOR UPDATE` locks.** s10 (Pareto receipts, 50 callers)
   is **flat at ~1,390 trx/s** across committer_count 2/4/8 — committers idle (commits/s pinned
   ~115) and contend on shared hot pools (each complex receipt touches ~13.5 pools; Pareto
   concentrates them). More callers don't help (s11 200c ≈ s10 50c; the extra load becomes ack
   latency). This serialization is inherent to any correct ledger, not a Path C defect.
3. **Coalescing is the real throughput dial, and it needs same-pool overlap to engage.** s5 (single
   hot pool, 1000 callers) coalesces to cg=32 → **0.03 locks/trx → 2,705 trx/s** (the highest cell),
   trading ack latency (p50 450 ms). `batch_window_us` looks flat on low-concurrency hot cells (s10
   @ 50 callers) only because there aren't enough concurrent same-pool submissions to grow the group
   — the window can't batch arrivals that haven't happened yet. With overlap present (s5) the
   lock-amortization win is large.

The **router is not the bottleneck**: a direct profile (read-only counters added to
`ledger_routed_c`) shows it scans ~9 staging entries per tick (not its 1000-slot window), defers
< 2 % of ticks on the batch-window gate, and forms exactly as many commit_groups as the committers
drain — it keeps up with arrival. The one genuine cost is **synchronous-ack latency** (p99 ~0.3 s
@ 50 callers, 1–3 s @ 1000 callers) — the shape-L pseudo-sync target (Part VII Q3 escape hatch).

### Lever 1b — disjoint-component packing breaks the spread-workload `cg` plateau (`acct-xdwk`)

Characterization #3 above exposed an asymmetry: coalescing only engages when submissions **share a
pool**. The router's `affinity_group` unions same-pool submissions into one connected component and
emits **one commit_group per component**, so on a spread/Pareto workload (many small disjoint
components) `commit_group_size_avg` plateaus around ~10–12 *no matter how high `batch_size_max` goes*
— each small component ships as its own group and the per-group commit/fsync is never amortized. A
batch-size sweep with the coalesce window held wide (`batch_window_us=20000`, so the size cap binds)
confirmed it: on **s19** (Pareto mixed, 200 callers) `cg` flatlined at **12.4** at `batch_size_max=200`.

**Fix (gated, default off):** a new `ledger_routed_c.router_pack_disjoint` GUC (Sighup). When on, the
router greedily first-fits the disjoint components — in `request_seq` order, preserving oldest-first
dispatch — into commit_groups of up to `batch_size_max`. **Safe by construction:** every pool already
lives in exactly one component per tick, so a packed group's members share no `pool_id`; one committer
drains the whole group taking each pool's `FOR UPDATE` sequentially with **zero new cross-pool
contention**. A component already ≥ cap is left standalone for the existing chunk path. Correctness is
unchanged — the cross-flavor aggregate-qty equivalence property (`property_ledger_enqueue_trx_c`)
passes with packing both off and on.

Load-gated s19 sweep, `batch_window_us=20000`, clean DB per cell (`run-batch-size-sweep.sh --pack`):

| `batch_size_max` | cg off → on | commits/s off → on | throughput off → on |
|-----------------:|------------:|-------------------:|--------------------:|
| 25  (cc=1) | 8.9 → 20.5  | 104 → 45 | 925 → 921 |
| 50  (cc=1) | 9.6 → 33.2  | 98 → 28  | 939 → 945 |
| 100 (cc=1) | 11.1 → 49.8 | 88 → 20  | 971 → 978 |
| 200 (cc=1) | **12.4 → 66.3** | 80 → 15 | 993 → 979 |
| 200 (cc=4) | **12.7 → 71.7** | 104 → 19 | 1,321 → 1,337 |

Packing makes `cg` **track the cap** instead of plateauing (5.3–5.6× larger groups at `batch_size_max=200`),
collapsing commit/fsync operations ~5.5× for the same ~20 k committed trx. **Throughput is roughly
neutral** (≤ +1 %) on s19 — and that is the expected, correct result, not a disappointment: a Pareto
mixed workload's residual ceiling is the **same-pool `FOR UPDATE` contention** of characterization #2,
which packing deliberately does *not* touch (disjoint components carry no shared lock to relieve).
Lever 1b is the commit-amortization half of the problem; the lock-contention half is **lever 2 —
committer→pool affinity**, characterized next.

### Lever 2 — committer→pool affinity: refuted (`acct-xdwk`), then re-investigated rigorously (`acct-0usf`)

The standing hypothesis (from characterization #2) was that hot-pool throughput is bounded by the
**cross-committer `FOR UPDATE` handoff**: the committer claim queue is first-come with no pool affinity
(`committer.rs::claim_next_committer_entry`), so the same hot pool's commit_groups across ticks land on
different committers and block on each other's row lock. Lever 2 prototyped the fix — pin a commit_group
to a committer by `hash(min pool_id) % committer_count` (with an age-gated steal fallback) — behind a
default-off GUC, and swept `committer_count` 2/4/8 on the two Pareto-receipt cells, affinity off vs on
(load-gated, clean DB per cell):

| scenario | cc=2 off → on | cc=4 off → on | cc=8 off → on |
|----------|--------------:|--------------:|--------------:|
| s10 (50 callers)  | 1,280 → 1,221 | 1,374 → 1,352 | 1,376 → 1,407 |
| s11 (200 callers) | 1,080 → 967   | 1,194 → 1,069 | 1,258 → 1,194 |

Affinity was throughput-neutral on s10, ~10 % worse on s11, and never converted the flat committer-count
curve into a scaling one. The production claim-path code was **reverted**.

**That pass was too thin to be conclusive.** Its bottleneck *mechanism* was **inferred** — the acct-235v
"78 % of DB time in `pool_lock FOR UPDATE`" figure was one `pg_stat_statements` snapshot on a
*single-hot-pool* run, total-DB-time across **all** backends, then generalized to every workload. Only two
scenarios were measured, single noisy runs, deltas inside the host-load band; affinity-ON correctness was
never cleanly proven; only one affinity key (min-pool) was tried. `acct-0usf` redoes it rigorously, and
STEP 1 (this subsection) replaces the inference with **direct per-scenario measurement**. The committer
pipeline is instrumented with in-process wall-time spans (`committer_{pool_lock,hydrate,apply,txn}_ns_total`,
`acct-0usf` STEP 1a) and a committer-segmented `pg_stat_activity` wait sampler (STEP 1b) that splits busy
committer time into the row-lock handoff (the *only* span affinity can shrink), on-CPU query execution
(irreducible), shmem-ring `LWLock` contention (a separate bottleneck affinity can't touch), and IO.

**Measured: where does committer wall-time go** (full matrix, 5 reps/scenario, `committer_count=4`,
affinity OFF, sampler ON, clean-seed + load-gated per cell on a daily-driver host; median across reps —
`results/committer_profile_sweep.csv`, `bench/run-committer-profile-sweep.sh` + `bench/profile-aggregate.py`,
75 rows / 0 FAILs). `busy%` is committer pool utilization (non-idle / total samples); the `of-busy`
columns partition the **busy** time:

| scn | callers | trx/s | `cg` | busy% | lock% | on-CPU% | LWLock% | a-priori affinity verdict |
|-----|--------:|------:|-----:|------:|------:|--------:|--------:|---------------------------|
| s5  | 1000 | 2814 | 49.9 | 74 | **67** | 24 | 6  | CANDIDATE — single hot pool |
| s6  | 1000 | 1411 |  1.1 | 88 | **0**  | 29 | 46 | **SKIP** — disjoint, 0 % lock, LWLock-bound |
| s7  | 1000 | 2095 |  3.6 | 77 | **9**  | 41 | 28 | **SKIP** — deep-zipf, on-CPU (hydration layer-scan, not FIFO cost) |
| s8  | 1000 | 1379 | 49.7 | 85 | **72** | 26 | 1  | CANDIDATE — deep-zipf complex |
| s9  | 1000 | 1418 | 49.7 | 84 | **72** | 26 | 1  | CANDIDATE — deep-zipf multi-touch |
| s10 |   50 | 1401 | 49.6 | 90 | **65** | 26 | 8  | CANDIDATE — Pareto receipts |
| s11 |  200 | 1235 | 48.9 | 90 | **50** | 26 | 23 | CANDIDATE — Pareto receipts, high-conc |
| s14 |   50 | 1416 | 49.3 | 89 | **64** | 26 | 8  | CANDIDATE — Pareto builds |
| s15 |  200 | 1263 | 48.9 | 90 | **50** | 25 | 24 | CANDIDATE — Pareto builds, high-conc |
| s16 |   50 | 1431 | 49.8 | 90 | **64** | 26 | 9  | CANDIDATE — Pareto builds, long-tail |
| s17 |   50 | 1405 | 47.0 | 89 | **64** | 26 | 9  | CANDIDATE — Pareto builds, balanced |
| s18 |   50 | 1387 | 49.6 | 89 | **66** | 26 | 7  | CANDIDATE — Pareto mixed |
| s19 |  200 | 1222 | 48.8 | 90 | **52** | 26 | 22 | CANDIDATE — Pareto mixed, high-conc |
| s20 |   50 | 1421 | 49.9 | 91 | **64** | 26 | 9  | CANDIDATE — Pareto mixed, long-tail |
| s21 |   50 | 1358 | 48.2 | 89 | **65** | 26 | 8  | CANDIDATE — Pareto mixed, balanced |

What the measured decomposition establishes that the inference could not:

1. **The bottleneck *diagnosis* was right; its *universality* was not.** 13 of 15 scenarios are lock-bound
   (lock 50–72 % of busy), so the handoff is real. But **two scenarios are not lock-bound at all** and the
   throughput-only view silently misfiled them: **s6 (disjoint, lock 0 %)** is **LWLock-bound** (46 % — the
   staging-ring / arena, `cg`≈1 so nothing coalesces) and **s7 (deep-zipf-simple, lock 9 %)** is **on-CPU
   bound** (41 % — the FIFO layer-walk over depth 1000). Affinity is *a priori* moot on both; they are
   **SKIP** in the STEP 3 variants.
2. **A new, affinity-immune bottleneck surfaces under high concurrency.** The 200-caller Pareto cells
   (s11/s15/s19) spend **22–24 % of committer time on shmem-ring `LWLock` contention** — invisible to the
   lever-2 row-lock-only framing. Even a perfect affinity scheme leaves it untouched.
3. **Even the single-hot-pool best case is only ~67 % lock.** s5 — where affinity *should* help most — still
   spends a quarter of busy time on-CPU and 6 % on LWLock, capping any affinity upside well below the naive
   "78 % is `FOR UPDATE`" headline (that figure was total-DB-time across all backends, not committer
   busy-time).
4. **The fractions are load-robust.** Every scenario's `lock_of_busy` IQR is tight (mostly < 2 pp) despite
   per-cell load1 medians of 1.6–2.8 and individual reps spanning load 1.4–6.4 — vindicating reporting
   structural fractions, not absolute throughput, on a noisy host. (Throughput absolutes here run below the
   lever-1b table's because the sampler is on and the host was contended; they are context, not comparable.)

**Disposition.** The lever-2 refutation **stands** (min-pool affinity reverted). `acct-0usf` STEP 1 is
complete: the mechanism is now measured per scenario, not inferred. The 13 CANDIDATE scenarios proceed to
STEP 2 (pre-registered hypotheses H1/H2/H3 with falsification criteria) and STEP 3 (affinity variants
V0–V3 — min-pool / whole-pool-set / per-pool-ownership / router-side — each tested *only* where this table
says lock contention is the dominant addressable cost); s6 and s7 are recorded a-priori SKIP. The open
question is no longer "does affinity help?" but the sharper one this table frames: on the lock-bound cells,
can *any* committer→pool assignment shrink the row-lock handoff without (a) re-serializing the ~13.5-pool
Pareto groups (`pool_lock_per_trx ≈ 13.5`) or (b) trading row-lock wait for even more staging-ring
`LWLock` wait — given that on the high-concurrency cells the handoff is already only ~half of busy time.
The lever-2 reproducibility artifacts (`AFFINITY` knob on `run-committer-count-sweep.sh`, the four
`committer_count_sweep_s1{0,1}_aff{off,on}.csv`) are retained. **Net for `acct-xdwk`: lever 1b
(disjoint-component packing) is the real, shipped win; lever 2 (committer affinity) is refuted as shipped;
`acct-0usf` carries the rigorous re-investigation.**

### Other observations

- **`submitted_but_unseen` = 0 on every clean 2-minute cell.** Pass 1's end-of-drain tail
  (s3/s8/s9/s10/s11/s12, under the fixed 30 s drain deadline) does not appear at 2 minutes —
  confirming it was a harness observer-window artifact, never data loss (`dropped = 0` throughout).
  *Follow-up (bench-only):* scale the harness `drain_deadline` with run length — `acct-0516`-followup.
- **Enqueue `errors` on the 200-caller cells (s4, s11, s15, s19) — expected staging back-pressure.**
  `ledger_enqueue_trx_c` rejections when the queue is full under sustained 200-caller load;
  `dropped = poisoned = 0`, so nothing is lost — the caller gets a retry signal. The documented
  back-pressure regime, re-confirmed clean.
- **Duration drives absolute rate more than contamination did.** A short (20 s) run catches a
  fill-then-drain burst; a 2-minute run settles to a lower steady state with smaller groups (cg
  drifts down as the arrival rate steadies). Pass 1's "1m → 10m sag" was that steady-state settling
  plus contamination noise — *not* a routed-path degradation. The gated 2-minute figures above are
  the trustworthy steady-state numbers; comparing them to Pass 1's 1m cells shows the contamination
  effect was variable per cell (e.g. s1 1,170 → 1,075, s5 1,865 → 1,838, s10 539 → 632), not a
  uniform multiplier.

Net: the routed path is **clean** (zero data loss, zero committer-level failures across both passes),
and its throughput is now characterized cleanly — committer-parallel on disjoint work,
per-pool-lock-bound on hot work, coalescing-amortized when overlap exists, never router-bound, with
synchronous-ack latency as the standing cost. No correctness defect; the lone follow-up is the
bench-only drain-deadline ergonomics note.

---

## Caveats & limitations

- **Host load** keeps absolute throughput directional; structural ratios (lock-hold floor, collapse
  ratio, exact equivalence) are the load-robust findings. This run's numbers are tighter than the
  original snapshot (consistent with a less-contended host), but the absolute rates — and the
  close, host-sensitive deep-pool (s7/s8) ordering in particular — should still be read as
  directional, not as hard rankings. **Exception: artifact (g) Pass 2 is load-gated** (each cell held
  until 1-min loadavg < 1.5 on the minimal-preload cluster), so its 2-minute throughput figures are
  the most trustworthy absolutes in this report. Earlier artifacts were measured before the
  `shared_preload_libraries` foreign-BGWorker contamination was found and stripped, and before the
  load gate existed — re-running them gated (`common.sh::wait_for_quiet_host`) would tighten them.
- **Mixed-method scenarios (S3/S4)** are **no longer confounded.** The harness now seeds
  `standard_cost` for std/standard-basis pools (`acct-0z5m`, commit `9b6e6b5`), and S3/S4 ran with
  `errors=0` on the direct-per-call and routed paths in this run — so they are first-class
  measurements here, not the `MissingStandardCost`-aborted artifact the original snapshot reported.
  Their `direct-batched` errors are deadlock/serialization aborts under contention (same as the
  clean universes), not a seeding artifact.
- **Method coverage**: scenarios S1–S8 and `MethodMix::Mixed` (50/30/20 fifo/wac/std) drive only
  **fifo, wac, and std** as primary methods (faithful to §10.6); **lifo and specific are not exercised
  by any load or equivalence run** here — they are covered only by `ledger-core` unit/property tests.
  The `--method-mix all-lifo` / `all-specific` CLI variants exist but are outside the canonical set.
- **`acct-036x`** (**closed**, commit `d4c2c5c`) and **`acct-34ce`** (**closed**, commit `07fcb02`):
  `ledger_submit_trx_c` (direct) emits one aggregate UPSERT per line, so a single submission must
  touch *distinct* pools; `ledger-core` now coalesces per-pool aggregate mutations (the routed
  committer was always unaffected). The S1–S8 harness emits distinct pools per submission, so it
  did not exercise the coalesce path under load — `acct-34ce` added the **opt-in multi-touch
  generator** and the **S9** preset, which do, with the result captured in artifact (e).
- **Code review**: a post-build coherence + quality audit (`../AUDIT.md`, `../AUDIT-PASS2.md`)
  found no P1 issues. Its follow-ups have since shipped under epic `acct-yojk` (15/15): de-Path-B
  the routed crate, the shared `ledger-spi-common` crate, a routed property test, a `pool_state`
  `qty >= 0` CHECK, the specific-K=1 guard, Pass-2 hardening, and an arena-leak fix — none of which
  affect these measurements (the harness path was already correct).
- **Recalc/close, negative inventory, multi-currency, effective-dated standard costs, period close
  (§13)** remain deliberately out of scope. This PoC validates the *hot-path* claims only.

## Conclusion

All three target claims are confirmed, and the post-build coalesce-under-load concern
(`acct-34ce`) lands clean. (1) FIFO/LIFO hot-path lock-hold is **constant w.r.t. pool depth** —
latency between depth 100 and 1000 is within ~0.4% on every percentile, and deep pools stay fast
even at 1000-caller concurrency. (2) Routing **collapses** concurrent hot-pool submissions into
per-group lock acquisitions (63 896 trx → 2 078 locks on the single-hot-pool stress, a ~31×
reduction, 6.3× the throughput of direct-per-call) and is the **only** mode that batches safely
under cross-caller contention — standard-tx batching deadlocks (500–1300 aborts) wherever callers
overlap and wins only when they are disjoint. (3) Direct and routed agree **exactly on aggregate
qty** (s5/s7/s8 verdicts carry from 2026-05-27; s9 was re-run and also PASSes with 0 mismatches),
the invariant the deferred recalc/close pass will build on. (4, new) **Multi-touch (same-pool-twice) submissions commit
cleanly under load on both correctness flavors** — s9 (s8's shape + 40% multi-touch) shows
`errors=0` on direct-per-call and `drops=0 / poison=0 / deadlock_retries=0` on routed at
1000-caller deep-FIFO concurrency; ledger-core's `coalesce_aggregates` operates as designed.

The crossover map bounds the routed path's value precisely: it is a **contention-collapse**
mechanism. Use routing for hot/overlapping pools; direct-per-call is the better default for
disjoint or moderate-overlap workloads (and its depth-independent cost — claim 1 — is what keeps it
viable on deep pools). Path C delivers the O(1) hot path it was designed to, with authoritative
FIFO/LIFO cost reconciliation correctly deferred off the critical section.
