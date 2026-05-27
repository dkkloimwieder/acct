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

---

## Environment & methodology

| Item | Value |
|---|---|
| DB | `poc_v3_1` on `localhost:5111`, container `acct-postgres`, Postgres 18 (`io_uring`) |
| Extensions | `ledger_direct_c` (`ledger_submit_trx_c`), `ledger_routed_c` (`ledger_enqueue_trx_c` + router + 4-committer pool), both preloaded |
| 1000-caller path | pgbouncer transaction pool, host `6432` → `acct-postgres:5432`, `pool_mode=transaction`, `default_pool_size=64` (`acct-8cn2`) |
| Harness | `target/release/ledger-harness` (sqlx + tokio, multi-session) |
| Submission modes (§10.0) | `direct-per-call` (one user-tx per submit), `direct-batched` (50 submits per user-tx), `routed` (enqueue → committer pool) |
| Crossover scale | `SEED_COUNT=10000` pools, `30s`/run, full S1–S8 × 3 modes |
| Lock-hold sweep | 256 FIFO pools, 16 callers, depths {10, 100, 1000} |

**Measurement provenance:** these numbers are a full re-measurement on **2026-05-27** against the
**production build** (no `test_hooks`) after the `acct-yojk` follow-ups, superseding the original
P5 snapshot. The verdicts are unchanged; the numbers are cleaner (see below).

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
1800 s per-call cap. Every run completed (24 crossover + 3 lock-hold + 3 equivalence) — none
wedged, failed, or timed out; no committer poisoned; **zero dropped submissions and zero
deadlock-retries on the routed path across the whole matrix** (all 8 scenarios).

---

## (a) Lock-hold time vs pool depth — §11.2 headline → **PREMISE HOLDS**

Identical workload (s7 shape: simple FIFO depletions, Zipf(1.2), 16 callers, direct-per-call),
the only variable being the seeded layer depth. A strict layer-walking FIFO would show latency
growing ~linearly with depth (100× more rows to walk from depth 10 → 1000). Path C does not.

| Depth | min µs | p50 µs | p95 µs | p99 µs | mean µs | throughput trx/s |
|------:|-------:|-------:|-------:|-------:|--------:|-----------------:|
| 10    | 2367   | 3872   | 50626  | 55246  | 14947   | 1069.6 |
| 100   | 2340   | 3903   | 50954  | 55410  | 15017   | 1064.5 |
| 1000  | 2451   | 3901   | 50987  | 56131  | 15087   | 1059.7 |

**Verdict: constant.** A 100× depth increase produces **no upward trend in any percentile.** Unlike
the original snapshot (which had to lean on the min noise-floor to see through host jitter), this
run is flat **end to end**: p50 within **0.8%** (3872→3901), p95 within **0.7%**, p99 within
**1.6%** (55246→56131), mean within **0.9%**, and throughput within **0.9%** across the entire 100×
depth range. A strict layer-walking FIFO would show ~linear growth (100× more rows to walk); Path C
shows none. The depth-1000 row also reappears in the crossover at full 1000-caller concurrency (s7
direct-per-call, 1222.8 trx/s) — deep pools do not crush the direct path, precisely because the
hot-path cost is depth-independent.

This is the load-bearing result: **Path C buys O(1) hot-path cost regardless of how many FIFO/LIFO
layers a pool has accumulated.** The deferred recalc/close (§13) is where layer-walking cost would
reappear — by design, off the hot path.

---

## (b) Direct ↔ routed crossover map — §11.4

Full matrix, all three modes. `cg` = routed commit_group_size_avg; `locks` = `pool_lock`
acquisitions; `err` = failed/dropped submissions. Clean (errors=0) cells in **bold**.

| Scn | shape | depth | direct-per-call | direct-batched | routed |
|-----|-------|------:|-----------------|----------------|--------|
| s1 | 10 callers, uniform, all-wac | 0 | **2253.9 trx/s** (p99 11ms) | 901.7 trx/s (err 36) | **1592.6 trx/s** (cg 1.10) |
| s2 | 200 callers, zipf1.5, all-wac | 0 | **744.3 trx/s** (p99 0.74s) | 6.0 trx/s (err 528) | **1377.8 trx/s** (cg 7.42) |
| s3 | 10 callers, complex, mixed | 0 | **1058.4 trx/s** | 9.5 (err 46) | **1146.9 trx/s** (cg 4.74) |
| s4 | 200 callers, zipf1.2, complex, mixed | 0 | **261.2 trx/s** | 7.8 (err 809) | **993.9 trx/s** (cg 15.36) |
| s5 | 1000 callers, **single hot pool**, all-fifo | 10 | **386.5 trx/s** (p99 2.70s) | 1243.4 (err 807) | **2381.7 trx/s** (cg 26.69, locks 2726) |
| s6 | 1000 callers, **disjoint stripes**, all-fifo | 10 | **3305.2 trx/s** | **4163.8 trx/s** | **1130.0 trx/s** (cg 1.07) |
| s7 | 1000 callers, zipf1.2, all-fifo, **deep** | 1000 | **1222.8 trx/s** (p99 1.14s) | 5.2 (err 1114) | **1892.3 trx/s** (cg 3.57, locks 16491) |
| s8 | 1000 callers, zipf1.2, complex, all-fifo, **deep** | 1000 | **245.4 trx/s** | 136.9 (err 772) | **1032.5 trx/s** (cg 13.97) |

S3/S4 (mixed-method) now run **clean** (`errors=0` on direct-per-call and routed) — the
`standard_cost` seeding gap that confounded them in the original snapshot is closed (`acct-0z5m`).
Their `direct-batched` error counts are deadlock/serialization aborts under the complex multi-pool
overlap, the same contention failure as the all-fifo/all-wac scenarios — not MissingStandardCost.

### The regions

- **Disjoint, no cross-caller overlap (s6):** `direct-batched (4164) > direct-per-call (3305) >
  routed (1130)`. Batching amortizes commit/fsync with nothing to contend over, so it wins (and
  here it runs clean, err 0). Routing *loses* — its serialize-then-handoff is pure overhead when
  there is nothing to collapse (cg 1.07 ≈ one submission per group).
- **Low concurrency, uniform (s1):** `direct-per-call (2254) > routed (1593) > direct-batched
  (902)`. At 10 uniform callers there's little to collapse (cg 1.10), so routing's handoff costs it
  and per-call wins outright.
- **Moderate-to-high concurrency + overlap (s2, s4):** `routed (1378, 994) > direct-per-call (744,
  261) ⋙ direct-batched (6.0, 7.8 — 500–800 deadlock-aborts)`. Routing's safe cross-caller batching
  pulls clearly ahead.
- **Extreme single-pool contention (s5):** `routed (2382, 0 err, 2726 locks) ⋙ direct-per-call
  (387, p99 2.70s)`; direct-batched 1243 but 807 aborts. ~6.2× over per-call. See artifact (c).
- **Deep pools (s7, s8, depth 1000):** `routed (1892, 1032) > direct-per-call (1223, 245) ⋙
  direct-batched (5.2, 137)`. Routing edges ahead on the deep shapes in this run — the *opposite*
  of the original snapshot, where direct-per-call led on s7/s8. The deep-pool ranking is close and
  **sensitive to host load** (both paths are clean; read it as "comparable, both viable" rather
  than a hard ordering); what is robust is that direct-per-call stays strong even at depth 1000
  because its hot path is **depth-independent** (artifact a).
- **Mixed-method, now clean (s3, s4):** with `standard_cost` seeded, S3 (`routed 1147 ≈
  direct-per-call 1058`) and S4 (`routed 994 ⋙ direct-per-call 261`) run error-free; `direct-batched`
  still aborts (9.5, 7.8) under the complex multi-pool overlap.

### Key question (§11.4): *does routed beat direct-batched, or does standard-tx batching capture most of routing's benefit?*

**Routing wins decisively wherever callers contend, and standard-tx batching does NOT capture the
benefit — under overlap it inverts into a deadlock liability.** In every scenario with cross-caller
pool overlap (s2, s4, s5, s7, s8), `direct-batched` collapsed — 6.0, 7.8, 1243*(lossy)*, 5.2, 137
trx/s respectively, each with **500–1100 deadlock/serialization aborts** — because a batch holds
its pools' locks for the whole 50-submission transaction, and overlapping batches across callers
invert lock order. `direct-batched` beats `direct-per-call` **only** when callers are disjoint (s6).
Routing avoids this entirely: a pool is owned by exactly one committer at a time, so a whole
commit_group's depletions serialize *inside* one transaction with **zero deadlock-retries observed
across the entire matrix**. Routing is the only mode that batches *safely* under contention.

The complementary truth: routing does **not** universally beat `direct-per-call`. Direct-per-call
wins on disjoint (s6) and at low-concurrency-uniform (s1). On the deep moderate-overlap shapes
(s7/s8) the two are close and the ordering flips with host load (routed led this run, per-call led
the original snapshot). The durable routing win is **contention collapse** (s5, s2, s4) plus
**operational cleanliness** (no aborts) everywhere.

---

## (c) Routed hot-pool throughput envelope + batching collapse — §11.3 / §6.7

The §6.7 win is clearest on **s5** (1000 callers all targeting **one** pool, depth 10):

| metric | direct-per-call | routed |
|---|---|---|
| throughput | 386.5 trx/s | **2381.7 trx/s** (6.2×) |
| trx committed | 13 623 | 72 769 |
| `pool_lock` acquisitions | 1 / trx (≈13 623) | **2 726** |
| aggregate UPSERTs | 1 / trx | **2 726** |
| commit_group_size_avg | — | **26.69** |
| failed / dropped | 0 | **0** |
| ack p99 | 2.70 s | 0.69 s |

**72 769 trx committed through 2 726 `pool_lock` acquisitions and 2 726 aggregate UPSERTs** — a
~27× collapse of the locking/fsync footprint, matching `commit_group_size_avg = 26.69` (72769 /
2726). Where direct-per-call forces every one of 1000 callers to queue for the single hot pool's
lock one trx at a time (p99 = 2.70 s of pure lock-wait), routing drains them in groups of ~27 under
a single lock-hold, then commits once — 6.2× the throughput at a quarter of the ack p99. The
collapse is the entire point of the routed path on hot pools, and it lands cleanly (0 drops, 0
poison, 0 deadlock-retries).

Corroborating envelope points:
- **s2** (200 callers, Zipf1.5): cg 7.42, 41 580 trx via **5 606** drains/locks/upserts — routing
  beats direct-per-call (1378 vs 744 trx/s) and annihilates direct-batched (6.0).
- **s7** (1000 callers, Zipf1.2, **depth 1000**): cg 3.57, 58 861 trx via **16 491** locks. Lower
  overlap → smaller groups, but the collapse mechanism still operates (one lock per ~3.6 trx),
  routing leads on raw throughput (1892 vs 1223), and the path stays clean (0 drops) even at depth
  1000.
- **s8** (complex multi-pool, deep): cg 13.97 submissions/group; because each trx touches ~6–7
  distinct pools the lock count (247 953) is per-(group×pool), so the simple "one lock per trx"
  collapse is muddied by multi-pool footprint — s5 (single pool, simple) remains the clean
  demonstration.

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

**Aggregate qty is identical across flavors on every pool** — the order-independent correctness
invariant (a signed sum of line qtys) holds regardless of how routing batches/reorders submissions
across callers. This is the property recalc/close (§13) will depend on: the *quantity* ledger is
exact under both paths, so the deferred authoritative cost pass has a sound foundation.

**On the 0 provisional-unit_cost divergences (the §9.4 note):** provisional unit_cost is
order-sensitive *for receipts* (WAC running-average), and §9.4 explicitly **permits** it to differ
across flavors. It did not differ here, for two compounding reasons:

1. **s5/s7 are pure-depletion** (`deplete_pct=100`). Depletions read the running aggregate but do
   **not** mutate aggregate unit_cost — so there is no order-sensitive quantity to diverge.
2. **s8 has receipts** (`deplete_pct=50`, random costs) yet still matched, because the router
   **preserves per-pool enqueue order** within a commit_group and the committer applies in that
   order. So the WAC running-average sees the *same per-pool operation order* as the caller-serial
   direct flavor, yielding identical provisional costs.

Divergence therefore remains **architecturally permitted but unobserved** in this PoC: it would
surface only if same-pool receipts were committed out of enqueue order (e.g. cross-chunk reordering
under a smaller `batch_size_max`). Crucially, even if/when provisional unit_cost diverges, **qty —
the recalc/close input — stays exact**, so the divergence is a cosmetic provisional-cost artifact,
not a correctness defect. Authoritative cost reconciliation is deferred (§13).

---

## Caveats & limitations

- **Host load** keeps absolute throughput directional; structural ratios (lock-hold floor, collapse
  ratio, exact equivalence) are the load-robust findings. This run's numbers are tighter than the
  original snapshot (consistent with a less-contended host), but the absolute rates — and the
  close, host-sensitive deep-pool (s7/s8) ordering in particular — should still be read as
  directional, not as hard rankings.
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
- **`acct-036x`** (**closed**, commit `d4c2c5c`): `ledger_submit_trx_c` (direct) emits one aggregate
  UPSERT per line, so a single submission must touch *distinct* pools; `ledger-core` now coalesces
  per-pool aggregate mutations (the routed committer was always unaffected). The harness generates
  distinct pools per submission, so it did not affect this run; noted for completeness.
- **Code review**: a post-build coherence + quality audit (`../AUDIT.md`, `../AUDIT-PASS2.md`)
  found no P1 issues. Its follow-ups have since shipped under epic `acct-yojk` (15/15): de-Path-B
  the routed crate, the shared `ledger-spi-common` crate, a routed property test, a `pool_state`
  `qty >= 0` CHECK, the specific-K=1 guard, Pass-2 hardening, and an arena-leak fix — none of which
  affect these measurements (the harness path was already correct).
- **Recalc/close, negative inventory, multi-currency, effective-dated standard costs, period close
  (§13)** remain deliberately out of scope. This PoC validates the *hot-path* claims only.

## Conclusion

All three target claims are confirmed. (1) FIFO/LIFO hot-path lock-hold is **constant w.r.t. pool
depth** — latency is flat across *every* percentile (p50/p99 within ~2%) over a 100× depth range,
and deep pools stay fast even at 1000-caller concurrency. (2) Routing **collapses** concurrent
hot-pool submissions into per-group lock acquisitions (72 769 trx → 2 726 locks on the
single-hot-pool stress, a ~27× reduction, 6.2× the throughput of direct-per-call) and is the
**only** mode that batches safely under cross-caller contention — standard-tx batching deadlocks
(500–1100 aborts) wherever callers overlap and wins only when they are disjoint. (3) Direct and
routed agree **exactly on aggregate qty**, the invariant the deferred recalc/close pass will build
on; provisional unit_cost is permitted to diverge and did not here.

The crossover map bounds the routed path's value precisely: it is a **contention-collapse**
mechanism. Use routing for hot/overlapping pools; direct-per-call is the better default for
disjoint or moderate-overlap workloads (and its depth-independent cost — claim 1 — is what keeps it
viable on deep pools). Path C delivers the O(1) hot path it was designed to, with authoritative
FIFO/LIFO cost reconciliation correctly deferred off the critical section.
