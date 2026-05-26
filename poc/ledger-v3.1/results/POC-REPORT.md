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

**Per-method seeding note:** the harness seeds every pool with `provisional_basis='running_avg'`
and **does not** establish `standard_cost` rows. The clean Path C measurements use **all-fifo**
(S5–S8) and **all-wac** (S1, S2) universes (`errors=0` on the direct-per-call and routed paths).
The **mixed-method** scenarios (S3, S4 — 50/30/20 fifo/wac/std) are reported for completeness but
are **confounded**: their std-method pools have no `standard_cost`, so any submission touching one
aborts with `MissingStandardCost`; for the Complex (10–20 distinct pools/submission) shape that is
nearly every submission. Treat S3/S4 absolute throughput as a seeding artifact, not a Path C
property. (Fixing this — seed `standard_cost` for std pools — is a harness follow-up, not a Path C
change.)

**Machine-load caveat:** the run was taken while the host carried substantial concurrent load (per
the operator). **Absolute throughput numbers are therefore directional.** The findings that
survive the noise are the *structural* ones, which are ratios/shapes rather than peak rates: the
flat lock-hold *floor* across depth (artifact a), the lock-acquisition *collapse* ratio (artifact
c), and the exact aggregate-qty *equivalence* (artifact d). Where noise matters to a conclusion it
is called out inline.

All harness invocations were hard-`timeout`-wrapped; the deep (depth-1000) reseeds ran under a
1800 s per-call cap. No run wedged; no committer poisoned; zero deadlock-retries on the routed
path across the whole matrix.

---

## (a) Lock-hold time vs pool depth — §11.2 headline → **PREMISE HOLDS**

Identical workload (s7 shape: simple FIFO depletions, Zipf(1.2), 16 callers, direct-per-call),
the only variable being the seeded layer depth. A strict layer-walking FIFO would show latency
growing ~linearly with depth (100× more rows to walk from depth 10 → 1000). Path C does not.

| Depth | min µs | p50 µs | p95 µs | p99 µs | mean µs | throughput trx/s |
|------:|-------:|-------:|-------:|-------:|--------:|-----------------:|
| 10    | 3180   | 15433  | 93126  | 149946 | 30425   | 525.5 |
| 100   | 3059   | 6823   | 65634  | 87556  | 19764   | 808.7 |
| 1000  | 3074   | 9732   | 80216  | 113901 | 24439   | 654.0 |

**Verdict: constant.** A 100× depth increase produces **no upward trend** in any percentile —
depth 10 is the *slowest* of the three and depth 1000 sits *below* it. The cleanest signal is the
**min (the noise floor, least contaminated by host load): 3180 / 3059 / 3074 µs — flat within 4%
across the 100× depth range.** The p50/p99/mean spread between rows is host-load jitter, not a
depth signal (if depth drove cost, depth-1000 would dominate; it does not). The depth-1000 column
also reappears in the crossover at full 1000-caller concurrency (s7 direct-per-call, 907 trx/s) —
deep pools do not crush the direct path, precisely because the hot-path cost is depth-independent.

This is the load-bearing result: **Path C buys O(1) hot-path cost regardless of how many FIFO/LIFO
layers a pool has accumulated.** The deferred recalc/close (§13) is where layer-walking cost would
reappear — by design, off the hot path.

---

## (b) Direct ↔ routed crossover map — §11.4

Full matrix, all three modes. `cg` = routed commit_group_size_avg; `locks` = `pool_lock`
acquisitions; `err` = failed/dropped submissions. Clean (errors=0) cells in **bold**.

| Scn | shape | depth | direct-per-call | direct-batched | routed |
|-----|-------|------:|-----------------|----------------|--------|
| s1 | 10 callers, uniform, all-wac | 0 | **1189.7 trx/s** (p99 24ms) | 834.6 trx/s (err 31) | **1159.5 trx/s** (cg 1.09) |
| s2 | 200 callers, zipf1.5, all-wac | 0 | **566.6 trx/s** | 4.0 trx/s (err 519) | **754.9 trx/s** (cg 7.25) |
| s3 | 10 callers, complex, mixed | 0 | 27.1 (err 17594†) | 33.0 (err 21958†) | 14.5 (err 9503†) |
| s4 | 200 callers, zipf1.2, complex, mixed | 0 | 81.0 (err 415†) | 6.4 (err 1175†) | 644.9 (cg 31.70, err 3193†) |
| s5 | 1000 callers, **single hot pool**, all-fifo | 10 | **374.9 trx/s** (p99 2.82s) | 1079.1 (err 845) | **941.5 trx/s** (cg 42.67, locks 680) |
| s6 | 1000 callers, **disjoint stripes**, all-fifo | 10 | **2388.9 trx/s** | 3195.0 (err 58) | **789.2 trx/s** (cg 1.06) |
| s7 | 1000 callers, zipf1.2, all-fifo, **deep** | 1000 | **907.3 trx/s** (p99 1.9s) | 5.9 (err 1108) | **326.3 trx/s** (cg 3.20, locks 3182) |
| s8 | 1000 callers, zipf1.2, complex, all-fifo, **deep** | 1000 | **239.8 trx/s** | 122.0 (err 914) | **194.7 trx/s** (cg 46.57) |

† S3/S4 are mixed-method and confounded by the `standard_cost` seeding gap (see methodology) —
their error counts are MissingStandardCost aborts, not contention. Read them directionally only.

### The regions

- **Disjoint, no cross-caller overlap (s6):** `direct-batched (3195) > direct-per-call (2389) >
  routed (789)`. Batching amortizes commit/fsync with nothing to contend over, so it wins.
  Routing *loses* here — its serialize-then-handoff is pure overhead when there is nothing to
  collapse (cg 1.06 ≈ one submission per group).
- **Low concurrency (s1):** `direct-per-call ≈ routed (≈1.16–1.19k) > direct-batched (835)`.
  Neither batching mode helps; routed's overhead is negligible (cg 1.09) and matches per-call.
- **Moderate concurrency + overlap (s2):** `routed (755) > direct-per-call (567) ⋙
  direct-batched (4.0, 519 deadlock-aborts)`. Routing's safe cross-caller batching pulls ahead.
- **Extreme single-pool contention (s5):** `routed (941, 0 err, 680 locks) ≈/> direct-batched
  (1079 but 845 aborts) > direct-per-call (375, p99 2.82s)`. See artifact (c).
- **Deep + moderate overlap (s7, s8):** `direct-per-call > routed > direct-batched(collapse)`.
  At Zipf(1.2) the per-pool overlap is too low (cg 3.2 on s7) to amortize routing's enqueue→
  router→committer handoff under host load, and direct-per-call's **depth-independent** hot path
  (artifact a) keeps it strong even at depth 1000.

### Key question (§11.4): *does routed beat direct-batched, or does standard-tx batching capture most of routing's benefit?*

**Routing wins decisively wherever callers contend, and standard-tx batching does NOT capture the
benefit — under overlap it inverts into a deadlock liability.** In every scenario with cross-caller
pool overlap (s2, s5, s7, s8 on the clean all-fifo/all-wac universes), `direct-batched` collapsed —
4.0, 1079*(lossy)*, 5.9, 122 trx/s respectively, each with **500–1100 deadlock/serialization
aborts** — because a batch holds its pools' locks for the whole 50-submission transaction, and
overlapping batches across callers invert lock order. `direct-batched` beats `direct-per-call`
**only** when callers are disjoint (s6). Routing avoids this entirely: a pool is owned by exactly
one committer at a time, so a whole commit_group's depletions serialize *inside* one transaction
with **zero deadlock-retries observed across the entire matrix**. Routing is the only mode that
batches *safely* under contention.

The complementary truth: routing does **not** universally beat `direct-per-call`. Direct-per-call
wins on disjoint (s6) and on deep-but-moderate-overlap (s7/s8) workloads, and ties at low
concurrency (s1). The routing win is specifically **contention collapse** (s5, s2) plus
**operational cleanliness** (no aborts) everywhere.

---

## (c) Routed hot-pool throughput envelope + batching collapse — §11.3 / §6.7

The §6.7 win is clearest on **s5** (1000 callers all targeting **one** pool, depth 10):

| metric | direct-per-call | routed |
|---|---|---|
| throughput | 374.9 trx/s | **941.5 trx/s** (2.5×) |
| trx committed | 13 377 | 29 014 |
| `pool_lock` acquisitions | 1 / trx (≈13 377) | **680** |
| aggregate UPSERTs | 1 / trx | **680** |
| commit_group_size_avg | — | **42.67** |
| failed / dropped | 0 | **0** |
| ack p99 | 2.82 s | 1.85 s |

**29 014 trx committed through 680 `pool_lock` acquisitions and 680 aggregate UPSERTs** — a ~43×
collapse of the locking/fsync footprint, matching `commit_group_size_avg = 42.67` (29014 / 680).
Where direct-per-call forces every one of 1000 callers to queue for the single hot pool's lock
one trx at a time (p99 = 2.82 s of pure lock-wait), routing drains them in groups of ~43 under a
single lock-hold, then commits once. The collapse is the entire point of the routed path on hot
pools, and it lands cleanly (0 drops, 0 poison, 0 deadlock-retries).

Corroborating envelope points:
- **s2** (200 callers, Zipf1.5): cg 7.25, 22 778 trx via **3143** drains/locks/upserts — routing
  beats direct-per-call (755 vs 567 trx/s) and annihilates direct-batched (4.0).
- **s7** (1000 callers, Zipf1.2, **depth 1000**): cg 3.20, 10 196 trx via **3182** locks. Lower
  overlap → smaller groups → here the handoff cost isn't fully amortized and direct-per-call leads
  on raw throughput; the collapse mechanism still operates (one lock per ~3.2 trx) and the path
  stays clean (0 drops).
- **s8** (complex multi-pool, deep): cg 46.57 submissions/group; because each trx touches ~6–7
  distinct pools the lock count (46 052) is per-(group×pool), so the simple "one lock per trx"
  collapse is muddied by multi-pool footprint — s5 (single pool, simple) remains the clean
  demonstration.

**Envelope summary:** routed hot-pool throughput scales with *overlap density*. The denser the
contention on a pool (s5 single pool → cg 42.7), the larger the collapse and the more routing
beats direct; as overlap thins toward disjoint (s6 → cg 1.06), the collapse vanishes and routing's
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

- **Host load** made absolute throughput directional; structural ratios (lock-hold floor, collapse
  ratio, exact equivalence) are the load-robust findings. A re-run on a quiet host would tighten
  the throughput numbers but is not expected to change any verdict.
- **Mixed-method scenarios (S3/S4)** are confounded by the harness not seeding `standard_cost` for
  std-method pools (→ MissingStandardCost aborts). Path C conclusions rest on the all-fifo (S5–S8)
  and all-wac (S1/S2) universes, which ran clean. Seeding std costs is a harness follow-up.
- **`acct-036x`** (open P2 bug): `ledger_submit_trx_c` (direct) emits one aggregate UPSERT per
  line, so a single submission must touch *distinct* pools (the routed committer coalesces and is
  unaffected). The harness already generates distinct pools per submission, so it did not affect
  this run; noted for completeness.
- **Recalc/close, negative inventory, multi-currency, effective-dated standard costs, period close
  (§13)** remain deliberately out of scope. This PoC validates the *hot-path* claims only.

## Conclusion

All three target claims are confirmed. (1) FIFO/LIFO hot-path lock-hold is **constant w.r.t. pool
depth** — the noise-floor latency is flat within 4% across a 100× depth range, and deep pools stay
fast even at 1000-caller concurrency. (2) Routing **collapses** concurrent hot-pool submissions
into per-group lock acquisitions (29 014 trx → 680 locks on the single-hot-pool stress, a ~43×
reduction) and is the **only** mode that batches safely under cross-caller contention — standard-tx
batching deadlocks (500–1100 aborts) wherever callers overlap and wins only when they are disjoint.
(3) Direct and routed agree **exactly on aggregate qty**, the invariant the deferred recalc/close
pass will build on; provisional unit_cost is permitted to diverge and did not here.

The crossover map bounds the routed path's value precisely: it is a **contention-collapse**
mechanism. Use routing for hot/overlapping pools; direct-per-call is the better default for
disjoint or moderate-overlap workloads (and its depth-independent cost — claim 1 — is what keeps it
viable on deep pools). Path C delivers the O(1) hot path it was designed to, with authoritative
FIFO/LIFO cost reconciliation correctly deferred off the critical section.
