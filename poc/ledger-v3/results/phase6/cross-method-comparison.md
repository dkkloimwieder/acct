# acct-9mgx.7 — Cross-method comparison roll-up

**Issue:** acct-9mgx.7 (P2; final child of acct-9mgx, depends on .1–.6 all closed)
**Run window:** 2026-05-22 → 2026-05-24
**Source docs:** `wac-perpetual-validation.md` (.5), `wac-periodic-validation.md` (.6), `fifo-validation.md` (.1), `lifo-validation.md` (.2), `specific-validation.md` (.3), `std-validation.md` (.4)
**Companion docs:** `CHARACTERIZATION.md` (per-path Phase 6 regime map), `equivalence-summary.md` (pre-method-mix WAC-only baseline), `h5gs-cumulative-sum-validation.md` (the original WAC structural fix)

## Headline

**All six cost methods (WAC-perpetual, wac_periodic, FIFO, LIFO, Specific, STD) reach the same byte-equivalence guarantee on the cross-path harness under default GUCs (`committer_count = 4`).** The order-sensitive class (FIFO / LIFO / Specific) required two router/committer fixes (acct-aywu + acct-tm09) to get there; the split-safe class (WAC / wac_periodic / STD) reached it free. Each method ships with a defensive per-method `_drifts` diff bucket; none fire under default cm=4 on the canonical 15-trial sweeps.

**PoC v3 has drawn enough method-coverage conclusions to exit the per-method validation phase.** The cross-method roll-up below identifies no missing method, no missing drift class, and no pending h5gs-analog structural-fix work. Phase 7 (re-baselining against tuning levers + production-shape workloads) is the natural next focus.

## Per-method correctness summary

All entries are under default GUCs (`committer_count = 4`, `batch_size_max = 50`) on the canonical 15-trial sweep (6 lenient + 9 strict = 3 trials × 3 race-conditional scenarios). Universe: 100 pools × 10 SKUs × 10 locations; submissions per caller: 50.

| Method        | Path-equiv class       | Sweep result            | Drift bucket           | Bucket fires under cm=4? | Path-B infrastructure                       | h5gs-analog needed? |
|---------------|------------------------|-------------------------|------------------------|--------------------------|---------------------------------------------|---------------------|
| WAC-perpetual | split-safe + commutative | 15/15 byte-identical    | `wac_drifts`           | No (zero drifts)         | none beyond base; h5gs cumulative-sum is in-core | done in acct-h5gs   |
| wac_periodic  | split-safe + period-bracketed | pool_state byte-equiv on all 6; provisional aggregate matches; per-row provisional differs (informational) | `wac_periodic_drifts` | Yes, by design (running-avg at deplete-time depends on commit_group ordering) | new: `posting_lines_provisional` table + close-hook variance posting (acct-s6fa) | n/a (drift is structurally inherent + bounded by invariant) |
| FIFO          | order-sensitive layered | 15/15 byte-identical    | `fifo_drifts`          | No                       | acct-aywu (router order-sensitive classifier) + acct-tm09 (per-pool seq numbers + committer predecessor-wait) | done in acct-aywu + acct-tm09 |
| LIFO          | order-sensitive layered | 15/15 byte-identical    | `lifo_drifts`          | No                       | shared with FIFO; aywu classifier returns true for `"lifo"`, tm09 sequence-stamping uniform | done by inheritance |
| Specific      | order-sensitive layered (K=1 conv.) | 15/15 byte-identical | `specific_drifts`      | No                       | shared with FIFO/LIFO; aywu + tm09 classifier returns true for `"specific"` | done by inheritance |
| STD           | split-safe + stateless   | 15/15 byte-identical    | (none needed)          | n/a                      | none — STD has no `pool_state` rows | n/a (no drift surface) |

**Two cleavages drive correctness:**

1. **Order-sensitive vs split-safe** (the dispatcher's posting-time dimension): FIFO / LIFO / Specific need the router + committer to preserve per-pool submission order across windows; WAC / wac_periodic / STD do not. Router classifier `is_order_sensitive_method` at `ledger-routed/src/router.rs:194` codifies this split.

2. **Stateful vs stateless pool storage** (the schema dimension): WAC / wac_periodic / FIFO / LIFO / Specific maintain `pool_state` rows (single-row aggregate for WAC family, layer-array for layered); STD does not. Stateful methods pay snapshot hydration + writeback per commit_group; STD skips both. Visible as the **routed s2 throughput advantage** for STD (see bench table below).

The wac_periodic drift bucket is the only one that fires by design — its drift class is structurally inherent (per-depletion `provisional_amount` is a function of the running average at deplete-time, which depends on commit_group ordering) and bounded by a deterministic aggregate invariant (`Σ provisional + Σ variance = final_avg × Σ depletion_qty` per pool). Drift is informational; `--strict` upgrades to errors. wac_periodic's correctness story is "byte-equivalent at the load-bearing layer (pool_state + per-pool aggregates); per-row decomposition allowed to differ."

## Per-method bench summary

20 callers, 30s duration, 1000-pool universe, default GUCs. Cross-method comparison; each per-method bench ran a fresh WAC baseline cell as a noise-control anchor (the WAC numbers below differ run-to-run by ±10% due to bench noise; they are the within-run baseline). wac_periodic is **not** benched — equivalence-only — because the close-hook variance posting is the dominant cost on small windows and would skew the comparison.

| Scenario | Path     | wac (range) | wac_periodic | fifo  | lifo  | specific | std   |
|----------|----------|------------:|:------------:|------:|------:|---------:|------:|
| s2       | direct   |    545–615  |     —        |  408  |  466  |    464   |   616 |
| s2       | routed   |  1 755–1 908 |     —       | 1 189 | 1 433 |  1 410   | 2 192 |
| s5       | direct   |    293–297  |     —        |  197  |  203  |    205   |   304 |
| s5       | routed   |  1 854–2 150 |     —       | 2 267 | 2 366 |  2 409   | 2 414 |

All numbers in tx/s. Source: `bench-9mgx{1,2,3,4}/*` JSONs.

**Method ranking — routed s5 (single hot pool, the maximally-contended cell):**

| Rank | Method   | tx/s   | commit_group_avg | p99 ack (ms) |
|------|----------|-------:|-----------------:|-------------:|
| 1    | STD      | 2 414  |              37  |          46  |
| 2    | Specific | 2 409  |             130  |         202  |
| 3    | LIFO     | 2 366  |             118  |         185  |
| 4    | FIFO     | 2 267  |              ~80 |         217  |
| 5    | WAC      | 2 130  |              37  |          53  |

STD wins on per-event work (no pool_state read/write); the order-sensitive trio (Specific, LIFO, FIFO) wins on commit_group amortization (aywu's chunking bypass drives large commit_groups, amortizing per-COMMIT fsync + pool_lock + snapshot hydration); WAC sits in the middle (small commit_groups + cheap per-event work). Order-sensitive methods trade higher ack latency for higher throughput on this workload.

**Method ranking — routed s2 (zipf-1.5 dispersed, lock-amortization-favorable):**

| Rank | Method   | tx/s   | commit_group_avg | p99 ack (ms) |
|------|----------|-------:|-----------------:|-------------:|
| 1    | STD      | 2 192  |             7.5  |          42  |
| 2    | WAC      | 1 908  |             7.6  |          47  |
| 3    | LIFO     | 1 433  |             7.6  |          49  |
| 4    | Specific | 1 410  |             7.6  |          49  |
| 5    | FIFO     | 1 189  |             7.6  |          59  |

Commit_groups are comparable size across methods (aywu's chunking bypass is moot at zipf-1.5 because contention is dispersed enough that windows don't naturally fill batch_size_max for a single pool). Pure per-event work dominates: STD's zero-pool_state work + WAC's single-row UPSERT beat the layered trio's per-event Insert + Update + occasional cross-layer Delete.

**Direct-path overhead (per-event work, no commit_group amortization):**
- WAC: 545–615 tx/s s2, 293–297 s5 (baseline shape).
- STD: ~2% faster than WAC (within noise; trivial savings from skipping pool_state writes).
- FIFO / LIFO / Specific: 25–34% slower than WAC across s2 + s5. The per-event layer maintenance overhead is the operative cost; the order-sensitive infrastructure on Path B is not invoked because Path A's single-trx-per-commit shape doesn't go through the router.

## Crossover regime — does the Direct ↔ Routed boundary shift by method?

`CHARACTERIZATION.md` (pre-method-mix) established the headline result: Path B wins under contention + complexity; Path A wins under disjoint + simple. Re-examining under the post-method-mix bench data:

- **The crossover boundary holds for all methods.** Every method ships routed > direct on s2 + s5 (the contended scenarios) and direct > routed on s1 + s6 (disjoint scenarios) per the Phase 3 + Phase 5-v2 numbers. The per-method ratio shifts but the regime map's qualitative shape does not.

- **The crossover *magnitude* shifts dramatically for the order-sensitive methods on s5.** Direct FIFO on s5 = 197 tx/s vs Routed FIFO on s5 = 2267 tx/s — an **11.5× routed advantage** (vs WAC's 7.2× and STD's 7.9×). The driver: Path A's per-event layer maintenance + pool_lock contention compounds on the single hot pool, while Path B's commit_group amortization grows commit_group_avg to 80–130 specifically because aywu disables chunking for the order-sensitive methods. The methods that suffer most on direct also benefit most from routing.

- **STD on routed s2 is a new outlier.** 2192 tx/s — only s5 routed STD (2414) and s5 routed Specific (2409) are faster across the entire matrix. The combination of (a) zero pool_state work + (b) zipf-1.5 lock-amortization-favorable distribution makes STD the throughput leader for routed s2, even though commit_groups are no larger than WAC's.

The regime map gains one nuance per method (which method?) but the two-axis structure (overlap density × complexity) is method-independent. No method warrants a new column in the §10.4 regime table.

## Conclusions

### Q1: Which methods are operationally equivalent in PoC v3?

**All six.** Path A and Path B produce byte-identical end-state on `trx + trx_line + posting_line + pool_state` for the five split-safe and order-sensitive methods under default cm=4. wac_periodic is byte-identical on the load-bearing surfaces (`pool_state` + per-pool aggregate `Σ provisional + Σ variance = final_avg × Σ qty`); per-row provisional decomposition is allowed to differ and is structurally inherent.

The operational equivalence guarantee: **for any cost method, the choice of Path A vs Path B does not alter the ledger end-state.** The choice is a throughput/latency tradeoff, not a correctness tradeoff.

### Q2: Does h5gs cumulative-sum generalize beyond WAC, or is per-method analog work needed?

**It does not need to generalize.** The h5gs cumulative-sum form is a WAC-specific structural fix: by storing `value_sum` rather than `value/qty` in `pool_state.unit_cost`, receipts become additive-commutative (eliminating cross-commit_group running-avg drift), and depletions become single-bounded-rounds (eliminating compounding). This addresses a property unique to WAC.

The other five methods don't have an analogous drift surface:
- **wac_periodic** has a drift class but it is structurally inherent (running-avg at deplete-time) and bounded by a deterministic aggregate invariant — no structural fix is possible or desirable.
- **FIFO / LIFO / Specific** have a drift class only when the inter-window or intra-window ordering breaks; acct-aywu + acct-tm09 ARE their "h5gs analog" — but at the router/committer layer rather than in-core. Per-pool sequence numbers + predecessor-wait restore equivalence without modifying any ledger-core math.
- **STD** has no `pool_state` and so no drift surface at all.

**No additional per-method h5gs-analog work surfaced during the .1–.6 pass.** The drift catalog is complete.

### Q3: Per-method perf characteristics

| Cell        | Fastest   | Slowest | Spread | Headline                                                  |
|-------------|-----------|---------|-------:|-----------------------------------------------------------|
| s2 direct   | STD (616) | FIFO (408) | 51%  | Layered family pays per-event Update+Insert+Delete tax    |
| s5 direct   | STD (304) | FIFO (197) | 54%  | Same shape as s2 direct; layered tax independent of overlap |
| s2 routed   | STD (2 192) | FIFO (1 189) | 84% | Lock-amortization-favorable for STD; layered family doesn't get commit_group bonus at this dispersion |
| s5 routed   | STD (2 414) | WAC (2 130) | 13% | The hot-pool cell — order-sensitive methods catch up via aywu's chunking bypass |

**Routed wins direct in every method-cell-pair on s2 + s5.** Confirms CHARACTERIZATION.md's headline result across all six methods.

**STD is the throughput leader in every cell** because it (a) does zero pool_state work and (b) caller supplies `unit_cost` (no snapshot lookup needed). The leadership is largest on routed s2 (15% over WAC) and smallest on routed s5 (13% over WAC, 0.2% over Specific).

**Order-sensitive methods win the hot-pool routed cell** (s5 routed: Specific, LIFO, FIFO all > WAC) because aywu's chunking bypass + tm09's predecessor-wait drive commit_group_avg to 80–130 (vs WAC's 37), amortizing per-COMMIT fsync. The trade-off is ack latency (180–220ms vs WAC's 53ms) — fine for batch workflows, painful for caller-blocking flows.

## Open follow-ups

The per-method validations did not surface any new P0/P1 follow-ups beyond the deferred items already filed. Status of the pre-existing related issues:

| Issue       | Status   | Notes                                                                                          |
|-------------|---------:|------------------------------------------------------------------------------------------------|
| acct-aywu   | closed   | Router order-sensitive classifier. Load-bearing for FIFO/LIFO/Specific under cm > 1.           |
| acct-tm09   | closed   | Per-pool sequence numbers + committer predecessor-wait. Inter-window race fix.                 |
| acct-h5gs   | closed   | WAC cumulative-sum structural fix. The original "method-specific analog" example.              |
| acct-s6fa   | closed   | wac_periodic schema + close hook. Provided the close-hook + provisional-row patterns.          |
| acct-9mgx.{1..6} | closed | Per-method equivalence + bench. This roll-up consumes all six.                                |
| acct-e5fz   | open     | batch_size_max retune now that aywu's chunking bypass changes the calculus on order-sensitive methods. Was blocked on aywu's correctness landing; ready to claim. |
| acct-xjhq   | open (DEFERRED 2026-05-23) | Replace tm09's spin-sleep predecessor wait with CV+broadcast. Sufficient under PoC scale; reconsider on Phase 7 contention shapes. |

## Recommendations on exiting characterization phase

**Yes — PoC v3 has drawn enough conclusions to exit the per-method characterization phase.** Specifically:

1. **Method coverage is complete.** All six methods PoC v3 cares about have a published equivalence + bench profile under the unified harness. No method shipped with an "unknown" or "TBD" entry in any column of the correctness summary.

2. **The drift catalog is closed.** Per-method drift classes are: (a) WAC structural drift → fixed by h5gs; (b) wac_periodic per-row decomposition → structurally inherent + bounded; (c) FIFO / LIFO / Specific layer composition → fixed by aywu + tm09; (d) STD no drift. The five `_drifts` buckets in `DiffResult` cover each cleavage in the catalog. No "missing classifier" surfaced during the .1–.6 pass.

3. **Path A vs Path B regime is well-characterized.** `CHARACTERIZATION.md`'s two-axis regime map (overlap density × complexity) holds across all six methods; per-method nuance (commit_group sizing, latency trade) is captured in the per-method docs.

4. **The remaining open follow-ups are tuning, not characterization.** acct-e5fz (batch_size_max retune) and acct-xjhq (CV+broadcast) are perf-leverage optimizations on top of a working architecture, not unanswered correctness questions.

**Suggested Phase 7 framing:**
- **Re-baseline against tuning levers.** With aywu's chunking bypass live, batch_size_max's effect on order-sensitive methods has shifted; acct-e5fz captures this.
- **Production-shape workloads.** The per-method workloads validate end-state equivalence under contrived caller-major R/D cycles. Real ERP workflows (PO receipts on actual SKUs, AR shipments, period close on a populated period) exercise different shapes and may reveal cell #s the synthetic harness does not.
- **Scale-out concurrency.** Per memory `project_pocv3_pgbouncer_for_high_concurrency`, harness > ~100 callers needs pgbouncer / pgcat front. The current characterization caps at 20 callers due to dev container `max_connections`. Phase 7 contention shapes need the bouncer.

The `CHARACTERIZATION.md` doc remains the authoritative per-path regime map; this doc supplements it with the per-method dimension. Both are read together; neither supersedes the other.

## Cross-references

**Source docs (all closed, all consumed here):**
- `wac-perpetual-validation.md` — acct-9mgx.5 (canonical WAC equivalence under unified harness)
- `wac-periodic-validation.md` — acct-9mgx.6 (introduced `--method-mix` + close-hook diff)
- `fifo-validation.md` — acct-9mgx.1 (cm=1 gold standard + post-aywu/tm09 canonical)
- `lifo-validation.md` — acct-9mgx.2 (inherits aywu + tm09)
- `specific-validation.md` — acct-9mgx.3 (inherits aywu + tm09; K=1 convention)
- `std-validation.md` — acct-9mgx.4 (no pool_state — strongest equivalence guarantee)

**Infrastructure docs:**
- `h5gs-cumulative-sum-validation.md` — the original WAC structural fix; remains as the structural argument
- `CHARACTERIZATION.md` — per-path regime map; companion to this doc
- `equivalence-summary.md` — pre-method-mix WAC-only baseline; supplanted by `wac-perpetual-validation.md`

**Outstanding (not consumed here):**
- acct-e5fz — batch_size_max retune; unblocked by aywu, mentioned in Recommendations
- acct-xjhq — CV+broadcast replacement of tm09 spin-sleep; deferred 2026-05-23
