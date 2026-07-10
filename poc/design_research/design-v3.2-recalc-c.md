# design-v3.2 recalc engine (c): cadence-vs-load control + quiet-backlog mitigation

> **Status: DESIGN (acct-q1oj.3, 2026-07-10).** The control-layer child of the v3.2 recalc/close workstream
> (acct-q1oj / `design-v3.2.md` §5) — the **load-bearing §19 risk**. Designs the levers that keep recalc from
> silently falling behind and the signals that make "behind" visible. Consumes the per-pool fold cost model
> and checkpoint/incremental scheduler from (b) (`design-v3.2-recalc-b.md`) and the settlement bookkeeping
> from (d) (`design-v3.2-recalc-d.md`). **Ratifies D8** (cursor-advance policy + gauge model — the one place
> the recalc design refines decided §17) and **resolves D11** (historical checkpoint retention). Design-only.

## 1. The risk, restated as a control problem

Under alt C (§16) recalc is the sole costing engine, doing strictly more work per line than the hot path,
**sequential per pool** (b §3), **concurrent with appends**. §19's quiet-backlog chain is the failure:
consumer falls behind → no authoritative cost/GL for the tail → mid-period drift compounds (biased under a
trend, oracle §2b) → close blocks → forced close emits one large one-signed valuation move. The failure is
*silent*, which is what makes it dangerous. This child's job is (i) the levers that bound the backlog and
(ii) the signals that surface it before close.

## 2. Cadence changes distribution-in-time, not total work — with one wrinkle (b)

The §16/§19 invariant: cadence (the recalc-period knob) does **not** change total work — the same event
volume must be costed regardless. It trades **peak backlog depth + drift latency** against **per-run
fixed-cost overhead**. But (b) sharpened this:

- **Forward-pass work is now incremental** (b's D2): a forward pass costs O(new events since `settled_through`),
  not O(pool depth). So the fixed-cost-per-run of a *continuous* cadence is small (per-pass tx + index
  range-scan setup over few new events), and the generation-delta model (d) means quiet pools post **nothing**
  (no-op passes don't bump the generation — d's D6, confirmed here). Continuous drain is therefore *cheap*, not
  the fixed-cost-heavy option §16 framed it as before the incremental scheduler existed.
- **Backdated re-cost work is NOT cadence-invariant** the same way. A longer period leaves more already-costed
  depletions in a pool, so a backdated event's re-cost **blast radius** (the events from its floor forward) is
  larger. Cadence thus also trades backdated-re-cost blast radius — a second-order term §19 didn't separate,
  surfaced by (b)'s checkpoint model.

## 3. Lever 1 — cadence: default continuous, force-sweep at close

With (b)'s claim-driven workers draining a dirty-set continuously, "cadence" is a spectrum, not a fixed period:

- **Continuous (period → 0):** workers drain the dirty-set as fast as it fills. Steady load, shallowest
  backlog, smallest drift latency. Cheap under (b)+(d) (incremental passes, no-op passes free). "Real-time
  FIFO" is this limit (§16) — the small-period end of the single provisional plane, not a second strict plane.
- **Periodic / at-close (period = nightly / month-end):** the dirty-set accumulates; a scheduled sweep drains
  it. Deep backlog, one big move, largest drift latency — the classic periodic-FIFO shape.

> **Recommendation: default to CONTINUOUS drain, with a forced full sweep + settle-through at each accounting
> boundary as a close backstop (child (e)).** Because (b) made forward passes incremental and (d) made no-op
> passes free, the historical reason to batch (fixed-cost repetition) is largely gone; continuous drain
> minimizes both backlog depth and drift latency at little extra cost. The §16 "period" abstraction then maps
> to **"force everything settled at these boundaries"** (a close gate, child (e)), not to gating forward
> passes. The knob that remains is operational — **worker count** (b's D10) and poll eagerness — governing how
> aggressively the dirty-set is drained, not a strict period.

Per-pool cadence is unnecessary: claim-driven draining (b) self-adjusts — dirty pools get claimed, cold pools
are never touched — so a global worker pool draining continuously already gives hot pools more attention
without a per-pool period knob.

## 4. Lever 2 — per-pool parallelism ceiling (from b) forces per-pool detection

(b §3) established parallelism is **across pools only**; a Pareto-hot / deep pool sets the close-drain floor.
The measured strict-curve (b §2) makes this concrete: a depth-50 000 pool folds at ~25 events/s, so even at
continuous drain with N workers its per-pool backlog can grow unboundedly relative to its own append rate, and
**no cadence and no added worker fixes it** (the fold is serial within the pool). This is the residual #5
ceiling.

The control consequence: **detection must be per-pool, not just global.** A global slot-lag or aggregate
backlog gauge can read healthy while one hot pool's `settled_through` lags catastrophically. So the
close-drain floor = `max` over pools of (pool's un-settled event count ÷ its fold rate), and the primary
valuation-staleness signal is **per-pool `settled_through` lag** (from d's `pool_settlement`), with the `max`
being what gates close (child (e)).

## 5. Lever 3 — backpressure: the deliberate, lazily-engaged re-coupling

When backlog exceeds a bound, throttle the hot path. The alt-C insert-only hot path absorbs backpressure
gracefully (it is just `INSERT`s), so this is a clean escape valve against unbounded quiet growth — **but it
reintroduces a hot-path→recalc coupling alt C deliberately severed (§16), so it is the last resort, not the
steady state.**

- **Granularity: per-pool, not global.** A hot pool's recalc lag should throttle appends **to that pool**, not
  to cold pools. But per-pool backpressure requires the hot path to consult the pool's backlog before
  appending — a synchronous per-pool read alt C removed (§16). Resolution: **lazy engagement.** A cheap global
  "backpressure active" flag gates the check; in the common case (no pool over its bound) the flag is off and
  the hot path stays read-free (alt-C's property preserved). Only when some pool breaches its bound does the
  flag flip and the hot path consult a small shared set of throttled `pool_id`s. The synchronous read is a
  rare-path cost, not the steady-state.
- **Bound:** tied to the gauges (§6/§7) — a per-pool `settled_through` lag ceiling and/or a dirty-set-depth
  ceiling. Crossing it throttles appends to the offending pool until its backlog drains below a low-water mark.

Backpressure caps unbounded growth; it does not make a hot pool fold faster (nothing does, §4). Its role is to
stop the physical (qty) plane from running arbitrarily far ahead of authoritative cost when a pool is
genuinely overwhelmed.

## 6. D8 ratified — cursor-advance policy and the two-gauge refinement

(b) surfaced D8 and recommended option B; this child **ratifies it**, because (c) owns the detection signal
and D8 *is* the shape of that signal.

> **D8 resolved: advance the §17 slot cursor ON INGESTION into a durable dirty-set (option B), not on recalc
> completion (option A).** As the slot delivers `trx_line`s in commit order, the feed consumer records them
> into `recalc_queue` / updates `recost_floor` (d) and advances `confirmed_flush_lsn` to that batch — durably,
> before the fold runs. Workers drain the dirty-set independently (b §4).

Reasoning: (i) the alt-C insert-only hot path can run far ahead of a deep-pool fold (§4); (ii) option A pins
**all cluster WAL** to the single slowest pool's fold (§17 flagged slot-lag pins WAL but did not cost it
against the per-pool ceiling) — a disk-exhaustion outage mode; (iii) the durable dirty-set is cheap and *is*
the crash-recovery boundary (a crash re-drains the dirty-set, not the slot).

**Consequence — §17's "single gauge" refines into two coupled gauges.** §17 said "slot-lag *is* recalc-backlog
— monitor `confirmed_flush_lsn` lag as the single backlog gauge," an operational corollary made before the
scheduler existed and resting on option A. Under option B:

- **G1 — cursor / ingestion lag** (`confirmed_flush_lsn` vs current LSN). Advances at *ingestion* speed, so it
  stays low unless the **feed consumer** (dirty-set writer) stalls — a rarer, distinct failure from a slow
  fold, remediated on the feed/WAL axis.
- **G2 — recalc backlog** (per-pool `settled_through` lag, un-costed-tail count/value, dirty-set depth).
  Measures *valuation staleness*, remediated on the recalc-throughput / backpressure axis.

The §17 unified-alarm intuition survives *in spirit* — G1 and G2 are **coupled**: a sustained recalc lag
eventually backs up the dirty-set, and backpressure (§5) re-couples ingestion to G2, so an unbounded G2 will
in the limit surface on G1. But they are distinct signals with distinct failure modes and distinct fixes, and
conflating them (option A) buys a single number at the price of a global-WAL outage mode.

> **This is the one place the recalc design touches a decided section (§17).** It does **not** reopen §17's
> *feed* decision (logical-decoding slot vs watermark-scan — untouched and correct); it specifies the
> cursor-advance policy §17 left implicit and refines its single-gauge operational corollary. Recorded here in
> the v3.2 design; a one-line forward-pointer is added to design-v3.1 §17 so the cross-reference stays
> honest, without rewriting the frozen PoC decision record. **Flagged for review** — if the reviewer prefers
> option A's single-gauge simplicity over WAL-safety, D8 flips and this section reverts.

## 7. Detection signals (consolidated)

| gauge | what it measures | source | remediation axis |
|-------|------------------|--------|------------------|
| **G1** cursor / ingestion lag | feed-consumer health, WAL pressure | slot `confirmed_flush_lsn` vs current LSN | feed / WAL |
| **G2a** per-pool `settled_through` lag; `max` = close-drain floor | valuation staleness, the hot-pool ceiling (§4) | d `pool_settlement` | recalc throughput / backpressure |
| **G2b** un-costed-tail count & value behind `settled_through` | drift *magnitude* bound | count/Σ over uncosted `trx_line` | recalc / close gating |
| **G2c** dirty-set depth | aggregate backlog | `recalc_queue` size | worker count / backpressure |
| **G3** conservation sweep over flagged negatives | how far qty ran ahead of cost | acct-0at4.5 sweep | correctness guard (alt-C allow-negative) |

**Drift bound (the business signal):** G2b × the oracle's per-depletion rel% (directional under a trend,
oracle §2b) = the mid-period misstatement magnitude — the number a period-end reader is exposed to until
recalc trues it up, and the size of the move a forced close would emit.

## 8. The quiet-backlog chain and where each lever cuts it

| §19 chain link | cut by |
|----------------|--------|
| slot lag pins WAL | **D8/option B** — WAL freed at ingestion (§6) |
| no authoritative cost for the tail accumulates | **continuous drain** (§3) minimizes tail; **G2** makes it visible |
| a hot pool's lag hides in a global gauge | **per-pool G2a** (§4) |
| drift compounds (biased under trend) | **G2b drift bound** (§7) surfaces magnitude before it's a surprise |
| unbounded quiet growth | **lazy per-pool backpressure** (§5) as last resort |
| close blocks / forced close emits one big move | **close gating on G2a** (child (e)) — refuse close until settled, or force with the alarm + the sized move |

## 9. D11 resolved — historical checkpoint retention

(b) deferred D11 (how many historical layer-state checkpoints to retain so a deep-history backdated event
avoids full-opening replay) to this child.

> **D11 resolved: no historical checkpoints in v3.2 scope — one live checkpoint (b's default).** A backdated
> event *before* `settled_through` triggers full-opening replay (a's O(depth) correctness baseline), priced by
> G2 and exercised by the soak (§10). Rationale: retaining historical checkpoints keyed to prior accounting
> boundaries only pays off for **backdating into an already-closed period**, which requires a period-reopen
> workflow that is **out of scope (§13, no period-close-reopen mechanics in v3.2)**. So within v3.2's scope the
> historical-checkpoint benefit is unreachable; adding the storage + invalidation machinery now would be
> speculative. Revisit when/if period reopen enters scope. Correctness is unaffected either way — full-opening
> replay is always correct, just O(depth).

## 10. Cadence-vs-load soak (hands to testing strategy, design-v3.2 §7)

Reuse the architecture-agnostic survivors: acct-0at4.8 open-loop load generator + acct-0at4.5 conservation
sweep. Add the two things the oracle flagged missing (oracle §2a/§2c): a **receipt-cost-volatility knob**
(varied costs comparable to pool value — without it the fixture measures zero cost variance) and a
**backdated-event injector** (to exercise R-2 re-cost blast radius, §2). Drive continuous vs periodic cadence
across append rates × Pareto-hotness and assert:

- continuous drain keeps G2a bounded **except** on genuinely hot/deep pools (where the §4 ceiling is expected
  and must be *visible*, not hidden);
- backpressure (§5) engages exactly at the bound and releases at the low-water mark, staying off otherwise
  (the common-path read-free property, §5);
- G1 stays low under option B even when G2 is high (the decoupling claim, §6);
- a forced close emits the G2b-sized move and fires the alarm (child (e) gate).

## 11. Interfaces and open items

- **← (a)/(b)/(d)** — consumes the per-pool fold cost (b §2), the incremental/checkpoint scheduler (b), and the
  `pool_settlement` / dirty-set gauges (d). Confirms **d's D6** (no-op passes don't bump the generation — what
  makes continuous cadence free).
- **→ (e) acct-q1oj.5** — close gating reads **G2a**: a pool is close-ready iff `recost_floor IS NULL` and
  `settled_through` is at the stream head; a forced close emits the accumulated authoritative move and fires
  the G2b-sized alarm. The variance-into-empty-pool sweep (d §6) is (e)'s.
- **D8** *(ratified: option B)* — + the design-v3.1 §17 forward-pointer; flagged for review.
- **D11** *(resolved: one live checkpoint, no historical retention in v3.2 scope)*.
- **D10** — worker count / poll eagerness is the residual operational cadence knob (start = 4, b); tuning is a
  soak-output, not a design-time constant.
