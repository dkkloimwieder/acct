# ledger-v3.2 phase-6 characterization — soak / drift / load (acct-qm7o.6)

**What this is.** The measurement half of phase 6: the `ledger-bench` harness running the whole
surviving architecture concurrently (open-loop load on the direct + staging hot paths, the
logical-decoding feed consumer, N recalc workers on continuous cadence, the G1/G2 gauge sampler),
then quiesce → conservation/oracle verify → orchestrated close → immutability probes. The
correctness *nets* shipped with phases 2–5 (10 acceptance/property binaries); this document records
what the at-scale runs measured and found. Survivor lineage: open-loop CO-free pacing +
throughput-at-SLO = acct-0at4.8; conservation sweep = acct-0at4.5 (ported to alt-C semantics);
strict-replay oracle equivalence + drift = acct-0at4.7 (the v3.1 binaries read a v3.1-shaped dump
and compare a provisional cost plane FL no longer has, so the ideas were ported, not the binaries).

**Host / noise caveat.** Bench host is the daily-driver workstation (loadavg ~1.4–1.9, a Chrome
session, and at times a foreign 100%-CPU compile during the runs). Absolute latencies and rates
move with ambient load; the structural ratios and the pass/fail correctness verdicts are the
durable results. All runs are bounded by a **hard 20 GB db+WAL footprint watchdog** that aborts
the run the moment it is crossed (see §4 for why it exists).

---

## 1. Headline soak — PASSED

90 s, 300 pools (methods cycling fifo/lifo/wac), offered 150 trx/s **per path** (direct: 16
callers, staging: 8 callers + 2 drain committers, Poisson arrivals), 4 recalc workers, `med`
volatility (receipt costs ±40% around 500 000 µ), **5% of submissions backdated** up to 120 s,
worker-pause window 25–50 s, rolled-back close probe at 35 s, feed-pause window 60–70 s.

| surface | result |
|---|---|
| direct | 13 483 acked at 149/s (offered 150), 84 wac qty-gate rejects, p50 8.1 ms / p99 20.8 ms / p999 36.7 ms, ~2 648 WAL bytes/commit |
| staging | 13 381 enqueued at 148/s, **all** 13 294 non-reject rows applied, 87 qty-gate rejects, enqueue p50 7.0 ms / p99 20.1 ms |
| recalc workers | 15 046 passes, 18 141 settlements, 17 688 GL adjustments, **0 errors** |
| quiesce | converged in **1.0 s** after load stop |
| verify (settled) | **0 failures** over 300 pools / 26 777 physical events / 8 108 settled depletions; oracle equivalence (independent reference walk) exact on every depletion; 8 pools V2-skipped as flush-wiped (§6) |
| close | unforced close **passed the gate first try** (recheck_rounds 0), swept 200 FL pools, Σ\|residue\| = 4.51 B µ; post-close `value_sum == Σ open-layer value` exact on every swept pool |
| immutability | in-period submit → SQLSTATE 55000 (PeriodClosed); next-day submit flows; re-close = `already_closed` no-op |

**The mid-run close probe** (unforced `ledger_close_period` inside a rolled-back transaction, fired
during the worker-pause window) returned exactly the designed gate-fail report: `closed: false`,
`state: closing`, **190 lagging pools**, G2b gross bound 38.6 B µ, `feed_lag_bytes` 39 728 — the
operator-visible "how big a move would a forced close emit" number, with zero mutation (advisory
locks are xact-scoped; the rollback releases them).

## 2. Two-gauge separation (G1 vs G2, recalc-c D8)

Gauge trace during the pause windows (1 Hz sampler, `bench/out/gauges.jsonl` shape):

- **Worker pause (25–50 s):** G2c dirty pins at 300/300 pools, G2b climbs monotonically to 4 596
  unsettled events / 95.0 B µ gross — while **G1 stays flat** (advance-on-ingestion: the feed keeps
  consuming, the cursor keeps moving). On resume the backlog drains within ~2 s.
- **Feed pause (60–70 s):** G1 lag climbs (max 4.97 MB, retained WAL 78.5 MB) while G2c goes
  **quiet** (no new marks arrive — exactly the failure mode a single conflated gauge would hide).
- At quiesce every gauge returns to zero.

This is the D8 two-gauge model doing its job: the two axes (ingestion health vs valuation
staleness) move independently and each pause window moves only its own axis.

## 3. Provisional-vs-authoritative drift on the live engine

The verify pass computes the drift distribution (authoritative − observed provisional, per settled
depletion) — the replay oracle's §2 variance table measured against the *live* engine rather than
an offline replay, using the receipt-cost-volatility knob the oracle flagged as missing:

| method | profile | depletions | bias (mean Δ) | mean\|Δ\| | p99\|Δ\| | rel | exact |
|---|---|---:|---:|---:|---:|---:|---:|
| fifo | med ±40% | 4 111 | −51 449 µ | 149 155 µ | 1 611 216 µ | **29.9%** | 4.4% |
| lifo | med ±40% | 3 997 | −18 188 µ | 108 245 µ | 639 474 µ | **22.0%** | 3.8% |
| fifo | trend 0.5×→1.5× | 2 662 | −63 853 µ | 137 188 µ | 1 006 478 µ | **36.0%** | 5.1% |
| lifo | trend 0.5×→1.5× | 2 629 | **+94 893 µ** | 131 288 µ | 624 409 µ | **27.6%** | 5.6% |

Reading: under ±40% cost volatility with 5% backdating, the provisional plane is ~22–30% wrong per
depletion on average until recalc trues it up — same order as the offline oracle's med-profile
findings (14–18%), amplified here by backdate-driven re-costing and negative-inventory episodes
(uncovered depletions observed at clamp-0 cost swing the tail; p99 exceeds the base cost). The
trend profile (monotone rising cost, 10% backdating; 60 s at 150 trx/s per path against the fixed
engine — the same configuration that wedged on the §5 defect, now 15 965 worker passes / 18 249
settlements with zero errors and a clean unforced close) exposes the directional asymmetry the
symmetric profiles hide: **lifo bias flips positive** (+94 893 µ — authoritative draws come from the
newest, most expensive layers while the observed provisional lags at the running average) while
fifo's stays negative and grows (old cheap layers anchor the authoritative below the provisional).
A consumer netting the two methods' provisional GL against each other would see the drift cancel;
per-method it is systematic, which is exactly why the D8 gauges are per-pool, not global.

## 4. The adjustment storm is real: write amplification filled a disk

The first full-scale attempt (240 s at 400 trx/s per path, backdate window **3600 s**) filled ~70 GB
of disk in minutes and took the dev cluster to ENOSPC (`pg_logical/snapshots/...: No space left on
device`), wedging the feed and workers in error-retry loops. Mechanism: **every backdated event
forces a re-cost of the settled tail behind it (R-2), and every re-cost appends a new generation of
`cost_settlement` + `cost_layer_consumption` rows for every affected depletion.** With hot-pool
streams thousands of events deep and a 1-hour backdate window, per-backdate work grows with the
tail length — the quadratic adjustment storm design-v3.2 §5/§19 warned about, now measured the hard
way. Even the passing 90 s headline run shows the amplification: 26 777 physical events produced
18 141 settlement generations + 17 688 GL adjustment rows (≈ 2.2 settlement writes per FL depletion,
growing with backdate density and window).

Consequences baked into the harness:

- **Hard 20 GB footprint cap** (`pg_database_size + pg_wal`, polled every 2 s) aborts any run that
  crosses it, and persistent feed/worker errors (10–20 consecutive) abort immediately instead of
  hanging until a quiesce timeout.
- Bounded knob defaults: backdate window 120 s for characterization runs.
- **Feed for acct-qm7o.7 (backpressure):** cadence alone does not bound the storm; the
  write-amplification rate (settlement rows per physical event) is the natural backpressure input,
  and slot-lag/ENOSPC is what "quiet backlog" escalates to when nothing bounds it.

## 5. Engine defect found: `cost_layer_consumption_pkey` duplicate → wedged pool

The trend-profile soak (10% backdate) aborted on a real phase-4 engine bug the property nets'
small cases never reached:

```
ERROR: duplicate key value violates unique constraint "cost_layer_consumption_pkey"
DETAIL: Key (depletion_trx_line_id, layer_trx_line_id, recalc_generation)=(9024, 5767, 33) already exists.
```

Two shapes in the evidence: a **permanent wedge** — the same key failing identically across every
worker that claimed the pool — and a **transient single-shot** that resolved on retry.

**Root cause (two independent code walks, adversarially cross-verified; both defects required):**

1. *Stale claim via EvalPlanQual.* Both claim paths (`claim_next` SKIP LOCKED and `claim_pool`
   blocking) read `pool_settlement.recalc_generation` / `settled_through_*` / `recost_floor_*`
   through an **unlocked LEFT JOIN inside the statement whose `FOR UPDATE OF rq` locks only the
   `recalc_queue` row**. Under READ COMMITTED, when a sibling pass commits (generation bump +
   requeue re-stamp of the queue row) between the claim's snapshot and its lock acquisition, the
   EPQ recheck locks the *new* queue tuple but re-evaluates the join with the *original-snapshot*
   settlement tuple — the claim carries the pre-commit generation and a stale (still-set) floor,
   while every post-claim statement sees fresh state.
2. *Non-monotonic settle.* `settle()` writes `recalc_generation = EXCLUDED.recalc_generation`
   unconditionally. The stale-claimed pass full-replays (stale floor), deterministically re-derives
   costs identical to the committed generation-33 rows, so it writes nothing (D6 no-op) — and then
   **commits the generation regression 33 → 32**. Committed gen-33 consumption rows now sit above
   the pool's recorded generation; the next genuine re-cost computes 32+1 = 33 and collides. The
   abort rolls the pass back (generation stays 32, floor stays set, queue row survives), so every
   worker repeats the identical replay and identical collision: a permanently wedged pool. The
   transient case is the same stale claim whose pass derived an overlapping write set directly.

**Fix (shipped as `acct-qm7o.8`):** split lock-then-read in both claim paths — lock the queue row
in its own statement, then read pool + settlement state in a second post-lock statement (fresh
snapshot; every writer of `recalc_generation` commits under this same queue-row lock) — plus a
defense-in-depth `GREATEST(pool_settlement.recalc_generation, EXCLUDED.recalc_generation)`
monotonicity guard in `settle()`, and a retry loop in the blocking `claim_pool` (a granted lock on
a deleted queue row re-ensures and retries instead of erroring). No schema change, no fold change;
D6 (no-op passes write nothing and do not bump) is preserved exactly. The deterministic
two-session regression test (`acceptance_recalc_stale_claim.rs`) uses the blocking `claim_pool`
path as the rendezvous — hold a worker pass open in one session, park `ledger_settle_pool` on the
queue-row lock in another, commit the first, and assert the generation did not regress — and went
**red on the pre-fix build on exactly the predicted mechanisms** (generation regressed to the
stale claim's value; blocking path raised `UnknownPool` on the vanished queue row) before going
green on the fix. The property net additionally gained an R8 generation-monotonicity invariant
sampled after every op and drain round.

The soak's fail-fast (consecutive worker-error trip) surfaced the wedge in seconds; without it the
run would have hung to timeout with four workers spinning on the poisoned pool. Neither the 400
random property cases nor the 12-case acceptance net could reach this — it needs two live sessions
racing the claim, which is precisely the soak's contribution.

## 6. Verify subtlety: the exact-empty flush wipes engine corrections

At-scale finding the per-case property nets could not reach: the FL hot path's exact-empty flush
(`qty − Δ = 0` ⇒ `value_sum := 0`) **discards whatever aggregate value corrections the recalc
engine had folded in before it**. The offline identity `value_sum == commit-order fold − settlement
deltas − swept residue` is therefore not reconstructible for a flushed pool with settlements (the
wiped amount depends on the run-time interleaving of engine passes and hot-path commits, which the
stream does not record). The drift is bounded, surfaces in the close sweep's residue GL, and the
post-close `value_sum == Σ open-layer value` check covers those pools exactly. The verify
skips-and-counts them (`v2_skipped_flush_wiped`; 8 of 300 pools in the headline run).

Addendum (acct-qm7o.9): the same phenomenon reached the per-case property net at ~1-in-850 random
cases (`property_recalc_engine` R3, then formulated as commit-order fold − telescoped deltas — a
deterministic repro pins it in `acceptance_recalc_engine::flush_wiped_correction_reconciles_in_apply_order`).
The property harness is sequential, so apply order IS recorded: trx_line ids interleave hot-path
commits and engine adjustment lines exactly as `value_sum` experienced them. R3 now replays ALL
lines in id order — physical lines through the FL formulas, each `cost_adjustment_line` through its
settlement's `(authoritative − prior) × qty` reconcile — and stays exact through flush wipes. The
V2 skip remains a concurrency concession (the soak's interleaving is unrecorded), not a semantic
one.

## 7. Load / throughput-at-SLO (structural)

Open-loop rungs, 12 s each, hot path only (no feed/workers), 300 pools, med profile, no
backdating. CO-free latency (measured from intended send-time). Noisy-host caveat applies; read
the *shape*, not the absolutes.

| rung (offered) | direct achieved | direct p50/p99/p999 (µs) | staging achieved (enqueue) | staging p50/p99/p999 (µs) |
|---:|---:|---|---:|---|
| 200/s | 174 | 5 419 / 11 100 / 14 401 | 190 | 7 921 / 34 799 / 56 197 |
| 400/s | 381 | 6 361 / 16 670 / 23 740 | 393 | 9 953 / 250 871 / 378 011 |
| 800/s | 790 | 7 217 / 47 349 / 110 166 | 448 | 1.9 s / 5.5 s / 6.0 s |
| 1600/s | 1 582 | 12 165 / 258 605 / 362 020 | 353 | 5.8 s / 9.4 s / 9.7 s |

Structural readings:

- **Direct keeps up with offered load through 1600/s** (achieved 99%) with p99 rising
  11 ms → 259 ms — latency degrades before goodput does.
- **Staging saturates ≈450/s end-to-end at this committer config** (2 drainers × batch 25): past
  that, the inbox backlog grows without bound, and enqueue-ack latency collapses into the seconds
  as the backlog's drain transactions and the callers contend. Committer count/batch is the scale
  knob (design-v3.2 §8's sharded-committer path).
- **Drain-batch convoy (found during smoke):** a drain transaction holds every touched pool's
  aggregate tuple lock until commit; with 200-row batches over a Pareto-hot universe a concurrent
  direct submitter stalled for the whole batch chain (p99 56 s!). Batch 25 bounds the convoy
  (headline-soak direct p99: 20.8 ms with both paths + workers live). Same-pool batching is the
  useful direction (the v3.1 routed lesson), not bigger mixed batches.

## 8. Backpressure engage/release under a stalled engine (acct-qm7o.7)

The recalc-c §5 lever, demonstrated live against the shipped defaults (bound 200 unsettled events
per pool, low-water 20, `recalc_backpressure_config`). Two runs, same trend/backdate shape as §3:

- **Stays off (60 s, 150/s per path, workers live):** max per-pool backlog **6** vs the bound of
  200 (33× headroom), zero pools throttled, zero 53400 rejects on either path, PASSED — and the
  drift table reproduces §3 within noise, so the lever's steady-state presence (one empty-index
  probe per admission) changes nothing.
- **Engage/release (60 s, 300/s per path, workers paused 10 s → 45 s):** during the stall the
  Pareto-hot pools accumulate ~8 events/s each; **40 pools engaged, max per-pool backlog 203** —
  the bound plus one feed batch's overshoot, then *frozen*: admission rejects (SQLSTATE 53400)
  stopped the growth, which is the lever doing its one job (an unthrottled hot pool would have
  reached ~280 by resume). 1 511 direct + 1 518 staging rejects were classified, not errors.
  Cold pools kept flowing under their bounds (per-pool granularity: 40 of 300 throttled at peak).
  On resume the workers drained the capped tails; quiesce converged in 3.0 s, the unforced close
  passed, and the post-close in-period probe returned 55000 (PeriodClosed) — a stuck throttle
  would have surfaced there as 53400. PASSED, 0 worker errors.

The deterministic §10 assertions (engages exactly at the bound — on either writer, releases at the
low-water mark, hysteresis retention inside the band, admission atomicity, adjustment-loopback
exclusion, and the late-delivery healing interleaving) live in `acceptance_recalc_backpressure` +
`property_backpressure`; these runs are the at-scale confirmation. Reproduce: the §3 command with
`--rate 300 --pause-workers 10:45`.

Scope note: the bound is the **un-costed tail** (events above the settlement frontier — the
ratified G2a-shape metric). A backdated/backfill flood is a different axis: those events settle
promptly (each pass drains to head) while invalidating already-settled costs behind them, so they
never accumulate in this counter — their cost is the §4 re-cost write amplification, bounded today
by cadence and the close gate, not by this lever. If that axis ever needs its own throttle, §4's
settlement-rows-per-physical-event rate is the natural input; that remains future work.

## Bottom line

- **Correctness at scale: green.** With everything running concurrently — two hot paths, feed,
  four workers, backdating, pause windows, a mid-run close probe — the settled state matches the
  independent strict oracle exactly, conservation holds, the unforced close passes its gate on the
  first re-check, the sweep trues every aggregate to its layer value, and the closed period is
  immutable. Quiesce converges in ~1 s.
- **The two-gauge model works** and the close report's per-pool lag + G2b gross bound give the
  operator the forced-close-move number the design promised.
- **Two real engine findings** the small-case nets couldn't reach: the adjustment-storm write
  amplification (fed the acct-qm7o.7 backpressure lever — shipped, demonstrated in §8) and the
  `cost_layer_consumption_pkey` generation-collision wedge (fixed as `acct-qm7o.8` with a
  deterministic two-session regression test; the trend soak that originally wedged now passes
  clean — §5, §3).
- **Structural load shape:** direct sustains offered load 3–4× beyond the 2-committer staging
  config; batching across hot pools is a lock convoy, not a throughput win.

Reproduce: `scripts/soak.sh` (env knobs documented in-file; `SOAK_PROFILE`, `SOAK_BACKDATE_PCT`,
`SOAK_PAUSE_WORKERS`, `SOAK_MIDRUN_CLOSE_AT`, …) and `scripts/slo-sweep.sh` (`SWEEP_PATH`,
`SWEEP_RUNGS`, `SWEEP_SLO_P99_US`). Both are wrappers over `cargo run --release -p ledger-bench`.
