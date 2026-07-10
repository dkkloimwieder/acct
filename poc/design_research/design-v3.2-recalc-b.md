# design-v3.2 recalc engine (b): cross-pool scheduler + parallelization + materialization

> **Status: DESIGN (acct-q1oj.2, 2026-07-10).** The scheduler-and-materialization child of the v3.2
> recalc/close workstream (acct-q1oj / `design-v3.2.md` §5). Takes child (a)'s per-pool replay unit
> (`design-v3.2-recalc-a.md`) and child (d)'s state schema (`design-v3.2-recalc-d.md`) and decides **how the
> replay runs across the whole pool population, concurrent with an appending hot path**, plus **what recalc
> materializes**. Resolves the checkpoint seams (a) deferred here — **D2** (full-replay vs incremental) and
> **D4** (re-cost-floor granularity). **Design-only.**

## 1. What this child owns

- Cross-pool work distribution: how dirty pools are assigned to recalc workers, and how a hot pool's long
  fold does not starve the rest.
- The moving-tail catch-up loop and its interaction with the §17 logical-decoding cursor.
- Materialization strategy — what recalc writes back, and (the payoff) how persisting it turns full-replay into
  an incremental steady state.
- Access-path requirements at scale.

## 2. Recalc's per-pool cost, grounded in the PoC

Recalc **is** the strict FIFO/LIFO layer-walk that child (a) moved off the hot path — so the PoC already
measured its per-pool cost. The strict Path A depth curve (POC-REPORT §A) is the recalc fold cost:

| depth | strict FIFO per-call | throughput |
|------:|---------------------:|-----------:|
| 1     | 434 µs   | 2127 trx/s |
| 1000  | 1010 µs  | 914 trx/s  |
| 50000 | 39219 µs | 25 trx/s   |

`≈ 460 µs + 0.79 µs × depth`, O(depth·log depth) per walk, throughput collapsing **85× (2127 → 25 trx/s)**
purely from depth. A full-pool replay (a's D2(i)) pays this over the pool's *entire* stream each pass. That
collapse is the throughput inequality (§19) made concrete, and it is the whole reason this child exists: at
depth 50000 a single pool folds at 25 events/s — no cross-pool scheduling helps *within* that pool.

## 3. The per-pool serial fold is intrinsic — and is NOT the killed hot-path affinity lever

A depletion's authoritative cost depends on the layer state left by **every prior event in the same pool** — an
intrinsic serial fold that cannot be parallelized *within* a pool. Therefore **exactly one worker owns a
pool's replay at a time**: two workers folding the same pool concurrently would race its layer state and
generation counter (d). Per-pool single-ownership is a **correctness requirement** for recalc.

> **Do not cite the acct-m4g5 affinity kill against this.** That kill found committer→pool affinity bought
> ~nothing on the *hot path* (B/A = 1.26×; the limiter was the shared staging-ring LWLock, not the row-lock
> affinity targets) — **because the alt-C hot path is insert-only / commutative and has no per-pool serial
> dependency**. Recalc is the exact opposite: its per-pool dependency is real and load-bearing, so per-pool
> ownership is intrinsic here, not an optional throughput lever. The m4g5 finding is scoped to the hot path
> and does not transfer.

Parallelism is therefore **across pools only** (pools are independent); wall-clock is bounded by the slowest
single pool's fold (§19). Under Pareto-hot pools a handful of deep/hot pools dominate — the #5 ceiling the
gate removed from the hot path (§18) reappears here, relieved of a latency SLA but setting the close-drain
floor. No scheduler removes that; (b) only ensures the *other* pools are not blocked behind it.

## 4. Cross-pool distribution — claim-driven `SKIP LOCKED`, not static shard

The scheduler's unit of work is a **dirty pool**: one whose stream advanced past `pool_settlement.settled_
through_*` or whose `recost_floor_*` is non-NULL (d). Two ways to hand pools to workers:

- **(i) Static hash shard** — `hash(pool_id) % N` workers (Kafka-partition / alt-E shape §20.1). No
  coordination, but a hot pool pins its shard's worker while others idle — poor balance under Pareto.
- **(ii) Claim-driven dynamic** — a dirty-set drained by `SELECT … FOR UPDATE SKIP LOCKED` (the **exact
  SPIKE-A pattern** the gate validated for the hot-path outbox). An idle worker claims the next dirty pool;
  the row lock gives per-pool exclusivity (§3) for free; hot pools don't starve others (a free worker just
  claims a different dirty pool).

> **D9 resolved: claim-driven `SKIP LOCKED` over a dirty-set.** It reuses the gate-blessed SPIKE-A drain,
> delivers the intrinsic per-pool single-ownership (§3) as a side effect of the claim's row lock, and
> load-balances under Pareto (workers pull the next dirty pool rather than being pinned to a hot shard). A
> thin `recalc_queue(pool_id PRIMARY KEY, recost_floor_posted_at, enqueued_at)` dirty-set — populated by the
> feed consumer (§5), drained by workers `... FOR UPDATE SKIP LOCKED LIMIT 1` — is the concrete form; it can
> collapse into a partial index on `pool_settlement` (WHERE dirty) if a separate table proves redundant.

**Worker model.** N recalc workers (mirror the committer_count = 4 posture; tunable — **D10**). Each worker
loops: claim next dirty pool → replay (incremental or full, §6) → write materialization + adjustments + bump
`pool_settlement` in **one transaction** (d's single-tx generation bump) → clear the dirty flag / advance
`settled_through_*` → release. Cross-pool parallelism = N workers on N distinct dirty pools; per-pool
exclusivity = the claim's row lock.

## 5. The moving tail and the §17 cursor — a real fork

Recalc chases a moving tail: the hot path keeps appending while recalc runs. The feed consumer reads the §17
slot in commit order and, per delivered `trx_line`, marks its pool dirty (enqueue + update `recost_floor` if
the event is backdated). Workers (§4) drain the dirty-set independently. The open question is **when the slot
cursor (`confirmed_flush_lsn`) advances**, because §17 tied slot-lag to recalc-backlog:

- **(A) advance-on-completion** — the cursor advances only past LSNs whose pools recalc has fully re-costed.
  This is §17 **literally**: slot-lag *is* recalc-backlog, one gauge, and a lagging pool pinning WAL is the
  *intended* unified alarm. **Cost:** one deep/hot pool (§2, 25 events/s at depth 50k) holds the cursor at its
  oldest unprocessed LSN for the *whole cluster*, pinning WAL globally even when 9 999 other pools are caught
  up — a disk-exhaustion hazard §17 flagged but did not fully cost.
- **(B) advance-on-ingestion + durable dirty-set** — the cursor advances as soon as delivered `trx_line`s are
  durably recorded into `recalc_queue` / `recost_floor` (d). WAL is freed at ingestion speed, decoupled from
  the slowest pool's fold. **Cost:** recalc backlog is now a *separate* gauge (dirty-set depth /
  `settled_through` lag), so §17's "single gauge" refines into **two coupled gauges** — cursor/ingestion lag
  (WAL pressure) and `settled_through` lag (valuation staleness).

> **D8 — surfaced, not silently chosen; handed to child (c) with the §17 interaction explicit.** (A) honors
> the decided §17 operational claim as written; (B) is better engineering (a single deep pool must not pin
> all cluster WAL) but *refines* §17's single-gauge model. Child (c) owns the backlog/detection signal, so it
> ratifies D8 — and if (B) is chosen, (c) carries the small §17 operational-note update (single gauge →
> ingestion-lag + settled-through-lag). **Recommendation: (B)**, because the alt-C insert-only hot path can
> run far ahead of a deep-pool fold and pinning all WAL to the slowest pool is a real outage mode; the
> durable dirty-set is cheap and the two-gauge refinement is honest about what's actually being measured.
> This is not reopening §17's *feed* decision (slot, not watermark-scan — untouched); it is specifying the
> cursor-advance policy §17 left implicit.

## 6. Materialization = the checkpoint (resolves D2 and D4)

**What a pass writes back** (per pool, per generation N — see (d)): reconciled aggregate (`pool_state`
`layer_id = 0`), the open **layer rows** (`pool_state.layer_id > 0` — the persistent form of (a)'s VecDeque;
already unconstrained by the 0006/0009 CHECKs, (d) §1), `cost_settlement` + `cost_layer_consumption`, and the
`cost_adjustment` trx/line/posting_line for the GL delta.

The load-bearing insight: **the persisted layer state at the `settled_through` frontier IS a checkpoint.**
That collapses (a)'s deferred D2/D4 into the materialization decision:

- **Normal forward pass (no backdated event).** The pool's persisted `layer_id > 0` rows already encode the
  layer sequence as of `settled_through`. A forward pass replays only the events with `(posted_at, id) >
  settled_through` (a range scan on (d)'s `(pool_id, posted_at, id)` index) against that persisted state —
  **incremental, O(new events)**, not O(pool depth). This is the steady state and it sidesteps the §2 depth
  collapse entirely for pools that only ever move forward.
- **Backdated pass (`recost_floor < settled_through`).** Replay must restart at or before the floor. If a
  persisted checkpoint `≤ floor` exists, replay from it; the live persisted state is a checkpoint *at
  settled_through* (too new for a floor below it), so a backdated event below `settled_through` falls back to
  **full replay from opening** (a's D2(i) correctness baseline) unless *historical* checkpoints are retained.

> **D2 resolved:** incremental-from-persisted-checkpoint is the **steady state** (adopted now, not deferred —
> it falls out for free because layer rows are persisted for queryability anyway); full-replay-from-opening
> (a's baseline) is retained as the **correctness fallback** for a backdated event with no checkpoint at or
> below its floor. This *refines* (a)'s "full-replay first, checkpoint later" — persisting the layer state
> makes the incremental path free, so it is worth having from the first cut.
>
> **D4 resolved:** the re-cost floor replays from the **nearest persisted checkpoint ≤ floor**. Start with a
> **single live checkpoint** = the current persisted layer state at `settled_through` (covers all
> forward-only pools and any backdated event at/after `settled_through` — the common case). **Historical
> multi-checkpoint retention** (older snapshots so a deep-history backdated event avoids full-opening replay)
> is the one genuinely deferred piece — a storage-vs-recost-depth tradeoff (**D11**) to add only if
> deep-backdated re-cost is measured to matter. The full-opening replay is always correct, just O(depth), so
> deferring historical checkpoints trades performance, never correctness.

**Materialization posture chosen: persist layers durably** (not reconstruct-transiently). Reasons: (i) it *is*
the checkpoint that enables the incremental steady state above; (ii) it lets queries read authoritative layer
state between passes (the alt-C mid-period GL is qty-only/standard-valued, §16 — persisted layers are where an
authoritative-valuation query lands); (iii) full-replay rewrites them wholesale but a forward pass only appends
new layers / decrements consumed ones, so steady-state churn is O(new events), not O(depth).

## 7. Access paths at scale

- **Ordered walk:** (d)'s `(pool_id, posted_at, id)` index (D1) — a forward pass is an index range scan
  `> settled_through`; a full/backdated replay is an index scan from the checkpoint/opening.
- **Dirty-pool claim:** `recalc_queue` PK on `pool_id` drained by `FOR UPDATE SKIP LOCKED LIMIT 1`, or a
  partial index on `pool_settlement` WHERE dirty. Either gives O(1) "next dirty pool."
- **Materialization writes:** `cost_settlement` / `cost_layer_consumption` are append per pass (batch INSERT);
  `pool_state` layer rows are upserted (append new, decrement/delete consumed).

## 8. Correctness / validation

- **Checkpoint equivalence (the load-bearing new gate):** for any pool, an incremental pass from the persisted
  checkpoint MUST produce byte-identical authoritative costs to a full-replay-from-opening — which (a) already
  pins to the acct-0at4.7 oracle TRUE arm. Test both against the oracle and against each other.
- **Backdated-from-checkpoint == from-opening (D4):** a backdated re-cost replayed from a checkpoint ≤ floor
  equals the from-opening result.
- **Per-pool exclusivity (§3):** a property test that concurrent workers never co-own a pool — the `SKIP
  LOCKED` claim + monotonic `recalc_generation` (d) — and that N-worker parallel recalc over disjoint pools
  equals serial recalc (cross-pool independence).
- **Moving-tail convergence:** with the hot path appending, recalc's `settled_through` monotonically advances
  and, once appends stop, converges to the full stream (no permanently-stranded dirty pool).

## 9. Interfaces to sibling children

- **← (a) acct-q1oj.1** — consumes the per-pool replay unit + re-cost-floor; **refines** (a)'s D2 (incremental
  steady state) and **resolves** D4.
- **← (d) acct-q1oj.4** — drives `pool_settlement` (`settled_through`, `recost_floor`, generation), writes
  `cost_settlement` / `cost_layer_consumption` / `pool_state` layers; the persisted layer state is (d)'s
  materialization made concrete as the checkpoint.
- **→ (c) acct-q1oj.3** — hands the per-pool fold cost model (§2 strict-curve), the Pareto residual ceiling
  (§3), and the **D8 cursor-advance fork** (WAL vs backlog gauge) — all direct inputs to cadence/backlog.
- **→ (e) acct-q1oj.5** — the materialized authoritative layer state + `settled_through` frontier is what
  close finalizes; a pool with `recost_floor = NULL` and `settled_through` at the stream head is "fully
  settled" for close gating.

## 10. Open decisions (for review)

- **D2** *(resolved here)* — incremental-from-persisted-checkpoint steady state + full-opening-replay fallback.
- **D4** *(resolved here)* — replay from nearest checkpoint ≤ floor; one live checkpoint to start.
- **D8** *(handed to (c))* — cursor advance-on-completion (§17-literal, one gauge, WAL pins to slowest pool)
  vs advance-on-ingestion + durable dirty-set (WAL-safe, two gauges). **Recommend (B)**; (c) ratifies + owns
  any §17 operational-note update.
- **D9** *(resolved here)* — claim-driven `SKIP LOCKED` distribution over static hash shard.
- **D10** — recalc worker count default (start = committer_count 4; tunable).
- **D11** *(handed to (c))* — historical checkpoint retention depth (start: one live; add historical only if
  deep-backdated re-cost is measured to matter — storage vs re-cost depth).

D8/D11 are the only ones that touch backlog/cadence and go to (c); the rest are settled here. None change
(a)'s correctness contract — every optimization is validated equal to full-replay-from-opening, which is the
oracle (§8).
