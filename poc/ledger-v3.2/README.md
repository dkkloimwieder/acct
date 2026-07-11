# ledger-v3.2 — cost-ledger implementation of the surviving architecture

Implements the architecture that **survived the design-v3.1 §18 decision gate** (2026-07-08). This is
not another PoC "path" — it is the consolidation of the decided pieces into a buildable system, with the
**recalc/close engine** (deferred throughout v3.1, §7/§13) as its hard core.

Spec: [`../design_research/design-v3.2.md`](../design_research/design-v3.2.md) (the skeleton + section
outline) plus the five recalc-engine design notes
[`design-v3.2-recalc-a…e.md`](../design_research/). The decided inputs are consolidated **by reference**
from design-v3.1 §16 (posture) / §17 (feed) / §18 (gate) / §19 (recalc risk) / §20 (alternatives) / §3.7
(wire contract) — settled at the gate, not re-litigated here.

## The surviving architecture (design-v3.1 §18)

- **Hot path** — single-statement **direct** (SPIKE-B: one commutative `UPDATE … WHERE qty − Δ >= 0`,
  `RETURNING old.unit_cost`) for the aggregate paths + a **staging-table outbox** drained by
  `SELECT … FOR UPDATE SKIP LOCKED` committers (SPIKE-A) where batching/coalescing is wanted. Replaces
  v3.1's §6 shmem router/committer stack wholesale.
- **Posture** — everything-provisional (alt C, §16): no cost leg on the hot path; append the physical
  event, optionally fire the commutative qty aggregate; quantity is a **flagged running signal**
  (negative allowed, not rejected).
- **Feed** — a logical-decoding replication slot (§17), commit-ordered, `confirmed_flush_lsn`-cursored.
- **Wire contract** (§3.7) — callers send inventory facts only (`pool_id, line_type, qty, unit_cost`);
  the ledger resolves debit/credit/variance accounts from `posting_account_map`.
- **Recalc = the sole costing engine** (the hard core): (a) per-pool strict layer-walk + R-1
  chronological re-sort + R-2 backdated re-cost, (b) cross-pool claim-driven scheduler +
  incremental-from-checkpoint materialization, (c) cadence/backlog control + two-gauge detection,
  (d) recalc state schema + generation-delta idempotency, (e) close-time finalize.

## Relationship to ledger-v3.1

`poc/ledger-v3.1/` (DB `poc_v3_1`, the shmem `ledger_routed_c` preload) is the **frozen, characterized
reference — do not modify**. v3.2 is a fresh build on a new database (`poc_v3_2`); it is not a
copy-and-tweak of v3.1 (the design deletes the shmem routed stack and changes the schema). The shmem
routed stack is **superseded**; its physical deletion is tracked separately as **acct-uena**.

## Correctness anchor

The whole recalc engine's correctness reduces to child (a)'s **full-opening replay**, which *is* the
acct-0at4.7 strict-FIFO/LIFO replay oracle (`../ledger-v3.1/bench/replay-oracle*`). Every scheduler /
checkpoint / cadence optimization across (b)–(e) is validated equal to that replay. Reuse the oracle as
the golden reference; reuse the architecture-agnostic survivors acct-0at4.4 (sequential reference), .5
(conservation sweep), .8 (open-loop load/SLO).

## Intended build order (to be filed as the v3.2 implementation epic)

1. **Schema** — alt-C base (drop the hot-path cost leg; qty flagged/negative-allowed), `posting_account_map`
   wiring, `trx_line.posted_at` denormalization + `(pool_id, posted_at, id)` index (D1), recalc tables
   (`cost_layer_consumption`, `pool_settlement`, `cost_settlement`), `cost_adjustment` enums + sequence (d).
2. **Hot path** — single-statement direct (SPIKE-B) + staging-table outbox + `SKIP LOCKED` committers
   (SPIKE-A); inventory-facts-only SPI + ledger-side account resolution (§3.7 / §4).
3. **Feed** — logical-decoding slot consumer (`pgoutput` to start), dirty-set population, advance-on-ingestion
   cursor (D8, two-gauge).
4. **Recalc engine** — strict `fifo.rs`/`lifo.rs` (a), claim-driven workers + incremental-from-checkpoint
   (b), cadence/backpressure + G1/G2 detection (c).
5. **Close** — `close_period` gate + variance-into-empty-pool sweep + immutable finalize + `settle_pool` SPI (e).
6. **Testing** — reuse acct-0at4.4/.5/.7/.8; add recalc-vs-strict-replay + cadence-vs-load soak + the
   receipt-cost-volatility and backdated-event knobs the oracle flagged missing (§2a/§2c).

## Status

**DESIGN COMPLETE** (the design-v3.2* notes; epic acct-q1oj closed 2026-07-10). Implementation epic:
**acct-qm7o** (six phase children mirroring the build order above).

**Phase 1 (schema, acct-qm7o.1) SHIPPED**: Cargo workspace with `ledger-core` (carried over from v3.1
minus the provisional hot-path dispatcher; FIFO/LIFO remain fail-loud stubs until the recalc engine),
nine migrations under `db/migrations/` (alt-C base with no aggregate qty/value CHECKs and no
`pool_lock`; `posting_account_map`; `trx_line.posted_at` denorm + `(pool_id, posted_at, id)` index;
`cost_adjustment` enum labels + sequence; `cost_layer_consumption` / `pool_settlement` /
`cost_settlement`), database `poc_v3_2` created and migrated. Migration-time decisions: **D5** no
separate `(pool_id, id)` index (the R-1 composite subsumes it), **D7** distinct `cost_adjustment`
posting_event_type. **D6** rides with the recalc engine phase. Remaining residual decisions D8–D14 are
per-note and operational.

**Phase 2 (hot path, acct-qm7o.2) SHIPPED**: the `ledger_direct` pgrx extension (crates
`ledger-spi-common` + `ledger-direct`) with `ledger_submit_trx` — inventory-facts-only wire contract
(§3.7/§4), per-method dispatch: WAC via the SPIKE-B commutative CTEs (strict qty gate, running average
via PG 18 `RETURNING old`, cost leg posted), FIFO/LIFO via alt-C appends (no gate, flagged negative
aggregates, observed cost on the line, NO cost leg), STD/specific via ledger-core (`plan_apply` under
pool-row locks — the `pool` row is the per-pool mutex; no `pool_lock` table). Mixed-method multi-line
submissions use a two-tier lock order (core pool-row locks before commutative tuple locks, each tier
ascending) — deadlock-free by construction. Plus `ledger_staging_drain` (SPIKE-A): `ledger_inbox`
migration, `FOR UPDATE SKIP LOCKED` claims, per-submission subtransaction failure isolation. Migrations
0010 (`banker_div` in SQL) + 0011 (`ledger_inbox`). Tests: 2 acceptance + 2 property binaries
(`scripts/run-tests.sh`), including a 100-case drain-vs-direct equivalence property.

**Phase 3 (feed, acct-qm7o.3) SHIPPED**: the `ledger-feed` crate — the logical-decoding feed consumer
(design-v3.1 §17 / design-v3.2 §6) implementing recalc-c **D8 advance-on-ingestion**. Transport:
`pgoutput` consumed over the **SQL logical-decoding interface** (`pg_logical_slot_peek_binary_changes`
+ explicit `pg_replication_slot_advance`), not the streaming replication protocol — peek/advance
expresses D8's advance-after-durable-ingestion exactly and stays sqlx-native; the streaming protocol
is a latency refinement deferred until measured to matter (open question 1 stays lean). Contract:
**at-least-once, idempotent ingestion** — peek (non-consuming) → apply batch in one transaction
(dirty marks into `recalc_queue` + guarded-min `recost_floor` lowering on `pool_settlement`) →
advance; a crash between apply and advance re-delivers harmlessly. The dirty-set is the
crash-recovery boundary; the slot's `confirmed_flush_lsn` is the ONLY stream cursor (no watermark
table). Empty batches still advance the cursor to a pre-peek anchor so unpublished WAL (other
tables/databases) never inflates G1. Consumer is method-agnostic (every touched pool is marked;
no-op passes are free per recalc-d D6). Migrations: 0012 (`recalc_queue` — thin `(pool_id,
enqueued_at)`; the recost floor lives solely on `pool_settlement`), 0013 (`ledger_feed` publication,
`publish = 'insert'`; the slot is consumer-created runtime state, not schema), 0014 (gauge views:
`feed_lag` G1, `recalc_queue_depth` G2c, `recalc_pool_lag` G2a/G2b). Cluster prerequisites applied:
`wal_level = logical`, plus `max_slot_wal_keep_size = 4GB` as the dev guardrail against an abandoned
slot pinning WAL. Tests: 1 acceptance + 1 property binary (floor convergence independent of batch
boundaries / crash-redelivery interleaving).

**Phase 4 (recalc engine, acct-qm7o.4) SHIPPED**: the hard core. `ledger_core::{fifo,lifo}::strict_fold`
(module `strict`) replaces the MethodMismatch-only stubs — the oracle-equivalent strict layer-walk
(recalc-a §2/§7): per-depletion authoritative cost = banker-rounded weighted average of consumed layers,
per-consumed-layer draws, final open-layer state; the uncovered (ill-defined) remainder of an
over-depletion is costed at the depletion's own observed cost (close sweeps the residue, phase 5). The
`plan_apply` FIFO/LIFO guards still raise MethodMismatch — the hot path never runs strict math.
`ledger_recalc_step()` (ledger_direct) is one claim-driven worker tick (recalc-b D9): claims ONE dirty
pool from `recalc_queue` FOR UPDATE SKIP LOCKED (per-pool exclusivity = the claim lock), replays the
physical stream in R-1 `(pool_id, posted_at, id)` order — incremental from the persisted layer
checkpoint at `settled_through` (D2/D4), full-opening on a recost floor (R-2; no historical checkpoints,
D11) — and writes generation N in the same transaction (recalc-d §5 Model 1): `cost_layer_consumption` +
`cost_settlement` per newly-costed or changed depletion, ONE `cost_adjustment` trx wrapping a
`cost_adjustment_line` + `posting_line` per nonzero delta (routed through the depletion's own
operation pair from `posting_account_map`, swapped to depletion direction; negative delta reverses),
layer materialization, aggregate `value_sum` reconciliation, `pool_settlement` upsert (generation bumped
iff written — no-op passes are free, D6). GL cost-adjustment trxs stamp `posted_at = now()` so their own
feed delivery is a free no-op, never a floor-lowering loop; replay and the 0015 gauge view exclude
`cost_adjustment_line`. Engine↔feed races are closed by protocol: feed applies floors BEFORE marks
(mark evaluated after any block on a settle sees the post-settle queue); engine clears the floor
only-if-unchanged, self-lowers the floor for unscanned mid-pass commits inside the replayed range, and
keeps/re-stamps the queue row when work remains — invariant: floor set ⇒ queue row exists, so
claim-by-queue is complete. Workers: N looping connections (`scripts/run-recalc.sh`, default 4 = D10);
cadence is continuous drain (recalc-c §3). Backpressure (recalc-c §5) deliberately deferred —
tracked as its own issue. The claim protocol is lock-then-read (acct-qm7o.8): the claim statement
locks ONLY the queue row and the settlement state is read post-lock on a fresh snapshot — every
generation writer commits under that lock, and reading through a join inside the locking statement
surfaces EvalPlanQual-mixed rows (new queue tuple, snapshot-stale generation/floor) under concurrent
requeue; `settle()` additionally clamps the generation write monotonic (`GREATEST`) and the blocking
`claim_pool` re-ensures + retries when a granted lock finds the queue row deleted. Tests:
`acceptance_recalc_engine` (10 cases: costing/linkage/GL directions, D6 idempotency, oracle vignette
A backdate-recost via the real feed loop, loopback no-op, incremental tail, uncovered-at-observed,
non-strict no-op, id tiebreak, rollback re-claim, concurrent workers) + `acceptance_recalc_stale_claim`
(two deterministic multi-session interleavings pinning the claim protocol: a parked claim granted
against a re-stamped queue row must read the sibling's committed generation — never regress it — and
a parked claim waking to a deleted queue row must retry, not error; `pg_stat_activity` lock-wait
polling is the rendezvous) + `property_recalc_engine` (100 random interleavings of
submits/ingests/steps/crash-steps over a random business-date grid, checked against an independent
in-test layer walk: R1 oracle equivalence, R2 layer materialization, R3 exact value reconciliation,
R4 frontier at rest, R5 idempotency, R6 derivability from consumption rows, R7 method isolation,
R8 generation monotonicity sampled after every op; bounded quiesce = livelock detection).

**Phase 5 (close, acct-qm7o.5) SHIPPED**: close-time finalize (recalc-e). `ledger_close_period(period_id,
actor, force)` is the orchestrated close — one transaction: closer mutex (advisory `(32021, period)`;
concurrent closers serialize, re-close = `already_closed` no-op; periods close in `start_date` order — an
earlier open period fails loud, since a backdate into it could re-cost this period's depletions and wedge
the engine against the settlement guard), the **recalc-drain gate** (period-scoped G2a over fifo/lifo pools
with activity ≤ end-of-period: no unsettled physical event and no recost floor at/below the period end;
gate-fail without force persists `state = 'closing'` — draining, not frozen — and returns per-pool lag +
the G2b gross value so no forced close is silent), **drain + sweep** (force = drain synchronously, NEVER
skip — there is no provisional cost leg to fall back on; every gate-scoped pool is claimed via its queue
row, its aggregate row locked — every hot-path FL statement touches the aggregate first, so no event can
commit on a swept pool mid-sweep — drained to stream head, then trued: `value_sum := Σ open-layer value`
in one `RETURNING old` statement whose difference — banker residue, the exact-empty flush, uncovered
depletion value — posts as a zero-qty `cost_adjustment_line` against `posting_account_map.variance_acct`),
then **fence + re-check + stamp**: advisory `(32022, period)` exclusive waits out in-flight inserts into
the period and blocks new ones, the gate re-runs on a fresh snapshot (stragglers can only be on unswept
pools; one more round converges), and `closed_at`/`closed_by`/`state = 'closed'` stamps. Finalize is
stamp-only (D13/D14): migration 0017's BEFORE INSERT triggers on `trx_line` (physical lines; the engine's
`cost_adjustment_line` output is exempt) and `cost_settlement` reject anything landing in a closed period —
**PeriodClosed, SQLSTATE 55000** (pgrx's SPI boundary maps non-enum SQLSTATEs to XX000, so the parent
repo's P00xx convention cannot cross it; the `PeriodClosed:` message prefix is the stable identity). The
trigger takes the shared side of the fence and re-reads state after acquiring it, closing every
gate-to-stamp interleaving. Migration 0016 relaxes the `trx_line.qty <> 0` CHECK for zero-qty
`cost_adjustment_line` rows (the sweep's pure value line). `ledger_settle_pool(pool_id)` is the on-demand
shape (recalc-e §7): targeted blocking claim of the pool's queue row + drain to head — the three worker
models (continuous `ledger_recalc_step` loops / on-demand / periodic close) are one engine. The close
report carries `feed_lag_bytes` (G1): the gate reads feed-maintained floor state, so it is only as current
as the slot — the documented premise is a running feed at close; an undelivered backdated straggler
surfaces post-close as a loud settlement-guard reject (G2a/G2c alarm), never a silent mis-valuation.
Tests: `acceptance_close_period` (12 cases: gate block/pass + closing-allows-backdates, idempotent
re-close, concurrent closer serialization, forced synchronous drain with G2b in the report, all three
sweep residue sources pinned separately with GL direction, next-period-tail drain on a passing gate,
immutability rejects + next-period flow, guard rails, settle_pool, MissingVarianceAccount fail-loud) +
`property_close_period` (100 random workloads over a two-day grid straddling the period boundary, closed
amid arbitrary unsettled state: C1 completeness, C2 oracle equivalence across the close, C3 sweep
exactness `value_sum == Σ layer value` + conservation including the swept residue, C4 random backdates all
reject, C5 re-close no-op, C6 post-close life with `settle_pool` + prefix stability across the closed
boundary, C7 method isolation).

**Phase 6 (testing, acct-qm7o.6) SHIPPED**: the `ledger-bench` harness — the acct-0at4 survivor oracles
ported v3.2-native plus the two knobs the replay oracle flagged missing. `ledger-bench soak` runs the
whole surviving architecture concurrently in one process (open-loop CO-free load on the direct + staging
paths, the feed consumer, N recalc workers on continuous cadence, the G1/G2 gauge sampler), then
quiesces (bounded — a floor/requeue livelock fails the run), verifies, closes the period unforced, and
probes immutability. The workload carries the **receipt-cost-volatility knob** (`low`/`med`/`high`/`trend`
profiles mirroring the oracle's sweep — a constant-cost seed measures nothing) and the **backdated-event
injector** (`--backdate-pct`/`--backdate-window-secs`: business-order ≠ commit-order at a controlled rate,
the exact R-2 breaking input). `--pause-workers`/`--pause-feed` windows demonstrate the two-gauge
separation (G2 climbs while G1 stays flat, and vice versa); `--midrun-close-at` runs an unforced close
inside a rolled-back transaction — a dry-run gate probe that records the per-pool lag report and G2b
gross bound without mutating period state (advisory locks are xact-scoped, so the rollback releases
them). `ledger-bench verify` is the conservation sweep (V1 qty, V2 value reconciliation via the
commit-order fold − settlement deltas − swept residue, V5 structural/posting shape, V6 intent, V7 method
isolation) plus, at quiesce, at-scale oracle equivalence (V3/V4: an independent reference walk sharing
only `banker_div`), and emits the provisional-vs-authoritative drift distribution per method — the
oracle's variance table measured on the live engine. `scripts/soak.sh` and `scripts/slo-sweep.sh`
(throughput-at-SLO ramp, acct-0at4.8 methodology) are the operator entry points; results in
[`bench/soak-results.md`](bench/soak-results.md). One invariant-formulation subtlety the soak surfaced
(small-case property nets can't reach it): the FL exact-empty flush wipes engine aggregate value
corrections applied before it, so the offline `fold − deltas` identity is not reconstructible for a
flushed pool with settlements — the drift is bounded, surfaces in the close sweep's residue GL, and the
post-close `value_sum == Σ open-layer value` check covers those pools exactly (the verify skips-and-counts
them pre-close as `v2_skipped_flush_wiped`).

## Stack

Rust + `sqlx` (raw SQL, compile-time-checked) + `sqlx-cli` migrations under `db/migrations/`, same as the
rest of the project. Database `poc_v3_2` on the dev container (`localhost:5111`). No task runners; scripts
under `scripts/` are the entry points (per project convention).
