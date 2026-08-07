# design-v3.2: cost-ledger spec-of-record — the surviving architecture

> **Status: spec-of-record.** This describes the v3.2 costing plane **as built**, merged to `main` at
> the convergence merge `8378a7d` (tree state `4a48512`). It is the authoritative description of the
> costing plane; the code is the implementation of this document, not the other way round.
>
> **Two sections are still being written** and are marked in place: **§7 (close semantics)** and the
> mechanical-guard half of **§3a (posted_at)**. Both are the subject of active hardening under
> `acct-1vur`, which is rewriting the close gate's feed premise; publishing their detail before that
> verdict would document semantics that are changing. Everything else here is settled.
>
> Decided inputs are consolidated **by reference** from design-v3.1 §16 (posture) / §17 (feed) / §18
> (gate verdict) / §19 (recalc risk) / §20 (alternatives) / §3.7 (wire contract), and from
> [`convergence-decisions-2026-08-07.md`](convergence-decisions-2026-08-07.md) (Q1–Q14). Neither is
> re-litigated here.

**Labels used below.** **T2** — the pending tranche named in the status block (§7 and §3a's guard
half); a `[T2]` marker means "decided, not yet written here". **SPIKE-A / SPIKE-B** — the two
hot-path spikes design-v3.1 §18 measured at the gate: the staging-table outbox and the
single-statement direct write, both of which survived. **D1, D3, D5, D7, D8-B, D11, D13, D14** —
design-decision IDs from the five [`design-v3.2-recalc-{a..e}.md`](.) notes, which are part of this
spec's reference set; each is cited where it was resolved. **R-1 / R-2** — the two recalc
correctness requirements: business-chronological re-sort, and re-costing forced by backdated events.



## 1. Purpose and standing

**What v3.2 is.** The cost ledger for SKU × location inventory valuation: a hot path that records
physical inventory events, a recalc engine that assigns authoritative cost, a logical-decoding feed
that couples the two, and a period close that gates and freezes the result.

**Its standing.** The `acct-0at4.11.5` gate verdict — verbatim: *"Machinery not justified:
staging-table + single-statement + alt-C + logical-decoding-feed is the surviving architecture"* —
named this shape the survivor over six predecessor generations. Convergence Q1 made the costing
plane the unification target and Q11 merged the line to `main`. v3.2 is therefore the **designated
starting architecture** for the costing plane, not a PoC path under evaluation.

**What it is not.** It is not yet wired into the `acct` document layer, which retains its own plpgsql
costing plane. Whether that layer is ported onto v3.2 or rebuilt is convergence **Q2**, decided by
the dossiers `acct-476a.2` / `acct-476a.4`. Until they report, the two planes coexist without a seam.

**Posture (Q13).** Correctness first; no fixed TPS target; baseline before complexity. One amendment
carries real weight for this document: the **Q3a drift-exposure bounds are in-scope product bounds**,
not performance targets — see §9. They bound how wrong a mid-period valuation may be, and their
numbers are filled from the gated `acct-63qs.6` baseline rather than chosen up front.

**What carries over from design-v3.1 (cite, do not restate).** The base schema (§2.1–§2.4), the
method semantics for the strict-cost methods (WAC §3.1, STD §3.3, specific-id §3.4), `ledger-core` as
the shared pure-Rust crate (§8), and the numeric / `banker_div` discipline (§3.0).

**What is superseded (do not carry forward).** The §6 shmem router/committer stack; the
RMW-under-`pool_lock` protocol with its sorted-acquisition and deadlock-ordering discipline; and the
incoherent *cost-provisional / qty-strict / order-unordered* posture the PoC shipped. All three were
dominated at the gate — by SPIKE-A, SPIKE-B, and the §16 alt-C endpoint respectively.

## 2. Schema

Twenty migrations under `poc/ledger-v3.2/db/migrations/`, all applied to database `poc_v3_2`.
Migrations are content-addressed by `sqlx` and therefore **immutable once applied**: corrections ship
as new migrations, never as edits.

| # | Surface | Substance |
|---|---|---|
| `0001` | Enums | `pool_method` (fifo/lifo/wac/std/specific), `pool_provisional_basis` (running_avg/standard), `trx_type` (7), `line_type` (9), `posting_event_type` (7), `account_type` (5), `dimension_type` (5) |
| `0002` | Reference | `sku`, `location`, `account`, `accounting_period`. Ids are application-managed `BIGINT`, not auto-allocated. `accounting_period.state ∈ {open, closing, closed}` with `UNIQUE (start_date, end_date)` |
| `0003` | Ledger core | `pool` (`UNIQUE (sku_id, location_id, identity_key)`; specific pools require a non-zero identity key), `standard_cost`, `pool_state`, `trx`, `trx_line`. **No `pool_lock` table** — writers take PG row locks directly |
| `0004` | Journal | `posting_line`, `posting_line_dimension`. Writers are the strict-cost hot paths, the recalc engine, and close-time finalize |
| `0005` | Indexes | Partial index on `account.parent_id`; `trx_line` by trx and by `source_trx_line_id`; `posting_line` by trx_line and `posted_at`; dimension lookup |
| `0006` | Account resolution | `posting_account_map` — one account set per `(sku_id, location_id)`; six operation pairs stored in receipt direction plus a nullable `variance_acct`. See §4 |
| `0007` | **D1 resolved** | `trx_line.posted_at` denormalized (`NOT NULL`) + index `(pool_id, posted_at, id)`. **D5 resolved**: no separate `(pool_id, id)` index — the composite's leading prefix serves pool-only lookups |
| `0008` | Recalc taxonomy | `cost_adjustment` labels on `trx_type` / `line_type` / `posting_event_type` + `cost_adjustment_id_seq`. **D7 resolved**: the posting event type is distinct from `variance`, keeping deferred-recalc GL movement auditably separable from hot-path purchase-price variance |
| `0009` | **D3 + idempotency state** | `cost_layer_consumption` (per-consumed-layer linkage, generation-keyed), `pool_settlement` (per-pool frontier + recost floor + generation), `cost_settlement` (per-depletion-per-generation authoritative record, append-only) |
| `0010` | `banker_div` in SQL | Round-half-to-even division, `IMMUTABLE PARALLEL SAFE`, byte-faithful to `ledger_core::numeric::banker_div`. Enables the single-statement hot path to derive running averages in-statement |
| `0011` | Staging outbox | `ledger_inbox` (`pending`/`done`/`failed` + error text) with a partial index confining the claim scan to pending rows |
| `0012` | Dirty set | `recalc_queue` — deliberately thin (`pool_id`, `enqueued_at`). The recost floor lives on `pool_settlement`; one authoritative home per fact |
| `0013` | Feed filter | `CREATE PUBLICATION ledger_feed FOR TABLE trx_line WITH (publish = 'insert')`. The **slot is not created here** — slots are cluster runtime state, not schema, and are not carried by dump/restore |
| `0014` | Gauges | Views `feed_lag` (G1), `recalc_queue_depth` (G2c), `recalc_pool_lag` (G2a/G2b) |
| `0015` | Gauge correction | `recalc_pool_lag` excludes `cost_adjustment_line` — engine output would otherwise count as forever-unsettled and permanently inflate the tip |
| `0016` | Zero-qty exemption | `trx_line` qty check relaxed to `qty <> 0 OR line_type = 'cost_adjustment_line'`, for the close sweep's pure-value line |
| `0017` | Period guards | `BEFORE INSERT` triggers on `trx_line` and `cost_settlement`. See §7 |
| `0018` | Backpressure | `recalc_backpressure_config` (single row, deletion-guarded, defaults 200/20), `recalc_backlog` (per-pool counter), `recalc_backpressure` (throttled set), `ledger_inbox` admission trigger |
| `0019` | Cost floor | `standard_cost.unit_cost >= 0` — a stored negative standard would be silently observed as 0 by the hot path's cost clamp, so it is rejected at the source |
| `0020` | Close gate config | `close_gate_config` — single row, `feed_required` default `TRUE`. Turns the close gate's feed-currency premise into an enforced leg. See §7 |

Two schema properties are load-bearing rather than incidental:

- **`pool_state` carries no non-negativity constraints on `qty` or `value_sum`.** Quantity is a
  flagged running signal (§9, Q3b): a FIFO/LIFO depletion beyond on-hand drives the aggregate
  negative and is flagged, not rejected. `value_sum` is the net posted book value, and the running
  average is *derived* as `banker_div(value_sum, qty)` — never re-rounded incrementally.
- **`trx.UNIQUE (trx_type, source_id)` is the ledger idempotency key.** Re-submitting a pair raises a
  constraint violation rather than duplicating. Engine-generated adjustment transactions satisfy it
  via `cost_adjustment_id_seq`.

## 3. Hot path

Two shapes, both PG-native, chosen per call site. Both share one dispatch (`submit::apply_submission`),
so their semantics differ only in transaction boundary and admission point.

**Direct single-statement (SPIKE-B).** One kept-plan statement per line folds the aggregate mutation,
the `trx_line` write, and (for WAC) the `posting_line` on PG's own tuple lock. Data-modifying CTEs
chain off the aggregate mutation, so a failed gate atomically writes nothing.

**Staging-table outbox (SPIKE-A).** The caller's enqueue is an in-tx heap `INSERT` into
`ledger_inbox` — atomicity with the caller's business write is free. Committers drain via
`SELECT … FOR UPDATE SKIP LOCKED ORDER BY id`, applying each envelope through the same dispatch. The
row *is* the status: durable, observable, cleanly re-claimed after a committer crash.

**Per-method behaviour on the hot path:**

| Method | Quantity | Cost | Journal |
|---|---|---|---|
| FIFO / LIFO | No gate; aggregate may go negative, flagged | Observed provisional cost recorded on the line (running average, or standard basis) | **None** — recalc posts the only cost legs (alt C) |
| WAC | Strict gate in the depletion's `WHERE (qty − Δ >= 0)` | Final, at the pre-update running average (PG 18 `RETURNING old`) | Cost leg posts |
| STD / specific | Strict gate via `ledger_core::plan_apply` under pool-row locks | Final | Cost leg posts |

**Lock order** is a fixed two-tier global order — `ledger-core` pool-row locks (ascending) strictly
before any commutative tuple lock (ascending pool) — so concurrent mixed-method submissions cannot
deadlock: every transaction acquires all tier-1 locks before its first tier-2 lock, and each tier is
internally sorted.

### 3.1 Failure semantics

Ledger errors surface as SQLSTATEs at the SPI boundary; the parent repo's `P00xx` convention cannot
be used, because pgrx re-reports any SQLSTATE outside its enum as `XX000`.

| Condition | SQLSTATE | Meaning |
|---|---|---|
| `InsufficientInventory` | `23000` | A strict-method depletion exceeds on-hand. The commutative WAC gate raises the same code |
| `SpecificPoolOccupied` | `23000` | A second receipt to an occupied specific pool (K = 1) |
| `UnknownPool` | `42704` | A line references a `pool_id` with no `pool` row — the most caller-facing of the set |
| `MethodMismatch` | `22000` | A FIFO/LIFO pool reached `plan_apply` — a dispatch bug; their costing is the engine's |
| `MissingStandardCost` | `22000` | STD pool (or standard-basis FIFO/LIFO depletion) with no `standard_cost` row |
| `MissingPostingAccounts` | `22000` | Touched pool's `(sku_id, location_id)` has no `posting_account_map` row |
| `MissingVarianceAccount` | `22000` | See §4 — two distinct call sites, one of them close-time |
| `Overflow` | `22003` | BIGINT arithmetic overflowed inside plan math |
| Backpressure reject | `53400` | Pool's unsettled backlog is over bound; admission refused (§5.3) |
| `PeriodClosed` | `55000` | Insert into a closed period; stable `PeriodClosed:` message prefix is the identity (§7) |

**Direct path:** any line failure raises `ERROR` and aborts the whole submission — the `trx` row and
every already-applied line roll back together.

**Staging path:** each submission applies inside a subtransaction. A failure rolls back only its own
writes and marks its row `failed` with the error text; the rest of the batch proceeds. Status updates
commit atomically with the batch's ledger writes, so a mid-drain crash commits nothing and the rows
re-claim cleanly.

**Cross-committer same-pool apply order is not guaranteed** (per-committer FIFO only). This is benign
under alt C: costing chronology is the engine's R-1 re-sort, and the qty aggregate delta is
commutative. The observed provisional cost a racing depletion records is order-sensitive — and
already provisional by construction.

### 3a. The `posted_at` write contract

**The contract.** Every submission carries `posted_at`, which lands on every `trx_line` it writes.
`posted_at` **MUST be the true business/effective time of the event** — the date the inventory
movement actually happened in the business, not the moment the row reached the database.

**Why it is load-bearing.** The recalc engine replays each pool in `(pool_id, posted_at, id)` order
(R-1). That order *is* the costing chronology: which layers a depletion consumes, and therefore its
authoritative cost, is determined by where `posted_at` places it in its pool's stream. `id` is the
deterministic within-date tiebreak, not a substitute.

**No constraint can enforce it.** The database cannot know an event's real business date. `NOT NULL`
is schema-enforceable; truthfulness is an SPI-contract obligation on the caller. Nor is recency a
valid proxy — **backdated events are admitted by design** (that is R-2, the whole reason the engine
replays rather than forward-scans), so "close to now" is not a test of correctness.

**Prohibited writer patterns:**

- **Wall-clock defaults.** A writer that stamps `now()` because the business date was inconvenient to
  thread through silently converts business chronology into commit chronology.
- **Constant or near-constant stamps.** The precedent is concrete: the v3.1 harness stamped a
  compile-time constant on every submission, producing two distinct values across an entire run —
  which degrades R-1 to pure `id` order and erases the business-order signal the engine exists to
  honour.
- **Deriving `posted_at` from ingestion batching.** The batch boundary is a property of the
  transport, not of the business event.

**The one exception, and it is the engine's own.** Cost-adjustment transactions the engine posts are
stamped `posted_at = now()` deliberately — never a business date — so that their own feed delivery
cannot lower a recost floor and loop the pool. These rows are excluded from replay, from the gauges,
and from the period guard.

> **[T2 — pending `acct-1vur.6`]** The *mechanical guard set* that backs this contract — staging-write
> validation, optional sanity bounds against the open-period window, and the audit of in-tree writers
> for constant or wall-clock stamping — is being specified under `acct-1vur.6` and will be recorded
> here. The contract text above is complete and binding as written; what is pending is the enforcement
> surface, not the rule. (Recorded as decided and shipped: **D1**, the denormalized column and its
> index — migration `0007`.)

## 4. SPI surface

Callers send **inventory facts only**: `(pool_id, line_type, qty, unit_cost)` per line, plus the
transaction's `trx_type`, `source_id`, and `posted_at`. Callers carry no chart-of-accounts knowledge.

- `ledger_submit_trx(trx_type, source_id, posted_at, lines) → trx_id` — the direct path. `posted_at`
  is RFC3339 text and is bound by §3a.
- `ledger_staging_drain(limit)` — claims and applies pending `ledger_inbox` rows.
- `ledger_recalc_step()` — one claim-driven recalc worker tick (§5).
- `ledger_settle_pool(pool_id)` — on-demand: drain one pool to its stream head synchronously.
- `ledger_close_period(period_id, actor, force) → report` — see §7.

**Account resolution.** The ledger resolves `debit` / `credit` from `posting_account_map`. Posting
accounts and standard cost are *configuration*, not contended hot state, and the two paths read them
at different points: the ledger-core path (STD/specific) hydrates them **under the pool-row lock**,
while the commutative path (WAC/FIFO/LIFO) resolves them in the single **unlocked reference read**
that precedes any write. Each operation's pair is stored in the **receipt** (inventory-increase)
direction; the engine uses it as-is for receipts and **swaps** it for depletions. `line_type` selects
the operation: `receipt ← po_receipt_line`; `transfer ← transfer_shipment_line / transfer_receipt_line`;
`build ← wo_output / wo_backflush`; `scrap ← wo_scrap`; `adjustment ← inv_adjustment_line /
manual_adjustment_line`; `revaluation ← revaluation_line`.

A touched pool whose `(sku_id, location_id)` has no map row **fails loud** (`MissingPostingAccounts`)
at submit time — including for FIFO/LIFO, which post no journal row on the hot path, because the
engine must find the config later and a submit-time check is the early, cheap place to fail.

**Admission control.** The direct path probes the throttled set before any write and rejects with
`53400`; the staging path is gated by a `BEFORE INSERT` trigger on `ledger_inbox`. The probe is one
lookup against a normally-empty primary key, which is simultaneously the cheap global "backpressure
active" flag and the per-pool consult — alt-C's read-free property is preserved in the common case.
`ledger_staging_drain` applies **already-admitted** envelopes unchecked: admission is the only gate,
and accepted work is never retroactively failed.

### 4.1 The variance account is required for every gate-scoped FIFO/LIFO pool

`posting_account_map.variance_acct` is nullable, and `MissingVarianceAccount` is raised from **two
distinct call sites** with different timing:

1. **Submit time** — an STD receipt whose actual cost differs from standard needs a variance account
   to absorb the purchase-price variance (`ledger_core::standard`).
2. **Close time** — the close sweep posts each pool's settlement residue as a zero-qty
   `cost_adjustment_line` against `variance_acct`. This applies to **every gate-scoped FIFO/LIFO
   pool**, whose residue is only defined at full settlement.

The operational rule is therefore: **every FIFO/LIFO pool needs `variance_acct` set**, whether or not
it will ever see an STD receipt. Only FIFO/LIFO pools are ever swept — the sweep bails loud on any
other method — so the rule is scoped to them, not to all pools.

A FIFO or LIFO pool configured without one succeeds on every hot-path write and every recalc pass,
then fails loud at the **first close that produces a nonzero residue** — correct, but late and
load-dependent. The sweep posts the residue GL only when truing the aggregate actually moves it (and
skips pools with no aggregate row at all), so a pool can sweep clean through several closes before the
first rounding or uncovered-depletion residue exposes the missing account. Do not rely on an early
close to surface the misconfiguration: populate `variance_acct` for every pool at configuration time.

> Migration `0006`'s header comment states the opposite ("pools that are never STD never need it"), as
> does the `ledger_core::snapshot` doc comment. Both predate the close sweep. `0006` is applied and
> therefore immutable, so the correction ships as a separate `COMMENT ON COLUMN` migration. **This
> section is the authoritative statement of the rule.**

## 5. Recalc engine

Under alt C the engine is the **sole costing plane for FIFO/LIFO pools**: their hot-path appends post
no GL, so recalc produces the only authoritative valuation. Its backlog gates whenever any
authoritative cost is needed — not merely when a correction lands.

### 5.1 The pass

`ledger_recalc_step()` is one worker tick, in one transaction:

1. **Claim** one dirty pool from `recalc_queue` (`FOR UPDATE SKIP LOCKED`, oldest mark first). The
   queue-row lock *is* the per-pool exclusivity the serial fold requires. Non-FIFO/LIFO pools drain as
   free no-ops — the feed is method-agnostic, and the engine decides per pool what a pass means.
2. **Replay** the pool's physical events in R-1 `(posted_at, id)` order through
   `ledger_core::{fifo,lifo}::strict_fold`. A clean pool replays **incrementally** from the persisted
   layer state at `settled_through` (the live checkpoint — O(new events), the steady state); a pool
   with a recost floor set, or with no frontier yet, replays **from opening** (the oracle-equivalent
   correctness baseline). `cost_adjustment_line` rows are engine output and are never replay input.
3. **Write generation N** in the same transaction: a `cost_settlement` + `cost_layer_consumption` row
   set per depletion newly costed or whose authoritative cost changed, and one `cost_adjustment` trx
   wrapping a `cost_adjustment_line` + `posting_line` per nonzero delta `(authoritative − prior) × qty`,
   routed through the depletion's own operation pair and swapped to depletion direction (a negative
   delta reverses). **Zero-delta passes write nothing and do not bump the generation.**
4. **Materialize** the open layers at `pool_state.layer_id > 0` — the next pass's checkpoint — and
   fold the net GL delta into the aggregate's `value_sum`.
5. **Settle**: advance `settled_through_*`, bump the generation *iff* anything was written, and clear
   the recost floor **only if it still equals the claimed value** — a feed batch may have lowered it
   mid-pass, and that newer floor must survive.
6. **Queue decision**, on the freshest snapshot: drop the claim's queue row unless the floor survived,
   a fresh tail exists past the new frontier, or an unscanned mid-pass commit landed *inside* the
   replayed range (the engine lowers the floor for that one itself — its feed mark hit the
   claim-locked row's `DO NOTHING` and nothing else would recover it). A kept row is **re-stamped to
   the back of the line**, so a hot pool cannot starve its siblings.

**Crash safety.** The whole pass is the caller's transaction. An abort leaves the queue row, the floor,
and generation N−1 untouched; the retry recomputes the same deterministic pass.

**Claim protocol.** The claim statement locks *only* the queue row; settlement state is read after the
lock lands, on a fresh snapshot. Reading through a join inside the locking statement would surface
EvalPlanQual-mixed rows — a new queue tuple with a snapshot-stale generation — under concurrent
requeue. The settle additionally clamps the generation write monotonic, and a blocking claim that
finds its queue row deleted re-ensures and retries.

### 5.2 Idempotency (generation-delta, Model 1)

The authoritative cost of a depletion is its **max-generation `cost_settlement` row**;
`prior_unit_cost` is generation N−1's authoritative (or the observed line cost at N = 1); the GL
posted is exactly the delta. This yields the re-run contract directly: a repeated pass over an
unchanged stream computes identical costs, finds zero deltas, posts nothing, and does not bump the
generation. A genuine re-cost posts exactly the inter-generation difference — never a double-post,
never a compensating pair.

The `cost_layer_consumption` history is append-only and generation-keyed: an R-2 re-cost writes a
fresh generation rather than deleting the prior one. The GL stays one `cost_adjustment_line` per
depletion (the net delta); the per-layer detail lives in the linkage table.

### 5.3 Backpressure

A deliberate, lazily-engaged re-coupling that stops the physical plane running arbitrarily far ahead
of authoritative cost on FIFO/LIFO pools. The bound metric is the per-pool **unsettled-event count**
(`recalc_backlog.pending_events`).

Ownership splits at the events that move the metric: the **feed** increments the counter for delivered
physical FIFO/LIFO events and engages at the bound; the **engine** resets the counter to the exact
committed tail above the new frontier at every settle — wiping feed-lag skew so it never drifts —
applies the same bound to its own reset, and releases at the low-water mark. Bounds live in
`recalc_backpressure_config` (defaults 200 / 20, calibrated ≈3× the observed healthy global maximum).

The feed's apply order is load-bearing — **floors → counter bumps → marks** — because the mark is what
guarantees a future engine pass. A pass can settle events whose delivery is still in flight; the late
bump then lands stale counts on top of that pass's committed reset, and the *trailing* mark re-creates
the queue row the pass deleted, so the guaranteed follow-up pass wipes the residue and releases any
spurious engage. Marks-first would leave a permanently throttled pool with an empty tail.

Row-lock order, shared by every writer (feed apply, engine pass, close sweep):
`pool_settlement → recalc_backlog → recalc_backpressure`, multi-pool writers ascending by pool id.

**Scope.** The bound is the *un-costed tail*. A backdated/backfill flood is a different axis: those
events settle promptly while invalidating already-settled costs behind them, so they never accumulate
in this counter. Their cost is re-cost write amplification, bounded today by cadence and the close
gate.

### 5a. The mid-period valuation read contract

This is an **interface rule** for any consumer reading inventory value between closes.

- **Materialized layers are authoritative.** `pool_state` rows at `layer_id > 0`, together with the
  `cost_settlement` max-generation rows, are the engine's authoritative output. A consumer that needs
  a correct number reads those.
- **The aggregate `value_sum` is provisional until close.** The `layer_id = 0` row is a running
  commutative signal maintained by the hot path and reconciled by each engine pass. Between passes it
  reflects observed provisional costs, not authoritative ones.
- **How wrong it can be is measured, not assumed.** Under ±40% receipt-cost volatility with 5%
  backdating, the provisional plane runs **~22–30% wrong per depletion** on average until recalc trues
  it up (`bench/soak-results.md` §3). The bias is *directional*, not mean-zero. Measuring
  **Δ = authoritative − provisional**: FIFO's Δ is negative, i.e. **the provisional plane overstates**
  — old cheap layers anchor the authoritative below it. Under a monotone rising trend LIFO's Δ flips
  **positive**, i.e. **the provisional plane understates** — authoritative draws come from the newest,
  most expensive layers while the observed running average lags behind. Because the error is
  directional it accumulates into a real period-level misstatement rather than cancelling; a consumer
  netting the two methods against each other would see it cancel, which is precisely why the gauges
  are per-pool rather than global. The trend profile is a different workload shape — 0.5×→1.5× rising
  cost at 10% backdating, not the ±40%/5% characterization run — and reaches 36.0% on FIFO.
- **One divergence is known and bounded.** The hot path's exact-empty flush (`qty − Δ = 0 ⇒
  value_sum := 0`) discards aggregate value corrections the engine folded in before it. For such a
  pool the offline identity `value_sum == fold − settlement deltas − swept residue` is not
  reconstructible from the stream alone, because the wiped amount depends on run-time interleaving.
  The drift is bounded, surfaces in the close sweep's residue GL, and is covered exactly by the
  post-close `value_sum == Σ open-layer value` check. The conservation verifier skips and **counts**
  these pools as `v2_skipped_flush_wiped` (8 of 300 in the headline soak) — a concurrency concession,
  not a semantic one.

**Consequence for consumers:** any report that must tie out to the general ledger reads post-close, or
reads layers. Mid-period aggregate reads are decision-support numbers with a known error band.

## 6. Feed

The feed is a **logical-decoding replication slot** delivering `trx_line` inserts in commit order with
a durable, resumable cursor. It is not a `WHERE id > watermark` scan (which has a silent
identity-vs-commit-order gap) and not a hand-rolled watermark column; both reimplement in application
code what the slot provides. **No watermark table exists or may be added.**

**Transport (resolved).** `pgoutput` consumed over the **SQL logical-decoding interface**
(`pg_logical_slot_peek_binary_changes` + explicit `pg_replication_slot_advance`), not the streaming
replication protocol. Peek/advance expresses advance-after-durable-ingestion exactly and stays
sqlx-native. The streaming protocol is a latency refinement, deferred until measured to matter. The
publication filters to `trx_line` inserts only.

**Delivery contract: at-least-once, idempotent ingestion.** The sequence is peek (non-consuming) →
apply the batch in one transaction (recost floors + backpressure counters + dirty marks) → advance the
cursor. A crash between apply and advance re-delivers the batch harmlessly: re-marking a pool dirty
and re-taking a guarded floor minimum are both no-ops. The one non-idempotent effect is the
backpressure counter bump, which over-counts in the conservative direction (an early engage, never a
missed one) and is wiped by the engine's exact-count reset at the next settle.

**The cursor is the delivery cursor (D8-B, advance-on-ingestion).** `confirmed_flush_lsn` means
*delivered and durably applied to the dirty-set*, not merely decoded. The **dirty-set is the
crash-recovery boundary** — a crash re-drains the dirty-set, not the slot. Empty batches still advance
to a pre-peek anchor, so unpublished WAL from other tables or databases never inflates G1.

**Two gauges, deliberately separate.** G1 (`feed_lag`) is cursor/ingestion lag and the WAL-retention
signal; G2 (`recalc_pool_lag`, `recalc_queue_depth`) is valuation staleness, per pool. They are coupled
but have distinct remediation axes — feed/WAL health versus recalc throughput — and conflating them is
the failure mode the single-gauge alternative would have shipped. The soak demonstrates the separation
directly: pausing the workers climbs G2 while G1 stays flat; pausing the feed climbs G1 while G2 goes
quiet.

**Single-consumer posture.** A logical slot has one cursor, so exactly one consumer process owns it.
Concurrency lives in the recalc workers draining the dirty-set.

## 7. Close  *[T2 — being written under `acct-1vur`]*

> This section is deliberately not yet written in full. `ledger_close_period(period_id, actor, force)`
> ships and is exercised by 14 acceptance cases plus a 100-workload property net (C1–C7), but the
> remaining `acct-1vur` children are still changing the surrounding admission surface, and a hardening
> verdict under that epic also decides convergence **Q8** (whether a period-reopen primitive is needed
> at all). Publishing the detailed semantics before those land would document a moving target.
>
> What is already decided and will be recorded here: close is a **consistency gate plus a finalize
> stamp**, not an adjustment storm; **force means drain synchronously, never skip** (there is no
> provisional cost leg to fall back on); periods close in `start_date` order; finalize is **stamp-only**,
> with migration `0017`'s `BEFORE INSERT` guards on `trx_line` and `cost_settlement` making closed-period
> immutability a schema invariant rather than API discipline; and `PeriodClosed` is **SQLSTATE 55000**
> with the `PeriodClosed:` message prefix as its stable identity, because pgrx maps non-enum SQLSTATEs
> to `XX000`.
>
> **Feed currency is enforced, not assumed** (migration `0020`): the gate requires a present,
> non-invalidated `ledger_feed` slot whose `confirmed_flush_lsn` has reached the WAL position captured
> at gate entry, so every event committed before the close has been delivered. `close_gate_config.feed_required`
> (default `TRUE`) is a stored policy rather than a call argument — the waiver should be a visible,
> auditable property of the database, not a flag a caller passes by habit. **Force does not bypass this
> leg**; it is the gate's only non-bypassable arm, because "pay the remaining fold synchronously" is
> coherent only when the fold has all its inputs.
>
> The period boundary convention is stable and used identically by the guards, the gate, and the sweep:
> a period covers `posted_at ∈ [start_date, end_date + 1)`.

## 8. Testing estate

Thirteen test binaries, all against `poc_v3_2`. **One test run in flight at a time** — they share the
database.

| Layer | Coverage |
|---|---|
| Acceptance (`ledger-direct`) | `acceptance_direct_methods`, `acceptance_staging_drain`, `acceptance_recalc_engine` (11 cases), `acceptance_recalc_stale_claim` (two deterministic multi-session interleavings), `acceptance_recalc_backpressure` (7 cases), `acceptance_close_period` (14 cases) |
| Acceptance (`ledger-feed`) | `acceptance_feed_ingest` |
| Property | `property_ledger_submit_trx`, `property_ledger_staging_drain` (100-case drain-vs-direct equivalence), `property_recalc_engine` (**R1–R9**), `property_close_period` (**C1–C7**), `property_backpressure`, `property_feed_ingest` |
| At scale | `ledger-bench soak` (whole architecture concurrently, then quiesce → verify → close → immutability probes), `ledger-bench verify` (**V1–V7** conservation + at-quiesce oracle equivalence + the drift distribution), `scripts/soak.sh`, `scripts/slo-sweep.sh` |

**The correctness anchor** is unchanged from the PoC line: the engine's output is validated equal to a
**full-opening strict replay** — an independent reference walk sharing only `banker_div`. Every
scheduler, checkpoint, and cadence optimization is measured against that replay, at unit scale in
`property_recalc_engine` R1 and at scale in `verify` V3/V4.

**Workload knobs that matter.** A constant-cost seed measures nothing: the harness carries a
receipt-cost-volatility knob (`low`/`med`/`high`/`trend`) and a backdated-event injector
(`--backdate-pct`, `--backdate-window-secs`) that makes business order diverge from commit order at a
controlled rate — the exact R-2 breaking input. `--pause-workers` / `--pause-feed` demonstrate the
two-gauge separation; `--midrun-close-at` runs an unforced close inside a rolled-back transaction as a
non-mutating gate probe.

**Two at-scale findings the small-case nets could not reach** are recorded in `bench/soak-results.md`:
the re-cost write amplification that filled a disk (§4 — which motivated backpressure and remains the
open write-amplification axis, `acct-m0ab`), and a generation-collision wedge in
`cost_layer_consumption` (§5, fixed with a deterministic two-session regression test).

## 9. Bounds and posture

**Q3a — the ratified posture, with bounds.** Alt C is ratified: the hot path records physical events
only for layer-tracked methods; recalc is the sole costing engine for them; WAC/STD/specific remain
**final on the hot path** for *cost*; close is a consistency gate plus a finalize stamp; force drains,
never bypasses. Ratification is **with bounds**, to be specified during hardening (natural home
`acct-63qs.6`): a recalc-lag SLO expressed on the G2 gauges, a close-cadence policy, and a sized
forced-close cost.

**These bounds bound *wrongness-exposure*, not throughput.** That is the substance of the Q13
amendment: how stale or provisional a mid-period cost may get is a **product** bound, in scope for this
spec, and §5a is the interface rule it constrains. The numbers are filled from the gated `acct-63qs.6`
baseline, not chosen up front. No TPS target exists or is wanted.

**Q3b — quantity is flagged, never gated.** The ledger flags negative inventory; it does not reject on
quantity. Gating is a document/seam concern, not a ledger concern.

> **Known deviation, stated plainly.** The posture binds *the ledger*, and the layer-tracked methods
> implement it: FIFO/LIFO appends take no gate and drive the aggregate negative with a flag. The
> **strict methods still gate quantity synchronously** — WAC in the depletion statement's `WHERE`,
> STD/specific in `plan_apply` — and the soak measured this in production shape: **84 WAC qty-gate
> rejects on the direct path and 87 qty-gate rejects on staging** in the headline run. The implementation is
> therefore not yet at the designed posture, and convergence Q3b directs the WAC qty-gate
> reconciliation **toward removal**. That follow-through has no issue carrying it yet and needs one.
> This is a named gap, not a second posture: nothing in this document should be read as endorsing a
> quantity gate as the ledger's steady state. Note the two
> axes are independent — a method can be *final-costed* on the hot path (Q3a) without *gating quantity*
> (Q3b); the strict methods currently do both, and only the second is at issue.

**Q4 — substrate.** Harden on the built pgrx artifact. `ledger_direct` is **shmem-free** and requires
no `shared_preload_libraries` entry; `ledger_feed` is an ordinary client of the logical-decoding SQL
interface. The substrate decision reopens only on **named triggers**: error-identity failure at the SPI
seam, PG-major-version friction, or upgrade-path cost. Absent one of those, do not reopen it.

**Q7 — currency.** The costing plane stays **single-currency as built**. Documents convert FX *before*
submitting inventory facts; the conversion contract at that seam is `acct-476a.3`'s currency half. Do
not re-import the parent repo's per-currency pools — that shape was a TigerBeetle-parity artifact and
carrying it here would re-import the parity tax the whole line exists to avoid.

## 10. Out of scope

- **App-tier partitioned consumer (E)** — structurally *enabled*, not instantiated. The staging-table
  pivot already reached its endpoint (Postgres as storage, ordinary drain); the only residual is where
  the committer loop runs (in-process versus a sharded worker fleet keyed by `hash(pool_id) % N`), a
  deployment choice that is additive later, not a substrate rewrite. Committer count and batch size are
  the scale knobs — the soak saturates the staging path at ≈450/s with two committers at batch 25, and
  the useful direction is same-pool batching, not bigger mixed batches.
- **TigerBeetle split (F)** — recorded and **rejected**. The alt-C insert-only path recovers TB's
  load-bearing write-path property inside a single datastore; the parent repo's Postgres-native,
  no-TB-parity direction stands.
- **Multi-currency** (Q7 fixes the seam instead), effective-dated standard costs, multi-tenant
  isolation, webhook delivery.
- **Period reopen** — out of scope pending the convergence **Q8** verdict, which `acct-1vur` decides.
- **Retention/pruning** of drained `ledger_inbox` rows and of superseded settlement generations —
  operational follow-ups (`acct-m0ab`).

---

## Appendix A — resolved design questions

The six questions the skeleton left open are resolved; each was settled in implementation and is
recorded here with its source.

| # | Question | Resolution | Where it lives |
|---|---|---|---|
| 1 | Recalc transport | `pgoutput` over the **SQL** logical-decoding interface (peek + explicit advance); streaming protocol deferred as a latency refinement | §6; `ledger-feed/src/consumer.rs`; migration `0013` |
| 2 | Worker model | **Continuous** default; all three shapes coexist as one engine — continuous `ledger_recalc_step` loops, on-demand `ledger_settle_pool`, periodic close | §5.1, §7; `scripts/run-recalc.sh` (default 4 workers) |
| 3 | Cadence + backpressure bound | Continuous drain; bounds **200 / 20** in `recalc_backpressure_config`, calibrated ≈3× the observed healthy global maximum | §5.3; migration `0018`; soak §8 |
| 4 | Materialization granularity | **Full layer-row materialization** at `pool_state.layer_id > 0` — the checkpoint that makes the steady-state pass O(new events) | §5.1 step 4; migration `0003` |
| 5 | Cross-pool scheduler | **Claim-driven** `SKIP LOCKED`, oldest-mark-first, with re-stamp-to-back anti-starvation | §5.1 steps 1 and 6; migration `0012` |
| 6 | Idempotency contract | **Generation-delta (Model 1)**: zero-delta passes write nothing and do not bump; a genuine re-cost posts exactly the inter-generation delta | §5.2; migration `0009`; `property_recalc_engine` R5 |

## Appendix B — generation counter overflow: a non-issue

`recalc_generation` appears on `pool_settlement`, `cost_settlement`, and `cost_layer_consumption`, all
`BIGINT`. The counter is per pool and is bumped **at most once per pass that actually wrote** —
zero-delta passes leave it untouched.

The arithmetic: `BIGINT` tops out at 9.22 × 10¹⁸. The headline soak ran 15 046 passes across 300 pools
in 90 s — an upper bound of ~0.56 generation bumps per second per pool, assuming every pass wrote
(most did not). At a sustained **1 bump/second/pool**, roughly double the measured rate and far above
any plausible steady state, exhausting the counter takes on the order of 10¹¹ years. Even at an
absurd 10⁶ bumps/second/pool — physically unreachable, since each bump is a committed transaction —
it takes ~10⁵ years.

No wraparound handling, no counter reset, and no migration to a wider type is warranted. The real cost
of high generation counts is **storage**, not overflow: `cost_settlement` and `cost_layer_consumption`
are append-only per generation, so re-cost-heavy workloads grow them without bound. That is the write
amplification measured in soak §4 and tracked as `acct-m0ab`, and it is a retention question.
