# design-v3.2: cost-ledger implementation spec — the surviving architecture + recalc/close engine

> **Status: SKELETON (acct-q1oj, 2026-07-10).** This is the *plan* for the v3.2 implementation spec, not
> the finished spec. It fixes the section outline, consolidates the decided architecture inputs **by
> reference** to design-v3.1 (§16 posture / §17 feed / §18 gate / §19 recalc risk / §20 alternatives /
> §3.7 wire contract — all settled at the FEEDBACK-ARCH decision gate, do **not** re-litigate), and
> decomposes the one genuinely-undesigned component — the recalc/close engine — into filable sub-issues
> (acct-q1oj.1–.5). Each section below states what the finished spec must contain; the recalc-engine
> section (§5) carries the decomposition. Section bodies marked *[spec-writing]* are placeholders for the
> full v3.2 spec; sections marked *[decided — consolidate]* only assemble a settled decision.

## 1. Purpose and relationship to v3.1

**What v3.2 is.** design-v3.1 characterized Path C (the provisional hot path with deferred recalc) as a
PoC and then, at the FEEDBACK-ARCH decision gate (§18, dispositioned 2026-07-08), fixed the *surviving*
architecture. v3.2 is the implementation spec that turns that surviving architecture into something
buildable. Its hard core is the recalc/close engine — the component the PoC deliberately deferred (§7,
§13) and the one production ERPs actually sink cost into.

**What carries over from v3.1 (cite, do not restate).** The base schema (§2.1–§2.4), method semantics for
the strict-on-the-hot-path methods (WAC §3.1, STD §3.3, specific-id §3.4 — all produce their final cost
directly and need no reconciliation), `ledger-core` as the shared pure-Rust crate (§8), and the numeric /
banker_div discipline (§3.0). These are unchanged; v3.2 references them.

**What is superseded (do not carry forward).** The §6 shmem router/committer stack (`ledger-routed-c`);
the RMW-under-`pool_lock` protocol and its sorted-acquisition / deadlock-ordering discipline; and the
incoherent *cost-provisional / qty-strict / order-unordered* posture the PoC shipped. All three are
dominated — SPIKE-A, SPIKE-B, and the §16 alt-C endpoint respectively (§18). Physical deletion of the
`ledger-routed-c` crate is a **separate cleanup follow-up (acct-uena)**, not part of this spec.

**Relationship to design-v3.1.** This document does **not** re-derive the gate decisions. §16/§17/§18/§19/
§20/§3.7 of design-v3.1 are the decision record; v3.2 consolidates them into an implementable shape and
adds the recalc-engine design on top. Where a v3.2 section restates a decided decision it does so to make
the spec self-contained, always with the design-v3.1 cross-reference.

## 2. Schema deltas from v3.1  *[decided posture — spec-writing for DDL]*

The v2 schema base (§2 of design-v3.1) stands. v3.2 deltas, all downstream of the §16 posture:

- **Drop the hot-path cost leg (alt C, §16).** The hot path records only the physical event — append
  `trx` / `trx_line` with qty and observed cost, optionally fire the commutative aggregate delta — with
  no synchronous read, no qty gate, and no running-average maintenance as a correctness dependency. There
  is **one costing plane** (recalc), not a provisional plane trued up by an authoritative one.
- **Quantity is a flagged running signal, not a gate (§16).** A depletion beyond on-hand drives the
  aggregate negative and is *flagged*, not rejected — the §3.6 negative-inventory extension becomes the
  default posture. The conservation-invariant sweep (acct-0at4.5) guards the flagged negatives.
- **`posting_account_map` (§3.7).** Ledger-side GL account resolution keyed on `(sku_id, location_id)`,
  hydrated like `standard_cost`. Callers stop carrying chart-of-accounts knowledge. See §4.
- **Recalc state / bookkeeping tables** — the schema surface §7 sketches (a `cost_adjustment` enum value
  across `trx_type` / `line_type` / `posting_event_type`; a `cost_adjustment_id_seq`; possibly a
  denormalized `trx_line.posted_at` for business-time ordering; possibly `total_value` on `pool_state`;
  materialized layer rows at `pool_state.layer_id > 0`). Designed by child **(d) acct-q1oj.4** — DDL is
  spec-writing work, not fixed here.
- **`wal_level = logical`** (§17) — a prerequisite cluster setting for the recalc feed (§6). A WAL-volume
  cost this fsync-bound system already substantially pays.

## 3. Hot-path semantics  *[decided — consolidate §18 / SPIKE-A / SPIKE-B]*

Two surviving shapes, both PG-native, chosen per path:

- **Direct single-statement (SPIKE-B shape).** One commutative `UPDATE … WHERE qty − Δ >= 0` (banker_div-
  in-SQL + PG 18 `RETURNING old.unit_cost` for the depletion running-average) on PG's own row lock —
  byte-identical to the RMW baseline and ≥ its throughput in every regime at ~4–6 % less WAL. `pool_lock`,
  the sorted-acquisition protocol, and the single-pool deadlock discipline delete for the aggregate paths.
  Ref `bench/spike-b-results.md`.
- **Staging-table outbox (SPIKE-A shape).** Where batching / coalescing is wanted: the caller's enqueue is
  an in-tx heap `INSERT` into `ledger_inbox`; committers drain via `SELECT … FOR UPDATE SKIP LOCKED ORDER
  BY id`. Delivers atomicity with the caller, **per-pool FIFO by construction**, free observability (the
  row *is* the status), and WAL-as-recovery — replacing the §6 shmem apparatus wholesale. Ref
  `bench/spike-a-results.md`.

Failure / backpressure: the insert-only hot path absorbs backpressure gracefully (it is just `INSERT`s) —
relevant to the §5(c) backpressure lever. The finished spec details per-path failure semantics.

## 4. SPI surface v3.2  *[decided — consolidate §3.7]*

Callers send **inventory facts only**: `(pool_id, line_type, qty, unit_cost)`. The ledger resolves
`debit` / `credit` (and the STD `variance_acct`) from `posting_account_map`, hydrated at lock time like
`standard_cost`. One uniform rule owns direction (stored receipt-direction, swapped for depletions). A
touched pool with no `posting_account_map` row **fails loud** (`MissingPostingAccounts`); a missing STD
variance account fails loud (`MissingVarianceAccount`). No caller-supplied account fallback. This is the
v3.2 wire-contract change (§3.7 / §4 of design-v3.1); it is GL-routing config off the hot-path locking
critical section.

## 5. Recalc / close engine — THE HARD CORE

This is the one genuinely undesigned piece of the surviving stack (§19 / §20). The PoC validated the cheap
hot path and deferred recalc — the component that sinks real ERPs. Under alt C (§16) recalc is the **sole
costing engine**: the hot path posts no cost leg, so recalc produces the *only* authoritative valuation
and its backlog gates whenever any authoritative cost/GL is needed — not merely when a correction lands.

**The risk this section resolves (from §19, do not re-argue — design against it).**
- *Throughput inequality.* Recalc processes the **same event volume** as the hot path while doing strictly
  more work per line (walk layers, assign cost, post GL, materialize position), **sequential per pool**
  (an intrinsic serial fold), **concurrent with appends** (a moving tail). The #5 per-pool ceiling §18
  removed from the hot path *reappears here*, relieved of a latency SLA but setting the close-drain floor.
- *R-1 chronological re-sort.* Recalc must order each pool by `(pool_id, posted_at, id)` = business
  chronology, not the commit order the feed (§6) delivers — the feed fixes *delivery* ordering, not
  *costing* chronology (§17).
- *R-2 backdated receipt.* A receipt committing after events it should precede forces re-costing of
  already-costed depletions in its pool — so recalc is not a naïve forward scan and incremental streaming
  does not bound per-event work.
- *Quiet backlog.* Falling behind is silent: slot-lag pins WAL *and* starves the value plane → mid-period
  drift (biased under a trend, §19 table) compounds → close blocks (SAP CKMLCP precedent) → forced close
  emits one large one-signed valuation move. Detection signal is unified by §17: **slot-lag IS recalc-
  backlog** (`confirmed_flush_lsn` lag = single backlog gauge).

**Magnitude / acceptance target.** The acct-0at4.7 offline strict-FIFO replay oracle
(`bench/replay-oracle-results.md`) sizes provisional-vs-authoritative drift per basis / volatility. The
load-bearing finding is the **trend row** — drift is *directional*, not mean-zero (FIFO overstates, LIFO
understates, fixed standard worst at rel 61.72 %), so it accumulates into a real period-level
misstatement. That is the quantitative proof recalc is load-bearing, and the oracle is the correctness
acceptance for child (a).

**Decomposition (the filed sub-issues).**

| child | issue | scope |
|-------|-------|-------|
| (a) layer-walk + chronological re-sort (R-1) | **acct-q1oj.1** (P1) — **designed, see [`design-v3.2-recalc-a.md`](design-v3.2-recalc-a.md)** | Per-pool strict FIFO/LIFO layer math + variance vs the recorded provisional; the R-1 `(pool_id, posted_at, id)` re-sort; R-2 backdated re-cost via per-pool replay from a re-cost floor (full-replay baseline = the oracle, idempotent; checkpointing deferred to (b)); the real `fifo.rs`/`lifo.rs` that §8 left as `MethodMismatch` stubs. Validated against acct-0at4.7. **Foundational** — b/c/d/e build on it. Hands D1 (posted_at denorm) / D3 (per-layer linkage) to (d), D2/D4 (checkpoint) to (b). |
| (b) cross-pool scheduler + parallelization / materialization | **acct-q1oj.2** (P2) — **designed, see [`design-v3.2-recalc-b.md`](design-v3.2-recalc-b.md)** | Claim-driven `SKIP LOCKED` dirty-set (SPIKE-A shape) — per-pool single-ownership is intrinsic (real serial fold, **not** the killed m4g5 hot-path affinity lever), parallelize across pools; Pareto-hot-pool sets the close-drain floor. **Persisted layer state = the checkpoint** → incremental forward pass O(new events) is the steady state (resolves D2), full-opening replay the correctness fallback; re-cost floor replays from nearest checkpoint ≤ floor, one live checkpoint to start (resolves D4). Hands the D8 cursor-advance fork (WAL vs backlog gauge) + D11 historical-checkpoint depth to (c). |
| (c) cadence-vs-load control + quiet-backlog mitigation | **acct-q1oj.3** (P1) — **designed, see [`design-v3.2-recalc-c.md`](design-v3.2-recalc-c.md)** | Cadence = **default continuous drain** (cheap under (b) incremental + (d) no-op-free), force-sweep at close; per-pool parallelism ceiling forces **per-pool detection**; **lazy per-pool backpressure** as last resort (common path stays read-free, preserving alt-C §16). **D8 ratified: advance-on-ingestion + durable dirty-set** — WAL-safe; refines §17's single gauge into **two coupled gauges** (G1 cursor/ingestion lag, G2 recalc `settled_through` lag) — the one place recalc touches decided §17 (forward-pointer added; flagged for review). **D11 resolved: no historical checkpoints in v3.2** (backdate-into-closed-period needs period-reopen = out of scope §13). |
| (d) recalc state/bookkeeping schema + idempotent re-run | **acct-q1oj.4** (P2) — **designed, see [`design-v3.2-recalc-d.md`](design-v3.2-recalc-d.md)** | The §7 schema-add surface (`cost_adjustment` enum labels + `cost_adjustment_id_seq`; denormalized `trx_line.posted_at` + `(pool_id, posted_at, id)` index = D1; `cost_layer_consumption` linkage = D3; `pool_settlement` + append-only `cost_settlement`); **generation-delta** idempotency (Model 1) — a repeated pass posts zero net GL, a genuine re-cost posts exactly the inter-generation delta. Progress cursor is the §17 slot — **not** a home-grown watermark; `pool_settlement` is pool-side authoritative-state, distinct from stream progress. |
| (e) close-time semantics | **acct-q1oj.5** (P2) | The provisional/standard-valued → authoritative-valuation transition at close; close gating on recalc drain + the forced-close escape; the three §7 worker-model shapes (Oracle continuous / SAP on-demand `ledger_settle_pool` / Dynamics periodic Inventory-Close); the `accounting_period` close hook. |

Dependency shape: (a) is foundational; (b), (c), (d) depend on (a); (c) also on (b); (e) on (a) and (d).

## 6. Feed — logical-decoding slot consumer  *[decided — consolidate §17]*

The recalc feed is a **logical-decoding replication slot** delivering `trx_line` inserts in **commit
order** with a durable, resumable cursor (`confirmed_flush_lsn`). It is **not** a `WHERE id > watermark`
scan (the §14.6 identity-vs-commit-order silent-gap breakage) and **not** a hand-rolled safe-watermark /
settled-state column (both reimplement in app code what the slot gives for free). Logical decoding fixes
*delivery* ordering + durability; the within-pool **business-effective re-sort** for cost correctness is
recalc's job (child a), not the feed's. Slot-lag pins WAL and **is** the recalc-backlog gauge (§17 / §5).
Deferred sub-choice (recalc-design-time): transport — `pgoutput` + decoding consumer (lean start) vs a
custom output plugin, moved to only if consumer-side filter/projection is measured to matter.

## 7. Testing strategy  *[reuse survivor oracles + add recalc-correctness]*

The gate re-triage (§18 table) kept three architecture-agnostic oracles; v3.2 reuses them:

- **acct-0at4.4 sequential reference oracle** (exact + envelope mode) — validates the alt-C provisional
  plane.
- **acct-0at4.5 conservation-invariant sweep** — *more* central under alt C (negative qty allowed →
  conservation is the guard over flagged negatives).
- **acct-0at4.7 offline strict-FIFO replay oracle** — the recalc correctness acceptance (child a) and the
  drift magnitude (§5 / §19).

Added for v3.2: recalc-output-vs-strict-replay correctness; a cadence-vs-load soak (child c) exercising
the quiet-backlog chain and the `confirmed_flush_lsn` gauge; open-loop load-gen + SLO methodology
(acct-0at4.8, architecture-agnostic) against the surviving direct + staging paths.

## 8. Out of scope / deferred  *[decided — consolidate §20 / §13]*

- **E — app-tier partitioned consumer:** structurally *enabled*, not instantiated — kept as the documented
  scale-out path (§20.1). The SPIKE-A pivot already reached E's endpoint (PG-as-storage, ordinary drain);
  the only residual is *where the committer loop runs* (in-process vs sharded worker fleet keyed by
  `hash(pool_id) % N`) — a deployment choice, additive later, not a substrate rewrite.
- **F — TigerBeetle split:** recorded, **rejected**, parent commitment intact (§20.2). The alt-C insert-
  only path recovers TB's load-bearing write-path property within a single datastore; the parent `acct`
  repo's Postgres-native / no-TB-parity direction stands.
- **Per §13:** negative inventory as a first-class production feature beyond the alt-C flag; multi-
  currency; effective-dated standard costs; period-close mechanics beyond v3.2's own close semantics
  (child e); multi-tenant isolation; webhook delivery.
- **`ledger-routed-c` physical deletion:** separate cleanup follow-up **acct-uena** (not this spec).

## 9. Open questions

1. **Recalc transport** — `pgoutput` vs custom output plugin (§6 / §17). Recalc-design-time; lean `pgoutput`.
2. **Worker model** — continuous / on-demand / periodic (child e / §7); the cadence knob (child c)
   parameterizes across them, but a default posture must be chosen.
3. **Cadence default + backpressure bound** — the concrete recalc-period default and the slot-lag threshold
   that triggers hot-path backpressure (child c).
4. **Materialization granularity** — full layer-row materialization (`pool_state.layer_id > 0`) vs running-
   position-only, and the storage/replay tradeoff (child b / child d).
5. **Cross-pool scheduler assignment** — feed-driven vs claim-driven pool→worker assignment, and hot-pool
   starvation avoidance (child b).
6. **Idempotency proof obligation** — the exact re-run contract (crash, backdated re-cost, forced re-close)
   that guarantees no double-posted GL (child d).
