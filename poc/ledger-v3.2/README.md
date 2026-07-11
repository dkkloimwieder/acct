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

Phases 2–6 (hot path, feed, recalc engine, close, testing) not started.

## Stack

Rust + `sqlx` (raw SQL, compile-time-checked) + `sqlx-cli` migrations under `db/migrations/`, same as the
rest of the project. Database `poc_v3_2` on the dev container (`localhost:5111`). No task runners; scripts
under `scripts/` are the entry points (per project convention).
