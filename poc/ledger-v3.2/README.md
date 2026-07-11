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

Phases 4–6 (recalc engine, close, testing) not started.

## Stack

Rust + `sqlx` (raw SQL, compile-time-checked) + `sqlx-cli` migrations under `db/migrations/`, same as the
rest of the project. Database `poc_v3_2` on the dev container (`localhost:5111`). No task runners; scripts
under `scripts/` are the entry points (per project convention).
