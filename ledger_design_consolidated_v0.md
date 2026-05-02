# ERP Ledger — Consolidated Review and Redesign

Version: 0.1
Status: Working document. Supersedes:
- `ledger_inventory_design_spec_v0.md` (v0.1 design — TB-parity)
- `phased_migration_spec_v0.md` (v0.1 migration roadmap — Postgres → TB)
- `postgres_native_design_v0.md` (redesign argument)
- `spec_review_v0.md` (review of v0.1)

This is a single working document covering: the v0.1 baseline as proposed, a critical review of that baseline, a root-cause argument for redesign, a Postgres-native v0.2 design, a §△-item resolution scoreboard, and the open questions that gate v0.2.

---

# Part 0 — Executive summary

The original v0.1 specs propose a Postgres ledger that is byte-for-byte semantically equivalent to TigerBeetle, plus a phased migration roadmap that makes Postgres the day-1 implementation and TB an "if needed" Phase 3+ destination. The intent is good — buy optionality cheaply by enforcing discipline now.

The execution has three structural problems:

1. **Three correctness blockers.** The self-pending reservation pattern is mechanically impossible in either system; multiple linked-batch patterns require read-then-write that the same spec forbids; the Phase 0 double-entry invariant test is unsound for multi-currency.
2. **Material under-scoping.** The TB-flag-parity write function is described as "~200 LoC" but is realistically 1,500–2,500 LoC of conformance-tested PL/pgSQL. The Phase 0 timeline is wrong by 3–5×.
3. **Compound parity tax.** Roughly 60% of the design's complexity exists to keep TB optional. Of 30 catalogued §△ open issues, 15 dissolve when the parity constraint is dropped. The remainder split into trivial implementation choices and genuine business decisions.

The recommendation in this document: drop TB parity, build on Postgres natively, preserve a CDC seam (logical replication) so a downstream specialized store can be added if and only if the workload demands it. Phases 3–5 of the v0.1 roadmap retire. Phase 0 timeline shrinks from 6–10 weeks (realistic) to ~3 weeks. Phase 2 (the projection layer) becomes optional infrastructure applied per-need, not a universal architecture.

The case for or against this recommendation hinges on two questions, repeated in §VII:
- Is TB optionality a hard requirement, or a "nice to have"?
- What's the realistic 24-month TPS projection?

If TB optionality is hard-required (regulatory, strategic), most of this document is moot — but B1, B2, B3 still need fixing in v0.1. If it's "nice to have," the v0.2 redesign drops the tax and ships faster.

---

# Part I — v0.1 baseline (orientation)

The v0.1 design is split across two specs. This section summarizes their architecture so the rest of this document is self-contained.

## I.1 Architecture

```
 Clients ──► API tier ──► Postgres (documents, outbox, metadata)
                    │
                    └───► TigerBeetle (accounts, transfers, balances)
                             │
                       CDC (AMQP) ──► RabbitMQ ──► Projector ──► Postgres read model
                                                              └──► ClickHouse (reports)
```

**Two-system split with a one-way invariant.** Documents and outbox in Postgres; ledger state in TigerBeetle; reads served by an async projection fed via AMQP CDC.

In Phase 1 (the actual day-1 build), the same architecture runs entirely in Postgres: a `ledger_accounts`/`ledger_transfers` schema mirrors TB's data shape exactly (`NUMERIC(39)` for u128 ID/amount, six fixed transfer fields, bit-field flags), and a `post_transfer_batch` PL/pgSQL function reproduces TB's transfer semantics (linked, pending, post_pending, void_pending, balancing, closing, imported flags).

**Migration roadmap (Phase 0 → 5):**

| Phase | Scope | Timeline |
|-------|-------|----------|
| 0 | Postgres ledger schema + write function + invariant tests | 2–3 weeks (claimed) |
| 1 | Full application on Postgres ledger | 3–4 months |
| 2 | Async projection layer (CDC + projector + read tables) | overlapping |
| 3 | TB shadow mode (if entry criteria met) | 2–3 months |
| 4 | Per-subledger cutover to TB | 2–4 months |
| 5 | Steady-state hybrid operations | ongoing |

## I.2 Domain in scope

- SKU × location quantity tracking; per-routing-step WIP.
- Standard ERP document lifecycle: WO, SO, TO, PO.
- Double-entry GL with multi-currency.
- Reservation/allocation.
- Reporting via CDC projection.
- Commodity provisional pricing (§17 of the design spec).

## I.3 Out of scope (deliberately)

- Lot/serial identity in TB. (Lives in Postgres with reconciliation.)
- FIFO/LIFO cost layering in TB.
- FEFO ordering.
- OLAP in TB.
- Cross-cluster atomicity.
- Browser-direct TB access.

## I.4 Key design decisions in v0.1

- Client-generated u128 (NUMERIC(39)) IDs for transfers and accounts; same ID survives migration to TB.
- Lazy account materialization via a `tb_account_map` table.
- Outbox pattern for the write path: Postgres documents + outbox row → ledger write.
- Idempotency via `ON CONFLICT (id) DO NOTHING` semantics matching TB's `ok`/`exists`.
- Reservations as self-pending transfers on the Available account (§5.3 of the design spec).
- Counterparty attribution via hashing into `user_data_64` (§3.4).
- 17 explicit §△ deferral markers in the design spec, 13 in the migration spec.

These decisions are the inputs to Part II.

---

# Part II — Critical review of v0.1

Findings are grouped by severity. Severity legend:

- **Blocker** — design will not work as written; must be resolved before code.
- **Major** — internally contradictory, materially under-scoped, or operationally unsafe.
- **Minor** — naming, completeness, or clarity issue; cheap to fix.
- **Note** — observation worth recording, no required action.

## II.1 Blockers

### B1 — Self-pending reservation pattern is mechanically impossible

**Source:** `ledger_inventory_design_spec_v0.md` §5.3, §△-4.

**Claim:**
```
T_reserve  ledger=1 credit (sku, loc) Available, debit (sku, loc) Available
           flags=pending, timeout=<reservation_ttl_seconds>
```

A transfer with `debit_account_id == credit_account_id` is rejected by TigerBeetle (`accounts_must_be_different`). The Postgres ledger spec enforces the same rule directly: `phased_migration_spec_v0.md:118` — `CHECK (debit_account_id <> credit_account_id)`.

The pattern as written cannot post in *either* system. The §△-4 note frames it as a tradeoff between "self-pending" and "Available/Reserved pair," but the self-pending branch is not a viable option.

**Secondary issue.** The "Promisable" formula given in §5.3 — `credits_posted − debits_posted − debits_pending` — has the wrong sign for an asset-normal account flagged `credits_must_not_exceed_debits`. On-hand is `debits_posted − credits_posted`; promisable would subtract pending *credits* (outflows reserved), giving `debits_posted − credits_posted − credits_pending`.

**Resolution direction:** drop the self-pending option entirely. v0.2 (Part IV) replaces it with a first-class `inventory_reservations` table.

### B2 — Read-then-write contradicts §2

**Source:** `ledger_inventory_design_spec_v0.md` §2 ("NEVER perform a TB lookup that gates a subsequent TB write on the hot path") versus §5.4, §5.5, §6.2.

**Patterns that violate the rule:**

| Section | Field | Source of value |
|---------|-------|-----------------|
| §5.4 `OP_MOVE T2` | `accumulated_cost_per_unit * qty` | `WIP_OpNN_Value.balance / WIP_OpNN.qty` |
| §5.4 `WO_COMPLETE T3` | `residual` | `WIP_Op30_Value.balance − qty * std_cost` |
| §5.5 `SCRAP T2` | `accumulated_cost` | `WIP_OpNN_Value.balance / WIP_OpNN.qty * scrap_qty` |
| §6.2 WAC alternative | unit cost at issue | `Raw_Inv_Value.balance / qty_on_hand` |

Each value is computed from the current balance of an account that is *also* mutated in the same linked batch. Two parts of the same spec disagree.

**Why it matters.** TB has no row locks; under concurrency, two op-moves on the same WIP_Op account can read pre-update balances and produce diverging accumulated-cost figures. Phase 1 Postgres mitigates with `FOR UPDATE`, but post-migration to TB the pattern silently becomes racy.

**Resolution direction:** in PG-native v0.2, `FOR UPDATE` on the locked accounts row makes read-then-write safe. The §2 prohibition is dropped because it was specifically a TB constraint.

### B3 — Phase 0 double-entry invariant is unsound for multi-currency

**Source:** `phased_migration_spec_v0.md` §3.5, §4.7.

**Claim:** `SUM(debits_posted) = SUM(credits_posted)` across all accounts, always.

**Problem.** Once ledger 1 (qty), ledger 840 (USD), ledger 978 (EUR) coexist, the global sum mixes incompatible units. Either fails on every multi-currency batch (false positives) or sums to zero by numerical coincidence and *masks* a real per-ledger imbalance.

**Resolution direction:**

```sql
SELECT ledger_kind, currency, SUM(debits_total) - SUM(credits_total) AS imbalance
FROM accounts
GROUP BY ledger_kind, currency
HAVING SUM(debits_total) <> SUM(credits_total);
```

Must return zero rows. Applied as the standing daily reconciliation invariant in v0.2.

## II.2 Major findings

### M1 — `_apply_transfer_effect` is materially under-scoped

**Source:** `phased_migration_spec_v0.md` §3.2.

**Claim:** "(full implementation: ~200 LoC, branching on transfer flags)".

**Problem.** Faithful semantic parity with TB's transfer flag matrix is the central engineering risk of the entire roadmap. The flag combinations:

| Flag | Behavior |
|------|----------|
| `pending` | Increments `*_pending`, NOT `*_posted`. Records timeout. |
| `post_pending_transfer` | Decrements `*_pending`, increments `*_posted`. Optional partial amount. |
| `void_pending_transfer` | Decrements `*_pending`. Idempotent against expiry. |
| `balancing_debit` | Amount = `min(requested, debit_account.available_under_limit)`. |
| `balancing_credit` | Symmetric. |
| `closing_debit` / `closing_credit` | Sets `account.flags |= closed` as a side effect. |
| `imported` | User-supplied timestamps; monotonicity within batch; bypasses some validation. |
| `linked` | Chain failure rolls back the entire batch. |

Plus the result-code matrix: `ok`, `exists`, `linked_event_failed`, `linked_event_chain_open`, `pending_transfer_not_found`, `pending_transfer_already_posted`, `pending_transfer_already_voided`, `pending_transfer_expired`, `exceeds_credits`, `exceeds_debits`, `accounts_must_have_the_same_ledger`, `account_not_found`, `transfer_must_have_the_same_ledger_as_accounts`, `imported_event_timestamp_*` (several), and a dozen others.

Realistic size: 1,500–2,500 LoC PL/pgSQL. Phase 0 timeline (2–3 weeks) is wrong by 3–5×.

**Resolution direction:** in v0.2 the flag matrix is gone. The write function is ~70 lines (Part IV §3.1) because Postgres transactions handle linking, row-level state replaces pending semantics, and `BIGSERIAL` replaces client IDs.

### M2 — `NUMERIC(39)` is an unacknowledged performance compromise

**Source:** `phased_migration_spec_v0.md` §3.1.

| Operation | NUMERIC(39) | BIGINT | Ratio |
|-----------|-------------|--------|-------|
| Add/subtract | software | hardware | ~5–10× |
| Index width | 16+ bytes | 8 bytes | 2× |
| HOT update friendliness | worse | better | — |

Inventory qty fits in INT4. Value in minor currency units fits in BIGINT (~$92 quadrillion cap). The NUMERIC(39) choice exists for migration symmetry with TB's u128, not for application need.

**Resolution direction:** v0.2 uses `BIGINT` throughout. Stated as an explicit choice with the throughput-ceiling tradeoff documented (Part VI).

### M3 — `user_data_128 UUID` is more restrictive than `u128`

**Source:** `phased_migration_spec_v0.md` §3.1.

`UUID` enforces version/variant bit patterns; not all u128 values are valid UUIDs. If `user_data_128` is ever populated from anywhere other than a UUID generator (hash, ULID-shaped TB id, external u128), the schema rejects it.

**Resolution direction:** v0.2 replaces the polymorphic `user_data_128` with a typed `document_id UUID` column scoped per `document_kind` — a real reference, not a polymorphic field.

### M4 — Phase 4 Step 2 "halt outbox worker briefly (~30s downtime)" is glossed over

**Source:** `phased_migration_spec_v0.md` §7.2.

The roadmap's whole *justification* for Phase 3+ is sustained 10K TPS. 30 seconds at that rate = 300K queued events at the API tier. None of: API rejecting writes for 30s; in-memory buffering; outbox stalling; is acceptable without explicit handling.

**Resolution direction:** v0.2 retires Phase 4 entirely. If the team retains the roadmap, replace the halt with a streaming cutover (snapshot via `LOCK TABLE … IN SHARE MODE` in milliseconds, parallel imported-transfer ingestion, atomic flip).

### M5 — Phase 3 entry gates AND three correlated signals

**Source:** `phased_migration_spec_v0.md` §6.1.

Lock contention, P99 latency, and TPS-with-hot-account-skew are correlated, not independent. The three-way AND looks rigorous but is mostly aesthetic.

**Resolution direction:** v0.2 retires Phase 3. If the roadmap is retained, replace with a single composite signal calibrated against production data ("sustained write-side contention on hot accounts that is not improved by index/sharding/tuning, demonstrated over a 4+ week peak window").

### M6 — Commodity §17.6 WAC default leaves permanently-wrong unit costs on consumed inventory

**Source:** `ledger_inventory_design_spec_v0.md` §17.6.

The aggregate true-up at settlement (debit COGS, amount = Δ_consumed) does not restate per-shipment cost. Inventory shipped during the unsettled window has permanently wrong unit cost on its COGS row. §11 reconciliation can't catch it because the *sums* tie. Auditors will see it.

**Resolution direction:** add a materiality threshold (suggested 5%): if aggregate Δ exceeds the threshold, book a `cost_restate` reversal-and-rebook against affected COGS events. Below the threshold, document explicitly as a known limitation in accounting policy. This is a v0.2 policy decision, not a structural change.

### M7 — `expire_pending_transfers` re-enters `post_transfer_batch` under load

**Source:** `phased_migration_spec_v0.md` §3.3.

The expiry worker calls `post_transfer_batch` once per expired pending. Under reservation churn (cart timeouts, flash-sale expiry storms), this becomes a serial bottleneck.

**Resolution direction:** v0.2 replaces pending transfers with first-class reservations (Part IV §4). Expiry becomes a single SQL `UPDATE` statement; no worker re-entry.

## II.3 Minor findings

### m1 — Account taxonomy gaps

`ledger_inventory_design_spec_v0.md` §3.2 omits accounts that appear later: `Physical_Adj_Pool(sku)` (§5.8), `Inventory_Adj_Expense` (§5.8), `FX_Revaluation` (§9). The `Creation_Void` "fixed known id" should specify what the id is for reproducible deployments.

**Resolution:** v0.2's `account_kind` enum is exhaustive (Part IV §1.1).

### m2 — Naming inconsistencies

- "Quarantine pool" is `(sku, hold_pool)` in §3.2 but `(sku, Quarantine_Pool)` in §5.6.
- "Scrap" is `(sku, scrap_pool)` in §3.2 but `(parent, Scrap_Pool)` in §5.5.
- `tb_account_map` carried into Phase 1 with a "it's an abstraction" footnote.

**Resolution:** v0.2 uses `snake_lower` consistently and drops `tb_account_map` (no second system to map to).

### m3 — `code` u16 reserve-range commentary is unnecessary

The 65,536-value space will never be approached. Reserve-range editorial structure is for humans, not a constraint that needs justification.

**Resolution:** v0.2's `transfer_reason` enum names every reason explicitly.

### m4 — §5.7 AR/AP payment is too terse

Every other §5.x section spells out the linked batch in a consistent format; §5.7 says only "Standard double-entry, ledger=ccy only."

**Resolution:** v0.2 (Part IV §3) spells out all transaction patterns including AR/AP payment.

### m5 — `flags.history` semantics across docs

Design spec §3.5 says set the flag on accounts feeding period-end snapshots. Phase 0 schema marks the bit "reserved; no-op in PG phase, retained for parity." Period-snapshot strategy in Phase 1 is unstated.

**Resolution:** v0.2 drops the flag entirely. Period snapshots are produced by a snapshot job at close (Part IV §6).

### m6 — Phase 1 perf claims need sourcing or hedging

- "10–50× speedup over per-row calls" — depends on per-call overhead. Hedge to "5–20×" or measure.
- "synchronous_commit = local … 2–5×" — workload-dependent.
- "COPY … 3–5× faster" — true for very large batches; small batches are comparable to multi-row INSERT.

**Resolution:** v0.2 (Part IV §8) restates throughput as envelope estimates, not benchmarks.

### m7 — Reverse-migration §8.2 is hand-waved

"Bulk-insert opening-balance transfers into Postgres at T=0" requires `imported`-equivalent semantics in `post_transfer_batch` — currently unimplemented in the v0.1 Phase 0 schema.

**Resolution:** v0.2 retires reverse migration. If retained, the imported semantics must be specified explicitly (currently a TODO in v0.1).

### m8 — Cross-system batch policy strategy B sacrifices atomicity quietly

`phased_migration_spec_v0.md` §7.3 strategy B ("outbox two-phase with idempotent retry") is correct as a mechanism but does not state the consequence: a *visible window* where Postgres has the debit half and TB does not yet have the credit half. Reports during the window see imbalance; reconciliation alerts fire.

**Resolution:** v0.2 retires hybrid operation. If retained, document the in-flight window and projection-masking explicitly.

## II.4 Notes

### N1 — The §△ deferral mechanism is excellent

Both specs use §△ markers consistently and gather them in single indexes. The right shape for an in-flight design.

### N2 — Thirty open §△ items is a smell, not a fault

Some are *gating* (cross-system policy, attribution policy, period lock, reverse migration playbook) and need answers before code. Others are genuine "revisit with data" items. Volume is high because TB's primitives don't fit the domain cleanly.

**Suggestion:** split the §△ list into "gating" (must close before Phase N) and "monitoring" (close on signal).

### N3 — Outbox-as-shared-substrate is the strongest idea in the migration spec

The reason migration is "mechanical, not architectural" is that the outbox is the seam — same rows, different sink. v0.2 preserves the outbox pattern (with a question raised about whether it's strictly required in a single-database world) and identifies logical replication as the *real* migration seam.

### N4 — Per-subledger cutover with reverse-shadow is correctly identified as the right shape

Ability to roll back at every step is the load-bearing property. If the v0.1 roadmap is retained, do not let this weaken under schedule pressure.

### N5 — Explicit non-decisions sections are excellent

`ledger_inventory_design_spec_v0.md` §15 and `phased_migration_spec_v0.md` §11 are good practice. v0.2 inherits and extends them (Part IV §11).

---

# Part III — Root cause: the TigerBeetle parity tax

The v0.1 specs make one structural bet: **the Postgres implementation should be a drop-in for TigerBeetle, byte-for-byte semantically.** Every other choice is downstream.

## III.1 Itemized parity tax

| Constraint inherited from TB | Cost in Postgres |
|------------------------------|------------------|
| `NUMERIC(39)` on all amount/ID columns | 5–10× slower arithmetic, 2× wider indexes vs `BIGINT` |
| Six fixed transfer fields (`user_data_*`, `code`, `flags`) | Loss of FK integrity; polymorphism in indexing; counterparty hashing required (§△-5) |
| Bit-field `flags` column | Bit-arithmetic in CHECK constraints; harder to read in queries |
| Linked-chain rollback via SQLSTATE | ~150 LoC of plpgsql machinery duplicating what `BEGIN/COMMIT` already does |
| Client-generated u128 IDs | Re-implements `BIGSERIAL` with worse cache locality |
| Pending/post/void primitive | Awkward for reservations (which carry business state); requires expiry worker; complicates balance reads |
| Self-pending reservations (B1) | Forces a redesign anyway; pretending it's a tradeoff is costly |
| Lazy account materialization | Conditional `create_accounts` in every batch; map table; cache invalidation logic |
| `flags.history` | Reserved bit in PG that does nothing until Phase 4+ |
| `flags.imported` | Required for backfill but not implemented in Phase 0 (m7) |
| Async CDC + projector for reads | Latency + reconciliation overhead + operational footprint |
| `user_data_128 = document_id` UUID | Constrains values; FK lost (M3) |
| `code` u16 enum | Solves a problem TB has, not one PG has |
| Hot-account contention as Phase 3 trigger | Shards "removed at migration" become permanent; framing is wrong |
| `_apply_transfer_effect` faithful flag matrix | 1,500–2,500 LoC of conformance-tested PL/pgSQL (M1) |
| Read-then-write prohibition | Forces awkward patterns for cost computation (B2) |

The parity tax is also a **reasoning tax**. Every developer on the project has to learn TB's semantic model to write Postgres code, even though TB will never be deployed in many timelines. That cognitive overhead compounds.

## III.2 Why the bet underwhelms for this workload

1. The workload (ERP/inventory ledger, ~10K TPS upper-bound estimate, multi-currency, WIP, commodity pricing) is squarely in Postgres's wheelhouse.
2. The v0.1 TB entry criteria require sustained 10K TPS with hot-account skew — most ERP-style businesses never reach this.
3. The §△-list catalogues 30 open issues; the *majority* exist because TB's primitives don't fit the domain cleanly.
4. Dropping the parity constraint resolves several of those §△ items directly, eliminates ~40% of the implementation work, and produces a system that is faster, simpler, and more debuggable than either pole of the current design.

The right framing is not "Postgres ledger that becomes TigerBeetle later." It is "Postgres ledger sized for the workload, with a documented escape hatch if the workload changes." The escape hatch is real (CDC out to a downstream system, sharding by ledger, etc.) but doesn't dictate the day-1 schema.

---

# Part IV — Postgres-native v0.2 design

This section specifies the v0.2 design end-to-end. It is written to be implementable as a single document.

## §1. Schema

### 1.1 Accounts

```sql
CREATE TYPE account_kind AS ENUM (
  -- Inventory quantity
  'stock_available', 'stock_reserved', 'stock_quarantine', 'stock_scrap',
  'stock_in_transit', 'stock_consumed', 'stock_wip',
  -- Counterparty (qty side, optional)
  'vendor_pool', 'customer_pool',
  -- Value
  'inv_value_raw', 'inv_value_wip', 'inv_value_fg', 'cogs',
  'ap', 'ap_unsettled', 'ar', 'cash',
  'revenue', 'sales_tax_payable',
  'labor_applied', 'oh_applied', 'labor_expense',
  'variance_ppv', 'variance_muv', 'variance_lv', 'variance_ohv',
  'variance_scrap', 'variance_wo_close', 'variance_price_settlement',
  'fx_revaluation', 'inv_adj_expense',
  -- System
  'creation_void'
);

CREATE TYPE balance_direction AS ENUM ('debit', 'credit', 'unrestricted');

CREATE TABLE accounts (
  id              BIGSERIAL PRIMARY KEY,
  kind            account_kind NOT NULL,
  ledger_kind     TEXT NOT NULL CHECK (ledger_kind IN ('qty', 'value')),
  currency        CHAR(3),                    -- NULL for ledger_kind='qty'
  -- Domain references — proper FKs
  sku_id          UUID REFERENCES skus(id),
  location_id     UUID REFERENCES locations(id),
  routing_op      INT,                        -- WIP only
  counterparty_id UUID,
  -- Balance enforcement
  normal_side     balance_direction NOT NULL,
  -- Materialized balances, maintained by the write function
  debits_total    BIGINT NOT NULL DEFAULT 0 CHECK (debits_total  >= 0),
  credits_total   BIGINT NOT NULL DEFAULT 0 CHECK (credits_total >= 0),
  -- Lifecycle
  is_closed       BOOLEAN NOT NULL DEFAULT FALSE,
  closed_at       TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (
    CASE normal_side
      WHEN 'debit'  THEN credits_total <= debits_total
      WHEN 'credit' THEN debits_total  <= credits_total
      ELSE TRUE
    END
  ),
  CHECK (
    (ledger_kind = 'value' AND currency IS NOT NULL) OR
    (ledger_kind = 'qty'   AND currency IS NULL)
  )
);

CREATE UNIQUE INDEX accounts_stock_avail_uk
  ON accounts (sku_id, location_id) WHERE kind = 'stock_available' AND NOT is_closed;
CREATE UNIQUE INDEX accounts_wip_uk
  ON accounts (sku_id, routing_op) WHERE kind = 'stock_wip' AND NOT is_closed;
CREATE UNIQUE INDEX accounts_value_uk
  ON accounts (kind, sku_id, currency)
  WHERE ledger_kind = 'value' AND sku_id IS NOT NULL AND NOT is_closed;
CREATE INDEX accounts_kind ON accounts(kind) WHERE NOT is_closed;
CREATE INDEX accounts_counterparty
  ON accounts(counterparty_id) WHERE counterparty_id IS NOT NULL;
```

**What changed vs v0.1:**
- `BIGSERIAL` IDs. Half the index width of `NUMERIC(39)`.
- `account_kind` enum replaces `code` + flag conventions. Indexable, foreign-keyable.
- `sku_id`, `location_id`, `routing_op`, `counterparty_id` are real FKs. Hash collision risk eliminated.
- `normal_side` enum + plain CHECK replaces flag-bit arithmetic.
- `BIGINT` balances. Inventory qty caps at ~9.2 × 10¹⁸; value in minor currency units caps at ~$92 quadrillion.
- `is_closed BOOLEAN` instead of bit flag.
- `flags.history` dropped. Period snapshots produced by snapshot job.

### 1.2 Transfers

```sql
CREATE TYPE transfer_reason AS ENUM (
  'po_receipt', 'po_receipt_provisional', 'po_return_to_vendor', 'customer_return',
  'so_ship', 'rm_issue_to_wo',
  'to_release', 'to_receipt', 'bin_move',
  'wo_start', 'op_move', 'wo_complete', 'rework',
  'labor_apply', 'oh_apply',
  'quarantine', 'release_from_quarantine', 'scrap', 'damage',
  'ar_invoice', 'ar_payment', 'ap_bill', 'ap_payment',
  'ppv', 'muv', 'lv', 'ohv', 'scrap_v', 'wo_close_v', 'price_settlement',
  'cycle_count_adj', 'cost_restate', 'reversal',
  'fx_leg', 'fx_spread',
  'po_settlement',
  'price_trueup_inventory', 'price_trueup_cogs', 'price_trueup_wip'
);

CREATE TABLE transfers (
  id                BIGSERIAL PRIMARY KEY,
  reason            transfer_reason NOT NULL,
  document_kind     TEXT NOT NULL,
  document_id       UUID NOT NULL,
  document_line_id  UUID,
  debit_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  credit_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount            BIGINT NOT NULL CHECK (amount > 0),
  routing_op        INT,
  counterparty_id   UUID,
  period_id         BIGINT NOT NULL REFERENCES periods(id),
  business_date     DATE NOT NULL,
  idempotency_key   UUID NOT NULL UNIQUE,
  posted_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  posted_by         UUID NOT NULL,
  CHECK (debit_account_id <> credit_account_id)
) PARTITION BY RANGE (posted_at);

CREATE INDEX transfers_document  ON transfers(document_kind, document_id, posted_at);
CREATE INDEX transfers_debit_ts  ON transfers(debit_account_id, posted_at DESC);
CREATE INDEX transfers_credit_ts ON transfers(credit_account_id, posted_at DESC);
CREATE INDEX transfers_reason_ts ON transfers(reason, posted_at);
CREATE INDEX transfers_counterparty
  ON transfers(counterparty_id) WHERE counterparty_id IS NOT NULL;
CREATE INDEX transfers_routing_op
  ON transfers(routing_op) WHERE routing_op IS NOT NULL;
```

**What changed vs v0.1:**
- `document_id UUID` is a real reference per `document_kind`. WO cost trail = single B-tree lookup.
- `routing_op INT` is a column with its own index, not a `user_data_32` reinterpretation.
- `counterparty_id UUID` is a real value, indexable, joinable. Hash collisions impossible.
- `period_id` is mandatory and FK-enforced. Period lock at API (§△-10) is now solved at the schema level.
- `idempotency_key UUID` is the per-event idempotency mechanism, decoupled from PK.
- `posted_by` audit column is mandatory.
- Monthly partitioning by `posted_at` for time-range query efficiency and cheap archival.

**No `flags`. No `user_data_*`. No `code` u16. No `pending_id`, `timeout_seconds`, or pending/post/void flag bits.**

### 1.3 Inventory reservations (replaces self-pending pattern)

```sql
CREATE TYPE reservation_status AS ENUM (
  'active', 'allocated', 'cancelled', 'expired'
);

CREATE TABLE inventory_reservations (
  id              UUID PRIMARY KEY,
  sku_id          UUID NOT NULL REFERENCES skus(id),
  location_id     UUID NOT NULL REFERENCES locations(id),
  qty             BIGINT NOT NULL CHECK (qty > 0),
  so_id           UUID NOT NULL REFERENCES sales_orders(id),
  so_line_id      UUID NOT NULL,
  status          reservation_status NOT NULL DEFAULT 'active',
  expires_at      TIMESTAMPTZ NOT NULL,
  reserved_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  resolved_at     TIMESTAMPTZ,
  unit_price      BIGINT,
  notes           TEXT
);

CREATE INDEX rsv_sku_loc_active
  ON inventory_reservations(sku_id, location_id) WHERE status = 'active';
CREATE INDEX rsv_so      ON inventory_reservations(so_id);
CREATE INDEX rsv_expires ON inventory_reservations(expires_at) WHERE status = 'active';
```

### 1.4 Periods

```sql
CREATE TABLE periods (
  id        BIGSERIAL PRIMARY KEY,
  code      TEXT NOT NULL UNIQUE,
  opens_at  DATE NOT NULL,
  closes_at DATE NOT NULL,
  closed_at TIMESTAMPTZ,
  closed_by UUID,
  CHECK (opens_at <= closes_at)
);
```

### 1.5 FX rates

```sql
CREATE TABLE fx_rates (
  id            BIGSERIAL PRIMARY KEY,
  from_currency CHAR(3) NOT NULL,
  to_currency   CHAR(3) NOT NULL,
  rate          NUMERIC(20, 10) NOT NULL,
  effective_at  TIMESTAMPTZ NOT NULL,
  source        TEXT NOT NULL
);
CREATE INDEX fx_rates_lookup ON fx_rates(from_currency, to_currency, effective_at DESC);
```

### 1.6 Period snapshots

```sql
CREATE TABLE period_snapshots (
  period_id     BIGINT REFERENCES periods(id),
  account_id    BIGINT REFERENCES accounts(id),
  debits_total  BIGINT NOT NULL,
  credits_total BIGINT NOT NULL,
  PRIMARY KEY (period_id, account_id)
);
```

### 1.7 Commodity receipts (provisional pricing cohort ledger)

```sql
CREATE TABLE commodity_receipts (
  id                          UUID PRIMARY KEY,
  po_id                       UUID NOT NULL REFERENCES purchase_orders(id),
  po_line_id                  UUID NOT NULL,
  sku_id                      UUID NOT NULL,
  qty_received                BIGINT NOT NULL,
  provisional_price           BIGINT NOT NULL,   -- per-unit, minor currency units
  final_price                 BIGINT,
  received_at                 TIMESTAMPTZ NOT NULL,
  settled_at                  TIMESTAMPTZ,
  settlement_formula          TEXT,
  qty_consumed_at_settlement  BIGINT,
  qty_on_hand_at_settlement   BIGINT
);
```

### 1.8 Outbox (optional — see §2.2)

```sql
CREATE TABLE ledger_outbox (
  id            UUID PRIMARY KEY,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  batch_payload JSONB NOT NULL,
  state         TEXT NOT NULL DEFAULT 'pending'
                CHECK (state IN ('pending', 'processing', 'committed', 'failed')),
  processor_id  UUID,
  submitted_at  TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  attempts      INT NOT NULL DEFAULT 0,
  error         TEXT
);
CREATE INDEX outbox_pending    ON ledger_outbox(created_at) WHERE state = 'pending';
CREATE INDEX outbox_processing ON ledger_outbox(submitted_at) WHERE state = 'processing';
```

### 1.9 Append-only enforcement on `transfers`

```sql
CREATE OR REPLACE FUNCTION fn_block_transfer_modifications()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'transfers are append-only; UPDATE/DELETE rejected'
    USING ERRCODE = 'P9999';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transfers_append_only
  BEFORE UPDATE OR DELETE ON transfers
  FOR EACH ROW EXECUTE FUNCTION fn_block_transfer_modifications();
```

Combined with strict role-based access (no human/role has UPDATE/DELETE on `transfers` except a documented break-glass DBA role), this is the practical equivalent of TB's append-only guarantee. See Part VI §2 for the honest delta.

## §2. Write path

### 2.1 The write function

```sql
CREATE OR REPLACE FUNCTION post_transfers(
  p_events                 JSONB,
  p_override_closed_period BOOLEAN DEFAULT FALSE
)
RETURNS JSONB AS $$
DECLARE
  v_event       JSONB;
  v_results     JSONB := '[]'::jsonb;
  v_idx         INT := 0;
  v_account_ids BIGINT[];
  v_period_id   BIGINT;
  v_period_closed BOOLEAN;
  v_amount      BIGINT;
  v_debit_id    BIGINT;
  v_credit_id   BIGINT;
  v_d_acct      accounts%ROWTYPE;
  v_c_acct      accounts%ROWTYPE;
BEGIN
  -- Step 1: gather and lock all referenced accounts in ascending ID order
  SELECT array_agg(DISTINCT x ORDER BY x) INTO v_account_ids
    FROM (
      SELECT (e->>'debit_account_id')::BIGINT  AS x FROM jsonb_array_elements(p_events) e
      UNION
      SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) e
    ) t;
  PERFORM 1 FROM accounts WHERE id = ANY(v_account_ids) ORDER BY id FOR UPDATE;

  FOR v_event IN SELECT * FROM jsonb_array_elements(p_events) LOOP
    v_idx := v_idx + 1;

    -- Idempotency
    IF EXISTS (
      SELECT 1 FROM transfers WHERE idempotency_key = (v_event->>'idempotency_key')::UUID
    ) THEN
      v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'exists');
      CONTINUE;
    END IF;

    v_debit_id  := (v_event->>'debit_account_id')::BIGINT;
    v_credit_id := (v_event->>'credit_account_id')::BIGINT;
    v_amount    := (v_event->>'amount')::BIGINT;

    SELECT * INTO v_d_acct FROM accounts WHERE id = v_debit_id;
    SELECT * INTO v_c_acct FROM accounts WHERE id = v_credit_id;

    -- Validate
    IF v_d_acct.is_closed OR v_c_acct.is_closed THEN
      RAISE EXCEPTION 'account_closed' USING ERRCODE = 'P0001';
    END IF;
    IF v_d_acct.ledger_kind <> v_c_acct.ledger_kind THEN
      RAISE EXCEPTION 'ledger_mismatch' USING ERRCODE = 'P0002';
    END IF;
    IF v_d_acct.ledger_kind = 'value' AND v_d_acct.currency <> v_c_acct.currency THEN
      RAISE EXCEPTION 'currency_mismatch' USING ERRCODE = 'P0003';
    END IF;

    -- Period lock
    SELECT id, (closed_at IS NOT NULL) INTO v_period_id, v_period_closed
      FROM periods
      WHERE (v_event->>'business_date')::DATE BETWEEN opens_at AND closes_at;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'period_missing' USING ERRCODE = 'P0004';
    END IF;
    IF v_period_closed AND NOT p_override_closed_period THEN
      RAISE EXCEPTION 'period_closed' USING ERRCODE = 'P0005';
    END IF;

    -- Apply
    UPDATE accounts SET debits_total  = debits_total  + v_amount WHERE id = v_debit_id;
    UPDATE accounts SET credits_total = credits_total + v_amount WHERE id = v_credit_id;
    -- The CHECK on (debits_total, credits_total, normal_side) raises if violated
    -- and the surrounding transaction rolls back the entire batch — that IS the linked semantics.

    INSERT INTO transfers (
      reason, document_kind, document_id, document_line_id,
      debit_account_id, credit_account_id, amount,
      routing_op, counterparty_id, period_id, business_date,
      idempotency_key, posted_by
    ) VALUES (
      (v_event->>'reason')::transfer_reason,
      v_event->>'document_kind',
      (v_event->>'document_id')::UUID,
      NULLIF(v_event->>'document_line_id','')::UUID,
      v_debit_id, v_credit_id, v_amount,
      NULLIF(v_event->>'routing_op','')::INT,
      NULLIF(v_event->>'counterparty_id','')::UUID,
      v_period_id,
      (v_event->>'business_date')::DATE,
      (v_event->>'idempotency_key')::UUID,
      (v_event->>'posted_by')::UUID
    );

    v_results := v_results || jsonb_build_object('index', v_idx, 'result', 'ok');
  END LOOP;

  RETURN v_results;
END;
$$ LANGUAGE plpgsql;
```

**Total: ~80 lines** with the period-override path included. The reasons it is small:

- No flag matrix.
- No pending lifecycle.
- No balancing-transfer read-min-write semantics.
- No closing-flag side effects.
- No imported-flag user-supplied-timestamp validation.
- No linked-chain rollback machinery — the transaction handles it.
- No client-side ID generation.

### 2.2 Outbox — required or optional?

The outbox decouples document write from ledger write. In a single-database world, both are the same Postgres transaction; the outbox is not strictly required.

**Use the outbox if:**
- You have external integrations (commodity price feeds, payment gateways) where the write needs to fan out beyond the database.
- You want decoupled retries against an external sink.
- You want backpressure visibility (queue depth > X = alert).
- You want async batching for throughput beyond the write path's natural limit.

**Skip the outbox if:**
- The ledger write is the only durable side effect of an API call.
- Synchronous response to the client (success/failure of the post) is preferred over async confirmation.

This is an explicit team decision; v0.2 does not mandate either way. The schema in §1.8 supports outbox if chosen.

### 2.3 Outbox worker (when used)

```python
# Pseudocode
def drain_outbox():
    while True:
        batch = pg.execute("""
            UPDATE ledger_outbox
            SET state='processing', processor_id=%s, submitted_at=now(), attempts=attempts+1
            WHERE id IN (
                SELECT id FROM ledger_outbox
                WHERE state='pending'
                ORDER BY created_at
                LIMIT 100
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, batch_payload
        """, [worker_id])

        for row in batch:
            try:
                with pg.transaction():
                    pg.execute("SELECT post_transfers(%s)", [row.batch_payload])
                    pg.execute(
                        "UPDATE ledger_outbox SET state='committed', completed_at=now() WHERE id=%s",
                        [row.id]
                    )
            except LedgerHardError as e:
                pg.execute(
                    "UPDATE ledger_outbox SET state='failed', error=%s WHERE id=%s",
                    [str(e), row.id]
                )
            except TransientError:
                pg.execute(
                    "UPDATE ledger_outbox SET state='pending' WHERE id=%s",
                    [row.id]
                )
```

Stale-processing recovery: a separate job resets `state='processing' AND submitted_at < now() - interval '5 minutes'` back to `'pending'`.

## §3. Transaction patterns

All patterns are a single `post_transfers` call with one or more events in the JSONB array. Atomicity comes from the surrounding Postgres transaction.

### 3.1 PO receipt (firm-priced)

Slice A inflow workflow (acct-7mg, migrations 0034/0035). The document layer is `purchase_orders` (header) + `purchase_order_lines` (per-line) + `po_receipts` (header) + `po_receipt_lines` (per-line). The function `post_po_receipt(p_po_id, p_lines, ...)` validates over-receipt, resolves accounts per SKU's cost method, and emits the event batch below per received line.

**GRNI semantics (D1, 2026-05-01).** Receipts credit `ap_unsettled` (goods received not invoiced), not `ap` directly. The vendor bill (`post_ap_bill`, §3.x below) clears the GRNI accrual to `ap` once the bill arrives. This matches mainstream ERP convention (SAP/Oracle/D365) and prevents AP from being credited before invoice approval. Earlier draft of this section credited `ap` directly at receipt; revised when Slice A landed.

```
events (per po_line received):
  - reason='po_receipt'
    debit:  accounts(stock_available, sku, recv_loc).id
    credit: accounts(vendor_pool, vendor_id).id
    amount: qty
    qty:    qty
  - reason='po_receipt'
    debit:  accounts(inv_value_raw, sku, recv_loc, currency).id
    credit: accounts(ap_unsettled, vendor_id, currency).id
    amount: qty * std_cost   -- standard SKU; std at business_date
            qty * po_unit_cost  -- WAC SKU (perpetual / periodic / retroactive)
    qty:    qty
  - (standard SKU only, when po_unit_cost ≠ std_cost)
    reason='ppv'
    -- unfavorable (po > std):
    debit:  accounts(variance_ppv, currency).id
    credit: accounts(ap_unsettled, vendor_id, currency).id
    amount: qty * (po_unit_cost - std_cost)
    -- favorable (po < std): debit and credit roles flip; amount = |delta|
```

Net effect at the vendor-side: `ap_unsettled` = `qty * po_unit_cost` regardless of cost method (it's what we owe); `inv_value_raw` lands at `qty * std` for standard SKUs (PPV absorbs the variance) and at `qty * po_unit_cost` for WAC SKUs (the pool re-averages organically).

Strict over-receipt rejection (D3): cumulative `qty_received` per `po_line` cannot exceed `qty_ordered` (`P0023`). Tolerance / over-receipt is Phase 2.

`fifo` / `lot` cost methods raise `P0006` at receipt time (acct-8gg).

### 3.2 Inter-location transfer

**Immediate:**

```
events:
  - reason='bin_move'
    debit:  accounts(stock_available, sku, dest_loc).id
    credit: accounts(stock_available, sku, origin_loc).id
    amount: qty
```

**With transit:**

```
release:
  - reason='to_release'
    debit:  accounts(stock_in_transit, sku, lane).id
    credit: accounts(stock_available, sku, origin_loc).id
    amount: qty

receipt:
  - reason='to_receipt'
    debit:  accounts(stock_available, sku, dest_loc).id
    credit: accounts(stock_in_transit, sku, lane).id
    amount: qty
```

### 3.3 SO reservation, allocation, ship

Reservations are NOT transfers in v0.2. They are rows in `inventory_reservations`.

**Reserve** — call the `reserve_inventory()` PL/pgSQL function (migration `0014_reserve_inventory.up.sql`). Returns the new reservation's UUID on success, or NULL when `qty_promisable < qty`:

```sql
SELECT reserve_inventory(
  $sku_id, $location_id, $qty, $so_id, $so_line_id, $expires_at, $unit_price
);
```

The function takes `FOR UPDATE` on the matching `stock_available` row, then computes `qty_promisable = (debits − credits) − SUM(active reservations)`, and either inserts the reservation row or returns NULL. Raises `P0010` if no open `stock_available` account exists for the (sku, location) pair (caller bug — accounts must be pre-created).

#### Why a function and not a single-statement CTE+INSERT

The naïve form looks like it should work:

```sql
-- DO NOT USE — unsafe under concurrent reservers in READ COMMITTED
WITH avail AS (
  SELECT
    a.id,
    (a.debits_total - a.credits_total) - COALESCE((
      SELECT SUM(qty) FROM inventory_reservations r
      WHERE r.sku_id = a.sku_id AND r.location_id = a.location_id AND r.status = 'active'
    ), 0) AS qty_promisable
  FROM accounts a
  WHERE a.kind = 'stock_available' AND a.sku_id = $sku AND a.location_id = $loc
  FOR UPDATE
)
INSERT INTO inventory_reservations (...)
SELECT ... FROM avail WHERE qty_promisable >= $qty
RETURNING id;
```

It isn't. **Postgres takes one snapshot per command in `READ COMMITTED`.** The `FOR UPDATE` on the `accounts` row correctly serializes contenders — only one waiter at a time runs to completion — but the inner `SUM(qty) FROM inventory_reservations` subquery still uses the snapshot that was taken at command start, *before* the `FOR UPDATE` wait. Each waiter therefore computes the same pre-contention `qty_promisable` and the constraint silently over-promises. Demonstrated in `tests/reserve_concurrency.rs` (T3): on-hand=10, five concurrent 3-unit requests, the CTE form admits four (sum=12) where the correct outcome is exactly three (sum=9).

In a PL/pgSQL function each SQL statement takes its own snapshot. The post-`FOR UPDATE` `SELECT` on `inventory_reservations` sees the prior winner's `INSERT`, and the function returns NULL when the budget is exhausted. T3 now passes deterministically (`successes == 3`).

This is a Postgres-specific snapshot-semantics gotcha; not a deadlock or a missing index. The fix is the function boundary, not a different operator.

**Allocate** (pick confirm):

```sql
UPDATE inventory_reservations
SET status = 'allocated', resolved_at = clock_timestamp()
WHERE id = $rsv_id AND status = 'active';
```

**Cancel:**

```sql
UPDATE inventory_reservations
SET status = 'cancelled', resolved_at = clock_timestamp()
WHERE id = $rsv_id AND status = 'active';
```

**Expire** (cron, single SQL statement, runs every 30s):

```sql
UPDATE inventory_reservations
SET status = 'expired', resolved_at = clock_timestamp()
WHERE status = 'active' AND expires_at < clock_timestamp();
```

**Ship** (post-allocation):

```
events:
  - reason='so_ship'
    debit:  accounts(customer_pool, customer_id).id
    credit: accounts(stock_available, sku, ship_loc).id
    amount: qty
  - reason='so_ship'
    debit:  accounts(cogs, sku, currency).id
    credit: accounts(inv_value_fg, sku, currency).id
    amount: qty * unit_cost
  - reason='so_ship'
    debit:  accounts(ar, customer_id, currency).id
    credit: accounts(revenue, currency).id
    amount: qty * unit_price
  - (if applicable)
    reason='so_ship'
    debit:  accounts(ar, customer_id, currency).id
    credit: accounts(sales_tax_payable, currency).id
    amount: tax
```

The same Postgres transaction also marks the reservation as allocated (or shipped, if you add a status), so reservation + ship are atomic.

### 3.4 Work order lifecycle

**Slice B (acct-b82, 2026-05-02)** introduces the document layer for WOs. Schema: `work_orders` (header), `wo_routings` (per-WO ops, no shared template table at MVP), `wo_routing_burdens(wo_id, routing_op, applied_account_kind, std_amount)` (per-op standard absorption rates by burden type), `boms(parent_sku_id, component_sku_id, qty_per_parent, component_loc_id)`. The four document-layer functions are `post_wo_start`, `post_op_move`, `post_wo_complete`, `post_scrap`. MVP restriction: WO parent_sku.cost_method = `standard` (P0006 otherwise; lifted under acct-p7v).

**Cost rollup model.** A WO's standard cost is:

```
parent_std_cost = Σ (bom.qty_per_parent × component_std_cost)        (RM)
                + Σ_op Σ_kind wo_routing_burdens.std_amount          (per-op burdens)
```

**BOM components and per-op burdens are the same idea** — both are per-unit costs that apply to a unit as it moves through the routing. They differ only in *what* they substantively are (RM vs absorption: labor / OH / outside-processing / setup / tooling / energy / ...) and *when* they apply (RM at WO start; burdens at the op they're declared on). Burden rows credit `<applied_account_kind>(currency)` accounts (which are absorption accounts, e.g. `labor_applied`, `oh_applied`); reconciliation between absorbed and actual (vendor bill, payroll, allocation) is separate variance work.

**Open extension via `applied_account_kind`.** MVP supports `labor_applied` and `oh_applied`. New burden types (`outside_processing_applied`, `setup_applied`, `tooling_applied`, energy, …) are added by:
1. `ALTER TYPE account_kind ADD VALUE '<X>_applied'`
2. `ALTER TYPE transfer_reason ADD VALUE '<X>_apply'`
3. Extending the `_wo_apply_reason_for(account_kind)` mapping in the WO functions
4. Scaffolding the per-currency `<X>_applied` account

The `wo_routing_burdens` table itself does not change.

**WO start (releases the WO; charges WIP@first_op):**

```
events (post_wo_start):
  - reason='wo_start', routing_op=first_op
    debit:  accounts(stock_wip, parent_sku, first_op).id
    credit: accounts(creation_void).id
    amount: qty_target

  for each (component_sku, qty_per_parent, component_loc) in boms(parent_sku):
    - reason='rm_issue_to_wo'
      debit:  accounts(stock_consumed, component_sku).id
      credit: accounts(stock_available, component_sku, component_loc).id
      amount: qty_target * qty_per_parent
    - reason='rm_issue_to_wo'
      debit:  accounts(inv_value_wip, parent_sku, first_op, currency).id
      credit: accounts(inv_value_raw, component_sku, component_loc, currency).id
      amount: qty_target * qty_per_parent * resolve_standard_cost_at(component_sku, business_date)

  for each (applied_account_kind, std_amount) in wo_routing_burdens(wo_id, first_op):
    - reason=_wo_apply_reason_for(applied_account_kind)  -- e.g. labor_apply, oh_apply
      debit:  accounts(inv_value_wip, parent_sku, first_op, currency).id
      credit: accounts(applied_account_kind, currency).id
      amount: qty_target * std_amount
```

After `post_wo_start`, `WIP@first_op = qty_target × std_cum_at_first_op` (RM + first-op burdens).

**Op move (from_op → to_op):**

The value-leg amount equals `qty × std_cum_at_from_op` (RM + sum of burdens for ops ≤ from_op) — i.e. all the cost a unit accumulated by the time it arrived at `from_op`. After the move, the destination op's burdens are applied against the moved qty.

The dispatcher's standard branch returns `qty × parent_std_cost` (resolved at SKU level), which is correct only at the last op. For intermediate ops `post_op_move` computes the value externally and passes it via reason `op_move_v` (NOT in the dispatcher's cost-event list). Mirrors the `scrap` / `scrap_v` qty-leg / value-leg split.

```
events (post_op_move(wo_id, from_op, to_op, qty)):
  - reason='op_move'
    debit:  accounts(stock_wip, parent_sku, to_op).id
    credit: accounts(stock_wip, parent_sku, from_op).id
    amount: qty
  - reason='op_move_v'                                        -- value leg
    debit:  accounts(inv_value_wip, parent_sku, to_op, currency).id
    credit: accounts(inv_value_wip, parent_sku, from_op, currency).id
    amount: qty * std_cum_at_from_op
                where std_cum_at_from_op
                  = Σ (bom.qty_per_parent × resolve_standard_cost_at(comp, business_date))
                  + Σ wo_routing_burdens.std_amount  for ops ≤ from_op

  for each (applied_account_kind, std_amount) in wo_routing_burdens(wo_id, to_op):
    - reason=_wo_apply_reason_for(applied_account_kind)
      debit:  accounts(inv_value_wip, parent_sku, to_op, currency).id
      credit: accounts(applied_account_kind, currency).id
      amount: qty * std_amount
```

After `post_op_move`, `WIP@to_op` grew by `qty × std_cum_at_to_op`. Rework moves (to_op < from_op) are allowed and re-apply the destination op's burdens — realistic ERP rework semantics.

**WO complete (last op → FG; residual variance at close):**

At the last op, `std_cum_at_last_op == parent_std_cost`, so the dispatcher's standard branch returns the right number for the value-leg. Reason stays `wo_complete` (in the dispatcher cost-event list).

```
events (post_wo_complete(wo_id, qty)):
  - reason='wo_complete'
    debit:  accounts(stock_available, parent_sku, fg_loc).id
    credit: accounts(stock_wip, parent_sku, last_op).id
    amount: qty
  - reason='wo_complete'                                       -- dispatcher-priced
    debit:  accounts(inv_value_fg, parent_sku, fg_loc, currency).id
    credit: accounts(inv_value_wip, parent_sku, last_op, currency).id
    amount: qty * resolve_standard_cost_at(parent_sku, business_date)

  -- only on final completion (qty_completed + qty_scrapped reaches qty_target),
  -- if WIP@last_op holds nonzero residual (read FOR UPDATE):
  - reason='wo_close_v'
    debit:  accounts(variance_wo_close, currency).id  (or flip when favorable)
    credit: accounts(inv_value_wip, parent_sku, last_op, currency).id
    amount: |residual|
```

Per-op MUV/LV/OHV (operation-level variance grain) is deferred — the residual at WO close is the only variance the MVP surfaces. (Slice B Q3 lean.)

### 3.5 Scrap at operation

```
events:
  - reason='scrap', routing_op=NN
    debit:  accounts(stock_scrap, parent_sku).id
    credit: accounts(stock_wip, parent_sku, NN).id
    amount: qty
  - reason='scrap_v', routing_op=NN
    debit:  accounts(variance_scrap, currency).id
    credit: accounts(inv_value_wip, parent_sku, currency).id
    amount: accumulated_cost  (computed read-then-write under lock)
```

### 3.6 Quarantine and release

```
quarantine:
  - reason='quarantine'
    debit:  accounts(stock_quarantine, sku).id
    credit: accounts(stock_available, sku, loc).id
    amount: qty

release:
  - reason='release_from_quarantine'
    debit:  accounts(stock_available, sku, loc).id
    credit: accounts(stock_quarantine, sku).id
    amount: qty
```

A `qc_holds` table in Postgres holds the reason, authorizer, test results, release date — referenced via `document_kind='qc_hold', document_id=$hold_id`.

### 3.7 AR / AP payment

**AR payment:**

```
events:
  - reason='ar_payment'
    debit:  accounts(cash, currency).id
    credit: accounts(ar, customer_id, currency).id
    amount: payment_amount
```

**AP payment:**

```
events:
  - reason='ap_payment'
    debit:  accounts(ap, vendor_id, currency).id
    credit: accounts(cash, currency).id
    amount: payment_amount
```

### 3.8 Cycle-count adjustment

```
events:
  - reason='cycle_count_adj'
    debit:  accounts(stock_available, sku, loc).id      (if surplus; flip if shortage)
    credit: accounts(inv_adj_expense).id  (qty side: a counterpart pool)
    amount: |delta_qty|
  - reason='cycle_count_adj'
    debit:  accounts(inv_value_raw, sku, currency).id   (or COGS for shortages)
    credit: accounts(inv_adj_expense, currency).id
    amount: |delta_qty| * unit_cost
```

### 3.9 Reversals

Never UPDATE or DELETE a transfer. A reversal is a new transaction with `reason='reversal'` (or the domain-specific reversal reason where one exists, e.g., `po_return_to_vendor`) that posts the inverse debit/credit assignments. The original `document_id` is preserved on the reversal for traceability.

### 3.10 Cross-currency transfer (Currency Exchange recipe)

Two transfers in one transaction, through an FX liquidity-provider account pair:

```
events:
  - reason='fx_leg'
    debit:  accounts(fx_liquidity, EUR).id
    credit: accounts(cash, USD).id
    amount: usd_amount
  - reason='fx_leg'
    debit:  accounts(cash, EUR).id
    credit: accounts(fx_liquidity, USD).id
    amount: eur_amount
  - reason='fx_spread'
    debit:  accounts(fx_revaluation, USD).id  (or EUR; policy)
    credit: accounts(fx_liquidity, USD).id
    amount: spread_amount
```

### 3.11 Inventory adjustment (general — not cycle-count-specific)

Inventory adjustments bring inventory into or out of the system without an external counterparty (no PO, no SO, no WO). Use cases include physical-count corrections, write-downs, write-ins, post-receipt damage detected before a return, and rounding reconciliations. This is distinct from `cycle_count_adj`, which is reserved for cycle-count-specific document workflows.

The document layer is the `inventory_adjustments` table (migration 0022); the function `post_inventory_adjustment` wraps the `inventory_adjustment` ledger primitive (also added in migration 0022). Caller passes a signed `qty_delta` (positive = in, negative = out) and a `unit_cost`. The function generates the 2-event batch:

```
qty_delta > 0 (in):
  - reason='inventory_adjustment'
    debit:  accounts(stock_available, sku, loc).id
    credit: accounts(creation_void, qty).id
    amount: |qty_delta|
  - reason='inventory_adjustment'
    debit:  accounts(inv_value_{class}, sku, loc, ccy).id   -- class ∈ {'raw','fg'}
    credit: accounts(inv_adj_expense, ccy).id               -- adjustment income
    amount: |qty_delta| * unit_cost

qty_delta < 0 (out): debit and credit roles flip on both legs.
  -- value leg becomes: debit inv_adj_expense, credit inv_value_*
  --                    (adjustment expense)
```

Notes:
- `inventory_class` is `'raw'` or `'fg'` (MVP); WIP adjustments require routing_op resolution and are deferred.
- Idempotent on `idempotency_key` at the document-table level — replay returns the existing document id without re-posting.
- **Qty-side counterpart is `creation_void` (qty has no P&L concept). Value-side counterpart is `inv_adj_expense` — a bidirectional P&L account (`normal_side='unrestricted'`) that holds adjustment income (credit balance) and adjustment expense (debit balance). The accumulated balance at period close is the net adjustment gain/loss on the income statement.**
- The ledger reason `inventory_adjustment` is added in migration 0022. Down-migration drops the table and function but leaves the enum value in place (Postgres can't cleanly remove an enum value once added without recreating the type).

**Cost-method dispatch.** `p_unit_cost` is NULLable. The SKU's `cost_method` determines what cost is applied:

| `cost_method` | `p_unit_cost = NULL` | `p_unit_cost = explicit` |
|---|---|---|
| `standard` | use `skus.standard_cost` | **P0011** — standard SKUs have a fixed cost; do not pass one |
| `wac_perpetual` IN | use pool average; **P0011** if pool empty (caller must seed) | use it; pool re-averages |
| `wac_perpetual` OUT | use pool average (classic WAC; pool average preserved); **P0010** if pool empty | **P0011** — asserted-cost-on-depletion belongs in `'lot'` cost_method (see `acct-8gg`) |
| `wac_periodic` | use pool average; flagged in `transfers_provisional` for re-cost at close (`acct-qfj`, migration 0029); P0006 if pool empty on depletion | **P0011** (asserted-cost-on-depletion is `'lot'` territory) |
| `wac_retroactive` | use pool average; flagged in `transfers_provisional` for chronological replay at close (`acct-9tw`, migration 0031) | **P0011** (asserted-cost-on-depletion is `'lot'` territory) |
| `fifo` / `lot` | always **P0006** (`acct-8gg`) | always **P0006** |

WAC perpetual is one of three textbook WAC variants (per Oracle/PeopleSoft): perpetual (live average, recomputed every putaway), periodic (single average per period applied at close), and retroactive perpetual (perpetual chain with late-data corrections at close). Phase 1 ships only perpetual; the other two are filed as future epics. The audit row records the **effective** unit cost — what was actually applied — not the caller's input.

### 3.12 Cost adjustment (value-only revaluation)

Distinct from inventory adjustment (qty + value together) and from cost_restate (commodity provisional-to-actual settlement, §10). This is the workflow for explicitly revaluing the per-unit average cost of an existing inventory pool **without moving qty**.

Use cases:
- Lower-of-cost-or-market write-down on existing inventory.
- Quality issue revealed: pool's recorded cost was overstated.
- Late vendor credit applied retroactively to current inventory.
- Cost basis correction after audit.

Document layer: `inventory_cost_adjustments` table (migration 0024). Function: `post_cost_adjustment`. Underlying ledger reason: `cost_adjustment` (transfer_reason). P&L counterpart: `variance_cost_adjustment` — bidirectional account_kind, `normal_side='unrestricted'`. Distinct from `inv_adj_expense` (qty-driven adjustments) so the income statement reports revaluation events separately from cycle-count gain/loss.

```
post_cost_adjustment(sku, location, currency, inventory_class, target_unit_cost, ...)

reads pool under FOR UPDATE → current_qty, current_value
delta = (target_unit_cost * current_qty) - current_value

delta > 0 (write-up):
  - reason='cost_adjustment'
    debit:  accounts(inv_value_{class}, sku, loc, ccy).id
    credit: accounts(variance_cost_adjustment, ccy).id   -- revaluation gain
    amount: delta

delta < 0 (write-down):
  - reason='cost_adjustment'
    debit:  accounts(variance_cost_adjustment, ccy).id   -- revaluation loss
    credit: accounts(inv_value_{class}, sku, loc, ccy).id
    amount: -delta

delta = 0: audit row recorded, no transfer posted.
```

Cost-method dispatch:

| `cost_method` | Behavior |
|---|---|
| `standard` | **P0011** — to change a standard SKU's cost, use `post_standard_cost_roll` (§3.13) |
| `wac_perpetual` | computes delta against the live pool average; posts immediately |
| `wac_periodic` | **P0006** — depends on period-close machinery (`acct-s6n`) and the `wac_periodic` epic (`acct-qfj`) |
| `wac_retroactive` | **P0006** — same dependency chain, plus `acct-9tw` |
| `fifo` / `lot` | **P0006** — `acct-8gg` |

Empty pool (qty ≤ 0): **P0010** — there's no average to adjust on an empty pool.

The audit row records the **prior** unit cost (pool avg before adjustment), the **target** unit cost, the resulting **delta_value**, and the **pool_qty** at adjustment time. This makes the operation self-explanatory in reports without needing to look elsewhere.

Idempotent at the document-table level: replay returns the existing id without re-posting.

### 3.13 Standard cost as a separate transactional entity

`skus.standard_cost` does not exist as a column. Standard cost is a **stream of cost estimates with effective dates** (Oracle/SAP idiom: "cost estimate" / "cost update"), tracked in its own table. The item master records that a SKU exists; the cost stream records what its standard is at any given date.

```sql
CREATE TABLE standard_costs (
  id              UUID PRIMARY KEY DEFAULT uuidv7(),
  sku_id          UUID NOT NULL REFERENCES skus(id),
  cost            BIGINT NOT NULL CHECK (cost >= 0),
  effective_at    DATE NOT NULL,
  posted_by       UUID NOT NULL,
  posted_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  idempotency_key UUID NOT NULL UNIQUE,
  notes           TEXT
);
```

Append-only by convention. Each row is the standard that takes effect on `effective_at`. Multiple rows per SKU are expected — the cost stream evolves over time.

**Canonical lookup.** All cost-relevant operations on standard-method SKUs go through one helper:

```sql
resolve_standard_cost_at(p_sku_id UUID, p_business_date DATE) RETURNS BIGINT
```

Returns the cost from the latest row with `effective_at <= p_business_date`, or raises **P0018** (`standard_cost_not_established`) if no such row exists for the SKU at that date. This is a STABLE function (no side effects).

**P0018 gate.** A standard-method SKU with no `standard_costs` row in effect at `business_date` is in an incomplete state. Cost-relevant operations refuse with P0018. Currently caught by:
- `_post_transfers_compute_amount` — `post_transfers` value-side dispatcher (touches `op_move`, `scrap`, `wo_complete`, `so_ship`).
- `post_inventory_adjustment` — standard branch with `NULL` p_unit_cost.

Future workflows (PO receipt, SO ship, etc.) inherit the gate by going through these helpers. Operations that don't read the cost — qty-only events, account creation, metadata queries — are unaffected.

**Establishing or rolling the cost.** `post_standard_cost_roll` is the single entry point:

```sql
post_standard_cost_roll(
  p_sku_id            UUID,
  p_new_cost          BIGINT,
  p_effective_at      DATE,                     -- when the new standard takes effect
  p_business_date     DATE,                     -- date for variance transfers
  p_posted_by         UUID,
  p_idempotency_key   UUID,
  p_notes             TEXT   DEFAULT NULL,
  p_expected_old_cost BIGINT DEFAULT NULL       -- optimistic concurrency guard
) RETURNS UUID                                  -- inventory_standard_cost_rolls.id
```

Behavior:

1. **Replay check** — same `idempotency_key` returns the existing audit id without re-posting.
2. **Cost-method dispatch** — standard ✓; `wac_perpetual` / `wac_periodic` / `wac_retroactive` → **P0011** (use `post_cost_adjustment` instead, §3.12); `fifo` / `lot` → **P0006** (acct-8gg).
3. **Retroactive guard** — `p_effective_at` must be strictly greater than every existing `standard_costs.effective_at` for the SKU. Otherwise raises **P0019** (`retroactive_std_cost_roll_blocked`). Phase 1 doesn't support retroactive corrections to past costs.
4. **Optimistic concurrency** — if `p_expected_old_cost` is non-NULL it must match the active standard at `p_business_date` (or both must be NULL for the first roll). Mismatch raises **P0017**.
5. **WIP guard** — if any open `inv_value_wip` pool exists for the SKU with a non-zero balance, raises **P0006** referencing `acct-bru` (Epic G — WIP material revaluation companion). Phase 1 blocks rolls when WIP is in flight; the companion workflow is deferred to Phase 2.
6. **INSERT** the new `standard_costs` row.
7. **Revaluation pass** — if the new cost takes effect at or before `p_business_date`, walk every open `inv_value_raw` and `inv_value_fg` pool for the SKU; per pool with non-zero on-hand, post a variance transfer for `on_hand × (new − prior)` against `variance_std_cost_roll(currency)`. Write-up: dr inventory, cr variance. Write-down: reverse. WIP excluded by step 5's guard.
8. **Audit row** — records `prior_standard_cost` (NULLABLE; first roll has no prior), `target_standard_cost`, `total_delta_value`, `pool_qty`, plus `effective_at` and `business_date` for self-explanatory reporting.

**Future-dated rolls.** If `p_effective_at > p_business_date`, the new cost is queued (INSERT only) but no revaluation posts. Transactions whose `business_date >= effective_at` automatically pick up the new standard via `resolve_standard_cost_at`. Phase 1 has no scheduled-revaluation mechanism for the future-dated case — when the effective date passes, on-hand pools simply continue to carry the prior standard until they flow out or a follow-up roll re-revalues. This is acknowledged imprecision; out of scope for Phase 1 to schedule.

**Lock order.** `skus` row `FOR UPDATE` first (serializes concurrent rolls on the same SKU even though we don't UPDATE the row directly), then `inv_value_raw` + `inv_value_fg` accounts in ascending id order. Matches `post_transfers`' lock-order invariant.

**Variance account.** `variance_std_cost_roll` (one per currency, `normal_side='unrestricted'`). Distinct from:
- `variance_cost_adjustment` (§3.12) — WAC pool revaluation
- `inv_adj_expense` (§3.11) — qty-driven adjustments
- `variance_ppv` — purchase price variance per receipt

Each tells a different income-statement story.

**Audit table.** `inventory_standard_cost_rolls` records every roll, including no-ops (target == prior) and first rolls (prior IS NULL). The full history is preserved.

**Known limitation.** `p_posted_by` is unvalidated (RBAC = Part VII Q6, still open). Same convention as the other document-layer functions.

### 3.14 Cost adjustment retroactive (period-close-applied)

A second cost-adjustment workflow that complements §3.12. §3.12 (`post_cost_adjustment`) revalues the **live pool** at the moment of call — instantaneous effect, only on the value side, only on `wac_perpetual` pools. §3.14 (`post_cost_adjustment_retroactive`) is **operator-queued** and **flushes at period close** — it walks every credit-side qty-bearing depletion in the target period and re-costs it against an operator-supplied `target_avg`, posting one variance batch per non-zero-variance depletion through `variance_cost_adjust_retro`.

Use cases (different from §3.12):
- Audit determines the period's effective cost should have been different from what mid-period perpetual produced.
- Late vendor pricing data arrives after the period's first depletion but before close, and the operator wants every depletion re-costed (not just the live pool).
- Regulatory or reporting cost basis correction across all in-period consumption.

Document layer: `inventory_cost_adjustments_retroactive` table (queue, migration 0032). Function: `post_cost_adjustment_retroactive`. Underlying ledger reason: `cost_restate` (same as wac_periodic / wac_retroactive variances). Document kind: `cost_adjust_retroactive_close`. P&L counterpart: `variance_cost_adjust_retro` (added in migration 0025; seeded in `db/fixtures/small/seed.sql` for USD + EUR).

```
post_cost_adjustment_retroactive(
  p_target_period_id, p_sku_id, p_location_id, p_currency,
  p_inventory_class, p_target_avg, p_business_date,
  p_posted_by, p_idempotency_key, p_notes
) RETURNS UUID

queue-time (synchronous):
  - validate target period exists AND open       → P0014 / P0021
  - validate p_business_date in period bounds    → P0004
  - validate inventory_class in (raw, fg)        → P0006 if wip (acct-p7v)
  - validate target_avg >= 0                     → 23514
  - validate sku exists, pool exists             → P0010
  - INSERT queue row, return its id (no transfers posted yet)

close-time (cost_adjust_retroactive_hook):
  for each un-finalized queue row in this period (FOR UPDATE):
    walk transfers WHERE credit_account_id = pool
                     AND business_date IN period
                     AND qty IS NOT NULL AND qty > 0
    for each depletion:
      provisional_unit = amount / qty
      variance = qty × (target_avg − provisional_unit)
      if variance != 0:
        post 2-transfer batch routed through variance_cost_adjust_retro
    UPDATE queue row finalized_at, finalized_count, total_variance
```

**Method-agnostic.** Works on any `cost_method` — `standard`, `wac_perpetual`, `wac_periodic`, `wac_retroactive`. Operator override of whatever cost was originally applied. With `wac_periodic` / `wac_retroactive` the close-time variance posted by their own hooks is **already in place** (their hooks run before this one); §3.14 then layers an additional variance on top. Documented as "double-correction is acceptable" — the simplest semantic, matching what the operator sees in the depletion's `amount` (the original, not the wac-corrected value).

**Why qty=NULL on prior-hook variances matters.** Variance transfers from `wac_periodic_close_hook` and `wac_retroactive_close_hook` carry `qty=NULL` (they're value-only corrections, not new physical movement). The `qty IS NOT NULL AND qty > 0` filter on the depletion walk naturally excludes those rows from being re-walked, so each depletion contributes to §3.14's variance exactly once even when other hooks have already posted variance.

**Period must be open at queue time.** Closed periods raise **P0021** (`target_period_closed`) referencing `acct-7h4` (Phase 2 Epic K — period reopen workflow). For a closed period the operator must reopen it first, then queue, then re-close. This is intentional — the queue is the period's audit trail and must be seen by `close_period`.

**WIP class.** Deferred. `inventory_class='wip'` raises **P0006** referencing `acct-p7v` (Phase 2 Epic J — wac_periodic / wac_retroactive across WIP pools). The cost-adjustment-retroactive workflow on WIP requires the same machinery as periodic-WAC on WIP, so they ship together.

**Idempotency.** `idempotency_key UUID NOT NULL UNIQUE` on the queue table. Replay returns the existing queue row's id without re-inserting; the hook's `FOR UPDATE` walk also tolerates concurrent close attempts.

The audit row records `target_avg`, `business_date`, `posted_by`, `posted_at`, `finalized_at`, `finalized_count` (depletions processed at close), `total_variance` (signed sum of variance_amount across all 2-transfer batches), and free-form `notes`. The full lifecycle is reconstructable from this row plus the variance transfers (joined via `document_id = queue_row.id`, `document_kind = 'cost_adjust_retroactive_close'`).

### 3.15 AP bill (vendor invoice)

Slice A inflow workflow companion to §3.1 (acct-7mg, migration 0035). The document layer is `vendor_bills` (header) + `vendor_bill_lines` (per-line). The function `post_ap_bill(p_vendor_id, p_currency, p_lines, ...)` validates each line per its `kind` and emits the event below.

Two line modes coexist in one bill:

**`po_match` — clears the GRNI accrual from §3.1.** Strict three-way match (D3, 2026-05-01): per `po_line`, the bill's `qty` cannot exceed the received-not-yet-billed remainder (`SUM(po_receipt_lines.qty_received) − SUM(prior vendor_bill_lines.qty WHERE kind='po_match')`); `unit_cost` must equal `po_line.unit_cost`; `amount` must equal `qty × unit_cost`. Cumulative across the bill batch (later lines see earlier inserts via `READ COMMITTED`).

```
event (per po_match line):
  - reason='ap_bill'
    debit:  accounts(ap_unsettled, vendor_id, currency).id
    credit: accounts(ap, vendor_id, currency).id
    amount: qty * unit_cost
```

Net effect: `ap_unsettled` (GRNI accrual from §3.1's receipt event) clears to `ap` (real liability). The flow now reads §3.1 → §3.15 → §3.7 (`ap_payment`).

**`service` — caller-supplied expense (no PO reference).** Covers utilities, professional services, rent, software, etc. The caller passes `expense_account_id` per line; the function validates that the account is open, value-side, and matches the bill currency. Taxonomy (operating-expense account_kind enum) deferred to acct-063 (D2, 2026-05-01).

```
event (per service line):
  - reason='ap_bill'
    debit:  accounts(<caller-supplied expense_account>).id
    credit: accounts(ap, vendor_id, currency).id
    amount: line.amount
```

Both modes can be mixed in one document (one `vendor_bill`, one `post_ap_bill` call). Replays on `idempotency_key` short-circuit before any post.

Error codes (all new in Slice A): **P0022** po_receipt_invalid (deferred from §3.1), **P0023** po_line_overreceived, **P0024** ap_bill_three_way_mismatch (qty over remainder, unit_cost mismatch, or amount ≠ qty × unit_cost), **P0025** ap_bill_invalid_line (wrong vendor on po_line, currency mismatch, missing or closed expense account, unknown line kind).

## §4. WIP model

### 4.1 Account grain

The schema partitions value accounts by the same dimensions as their qty-side counterparts so WAC-style cost computation can be done per-pool with a clean lookup.

| Account kind | Partitioned by | Notes |
|---|---|---|
| `stock_available` (qty) | `(sku, location)` | Per migration 0006 `accounts_stock_avail_uk`. |
| `stock_wip` (qty) | `(sku, routing_op)` | Per migration 0006 `accounts_wip_uk`. WIP qty is intrinsic to an op; location is not on the key. |
| `inv_value_raw` (value) | `(sku, location, currency)` | Per-location raw-materials value pool — pairs with `stock_available`. |
| `inv_value_wip` (value) | `(sku, routing_op, currency)` | Per-op WIP value pool — pairs with `stock_wip`. |
| `inv_value_fg` (value) | `(sku, location, currency)` | Per-location finished-goods value pool — pairs with `stock_available` (FG location). |
| `cogs`, `revenue`, `cash`, `ar`, `ap`, `variance_*`, `labor_applied`, etc. | `(currency)` only | No per-sku, per-loc, or per-op partition. Aggregated. |

**Per-WO breakdown:** reconstructed via `SELECT * FROM transfers WHERE document_kind='work_order' AND document_id=$wo_id`. Single B-tree lookup, no projector required.

For long-cycle/regulated job-cost WOs, opt in to per-WO per-op accounts at WO creation time. Close them via `is_closed=true` on WO completion. Default off; enable per SKU family or WO type.

**Phase 0 schema state:** migration 0006 originally shipped value accounts with UK `(kind, sku_id, currency)` only — sufficient on the standard path because cost is global per (sku, currency). Migration 0020 (`acct-nfr`) replaced that with the per-location / per-routing-op partition shown in the table above, gating WAC's per-pool cost lookup. Migration 0021 (`acct-uxu`) lit up the WAC dispatcher branch on top of it.

### 4.2 Cost method

**Default: standard costing with variance capture.** Standard costs from `skus.standard_cost` and `routings.std_*_cost`. PPV at receipt, MUV/LV/OHV at apply, scrap_v at scrap, wo_close_v at close.

**Cost is computed inside `post_transfers`.** As of acct-0ig (migration 0019), the value-side amount on cost-relevant events (`op_move`, `scrap`, `wo_complete`, `so_ship`) is computed by a dispatcher helper keyed on `skus.cost_method`. The system is the cost engine — there is no external cost engine.

**Implemented branches:** `'standard'` (migration 0019, `amount = qty × resolve_standard_cost_at(sku, business_date)` after the standard-cost refactor in migration 0027 / `acct-x4t`); `'wac_perpetual'` (migration 0021 / `acct-uxu`, refactored in migration 0030 / `acct-1vr` for per-class qty correctness); `'wac_periodic'` (migration 0029 / `acct-qfj`, mid-period dispatcher matches wac_perpetual but flags depletions in `transfers_provisional` for re-cost at close); `'wac_retroactive'` (migration 0031 / `acct-9tw`, same mid-period dispatcher and provisional flagging as wac_periodic; differs only at close-time replay). The qty-side gate relaxes for all four. `wac_perpetual` / `wac_periodic` / `wac_retroactive` raise `P0006` only when the qty pool is zero at depletion time.

**Per-class qty divisor (acct-1vr, migration 0030).** `wac_perpetual` and `wac_periodic` no longer divide by `stock_available.balance` — that account pools qty across raw and fg lifecycle states for the same `(sku, location)`, which gave incorrect per-class unit costs when a SKU had multiple value pools active. Instead, the divisor is a per-pool sum from transfer history:

```sql
SELECT SUM(CASE WHEN t.debit_account_id  = pool_id THEN  t.qty
                WHEN t.credit_account_id = pool_id THEN -t.qty END)
  FROM transfers t
 WHERE pool_id IN (t.debit_account_id, t.credit_account_id) AND t.qty IS NOT NULL
```

Each transfer that touches the value pool carries `qty` (added as a column in 0030, populated at INSERT time from the event JSONB), so the sum is class-isolated. Numerator (`pool.balance`) is unchanged — value-side accounts are already partitioned per class. `_post_transfers_lookup_qty_account` is retained for `post_transfers`'s lock pre-scan but no longer participates in divisor computation.

**Three WAC variants (Oracle/PeopleSoft taxonomy).** `wac_perpetual` is one of three textbook variants — all three are shipped:
- `wac_perpetual` — live average, recomputed on every putaway. Mid-period and post-close are the same number.
- `wac_periodic` — single average per period, computed at close, applied to every depletion in the period (acct-qfj, migration 0029). Mid-period the dispatcher posts depletions at the running pool average — exact same math as wac_perpetual — but the value-leg transfer is flagged in `transfers_provisional` with `cost_method='wac_periodic'`. At close, `wac_periodic_close_hook` walks each pool with un-finalized rows, computes `final_avg = Σ(in-period receipts value) / Σ(in-period receipts qty)` (Oracle PAC convention; numerator and denominator both per-pool, signed by debit/credit on the value account), and posts variance per depletion routed through `variance_wac_period`. Empty-pool-on-depletion = P0006 (same as wac_perpetual). Zero-receipts-in-period = **P0020** (`wac_periodic_close_no_receipts`); operator either posts a receipt and retries, or closes with `p_force_provisional=TRUE` to leave un-finalized rows on the side table for forensics. Receipts on wac_periodic SKUs do not flag — they post at their actual asserted cost (po_unit_price, cycle_count_adj amount, etc.) and contribute to `final_avg` directly. Alternate provisional cost sources (last period close avg, last purchase price, configured value, zero, standard) tracked as Epic I (`acct-cms`).
- `wac_retroactive` — perpetual chain mid-period; chronological replay at period close re-costs each depletion against the running avg it should have had given full-period data, including late-arriving receipts that were originally booked out of order (acct-9tw, migration 0031). Replay order is `(business_date, posted_at, id)` with full determinism. Variance per depletion routes through `variance_wac_retroactive`. WIP class deferred (`acct-p7v`).

**FIFO/LIFO/lot scaffolded but unimplemented.** The dispatcher's `'fifo'` and `'lot'` branches RAISE `P0006`. Real implementation requires lot-tracking infrastructure (lot creation, expiry, traceability), which is a feature larger than just costing. Tracked as `acct-8gg` once lot infrastructure is scoped.

### 4.3 Operational queries

All served by base-table queries on `transfers`:

- "Value in WIP right now by op": `SELECT (debits_total - credits_total) FROM accounts WHERE kind='inv_value_wip' AND routing_op=$op`.
- "RM consumed at Op N this period": `SELECT SUM(amount) FROM transfers WHERE reason='rm_issue_to_wo' AND routing_op=$op AND period_id=$p`.
- "Labor applied by op by period": same shape.
- "Full cost trail for WO X": `SELECT * FROM transfers WHERE document_kind='work_order' AND document_id=$wo ORDER BY posted_at`.
- "Scrap $ by op YTD": `SELECT routing_op, SUM(amount) FROM transfers WHERE reason='scrap_v' AND posted_at >= $year_start GROUP BY routing_op`.

### 4.4 Sub-assembly consumption

A sub-assembly consumed at an op of a higher-level parent is a component issue: `debit stock_consumed(sub_sku), credit stock_available(sub_sku, fg_loc)`. Multi-level BOMs nest naturally because the schema is flat.

## §5. Multi-currency / FX

- Transacting currency is a column on `accounts` and is enforced same-currency on every transfer (write-function check).
- Reporting currency (USD) rollups happen at query time joining `fx_rates`.
- Period-end revaluation: posted as `fx_leg` transfers against `fx_revaluation` accounts (Part IV §3.10 pattern).
- Cross-currency transactions: two transfers in one Postgres transaction (Part IV §3.10).

The "Currency Exchange recipe" carries forward in shape; the implementation drops the "linked transfer" flag because Postgres transactions provide the same atomicity.

## §6. Period close

Period close is an orchestrated operation, not a manual `UPDATE periods SET closed_at`. The orchestration ships in migration 0026 (`acct-s6n` / Phase 1) and lives behind one function:

```sql
close_period(
  p_period_id         BIGINT,
  p_actor             UUID,
  p_force_provisional BOOLEAN DEFAULT FALSE,  -- bypass un-finalized provisional rows
  p_force_recon       BOOLEAN DEFAULT FALSE   -- bypass reconciliation alerts
) RETURNS JSONB                               -- audit summary
```

Sequence inside `close_period`:

1. **Validate** — `SELECT ... FROM periods WHERE id = p_period_id FOR UPDATE`. Raises **P0014** (`period_close_invalid`) if the period is missing or already closed. The row-level lock serializes concurrent close attempts on the same period — the loser re-reads `closed_at` after the winner commits and surfaces P0014.

2. **Run hooks** in a fixed order. As of `acct-og1` (migration 0032) all three hooks have real bodies; the s6n stub-bullet era is over.
   - `wac_periodic_close_hook(period_id, p_force_provisional)` — real body landed `acct-qfj` (migration 0029). For each pool with un-finalized `wac_periodic` provisional rows, computes the period avg as `Σ(in-period receipts value) / Σ(in-period qty)` and re-costs every depletion in the period against it; posts variance through `variance_wac_period`. Oracle PAC convention.
   - `wac_retroactive_close_hook(period_id, p_force_provisional)` — real body landed `acct-9tw` (migration 0031). Walks pool events chronologically `(business_date, posted_at, id)`, re-costs each provisional depletion against the recomputed running avg, posts variance through `variance_wac_retroactive`.
   - `cost_adjust_retroactive_hook(period_id, p_force_provisional)` — real body landed `acct-og1` (migration 0032). Walks the `inventory_cost_adjustments_retroactive` queue rows for this period (operator-queued via §3.14); for each, walks every in-period credit-side qty-bearing depletion on the pool and posts a 2-transfer variance batch through `variance_cost_adjust_retro` per non-zero-variance depletion. Method-agnostic — works regardless of the SKU's `cost_method`.

   Each hook returns `BIGINT` — the count of rows it finalized. Hooks run **before** step 5 stamps `closed_at` so any variance transfers they post don't trip `post_transfers`' P0005 gate on the period being closed.

3. **Provisional gate** — count `transfers_provisional` rows in this period with `finalized_at IS NULL`. If > 0 and `p_force_provisional = FALSE`, raise **P0015** (`period_close_provisional`). Forced close leaves un-finalized rows on the side table as-is for forensics — it does not auto-finalize them.

4. **Reconciliation gate** — call `run_daily_reconciliation()`. If new alerts > 0 and `p_force_recon = FALSE`, raise **P0016** (`period_close_reconciliation`). Forced close still records the alerts; the gate just doesn't block.

5. **Stamp** — `UPDATE periods SET closed_at = clock_timestamp(), closed_by = p_actor`.

6. **Return** a JSONB audit summary the caller persists or logs:

   ```json
   {
     "period_id": 1,
     "period_code": "2026-04",
     "closed_at": "2026-05-01T12:00:00Z",
     "closed_by": "uuid",
     "finalized_count": 0,
     "hook_results": {
       "wac_periodic": 0,
       "wac_retroactive": 0,
       "cost_adjust_retroactive": 0
     },
     "unfinalized_remaining": 0,
     "alerts": 0,
     "forced": { "provisional": false, "recon": false }
   }
   ```

The two force flags are **independent** by design — provisional and reconciliation gates protect against very different failure modes (incomplete workflow vs corrupt ledger), and an operator might reasonably need to bypass one without bypassing the other.

**`transfers_provisional` side table** (migration 0025, `acct-4mt`):

```sql
CREATE TABLE transfers_provisional (
  transfer_id          BIGINT PRIMARY KEY REFERENCES transfers(id),
  period_id            BIGINT NOT NULL REFERENCES periods(id),
  cost_method          cost_method NOT NULL,
  finalized_at         TIMESTAMPTZ,
  variance_amount      BIGINT,
  variance_transfer_id BIGINT REFERENCES transfers(id)
);
```

Three lifecycle states enforced by CHECK:

| `finalized_at` | `variance_amount` | `variance_transfer_id` | Meaning |
|---|---|---|---|
| `NULL` | `NULL` | `NULL` | un-finalized (writer just inserted) |
| `NOT NULL` | `0` | `NULL` | finalized, no variance to post |
| `NOT NULL` | `<> 0` | `NOT NULL` | finalized, variance posted (transfer FK) |

A side table is required because the append-only trigger on `transfers` (§1.9) blocks `UPDATE`/`DELETE`, so `finalized_at` can't live as a column on the transfer itself.

**Variance account kinds** (added in migration 0025, seeded in `db/fixtures/small/seed.sql` for USD + EUR):

| `account_kind` | Posted by | Income-statement story |
|---|---|---|
| `variance_wac_period`         | `wac_periodic_close_hook`         | wac_periodic re-pricing close adjustment |
| `variance_wac_retroactive`    | `wac_retroactive_close_hook`      | wac_retroactive late-data correction |
| `variance_cost_adjust_retro`  | `cost_adjust_retroactive_hook`    | retroactive cost_adjustment correction |

All three are bidirectional (`normal_side='unrestricted'`); write-ups credit them, write-downs debit them — same convention as `inv_adj_expense` and `variance_cost_adjustment`. Three separate kinds rather than one shared `variance_close` so the income statement reports the three close-time correction stories distinctly.

Adjusting entries posted **after** close use `post_transfers(..., p_override_closed_period := TRUE)` with the `reversal` reason or one of the variance reasons.

**Known limitation.** `p_actor` is unvalidated — `close_period` accepts any UUID and stores it. RBAC is Part VII Q6, still open.

**§△-10 closed.** Period locking is a database invariant, not API discipline.

## §7. Reconciliation

Daily jobs:

1. **Per-ledger double-entry** (B3 fix):
   ```sql
   SELECT ledger_kind, currency, SUM(debits_total) - SUM(credits_total) AS imbalance
   FROM accounts
   GROUP BY ledger_kind, currency
   HAVING SUM(debits_total) <> SUM(credits_total);
   ```
   Must return zero rows.

2. **Subledger-to-control:** sum per-counterparty AR/AP from base tables (when per-counterparty accounts are used); compare to control account balance. Same database, same transaction snapshot — guaranteed consistent.

3. **Tier-2 mat view freshness:** if using trigger-maintained aggregates (Part IV §9), periodically rebuild from base tables and diff. Detects trigger bugs.

4. **Reservations vs accounts:** sum active reservations per (sku, location); the `v_inventory_available` view's `qty_promisable` matches.

5. **Lot/serial reconciliation** (if those modules are enabled): serial rows grouped by `(sku, current_location, status)` must sum to the corresponding inventory account balances.

## §8. Hot-account scaling — sharding as first-class

```sql
CREATE TABLE account_shards (
  parent_account_id BIGINT NOT NULL REFERENCES accounts(id),
  shard_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  shard_index       INT NOT NULL,
  PRIMARY KEY (parent_account_id, shard_index),
  UNIQUE (shard_account_id)
);
```

Each parent (logical) account has N child (physical) accounts. The write function picks one shard at random (or by hash of `document_id`); the read aggregates across shards.

Materialized parent balance:

```sql
CREATE MATERIALIZED VIEW v_account_parent_balances AS
SELECT
  s.parent_account_id AS id,
  SUM(a.debits_total)  AS debits_total,
  SUM(a.credits_total) AS credits_total
FROM account_shards s JOIN accounts a ON a.id = s.shard_account_id
GROUP BY s.parent_account_id;
```

Shard when `pg_stat_activity` shows lock waits >0.5% on a row, sustained. Typical shard count: 4–16. De-sharding: sum shards into the parent and drop shards — a SQL operation, runnable online.

## §9. Read model — tiered

**Tier 1 — direct queries against base tables.** Sufficient for most reads:
- "Available stock at (sku, location)": one accounts row + one reservations aggregate.
- "WO cost trail": one transfers index range scan.
- "GL trial balance": `SELECT kind, SUM(debits_total - credits_total) FROM accounts GROUP BY kind`.

**Tier 2 — incremental materialized views.** For aggregations too heavy for tier-1:
- `inv_by_sku_location` (with reservation-adjusted qty): trigger-maintained or `pg_ivm`.
- `gl_balances_by_period` (debits, credits, balance per period): trigger-maintained.
- `wo_summary` (qty in WIP, accumulated cost, age): trigger-maintained.

```sql
CREATE TRIGGER trg_update_inv_by_sku_loc
  AFTER INSERT ON transfers
  FOR EACH ROW
  WHEN (NEW.reason IN ('po_receipt', 'so_ship', 'bin_move', 'to_release', 'to_receipt',
                       'cycle_count_adj', 'rm_issue_to_wo', 'wo_complete'))
  EXECUTE FUNCTION fn_update_inv_by_sku_loc();
```

Cost: 10–25% added write latency. Acceptable for ERP workloads.

**Tier 3 — async projection (only when justified).**
- ClickHouse mirror for OLAP reporting (large GROUP BYs, TBs of history).
- Search over text fields (Elasticsearch / Postgres FTS).
- Cross-system fanout (data warehouse, ML feature store).

Use logical replication (`wal2json`, Debezium). **No AMQP, no RabbitMQ.**

## §10. Commodity provisional pricing

The §17 patterns from `ledger_inventory_design_spec_v0.md` carry forward; only the implementation simplifies.

### 10.1 Provisional receipt

```
events:
  - reason='po_receipt_provisional'
    debit:  accounts(stock_available, sku, recv_loc).id
    credit: accounts(vendor_pool, vendor_id).id
    amount: qty
  - reason='po_receipt_provisional'
    debit:  accounts(inv_value_raw, sku, currency).id
    credit: accounts(ap_unsettled, vendor_id, currency).id
    amount: qty * provisional_price
```

Same Postgres transaction inserts the `commodity_receipts` row and sets `purchase_orders.pricing_status='provisional'`.

### 10.2 Settlement

**Step 1 — compute attribution (Postgres).** Default policy: **FIFO** (recommended — §△-15 resolution). Maintain `commodity_pool_activity` as a tier-2 mat view from `transfers` filtered to receipts/issues for commodity SKUs:

```sql
CREATE MATERIALIZED VIEW commodity_pool_activity AS
SELECT
  t.document_id AS receipt_id,
  t.posted_at,
  t.amount AS qty,
  CASE WHEN t.reason = 'po_receipt_provisional' THEN 'in' ELSE 'out' END AS direction
FROM transfers t
WHERE t.reason IN ('po_receipt_provisional', 'so_ship', 'rm_issue_to_wo', 'scrap')
  AND EXISTS (SELECT 1 FROM skus s WHERE s.id = ... AND s.is_commodity);
```

(Exact predicate elaborated per implementation.)

For the settling receipt, derive `Δ_onhand`, `Δ_consumed`, `Δ_wip` such that `Δ = (final_price − provisional_price) × qty_received`.

**Step 2 — post the settlement batch:**

```
events (signs shown for Δ > 0):
  - reason='po_settlement'
    debit:  accounts(ap_unsettled, vendor_id, currency).id
    credit: accounts(ap, vendor_id, currency).id
    amount: qty * provisional_price

  - reason='price_trueup_inventory'
    debit:  accounts(inv_value_raw, sku, currency).id
    credit: accounts(ap, vendor_id, currency).id
    amount: Δ_onhand

  - reason='price_trueup_cogs'
    debit:  accounts(cogs, sku, currency).id
    credit: accounts(ap, vendor_id, currency).id
    amount: Δ_consumed

  - reason='price_trueup_wip', routing_op=NN
    debit:  accounts(inv_value_wip, parent_sku, currency).id
    credit: accounts(ap, vendor_id, currency).id
    amount: Δ_wip_for_op_NN
    (one event per affected op)
```

Same transaction updates `commodity_receipts.final_price`, `settled_at`, and sets `purchase_orders.pricing_status='settled'`.

### 10.3 M6 resolution — materiality threshold

Default policy: forward-only true-up (no per-shipment COGS restatement).

If aggregate Δ exceeds **5%** of the settling cohort's value at provisional pricing, additionally book `cost_restate` reversal-and-rebook events against affected COGS posts (one per consumed shipment). The 5% threshold is set by accounting policy and tunable.

Below the threshold, document explicitly: per-shipment unit cost in COGS reflects provisional price for shipments closed before settlement; reports on commodity margin must read with this caveat.

### 10.4 Edge cases

Carried from `ledger_inventory_design_spec_v0.md` §17.6 — partial settlements, retroactive quality adjustments, formula-priced-with-no-cap, prior-period settlement (default = current-period delta unless materiality flips).

## §11. Explicit non-decisions

- **No lot/serial accounts in the ledger.** Lots and serials live in dedicated Postgres tables with FK to inventory accounts. Aggregate balance reconciliation between (sku, location, status) groupings of serial/lot rows and inventory account balances catches drift. Do not revisit without compelling cause.
- **No FEFO in the ledger.** External sorted projection if needed.
- **No FIFO/LIFO cost layers in the ledger.** Cost engine produces unit cost; ledger records the result.
- **No reporting in the ledger.** Tier-1, tier-2, or tier-3 — never mixed in the write path.
- **No browser-direct database access.** API tier is mandatory.
- **No server-side ID generation in the API tier.** All IDs are `BIGSERIAL` from Postgres or `UUID` from clients (idempotency key) — explicit.
- **No direct UPDATE of `accounts.debits_total` / `credits_total` from application code.** The `post_transfers` function is the only path.
- **No deletion of accounts or transfers.** Closed, yes; deleted, no.
- **No `synchronous_commit = off` on the ledger database.** RPO unacceptable.
- **No reliance on TigerBeetle features.** If TB is needed, it is added downstream via logical replication.

## §12. Operations

### 12.1 What runs

- Postgres primary + 2–3 streaming replicas.
- pgBouncer or PgCat (transaction pooling).
- Standard PITR / WAL archiving.
- pg_cron for scheduled jobs (reservation expiry, period-close snapshots, daily reconciliation).
- Standard Postgres monitoring (`pg_stat_*`, autovacuum, replication lag).
- Optional: outbox worker (if §2.2 outbox is used), tier-2 mat view refresh, tier-3 logical replication consumers.

### 12.2 What does not run

- TB cluster.
- AMQP CDC sidecar.
- RabbitMQ broker (unless used for something else).
- Projector service for tier-3 (until and unless tier-3 is justified).
- Reconciliation jobs comparing TB to Postgres or outbox to TB transfer ids.

### 12.3 Throughput envelope (rough)

Single Postgres node, well-tuned (NVMe, 64+ GB RAM, modern CPU, `synchronous_commit=on`):

| Workload pattern | Sustained TPS |
|------------------|---------------|
| Pure inserts to `transfers`, batch=100 | 30–60K |
| `post_transfers` with full validation, batch=10 | 5–15K |
| `post_transfers` with hot-account contention, no sharding | 500–2K |
| With 8-way sharding on hot accounts | 4–10K |
| Read-heavy mix served by replicas | unchanged write side |

Envelope estimates, not benchmarks. Most ERP workloads (10s–100s of transfers/sec sustained, 1–2K peak) live well inside without sharding.

### 12.4 Monitoring

- `pg_stat_activity` lock_waits by relation — alert on `accounts` lock_waits > 1% sustained.
- `pg_stat_database` deadlocks involving ledger tables — alert any.
- Outbox worker (if present): `state='pending'` rows older than 10s; failed rows immediately.
- `post_transfers` latency: P50, P99, P99.9.
- Per-account contention heatmap.
- Reconciliation jobs: daily success, any non-zero invariant alerts immediately.
- Tier-2 mat view refresh lag (if applicable).
- Tier-3 logical replication slot lag (if applicable).

### 12.5 Security

- API tier authenticates all writers; Postgres role per service.
- Network: ledger database in a private subnet; only the API tier reaches it.
- TLS on all Postgres connections.
- Authorization: every API call validates (user, action, document) against application RBAC before composing the transfer batch.
- `transfers` table append-only via the trigger in §1.9; UPDATE/DELETE permissions revoked from all but a documented break-glass DBA role.
- Audit: `posted_by` is mandatory on every transfer; logical replication to a write-once audit store (e.g., S3 Object Lock) for regulated environments.

## §13. Phasing

**Phase 0 — Schema and write function (~3 weeks):**
- Tables (accounts, transfers, inventory_reservations, periods, fx_rates, period_snapshots, commodity_receipts, optional outbox).
- `post_transfers` function.
- Append-only enforcement on `transfers`.
- Phase 0 invariant tests: per-ledger double-entry, no negative balances, idempotency, deadlock freedom (100 concurrent batches, random subsets, 30 min, zero deadlocks), period-lock enforcement, reservation atomicity.
- Done when invariant tests pass.

**Phase 1 — Application functionality (3–4 months):**
- All transaction patterns (Part IV §3).
- WIP with per-op accounts, standard costing, WO lifecycle, variances.
- Quarantine/scrap, cycle counts, reservations.
- Multi-currency with FX recipe.
- Commodity provisional pricing.
- Reconciliation jobs.
- Daily double-entry invariant.

**Phase 2 — Tier-2 mat views (1–2 weeks, applied per-need):**
- `inv_by_sku_location`, `gl_balances_by_period`, `wo_summary` (only those measurably needed).
- Trigger-maintained or `pg_ivm`.
- Recon job to detect trigger bugs.

**Phase 3 — Tier-3 if justified (out of scope unless triggered):**
- Logical replication slot.
- ClickHouse / Elasticsearch / search consumer per-need.

No Phase 4 or 5. The CDC seam (logical replication) is the migration option if a future signal (Part VI §3) justifies it.

## §14. Performance and correctness testing

Testing the ledger is not one activity — it is three layers, run in order. Skipping a layer or starting in the wrong order produces measurements that look authoritative but mislead. The progression: **exploratory → structured methodology → integrated perf testing.**

### 14.1 Exploratory phase

**Purpose.** Discover the system's actual behavior on real hardware before formalizing what to measure. Replace the envelope estimates in §12.3 with measured numbers. Identify the dominant bottleneck (CPU, lock contention, WAL, index maintenance, disk IOPS) so the structured phase targets it.

**Pre-conditions.**
- Phase 0 schema and `post_transfers` function are implemented and pass invariant tests.
- A representative hardware tier is provisioned (e.g., 8 cores / 32 GB RAM / NVMe; and a second tier of 32 cores / 128 GB RAM for headroom). Cloud equivalents acceptable as long as IO is consistent.
- `pg_stat_statements`, `auto_explain`, `pg_wait_sampling` extensions installed.

**Fixture data, three sizes:**

| Fixture | Accounts | Transfers | SKUs × locations | Use |
|---------|----------|-----------|------------------|-----|
| small   | 1K       | 100K      | 100 × 10         | smoke / dev loop |
| medium  | 100K     | 10M       | 1K × 100         | typical perf runs |
| large   | 1M       | 100M      | 10K × 1000       | capacity / soak |

Generated by a deterministic script, checked into the repo, regenerable from seed.

**Activities.**

1. **Per-pattern micro-benchmarks.** For each transaction pattern in Part IV §3 (PO receipt, SO ship, WO op-move, scrap, cycle count, AR/AP payment, FX leg, commodity settlement), measure:
   - P50/P99/P99.9 latency at single-writer
   - Sustained TPS at 1, 4, 16, 64 concurrent writers
   - CPU%, IO wait, lock wait time, WAL bytes/op
   - Behavior across the three fixture sizes

2. **Workload mixes.** Run blended workloads at varying ratios — 70/30, 50/50, 90/10 read/write — to surface contention shapes that single-pattern tests miss. Measure where blended throughput diverges from the sum of single-pattern throughputs.

3. **Hot-account stress.** Concentrate writes on N hot accounts (N = 1, 10, 100) with M writers each. Plot the contention curve. This is the data that drives the §8 sharding decision and replaces the §△-E hand-wave.

4. **Resource-limit sweeps.** Vary cores (2, 4, 8, 16, 32), RAM (8, 32, 128 GB), `shared_buffers`, `synchronous_commit`, batch size, partition granularity. Find the knees in each curve.

5. **Bottleneck identification.** Use `pg_wait_sampling` and `pg_stat_statements` to attribute time. Classify each scenario's limiting resource: CPU-bound, lock-bound, WAL-bound, IO-bound, index-maintenance-bound.

**Tooling.**
- `pgbench` for raw primitive ops (baseline INSERT/UPDATE/SELECT throughput).
- Custom harness (Python or Go) that calls `post_transfers` with realistic batch payloads.
- `pg_stat_statements` + `auto_explain` for query attribution.
- `pg_wait_sampling` for fine-grained wait analysis.
- `pg_buffercache` for hot-page identification.

**Output.** A baseline document — committed to the repo as `perf_baseline_vN.md` — recording: per-pattern P50/P99/P99.9 and sustained TPS at each fixture size, hot-account contention curves, identified bottlenecks per scenario, observed WAL throughput, and pgbench primitives for hardware comparability. This document supersedes §12.3's envelope estimates.

**Time budget.** ~2 weeks. Less than that and the structured phase will rest on thin data.

### 14.2 Structured methodology

**Purpose.** Once exploratory data is in hand, formalize the test surface so performance becomes a regression-detected property of the codebase, not a thing checked at release time.

**Workload definitions.** Capture three to five reference workloads representing real customer profiles:

| Workload | Mix |
|----------|-----|
| ecommerce | 60% SO reservation/ship, 20% PO receipt, 10% bin_move, 10% AR payment |
| manufacturing | 50% WO ops (start/op_move/complete), 20% RM issue, 15% PO receipt, 10% scrap, 5% cycle count |
| distribution | 40% bin_move, 30% TO release/receipt, 20% PO receipt, 10% SO ship |
| month-end close | reconciliation queries + FX revaluation + period close, large fixture |
| backfill | bulk COPY into transfers + accounts opening balance |

Each workload is a script consuming a fixture and producing a measurable load. Mixes are derived from real customer telemetry where possible; otherwise from operational expectations.

**SLO definitions per pattern.** Set after exploratory data is in:

| Pattern | P99 latency | Sustained TPS | Notes |
|---------|-------------|---------------|-------|
| PO receipt | TBD ms | TBD | medium fixture, 16 writers |
| SO ship | TBD ms | TBD | "" |
| WO op_move | TBD ms | TBD | "" |
| Reservation insert | TBD ms | TBD | "" |
| Period close | < TBD min | N/A | large fixture |
| Daily reconciliation | < TBD min | N/A | large fixture |

(Numbers fill in from §14.1 output.)

**Test types and cadence.**

| Type | Cadence | Fixture | Gate |
|------|---------|---------|------|
| Unit perf | every PR (CI) | small | P99 regression >10% blocks merge |
| Integration perf | nightly | medium | P99 regression >5% pages oncall |
| Soak | weekly | large | 24h run; alerts on memory growth, vacuum lag, index bloat, replication lag |
| Capacity | per release | large | ramp until SLO breaks; record max sustained TPS |
| Chaos / fault injection | per release | medium | kill primary, OOM a worker, fill disk; verify recovery |

**Correctness under load.**

- **Property-based tests.** Generate random valid batches (Hypothesis / fast-check style). Assert per-ledger double-entry holds after each batch; reservations + transfers stay consistent; no torn writes.
- **Fault injection.** Mid-batch backend kill, connection pause, disk-full simulation, replica lag spike. Recover; verify idempotency-key uniqueness, no half-applied state.
- **Determinism replay.** Capture a batch sequence; replay against a fresh DB; assert identical resulting state.

**Perf-regression CI.**

- Every PR runs unit perf tests on the small fixture in a sized CI runner.
- Results compared against the main-branch baseline (rolling window of last 20 main runs).
- Regression threshold: P99 latency >10% or TPS <-10% blocks merge unless waived with rationale.
- Baseline updates automatically on main merge after passing.

**Reporting.** Standardized JSON output per run: TPS, P50/P99/P99.9, CPU%, lock wait time, WAL bytes/op, IO wait, error rate. Trended in Grafana (or equivalent). A per-pattern SLO scorecard surfaces in the release readiness check.

**Time budget.** ~3 weeks to set up; ongoing maintenance.

### 14.3 Integrated perf testing — application to database

**Purpose.** §14.1 and §14.2 measure the ledger in isolation. Production workloads traverse the API tier, optional outbox, network, and connection pool before reaching `post_transfers`. Integrated testing catches issues that only manifest end-to-end: connection-pool exhaustion, API-tier CPU saturation under fan-out, outbox queue spikes, transaction wrapping anomalies.

**Pre-conditions.**
- API tier deployed in a staging environment matching production topology (N nodes behind a load balancer, pgBouncer/PgCat in front of the DB, optional outbox worker pool).
- Tracing instrumentation (OpenTelemetry or equivalent) propagating through every layer.
- Standard workloads from §14.2 wrapped as HTTP/gRPC client load.

**Layers to instrument.**

```
Load generator → API tier → [outbox] → post_transfers → Postgres
     │              │            │           │              │
   client P99   API-tier      queue       function       commit
                latency       depth       latency        latency
```

Per-request trace must capture each layer's contribution to total latency.

**Test scenarios.**

| Scenario | What it measures |
|----------|------------------|
| Steady state at 50%/80%/95% of measured capacity | Stable-state SLO compliance |
| Spike: 0 → 200% capacity in 10s, sustain 5 min, return | Backpressure behavior, queue depth, recovery time |
| API node loss: kill 1 of N mid-flight | Graceful degradation, no lost requests |
| DB primary failover | RTO/RPO; idempotency under retry storm |
| Outbox worker death (if used) | Drain time after restart; no stuck rows |
| Capacity ceiling | Ramp until SLO breaks; identify limiting resource (API CPU, DB connections, DB CPU, disk) |
| Multi-tenant fairness | One tenant ramps to 10× their quota; other tenants' SLO unaffected |
| Long-running soak (24–72h) | Memory leaks, file descriptor leaks, replication slot bloat, vacuum behavior |

**What this enables.**

- Honest capacity planning: translate a customer's projected order rate into a hardware spec with confidence.
- Sharding decisions backed by data, not vibes (§8 trigger).
- API-tier sizing (cores/instances).
- Tier-3 trigger evidence (when does logical replication itself need its own scaling?).
- Post-incident replay: reproduce a production incident as a load test scenario; verify the fix.

**Tooling.**
- k6, Locust, or Gatling for load generation. k6 has the lowest overhead for >1K RPS scenarios.
- OpenTelemetry traces propagated client → API → DB. Jaeger or Tempo for retention.
- Per-layer metrics in Prometheus; Grafana dashboards composing all layers into one timeline.
- Chaos tooling: Toxiproxy for network faults, Postgres chaos commands for primary kill, container restart for API node loss.

**Output.** A capacity model — how many cores/GB/IOPS are needed for X transactions/sec at Y P99 latency, with what failure-mode envelope. Reviewed each release; updated when measurable changes shift the curves.

**Time budget.** ~4 weeks to set up; ongoing maintenance.

### 14.4 Phasing of the testing layers

| Layer | Earliest start | Gates |
|-------|----------------|-------|
| Exploratory (§14.1) | end of Phase 0 (write function works) | required before §14.2 |
| Structured (§14.2) | mid Phase 1 (≥3 transaction patterns implemented) | required before MVP release |
| Integrated (§14.3) | late Phase 1 / early Phase 2 (API tier in staging) | required before first customer onboarding |

The three layers persist indefinitely once started — exploratory work continues whenever a new pattern is added or a new bottleneck is suspected; structured tests run forever in CI; integrated tests run per release.

### 14.5 Anti-patterns to avoid

- **Skipping exploratory.** Setting SLO targets before measurement either picks numbers the system can hit trivially (no signal) or numbers it can't (perpetual red dashboards).
- **Benchmarking on dev hardware.** A laptop's NVMe is faster than most cloud disk; results will not transfer.
- **Single-pattern proxies for mixed workloads.** Mix-effects (lock contention between patterns) only show up in mixed runs.
- **Synthetic IDs that defeat caching.** Use realistic distributions (Zipfian for SKU access, etc.); flat-random masks hot-row issues.
- **Ignoring vacuum and replication lag.** Steady-state TPS at hour 1 is not the same as at hour 24.
- **Measuring only happy path.** Failure-mode latency (during failover, during replication catch-up) is what customers actually feel during incidents.

---

# Part V — §△ resolution scoreboard

Original v0.1 had 17 design-spec §△ items + 13 migration-spec §△ items = 30 total. Status under v0.2:

| Item | v0.1 origin | Status in v0.2 |
|------|-------------|----------------|
| §△-1 Per-(SKU, location) value accounts | Account count cost | **Just do it.** PG account creation is cheap. |
| §△-2 Per-warehouse qty ledger | Cluster sharding | **N/A.** Single ledger. |
| §△-3 Counterparty pools — global vs per-entity | Account count cost | **Per-entity.** Trivial in PG. |
| §△-4 Self-pending vs Available/Reserved | Broken pattern (B1) | **Resolved.** Reservations table. |
| §△-5 user_data_64 hash collisions | TB schema | **Eliminated.** Real FK on `counterparty_id`. |
| §△-6 Backfill historical GL | TB import semantics | **Plain COPY.** No flag semantics. |
| §△-7 Standard → WAC transition | Read-on-write taboo (B2) | **Resolved.** Read-on-write fine in PG. |
| §△-8 Kafka/alternative CDC | TB CDC sink | **N/A.** Logical replication if needed. |
| §△-9 Account creation rate limit | TB cost | **N/A.** PG creation cheap. |
| §△-10 Period lock at API | TB has no periods | **Resolved.** Schema enforces. |
| §△-11 Serial/lot reconciliation | TB excludes lots/serials | Carried — modeled in PG with FK. |
| §△-12 Per-WO per-op accounts | TB account cost | **Just do it for job costing** when business demands. |
| §△-13 Backflush implementation | Phase decision | Carried as v0.2 Phase 1 option. |
| §△-14 DR / zero-RPO | TB enterprise | **N/A.** PG PITR + replicas. |
| §△-15 Commodity attribution policy | Business decision | **Pick FIFO** (recommended). |
| §△-16 WAC recompute at settlement | Audit | Resolved by M6 materiality clause. |
| §△-17 Prior-period commodity settlement | GAAP | Carried as policy call (Part IV §10.4). |
| §△-A Sub-second timeout precision | Reservation expiry | **N/A.** SQL + cron; sub-second via `LISTEN/NOTIFY` if needed. |
| §△-B get_balances_batch | PG ergonomics | **Resolved.** A view + WHERE id IN. |
| §△-C Outbox batch size tuning | Tunable | Carried (if outbox used). |
| §△-D COPY-based bulk insert | Tunable | Carried (backfill). |
| §△-E Hot-account sharding | Pre-migration bandage | **First-class technique.** Not a bandage. |
| §△-F CDC mechanism | Logical vs trigger | Per-consumer choice. |
| §△-G Projector partitioning | If projector exists | Mostly N/A. |
| §△-H Phase 3 entry criteria | Migration trigger | **N/A.** No Phase 3. |
| §△-I Reconciliation tolerance | Hybrid recon | **N/A.** Single-system. |
| §△-J Cutover order | Migration plan | **N/A.** |
| §△-K Cross-system batch policy | Hybrid txns | **N/A.** Single-system. |
| §△-L Projection merge | Hybrid sources | **N/A.** Single source. |
| §△-M Reverse migration playbook | Roll back from TB | **N/A.** |

**Score:** Of 30 §△ items, 15 are resolved or N/A. 11 become trivial implementation choices. 4 remain genuine business/policy decisions independent of the system (lot/serial recon cadence, commodity attribution edge cases, prior-period settlement treatment, backflush opt-in).

The §△ list under v0.2 reduces to a tractable tail.

---

# Part VI — What v0.2 keeps and gives up

## VI.1 What v0.2 keeps

- **Double-entry correctness.** Per-ledger invariant test enforces.
- **Atomicity within a batch.** Postgres transactions.
- **Idempotent retries.** `idempotency_key UNIQUE` per event.
- **Append-only audit.** Trigger-blocked UPDATE/DELETE on `transfers`.
- **Multi-currency support.** Same-currency invariant + FX rates table.
- **Reservation semantics.** Better than v0.1 — first-class table, not pending-transfer kludge.
- **WIP modeling.** Same shape; simpler implementation.
- **Period close.** Better than v0.1 — schema-enforced.
- **Reconciliation.** Fewer moving parts; same correctness checks.
- **All ERP semantics.** WO, SO, PO, TO, multi-currency, commodity pricing.
- **Optionality to add downstream specialized systems** (TB, ClickHouse, search) via logical replication when signal justifies.

## VI.2 What v0.2 gives up

### Throughput ceiling

TB sustains 1M+ transfers/sec on a 6-node cluster. Postgres caps at ~50K TPS sustained on a single node (with batching) or hundreds of thousands across a multi-node setup with Citus or similar. If the roadmap genuinely targets >100K TPS, additional engineering is required.

**Mitigation:** the CDC seam (logical replication slot) lets you fan out to a downstream system if the workload changes. The seam is logical replication, not the outbox.

### Cluster-level immutability by construction

TB provides cluster-level guarantees about transfer immutability stronger than Postgres's row-level immutability. In Postgres, an attacker with database write access can `UPDATE transfers SET amount = ...`. Mitigated by:
- Strict role-based access (no human has UPDATE on `transfers`).
- Trigger-enforced "no UPDATE/DELETE on transfers" (raises exception).
- Append-only audit log via logical replication to a write-once store.

Equivalent to TB in practice if disciplined; not equivalent by construction. A regulated environment with strict immutability requirements may need an external WORM tier.

### Native batching at the storage layer

TB's storage engine is built for batch ingestion. Postgres benefits from batching but at a smaller multiplier. For batch-heavy ingestion (bulk migration of 5+ years of GL history), TB has a clear edge. Postgres handles it with COPY + careful tuning.

### The optionality itself

Some teams want the *option* to migrate to TB later, even if they never exercise it. v0.2 drops the day-1 parity tax and defers the option to "if the workload changes, stream to TB via CDC." That is a partial restoration, not the full v0.1 promise.

If retaining TB optionality is itself the requirement (regulatory, organizational), v0.2 does not apply — but B1, B2, B3 must still be fixed in v0.1.

### Schema drift risk

Postgres's flexibility means the schema can drift via accreted columns, tables, partial indexes, triggers. TB's rigid schema is its own form of discipline. Mitigate with code review discipline and migration governance — same as any Postgres app.

The list of "what you give up" is shorter than the list of "what you keep." That is the case being made.

## VI.3 When to reconsider — re-introducing TB or alternatives

Future signals that legitimately justify revisiting:

1. **Sustained >50K TPS** on the write path. Single-node Postgres is past its envelope.
2. **Hot-account contention not resolvable with sharding.** Sharded to 16 ways, still seeing lock waits.
3. **Regulatory append-only-by-construction requirement.** Some compliance regimes want immutability that Postgres-with-discipline doesn't quite deliver.
4. **Cross-region active-active.** Postgres logical replication is multi-master-capable but operationally finicky; TB's replication is tighter; CRDB/Spanner are also candidates.

When such a signal arrives:
- **Don't migrate the write authority.** Keep Postgres as the source of truth. Stream to the secondary system (TB, BigQuery, S3+Iceberg) via logical replication. Use the secondary for the workload it is good at.
- **Re-evaluate before assuming TB.** "10K TPS" was a meaningful number ten years ago. Modern Postgres does that comfortably.

The CDC seam is the thing to preserve. Logical replication slots, named in the schema, are the migration option. Not the outbox table.

---

# Part VII — Open questions (gating v0.2)

Decisions that need answers before v0.2 implementation can proceed:

1. ~~**Is TB optionality a hard requirement** (regulatory, organizational, strategic), or a "nice to have" that has been rationalized into the architecture?~~ **Resolved (2026-04-29):** TB optionality is **not a hard requirement**. TigerBeetle remains a *reference model* for behavioral correctness (atomicity, lock semantics, idempotency, append-only invariants) but **not** a parity target the implementation has to shape itself against. Postgres-native ergonomics win where they conflict with TB-shape decisions. The "TB-parity tax" framing in Part III stays as historical context, not as a future-cutover obligation.

2. ~~**What is the realistic 24-month TPS projection** for this workload?~~ **Resolved (2026-04-29):** No fixed TPS target. The project is an **exploration of what's possible with Postgres-native** in place of the v0.1 hybrid Postgres/TigerBeetle design. Goal ordering is **correctness first, performance second**. The implication for the perf baseline (§14.1): we measure to establish a *yardstick*, not to chase a number. Specifically — establish a baseline on the simplest schema before any Phase 1 complexity (customers, work orders, routings, BOMs, alternate cost methods, etc.) is added; subsequent additions get diff'd against the baseline so that "did adding feature X regress throughput?" is answerable. The baseline shipped as `perf_baseline_v0.md` (acct-1ia closed 2026-04-29; outbox shapes G/J/K/L/M added through 2026-04-30 via acct-tyq → acct-yjn). `acct-e8g` (transfers partitioning) and the §14.1 follow-up cost-method workloads (`acct-8gg`) are the remaining downstream items.

3. ~~**Is the outbox load-bearing** in a single-database world?~~ **Originally resolved (Phase 0, 2026-04-27, bd `acct-93b.3`):** No outbox in Phase 0; `post_transfers` is sync, inline. **Re-examined and re-resolved (2026-04-30, bd `acct-0oy`):** Stay sync for now; pivot to shape **L** (pseudo-sync via LISTEN/NOTIFY) deferred to Phase 1+ when contention emerges. The five-shape outbox characterization (G/J/K/L/M) shipped via `acct-tyq` → `acct-hbg` → `acct-dtv` → `acct-yjn` and is documented in `perf_baseline_v0.md` observations 15-17. The original D3 rationale ("sync is dominant by every measure") is **wrong** — under 100-writer contention, L's caller p99 = 547 ms vs F's 8.25 s (15× better) and throughput median is ~1.8× F. The corrected rationale for staying sync is **operational simplicity, not perf superiority**: every Phase 0 test and every Phase 1 fixture is structured around an inline function call, and realistic Phase 1 workflows (per-document transfers, naturally spread across SKUs/locations) don't hit the high-contention regime where L's advantages are load-bearing. We commit to revisiting once Phase 1 produces measured contention — tracked as `acct-c4p` with explicit triggers. The L infrastructure (DrainConfig, listener dispatcher, drain-tx pg_notify with SQLSTATE payload) is already built and benched; the future pivot is additive, not a foundation rewrite.

4. ~~**Cost method:** standard, WAC, or hybrid?~~ **Resolved (Phase 0, 2026-04-27, bd `acct-93b.4`):** Phase 0 shipped `'standard'` first (migration 0019, `acct-0ig`) then `'wac'` (migration 0021, `acct-uxu`, 2026-04-30). The `cost_method` enum carries `'standard' | 'wac' | 'fifo' | 'lot'`; `skus.cost_method` (default `'standard'`) drives the dispatcher in `post_transfers`. `'fifo'` and `'lot'` branches still raise `P0006 cost_method_not_implemented` — tracked as `acct-8gg` once lot infrastructure is scoped. WAC's pool-balance read uses FOR UPDATE on the matching qty + value accounts (B2 explicitly resolved: read-then-write under lock is safe in PG).

5. **Reservation lifetime expectations:** are sub-second timeouts ever needed? Drives whether `pg_cron`-based expiry is sufficient or whether `LISTEN/NOTIFY` is needed.

6. **Append-only enforcement model:** trigger-blocked UPDATE/DELETE on transfers (as in §1.9), RBAC-only, or both? Implication for cluster-level immutability claims.

7. **CDC sinks at MVP:** none, search index, OLAP store, all of the above? Drives the tier-3 design timing.

8. **Commodity materiality threshold:** the 5% suggestion in §10.3 is a placeholder. Accounting policy sets the real number.

9. **Tier-2 mat view scope at MVP:** which aggregations are measurably worth the trigger overhead at Phase 1? (Default: none — start at tier-1, promote on signal.)

10. **Per-WO per-op account opt-in:** which SKU families or WO types warrant job-cost grain? (Default: none; opt in when Finance or operations requests.)

~~Answers to (1) and (2) determine whether this entire document is the right framing or a footnote.~~ With (1) and (2) resolved, this document is the framing. Answers to (5)–(10) determine the v0.2 spec's final shape.

---

# Appendix A — Document map

This consolidated document supersedes four working files now archived under `ARCHIVE/`:

- `ARCHIVE/ledger_inventory_design_spec_v0.md` — v0.1 design spec (TB-parity). Historical reference.
- `ARCHIVE/phased_migration_spec_v0.md` — v0.1 migration roadmap (Postgres → TB Phase 0–5). Historical reference.
- `ARCHIVE/spec_review_v0.md` — critical review of v0.1. **Folded into Part II** of this document.
- `ARCHIVE/postgres_native_design_v0.md` — Postgres-native redesign argument. **Folded into Parts III, IV, V, VI** of this document.

The archived files are preserved as a record of the design evolution; this consolidated document is the working reference going forward.

# Appendix B — Suggested next steps

Status as of 2026-04-30: items 1-2 and 4 are complete; item 3 is dead (v0.1 abandoned in favor of v0.2 by this consolidated doc). What's left is Phase 1 framing, which Part VII items (5)-(10) shape.

1. ~~**Resolve the gating questions** in Part VII items (1) and (2) explicitly~~ — Done 2026-04-29. v0.2 is the framing.
2. ~~**Start Phase 0** (Part IV §13).~~ — Done. Schema, `post_transfers` (with WAC dispatcher), reservations, period close, reconciliation, perf baseline (13 shapes), and the conformance harness all shipped under epic `acct-93b`. The three remaining open issues (`acct-0oy`, `acct-e8g`, `acct-8gg`) are P3 and explicitly gated on Phase 1 framing or §14.1 follow-up.
3. ~~**If v0.1 proceeds instead:**~~ — Not applicable. v0.1 was abandoned by this consolidated doc.
4. ~~**Add a conformance test fixture** for the write function.~~ — Done. `tests/data/conformance.json` + `tests/conformance.rs` (107 cases, 11 batch-vs-split tagged).
