# Postgres-Native Ledger — Critical Redesign

Version: 0.1
Status: Draft. Companion critique to `phased_migration_spec_v0.md` and `ledger_inventory_design_spec_v0.md`. Argues for dropping TigerBeetle parity as a constraint and rebuilding the spec around what Postgres is actually good at.

---

## 0. Framing

The current v0.1 specs make one structural bet: **the Postgres implementation should be a drop-in for TigerBeetle, byte-for-byte semantically.** Every other choice is downstream:

- `NUMERIC(39)` everywhere because TB uses u128.
- Six fixed transfer fields (`user_data_128/64/32`, `code`, `ledger`, `flags`) because TB does.
- Linked-chain rollback machinery because TB has no transactions.
- Pending/post/void primitive because TB has no row-level state.
- Lazy account materialization because TB account creation is expensive.
- Client-side time-ordered IDs because TB has no sequences.
- Flat account model with `flags` bit field because TB has no schema.
- Async projection layer because TB has no joins.
- AMQP CDC sidecar + RabbitMQ + Kafka evaluation because TB has no logical replication.
- Counterparty hashing into `user_data_64` because TB has no foreign keys.

**Every one of these is a tax you pay to keep TB optional.** The implicit bet is that the optionality is worth the tax. This document argues the opposite for the workload described:

1. The workload (ERP/inventory ledger, ~10K TPS upper-bound estimate, multi-currency, WIP, commodity pricing) is squarely in Postgres's wheelhouse.
2. The TB entry criteria (`phased_migration_spec_v0.md` §6.1) require sustained 10K TPS with hot-account skew, which most ERP-style businesses never reach.
3. The §△-list catalogues 30 open issues across both specs; the *majority* exist because TB's primitives don't fit the domain cleanly.
4. Dropping the parity constraint resolves several of those §△ items directly, eliminates ~40% of the implementation work, and produces a system that is faster, simpler, and more debuggable than either pole of the current design.

The right framing is not "Postgres ledger that becomes TigerBeetle later." It is "Postgres ledger sized for the workload, with a documented escape hatch if the workload changes." The escape hatch is real (CDC out to a downstream system, sharding by ledger, etc.) but doesn't dictate the day-1 schema.

This document details what changes when TB parity is dropped: schema, write path, read path, reservations, cost computation, multi-currency, period close, hot-account scaling, operations. It also catalogues honestly what you give up, and what signal would justify revisiting.

---

## 1. What TB-parity costs, line by line

A summary of the parity tax in the v0.1 specs, before proposing replacements:

| Constraint inherited from TB | Cost in Postgres |
|------------------------------|------------------|
| `NUMERIC(39)` on all amount/ID columns | 5–10× slower arithmetic, 2× wider indexes vs `BIGINT` |
| Six fixed transfer fields (`user_data_*`, `code`, `flags`) | Loss of FK integrity; polymorphism in indexing; counterparty hashing required (§△-5) |
| Bit-field `flags` column | Bit-arithmetic in CHECK constraints; harder to read in queries |
| Linked-chain rollback via SQLSTATE | ~150 LoC of plpgsql machinery duplicating what `BEGIN/COMMIT` already does |
| Client-generated u128 IDs | Re-implements `BIGSERIAL` with worse cache locality |
| Pending/post/void primitive | Awkward for reservations (which carry business state), requires expiry worker, complicates balance reads |
| Self-pending reservations (broken — see prior review B1) | Forces a redesign anyway; pretending it's a tradeoff is costly |
| Lazy account materialization | Conditional `create_accounts` in every batch; map table; cache invalidation logic |
| `flags.history` semantics | Reserved bit in PG that does nothing until Phase 4+ |
| `flags.imported` semantics | Required for backfill but not implemented in Phase 0 (m7 in prior review) |
| Async CDC + projector for reads | Latency + reconciliation overhead + operational footprint |
| `user_data_128 = document_id` | UUID column constrains values; FK lost (M3 in prior review) |
| `code` u16 enum (65,536 max) | Solves a problem TB has, not one PG has |
| Hot-account contention as Phase 3 trigger | Shards are removed at migration; in PG-native, shards are a feature |
| `_apply_transfer_effect` faithful flag matrix | 1,500–2,500 LoC of conformance-tested PL/pgSQL (M1 in prior review) |
| Read-then-write prohibition (§2 of design spec) | Forces awkward patterns for cost computation (B2 in prior review) |

The parity tax is also a **reasoning tax**. Every developer on the project has to learn TB's semantic model to write Postgres code, even though TB will never be deployed in many timelines. That cognitive overhead compounds.

---

## 2. Revised schema

The schema below targets the same workload and the same correctness properties. It is not a drop-in for TB. It uses Postgres types and Postgres idioms.

### 2.1 Accounts

```sql
CREATE TYPE account_kind AS ENUM (
  -- Inventory quantity
  'stock_available', 'stock_reserved', 'stock_quarantine', 'stock_scrap',
  'stock_in_transit', 'stock_consumed', 'stock_wip',
  -- Counterparty (qty side, optional)
  'supplier_pool', 'customer_pool',
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
  -- Domain references — proper FKs, not user_data hashes
  sku_id          UUID REFERENCES skus(id),
  location_id     UUID REFERENCES locations(id),
  routing_op      INT,                        -- WIP only
  counterparty_id UUID,                       -- supplier/customer; FK enforced via partial constraint
  -- Balance enforcement
  normal_side     balance_direction NOT NULL,
  -- Materialized balances, maintained by the write function
  debits_total    BIGINT NOT NULL DEFAULT 0 CHECK (debits_total  >= 0),
  credits_total   BIGINT NOT NULL DEFAULT 0 CHECK (credits_total >= 0),
  -- Lifecycle
  is_closed       BOOLEAN NOT NULL DEFAULT FALSE,
  closed_at       TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  -- Invariant: balance respects normal side
  CHECK (
    CASE normal_side
      WHEN 'debit'  THEN credits_total <= debits_total
      WHEN 'credit' THEN debits_total  <= credits_total
      ELSE TRUE
    END
  ),
  -- Currency must be set for value, null for qty
  CHECK (
    (ledger_kind = 'value' AND currency IS NOT NULL) OR
    (ledger_kind = 'qty'   AND currency IS NULL)
  )
);

-- Uniqueness: one account per (kind + grain). Partial unique indexes per kind shape.
CREATE UNIQUE INDEX accounts_stock_avail_uk
  ON accounts (sku_id, location_id)
  WHERE kind = 'stock_available' AND NOT is_closed;

CREATE UNIQUE INDEX accounts_wip_uk
  ON accounts (sku_id, routing_op)
  WHERE kind = 'stock_wip' AND NOT is_closed;

CREATE UNIQUE INDEX accounts_value_uk
  ON accounts (kind, sku_id, currency)
  WHERE ledger_kind = 'value' AND sku_id IS NOT NULL AND NOT is_closed;

-- ... one partial index per (kind, grain shape).

CREATE INDEX accounts_kind ON accounts(kind) WHERE NOT is_closed;
CREATE INDEX accounts_counterparty ON accounts(counterparty_id) WHERE counterparty_id IS NOT NULL;
```

**What changed vs v0.1:**
- `BIGSERIAL` IDs. Server-generated, monotonic, half the index width of `NUMERIC(39)`.
- `account_kind` enum replaces `code` + flag conventions. Self-documenting, indexable, foreign-keyable.
- `sku_id`, `location_id`, `routing_op`, `counterparty_id` are real FKs. Hash collision risk (§△-5) eliminated.
- `normal_side` enum + plain CHECK replaces flag-bit arithmetic. Same enforcement, readable.
- `BIGINT` balances. Inventory qty caps at ~9.2 × 10¹⁸ — ample. Value in minor currency units caps at ~$92 quadrillion — ample.
- `is_closed BOOLEAN` instead of bit flag. Plain.
- `flags.history` is dropped. Period snapshots are produced by a snapshot job at close; no per-account flag needed.

**Per-(SKU, location) value accounts (§△-1) become trivial.** Just create the row. The TB concern was account-creation overhead; PG account creation is one INSERT.

**Per-counterparty AR/AP (§△-3) becomes trivial.** Same.

### 2.2 Transfers

```sql
CREATE TYPE transfer_reason AS ENUM (
  -- Receipts
  'po_receipt', 'po_receipt_provisional', 'po_return_to_vendor', 'customer_return',
  -- Issues / Ship
  'so_ship', 'rm_issue_to_wo',
  -- Transfers
  'to_release', 'to_receipt', 'bin_move',
  -- WIP
  'wo_start', 'op_move', 'wo_complete', 'rework',
  -- Labor / OH
  'labor_apply', 'oh_apply',
  -- Holds / Scrap
  'quarantine', 'release_from_quarantine', 'scrap', 'damage',
  -- Financial
  'ar_invoice', 'ar_payment', 'ap_bill', 'ap_payment',
  -- Variances
  'ppv', 'muv', 'lv', 'ohv', 'scrap_v', 'wo_close_v', 'price_settlement',
  -- Corrections
  'cycle_count_adj', 'cost_restate', 'reversal',
  -- FX / liquidity
  'fx_leg', 'fx_spread',
  -- Commodity
  'po_settlement',
  'price_trueup_inventory', 'price_trueup_cogs', 'price_trueup_wip'
);

CREATE TABLE transfers (
  id                BIGSERIAL PRIMARY KEY,
  reason            transfer_reason NOT NULL,
  -- Domain references — first-class
  document_kind     TEXT NOT NULL,           -- 'work_order', 'sales_order', 'po', 'to', 'invoice', 'qc_hold', 'count_doc', ...
  document_id       UUID NOT NULL,           -- FK enforced application-side per document_kind
  document_line_id  UUID,                    -- when applicable
  -- Posting
  debit_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  credit_account_id BIGINT NOT NULL REFERENCES accounts(id),
  amount            BIGINT NOT NULL CHECK (amount > 0),
  -- WIP-specific (avoids the user_data_32 polymorphism)
  routing_op        INT,
  -- Counterparty attribution (avoids the user_data_64 hash collision)
  counterparty_id   UUID,
  -- Period attribution (solves §△-10)
  period_id         BIGINT NOT NULL REFERENCES periods(id),
  business_date     DATE NOT NULL,
  -- Idempotency
  idempotency_key   UUID NOT NULL UNIQUE,
  -- Audit
  posted_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  posted_by         UUID NOT NULL,           -- user/service id
  -- Same-ledger and currency invariants enforced via trigger or write function:
  --   accounts(debit).ledger_kind = accounts(credit).ledger_kind
  --   accounts(debit).currency    = accounts(credit).currency
  --   different accounts
  CHECK (debit_account_id <> credit_account_id)
);

CREATE INDEX transfers_document    ON transfers(document_kind, document_id, posted_at);
CREATE INDEX transfers_debit_ts    ON transfers(debit_account_id, posted_at DESC);
CREATE INDEX transfers_credit_ts   ON transfers(credit_account_id, posted_at DESC);
CREATE INDEX transfers_reason_ts   ON transfers(reason, posted_at);
CREATE INDEX transfers_counterparty
  ON transfers(counterparty_id) WHERE counterparty_id IS NOT NULL;
CREATE INDEX transfers_routing_op
  ON transfers(routing_op) WHERE routing_op IS NOT NULL;

-- Time-range queries: partition by month
-- (PARTITION BY RANGE (posted_at) is straightforward and unchanged from v0.1)
```

**What changed:**
- `document_id UUID` is a real reference per `document_kind`. WO cost trail (`§6.3` of design spec) becomes `SELECT * FROM transfers WHERE document_kind='work_order' AND document_id=$1 ORDER BY posted_at` — a single B-tree lookup, no polymorphic decoding.
- `routing_op INT` is a column with its own index, not a TB `user_data_32` reinterpretation.
- `counterparty_id UUID` is a real value, indexable, joinable. Hash collisions impossible.
- `period_id` is mandatory and FK-enforced. **§△-10 (period lock at API) is now solved at the schema level**: the write function consults `periods.closed_at` and rejects writes to closed periods unless an override role is present.
- `idempotency_key UUID` is the per-event idempotency mechanism. Decoupled from the row's PK (no need for the ID itself to be the idempotency key).
- The `posted_by` audit column is mandatory. (TB has no equivalent; v0.1 assumes it lives in the outbox payload, which is fine but parallel — making it a first-class column simplifies audit.)

**No `flags` column. No `user_data_*`. No `code` u16.** Each replaced by a typed column with proper indexing.

**No `pending_id`, no `timeout_seconds`, no flag bits for pending/post/void.** Pending transfers become reservations (§4 below) and live in a different table with their own lifecycle.

### 2.3 What goes away entirely

| Construct from v0.1 | Why removed |
|---------------------|-------------|
| `flags` column on accounts | Replaced by `is_closed` BOOLEAN + `normal_side` enum |
| `flags` column on transfers | Replaced by transactions, no need for `linked`. Pending modeled separately |
| `tb_account_map` table | Accounts table IS the canonical map; no second system to map to |
| `_apply_transfer_effect` flag matrix | Reduced to: validate, increment `debits_total`/`credits_total`, INSERT row |
| `expire_pending_transfers` worker | Reservations expire via a query + state column (see §4) |
| `flags.history` | Period snapshots produced by snapshot job, not a per-account flag |
| `flags.imported` | Backfill is plain `COPY`; no semantic flag needed |
| Linked chain SQLSTATE rollback | Standard Postgres transaction |
| Per-batch result code matrix | Postgres exceptions / explicit return values |
| Conformance test fixture (TB I/O parity) | No second implementation to conform to |
| AMQP CDC sidecar | Logical replication or trigger-fed view, optional |
| RabbitMQ broker | Not required for write path |
| Projector service (Phase 2) | Sync materialized views or trigger-maintained tables; see §6 |
| Kafka/Redpanda evaluation (§△-8) | Not on the critical path |
| Account creation rate limiting (§△-9) | Account creation is cheap |
| Cross-system batch policy (§△-K) | One system |
| Reverse migration playbook (§△-M) | Nothing to migrate from |

That's eight closed §△ items, two phases (3 and 4) entirely retired, and roughly 40% of the implementation surface area.

---

## 3. Write path

### 3.1 The write function

```sql
CREATE OR REPLACE FUNCTION post_transfers(p_events JSONB)
RETURNS JSONB AS $$
DECLARE
  v_event       JSONB;
  v_results     JSONB := '[]'::jsonb;
  v_idx         INT := 0;
  v_account_ids BIGINT[];
  v_period_id   BIGINT;
  v_amount      BIGINT;
  v_debit_id    BIGINT;
  v_credit_id   BIGINT;
  v_d_acct      accounts%ROWTYPE;
  v_c_acct      accounts%ROWTYPE;
BEGIN
  -- Lock all accounts in ascending order (deadlock prevention)
  SELECT array_agg(DISTINCT x ORDER BY x) INTO v_account_ids
    FROM (
      SELECT (e->>'debit_account_id')::BIGINT  AS x FROM jsonb_array_elements(p_events) e
      UNION
      SELECT (e->>'credit_account_id')::BIGINT FROM jsonb_array_elements(p_events) e
    ) t;
  PERFORM 1 FROM accounts WHERE id = ANY(v_account_ids) ORDER BY id FOR UPDATE;

  FOR v_event IN SELECT * FROM jsonb_array_elements(p_events) LOOP
    v_idx := v_idx + 1;

    -- Idempotency: short-circuit on existing key
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
      RAISE EXCEPTION 'account_closed' USING ERRCODE='P0001';
    END IF;
    IF v_d_acct.ledger_kind <> v_c_acct.ledger_kind THEN
      RAISE EXCEPTION 'ledger_mismatch' USING ERRCODE='P0002';
    END IF;
    IF v_d_acct.ledger_kind = 'value' AND v_d_acct.currency <> v_c_acct.currency THEN
      RAISE EXCEPTION 'currency_mismatch' USING ERRCODE='P0003';
    END IF;

    -- Period lock
    SELECT id INTO v_period_id FROM periods
      WHERE (v_event->>'business_date')::DATE BETWEEN opens_at AND closes_at
        AND closed_at IS NULL;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'period_closed_or_missing' USING ERRCODE='P0004';
    END IF;

    -- Apply
    UPDATE accounts SET debits_total  = debits_total  + v_amount WHERE id = v_debit_id;
    UPDATE accounts SET credits_total = credits_total + v_amount WHERE id = v_credit_id;

    -- The CHECK constraint on (debits_total, credits_total, normal_side) raises if violated
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

**Total length: ~70 lines.** Compare to the v0.1 design's `~200 LoC` claim that was actually closer to 1,500–2,500 (M1 in prior review). The reason it's small:
- No flag matrix (8+ TB transfer flags × interaction combinations).
- No pending lifecycle (pending → post → void → expire).
- No balancing-transfer read-min-write semantics.
- No closing-flag side effects.
- No imported-flag user-supplied-timestamp validation.
- No linked-chain rollback machinery — the surrounding transaction handles it.
- No client-side ID generation — `BIGSERIAL` does that.

**Linked-chain rollback** is "any RAISE inside the LOOP rolls back all UPDATEs and INSERTs in the surrounding transaction." That's literally how Postgres works.

**Idempotency** is per-event via `idempotency_key UNIQUE`. Skipping (CONTINUE) is correct because: (a) the prior application of this key is already durable, (b) the prior application's account effects are already in `debits_total`/`credits_total`. Re-submitting is a no-op.

### 3.2 Outbox

The outbox pattern is still valuable — it decouples API-tier latency from ledger write latency, and gives a clean retry surface. But the simplification is meaningful:

```sql
CREATE TABLE ledger_outbox (
  id              UUID PRIMARY KEY,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  batch_payload   JSONB NOT NULL,
  state           TEXT NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending', 'processing', 'committed', 'failed')),
  processor_id    UUID,
  submitted_at    TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  attempts        INT NOT NULL DEFAULT 0,
  error           TEXT
);

CREATE INDEX outbox_pending    ON ledger_outbox(created_at) WHERE state = 'pending';
CREATE INDEX outbox_processing ON ledger_outbox(submitted_at) WHERE state = 'processing';
```

Worker drains as before. No `route_hint` column (no second route). No deferred handling for the imported flag.

**Question worth raising explicitly:** does this workload need an outbox at all?

The outbox decouples Postgres-document-write from Postgres-ledger-write. In a single-database world, both are the same Postgres transaction. You can write the documents and call `post_transfers` in one COMMIT. The outbox becomes redundant *unless* you want:
- Asynchronous batching (drain N batches together for throughput).
- Decoupled retries (an external system fails; retry later).
- Backpressure (queue grows; alert before failures cascade).

For the inventory/ERP workload at <5K TPS, the outbox is optional. Use it if you have external integrations (commodity price feeds, payment gateways) where the write needs to fan out beyond the database. Skip it if the ledger write is the only durable side effect.

§△-revisit: "Is the outbox load-bearing in a Postgres-only world, or is it cargo-culted from the TB hybrid model?" Decide explicitly.

---

## 4. Reservations as first-class entities

The single biggest semantic improvement in dropping TB parity. The pending-transfer abstraction is **wrong for reservations** — it forces business state (which SO, which line, what price quote, sales rep) into either `user_data_*` fields or a parallel table that has to be kept in sync.

### 4.1 Schema

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
  unit_price      BIGINT,                  -- snapshot of quoted price at reservation
  notes           TEXT
);

CREATE INDEX rsv_sku_loc_active
  ON inventory_reservations(sku_id, location_id) WHERE status = 'active';
CREATE INDEX rsv_so
  ON inventory_reservations(so_id);
CREATE INDEX rsv_expires
  ON inventory_reservations(expires_at) WHERE status = 'active';
```

### 4.2 Operations

**Reserve:**

```sql
INSERT INTO inventory_reservations (id, sku_id, location_id, qty, so_id, so_line_id, expires_at, unit_price)
VALUES (...)
WHERE EXISTS (
  -- Atomic check: enough available
  SELECT 1 FROM v_inventory_available
  WHERE sku_id = $1 AND location_id = $2 AND qty_available >= $3
);
```

(Or as a function with `FOR UPDATE` on the relevant accounts row.)

**Allocate (pick confirm):**

```sql
UPDATE inventory_reservations
SET status = 'allocated', resolved_at = clock_timestamp()
WHERE id = $1 AND status = 'active';
```

**Cancel:**

```sql
UPDATE inventory_reservations
SET status = 'cancelled', resolved_at = clock_timestamp()
WHERE id = $1 AND status = 'active';
```

**Expire (single SQL statement — no worker function call needed):**

```sql
UPDATE inventory_reservations
SET status = 'expired', resolved_at = clock_timestamp()
WHERE status = 'active' AND expires_at < clock_timestamp();
```

Run as a `pg_cron` job every 30 seconds. Atomic. Idempotent. No `post_transfer_batch` re-entry (M7 fix in prior review).

### 4.3 Available stock query

```sql
CREATE VIEW v_inventory_available AS
SELECT
  a.sku_id,
  a.location_id,
  (a.debits_total - a.credits_total) AS qty_on_hand,
  COALESCE((
    SELECT SUM(qty) FROM inventory_reservations r
    WHERE r.sku_id = a.sku_id AND r.location_id = a.location_id AND r.status = 'active'
  ), 0) AS qty_reserved,
  (a.debits_total - a.credits_total) - COALESCE((
    SELECT SUM(qty) FROM inventory_reservations r
    WHERE r.sku_id = a.sku_id AND r.location_id = a.location_id AND r.status = 'active'
  ), 0) AS qty_promisable
FROM accounts a
WHERE a.kind = 'stock_available' AND NOT a.is_closed;
```

**Per-SO reservation visibility** (the §△-4 concern in v0.1) is now a single `SELECT * FROM inventory_reservations WHERE so_id = $1`. No projection lag. No reconstruction from event streams.

### 4.4 What this resolves

- §△-4 (self-pending vs Available/Reserved) — neither; reservations are their own table.
- B1 (broken self-pending pattern) — fixed by not using transfers for reservations.
- M7 (expiry worker re-enters write function) — fixed by single-statement expiry.
- The whole `pending_id`, `timeout_seconds`, `flags & 2`, `flags & 4`, `flags & 8` complex on the transfers table — gone.

The transfer log no longer has to model state. Transfers are immutable posted facts. Period.

### 4.5 What about non-reservation pending semantics?

TB's pending primitive is *also* used for things like provisional FX legs or two-phase commits with external systems. These are rare in an ERP workload. When they arise:

- Two-phase commit with payment gateway: model as a `payment_attempts` table with `status` and a paired transfer that's only inserted on confirmation.
- Provisional commodity pricing already has its own table (`commodity_receipts`); don't shoehorn it into pending transfers.

Pending-as-a-primitive is solving a TB problem (no row-level state). Postgres has row-level state. Use it.

---

## 5. Cost computation — read-then-write is fine

The §2 design-spec rule "NEVER perform a TB lookup that gates a subsequent TB write" exists because TB has no transactions and no row locks; a read-then-write pattern is genuinely racy there. Postgres has both.

In the revised write path, the WIP cost transfers can read the current accumulated WIP balance under the same `FOR UPDATE` lock the function already holds:

```sql
-- Inside post_transfers, when reason = 'op_move':
SELECT (debits_total - credits_total) AS accumulated_value,
       (SELECT debits_total - credits_total FROM accounts WHERE id = wip_qty_account_id)
       AS qty_in_wip
INTO v_accum_value, v_qty
FROM accounts WHERE id = wip_value_account_id;

-- Compute unit cost
v_unit_cost := v_accum_value / NULLIF(v_qty, 0);
v_amount := v_unit_cost * (v_event->>'qty')::BIGINT;
```

Because we already locked all referenced accounts in ID order at the top of `post_transfers`, the read is consistent and the subsequent UPDATE/INSERT cannot race.

**This resolves B2 from the prior review** (read-then-write contradicting §2). The contradiction only existed because the spec was carrying TB's prohibition forward.

**WAC (§6.2 of design spec, §△-7)** also becomes straightforward:

```sql
-- Compute weighted-average cost at issue time
SELECT (debits_total - credits_total) /
       NULLIF((SELECT debits_total - credits_total FROM accounts WHERE id = qty_acct), 0)
INTO v_wac
FROM accounts WHERE id = value_acct;
```

Same lock discipline. Same atomicity. The "WAC requires a read on the write path" caveat that v0.1 §6.2 flagged as a problem is simply not a problem in Postgres.

**Standard costing remains an option.** WAC is now also an option, with no hot-path performance penalty. The cost-method choice becomes a business decision rather than a system constraint.

---

## 6. Read model — sync materialization is a real choice now

In the v0.1 design, projection is async via CDC because TB has no joins or aggregations. In a Postgres-native design, projection is **a choice**, not a requirement, with a real spectrum:

### 6.1 Tier 1 — direct queries against base tables

For most reads, no projection is needed. The base tables, well-indexed, serve queries fast enough:

- "Available stock at (sku, location)": one row from `accounts` (single index lookup) minus one aggregate from `inventory_reservations` — sub-millisecond.
- "WO cost trail": `SELECT * FROM transfers WHERE document_kind='work_order' AND document_id=$1 ORDER BY posted_at` — single index range scan.
- "RM consumed at op N this period": `SELECT SUM(amount) FROM transfers WHERE reason='rm_issue_to_wo' AND routing_op=$1 AND period_id=$2` — single index lookup.
- "GL trial balance": `SELECT kind, SUM(debits_total - credits_total) FROM accounts GROUP BY kind` — already materialized in `accounts.debits_total`/`credits_total`.

These are not "hot reads to be projected" — they're operational queries that hit indexes.

### 6.2 Tier 2 — incremental materialized views

For aggregations that are too heavy for tier-1 queries:

- `inv_by_sku_location` (with reservation-adjusted qty): use `pg_ivm` extension or trigger-maintained.
- `gl_balances_by_period` (debits, credits, balance per period): trigger-maintained on transfer insert.
- `wo_summary` (qty in WIP, accumulated cost, age) per WO: trigger-maintained.

Trigger-maintained is straightforward:

```sql
CREATE TRIGGER trg_update_inv_by_sku_loc
  AFTER INSERT ON transfers
  FOR EACH ROW
  WHEN (NEW.reason IN ('po_receipt', 'so_ship', 'bin_move', ...))
  EXECUTE FUNCTION fn_update_inv_by_sku_loc();
```

The cost is write amplification. Measured cost in similar systems: 10–25% added write latency. Acceptable for an ERP workload.

### 6.3 Tier 3 — async projection (only when needed)

For:
- ClickHouse mirror for OLAP reporting (large GROUP BYs, TBs of history).
- Search over text fields (Elasticsearch / Postgres FTS).
- Cross-system fanout (data warehouse, ML feature store).

Use logical replication (`wal2json`, Debezium) for these. **Drop AMQP entirely.** RabbitMQ was on the diagram because TB's CDC sink is AMQP; in a Postgres-only world, logical replication is the standard.

### 6.4 What this resolves

- The entire Phase 2 of the migration spec (a "projection layer") becomes optional infrastructure, applied per-need.
- The projector service and its handlers (§5.4 of migration spec) are not built in MVP. They exist only when a tier-3 sink is justified.
- §△-F (logical replication vs trigger changefeed) is decided per-consumer, not as a universal architecture choice.
- §△-G (projector partitioning) becomes irrelevant for tier-1/tier-2.
- §△-L (projection merge across hybrid sources) is gone.
- Reconciliation simplifies dramatically — base tables are the source of truth; tier-2 mat views can be rebuilt from them.

### 6.5 What you give up

The async projector pattern decouples writes from read shape evolution. Sync materialization couples them. Concretely:

- **Adding a new aggregation** to a tier-2 mat view requires a backfill from base tables. Doable but not free.
- **Schema changes to base tables** propagate through tier-2 triggers; needs migration discipline.
- **Read scaling** in tier-1/tier-2 lives on the same database as writes. Use read replicas (streaming replication) for read-heavy reports.

These are real tradeoffs. They become real problems only at workloads where tier-3 is justified (multi-TB datasets, complex BI, search over millions of documents). Most ERP installations live in tier-1 + tier-2 forever.

---

## 7. Multi-currency simplification

V0.1 models each currency as a separate ledger (TB convention). That's fine, but it carries forward to Postgres unnecessarily.

Postgres-native version:

```sql
-- Account already has currency CHAR(3) NULL for qty, NOT NULL for value.
-- Transfer inherits same-currency invariant from accounts.
-- FX is its own concern:

CREATE TABLE fx_rates (
  id            BIGSERIAL PRIMARY KEY,
  from_currency CHAR(3) NOT NULL,
  to_currency   CHAR(3) NOT NULL,
  rate          NUMERIC(20, 10) NOT NULL,  -- precision matters for FX
  effective_at  TIMESTAMPTZ NOT NULL,
  source        TEXT NOT NULL              -- 'manual', 'feed_xyz', 'period_end_revaluation'
);

CREATE INDEX fx_rates_lookup ON fx_rates(from_currency, to_currency, effective_at DESC);

-- Cross-currency transaction: a single domain operation, two transfers in one transaction.
-- No "linked transfer" flag needed — same Postgres transaction.
-- The Currency Exchange recipe (§3.1 of design spec) is unchanged in shape, simpler in mechanics.
```

**FX revaluation at period close** becomes a SQL operation:

```sql
INSERT INTO transfers (reason, document_kind, document_id, debit_account_id, credit_account_id, amount, ...)
SELECT
  'fx_leg', 'period_close', $period_id,
  fx_revaluation_account, original_account,
  ROUND((debits_total - credits_total) * (current_rate - prior_rate)),
  ...
FROM accounts
WHERE ledger_kind = 'value' AND currency <> 'USD'  -- reporting currency
  AND ...
```

**Reporting in USD** at any point in time joins through `fx_rates`. No projector required.

---

## 8. Period close and back-dated postings (closes §△-10)

V0.1 §10: "TB does not enforce period locks. Enforce at the API layer."

Postgres-native: the **schema** enforces it.

```sql
CREATE TABLE periods (
  id           BIGSERIAL PRIMARY KEY,
  code         TEXT NOT NULL UNIQUE,
  opens_at     DATE NOT NULL,
  closes_at    DATE NOT NULL,
  closed_at    TIMESTAMPTZ,
  closed_by    UUID,
  CHECK (opens_at <= closes_at)
);
```

The `post_transfers` write function (shown in §3.1) looks up the period by `business_date` and rejects if `closed_at IS NOT NULL`. The check is one query, in the same lock context as the rest of the post.

**Override role** (e.g., for legitimate prior-period adjustments by Finance):

```sql
-- Add a parameter to post_transfers
CREATE OR REPLACE FUNCTION post_transfers(p_events JSONB, p_override_closed_period BOOLEAN DEFAULT FALSE) ...

-- Caller sets p_override_closed_period only if the requesting user has the ADJ_CLOSED_PERIOD role.
-- The role check is at the API tier; the function trusts the flag once passed.
```

**§△-10 resolved.** Period locking is not "API-tier discipline" — it's a database invariant.

**Period snapshots** at close:

```sql
CREATE TABLE period_snapshots (
  period_id    BIGINT REFERENCES periods(id),
  account_id   BIGINT REFERENCES accounts(id),
  debits_total BIGINT NOT NULL,
  credits_total BIGINT NOT NULL,
  PRIMARY KEY (period_id, account_id)
);

-- At period close:
INSERT INTO period_snapshots (period_id, account_id, debits_total, credits_total)
SELECT $period_id, id, debits_total, credits_total FROM accounts;

UPDATE periods SET closed_at = clock_timestamp(), closed_by = $user WHERE id = $period_id;
```

One transaction. No `flags.history` semantics. No projector to coordinate. Snapshot is exact (transactional consistency with the close).

---

## 9. Hot-account scaling — sharding as a first-class technique

V0.1 §△-E flags hot-account sharding as a Phase 1 technique that "is removed at migration to TB." In Postgres-native, sharding is a permanent answer. Refined:

### 9.1 Shard table

```sql
CREATE TABLE account_shards (
  parent_account_id BIGINT NOT NULL REFERENCES accounts(id),
  shard_account_id  BIGINT NOT NULL REFERENCES accounts(id),
  shard_index       INT NOT NULL,
  PRIMARY KEY (parent_account_id, shard_index),
  UNIQUE (shard_account_id)
);
```

Each parent (logical) account has N child (physical) accounts. The write function picks one shard at random (or by hash of the document ID); the read aggregates across shards.

### 9.2 Materialized view for the parent balance

```sql
CREATE MATERIALIZED VIEW v_account_parent_balances AS
SELECT
  s.parent_account_id AS id,
  SUM(a.debits_total)  AS debits_total,
  SUM(a.credits_total) AS credits_total
FROM account_shards s JOIN accounts a ON a.id = s.shard_account_id
GROUP BY s.parent_account_id;
```

Or maintain via trigger on `accounts` to keep the parent in sync (write amplification but constant cost per shard count).

### 9.3 When to shard

A hot account should shard when `pg_stat_activity` shows lock waits >0.5% on its row, sustained. Shard count: 4 to 16 typically; doubling beyond is rarely justified.

### 9.4 Shard eviction (de-sharding)

When load drops, sum shards into the parent and drop shards. A SQL operation, runnable online.

This is the same sharding §△-E describes — but here it doesn't sit under a "remove on TB migration" cloud. It's the answer.

---

## 10. Operations

### 10.1 What you don't run

- TB cluster (6 replicas, 2–3 AZs, weekly upgrade window).
- AMQP CDC sidecar.
- RabbitMQ (unless you use it for something else).
- The projector service (until/unless tier-3 is needed).
- Reconciliation jobs comparing TB to Postgres.
- Reconciliation jobs comparing outbox to TB transfer ids.

### 10.2 What you do run

- Postgres primary + 2–3 streaming replicas (read scaling, HA, PITR).
- pgBouncer or PgCat (transaction pooling).
- Standard backup/PITR (WAL archiving).
- pg_cron for scheduled jobs (reservation expiry, period-close snapshots, daily reconciliation).
- Standard Postgres monitoring (pg_stat_*, autovacuum, replication lag).

### 10.3 Reconciliation, slimmed

Daily jobs:
1. **Per-ledger double-entry:** `SELECT ledger_kind, currency, SUM(debits_total - credits_total) FROM accounts GROUP BY ...` — must be zero per (ledger_kind, currency). (Note: this is the per-ledger fix from B3 of the prior review.)
2. **Subledger-to-control:** sum per-counterparty AR/AP from base tables; compare to control account balance. Same database, same transaction snapshot — guaranteed consistent.
3. **Tier-2 mat view freshness:** if using trigger-maintained aggregates, periodically rebuild from base tables and diff. Detects trigger bugs.
4. **Reservations vs accounts:** sum active reservations per (sku, location); the `v_inventory_available` view's `qty_promisable` matches.

### 10.4 Throughput envelope (rough)

Single Postgres node, well-tuned (NVMe, 64+ GB RAM, modern CPU, `synchronous_commit=on`):

| Workload pattern | Sustained TPS |
|------------------|---------------|
| Pure inserts to `transfers`, batch=100 | 30–60K |
| `post_transfers` with full validation, batch=10 | 5–15K |
| `post_transfers` with hot-account contention, no sharding | 500–2K |
| With 8-way sharding on hot accounts | 4–10K |
| With read-heavy mix served by replicas | unchanged write side |

These are envelope estimates, not benchmarks. Actual numbers depend on row width, index count, and disk subsystem. The point: most ERP workloads (often 10s–100s of transfers/sec sustained, 1–2K peak) live well inside this envelope without sharding.

The Phase 3 entry trigger in v0.1 (`>10K TPS sustained`) is at the edge of single-node Postgres. Most installations never approach it.

---

## 11. What you give up by dropping TB-parity

Honest accounting. Each of these is a real tradeoff, not a non-issue:

### 11.1 Throughput ceiling

TB can sustain 1M+ transfers/sec on a 6-node cluster. Postgres caps out at ~50K TPS sustained on a single node (with batching) or hundreds of thousands across a multi-node setup with citus or similar. If your roadmap genuinely targets >100K TPS, Postgres will need additional engineering (sharding by ledger, multi-master, etc.) and TB starts looking like the right answer.

**Mitigation:** The CDC seam lets you fan out to a downstream system (TB included) if the workload changes. The seam is the logical replication slot, not the outbox table — but it's a seam.

### 11.2 Cluster-level immutability guarantees

TB provides cluster-level guarantees about transfer immutability that are stronger than Postgres's row-level immutability. In Postgres, an attacker with database write access can `UPDATE transfers SET amount = ...`. Mitigated by:
- Strict role-based access (no human has UPDATE on `transfers`).
- Trigger-enforced "no UPDATE/DELETE on transfers" (raises exception).
- Append-only audit log via logical replication to a write-once store.

Equivalent to TB in practice if disciplined; not equivalent by construction.

### 11.3 Native batching at the storage layer

TB's storage engine is built for batch ingestion. Postgres benefits from batching (multi-row INSERT, COPY) but at a smaller multiplier. For batch-heavy ingestion (e.g., bulk migration of 5 years of GL history), TB has a clear edge. Postgres handles it with COPY + careful tuning, but it's slower.

### 11.4 The optionality itself

Some teams want the *option* to migrate to TB later, even if they never exercise it. Dropping parity dropd this option. Not free. The argument here is: the parity tax is being paid every day to preserve an option that >90% of installations will never exercise, and the option can be partially restored later via CDC if the workload genuinely changes.

If retaining TB optionality is itself the requirement (regulatory, organizational), this entire document doesn't apply. State it explicitly.

### 11.5 Schema drift risk

Postgres's flexibility means the schema can drift via accreted columns, tables, partial indexes, triggers. TB's rigid schema is its own form of discipline. Mitigate with code review discipline and migration governance — same as any Postgres app.

### 11.6 What you don't give up

- Double-entry correctness — same as TB.
- Atomicity within a batch — same (Postgres transactions).
- Idempotent retries — same (idempotency key).
- Audit trail — same (immutable transfers table).
- Multi-currency support — unchanged.
- Reservation semantics — *better* (first-class table, not pending-transfer kludge).
- WIP modeling — same shape, simpler implementation.
- Period close — *better* (schema-enforced).
- Reconciliation — fewer moving parts.

The list of "what you give up" is shorter than the list of "what you keep." That's the case being made.

---

## 12. Migration from current v0.1 to PG-native

If the team has not started implementation, the migration is editorial — rewrite the specs, drop the §△ items that no longer apply.

If implementation has started against v0.1:

1. **Preserve outbox and idempotency-key concepts.** They survive.
2. **Replace `ledger_accounts` and `ledger_transfers` schemas** with the §2 tables in this document. Type changes (`NUMERIC(39)` → `BIGINT`, `flags` → typed columns) are mechanical.
3. **Drop the `_apply_transfer_effect` flag matrix.** Replace with the ~70-line `post_transfers` function.
4. **Build `inventory_reservations` table.** Migrate any in-flight pending transfers to reservation rows (one-time script).
5. **Drop the planned CDC + projector for Phase 2.** Replace with: tier-1 base-table queries, tier-2 trigger-maintained mat views (only where measured benefit justifies), tier-3 logical replication (only when an external sink demands it).
6. **Strip TB phases (3, 4, 5).** Or keep them as a footnote: "if workload exceeds X TPS sustained, evaluate cluster sharding or external ledger system."
7. **Resolve §△ items** as in §13 below.

Estimated work delta: 6–10 weeks of Phase 0 + 1 (TB-parity Phase 0) becomes ~3 weeks of plain Postgres schema + write function. Phase 2 (projection layer, 1 month) becomes 1–2 weeks of mat views as needed. Phase 3+ work is not done.

---

## 13. §△ items resolved or retired

| Item | v0.1 origin | Status in PG-native |
|------|-------------|---------------------|
| §△-1 Per-(SKU, location) value accounts | Account count cost | **Just do it.** Account creation is cheap. |
| §△-2 Per-warehouse qty ledger | Cluster sharding | **N/A.** Single ledger. |
| §△-3 Counterparty pools — global vs per-entity | Account count cost | **Per-entity.** Trivial in PG. |
| §△-4 Self-pending vs Available/Reserved | Broken pattern | **Resolved.** Reservations are their own table. |
| §△-5 user_data_64 hash collisions | TB schema | **Eliminated.** Real FK on `counterparty_id`. |
| §△-6 Backfill historical GL | TB import semantics | **Plain COPY.** No flag semantics needed. |
| §△-7 Standard → WAC transition | Read-on-write taboo | **Resolved.** Read-on-write is fine in PG with locks. |
| §△-8 Kafka/alternative CDC | TB CDC sink | **N/A.** Logical replication if/when needed. |
| §△-9 Account creation rate limit | TB cost | **N/A.** PG account creation is cheap. |
| §△-10 Period lock at API | TB has no periods | **Resolved.** Schema enforces. |
| §△-11 Serial/lot reconciliation | TB excludes lots/serials | Unchanged — modeled in PG with FK to inventory accounts. |
| §△-12 Per-WO per-op accounts | TB account cost | **Just do it for job costing** when business demands. |
| §△-13 Backflush implementation | Phase decision | Unchanged. |
| §△-14 DR / zero-RPO | TB enterprise | **N/A.** Standard PG PITR + replicas. |
| §△-15 Commodity attribution policy (FIFO/proportional/all-to-variance) | Business decision | **Pick FIFO** (recommended). Now backed by a real table, not projector reconstruction. |
| §△-16 WAC recompute at settlement | Audit | Unchanged — still a policy call. |
| §△-17 Prior-period commodity settlement | GAAP | Unchanged — still a policy call. |
| §△-A Sub-second timeout precision | Reservation expiry | **N/A.** Reservations are SQL, cron drives expiry; sub-second is `NOTIFY/LISTEN` if needed. |
| §△-B get_balances_batch | PG ergonomics | **Resolved.** A view + WHERE id IN. |
| §△-C Outbox batch size tuning | Tunable, unchanged | Unchanged. |
| §△-D COPY-based bulk insert | Tunable, unchanged | Unchanged for backfill. |
| §△-E Hot-account sharding | Pre-migration bandage | **First-class technique.** Not a bandage. |
| §△-F CDC mechanism | Logical vs trigger | Per-consumer choice. |
| §△-G Projector partitioning | If projector exists | Mostly N/A. |
| §△-H Phase 3 entry criteria | Migration trigger | **N/A.** No Phase 3. |
| §△-I Reconciliation tolerance | Hybrid recon | **N/A.** Single-system. |
| §△-J Cutover order | Migration plan | **N/A.** |
| §△-K Cross-system batch policy | Hybrid txns | **N/A.** Single-system. |
| §△-L Projection merge | Hybrid sources | **N/A.** Single source. |
| §△-M Reverse migration playbook | Roll back from TB | **N/A.** |

**Score:** Of 30 §△ items, 15 are resolved or retired (50%), 11 become trivial implementation choices, 4 remain genuine business/policy decisions independent of the system.

---

## 14. When to reconsider — re-introducing TB or alternatives

A future signal that legitimately justifies revisiting:

1. **Sustained >50K TPS** on the write path. Single-node Postgres is past its envelope. Options: shard by ledger across multiple PG instances, introduce TB or QLDB or similar, or migrate to a distributed ledger.
2. **Hot-account contention not resolvable with sharding.** Sharded to 16 ways, still seeing lock waits. Indicates a workload mismatch, not a system limit. Re-architect the domain (do you really need a single-account view of this cardinality?).
3. **Regulatory append-only-by-construction requirement.** Some compliance regimes want strong immutability guarantees that Postgres-with-discipline doesn't quite deliver. TB or a write-once-read-many storage tier is appropriate.
4. **Cross-region active-active.** Postgres logical replication is multi-master-capable but operationally finicky. TB's replication model is tighter. CRDB or Spanner are also candidates.

When that signal arrives:

- **Don't migrate the write authority.** Keep Postgres as the source of truth. Stream to TB (or to BigQuery, or to S3+Iceberg) via logical replication. Use the secondary system for the workload it's good at (high-throughput append, OLAP, archive).
- **Re-evaluate before assuming TB.** "10K TPS" was a meaningful number ten years ago. A modern Postgres node with a sane schema does that comfortably.

The CDC seam (§6.3) is the thing to preserve. Logical replication slots, named in the schema, are the migration option. Not the outbox table.

---

## 15. Summary

Dropping TB parity removes:
- ~1,500–2,500 LoC of `_apply_transfer_effect` flag-matrix machinery.
- The pending/post/void primitive (replaced by `inventory_reservations`).
- The `flags` bit field on accounts and transfers (replaced by typed columns).
- The `user_data_*` polymorphism (replaced by real FKs).
- Hash-based counterparty attribution (replaced by `counterparty_id UUID`).
- The `tb_account_map` table.
- Account lazy-materialization machinery.
- The conformance test fixture.
- AMQP CDC sidecar + RabbitMQ.
- The Phase 2 projector service (or radically simplifies it to optional tier-3).
- Phases 3, 4, 5 of the migration spec entirely.
- 15 of 30 §△ items.

Adds:
- One enum (`account_kind`).
- One typed `inventory_reservations` table.
- An honest tradeoff section about throughput ceiling and immutability guarantees.

Keeps:
- Double-entry correctness.
- Atomic batches.
- Idempotent retries.
- Append-only audit.
- All the actual ERP semantics (WO, SO, PO, TO, WIP, multi-currency, commodity pricing).
- The optionality to introduce a downstream specialized system (TB, ClickHouse, search) via logical replication when justified by signal.

The case is not "TB is bad." TB is excellent at what it does. The case is that **the v0.1 specs pay TB's tax in a Postgres world for an option that most installations will never exercise**, and the tax compounds in the Postgres implementation in ways that make it worse than either a TB-native build or a Postgres-native build.

Pick one. If the workload is in TB territory, build on TB. If it's in Postgres territory (most ERP installations), build on Postgres without the parity overhead. The v0.1 specs straddle, and pay for both — that's the most expensive option.

---

## 16. Open questions for the team

These are the actual decisions that need to be made before this redesign can proceed:

1. **Is TB optionality a hard requirement** (regulatory, organizational, strategic), or a "nice to have" that's been rationalized into the architecture? If the former, this redesign is moot. If the latter, the cost-benefit analysis above applies.
2. **What is the realistic 24-month TPS projection** for this workload? Numbers above 30K sustained reframe the conversation. Numbers below 5K make TB optionality nearly free to drop.
3. **Is the outbox load-bearing** in a single-database world? (See §3.2.) Decide explicitly.
4. **Cost method:** standard, WAC, or hybrid? PG-native makes WAC cheap; the choice is now business-driven, not system-constrained.
5. **Reservation lifetime expectations:** are sub-second timeouts ever needed? Drives whether `pg_cron`-based expiry is sufficient or whether `LISTEN/NOTIFY` is needed.
6. **Append-only enforcement model:** trigger-blocked UPDATE/DELETE on transfers, or rely on RBAC + audit? Implication for cluster-level immutability claims.
7. **CDC sinks at MVP:** none, search index, OLAP store, all of the above? Drives the tier-3 design timing.

Answers to (1) and (2) determine whether this document is the right framing or a footnote. Answers to (3)–(7) determine the v0.2 spec's shape.
