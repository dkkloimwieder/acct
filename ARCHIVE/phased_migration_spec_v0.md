# Phased Ledger Implementation — Technical Specification
## Postgres-First with Option to Extend to TigerBeetle

Version: 0.1 (pre-implementation)
Status: Draft — sections marked §△ are expected to require revision during implementation.

Companion document to: `ledger_inventory_design_spec.md` (the target architecture). This document specifies the migration path and the Postgres-side implementation that preserves TB optionality.

---

## 0. Guiding principles

The entire roadmap rests on four rules. Every design choice that follows is downstream of these.

1. **The Postgres ledger is architecturally a drop-in for TigerBeetle.** Same data shapes, same semantic primitives, same result codes. The only difference is where the bytes live.
2. **Transfers are immutable. Balances are derived.** NEVER `UPDATE accounts SET balance = ...` from application code. Balance state only mutates as a consequence of INSERT into `transfers`, via the canonical write function.
3. **Writes flow through one function.** `post_transfer_batch()` is the only write path for ledger state. Swapping its implementation is the migration.
4. **Reads flow through an abstraction.** Applications call `get_balance()`, not direct table selects. Reports read the projection, not the ledger tables.

If these rules feel pedantic in Phase 0, good — they are the cheap insurance that makes Phase 4 possible. Violating them is cheap upfront and ruinously expensive later.

---

## 1. Scope and non-goals

**In scope:**
- Postgres-native ledger matching TB's semantics (accounts, transfers, pending, linked, flags, result codes).
- Full application functionality per the target spec: SKU × location × status, WIP, WO/SO/TO/PO, multi-currency, commodity provisional pricing, reservations, reconciliation.
- Explicit migration path: shadow mode → per-subledger cutover → steady-state hybrid.

**Out of scope for this document:**
- The target architecture details (covered in the companion spec).
- Detailed Postgres operational tuning (autovacuum, WAL archiving, PITR). Standard practices assumed.

**Non-goals:**
- Full TB semantic parity in Phase 0 — we match what we'll use, not the entire TB surface area.
- Migrating every account to TB. Only accounts with demonstrated contention migrate.
- Supporting rollback from TB to Postgres as a primary path. Possible but not a design goal.

---

## 2. Phase overview

| Phase | Duration     | Deliverable                                              | Trigger to proceed                                  |
|-------|--------------|----------------------------------------------------------|-----------------------------------------------------|
| 0     | 2–3 weeks    | Ledger schema, write function, read abstractions         | Schema + function pass invariant tests               |
| 1     | 3–4 months   | Full application on Postgres ledger                       | Production-ready, passing reconciliation            |
| 2     | overlapping  | Projection layer, reporting read path                     | All reports served from projection                  |
| 3     | 2–3 months   | TB shadow mode (if triggered)                             | 30 days clean reconciliation under production load  |
| 4     | 2–4 months   | Per-subledger cutover to TB                               | Each subledger: 14 days clean reconciliation        |
| 5     | ongoing      | Steady-state hybrid operations                            | N/A — ongoing operational posture                   |

The Phase 3 trigger conditions are explicit (§9). If they're not met, Phases 3–5 never happen and that's the correct outcome.

---

## 3. Phase 0 — Ledger schema and write function

### 3.1 Schema

All column types match TB semantics. Where Postgres types differ from TB native types, the choice is documented.

```sql
-- Accounts
CREATE TABLE ledger_accounts (
  id              NUMERIC(39) PRIMARY KEY,   -- u128 analog; NUMERIC(39) covers full range
  ledger          INTEGER NOT NULL,
  code            INTEGER NOT NULL CHECK (code >= 0 AND code <= 65535),
  user_data_128   UUID,
  user_data_64    BIGINT,
  user_data_32    INTEGER,
  flags           INTEGER NOT NULL DEFAULT 0,
  debits_posted   NUMERIC(39) NOT NULL DEFAULT 0 CHECK (debits_posted >= 0),
  credits_posted  NUMERIC(39) NOT NULL DEFAULT 0 CHECK (credits_posted >= 0),
  debits_pending  NUMERIC(39) NOT NULL DEFAULT 0 CHECK (debits_pending >= 0),
  credits_pending NUMERIC(39) NOT NULL DEFAULT 0 CHECK (credits_pending >= 0),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  closed_at       TIMESTAMPTZ
);

-- Flag semantics, matching TB:
--   1   debits_must_not_exceed_credits   (liability/equity/revenue normal)
--   2   credits_must_not_exceed_debits   (asset/expense normal, non-negative stock)
--   4   history                          (reserved; no-op in PG phase, retained for parity)
--   8   closed                           (rejects future non-voiding transfers)
--   16  imported                         (set via admin-only path)

ALTER TABLE ledger_accounts ADD CONSTRAINT chk_flag_exclusive
  CHECK ((flags & 1 = 0) OR (flags & 2 = 0));  -- the two balance-direction flags are exclusive

ALTER TABLE ledger_accounts ADD CONSTRAINT chk_balance_invariant
  CHECK (
    CASE
      WHEN flags & 1 <> 0 THEN debits_posted <= credits_posted
      WHEN flags & 2 <> 0 THEN credits_posted <= debits_posted
      ELSE TRUE
    END
  );

CREATE INDEX idx_accounts_ledger_code  ON ledger_accounts(ledger, code);
CREATE INDEX idx_accounts_user_data_128 ON ledger_accounts(user_data_128) WHERE user_data_128 IS NOT NULL;

-- Transfers
CREATE TABLE ledger_transfers (
  id                 NUMERIC(39) PRIMARY KEY,
  debit_account_id   NUMERIC(39) NOT NULL,
  credit_account_id  NUMERIC(39) NOT NULL,
  amount             NUMERIC(39) NOT NULL CHECK (amount >= 0),
  pending_id         NUMERIC(39),
  user_data_128      UUID,
  user_data_64       BIGINT,
  user_data_32       INTEGER,
  timeout_seconds    INTEGER,
  ledger             INTEGER NOT NULL,
  code               INTEGER NOT NULL CHECK (code >= 0 AND code <= 65535),
  flags              INTEGER NOT NULL DEFAULT 0,
  timestamp          TIMESTAMPTZ NOT NULL,
  CHECK (debit_account_id <> credit_account_id),
  FOREIGN KEY (debit_account_id)  REFERENCES ledger_accounts(id),
  FOREIGN KEY (credit_account_id) REFERENCES ledger_accounts(id),
  FOREIGN KEY (pending_id)        REFERENCES ledger_transfers(id)
);

-- Transfer flag semantics, matching TB:
--   1   linked
--   2   pending
--   4   post_pending_transfer
--   8   void_pending_transfer
--   16  balancing_debit
--   32  balancing_credit
--   64  closing_debit
--   128 closing_credit
--   256 imported

CREATE INDEX idx_transfers_debit_ts   ON ledger_transfers(debit_account_id, timestamp DESC);
CREATE INDEX idx_transfers_credit_ts  ON ledger_transfers(credit_account_id, timestamp DESC);
CREATE INDEX idx_transfers_user_data_128 ON ledger_transfers(user_data_128) WHERE user_data_128 IS NOT NULL;
CREATE INDEX idx_transfers_code_ts    ON ledger_transfers(code, timestamp);
CREATE INDEX idx_transfers_pending    ON ledger_transfers(pending_id) WHERE pending_id IS NOT NULL;

-- Pending transfer expiry worker indexed tail
CREATE INDEX idx_transfers_pending_expiry
  ON ledger_transfers((timestamp + (timeout_seconds || ' seconds')::INTERVAL))
  WHERE flags & 2 <> 0 AND timeout_seconds IS NOT NULL;
```

**ID strategy:**
- Account IDs and transfer IDs are time-ordered 128-bit values (ULID or Snowflake-like) generated by the **client** (API tier). NEVER generated inside the database.
- NUMERIC(39) holds u128 without loss. This is the type that moves directly to TB later.
- Client-generated IDs are also the idempotency keys. INSERT uses `ON CONFLICT (id) DO NOTHING` + `RETURNING` to distinguish `ok` (inserted) from `exists` (already present).

**Reasoning:** This mirrors TB's ID contract exactly. When you migrate, the IDs move with the data; no re-keying. If you generate server-side now, you lose idempotency on retries and gain a migration cost later.

### 3.2 Canonical write function

The write path. The only function that mutates ledger state.

```sql
CREATE OR REPLACE FUNCTION post_transfer_batch(p_events JSONB)
RETURNS JSONB AS $$
DECLARE
  v_result        JSONB := '[]'::jsonb;
  v_event         JSONB;
  v_event_idx     INTEGER := 0;
  v_debit_acct    ledger_accounts%ROWTYPE;
  v_credit_acct   ledger_accounts%ROWTYPE;
  v_pending       ledger_transfers%ROWTYPE;
  v_status        TEXT;
  v_amount        NUMERIC(39);
  v_any_linked    BOOLEAN := FALSE;
  v_chain_failed  BOOLEAN := FALSE;
  v_account_ids   NUMERIC(39)[];
BEGIN
  -- Step 1: gather all account IDs in the batch, lock in ascending ID order
  SELECT array_agg(DISTINCT x ORDER BY x)
    INTO v_account_ids
    FROM (
      SELECT (e->>'debit_account_id')::NUMERIC(39) AS x FROM jsonb_array_elements(p_events) e
      UNION
      SELECT (e->>'credit_account_id')::NUMERIC(39) FROM jsonb_array_elements(p_events) e
    ) t;

  PERFORM 1 FROM ledger_accounts WHERE id = ANY(v_account_ids) FOR UPDATE;
  -- ordered locking prevents deadlocks between concurrent batches

  -- Step 2: iterate events, apply each within the linked-chain semantics
  FOR v_event IN SELECT * FROM jsonb_array_elements(p_events)
  LOOP
    v_event_idx := v_event_idx + 1;
    v_status := 'ok';

    -- Idempotency: already-inserted id returns 'exists'
    IF EXISTS (SELECT 1 FROM ledger_transfers WHERE id = (v_event->>'id')::NUMERIC(39)) THEN
      v_result := v_result || jsonb_build_object('index', v_event_idx, 'result', 'exists');
      CONTINUE;
    END IF;

    -- Load accounts (already locked)
    SELECT * INTO v_debit_acct  FROM ledger_accounts WHERE id = (v_event->>'debit_account_id')::NUMERIC(39);
    SELECT * INTO v_credit_acct FROM ledger_accounts WHERE id = (v_event->>'credit_account_id')::NUMERIC(39);

    IF NOT FOUND OR v_debit_acct.id IS NULL THEN
      v_status := 'debit_account_not_found';
    ELSIF v_credit_acct.id IS NULL THEN
      v_status := 'credit_account_not_found';
    ELSIF v_debit_acct.ledger <> v_credit_acct.ledger THEN
      v_status := 'accounts_must_have_the_same_ledger';
    ELSIF (v_debit_acct.flags & 8) <> 0 OR (v_credit_acct.flags & 8) <> 0 THEN
      -- closed account cannot receive non-voiding transfers
      v_status := 'account_closed';
    ELSE
      v_amount := (v_event->>'amount')::NUMERIC(39);
      -- apply pending, post_pending, void_pending, balancing_*, closing_* per flag combinations
      -- (full implementation: ~200 LoC, branching on transfer flags)
      -- handles: posted vs pending totals, invariant checks, closing flag side-effects
      -- returns 'ok' / 'exceeds_credits' / 'exceeds_debits' / 'pending_transfer_not_found' / ...
      PERFORM _apply_transfer_effect(v_event, v_debit_acct, v_credit_acct);
      -- _apply_transfer_effect raises the status as a SQLSTATE; caught here
    END IF;

    -- Linked chain semantics: if flag.linked and this event failed, mark chain failure
    IF (v_event->>'flags')::INTEGER & 1 <> 0 THEN
      v_any_linked := TRUE;
      IF v_status <> 'ok' THEN
        v_chain_failed := TRUE;
      END IF;
    END IF;

    v_result := v_result || jsonb_build_object('index', v_event_idx, 'result', v_status);
  END LOOP;

  -- Step 3: if any linked event failed, roll back the whole batch
  IF v_chain_failed THEN
    RAISE EXCEPTION 'linked_event_failed'
      USING DETAIL = v_result::text;
    -- surrounding BEGIN/COMMIT rolls back all INSERTs and UPDATEs
  END IF;

  RETURN v_result;
END;
$$ LANGUAGE plpgsql;
```

**Reasoning for the shape:**

- JSONB input, not a set of arguments. One round trip, arbitrary batch size, identical in shape to TB's `create_transfers` array.
- Account locks acquired in ID order at the top of the function. Standard deadlock avoidance. No application-level coordination needed.
- Idempotency per event via `ON CONFLICT` semantics. Matches TB's `ok`/`exists` distinction. Retries from the outbox are safe.
- Linked-chain rollback via exception + surrounding transaction. Matches TB's linked semantics. Caller wraps the function call in BEGIN/COMMIT (or it is itself called inside a transaction from the outbox worker).
- Per-event result codes returned as JSONB. Direct equivalent of TB's response format.

**The `_apply_transfer_effect` helper** handles every transfer-flag combination. The combinations are:

| Flags (hex)       | Semantics                                                              |
|-------------------|------------------------------------------------------------------------|
| 0x00              | Posted transfer: debits_posted/credits_posted updated                  |
| 0x02 (pending)    | Pending: debits_pending/credits_pending updated, timeout applies       |
| 0x04 (post_pending) | Resolves pending: pending→posted                                     |
| 0x08 (void_pending) | Resolves pending: pending released, no posted                        |
| 0x10 / 0x20 (balancing) | Amount is min(requested, available-under-limit)                  |
| 0x40 / 0x80 (closing) | Sets `closed_at` and flag on the appropriate account               |

Implementation detail on balancing: read the locked account's current available (posted - pending delta), compute `min(requested, available)`, use as actual amount. Record the actual amount in `transfers.amount`, same as TB.

### 3.3 Expiry worker

Pending transfers with `timeout_seconds` need to be voided when they expire. TB handles this in the primary's event loop; we handle it with a scheduled worker.

```sql
CREATE OR REPLACE FUNCTION expire_pending_transfers(p_batch_size INTEGER DEFAULT 1000)
RETURNS INTEGER AS $$
DECLARE
  v_expired ledger_transfers%ROWTYPE;
  v_count   INTEGER := 0;
BEGIN
  FOR v_expired IN
    SELECT * FROM ledger_transfers
    WHERE flags & 2 <> 0                            -- pending
      AND timeout_seconds IS NOT NULL
      AND timestamp + (timeout_seconds || ' seconds')::INTERVAL < clock_timestamp()
      AND NOT EXISTS (
        SELECT 1 FROM ledger_transfers r WHERE r.pending_id = ledger_transfers.id
      )                                              -- not already resolved
    ORDER BY timestamp
    LIMIT p_batch_size
    FOR UPDATE SKIP LOCKED
  LOOP
    -- Construct a void_pending_transfer with deterministic id derived from pending id
    PERFORM post_transfer_batch(jsonb_build_array(jsonb_build_object(
      'id', deterministic_void_id(v_expired.id),
      'debit_account_id', v_expired.debit_account_id,
      'credit_account_id', v_expired.credit_account_id,
      'amount', 0,
      'pending_id', v_expired.id,
      'ledger', v_expired.ledger,
      'code', v_expired.code,
      'flags', 8                                     -- void_pending_transfer
    )));
    v_count := v_count + 1;
  END LOOP;
  RETURN v_count;
END;
$$ LANGUAGE plpgsql;
```

Scheduled via pg_cron or an external scheduler every 1–5 seconds. `FOR UPDATE SKIP LOCKED` lets multiple worker instances run safely.

**Reasoning:** Deterministic void IDs ensure idempotency if the worker runs twice. The "NOT EXISTS" clause prevents double-voiding if the application beats the worker to resolution. Running frequently is fine because `SKIP LOCKED` makes contention on the scheduler row cheap.

§△-A **Sub-second timeout precision:** If reservation timeouts need sub-second precision, move to a `pg_notify`-driven in-process worker per API node. The SQL-scheduled version is ±5s accurate. For cart reservations where ±5s is fine, keep simple.

### 3.4 Read abstraction

Every read of balance data flows through one function:

```sql
CREATE OR REPLACE FUNCTION get_balance(p_account_id NUMERIC(39))
RETURNS JSONB AS $$
  SELECT jsonb_build_object(
    'id', id,
    'debits_posted', debits_posted,
    'credits_posted', credits_posted,
    'debits_pending', debits_pending,
    'credits_pending', credits_pending,
    'net_posted', credits_posted - debits_posted,
    'available', 
      CASE WHEN flags & 2 <> 0 
        THEN credits_posted - debits_posted - debits_pending
        ELSE debits_posted - credits_posted - credits_pending
      END
  )
  FROM ledger_accounts WHERE id = p_account_id;
$$ LANGUAGE sql STABLE;
```

**Reasoning:** The application reads through a function, not a direct SELECT. When migration happens, this function's internals change (route to TB for migrated accounts); the callers don't. The `available` field computes the TB-native promisable balance, saving every caller from re-deriving it.

§△-B **get_balances_batch:** Implement for efficiency when the API tier reads many accounts at once (e.g., showing a stock table). Same pattern, takes an array.

### 3.5 Invariant tests

Phase 0 is not done until these tests pass. They are the gate to Phase 1.

- **Double-entry:** `SUM(debits_posted) = SUM(credits_posted)` across all accounts, always. Assert after every batch in test.
- **No negative balances:** balance-direction-flagged accounts never violate the check constraint. Exercise with 10K concurrent attempted overdrafts.
- **Linked rollback:** a 5-event linked batch where event 3 fails leaves zero inserted rows. Exercise with property tests.
- **Idempotency:** submitting the same batch twice produces identical state and returns `exists` on the second call.
- **Pending lifecycle:** pending → post_pending shows correct movement between `_pending` and `_posted` balances. pending → void_pending releases without posting. pending → expired (via worker) equivalent to void.
- **Deadlock freedom:** run 100 concurrent batches, each touching random account subsets, for 30 minutes. Zero deadlocks.
- **Same-ledger enforcement:** cross-ledger transfer rejected at batch time, not via foreign key.

---

## 4. Phase 1 — Application on Postgres ledger

### 4.1 Architecture

```
API tier ─► Postgres transaction:
              1. Document rows (WO, SO, PO, ...)
              2. Outbox row with batch payload
              3. Account map entries for any new accounts
              COMMIT

Outbox worker ─► reads pending rows, calls post_transfer_batch(), marks committed.
```

The outbox is the same outbox the TB hybrid uses. This is deliberate — when migration happens, the outbox worker's sink swaps from `post_transfer_batch()` to TB's `create_transfers` + fallback to `post_transfer_batch()` for non-migrated accounts. Same table, same rows, different consumer.

### 4.2 Outbox table

```sql
CREATE TABLE ledger_outbox (
  id              UUID PRIMARY KEY,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  batch_payload   JSONB NOT NULL,       -- array of transfer events (TB shape)
  batch_size      INTEGER GENERATED ALWAYS AS (jsonb_array_length(batch_payload)) STORED,
  state           TEXT NOT NULL DEFAULT 'pending',
                  -- 'pending', 'processing', 'committed', 'failed'
  processor_id    UUID,
  submitted_at    TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  error           TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  route_hint      TEXT                   -- 'pg', 'tb', 'mixed' (populated Phase 4+)
);

CREATE INDEX idx_outbox_pending
  ON ledger_outbox(created_at)
  WHERE state = 'pending';

CREATE INDEX idx_outbox_processing_stale
  ON ledger_outbox(submitted_at)
  WHERE state = 'processing';
```

### 4.3 Outbox worker

```python
# Pseudocode for clarity
def drain_outbox():
    while True:
        batch = pg.execute("""
            UPDATE ledger_outbox
            SET state = 'processing', processor_id = %s, submitted_at = now(), attempts = attempts + 1
            WHERE id IN (
                SELECT id FROM ledger_outbox
                WHERE state = 'pending'
                ORDER BY created_at
                LIMIT 100
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, batch_payload
        """, [worker_id])

        for row in batch:
            try:
                with pg.transaction():
                    result = pg.execute("SELECT post_transfer_batch(%s)", [row.batch_payload])
                    pg.execute(
                        "UPDATE ledger_outbox SET state='committed', completed_at=now() WHERE id=%s",
                        [row.id]
                    )
            except LinkedEventFailed as e:
                pg.execute(
                    "UPDATE ledger_outbox SET state='failed', error=%s WHERE id=%s",
                    [str(e), row.id]
                )
            except TransientError:
                pg.execute(
                    "UPDATE ledger_outbox SET state='pending' WHERE id=%s",
                    [row.id]
                )
                # retry on next drain cycle
```

**Stale-processing recovery:** a separate job resets `state='processing' AND submitted_at < now() - interval '5 minutes'` back to `'pending'`. Handles worker crashes.

**Reasoning:** SKIP LOCKED lets many workers run concurrently without contention. Idempotency via transfer IDs means re-submitting a partially-processed batch is safe. Failed rows stay for manual triage.

### 4.4 Performance optimizations — Postgres-only

Applied in order of ROI:

1. **Batch at the outbox drain.** Worker reads 100–1000 outbox rows, concatenates their payloads, calls `post_transfer_batch` once. This is the Postgres analog of TB's batched ingestion. 10–50× speedup over per-row calls. §△-C: tune batch size based on lock-hold-time measurements under load.

2. **`synchronous_commit = local`** on the ledger database if RPO of ~100ms is acceptable. Meaningful throughput gain (2–5× on small transactions). Document the RPO implication in the DR runbook.

3. **Connection pooling with pgbouncer** in transaction mode. Each outbox worker holds a session briefly; many workers share few backend connections.

4. **Partition `ledger_transfers` by timestamp** (monthly partitions). Keeps hot indexes small, makes archival cheap. `ledger_accounts` does NOT need partitioning.

5. **COPY for bulk inserts inside the batch function** when batch size > 100 events. Use a temp table, COPY into it, then `INSERT INTO ledger_transfers SELECT * FROM temp_transfers`. 3–5× faster than multi-row INSERT for large batches. §△-D: implementation detail, can be skipped in MVP.

6. **Account-shard hot accounts.** For the top-N SKUs by contention, split (SKU, location) Available into N shards. Writes route randomly; reads sum. Only implement if monitoring shows lock waits. §△-E: document which accounts are sharded; migration to TB removes the shards.

Not done:
- No `synchronous_commit = off` (unacceptable RPO for a ledger).
- No unlogged tables (non-durable, wrong tool).
- No "fast" stored procedures that bypass triggers (there are no triggers; the write function is the path).

### 4.5 Document tables and account map

As per the target spec. The `tb_account_map` table is created with NUMERIC(39) IDs from day one. The name stays `tb_account_map` even in Phase 1 — it's the abstraction, not the system. Alternative: call it `ledger_account_map`; doesn't matter, just be consistent.

### 4.6 Full application functionality

Per the target spec, all of it:

- PO receipt, SO reservation/allocation/ship, TO release/receipt
- WO lifecycle with per-op WIP accounts
- Quarantine, scrap, rework
- Cycle-count adjustments
- Multi-currency with the Currency Exchange recipe (same two-linked-transfer pattern; works fine in the Postgres function)
- Commodity provisional pricing (§17 of target spec)
- Reconciliation jobs

No feature is deferred to a TB phase. The application is feature-complete on Postgres.

### 4.7 Reconciliation

Daily jobs:

1. **Double-entry invariant:** `SELECT SUM(debits_posted) - SUM(credits_posted) FROM ledger_accounts` must be zero. If non-zero, stop the world and investigate.
2. **Subledger-to-control:** sum per-customer AR balances, compare to AR control account balance. Zero delta expected.
3. **Outbox consistency:** committed outbox rows have their transfer IDs resolvable. Stragglers flagged.
4. **Projection consistency:** projection's per-account balances match `ledger_accounts` balances (see Phase 2).

---

## 5. Phase 2 — Projection layer

### 5.1 Why this phase exists

Without it, reports query `ledger_transfers` and `ledger_accounts` directly. When migration to TB happens, every such report is a migration blocker. With it, reports query the projection, and migration touches only the projection's upstream.

### 5.2 Change data capture in Postgres

Two viable mechanisms:

**Option A — Logical replication (preferred):** Postgres logical decoding publishes `ledger_transfers` inserts and `ledger_accounts` updates. A consumer (Debezium, pg_recvlogical + custom, or a direct `wal2json` reader) feeds the projection.

Advantage: zero application overhead, no triggers, no write amplification.
Disadvantage: operational complexity of managing a replication slot.

**Option B — Trigger-fed changefeed table:** an AFTER INSERT trigger on `ledger_transfers` writes a row to `ledger_changefeed`. The projection reads from `ledger_changefeed` in order.

Advantage: simpler to deploy, no logical replication ops.
Disadvantage: 10–20% write overhead, table to manage, harder to scale.

§△-F **Choice of mechanism:** start with Option B (trigger + changefeed table) for simplicity; move to Option A if trigger overhead becomes measurable or if the changefeed table becomes a contention point. Document the decision and the threshold for switching.

**Reasoning for either:** both produce the same output stream of "transfer events with post-transfer balances." That stream is structurally identical to TB's AMQP CDC output (which emits transfer + post-transfer balances per event). When migration happens, the projector's consumer barely changes — only the source connector.

### 5.3 Projection schema

Directly per the target spec §8.2:

- `inv_by_sku_location(sku_id, location_id, qty_available, qty_reserved, qty_on_hold, value, updated_at)`
- `wip_by_wo(wo_id, op_number, qty, value, last_event_ts)`
- `wip_by_op(parent_sku_id, op_number, qty, value)`
- `gl_balances(account_kind, ledger, period_id, debits, credits, balance)`
- `counterparty_balances(counterparty_id, account_kind, balance)`
- `wo_cost_trail(wo_id, event_seq, code, op, amount, account_kind, ts)` (append-only)
- `commodity_pool_activity(pool_id, event_ts, qty_in, qty_out, receipt_id)` (FIFO attribution feed)
- `period_snapshots(period_id, account_id, balance_close)`

Each table is maintained by one idempotent projector handler keyed on event `code` + `account_kind`.

### 5.4 Projector service

Single stateless process (or N replicas with stream partitioning). Reads the change stream in timestamp order. For each event:

1. Identify which projection tables are affected (by `code` + account kinds).
2. UPSERT into each affected table.
3. Advance cursor (store last-processed timestamp in a `projector_state` table).

**Idempotency:** every event's ID + timestamp is recorded in the projection's own dedupe table (or handled via UPSERT semantics). Replaying the stream from any earlier cursor position yields the same result.

§△-G **Stream partitioning for scale:** Phase 2 uses a single projector instance. If throughput demands multiple projectors, partition by account ID or SKU ID with per-partition cursors. Don't pre-optimize.

### 5.5 Reporting read path

Every report, every dashboard, every API endpoint that reads ledger-derived data reads from the projection, not from `ledger_transfers` or `ledger_accounts` directly.

Exceptions:
- Admin/DBA tools and reconciliation jobs may read the ledger tables directly.
- `get_balance()` reads `ledger_accounts` directly (it IS the balance read path).

**Reasoning:** when accounts migrate to TB, `ledger_accounts` for those accounts stops being updated. Any report reading `ledger_accounts` directly stops being correct. Reports reading the projection continue to work because the projection is fed from both sources in Phase 4+.

### 5.6 Gate to Phase 3

Phase 2 is complete when:
- All customer-facing reports read from the projection.
- Reconciliation job (projection vs ledger tables) runs clean for 14 consecutive days.
- Projector can replay from scratch and produce identical state (tested quarterly).

---

## 6. Phase 3 — TigerBeetle shadow mode

### 6.1 Entry criteria (all three must be true)

1. **Lock contention signal:** `pg_stat_activity` shows `lock_waits` or `deadlock_retry` > 0.5% of transactions during peak, sustained over 4+ weeks, and not fixable by routine indexing/tuning.
2. **Latency signal:** P99 write latency on the outbox drain exceeds business-acceptable threshold during peak, and batch-size tuning doesn't resolve it.
3. **Volume trajectory:** 12-month extrapolation of current growth exceeds 10,000 sustained transfers/sec with hot-account concentration (top 10% of accounts getting >50% of writes).

If any of the three is false, stop. Phase 3 is not justified. The Postgres ledger is working as designed.

§△-H **Entry criteria tuning:** these thresholds are placeholders. Calibrate them based on observed production data in Phase 1–2. The goal is "migration is cheaper than continuing to optimize Postgres," which is a business decision informed by monitoring.

### 6.2 Shadow mode architecture

```
API tier ─► Postgres (documents, outbox, account map)
           │
           ▼
  Outbox worker ─► post_transfer_batch() (Postgres, authoritative)
                ─► create_transfers() (TB, shadow — writes ignored for reads)

  Reconciliation worker ─► every 10 minutes:
    for each account in shadow mode:
      pg_balance = SELECT from ledger_accounts
      tb_balance = lookup_accounts([tb_id])
      IF pg_balance != tb_balance: alert
```

**The outbox worker is the critical integration point.** Post-shadow-mode, it:

1. Receives outbox row.
2. Calls `post_transfer_batch()` on Postgres. Commits.
3. Calls TB `create_transfers()` with the same IDs. Result ignored for correctness; logged for reconciliation.
4. Marks outbox row as committed.

TB write failures do not fail the overall transaction. Postgres is the authority. TB is training.

### 6.3 TB account and transfer ID generation

For every Postgres account in `ledger_accounts` that will be shadow-written to TB:
- Generate a corresponding TB u128 account ID.
- Insert into `tb_account_map` with both IDs.
- At account creation time, dual-write: Postgres first, then TB.

Transfer IDs: use the same NUMERIC(39) ID in both systems. No remapping needed. TB accepts any u128.

**Reasoning:** keeping transfer IDs identical across systems means every reconciliation check, every debug session, every audit trace uses one ID. Huge operational win.

### 6.4 Reconciliation in shadow mode

Nightly full sweep: for every shadowed account, compare Postgres and TB balances. Mismatches → alert, root-cause, fix (usually a projector or outbox bug).

Every shadow-written batch: compare per-batch results. If Postgres says `ok` and TB says `exceeds_credits`, that's a bug in the flag translation or the account creation sequencing. Log and fix.

§△-I **Reconciliation tolerance:** zero delta expected, zero delta required. Any mismatch blocks Phase 4 for the affected subledger. Document the triage runbook.

### 6.5 Duration

2–3 months of shadow mode under full production load. During this time:
- All features exercised (WO lifecycle, commodity settlement, multi-currency, edge cases).
- A full period close run (catches accrual/revaluation logic).
- Operational muscle built: CDC supervision, TB upgrade cycles, account lazy-materialization under load.
- Rollback tested: "turn off TB writes" must be a single config flag.

### 6.6 Gate to Phase 4

- 30 consecutive days zero reconciliation deltas.
- Full period close completed with TB output matching Postgres output.
- Operations team signs off on TB runbook.
- Rollback to Postgres-only tested and works.

---

## 7. Phase 4 — Per-subledger cutover

### 7.1 Strategy

Do not "switch to TB." Migrate one subledger at a time. A subledger is a coherent set of accounts that transact primarily with each other.

Typical cutover order (lowest risk first):

1. **Inventory quantity ledger** (ledger 1, all inventory accounts). Highest contention, lowest cross-coupling with other subledgers. First migration.
2. **WIP value accounts** (value ledger, WIP_OpNN_Value). Tightly coupled to inventory; migrate together with quantity in practice.
3. **Inventory value and COGS** (value ledger, Raw_Inv_Value, FG_Inv_Value, COGS, variance accounts).
4. **AP and AP_Unsettled** (commodity-heavy environments benefit). Counterparty accounts stay in Postgres unless very high volume.
5. **AR** (typically low contention; often stays in Postgres forever).
6. **Cash, Revenue, GL control accounts** (typically low contention; often stays in Postgres forever).

§△-J **Cutover order:** exact order depends on observed contention. The principle is "migrate high-contention subledgers first and migrate coherent units together."

### 7.2 Per-subledger migration procedure

For each subledger:

**Step 1 — Pre-cutover validation (1 week):**
- Shadow mode has been clean for 30+ days for this subledger.
- All reports involving this subledger read from the projection.
- Rollback procedure tested specifically for this subledger.

**Step 2 — Baseline snapshot (T=0):**
- Halt outbox worker briefly (~30s downtime).
- For each account in the subledger: record current balance.
- Submit `imported` transfers to TB establishing the opening balance at T=0.
- TB's `imported` flag allows user-supplied timestamps; use T=0 for all imports.
- Resume outbox worker.

**Step 3 — Flip write authority:**
- Deploy config change: accounts in the subledger route writes to TB first, then Postgres (for continued reconciliation).
- Outbox worker changes: for events touching subledger accounts, call TB first and treat TB result as authoritative. Also write to Postgres for reconciliation.

**Step 4 — Reverse shadow (2–4 weeks):**
- TB is authoritative. Postgres is the shadow.
- Reconciliation still runs; deltas now indicate bugs in the reverse direction.
- Projector starts consuming from TB's AMQP CDC for this subledger instead of from Postgres changefeed.

**Step 5 — Retire Postgres writes:**
- Stop writing to Postgres for this subledger.
- `ledger_accounts` and `ledger_transfers` rows for these accounts become historical; no new inserts.
- Reports verified to still function from projection.

Rollback: at any step before Step 5, flip the config back. Postgres has been receiving all writes up to that point. Zero data loss, minutes of reconciliation to catch up.

### 7.3 Cross-system batches

When a batch touches both TB-migrated and Postgres-resident accounts (e.g., debit TB inventory, credit Postgres AP), atomicity is lost.

Three strategies, in order of preference:

**A. Coherent-unit migration.** Migrate all accounts that commonly transact together as a unit. Inventory and COGS go together; AR stays alone. Most batches touch accounts in one system. This is the default.

**B. Outbox two-phase with idempotent retry.** For batches that must cross systems:
1. Write Postgres half; commit.
2. Write TB half with deterministic IDs; on failure, retry from outbox.
3. Reconciliation catches orphaned half-writes.

Acceptable for occasional cross-system events (month-end GL postings touching migrated inventory value). Not acceptable for hot-path transactions.

**C. Defer cross-system accounts to later migration.** If inventory crosses to AR often, migrate AR too. If it crosses rarely, strategy A + B handles it.

§△-K **Cross-system transaction policy:** formally document which account pairs are permitted to cross systems and what the recovery procedure is. This becomes a review gate for any new transaction type.

### 7.4 Projector in hybrid mode

The projector now consumes from two sources:
- Postgres changefeed for Postgres-resident accounts
- TB AMQP CDC for TB-resident accounts

Both streams carry the same event shape (transfer + post-transfer balances). The projector handlers don't care which source an event came from.

**Timestamp ordering across sources** is the tricky part. TB and Postgres each produce monotonic streams, but merging them requires an epoch-ordered merge. Options:

1. Two independent projections, each for its own source, joined at report time. Simpler but report queries span two tables.
2. Merge streams in the projector using a configurable lag window (wait N seconds for stragglers before committing a timestamp). Unified projection table. More complex.

§△-L **Projection merge strategy:** start with two separate projections; consolidate if report-time joins become painful. Revisit based on report complexity observed.

---

## 8. Phase 5 — Steady-state hybrid operations

### 8.1 Operational model

- **Routine ops:** Postgres and TB coexist indefinitely. Monitoring covers both. Reconciliation runs daily. Projection is the read-side canonical source.
- **New features:** default to Postgres-resident accounts for new subledgers. Migrate to TB only if contention emerges. Most new accounts never move to TB.
- **Account retirement:** closed accounts in either system stay forever. TB doesn't allow deletion; Postgres is treated the same for consistency.
- **Period close:** projector runs close across both sources. Snapshots captured for all accounts regardless of residency.

### 8.2 Reverse migration (rare)

If a subledger was migrated to TB but the operational overhead exceeds the benefit (e.g., volume decreased), it can be migrated back to Postgres:

1. Capture current TB balances at T=0.
2. Bulk-insert opening-balance transfers into Postgres at T=0.
3. Flip write authority back.
4. Run in shadow mode (reverse direction) for 2 weeks.
5. Retire TB for this subledger (but keep the TB cluster and account records for audit).

Not a design goal but a supported operation.

§△-M **Reverse migration playbook:** write the detailed runbook before it's ever needed. Keeps the option real, not theoretical.

### 8.3 Long-term evolution

- If the entire hot path migrates to TB, Postgres becomes a metadata + document + projection store. The ledger tables become empty for all new activity but stay for historical audit.
- If TB never gets used, Phase 0 discipline cost ~2 weeks of schema design and ~200 lines of idempotency/linked-chain code. The Postgres ledger operates indefinitely as built.
- The projection layer is the load-bearing abstraction for both outcomes. Never let it calcify.

---

## 9. Monitoring and decision signals

### 9.1 Phase 1–2 monitoring (Postgres-only mode)

- `pg_stat_activity` lock_waits by relation — alert if `ledger_accounts` lock_waits > 1% of activity
- `pg_stat_database` deadlocks — alert if any deadlocks involving ledger tables
- Outbox worker lag — alert if `state='pending'` rows older than 10s
- Outbox failed rows — alert immediately
- `post_transfer_batch` latency distribution — track P50, P99, P99.9
- Per-account contention heatmap — which 100 accounts take which fraction of writes
- Reconciliation job — daily success, any non-zero delta alerts immediately

### 9.2 Phase 3 monitoring (shadow mode)

- All above, plus:
- TB shadow write success rate
- Per-batch PG vs TB result code comparison (alert on any mismatch)
- TB cluster health: writes/sec, batch size, commit latency, replica lag
- AMQP sidecar: liveness, queue depth, lag
- Projector: lag from each source, per-handler error rate

### 9.3 Phase 4+ monitoring (hybrid)

- All Phase 3 monitoring
- Per-subledger write routing metrics (which accounts go to which system)
- Cross-system batch count and success rate
- Projection stream-merge lag (if using unified merged projection)
- Coherence: per-subledger reconciliation delta

### 9.4 Decision signals for progressing phases

A phase transition is a business decision, not automatic. Signals that should trigger the decision conversation:

| Signal                                                              | Consider action                                   |
|---------------------------------------------------------------------|---------------------------------------------------|
| Phase 1 lock_waits > 0.5% sustained                                  | Account sharding OR begin Phase 3 planning        |
| Phase 1 projected volume > 5,000 TPS sustained                       | Begin Phase 2 if not started; plan Phase 3        |
| Phase 2 projection lag > 10s under peak                              | Scale projector; consider stream partitioning     |
| Phase 3 reconciliation delta > 0 for any period                      | Halt Phase 4 planning; root-cause                 |
| Phase 4 cross-system transaction rate > 5% of batches                | Re-evaluate cutover order                         |
| Phase 5 hybrid reconciliation delta                                  | Treat as production incident                      |

---

## 10. Known deferrals and open issues

- §△-A Sub-second timeout precision for pending transfers (§3.3)
- §△-B get_balances_batch implementation (§3.4)
- §△-C Outbox batch size tuning under load (§4.4)
- §△-D COPY-based insert inside batch function (§4.4)
- §△-E Hot-account sharding list and lifecycle (§4.4)
- §△-F CDC mechanism: logical replication vs trigger changefeed (§5.2)
- §△-G Projector partitioning for scale (§5.4)
- §△-H Phase 3 entry criteria calibration (§6.1)
- §△-I Reconciliation tolerance policy (§6.4)
- §△-J Cutover order — subledger dependency mapping (§7.1)
- §△-K Cross-system transaction policy (§7.3)
- §△-L Projection merge strategy for hybrid sources (§7.4)
- §△-M Reverse migration playbook (§8.2)

---

## 11. Explicit non-decisions

- NO server-side ID generation. All IDs are client-generated and time-ordered from Phase 0.
- NO direct UPDATE of account balance columns from application code. Ever.
- NO reports reading from `ledger_transfers` or `ledger_accounts` directly after Phase 2.
- NO `synchronous_commit = off` on the ledger database (RPO unacceptable).
- NO skipping Phase 3 shadow mode. "We tested it in staging" is insufficient.
- NO partial feature migration (e.g., "migrate inventory writes but not WO closes"). Subledgers migrate as coherent units.
- NO deletion of accounts, in either system. Closed, yes; deleted, no.
- NO reliance on TB-specific features (e.g., `history` flag semantics) until Phase 3. Phase 0 schema records the flag for forward compatibility but doesn't depend on it.

---

## 12. Reasoning summary

The cost of this roadmap over a naive Postgres build is **about 2–3 weeks of Phase 0 discipline and about 1 month of Phase 2 infrastructure**. If TB is never needed, that's the total cost; the Postgres ledger operates cleanly forever.

The cost of this roadmap over a naive TB build is **saved months** — Phase 1 production is on Postgres, which is operationally well-understood, and TB is introduced only after real workload data justifies it.

The optionality value is real: **the migration from Phase 2 to Phase 5 is mechanical**, not architectural. Schemas don't change. Application code changes only in the `post_transfer_batch` wrapper and the projector's source connector. Reports don't change. This is the entire point.

The risk that kills this roadmap: **violating the Phase 0 discipline** — writing directly to balance columns, skipping the outbox, letting reports query ledger tables. Each violation adds migration cost. Enforce the discipline from day one via code review, not via policy.

If Phase 3 is never triggered, the roadmap has delivered a clean, well-architected Postgres ledger with a production-grade projection layer. Not wasted. Not even a loss. The Phase 0 and Phase 2 work is directly valuable regardless of whether TB ever enters the picture.

If Phase 3 is triggered, the roadmap has delivered a migration path that takes months instead of years, with rollback available at every step. That's the insurance the discipline buys.
