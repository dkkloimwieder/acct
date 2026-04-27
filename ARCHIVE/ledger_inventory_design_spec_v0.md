# Ledger + Inventory System — Design Specification

Version: 0.1 (pre-implementation)
Status: Draft — sections marked §△ are expected to require revision after initial build.

## 1. Scope

**In scope:** Combined GL and inventory subledger on TigerBeetle (TB). SKU-and-location quantity tracking, per-routing-step WIP, standard ERP document lifecycle (WO/SO/TO/PO), double-entry financial accounting, multi-currency, reservation/allocation, reporting via CDC projection.

**Out of scope for TB:** Lot-level identity, serial number identity, FEFO ordering, FIFO/LIFO cost layering, document workflow state, authorization/approval, reference data, human-readable search, analytical/OLAP reporting. These live in Postgres or in the read-side projection.

**Non-goals:** TB as single source of truth for lot/serial identity. Real-time OLAP in TB. Cross-cluster atomicity. Browser-direct TB access.

## 2. Architecture

Two-system split with a one-way invariant.

```
 Clients ──► API tier ──► Postgres (documents, outbox, metadata)
                    │
                    └───► TigerBeetle (accounts, transfers, balances)
                             │
                       CDC (AMQP) ──► RabbitMQ ──► Projector ──► Postgres read model
                                                              └──► ClickHouse (reports)
```

**Rule:** Write to Postgres first with the outbox row. Write to TB last. Read from TB first when a balance or invariant is needed; from Postgres read model when metadata or rollup is needed. NEVER perform a TB lookup that gates a subsequent TB write on the hot path beyond the balances TB itself returns.

**Rule:** The document is a Postgres row. The movements the document causes are TB transfers. The join is `postgres_document.id == transfer.user_data_128`.

## 3. TigerBeetle data model

### 3.1 Ledgers

| Ledger ID | Name          | Purpose                                  | Enforcement               |
|-----------|---------------|------------------------------------------|---------------------------|
| 1         | `qty`         | Inventory quantity (unitless integer)    | Non-negative per account  |
| 840       | `usd`         | USD value — inventory, GL, variance      | Per-account normal side   |
| 978, 826… | per currency | Additional transacting currencies (EUR, GBP…) | Same as 840            |

Cross-currency transactions use the Currency Exchange recipe: two linked transfers through a liquidity-provider account pair. Reporting currency translation is a projector concern, NOT a TB concern — TB records the economic event in the transacting currency.

§△ **Single qty ledger vs per-warehouse qty ledger:** Initial build uses ledger 1 globally. Revisit if inter-warehouse transfers become a small minority of movements AND a single cluster's primary shows saturation — at which point per-warehouse ledgers plus cross-ledger transit become appropriate. Do not pre-optimize.

### 3.2 Account taxonomy

All accounts are created lazily on first use via a linked `create_accounts` + first transfer batch. NEVER pre-materialize cartesian products.

**Inventory quantity accounts (ledger=1, flags=credits_must_not_exceed_debits):**

| Account kind            | Grain                     | Id source                               |
|-------------------------|---------------------------|-----------------------------------------|
| Available stock         | (sku, location)           | `ulid()`, mapped in Postgres             |
| WIP by operation        | (parent_sku, wip_op_NN)   | `ulid()`, mapped in Postgres             |
| Quarantine / hold       | (sku, hold_pool)          | `ulid()`, one per SKU or per hold type  |
| Scrap                   | (sku, scrap_pool)         | `ulid()`                                 |
| In-transit per lane     | (sku, origin→dest)        | `ulid()` per lane                        |
| Consumed pool           | (sku) or global           | `ulid()`                                 |
| Supplier counterparty   | (supplier_id) or global   | `ulid()`                                 |
| Customer counterparty   | (customer_id) or global   | `ulid()`                                 |
| Creation void           | global singleton          | fixed known id                           |

**Value accounts (ledger=840/978/..., flags per normal side):**

| Account                 | Normal side | Flag                                 |
|-------------------------|-------------|--------------------------------------|
| Raw_Inv_Value           | debit       | `credits_must_not_exceed_debits`      |
| WIP_OpNN_Value          | debit       | `credits_must_not_exceed_debits`      |
| FG_Inv_Value            | debit       | `credits_must_not_exceed_debits`      |
| COGS                    | debit       | none (can go either direction for adj) |
| AP                      | credit      | `debits_must_not_exceed_credits`      |
| AR                      | debit       | `credits_must_not_exceed_debits`      |
| Cash / Bank_NNN         | debit       | `credits_must_not_exceed_debits`      |
| Revenue                 | credit      | `debits_must_not_exceed_credits`      |
| Sales_Tax_Payable       | credit      | `debits_must_not_exceed_credits`      |
| Labor_Applied (clearing)| credit      | `debits_must_not_exceed_credits`      |
| OH_Applied (clearing)   | credit      | `debits_must_not_exceed_credits`      |
| Labor_Expense           | debit       | none                                  |
| Variance_* (PPV, MUV, LV, OHV, ScrapV, WOCloseV) | debit | none       |

§△ **Value accounts per-SKU aggregate vs per-(SKU, location):** Initial build uses per-SKU aggregate value accounts. Per-(SKU, location) value accounts double account count and are only justified if location-level inventory valuation becomes a reporting hard requirement. Revisit after first reporting iteration.

§△ **Counterparty pools — one global vs per-supplier/customer:** Initial build uses one supplier pool and one customer pool globally; attribution is via `user_data_128 = supplier_id / customer_id` on the transfer. Per-counterparty accounts become warranted if supplier/customer balance queries become a hot read path OR if subledger AP/AR reconciliation demands per-counterparty balances at the TB layer rather than from the projection.

### 3.3 Account ID strategy

ALWAYS use TB time-based IDs (`ulid()`-style: high bits = ms timestamp, low bits = random) for new accounts. NEVER deterministically hash business keys into TB ids — the LSM optimizes for strictly-increasing keys and random-looking ids cost throughput.

Postgres owns the `account_map` table:

```sql
CREATE TABLE tb_account_map (
  tb_id          NUMERIC(39) PRIMARY KEY,   -- u128
  account_kind   TEXT NOT NULL,             -- 'stock_available', 'wip_op', 'ap', ...
  sku_id         UUID,                      -- nullable, only for inventory accounts
  location_id    UUID,                      -- nullable
  routing_op     INT,                       -- nullable, for WIP accounts
  counterparty_id UUID,                     -- nullable, for supplier/customer pools
  ledger         INT NOT NULL,
  currency       CHAR(3),                   -- nullable, for value accounts
  created_at     TIMESTAMPTZ NOT NULL,
  closed_at      TIMESTAMPTZ                -- if tb account has flags.closed set
);

CREATE UNIQUE INDEX ON tb_account_map (account_kind, sku_id, location_id, ledger)
  WHERE closed_at IS NULL;
CREATE UNIQUE INDEX ON tb_account_map (account_kind, sku_id, routing_op, ledger)
  WHERE closed_at IS NULL;
-- additional partial indexes per account_kind shape
```

API tier caches this map in-process with TTL + invalidation on account creation.

### 3.4 Transfer encoding conventions

Every transfer MUST populate these fields:

| Field             | Meaning                                                          |
|-------------------|------------------------------------------------------------------|
| `user_data_128`   | **Document id** (WO, SO, TO, PO, invoice, adjustment, etc.)      |
| `user_data_64`    | Context-specific (see below)                                     |
| `user_data_32`    | Context-specific (see below)                                     |
| `code`            | **Transfer reason** (see enum below)                             |
| `ledger`          | As required by account pair                                      |
| `amount`          | Unitless integer — qty for ledger 1, minor-currency units for value ledgers |
| `timestamp`       | Cluster-assigned; NEVER rely on app time                         |

**`code` enum (reserve ranges, document in Postgres):**

```
  1000–1099  Receipts
    1000 PO_RECEIPT, 1001 PO_RETURN_TO_VENDOR, 1010 CUSTOMER_RETURN
  1100–1199  Issues / Ship
    1100 SO_SHIP, 1110 RM_ISSUE_TO_WO
  1200–1299  Transfers
    1200 TO_RELEASE, 1201 TO_RECEIPT, 1210 BIN_MOVE
  1300–1399  WIP
    1300 WO_START, 1310 OP_MOVE, 1320 WO_COMPLETE, 1330 REWORK
  1400–1499  Labor / OH
    1400 LABOR_APPLY, 1410 OH_APPLY
  1500–1599  Holds / Scrap
    1500 QUARANTINE, 1510 RELEASE_FROM_QUARANTINE, 1520 SCRAP, 1530 DAMAGE
  1600–1699  Financial
    1600 AR_INVOICE, 1610 AR_PAYMENT, 1620 AP_BILL, 1630 AP_PAYMENT
  1700–1799  Variances
    1700 PPV, 1710 MUV, 1720 LV, 1730 OHV, 1740 SCRAP_V, 1750 WO_CLOSE_V
  1800–1899  Corrections / adjustments
    1800 CYCLE_COUNT_ADJ, 1810 COST_RESTATE, 1820 REVERSAL
  1900–1999  FX / liquidity
    1900 FX_LEG, 1910 FX_SPREAD
  2000–2099  Commodity provisional pricing (see §17)
    2000 PO_RECEIPT_PROVISIONAL, 2010 PO_SETTLEMENT,
    2020 PRICE_TRUEUP_INVENTORY, 2030 PRICE_TRUEUP_COGS, 2040 PRICE_TRUEUP_WIP
```

`code` is `u16` (65,536 max). Do not encode SKUs or entity IDs here. Reason taxonomy only.

**`user_data_64` / `user_data_32` conventions per code family:**

| Code family      | `user_data_64`                | `user_data_32`         |
|------------------|-------------------------------|------------------------|
| Receipts         | vendor_id hash (optional)     | PO line number         |
| Issues / Ship    | customer_id hash (optional)   | SO line number         |
| Transfers        | lane id                       | TO line number         |
| WIP              | BOM component id hash (RM)    | **routing operation #** |
| Labor / OH       | work_center_id                | routing operation #    |
| Holds / Scrap    | reason code subtype           | routing operation # if in WIP |
| Financial        | counterparty_id hash          | line number            |
| Variances        | BOM / routing reference       | routing operation #    |

§△ **user_data_64 for counterparty hashing:** Hashing customer/vendor ids into a u64 for filterable attribution is a pragmatic compromise; collisions are theoretically possible but improbable (~4 billion values, birthday-bound). Revisit if collision handling becomes a reconciliation concern — at that point either move to per-counterparty accounts or accept the projection as the sole attribution source.

### 3.5 Account flags

| Account kind                    | Flags                                         |
|---------------------------------|-----------------------------------------------|
| All inventory qty accounts      | `credits_must_not_exceed_debits`              |
| All value accounts with a normal side | the appropriate `*_must_not_exceed_*`  |
| COGS, Labor_Expense, Variances  | (no balance-sign enforcement)                 |
| Value accounts needing period snapshots | `history` ALSO set                    |

ALWAYS set `flags.history` on accounts that feed period-end valuation, balance-sheet reporting, or any `get_account_balances` call. This is decided at account creation and cannot be changed later.

## 4. Postgres schema (canonical document tables)

Not exhaustive — these are the tables TB transfers reference via `user_data_128`:

```
skus(id, code, description, uom, standard_cost, std_cost_effective_at, ...)
locations(id, code, name, facility_id, kind, parent_location_id, ...)
facilities(id, code, name, ...)
lots(id, sku_id, lot_number, mfg_date, exp_date, attributes_jsonb)   -- if lots in use
serials(id, sku_id, serial_number, current_location_id, status,
        last_tb_transfer_id, ...)                                    -- if serials in use
routings(id, parent_sku_id, op_number, work_center_id, std_labor_cost,
         std_oh_cost, description, ...)
boms(id, parent_sku_id, component_sku_id, op_number, qty_per, scrap_pct, ...)
work_orders(id, sku_id, qty, status, started_at, closed_at, ...)
sales_orders(id, customer_id, status, ...)
sales_order_lines(id, so_id, sku_id, qty, unit_price, ...)
purchase_orders / po_lines
transfer_orders / to_lines
invoices_ar / invoices_ap
customers, suppliers
fx_rates(id, from_ccy, to_ccy, rate, effective_at)
periods(id, code, starts_at, ends_at, closed_at)

outbox(id UUID PK, created_at, payload_jsonb, tb_submitted_at,
       tb_completed_at, error)
tb_account_map(...)  -- see §3.3
```

Document tables NEVER hold balance fields. Balances come from TB (authoritative) or from the read model (cached rollup).

## 5. Transaction patterns

Each pattern is one atomic linked batch. ALWAYS construct the full chain before submission. NEVER split what must be atomic across multiple batches.

### 5.1 PO receipt (goods in)

Inputs: PO_id, sku, qty, unit_cost, receiving_location, currency.

```
Linked batch:
  T1 ledger=1   credit Supplier_Pool,      debit (sku, receiving_loc) Available,    amount=qty
                code=PO_RECEIPT, u128=PO_id, u32=PO_line
  T2 ledger=ccy credit AP,                 debit Raw_Inv_Value,                     amount=qty*unit_cost
                code=PO_RECEIPT, u128=PO_id, u32=PO_line
```

If standard-cost with PPV:

```
  T2 credit AP, debit Raw_Inv_Value, amount=qty*std_cost
  T3 credit AP (or debit if favorable), debit/credit PPV_Variance,
     amount=qty*(actual_cost - std_cost), code=PPV
```

### 5.2 Inter-location transfer (TO)

Immediate (no transit window needed):

```
Linked batch:
  T1 ledger=1 credit (sku, origin) Available, debit (sku, dest) Available, amount=qty
     code=BIN_MOVE, u128=TO_id
```

With transit window:

```
Release:
  T1 ledger=1 credit (sku, origin) Available, debit (sku, Transit_origin→dest), amount=qty
     code=TO_RELEASE, u128=TO_id

Receipt at dest:
  T2 ledger=1 credit (sku, Transit_origin→dest), debit (sku, dest) Available, amount=qty
     code=TO_RECEIPT, u128=TO_id
```

Value ledger movements accompany T1 and T2 if per-(sku, location) value accounts are in use; omitted if per-SKU aggregate value is used (net effect zero).

### 5.3 SO reservation → allocation → ship

Reservation via pending transfer:

```
T_reserve  ledger=1 credit (sku, loc) Available, debit (sku, loc) Available
           flags=pending, timeout=<reservation_ttl_seconds>
           code=SO_RESERVE, u128=SO_id
```

Reservation is a *self-pending* on the Available account. `debits_pending` on that account is total reserved qty. Promisable = `credits_posted − debits_posted − debits_pending`. NEVER model reservations as a debit to a separate Reserved account unless the reservation lifecycle justifies the account-count doubling (it rarely does).

§△ **Self-pending vs separate Reserved account:** The self-pending approach gives atomicity and expiry without a second account. The tradeoff: `debits_pending` includes reservations from all SOs against that account — per-SO reservation balance is not queryable from TB alone, it's a projection concern. If per-SO real-time reservation visibility is required from the authoritative store, switch to Available/Reserved account pair.

Allocation (pick confirm):

```
T_allocate  post_pending_transfer of T_reserve, amount=AMOUNT_MAX (or partial)
            code=SO_ALLOCATE, u128=SO_id
```

Ship:

```
Linked batch:
  T1 ledger=1   credit (sku, loc) Available,    debit Customer_Pool,              amount=qty
                code=SO_SHIP, u128=SO_id
  T2 ledger=ccy credit FG_Inv_Value,            debit COGS,                       amount=qty*unit_cost
                code=SO_SHIP, u128=SO_id
  T3 ledger=ccy credit Revenue,                 debit AR,                         amount=qty*unit_price
                code=SO_SHIP, u128=SO_id
  T4 ledger=ccy credit Sales_Tax_Payable,       debit AR,                         amount=tax
                code=SO_SHIP, u128=SO_id (if applicable)
```

### 5.4 Work order lifecycle (condensed — see §6 for detail)

WO start at Op 10 (standard cost, issue-by-issue RM):

```
Linked batch:
  T1 ledger=1   credit (compA, loc) Available,  debit Consumed_Pool(compA),       amount=qty_A
                code=RM_ISSUE_TO_WO, u128=WO_id, u32=10
  T2 ledger=1   credit Creation_Void,           debit (parent_sku, WIP_Op10),     amount=1
                code=WO_START, u128=WO_id, u32=10
  T3 ledger=ccy credit Raw_Inv_Value,           debit WIP_Op10_Value,             amount=compA_cost
                code=RM_ISSUE_TO_WO, u128=WO_id, u32=10
  T4 ledger=ccy credit Labor_Applied,           debit WIP_Op10_Value,             amount=op10_std_labor
                code=LABOR_APPLY, u128=WO_id, u32=10
  T5 ledger=ccy credit OH_Applied,              debit WIP_Op10_Value,             amount=op10_std_oh
                code=OH_APPLY, u128=WO_id, u32=10
```

Op move (Op 10 → Op 20):

```
Linked batch:
  T1 ledger=1   credit (parent, WIP_Op10),      debit (parent, WIP_Op20),         amount=qty
                code=OP_MOVE, u128=WO_id, u32=20
  T2 ledger=ccy credit WIP_Op10_Value,          debit WIP_Op20_Value,             amount=accumulated_cost_per_unit * qty
                code=OP_MOVE, u128=WO_id, u32=20
```

WO complete (Op 30 → FG, standard-cost variance):

```
Linked batch:
  T1 ledger=1   credit (parent, WIP_Op30),      debit (parent, FG_loc) Available, amount=qty
                code=WO_COMPLETE, u128=WO_id
  T2 ledger=ccy credit WIP_Op30_Value,          debit FG_Inv_Value,               amount=qty*std_cost
                code=WO_COMPLETE, u128=WO_id
  T3 ledger=ccy credit WIP_Op30_Value,          debit WO_Close_Variance,          amount=residual
                code=WO_CLOSE_V, u128=WO_id
     (T3 direction depends on sign of residual)
```

§△ **Backflush vs issue-by-issue:** Initial build supports issue-by-issue per RM line at each op. Backflush is a mode where op-complete triggers a computed multi-transfer linked chain consuming the standard BOM for that op. Implement backflush in phase 2 once issue-by-issue is stable and per-op variance reporting proves the standard costs are trustworthy.

### 5.5 Scrap at operation

```
Linked batch:
  T1 ledger=1   credit (parent, WIP_OpNN),      debit (parent, Scrap_Pool),       amount=qty
                code=SCRAP, u128=WO_id, u32=NN
  T2 ledger=ccy credit WIP_OpNN_Value,          debit Scrap_Variance,             amount=accumulated_cost
                code=SCRAP_V, u128=WO_id, u32=NN
```

### 5.6 Quarantine and release

```
Quarantine:
  T1 ledger=1 credit (sku, loc) Available, debit (sku, Quarantine_Pool), amount=qty
     code=QUARANTINE, u128=QC_hold_doc_id, u64=reason_subtype

Release:
  T1 ledger=1 credit (sku, Quarantine_Pool), debit (sku, loc) Available, amount=qty
     code=RELEASE_FROM_QUARANTINE, u128=QC_hold_doc_id
```

Postgres holds the QC hold document with reason, authorizer, test results, release date.

### 5.7 AR payment / AP payment

Standard double-entry, ledger=ccy only. Reference invoice via `user_data_128`.

### 5.8 Cycle-count adjustment

```
Linked batch:
  T1 ledger=1   credit/debit (sku, loc) Available vs Physical_Adj_Pool(sku), amount=|delta|
                code=CYCLE_COUNT_ADJ, u128=count_doc_id
  T2 ledger=ccy corresponding value adjustment against Inventory_Adj_Expense
                code=CYCLE_COUNT_ADJ, u128=count_doc_id
```

### 5.9 Reversals

NEVER update or delete. A reversal is a new linked batch that undoes T1…Tn with opposite debit/credit assignments, carrying the same `user_data_128` and `code=REVERSAL` (or the domain-specific reversal code where one exists — e.g., `PO_RETURN_TO_VENDOR`).

## 6. WIP model (detail)

### 6.1 Account grain

- **WIP qty:** one account per `(parent_sku, routing_op)`. Aggregated across all WOs.
- **WIP value:** one account per `(parent_sku, routing_op)`. Aggregated.
- **Per-WO breakdown:** reconstructed by the projection from `query_transfers(user_data_128=WO_id)`. NEVER create per-WO accounts by default.

§△ **Per-WO accounts for project/job-costing:** For long-cycle or regulated job-cost work orders, create dedicated per-WO per-op accounts. Close them via the Close Account recipe on WO completion. Default off; enable per SKU family or WO type.

### 6.2 Cost method

Initial build: **standard costing with variance capture**. Standard costs pulled from `skus.standard_cost` and `routings.std_*_cost` in Postgres at transaction time. PPV at receipt, MUV/LV/OHV at apply, Scrap_V at scrap, WO_Close_V at close.

§△ **Standard vs WAC vs actual:** WAC requires client-computed running average against `Raw_Inv_Value.balance / qty_on_hand`, read immediately before the transfer. Acceptable but adds a TB read on the write path. Actual costing requires cost engine outside TB. Revisit if cost accuracy demands force WAC; avoid layered FIFO/LIFO entirely.

### 6.3 Operation-level reporting queries

All of these are projector queries; TB transfers are the source:

- "Value in WIP right now by op": balance read on `WIP_OpNN_Value` accounts.
- "RM consumed at Op NN this period": `query_transfers(code=RM_ISSUE_TO_WO, user_data_32=NN, ts_range)` → sum.
- "Labor applied by op by period": `query_transfers(code=LABOR_APPLY, user_data_32=NN, ts_range)`.
- "Full cost trail for WO X": `query_transfers(user_data_128=WO_id)` → ordered by timestamp.
- "Scrap $ by op YTD": `query_transfers(code=SCRAP_V, ts_range)` grouped by `user_data_32` in projector.

### 6.4 Sub-assembly consumption

A sub-assembly consumed at an op of a higher-level parent is treated as a component: `credit (sub_sku, FG_loc), debit Consumed_Pool(sub_sku)`. Multi-level BOMs nest naturally because TB is flat.

## 7. Integration — write path

### 7.1 Outbox pattern

EVERY TB-mutating API call:

```
BEGIN Postgres transaction
  1. Insert/update document rows (SO, WO, PO, etc.)
  2. Insert outbox row with:
     - generated TB transfer ids (time-based ULIDs)
     - full serialized linked batch payload
     - state = 'pending'
  3. Insert any tb_account_map rows for lazily-created accounts
COMMIT

Submit linked batch to TB (create_accounts + create_transfers, linked).
  - On ok / exists: update outbox.state='committed', tb_completed_at=now()
  - On timeout: do nothing; recovery will retry with same ids (idempotent)
  - On hard error: update outbox.state='failed', outbox.error=...
```

TB transfer ids are generated client-side and persisted in the outbox BEFORE TB submission. Retries are safe: TB returns `exists` on duplicate id, treated as success. NEVER regenerate ids on retry.

### 7.2 Recovery worker

Polls `outbox WHERE state='pending' AND tb_submitted_at IS NULL OR < now()-interval '30s'`. Resubmits. Idempotent via ids.

### 7.3 Account lazy-materialization

When a transfer needs an account that isn't in `tb_account_map`:

```
Linked batch prepends:
  create_accounts(new_account) linked=true
  create_transfers(first_transfer_using_it) linked=(next_event_flag)
```

Account creation is part of the atomic transaction. Postgres `tb_account_map` insert happens in the same transaction as the outbox row, BEFORE TB submission.

§△ **Account creation rate limiting:** On large catalog or inter-location move days, many new accounts may be minted. Monitor TB account creation rate; if it becomes a throughput concern, pre-create accounts via a warming job for predictable (sku, location) grids.

## 8. Read model / CDC

### 8.1 CDC transport

`./tigerbeetle amqp` sidecar → RabbitMQ exchange `tb.events` → per-consumer queues.

Run as a supervised process (systemd/k8s). Single instance guarded by RMQ locker queue. Monitor liveness and lag.

§△ **Kafka / alternative transports:** AMQP is the only TB-native CDC as of TB 0.17. A RabbitMQ-to-Kafka bridge (or a dedicated consumer publishing to Kafka/Redpanda) is the interim if Kafka is an infra standard. Revisit when TB ships additional CDC sinks.

### 8.2 Projector

Idempotent, stateless consumer. Per-event handler dispatches on `code` and updates:

- `inv_by_sku_location(sku_id, location_id, qty_available, qty_reserved, qty_on_hold, value, updated_at)`
- `wip_by_wo(wo_id, op_number, qty, value, last_event_ts)`
- `wip_by_op(parent_sku_id, op_number, qty, value)`
- `gl_balances(account_kind, ledger, period_id, debits, credits, balance)`
- `counterparty_balances(counterparty_id, account_kind, balance)` — for subledger AP/AR
- `wo_cost_trail(wo_id, event_seq, code, op, amount, account_kind, ts)` — append-only
- `period_snapshots(period_id, account_id, balance_close)` — populated at period close

ALWAYS use the transfer timestamp as the idempotency key (strictly monotonic). Re-processing from any prior timestamp is supported.

### 8.3 Reporting queries

All GROUP BY, hierarchical rollup (warehouse → zone → bin), cross-entity joins, BI/OLAP live in the projection (Postgres read model) or in a downstream analytical store (ClickHouse) fed from the same CDC stream.

NEVER attempt reporting queries directly against TB beyond the four native query APIs.

### 8.4 Human-readable search

Search over SKUs, customers, vendors, WOs, lots: Postgres full-text or Elasticsearch. NEVER search TB.

## 9. FX and multi-currency

- Transacting currency = ledger. A receipt in EUR posts on ledger 978.
- Reporting currency (USD) rollups happen in the projector using `fx_rates` at event timestamp.
- Revaluation at period end: posted as linked transfers on ledger=reporting_ccy against `FX_Revaluation` accounts. Source of truth for historical rates is Postgres; TB records the economic fact only.
- Cross-currency transaction: two linked transfers through FX liquidity accounts per the Currency Exchange recipe. Spread is a separate linked transfer tagged `code=FX_SPREAD`.

## 10. Period close

TB has no period concept. Close is a projector-side operation.

1. Freeze the reporting projection at period-end timestamp.
2. Snapshot balances into `period_snapshots` for every account with `flags.history` set.
3. Book adjusting entries (accruals, deferrals, revaluations) as regular TB transfers with `code` in the adjustment range.
4. Run variance analysis over the period's transfer stream.
5. Mark `periods.closed_at`. New transactions in the prior period are flagged in the projector as prior-period adjustments (they still post — TB does not restrict by date — but are attributed to the correct period for reporting).

§△ **Preventing back-dated postings:** TB does not enforce period locks. Enforce at the API layer: reject any transfer whose business-date (passed in `user_data_64` or the document's business_date) falls inside a closed period, unless the user has an override role.

## 11. Reconciliation

Nightly jobs:

1. **Projection vs TB:** For each account, compare projector's `balance` against `lookup_accounts([id]).debits_posted − credits_posted`. Mismatches indicate missed CDC events or projector bugs. Threshold alerts.
2. **Subledger vs GL:** Sum per-SKU inventory value from projector; compare to `Raw_Inv_Value + Σ WIP_OpNN_Value + FG_Inv_Value` balances. Zero delta expected.
3. **AR subledger vs AR control:** Sum per-customer balances (from projector) vs `AR.balance`. Zero delta.
4. **Outbox vs TB:** Every outbox row with `state='committed'` has its transfer ids resolvable via `lookup_transfers`. Flag stragglers.

§△ **Serial/lot reconciliation (if those modules are enabled):** Serial rows in Postgres grouped by `(sku, current_location, status)` must sum to the corresponding TB account balances. Mismatches flag operational incidents; daily cadence initially, tune based on volume.

## 12. Operations

### 12.1 Cluster topology

- 6-replica TB cluster across 2 availability zones minimum, 3 zones preferred.
- Single primary; quorum 4/6. All writes go to primary.
- Weekly upgrade window; expect ~5s unavailability per rolling upgrade.
- NEVER run a single-replica TB cluster in production. Single-node data loss is unrecoverable from TB alone.

§△ **DR / zero-RPO:** OSS TB relies on replica survival. Enterprise TB offers object-storage DR; evaluate at scale-up checkpoint.

### 12.2 API tier

Stateless, horizontally scaled. In-process cache of `tb_account_map`, ledger enums, code enums. Invalidation via a RMQ fanout or polling.

Request batching at API tier: batch concurrent client requests into a single TB `create_transfers` call up to 8,189 events or 10ms debounce. NEVER ship one transfer per request if avoidable — TB throughput depends on batching.

### 12.3 Monitoring

- TB primary: writes/sec, batch size, commit latency, LSM compaction lag, disk space.
- AMQP CDC sidecar: process liveness, lag from TB wall clock, RMQ queue depth.
- Projector: per-handler lag, last-processed timestamp, error rate.
- Outbox: rows in `pending` > 30s (alert), rows in `failed` (alert immediate).
- Reconciliation jobs: last success, any mismatches.

### 12.4 Backfill / initial load

Use `flags.imported` on accounts and transfers. Strictly-increasing user-supplied timestamps. Batches must be homogeneous (all imported or all live). Run on a fresh cluster; NEVER interleave imported and live traffic.

§△ **Historical GL migration:** Importing N years of GL history as TB transfers is mechanically straightforward but large. Decide whether to import only opening balances per period + current year transactional detail, or full history. Recommend: opening balances only, with historical detail archived in a read-only Postgres table for audit.

## 13. Security

- TB has no authentication. ALWAYS front with an authenticated API tier.
- Network: TB cluster in a private subnet; only the API tier and CDC sidecar can reach it.
- TLS: TB does not speak TLS natively. Use network-layer encryption (VPC, WireGuard). AMQP side: stunnel if AMQPS is required by the broker.
- Authorization: every write path API call validates (user, action, document) against Postgres RBAC before composing the TB batch.

## 14. Known deferrals and open issues (will require post-initial-build attention)

Collected here for traceability.

- §△-1 Per-SKU vs per-(SKU, location) value accounts (§3.2).
- §△-2 Single `qty` ledger vs per-warehouse (§3.1).
- §△-3 Counterparty pools — global vs per-entity (§3.2).
- §△-4 Self-pending reservations vs Available/Reserved pair (§5.3).
- §△-5 user_data_64 counterparty hash collisions (§3.4).
- §△-6 Backfill strategy for historical GL (§12.4).
- §△-7 Standard → WAC transition criteria (§6.2).
- §△-8 Kafka/alternative CDC (§8.1).
- §△-9 Account creation rate limiting / pre-warming (§7.3).
- §△-10 Period lock enforcement at API (§10).
- §△-11 Serial/lot reconciliation cadence (§11) — only if serial module enabled.
- §△-12 Per-WO per-op accounts for job costing (§6.1).
- §△-13 Backflush implementation (§5.4).
- §△-14 DR / zero-RPO upgrade path (§12.1).
- §△-15 Commodity pricing attribution policy — FIFO vs proportional vs all-to-variance (§17).
- §△-16 WAC recomputation policy at commodity settlement (§17).
- §△-17 Prior-period commodity settlement handling (§17).

## 15. Explicit non-decisions (design deliberately omits)

- No lot or serial accounts in TB. If the business requires lot/serial identity tracking, it lives in Postgres with aggregate balance reconciliation against TB. Do not revisit without compelling cause — it's a well-considered structural exclusion.
- No FEFO in TB. External sorted projection is the only viable path if FEFO is needed.
- No FIFO/LIFO cost layers in TB. Cost engine (external) produces the unit cost; TB records the resulting transfer.
- No reporting queries in TB. Projection or analytical store, always.
- No browser-direct TB access. API tier is mandatory.

## 16. Phasing

**Phase 1 (MVP):** Ledger 1 + USD ledger. Stock by (sku, location) Available only. Simple PO receipt, TO, SO ship. Basic GL (AR, AP, Cash, Revenue, COGS, Inventory). Outbox. CDC → Postgres projection with balance-by-sku-location and GL trial balance. No WIP, no reservations, no multi-currency, no variance.

**Phase 2:** WIP with per-op accounts, standard costing, WO lifecycle, variances. Quarantine/scrap. Reservation pending transfers. Period close mechanics.

**Phase 3:** Multi-currency, FX revaluation. Serialized/lot modules (Postgres + reconciliation). Backflush. Subledger per-counterparty accounts if needed.

**Phase 4:** Analytical projection to ClickHouse. Advanced reporting. DR evaluation. Backfill of historical GL if required.

Each phase ends with a reconciliation pass and a design review against the §14 deferral list. Expect at least §△-1, §△-4, and §△-7 to surface real decisions by end of Phase 2.

**Commodity provisional pricing (§17)** is orthogonal to the phase ordering above — introduce it in whichever phase the first provisionally-priced commodity is transacted. Mechanically it is a Phase 2 addition (requires WIP/COGS attribution); if commodities are not a business requirement, the section does not apply.

## 17. Commodity provisional pricing

For goods received physically at an estimated price, with final price settled later against a market index or quality grading. Typical in grain, scrap metal, livestock, crude, dairy, timber.

### 17.1 Additional accounts (value ledger, per currency)

| Account                     | Normal | Flag                            | Purpose                                              |
|-----------------------------|--------|---------------------------------|------------------------------------------------------|
| `AP_Unsettled`              | credit | `debits_must_not_exceed_credits` | Liability for provisionally-priced receipts         |
| `Price_Settlement_Variance` | debit  | none                            | Residual bucket when attribution is unavailable     |

`AP_Unsettled.balance` is the live exposure to commodity price risk — useful operational metric on its own.

### 17.2 Additional Postgres table

```sql
CREATE TABLE commodity_receipts (
  id                          UUID PRIMARY KEY,
  po_id                       UUID NOT NULL REFERENCES purchase_orders(id),
  po_line_id                  UUID NOT NULL,
  sku_id                      UUID NOT NULL,
  qty_received                NUMERIC NOT NULL,
  provisional_price           NUMERIC NOT NULL,
  final_price                 NUMERIC,             -- NULL until settled
  received_at                 TIMESTAMPTZ NOT NULL,
  settled_at                  TIMESTAMPTZ,
  settlement_formula          TEXT,                -- e.g. 'CBOT_CORN_MAR26 + grade_adj'
  qty_consumed_at_settlement  NUMERIC,             -- populated at settlement
  qty_on_hand_at_settlement   NUMERIC              -- populated at settlement
);
```

This is the **pricing-cohort ledger**. It does NOT track physical identity (TB model remains commingled per §15); it tracks which vendor receipts remain unpriced and how to attribute the delta at settlement.

`purchase_orders` gains `pricing_status` ENUM(`firm`, `provisional`, `settled`).

### 17.3 Provisional receipt flow

```
Linked batch:
  T1 ledger=1   credit Supplier_Pool,   debit (sku, recv_loc) Available,   amount=qty
                code=PO_RECEIPT_PROVISIONAL, u128=PO_id, u32=PO_line
  T2 ledger=ccy credit AP_Unsettled,    debit Raw_Inv_Value,               amount=qty*provisional_price
                code=PO_RECEIPT_PROVISIONAL, u128=PO_id, u32=PO_line
```

Same Postgres transaction writes the `commodity_receipts` row and sets `purchase_orders.pricing_status='provisional'`.

### 17.4 Consumption during the unsettled window

No special handling. Normal issue/ship/transfer flows apply. The commodity is usable. Cost at consumption uses whatever cost method is active (standard, WAC including the provisional price). The true-up happens at settlement; provisional-ness is a property of the **vendor obligation**, not of inventory state.

### 17.5 Settlement flow

**Step 1 — compute attribution in Postgres** (does NOT touch TB yet):

For the settling receipt, determine how its `qty_received` maps across current state:
- `Δ_onhand` = attributed portion still sitting in `Raw_Inv_Value`
- `Δ_consumed` = attributed portion that hit COGS (shipped to customers)
- `Δ_wip` = attributed portion currently in WIP (any op)
- Sum = `Δ = (final_price − provisional_price) × qty_received`

Attribution policy (§△-15) — pick one:
- **FIFO (recommended for commodities):** oldest unsettled receipt is consumed first. Requires the projector to maintain a `commodity_pool_activity` rolling log from CDC.
- **Proportional:** settling receipt's share of outflow equals its share of total inflow during the window. Simpler, less accurate.
- **All-to-variance:** book the full delta to `Price_Settlement_Variance`, no inventory/COGS/WIP true-up. Acceptable only when deltas are immaterial.

**Step 2 — post the TB settlement batch:**

```
Linked batch (signs shown for Δ > 0; flip all true-up directions for Δ < 0):

  T1 ledger=ccy credit AP_Unsettled,   debit AP,                amount=qty*provisional_price
                code=PO_SETTLEMENT, u128=PO_id
                (retire provisional liability, establish settled liability at original provisional amount)

  T2 ledger=ccy credit AP,             debit Raw_Inv_Value,     amount=Δ_onhand
                code=PRICE_TRUEUP_INVENTORY, u128=PO_id

  T3 ledger=ccy credit AP,             debit COGS,              amount=Δ_consumed
                code=PRICE_TRUEUP_COGS, u128=PO_id

  T4 ledger=ccy credit AP,             debit WIP_OpNN_Value,    amount=Δ_wip_for_op_NN
                code=PRICE_TRUEUP_WIP, u128=PO_id, u32=NN
                (one transfer per affected op)
```

Post-batch invariants:
- `AP_Unsettled` contribution for this receipt: 0
- Net `AP` credit: `qty*provisional_price + Δ = qty*final_price` (the true amount owed)
- `Raw_Inv_Value`, `COGS`, and each `WIP_OpNN_Value` trued up by its attributed share

Same Postgres transaction updates `commodity_receipts.final_price`, `settled_at`, `qty_consumed_at_settlement`, `qty_on_hand_at_settlement`, and sets `purchase_orders.pricing_status='settled'`.

### 17.6 Edge cases

**Partial settlements** (e.g., 80% locked at delivery, 20% on assay): two approaches —
- Separate receipts with separate provisional prices (clean if the contract has a clear split)
- Single receipt with the delta booked at each partial settlement (cleaner for continuous formula settlements)

**Retroactive quality adjustments:** same settlement flow; `final_price` reflects the grade adjustment; Δ may be negative.

**Formula-priced with no cap** (market on last business day of month): identical pattern, settlement waits for the formula date.

**Prior-period settlement** (§△-17): if settlement occurs in a period after the original receipt/consumption, two treatments —
- Book delta in current period as commodity settlement variance. Simpler, GAAP-acceptable when immaterial. **Default.**
- Restate prior period. Rare, reserved for material amounts.

To support either cleanly, settlement transfers MUST carry `user_data_64 = original_receipt_period_id` so the projector can separate current-period activity from prior-period settlement impact in financial reports.

**WAC recomputation** (§△-16): consumption during the unsettled window used WAC that included the provisional price. Strict WAC would retroactively recompute the pool's WAC at settlement. Default policy: **book the delta going forward via the settlement batch; do NOT retroactively recompute WAC.** Document this explicitly in accounting policy. Revisit only if auditors raise it.

### 17.7 API-level rules

- ALWAYS write the `commodity_receipts` row in the same Postgres transaction as the outbox row for the `PO_RECEIPT_PROVISIONAL` batch.
- NEVER allow a settlement batch to post without a confirmed `final_price` and a completed attribution calculation — the TB batch is constructed from the attribution, not the other way around.
- The settlement attribution job MUST be idempotent on `(commodity_receipts.id, final_price, final_price_source_id)` — re-running with the same inputs produces the same TB batch with the same transfer ids.
- Unsettled-aging report: `SELECT * FROM commodity_receipts WHERE settled_at IS NULL ORDER BY received_at` plus the sum of `AP_Unsettled` by vendor from the projector. Surface in the controller dashboard.
