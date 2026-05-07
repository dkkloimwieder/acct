# Costing Methods and Subledger Design

**A Comprehensive Reference for Inventory Costing Architecture**

*Companion to the Postgres-Based Ledger Architecture Proposal. Revised May 2026.*

---

## Contents

1. [Introduction](#1-introduction)
2. [The Architectural Principle](#2-the-architectural-principle-gl-aggregates-subledger-details)
3. [Cost Method Taxonomy](#3-cost-method-taxonomy)
4. [Standard Costing](#4-standard-costing)
5. [Moving Average (Perpetual Average) Costing](#5-moving-average-perpetual-average-costing)
6. [Weighted Average (Periodic) Costing](#6-weighted-average-periodic-costing)
7. [FIFO Costing](#7-fifo-costing)
8. [LIFO Costing](#8-lifo-costing)
9. [Lot-Based Costing](#9-lot-based-costing)
10. [Specific Identification / Serialized Costing](#10-specific-identification--serialized-costing)
11. [Group Average Costing](#11-group-average-costing)
12. [Actual Costing (Period-End Revaluation)](#12-actual-costing-period-end-revaluation)
13. [Parallel Costing Across Ledgers](#13-parallel-costing-across-ledgers)
14. [Cross-Cutting Concerns](#14-cross-cutting-concerns)
15. [Performance and Storage Strategies](#15-performance-and-storage-strategies)
16. [Implementation Phasing](#16-implementation-phasing)
17. [Synthesis](#17-synthesis)

---

## 1. Introduction

This document specifies the data model and processing logic for the eight inventory costing methods used by the four major commercial ERP systems: Standard, Moving Average (Perpetual Average), Weighted Average (Periodic), FIFO, LIFO, Lot-Based, Specific Identification (Serialized), and Group Average. For each method, the design covers the subledger tables required to support it, how operational events translate into subledger updates and GL postings, how reversals and corrections are handled within the append-only model, and how the method integrates with parallel ledgers for multi-GAAP reporting.

The document is a companion to the broader ledger architecture proposal. The `posting_lines` schema, the rules engine, and the operational patterns (outbox, idempotency, bi-temporal modeling, period close) are taken as given. What follows is the costing layer that sits on top of the GL and produces the postings that flow into it.

A working ERP must support multiple costing methods because different items in the same organization frequently warrant different methods. Raw materials with volatile prices commonly use moving average; finished goods in standardized manufacturing use standard with variance tracking; high-value items use FIFO or specific identification; consigned inventory follows the consignor's method. The platform's job is not to pick a method but to support all the common ones with consistent operational patterns and produce correct GL impact under each.

The recurring pattern that organizes the entire design: **the general ledger holds aggregate financial impact; the inventory subledger holds the per-unit, per-lot, or per-layer detail required to support the chosen costing method**. This separation is what makes the architecture scale — the GL stays compact and financially focused; the subledger absorbs the operational granularity and can be sized, indexed, and stored independently.

---

## 2. The Architectural Principle: GL Aggregates, Subledger Details

Before treating individual methods, the principle that organizes all of them must be made explicit. It is the same principle that governs accounts receivable, accounts payable, fixed assets, and any other domain where aggregate balances on the GL must be supported by detailed records below.

### 2.1 What the GL records

The general ledger records financial impact at the granularity needed for financial statements. For inventory, this means:

- The total inventory asset balance, by GL account
- The cost of goods sold for the period, by GL account
- The inventory adjustments, variances, and revaluations, by their respective accounts
- The dimensions (cost center, location, profit center, business unit) needed for management reporting and statutory disclosures

The GL does *not* need to know which specific lot, serial, or receipt layer was depleted to fulfill an issue. The GL needs to know that inventory decreased by some amount and that COGS increased by the same amount. If five different lots were depleted in one issue, the GL is satisfied with one COGS line summing them all (potentially split by GL account if different lots map to different accounts, but not by lot identifier).

### 2.2 What the subledger records

The inventory subledger records every detail the GL doesn't need but that operational and audit users do:

- Which receipt brought in which units, at what cost, with what reference to the source document
- Which receipt layers, lots, or serials are still on hand
- Which depletions consumed which receipts (the cost flow trace)
- Per-unit or per-lot status (in-stock, in-transit, sold, scrapped, returned)
- The history of every state change with timestamps and references

The subledger is what enables the operational queries that any inventory system must support: "where is serial W042?", "what's the cost layer composition of our current stock?", "for this customer return, what was the original sale's specific lot and what cost should we restore?", "show me all issues from receipt R-12345 with their realization dates and customer references."

### 2.3 The reciprocal linkage

The GL and the subledger are reciprocally linked: every subledger event references the GL posting it produced; every GL posting can be expanded to the subledger detail that justified it. This linkage is the auditability guarantee. From any GL line, the auditor can drill to the subledger detail; from any subledger event, the auditor can trace to the GL impact.

The linkage is implemented in the schema. Subledger event tables carry `posting_id` and `posting_line_seq` foreign-key columns referencing the corresponding GL line. The GL `posting_lines` table carries enough source-document references (`source_module`, `source_doc_type`, `source_doc_id`) for the reverse direction. Aggregate reconciliations periodically verify that the sum of subledger detail equals the GL control account balance — if these diverge, something is wrong.

### 2.4 Why this isn't optional

Some systems try to skip the subledger by putting all the detail directly in the GL. This is a category error and breaks at scale.

A high-volume organization with serialized inventory might generate one billion GL lines per year purely to record per-serial detail. The financial reality of that same organization might be 50 million GL lines representing actual financial events. Conflating the two means every period-close report drags through 20× more data than necessary; index sizes balloon; query performance degrades; partition management becomes unwieldy. The subledger separation reduces the GL to its actual financial size and lets the operational detail live in a structure sized appropriately.

Equally importantly, the subledger can have different operational properties than the GL: different retention (operational detail can age out faster than financial records, or vice versa depending on regulatory requirements); different storage tier (cold archives for old serial history; hot storage for recent activity); different indexes (operational queries optimize for entity lookup; financial queries optimize for account-and-period scans). One physical table cannot serve both well.

### 2.5 Append-only applies to subledgers too

The subledger inherits the append-only constraint from the GL. Subledger events are immutable once committed; state changes create new events; current state is a derived projection over the event history.

This sometimes surprises people coming from systems where the inventory subledger is mutable — a row per item per location with a `current_quantity` and `current_cost` column that gets updated as transactions occur. The append-only equivalent is an event log per item per location, with current state computed by aggregating events. The performance properties are different (event-log reads require aggregation; mutable-row reads are direct), but the correctness properties are dramatically better (event log preserves history; mutable rows lose it).

In practice, the architecture combines both: append-only events as the source of truth, materialized current-state tables as a refreshed projection. Reports read the materialized projection for speed; audits replay the event log for correctness; reconciliations verify that the projection equals the replayed sum.

---

## 3. Cost Method Taxonomy

Before examining methods individually, it helps to organize them by their underlying mechanics. The eight methods divide along three orthogonal axes.

### 3.1 Real cost versus reference cost

**Reference-cost methods** assign a predetermined value to inventory regardless of acquisition price. **Standard cost** is the only method in this family. Variances between the predetermined value and the actual purchase price are isolated in variance accounts and analyzed separately.

**Real-cost methods** assign a value derived from actual acquisition prices. The remaining seven methods (Moving Average, Weighted Average, FIFO, LIFO, Lot-based, Specific Identification, Group Average) are all real-cost; they differ in how they compute the value from actual acquisitions.

The implication for design: standard cost requires version-controlled cost master data and variance posting; real-cost methods require receipt-tracking infrastructure that traces costs from acquisition to depletion.

### 3.2 Aggregation level

**Aggregated methods** combine acquisitions into pools whose value is computed across many transactions. Moving Average, Weighted Average, and Group Average are aggregated — the cost of any specific unit is the pool's average, not the unit's individual acquisition price.

**Layered methods** keep each acquisition separate and assign costs based on which layer is depleted. FIFO and LIFO are layered — the cost of a depletion is the cost of the specific receipt layer the depletion consumes.

**Identified methods** track each unit (or lot) individually with its own cost. Specific Identification (serialized) is identified at the unit level; Lot-based is identified at the lot level. The cost of a depletion is the cost of the specific identified entity.

The implication for design: aggregated methods need a single "current cost" record per pool; layered methods need a layer table tracking each acquisition's residual quantity; identified methods need a record per unit or per lot.

### 3.3 Timing — perpetual versus periodic

**Perpetual** methods recompute or apply costs at every transaction. Moving Average is perpetual — every receipt updates the running average; every issue uses the current average. Standard is also effectively perpetual in that the standard cost is applied immediately to every transaction.

**Periodic** methods apply costs at period boundaries. Weighted Average (periodic) computes the period's weighted average at close and revalues issues that posted at running estimates during the period. FIFO and LIFO can be implemented either way: NetSuite implements them perpetually (real-time layer depletion); D365 implements them periodically (running average during the period, true-up at inventory close).

The implication for design: perpetual methods produce final costs at transaction time; periodic methods produce estimated costs at transaction time and adjust at close. Periodic methods require a revaluation process; perpetual methods do not.

### 3.4 The classification matrix

| Method | Cost basis | Aggregation | Timing |
|---|---|---|---|
| Standard | Reference | N/A | Perpetual |
| Moving Average | Real | Aggregated | Perpetual |
| Weighted Average (periodic) | Real | Aggregated | Periodic |
| FIFO | Real | Layered | Perpetual or Periodic |
| LIFO | Real | Layered | Perpetual or Periodic |
| Lot-Based | Real | Identified (lot) | Perpetual |
| Specific Identification (Serialized) | Real | Identified (unit) | Perpetual |
| Group Average | Real | Aggregated (cross-location) | Perpetual |

The classification predicts the subledger structure: aggregated methods need a small "current cost" table; layered methods need a layer table; identified methods need a unit or lot table; periodic methods additionally need a revaluation event mechanism.

---

## 4. Standard Costing

Standard costing assigns a predetermined cost to inventory items regardless of actual acquisition cost. Receipts post at the standard cost; the difference between standard and actual is captured in variance accounts. Issues post at standard. At period end, variance accounts are analyzed and may be allocated back to inventory and COGS through a periodic actual costing run (covered in Section 12).

Standard costing is the dominant method for discrete and repetitive manufacturing. It provides cost stability for production planning, isolates variance for management analysis, and simplifies transactional accounting. It is required for many costing analyses (cost rollup through bills of material, capacity utilization measurement, profitability analysis) that depend on stable per-unit costs.

### 4.1 Design overview

Standard costing requires:

1. A version-controlled standard cost per item (potentially per cost book, per legal entity)
2. Variance accounts categorized by source (purchase price variance, exchange rate variance, production variance, etc.)
3. The mechanism to capture variance at every cost-bearing event
4. A revaluation mechanism for when standards change

The subledger structure is small relative to other methods because the per-unit cost detail is embedded in the standard cost master, not in receipt layers or unit records.

### 4.2 The standard cost master

```sql
CREATE TABLE item_standard_costs (
  cost_id              BIGSERIAL PRIMARY KEY,
  product_id           BIGINT NOT NULL,
  cost_book_id         INT NOT NULL,         -- supports parallel costing per ledger
  legal_entity_id      INT NOT NULL,
  
  -- the cost
  unit_cost            NUMERIC(19,4) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- cost composition (optional but commonly required)
  material_cost        NUMERIC(19,4),
  labor_cost           NUMERIC(19,4),
  overhead_cost        NUMERIC(19,4),
  outside_processing   NUMERIC(19,4),
  
  -- effective dating
  effective_from       DATE NOT NULL,
  effective_to         DATE,
  status               VARCHAR(16) NOT NULL CHECK (status IN ('draft','approved','active','retired')),
  version              INT NOT NULL,
  
  -- audit
  created_by           INT NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  approved_by          INT,
  approved_at          TIMESTAMPTZ,
  
  UNIQUE (product_id, cost_book_id, legal_entity_id, effective_from, version)
);

CREATE INDEX ON item_standard_costs (product_id, cost_book_id, legal_entity_id) 
  WHERE status = 'active';
```

The standard cost master is itself an append-only history. New standards are added with future effective dates; old standards are retired but kept for audit. The active standard for any (product, cost book, entity, date) tuple is the one whose effective range includes the date.

Standards go through draft → approved → active → retired lifecycle, identical to the rules-engine lifecycle. Cost changes require approval; the approver must differ from the creator (segregation of duties); the audit trail records every transition.

### 4.3 Receipts under standard costing

When inventory is received, the GL posting values it at the standard cost. The difference between standard and actual is captured as Purchase Price Variance:

```
Receipt of 100 units of product P at $11 actual (standard is $10):

GL postings:
  Dr Inventory (1100)               $1000   (100 × $10 standard)
  Dr Purchase Price Variance (5510) $100    ($11 - $10) × 100
  Cr GR/IR (2150)                   $1100   (100 × $11 actual)
```

The inventory subledger records the receipt with both the standard cost (which determines the inventory value) and the actual cost (which is the basis for variance reporting):

```sql
CREATE TABLE inventory_movements (
  movement_id          BIGSERIAL PRIMARY KEY,
  
  -- identity
  product_id           BIGINT NOT NULL,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  location_id          INT,
  
  -- event
  event_type           SMALLINT NOT NULL,    -- receipt, issue, transfer, adjustment, etc.
  movement_date        DATE NOT NULL,
  quantity             NUMERIC(19,6) NOT NULL,    -- positive = increase, negative = decrease
  
  -- cost (under standard, both stored)
  standard_unit_cost   NUMERIC(19,4) NOT NULL,
  actual_unit_cost     NUMERIC(19,4),             -- for variance computation
  cost_currency        CHAR(3) NOT NULL,
  
  -- variance amounts (computed at posting time)
  ppv_amount           NUMERIC(19,4),
  
  -- traceability
  posting_id           BIGINT NOT NULL,
  posting_line_seq     INT NOT NULL,
  source_doc_type      SMALLINT,
  source_doc_id        BIGINT,
  source_doc_line      INT,
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (movement_date);
```

This `inventory_movements` table is the foundational subledger for *all* real-cost methods. It records every inventory event with quantity and cost. Different methods interpret the cost columns differently — for standard costing, both standard and actual are recorded; for FIFO, the actual cost is what matters and the layer reference is added; for moving average, only one cost (the running average at the time) is recorded.

### 4.4 Issues under standard costing

Issues post at the current standard cost. There is no variance at issue time (variance is captured at receipt time only):

```
Issue of 30 units to a sales order:

GL postings:
  Dr COGS (5000)                    $300    (30 × $10 standard)
  Cr Inventory (1100)               $300

Inventory movement:
  product_id=P, quantity=-30, standard_unit_cost=$10, actual_unit_cost=NULL
```

The issue records reference the standard at the issue date — important if standards change between receipts and issues. If product P was received at standard $10 and the standard subsequently changed to $11, an issue posted at the new standard would have a different inventory impact than the original receipt. The difference is reconciled by the standard cost revaluation process (Section 4.6).

### 4.5 Variance categories

Standard costing produces several variance categories, each with its own GL account:

| Variance | Source | Captured at |
|---|---|---|
| Purchase Price Variance (PPV) | PO price differs from standard | Receipt |
| Invoice Price Variance (IPV) | Invoice price differs from PO price | Invoice match |
| Exchange Rate Variance | FX rate at invoice differs from rate at receipt | Invoice match |
| Material Usage Variance | Production used more/less than BOM specifies | Production confirmation |
| Labor Efficiency Variance | Production took more/less time than routing | Production confirmation |
| Overhead Variance | Actual overhead differs from absorbed overhead | Production order close |
| Resale Variance | (Some retail) Sale price differs from standard markup | Sales |

Each variance is its own event class for the rules engine. Each produces a posting line to the corresponding variance account. The subledger (`inventory_movements` and others) records the variance amounts for downstream variance reporting.

### 4.6 Standard cost revaluation

When a standard cost changes, on-hand inventory must be revalued from the old standard to the new. This is itself an inventory event:

```
Product P has 500 units on hand at $10 standard.
Standard changes from $10 to $11 effective May 1.

Revaluation posting on May 1:
  Dr Inventory (1100)               $500    (500 × ($11 - $10))
  Cr Standard Cost Adjustment (5520) $500
```

Implementation: a periodic process runs at standard-change activation. For each item with an activated new standard, it queries the on-hand quantity (derived from `inventory_movements`), computes the revaluation amount, posts the adjustment, and records an `inventory_movements` row of `event_type='standard_revaluation'`.

The math is simple but the operational complexity is real: the revaluation must run *before* any transactions at the new standard occur, must respect inventory locations (different locations may have different on-hand quantities), and must produce one posting per (item, location, cost book, legal entity) combination with non-zero on-hand. For a customer with 10,000 active items in 50 locations, a standard cost rollup might generate 500,000 revaluation postings — bounded but substantial.

### 4.7 Reversal and correction patterns

Under standard costing, reversals follow the same pattern as other postings: a new posting with negated amounts referencing the original. The variance accounts also reverse, restoring the original financial position.

A complication: if a standard cost change occurred between the original posting and the reversal, the reversal still reverses at the *original* standard, not the current one. The append-only model handles this naturally — the original posting is unchanged, and the reversal references its original cost. The current standard is not relevant to the reversal.

### 4.8 Subledger view for reporting

A materialized view computes current on-hand by item and location:

```sql
CREATE MATERIALIZED VIEW inventory_current_balance AS
SELECT
  product_id,
  legal_entity_id,
  cost_book_id,
  location_id,
  SUM(quantity) AS on_hand_quantity,
  -- value at current standard (joined to active standard cost):
  SUM(quantity) * (
    SELECT unit_cost FROM item_standard_costs
    WHERE product_id = m.product_id 
      AND cost_book_id = m.cost_book_id
      AND legal_entity_id = m.legal_entity_id
      AND status = 'active'
      AND CURRENT_DATE BETWEEN effective_from AND COALESCE(effective_to, '9999-12-31')
  ) AS on_hand_value
FROM inventory_movements m
GROUP BY product_id, legal_entity_id, cost_book_id, location_id
HAVING SUM(quantity) <> 0;
```

This view is refreshed at intervals. It serves the question "how much inventory do we have, and what's it worth?" without scanning the full movement history.

The reconciliation invariant: the sum of `on_hand_value` across all locations should equal the inventory GL control account balance. Daily reconciliation catches any drift.

---

## 5. Moving Average (Perpetual Average) Costing

Moving average — also called perpetual weighted average — recomputes the unit cost after every receipt. The new cost is the weighted average of the prior on-hand value and the new receipt value. Issues post at the current moving average. There are no receipt layers, no variances (except FX and price variances against the average), and no period-end revaluation.

It is the natural choice for items where the cost varies frequently but tracking individual receipts is operationally undesirable. Many raw materials, commodities, and pass-through purchases use moving average. SAP's price control 'V' implements it; D365 calls it "Moving average"; Oracle Fusion calls it "Perpetual Average"; NetSuite calls it "Average" (its default method).

### 5.1 Design overview

Moving average requires:

1. A current "running cost" per item per cost book per location (or per pool, depending on aggregation level)
2. The mechanism to recompute the running cost on every receipt
3. The mechanism to apply the current running cost to issues

The append-only subledger pattern: instead of mutating a "current cost" cell, every recalculation is an event in a cost-recalculation log. The current cost is a derived projection over the recalculation events.

### 5.2 The cost recalculation log

```sql
CREATE TABLE moving_average_costs (
  recalc_id            BIGSERIAL PRIMARY KEY,
  
  -- identity (the pool over which average is computed)
  product_id           BIGINT NOT NULL,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  location_id          INT,                  -- NULL means "across all locations" (entity-wide pool)
  
  -- the new running average after this event
  new_unit_cost        NUMERIC(19,4) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- the on-hand quantity at this point
  on_hand_quantity     NUMERIC(19,6) NOT NULL,
  on_hand_value        NUMERIC(19,4) NOT NULL,
  
  -- what triggered the recalculation
  triggering_movement_id BIGINT NOT NULL REFERENCES inventory_movements,
  recalc_type          SMALLINT NOT NULL,    -- receipt, return_in, adjustment, revaluation
  
  -- audit
  recalc_date          DATE NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON moving_average_costs (product_id, legal_entity_id, cost_book_id, location_id, recalc_date DESC);
```

Each row records: at the time of this triggering movement, the new running average became *X*. The current running average for any (product, entity, book, location) is the most recent row.

This structure is append-only and idempotent. A receipt is processed; it updates the moving average; the new average is recorded; nothing previous is mutated. The current value is the latest row by date and ID.

### 5.3 Receipt processing

When a receipt arrives at moving average cost:

```
Pool state before receipt:
  product P, location L: on_hand = 100, unit_cost = $10, total_value = $1000

Receipt: 50 units at $12

Pool state after receipt:
  on_hand = 150
  total_value = $1000 + (50 × $12) = $1600
  new_unit_cost = $1600 / 150 = $10.67

GL postings (no variance):
  Dr Inventory (1100)        $600    (50 × $12 actual)
  Cr GR/IR (2150)            $600

Subledger writes:
  inventory_movements: receipt of +50 at unit_cost=$12
  moving_average_costs: new row with new_unit_cost=$10.67, on_hand=150, on_hand_value=$1600
```

The recalculation is mathematically straightforward but operationally important: it must happen atomically with the receipt posting (in the same database transaction) to maintain consistency. The next issue must see the new average.

### 5.4 Issue processing

Issues post at the *current* moving average:

```
Pool state before issue: unit_cost = $10.67
Issue: 30 units

GL postings:
  Dr COGS (5000)             $320.10  (30 × $10.67)
  Cr Inventory (1100)        $320.10

Subledger writes:
  inventory_movements: issue of -30 at unit_cost=$10.67 (the current running average)
  moving_average_costs: NO new row (issues don't change the average; they just deplete quantity at the current cost)
```

Issues do not produce moving-average recalculation rows. Only receipts (and a few other events covered below) do.

### 5.5 The negative inventory edge case

What happens if an issue takes inventory negative? Different systems handle this differently:

- **Reject the issue.** The simplest approach. If inventory would go negative, the application refuses the issue. Operations users must wait for receipts to catch up. SAP's default behavior leans this way (configurable per material).
- **Process the issue at current cost.** Allow the inventory to go negative. The running cost is unchanged. When the next receipt arrives, the negative balance is consumed first at the new cost; the variance between the original issue's cost and the eventual receipt's cost is captured as a variance.
- **Process the issue with cost variance.** Allow it; produce a variance posting because the negative inventory implies the issue's cost was an estimate.

The architectural recommendation: configure the policy per item (or globally) and enforce it explicitly. Negative inventory is a legitimate scenario in some industries (cross-docking, just-in-time manufacturing) and a sign of bad data in others (retail back-of-house). The platform should not impose one policy.

### 5.6 Cost adjustments and revaluations

Moving average pools can be adjusted via:

**Manual cost adjustment**: an explicit revaluation entered by an accountant, either as an absolute cost or as a delta. Posts as Dr/Cr Inventory against a Cost Adjustment account.

**Invoice price variance reabsorption**: when invoices arrive after receipts and at different prices, the variance can be absorbed back into the moving average. Process: compute the variance amount; if on-hand quantity is sufficient, absorb the full variance into the average (recompute); otherwise, absorb the on-hand-proportion and expense the rest.

**Currency revaluation**: foreign-currency-denominated inventory is revalued at period end at current rates. Adjustments post against Unrealized FX Gain/Loss.

Each of these triggers a `moving_average_costs` row with the corresponding `recalc_type`.

### 5.7 Materialized current state

The current moving-average cost is the most recent recalculation row:

```sql
CREATE MATERIALIZED VIEW current_moving_average AS
SELECT DISTINCT ON (product_id, legal_entity_id, cost_book_id, location_id)
  product_id,
  legal_entity_id,
  cost_book_id,
  location_id,
  new_unit_cost AS current_cost,
  cost_currency,
  on_hand_quantity,
  on_hand_value,
  recalc_date AS as_of_date
FROM moving_average_costs
ORDER BY product_id, legal_entity_id, cost_book_id, location_id, recalc_date DESC, recalc_id DESC;
```

For high-volume scenarios, a separate periodically-refreshed table is faster than computing this view ad hoc. The materialized view is refreshed concurrently after batches of receipts complete.

### 5.8 Concurrency consideration

Moving average has a subtle concurrency issue: two receipts processed simultaneously must serialize their recalculations. If receipt A and receipt B both read the running cost as $10 and both produce new costs, one will be wrong.

The fix is row-level locking on the pool's most recent recalculation row, or serializing all moving-average updates per pool through a single worker. In a sharded architecture, the natural unit is per-shard (per legal entity); within a shard, per-pool locking ensures correctness. The cost is reduced parallelism on hot pools — typically not a problem because receipts are typically not the highest-volume operation.

### 5.9 Comparison to FIFO/LIFO at zero variance

A subtle property: when all receipts of an item arrive at the same cost, moving average produces the same cost as FIFO and LIFO. The methods only diverge when costs change. This is sometimes useful for migrating items between methods, when costs have been stable.

---

## 6. Weighted Average (Periodic) Costing

Periodic weighted average computes a single weighted-average cost for the period at period close. During the period, issues post at a running average estimate; at close, the period's weighted average is computed from all receipts; issues are revalued from estimate to final; the difference flows to inventory adjustment accounts.

This is D365's "weighted average" method (distinct from "moving average" which is perpetual). It's how SAP's Material Ledger Actual Costing produces Periodic Unit Price (PUP). Oracle Fusion does not have a separate periodic weighted average; its Perpetual Average is moving-average style.

### 6.1 When periodic weighted average is preferred

Periodic methods reduce within-period cost volatility. If receipts vary widely during the month — some at $10, some at $15 — moving average produces issues at varying costs depending on timing. Periodic weighted average produces all issues at the period's single weighted average, which is more comparable across periods and easier to analyze.

Periodic methods also tolerate out-of-order receipt arrival better. If a receipt is recorded late (after several issues), moving average cannot retroactively adjust issues that already posted; periodic methods naturally include all of the period's receipts in the average regardless of their order.

The trade-off: issues during the period have estimated costs; financial statements are not final until close runs. This is acceptable for monthly reporting cycles but unsuitable for daily real-time reporting on COGS.

### 6.2 Design overview

Periodic weighted average requires:

1. A running estimate cost during the period (the same machinery as moving average)
2. A period-end revaluation process that computes the period's weighted average and adjusts issues
3. Storage of both estimated and final costs on each issue, for audit

The subledger structure builds on moving average but adds the period-end true-up.

### 6.3 The estimated-cost mechanism during the period

During the period, the system maintains a running estimate cost similar to moving average. Receipts update the running estimate; issues post at the running estimate; the `inventory_movements` table records the estimate as the cost at the time:

```
Receipt R1 on May 5: 100 units at $10 → running estimate = $10
Issue I1 on May 10: 30 units at running estimate $10 → COGS = $300
Receipt R2 on May 15: 50 units at $14 → running estimate = (70 × $10 + 50 × $14) / 120 = $11.67
Issue I2 on May 20: 40 units at running estimate $11.67 → COGS = $466.80
```

These postings are real GL postings, not provisional. Financial statements computed mid-period reflect these costs. The period-end true-up will adjust them.

### 6.4 The period-end weighted average computation

At period close, the system computes the period's weighted average:

```
Period weighted average = total cost of all receipts / total quantity of all receipts

For our example:
  Total cost = (100 × $10) + (50 × $14) = $1000 + $700 = $1700
  Total quantity = 100 + 50 = 150
  Period weighted average = $1700 / 150 = $11.33
```

This becomes the final per-unit cost for the period. Note: it differs from the running average that issues posted at. Issue I1 posted at $10 but should have been at $11.33; Issue I2 posted at $11.67 but should have been at $11.33. Both need adjustment.

### 6.5 The true-up postings

The period-end revaluation process generates adjustment postings for issues:

```
Issue I1 adjustment: 30 × ($11.33 - $10) = $39.90 understated COGS
  Dr COGS (5000)               $39.90
  Cr Cost Adjustment (5530)    $39.90

Issue I2 adjustment: 40 × ($11.33 - $11.67) = -$13.60 overstated COGS
  Dr Cost Adjustment (5530)    $13.60
  Cr COGS (5000)               $13.60

Inventory ending balance adjustment:
  Period beginning inventory revaluation, plus residual differences flow through ending inventory
  Dr/Cr Inventory (1100) for the residual
```

The mathematical bookkeeping is more involved than this simplified example — there's beginning inventory at the prior period's final cost, period activity, and ending inventory to reconcile — but the structure is the same: revalue issues from estimated to final, with the difference flowing to a Cost Adjustment account that nets to the correct ending inventory value.

### 6.6 The revaluation event log

```sql
CREATE TABLE periodic_cost_revaluations (
  revaluation_id       BIGSERIAL PRIMARY KEY,
  
  -- scope
  product_id           BIGINT NOT NULL,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  location_id          INT,
  fiscal_year          SMALLINT NOT NULL,
  fiscal_period        SMALLINT NOT NULL,
  
  -- the period's final cost
  period_unit_cost     NUMERIC(19,4) NOT NULL,
  period_total_cost    NUMERIC(19,4) NOT NULL,
  period_total_quantity NUMERIC(19,6) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- the cost going forward (typically same as period_unit_cost; can differ if revaluation policy specifies)
  carry_forward_cost   NUMERIC(19,4) NOT NULL,
  
  -- aggregate revaluation amount for this pool
  revaluation_amount   NUMERIC(19,4) NOT NULL,
  
  -- traceability to GL
  posting_id           BIGINT NOT NULL,
  
  -- audit
  computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  computed_by_process  VARCHAR(64) NOT NULL,
  
  UNIQUE (product_id, legal_entity_id, cost_book_id, location_id, fiscal_year, fiscal_period)
);
```

This table records, for each (item, location, period) combination, what the final period cost was and what aggregate adjustment was posted. The unique constraint prevents double-revaluation of a period.

Individual issue-level adjustments are visible in `inventory_movements` as new rows with `event_type='periodic_revaluation'` and the per-issue adjustment amount. Together, the revaluation row and the individual adjustment rows give a complete picture: aggregate posting in the GL, per-movement details in the subledger.

### 6.7 What happens if the period reopens

Periods can be reopened for adjustment. If a period that already underwent periodic revaluation is reopened and additional transactions are posted:

- The new transactions post at estimated costs based on the revaluated balances
- A subsequent revaluation run on the reopened period recomputes the period weighted average including the new transactions
- The previous revaluation's adjustments are *not* reversed; instead, a delta-revaluation captures the additional adjustment needed
- Both revaluation runs are visible in `periodic_cost_revaluations` (the unique constraint allows only one row per period; in a reopen scenario, the original is retired and a new one created with version tracking)

This is operationally complex enough that some systems disallow reopening periods after periodic revaluation. The architecture should support it but flag it as exceptional.

### 6.8 Storage and performance considerations

Periodic weighted average produces a burst of revaluation postings at period close — one per (item, location, cost book) combination with activity in the period. For an organization with 50,000 active items, 20 locations, and 2 cost books, period close generates up to 2 million revaluation postings.

This is high but bounded. The volume occurs once per period and is well-distributed across products. The architecture handles it via batched inserts (the rules engine processes revaluations in batches of thousands), partition-by-month locality (revaluation postings cluster in the closing-period's partition), and async processing (revaluation can run over hours, not requiring real-time response).

The bigger operational concern is the order of operations at period close: depreciation, accruals, allocations, intercompany eliminations, FX revaluation, inventory revaluation. Each must complete before the next; failures must be recoverable. Chapter 4.6 of the broader architecture document covers this orchestration; periodic weighted average is one of several period-end processes that share the orchestration framework.

---

## 7. FIFO Costing

FIFO (First-In, First-Out) tracks each receipt as a separate **cost layer**. When inventory is depleted, the oldest layer is consumed first; the depletion's cost is the layer's cost. If a depletion exceeds one layer's quantity, the depletion is split across multiple layers, with each portion at that layer's cost.

FIFO is the most widely used real-cost method that distinguishes between different acquisition prices. It's common in industries where physical flow approximates first-in-first-out (perishables, regulated goods with expiration, fashion/seasonal items) and in any context where current inventory should reflect recent costs (which FIFO produces because old layers are depleted first, leaving recent receipts on hand).

### 7.1 Design overview

FIFO requires:

1. A **cost layer table** that records each receipt with its original quantity, original cost, and current residual quantity
2. A **layer depletion log** that records every issue's consumption of layers
3. The depletion logic that selects layers in chronological order

The subledger is more substantial than aggregate methods because each receipt creates a layer that persists until fully depleted (which can take days, months, or years).

### 7.2 The cost layer table

```sql
CREATE TABLE cost_layers (
  layer_id             BIGSERIAL PRIMARY KEY,
  
  -- identity (the pool over which FIFO is applied)
  product_id           BIGINT NOT NULL,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  location_id          INT,
  
  -- the receipt this layer represents
  receipt_movement_id  BIGINT NOT NULL REFERENCES inventory_movements,
  receipt_date         DATE NOT NULL,
  
  -- original layer values (immutable)
  original_quantity    NUMERIC(19,6) NOT NULL,
  unit_cost            NUMERIC(19,4) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (receipt_date);

CREATE INDEX ON cost_layers (product_id, legal_entity_id, cost_book_id, location_id, receipt_date)
  WHERE receipt_date >= '2020-01-01';
```

A few things to note:

**The layer is immutable.** Once created, the original_quantity and unit_cost never change. The current residual quantity is a derived projection over depletion events, not a column on this table.

**One layer per receipt.** Even if two receipts of the same item arrive on the same day at the same cost, they create two separate layers. This is correct: depletion order follows arrival order, and combining them would lose the timing distinction.

**No "current quantity" column.** Maintaining a mutable current quantity violates append-only and creates contention (two depletions racing to update the same layer row). The current quantity is computed from depletion events.

### 7.3 The layer depletion log

```sql
CREATE TABLE cost_layer_depletions (
  depletion_id         BIGSERIAL PRIMARY KEY,
  
  -- the layer being depleted
  layer_id             BIGINT NOT NULL REFERENCES cost_layers,
  
  -- the issue movement consuming this layer
  issue_movement_id    BIGINT NOT NULL REFERENCES inventory_movements,
  issue_date           DATE NOT NULL,
  
  -- the depletion amount
  depleted_quantity    NUMERIC(19,6) NOT NULL CHECK (depleted_quantity > 0),
  unit_cost            NUMERIC(19,4) NOT NULL,        -- redundant with layer.unit_cost but speeds queries
  cost_amount          NUMERIC(19,4) NOT NULL,        -- depleted_quantity × unit_cost
  
  -- traceability
  posting_id           BIGINT NOT NULL,
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (issue_date);

CREATE INDEX ON cost_layer_depletions (layer_id);
CREATE INDEX ON cost_layer_depletions (issue_movement_id);
```

Every issue produces one or more depletion rows — one per layer it consumed. A simple issue (within one layer) produces one row; an issue that spans multiple layers produces one row per layer consumed.

### 7.4 The FIFO depletion algorithm

When an issue arrives, the system selects layers in receipt-date order until enough quantity is accumulated:

```python
def deplete_fifo(product_id, location_id, quantity_to_deplete, cost_book_id, legal_entity_id):
    layers = SELECT layer_id, unit_cost, original_quantity,
                    original_quantity - COALESCE(SUM(d.depleted_quantity), 0) AS remaining
             FROM cost_layers l
             LEFT JOIN cost_layer_depletions d ON d.layer_id = l.layer_id
             WHERE product_id = :product_id 
               AND location_id = :location_id
               AND cost_book_id = :cost_book_id
               AND legal_entity_id = :legal_entity_id
             GROUP BY layer_id, unit_cost, original_quantity, receipt_date
             HAVING (original_quantity - COALESCE(SUM(d.depleted_quantity), 0)) > 0
             ORDER BY receipt_date ASC, layer_id ASC
    
    depletions = []
    remaining = quantity_to_deplete
    for layer in layers:
        if remaining <= 0:
            break
        consume = min(remaining, layer.remaining)
        depletions.append((layer.layer_id, consume, layer.unit_cost))
        remaining -= consume
    
    if remaining > 0:
        # negative inventory case
        handle_negative_inventory(remaining)
    
    return depletions
```

The query computes remaining quantity per layer as `original - SUM(depleted)`, sorts by receipt date, and walks the layers consuming from each. Performance depends on the number of open layers per pool — typically small (dozens to hundreds for active items) but can grow if old layers don't fully deplete.

### 7.5 Issue processing

A FIFO issue produces:

1. One `inventory_movements` row recording the total issue (negative quantity, weighted-average cost across the depleted layers)
2. One `cost_layer_depletions` row per layer consumed
3. One GL posting with one or more lines depending on whether the layers map to different GL accounts

Concrete example:

```
Pool state before issue:
  Layer L1 (May 1): 100 units @ $10, remaining 100
  Layer L2 (May 15): 50 units @ $14, remaining 50

Issue: 120 units (May 20)

Depletion plan:
  L1: consume 100 @ $10 = $1000
  L2: consume 20 @ $14 = $280
  Total: 120 units, $1280

GL posting:
  Dr COGS (5000)           $1280
  Cr Inventory (1100)      $1280

Subledger writes:
  inventory_movements: issue of -120, cost $1280, weighted avg $10.67
  cost_layer_depletions: 
    - L1: depleted 100, $1000
    - L2: depleted 20, $280
```

The issue's movement record has the total amount; the layer depletion records show *how* that amount was computed. Both are needed: the movement for inventory and quantity tracking; the depletion for cost-flow audit.

### 7.6 Returns and reversals

Returns and reversals are FIFO's most operationally complex aspect. There are several patterns, each with trade-offs:

**Customer return (sale reversal)**: The customer returns goods that were issued from a specific layer. Three options:
- *Reinstate the original layer*: Look up which layers the original sale depleted; create reverse-depletion records that restore those layers' remaining quantity. The returned units go back to their original cost basis. Most accurate but operationally complex.
- *Create a new layer*: The return creates a new cost layer at the original sale's cost. The pool grows by one new layer. Simpler operationally, slightly inaccurate (the layer-creation date is the return date, not the original receipt date).
- *Aggregate to current cost*: The return adds quantity at the current weighted average. Loses cost-flow accuracy. Only acceptable for low-value items.

The recommended pattern is reinstating the original layer when the original depletion record is identifiable (typical for invoiced sales with traceable cost flow). Creating a new layer is the fallback when traceability is incomplete.

**Vendor return (receipt reversal)**: A receipt is returned to the vendor before being issued. The original layer is reduced (or eliminated if fully reversed). Handled by:
- A negative inventory_movement (the return)
- A `cost_layer_depletions` row with negative quantity, depleting the original layer "as if" the receipt were partially consumed

A negative-quantity depletion row is unusual but maintains the invariant that "residual = original - sum(depletions)". The architecture has to permit negative depletions for this case.

### 7.7 The "perpetual vs periodic" FIFO choice

NetSuite implements FIFO perpetually: every issue immediately deplets specific layers; the cost is final at the time of the issue. D365 implements FIFO periodically: issues post at running average during the period; at inventory close, the system reconciles to actual FIFO depletion order and adjusts.

The data model is the same; only the timing differs. In the periodic implementation, layer depletions are computed at close, and adjustment postings true up issues from running average to FIFO. In the perpetual implementation, depletions are computed at issue time.

The architectural recommendation is **perpetual** for new builds: it produces final costs immediately, makes financial statements current at any time, and avoids the period-close revaluation complexity. The arguments for periodic — handling out-of-order arrivals, smoothing within-period volatility — are mitigated by good operational discipline and by the fact that perpetual systems can handle late-arriving receipts via the layer-creation date being the actual receipt date, not the entry date.

### 7.8 The current layer balance projection

For reporting, the "current layer balance" is computed:

```sql
CREATE MATERIALIZED VIEW current_cost_layers AS
SELECT
  l.layer_id,
  l.product_id,
  l.legal_entity_id,
  l.cost_book_id,
  l.location_id,
  l.receipt_date,
  l.unit_cost,
  l.cost_currency,
  l.original_quantity,
  COALESCE(SUM(d.depleted_quantity), 0) AS depleted_quantity,
  l.original_quantity - COALESCE(SUM(d.depleted_quantity), 0) AS remaining_quantity,
  (l.original_quantity - COALESCE(SUM(d.depleted_quantity), 0)) * l.unit_cost AS remaining_value
FROM cost_layers l
LEFT JOIN cost_layer_depletions d ON d.layer_id = l.layer_id
GROUP BY l.layer_id
HAVING l.original_quantity - COALESCE(SUM(d.depleted_quantity), 0) > 0;
```

This view shows the current state of all open layers: which receipts haven't been fully depleted, how much remains, and the value. Refreshed after batches of issues complete.

The pool's total on-hand inventory is the sum of `remaining_quantity` across its layers; the pool's total inventory value is the sum of `remaining_value`. The reconciliation invariant: this sum equals the GL inventory balance for that pool.

### 7.9 Performance considerations

Layer count grows over time. For active items with constant turnover, old layers fully deplete and the count stabilizes. For slow-moving items or items with intermittent purchases, layers accumulate. A 10-year-old layer with 5 units remaining is operationally annoying — every depletion query has to consider it.

Mitigations:

- **Layer aging policy**: layers below a residual threshold (e.g., 1% of original quantity, or under some absolute amount) are administratively closed; the residual goes to a write-off account. This is operational hygiene, not a fundamental architectural change.
- **Partitioning by receipt_date**: old partitions (with old layers) are queried less frequently; the active layers are concentrated in recent partitions, where the bulk of queries hit.
- **Indexed materialized current state**: rather than computing remaining quantity on every depletion, maintain a `current_cost_layers` table refreshed after each batch of issues. This is read-mostly with bounded staleness.

For high-volume environments — millions of issues per day across millions of layers — the depletion algorithm's performance is the architecture's bottleneck. Database-side functions, careful indexing, and per-pool worker assignment (so all depletions for a pool serialize through one worker) are necessary.

---

## 8. LIFO Costing

LIFO (Last-In, First-Out) is the mirror image of FIFO: depletion consumes the most recent layers first. Inventory on hand reflects oldest costs; COGS reflects most-recent costs.

LIFO is permitted under U.S. GAAP but not under IFRS. Most non-U.S. jurisdictions disallow it. In U.S. inflationary environments, LIFO understates inventory (oldest, lower-cost layers remain) and overstates COGS (recent, higher-cost layers deplete first), reducing taxable income — which is why some U.S. companies elect LIFO for tax purposes. The IRS LIFO Conformity Rule requires that companies using LIFO for tax also use it for financial reporting.

### 8.1 Design overview

LIFO uses **the same data structures as FIFO**. The only difference is the ORDER BY clause in the depletion algorithm: descending by receipt date instead of ascending.

```python
# FIFO depletion query
ORDER BY receipt_date ASC, layer_id ASC

# LIFO depletion query  
ORDER BY receipt_date DESC, layer_id DESC
```

Everything else — the `cost_layers` table, the `cost_layer_depletions` table, the materialized views — is identical to FIFO. A single subledger structure supports both methods; the configuration property on the item determines which depletion order applies.

This is one of the few places in the architecture where a single mechanism serves two methods cleanly. The data model captures the underlying reality (receipts as layers); the methods differ only in their consumption strategy.

### 8.2 LIFO-specific operational issues

While the data structure is the same, LIFO has operational characteristics worth noting:

**LIFO Reserve**: Companies using LIFO for tax must report the difference between LIFO inventory and what FIFO inventory would have been. The "LIFO reserve" is the cumulative difference, disclosed in financial statement footnotes. The architecture supports this naturally: maintain two parallel cost books (one LIFO, one FIFO) and compute the reserve as the difference between their inventory balances.

**LIFO Layers ("LIFO pools")**: U.S. tax law allows LIFO to be applied to pools of similar items, not just individual items. A pool aggregates many similar items into a single LIFO computation. This is more complex to implement: instead of layers per item, layers per pool, with depletion computed at pool level and apportioned back to items for inventory reporting. The architecture supports this via a pool concept on the cost layer table — `cost_layers.product_id` becomes optional, replaced by `pool_id` for pooled-LIFO scenarios.

**LIFO Liquidation**: When inventory drops below historical levels, old (low-cost) layers are depleted, producing unusual gains. This is a financial reporting issue, not an architectural one — the architecture posts the liquidation correctly; the disclosure is downstream.

**Current cost vs. LIFO cost adjustments**: Some tax LIFO methods compute current-year layers using a price index applied to the prior year's costs, rather than using actual receipt costs. This is the "Dollar-Value LIFO" method and is implemented via a separate cost-book-level computation rather than receipt-level layers. The standard layer-based architecture above is "specific-goods LIFO"; Dollar-Value LIFO is a distinct, less-common variant that requires its own design.

### 8.3 LIFO date

D365 has a separate "LIFO date" method that prioritizes by date in a specific way (the latest date with available inventory, rather than the latest receipt strictly). This is a minor variant and is implemented as a configuration option on the LIFO depletion algorithm rather than a fundamentally different method.

### 8.4 Should a new system support LIFO?

LIFO is decreasing in use. Most jurisdictions don't permit it. Even in the U.S., Dollar-Value LIFO is more common than specific-goods LIFO, and many companies abandoned LIFO when book-tax differences became operationally painful. A new ERP design has a legitimate option to defer LIFO support — implement specific-goods LIFO via the FIFO data structures (just changing the depletion order), defer Dollar-Value LIFO to later phases, and accept that customers requiring extensive LIFO will need a more specialized system.

The recommendation: build the layered structures generally enough that LIFO falls out for free (specific-goods); document Dollar-Value LIFO as a phase 2+ feature; don't engineer special LIFO logic upfront beyond the depletion-order configuration.

---

## 9. Lot-Based Costing

Lot-based costing tracks each lot (batch) of received inventory with its own cost. When the lot is depleted, the lot's cost is the depletion's cost. Lots are typically defined by manufacturing batches, supplier shipments, or regulatory tracking requirements (pharmaceuticals, food).

Lot-based is conceptually similar to FIFO with each lot being its own layer, but with a key operational difference: lot identity is meaningful. A depletion is from a *specific* lot (chosen by the user or by allocation rules), not from the chronologically-oldest receipt. Lots may be depleted out of receipt order based on expiration dates, customer allocation rules, regulatory requirements, or operational decisions.

### 9.1 Design overview

Lot-based costing requires:

1. A **lot table** that records each lot's cost and metadata
2. A **lot event log** for receipt, partial issue, and full depletion
3. The mechanism for issues to specify which lot is being depleted

The structure parallels FIFO/LIFO but with explicit lot identity rather than implicit layer-by-receipt-date.

### 9.2 The lot table

```sql
CREATE TABLE inventory_lots (
  lot_id               BIGSERIAL PRIMARY KEY,
  
  -- identity
  lot_number           VARCHAR(64) NOT NULL,
  product_id           BIGINT NOT NULL,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  
  -- the receipt that created this lot
  receipt_movement_id  BIGINT NOT NULL REFERENCES inventory_movements,
  receipt_date         DATE NOT NULL,
  supplier_lot_number  VARCHAR(64),          -- vendor's lot identifier if different
  
  -- cost (immutable once set)
  original_quantity    NUMERIC(19,6) NOT NULL,
  unit_cost            NUMERIC(19,4) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- lot metadata
  manufacture_date     DATE,
  expiration_date      DATE,
  quality_status       VARCHAR(16),          -- pending, approved, rejected, expired
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  UNIQUE (lot_number, product_id, legal_entity_id)
) PARTITION BY RANGE (receipt_date);
```

The lot is identified by `(lot_number, product_id, legal_entity_id)` — lot numbers are scoped to product and entity. Two products can have the same lot number; a lot exists only in one entity.

### 9.3 The lot event log

```sql
CREATE TABLE inventory_lot_events (
  event_id             BIGSERIAL PRIMARY KEY,
  
  -- identity
  lot_id               BIGINT NOT NULL REFERENCES inventory_lots,
  event_date           DATE NOT NULL,
  event_type           SMALLINT NOT NULL,    -- receipt, issue, transfer, hold, release, expiration_writeoff, ...
  
  -- quantities
  quantity_change      NUMERIC(19,6) NOT NULL,    -- positive for receipt/release, negative for issue/hold
  
  -- traceability
  movement_id          BIGINT NOT NULL REFERENCES inventory_movements,
  posting_id           BIGINT NOT NULL,
  source_doc_type      SMALLINT,
  source_doc_id        BIGINT,
  
  -- location (lots can move between warehouses while retaining identity)
  location_id_from     INT,
  location_id_to       INT,
  
  -- status changes
  new_quality_status   VARCHAR(16),
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON inventory_lot_events (lot_id, event_date);
CREATE INDEX ON inventory_lot_events (movement_id);
```

The event log captures every state change to a lot: receipts (creating the lot, with positive quantity), issues (depleting the lot, with negative quantity), transfers (moving the lot to a different location, neutral), holds and releases (changing the quality status), and write-offs (e.g., expired pharmaceuticals).

### 9.4 Issue processing

A lot-based issue must specify which lot:

```
Issue: 50 units of product P from lot L-12345 (May 20)

Lot L-12345 state before:
  Original 100 units at $12, remaining 80 (20 previously issued)

After issue:
  Original 100, depleted 70, remaining 30

GL postings:
  Dr COGS (5000)               $600    (50 × $12 from this lot)
  Cr Inventory (1100)          $600

Subledger writes:
  inventory_movements: issue of -50, unit_cost=$12, lot_number='L-12345'
  inventory_lot_events: lot_id=12345, quantity_change=-50, event_type='issue'
```

The lot's cost is fixed; the depletion uses that cost regardless of when the issue happens or how many other lots are available.

### 9.5 Lot allocation strategies

The system needs a strategy for selecting lots when an issue arrives without a specified lot:

- **Earliest expiration first (FEFO)**: Select the lot expiring soonest. Common for perishables and pharmaceuticals.
- **First-in-first-out (FIFO by receipt date)**: Select the oldest lot. Common when expiration isn't a concern but rotation matters.
- **Quality-status priority**: Among approved lots, apply secondary sort.
- **Customer-specific allocation**: Some customers receive specific lots (e.g., pharmaceutical traceability requires lot-level customer linkage).

Allocation strategy is configurable per item or item group. Within the architecture, the strategy is a function that, given an issue context (product, location, quantity), returns the lot(s) to deplete. The function is part of the inventory management module, separate from the cost engine.

### 9.6 Lot expiration and write-offs

Lots may expire before being depleted. The lot's residual quantity is written off:

```
Lot L-99999: 100 received at $15, 30 issued, 70 remaining. Expires June 30.
On July 1, the system identifies expired lots and posts write-offs:

GL posting:
  Dr Inventory Write-off (5550)    $1050  (70 × $15)
  Cr Inventory (1100)              $1050

Subledger writes:
  inventory_movements: write-off of -70 from lot L-99999
  inventory_lot_events: lot_id=99999, quantity_change=-70, event_type='expiration_writeoff'
```

The write-off process runs periodically (typically nightly), identifies lots past their expiration date with non-zero remaining quantity, and posts the write-offs. The architecture supports this via a scheduled job that operates on the lot subledger.

### 9.7 Lot transfers

Lots can move between locations while retaining identity. A lot transfer:

```
Transfer 50 units of lot L-12345 from warehouse W1 to warehouse W2 (May 25):

GL postings (typically internal — no external GL impact unless plants have different valuations):
  Dr Inventory at W2 (1100)        $600
  Cr Inventory at W1 (1100)        $600

Subledger writes:
  inventory_movements: 
    - issue of -50 at W1, lot L-12345
    - receipt of +50 at W2, lot L-12345
  inventory_lot_events: lot_id=12345, event_type='transfer', 
    location_from=W1, location_to=W2, quantity_change=0
```

A transfer is two movements (out from source, in to destination) but one lot event (the lot moves; its identity is preserved). The transfer event has a quantity_change of 0 because the lot's total quantity didn't change.

### 9.8 The lot status changes

Beyond quantity and location, lots have status (pending quality inspection, approved, rejected, on hold). Status changes are events:

```
Lot L-12345 was received pending quality inspection. After QA passes on May 16:

Lot status change event:
  inventory_lot_events: lot_id=12345, event_type='status_change',
    new_quality_status='approved', quantity_change=0
```

Status events have no GL impact (no posting_id, or a NULL placeholder) but are recorded for operational and regulatory traceability.

### 9.9 Volume implications

For lot-managed items, every receipt creates one lot row. For high-volume items (e.g., a pharmaceutical company processing 1,000 lot receipts per day), this is 365,000 lot rows per year per facility. The lot event log is larger — every receipt, issue, transfer, status change, and write-off generates an event.

These volumes are tractable but require attention. Partitioning by receipt date (for lots) and event date (for events) keeps recent activity hot while old, depleted lots can move to archived storage. A lot that's fully depleted, expired, or written off can be marked terminal and its events archived after a retention period.

### 9.10 Industry-specific extensions

Pharmaceutical, medical device, and food industries have specific lot-tracking requirements:

- **Track-and-trace** (e.g., DSCSA in U.S. pharmaceuticals): lots must be traceable from manufacturer to dispenser, with every transfer recorded
- **Recall management**: when a lot is recalled, the system must identify all customers who received it and trigger return processes
- **Genealogy**: in manufacturing, output lots must be linked to the input lots they were produced from, allowing forward and backward traceability

These extensions add tables (lot_genealogy, lot_recall_events, customer_lot_shipments) but build on the same foundational lot subledger. The cost engine doesn't change; the operational layer wraps it with industry-specific functionality.

---

## 10. Specific Identification / Serialized Costing

Specific identification tracks each individual unit of inventory with its own cost. Receipts create unit records; issues specify the unit being shipped; the unit's cost flows directly to COGS.

This is the most granular costing method. It's required for high-value items (vehicles, real estate, jewelry, fine art), regulated items (firearms, certain pharmaceuticals), and any context where individual units have meaningfully different costs (custom-manufactured products, items with significant per-unit variance).

As established earlier, **serialized costing is conceptually lot-based costing with lot size 1**. The structures are similar but the volume implications differ substantially: lots typically contain many units; serials are one-to-one with units.

### 10.1 Design overview

Serialized costing requires:

1. A **unit table** that records each individual unit with its identity, cost, and current state
2. A **unit event log** for receipt, transfer, status change, issue, and return
3. The mechanism for issues to specify which serial(s) are being shipped

The architecture from Section 9 (lot-based) carries over directly. The differences are scale and the absence of "partial depletion" — a serial is either consumed entirely or not at all.

### 10.2 The serialized unit table

The lot table from Section 9.2 can serve serialized units with minor adjustments:

```sql
CREATE TABLE inventory_units (
  unit_id              BIGSERIAL PRIMARY KEY,
  
  -- identity
  serial_number        VARCHAR(64) NOT NULL,
  product_id           BIGINT NOT NULL,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  
  -- the receipt that created this unit
  receipt_movement_id  BIGINT NOT NULL REFERENCES inventory_movements,
  receipt_date         DATE NOT NULL,
  
  -- cost (immutable once set)
  unit_cost            NUMERIC(19,4) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- unit metadata
  manufacture_date     DATE,
  expiration_date      DATE,
  warranty_start_date  DATE,
  warranty_end_date    DATE,
  
  -- traceability extensions for regulated industries
  parent_lot_number    VARCHAR(64),         -- parent lot if serial within a lot
  supplier_serial      VARCHAR(64),         -- vendor's serial if different from internal
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  UNIQUE (serial_number, product_id, legal_entity_id)
) PARTITION BY RANGE (receipt_date);
```

Note that quantity is not stored — a unit is implicitly quantity 1. Whether the unit is currently in stock is derived from its event history.

### 10.3 The unit event log

```sql
CREATE TABLE inventory_unit_events (
  event_id             BIGSERIAL PRIMARY KEY,
  
  -- identity
  unit_id              BIGINT NOT NULL REFERENCES inventory_units,
  event_date           DATE NOT NULL,
  event_type           SMALLINT NOT NULL,    -- received, issued, transferred, returned, scrapped, status_change, ...
  
  -- traceability
  movement_id          BIGINT NOT NULL REFERENCES inventory_movements,
  posting_id           BIGINT,                -- NULL for status-only events with no GL impact
  source_doc_type      SMALLINT,
  source_doc_id        BIGINT,
  
  -- location and status
  location_id_from     INT,
  location_id_to       INT,
  new_status           VARCHAR(16),          -- in_stock, in_transit, sold, returned, scrapped, hold
  
  -- customer linkage (for sold serials)
  customer_id          BIGINT,
  customer_doc_id      BIGINT,
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (event_date);

CREATE INDEX ON inventory_unit_events (unit_id, event_date);
CREATE INDEX ON inventory_unit_events (movement_id);
CREATE INDEX ON inventory_unit_events (customer_id) WHERE customer_id IS NOT NULL;
```

Each unit's lifecycle is captured by its sequence of events. The current state — where is it, is it in stock, who owns it — is derived from the most recent event of each type.

### 10.4 Receipt processing

A receipt of N serialized units creates N unit rows and N event rows:

```
Receipt of 5 vehicles, each at unique serial, all at $25,000:

GL posting (aggregate):
  Dr Inventory (1100)              $125,000   (5 × $25,000)
  Cr GR/IR (2150)                  $125,000

Subledger writes:
  inventory_movements: 1 row, quantity=+5, total cost $125,000
  inventory_units: 5 rows (one per VIN)
  inventory_unit_events: 5 rows (event_type='received', one per unit)
```

The GL has one posting line per account (no per-unit GL detail). The movement table has one aggregate row. The unit and event tables have one row per serialized unit. The volumes are linear in the unit count.

### 10.5 Issue processing

An issue of a specific unit:

```
Sale of vehicle VIN-1234 to customer C42 (May 30):

GL posting:
  Dr COGS (5000)                   $25,000
  Cr Inventory (1100)              $25,000

Subledger writes:
  inventory_movements: 1 row, quantity=-1, unit_cost=$25,000
  inventory_unit_events: 1 row, event_type='issued', unit_id=(VIN-1234), customer_id=C42
```

The unit's cost flows directly to COGS — no FIFO/LIFO calculation, no average. The serial number ties the COGS posting back to the specific unit and customer for full audit traceability.

### 10.6 Transfers and status changes

These follow the lot pattern from Section 9. Transferring a unit between warehouses creates two movement rows (out and in) and one unit event row (the unit moved). Status changes (e.g., placing a unit on hold for warranty repair) create unit events without movement rows or GL postings.

### 10.7 Returns

Customer returns of serialized items are operationally rich. The architecture supports:

- **Identifiable return**: customer returns a specific serial that was previously sold. Look up the original sale event; reverse it; restore the unit to in-stock status. The unit's cost remains its original cost. This is the cleanest pattern.
- **Unidentified return**: customer returns an item without specifying serial. The system must guess (or refuse). For high-value items, force serial identification; for low-value, pick any available serial of the same product or aggregate to a single anonymous unit.

Both patterns are events on the unit (or lot) and movements on the inventory subledger. The architecture handles them via specific event_type values that the rules engine recognizes.

### 10.8 Volume strategies

Serialized inventory at high volume is the architecture's most demanding scenario. The mitigation strategies build on those for lots, with additional emphasis:

**Time-based partitioning of unit and event tables** is mandatory. Hot partitions (current period) versus cold partitions (terminated units' events) gives most of the storage savings.

**Status-based archival**. A unit in 'sold' status for more than 30 days, or 'scrapped' for any duration, can have its events moved to an archive table. The archive supports audit queries but isn't on the OLTP critical path.

**Aggregation for terminated units**. A unit that has gone through its full lifecycle (received → issued → settled) can collapse to a single archive row containing the lifecycle summary (receipt date, issue date, customer, total cost, GL postings) without the per-event detail. This loses some information but vastly reduces row count.

**Columnar storage on the analytics replica**. The unit event log compresses dramatically in column-store format (most fields have low cardinality: event types, locations, statuses). A columnar replica handles audit and analytical queries; the OLTP primary handles real-time operations.

**Explicit retention policies**. Operational records may need 7-10 year retention for audit; some industries (medical devices, pharmaceuticals) require longer. Beyond that, archives can move to cold object storage and be loaded on demand for the rare regulatory query.

### 10.9 Mass-serialized scenarios

Some items are serialized but at very high volumes — semiconductors, individual pharmaceutical pills under DSCSA serialization, electronic components. These cases stress the architecture beyond normal limits:

- 100 million serialized units per day means 100M unit rows and 200M+ event rows daily
- 5-year retention means 200B+ rows in the unit event log
- Even with partitioning and columnar storage, this is operationally challenging

For these scenarios, the architecture has two responses:

1. **Selectively serialize**. Not every unit needs cost-level serialization. For semiconductors, the cost may be tracked per wafer (lot of ~1000 chips); the chip-level serial is for tracking only, not costing. The cost engine treats the wafer as the unit; the chip serial is operational metadata. This is the "serialized identification, lot-based costing" pattern.
2. **Use specialized infrastructure for ultra-high-volume serialization**. A general-purpose ledger with subledger augmentation isn't ideal for 100M serializations/day. Industries with this requirement typically use specialized track-and-trace platforms that integrate with the ERP rather than living in it. The ERP receives aggregate cost flows; the track-and-trace handles unit-level visibility.

The architectural recommendation: support serialization at moderate scale (millions of units, billions of events at 5-year horizon) within the standard subledger; for ultra-high-volume serialization, integrate with specialized platforms rather than scaling the ERP database.

### 10.10 The relationship between lots and serials

A serial within a lot is a common pattern: the unit has its own serial, but it was received as part of a lot, and may be tracked at both levels. The architecture supports this via the `parent_lot_number` field on `inventory_units`. Reports can aggregate to lot level (for recall management) or drill to serial level (for individual unit tracking).

When parent lot tracking is used, both the lot and the unit get records. The cost is at the unit level (since serialization implies per-unit cost variation, even within a lot). The lot record captures the receipt batch; the unit records capture each individual unit's identity and cost.

---

## 11. Group Average Costing

Group Average is NetSuite's distinct contribution to costing methods. It computes a single average cost across multiple defined locations (a "group"). When inventory is received in any location of the group, the group's running average updates; when inventory is issued from any location, the issue uses the group average regardless of which specific location it came from.

This handles a common business pattern: a company with multiple warehouses where inventory is fungible across locations. Without group average, each location maintains its own moving average, and transferring inventory between locations may post differential cost adjustments. With group average, all locations share a single cost; transfers are cost-neutral; the company's COGS is consistent regardless of which location fulfilled an order.

### 11.1 Design overview

Group Average is a generalization of moving average where the pool spans multiple locations. The data structures are similar:

1. A **group definition** specifying which locations participate
2. A **group running cost** maintained per group per item
3. The standard inventory_movements with the group's cost applied

### 11.2 The group definition

```sql
CREATE TABLE cost_groups (
  group_id             BIGSERIAL PRIMARY KEY,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  group_name           VARCHAR(128) NOT NULL,
  group_code           VARCHAR(32) NOT NULL,
  description          TEXT,
  
  -- audit
  effective_from       DATE NOT NULL,
  effective_to         DATE,
  status               VARCHAR(16) NOT NULL CHECK (status IN ('active','retired')),
  
  UNIQUE (legal_entity_id, cost_book_id, group_code)
);

CREATE TABLE cost_group_locations (
  group_id             BIGINT NOT NULL REFERENCES cost_groups,
  location_id          INT NOT NULL,
  effective_from       DATE NOT NULL,
  effective_to         DATE,
  PRIMARY KEY (group_id, location_id, effective_from)
);
```

A location belongs to a group (or to no group, in which case it costs independently). A group can include any subset of locations within a legal entity. Group membership can change over time (effective dating), though changes are operationally complex (require revaluation when locations join or leave).

### 11.3 The group running cost log

```sql
CREATE TABLE group_average_costs (
  recalc_id            BIGSERIAL PRIMARY KEY,
  
  -- identity
  product_id           BIGINT NOT NULL,
  group_id             BIGINT NOT NULL REFERENCES cost_groups,
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  
  -- the new running average after this event
  new_unit_cost        NUMERIC(19,4) NOT NULL,
  cost_currency        CHAR(3) NOT NULL,
  
  -- the on-hand state at this point
  total_on_hand_quantity NUMERIC(19,6) NOT NULL,    -- summed across all locations in group
  total_on_hand_value    NUMERIC(19,4) NOT NULL,
  
  -- what triggered the recalculation
  triggering_movement_id BIGINT NOT NULL REFERENCES inventory_movements,
  recalc_type          SMALLINT NOT NULL,
  recalc_date          DATE NOT NULL,
  
  -- audit
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

This is identical in structure to `moving_average_costs` from Section 5 but with `group_id` replacing `location_id`. The cost is at the group level, not per-location.

### 11.4 Receipt processing

A receipt to any location in the group updates the group's running cost:

```
Group G1 contains locations W1, W2, W3.
Group state before receipt: total on-hand 200 units across all locations, unit cost $10, total value $2000.

Receipt of 50 units at W2 at $14:

Group state after:
  total on-hand = 250
  total value = $2000 + (50 × $14) = $2700
  new unit cost = $2700 / 250 = $10.80

GL posting:
  Dr Inventory at W2 (1100)        $700  (50 × $14)
  Cr GR/IR (2150)                  $700

Subledger writes:
  inventory_movements: receipt at W2, +50 at unit_cost=$14
  group_average_costs: new row with new_unit_cost=$10.80
```

Note that the GL inventory account may still be subdivided by location (1100 might be parent; 1100-W1, 1100-W2, 1100-W3 might be sub-accounts). The group average affects the unit cost; the GL still tracks per-location balance. This is a representation choice — alternatively, the GL holds a single consolidated inventory account with location as a dimension, and the per-location breakdown is in the subledger only.

### 11.5 Issue processing

An issue from any location uses the group's current cost:

```
Group G1's current unit cost is $10.80.
Issue of 30 units from W1:

GL posting:
  Dr COGS (5000)                   $324    (30 × $10.80)
  Cr Inventory at W1 (1100)        $324

Subledger writes:
  inventory_movements: issue at W1, -30 at unit_cost=$10.80
  group_average_costs: NO new row (issues don't change average)
```

The issue's cost is the group's average, not W1's specific cost. From W1's perspective, COGS is $324 — the same as if the same unit had been issued from W2 or W3.

### 11.6 Inter-location transfers within a group

Transfers between locations *within* the group are cost-neutral:

```
Transfer 25 units from W1 to W3 (both in group G1):

GL postings:
  Dr Inventory at W3 (1100)        $270    (25 × $10.80)
  Cr Inventory at W1 (1100)        $270

Subledger writes:
  inventory_movements: 
    - issue at W1, -25 at $10.80
    - receipt at W3, +25 at $10.80
  group_average_costs: NO new row (intra-group transfer is cost-neutral; running average unchanged)
```

The total group on-hand quantity is unchanged; the total group value is unchanged; the running average is unchanged. This is the operational benefit of group average: transfers don't trigger cost variances.

### 11.7 Inter-group transfers

Transfers between locations in *different* groups (or from a group to a non-group location) are not cost-neutral. They behave as: an issue from the source group at the source group's average, and a receipt at the destination group at the same cost (which then triggers the destination group's average recalculation).

```
Transfer 25 units from W1 (in group G1, avg $10.80) to W4 (in group G2, avg $11.50):

Source perspective:
  inventory_movements: issue at W1, -25 at $10.80
  group_average_costs: NO recalculation (issue doesn't change avg)

Destination perspective:
  inventory_movements: receipt at W4, +25 at $10.80
  group_average_costs: G2 recalculates with new receipt at $10.80, producing new G2 average

GL posting:
  Dr Inventory at W4 (1100)        $270
  Cr Inventory at W1 (1100)        $270
```

Note that the destination group's average changes because of the inbound receipt at $10.80. The cost basis is preserved across the transfer; the destination group's average drifts toward the source group's average proportionally.

### 11.8 Group definition changes

Adding or removing locations from a group is operationally complex because it requires revaluation:

- **Adding a location to a group**: the location's existing inventory must be revalued from its prior cost basis to the group's average. The difference posts as a Cost Adjustment.
- **Removing a location from a group**: the location's existing inventory keeps the group's last cost basis but going forward will have its own (or be added to a different group). No immediate revaluation.

These transitions are uncommon but must be supported. The architecture handles them via explicit revaluation events triggered by group membership changes.

### 11.9 Why this method matters

Group Average is unique to NetSuite among the four major ERPs, but the concept is broadly useful. Multi-location distributors, retailers with regional warehouses, and contract manufacturers with multi-site operations all benefit from group-level costing. Without it, every internal transfer is a cost event; with it, transfers within the group are operational moves with no financial impact.

For a custom-built ledger system, supporting Group Average is a moderate complexity addition over Moving Average. The data structures parallel; the difference is the pool definition and cross-location aggregation. It's worth supporting if the customer base includes multi-location operations.

---

## 12. Actual Costing (Period-End Revaluation)

Actual costing is not a separate method but a *layer* applied to other methods. It revalues inventory and COGS from estimated costs (used during the period) to actual costs (computed at period close from all of the period's transactions). SAP's Material Ledger Actual Costing is the most sophisticated implementation; D365's inventory close performs similar functions for periodic methods; Oracle Fusion's actual costing relies on perpetual mechanisms with adjustment postings.

The motivation: during a period, costs are best estimates (running averages, current standards). At period close, all of the period's actual purchase prices, exchange rates, freight, and price variances are known. The system can compute what costs *should have been* and adjust the period's postings to reflect that.

### 12.1 The general pattern

Actual costing applies the following pattern at period close:

1. **Capture all variances** during the period in dedicated variance accounts (PPV, IPV, FX variance, freight variance, etc.)
2. **At period close, allocate variances** to the inventory and consumption that caused them, weighted by quantity flow
3. **Post adjustment entries** revaluing inventory and COGS from estimated to actual
4. **Establish the actual cost** as the basis for the next period's running estimates

The architectural impact: a period-end process that reads variance accounts, computes per-item adjustment amounts, and posts revaluations.

### 12.2 The variance allocation algorithm

Conceptually, the process is:

```
For each item in the period:
  total_variance = sum of all variance postings for the item this period
  total_quantity_flow = receipts + transfers in (the cost-bearing inflows)
  per_unit_variance = total_variance / total_quantity_flow
  
  For each consumption/transfer-out during the period:
    consumption_adjustment = quantity × per_unit_variance
    post adjustment to revalue COGS or destination inventory
  
  For ending inventory:
    ending_adjustment = ending_quantity × per_unit_variance
    post adjustment to revalue inventory asset
```

Real implementations are more complex. They handle:

- Multi-level BOM cost flow (variance on raw materials flows to WIP, then to finished goods, then to COGS for sold finished goods)
- Co-products and by-products (allocation across multiple outputs of a single production process)
- Multiple variance categories (PPV vs IPV vs production variance, each with different allocation rules)
- Inter-organization transfers (variance on a cross-company transfer flows to the receiving organization's inventory or COGS)

SAP's Material Ledger handles all of these; Oracle Fusion's cost accounting handles most; D365 handles the simpler scenarios. A custom build typically starts with single-level allocation and adds complexity as needed.

### 12.3 The actual cost run subledger

```sql
CREATE TABLE actual_cost_runs (
  run_id               BIGSERIAL PRIMARY KEY,
  
  -- scope
  legal_entity_id      INT NOT NULL,
  cost_book_id         INT NOT NULL,
  fiscal_year          SMALLINT NOT NULL,
  fiscal_period        SMALLINT NOT NULL,
  
  -- run lifecycle
  status               VARCHAR(16) NOT NULL CHECK (status IN ('initiated','calculating','calculated','posted','reversed')),
  initiated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  initiated_by         INT NOT NULL,
  calculated_at        TIMESTAMPTZ,
  posted_at            TIMESTAMPTZ,
  
  -- summary
  items_processed      INT,
  total_revaluation_amount NUMERIC(19,4),
  
  UNIQUE (legal_entity_id, cost_book_id, fiscal_year, fiscal_period)
);

CREATE TABLE actual_cost_run_results (
  result_id            BIGSERIAL PRIMARY KEY,
  run_id               BIGINT NOT NULL REFERENCES actual_cost_runs,
  
  -- item-level results
  product_id           BIGINT NOT NULL,
  location_id          INT,
  
  -- the period's actual unit cost (the PUP equivalent)
  period_actual_cost   NUMERIC(19,4) NOT NULL,
  period_estimated_cost NUMERIC(19,4) NOT NULL,
  per_unit_variance    NUMERIC(19,4) NOT NULL,
  
  -- quantity flows
  beginning_quantity   NUMERIC(19,6) NOT NULL,
  receipts_quantity    NUMERIC(19,6) NOT NULL,
  issues_quantity      NUMERIC(19,6) NOT NULL,
  ending_quantity      NUMERIC(19,6) NOT NULL,
  
  -- adjustment amounts
  ending_inventory_adjustment NUMERIC(19,4) NOT NULL,
  cogs_adjustment      NUMERIC(19,4) NOT NULL,
  
  -- traceability
  posting_id           BIGINT,                       -- the aggregate adjustment posting
  
  UNIQUE (run_id, product_id, location_id)
);
```

The run table tracks the lifecycle of the actual cost calculation. The results table records, per item, what the calculation produced. This is a well-bounded volume — one row per (item, location) with activity in the period.

### 12.4 The lifecycle of an actual cost run

The run goes through stages:

1. **Initiated**: the run is created; the period is locked from new transactions
2. **Calculating**: variances are collected, allocations computed, results generated; this can take hours for large customers
3. **Calculated**: the results are visible but not yet posted; users can review and approve
4. **Posted**: adjustment postings are generated and committed; the period's actuals are now in the GL
5. **Reversed**: in exceptional cases, a posted run is reversed (with audit trail), typically followed by a corrective rerun

This lifecycle gives operational control. The calculation is expensive; running it without commit lets operators verify before the GL impact is locked in.

### 12.5 The integration with cost methods

Actual costing applies differently to different methods:

**Standard costing**: variance accounts (PPV, IPV, etc.) accumulate during the period. At close, variances are allocated to ending inventory and COGS proportionally. This is where SAP's Material Ledger Actual Costing produces its biggest value — capturing standard cost variances and revaluing them.

**Moving average**: variances are absorbed into the running average as they occur. Period-end actual costing typically captures only the residual variances that couldn't be absorbed (e.g., variance on inventory that was already issued before the variance was known).

**Periodic weighted average**: this *is* actual costing. The period-end weighted average is the actual cost; no separate actual costing run is needed.

**FIFO/LIFO**: actual cost flows naturally — the cost of an issue is the cost of the depleted layer, which is the actual receipt cost. Period-end actual costing handles only late-arriving variances (e.g., invoice variance on a receipt that was later depleted).

**Lot-based and Specific Identification**: cost is per lot or per unit, captured at receipt. Late-arriving variances on a specific lot or unit can be absorbed if the lot/unit is still on hand, or expensed if already issued.

### 12.6 Integration with parallel ledgers

The actual cost run produces results per cost book. If a customer maintains parallel cost books (IFRS, US GAAP, tax), each book runs its own actual cost calculation, potentially producing different results. The same operational events feed all books; the variances and allocations differ per book (because the methods can differ per book — IFRS may use FIFO while tax uses LIFO).

This is where SAP's parallel ledgers shine and where simpler systems struggle. The architecture supports it because cost_book_id is a dimension throughout — every cost layer, lot, unit, average, and run is qualified by cost book. The actual cost run runs per book; results are per book; postings are tagged with book.

### 12.7 The volume of actual cost adjustments

Actual cost runs produce a burst of postings at period close. For a customer with 20,000 active items, the run generates up to 20,000 inventory adjustment postings plus equivalent COGS adjustments. With multi-level BOM allocation, the cascade can produce 5-10× that volume.

This is a known pattern. Period-end is a high-write window; the architecture absorbs it via the same mechanisms used for other period-end processes (depreciation, FX revaluation, allocation runs): batched inserts, async processing, partitioned writes that cluster in the closing period's partition.

The closing period's partition can become 50%+ larger than non-closing partitions due to revaluation traffic. Partition design should anticipate this — sizing partitions for closing-period volume, not average volume.

---

## 13. Parallel Costing Across Ledgers

Most enterprise customers need to maintain inventory under multiple accounting standards simultaneously: IFRS for international consolidated reporting, US GAAP for U.S. statutory reporting, tax basis for income tax filings, possibly local statutory bases for individual jurisdictions. The same physical goods movement may need different accounting under each.

Examples of where the methods diverge:

- **Inventory valuation method**: A US-headquartered company files US tax under LIFO (legally permitted) but reports IFRS results to international markets under FIFO (LIFO not permitted under IFRS). Same receipts, different methods.
- **Capitalization rules**: GAAP capitalizes more types of costs into inventory (certain overhead, certain R&D) than tax basis. Same receipts, different cost amounts.
- **Standard cost revisions**: a standard cost change might apply differently across books. The IFRS book updates standards quarterly; the tax book uses annual standards.
- **Variance treatment**: variance allocations may differ. IFRS may capitalize variance into ending inventory (full absorption); tax may expense it (direct costing).

The architecture must support all of this without duplicating operational entry — one receipt produces postings in all relevant books, each with its own valuation rules.

### 13.1 The cost book as the unifying concept

The architecture's central abstraction is the **cost book**. A cost book is to inventory what a ledger is to general accounting: a complete set of valuation rules and balances, attached to a specific accounting standard or purpose.

Cost books are scoped to legal entities. Each legal entity has at minimum one cost book (the primary, typically tied to the leading ledger). Additional cost books support secondary ledgers, tax bases, simulation, and management views.

```sql
CREATE TABLE cost_books (
  cost_book_id         INT PRIMARY KEY,
  legal_entity_id      INT NOT NULL,
  ledger_id            SMALLINT,             -- the GL ledger this book maps to (NULL for ledgerless)
  name                 VARCHAR(128) NOT NULL,
  
  -- valuation policy
  default_costing_method VARCHAR(32) NOT NULL,    -- 'standard', 'moving_avg', 'fifo', etc.
  capitalization_rule_id INT,                    -- which costs to capitalize
  
  -- accounting standard
  accounting_standard  VARCHAR(16) NOT NULL,    -- 'IFRS', 'US_GAAP', 'TAX', 'STAT_DE', 'MGMT', ...
  
  -- audit
  effective_from       DATE NOT NULL,
  effective_to         DATE,
  status               VARCHAR(16) NOT NULL,
  
  UNIQUE (legal_entity_id, name)
);
```

Every subledger table — `inventory_movements`, `cost_layers`, `inventory_lots`, `inventory_units`, `moving_average_costs`, `actual_cost_runs` — carries `cost_book_id` as a partition column or filter. A receipt processed through the rules engine produces postings per book; each book maintains its own valuation state.

### 13.2 The challenge of method divergence

Different methods produce structurally different cost data:

- A FIFO book has cost layers
- A moving-average book has running averages
- A standard book has standards plus variances
- A serialized book has unit-level costs

Maintaining all of these simultaneously requires that the *same receipt* trigger different subledger updates per book:

```
Receipt of 100 units at $12 actual:

Book 1 (IFRS, FIFO):
  - inventory_movements row for book 1
  - cost_layers row for book 1
  - GL posting in ledger 0 (IFRS)

Book 2 (US Tax, LIFO):
  - inventory_movements row for book 2
  - cost_layers row for book 2 (same data, different book)
  - GL posting in ledger 2 (Tax)

Book 3 (Management, Standard at $10):
  - inventory_movements row for book 3 with standard $10, actual $12
  - PPV variance posting in book 3
  - GL posting in ledger 3 (Management)
```

Each book gets its own subledger writes and GL postings. The rules engine's job is to produce all of this from a single receipt event — by evaluating the rule set for each book separately and persisting the results across all books in one atomic transaction.

### 13.3 Method consistency across books

Within a single book, the costing method must be consistent for an item. You cannot have an item that's FIFO for half the year and LIFO for the other half within one book — that's not a coherent accounting policy.

Across books, the same item can have different methods:

```
Product P:
  - In book "IFRS-FIFO": FIFO method
  - In book "US-Tax-LIFO": LIFO method  
  - In book "Mgmt-Std": Standard method, standard cost $10
```

Each book independently evaluates its rules. The receipt of 100 units at $12 produces three different subledger states:

- IFRS-FIFO book: a new layer of 100 units at $12
- US-Tax-LIFO book: a new layer of 100 units at $12 (same layer data; LIFO is just a different consumption order)
- Mgmt-Std book: a movement at $10 standard plus a $200 PPV variance

When 30 units are subsequently issued, each book computes its own COGS:

- IFRS-FIFO: depletes the oldest layer (perhaps an old layer at $10), COGS = 30 × $10 = $300
- US-Tax-LIFO: depletes the newest layer ($12), COGS = 30 × $12 = $360
- Mgmt-Std: COGS at standard = 30 × $10 = $300

Different books can produce dramatically different COGS for the same physical issue. This is the entire point of parallel valuation — different bases for different reporting needs.

### 13.4 Storage implications

Parallel cost books multiply storage. Three books = 3× the inventory_movements rows, 3× the cost layers, 3× the running averages. For a high-volume environment, this is a real cost.

Mitigations:

**Selective bookkeeping**. Not every operational event needs to post in every book. A purely operational receipt (intra-organization transfer that doesn't affect external valuation) might only post in management books, not statutory books. The rules engine's per-book rule sets decide.

**Shared structures where possible**. The cost_layers table can be shared across books *if* the layer data is identical (same receipt, same actual cost, same physical movement). The depletion order can differ per book; the layer itself is the same. This is a non-trivial optimization but can reduce layer storage by N (the number of books).

**Compressed columnar archival for non-primary books**. The primary book (typically IFRS or local GAAP, mapped to the leading ledger) needs hot OLTP performance. Secondary books (tax, management) might tolerate slightly higher latency and can use columnar storage on a read replica that lags by minutes. This provides cost savings without sacrificing the primary book's performance.

**Materialized rollups per book at period boundaries**. For management reporting, the period-end balances and turnover by book can be materialized as small summary tables, replacing detailed scans.

### 13.5 Reconciliation across books

The reconciliation invariant per book: the sum of subledger detail in book X equals the GL balance in book X's ledger. This is the same invariant as single-book scenarios, just enforced per book.

A second invariant useful for parallel-book debugging: aggregate physical quantities should match across books. If IFRS book shows 1000 units on hand and tax book shows 1100 units, something is wrong — both books reflect the same physical reality. The valuation differs; the quantity does not.

Periodic reconciliation jobs verify both invariants per book. Mismatches indicate bugs in the per-book rule evaluation or in the subledger update logic. Catch them via daily reconciliation, fix them before they accumulate.

### 13.6 Practical scope

For mid-sized companies, two cost books is typical: one for primary reporting (IFRS or local GAAP) and one for tax. Three or four is common for global enterprises with multiple statutory reporting needs.

For a custom-built ledger, supporting two cost books is essential; supporting four is a stretch goal; supporting more than four is rarely needed and probably indicates the customer should be using SAP's Material Ledger or Oracle Fusion's cost books for more sophisticated multi-book scenarios.

The architecture scales linearly with book count. Two books is 2× the storage and computation; ten books is 10×. There's no qualitative breakage at any specific count, just increasing operational cost.

---

## 14. Cross-Cutting Concerns

Several concerns span all costing methods. They appear in different forms but require consistent handling.

### 14.1 Receipt versus invoice timing

Most ERPs separate the goods receipt (physical arrival) from the vendor invoice (financial document). The receipt establishes the cost basis tentatively (using the PO price); the invoice may arrive days or weeks later with a different price (price increase, quantity adjustment, freight charges).

The implication: cost can change *after* the inventory is already received and possibly partially issued.

The architectural pattern:

1. **Receipt at PO price**. Inventory is valued at the PO price; the offset is to a "Goods Received Not Invoiced" (GR/IR) liability account.
2. **Invoice match**. When the invoice arrives, it's matched to the receipt. Differences from PO price flow to Invoice Price Variance (IPV).
3. **IPV absorption**. Depending on policy, IPV can:
   - Stay in IPV (expensed)
   - Be capitalized into ending inventory if quantity remains
   - Be allocated proportionally to ending inventory and consumed inventory (the latter via COGS adjustment)

The capitalization decision is a per-book accounting policy. Some books capitalize all IPV; some capitalize only on-hand portion; some never capitalize. The rules engine handles this via a per-book IPV treatment rule.

The subledger captures this via additional movement events:

```
Original receipt: 100 units at $10 (PO price), inventory movement, GR/IR liability
Invoice match: invoice at $10.50, inventory movement of 0 quantity at $0.50 unit variance
  If capitalized: cost adjustment to inventory, MA recalculation, layer cost adjustment
  If expensed: posting to IPV account, no subledger cost change
```

This complexity is one of the reasons period-end actual costing exists — by sweeping all variances at period close, the architecture handles the timing mismatch between receipts and invoices systematically.

### 14.2 Returns to vendor

A receipt that's returned before being issued is straightforward — reverse the receipt, including any cost subledger updates. The original layer or lot is reduced; the running average is recomputed; standard variance reverses.

A return *after* the receipt has been partially or fully issued is more complex. The original layer was depleted; reversing the receipt has to handle the depletions:

- **If the original layer is partially intact**: reduce the layer by the return quantity (creating a negative depletion or reducing the layer's effective original quantity)
- **If the original layer is fully depleted**: the return reduces a different layer (potentially the next-oldest), with cost differences flowing to a Return Variance account
- **If the system can't trace specific layers**: aggregate adjustment to current cost basis, expensing the difference

The architecture supports all three patterns via flexible event types in the subledger and rule evaluation that determines the correct treatment per scenario.

### 14.3 In-transit valuation

Inventory in transit between locations (or between legal entities) creates valuation timing questions:

- When does title transfer? At pickup, at handoff to carrier, at receipt? This affects who owns the inventory while in transit.
- What is the in-transit cost basis? The source location's cost (typical for FIFO/standard), the destination location's cost (rarely used), or a separate in-transit pool cost?
- How are in-transit balances reported? In source's inventory? Destination's? A separate in-transit account?

The architecture handles this via:

- A separate **in-transit location** (or pseudo-location) for each transfer pair, with its own subledger entries
- Transfers consist of three events: issue from source (debit in-transit, credit source inventory), receipt at in-transit (already represented), receipt at destination (debit destination inventory, credit in-transit)
- Periodically, in-transit balances aging beyond an expected threshold are flagged for investigation

This is operationally rich but mechanically the same as other location-to-location moves; the in-transit "location" is a virtual one with reporting visibility.

### 14.4 Negative inventory

When issues exceed receipts (issuing inventory that hasn't been recorded as received yet), inventory goes negative. Different costing methods handle this differently:

- **Standard**: post the issue at standard cost; the inventory balance goes negative and corrects when the missing receipt is recorded
- **Moving average**: post at the current running average; the running average is NOT updated by the issue (issues never update average); when the missing receipt is recorded, the average is computed correctly
- **FIFO/LIFO**: there's no layer to deplete; the system creates a "phantom" depletion at the current average cost; when the matching receipt arrives, the phantom is reconciled with the actual layer cost; the difference flows to a variance account
- **Lot/Serialized**: the issue must specify a lot or serial; if none exists, the issue is rejected

The architectural recommendation: configure negative-inventory policy per item (or item group). Allow it for fast-moving items where back-order processing requires it; reject it for high-value items where missing inventory indicates a data or operational issue.

### 14.5 Cost adjustments (manual revaluation)

Sometimes accountants need to manually adjust an item's cost outside the normal flow — to write down inventory due to obsolescence, to revalue at fair market value, to correct an error. The architecture must support this without violating cost-method invariants.

The pattern: manual adjustments are explicit events in the subledger that flow through the GL. They're typed (`event_type='cost_adjustment'`) and carry justification metadata (reason code, approval reference). Per method:

- Standard: adjusts the standard cost (which triggers full revaluation) or posts to a non-standard adjustment account
- Moving average: triggers a recalculation event; the new running average reflects the adjustment
- FIFO/LIFO: adjusts a specific layer's cost (rarely correct) or creates an adjustment posting to inventory with method-specific allocation
- Lot/Serialized: adjusts the specific lot or unit's cost

Manual adjustments require approval workflow — they're high-risk operations that an unprivileged user should not be able to perform. The architecture treats them as governed transactions with mandatory review.

### 14.6 Consigned inventory

Consigned inventory — owned by the supplier until consumed by the consignee — has unusual costing properties. The consignee holds the goods physically but doesn't own them; the cost is recognized only at consumption.

Two architectural patterns:

**Memo tracking**: consigned inventory is tracked in a separate subledger (or a flagged subset of the main subledger) but does NOT create GL postings. When consumed, an "ownership transfer" event creates the financial liability and inventory entry simultaneously. The cost basis is the consignor's price at consumption.

**Dual subledger with offsetting accounts**: consigned inventory creates GL entries with offsetting Consigned Inventory and Consigned Liability accounts (debit equals credit; net zero on balance sheet). At consumption, the offsetting accounts close and the standard receipt entries fire.

Both patterns are valid; the choice depends on visibility and control requirements. The architecture supports both via configuration of the consigned-inventory event class in the rules engine.

---

## 15. Performance and Storage Strategies

The costing subledgers can produce significantly more rows than the GL. A high-volume manufacturer with serialized inventory might generate ten or more subledger rows per GL line. Designing for this volume is essential.

### 15.1 Volume estimation

Rough order-of-magnitude estimates for various scenarios, per year:

| Scenario | GL lines | Subledger rows |
|---|---|---|
| Mid-market, mostly standard | 10M | 20M (movements + variances) |
| Mid-market, FIFO with active layers | 10M | 50M (movements + layers + depletions) |
| Mid-market with lots, 2 cost books | 20M | 200M |
| Large enterprise, mixed methods, 3 cost books | 100M | 1B+ |
| Large enterprise, serialized at scale, 3 cost books | 200M | 5B+ |

The progression is dramatic. For very high-volume serialized scenarios, the subledger storage exceeds the entire GL by 25-50×.

### 15.2 Partitioning strategy

All subledger tables partition by a date column. The partition cadence trades query performance against partition count:

- **Monthly partitions**: balances both. Most queries scope to one or a few months. Partition count grows by 12 per year per table. After 5 years, 60 partitions per table is manageable.
- **Daily partitions**: too granular for most use cases. Generates 365 partitions per year. Useful only for ultra-high-volume tables (event logs at multi-billion-row scale).
- **Weekly or quarterly partitions**: alternatives that may suit specific operational rhythms.

Partition pruning is the primary performance benefit. Queries that include `WHERE event_date BETWEEN ...` scan only relevant partitions; the planner skips others entirely.

### 15.3 Sub-partitioning by cost book

For multi-book scenarios, sub-partitioning by `cost_book_id` adds another level of pruning:

```sql
CREATE TABLE inventory_movements_2026_05 PARTITION OF inventory_movements
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01')
  PARTITION BY LIST (cost_book_id);

CREATE TABLE inventory_movements_2026_05_b1 PARTITION OF inventory_movements_2026_05 FOR VALUES IN (1);
CREATE TABLE inventory_movements_2026_05_b2 PARTITION OF inventory_movements_2026_05 FOR VALUES IN (2);
```

Most queries scope to one cost book (financial reporting is per-book). Sub-partitioning by book gives the planner direct access to just that book's data. The trade-off is more partitions (12 months × 3 books = 36 sub-partitions per year per table); manageable up to a point.

### 15.4 Index strategy

Subledger tables are write-heavy and read for both transactional and analytical queries. The index strategy balances them:

**On the OLTP primary**:
- Primary key (insertion-friendly with BIGSERIAL)
- BRIN index on the date column (cheap, append-friendly)
- B-tree on the most common lookup keys (e.g., `(product_id, location_id, cost_book_id)`)
- Partial indexes for sparse foreign keys (e.g., serial_number when not NULL)

**On the analytical replica**:
- Wider index set including dimension fields used in reports
- GIN indexes on JSONB extension fields if used
- Possibly columnar storage (Citus columnar, Hydra) for the largest tables

The OLTP primary stays lean; the analytical replica absorbs the index complexity.

### 15.5 Materialized current-state tables

Computing current state from event history is correct but slow. Materialized tables capture current state for fast reads:

- `current_moving_average`: latest running average per pool
- `current_cost_layers`: open layers with remaining quantity > 0
- `current_inventory_lots`: lots still on hand
- `current_inventory_units`: units in stock (status = 'in_stock')
- `current_account_balance`: GL account balance per period

These are refreshed via scheduled jobs (every few minutes for hot data, hourly for less critical) or via event-driven refresh (triggered by relevant inserts). Refresh strategies:

- **Full refresh**: recompute the entire materialized table. Simple but expensive for large tables. Suitable for low-volume tables.
- **Incremental refresh**: process only new events since the last refresh. Requires a watermark column (e.g., latest event_id processed). More efficient for large tables.
- **CDC-based refresh**: a change-data-capture pipeline streams new events to the materialized table. Most efficient for very large tables with strict freshness requirements.

The architecture starts with full refresh and migrates specific high-volume tables to incremental as needed.

### 15.6 Archival strategies

Subledger tables grow without bound unless archived. Strategies:

**Time-based archival**: events older than N years (typically 7-10 for accounting records, longer for regulated industries) move to a separate archive table or to cheap object storage. The active table contains recent events; the archive contains everything older. Audit queries that need historical data join across both.

**Status-based archival**: events for "terminated" entities (sold serials, fully-depleted lots, fully-depleted layers) move to archive after a retention period. Active entities' events stay hot.

**Compression**: archive tables use columnar compression (Citus columnar, Parquet on object storage). Compression ratios of 5-10× are typical for event-log data with low-cardinality fields.

**Retrieval policies**: archived data is retrievable for audit but with higher latency. SLAs distinguish hot data (sub-second access) from archived data (minutes to hours for retrieval).

### 15.7 Read/write separation

The architecture relies on logical replication to separate OLTP writes from analytical reads:

- **OLTP primary**: receives all writes; serves transactional reads (lookups by ID, current balances for posting validation)
- **Analytical replica**: receives writes via replication, serves analytical queries (period reports, drill-throughs, dashboards)

The replica can have additional indexes, columnar storage, and different VACUUM/maintenance schedules without affecting the primary. Replication lag (typically seconds to a minute) is acceptable for analytical workloads.

For very high-volume environments, multiple replicas serve different workloads — one for management reports, one for regulatory reports, one for ad-hoc analysis. Each replica is sized and indexed for its workload.

### 15.8 Cost-method-specific volume hot spots

Different methods have different volume profiles:

- **Standard**: low volume (movements + variances). Easy to scale.
- **Moving average**: moderate volume (movements + recalculations on receipts). Recalculations only on receipts, not issues — keeps volume bounded.
- **FIFO/LIFO**: higher volume (movements + layer creates + depletion records). One depletion record per layer consumed; high-volume issues that span multiple layers multiply the records.
- **Lot-based**: high volume in lot-tracked industries. One lot per receipt; one event per state change.
- **Serialized**: highest volume. One unit per individual item; one event per state change. Pharmaceuticals, electronics, vehicles — these can multiply GL volume by 10-1000×.

The architecture's scaling concerns are most acute for serialized at high volume. Other methods are well within reach of standard PostgreSQL infrastructure.

---

## 16. Implementation Phasing

A custom ledger system with full costing support is a multi-year build. Sequencing matters — building the wrong things first creates rework and delays time-to-value. The following phasing reflects dependencies and customer demand.

### 16.1 Phase 1: Standard and Moving Average

Start here. These two methods cover 70%+ of typical use cases, share most infrastructure (variance accounts and inventory movements), and have the simplest subledger requirements.

Deliverables:
- `inventory_movements` subledger
- `item_standard_costs` master with effective dating and approval workflow
- `moving_average_costs` recalculation log
- Integration with the rules engine for receipt and issue events
- Materialized current-balance and current-moving-average views
- Daily reconciliation jobs (subledger to GL)

This phase produces a working inventory system handling the most common methods. Many customers can adopt the platform with just these.

### 16.2 Phase 2: FIFO and LIFO

Once Standard and Moving Average are stable, add layered methods. The infrastructure is largely new (cost layers, depletions) but follows established subledger patterns.

Deliverables:
- `cost_layers` table with monthly partitioning
- `cost_layer_depletions` table with monthly partitioning
- FIFO depletion algorithm with per-pool serialization
- LIFO depletion (same data, different sort order)
- Materialized `current_cost_layers` view
- Reconciliation of layer residuals to GL inventory balance

Phase 2 makes the platform suitable for most additional industries. Discrete manufacturing, distribution, and retail with FIFO are well-served.

### 16.3 Phase 3: Period-End Actual Costing

With the perpetual methods working, add the actual costing layer. This handles standard cost variance allocation and periodic weighted average — the things that make standard and moving-average methods practical for accuracy-sensitive industries.

Deliverables:
- `actual_cost_runs` and `actual_cost_run_results` tables
- The variance allocation algorithm (single-level initially)
- Period-end orchestration integrating actual costing with depreciation, FX revaluation, and other period-end processes
- Multi-level variance allocation (cascading from raw materials through WIP to finished goods)

Phase 3 brings the platform up to mid-market manufacturing parity. SAP-class actual costing is more sophisticated; phase 3 covers most use cases.

### 16.4 Phase 4: Lot-Based and Serialized

These build on similar infrastructure but at different scales. Lot-based first (typically lower volume than serialized); serialized second (higher operational complexity).

Deliverables:
- `inventory_lots` and `inventory_lot_events` tables
- Lot allocation strategies (FEFO, FIFO by lot, etc.)
- `inventory_units` and `inventory_unit_events` tables
- Serialized depletion mechanics
- Integration with operational features (recall management, expiration, traceability)

Phase 4 opens regulated industries (pharmaceuticals, food, medical devices) and high-value-item industries (vehicles, equipment).

### 16.5 Phase 5: Group Average and Multi-Book

The remaining methods plus parallel costing.

Deliverables:
- `cost_groups` and group_average_costs tables
- Multiple cost books per legal entity
- Per-book rule evaluation in the rules engine
- Per-book reconciliation jobs
- Storage and indexing strategies for multi-book volumes

Phase 5 supports complex global enterprises and multi-jurisdiction operations.

### 16.6 Phase 6: Industry-Specific Extensions

The long tail.

Deliverables:
- Pharmaceutical track-and-trace integrations
- Process manufacturing co-product / by-product costing
- Project-based costing with capitalization paths
- Repair and maintenance cost handling
- Specialized industry rules

This phase is open-ended. Each industry has its own requirements; the platform extends to meet them as customer demand justifies.

### 16.7 Why this order matters

The phasing reflects dependencies. Phase 1 establishes the inventory_movements foundation that all other phases use. Phase 2 introduces the layered structures that lots and serials extend. Phase 3 establishes the period-end orchestration. Phase 4 builds on layers conceptually. Phase 5 generalizes everything to multi-book. Building any phase out of order requires either rework (when later phases require restructuring of earlier work) or weak foundations (when phases skip required dependencies).

The phasing also reflects customer demand. Phase 1 customers exist immediately. Phase 2 expands the addressable market. Phase 3-4 reach industries with specialized needs. Phase 5 supports the complex global customers who would otherwise require SAP. Each phase opens a market segment.

---

## 17. Synthesis

This document has covered eight costing methods and the subledger structures that support them. A few cross-cutting themes are worth highlighting.

### 17.1 The dominant pattern

Across all methods, one pattern recurs:

1. **The GL records aggregate financial impact**, sized to financial reality
2. **Subledger tables record operational detail**, sized to operational reality
3. **The two are reciprocally linked** — every subledger event references its GL posting; every GL line can be expanded to subledger detail
4. **Both are append-only**; current state is derived
5. **Reconciliation invariants** verify that subledger sums equal GL balances
6. **Materialized current-state tables** provide fast access to projected state

This pattern works for inventory costing exactly as it works for AR open items, AP open items, fixed asset registers, and intercompany matching. It is not specific to inventory; it is the universal pattern for any domain where the GL holds aggregate balances and the subledger holds detailed records.

### 17.2 The variation across methods

Methods vary along three dimensions:

- **Cost basis**: reference (standard) versus real (everything else)
- **Aggregation level**: aggregated (averages), layered (FIFO/LIFO), or identified (lots, serials)
- **Timing**: perpetual (cost final at transaction time) versus periodic (cost final at period close)

These variations drive different subledger structures: average methods need running-cost logs; layered methods need layer tables and depletion logs; identified methods need entity tables and event logs; periodic methods need revaluation infrastructure.

The custom ledger architecture supports all of these on the same foundation. The `inventory_movements` table is universal; the additional structures stack on top per method. A single platform supports all eight methods without architectural fragmentation.

### 17.3 The ledger-engine interaction

The rules engine produces postings; the costing subledgers compute values. The two interact via:

- The rules engine reading the current cost from the costing subledger (e.g., the current moving average) at posting time
- The rules engine writing both GL postings and subledger updates atomically in the same transaction
- The rules engine running per cost book to produce parallel valuations

This separation — rules for accounting derivation, subledgers for cost computation — keeps both layers clean. The rules engine doesn't need to know how FIFO works; it knows to call the costing service for the cost of an issue. The costing service doesn't need to know what GL accounts are debited; it returns the cost amount and lets the rules engine produce the postings.

### 17.4 The multi-book reality

For all but the smallest customers, multi-book costing is a real requirement. The architecture supports it from the beginning by qualifying every subledger table with `cost_book_id`. Adding additional books later is operational (data migration, configuration) rather than architectural. Doing this from day one avoids the painful retrofit that single-book systems face when their first multi-jurisdiction customer arrives.

### 17.5 The volume reality

Costing subledgers, especially for serialized inventory, can produce volumes substantially larger than the GL. The architecture handles this through:

- Partitioning (mandatory)
- Subledger separation from GL (always)
- Read/write separation via replicas (essential for analytics)
- Time-based and status-based archival (operational discipline)
- Materialized current-state tables (performance optimization)
- Columnar storage on analytics replicas (storage efficiency)

These are not optional optimizations; they are the architecture working at scale. Operations teams must understand and manage them; otherwise the design's capacity is unrealized.

### 17.6 What this document does not cover

Several adjacent topics are deliberately out of scope:

- **Detailed BOM cost roll-up algorithms** (multi-level cost cascading from raw materials to finished goods). The principles are similar to actual costing variance allocation; the implementation is industry-specific.
- **Project costing capitalization** (when project costs become capital assets). This is a separate domain that interacts with inventory but isn't inventory costing per se.
- **Transfer pricing across legal entities** with intercompany markup elimination. This builds on the methods above but adds intercompany complexity that warrants its own treatment.
- **Specific industry implementations** (oil and gas depletion accounting, broadcasting amortization, healthcare variable consideration). Each is a specialization of the patterns above.

A complete reference would cover these; the present document focuses on the foundational methods and their subledger architectures.

### 17.7 The build-or-buy question

A custom-built ledger with full costing support is a major investment. Phases 1-2 are achievable in a year or two for a focused team; phases 3-5 add several more years; phase 6 is open-ended. SAP, Oracle Fusion, D365, and NetSuite all provide phases 1-5 (with varying depth) out of the box.

The build decision is justified when:

- The company is a SaaS or fintech with embedded financial workflows that don't fit commercial ERPs
- The volume exceeds what commercial ERPs handle without painful customization (which, for serialized inventory in particular, is rare but real)
- The company has the engineering capacity to maintain a financial system long-term

For most enterprises, the decision is to buy and customize. The architecture in this document and its companion is for the rarer cases where build is the right call. In those cases, the patterns described — append-only postings, subledger separation, multi-book costing, period-end orchestration — give a defensible foundation.

### 17.8 Closing observation

Inventory costing is one of the harder problems in enterprise accounting. Multiple methods, complex subledger structures, multi-book parallel valuation, period-end revaluation, and high-volume serialized scenarios all combine to make it operationally rich.

But the underlying patterns are consistent. Every method produces postings to the GL via the same rules engine. Every method maintains its detail in a subledger that's reciprocally linked to the GL. Every method follows append-only with derived current state. Every method requires the same period-end orchestration and reconciliation discipline.

Designed coherently, the architecture is no harder than the GL itself — it is just larger. The rules and patterns travel; the data structures multiply. A custom build that respects the patterns will produce a system that scales gracefully across customer needs. A custom build that improvises will produce a tangle of incompatible mechanisms that breaks at the first customer with non-trivial requirements.

The recommendation, as throughout this series of design documents: respect the domain. Inventory costing is what it is — half a millennium of accountants have refined it. The architecture's job is to give the domain a faithful, performant, auditable expression on modern infrastructure. Any architectural choice that fights the domain will lose; any choice that respects it will succeed.
