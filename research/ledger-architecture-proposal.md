# A Postgres-Based Ledger: Schema Design and a Two-Tier Rules Engine

**A Design Proposal with Industry Context and Critical Analysis**

---

## Contents

1. [Introduction and Design Philosophy](#1-introduction-and-design-philosophy)
2. [Part I — The Posting Lines Schema](#part-i--the-posting-lines-schema)
   - 2.1 [Design Goals and Constraints](#21-design-goals-and-constraints)
   - 2.2 [How the Industry Got Here](#22-how-the-industry-got-here)
   - 2.3 [The Proposed Schema](#23-the-proposed-schema)
   - 2.4 [Field-by-Field Rationale](#24-field-by-field-rationale)
   - 2.5 [Partitioning, Sharding, and Indexes](#25-partitioning-sharding-and-indexes)
   - 2.6 [Append-Only Enforcement](#26-append-only-enforcement)
   - 2.7 [Invariants at the Database Layer](#27-invariants-at-the-database-layer)
   - 2.8 [Critical Analysis — What This Schema Gives Up](#28-critical-analysis--what-this-schema-gives-up)
3. [Part II — The Two-Tier Rules Engine](#part-ii--the-two-tier-rules-engine)
   - 3.1 [Why Two Tiers](#31-why-two-tiers)
   - 3.2 [Tier 1: Configuration-Based Rules](#32-tier-1-configuration-based-rules)
   - 3.3 [Tier 2: WASM Modules for Custom Logic](#33-tier-2-wasm-modules-for-custom-logic)
   - 3.4 [How the Tiers Cooperate](#34-how-the-tiers-cooperate)
   - 3.5 [Critical Analysis — What This Engine Gives Up](#35-critical-analysis--what-this-engine-gives-up)
4. [Part III — Cross-Cutting Concerns](#part-iii--cross-cutting-concerns)
   - 4.1 [The Outbox Pattern and Event Lifecycle](#41-the-outbox-pattern-and-event-lifecycle)
   - 4.2 [Idempotency Throughout the Pipeline](#42-idempotency-throughout-the-pipeline)
   - 4.3 [Bi-Temporal Modeling in Practice](#43-bi-temporal-modeling-in-practice)
   - 4.4 [Open Items, Balances, and Other Derived State](#44-open-items-balances-and-other-derived-state)
   - 4.5 [Multi-Currency Handling](#45-multi-currency-handling)
   - 4.6 [Period Close Orchestration](#46-period-close-orchestration)
   - 4.7 [Reconciliation as a First-Class Concern](#47-reconciliation-as-a-first-class-concern)
   - 4.8 [Cross-Shard Invariants](#48-cross-shard-invariants)
5. [Part IV — Synthesis](#part-iv--synthesis)
   - 5.1 [What This Design Achieves](#51-what-this-design-achieves)
   - 5.2 [What It Cannot Achieve](#52-what-it-cannot-achieve)
   - 5.3 [Where to Begin](#53-where-to-begin)

---

## 1. Introduction and Design Philosophy

This document proposes a ledger architecture that takes the most coherent ideas from each major commercial ERP — SAP S/4HANA's Universal Journal data model, Oracle Fusion's rule-based subledger accounting, Microsoft Dynamics 365's posting layers, and Oracle NetSuite's source-record GL visibility and SuiteGL extensibility — and recombines them into a system buildable on commodity infrastructure. The constraints are intentional: PostgreSQL as the database, append-only postings, batched inserts, analytics served from replicas, and sharding at the legal-entity boundary. The goal is not to replicate any single existing ERP but to identify what each got right and design a system that respects 500 years of double-entry bookkeeping while taking advantage of modern engineering practices.

The design rests on three principles that organize everything below.

**The data model and the rule layer are separable concerns and should be physically separate.** SAP entangles them through configuration tables interpreted by application code; Oracle Fusion's SLA decouples them more cleanly but the rule layer is opaque; NetSuite's GL impact is calculated from item and entity assignments at save time, which is fast but limited. The proposed design treats persistence as a moderately-narrow append-only core with typed extensions, and rule evaluation as a separate engine that produces both core and extension rows, with the only contract between them being the schema and a strict double-entry invariant.

**The data model respects the database platform's nature.** SAP's ACDOCA wide-table design is co-designed with HANA's column-store engine — width is effectively free, every column is independently stored and read. Replicating that design on Postgres's row-store engine fights the database. The design here adopts a moderately-narrow core (~17 universally-needed columns) with typed extension tables for sparse and specialized data. This preserves the single-source-of-truth unification benefit (all universal facts in the core) while letting Postgres do what it does well: smaller rows, less WAL per write, no NULL waste, easier schema evolution. The same design would be wrong on HANA. Architecture is platform-aware.

**Configuration is the primary mechanism; code is the escape hatch.** Most accounting needs are satisfied by typed configuration that an accountant can read, edit (with review), and audit. A small fraction genuinely require Turing-complete logic, and those should have a path that doesn't require a platform release. Pushing customers into code unnecessarily — as NetSuite's SuiteGL approach can do for cases that should have been simple configuration — creates audit overhead and operational burden. Forcing complex cases into configuration — as SAP's account-determination tables sometimes do — produces unreadable spaghetti.

**Append-only is not a performance compromise; it is the correct domain model.** Double-entry bookkeeping has been event-sourced and immutable since 1494. Every modern system that has tried to "improve" on this — by storing mutable balances, by allowing edits to past postings, by collapsing events into current-state — has either failed or quietly retrofitted standard double-entry. The architecture below treats append-only as foundational, not as a constraint to work around. Balances, open items, period totals, and other apparently-stateful concepts are all derived projections over the immutable line-item log.

The document is organized in four parts. Part I covers the posting-lines schema in detail, including how it relates to ACDOCA and the other vendor data models. Part II covers the two-tier rules engine. Part III addresses the operational concerns that span both layers — the outbox pattern, idempotency, multi-currency, period close, reconciliation. Part IV synthesizes the design's achievements and limitations.

A note on intended audience: this document is written for engineers and architects designing or evaluating ledger systems. Familiarity with double-entry bookkeeping, transactional databases, and ERP fundamentals is assumed. Where domain context matters — for example, why bi-temporal modeling matters in accounting specifically — it is explained, but the document is not an introduction to accounting.

---

## Part I — The Posting Lines Schema

### 2.1 Design Goals and Constraints

The schema must satisfy a set of requirements that are simultaneously demanding, in tension with each other, and non-negotiable for an enterprise-grade ledger:

- **Schema-level consistency.** The same business event must appear identically in financial accounting, management accounting, asset accounting, and material ledger views — not as separate records that must be reconciled, but as the same data viewed through different lenses.
- **Immediate auditability.** Every posting must be traceable to its originating business event, the user or process that created it, and the rule version that produced it.
- **Multi-currency at the row level.** Every posting carries amounts in transaction currency, functional currency, and reporting currency without requiring joins for typical reporting.
- **Multi-ledger with parallel accounting standards.** A single business event may produce different accounting in IFRS, local GAAP, tax basis, and management ledgers. The schema must accommodate this without duplicating data unnecessarily.
- **Extensibility.** Customers add custom dimensions (project codes, regulatory categories, internal allocations) without DDL changes for every new use case.
- **Append-only with strong integrity.** No update or delete of posting lines once committed. Reversals create new lines, never edit old ones.
- **Performance at scale.** Handle billions of rows per legal entity over multi-year retention. OLTP throughput supports realistic transaction volumes; analytical queries return in usable time.

The constraints imposed by the platform choice further shape the design:

- **PostgreSQL only.** No HANA, no Oracle Database, no proprietary HTAP engines. Postgres is row-store by default, has native partitioning, supports JSONB, and has logical replication. Its weaknesses for this workload — analytical performance on wide tables, native sharding — must be addressed by replication topology and operational discipline rather than by switching databases.
- **Append-only.** Strictly enforced at the database level via permission grants and triggers, not merely by application convention.
- **Batched inserts.** Single-row inserts at high frequency are pathological for Postgres on this workload. The schema and surrounding architecture assume that writes flow through a queue and are applied in batches of hundreds to thousands of rows.
- **Analytics from replicas.** The OLTP primary is not asked to serve heavy analytical scans. Logical replication feeds an analytical store with appropriate columnar storage (Citus columnar extension, Hydra, or external warehouse). The primary stays lean.
- **Sharded by legal entity.** Each legal entity (or a small group of related entities) lives on its own Postgres cluster. Cross-entity reporting happens in a consolidation database fed from each shard.

These are not arbitrary engineering choices but the conditions under which the design is viable on Postgres. Relax any of them — allow updates, allow row-by-row writes, expect analytical queries on the OLTP primary — and the architecture begins to fail.

### 2.2 How the Industry Got Here

The schema below borrows ideas from each major ERP. Understanding the lineage helps explain why specific design choices are made.

**SAP's classical ERP** (pre-S/4HANA, the world from R/3 in 1992 to ECC 6.0 around 2015) stored financial accounting in BSEG (line items) and BKPF (headers), management accounting in COEP, asset accounting in ANEP/ANEA, and material ledger in MLIT. Each subsystem had its own data model, and reconciling them was a major activity at every period close. The reconciliation programs were notoriously fragile; SAP support handled an entire category of bugs caused by FI/CO mismatches that appeared after specific posting sequences. The architectural lesson: separate physical structures for the same business event create reconciliation overhead and a class of bugs that should not exist in a correctly-designed system.

**SAP S/4HANA's Universal Journal** (table ACDOCA, introduced in 2015) consolidates all of this into a single wide table. Every posting — financial, controlling, asset, material ledger, profitability analysis — writes one or more lines to ACDOCA. There is no FI-CO reconciliation because the FI line and the CO line are the same row viewed through different selection columns. The table is approximately 350 columns wide; storage is optimized by HANA's columnar in-memory engine, which compresses each column independently and reads only the columns a query references. This is the architectural innovation worth replicating: schema-level consistency through unification rather than through reconciliation.

**Oracle Fusion Cloud** maintains separate physical structures for subledger journals (XLA tables) and the general ledger (GL_JE_HEADERS, GL_JE_LINES, GL_BALANCES). The Subledger Accounting engine writes to XLA based on event evaluation, then transfers to GL via the Create Accounting and Post processes. Drill-back from GL line to source is mediated by the SLA engine. This is a more decoupled model than SAP's, with the trade-off that drill-back is mediated rather than direct.

**Microsoft Dynamics 365 F&O** uses GeneralJournalEntry and GeneralJournalAccountEntry tables as the unified ledger, with subledger transactions (CustTrans, VendTrans, InventTrans, AssetTrans) maintaining their own tables alongside. Posting layers tag each entry, allowing a single line to indicate which views (Current, Operations, Tax, Custom) it participates in. The posting layer concept is elegant for parallel accounting because it tags rather than duplicates.

**Oracle NetSuite** stores transactions in the `transaction` and `transactionline` tables, with GL impact in `transactionaccountingline`. The transaction record carries the operational view; the accountingline carries the GL view; they're joined by transaction reference. This separation makes drill-back from GL to source straightforward but creates the same kind of two-table pattern that SAP unified in S/4HANA.

The proposed schema synthesizes: SAP's unification principle (one place for the universal financial facts), adapted to Postgres's row-store nature via a moderately-narrow core with typed extensions; D365's posting-layer concept for parallel accounting (a single core row tagged with multiple layers); NetSuite's emphasis on visible source linkage (the source extension preserves the operational drill-back); and Oracle Fusion's event-classification model for downstream rule evaluation (event class and type as first-class fields on the core).

The deliberate departure from ACDOCA's maximally-wide approach reflects the database platform. ACDOCA works on HANA because column-store storage makes width effectively free — each column is independently stored, queries read only what they need, NULL columns cost essentially nothing. On Postgres row-store, every read drags the full row width through memory, every insert writes the full row width to WAL, and sparse columns waste space. The same data model has different optimal expressions on different storage engines. The narrow core with extensions is the Postgres-native expression of SAP's unification principle.

### 2.3 The Proposed Schema

The schema follows a **moderately-narrow core with typed extensions** pattern. The core table holds the universal facts that every query touches: identity, temporal, ledger, account, primary amount, and provenance. Extension tables hold the dimensions, multi-currency representations, source linkages, and inventory specifics that are only relevant to subsets of postings.

This is a deliberate departure from SAP's ACDOCA-style ultra-wide table. ACDOCA works on HANA because column-store storage makes width effectively free — each column is independently stored and queries read only what they need. On Postgres row-store, every read drags the full row width through memory, every insert writes the full row width to WAL, and sparse columns waste space and bandwidth. The moderately-narrow core preserves the unification benefit (one place for the universal financial facts) while pushing sparse and specialized data into extensions where it's only present when needed.

The design goals from Section 2.1 — schema-level consistency, multi-currency, multi-ledger, extensibility, append-only, performance — are all achieved by this structure. The core is the source of truth; extensions are typed, well-defined attachments that link 1:1 (or 1:0) to the core via the same primary key. A row in any extension exists if and only if that posting line has the relevant data; absence in the extension means absence of the data, not NULL.

#### 2.3.1 The core posting_lines table

```sql
CREATE TABLE posting_lines (
  -- =========================================================
  -- Identity
  -- =========================================================
  posting_id          BIGINT       NOT NULL,
  line_seq            INT          NOT NULL,

  -- =========================================================
  -- Temporal (universal — every query filters on date)
  -- =========================================================
  posting_date        DATE         NOT NULL,
  fiscal_year         SMALLINT     NOT NULL,
  fiscal_period       SMALLINT     NOT NULL,
  created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),

  -- =========================================================
  -- Event classification (universal — every line has these)
  -- =========================================================
  event_class         INT          NOT NULL,
  event_type          INT          NOT NULL,
  source_module       SMALLINT     NOT NULL,

  -- =========================================================
  -- Ledger and parallel accounting (universal — drives nearly every filter)
  -- =========================================================
  legal_entity_id     INT          NOT NULL,
  ledger_id           SMALLINT     NOT NULL,
  posting_layer       INT          NOT NULL,

  -- =========================================================
  -- Account (universal — every line posts to one account)
  -- =========================================================
  account_id          BIGINT       NOT NULL,

  -- =========================================================
  -- Primary amount (universal — functional currency, present on every posting)
  -- =========================================================
  amount_functional   NUMERIC(19,4) NOT NULL,
  currency_functional CHAR(3)      NOT NULL,

  -- =========================================================
  -- Rule provenance (universal — every line was produced by some rule)
  -- =========================================================
  rule_set_id         INT,
  rule_set_version    INT,

  -- =========================================================
  -- Idempotency and audit (universal)
  -- =========================================================
  idempotency_key     UUID         NOT NULL,
  created_by_user     INT          NOT NULL,

  -- =========================================================
  -- Constraints
  -- =========================================================
  PRIMARY KEY (posting_id, line_seq),
  CHECK (line_seq > 0),
  CHECK (currency_functional = UPPER(currency_functional))
) PARTITION BY RANGE (posting_date);

-- Monthly partitions (illustrative)
CREATE TABLE posting_lines_2026_05 PARTITION OF posting_lines
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
```

This is approximately **17 columns**, roughly 120-150 bytes per row including overhead. Every operational query that needs an account balance, a P&L slice by ledger and period, or a posting-by-account-and-date scan reads this table only — no joins required.

#### 2.3.2 Dimensions extension

Most postings populate two or three dimensions out of the dozen that the platform supports. Storing all dozen as typed columns on the core wastes space (NULL bitmap, alignment, header overhead per row) and bandwidth (every read pulls all twelve columns even when only one is referenced). The dimensions extension uses an EAV-style typed table that contains rows only for the dimensions a posting actually populates:

```sql
CREATE TABLE posting_line_dimensions (
  posting_id      BIGINT       NOT NULL,
  line_seq        INT          NOT NULL,
  posting_date    DATE         NOT NULL,    -- denormalized for partition alignment
  dimension_type  SMALLINT     NOT NULL,    -- references dimension_types lookup
  dimension_value BIGINT       NOT NULL,    -- the entity ID (cost_center_id, customer_id, etc.)
  
  PRIMARY KEY (posting_id, line_seq, dimension_type)
) PARTITION BY RANGE (posting_date);

-- Lookup table for dimension types (small, memory-resident)
CREATE TABLE dimension_types (
  dimension_type    SMALLINT PRIMARY KEY,
  name              VARCHAR(64) NOT NULL,    -- 'cost_center', 'profit_center', 'customer', etc.
  reference_table   VARCHAR(64) NOT NULL,    -- the entity table for this dimension
  description       TEXT
);
```

A posting line with cost center, profit center, and project produces three dimension rows. A simple cash adjustment with no operational dimensions produces zero. The total storage is proportional to dimensions actually populated, not to the universe of possible dimensions.

For very common dimensions where most postings populate them, the extension query cost adds up. The mitigation: covering indexes on `(dimension_type, dimension_value, posting_id, line_seq)` make dimension-keyed queries efficient, and the analytical replica materializes a denormalized wide view for reporting.

#### 2.3.3 Multi-currency extension

Most postings happen in the legal entity's functional currency. For these, the only amount needed is `amount_functional`, which lives on the core. Multi-currency data is needed only for the subset of postings that span currencies — vendor invoices in foreign currencies, customer payments in foreign currencies, intercompany postings with reporting currency consolidation, etc.

```sql
CREATE TABLE posting_line_currencies (
  posting_id            BIGINT       NOT NULL,
  line_seq              INT          NOT NULL,
  posting_date          DATE         NOT NULL,
  
  -- transaction currency (the original currency of the business event)
  amount_transaction    NUMERIC(19,4) NOT NULL,
  currency_transaction  CHAR(3)       NOT NULL,
  fx_rate_to_functional NUMERIC(19,9) NOT NULL,
  
  -- group reporting currency (when group consolidation requires it)
  amount_group          NUMERIC(19,4),
  fx_rate_to_group      NUMERIC(19,9),
  
  -- statutory local currency (rarely populated; for cases where local statutory ≠ functional)
  amount_local          NUMERIC(19,4),
  
  PRIMARY KEY (posting_id, line_seq),
  CHECK (currency_transaction = UPPER(currency_transaction))
) PARTITION BY RANGE (posting_date);
```

For a US-functional entity processing primarily USD transactions, this table holds rows only for foreign-currency postings — a small fraction of total volume. For a globally-distributed entity with significant cross-currency activity, this table grows but remains far smaller than if the four currency-related columns lived on every core row.

The convention: when this row is absent, the posting is in functional currency only (transaction currency = functional currency, group currency = functional currency translated at the entity's parent rate, no statutory local currency override). Reports that need full multi-currency detail join to this extension.

#### 2.3.4 Source linkage extension

Most postings reference a source document (an invoice, a fulfillment, a depreciation run). Some don't (manual journal entries with no source other than user intent). Source linkage is moderately sparse and well-suited to an extension:

```sql
CREATE TABLE posting_line_sources (
  posting_id              BIGINT       NOT NULL,
  line_seq                INT          NOT NULL,
  posting_date            DATE         NOT NULL,
  
  -- Source document
  source_doc_type         SMALLINT,
  source_doc_id           BIGINT,
  source_doc_line         INT,
  source_doc_external_ref VARCHAR(64),
  
  -- Relationships between postings
  reverses_line_id        BIGINT,                  -- original posting_id × 2^32 + line_seq
  parent_posting_id       BIGINT,                  -- for derivative postings
  intercompany_pair_id    UUID,                    -- cross-shard intercompany correlation
  
  -- Tier-2 WASM module provenance (when applicable)
  custom_module_hash      CHAR(64),
  
  -- Process attribution
  created_by_process      VARCHAR(64),             -- 'depreciation_run', 'allocation_run', etc.
  
  PRIMARY KEY (posting_id, line_seq)
) PARTITION BY RANGE (posting_date);
```

Auto-generated postings (depreciation, allocations, period-end revaluation) populate `created_by_process` and may have no source document. Reversals populate `reverses_line_id`. Intercompany pairs populate `intercompany_pair_id`. Manual journal entries may have all of these NULL.

For postings with no relationships and no source document (rare but possible), this row is omitted entirely. The dominant case — operational postings with a source document — is one extension row per core row.

#### 2.3.5 Inventory extension

Inventory and manufacturing postings carry quantity and unit-of-measure data; financial-only postings (interest accruals, allocations, journal entries) do not. Putting quantity on the core would mean ~80% of postings carry NULL quantity columns. The inventory extension holds these only when relevant:

```sql
CREATE TABLE posting_line_inventory (
  posting_id      BIGINT       NOT NULL,
  line_seq        INT          NOT NULL,
  posting_date    DATE         NOT NULL,
  
  product_id      BIGINT       NOT NULL,
  quantity        NUMERIC(19,6) NOT NULL,
  quantity_uom    VARCHAR(10)   NOT NULL,
  
  -- Costing-related fields used by costing subledger linkages
  unit_cost       NUMERIC(19,4),
  cost_layer_id   BIGINT,                          -- for FIFO/LIFO layered methods
  lot_id          BIGINT,                          -- for lot-tracked items
  unit_id         BIGINT,                          -- for serialized items
  
  PRIMARY KEY (posting_id, line_seq)
) PARTITION BY RANGE (posting_date);
```

This extension links the GL posting to the costing subledger (cost layers, lots, units described in the companion document on costing methods). Inventory-related queries join to this extension; non-inventory queries don't see it at all.

#### 2.3.6 Custom segments extension

For customer-defined dimensions that don't fit the standard `dimension_types` lookup — bespoke regulatory categories, internal management codes, integration-specific tags — the custom segments extension holds JSONB:

```sql
CREATE TABLE posting_line_custom (
  posting_id      BIGINT       NOT NULL,
  line_seq        INT          NOT NULL,
  posting_date    DATE         NOT NULL,
  custom_segments JSONB        NOT NULL,
  
  PRIMARY KEY (posting_id, line_seq)
) PARTITION BY RANGE (posting_date);

CREATE INDEX ON posting_line_custom USING GIN (custom_segments);
```

This row exists only when a posting has custom data. The vast majority of postings don't, and the row is absent. When present, GIN indexes support JSONB-path queries.

When customer-defined dimensions become heavily used and stable, they should graduate from JSONB into the typed `dimension_types` lookup with their own `dimension_type` value, moving from this extension to the structured `posting_line_dimensions` extension. This is a controlled migration, not an ad-hoc proliferation.

#### 2.3.7 The postings header table

The `postings` header table is unchanged from the prior design — it holds posting-level metadata that doesn't repeat on every line:

The `postings` header table is much smaller and holds posting-level metadata that doesn't need to repeat on every line:

```sql
CREATE TABLE postings (
  posting_id           BIGSERIAL PRIMARY KEY,
  posting_date         DATE         NOT NULL,
  fiscal_year          SMALLINT     NOT NULL,
  fiscal_period        SMALLINT     NOT NULL,
  legal_entity_id      INT          NOT NULL,
  document_type        SMALLINT     NOT NULL,
  event_class          INT          NOT NULL,
  event_type           INT          NOT NULL,
  source_module        SMALLINT     NOT NULL,
  source_doc_id        BIGINT,
  description          TEXT,
  reference            VARCHAR(64),
  created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
  created_by_user      INT          NOT NULL,
  created_by_process   VARCHAR(64),
  idempotency_key      UUID         NOT NULL,
  reversed_at          TIMESTAMPTZ,                  -- denormalized convenience; the reversal posting is the source of truth
  reversed_by_posting_id BIGINT,
  
  UNIQUE (source_module, idempotency_key)
);
```

Reference tables that the posting lines join to — `accounts`, `cost_centers`, `event_classes`, `event_types`, `ledgers`, etc. — are conventional and are not detailed here.

### 2.4 Field-by-Field Rationale

The schema accumulates design decisions, and each one has a reason. This section explains the choices that warrant explanation, organized by what's in the core versus what's in extensions.

#### 2.4.1 What lives in the core, and why

The core table holds fields that satisfy two criteria: **universal** (every posting has it) and **filterable** (queries frequently filter or group by it). Splitting these out to extensions would force joins on the hot path of every operational query.

**`posting_id` and `line_seq` as composite primary key.** A posting is a single journal entry — a unit that must balance debits to credits. It contains one or more lines. The composite key reflects this hierarchy. The `posting_id` is a `BIGINT`, sized for the long term: at 100 million postings per year, a `BIGINT` lasts approximately 92 billion years. The `line_seq` is a small integer because postings rarely have more than a few hundred lines. Every extension table uses this same composite key as its primary key, ensuring 1:1 (or 1:0) relationships and clean joins.

**`posting_date` as the bi-temporal effective time, plus `created_at` for transaction time.** `posting_date` is when the economic event is deemed to have occurred — December 31 for a December accrual, even if posted on January 5. `created_at` is when the system recorded the line. Most reports use `posting_date`; audit and restatement analysis sometimes need `created_at`. Both are universal; both are kept on the core. `posting_date` also serves as the partition key for the core and for every extension, ensuring partition-aligned joins.

**`fiscal_year` and `fiscal_period` denormalized.** These could be derived from `posting_date` via a function, but they appear in nearly every analytical query. Denormalizing avoids the function call cost and lets the query optimizer use them directly. The risk of inconsistency with the fiscal calendar is mitigated by computing them at insert time from a centralized fiscal-calendar service.

**`event_class` and `event_type` as integer references.** Every posting derives from a specific business event with a specific lifecycle state. `event_class` might be "vendor_invoice" or "asset_depreciation"; `event_type` distinguishes "validated" from "adjusted" from "canceled". Integer references are used rather than string codes to keep row size down — at billions of rows, the difference between a 4-byte int and a 32-byte string compounds significantly. The lookup tables are small and memory-resident.

**`source_module` denormalized.** Indicates which subledger originated the posting (AP, AR, INV, FA, GL, PROJ, PAYROLL). Reports and access controls frequently filter by source module; keeping it on the core avoids joining to the event_classes lookup table for this single attribute.

**`legal_entity_id` as the shard key.** This is the most consequential decision in the schema. Sharding is by legal entity because legal entity is the boundary that bounds postings: a single posting belongs to exactly one legal entity. Cross-entity transactions (intercompany) are explicitly modeled as two separate postings. With this shard key, the vast majority of operational queries hit one shard.

**`ledger_id` and `posting_layer` as separate concepts.** This is subtle and worth understanding. `ledger_id` distinguishes parallel ledgers in the SAP sense — leading ledger (0L, typically IFRS) and non-leading ledgers (2L for US GAAP, 3L for tax). Each ledger receives complete postings; an entry that's relevant to both 0L and 2L produces two rows, one per ledger, with potentially different amounts (e.g., different depreciation methods). `posting_layer` is the D365 concept — a bitmask indicating which "views" a single line participates in. A single ledger entry might be tagged as participating in Current and Operations layers; a tax adjustment might be tagged Tax-only. Layers don't duplicate rows; they tag them.

Why both? Because they serve different needs. Parallel ledgers handle situations where the *amount* differs by accounting standard; posting layers handle situations where the *inclusion* differs. Pure D365-style layering can't easily handle different amounts per standard; pure SAP-style parallel ledgers can't easily handle inclusion-only filtering. Combining them gives both capabilities, and both are universal — every posting has both.

**`account_id` as the only inline account reference.** Earlier designs included `account_natural` (the human-readable code like "1100") denormalized alongside `account_id`. The natural code is convenient but not necessary on the hot path — the analytical replica can join to the `accounts` table. Keeping the core narrow means queries that need the natural code pay one join; queries that don't never see it.

**`amount_functional` and `currency_functional` inline.** Functional currency is the legal entity's primary reporting currency. Every posting line has an amount in functional currency (zero for memo lines, but never NULL). This is the universal monetary representation, and balance verification (debits equal credits per ledger per layer) operates on it. Currency code is denormalized as a 3-character ISO code rather than a foreign key to a currency table — the values are stable, the lookup is unnecessary.

**`rule_set_id` and `rule_set_version` for provenance.** Every posting line was produced by some rule (or by manual entry, in which case these are NULL). Knowing which rule produced which line is essential for debugging "why did this post to this account?" The version tag allows tracing through historical rule changes — the rule active when this line was posted may differ from the current rule.

**`idempotency_key` denormalized to lines.** The idempotency key is fundamentally per-posting (the unit of exactly-once delivery), but denormalizing it to the line level makes lookups efficient when investigating duplicate-detection scenarios. The unique constraint that enforces idempotency lives on the `postings` header table.

**`created_by_user` inline.** Audit-essential and universal — every line has a user (or system process attribution, but a system process still has an associated user identity).

#### 2.4.2 What's in extensions, and why

Extensions hold data that fails one or both of the core's criteria — not universal (some postings don't have it) or not commonly filtered (queries don't typically filter by it directly).

**Dimensions go to `posting_line_dimensions`.** A single posting line populates 0 to 5 dimensions out of the dozen the platform supports. Storing all twelve as typed columns wastes space (12 NULL bytes plus per-column overhead per row) and bandwidth (every read pulls all twelve). The EAV-style typed dimension table holds rows only for dimensions actually populated. Common queries like "P&L by cost center" join to this extension filtering on `dimension_type = cost_center`; the join is one indexed lookup per row of the result set.

The `dimension_type` lookup table maps integer codes to dimension semantics. Adding a new dimension is data-only (a new lookup row, not a schema change). This is the platform's native extensibility for known dimensions.

**Multi-currency goes to `posting_line_currencies`.** Most postings are in functional currency only — `amount_functional` on the core is the complete picture. Multi-currency representations matter only for postings that span currencies. For a US-functional entity, that's a small fraction of total volume; for an internationally-active entity, it's larger. Either way, putting four currency-related columns on every row when most rows don't need them is wasteful.

When the extension row is absent, the convention is: transaction currency = functional currency, group currency = computed via standard translation, no statutory local currency override. When present, the extension carries the actual values used at the time of posting (immutable, captured at insert time, never updated).

**Source linkage goes to `posting_line_sources`.** Most operational postings reference a source document. Some postings (manual journal entries, certain adjustment postings) don't. The source extension also carries `reverses_line_id`, `parent_posting_id`, and `intercompany_pair_id` — relationships among postings that are sparse (only reversal entries have `reverses_line_id`; only intercompany pairs have `intercompany_pair_id`).

For audit, the extension is critical — drill-back from GL line to source document goes through this table. For most operational queries, the extension isn't touched. The split keeps the core fast for non-audit workloads.

**Inventory data goes to `posting_line_inventory`.** Inventory and manufacturing postings carry product, quantity, and unit-of-measure; financial-only postings (interest accruals, allocations, journal entries) don't. Inventory is roughly 10-30% of postings in mixed-business scenarios; for pure-financial businesses (insurance, banking) it's near zero. Putting these columns on the core would add 30+ bytes per row to ~70-90% of postings that don't use them.

This extension also carries the linkage to the costing subledger — `cost_layer_id`, `lot_id`, `unit_id` — that's covered in detail in the companion document on costing methods.

**Custom segments go to `posting_line_custom`.** When customers need dimensions that don't fit the standard `dimension_types` lookup, JSONB in this extension handles them. Most postings don't have custom segments; the row is absent. When present, GIN indexes support JSONB-path queries.

#### 2.4.3 The relationships among postings

A few relationship fields warrant separate explanation since they cross the core/extension boundary in interesting ways.

**`reverses_line_id` (in source extension).** Append-only means reversals create new lines. The new line points to the original via `reverses_line_id`. This makes "is this line reversed?" answerable by an outer join. Multiple reversals chain through this field.

**`parent_posting_id` (in source extension).** Some postings derive from others — an allocation entry derives from a source cost entry, a settlement derives from a WIP accumulation, an intercompany mirror derives from an originating sale. `parent_posting_id` makes these relationships explicit. This matters for reporting and audit.

**`intercompany_pair_id` as a UUID (in source extension).** Intercompany postings come in pairs across legal entities (and therefore across shards). Each side is a complete posting in its own legal entity. A shared UUID, generated at the originating-system level, lets the consolidation database match them. UUIDs are used rather than sequence values because they can be generated client-side without coordination across shards.

#### 2.4.4 Why this division is the right one for Postgres

The split between core and extensions reflects Postgres-specific economics:

- **Row-store storage** means every read pulls full row width; narrow rows are faster to read and write
- **Append-only operation** means each insert generates WAL proportional to row size; smaller rows = less WAL = higher write throughput
- **Sparse columns waste space** — Postgres's null bitmap reduces the cost but doesn't eliminate it
- **Schema evolution** is easier on small tables; the core stays stable, extensions evolve independently
- **The hot path is the core**; extensions are touched only when their data is needed, and even then only via partition-aligned joins

The earlier maximally-wide ACDOCA-style design fights Postgres's row-store nature. The moderately-narrow core preserves the unification benefit (universal facts in one place) while letting Postgres do what it's good at.

This design would still be wrong on HANA — there, columnar storage makes a wider table strictly better. The architecture is platform-aware. SAP's choices reflect HANA; this design's choices reflect Postgres.

### 2.5 Partitioning, Sharding, and Indexes

The core and all extensions partition by `posting_date` at monthly granularity. Monthly is the right cadence: it matches fiscal calendars, keeps individual partitions small enough for fast index maintenance (tens to hundreds of millions of rows), and gives a natural archival boundary.

Aligned partitioning across all tables is essential for query performance. When the core's May 2026 partition is queried with a date filter, the query planner must be able to prune corresponding partitions in extensions. This is why every extension has `posting_date` denormalized — it enables partition-aligned joins.

```sql
-- Core partitions
CREATE TABLE posting_lines_2026_05 PARTITION OF posting_lines
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

-- Aligned extension partitions (one per extension table)
CREATE TABLE posting_line_dimensions_2026_05 PARTITION OF posting_line_dimensions
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE posting_line_currencies_2026_05 PARTITION OF posting_line_currencies
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE posting_line_sources_2026_05 PARTITION OF posting_line_sources
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE posting_line_inventory_2026_05 PARTITION OF posting_line_inventory
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
```

Within a partition, the row organization is insertion order — Postgres heap storage. With append-only, rows for a given period are physically clustered in time order, helping cache locality for time-bounded queries.

#### 2.5.1 Sub-partitioning by ledger

For very large entities, sub-partitioning by `ledger_id` adds another level of pruning. Most operational queries scope to a single ledger (the leading ledger), so this gives meaningful speedup at the cost of more partitions to manage:

```sql
CREATE TABLE posting_lines_2026_05 PARTITION OF posting_lines
  FOR VALUES FROM ('2026-05-01') TO ('2026-06-01')
  PARTITION BY LIST (ledger_id);

CREATE TABLE posting_lines_2026_05_l0 PARTITION OF posting_lines_2026_05 FOR VALUES IN (0);
CREATE TABLE posting_lines_2026_05_l1 PARTITION OF posting_lines_2026_05 FOR VALUES IN (1);
```

Extensions don't need to sub-partition by ledger because joins from a ledger-pruned core to an extension naturally restrict to the relevant rows; the extra pruning would help less than it costs in operational complexity.

#### 2.5.2 Sharding strategy

Sharding is at the legal-entity boundary. Each legal entity (or small group of related entities, sized to balance shard count against per-shard load) lives on its own Postgres cluster: primary, streaming replicas, and connection to the analytical replica. Cross-shard queries are not part of the normal operational path; they happen in the consolidation database fed by replication.

#### 2.5.3 Index strategy on the OLTP primary

Indexes are kept minimal on the OLTP primary. Append-only operation removes the worst index-maintenance cost (no update-induced churn), but every index still costs something on insert. The narrow core needs fewer indexes because most dimension-keyed queries hit extensions:

```sql
-- Core indexes

-- BRIN on posting_date: very cheap, append-friendly, time-range pruning
CREATE INDEX ON posting_lines USING BRIN (posting_date) WITH (pages_per_range = 32);

-- B-tree for the most common operational query: account balance over a period
CREATE INDEX ON posting_lines (account_id, fiscal_year, fiscal_period);

-- B-tree for posting-id lookup (drill-back from any other table)
-- This is implicit via the primary key; no separate index needed

-- B-tree for ledger-scoped queries (most operational queries scope to leading ledger)
CREATE INDEX ON posting_lines (ledger_id, account_id, posting_date);

-- B-tree for event-class-driven queries (e.g., audit by event type)
CREATE INDEX ON posting_lines (event_class, posting_date);
```

The index count is much smaller than the prior wide design because the dimension-keyed indexes (customer_id, vendor_id, asset_id, etc.) live on the dimensions extension instead.

#### 2.5.4 Index strategy on extensions

Each extension carries its own indexes optimized for its access patterns:

```sql
-- Dimensions extension: lookup by dimension type and value
CREATE INDEX ON posting_line_dimensions (dimension_type, dimension_value, posting_date);
CREATE INDEX ON posting_line_dimensions USING BRIN (posting_date);

-- Currencies extension: lookup by transaction currency for FX revaluation queries
CREATE INDEX ON posting_line_currencies (currency_transaction, posting_date)
  WHERE currency_transaction IS NOT NULL;

-- Sources extension: drill-back from operational document
CREATE INDEX ON posting_line_sources (source_doc_type, source_doc_id);

-- Sources extension: reversal traceability
CREATE INDEX ON posting_line_sources (reverses_line_id) 
  WHERE reverses_line_id IS NOT NULL;

-- Sources extension: intercompany correlation
CREATE INDEX ON posting_line_sources (intercompany_pair_id)
  WHERE intercompany_pair_id IS NOT NULL;

-- Inventory extension: product-keyed queries
CREATE INDEX ON posting_line_inventory (product_id, posting_date);
CREATE INDEX ON posting_line_inventory (cost_layer_id) WHERE cost_layer_id IS NOT NULL;
CREATE INDEX ON posting_line_inventory (lot_id) WHERE lot_id IS NOT NULL;
CREATE INDEX ON posting_line_inventory (unit_id) WHERE unit_id IS NOT NULL;

-- Custom segments: GIN for JSONB queries
CREATE INDEX ON posting_line_custom USING GIN (custom_segments);
```

The use of BRIN for `posting_date` on every table is deliberate. BRIN indexes store one entry per range of pages, recording the min/max values in that range. They're tiny — kilobytes for tables of billions of rows — and append-friendly because new pages just extend the index. For append-only tables clustered in date order, BRIN on the date column is dramatically more efficient than B-tree and serves the partition-pruning purpose well.

#### 2.5.5 The replica's role

The OLTP primary stays lean. The analytical replica gets a different index set and storage strategy:

- Wider analytical indexes (covering, multi-column for common report queries)
- Materialized denormalized views joining the core to extensions for fast wide-table reads
- Columnar storage on the largest tables (Citus columnar extension or Hydra)

A typical materialized denormalized view on the replica:

```sql
-- Materialized view that recreates the wide-row appearance for analytics
CREATE MATERIALIZED VIEW posting_lines_denormalized AS
SELECT
  pl.posting_id, pl.line_seq, pl.posting_date, pl.fiscal_year, pl.fiscal_period,
  pl.legal_entity_id, pl.ledger_id, pl.posting_layer,
  pl.account_id, a.natural_code AS account_natural,
  pl.amount_functional, pl.currency_functional,
  pl.event_class, pl.event_type, pl.source_module,
  -- pivoted dimensions
  MAX(CASE WHEN d.dimension_type = 1 THEN d.dimension_value END) AS cost_center_id,
  MAX(CASE WHEN d.dimension_type = 2 THEN d.dimension_value END) AS profit_center_id,
  MAX(CASE WHEN d.dimension_type = 3 THEN d.dimension_value END) AS customer_id,
  MAX(CASE WHEN d.dimension_type = 4 THEN d.dimension_value END) AS vendor_id,
  MAX(CASE WHEN d.dimension_type = 5 THEN d.dimension_value END) AS asset_id,
  MAX(CASE WHEN d.dimension_type = 6 THEN d.dimension_value END) AS project_id,
  -- multi-currency
  c.amount_transaction, c.currency_transaction, c.amount_group,
  -- source
  s.source_doc_type, s.source_doc_id, s.source_doc_external_ref,
  s.reverses_line_id, s.intercompany_pair_id,
  -- inventory
  i.product_id, i.quantity, i.quantity_uom
FROM posting_lines pl
LEFT JOIN posting_line_dimensions d ON (pl.posting_id, pl.line_seq) = (d.posting_id, d.line_seq)
LEFT JOIN posting_line_currencies c ON (pl.posting_id, pl.line_seq) = (c.posting_id, c.line_seq)
LEFT JOIN posting_line_sources s ON (pl.posting_id, pl.line_seq) = (s.posting_id, s.line_seq)
LEFT JOIN posting_line_inventory i ON (pl.posting_id, pl.line_seq) = (i.posting_id, i.line_seq)
LEFT JOIN accounts a ON pl.account_id = a.account_id
GROUP BY pl.posting_id, pl.line_seq, pl.posting_date, pl.fiscal_year, pl.fiscal_period,
  pl.legal_entity_id, pl.ledger_id, pl.posting_layer, pl.account_id, a.natural_code,
  pl.amount_functional, pl.currency_functional, pl.event_class, pl.event_type, pl.source_module,
  c.amount_transaction, c.currency_transaction, c.amount_group,
  s.source_doc_type, s.source_doc_id, s.source_doc_external_ref,
  s.reverses_line_id, s.intercompany_pair_id,
  i.product_id, i.quantity, i.quantity_uom;
```

This materialized view recreates the appearance of the wide-row design for analytical consumers — a single table to query — while the OLTP primary stays narrow. The materialization is refreshed via incremental ETL (CDC or batch refresh) with bounded staleness acceptable for analytical workloads.

The result: writes go to a narrow, fast OLTP schema; reads come from a wide, denormalized materialization. Each path is optimized for its workload. This is the canonical pattern for "command-query separation" applied to ledger architecture.

### 2.6 Append-Only Enforcement

Append-only is the foundational invariant, applied to the core and to every extension table. It must be enforced, not assumed.

**Permission-level enforcement.** Application users get INSERT only, never UPDATE or DELETE — across all tables:

```sql
-- Apply to core
REVOKE UPDATE, DELETE ON posting_lines FROM application_role;
REVOKE UPDATE, DELETE ON postings FROM application_role;
GRANT INSERT, SELECT ON posting_lines TO application_role;
GRANT INSERT, SELECT ON postings TO application_role;

-- Apply to every extension table
REVOKE UPDATE, DELETE ON posting_line_dimensions FROM application_role;
REVOKE UPDATE, DELETE ON posting_line_currencies FROM application_role;
REVOKE UPDATE, DELETE ON posting_line_sources FROM application_role;
REVOKE UPDATE, DELETE ON posting_line_inventory FROM application_role;
REVOKE UPDATE, DELETE ON posting_line_custom FROM application_role;
GRANT INSERT, SELECT ON posting_line_dimensions TO application_role;
GRANT INSERT, SELECT ON posting_line_currencies TO application_role;
GRANT INSERT, SELECT ON posting_line_sources TO application_role;
GRANT INSERT, SELECT ON posting_line_inventory TO application_role;
GRANT INSERT, SELECT ON posting_line_custom TO application_role;
```

Only a separate administrative role (used for partition management, archival, and exceptional corrections via well-controlled procedures) has UPDATE/DELETE on any of these tables.

**Trigger-level enforcement (defense in depth).** Even with permission control, trigger-based enforcement guards against permission misconfigurations:

```sql
CREATE OR REPLACE FUNCTION prevent_update_delete() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION '% is append-only (% blocked)', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

-- Apply to core and every extension
CREATE TRIGGER prevent_update_delete BEFORE UPDATE OR DELETE ON posting_lines
  FOR EACH ROW EXECUTE FUNCTION prevent_update_delete();
CREATE TRIGGER prevent_update_delete BEFORE UPDATE OR DELETE ON posting_line_dimensions
  FOR EACH ROW EXECUTE FUNCTION prevent_update_delete();
-- ... etc for each extension
```

**Reversals as new rows.** When a posting needs to be undone, the system inserts new rows that mirror the originals with negated amounts and a `reverses_line_id` reference (in the source extension):

```
Original posting (12345):
  posting_lines: line 1 acct=1100 amount=+100; line 2 acct=2100 amount=-100
  posting_line_sources: line 1, line 2 (with source_doc references)

Reversal posting (67890):
  posting_lines: line 1 acct=1100 amount=-100; line 2 acct=2100 amount=+100
  posting_line_sources: line 1 reverses_line_id=(12345,1); line 2 reverses_line_id=(12345,2)
```

The reversal creates new core rows AND new extension rows that mirror the original's extensions (where appropriate). The originals are unchanged. "Has this been reversed?" is answerable by querying `posting_line_sources.reverses_line_id`.

**Corrections via reverse-and-replace.** A line that needs to be changed gets reversed, and a new (correct) line is inserted with its own complete set of core and extension rows. The audit trail shows: original line, reversal of original, new line. The economic effect of the original is undone; the corrected effect is recorded; nothing is lost.

This pattern conflicts mildly with how some users think about "fixing" a wrong entry — they want to edit the original. The system explicitly does not allow this. The cost is that an entry mistyped during entry generates three audit lines instead of one. The benefit is that the audit trail is permanent and reconstructible.

### 2.7 Invariants at the Database Layer

Beyond append-only, several invariants must hold for the ledger to be correct. The architecture enforces them at the database level via constraints and deferred-constraint triggers, providing defense-in-depth even when the application has bugs.

**Double-entry balance per posting per ledger per layer.** Sum of `amount_functional` across all lines of a posting, grouped by ledger_id, must be zero. This is the foundational accounting invariant.

```sql
CREATE OR REPLACE FUNCTION check_posting_balanced() RETURNS trigger AS $$
DECLARE
  unbalanced RECORD;
BEGIN
  FOR unbalanced IN
    SELECT ledger_id, SUM(amount_functional) AS net
    FROM posting_lines
    WHERE posting_id = NEW.posting_id
    GROUP BY ledger_id
    HAVING SUM(amount_functional) <> 0
  LOOP
    RAISE EXCEPTION 'Posting % unbalanced in ledger %: net=%',
      NEW.posting_id, unbalanced.ledger_id, unbalanced.net;
  END LOOP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER posting_balance_check
  AFTER INSERT ON posting_lines
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION check_posting_balanced();
```

The `DEFERRABLE INITIALLY DEFERRED` is essential: it means the check fires at transaction commit, not on each row insert. The application can insert all the lines of a posting in one transaction; the trigger validates the whole posting at commit time.

Note this only checks `amount_functional` — the functional currency is the legal entity's reporting currency and is the invariant that must always hold. Transaction currency may not balance (e.g., a multi-currency journal that converts at differing rates produces a tiny FX difference that's posted to a rounding account in functional currency). Group currency balance is an analytical concern, not a transactional one.

**Period not closed for new postings.** A posting cannot land in a closed period:

```sql
CREATE OR REPLACE FUNCTION check_period_open() RETURNS trigger AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM closed_periods
    WHERE legal_entity_id = NEW.legal_entity_id
      AND ledger_id = NEW.ledger_id
      AND fiscal_year = NEW.fiscal_year
      AND fiscal_period = NEW.fiscal_period
  ) THEN
    RAISE EXCEPTION 'Period %-%-% closed for ledger % (entity %)',
      NEW.fiscal_year, NEW.fiscal_period, NEW.legal_entity_id,
      NEW.ledger_id, NEW.legal_entity_id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER posting_period_check
  BEFORE INSERT ON posting_lines
  FOR EACH ROW EXECUTE FUNCTION check_period_open();
```

The `closed_periods` table is small and lookup is cheap. Enforcement at the database level guards against application bugs that fail to check period status.

**Account exists and is postable.** Foreign key to `accounts`, plus check that the account is currently active:

```sql
ALTER TABLE posting_lines ADD CONSTRAINT fk_account
  FOREIGN KEY (account_id) REFERENCES accounts(account_id);

CREATE OR REPLACE FUNCTION check_account_postable() RETURNS trigger AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM accounts
    WHERE account_id = NEW.account_id
      AND status = 'active'
      AND posting_allowed = TRUE
      AND NEW.posting_date BETWEEN effective_from AND COALESCE(effective_to, '9999-12-31')
  ) THEN
    RAISE EXCEPTION 'Account % not postable as of %', NEW.account_id, NEW.posting_date;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

**Idempotency.** The unique constraint on `(source_module, idempotency_key)` in the `postings` header prevents duplicate postings from retries:

```sql
ALTER TABLE postings ADD CONSTRAINT uq_idempotency
  UNIQUE (source_module, idempotency_key);
```

INSERT with `ON CONFLICT (source_module, idempotency_key) DO NOTHING` returns the existing posting on retry, never creating a duplicate.

**Extension consistency.** Each extension table has the same composite primary key `(posting_id, line_seq)` as the core. This guarantees that any extension row is unique per core row and that joins are 1:1. Foreign key constraints from extension to core are an option but are typically omitted for performance — the application enforces atomic writes via single-transaction inserts to core and extensions, and reconciliation jobs verify integrity periodically:

```sql
-- Daily reconciliation: every extension row has a matching core row
SELECT 'orphaned dimension rows' AS issue, COUNT(*)
FROM posting_line_dimensions d
LEFT JOIN posting_lines pl USING (posting_id, line_seq)
WHERE pl.posting_id IS NULL;

-- (and similarly for other extension tables)
```

The trade-off: foreign keys would catch this at insert time but add per-row verification cost on the hot write path. Periodic reconciliation gives the same guarantee with bounded staleness, at lower steady-state cost.

### 2.8 Critical Analysis — What This Schema Gives Up

The schema makes specific trade-offs. Naming them honestly is essential.

**Multi-table writes per posting line.** A single posting line typically writes 2-5 rows: one to the core, plus one to each relevant extension (dimensions are often 2-3 rows, currencies if foreign, sources for traceability, inventory if applicable). The total WAL volume per posting is comparable to a wide-row design, but the number of write statements is higher. Mitigated by batching all writes for a posting into a single transaction, but it does increase per-transaction lock acquisitions. For typical accounting workloads (thousands of postings per second per shard, not millions), this is well within Postgres's capability; for extreme write volumes, the wide-row design might actually be faster despite its other drawbacks.

**Dimension queries require joins.** "P&L by cost center" cannot be answered from the core alone — it needs `posting_line_dimensions` joined on `(posting_id, line_seq)` filtered by `dimension_type=cost_center`. On the OLTP primary this is one indexed lookup per matching row; on the analytical replica it's served by the materialized denormalized view. Either way, queries that touched typed columns directly under the wide-row design now incur join cost. For analytical workloads served by the replica's materialized views, the cost is amortized once at materialization time; for ad-hoc OLTP queries, it's a real per-query cost.

**Atomic write integrity depends on the application.** With multiple tables involved per posting line, the write atomicity is per-transaction (not per-statement). If the application commits the core but not the extensions (e.g., due to a connection failure between statements), the database is in an inconsistent state. Postgres's transactional semantics prevent this within a single connection's transaction, but the application must explicitly group all writes for a posting into one BEGIN/COMMIT block. This is a discipline issue, not a database issue, but it's a real operational requirement.

**Cross-shard queries remain slow.** The narrow-core design doesn't change the cross-shard story. Consolidation reports that aggregate across all legal entities still require either federated queries or a separate consolidation database fed by replication. SAP avoids this by running everything on one HANA system; this design handles it only via additional infrastructure.

**Schema migrations on the core are still operational events, but extension migrations are easier.** Adding a column to the core requires the full multi-billion-row partition-by-partition migration. But adding a new dimension type is just a row in `dimension_types` — no DDL. Adding a new extension table is creating a new (initially small) table — no impact on existing tables. Adding new fields to existing extensions is a partition-by-partition migration of just that extension, smaller than the core. The narrow-core design moves most schema evolution to small extension tables where it's operationally cheap.

**Posting layers as a bitmask have limits.** A single line can be tagged with multiple layers, but cannot have *different amounts* per layer. If a transaction has different amounts under IFRS vs. GAAP, that requires parallel ledgers (two core rows), not posting layers. The schema supports both — `ledger_id` for amount-divergent cases, `posting_layer` for inclusion-divergent cases — but the user-facing model is more complex than either alone. Distinguishing "use parallel ledger" from "use posting layer" is a design choice that has to be made for each accounting standard divergence, and customers will get it wrong.

**JSONB extensibility in `posting_line_custom` has query costs.** Custom segments in JSONB are slower to query than typed columns, even with GIN indexes. The recommended migration path — frequently-used custom segments graduate to typed `dimension_types` entries — provides a clean evolution, but the migration itself is a controlled data move (moving rows from custom-segment JSONB to typed dimension rows). Less painful than a wide-table column migration, but not free.

**The reference data dependency is unchanged.** Every posting joins to `accounts` (account_id), `dimension_types`, etc. for any meaningful reporting. Reference data is small per legal entity but globally substantial; it must be replicated to the analytics replica and the consolidation database. Stale reference data causes incorrect reports. This is not a schema issue per se but an architectural cost easy to underestimate.

**Append-only generates more rows than mutable storage.** A correction that would be a single UPDATE in a mutable design is multiple rows here (original core + extensions, reversal core + extensions, replacement core + extensions). The narrow-core design slightly amplifies this (more rows per "logical entry") compared to the wide-row design. The audit value is worth it for accounting; it's not free.

**Operational discipline is mandatory, not optional.** Every constraint above can be bypassed by an administrator with sufficient permissions, by a poorly-configured replication path, or by a partition-management operation that goes wrong. The schema enforces what it can; the operations team enforces the rest. Backup integrity, replication monitoring, partition management, access-control auditing, and now per-extension reconciliation are all part of "the design" in a way that they aren't for a system you buy.

#### 2.8.1 What this design gains compared to a maximally wide design

It's worth being explicit about what this design improves over the prior ACDOCA-style approach:

- **Smaller core rows** (~150 bytes vs ~400 bytes): faster reads, less WAL per write, more rows per buffer page
- **No NULL waste**: extensions exist only when their data exists; sparse fields don't pay rent on every row
- **Easier schema evolution**: extensions evolve independently; new dimensions are configuration, not DDL
- **Better Postgres-native fit**: row-store and column-store have opposite optimal designs; this design respects Postgres's row-store nature

#### 2.8.2 What this design loses compared to a maximally wide design

And what's been given up:

- **Joins on the OLTP primary** for dimension-keyed queries (mitigated by the analytical replica's materialized view, but it's a real cost on the primary)
- **More transaction statements per posting** (mitigated by single-transaction batching, but it's a real cost)
- **Application-level atomicity dependency** (the database doesn't enforce that all extensions are written together; the application does)
- **Slightly more reconciliation surface area** (each extension needs orphan-row reconciliation against the core)

The honest framing is: **the moderately-narrow core is a better fit for Postgres than the maximally-wide alternative**. The same design would be wrong on HANA — there, columnar storage makes width effectively free, and consolidating into ACDOCA's mega-table is the right answer. Architecture is platform-aware. The right Postgres design is not the right HANA design.

For organizations with the option of buying SAP and running on HANA, that's likely the better path for genuinely high-volume accounting. For organizations building on Postgres, the narrow-core extension design is the most defensible Postgres-native choice — better than mimicking ACDOCA on a database whose storage engine works against the design.

---

## Part II — The Two-Tier Rules Engine

### 3.1 Why Two Tiers

The schema in Part I describes how postings are stored. It says nothing about how they are produced. A vendor invoice arrives in the AP subledger; the system must determine which accounts to debit and credit, in which currencies, with which dimensions, on which ledgers. This is rule evaluation, and it is where ERP systems accumulate their complexity.

Customer rule needs follow a power-law distribution. Most needs are variations of a small set of patterns: vendor invoice produces accrual debit and AP credit; customer invoice produces AR debit and revenue credit; depreciation produces expense debit and accumulated-depreciation credit. The exact accounts, dimensions, and tax treatments vary, but the structure is consistent. A typed configuration model handles these well — accountants can read and edit the rules, audit trails are clear, performance is excellent.

A long tail of needs requires more flexibility. Brazilian withholding tax has jurisdiction-specific calculations that cascade across multiple authorities. German VAT has special handling for triangulation. Specific industries have allocation methods that don't fit standard models — broadcasting industry's amortization of programming rights, oil-and-gas industry's depletion accounting, healthcare's revenue recognition under variable-consideration contracts. These cases require Turing-complete logic. Pushing them into configuration produces a DSL that nobody can read; pushing every customer's simple needs into code produces unmaintainable boilerplate.

The two-tier solution: configuration handles the 80%, code handles the 20%, both produce posting lines that flow through the same validation and persistence path. The architectural commitment is that both tiers are first-class — neither is an afterthought, both are testable, both are version-controlled, both have audit trails.

### 3.2 Tier 1: Configuration-Based Rules

The configuration tier stores rules as typed data in database tables. A rule evaluation engine, written in application code, reads these rules and evaluates them against incoming events. The expressiveness of the configuration determines how many cases stay in tier 1 vs. escape to tier 2 — the richer the configuration, the smaller the WASM tier becomes, the less audit overhead and operational complexity the platform carries.

#### 3.2.1 The Rule Schema

The core idea is a hierarchy: **rule sets** group rules for an event class; **journal line rules** specify what gets posted; **account rules** derive accounts; **mapping sets** are lookup tables that account rules use.

```sql
CREATE TABLE rule_sets (
  rule_set_id      BIGSERIAL PRIMARY KEY,
  name             VARCHAR(128) NOT NULL,
  event_class_id   INT NOT NULL REFERENCES event_classes,
  ledger_id        SMALLINT NOT NULL,
  effective_from   DATE NOT NULL,
  effective_to     DATE,
  status           VARCHAR(16) NOT NULL CHECK (status IN ('draft','approved','active','retired')),
  version          INT NOT NULL,
  created_by       INT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  approved_by      INT,
  approved_at      TIMESTAMPTZ,
  UNIQUE (event_class_id, ledger_id, effective_from, version)
);

CREATE TABLE journal_line_rules (
  rule_id          BIGSERIAL PRIMARY KEY,
  rule_set_id      BIGINT NOT NULL REFERENCES rule_sets,
  line_seq         INT NOT NULL,
  description      TEXT,
  
  -- Condition: when does this line fire?
  condition        JSONB,           -- typed AST, NULL = always
  
  -- Side and account derivation
  side             CHAR(1) NOT NULL CHECK (side IN ('D','C')),
  account_rule_id  BIGINT NOT NULL REFERENCES account_rules,
  
  -- Amount source
  amount_source    JSONB NOT NULL,  -- typed expression: e.g., {"event":"subtotal"} or {"compute":"subtotal*0.1"}
  amount_currency  VARCHAR(20) NOT NULL,  -- 'transaction' / 'functional' / 'fixed:USD'
  
  -- Posting layers this line participates in (bitmask)
  posting_layer_mask INT NOT NULL DEFAULT 1,
  
  -- Dimension assignment
  dimension_rules  JSONB,           -- which event attributes map to which posting dimensions
  
  UNIQUE (rule_set_id, line_seq)
);

CREATE TABLE account_rules (
  account_rule_id  BIGSERIAL PRIMARY KEY,
  name             VARCHAR(128) NOT NULL,
  derivation_type  VARCHAR(32) NOT NULL CHECK (derivation_type IN
                     ('constant','event_attribute','mapping_set','compound')),
  
  -- For 'constant': a specific account
  constant_account_id BIGINT REFERENCES accounts,
  
  -- For 'event_attribute': pull from event payload (e.g., event.expense_account_id)
  event_attribute_path JSONB,
  
  -- For 'mapping_set': lookup with optional fallback
  mapping_set_id   BIGINT REFERENCES mapping_sets,
  fallback_action  VARCHAR(16) CHECK (fallback_action IN ('error','default','null')),
  fallback_account_id BIGINT REFERENCES accounts,
  
  -- For 'compound': nested rules with conditions
  compound_rules   JSONB
);

CREATE TABLE mapping_sets (
  mapping_set_id   BIGSERIAL PRIMARY KEY,
  name             VARCHAR(128) NOT NULL,
  source_columns   TEXT[] NOT NULL,    -- e.g., ['cost_center_code', 'natural_account']
  target_account   BOOLEAN NOT NULL DEFAULT TRUE   -- whether the target is an account_id
);

CREATE TABLE mapping_set_rows (
  mapping_set_id   BIGINT NOT NULL REFERENCES mapping_sets,
  source_values    JSONB NOT NULL,     -- e.g., {"cost_center_code":"CC100","natural_account":"5000"}
  target_value     BIGINT NOT NULL,    -- the account_id (or other target)
  effective_from   DATE NOT NULL,
  effective_to     DATE,
  PRIMARY KEY (mapping_set_id, source_values, effective_from)
);
```

A few design decisions worth understanding.

**The condition is a typed AST, not an expression string.** A condition might be `event.amount > 1000 AND event.country IN ('US', 'CA')`. Storing this as a string of code is tempting and disastrous: validation becomes parsing, version diff becomes textual diff, and the configuration UI has to handle arbitrary expression complexity. Instead, the condition is a structured tree:

```json
{
  "and": [
    { "gt": [{ "event_attr": "amount" }, { "constant": 1000 }] },
    { "in": [{ "event_attr": "country" }, { "constant": ["US","CA"] }] }
  ]
}
```

This is verbose to write by hand but easy to construct via UI, easy to validate (the schema is constrained), easy to diff (compare nodes), and easy to evaluate (recursive descent).

**The amount source is similarly structured.** Most lines reference an event attribute directly (`{"event_attr": "subtotal"}`); some compute an amount (`{"multiply": [{"event_attr": "subtotal"}, {"constant": 0.10}]}`). The expression language is intentionally limited — arithmetic, comparison, attribute access, mapping lookup, simple aggregation. Anything more complex belongs in tier 2.

**Account derivation supports four modes.** *Constant* — a hard-coded account, used for system accounts like accumulated depreciation or AR control. *Event attribute* — pull the account from the event itself, used when the originating system already determined the account (e.g., asset acquisition where the asset record specifies the cost account). *Mapping set* — lookup based on source columns, used for cost-center-to-account mappings, customer-segment-to-revenue-account mappings, and similar dimensional derivations. *Compound* — nested rules with their own conditions, used for cases like "if intercompany then this account, else if foreign customer then that account, else the default account."

**Mapping sets handle the most common derivation pattern in accounting.** Every accountant who has worked with SAP knows account determination: "for transaction key BSX (inventory) and valuation class 7900 (raw materials), the account is 1100 (Raw Materials Inventory)." This is a mapping set. Generalizing it as a typed lookup table — with multiple source columns, effective dating, and explicit fallback handling — gives the platform a powerful primitive. Most account derivation across all subledgers is a mapping set lookup.

**Effective dating is mandatory throughout.** Rules, mapping set rows, account validity — everything is effective-dated. When a rule changes on April 15, an event posting back to March 10 evaluates against the rule that was effective on March 10, not the current rule. This prevents the worst class of accounting bugs: "we changed the rule on Tuesday and Monday's invoices now post to the wrong account."

#### 3.2.2 The Rule Evaluation Engine

The engine is application code that reads rules and applies them to events. Pseudocode:

```python
def evaluate(event):
    # 1. Find the active rule set for this event class, ledger, and date
    rule_set = active_rule_set(
        event_class=event.event_class,
        ledger_id=event.ledger_id,
        effective_date=event.posting_date
    )
    
    # 2. Evaluate each line rule
    posting_lines = []
    for rule in rule_set.line_rules:
        if rule.condition is None or evaluate_condition(rule.condition, event):
            account_id = derive_account(rule.account_rule, event)
            amount = evaluate_amount(rule.amount_source, event, rule.amount_currency)
            dimensions = derive_dimensions(rule.dimension_rules, event)
            posting_lines.append(make_line(
                side=rule.side,
                account_id=account_id,
                amount=amount,
                posting_layer_mask=rule.posting_layer_mask,
                dimensions=dimensions,
                rule_set_id=rule_set.id,
                rule_set_version=rule_set.version,
                rule_id=rule.rule_id
            ))
    
    # 3. Validate balance
    require_balanced(posting_lines, event.ledger_id)
    
    return posting_lines
```

The engine is straightforward. Complexity lives in the rule definitions, not in the engine itself. The engine's responsibilities are bounded: load rules, evaluate conditions, derive accounts, compute amounts, assign dimensions, validate output, attach provenance.

**Caching matters for performance.** Loading rule sets from the database on every event is wasteful. The active rule set for each (event_class, ledger_id, date_range) tuple is loaded once and cached in-memory for the worker process. Cache invalidation happens via Postgres LISTEN/NOTIFY: when a rule set is activated, the platform NOTIFYs a channel; worker processes LISTEN and refresh their caches. The cache is correct because rules are effective-dated — a "newly active" rule has an effective_from in the future or at least bounded in the past.

**Determinism is contract.** Given the same event and the same active rules, evaluation produces identical output. No system clock, no random, no external state. Reference data captured into the event payload at originate time, not looked up during evaluation. This makes evaluation replayable, testable, and auditable.

**Output validation is part of the engine.** Before returning posting lines, the engine validates that they balance per ledger, that all required accounts are valid, that dimensions are assigned correctly. A rule that produces unbalanced output is a bug; the engine refuses the output and raises an alert rather than persisting bad data.

#### 3.2.3 Configuration Lifecycle

Rules go through a lifecycle: draft → approved → active → retired. The transitions are managed:

- **Draft**: a user (typically an accounting configurator) creates or modifies a rule. Drafts can be edited freely. They have no effect on production evaluation.
- **Approved**: a separate user with approval authority reviews the draft and approves it. Approval requires a reviewer different from the creator (segregation of duties). Approved rules are immutable.
- **Active**: at the rule's effective_from date, an approved rule becomes active. Multiple approved rules with overlapping effective ranges are an error caught at approval time.
- **Retired**: when an active rule's effective_to date passes, it retires. Retired rules are kept (audit), not deleted. Events posted into the retired period evaluate against the rules active at that time.

The audit trail records every state transition: who created, who approved, when activation occurred, when retirement occurred. Modifications to an approved rule require creating a new version (increment the version number), which goes through draft and approval again. The original version is preserved.

This workflow is mandatory, not optional. It is what makes the configuration tier auditable. Without it, "who changed this rule and why?" becomes unanswerable.

#### 3.2.4 Configuration Tier Expressiveness

The richer the configuration, the fewer customers escape to tier 2. The tier-1 expressiveness should support, without code:

- **Compound conditions** with AND/OR/NOT nesting and operator combinations
- **Multi-column mapping lookups** with effective dating, default handling, and fallback chains
- **Arithmetic on event attributes** including percentages, allocations, and aggregations across event lines
- **Conditional line generation** ("only if intercompany flag is set")
- **Multiple lines per event** with different conditions, dimensions, and accounts
- **Cross-line aggregation** ("sum the asset-tracked distributions and post one capitalization line per asset category")
- **Currency-aware computation** with explicit handling of FX and rounding

What tier 1 deliberately does not support, pushing those cases to tier 2:

- **External lookups** — anything requiring a call to another system or service
- **Iterative algorithms** — loops, recursive computations, accumulator patterns beyond simple SUM
- **Complex string manipulation** — extracting values from free-form text fields
- **Industry-specific calculations** — the third decimal of how Brazilian ICMS-ST is computed, oil-and-gas production-allocation algorithms, telecommunications revenue-share computations
- **Customer-specific business logic** — anything that's idiosyncratic to one customer's organization

Drawing this line correctly is half the design. The line moves over time as patterns emerge: features used by many customers in tier 2 graduate to first-class configuration support in tier 1. The platform owners watch which custom WASM modules customers write and look for patterns that should be configuration features.

### 3.3 Tier 2: WASM Modules for Custom Logic

Tier 2 is for the cases where configuration cannot express what the customer needs. The customer writes code, compiles it to WebAssembly, uploads it to the platform, and the platform invokes it on matching events.

#### 3.3.1 Why WASM Specifically

The choice of WASM over alternatives — embedded JavaScript, Lua, Python, custom DSL, native plugin — deserves justification.

**Sandboxing is the dominant requirement.** Customer code runs in the platform's infrastructure. A bug, malicious code, or runaway loop must not affect other customers, must not access the platform's data outside what it's explicitly granted, and must not exhaust resources. WASM's capability-based design — modules have no ambient authority; everything available to the module is explicitly imported by the host — is the strongest sandboxing primitive in any embeddable runtime today. JavaScript V8 isolates are close, but JavaScript's spec has more dark corners (prototype pollution, regex catastrophic backtracking, JIT side channels). WASM's smaller spec and explicit import model are easier to reason about.

**Determinism is the second requirement.** Accounting rules must be deterministic — same inputs produce same outputs, replayable in test, auditable in production. WASM is deterministic in the absence of explicit non-deterministic imports. By choosing not to import `current_time()`, `random()`, network calls, or filesystem access, the host can guarantee that customer modules are deterministic. JavaScript's spec has many subtly non-deterministic behaviors that would have to be locked down individually.

**Performance is good enough.** WASM modules execute at near-native speed after JIT or AOT compilation. Cold start is 1-10ms (compilation), warm execution is microseconds for simple rules. JavaScript V8 is similar; Lua is similar; Python is significantly slower. For accounting workloads measured in thousands of events per second per shard, all of these are adequate, but the WASM ecosystem has invested heavily in production-grade runtimes (Wasmtime, Wasmer, WasmEdge) optimized for embedded host scenarios.

**Language agnosticism matters at enterprise scale.** A platform that targets multiple customers can't pick one programming language without alienating customers who prefer another. WASM is a compilation target, not a source language: customers can write in Rust, Go (via TinyGo), AssemblyScript, C/C++, Kotlin/Native, .NET, increasingly Python. The platform provides SDKs in each supported language; customers use the language their team already knows.

**Industry direction.** WASM is the platform that Cloudflare Workers, Fastly Compute@Edge, Shopify Functions, and numerous other "extend our SaaS with your code" products are converging on. The tooling, runtime quality, security posture, and ecosystem are all improving rapidly. Building on WASM today is building on a platform that will be more mature in five years, not less.

The trade-offs against WASM are real but addressable: customers need a compilation toolchain (mitigated by official SDKs and templates), debugging binary modules is harder than text scripts (mitigated by source maps and observability), and the WebAssembly Component Model — the emerging standard for typed module interfaces — is still stabilizing (mitigated by a wrapper layer that can adapt as the standard evolves).

#### 3.3.2 The Host API

What capabilities a WASM module has, the platform decides. This is the host API, and it is the most important architectural decision in tier 2 because it is a contract that lasts forever — once customers write modules against an API, breaking it is unacceptable.

The minimum viable host API has four capabilities:

1. **Receive the event payload** — the originating event's attributes, including all reference data the rule might need (customer attributes, vendor attributes, asset attributes, etc.). The payload is a structured value passed at module invocation.

2. **Look up reference data** — a constrained, read-only API for fetching reference data not in the event payload (chart of accounts entries, tax rates, mapping set entries). All lookups return immutable snapshots; the module cannot modify reference data.

3. **Compute** — arithmetic, comparison, conditional logic, string manipulation. This is what makes WASM Turing-complete; the host doesn't restrict it (within resource limits).

4. **Emit posting lines** — the only output mechanism. The module returns a list of posting lines (account, amount, dimensions, side, layer mask) and the platform validates and persists them.

The host API is *not* extended to include things the platform can do better externally:

- **No database writes.** Modules cannot insert, update, or delete anything. They produce posting lines; the platform persists them. This preserves transactional integrity and prevents customer modules from corrupting state.
- **No network calls.** Modules cannot call external services. If a rule needs data from an external system, that data is fetched into the event payload before evaluation; the module operates on the payload only.
- **No filesystem.** Modules cannot read or write files. All inputs come through the host API; all outputs come through the return value.
- **No clock.** Modules cannot read wall-clock time. If the rule needs to know "what time is the event?" it gets the event's posting_date or source_event_time from the payload.
- **No randomness.** Modules cannot generate random numbers. If the rule needs to do something that looks like randomness (e.g., distributing remainders), it must use a deterministic hash of the event's attributes.

These restrictions are the determinism guarantee. By absence — capabilities not imported into the module — the platform ensures that customer modules cannot introduce non-determinism, side effects, or external dependencies.

A sketch of the host API in Rust binding form:

```rust
// What the WASM module imports from the host
extern "C" {
    fn host_get_event_payload(buf: *mut u8, max_len: usize) -> usize;
    fn host_lookup_account(natural_code: *const u8, len: usize) -> u64;
    fn host_lookup_mapping(set_id: u64, key_json: *const u8, key_len: usize, 
                          out: *mut u64) -> i32;
    fn host_emit_posting_line(line_json: *const u8, len: usize) -> i32;
    fn host_log_debug(msg: *const u8, len: usize);
}
```

In practice, customers write against an idiomatic SDK (one per supported language) that wraps these raw imports in typed interfaces:

```rust
// What the customer's Rust code looks like
use ledger_sdk::{Event, PostingLine, AccountId, Money, Side};

#[no_mangle]
pub extern "C" fn evaluate(event: Event) -> Vec<PostingLine> {
    let mut lines = vec![];
    
    // Customer's logic
    if event.is_intercompany() && event.country() == "BR" {
        let liability_account = ledger_sdk::lookup_account("2210");
        let expense_account = ledger_sdk::lookup_account("5500");
        
        lines.push(PostingLine {
            side: Side::Debit,
            account_id: expense_account,
            amount: event.amount_in_functional(),
            ...
        });
        // ... more lines
    }
    
    lines
}
```

The SDK handles serialization, error handling, and the calling convention. Customer code looks like ordinary Rust (or TypeScript via AssemblyScript, Go via TinyGo, etc.).

#### 3.3.3 Numerical Precision

Money arithmetic in floats is wrong. The classic example: `0.1 + 0.2 != 0.3` in IEEE 754. Every accounting system has to address this, and the answer is integer minor units.

Amounts in the host API and in the SDK are integer minor units of currency: cents for USD, pence for GBP, satoshi for BTC, and so on for currencies whose minor unit is 1/100 (most), 1/1000 (some Middle Eastern), or other. The currency's minor-unit ratio is part of the currency's metadata (`minor_unit_factor`), and the SDK provides currency-aware arithmetic that respects it.

```rust
// Money is integer minor units + currency code
struct Money {
    amount: i64,         // signed integer in minor units
    currency: Currency,  // includes minor_unit_factor
}

impl Money {
    fn add(&self, other: &Money) -> Result<Money, CurrencyMismatch> {
        if self.currency != other.currency {
            return Err(CurrencyMismatch);
        }
        Ok(Money { amount: self.amount + other.amount, currency: self.currency })
    }
    
    fn multiply_by_rate(&self, rate: Decimal) -> Money {
        // Decimal math, then convert back to integer minor units
        // with explicit rounding policy
        ...
    }
}
```

For percentages and FX rates — values that genuinely need fractional precision — a Decimal type is provided (typically backed by 128-bit fixed-point or by a software decimal library). The SDK exposes Decimal × Money operations that produce Money results with explicit rounding policy.

This approach eliminates a class of subtle bugs that plague accounting systems. It does add cognitive overhead for customers who might naturally reach for floating-point. The SDK's documentation should be emphatic on this point.

#### 3.3.4 Resource Limits

A buggy or malicious customer module must not affect the platform. Resource limits enforce this:

- **CPU time per invocation**: 50ms wall-clock or 10 million WASM instructions, whichever first. Generous enough for legitimate complex rules; tight enough that infinite loops and exponential algorithms terminate quickly.
- **Memory per invocation**: 32 MB linear memory. Sufficient for any reasonable rule; bounded enough that runaway memory use can't impact the host.
- **Output size per invocation**: 1000 posting lines maximum. Beyond this, the customer is doing something the platform should know about — likely an algorithmic mistake.
- **Lookup count per invocation**: 100 reference data lookups. Prevents N+1 query patterns and ensures predictable evaluation latency.
- **Total module size**: 10 MB compiled module. Larger than any reasonable rule needs; prevents abuse.

Limits are enforced by the WASM runtime (Wasmtime supports per-invocation fuel/instruction limits and memory limits natively). Violations produce specific, actionable errors: "rule v1.2.3 exceeded 50ms CPU limit on event abc-123 after 12 million instructions." The error is surfaced to the customer immediately, not buried in logs.

Per-customer aggregate limits (total CPU time per day, total memory across all modules) are enforced at the platform layer. Customers exceeding limits get throttled with clear feedback.

#### 3.3.5 Testing Infrastructure

The unspoken hard part of tier 2 is making it possible for customers to test their rules. Without good testing tools, every rule deploy is a leap of faith and bugs are discovered in production.

The platform provides:

**Local CLI for module evaluation.** A tool that takes a WASM module and a JSON event payload and emits the resulting posting lines. Customers run this in their development environment to see what their module produces.

```
$ ledger-cli evaluate --module my_rule.wasm --event test_event.json
Posting lines:
  D 5500 (Travel Expense)         $1,000.00 [cost_center=CC100]
  C 2100 (Accounts Payable)       $1,000.00 [vendor=V42]
```

**Test framework with mock host API.** An SDK component that lets customers write unit tests in their language, with a mocked host API that responds to lookups with test fixtures.

```rust
#[test]
fn intercompany_brazilian_invoice_uses_special_accounts() {
    let mut host = MockHost::new();
    host.add_account("2210", AccountId(2210));
    host.add_account("5500", AccountId(5500));
    
    let event = Event::builder()
        .event_class("vendor_invoice")
        .country("BR")
        .intercompany(true)
        .amount_functional(100000)  // $1,000.00 in cents
        .build();
    
    let lines = evaluate_with_host(&event, &host);
    
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].account_id, AccountId(5500));
    assert_eq!(lines[0].side, Side::Debit);
}
```

**Golden master testing.** As the rule evolves, the customer accumulates test cases that lock in behavior. Regression tests run on every change. The customer can build up a library of "this exact event produces these exact lines" pairs that protects against accidental rule breakage.

**Shadow mode in production.** A new module version can be deployed in shadow mode: it evaluates alongside the current active version on real production events, but its output is logged for comparison rather than persisted. Customers see how the new version behaves on real traffic before activating it. Differences between shadow and active outputs are surfaced for review.

**Production replay.** For investigating production issues or validating rule changes against historical traffic, the platform retains event payloads for some retention period and provides tooling to replay them through alternative module versions.

This testing infrastructure is genuinely 30-50% of the work of building tier 2. Skimping on it produces a "rules engine" that customers are afraid to use because they can't validate their changes. Investing in it heavily is the difference between a platform feature and a perpetual support burden.

#### 3.3.6 Module Lifecycle and Versioning

WASM modules are content-addressed by their SHA-256 hash. This is the natural model: the binary's hash is its identity. Customers upload modules; the platform stores them by hash; activation references a specific hash.

The lifecycle:

1. **Upload.** Customer uploads a compiled WASM module along with associated metadata (description, source-language tag, source map, optional source archive). Platform validates the module (size limits, instruction count check on canary input, host API compatibility check) and stores it indexed by hash.

2. **Test.** Customer runs the module against test events via the platform's testing tools (or has done so locally). May run shadow mode in production.

3. **Approve.** A separate user with approval authority (segregation of duties enforced) approves the specific module hash for activation.

4. **Activate.** Customer specifies effective dating: this module hash is active for events of class X in entity Y from date D1 to date D2 (or open-ended). Multiple module versions with overlapping ranges are an error.

5. **Retire.** When the active period ends, the module retires. It's not deleted — historical events that posted under it can still be inspected — but it no longer evaluates new events.

6. **Rollback.** A previously-active module version can be reactivated as the current version, displacing whatever was active before. This is a one-click operation; customers in distress can revert immediately.

The audit trail records every transition: upload, test results, approval, activation, retirement, rollback. The hash of the module that produced each posting line is captured on the line itself (`custom_module_hash`), making the connection from posting back to module version permanent.

### 3.4 How the Tiers Cooperate

Tier 1 and Tier 2 are not isolated. They share infrastructure and cooperate on evaluation.

**Both tiers produce posting lines** that flow through the same validation and persistence path. The validator doesn't care whether a line came from a configuration rule or a WASM module; it checks balance, account validity, period status, and so on uniformly. Persistence is the same code path regardless of source.

**Both tiers operate within the same event class hierarchy.** An event class might have a tier-1 rule set covering most cases and a tier-2 module for specific carve-outs. The dispatch logic checks tier 2 first (specific cases override generic ones), falling through to tier 1 if no tier-2 rule matches.

**Tier-2 modules can call tier-1 mapping sets.** A common pattern: the customer needs custom logic for some of the rule, but most of the account derivation is standard mapping-set lookups. The host API exposes mapping set lookup as a primitive, so the WASM module can do its custom logic and then call `host_lookup_mapping(set_id, key)` for the standard parts. This avoids duplicating mapping set data in customer code.

**Migration from tier 2 to tier 1.** When the platform owners notice that many customers are writing similar tier-2 modules for a particular pattern, that pattern becomes a candidate for tier-1 support. New configuration features get added; existing tier-2 modules can be migrated to tier-1 rules (and the tier-2 modules retired). This is a healthy cycle: customer needs surface in tier 2, common patterns graduate to tier 1, the platform's expressiveness grows over time without forcing customers to rewrite when patterns change.

**Effective dating works across tiers.** A rule change might be a tier-1 update, a tier-2 module activation, or both. The evaluation engine, given an event with a posting date, looks up the active rules (tier 1) and active modules (tier 2) for that date and evaluates accordingly. Past events are evaluated against past rules; current events against current rules; future events (e.g., scheduled depreciation runs) against the rules expected to be active at that future date.

### 3.5 Critical Analysis — What This Engine Gives Up

The two-tier engine is powerful, but it has costs.

**Two systems to maintain.** Tier 1 and Tier 2 share infrastructure but have distinct concerns: Tier 1 is data-driven configuration with a custom evaluator; Tier 2 is WASM with a host API. Both need version control, audit trails, testing tools, deployment workflows. The platform team is building two products, not one. This is the cost of meeting customer needs across the power-law distribution; it is real and it is permanent.

**Determining which tier to use is itself a design decision customers will get wrong.** Customers facing a moderately complex rule will sometimes reach for WASM when configuration could have served, because writing code feels familiar. Other times they'll twist configuration into pretzels when WASM would have been cleaner. Both errors are common and produce technical debt. Documentation, examples, and (eventually) tooling that suggests "this case might fit configuration" / "this case probably needs WASM" are essential.

**The configuration tier expressiveness ceiling is hard to predict.** Some rules that look simple end up requiring tier 2 because of an edge case the configuration model didn't anticipate. Tier 1's expressiveness should grow over time, but every addition is a permanent commitment. The schema gets baroque if the platform tries to handle every customer's edge case in configuration.

**The host API is forever.** Once customers have WASM modules calling specific host functions, those functions cannot change. Five years in, the host API will have accumulated functions that are barely used but cannot be removed. Versioning the API helps but doesn't eliminate the problem; major version bumps require customers to migrate, which is contentious.

**Determinism is hard to fully guarantee.** WASM modules are deterministic in the absence of non-deterministic imports, but customers can introduce subtle non-determinism: depending on iteration order of a HashMap (which is non-deterministic in Rust by default), depending on floating-point reassociation in optimized code, depending on the bit pattern of NaN values. The platform can't catch all of these. Documentation, linters, and golden-master testing partially mitigate; permanent risk remains.

**Performance variance.** Tier 1 evaluation is fast and predictable — a few hundred microseconds per event for typical rules. Tier 2 evaluation has higher and more variable latency: cold starts (compilation) take milliseconds; even warm execution depends on the specific module's complexity. Workloads dominated by tier 2 will have higher tail latencies than workloads dominated by tier 1. Customers writing inefficient WASM modules slow down their own event processing; the platform must isolate them so they don't slow down others.

**Audit overhead is higher with tier 2.** A configuration rule is stored as data; "what does it do?" is answerable by reading the config row. A WASM module is a binary; "what does it do?" requires either reading the source (which the platform may or may not have) or decompiling. For SOX, regulatory audits, and similar, tier 2 introduces complexity. Mitigations: require source archive uploads with modules; store source archives indexed by hash; provide tooling to view sources; encourage customers to write self-documenting rule descriptions.

**Tooling investment is enormous.** Building a testing framework, a CLI, language SDKs, source map handling, replay tooling, shadow mode, and the rest is a multi-quarter engineering project. Skimping produces a tier 2 that customers don't use because they can't trust their changes. Doing it well is expensive.

**Operational responsibility is shared with customers.** A bug in a tier-2 module that produces incorrect accounting is the customer's bug, but customers will call the platform for help. The platform contract — what's guaranteed, what's the customer's responsibility — must be explicit. SLAs around rule execution should be on availability and resource policy, not correctness of customer logic. This is an important conversation to have with customers up front.

**The "prepackaged rules" assumption is doing significant work.** The configuration tier is most useful when there's a starting set of well-designed prepackaged rules — vendor invoice rules, customer invoice rules, depreciation rules, etc. — that customers adapt rather than build from scratch. Without a strong prepackaged starting point, customers face an empty configuration screen and either build incorrect rules or escape to tier 2 for everything. The platform must invest in prepackaged rule libraries for common scenarios across major industries and accounting standards. This is a content engineering problem, not just a platform engineering problem.

The two-tier engine is the right architecture for the problem. But naming what it costs is necessary because the costs are large enough that some platforms will be better served by a simpler approach (configuration only, with tier 2 as a future phase) until they have the engineering capacity and customer base to support both tiers well.

---

## Part III — Cross-Cutting Concerns

The schema and the rules engine are the two big pieces, but a working ledger system involves operational patterns that span both. This section addresses the concerns that determine whether the architecture works in production.

### 4.1 The Outbox Pattern and Event Lifecycle

Events flow from operational subledgers (a vendor invoice was validated; a customer payment was received; a depreciation run completed) into the rules engine, which produces postings, which land in the posting tables. The architectural challenge is making this flow atomic and exactly-once.

The naive approach — operational subledger commits its transaction, then calls the rules engine, which commits postings — has a race condition: if the call fails between the two commits, the operational record exists without postings. Retrying the call may work, but if the operational record was modified between the original call and the retry, the retry produces postings inconsistent with the latest state. Worse, calling the rules engine before the operational commit has the inverse problem: postings exist for an operational state that never persisted.

The outbox pattern solves this. The operational subledger, in the same database transaction that commits its operational record, also writes an event to an outbox table:

```sql
CREATE TABLE accounting_events (
  event_id          BIGSERIAL PRIMARY KEY,
  event_class       INT NOT NULL,
  event_type        INT NOT NULL,
  legal_entity_id   INT NOT NULL,
  source_module     SMALLINT NOT NULL,
  source_doc_id     BIGINT,
  payload           JSONB NOT NULL,
  posting_date      DATE NOT NULL,
  source_event_time TIMESTAMPTZ NOT NULL,
  recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  idempotency_key   UUID NOT NULL,
  status            VARCHAR(16) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','processing','posted','failed','skipped')),
  attempts          INT NOT NULL DEFAULT 0,
  last_attempted_at TIMESTAMPTZ,
  last_error        TEXT,
  posting_id        BIGINT,                  -- set when posted
  
  UNIQUE (source_module, idempotency_key)
);
```

The operational transaction commits both its own changes and the outbox row atomically. Either both happen or neither does. There is no "operational record without event" or "event without operational record" possibility.

A separate worker process polls the outbox for pending events, evaluates them through the rules engine, persists the resulting postings, and marks the event as posted — all in a single database transaction:

```python
def process_outbox_batch(batch_size=100):
    with transaction():
        events = SELECT * FROM accounting_events
                 WHERE status='pending'
                 ORDER BY recorded_at LIMIT batch_size
                 FOR UPDATE SKIP LOCKED
        
        for event in events:
            try:
                lines = rules_engine.evaluate(event)
                posting_id = persist_posting(event, lines)
                UPDATE accounting_events
                SET status='posted', posting_id=:posting_id, attempts=attempts+1
                WHERE event_id=:event.event_id
            except Exception as e:
                UPDATE accounting_events
                SET status='failed', attempts=attempts+1,
                    last_attempted_at=now(), last_error=:str(e)
                WHERE event_id=:event.event_id
```

The `FOR UPDATE SKIP LOCKED` lets multiple worker processes pick up disjoint batches without contention. Failures are isolated — a single failing event doesn't block the rest of the batch. Retries are bounded by attempt count; persistent failures escalate to alerting.

The outbox approach has a few important properties:

- **Exactly-once posting per event.** The unique constraint on `idempotency_key` prevents duplicates. If a worker crashes after posting but before updating the event status, the next attempt sees the existing posting via the unique constraint and updates the event accordingly without re-creating.
- **Strict ordering preserved.** Within a single legal entity's outbox, events are processed in the order they were recorded. This matters for some accounting scenarios (e.g., applying a payment before applying a credit memo) where ordering affects outcomes.
- **Visibility into failures.** A failed event is in the database, with its error message, retry count, and last attempt time. Operators can investigate and manually retry or skip.
- **Backpressure absorbed.** If the rules engine is slow, the outbox grows but the operational subledger is unaffected. Events accumulate; workers drain them when capacity allows.

The trade-off: events are not posted instantaneously with the operational commit. There's a latency between operational record commit and posting commit — typically sub-second under normal load, longer when the outbox grows. For most accounting use cases this is acceptable. For use cases requiring synchronous posting (some real-time dashboards), the outbox pattern is wrong; those would need a different (and more failure-prone) integration.

#### Event Lifecycle

Beyond the outbox, events themselves have a lifecycle in the source subledgers. An invoice goes through created → validated → adjusted → canceled → cleared. Each transition can have accounting consequences:

- **Created**: typically no accounting (the invoice exists but isn't yet a liability)
- **Validated**: accounting fires (Dr Expense, Cr AP)
- **Adjusted**: accounting fires (incremental Dr/Cr to reflect the adjustment)
- **Canceled**: accounting fires (reversal of original)
- **Cleared (paid)**: typically reduces AP, not accounting under the validation event

The event class taxonomy in the rules engine reflects this: each lifecycle transition that has accounting consequences is its own event class/type combination. The rules for "vendor_invoice/validated" differ from "vendor_invoice/adjusted" differ from "vendor_invoice/canceled". This is exactly Oracle Fusion's event-class/event-type model and it's the right abstraction.

### 4.2 Idempotency Throughout the Pipeline

Idempotency keys appear at multiple layers, and getting them right matters.

**Source-event idempotency.** When the operational subledger emits an event, it generates an idempotency key — typically a deterministic hash of the event's identifying fields (source_module, doc_type, doc_id, doc_version, lifecycle_state). The same source event regenerated at any point produces the same key. The `accounting_events.idempotency_key` unique constraint catches duplicates.

**Posting idempotency.** When a posting is persisted, it carries the idempotency key from its triggering event. The `postings.idempotency_key` unique constraint catches the case where the worker tries to post the same event twice (which shouldn't happen given the outbox, but defense in depth).

**Rule evaluation idempotency.** Given the same event and the same active rules, the rules engine produces the same posting lines. This is the determinism contract. Replaying an event after a worker restart produces identical lines; comparing against the existing posting confirms idempotency at the data level.

These three layers compose. A well-functioning system has the property that retrying any operation, at any point, produces the same final state as if the retry hadn't happened. This is what makes the system safe to operate in the face of network partitions, worker crashes, and replication failures.

### 4.3 Bi-Temporal Modeling in Practice

The schema has both `posting_date` (effective time) and `created_at` (transaction time). The discipline of using them correctly is operational.

**Reports default to effective time.** "December revenue" means revenue with `posting_date IN ('2025-12-01', '2025-12-31')`. Late entries posted in February with December posting dates are included.

**Audit and restatement reports use transaction time.** "What did we believe December revenue was as of January 15?" means revenue with `posting_date IN December AND created_at <= '2026-01-15'`. This produces the answer that would have been on the original report.

**Closed periods constrain effective time, not transaction time.** When December closes on January 5, no new postings are accepted with `posting_date IN December` regardless of `created_at`. Late entries discovered after close land in the current open period as prior-period adjustments — `posting_date` in the open period, with reference to the closed-period transaction in description.

**Reversals use the original's posting date when in an open period.** If a December posting needs reversal and December is still open, the reversal uses December's posting date — the original effect is undone in the period where it occurred. If December is closed, the reversal is in the current open period, treating the original as a fact and the reversal as a current adjustment.

This discipline must be encoded somewhere — in policies, in code, in user training. The schema supports it; the schema does not enforce it. Mismatches between effective and transaction time are a frequent source of audit findings; the operational team must understand and enforce the policy.

### 4.4 Open Items, Balances, and Other Derived State

Append-only postings as the source of truth means everything that looks like state is actually a projection over the immutable log. Open items, balances, aging, asset net book values, inventory quantities — all derived.

**Account balances** as a materialized view, refreshed at each period close:

```sql
CREATE MATERIALIZED VIEW account_balances AS
SELECT
  legal_entity_id,
  ledger_id,
  account_id,
  fiscal_year,
  fiscal_period,
  SUM(amount_functional) AS net_amount,
  SUM(CASE WHEN amount_functional > 0 THEN amount_functional ELSE 0 END) AS debit_total,
  SUM(CASE WHEN amount_functional < 0 THEN -amount_functional ELSE 0 END) AS credit_total,
  COUNT(*) AS line_count
FROM posting_lines
WHERE posting_layer & 1 != 0  -- current layer only
GROUP BY legal_entity_id, ledger_id, account_id, fiscal_year, fiscal_period;

CREATE UNIQUE INDEX ON account_balances (legal_entity_id, ledger_id, account_id, fiscal_year, fiscal_period);
```

Refreshed via `REFRESH MATERIALIZED VIEW CONCURRENTLY` at period close. Within a period, balances are computed on-demand from the live posting_lines table — fast because the typical query scopes to a single period and a single account.

**Open items** as a view computed from the full posting history of an open-item-managed account:

```sql
CREATE VIEW open_items AS
SELECT
  source_doc_type,
  source_doc_id,
  legal_entity_id,
  customer_id,
  vendor_id,
  asset_id,
  SUM(amount_functional) AS open_balance,
  MIN(posting_date) AS earliest_posting,
  MAX(posting_date) AS latest_activity
FROM posting_lines pl
JOIN accounts a ON a.account_id = pl.account_id
WHERE a.is_open_item = TRUE
  AND pl.ledger_id = 0
  AND pl.posting_layer & 1 != 0
GROUP BY source_doc_type, source_doc_id, legal_entity_id, customer_id, vendor_id, asset_id
HAVING SUM(amount_functional) <> 0;
```

For performance, this is materialized too, refreshed at intervals or invalidated on relevant inserts. The point is that "is this invoice paid?" is computed from postings, not stored separately.

**Asset net book values, inventory balances by item, customer aging buckets** — all the same pattern. State that traditional systems would store in mutable tables is computed from immutable postings here. The benefit is that inconsistencies between subledger state and ledger state are impossible (they're the same data); the cost is computational, mitigated by materialization and incremental refresh.

### 4.5 Multi-Currency Handling

The schema stores three or four currency representations per row. The operational model has a few subtleties.

**Functional currency is property of the legal entity.** Each legal entity has one functional currency, typically the currency of its primary operating environment. Functional currency rarely changes over the life of the entity (and when it does, that's a major accounting event with specific treatment under IAS 21 / ASC 830).

**Transaction currency is property of the event.** A vendor invoice has a transaction currency (the currency the vendor billed in). A customer payment has a transaction currency (the currency they paid in). These can differ from functional currency, in which case FX conversion is required.

**Group currency is property of the consolidation hierarchy.** The parent company's reporting currency is the group currency for the entire group. Every line is translated to group currency for consolidated reporting.

**Local statutory currency** (the optional fourth currency) handles cases like a US-functional subsidiary of a German parent that must file local German statutory reports in EUR. Most rows don't populate this; ones that need it do.

**FX rates and the rate types.** Different rates are used in different contexts:

- **Spot rate** at the transaction date: for individual transactions
- **Period-average rate**: for P&L items in monthly translations
- **Period-end rate**: for balance sheet items in monthly translations
- **Historical rate**: for non-monetary items measured at original cost

The rate used for each row's translation is determined at posting time by the rules engine and applied to compute `amount_functional`, `amount_group`, `amount_local`. The `fx_rate_to_functional` and `fx_rate_to_group` columns record what rate was used, for audit and reconciliation.

**Realized vs. unrealized FX gain/loss.** When a transaction-currency open item is settled, the rate at settlement may differ from the rate at original posting. The difference is realized FX gain/loss, posted at settlement time. When a transaction-currency open item exists across a period boundary, period-end revaluation creates unrealized FX gain/loss, which is reversed at the next period start (so realized FX captures the actual settlement-versus-original difference cleanly). This is a standard pattern; the rules engine handles it via specific event classes for settlement and revaluation.

**Currency conversion is monotonic.** Once a row's amounts in functional, group, and local currencies are computed and persisted, they are never changed. Subsequent rate revisions don't restate prior postings. This is what makes append-only viable for currency-sensitive accounting: every posting is immutable in every currency.

### 4.6 Period Close Orchestration

Period close is where the architecture earns or loses its keep. A clean close depends on disciplined orchestration.

**Subledger close before GL close.** Each subledger (AP, AR, INV, FA, etc.) closes for the period first, indicating no more new postings of that type for that period. Once subledgers are closed, period-end processes run (depreciation, accruals, allocations, FX revaluation). After period-end, GL closes.

**Period-end processes are idempotent and replayable.** Depreciation calculation, for example, computes the depreciation for each asset based on its parameters and the period; running it twice produces the same result (the second run finds the postings already exist via idempotency). This is essential for reliability — if a process fails partway, it can be rerun without producing duplicate postings.

**Reconciliation runs before close-confirmation.** Subledger control accounts are compared to subledger balances:

- AR control account balance = sum of customer open items
- AP control account balance = sum of vendor open items  
- Inventory control account balance = sum of valuated inventory by item
- Asset cost control account balance = sum of asset costs by asset

Mismatches are investigated and resolved before the period closes. Append-only postings make these reconciliations naturally idempotent — recompute from scratch any time, deterministic answer.

**The close itself is a database operation.** When all checks pass, the period is marked closed via insertion into `closed_periods`. The trigger on `posting_lines` (Section 2.7) thereafter rejects insertions for that period.

```sql
CREATE TABLE closed_periods (
  legal_entity_id  INT NOT NULL,
  ledger_id        SMALLINT NOT NULL,
  fiscal_year      SMALLINT NOT NULL,
  fiscal_period    SMALLINT NOT NULL,
  closed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_by_user   INT NOT NULL,
  status           VARCHAR(16) NOT NULL DEFAULT 'closed'
                    CHECK (status IN ('soft_closed','closed','locked','reopened')),
  PRIMARY KEY (legal_entity_id, ledger_id, fiscal_year, fiscal_period)
);
```

Close has multiple states: *soft-closed* (no new operational postings, but adjustments still allowed by approved users), *closed* (no new postings of any kind without explicit reopen), *locked* (extra protection for closed periods that should no longer change for any reason), and *reopened* (a closed period was reopened for adjustment, with audit trail).

**Reopening is auditable and bounded.** A closed period can be reopened, but it requires elevated approval, the reopen is logged, and the period is automatically reclosed at a defined boundary. The audit trail records the reopen, the postings made during the reopen, and the reclose.

**Year-end carryforward** runs at fiscal year end. It computes ending balances of P&L accounts, posts them to retained earnings, and zeros the P&L accounts for the new year. This is itself a posting (or sequence of postings) flowing through the same architecture — no special-case mutation of balances.

### 4.7 Reconciliation as a First-Class Concern

Append-only and database-enforced invariants prevent many bugs but not all. Reconciliation jobs running continuously — daily, hourly, on each period close — verify cross-subsystem invariants that no single transaction can guarantee.

**Subledger-to-control-account reconciliations.** Already mentioned. AR, AP, inventory, fixed assets, and any other open-item-managed area reconciles its detail-level balances to its GL control account daily. Mismatches alert immediately.

**Cross-shard intercompany reconciliations.** Each intercompany posting carries an `intercompany_pair_id`. The consolidation database joins paired postings across shards and verifies that each side mirrors the other in the right way (signs flipped, amounts equal in group currency). Unpaired or mismatched intercompany items are flagged.

**Tier-1 vs. tier-2 rule output reconciliations.** When a rule has been migrated from tier 2 to tier 1, shadow-mode comparison verifies that tier-1 produces the same output as tier-2 did. Differences are investigated before the migration completes.

**Replication lag monitoring.** The analytics replica should be no more than N seconds behind the primary. The consolidation database should be updated within M minutes of subsidiary commits. Monitoring catches when these bounds are exceeded.

**Rule-rule consistency.** Across all active rule sets, certain invariants should hold (e.g., for every invoice rule that debits an expense account, there is a corresponding canceled-invoice rule that credits the same expense account). Periodic consistency checks across the rule definitions catch authoring mistakes.

These reconciliations are not optional. They are how the system catches what individual transactions cannot. The ratio of reconciliation engineering to feature engineering is higher than most teams expect. Plan for it.

### 4.8 Cross-Shard Invariants

A few invariants span multiple shards (legal entities) and cannot be enforced in any single transaction:

- **Intercompany posting pairs.** A sale in subsidiary A must have a matching purchase in subsidiary B. The two postings are in different shards; their atomicity is at best eventual.
- **Group-level reporting consistency.** Consolidated balance sheets should add up across all subsidiaries; mismatches indicate data sync problems.
- **Cross-entity allocations.** A management allocation that spreads cost across subsidiaries must produce balanced postings in each.

These are handled by sagas: a process manager that watches for one-side postings and ensures the other side gets created within a bounded window. Failure to find the matching side raises an alert. The window depends on the use case — minutes for real-time intercompany, hours for batch allocations.

The accounting concept for handling cross-shard ambiguity is the **suspense account**. When a posting arrives that belongs to entity B but originates in entity A, the originating side posts to a suspense account. The mirror saga moves the suspense balance to the proper accounts in entity B and clears the suspense in entity A. Suspense balances at period end are aged and investigated; old unresolved items are escalated.

This is operationally complex but unavoidable in any sharded architecture. SAP and Oracle Fusion handle it the same way at multi-system scale (intercompany matching across system boundaries); the proposed design replicates the pattern within the architecture.

---

## Part IV — Synthesis

### 5.1 What This Design Achieves

The design, implemented faithfully, achieves a specific set of properties:

**Data correctness as a structural guarantee, not a process.** The append-only invariant, double-entry constraint, idempotency keys, and effective-dated rules combine to make many classes of incorrect state unrepresentable. You cannot have an unbalanced posting because the database refuses it. You cannot have duplicate postings from retries because the unique constraint prevents it. You cannot have an FI-CO mismatch because they reference the same `posting_id` in the same database transaction; the linkage between FI fields (in the core) and CO fields (in extensions or rule provenance) is enforced by shared keys. Process discipline still matters, but the system protects itself in ways most accounting systems don't.

**Visible audit trail by design.** Every posting points to its triggering event; every event points to its source operational record; every line carries the rule version that produced it. "Why did this post here?" is answerable by following references, not by archaeology.

**Cost structure proportional to use.** A small entity uses a single Postgres instance with monthly partitions; the architecture scales down. A large enterprise uses sharding by legal entity, columnar analytics replicas, and a consolidation database; the architecture scales up. Investment is proportional to need.

**Operational pain at expected places.** When something goes wrong, the failure modes are predictable: a rule produces unexpected output (debug via test framework), a partition fills (rotate to next month), a replica lags (check WAL traffic), a reconciliation flags a mismatch (investigate via the audit trail). Production systems built on this architecture have a manageable list of known failure modes, not a long tail of mystery bugs.

**Adaptability through tier 2.** When a customer's needs exceed configuration, they have a path that doesn't require a platform release. WASM modules, deployed by the customer, evaluated in a sandbox, integrated through a stable host API, give flexibility without sacrificing isolation.

**Industry-standard accounting outputs.** Financial statements, regulatory reports, audit trails, intercompany eliminations — all the standard outputs flow from this architecture without special-casing.

### 5.2 What It Cannot Achieve

Equally important is naming what the design does not give you.

**HANA-class analytical performance is not achievable on Postgres.** The wide-table column-store advantages of HANA are real and substantial. Multi-dimensional pivots over billions of rows return in milliseconds on HANA; on Postgres they return in seconds or minutes, even with a columnar replica. Workloads requiring real-time slice-and-dice across all dimensions on the full posting history are better served by purpose-built HTAP or by exporting to a data warehouse.

**Cross-shard transactions are not transactional.** Intercompany postings are eventually consistent across shards, not atomically consistent. For most accounting, this is fine; for use cases where atomic cross-entity posting is required, this design is wrong.

**Schema evolution is operationally heavy at scale.** Adding a typed column to a multi-billion-row partitioned table is a managed operation with downtime risk, even with `pg_repack`. The architecture is not maintenance-free.

**Configuration cannot fully replace customization.** Some customer needs require code; tier 2 exists for a reason. Platforms that try to push everything into configuration produce baroque DSLs.

**The operations team is part of the architecture.** Backup integrity, partition management, replication monitoring, access-control auditing, performance tuning — all are required for the system to function correctly. The architecture is not self-maintaining. Organizations that underinvest in operations will find this out at the worst time.

**Compliance frameworks aren't satisfied by architecture alone.** SOX, IFRS audit requirements, regulatory reporting all require process controls, segregation of duties, change management, and documentation that the architecture supports but does not provide. The architecture makes compliance possible; the organization makes compliance happen.

### 5.3 Where to Begin

A design document is not an implementation plan, but a reasonable order of building emerges from the dependencies.

**Phase 1: the schema and the OLTP write path.** Build the `posting_lines` core table and its initial set of extensions (`posting_line_dimensions`, `posting_line_currencies`, `posting_line_sources`, `posting_line_inventory`, `posting_line_custom`) with their constraints. Build the `postings` header table. Build the application layer that takes a structured posting (header + lines + dimensions + sources + currencies + inventory) and persists it via batched insert in a single transaction, with full validation. Build the outbox and the worker that drains it. Build the basic audit trail and reconciliation jobs. At this point, you have a working ledger that accepts postings via API. No rules engine; postings are produced by external code that calls the API directly. This phase produces a system that's already useful — many in-house ledger systems are this and nothing more.

**Phase 2: the configuration tier of the rules engine.** Define the rule schema, build the evaluation engine, build a configuration UI. Wire the engine into the outbox worker so events arriving in the outbox flow through the engine into postings. Ship prepackaged rules for common scenarios (vendor invoice, customer invoice, basic depreciation). Customers can adapt the prepackaged rules and create their own through the UI. At this point, you have a configurable ledger system that handles most accounting needs.

**Phase 3: replicas and analytics.** Set up logical replication to an analytics database with columnar storage. Build the standard reports — trial balance, P&L, balance sheet, AR aging, AP aging, asset register. Implement period close orchestration. Implement the reconciliation jobs. At this point, you have a complete accounting system, just without the customization escape hatch.

**Phase 4: the WASM tier.** Pick one source language (Rust is the safest first choice — mature WASM support, strong type system, deterministic by default) and build the SDK. Build the host API with a minimal capability set. Build the testing CLI and shadow mode. Onboard a small number of customers with specific tier-2 needs and learn from their experience. Expand the SDK to additional languages once the first is stable.

**Phase 5: scale-out.** Implement legal-entity-level sharding. Build the consolidation database. Implement intercompany matching and elimination. Build cross-shard reconciliation. At this point, the architecture handles large multi-entity organizations.

**Phase 6: the long tail.** Industry-specific prepackaged rules. Specialized event classes (lease accounting, revenue recognition under ASC 606, project accounting variants). Advanced configuration features (cross-line aggregation, complex conditional logic). Component Model migration as the WebAssembly ecosystem stabilizes.

The most important sequencing principle: do not build tier 2 until tier 1 is mature. A platform with a half-built configuration tier and a flashy WASM tier will see customers escape to WASM for cases that should have been configuration, and the maintenance burden on the platform team will exceed the value delivered. Tier 1 done well is where most customer value comes from. Tier 2 is the escape hatch, not the centerpiece.

---

## Closing

The design above is not novel in its parts. The append-only line-item store with universal facts in a single core table is from SAP, adapted to Postgres's row-store nature with typed extensions instead of mega-wide rows. The event-class/event-type taxonomy is from Oracle Fusion. The posting-layer concept is from Microsoft Dynamics 365. The visible source-record GL impact is from NetSuite. The two-tier rules engine is a synthesis of patterns from production rule systems, Shopify Functions, and the modern API-first ledger startups.

What is novel is the synthesis: bringing these ideas together on commodity infrastructure (PostgreSQL, standard cloud services, WebAssembly), with a clear architectural commitment to append-only as foundational rather than accidental, with a moderately-narrow core that respects Postgres's strengths instead of fighting them, and with a deliberate effort to respect 500 years of accounting practice rather than reinvent it.

The platform-awareness is essential. Database choices and schema choices are inseparable. ACDOCA's wide design is the right answer on HANA's column store and the wrong answer on Postgres's row store. The narrow-core design with extensions is the right answer on Postgres and would be the wrong answer on HANA (where the joins would be unnecessary overhead). A general-purpose architecture document that ignores the database platform produces designs that work poorly on whatever they're actually deployed to. This document is explicit about its target.

Whether to actually build this depends on context. Most organizations should not build their own ledger; they should buy SAP, Oracle Fusion, Dynamics 365, or NetSuite. The four big ERPs are the right answer for most enterprises because the implementation, customization, and ongoing operations of a financial system represent more work than the initial architecture. The decision to build a custom ledger is justified only when none of the available options fit — typically because the organization is a SaaS company with embedded financial workflows, a fintech with non-standard product structures, or a company with operational scale that genuinely exceeds what any commercial ERP supports without painful customization.

If the build decision is justified, the architecture above provides a defensible starting point. It will not be optimal in every dimension — HANA-based systems are faster on analytical pivots, Oracle Fusion is more configurable, NetSuite is more accessible. But it is buildable on Postgres, scales to substantial enterprise volumes, and respects the underlying accounting domain — and the underlying database platform — in ways that ad-hoc designs typically don't. That combination is rare enough to be worth the effort when the build decision is genuinely warranted.

The non-architectural advice closes the document: respect the domain, and respect the platform. Double-entry bookkeeping, immutable postings, debits-equal-credits, balance-via-derivation — these are not legacy practices that modern systems have outgrown. They are the right answer that any modern system would have to invent if it didn't already exist. Architectures that respect these constraints age well; architectures that fight them accumulate increasingly elaborate workarounds for problems that the constraints would have prevented. The schema and engine described here exist to give the accounting domain a faithful expression on modern infrastructure. The work of using them well is the same work accountants have been doing for half a millennium — augmented now with the operational discipline that distributed systems require.
