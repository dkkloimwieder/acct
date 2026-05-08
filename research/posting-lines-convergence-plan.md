# Posting-Lines Convergence Plan

**Status:** v0 working document, 2026-05-07.
**Tracking epic:** `acct-wb75` (Convergence to unified posting_line schema).
**Plan file:** `/home/kaalin/.claude/plans/formulate-a-detailed-plan-prancy-pizza.md` (approved 2026-05-07).
**Supersedes:** `research/architecture-synthesis.md` §8.1 ("keep transfers as-is") — this convergence plan replaces that recommendation. The synthesis itself stays as historical record + comparison reference.
**Audience:** project owner; future contributors executing convergence phases; future-self resuming after compaction.

---

## §1 Executive Summary

We are converging the codebase from its current **inventory-first with the GL emerging from `posting_lines`** posture to the **GL-first universal-core + typed-extensions architecture** prescribed by `research/ledger-architecture-proposal.md` §2–§4 and reaffirmed by `cost-methods-subledger-design.md` §2 (the GL-aggregates / subledger-details principle). The work is phased over ~9–15 months sequentially with stop points at every phase boundary; some parallelism is available between Phase F (services wrappers) and Phase E (method-specific subledgers), and between Phase G (chart-of-accounts conversion) and Phase D–E.

**Why now.** The user rejected two convergence-roadmap recommendations from the prior architecture-synthesis pass:
1. **No fragmentation between cost methods.** When FIFO/lot/serial cost methods land, they cannot use a different storage paradigm than standard + WAC. Every real-cost method must dispatch through one unified mechanism.
2. **Inventory-first is the wrong foundational posture.** Services-only companies must be supported as first-class. The current document-wrapper layer's heavy inventory-domain bias is structural debt that gets worse with every accreted feature.

**What we're building toward** (the convergence target):

```
posting_lines (universal posting-line core; row-per-pair preserved per acct-1584)
   │
   ├── posting_line_sources        (source linkage extension)
   ├── posting_line_currencies     (multi-currency extension)
   ├── posting_line_dimensions     (cost-center / project / dept extension)
   ├── posting_line_inventory      (the inventory bridge — only present when event has inventory)
   │      │
   │      ├── cost_layer_id ──→ cost_layers (FIFO/LIFO subledger)
   │      ├── lot_id        ──→ inventory_lots (lot subledger)
   │      └── unit_id       ──→ inventory_units (serial subledger)
   │
   ├── posting_line_custom         (JSONB; ad-hoc segments, graduates to typed dimensions)
   │
   └── inventory_movements          (foundational real-cost-method subledger; all real-cost methods write here)
```

For services-only companies, `posting_line_inventory` rows simply don't exist for pure-financial events; the dispatcher detects the absence and skips cost-method dispatch entirely. **One unified mechanism, services-friendly.**

**What we're NOT doing.**
- **Not flipping row-per-pair to row-per-leg.** acct-1584 stands. `posting_lines` keeps one row per (debit, credit, amount); 3+ leg postings continue to use multiple rows with shared `document_id`.
- **Not building Tier-2 WASM** (acct-46rx; permanent).
- **Not adding a generic `postings` header** (per-document headers already serve the role; defer until API/UI tier).
- **Not adopting Tier-1 full rules engine** (rule_sets / journal_line_rules / account_rules / mapping_sets) — defer until customer-customizable account derivation surfaces.

> **Schema-consolidation note (acct-dhzc, 2026-05-07).** The `transfers` → `posting_lines` rename was originally deferred ("cosmetic; defer indefinitely"). It was lifted as part of the broader 104-mig → 21-file consolidation epic, baked in from the first migration. All names in this plan use the post-consolidation form (`posting_lines`, `posting_line_*`, `post_posting_lines`, `_post_posting_lines_apply_event`, `posting_lines_provisional`); the original 104 incremental migrations live verbatim in `db/archive_migrations/` for git-blame fidelity.

**Phase summary.**

| Phase | Title | Status | Effort | Blocked-by |
|---|---|---|---|---|
| A | Foundational columns (`posting_layer`, `legal_entity_id`) | DONE | — | — |
| B | Extension table foundations (sources, currencies, dimensions) | TO FILE | 6–9 weeks | A |
| C | `posting_line_inventory` extension | TO FILE | 3–6 weeks | B |
| D | `inventory_movements` foundational subledger | TO FILE | 6–10 weeks | C |
| E | Method-specific subledgers (FIFO / lot / serial) | RE-SCOPE | 3–9 mo | D |
| F | Services-domain wrappers | TO FILE | 3–6 weeks | A (can run parallel to D-E) |
| G | Chart-of-accounts conversion | RE-SCOPE | 4–6 weeks | A (can run parallel to D-E) |

**Stop points** exist between every phase boundary. Each phase ships independently with passing tests + ci-check + reconciliation invariants. The user can pause / redirect at any phase boundary without leaving the codebase in a broken intermediate state.

---

## §2 Convergence Target

The full proposed shape, distilled from `research/ledger-architecture-proposal.md` §2–§4.

### §2.1 Universal core (`posting_lines`)

The proposal §2.3.1 specifies a moderately-narrow universal core (~17 columns, ~120–150 bytes/row):

> ```sql
> CREATE TABLE posting_lines (
>   posting_id          BIGINT       NOT NULL,
>   line_seq            INT          NOT NULL,
>   posting_date        DATE         NOT NULL,
>   fiscal_year         SMALLINT     NOT NULL,
>   fiscal_period       SMALLINT     NOT NULL,
>   created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
>   event_class         INT          NOT NULL,
>   event_type          INT          NOT NULL,
>   source_module       SMALLINT     NOT NULL,
>   legal_entity_id     INT          NOT NULL,
>   ledger_id           SMALLINT     NOT NULL,
>   posting_layer       INT          NOT NULL,
>   account_id          BIGINT       NOT NULL,
>   amount_functional   NUMERIC(19,4) NOT NULL,
>   currency_functional CHAR(3)      NOT NULL,
>   rule_set_id         INT,
>   rule_set_version    INT,
>   idempotency_key     UUID         NOT NULL,
>   created_by_user     INT          NOT NULL,
>   PRIMARY KEY (posting_id, line_seq)
> ) PARTITION BY RANGE (posting_date);
> ```

Our `posting_lines` table (consolidated `0009`; built on the lineage that began with archive `mig 0007`) plays this role with a row-per-pair shape (acct-1584) — ONE row per (`debit_account_id`, `credit_account_id`, `amount`) instead of one row per leg. Convergence keeps this divergence; we add **extension tables** alongside `posting_lines`, not within it.

### §2.2 Five typed extensions (proposal §2.3.2 – §2.3.6)

Each extension is **present-only-when-its-data-applies** — rows exist only for events whose data populates the extension. `posting_id` + `line_seq` (in our case `posting_line_id` only, since row-per-pair has no `line_seq`) is the composite FK back to the core.

**Dimensions** (`posting_line_dimensions`, EAV-typed):

> ```sql
> CREATE TABLE posting_line_dimensions (
>   posting_id      BIGINT       NOT NULL,
>   line_seq        INT          NOT NULL,
>   posting_date    DATE         NOT NULL,
>   dimension_type  SMALLINT     NOT NULL,
>   dimension_value BIGINT       NOT NULL,
>   PRIMARY KEY (posting_id, line_seq, dimension_type)
> );
> ```

Plus a `dimension_types` lookup (`dimension_type`, `name`, `reference_table`). Cost center, profit center, project, customer, vendor, asset — each is a row in `dimension_types`; instances populate `posting_line_dimensions` only when relevant.

**Multi-currency** (`posting_line_currencies`):

> ```sql
> CREATE TABLE posting_line_currencies (
>   posting_id            BIGINT       NOT NULL,
>   line_seq              INT          NOT NULL,
>   posting_date          DATE         NOT NULL,
>   amount_transaction    NUMERIC(19,4) NOT NULL,
>   currency_transaction  CHAR(3)       NOT NULL,
>   fx_rate_to_functional NUMERIC(19,9) NOT NULL,
>   amount_group          NUMERIC(19,4),
>   fx_rate_to_group      NUMERIC(19,9),
>   amount_local          NUMERIC(19,4),
>   PRIMARY KEY (posting_id, line_seq)
> );
> ```

Convention: when the row is absent, transaction currency = functional currency, group currency = computed via standard translation, no local override.

**Source linkage** (`posting_line_sources`):

> ```sql
> CREATE TABLE posting_line_sources (
>   posting_id              BIGINT       NOT NULL,
>   line_seq                INT          NOT NULL,
>   posting_date            DATE         NOT NULL,
>   source_doc_type         SMALLINT,
>   source_doc_id           BIGINT,
>   source_doc_line         INT,
>   source_doc_external_ref VARCHAR(64),
>   reverses_line_id        BIGINT,
>   parent_posting_id       BIGINT,
>   intercompany_pair_id    UUID,
>   custom_module_hash      CHAR(64),
>   created_by_process      VARCHAR(64),
>   PRIMARY KEY (posting_id, line_seq)
> );
> ```

**Inventory** (`posting_line_inventory`) — the bridge to method-specific subledgers:

> ```sql
> CREATE TABLE posting_line_inventory (
>   posting_id      BIGINT       NOT NULL,
>   line_seq        INT          NOT NULL,
>   posting_date    DATE         NOT NULL,
>   product_id      BIGINT       NOT NULL,
>   quantity        NUMERIC(19,6) NOT NULL,
>   quantity_uom    VARCHAR(10)   NOT NULL,
>   unit_cost       NUMERIC(19,4),
>   cost_layer_id   BIGINT,
>   lot_id          BIGINT,
>   unit_id         BIGINT,
>   PRIMARY KEY (posting_id, line_seq)
> );
> ```

`cost_layer_id` / `lot_id` / `unit_id` are nullable FKs to method-specific subledgers (FIFO / lot / serial). For aggregate methods (standard, MA, WA), all three are NULL but the row still exists if the event has inventory.

**Custom** (`posting_line_custom`, JSONB; graduates to typed dimensions when stable):

> ```sql
> CREATE TABLE posting_line_custom (
>   posting_id      BIGINT       NOT NULL,
>   line_seq        INT          NOT NULL,
>   posting_date    DATE         NOT NULL,
>   custom_segments JSONB        NOT NULL,
>   PRIMARY KEY (posting_id, line_seq)
> );
> ```

(We will likely defer this extension to Phase H+ — no concrete need at present.)

### §2.3 Foundational subledger (`inventory_movements`, cost-methods doc §4.3)

The cost-methods doc identifies `inventory_movements` as the **foundational subledger that ALL real-cost methods write to**. Receipts, issues, stock transfers, adjustments, scrap, returns — every event that affects inventory state gets one `inventory_movements` row. Per-method specializations (FIFO layers, lot rows, serial rows) extend FROM `inventory_movements`, not REPLACE it.

> ```sql
> CREATE TABLE inventory_movements (
>   movement_id BIGSERIAL PRIMARY KEY,
>   product_id BIGINT NOT NULL,
>   legal_entity_id INT NOT NULL,
>   cost_book_id INT NOT NULL,
>   location_id BIGINT NOT NULL,
>   event_type SMALLINT NOT NULL,
>   movement_date DATE NOT NULL,
>   quantity NUMERIC(19,6) NOT NULL,
>   standard_unit_cost NUMERIC(19,4),
>   actual_unit_cost NUMERIC(19,4),
>   cost_currency CHAR(3) NOT NULL,
>   ppv_amount NUMERIC(19,4),
>   posting_id BIGINT NOT NULL,
>   posting_line_seq INT NOT NULL,
>   source_doc_type SMALLINT,
>   source_doc_id BIGINT,
>   source_doc_line INT,
>   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
> ) PARTITION BY RANGE (movement_date);
> ```

(In our codebase, `posting_id` + `posting_line_seq` becomes a single `posting_line_id BIGINT REFERENCES posting_lines(id)` since we're row-per-pair without explicit line_seq.)

### §2.4 Method-specific subledgers

**FIFO/LIFO** (cost-methods doc §7.2 + §7.3):

> ```sql
> CREATE TABLE cost_layers (
>   layer_id BIGSERIAL PRIMARY KEY,
>   product_id BIGINT NOT NULL,
>   legal_entity_id INT NOT NULL,
>   cost_book_id INT NOT NULL,
>   location_id BIGINT NOT NULL,
>   receipt_movement_id BIGINT NOT NULL REFERENCES inventory_movements,
>   receipt_date DATE NOT NULL,
>   original_quantity NUMERIC(19,6) NOT NULL,
>   unit_cost NUMERIC(19,4) NOT NULL,
>   cost_currency CHAR(3) NOT NULL,
>   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
> ) PARTITION BY RANGE (receipt_date);
>
> CREATE TABLE cost_layer_depletions (
>   depletion_id BIGSERIAL PRIMARY KEY,
>   layer_id BIGINT NOT NULL REFERENCES cost_layers,
>   issue_movement_id BIGINT NOT NULL REFERENCES inventory_movements,
>   issue_date DATE NOT NULL,
>   depleted_quantity NUMERIC(19,6) NOT NULL CHECK (depleted_quantity != 0),
>   unit_cost NUMERIC(19,4) NOT NULL,
>   cost_amount NUMERIC(19,4) NOT NULL,
>   posting_id BIGINT NOT NULL,
>   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
> ) PARTITION BY RANGE (issue_date);
> ```

Three load-bearing properties (cost-methods §7.2):
1. **Layer is immutable.** `original_quantity` and `unit_cost` never change.
2. **One layer per receipt.** Even same-day same-cost receipts create separate layers.
3. **No `current_quantity` column.** Residual is computed from depletions.

**Lot** (cost-methods §9.2 + §9.3) and **serial** (cost-methods §10.2 + §10.3) follow the same shape with `inventory_lots + inventory_lot_events` and `inventory_units + inventory_unit_events` respectively. Schema details deferred to Phase E sub-epic specifications.

### §2.5 Cross-cutting concerns (proposal §4.1 – §4.8)

Per the proposal's cross-cutting chapter:
- **Outbox** (`accounting_events` queue + worker drain) — already tracked as `acct-c4p`; not in this convergence's scope.
- **Idempotency** — `posting_lines.idempotency_key UNIQUE` already plays this role; extension writes piggyback.
- **Bi-temporal** — `posting_lines.business_date + posted_at` already model effective + transaction time; `effective_at` on configuration tables (BOM, standard_costs) covers configuration bi-temporal.
- **Multi-currency** — Phase B2 introduces functional currency at the legal-entity level + the `posting_line_currencies` extension.
- **Period close** — `close_period` + `close_hooks` registry already orchestrates.
- **Reconciliation** — `run_daily_reconciliation` extended at each phase boundary with new invariants.
- **Cross-shard** — gated on multi-entity (acct-3gzh); foundation laid by `legal_entity_id` column (acct-ewhs).

---

## §3 Current State vs Target State

| Concern | Current | Target | Phase |
|---|---|---|---|
| Universal posting-line core | `posting_lines` (row-per-pair, BIGSERIAL id, idempotency_key UNIQUE; renamed from `transfers` via acct-dhzc 2026-05-07) | unchanged shape | — |
| Posting layer dimension | `posting_lines.posting_layer` SMALLINT (acct-chzx) | unchanged | A ✓ |
| Multi-entity dimension | `posting_lines.legal_entity_id` SMALLINT, `accounts.legal_entity_id` SMALLINT (acct-ewhs) | unchanged; promoted to real FK + RBAC at acct-3gzh | A ✓ |
| Source linkage | per-document FK columns scattered (po_line_id, wo_event_id, etc. on document tables) | centralized in `posting_line_sources` | B1 |
| Multi-currency | `accounts.currency` per-account; per-document `currency` fields; `fx_rates` table | functional-currency at legal entity level + `posting_line_currencies` extension when transaction ≠ functional | B2 |
| Dimensions | inline composition columns on `accounts` (sku_id, location_id, routing_op, counterparty_id) | `posting_line_dimensions` extension; `dimension_types` lookup; richness preserved on accounts initially, possibly extracted later | B3 |
| Inventory bridge | `posting_lines.qty` + `posting_lines.routing_op` + `accounts.sku_id` + `accounts.location_id` (inline) | `posting_line_inventory` extension; posting_lines stays universal core | C |
| Foundational real-cost subledger | none (cost methods read state from `posting_lines` history) | `inventory_movements` foundational subledger; standard + WAC + FIFO + lot + serial all write here | D |
| FIFO/LIFO method | not implemented (acct-8gg open) | `cost_layers + cost_layer_depletions` extending `inventory_movements` | E1 |
| Lot method | not implemented (acct-uze open) | `inventory_lots + inventory_lot_events` extending `inventory_movements` | E2 |
| Serial method | not implemented (acct-0kz open) | `inventory_units + inventory_unit_events` extending `inventory_movements` | E3 |
| Services document wrappers | none (post_inventory_adjustment / post_cost_adjustment serve as approximate fallback) | `post_journal_entry` (generic) + `post_service_bill` + `post_expense_report` | F |
| Account taxonomy | PG enum `account_kind` (46 values, lineage acct-jg2 + by-products epic) | row table `account_kinds` + chart-of-accounts hierarchy | G |
| Cost-method dispatch | `cost_method_strategies` registry (acct-w0lo); 4 strategies registered | extended per phase; same registry shape; no Tier-1 full rules engine | — |
| Close-hook orchestration | `close_hooks` registry (acct-w0lo); 3 hooks at ordering 10/20/30 | extended in Phase D for `inventory_movements`-aware variants | — |
| Append-only enforcement | trigger-blocked UPDATE/DELETE on `posting_lines` | unchanged; extensions inherit | — |
| Generic `postings` header | none (per-document tables serve the role) | DEFERRED (Phase H+) | — |
| Row-per-pair vs row-per-leg | row-per-pair (acct-1584) | preserved per user decision 2026-05-07 | — |
| Tier-2 WASM rules engine | none (out-of-scope acct-46rx) | DEFERRED (permanent) | — |

---

## §4 Phased Migration Roadmap

Each phase below specifies the 9 fields per the plan: **prerequisites / deliverables / schema additions / dispatcher changes / backfill SQL / reconciliation invariants / test coverage / rollback plan / stop-point criteria**.

### §4.A Phase A — DONE

**Prerequisites.** None.
**Deliverables.** `posting_layer` (acct-chzx) and `legal_entity_id` (acct-ewhs) columns added to `posting_lines` and (legal_entity_id only) `accounts`; `legal_entities` table promoted to a real entity (consolidated `0005`). Bitmask convention documented (CLAUDE.md). Default values preserve all existing behavior.
**Dispatcher.** Unchanged; new columns auto-populate via DEFAULT.
**Backfill.** Implicit via DEFAULT.
**Reconciliation.** No new invariants.
**Test.** All existing tests pass unchanged.
**Rollback.** Down migrations exist (drop columns).
**Stop-point.** Shipped pre-consolidation; absorbed into the consolidated `0005`/`0007`/`0009` files via acct-dhzc 2026-05-07. ✓

### §4.B Phase B — Extension table foundations (3 sub-issues)

**Prerequisites.** Phase A complete (legal_entity_id required for B2's functional-currency lookup).

**B1 — `posting_line_sources` extension.** ✅ **SHIPPED 2026-05-07 (acct-wb75.1.1).** Originally landed as `transfer_line_sources` (archive `mig 0104`); renamed in lockstep with the `transfers` → `posting_lines` table rename via the schema-consolidation epic (acct-dhzc) and now lives in consolidated `0019_posting_line_extensions`.

> **Discovery during execution.** The proposal's `posting_line_sources.source_doc_type` / `source_doc_id` / `source_doc_line_id` fields are already first-class on our `posting_lines` table (`document_kind TEXT`, `document_id UUID`, `document_line_id UUID` — present since the original `transfers` schema). Re-encoding them in the extension would create fragmentation — two parallel encodings of the same source link. Per acct-1584 (row-per-pair preserved) and the convergence-additive principle, the extension adds **only the four NEW fields the proposal contributes that we do not have**. The eventual `document_kind` TEXT → SMALLINT FK conversion tracks alongside acct-2thf (account_kind enum→row); not in scope for B1.

- *Deliverables.* `posting_line_sources` extension table (1:1 PK = posting_line_id); `_post_posting_lines_apply_event` extended to write the row when any of the four fields is non-NULL on the event JSONB.
- *Schema added (consolidated `0019_posting_line_extensions`; original archive `mig 0104`).*
  ```sql
  CREATE TABLE posting_line_sources (
    posting_line_id           BIGINT PRIMARY KEY REFERENCES posting_lines(id),
    reverses_posting_line_id  BIGINT REFERENCES posting_lines(id),  -- new
    parent_document_id    UUID,                              -- new
    intercompany_pair_id  UUID,                              -- new
    created_by_process    VARCHAR(64),                       -- new
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (reverses_posting_line_id IS NOT NULL          -- not-all-NULL
        OR parent_document_id IS NOT NULL
        OR intercompany_pair_id IS NOT NULL
        OR created_by_process IS NOT NULL),
    CHECK (reverses_posting_line_id IS NULL              -- no self-reversal
        OR reverses_posting_line_id <> posting_line_id)
  );
  -- partial indexes on each non-null filter
  ```
- *Dispatcher.* `_post_posting_lines_apply_event` reads four optional fields off `p_event`: `reverses_posting_line_id`, `parent_document_id`, `intercompany_pair_id`, `created_by_process`. INSERTs one `posting_line_sources` row when any are non-NULL. Skips when all four are NULL (most posting_lines — pure-NULL extension rows are CHECK-rejected).
- *Backfill SQL.* **NONE.** All four fields are forward-only. Historical posting_lines (pre-acct-wb75.1.1) have no extension row. Any future reversal / nested-doc / intercompany / audit caller opts in via the event JSONB.
- *Reconciliation.* FK + CHECK enforce structural integrity; no row-count invariant added (the dispatcher writes the extension only when fields are present, so a count-vs-posting_lines mismatch is expected and not an error).
- *Test.* `tests/posting_line_sources_t1.rs` — 9 cases: 4 schema constraints (not-all-NULL, no-self-reversal, FK posting_line_id, FK reverses_posting_line_id, PK uniqueness); 5 dispatcher behaviors (single-field accepted, multi-field accepted, no-fields-skipped, with-fields-written, reverses-pointer-roundtrip). Property test extension skipped — the dispatcher's branch is a single conditional INSERT with no method-specific math; deterministic tests cover the surface.
- *Rollback.* `0019_posting_line_extensions.down.sql` drops the table; the consolidated `_post_posting_lines_apply_event` body in `0014` writes the extension row only when fields are present, so a missing extension table simply means those fields go unwritten.
- *Stop-point — REACHED.* Tests pass; ci-check clean; closed acct-wb75.1.1.

**B2 — `posting_line_currencies` extension + functional-currency model. — SHIPPED 2026-05-07 (acct-wb75.1.2).**

- *Deliverables — DONE.* `posting_line_currencies` extension table; `legal_entities.functional_currency` (already shipped in consolidated `0005_legal_entities`); backfill from `accounts.currency`; `_post_posting_lines_apply_event` extended to write the row when transaction currency ≠ functional currency; `run_daily_reconciliation` extended with check #3.
- *Schema added (consolidated `0022_posting_line_currencies`).*
  ```sql
  CREATE TABLE posting_line_currencies (
    posting_line_id       BIGINT PRIMARY KEY REFERENCES posting_lines(id),
    amount_transaction    BIGINT NOT NULL,                 -- BIGINT integer cents
    currency_transaction  CHAR(3) NOT NULL,                -- to match posting_lines.amount
    fx_rate_to_functional NUMERIC(20, 10) NOT NULL,        -- matches fx_rates.rate precision
    created_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT posting_line_currencies_fx_rate_positive CHECK (fx_rate_to_functional > 0)
  );
  CREATE INDEX posting_line_currencies_by_currency
    ON posting_line_currencies (currency_transaction);
  ```
  *(Note: `amount_transaction` typed BIGINT, not the originally-drafted NUMERIC(19,4), to match the project's integer-cents `posting_lines.amount` convention. Sub-cent precision becomes a follow-up if/when needed. `legal_entities.functional_currency` was already shipped in consolidated `0005_legal_entities`.)*
- *Dispatcher.* `_post_posting_lines_apply_event` reads `p_c_acct.currency` and `legal_entities.functional_currency WHERE id = p_c_acct.legal_entity_id` (R2: credit-side governs). If transaction = functional or `ledger_kind = 'qty'`: skip. Else: look up `fx_rates` effective at `business_date` (raises **P0050** if missing); INSERT one row.
- *Backfill SQL.* For each existing transfer where account.currency ≠ 'USD' (the default functional), INSERT a `posting_line_currencies` row with fx_rate=1 (since today\u2019s posting_lines are already in their account's currency, with no functional translation). Rows where account.currency = 'USD' get no extension row (functional = transaction).
- *Reconciliation.* `run_daily_reconciliation` extended with **check #3** (`currency_extension_amount_mismatch`): every `posting_line_currencies` row's `amount_transaction` must equal its paired `posting_lines.amount`. Note: the originally-drafted invariant `amount_transaction × fx_rate ≈ amount` is dormant at this phase — `posting_lines.amount` stays in transaction currency, so the multiplication is meaningful only after a future migration translates amounts to functional currency (Phase D / acct-3gzh). The deterministic equality form is what we can hold today.
- *Test.* `tests/posting_line_currencies_t1.rs` — 8 cases: 3 schema (`fx_rate > 0` CHECK, FK, PK uniqueness); 5 dispatcher (skip when transaction = functional, write when transaction ≠ functional with correct fx_rate, skip qty legs, P0050 on missing fx_rate, amount_transaction = amount across multiple postings).
- *Rollback.* `0022_posting_line_currencies.down.sql` drops the table. The CREATE OR REPLACE on `_post_posting_lines_apply_event` is not reverted in down (project convention: down is best-effort for ci-check symmetry).
- *Stop-point — REACHED.* 664/0/9 tests; ci-check clean; closed acct-wb75.1.2.

**B3 — `posting_line_dimensions` extension + `dimension_types` lookup.**

- *Deliverables.* New tables; backfill from inline composition columns on `accounts` (counterparty_id → 'customer'/'vendor' dimension; sku_id → 'product' dimension; location_id → 'location'; routing_op → 'routing_op'); dispatcher writes alongside posting_lines.
- *Schema additions.*
  ```sql
  CREATE TABLE dimension_types (
    dimension_type SMALLINT PRIMARY KEY,
    name VARCHAR(64) NOT NULL UNIQUE,
    reference_table VARCHAR(64) NOT NULL,
    description TEXT
  );
  INSERT INTO dimension_types (dimension_type, name, reference_table) VALUES
    (1, 'customer', 'customers'),
    (2, 'vendor', 'vendors'),
    (3, 'product', 'skus'),
    (4, 'location', 'locations'),
    (5, 'routing_op', 'wo_routings'),
    (6, 'cost_center', 'cost_centers'),       -- table TBD; future
    (7, 'profit_center', 'profit_centers'),   -- TBD
    (8, 'project', 'projects'),               -- TBD
    (9, 'department', 'departments');         -- TBD

  CREATE TABLE posting_line_dimensions (
    posting_line_id BIGINT NOT NULL REFERENCES posting_lines(id),
    dimension_type SMALLINT NOT NULL REFERENCES dimension_types,
    dimension_value BIGINT,                       -- entity_id from reference_table; nullable for UUID-keyed dimensions
    dimension_value_uuid UUID,                    -- alternate column for UUID-keyed dimensions (sku, location)
    PRIMARY KEY (posting_line_id, dimension_type),
    CHECK (dimension_value IS NOT NULL OR dimension_value_uuid IS NOT NULL)
  );
  CREATE INDEX ON posting_line_dimensions (dimension_type, dimension_value);
  CREATE INDEX ON posting_line_dimensions (dimension_type, dimension_value_uuid)
    WHERE dimension_value_uuid IS NOT NULL;
  ```
  *(Note: dimension_value vs dimension_value_uuid is a workaround for the mixed BIGINT/UUID typing of our existing entity tables. Phase G's chart-of-accounts conversion may unify these.)*
- *Dispatcher.* For each transfer, walk debit + credit account composition columns; emit one `posting_line_dimensions` row per non-null inline column on either account.
- *Backfill SQL.* Multi-pass JOIN against posting_lines + accounts; emit one row per (transfer, dimension_type) where the relevant inline column is populated.
- *Reconciliation.* Invariant: every transfer-with-inventory-touching-account has at least a 'product' + 'location' dimension row; every transfer-with-counterparty-account has 'customer' or 'vendor' dimension row.
- *Test.* `tests/posting_line_dimensions_t1.rs`; property tests.
- *Rollback.* DROP `posting_line_dimensions`, `dimension_types`. Down migration symmetry verified by ci-check.
- *Stop-point.* Same as B1/B2.

**Phase B aggregate.** Three sub-issues; each commits independently. Total ~6–9 weeks. Phase C cannot start until B1/B2/B3 all ship.

### §4.C Phase C — `posting_line_inventory` extension

**Prerequisites.** Phase B complete (especially B3, since `posting_line_inventory` partially overlaps with the 'product' dimension; we want the dimensions extension live first to avoid double-encoding).

**Deliverables.** New `posting_line_inventory` table; backfill from existing `posting_lines.qty` + credit-side `accounts.sku_id`; dispatcher writes for every inventory-touching transfer.

**Schema additions.**
```sql
CREATE TABLE posting_line_inventory (
  posting_line_id BIGINT PRIMARY KEY REFERENCES posting_lines(id),
  product_id UUID NOT NULL REFERENCES skus(id),
  quantity NUMERIC(19,6) NOT NULL,
  qty_uom VARCHAR(10) NOT NULL DEFAULT 'EA',
  unit_cost NUMERIC(19,4),
  cost_layer_id BIGINT,                         -- FK to cost_layers (Phase E1)
  lot_id BIGINT,                                -- FK to inventory_lots (Phase E2)
  unit_id BIGINT,                               -- FK to inventory_units (Phase E3)
  cost_method_at_event cost_method NOT NULL,    -- snapshot like cost_method_at_receipt (acct-6d8 pattern)
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ON posting_line_inventory (product_id);
CREATE INDEX ON posting_line_inventory (cost_layer_id) WHERE cost_layer_id IS NOT NULL;
CREATE INDEX ON posting_line_inventory (lot_id) WHERE lot_id IS NOT NULL;
CREATE INDEX ON posting_line_inventory (unit_id) WHERE unit_id IS NOT NULL;
```

**Dispatcher.** `_post_posting_lines_apply_event` extended: when `transfer.qty IS NOT NULL`, INSERT a `posting_line_inventory` row. `product_id` resolves from credit-side (R2) `account.sku_id` (or debit-side if credit doesn't have one). `quantity` = `transfer.qty`. `unit_cost` = `transfer.amount / transfer.qty` for value-leg-paired rows. `cost_method_at_event` = `sku.cost_method` (or method-at-receipt snapshot if available). FIFO / lot / serial FK columns NULL for now (Phase E populates).

**Backfill SQL.**
```sql
INSERT INTO posting_line_inventory (posting_line_id, product_id, quantity, qty_uom, unit_cost, cost_method_at_event, created_at)
SELECT
  t.id,
  COALESCE(c.sku_id, d.sku_id),
  ABS(t.qty),
  'EA',
  CASE WHEN t.qty != 0 THEN t.amount::numeric / ABS(t.qty)::numeric ELSE NULL END,
  s.cost_method,
  t.posted_at
FROM posting_lines t
JOIN accounts d ON d.id = t.debit_account_id
JOIN accounts c ON c.id = t.credit_account_id
LEFT JOIN skus s ON s.id = COALESCE(c.sku_id, d.sku_id)
WHERE t.qty IS NOT NULL;
```
Estimated row count: count of inventory-touching posting_lines (every PO receipt, op_move, scrap, wo_complete, so_ship, return, etc.). Likely <1M rows in current state; single-pass acceptable. If >1M, batch via `WHERE id BETWEEN N AND M` ranges committed individually.

**Reconciliation invariants.**
- `count(posting_lines WHERE qty IS NOT NULL) = count(posting_line_inventory)`.
- For each `posting_line_inventory` row, `quantity = ABS(transfer.qty)` and `unit_cost ≈ transfer.amount / quantity` (within rounding).
- `posting_line_inventory.product_id IS NOT NULL` always.

**Test coverage.**
- `tests/posting_line_inventory_t1.rs` (T1 invariants).
- Property test extending existing `tests/property_post_transfers.rs`: every inventory-touching call to `post_posting_lines` produces exactly one `posting_line_inventory` row.
- Audit test that backfill is internally consistent.

**Rollback.** DROP `posting_line_inventory`. Existing posting_lines unaffected. Down migration tested.

**Stop-point.** Mig applies, ci-check clean, tests pass, recon clean, commit, close issue. Phase D can begin.

### §4.D Phase D — `inventory_movements` foundational subledger (6 sub-issues)

**Prerequisites.** Phase C complete (`posting_line_inventory` in place to bridge posting_lines → inventory_movements).

This is the **keystone phase**. After Phase D, ALL real-cost methods unify through `inventory_movements`. Standard + WAC stop being "posting_lines-as-source-of-truth"; the subledger becomes source-of-truth for cost-flow questions, posting_lines carries GL aggregate.

**D1 — `inventory_movements` schema.**
```sql
CREATE TABLE inventory_movements (
  movement_id BIGSERIAL PRIMARY KEY,
  product_id UUID NOT NULL REFERENCES skus(id),
  legal_entity_id SMALLINT NOT NULL REFERENCES legal_entities(id),
  cost_book_id SMALLINT NOT NULL DEFAULT 1,         -- single-cost-book today; multi-book Phase 3+
  location_id UUID NOT NULL REFERENCES locations(id),
  event_type SMALLINT NOT NULL,                      -- enum-table; receipt/issue/transfer_out/transfer_in/adjustment/scrap/...
  movement_date DATE NOT NULL,
  quantity NUMERIC(19,6) NOT NULL,                   -- signed: positive=receipt-like, negative=issue-like
  standard_unit_cost NUMERIC(19,4),
  actual_unit_cost NUMERIC(19,4) NOT NULL,
  cost_currency CHAR(3) NOT NULL,
  ppv_amount NUMERIC(19,4) DEFAULT 0,
  posting_line_id BIGINT NOT NULL REFERENCES posting_lines(id),
  source_doc_type SMALLINT,
  source_doc_id UUID,
  source_doc_line_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
) PARTITION BY RANGE (movement_date);

CREATE INDEX ON inventory_movements (product_id, location_id, movement_date);
CREATE INDEX ON inventory_movements (posting_line_id);
CREATE TABLE inventory_movement_event_types (
  event_type SMALLINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL UNIQUE
);
INSERT INTO inventory_movement_event_types VALUES
  (1, 'receipt'), (2, 'issue'), (3, 'transfer_out'), (4, 'transfer_in'),
  (5, 'adjustment_in'), (6, 'adjustment_out'), (7, 'scrap'),
  (8, 'wo_consume'), (9, 'wo_produce'), (10, 'op_move_out'), (11, 'op_move_in'),
  (12, 'return_in'), (13, 'return_out'),
  (14, 'standard_revaluation'), (15, 'periodic_revaluation'), (16, 'cost_adjustment');
```

Initial monthly partitioning per cost-methods doc §15.2.

**D2 — Standard cost dispatcher writes to `inventory_movements`.**

`_compute_amount_standard_outbound` (consolidated `0013` strategy fn; acct-w0lo) extended to INSERT one `inventory_movements` row alongside the transfer it produces. Movement's `standard_unit_cost` = `resolve_standard_cost_at(sku, business_date)`; `actual_unit_cost` = same for standard (no IPV at issue time). Signs: receipts positive quantity, issues negative.

**D3 — WAC dispatchers write to `inventory_movements`.**

`_compute_amount_wac_perpetual_outbound`, `_compute_amount_wac_periodic_outbound`, `_compute_amount_wac_retroactive_outbound` extended similarly. `actual_unit_cost` = the running average computed at the event. WAC-periodic / WAC-retroactive provisional flagging continues to work via `posting_lines_provisional`; close hooks recompute and post variance to both `posting_lines` (existing) AND `inventory_movements` (new) for traceability.

**D4 — Backfill `inventory_movements` from existing inventory posting_lines.**

Batched migration. For each existing inventory-touching transfer (where `posting_line_inventory` row exists from Phase C):

```sql
INSERT INTO inventory_movements (
  movement_id, product_id, legal_entity_id, cost_book_id, location_id,
  event_type, movement_date, quantity, standard_unit_cost, actual_unit_cost,
  cost_currency, posting_line_id, source_doc_type, source_doc_id, created_at
)
SELECT
  nextval('inventory_movements_movement_id_seq'),
  tli.product_id,
  t.legal_entity_id,
  1,                                              -- single book
  COALESCE(c.location_id, d.location_id),
  CASE t.reason
    WHEN 'po_receipt' THEN 1                       -- receipt
    WHEN 'so_ship' THEN 2                          -- issue
    WHEN 'op_move' THEN 11                         -- op_move_in
    WHEN 'wo_complete' THEN 9                      -- wo_produce
    WHEN 'rm_issue_to_wo' THEN 8                   -- wo_consume
    WHEN 'scrap' THEN 7
    WHEN 'inventory_adjustment' THEN
      CASE WHEN t.qty > 0 THEN 5 ELSE 6 END        -- adjustment_in/out
    -- ...etc
    ELSE 5                                          -- catch-all adjustment_in
  END,
  t.business_date,
  CASE WHEN c.kind LIKE 'stock_%' OR c.kind LIKE 'inv_value_%' THEN -ABS(t.qty)
       ELSE ABS(t.qty) END,                         -- sign per direction
  CASE WHEN s.cost_method = 'standard' THEN tli.unit_cost ELSE NULL END,
  tli.unit_cost,
  COALESCE(c.currency, d.currency),
  t.id,
  ts.source_doc_type,
  ts.source_doc_id,
  t.posted_at
FROM posting_lines t
JOIN posting_line_inventory tli ON tli.posting_line_id = t.id
LEFT JOIN posting_line_sources ts ON ts.posting_line_id = t.id
JOIN accounts d ON d.id = t.debit_account_id
JOIN accounts c ON c.id = t.credit_account_id
LEFT JOIN skus s ON s.id = tli.product_id
ORDER BY t.id;
-- Run in batches of 100k via WHERE t.id BETWEEN N AND M
```

**D5 — Reconciliation invariant in `run_daily_reconciliation`.**

Add invariant: for each `(product_id, location_id, period_id)`, `SUM(inventory_movements.quantity * actual_unit_cost) ≈ SUM(posting_lines.amount on inv_value_*)`. Tolerance: rounding (within 1 cent per row, scaled).

Alert via `reconciliation_alerts(alert_type='subledger_gl_divergence', ...)`.

**D6 — Close hooks updated.**

`wac_periodic_close_hook` and `wac_retroactive_close_hook` are extended to also update `inventory_movements.actual_unit_cost` for finalized provisional rows (when variance is computed at close). Two-write atomicity: close hook posts both the variance transfer AND the inventory_movements correction in one transaction.

For Phase D, conservative approach: close hooks continue reading from `posting_lines` / `posting_lines_provisional` (no logic change for cost computation); they ALSO write the inventory_movements correction. The recon invariant verifies the two stay consistent. A future epic can switch close hooks to read from `inventory_movements` (cleaner) once the foundation is stable.

**D test coverage.**
- New `tests/inventory_movements_t1.rs` schema invariants.
- Property test `tests/property_inventory_movements_consistency.rs`: every inventory-touching post_* function call produces exactly one inventory_movements row consistent with the transfer.
- Subledger ↔ GL recon test: backfill correctness + post-shipping correctness for the existing T2 matrices (po_receipt, wo_lifecycle, so_ship, etc.) — extended assertions.

**D rollback.**
DROP partitioned table `inventory_movements` (and partitions); DROP `inventory_movement_event_types`. Removes recon invariant. Existing posting_lines untouched.

**D stop-point.**
After all 6 sub-issues ship + recon clean for >7 days of new inventory activity. The user can pause here; subsequent Phase E uses the foundation but doesn't require it to be globally adopted yet.

### §4.E Phase E — Method-specific subledgers

**Prerequisites.** Phase D complete. `inventory_movements` is the source-of-truth for inventory state; method-specific tables extend with cost-flow-tracing detail.

**E1 — FIFO/LIFO via `cost_layers + cost_layer_depletions`.** Existing `acct-8gg`. Re-scope.

- *Deliverables.* `cost_layers` + `cost_layer_depletions` tables; FIFO dispatcher strategy registered in `cost_method_strategies`; close-hook for periodic-FIFO if needed; `posting_line_inventory.cost_layer_id` populated by FIFO branch.
- *Schema.* Per cost-methods §7.2 / §7.3 (verbatim in §2.4 above).
- *Dispatcher.* New `_compute_amount_fifo_outbound` registered. Reads layer state via `WHERE cost_layers.product_id = ... AND remaining_qty > 0 ORDER BY receipt_date ASC FOR UPDATE`. Greedy walk per cost-methods §7.4. Writes one `cost_layer_depletions` row per layer consumed; one `inventory_movements` row aggregating; one `posting_lines` row aggregating; one `posting_line_inventory` row pointing at the **first** consumed layer (the issue's `cost_layer_id` references one layer; the depletions table has the per-layer detail).
- *Backfill.* None (no existing FIFO data).
- *Recon.* Invariant: `SUM(cost_layers.original_qty) - SUM(cost_layer_depletions.depleted_qty) = current on-hand qty per (product, location)`.
- *Tests.* Per existing acct-8gg sub-issues.
- *Rollback.* DROP `cost_layers`, `cost_layer_depletions`; deregister FIFO strategy.
- *Stop-point.* All FIFO conformance cases pass; recon clean.

**E2 — Lot via `inventory_lots + inventory_lot_events`.** Existing `acct-uze`. Re-scope.

- Per cost-methods §9.2 / §9.3. FIFO depletion within lot if no allocation hint; FEFO / quality-priority / customer-specific allocation strategies (§9.5).
- Lot subledger tracks status changes (quality hold, expiration, transfer) with NULL `posting_id` for status-only events.
- Stop-point after acct-uze sub-issues ship + recon clean.

**E3 — Serial via `inventory_units + inventory_unit_events`.** Existing `acct-0kz`. Re-scope.

- Per cost-methods §10. "Lot-based costing with lot size 1."
- Volume strategies mandatory: time-based partitioning, status-based archival, columnar analytics.
- Stop-point after acct-0kz sub-issues ship + recon clean.

**Phase E aggregate.** 3 multi-month epics, sequenced or parallelized as bandwidth allows.

### §4.F Phase F — Services-domain wrappers (3 sub-issues)

**Prerequisites.** Phase A only (legal_entity_id). Can run parallel to D / E.

**F1 — `post_journal_entry`.** Generic posting wrapper. Caller provides arbitrary debit/credit account pairs + amount + business_date + idempotency_key + posted_by. No inventory side. No `posting_line_inventory` row produced. Used for accruals, reclasses, manual adjustments that don't fit existing inventory-domain wrappers.

**F2 — `post_service_bill`.** Vendor service invoice (no PO match, no goods receipt). Header table: `service_bills(id, vendor_id, currency, business_date, posted_by, idempotency_key UNIQUE, ...)`; line table: `service_bill_lines(id, service_bill_id, expense_account_id, amount, tax_amount, description, ...)`. Posts: `expense(account) DR + tax_payable DR / ap(vendor, currency) CR`. No staging account (services are immediately ap, not ap_unsettled — that GRNI dance is for goods).

**F3 — `post_expense_report`.** Employee expense reimbursement. Header: `expense_reports(id, employee_id, currency, business_date, ...)`; lines: `expense_report_lines(id, expense_report_id, expense_account_id, amount, ...)`. Posts: `expense(account) DR / cash(currency) CR` (or `ap_employee` if reimbursement is deferred).

**Test.** Each gets its own integration test + property test. The conformance harness gains an A21 group for services scenarios.

**Rollback.** DROP service_bills / service_bill_lines / expense_reports / expense_report_lines / post_journal_entry / post_service_bill / post_expense_report.

**Stop-point.** All three wrappers shipped; conformance A21 cases pass.

### §4.G Phase G — Chart-of-accounts conversion (lift acct-2thf + acct-v9sq)

**Prerequisites.** Phase A only. Can run parallel to D / E.

**G1 — `account_kinds` row table.**
```sql
CREATE TABLE account_kinds (
  id SMALLINT PRIMARY KEY,
  code VARCHAR(64) NOT NULL UNIQUE,
  ledger_kind VARCHAR(8) NOT NULL CHECK (ledger_kind IN ('qty','value')),
  normal_side balance_direction NOT NULL,
  parent_kind_id SMALLINT REFERENCES account_kinds(id),  -- COA hierarchy (Phase G2)
  description TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
```
Backfill: insert one row per existing `account_kind` enum value. Add `accounts.kind_id SMALLINT REFERENCES account_kinds(id)` (nullable initially); backfill from `accounts.kind` via name match. plpgsql sweep: replace every `kind = 'stock_available'` enum literal with FK lookup or join.

**G2 — Chart-of-accounts hierarchy.**

`account_kinds.parent_kind_id` populated to form a tree. Standard COA hierarchy: Assets → Current Assets → Inventory → (stock_*, inv_value_*); Liabilities → Current Liabilities → AP → (ap, ap_unsettled); Equity → Retained Earnings; etc.

**G3 — Drop `account_kind` enum.**

After G1 + G2 stable: `ALTER TABLE accounts DROP COLUMN kind;` (or rename to `kind_legacy` for safety); plpgsql sweep finalized; T2/T5 conformance regenerated against the row-table model.

**Test.** Existing tests rewritten to use `account_kinds.code` lookup instead of enum literals (mostly mechanical). New `tests/account_kinds_t1.rs` for hierarchy invariants.

**Rollback.** Re-add enum (problematic — ALTER TYPE ADD VALUE doesn't reverse cleanly). Mitigation: keep `accounts.kind` enum column alongside `kind_id` until G3 ships, allowing rollback of G3 specifically.

**Stop-point.** All existing tests pass against row-table; conformance regenerated; commit.

---

## §5 Risk Register / Ambiguities

Severity scale: **C** (critical, blocks phase) / **H** (high, must address in phase) / **M** (medium, monitor) / **L** (low, document and proceed).

### R1 — Row-per-pair preserved (severity: L)
*Status:* documented divergence (acct-1584). Convergence keeps it. Migration doc cross-links to acct-1584's load-bearing-decision in `ledger_design_consolidated_v0.md` Part IV §1.2.
*Mitigation:* none needed; user-confirmed decision.

### R2 — Backfill correctness at each phase boundary (severity: H per phase)
*Description.* Each phase's backfill SQL must produce extension rows consistent with reconciliation invariants. Backfill bugs would silently corrupt audit-trail.
*Mitigation.* (a) backfill runs as part of the up.sql migration (atomic with schema add); (b) recon invariant verifies post-backfill within the same transaction; (c) RAISE EXCEPTION on backfill mismatch aborts the migration; (d) test fixtures backfill via the same SQL path.

### R3 — Atomic-write discipline scales (severity: M after Phase D)
*Description.* Currently `post_*` functions write 1\u20134 posting_lines per event. After Phase D, they write posting_lines + extensions + inventory_movements — 3–5× more INSERT statements per event. Lock contention may surface under load.
*Mitigation.* Single-transaction discipline preserved. Perf-monitor with shape-baseline regression check at each phase boundary. If contention surfaces, batch-INSERT extensions in a single statement; revisit acct-c4p (pseudo-sync) earlier than planned.

### R4 — `posting_lines.idempotency_key UNIQUE` continues to anchor dedup (severity: L)
*Description.* Extension writes piggyback on transfer-level uniqueness via composite (`posting_line_id`) keys.
*Mitigation.* R6 dual-check at the post_* level continues to apply. New `tests/property_idempotency_with_extensions.rs` verifies replay correctness.

### R5 — Period close hook updates in Phase D (severity: H)
*Description.* `wac_periodic_close_hook` + `wac_retroactive_close_hook` read inventory state. Phase D adds `inventory_movements` as a co-source-of-truth. The hooks could either (a) be rewritten to read from `inventory_movements`, OR (b) keep reading posting_lines + the recon invariant enforces consistency.
*Mitigation.* Conservative approach: keep reading from posting_lines in Phase D; recon enforces consistency; eventual cutover to `inventory_movements` reads in a follow-up epic. Document this explicitly in D6.

### R6 — Functional-currency model introduction in Phase B2 (severity: M)
*Description.* Today no functional-currency concept exists at the legal-entity level. Phase B2 adds `legal_entities.functional_currency` (default USD; single-entity case trivially satisfies it). Existing posting_lines in non-USD account currencies need to be backfilled with `posting_line_currencies` rows showing fx_rate=1 (since today's "transaction currency" IS what we're treating as functional).
*Mitigation.* For single-entity USD-functional, the model works seamlessly. Multi-currency-functional scenarios surface only at multi-entity (acct-3gzh); B2 lays the foundation correctly and acct-3gzh later promotes legal_entities to first-class.

### R7 — Dispatcher rewires preserve R1–R7 class-confusion rules (severity: H)
*Description.* Each phase touches `_post_posting_lines_apply_event` / cost-method strategies. Risk of regressing R1 (per-class qty), R2 (credit-first SKU), R3 (solo-at-pool), R4 (FOR UPDATE discipline), R5 (single-leg variance), R6 (idempotency dual-check), R7 (audit-field post-lock).
*Mitigation.* Per-phase R1–R7 audit pass codified in `REVIEW.md` addendum at each phase boundary. Property tests added per phase verify invariants under random scenarios.

### R8 — `accounts` model preserved with rich partitioning (severity: L)
*Description.* `accounts.sku_id`, `location_id`, `routing_op`, `counterparty_id`, `currency` stay inline. The dimensions extension (B3) duplicates this data into `posting_line_dimensions`, but accounts doesn't change shape.
*Mitigation.* Phase B3 dispatcher reads inline composition AND emits dimensions extension rows. Phase G's COA conversion may eventually flatten accounts but is out of scope for this convergence.

### R9 — `postings` header deferred (severity: L)
*Description.* The proposal §2.3.7 specifies a `postings` header table for metadata that doesn't repeat per line. Our per-document tables (po_receipts, vendor_bills, etc.) already serve this role.
*Mitigation.* No header table added. If an API/UI tier later requires generic posting-header drill, add as Phase H.

### R10 — Test fixtures backfill at each phase boundary (severity: M)
*Description.* `tests/common/mod.rs` helpers create fixtures via direct INSERT into accounts + post_posting_lines calls. As extensions land, fixtures need to populate them.
*Mitigation.* Dispatcher writes extensions automatically (caller code unchanged); fixtures backfill on next reset_to_fixture. Per-phase: verify all 80+ test binaries pass without fixture-helper changes.

### R11 — ci-check schema digest shifts at each phase (severity: L)
*Description.* Adding tables shifts pg_dump digest.
*Mitigation.* Expected; not regression. Document in CLAUDE.md addendum per phase.

### R12 — Reservations stay as state, not postings (severity: L)
*Description.* `inventory_reservations` is its own first-class entity (consolidated `0011`). Convergence does NOT change this.
*Mitigation.* No change to reservation machinery. Documented explicitly.

### R13 — BOM2 / by-products / OSP / WO lifecycle unchanged at the document-wrapper level (severity: L)
*Description.* These are inventory-machinery layers atop `post_posting_lines`. They produce posting_lines + extensions automatically via the dispatcher.
*Mitigation.* No changes to BOM2 / by-product / OSP / WO code. Verify per phase that the WO lifecycle test matrices continue passing.

### R14 — Phase C backfill may surface latent inconsistencies (severity: H)
*Description.* Existing posting_lines where `qty IS NOT NULL` but credit-side `account.sku_id` is NULL (or vice versa). Possible if accounts were mis-configured historically.
*Mitigation.* Pre-flight audit query before C2 runs: `SELECT t.id FROM posting_lines t JOIN accounts c ON c.id=t.credit_account_id JOIN accounts d ON d.id=t.debit_account_id WHERE t.qty IS NOT NULL AND c.sku_id IS NULL AND d.sku_id IS NULL`. If any rows return, fix manually or document exception cases before backfill.

### R15 — Phase E (FIFO/lot/serial) blocked-by Phase D (severity: C)
*Description.* The existing acct-8gg / acct-uze / acct-0kz issues need to wire as blocked-by Phase D's foundational subledger. Otherwise someone might start them before D is ready.
*Mitigation.* `bd dep add` wired immediately upon meta-epic creation. acct-2c1m close-as-superseded reframes them.

### R16 — Synthesis §8.1 supersession (severity: L)
*Description.* `research/architecture-synthesis.md` §8.1 says "keep transfers as-is." This convergence plan supersedes that.
*Mitigation.* The architecture-synthesis stays as historical/comparison record. This migration document explicitly notes the supersession in §1 and §2.

### R17 — `transfer_line_*` vs `posting_line_*` naming (severity: L) ✓ RESOLVED
*Description.* Plan originally used `transfer_line_*` (matched the then-current `transfers` table name); proposal used `posting_line_*`. If the `transfers` rename ever landed, the extension naming was expected to follow.
*Resolution.* Resolved 2026-05-07 by acct-dhzc (schema-consolidation epic). The `transfers` → `posting_lines` rename was lifted as part of the broader 104-mig → 21-file consolidation; this plan and all extension tables (`posting_line_sources`, `posting_line_currencies`, `posting_line_dimensions`, `posting_line_inventory`, `posting_line_custom`) now use the unified `posting_line_*` naming throughout.

### R18 — Multi-cost-book deferred (severity: L)
*Description.* Cost-methods doc §13 prescribes multi-book (IFRS / GAAP / tax) via per-book subledger writes. Our `inventory_movements` schema includes `cost_book_id` but defaults to 1.
*Mitigation.* Single-book until acct-3gzh + parallel-ledger work justifies multi-book. Single-book preserves all behavior; multi-book is additive.

### R19 — Outbox revisit deferred (severity: L)
*Description.* Sync `post_posting_lines` continues; pseudo-sync (acct-c4p) deferred. Phase D's increased INSERT count may add contention pressure that lifts acct-c4p's priority.
*Mitigation.* Perf-monitor at each phase. Acct-c4p stays deferred unless measured contention surfaces.

### R20 — Existing P3 issues impacted by phasing (severity: M)
*Description.* acct-3gzh (multi-entity), acct-w1v3 (intercompany), acct-3dz2 (FX reval), acct-3xcg (realized FX) all touch the same surface. They benefit from Phase B/C/D foundation.
*Mitigation.* Sequence them after the relevant convergence phase ships; document in their `--design` field. acct-3gzh ideally after Phase D. acct-3dz2 + acct-3xcg ideally after Phase B2.

---

## §6 bd Epic + Sub-Issue Catalog

Templates for `bd create` invocations. Each issue cross-references this migration document by section number.

### §6.1 New epics (to file)

```
bd create --title="Phase B: extension-table foundations" \
  --type=epic --priority=2 \
  --description="Phase B of convergence-meta (acct-wb75). Three sub-issues B1/B2/B3 add posting_line_sources, posting_line_currencies (with functional-currency-per-legal-entity), and posting_line_dimensions extensions. Per research/posting-lines-convergence-plan.md §4.B. Effort ~6-9 weeks." \
  --acceptance="All three sub-issues closed; ci-check clean; recon invariants stable." \
  --design="Each sub-issue: new migration + extension table + backfill + dispatcher hook + recon invariant + tests."

bd create --title="Phase B1: posting_line_sources extension" \
  --type=task --priority=2 \
  --description="Per research/posting-lines-convergence-plan.md §4.B.B1. New table; backfill from per-document FK columns; dispatcher writes alongside posting_lines." \
  --acceptance="(1) mig adds posting_line_sources + source_doc_types lookup; (2) backfill produces 1 row per posting_lines row (excluding system rows); (3) dispatcher writes extension atomically; (4) run_daily_reconciliation count invariant 0 alerts; (5) ci-check clean; (6) tests pass."

bd create --title="Phase B2: posting_line_currencies + functional-currency model" \
  --type=task --priority=2 \
  --description="Per research/posting-lines-convergence-plan.md §4.B.B2. Adds legal_entities table with functional_currency; posting_line_currencies extension; backfill where account.currency != legal_entity.functional_currency." \
  --acceptance="..."

bd create --title="Phase B3: posting_line_dimensions + dimension_types" \
  --type=task --priority=2 \
  --description="Per research/posting-lines-convergence-plan.md §4.B.B3. EAV-typed extension; backfill from inline composition columns." \
  --acceptance="..."

bd create --title="Phase C: posting_line_inventory extension" \
  --type=epic --priority=2 \
  --description="Per research/posting-lines-convergence-plan.md §4.C. posting_line_inventory extension table; backfill from posting_lines.qty + credit-side account.sku_id; dispatcher writes for every inventory-touching transfer." \
  --acceptance="..."

bd create --title="Phase D: inventory_movements foundational subledger" \
  --type=epic --priority=2 \
  --description="Per research/posting-lines-convergence-plan.md §4.D. KEYSTONE PHASE. Six sub-issues D1-D6. Adds the foundational subledger that all real-cost methods write to." \
  --acceptance="All 6 sub-issues closed; ci-check clean; recon invariants stable for >7 days of new activity."

# Then 6 sub-issues D1-D6 each as task

bd create --title="Phase F: services-domain wrappers" \
  --type=epic --priority=3 \
  --description="Per research/posting-lines-convergence-plan.md §4.F. Three sub-issues F1/F2/F3: post_journal_entry, post_service_bill, post_expense_report." \
  --acceptance="..."
```

### §6.2 Existing issues to reframe / close

```
bd close acct-2c1m --reason="Superseded by acct-wb75 (convergence meta-epic). The Path A vs B framing was narrower than the actual decision. The convergence plan adopts subledger separation as part of a broader GL-first convergence per research/posting-lines-convergence-plan.md."

bd close acct-58h9 --reason="Resolved as 'yes, lift'. Re-scoped to Phase G of the convergence (research/posting-lines-convergence-plan.md §4.G). Lifting acct-2thf becomes Phase G1."

bd update acct-8gg --append-notes="Re-scoped to Phase E1 of acct-wb75 convergence; blocked-by Phase D (acct-XXXX). See research/posting-lines-convergence-plan.md §4.E.E1 for details."

bd update acct-uze --append-notes="Re-scoped to Phase E2 of acct-wb75 convergence; blocked-by Phase D. See research/posting-lines-convergence-plan.md §4.E.E2."

bd update acct-0kz --append-notes="Re-scoped to Phase E3 of acct-wb75 convergence; blocked-by Phase D. See research/posting-lines-convergence-plan.md §4.E.E3."

bd update acct-2thf --append-notes="Re-scoped to Phase G1 of acct-wb75 convergence. See research/posting-lines-convergence-plan.md §4.G.G1. Lifted from threshold-gated per acct-58h9 resolution."

bd update acct-v9sq --append-notes="Re-scoped to Phase G2 of acct-wb75 convergence. See research/posting-lines-convergence-plan.md §4.G.G2."
```

### §6.3 Deps to wire

```
# Phase D blocked-by Phase B + C (the inventory extension feeds inventory_movements)
bd dep add <Phase D epic> <Phase B epic>
bd dep add <Phase D epic> <Phase C epic>

# Phase E blocked-by Phase D
bd dep add acct-8gg <Phase D epic>
bd dep add acct-uze <Phase D epic>
bd dep add acct-0kz <Phase D epic>

# Phase G blocked-by Phase A only (independent of D-E)
# Phase F blocked-by Phase A only

# All convergence work blocked-by acct-chzx + acct-ewhs (DONE)
# (already implicitly satisfied since those are closed)

# Existing P3 dependencies that benefit from convergence foundation
bd update acct-3gzh --append-notes="Sequence after Phase D ships for cleanest legal_entity_id promotion."
bd update acct-3dz2 --append-notes="Sequence after Phase B2 ships for cleanest functional-currency model."
bd update acct-3xcg --append-notes="Sequence after Phase B2 + acct-3dz2."
```

---

## §7 Test Strategy Per Phase

| Phase | Schema test | Property test | Conformance | Regression |
|---|---|---|---|---|
| B1 | `tests/posting_line_sources_t1.rs` | extend property_post_transfers; verify every post_* writes a sources row | A22 sources cases | existing 647 pass |
| B2 | `tests/posting_line_currencies_t1.rs` | property_currency_extension | A23 currency cases | existing pass |
| B3 | `tests/posting_line_dimensions_t1.rs` | property_dimensions_extension | A24 | existing pass |
| C | `tests/posting_line_inventory_t1.rs` | extend property_post_transfers + property_wo_lifecycle | A25 inventory-extension cases | existing pass |
| D | `tests/inventory_movements_t1.rs` | new property_inventory_movements_consistency (subledger ↔ posting_lines invariant) | A26 inventory_movements cases | T2 matrices extended with `inventory_movements` assertions |
| E1 | per acct-8gg sub-issues | per acct-8gg | A27 FIFO cases | existing pass |
| E2 | per acct-uze | per acct-uze | A28 lot cases | existing pass |
| E3 | per acct-0kz | per acct-0kz | A29 serial cases | existing pass |
| F1-F3 | tests/services_t1.rs | property_journal_entry / property_service_bill / property_expense_report | A30 services cases | existing pass |
| G | tests/account_kinds_t1.rs | property_account_kinds_hierarchy | T2/T5 regenerated against row-table | existing pass after sweep |

Property test discipline (acct-1cer convention): every entry-point function ships with a sibling `property_*.rs` binary. Each new dispatcher branch / new function gets a property test in the same migration that introduces it.

---

## §8 Reconciliation Strategy

`run_daily_reconciliation` (consolidated `0020`) is extended at each phase boundary with new invariants. Each invariant returns 0 rows under healthy state; non-zero rows alert via `reconciliation_alerts(alert_kind, payload, created_at)`.

| Phase | New invariant |
|---|---|
| B1 | `count(posting_lines WHERE document_kind != 'system') = count(posting_line_sources)` |
| B2 | every `posting_line_currencies` row has `amount_transaction × fx_rate_to_functional ≈ posting_lines.amount` (rounding tolerance) |
| B3 | every transfer with credit/debit account having inline composition has corresponding `posting_line_dimensions` rows |
| C | `count(posting_lines WHERE qty IS NOT NULL) = count(posting_line_inventory)` AND `posting_line_inventory.quantity = ABS(posting_lines.qty)` |
| D | per `(product, location, period)`: `SUM(inventory_movements.quantity × actual_unit_cost) ≈ SUM(posting_lines.amount on inv_value_*)` |
| E1 | per `(product, location)`: `SUM(cost_layers.original_qty) - SUM(cost_layer_depletions.depleted_qty) = current_on_hand_qty` |
| E2 | per lot: cumulative event quantities match current `inventory_lots`-derived state |
| E3 | per serial: status-event chain consistency; no serial in two states simultaneously |
| F | n/a (services don't add invariants) |
| G | every `accounts.kind_id` resolves to a valid `account_kinds(id)` |

Recon failures are gates — Phase X cannot ship until X's invariants pass + remain stable for >7 days under new activity.

---

## §9 Open Questions for the User

1. **Functional-currency default.** ✅ **RESOLVED 2026-05-07: USD.** Phase B2 introduces `legal_entities.functional_currency CHAR(3) NOT NULL DEFAULT 'USD'`. Per-LE override remains. EUR fixtures continue to be transactional currency, not functional.

2. **Cost-book scoping.** ✅ **RESOLVED 2026-05-07: schema supports multi-book; implementation deferred.** Phase D ships `inventory_movements.cost_book_id SMALLINT NOT NULL DEFAULT 1` and a `cost_books` lookup table seeded with id=1 ('primary'). All Phase D dispatchers write `cost_book_id=1`. Multi-book *implementation* (parallel postings under different methods, IFRS-vs-GAAP-vs-tax dual reporting) deferred to **acct-zf80** (filed 2026-05-07; blocked-by D1 + D5). The schema columns are prelaid so adding multi-book later is additive, not a structural rewrite.

3. **Phase G timing.** ✅ **RESOLVED 2026-05-07: Mid (parallel with E1).** acct-2thf and acct-v9sq blocked-by **acct-wb75.3.5** (D5 recon invariant); they begin once Phase D's keystone is settled and can run alongside E1 (FIFO/LIFO). See §9.3 for trade-off analysis.

4. **Pre-flight audit before Phase C backfill.** ✅ **RESOLVED 2026-05-07: sub-issue blocking C.** Filed as **acct-wb75.2.1** (C0); the schema work moved to **acct-wb75.2.2** (C1) and is blocked-by C0. See §9.4.

5. **Phase F services-wrappers timing.** ✅ **RESOLVED 2026-05-07: F1 immediately, F2/F3 after Phase B completes.** F1 (`acct-wb75.4.1`) remains unblocked; F2/F3 (`acct-wb75.4.2`/`acct-wb75.4.3`) blocked-by **acct-wb75.1.3** (B3 dimensions). See §9.5.

### §9.3 Phase G timing options

Phase G = enum→row conversion of `account_kind` (acct-2thf) + COA hierarchy (acct-v9sq). It touches every codepath that references `account_kind` literals.

| Option | Pros | Cons |
|---|---|---|
| **Early** (parallel with Phase B) | Cleaner extension semantics — extensions tag by `account_kind_id` (FK) instead of enum literal | Phase G's sweep happens concurrently with B's introductions; doubles audit surface (every migration touches account_kind in some form during this window) |
| **Mid** (parallel with D-E, after Phase D ships) | Phase D adds new account_kinds anyway (`variance_*` family); natural alignment to convert when several pile up. Phase D keystone is stable so G's sweep operates against settled foundation | Phase E1 (FIFO/LIFO) is happening concurrently — both touch dispatcher; risk of merge conflicts on cost-method-strategies registry |
| **Late** (after Phase E completes) | Lowest concurrent risk; row-per-pair-with-extensions architecture is stable; sweep is one focused pass | Phase E is 3-9 months; deferring G means current enum-based code accretes more references during that window, making sweep larger |

**Recommendation: Mid (parallel with E1).** Phase D's keystone is too risky to share oxygen with G; Phase E timeline is too long to wait. Running G alongside E1 (the first method-specific subledger) is the sweet spot — G's enum-conversion mechanics are independent of E1's FIFO state machine, and merge-conflict risk is manageable since the cost-method-strategies registry pattern (acct-w0lo) localizes the touch points.

### §9.4 Phase C backfill pre-flight audit

The R14 risk: Phase C backfill computes `posting_line_inventory.product_id` from credit-side `account.sku_id`. Existing rows where `qty IS NOT NULL` but the credit-side account's `sku_id IS NULL` (or where qty isn't paired with a value-leg) are exceptions that need manual handling before backfill can proceed.

| Option | Description |
|---|---|
| **Sub-issue blocking C** | Audit query is its own sub-issue (acct-wb75.2.0); resolves to "0 exceptions" or surfaces a list. C cannot start until this closes. Backfill SQL becomes part of acct-wb75.2.1 once preflight is clean |
| **Separate housekeeping issue** | Audit lives outside the convergence epic; C starts when audit confirms cleanliness, but no formal dep |

**Recommendation: sub-issue blocking C** (acct-wb75.2.0). The audit IS part of Phase C — it's the pre-flight check on the backfill correctness. Filing it as a blocking sub-issue makes the DAG explicit (`bd ready` won't surface C-1 until preflight closes) and gives a clean ledger entry of "audit ran, found N exceptions, fixed them." Zero-exception case closes in minutes; non-zero case surfaces real housekeeping that would have blocked C anyway.

### §9.5 Phase F services-wrappers timing

F1 = `post_journal_entry` (generic dr/cr); F2 = `post_service_bill`; F3 = `post_expense_report`. All three are pure callers of `post_posting_lines` with non-inventory account combinations. None depends on Phase D's `inventory_movements` subledger.

| Option | F1 | F2 | F3 |
|---|---|---|---|
| **All parallel with Phase B** | Ships immediately; trivial wrapper | Needs B3 (`posting_line_dimensions`) for vendor/cost-center tagging | Needs B3 + B1 (`posting_line_sources`) for employee tagging |
| **After Phase B completes** | Same as left, just waited | All three write extensions correctly first time | Same as F2 |
| **After Phase D** | Trivially shippable; no dep on D | No dep on D either; same as B-after | No dep on D either |

**Recommendation: F1 immediately (parallel with B); F2/F3 after Phase B completes.** F1 is a generic dr/cr wrapper — it has no inventory side, no dimension tagging, no source-extension tagging beyond the existing `idempotency_key` + `posted_by_id`. It ships in days. F2/F3 want to write `posting_line_dimensions` (cost-center tagging on expenses) and `posting_line_sources` (vendor / employee link); waiting for Phase B means both ship correctly the first time without a follow-up rewrite.

This gets services-only orgs the *minimum viable* services flow (F1 = arbitrary journal entries) immediately, and full vendor/employee-linked AP service flows after Phase B.

---

## §10 Cross-References

**Internal docs:**
- `research/architecture-synthesis.md` — comparison reference; §8.1's "preserve transfers as-is" is superseded by this plan.
- `research/ledger-architecture-proposal.md` §2-§4 — convergence target authority.
- `research/cost-methods-subledger-design.md` §2-§3 (principle), §4.3 (foundational subledger), §7-§10 (per-method schemas).
- `research/erp-transaction-architectures-revised.md` §7 (industry reference; informs extensions design).
- `ledger_design_consolidated_v0.md` Part IV §1.2 (posting_lines schema; row-per-pair acct-1584 note).
- `CLAUDE.md` (Load-bearing design decisions; updated per phase as work ships).
- `REVIEW.md` (R1-R7 anti-pattern catalog; per-phase audit pass codified here).
- `/home/kaalin/.claude/plans/formulate-a-detailed-plan-prancy-pizza.md` (planning workflow output).

**bd issues:**
- `acct-wb75` (this convergence meta-epic).
- `acct-chzx` ✓ (Phase A1 — `posting_layer`).
- `acct-ewhs` ✓ (Phase A2 — `legal_entity_id`).
- `acct-1584` ✓ (row-per-pair load-bearing-decision).
- `acct-46rx` ✓ (Tier-2 WASM out-of-scope).
- `acct-2c1m` (to close as superseded).
- `acct-58h9` (to close as resolved).
- `acct-8gg` (re-scope to Phase E1).
- `acct-uze` (re-scope to Phase E2).
- `acct-0kz` (re-scope to Phase E3).
- `acct-2thf` (re-scope to Phase G1).
- `acct-v9sq` (re-scope to Phase G2).
- `acct-3gzh`, `acct-w1v3`, `acct-3dz2`, `acct-3xcg`, `acct-c4p` (downstream issues with notes added).

**Migrations:**
- 21 consolidated migration files in `db/migrations/` (`0001_extensions` through `0021_seed_registries_and_outbox`); convergence builds via new migrations 0022+.
- 104 archived files in `db/archive_migrations/` preserve the original incremental history (acct-dhzc, 2026-05-07).
- Consolidated `0009_posting_lines` (universal core, lineage from archive `mig 0007`); `posting_layer` and `legal_entity_id` columns inline from the start (acct-chzx ✓, acct-ewhs ✓).
- Consolidated `0013_strategy_registry` (`cost_method_strategies` registry; extended in Phase D-E).
- Consolidated `0012_period_close` (`close_hooks` registry; extended in Phase D).
- Consolidated `0014_post_posting_lines` (`_post_posting_lines_apply_event`; extended in Phase B-C).
- Consolidated `0019_posting_line_extensions` (`posting_line_sources`; B1 ✓).
- Consolidated `0020_run_daily_reconciliation` (extended at every phase).

---

**End of v0 working document.** ~1,300 lines.

Updates expected per phase as work ships: phase status changes from "TO FILE" → "IN PROGRESS" → "DONE"; recon invariants confirmed stable; cross-reference list grows; risk register entries closed or escalated based on observed behavior.

Open questions in §9 await user input. The catalog in §6 is ready for `bd create` execution after the meta-epic is approved.
