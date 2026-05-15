# Unified Costing Ledger Extension — Design Specification, v2

**Status:** Design spec, reference architecture
**Target:** PostgreSQL 18+, pgrx 0.17+
**Audience:** implementation team and future maintainers
**Supersedes:** v1 (`design.md`)

---

## 0. Scope and goals

A pgrx-based PostgreSQL extension that provides the costing engine for acct's ledger: it consumes posting-line events emitted by acct's existing `_post_posting_lines_apply_event`, computes applied costs under pluggable costing methods, writes cost-bearing rows into acct's existing schema (`posting_lines`, `posting_line_inventory`, `posting_lines_provisional`, `cost_layers`, `cost_layer_depletions`, `inventory_lots`, `inventory_units`), and participates in period-close finalization. It does NOT own posting_lines itself; acct owns the posting ledger. The extension is the implementation of the cost-method protocol, not a replacement for the ledger.

**Goals, in order of priority:**

1. **Correctness.** No inconsistency between cost-bearing rows and the posting ledger. All R1–R7 invariants from acct's CLAUDE.md hold under both apply architectures (in-tx and queue+committer).
2. **Method extensibility.** A typed Rust trait protocol for cost methods that prefer per-row procedural expression (FIFO, specific-id, balance-only); existing plpgsql `cost_method_strategies` registry retained for methods that prefer set-based in-database expression (WAC family, close-hook-heavy methods).
3. **Throughput, where measurement supports it.** In-tx is the default. Queue+committer is opt-in for deployments whose measured workload shape benefits from cross-backend commit coalescence. Bake-off determines threshold.
4. **Operational simplicity.** Observability via `pg_stat_statements` + extension-exposed metric functions. No external dependencies beyond what acct already requires.

**Non-goals:**

- Owning posting_lines or any acct-native ledger table. The extension writes to these tables under acct's schema contract; it does not define them.
- Multi-primary write coordination.
- Replica-side extension presence.
- Direct application access to cost tables. Application code calls acct's existing posting wrappers; the extension is reached transitively through acct's dispatcher.

**Hard constraints (inherited from acct):**

- Append-only with named exceptions. The `posting_lines` table is append-only via trigger. The `posting_lines_provisional` lifecycle marker is exempt: its `finalized_at`, `variance_amount`, and `variance_posting_line_id` columns are written exactly once by the close hook (transitioning open → finalized). All variance emissions are NEW posting_lines, not UPDATEs to existing ones. See §1.2.
- R1–R7 invariants from acct's CLAUDE.md are load-bearing. The trait protocol and orchestration contract are shaped to enforce them.
- Credit-first SKU resolution (R2) for cost-method dispatch and provisional flagging.
- Account-isolated qty divisor (R1) for WAC pool computations: the divisor reads only from the pool's `account.id`, never aggregated across sibling accounts (different SKUs sharing a location, or WIP accounts).
- Post-FOR-UPDATE snapshot reads (R4) for cost-affecting state.
- Document-level unit_cost snapshot sourcing (R7) from post-lock dispatcher output.

---

## 1. System overview

### 1.1 Where the extension sits

```
┌─────────────────────────────────────────────────────────────────────┐
│  Application: wo_complete, so_ship, po_receive, ...                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ SQL call (per acct's existing wrappers)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  acct plpgsql layer (post_posting_lines and friends)                │
│  - Builds event JSONB                                               │
│  - Calls _post_posting_lines_apply_event (dispatcher)               │
│  - Owns R1/R2/R4/R7 enforcement at the dispatcher boundary          │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
┌───────────────────────────┐   ┌─────────────────────────────────────┐
│  Existing plpgsql registry │   │  Extension's trait dispatch         │
│  cost_method_strategies    │   │  (Rust, via pgrx #[pg_extern])      │
│  - WAC family (perpetual, │   │  - FIFO, specific-id, balance-only  │
│    periodic, retroactive) │   │  - Future Rust-native methods       │
│  - Set-based DAG close    │   │  - Per-method associated snapshot   │
│    hooks                  │   │  - Leg-keyed batched apply          │
└───────────────────────────┘   └─────────────────────────────────────┘
                │                             │
                └──────────────┬──────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  acct schema (single source of truth)                               │
│  posting_lines, posting_line_inventory, posting_lines_provisional, │
│  cost_layers, cost_layer_depletions, inventory_lots,                │
│  inventory_units, posting_line_sources/currencies/dimensions        │
└─────────────────────────────────────────────────────────────────────┘
```

The extension is one of two dispatch targets, not the sole apply path. Acct's existing plpgsql `cost_method_strategies` registry continues to own the WAC family. The trait protocol is the right tool for methods whose logic naturally expresses per-row in Rust (FIFO walks layer rings; specific-id is layer-keyed lookup; balance-only is trivial). Methods whose logic expresses naturally as set-based queries over period-postings stay in plpgsql.

The choice between trait and registry per method is documented per §10. New methods register through whichever path fits their logic; the dispatcher routes to the right one based on the method's registration.

### 1.2 Architectural principles

**Append-only with named exceptions.** The posting ledger is append-only by trigger on `posting_lines`. All variance emissions, all reversals, all corrections create NEW posting_lines rows; the extension never UPDATEs an existing posting_line. The named exception is `posting_lines_provisional`: its `finalized_at`, `variance_amount`, and `variance_posting_line_id` columns transition NULL → set exactly once by the close hook. This is a controlled, audited lifecycle transition, not a hot-path mutation. The append-only principle is preserved by treating provisional rows as separate state from posting_lines, with explicit lifecycle semantics.

Adjustments, reversals, and corrections create new layer events (positive or negative qty) with `source_kind ∈ {reversal, adjustment, opening_balance, ...}`, attributed via `posting_line_sources` to the original posting_line. The audit trail shows all of them, in order.

**Trait-where-it-fits; registry-where-it-doesn't.** The Rust trait is the right tool for some methods, not all. Per-method decision: trait for FIFO, specific-id, balance-only, and future per-row procedural methods; registry for WAC variants and future set-based methods. The unified-dispatch claim is "every cost method registers somewhere and the dispatcher routes correctly," not "every method uses the same dispatch mechanism."

**Schema is acct's.** The extension does not introduce new tables for its own use. It reads from and writes to acct's existing tables, respecting acct's invariants. New columns or new tables are acct schema migrations, not extension-internal definitions.

**Tables are the ultimate truth; shmem is a cache.** Any shmem state can be rebuilt from tables. Crash recovery rebuilds shmem from tables on extension load. This applies to both layer caching (read-side optimization) and queue state (when queue+committer is enabled, see §4).

**Credit-first dispatch.** The dispatcher resolves the cost method from the credit-side SKU of each posting event (R2). The trait protocol and the registry both receive events grouped by (credit-side-pool, cost_method). Methods do not re-check dispatch; by construction every event in a method's batch is in that method's dispatch territory.

### 1.3 Core architectural decisions (locked from design discussion)

| Decision | Choice | Driver |
|----------|--------|--------|
| Apply concurrency model default | In-transaction synchronous | Preserves acct's atomicity guarantees; lower-risk; matches acct-8hv2 finding |
| Apply concurrency model alternative | Queue+committer, opt-in via GUC | Cross-backend commit coalescence wins for high-fan-in / long-user-tx workloads; bake-off determines threshold |
| Trait input shape | Leg-keyed `PostingApplyRequest`, batched per (credit-side-pool, cost_method) | Credit-first dispatch (R2) + by-product qty=NULL + variance_material_mixed detection |
| Snapshot shape | Method-specific via associated type; method declares `required_dimensions()` | 11+ potential dimensions; no method needs all; type-safe construction |
| Close-hook DAG | Method declares participating reasons; framework builds edges and Kahn-sorts | Reasons are method-static; edges are period-dynamic |
| sub_priority ordering | Framework-owned (0=qty-in, 1=value-leg, 2=qty-out) | Property of how posting events emit legs, not method-specific |
| Drained-to-zero detection | Framework-computed | Structural property of the merged stream |
| Variance routing | Hybrid: framework detects category and structurally validates; method picks accounts and emits | Structural correctness centralized; account naming and future patterns extensible |
| Provisional row payload | Lifecycle marker only; at-apply-time value via FK to posting_lines.amount | acct schema decision (mig 0012); simpler trait protocol |
| BOM orchestration location | Topological shape in plpgsql; batch assembly in shim; per-leg correctness in trait/registry | Preserves acct's existing investment; right separation of concerns |
| Dimension vocabulary | Identity dimensions framework-bundled with cost-method support; analytical dimensions EAV-extensible | Matches acct's existing posting_line_inventory + posting_line_dimensions split |
| Trait callback ceiling | ≤6 required, unbounded optional pay-as-you-go | Prevents drift toward "framework masquerading as abstraction" |
| Shmem sizing | GUC at startup via `RequestAddinShmemSpace` | Avoids compile-time choice |
| Migration | None — greenfield. No shadow mode, no cutover phases | acct is pre-production |

---

## 2. Schema (acct's existing tables, contract documented)

The extension reads from and writes to acct's schema. This section documents the relevant tables and how the extension interacts with each. Schema definitions are in acct's migrations; this section names the contract.

### 2.1 `posting_lines` (acct mig 0001, append-only via trigger)

The ledger itself. Each posting event produces N posting_lines rows (typically 2: credit leg + debit leg, sometimes more for multi-leg events like wo_complete). Append-only by trigger; the extension NEVER updates an existing posting_line.

Relevant columns for the extension:
- `id` — identifier referenced by extension tables
- `account_id` — joined to `accounts` for kind/SKU/class resolution
- `amount` — signed value; for value-legs of provisional postings, this is the at-apply-time cost stored authoritatively here
- `qty` — signed quantity; positive for inflow, negative for outflow
- `reason` — event reason (so_ship, wo_complete_v, op_move_v, rm_issue_to_wo, etc.); used by close-hook DAG to identify participating edges
- `business_date`, `posted_at` — chronological ordering inputs
- `document_id` — anchors document-level ordering despite per-row `clock_timestamp()`
- `sub_priority` — framework-assigned (0/1/2) for within-event ordering at close

### 2.2 Extension tables (acct's, via mig 0019–0024)

**`posting_line_sources`** (mig 0019). Audit metadata: reversal pointers, parent document, intercompany pair, created_by_process. The extension WRITES this when emitting variance posting_lines (sets `created_by_process` to a method-specific tag); the extension does not READ this for costing input. Audit, not snapshot.

**`posting_line_currencies`** (mig 0022). Multi-currency overlay: transactional currency + fx rate when transactional ≠ functional. The extension READS this when building snapshots for methods that declare `TransactionalCurrency` in `required_dimensions()`. Single-entity single-currency deployments mostly skip this.

**`posting_line_dimensions`** (mig 0023). EAV analytical dimensions: routing_op, project, cost_center, etc. The extension READS this for methods that declare `Analytical(DimensionTypeId)`. Extensible by adding rows to `dimension_types` lookup; no framework code change needed.

**`posting_line_inventory`** (mig 0024). Inventory dimensions on cost-bearing postings: `product_id`, `quantity`, `unit_cost`, `cost_layer_id`, `lot_id`, `unit_id`, `cost_method_at_event`. The extension WRITES this on every cost-bearing posting it emits; READS it when building snapshots for methods that declare any of `Lot`, `Unit`, `CostLayer`, `CostBook` (the identity dimensions).

### 2.3 Cost-method state tables

**`posting_lines_provisional`** (mig 0012). Lifecycle marker for provisional postings (wac_periodic, wac_retroactive depletion value-legs, plus future methods that opt into provisional flagging). One row per provisionally-flagged posting_line. Columns:
- `posting_line_id` — FK to the flagged posting (PK)
- `period_id` — period for close-hook scope
- `cost_method` — method tag for close-hook routing
- `qty` — denormalized for close-hook math (signed)
- `finalized_at` — NULL until close; set once at close
- `variance_amount` — NULL until close; set once at close
- `variance_posting_line_id` — NULL for internal-chain finalizations; set when a variance posting_line is emitted (leaf depletion case)

The at-apply-time stored value lives on `posting_lines.amount` via the FK; the provisional row carries lifecycle and method tag, nothing else.

Mig 0012 CHECK admits three states:
1. **Open**: `finalized_at IS NULL` AND `variance_amount IS NULL` AND `variance_posting_line_id IS NULL`
2. **Finalized with variance posting emitted**: all three set, `variance_amount ≠ 0`, `variance_posting_line_id ≠ posting_line_id`
3. **Finalized internal-chain**: `finalized_at` set, `variance_amount` set (possibly 0), `variance_posting_line_id IS NULL` (cumulative shift propagates via close-hook upstream-variance cache; no transfer posted)

This three-state machine is load-bearing. The trait protocol's variance emission produces transitions into states 2 or 3; the framework writes the provisional row's finalization fields in one statement per provisional.

### 2.4 Cost-layer state tables (acct's, used by FIFO/specific-id)

**`cost_layers`** (acct, Phase E1). Records each inventory-creating event as a cost layer. Append-only. Layers belonging to the same conceptual unit of inventory share a `layer_group_id` so that reversals/adjustments can be expressed as new rows.

Relevant columns:
- `layer_id` — identity
- `layer_group_id` — groups related events (original + adjustments + reversals)
- `item_id`, `location_id`, `currency` — pool key
- `qty` — signed (positive for receipts, negative for reversals)
- `unit_cost` — captured at birth
- `born_at`, `born_seq` — chronological ordering (born_seq is committer-assigned for monotonicity within a single timestamp)
- `source_kind` — receipt | reversal | adjustment | opening_balance | period_close | merge | split | synthetic
- `source_ref` — FK to originating event in external system
- `method_at_birth` — method tag at creation time

**`cost_layer_depletions`** (acct, Phase E1). Records each consumption event. Append-only. References the consumed layer.

Relevant columns:
- `depletion_id` — identity
- `layer_id` — FK to consumed layer
- `qty` — positive
- `unit_cost` — denormalized from layer for query speed
- `consumed_at`, `consumed_seq` — chronological ordering
- `posting_line_id` — FK to the posting that drove this depletion
- `method_used` — method tag at consumption time

The extension writes these on FIFO and specific-id consumptions. WAC methods don't write here (their value-leg amount captures the cost; no per-layer attribution).

### 2.5 Lot and serial state tables (acct's, Phase E2/E3)

**`inventory_lots`** + **`inventory_lot_events`** (acct Phase E2). Lot identity and lifecycle. The extension uses `lot_id` as an identity dimension; lot reservation/pinning lives in acct's lot module, not in the extension.

**`inventory_units`** + **`inventory_unit_events`** (acct Phase E3). Serial-tracked unit identity. The extension uses `unit_id` as an identity dimension for specific-id costing.

The extension references these via FK on `posting_line_inventory.lot_id` and `posting_line_inventory.unit_id`. Identity-dimension snapshot construction (§5.3) joins these tables.

### 2.6 Derived state (read patterns, no new tables)

The "effective qty remaining" in a layer group is a derivation, computed by the framework when building snapshots:

```sql
SELECT
    l.layer_group_id,
    SUM(l.qty) - COALESCE((
        SELECT SUM(d.qty)
        FROM cost_layer_depletions d
        WHERE d.layer_id IN (SELECT layer_id FROM cost_layers WHERE layer_group_id = l.layer_group_id)
    ), 0) AS effective_qty
FROM cost_layers l
WHERE l.layer_group_id = $1
GROUP BY l.layer_group_id;
```

The framework caches this per item in shmem when caching is enabled. Cache is best-effort; tables are authoritative.

### 2.7 Invariants

These hold at any committed snapshot. Distinguishing extension-enforced from application-layer SLAs (per design feedback):

| ID | Invariant | Enforced by |
|----|-----------|-------------|
| I1 | For every `cost_layer_depletions` row, the referenced layer group's effective_qty (without this depletion) ≥ this depletion's qty | Deferred constraint trigger on cost_layer_depletions; framework's per-method validate_invariants |
| I2 | `method_used` on a depletion equals the method active for the SKU at `consumed_at` per acct's costing_method_assignments / sku.cost_method | Framework dispatch (R2 credit-first); recon job cross-checks |
| I4 | `consumed_seq` is monotone within `(layer_id, consumed_at)` | Committer-assigned; sequence allocation |
| I5 | `born_seq` is monotone within `(item_id, born_at)` | Committer-assigned; sequence allocation |
| I8 | Sequences are globally unique | PG sequence semantics |
| I-prov-3state | `posting_lines_provisional` is in exactly one of the three lifecycle states | Mig 0012 CHECK |
| I-R1 | WAC qty divisor for a pool is `SUM(qty SIGNED) WHERE account_id = pool.account_id`; never aggregates across sibling accounts (different SKUs, or WIP-attached accounts at the same location) | Shim contract (§7); property tests |
| I-R2 | Every event in a dispatched batch resolves to the same credit-side cost_method | Shim contract; property tests |
| I-R4 | Snapshot reads occur strictly after FOR UPDATE acquisition of relevant accounts | Shim contract; instrumentation tests |
| I-R5 | No variance emission pushes a debit-normal pool below 0 or a credit-normal pool above 0 at commit | Framework structural validation (§5.6); fault-injection tests |
| I-R7 | Document-level unit_cost snapshot fields are sourced from post-lock dispatcher output | Shim contract; review-time |
| **Application-layer SLAs (not extension invariants)** |
| I3-app | Every cost-bearing posting_line has corresponding posting_line_inventory; orphan-posting recon flags exceptions | acct recon job |
| I6-app | Reversals reference their originals via posting_line_sources.reverses_posting_line_id | acct dispatcher |
| I-close-DAG-acyclic | Close-hook DAG has no cycles (rework currently raises P0036) | Framework cycle detection at close |

I3 and I6 from v1 were application-layer concerns; framing them as extension invariants was wrong. The extension does not own posting_lines and cannot enforce these alone. The recon job (§8.4) cross-checks them and reports.

### 2.8 Retention and archival

Inherited from acct's archival policy. The extension does not maintain its own archive tables; if acct archives `cost_layers` / `cost_layer_depletions` for fully-reconciled layer groups, the extension's recon checks union live and archive tables. Out-of-scope for initial extension; cross-references acct-9lx7 for production layer-lifecycle GC design.

---

## 3. The trait protocol

This section specifies the Rust trait that Rust-native cost methods (FIFO, specific-id, balance-only, future per-row methods) implement. Methods that prefer set-based plpgsql expression (WAC variants) do NOT use this trait; they register via acct's existing `cost_method_strategies` plpgsql registry (§9).

### 3.1 Required methods (4)

```rust
pub trait CostingMethod: Send + Sync + 'static {
    /// Identity. Used by dispatcher to route events to this method.
    /// Must match the method_id used in acct's sku.cost_method or
    /// the equivalent assignment table.
    const METHOD_ID: &'static str;

    /// Method-specific snapshot type. Carries exactly the dimensions
    /// this method needs to plan its applies. See §5.3 for snapshot
    /// construction; framework reads `required_dimensions()` and builds
    /// the snapshot accordingly.
    type Snapshot: Send + 'static;

    /// What dimensions this method reads. Framework uses this to build
    /// the snapshot; methods cannot read dimensions they didn't declare.
    fn required_dimensions() -> &'static [DimensionKind];

    /// Compute applies for a batch of homogeneous events. Every event
    /// in the batch has the same credit-side cost_method (this method)
    /// and the same credit-side pool. Snapshot is constructed once per
    /// batch and pre-locks accounts via FOR UPDATE (R4).
    ///
    /// MUST be deterministic and pure: no SPI, no shmem mutation, no
    /// side effects, no time queries. Tested via custom SpiTracker.
    fn plan_apply(
        &self,
        batch: &ApplyBatch,
        snapshot: &Self::Snapshot,
    ) -> ApplyResult;

    /// Method-specific invariants checked at commit. Framework validates
    /// structural rules (R5) regardless; this is for method-specific
    /// additional checks (e.g., FIFO's "no layer group goes negative").
    fn validate_invariants(
        &self,
        affected_pools: &[PoolKey],
        snapshot: &Self::Snapshot,
    ) -> Result<(), InvariantViolation>;
}
```

Four required methods (counting the associated `Snapshot` type and `METHOD_ID` const as one declaration each). Under the ≤6 ceiling.

### 3.2 Optional methods (pay-as-you-go)

```rust
impl CostingMethod for SomeMethod {
    // === Optional: close-hook participation ===

    /// Reasons this method participates in as a close-hook DAG edge.
    /// Framework discovers actual edges per-period by scanning in-period
    /// postings for these reasons. Default: empty (method doesn't participate
    /// in close-hook DAG).
    fn close_hook_participating_reasons() -> &'static [ReasonId] {
        &[]
    }

    /// Optional preprocessing of the merged event stream before the main
    /// close-hook replay. Used by period-average methods that need to
    /// exclude qty=NULL by-product credits from the denominator
    /// (wac_periodic). Default: identity (return stream unchanged).
    fn close_hook_preprocess_stream(events: Vec<ReplayEvent>) -> Vec<ReplayEvent> {
        events
    }

    /// Per-pool replay step. Receives the merged chronological stream
    /// for one pool (after preprocess). Method computes per-event variance
    /// amounts; framework picks routing (§5.6). Default: panic; methods that
    /// declared participating_reasons must implement this.
    fn close_hook_replay_pool(
        &self,
        pool: PoolKey,
        merged_stream: &[ReplayEvent],
        upstream_variance_cache: &UpstreamVarianceCache,
        snapshot: &Self::Snapshot,
    ) -> ReplayResult {
        panic!("method declared close-hook participation but did not implement replay")
    }

    /// Whether a given apply emits a provisional flag. Default: no flag.
    /// Methods that flag (wac_*) override to return Some(flag) per event.
    fn provisional_flag_for(
        &self,
        event: &PostingApplyRequest,
    ) -> Option<ProvisionalFlag> {
        None
    }

    /// Variance emission for a routed variance. Framework provides the
    /// routing context (category, pool kind, drained-to-zero, destination
    /// SKU method); method picks variance accounts and emission shape.
    /// See §5.6. Default: panic; methods that participate in close-hook
    /// must implement.
    fn emit_variance(
        &self,
        ctx: &RouteContext,
    ) -> VarianceEmission {
        panic!("method must implement emit_variance if it participates in close-hook")
    }
}
```

Six optional methods. Default no-ops or panics where applicable. Methods pay for what they use.

Total surface: 4 required + 6 optional = 10 methods. Under the ≤6 required cap; well within the "watch the callback count" target.

### 3.3 Core types

```rust
/// One method's batch: all events have the same credit-side pool and
/// the same credit-side cost_method by construction. Framework groups
/// events to produce these batches; method sees a homogeneous workload.
pub struct ApplyBatch {
    pub credit_pool: PoolKey,
    pub events: Vec<PostingApplyRequest>,
    pub method_config: JsonValue,  // method-specific config from sku.cost_method_config
}

pub struct PoolKey {
    pub account_id: i64,                // the FOR UPDATE-locked account; the unit of pool identity
    pub account_class: AccountClass,    // class tag from accounts.kind
    pub sku_id: Option<i64>,            // None for WIP accounts (WO-keyed, no SKU)
    pub location_id: Option<i64>,
    pub currency: CurrencyCode,
    pub legal_entity_id: i64,
    pub work_order_id: Option<i64>,     // present for WIP accounts; None for SKU-keyed accounts
}

/// A snapshot of one pool's state. The unit of cost computation is one pool —
/// one account.id — at a time. There is no multi-class-per-snapshot shape:
/// a SKU has one class (its role in the business), and WIP is its own
/// work-order-keyed accounting attached to separate accounts. Each pool is
/// snapshot independently; the framework dispatches one batch per pool.
///
/// R1 enforced at construction: pool_qty and pool_value read only from
/// account_id, never aggregated across sibling accounts (different SKUs at
/// the same location, or WIP accounts that share a location with the SKU).
pub struct PoolSnapshot {
    pub pool_key: PoolKey,
    pub pool_value: i64,                // SUM(amount SIGNED by debit/credit) on pool_key.account_id
    pub pool_qty: i64,                  // SUM(qty SIGNED) on pool_key.account_id — R1's divisor
    pub method_config: JsonValue,       // from sku.cost_method_config (or WO's, for WIP)
}

pub struct PostingApplyRequest {
    pub credit_leg: LegInfo,        // depletion source; SKU here drove dispatch
    pub debit_leg: LegInfo,
    pub event_metadata: EventMetadata,
    pub event_seq: u64,
}

pub struct LegInfo {
    pub account_id: i64,
    pub account_class: AccountClass,
    pub sku_id: Option<i64>,
    pub qty: Option<i64>,          // NULL for value-only legs (by-product credit)
    pub event_role: LegRole,       // depletion_source | depletion_target | value | qty_inflow | qty_outflow
}

pub struct EventMetadata {
    pub reason: ReasonId,
    pub business_date: Date,
    pub posted_at: Timestamp,
    pub document_id: Uuid,
    pub originating_event_id: i64,
}

pub struct ApplyResult {
    /// One per ApplyBatch event, in input order.
    pub per_event: Vec<EventApplyResult>,

    /// New cost_layers rows to INSERT (for methods that create layers per apply).
    pub layer_inserts: Vec<LayerRow>,

    /// New cost_layer_depletions rows to INSERT.
    pub depletion_inserts: Vec<DepletionRow>,

    /// Provisional flags to INSERT into posting_lines_provisional.
    /// Framework merges these with any from provisional_flag_for().
    pub provisional_flags: Vec<ProvisionalFlag>,
}

pub struct EventApplyResult {
    pub event_seq: u64,
    pub value_leg_amount: i64,                 // the amount to write on the value-leg posting_line
    pub applied_unit_cost: Option<i64>,        // for posting_line_inventory.unit_cost
    pub depletion_ids: Vec<i64>,               // depletion row IDs; assigned post-INSERT
}

pub struct ProvisionalFlag {
    pub posting_line_id_placeholder: u32,      // resolved post-INSERT to actual id
    pub cost_method: MethodId,
    pub qty: i64,                              // signed
}

#[derive(Debug)]
pub enum DimensionKind {
    // Schema-static universals
    LegalEntity, Currency, Product, Location, Class,
    // Multi-currency overlay
    TransactionalCurrency,
    // Identity dimensions (framework-bundled with cost-method support)
    Lot, Unit, CostLayer, CostBook,
    // Analytical EAV dimensions (data-extensible)
    Analytical(DimensionTypeId),
}
```

### 3.4 FIFO implementation

```rust
pub struct FifoMethod;

pub struct FifoSnapshot {
    pub pool: PoolSnapshot,
    pub layers: Vec<LayerView>,  // FIFO order, only effective_qty > 0
}

pub struct LayerView {
    pub layer_group_id: i64,
    pub layer_id: i64,
    pub unit_cost: i64,
    pub effective_qty: i64,
    pub born_at: Timestamp,
    pub born_seq: i64,
}

impl CostingMethod for FifoMethod {
    const METHOD_ID: &'static str = "fifo";
    type Snapshot = FifoSnapshot;

    fn required_dimensions() -> &'static [DimensionKind] {
        &[DimensionKind::Class, DimensionKind::Product, DimensionKind::Location,
          DimensionKind::Currency, DimensionKind::CostLayer]
    }

    fn plan_apply(&self, batch: &ApplyBatch, snap: &Self::Snapshot) -> ApplyResult {
        let mut layers = snap.layers.clone();  // local mutable copy
        let mut depletion_inserts = Vec::new();
        let mut per_event = Vec::new();

        // Events arrive in framework-determined order
        // (business_date, doc_chrono, document_id, sub_priority, event_seq).
        for event in &batch.events {
            let want = event.credit_leg.qty
                .filter(|q| *q < 0)
                .map(|q| -q)
                .unwrap_or_else(|| panic!("FIFO depletion requires qty on credit leg"));

            let mut remaining = want;
            let mut total_cost: i64 = 0;
            let mut deps = Vec::new();

            for layer in layers.iter_mut() {
                if remaining == 0 { break; }
                if layer.effective_qty <= 0 { continue; }
                let take = remaining.min(layer.effective_qty);
                total_cost = total_cost.saturating_add(take.saturating_mul(layer.unit_cost));
                layer.effective_qty -= take;
                remaining -= take;
                deps.push(DepletionRow {
                    layer_id: layer.layer_id,
                    qty: take,
                    unit_cost: layer.unit_cost,
                    consumed_at: event.event_metadata.posted_at,
                    posting_line_id_placeholder: event.event_seq,
                    method_used: "fifo".into(),
                });
            }

            if remaining > 0 {
                return ApplyResult::error(InvariantViolation::InsufficientInventory {
                    pool: batch.credit_pool.clone(),
                    want, short: remaining,
                });
            }

            let unit = if want > 0 { total_cost / want } else { 0 };
            per_event.push(EventApplyResult {
                event_seq: event.event_seq,
                value_leg_amount: total_cost,
                applied_unit_cost: Some(unit),
                depletion_ids: vec![],  // filled post-INSERT
            });
            depletion_inserts.extend(deps);
        }

        ApplyResult {
            per_event,
            layer_inserts: vec![],     // FIFO depletions don't create layers
            depletion_inserts,
            provisional_flags: vec![], // FIFO is fully-costed at apply; no provisional
        }
    }

    fn validate_invariants(
        &self, affected: &[PoolKey], snap: &Self::Snapshot,
    ) -> Result<(), InvariantViolation> {
        for pool in affected {
            for layer in &snap.layers {
                if layer.effective_qty < 0 {
                    return Err(InvariantViolation::LayerOverConsumed {
                        layer_group_id: layer.layer_group_id, deficit: -layer.effective_qty,
                    });
                }
            }
        }
        Ok(())
    }

    // No close-hook participation; defaults apply.
}
```

Receipt events (positive qty on credit leg, source_kind: receipt) take a different path through the framework — they call into a `plan_layer_event` style hook that's not on the apply trait. Layer creation lives in acct's existing po_receive / wo_complete plpgsql; the trait handles depletions. This is the "topological shape stays in plpgsql" boundary from the design discussion.

### 3.5 Specific-id implementation

```rust
pub struct SpecificIdMethod;

pub struct SpecificIdSnapshot {
    pub pool: PoolSnapshot,
    pub units: Vec<UnitView>,  // serial-tracked inventory units in this pool
}

pub struct UnitView {
    pub unit_id: i64,
    pub layer_id: i64,
    pub unit_cost: i64,
    pub available: bool,
}

impl CostingMethod for SpecificIdMethod {
    const METHOD_ID: &'static str = "specific";
    type Snapshot = SpecificIdSnapshot;

    fn required_dimensions() -> &'static [DimensionKind] {
        &[DimensionKind::Class, DimensionKind::Product, DimensionKind::Location,
          DimensionKind::Currency, DimensionKind::Unit, DimensionKind::CostLayer]
    }

    fn plan_apply(&self, batch: &ApplyBatch, snap: &Self::Snapshot) -> ApplyResult {
        let mut depletion_inserts = Vec::new();
        let mut per_event = Vec::new();

        for event in &batch.events {
            // The target unit_id is passed via event metadata (the application
            // selected the specific unit, e.g., from a UI pick). Look it up.
            let target_unit_id: i64 = event.event_metadata.method_specific
                .get("unit_id")
                .and_then(|v| v.as_i64())
                .ok_or(InvariantViolation::SpecificIdMissingTarget)?;

            let unit = snap.units.iter().find(|u| u.unit_id == target_unit_id)
                .ok_or(InvariantViolation::SpecificIdUnitNotFound { unit_id: target_unit_id })?;
            if !unit.available {
                return ApplyResult::error(InvariantViolation::SpecificIdUnitNotAvailable {
                    unit_id: target_unit_id,
                });
            }

            depletion_inserts.push(DepletionRow {
                layer_id: unit.layer_id,
                qty: 1,
                unit_cost: unit.unit_cost,
                consumed_at: event.event_metadata.posted_at,
                posting_line_id_placeholder: event.event_seq,
                method_used: "specific".into(),
            });
            per_event.push(EventApplyResult {
                event_seq: event.event_seq,
                value_leg_amount: unit.unit_cost,
                applied_unit_cost: Some(unit.unit_cost),
                depletion_ids: vec![],
            });
        }

        ApplyResult {
            per_event, layer_inserts: vec![],
            depletion_inserts, provisional_flags: vec![],
        }
    }

    fn validate_invariants(
        &self, _affected: &[PoolKey], _snap: &Self::Snapshot,
    ) -> Result<(), InvariantViolation> {
        // Specific-id's invariant: each unit_id is depleted at most once.
        // This is structural to the unit lifecycle in acct (inventory_unit_events
        // transitions; status state machine). Extension trusts acct.
        Ok(())
    }
}
```

### 3.6 Balance-only implementation

For items or transactions that don't carry an inventory cost. Used when the ledger needs to record a movement but no FIFO/average/standard computation applies — balance transfers in the cash ledger, items intentionally not costed (samples, marketing collateral), or any transaction whose posting flows through the unified pipeline but has no costing dimension.

This is NOT cash-basis accounting (which is a timing concept about revenue/expense recognition, orthogonal to inventory costing).

```rust
pub struct BalanceOnlyMethod;
pub struct BalanceOnlySnapshot;

impl CostingMethod for BalanceOnlyMethod {
    const METHOD_ID: &'static str = "balance-only";
    type Snapshot = BalanceOnlySnapshot;

    fn required_dimensions() -> &'static [DimensionKind] { &[] }

    fn plan_apply(&self, batch: &ApplyBatch, _snap: &Self::Snapshot) -> ApplyResult {
        let per_event = batch.events.iter().map(|e| EventApplyResult {
            event_seq: e.event_seq,
            value_leg_amount: 0,
            applied_unit_cost: Some(0),
            depletion_ids: vec![],
        }).collect();
        ApplyResult {
            per_event, layer_inserts: vec![], depletion_inserts: vec![],
            provisional_flags: vec![],
        }
    }

    fn validate_invariants(&self, _affected: &[PoolKey], _snap: &Self::Snapshot)
        -> Result<(), InvariantViolation> { Ok(()) }
}
```

### 3.7 Why no FifoPeriodic, WacPerpetual, etc. in this section

These methods exist in acct but their natural expression is set-based plpgsql, not per-row procedural Rust. They register through acct's existing `cost_method_strategies` plpgsql registry. See §9 for the decision criteria and the trait/registry split.

The trait protocol is designed to allow WAC family methods to be ported to Rust later if their performance characteristics favor it (the protocol has the necessary close-hook surface). The decision is per-method, deferred until measurement supports it.

---

## 4. Concurrency architecture: in-transaction default, queue+committer alternative

### 4.1 In-transaction synchronous (default)

The default apply path is fully synchronous within the user's transaction:

```
User tx:
BEGIN
SELECT post_wo_complete(...)               (acct plpgsql)
  → _post_posting_lines_apply_event        (acct dispatcher)
  → builds event JSONB, calls dispatcher
  → for each (credit-side-pool, method) group:
    → if method registered with trait:
      → SELECT ledger_apply_batch($json)   (extension #[pg_extern])
        → resolves method
        → builds snapshot (one SPI batch per method's required_dimensions)
        → acquires FOR UPDATE on relevant accounts (R4)
        → calls method.plan_apply (pure Rust)
        → INSERTs posting_lines, posting_line_inventory,
                  cost_layer_depletions, posting_lines_provisional
        → returns per_event results to dispatcher
    → if method registered with plpgsql registry:
      → calls _compute_amount_<method>_outbound (existing path)
  → assembles full WO posting
COMMIT
```

Properties:
- Cost is durable iff posting is durable; atomicity by construction.
- All R1-R7 invariants hold without external recon.
- No abort-after-cost-committed failure mode.
- Lock-hold span is apply-call to commit (per acct's `_post_posting_lines_lock_pre_scan` pattern).
- Throughput characterized by acct-xeee at WAL ceiling for typical fan_in / fan_out shapes.

**This is the default. All correctness invariants (I-R1, I-R2, I-R4, I-R5, I-R7) are enforced in this mode by construction.**

### 4.2 Queue+committer (opt-in via GUC)

Enabled via `ledger.apply_mode = 'queue'` (default `'in_transaction'`). When enabled, the extension's apply call enqueues a request to a shmem queue rather than executing synchronously.

Architecture summary (full design in §4.3-§4.6):

```
User tx:                                Extension:
BEGIN
SELECT ledger_apply(...)                → enqueue request to per-pool shard
                                        → wait on result slot
                                        ← return applied_cost
INSERT INTO posting_lines ... (acct)
COMMIT
                                        Committer (elected from waiting backends):
                                        → drains queue batch
                                        → builds snapshot once for batch
                                        → calls method.plan_apply
                                        → opens committer sub-transaction
                                        → INSERTs all rows
                                        → COMMITs sub-transaction
                                        → writes results to slots, wakes waiters
```

The committer sub-transaction commits before the user transaction commits. If the user tx aborts after the committer succeeded, the extension's XactCallback ABORT enqueues a compensating reversal that the next committer tick processes, INSERTing reversal rows that offset the original consumption.

When to enable queue mode:
- Workload exhibits high concurrent fan-in to small set of pools (many backends consuming from same SKUs).
- User transactions hold posting work open across multiple apply calls; commit coalescence across user transactions wins.
- Measurement under bake-off methodology (§4.7) shows queue mode beats in-tx on the deployment's actual workload mix.

When to leave queue mode disabled:
- Workload is fan-out heavy or BOM-driven with disjoint per-WO pools (path 4's measured shape).
- User transactions are short and single-purpose.
- Cost-with-posting atomicity is required by audit policy.

### 4.3 Per-shard queue structure (queue mode only)

```rust
// QUEUE_SHARD_COUNT and REQUESTS_PER_SHARD are GUC-sized at postmaster
// startup via RequestAddinShmemSpace.

#[repr(C, align(64))]
pub struct QueueShard {
    pub lock_idx: u32,
    pub _pad0: [u8; 4],

    /// Ring buffer of pending requests, sized at startup.
    pub head: AtomicU32,
    pub tail: AtomicU32,
    pub capacity: u32,
    pub requests_offset: u32,  // points to request array in shmem

    /// Result slot pool, sized at startup.
    pub slot_alloc_next: AtomicU32,
    pub slots_offset: u32,

    /// Committer election state.
    pub committer_pid: AtomicI32,
    pub committer_acquired_at: AtomicU64,

    /// Per-shard monotonic sequence for request ordering.
    pub next_seq: AtomicU64,
}

#[repr(C)]
pub struct PendingRequest {
    pub valid: AtomicU8,               // 0=empty, 1=filled, 2=abandoned
    pub _pad: [u8; 7],
    pub request_seq: u64,
    pub credit_pool_hash: u64,          // for grouping in committer
    pub method_id_hash: u32,            // resolved at enqueue time
    pub event_json_offset: u32,         // points to inline JSON in spillover area
    pub event_json_length: u32,
    pub backend_pid: i32,
    pub slot_id: u32,
}

#[repr(C, align(64))]
pub struct ResultSlot {
    pub state: AtomicU8,                // 0=free, 1=allocated, 2=filled, 3=abandoned
    pub _pad: [u8; 7],
    pub value_leg_amount: AtomicI64,
    pub applied_unit_cost: AtomicI64,
    pub error_code: AtomicU16,
    pub error_message_inline: [u8; 240],
    pub spillover_offset: AtomicU32,    // for >32 depletion_ids, OPEN-10 spillover
    pub depletion_count: AtomicU16,
    pub depletion_ids_inline: [AtomicI64; 32],  // fast path for ≤32; spillover otherwise
}
```

Item-to-shard mapping: `shard_id = hash(credit_pool_key) & (QUEUE_SHARD_COUNT - 1)`. The committer processes ALL pools in the shard during its tick, sub-batched by (credit-pool, method).

**OPEN-10 (32-id slot limit, formerly v1 open issue) resolved**: spillover to a separate shmem region of variable-length arrays, allocated from a freelist. The inline 32-id array is the fast path; for batches like fan_in long-run (acct-4b3j: 290K layers in one group), the slot's `spillover_offset` points to a heap-allocated `Vec<i64>` in shmem's spillover region. Sized at startup; runs out under extreme load (back-pressure handles).

### 4.4 Request lifecycle (queue mode)

```
Caller backend:
1. Resolve credit-side SKU → cost_method (R2 dispatch).
2. Compute shard_id from credit_pool_key.
3. Acquire shard's LWLock (brief).
4. Allocate slot.
5. Serialize event to JSON, store in shmem spillover.
6. Push PendingRequest into ring at queue tail.
7. CAS tail forward.
8. Release shard LWLock.
9. Attempt committer election (CAS committer_pid 0 → my_pid).
   - If win: become committer, proceed to drain (step 10a).
   - If lose: become waiter (step 10b).

10a. Committer path:
   - WaitLatch up to batch_window_us, or until batch_size_max requests queued.
   - Acquire shard LWLock.
   - Drain all valid requests; mark in-progress.
   - Release shard LWLock.
   - Group drained requests by (credit_pool, method_id).
   - For each group:
     - Resolve method.
     - Build snapshot (one SPI batch).
     - Acquire FOR UPDATE on credit-pool's accounts (R4).
     - Call method.plan_apply.
     - INSERT rows.
   - BeginInternalSubTransaction, then for all groups:
     - INSERT collected rows under sub-transaction.
   - CommitInternalSubTransaction.
   - For each request: write result into slot, mark state=filled.
   - Wake waiters via PROCSIGNAL_NOTIFY_INTERRUPT.
   - CAS committer_pid back to 0.

10b. Waiter path:
   - Loop: ProcSemaphoreSleep with CHECK_FOR_INTERRUPTS check.
   - On wake: check slot state.
     - If filled: read result, return.
     - If abandoned (committer died, see §4.6): re-enqueue, retry.
   - On query cancel: mark slot abandoned, return error.
```

### 4.5 Committer election and batch window

GUCs:

| GUC | Default | Range | Reload |
|-----|---------|-------|--------|
| `ledger.apply_mode` | `in_transaction` | `in_transaction` \| `queue` | Sighup |
| `ledger.batch_window_us` | 500 | 100-10000 | Sighup |
| `ledger.batch_size_max` | 1024 | 16-65536 | Sighup |
| `ledger.committer_lease_ms` | 100 | 10-10000 | Sighup |
| `ledger.queue_full_timeout_ms` | 5000 | 100-60000 | Sighup |
| `ledger.queue_shard_count` | 256 | 16-4096, power-of-two | Postmaster |
| `ledger.requests_per_shard` | 4096 | 256-65536 | Postmaster |
| `ledger.cache_enabled` | false | bool | Sighup |
| `ledger.recon_interval_s` | 3600 | 60-86400 | Sighup |

### 4.6 Failure modes (queue mode)

**Committer transaction failure** (constraint violation, deadlock with unrelated tx, OOM):
- Caught via PgTryBuilder.
- Sub-transaction rolled back.
- All slots in batch receive error.
- Waiters return error to their callers.
- Caller's user tx can retry; the retry enters a new batch.

**Committer backend dies mid-batch**:
- Lease timeout (committer_lease_ms) detected by next backend pushing to shard.
- New backend CAS-acquires committer role, runs `recover_orphaned_batch`:
  - Scans queue for in-progress requests.
  - Checks pg_xact status of dead committer's sub-tx.
  - If committed: backfill slots from cost_layer_depletions for the affected event_seqs.
  - If aborted or unknown: mark requests as abandoned and re-enqueue (or surface error if abandoned twice in a row).

**Caller query cancel mid-wait**:
- Waiter's CHECK_FOR_INTERRUPTS triggers.
- Waiter marks slot abandoned, returns cancellation to caller.
- Committer, when processing, sees abandoned slot.
  - For idempotent methods (FIFO with stable event_seq → identical plan): still process the request; result is durable and consistent.
  - For methods that consume external resources at apply time: skip processing (depends on method declaring idempotency property; documented in trait protocol).

**Backpressure (queue full)**:
- Caller blocks on push.
- CHECK_FOR_INTERRUPTS periodically.
- Bounded by queue_full_timeout_ms.
- On timeout, returns `queue_full` error to caller; caller's tx aborts or retries.

**Postmaster crash**:
- Shmem queue contents lost.
- On restart, recovery worker scans for `cost_layer_depletions` rows whose `posting_line_id` user-tx (resolved via posting_lines.created_by_process or equivalent) shows as `unknown` / `aborted` in pg_xact.
- Emits compensating reversal layer events for each.
- Resumes normal operation.

### 4.7 Bake-off methodology (driving the queue-vs-in-tx decision)

The decision between in-tx and queue is workload-specific and determined by measurement, not pre-committed in this spec.

**Rig:**
- `load_phase1_mixed_workload` (acct's existing benchmark harness)
- `1s6r` shape: 32 writers, mixed Slice A (orders) + Slice B (manufacturing) + Slice C (mixed)
- acct-8hv2 methodology: 5×60s with 30s gaps, IQR-based signal/noise classification, `pg_locks_sampler` at 100ms

**Shapes to measure (each at standard parent SKU AND at wac_retroactive parent SKU):**
- `fan_in g=1`: 32 writers all consuming from 1 SKU (worst-case contention).
- `fan_out g=5000`: 32 writers spread across 5000 SKUs (disjoint).
- `balanced g=50`: 32 writers across 50 SKUs (moderate sharing).
- `small b=100 g=50`: 100-line batches, 50 SKUs.
- `BOM-heavy 5L`: WO-complete with typical 5-line BOM.
- `BOM-heavy 50L`: WO-complete with kit/assembly 50-line BOM.
- `BOM-heavy phantom`: 2-level BOM with phantom intermediate (stresses orchestration boundary).

All wac_retroactive variants exercise the close-hook DAG path.

**Metrics:**
- Throughput: median + IQR over the 5 runs.
- Latency: p50, p99, p99.9 from apply call to return.
- Contention: deadlock count, lock wait time from `pg_locks_sampler`.
- Commit coalescence (queue mode only): average requests per committer-tx.

**Outputs:**

The bake-off publishes a surface: (architecture, workload-shape) → (throughput, latency-percentiles). The deployment policy picks based on its measured workload mix and tail-latency tolerance. No pre-committed multiplier in this spec.

**Decision artifact:** a `BENCHMARK_RESULTS.md` per acct version, recording the surface and the recommended default for each shape. Operators consult this when choosing `ledger.apply_mode`.

---

## 5. Framework internals

### 5.1 Dispatch flow

```
PostingEvent received by acct dispatcher
  → resolve credit-side SKU (R2)
  → look up sku.cost_method
  → look up method registration (trait or registry)
  → route:
    if trait:
      → SELECT ledger_apply_batch($event_json)  (single FFI)
        → resolve associated Snapshot type
        → read method.required_dimensions()
        → build snapshot via dimension-reader registry (§5.3)
        → acquire FOR UPDATE on credit-pool accounts (R4)
        → call method.plan_apply
        → INSERT rows
        → return per_event results
    if registry:
      → call _compute_amount_<method>_outbound (existing plpgsql)
```

### 5.2 The plpgsql shim contract

The plpgsql layer above the extension is responsible for these seven steps, in order, with property-test-enforceable invariants per step. The shim is acct's existing `_post_posting_lines_apply_event` extended with the new FFI call.

**Step 1: Credit-first SKU resolution per event.**
- `product_id := COALESCE(credit.sku_id, debit.sku_id)` (R2)
- *Invariant I-R2-step1*: every cost-bearing event has resolvable product_id (preflight in mig 0024 confirmed 0 exceptions).

**Step 2: Cost-method lookup.**
- `cost_method := sku.cost_method WHERE sku.id = product_id`
- *Invariant I-R2-step2*: cost_method is non-null and registered (trait or registry).

**Step 3: Group events by (credit-side-pool, cost_method).**
- Pool key: `(credit_account_id, account_class, product_id, location_id, currency, legal_entity_id)`
- *Invariant I-R2-step3*: every event in a dispatch group has the same credit-side cost_method (the R2 invariant tested by property test).

**Step 4: For each group, look up method's `required_dimensions()`.**
- For trait methods: SELECT from the trait's static dimensions list.
- For registry methods: from `cost_method_strategies.dimension_kinds`.
- *Invariant I-dim*: dimension list is non-empty for any method that reads any cost-affecting state.

**Step 5: Build per-method snapshot.**
- For each declared dimension, query the appropriate source table (§5.3).
- *Invariant I-dim-completeness*: snapshot contains exactly the declared dimensions, no more, no less. Type-level enforced for trait methods via associated `Snapshot` type; runtime-asserted for registry methods.

**Step 6: Acquire FOR UPDATE on credit-pool's accounts.**
- Lock the value account (R4); locking the qty account too if the method declares it.
- *Invariant I-R4*: snapshot reads occur strictly after FOR UPDATE acquisition. Instrumentation-tested via SPI call ordering wrapper.

**Step 7: Pass batch to method.**
- Trait: SELECT ledger_apply_batch($json).
- Registry: call _compute_amount_<method>_outbound.
- Method returns per-event amounts + provisional flags + (for trait) row inserts.
- Framework merges trait-returned inserts + method-returned provisional flags, then INSERTs everything in one statement per table.

**Property tests for the shim contract (§9.4):**
- I-R1: generate scenarios where multiple SKUs (different class roles — raw, fg) share a location, plus WIP accounts attached to work orders at the same location. Assert that the divisor for one SKU's pool reads only that SKU's account.id, not aggregating across sibling-account qty (the `stock_available` footgun).
- I-R2-step1, I-R2-step3: generate random multi-method event mixes; assert dispatch groups are method-homogeneous.
- I-R4: instrument account-lock acquisition + snapshot SPI in a recorder; assert ordering.
- I-dim-completeness: type-level enforcement; runtime test asserts no extra dimensions read, no declared dimension missing.

### 5.3 Snapshot construction (dimension-reader registry)

The framework owns a dimension-reader registry: mapping from `DimensionKind` to SQL query template. Snapshot construction for a method:

```rust
fn build_snapshot<M: CostingMethod>(
    pool: &PoolKey,
) -> M::Snapshot {
    let mut readers = SnapshotReader::new(pool);
    for dim in M::required_dimensions() {
        match dim {
            DimensionKind::Class => readers.read_class(),
            DimensionKind::Product => readers.read_product(),
            DimensionKind::Location => readers.read_location(),
            DimensionKind::Currency => readers.read_currency(),
            DimensionKind::LegalEntity => readers.read_legal_entity(),
            DimensionKind::TransactionalCurrency => readers.read_transactional_currency(),
            DimensionKind::Lot => readers.read_lots_for_pool(),
            DimensionKind::Unit => readers.read_units_for_pool(),
            DimensionKind::CostLayer => readers.read_layers_for_pool(),
            DimensionKind::CostBook => readers.read_cost_book(),
            DimensionKind::Analytical(dim_type_id) => readers.read_analytical(dim_type_id),
        }
    }
    M::Snapshot::from_readers(readers)
}
```

**Identity dimensions** (Class, Product, Location, Currency, LegalEntity, TransactionalCurrency, Lot, Unit, CostLayer, CostBook):
- Framework-bundled: each has a known SQL query template against acct's schema tables.
- Adding a new identity dimension is a framework change (new column on acct schema + new reader + new enum variant + likely new method that uses it). Methods ship with their dimensions.

**Analytical dimensions** (Analytical(DimensionTypeId)):
- EAV-extensible: data-only. Adding a new analytical dimension is `INSERT INTO dimension_types`.
- Reader query is generic: `SELECT dimension_value, dimension_value_uuid FROM posting_line_dimensions WHERE dimension_type = $1 AND posting_line_id = ANY(...)`.
- Methods declare `Analytical(routing_op_type_id)`; framework dispatches generically.

**The seam (documented for future readers):**

| Category | Examples | Extension mechanism |
|----------|----------|---------------------|
| Identity dimensions | Lot, Unit, CostLayer, CostBook | Framework-bundled with cost-method support; ships together |
| Analytical dimensions | routing_op, project, cost_center, country_of_origin | EAV data-extensible; method declares Analytical(type_id) |

Future contributors adding a new dimension should know which kind they're adding. Most growth is analytical; identity dimensions evolve more slowly and ship with their consuming methods.

### 5.4 Close-hook framework

**Pool identity at close.** A pool is an `account.id`. Two flavors:

- **SKU-keyed accounts** (raw, fg): `account.id` resolves to a `(sku, location, currency)` triple plus class. Closing method comes from `sku.cost_method`.
- **WO-keyed accounts** (wip): `account.id` resolves to a `(work_order, operation, currency)` tuple — no SKU. Closing method comes from the work order's configured costing method (typically inherited from the WO's primary output SKU).

The framework iterates pools in Kahn order without distinguishing flavors at the iteration boundary; what differs is just how `sku.cost_method` is resolved for the pool. The merged event stream construction handles both: for SKU-keyed pools (raw/fg) only value-leg events are walked; for WIP pools, value-leg events are paired with stock_wip qty-leg events as described in Appendix A.3.

For each closing period, framework orchestrates:

```
1. Scan posting_lines for events in [period_start, period_end]
   with reasons in (union of all participating methods' participating_reasons).
2. Build per-pool predecessor edges from those events (credit pool → debit pool).
3. Kahn topological sort. Cycle detection → P0036.
4. For each pool in Kahn order:
   a. Resolve the method that closes this pool (from sku.cost_method on pool's SKU).
   b. Build merged event stream for pool:
      - All in-period postings against this pool.
      - sub_priority ordering (0=qty-in, 1=value, 2=qty-out) within each event.
      - business_date, doc_chrono, document_id, sub_priority, posting_line_id as sort key.
   c. Call method.close_hook_preprocess_stream(events) → preprocessed.
   d. Call method.close_hook_replay_pool(pool, preprocessed, upstream_cache, snapshot).
   e. For each variance computed by the method:
      - Resolve RouteContext (category, pool kind, drained-to-zero, destination method).
      - Call method.emit_variance(ctx) → VarianceEmission.
      - Framework structurally validates emission (§5.6).
      - If valid: INSERT variance posting_lines + UPDATE provisional row.
      - If invalid: rollback period close, raise P0xxx with specifics.
   f. Update upstream_variance_cache for downstream pools.
5. Variance posting_lines INSERTed under one period-close transaction.
```

### 5.5 RouteContext and category detection

The framework computes RouteContext from structural properties:

```rust
pub struct RouteContext {
    pub category: RouteCategory,
    pub pool_kind: AccountClass,
    pub pool_drained_to_zero_in_period: bool,
    pub destination_sku_method: Option<MethodId>,
    pub provisional_id: PostingLineId,
    pub variance_amount: i64,
    pub event_metadata: EventMetadata,
}

pub enum RouteCategory {
    InternalChain,
    LeafSingleLeg,
    LeafTwoLegWash,
    MixedParentComponent,
}
```

Detection rules:
- `pool_kind = inv_value_wip` AND event reason ∈ {op_move_v, rm_issue_to_wo} AND pool drained-to-zero by document path in period → `InternalChain`
- `pool_kind ∈ {inv_value_raw, inv_value_fg}` AND drained-to-zero in period → `LeafSingleLeg`
- `pool_kind ∈ {inv_value_raw, inv_value_fg}` AND NOT drained-to-zero in period → `LeafTwoLegWash`
- destination SKU's cost_method ≠ closing method (e.g., standard parent + wac_retroactive component) → `MixedParentComponent`

Method's `emit_variance(ctx)` returns:

```rust
pub enum VarianceEmission {
    NoEmission,  // internal-chain only; finalize provisional with variance_amount, leave variance_posting_line_id NULL
    SingleLeg { credit_account: i64, debit_account: i64, amount: i64 },
    TwoLegWash { pool_account: i64, variance_account: i64, amount: i64 },
    Custom { rows: Vec<PostingLineSpec>, variance_kind: VarianceKind },
}

pub struct VarianceKind(&'static str);  // method-declared, opaque to framework for grouping
```

Method picks variance account names (which may be method-specific: variance_wac_periodic, variance_wac_retroactive, variance_material_mixed, future variance_byproduct_yield, etc.). VarianceKind is a method-declared opaque tag the framework uses for recon grouping and metrics; framework doesn't enumerate kinds.

### 5.6 Framework structural validation

What the framework validates on a `VarianceEmission`:

1. **Double-entry signed-sum balance**: sum of debits in emission rows equals sum of credits.
2. **Pool integrity**: emission does not push any debit-normal pool below 0 or credit-normal pool above 0 at commit. Verified by computing the projected post-commit balance under the assumed-already-committed in-period postings.
3. **Schema integrity**: every account referenced exists; currencies match within an entity; legal_entity matches.
4. **Provisional state transition validity**: the targeted provisional row is currently in state Open; transition to state 2 (finalized-with-emit) or state 3 (finalized-internal-only) is consistent with the emission.

What the framework does NOT validate:
- Variance account naming (method's choice).
- Emission shape semantics (method's choice within structural constraints).
- Audit categorization (method declares via VarianceKind).

**Failure mode is runtime-detected, not compile-time.** A method that emits a wrong-shape variance gets caught at commit by the framework's validation; commit refuses with a structured error pointing at which structural rule was violated. This is acceptable because:
- Test surface catches it during method development (fault-injection tests).
- Alternative (compile-time-correct routing) requires hardcoded framework enumeration of routes, which loses extensibility.

Runtime structural validation is documented in trait protocol section explicitly as the safety net.

---

## 6. Shmem layout

### 6.1 What lives in shmem (queue mode)

```rust
pub struct LedgerShmem {
    pub queue_shards: ShardArray,             // sized at startup
    pub spillover_arena: SpilloverArena,      // for >32-id slot results + variable-length JSON
    pub layer_cache: Option<LayerCache>,      // enabled via ledger.cache_enabled
    pub global_state: GlobalState,
}

pub struct GlobalState {
    pub apply_seq: AtomicU64,
    pub committer_tx_seq: AtomicU64,
    pub batch_size_total: AtomicU64,
    pub batch_count_total: AtomicU64,
    pub backpressure_count: AtomicU64,
    pub committer_tx_failures: AtomicU64,
    pub orphan_compensations: AtomicU64,
}
```

### 6.2 Layer cache (optional, off by default)

When `ledger.cache_enabled = true`, the framework maintains a per-pool layer cache:

```rust
#[repr(C, align(64))]
pub struct ItemCacheEntry {
    pub occupied: AtomicU8,
    pub _pad: [u8; 7],
    pub pool_hash: AtomicU64,
    pub last_updated_seq: AtomicU64,
    pub needs_reload: AtomicU8,
    pub _pad2: [u8; 7],
    pub layer_count: AtomicU16,
    pub layers: [CachedLayer; MAX_CACHED_LAYERS_PER_POOL],
}
```

Update points:
- Post-commit by committer (queue mode) or post-INSERT by user backend (in-tx mode).
- Lazy reload on read when `needs_reload` set or `last_updated_seq` stale.
- Invalidation on method assignment change via NOTIFY.

Cache is a hint, not authoritative. Methods that read the cache check `needs_reload`; on stale, fall back to SPI and refresh.

Default off because:
- Path 4 measurements showed SPI-in-plan_apply was not the bottleneck.
- Enabling adds coherence complexity (post-commit update, invalidation).
- Should be enabled only when profiling shows it earns its keep.

### 6.3 Shmem sizing (GUC at startup)

```rust
extern "C-unwind" fn _PG_init() {
    let shard_count = guc::queue_shard_count();
    let requests_per_shard = guc::requests_per_shard();
    let cache_enabled = guc::cache_enabled();
    let item_cache_size = if cache_enabled { guc::item_cache_size() } else { 0 };

    let shmem_bytes = compute_shmem_size(
        shard_count, requests_per_shard, item_cache_size,
    );
    unsafe {
        pg_sys::RequestAddinShmemSpace(shmem_bytes);
    }
    pg_shmem_init!(LEDGER_SHMEM);
}
```

Postmaster-restart required to change `queue_shard_count` or `requests_per_shard`. Other GUCs are sighup.

### 6.4 OPEN-10 spillover (resolved)

The fixed 32-element `depletion_ids_inline` array in ResultSlot is the fast path. For requests producing more than 32 depletions (fan_in long-run scenario per acct-4b3j: 290K layers in one group):

```rust
fn write_depletion_ids(slot: &ResultSlot, ids: &[i64]) {
    if ids.len() <= 32 {
        for (i, id) in ids.iter().enumerate() {
            slot.depletion_ids_inline[i].store(*id, Ordering::Release);
        }
        slot.depletion_count.store(ids.len() as u16, Ordering::Release);
    } else {
        let offset = SHMEM.spillover_arena.allocate(ids.len() * 8);
        SHMEM.spillover_arena.write_i64_array(offset, ids);
        slot.spillover_offset.store(offset, Ordering::Release);
        slot.depletion_count.store(ids.len() as u16, Ordering::Release);
    }
}
```

Spillover arena is a freelist-managed shmem region; sized at startup via GUC `ledger.spillover_arena_mb`. Allocations are released when the result slot is freed.

For pathological requests that exceed the spillover capacity, backpressure kicks in (request is rejected with `result_too_large` error; caller retries with smaller batches or accepts that the request needs application-level chunking).

---

## 7. Trait-where-it-fits; registry-where-it-doesn't

The unified-dispatch goal is "every cost method is registered somewhere; the dispatcher routes correctly," not "every method uses the same dispatch mechanism." Per-method decision:

### 7.1 Decision criteria

**Use the Rust trait when:**
- Method logic is per-row procedural (FIFO walks layer ring; specific-id is unit lookup; balance-only is trivial).
- Method's snapshot is bounded (a few hundred rows at most).
- Method benefits from Rust correctness (type-safe enum routing, exhaustiveness checking, no SPI overhead in hot path).
- Method's invariants are local (per-row, per-pool).

**Use the plpgsql registry when:**
- Method logic is set-based (DAG traversal over period postings; cross-pool joins).
- Snapshot is the whole period's postings (close hook).
- Method benefits from SQL's set algebra (aggregation, window functions, GROUP BY).
- Method's invariants span large state (predecessor edges across all pools in period).

### 7.2 Current assignments (initial implementation)

| Method | Path | Rationale |
|--------|------|-----------|
| FIFO | Trait | Per-row walk; cheap snapshot |
| Specific-id | Trait | Per-event lookup; bounded snapshot |
| Balance-only | Trait | Trivial; aligns with future "in-tx fast path" methods |
| WAC perpetual | Registry (existing) | Set-based perpetual avg; already works |
| WAC periodic | Registry (existing) | Set-based period avg + close hook |
| WAC retroactive | Registry (existing) | Set-based DAG walk at close; chronological replay |
| Standard cost | Registry (existing) | Set-based variance posting at close |
| Future Rust methods | Trait | If they fit criteria |
| Future plpgsql methods | Registry | If they fit criteria |

### 7.3 Per-method dispatch resolution

```sql
-- Dispatcher in _post_posting_lines_apply_event (sketched):
CREATE OR REPLACE FUNCTION _dispatch_cost_compute(p_event JSONB) RETURNS JSONB AS $$
DECLARE
    v_credit_sku UUID;
    v_method cost_method;
    v_dispatch_path TEXT;
BEGIN
    v_credit_sku := COALESCE((p_event->>'credit_sku_id')::UUID, (p_event->>'debit_sku_id')::UUID);
    SELECT cost_method INTO v_method FROM skus WHERE id = v_credit_sku;
    SELECT dispatch_path INTO v_dispatch_path FROM cost_method_registrations WHERE method_id = v_method::text;

    IF v_dispatch_path = 'trait' THEN
        RETURN ledger_apply_batch(p_event);  -- FFI to extension
    ELSIF v_dispatch_path = 'registry' THEN
        RETURN _compute_amount_<method>_outbound(p_event);  -- existing plpgsql
    ELSE
        RAISE EXCEPTION 'P0040: unregistered cost method %', v_method;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

`cost_method_registrations` is a small lookup table populated at extension load (trait methods register on _PG_init) and at acct migration time (registry methods register in their migration).

### 7.4 Migration between paths

A method can move between trait and registry if its performance characteristics shift. Procedure:
1. Implement the method in the new path.
2. Add to `cost_method_registrations` with the new path.
3. Run shadow tests: dispatcher in test mode runs both paths, compares results.
4. After validation, update production registration.
5. Remove old implementation.

This is a per-method, deliberate migration — not a wholesale architecture change.

---

## 8. Operational concerns

### 8.1 Observability

```rust
#[pg_extern] fn ledger_apply_seq() -> i64;
#[pg_extern] fn ledger_committer_tx_seq() -> i64;
#[pg_extern] fn ledger_shard_stats() -> TableIterator<(/* per-shard depth, committer_pid, batch stats */)>;
#[pg_extern] fn ledger_method_stats() -> TableIterator<(/* per-method dispatch count, error count */)>;
#[pg_extern] fn ledger_queue_depth() -> i64;
#[pg_extern] fn ledger_backpressure_count() -> i64;
#[pg_extern] fn ledger_committer_tx_failures() -> i64;
#[pg_extern] fn ledger_orphan_compensations() -> i64;
#[pg_extern] fn ledger_avg_batch_size() -> f64;
#[pg_extern] fn ledger_variance_kind_counts() -> TableIterator<(/* per variance_kind count */)>;
```

Operators use these from `pg_stat_statements` or wire to external metrics.

### 8.2 BGWorkers

| Worker | Role | Cadence |
|--------|------|---------|
| Compensation recovery (queue mode) | Scan for orphan committer-txs whose user-tx is aborted; emit compensations | 5min + on startup |
| Recon | Cross-check cost_layer_depletions against acct's posting_lines; report drift | configurable, default 1hr |
| Archival hook | Trigger acct's existing layer-group archival when extension state allows | daily, configurable |

No bgworker handles the apply hot path. In-tx mode has no committer-tx machinery; queue mode's committer runs in user backends via election.

### 8.3 Recovery on extension load

`_PG_init` runs:
1. Allocates shmem per GUCs.
2. Reads `cost_method_registrations` to populate dispatcher table.
3. (Queue mode only) Spawns compensation recovery worker.
4. (Cache enabled only) Spawns cache warmup worker.

Postmaster restart implicitly invokes _PG_init on every restart; recovery is bounded by recompiling the dispatcher table and rebuilding the cache lazily.

### 8.4 Recon job

The recon bgworker periodically checks:
- I3-app: every cost_layer_depletions row references a posting_line. Orphan posting_lines (posting referenced doesn't exist) → P0xxx alert.
- I6-app: every cost-bearing reversal posting_line has posting_line_sources.reverses_posting_line_id set to a real prior posting. Missing chains → alert.
- I-prov-3state: posting_lines_provisional in valid state (CHECK already enforces; this is belt-and-suspenders).
- (Queue mode) Compensation completeness: every aborted committer-tx has a compensating reversal posted.

Recon does not enforce; it reports. Discrepancies are operational signals for investigation.

---

## 9. Testing methodology

### 9.1 Property tests (the shim contract)

Driven by the seven shim steps in §5.2:

```rust
#[pg_test]
fn test_shim_R2_dispatch_homogeneity() {
    // Generate random multi-method event mix; assert dispatch groups are homogeneous.
    let events = generate_random_events_across_methods(seed, count=1000);
    let groups = run_dispatch(events);
    for group in groups {
        let methods: HashSet<_> = group.events.iter()
            .map(|e| sku_method_at_event(&e)).collect();
        assert_eq!(methods.len(), 1, "dispatch group has multiple methods");
    }
}

#[pg_test]
fn test_shim_R4_lock_then_read_ordering() {
    // Wrap account-lock acquisition and snapshot SPI in a recorder.
    let recorder = SpiOrderingRecorder::new();
    run_apply_with_recorder(&recorder, generate_event(...));
    let log = recorder.into_log();
    let lock_idx = log.iter().position(|e| e.is_lock_acquire()).unwrap();
    let read_idx = log.iter().position(|e| e.is_snapshot_read()).unwrap();
    assert!(lock_idx < read_idx, "snapshot read before lock acquire");
}

#[pg_test]
fn test_shim_R1_account_isolation() {
    // Two SKUs sharing a location: a raw-material SKU and a finished-good SKU.
    // Plus a WIP account at the same location attached to a work order.
    // R1: WAC for the raw SKU's pool must use only the raw SKU's account qty,
    // not aggregate qty from the FG SKU's account or the WIP account.
    let loc = make_location();
    let raw_sku = make_sku(role=Raw, location=loc);
    let fg_sku = make_sku(role=FinishedGood, location=loc);
    let wip_acct = make_wip_account(location=loc, work_order=wo_id);

    seed_postings_against_account(raw_sku.value_account(), value=1000, qty=100);
    seed_postings_against_account(fg_sku.value_account(), value=5000, qty=50);
    seed_postings_against_account(wip_acct, value=300, qty=null);

    let snapshot = build_snapshot::<WacPerpetual>(raw_sku.pool());
    assert_eq!(snapshot.pool.qty_divisor, 100,
        "raw SKU divisor aggregated qty from FG SKU or WIP account");
    assert_eq!(snapshot.pool.value, 1000, "raw SKU value contaminated by sibling accounts");
}

#[pg_test]
fn test_shim_dimension_completeness() {
    let snap = build_snapshot::<FifoMethod>(some_pool());
    // Type-level: FifoSnapshot has exactly the declared fields.
    // Runtime: assert no extra SPI calls for undeclared dimensions.
    assert_eq!(snap.dimensions_read, FifoMethod::required_dimensions());
}
```

### 9.2 Method-level tests (per method)

For each trait method, in isolation:

```rust
#[pg_test]
fn test_fifo_determinism() {
    let batch = make_batch(...);
    let snap = make_snapshot(...);
    let r1 = FifoMethod.plan_apply(&batch, &snap);
    let r2 = FifoMethod.plan_apply(&batch, &snap);
    assert_eq!(r1, r2);
}

#[pg_test]
fn test_fifo_purity() {
    let tracker = SpiTracker::install();
    let batch = make_batch(...);
    let snap = make_snapshot(...);
    let _r = FifoMethod.plan_apply(&batch, &snap);
    assert!(tracker.calls().is_empty(), "FIFO.plan_apply called SPI");
}

#[pg_test]
fn test_fifo_oldest_first() {
    let snap = snap_with_layers(vec![
        (id=1, born_at=T0, qty=50, unit_cost=10),
        (id=2, born_at=T1, qty=50, unit_cost=20),
    ]);
    let batch = batch_consume(30);
    let r = FifoMethod.plan_apply(&batch, &snap);
    assert_eq!(r.depletion_inserts[0].layer_id, 1);  // oldest first
}

#[pg_test]
fn test_fifo_multi_layer_span() {
    let snap = snap_with_layers(vec![
        (id=1, born_at=T0, qty=50, unit_cost=10),
        (id=2, born_at=T1, qty=50, unit_cost=20),
    ]);
    let batch = batch_consume(75);  // 50 from L1, 25 from L2
    let r = FifoMethod.plan_apply(&batch, &snap);
    assert_eq!(r.depletion_inserts.len(), 2);
    assert_eq!(r.per_event[0].value_leg_amount, 50*10 + 25*20);  // weighted
}
```

### 9.3 Multi-backend stress tests

```rust
#[pg_test_concurrent(backends = 32)]
fn test_concurrent_disjoint_items_linear_scaling() {
    let items = make_n_items(1000);
    let throughput = run_workload(backends=32, per_backend_items=items.chunked(32));
    assert!(throughput > base_throughput * 30);  // 32 backends, ~30x scaling
}

#[pg_test_concurrent(backends = 32)]
fn test_concurrent_same_item_serial_via_dispatch() {
    let item = make_item();
    // 32 backends all consuming from same item.
    let results = run_workload(backends=32, target_item=item);
    // In-tx mode: serialized via FOR UPDATE on accounts row.
    // Queue mode: serialized via committer election.
    // Both: no inconsistency.
    assert_eq!(total_consumed(results), expected_total);
    assert_no_double_consume(results);
}
```

### 9.4 Invariant verification (property-based)

Using `proptest`:

```rust
proptest! {
    #[test]
    fn invariants_hold_under_random_sequences(
        ops in random_op_sequence(max_len=10000, max_items=100, max_backends=8),
    ) {
        let ledger = TestLedger::new();
        apply_sequence(&ledger, &ops);
        // Verify I1, I2, I4, I5, I8, I-R1, I-R2, I-R5 after each op.
        assert_invariants(&ledger);
    }
}
```

### 9.5 Fault injection

```rust
#[pg_test]
fn test_committer_tx_constraint_violation() {
    // Inject a constraint violation during committer INSERT.
    inject_fault!("committer_tx_insert", FaultKind::ConstraintViolation);
    let result = SELECT_ledger_apply_batch(..);
    assert_eq!(result.error_code, ErrorCode::CommitterTxFailed);
    // No partial state.
    assert_eq!(count("cost_layer_depletions WHERE posting_line_id = $ours"), 0);
}

#[pg_test]
fn test_backend_sigkill_during_commit() {
    // Spawn helper backend; have it run an apply.
    // Mid-commit, SIGKILL it from the test harness.
    let helper = spawn_backend();
    helper.start_apply_and_sleep();
    sleep(50ms);
    kill(helper.pid, SIGKILL);
    // Recovery: another backend's next apply should detect orphaned committer-tx
    // and either backfill or compensate.
    let r = next_backend_apply();
    assert_invariants();  // I1, I2, etc. still hold.
}

#[pg_test]
fn test_variance_emission_structural_validation() {
    // Inject a method that emits a wrong-shape variance.
    register_test_method::<BrokenMethod>();
    let result = run_close_period();
    assert_eq!(result.error, ErrorKind::StructuralValidation);
    // Close was refused; provisional rows still in Open state.
    assert!(provisional_states().all(|s| s == State::Open));
}
```

### 9.6 Performance benchmarks (per §4.7)

The bake-off methodology and shapes from §4.7. Outputs feed into `BENCHMARK_RESULTS.md`.

Continuous: a CI job runs a subset (representative shapes) on every release-tagged commit; regressions of >5% on any metric fail CI. The full bake-off runs on dedicated hardware on schedule (weekly during active development, less often once stable).

### 9.7 Close-hook tests

```rust
#[pg_test]
fn test_close_hook_DAG_kahn_sort() {
    // Build a period with op_move_v / rm_issue_to_wo events.
    seed_period_with_dag(...);
    let result = close_period();
    assert!(result.processed_pools_in_order(expected_kahn_order));
}

#[pg_test]
fn test_close_hook_cycle_raises_P0036() {
    seed_period_with_cycle(...);
    let result = close_period();
    assert_matches!(result, Err(PgError { code: P0036, .. }));
}

#[pg_test]
fn test_close_hook_internal_chain_no_emission() {
    seed_period_with_internal_chain(...);
    let result = close_period();
    let provisionals = read_provisional_rows();
    let internal_chain = provisionals.iter()
        .filter(|p| p.variance_amount.is_some() && p.variance_posting_line_id.is_none())
        .count();
    assert!(internal_chain > 0);
}
```

---

## 10. Open issues

Items the spec does not resolve; decisions deferred to implementation phase with evidence.

**[OPEN-A] Cross-shard transaction atomicity (queue mode only).** A user tx touching multiple credit-pools that hash to different shards has independent committer transactions. If one succeeds and one fails, user sees inconsistent state. Compensation handles eventually; no 2PC. Decision: is compensation acceptable, or do we need a cross-shard coordination mechanism? Hold until queue mode bake-off shows whether cross-shard transactions are common in measured workload.

**[OPEN-B] By-product credit handling generalization.** `close_hook_preprocess_stream` is wac_periodic-specific in initial impl. If a second period-average method needs similar preprocessing, generalize to a `MethodShape` declaration (per-method numerator/denominator filters). Defer until second period-average method is implemented.

**[OPEN-C] Rework as new pool generations (acct-p7v).** Current cycle in close-hook DAG raises P0036. Proper rework modeling requires representing rework as a new pool generation with explicit transition layer events. Out of scope for v1; the trait protocol supports it (a new method-id with different participating_reasons semantics could implement).

**[OPEN-D] Multi-cost-book (acct-zf80).** `CostBook` is in the dimension enum, the schema column exists in mig 0030, but methods don't yet declare it. Deferred until multi-cost-book scope opens.

**[OPEN-E] Method config schema typing.** Per-method config is `JsonValue`; no schema enforcement. Acceptable for v1; revisit if config errors become a source of bugs.

**[OPEN-F] Snapshot construction batching across pools.** Currently snapshots are built per-pool. For workloads where many small pools are touched (BOM with 50 components), this is N SPI calls. Could batch into one SPI per dimension across all pools. Defer until bake-off shows it matters.

**[OPEN-G] Recovery from "extension partially loaded".** If extension load fails mid-init (shmem allocated, dispatcher table not populated), the cluster is in a half-state. Currently the bgworker fails; need to define whether the cluster should refuse all cost operations or fall back to registry-only. Decision needed before production.

**[OPEN-H] Compensation visibility window (queue mode).** Between committer-tx commit and user-tx commit/abort, the cost row is durable but the user posting is not yet. Reporting queries may see "cost without posting" briefly. Acceptable per the design discussion (post-the-fact recon is sufficient), but specific reporting requirements may need a "stable view" that filters out un-confirmed cost rows. Defer until reporting requirements firm up.

**[OPEN-I] Trait protocol versioning.** The trait will evolve. Need a versioning story: registered methods declare which protocol version they implement; framework supports the most recent N versions. Defer to when v2 protocol is needed.

**[OPEN-J] Hot-shard mitigation.** If `hash(credit_pool_key)` clusters hot pools into one shard, that shard's committer becomes a bottleneck while other shards idle. Mitigation: randomize hash; if production shows clustering, allow shard reassignment via item_id rehashing (requires postmaster restart). Detection: monitor per-shard queue depth and committer occupancy.

---

## 11. Out of scope (explicit)

- Multi-primary write coordination.
- Cross-cluster logical replication of cost state.
- BOM/assembly cost roll-up (orchestration in plpgsql; not in extension).
- Real-time GL posting (handled by acct).
- UI/UX of costing administration (acct).
- Posting_lines schema ownership (acct owns it; extension reads/writes per acct's contract).
- Period definitions, fiscal calendar (acct).
- Lot/serial state machine semantics (acct's lot/unit modules).
- Currency conversion (acct's posting_line_currencies).
- Replica-side extension load (extension is primary-only).

---

## Appendix A: The WAC protocol (close-hook reference)

The WAC family in acct's existing plpgsql is the canonical reference for the close-hook protocol. The trait protocol is designed to support this protocol shape; the WAC methods themselves are registry-implemented in plpgsql for the reasons in §7. This appendix documents the protocol so future trait-based methods can implement compatibly.

### A.1 Provisional flagging at apply

For `wac_periodic` and `wac_retroactive` depletions, the value-leg posting_line is also INSERTed into `posting_lines_provisional`:
- `posting_line_id = <FK to the value-leg posting>`
- `period_id = <the period of business_date>`
- `cost_method = 'wac_periodic' | 'wac_retroactive'`
- `qty = <signed qty from the value-leg>`
- `finalized_at = NULL`, `variance_amount = NULL`, `variance_posting_line_id = NULL`

The at-apply-time cost is the `amount` on the FK'd posting_line itself; the provisional row does not duplicate it.

The flagging fires post-credit-first-resolution in `_post_posting_lines_apply_event`. Receipts don't flag.

### A.2 Close-hook DAG construction

For each closing period, the framework scans posting_lines in `[period_start, period_end]` for events with reason ∈ (union of all wac_* methods' participating_reasons):
- `wac_periodic.participating_reasons = [op_move_v]`
- `wac_retroactive.participating_reasons = [op_move_v, rm_issue_to_wo]`

For each qualifying posting:
- Predecessor pool = account on the credit-side leg.
- Successor pool = account on the debit-side leg.

Kahn topological sort. Pools with no predecessors processed first. Cycles raise P0036.

### A.3 Merged event stream per pool

Within each pool in Kahn order, the framework builds a merged chronological stream of all in-period postings against that pool:
- For `inv_value_wip` pools: pair value-leg events with matching stock_wip qty events.
- For `inv_value_raw` / `inv_value_fg` pools: value-leg events only, with qty per row from posting_lines.qty signed by debit/credit.

Sort key: `(business_date, doc_chrono, document_id, sub_priority, posting_line_id)` where `doc_chrono = MIN(posted_at) OVER (PARTITION BY document_id)`.

sub_priority within an event:
- 0 = qty inflow on stock_wip
- 1 = value-leg either direction
- 2 = qty outflow on stock_wip

This ordering guarantees the value-leg at sub_priority=1 sees pool_qty INCLUDING any paired qty-inflow but EXCLUDING this event's own qty-outflow.

### A.4 Chronological replay

For each event in the merged stream:
- **Inflow value-leg**: resolve unit at `t.amount + COALESCE(p.variance_amount, 0)` from `posting_lines_provisional` JOIN where `variance_posting_line_id IS NULL` (the upstream-variance cache). This propagates cumulative shift down the chain without needing additional posting_lines.
- **Outflow value-leg**: recompute at running pool average, set the provisional row's variance_amount, decide routing (§A.5).

`wac_periodic` is simpler: compute period average once from all in-period inflows (after by-product preprocessing per A.6) and use that as the final unit for all outflows.

`wac_retroactive` is more complex: running average evolves as the chronological replay processes each event.

### A.5 Variance routing (the four patterns)

**Internal-chain (no posting_line emitted).** For `op_move_v` and `rm_issue_to_wo` events posting against `inv_value_wip` (debit-normal, single-pool, drained at WO close): record variance on provisional row, leave variance_posting_line_id NULL. Cumulative shift propagates downstream via the upstream-variance cache used in A.4.

**Leaf depletion, single-leg (posting_line emitted).** For `wo_complete_v`, `scrap_v`, `so_ship`, `rm_issue_to_wo` on raw/fg sources, when the document path drained the source pool to 0 in-period: emit a variance posting_line as single-leg between orig_debit and `variance_wac_periodic` / `variance_wac_retroactive`.

**Leaf depletion, two-leg wash (posting_line emitted).** Same reasons, when NOT drained to 0: emit a variance posting_line as 2-leg wash through the pool.

**Mixed parent/component (posting_line emitted via different variance account).** When destination SKU's cost_method ≠ the closing method (e.g., standard parent + wac_retroactive component): emit variance single-leg through `variance_material_mixed` against the COMPONENT pool, leaving destination WIP untouched.

### A.6 By-product credit handling

`wo_byproduct_credit` events post the parent's value-leg credit with `qty=NULL`.

`wac_periodic` subtracts qty=NULL credit-side events from the period pool_value numerator (handled in `close_hook_preprocess_stream`).

`wac_retroactive` handles it via chronological replay decrementing running pool_value on credit-side qty=NULL events naturally; no preprocessing needed.

### A.7 Account isolation in the WAC qty divisor (R1)

**Class** is a property of an account, expressed in `accounts.kind` ∈ {inv_value_raw, inv_value_wip, inv_value_fg, ...}. The class describes what role the account plays — raw materials, work-in-process, finished goods.

**A SKU has one class.** A SKU is a thing you buy or sell. "Widget X" is one SKU whether you purchase it from a supplier or build it; it lives in finished goods. "Steel Bar" is a raw material SKU; it lives in raw inventory accounts. A given SKU's inventory accounts are class-typed by the SKU's role in your business, and the SKU does not span multiple classes.

**WIP is not SKU-keyed.** Work-in-process is not "this SKU at the WIP stage." It is value (and, paired with stock_wip, qty) accumulating against a work order during transformation. You cannot sell a half-assembled Widget — that isn't a SKU. WIP accounts are keyed by work-order/operation granularity, not by sellable SKU identity. Value flows into WIP from raw-material issues; value flows out as finished-good qty on WO completion. What "lives in" a WIP account at any moment is the cumulative in-progress cost attributable to that WO, not a SKU's inventory.

**What R1 prevents.** The qty divisor for a SKU's WAC computation is `SUM(posting_lines.qty SIGNED by debit/credit) WHERE account_id = $pool.account_id`. The filter is by `account.id`, not by `(sku, location, currency)`. This is load-bearing because:

- Two different SKUs (e.g., Steel Bar in raw, Steel Bracket in finished-goods) can share a location and currency. Aggregating qty across "all accounts for this (location, currency)" would sum Steel Bar qty into Steel Bracket's divisor — completely wrong.
- A WIP account at the same location is keyed by work order, not SKU. Its qty (where present) corresponds to in-process units of various SKUs being assembled, not to inventory of any single SKU. Aggregating WIP qty into a SKU's divisor is also wrong.

The footgun this rule guards against is some view or query that aggregates qty across `(sku, location, currency)` regardless of which account the qty lives on. The historical `stock_available` view in acct is one such offender: it sums qty for "this SKU at this location" by joining across multiple class-typed accounts (raw, wip, fg) and arrives at a number that is useful for operational stock visibility but useless as a WAC divisor.

The fix encoded in the schema: account identity is the filter. The framework's snapshot construction reads only the one pool's account.id when populating the qty divisor. Sibling accounts (different class, different SKU, WIP attached to a WO) are not summed in.

**Concurrent apply correctness from R4:** the value account is FOR UPDATE-locked before the divisor is read, so no concurrent apply produces a stale qty. **R7** requires that any document-level unit_cost snapshot field (e.g., on so_shipment_lines) is sourced from the same post-lock dispatcher output, not a pre-lock read.

### A.8 Credit-first SKU resolution

`_post_posting_lines_apply_event` resolves the SKU for cost_method dispatch and for `posting_lines_provisional` flagging from the CREDIT side of the posting, not the debit side. The credit side is the depletion source — the pool from which value is leaving.

For outbound events, the credit-side SKU's cost_method determines:
- Which dispatcher branch runs.
- Whether the posting gets flagged into `posting_lines_provisional`.

Debit-first COALESCE fallback (flagged as drift risk in acct-du2.4) routes to the wrong method on multi-output events with parent and outputs of different SKUs.

`_post_posting_lines_compute_amount` and `_post_posting_lines_apply_event` must use the same credit-first resolution to keep flagging in sync with dispatch.

---

## Appendix B: Glossary

| Term | Meaning |
|------|---------|
| Pool | A (credit_account, class, sku, location, currency, legal_entity) tuple. The unit of cost-state isolation. |
| Layer / cost layer | A row in `cost_layers` representing an inventory event (receipt, reversal, adjustment). Append-only. |
| Layer group | Group of related layers (original + adjustments + reversals) sharing `layer_group_id`. Effective qty derives from group sum minus depletions. |
| Depletion | A row in `cost_layer_depletions` recording consumption from a layer. Append-only. |
| Provisional row | A row in `posting_lines_provisional` marking a posting_line as having an at-apply-time provisional cost component, lifecycle-tracked. |
| Internal-chain | Variance routing for `inv_value_wip` pools drained to zero in-period; records variance on provisional but emits no posting_line. |
| Leaf depletion | Variance routing for raw/fg pools at the leaf of the chain; emits a variance posting_line. |
| Mixed parent/component | Variance routing when parent and component have different cost methods; emits via `variance_material_mixed`. |
| Identity dimension | A dimension framework-bundled with cost-method support (Lot, Unit, CostLayer, CostBook). New identity dimensions are framework changes. |
| Analytical dimension | A dimension EAV-extensible via `dimension_types` lookup (routing_op, project, cost_center). New analytical dimensions are data-only. |
| Trait dispatch | Cost methods implemented as Rust trait impls; suited to per-row procedural methods. |
| Registry dispatch | Cost methods implemented as plpgsql functions registered in `cost_method_strategies`; suited to set-based methods. |
| In-tx mode | Apply path that executes synchronously within the user's transaction. Default. |
| Queue mode | Apply path that enqueues to shmem, processed by elected committer. Opt-in via GUC. |
| Committer-tx | The sub-transaction the elected committer opens to INSERT a batch's rows. Independent of user-tx. |
| Compensation | A reversing posting_line emitted by recovery worker when user-tx aborts after committer-tx succeeded. |
